//! Versioned Muse Code export-v1 reader. Retained frames can contain large
//! serialized payloads, so export events are decoded one record at a time.

use super::{envelope, validate_session_id};
use crate::wire::Envelope;
use anyhow::{anyhow, bail, Context, Result};
use serde::de::{DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

#[derive(Deserialize)]
struct Header {
    export_schema_version: u64,
    exporter_version: Option<Version>,
    sessions: Vec<Session>,
    #[serde(default)]
    session_terminated_abnormally: bool,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Version {
    String(String),
    Object { semver: String },
}

impl Version {
    fn into_string(self) -> String {
        match self {
            Self::String(value) | Self::Object { semver: value } => value,
        }
    }
}

#[derive(Clone, Deserialize)]
struct Session {
    session_id: String,
    trajectory_id: Option<String>,
    root_session_id: Option<String>,
    session_end: Option<SessionEnd>,
}

#[derive(Clone, Deserialize)]
struct SessionEnd {
    exit_reason: Option<String>,
}

#[derive(Deserialize)]
struct ExportRecord {
    recorded_at: Option<i64>,
    envelope: Option<NativeEnvelope>,
}

#[derive(Deserialize)]
struct NativeEnvelope {
    recorded_at: Option<i64>,
    payload: Option<NativePayload>,
}

#[derive(Deserialize)]
struct NativePayload {
    kind: Option<String>,
    run_id: Option<String>,
    event: Option<Value>,
    record: Option<Value>,
}

#[derive(Default)]
struct RunState {
    active_model: Option<ModelState>,
    last_assistant: Option<Value>,
    model_ordinal: u64,
    step_attempts: HashMap<u64, u64>,
}

struct ModelState {
    request_id: String,
    usage: Option<Value>,
    text: Option<Value>,
    response_id: Option<Value>,
    completed_at: Option<i64>,
}

struct Reader<'a, F> {
    path: &'a Path,
    session: Session,
    session_id: String,
    version: Option<String>,
    terminated_abnormally: bool,
    emit: F,
    runs: HashMap<String, RunState>,
    first_ts: Option<i64>,
    last_ts: Option<i64>,
    current_session_ended: bool,
    activity_count: usize,
    metadata: Value,
    tools: HashMap<String, Value>,
    tool_errors: HashMap<String, String>,
    tool_tasks: HashMap<String, String>,
    run_metadata: HashMap<String, Value>,
}

impl<F> Reader<'_, F>
where
    F: FnMut(Envelope) -> Result<()>,
{
    fn emit(&mut self, name: &str, ts: i64, payload: Value) -> Result<()> {
        (self.emit)(envelope(
            "muse",
            self.version.clone(),
            &self.session_id,
            name,
            ts,
            payload,
        ))
    }

    fn ensure_start(&mut self, ts: i64) -> Result<()> {
        if self.first_ts.is_none() {
            self.first_ts = Some(ts);
            self.emit(
                "SessionStart",
                ts,
                json!({
                    "session_id": self.session_id,
                    "hook_event_name": "SessionStart",
                    "source": "muse_export_v1",
                    "trajectory_id": self.session.trajectory_id,
                    "model": self.metadata.get("model_id"),
                    "provider": self.metadata.get("provider_id"),
                    "cwd": self.metadata.get("workspace_root"),
                    "root_session_id": self.session.root_session_id,
                    "transcript_path": self.path.to_string_lossy(),
                }),
            )?;
        }
        self.last_ts = Some(self.last_ts.unwrap_or(ts).max(ts));
        Ok(())
    }

    fn start_model(
        &mut self,
        run_id: &str,
        ts: i64,
        request_id: Option<&str>,
        input: Value,
        model_step: Option<u64>,
    ) -> Result<()> {
        let model_metadata = self
            .run_metadata
            .get(run_id)
            .unwrap_or(&self.metadata)
            .clone();
        let run = self.runs.entry(run_id.to_owned()).or_default();
        if run.active_model.is_some() {
            bail!("Muse run {run_id} starts another model request before the previous response")
        }
        let native_request_id = request_id.map(str::to_owned);
        let request_id = match (model_step, request_id) {
            (Some(step), _) => {
                let attempt = run.step_attempts.entry(step).or_default();
                *attempt += 1;
                format!("{run_id}:{step}:{attempt}")
            }
            (None, Some(id)) if !id.is_empty() => id.to_owned(),
            (None, Some(_)) => {
                bail!("Muse model_input_trace_recorded has an empty request_record_id")
            }
            (None, None) => {
                run.model_ordinal += 1;
                format!("{run_id}:model:{}", run.model_ordinal)
            }
        };
        run.active_model = Some(ModelState {
            request_id: request_id.clone(),
            usage: None,
            text: None,
            response_id: None,
            completed_at: None,
        });
        self.emit(
            "PreLLMCall",
            ts,
            json!({
                "session_id": self.session_id,
                "hook_event_name": "PreLLMCall",
                "turn_id": run_id,
                "request_id": request_id,
                "request_record_id": native_request_id,
                "step": model_step,
                "model": model_metadata.get("model_id"),
                "provider": model_metadata.get("provider_id"),
                "cwd": self.metadata.get("workspace_root"),
                "messages": input.get("messages"),
                "tools": input.get("tools"),
                "options": input.get("options"),
                "input_summary": input.get("bounded"),
                "source": "muse_export_v1",
            }),
        )
    }

    fn ensure_run(&mut self, run_id: &str, ts: i64) -> Result<()> {
        if self.runs.contains_key(run_id) {
            return Ok(());
        }
        self.runs.insert(run_id.to_owned(), RunState::default());
        self.emit(
            "UserPromptSubmit",
            ts,
            json!({
                "session_id": self.session_id,
                "hook_event_name": "UserPromptSubmit",
                "turn_id": run_id,
                "prompt": Value::Null,
                "source": "muse_export_v1",
            }),
        )
    }

    fn finish_model(&mut self, run_id: &str, ts: i64, error: Option<&str>) -> Result<()> {
        let Some(model) = self
            .runs
            .get_mut(run_id)
            .and_then(|run| run.active_model.take())
        else {
            return Ok(());
        };
        self.emit(
            "PostLLMCall",
            model.completed_at.unwrap_or(ts),
            json!({
                "session_id": self.session_id,
                "hook_event_name": "PostLLMCall",
                "turn_id": run_id,
                "request_id": model.request_id,
                "usage": model.usage,
                "usage_scope": "model",
                "output_text_preview": model.text,
                "response_id": model.response_id,
                "error": if model.completed_at.is_some() { None } else { error },
                "source": "muse_export_v1",
            }),
        )
    }

    fn process(&mut self, item: ExportRecord) -> Result<()> {
        let Some(native_envelope) = item.envelope else {
            return Ok(());
        };
        let Some(payload) = native_envelope.payload else {
            return Ok(());
        };
        if payload.kind.as_deref() == Some("run_model") {
            if let Some(record) = payload.record {
                if let Some(run) = record.pointer("/run_stream/id").and_then(Value::as_str) {
                    self.run_metadata.insert(run.to_owned(), record);
                }
            }
            return Ok(());
        }
        if payload.kind.as_deref() == Some("task") {
            if let Some(event) = &payload.event {
                let kind = event.get("kind").and_then(Value::as_str);
                let task = event.get("task_id").and_then(Value::as_str);
                if let (Some(task), Some(call)) = (
                    task,
                    event
                        .get("idempotency_key")
                        .and_then(Value::as_str)
                        .and_then(|key| key.strip_prefix("tool:")),
                ) {
                    self.tool_tasks.insert(task.to_owned(), call.to_owned());
                }
                if kind == Some("rejected") {
                    if let (Some(id), Some(reason)) = (
                        event
                            .pointer("/origin/tool_call_id")
                            .and_then(Value::as_str),
                        event.get("reason").and_then(Value::as_str),
                    ) {
                        self.tool_errors.insert(id.to_owned(), reason.to_owned());
                    }
                }
                if let Some(call) = task.and_then(|task| self.tool_tasks.get(task)).cloned() {
                    if kind == Some("started") {
                        if let Some(tool) = self.tools.get(&call).cloned() {
                            let ts = item
                                .recorded_at
                                .or(native_envelope.recorded_at)
                                .context("Muse tool task has no recorded_at")?
                                / 1000;
                            self.emit("PreToolUse", ts, tool)?;
                        }
                    } else if matches!(kind, Some("failed" | "cancelled" | "canceled")) {
                        let reason = event
                            .get("reason")
                            .and_then(Value::as_str)
                            .unwrap_or("Muse tool task failed");
                        self.tool_errors.insert(call, reason.to_owned());
                        if let Some(task) = task {
                            self.tool_tasks.remove(task);
                        }
                    } else if kind == Some("completed") {
                        if let Some(tool) = self.tools.get_mut(&call) {
                            tool["status"] = json!("completed");
                        }
                        if let Some(task) = task {
                            self.tool_tasks.remove(task);
                        }
                    }
                }
            }
            return Ok(());
        }
        if payload.kind.as_deref() == Some("metadata") {
            if let Some(record) = payload.record {
                self.metadata = record;
            }
            return Ok(());
        }
        let kind = payload
            .event
            .as_ref()
            .and_then(|event| event.get("kind"))
            .and_then(Value::as_str);
        if payload.kind.as_deref() == Some("run") && payload.event.is_some() && kind.is_none() {
            bail!("Muse run event has no kind");
        }
        let recognized = payload.kind.as_deref() == Some("session_end")
            || (payload.kind.as_deref() == Some("run")
                && matches!(
                    kind,
                    Some(
                        "started"
                            | "model_input_trace_recorded"
                            | "model_completed"
                            | "model_response_created"
                            | "assistant_tool_calls_committed"
                            | "tool_result_batch_committed"
                            | "model_failure_terminal_intent"
                            | "resource_usage_sampled"
                            | "assistant_message_committed"
                            | "terminal"
                    )
                ));
        if !recognized {
            return Ok(());
        }
        let micros = item
            .recorded_at
            .or(native_envelope.recorded_at)
            .ok_or_else(|| anyhow!("Muse {kind:?} record has no recorded_at timestamp"))?;
        let ts = micros / 1_000;
        self.ensure_start(ts)?;
        if payload.kind.as_deref() == Some("session_end") {
            self.current_session_ended = true;
            return Ok(());
        }
        self.current_session_ended = false;
        let event = payload
            .event
            .as_ref()
            .ok_or_else(|| anyhow!("Muse run record has no event"))?;
        let kind = kind.context("recognized Muse run event has no kind")?;
        let run_id = payload
            .run_id
            .as_deref()
            .filter(|id| !id.is_empty())
            .ok_or_else(|| anyhow!("Muse {kind} record has no run_id"))?
            .to_owned();
        self.activity_count += 1;
        if kind != "started" {
            self.ensure_run(&run_id, ts)?;
        }
        match kind {
            "started" => {
                if self.runs.contains_key(&run_id) {
                    bail!("Muse run {run_id} has duplicate started event")
                }
                self.runs.insert(run_id.clone(), RunState::default());
                self.emit(
                    "UserPromptSubmit",
                    ts,
                    json!({
                        "session_id": self.session_id,
                        "hook_event_name": "UserPromptSubmit",
                        "turn_id": run_id,
                        "prompt": event.get("prompt"),
                        "source": "muse_export_v1",
                    }),
                )?;
            }
            "model_input_trace_recorded" => {
                let request_id = event
                    .get("request_record_id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        anyhow!("Muse model_input_trace_recorded has no request_record_id")
                    })?;
                if self
                    .runs
                    .get(&run_id)
                    .and_then(|run| run.active_model.as_ref())
                    .is_some()
                {
                    self.finish_model(&run_id, ts, None)?;
                }
                self.start_model(
                    &run_id,
                    ts,
                    Some(request_id),
                    json!({"bounded":event.get("bounded")}),
                    event.get("model_step").and_then(Value::as_u64),
                )?;
            }
            "assistant_tool_calls_committed" => {
                let calls = event
                    .get("tool_calls")
                    .and_then(Value::as_array)
                    .context("Muse assistant_tool_calls_committed has no tool_calls array")?;
                let mut output_calls = Vec::new();
                for call in calls {
                    let id = call
                        .get("call_id")
                        .and_then(Value::as_str)
                        .filter(|id| !id.is_empty())
                        .context("Muse committed tool call has no call_id")?;
                    let name = call
                        .get("name")
                        .and_then(Value::as_str)
                        .context("Muse committed tool call has no name")?;
                    let args = call.get("args").cloned().unwrap_or(Value::Null);
                    let input = if let Some(args) = args.as_str() {
                        serde_json::from_str(args).unwrap_or_else(|_| args.into())
                    } else {
                        args.clone()
                    };
                    let payload = json!({"turn_id":run_id,"tool_use_id":id,"tool_name":name,"tool_input":input,"source":"muse_export_v1"});
                    self.tools.insert(id.to_owned(), payload.clone());

                    output_calls.push(json!({"id":id,"type":"function","function":{"name":name,"arguments":args}}));
                }
                if let Some(model) = self
                    .runs
                    .get_mut(&run_id)
                    .and_then(|run| run.active_model.as_mut())
                {
                    model.text = Some(json!([{"role":"assistant","tool_calls":output_calls}]));
                    model.response_id = event.get("response_id").cloned();
                }
            }
            "tool_result_batch_committed" => {
                let results = event
                    .get("results")
                    .and_then(Value::as_array)
                    .context("Muse tool_result_batch_committed has no results array")?;
                for result in results {
                    let id = result
                        .get("tool_call_id")
                        .and_then(Value::as_str)
                        .filter(|id| !id.is_empty())
                        .context("Muse tool result has no tool_call_id")?;
                    let mut payload = self.tools.remove(id).unwrap_or_else(
                        || json!({"turn_id":run_id,"tool_use_id":id,"source":"muse_export_v1"}),
                    );
                    payload["tool_response"] = result.get("text").cloned().unwrap_or(Value::Null);
                    let error = self.tool_errors.remove(id);
                    payload["error"] = json!(error);
                    if error.is_some() {
                        self.emit("PostToolUseFailure", ts, payload)?;
                    } else if payload.get("status").and_then(Value::as_str) == Some("completed") {
                        self.emit("PostToolUse", ts, payload)?;
                    } else {
                        self.emit("PostToolBatch", ts, json!({"turn_id":run_id,"tool_calls":[payload],"source":"muse_export_v1"}))?;
                    }
                }
            }
            "model_failure_terminal_intent" => {
                let reason = event
                    .get("reason")
                    .and_then(Value::as_str)
                    .unwrap_or("Muse model failed");
                self.finish_model(&run_id, ts, Some(reason))?;
            }
            "model_response_created" => {
                if let Some(model) = self
                    .runs
                    .get_mut(&run_id)
                    .and_then(|run| run.active_model.as_mut())
                {
                    model.response_id = event.get("response_id").cloned();
                }
            }
            "model_completed" => {
                if self
                    .runs
                    .get(&run_id)
                    .and_then(|run| run.active_model.as_ref())
                    .is_none()
                {
                    self.start_model(&run_id, ts, None, Value::Null, None)?;
                }
                if let Some(model) = self
                    .runs
                    .get_mut(&run_id)
                    .and_then(|run| run.active_model.as_mut())
                {
                    model.usage = event.get("usage").cloned();
                    model.completed_at = Some(ts);
                }
            }
            "resource_usage_sampled" => {
                if let Some(model) = self
                    .runs
                    .get_mut(&run_id)
                    .and_then(|run| run.active_model.as_mut())
                {
                    if model.usage.is_none() {
                        model.usage = event.get("usage").cloned();
                    }
                }
            }
            "assistant_message_committed" => {
                let text = event
                    .get("text")
                    .cloned()
                    .ok_or_else(|| anyhow!("Muse assistant_message_committed has no text"))?;
                if self
                    .runs
                    .get(&run_id)
                    .and_then(|run| run.active_model.as_ref())
                    .is_none()
                {
                    self.start_model(&run_id, ts, None, Value::Null, None)?;
                }
                let run = self
                    .runs
                    .get_mut(&run_id)
                    .context("Muse model start did not create a run")?;
                run.last_assistant = Some(text.clone());
                run.active_model
                    .as_mut()
                    .context("Muse run has no active model")?
                    .text = Some(text);
                if let Some(model) = run.active_model.as_mut() {
                    model.response_id = event.get("response_id").cloned();
                }
            }
            "terminal" => {
                let terminal = event
                    .get("terminal")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow!("Muse terminal record has no terminal status"))?;
                let error = (terminal != "completed").then(|| {
                    event
                        .get("reason")
                        .filter(|reason| !reason.is_null())
                        .map(|reason| {
                            reason
                                .as_str()
                                .map(str::to_owned)
                                .unwrap_or_else(|| reason.to_string())
                        })
                        .unwrap_or_else(|| terminal.to_owned())
                });
                self.finish_model(&run_id, ts, error.as_deref())?;
                let run = self.runs.remove(&run_id);
                self.run_metadata.remove(&run_id);
                self.emit(
                    "Stop",
                    ts,
                    json!({
                        "session_id": self.session_id,
                        "hook_event_name": "Stop",
                        "turn_id": run_id,
                        "last_assistant_message": run.and_then(|run| run.last_assistant),
                        "terminal": terminal,
                        "error": error,
                        "source": "muse_export_v1",
                    }),
                )?;
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    fn finish(mut self, attach_snapshot: bool) -> Result<bool> {
        if self.activity_count == 0 {
            if attach_snapshot {
                return Ok(self.current_session_ended);
            }
            bail!(
                "Muse export {} has no traceable run events",
                self.path.display()
            );
        }
        let end_ts = self
            .last_ts
            .ok_or_else(|| anyhow!("Muse export {} has no timestamps", self.path.display()))?;
        let current_session_ended = self.current_session_ended;
        if attach_snapshot && !current_session_ended {
            return Ok(false);
        }
        for run_id in self.runs.keys().cloned().collect::<Vec<_>>() {
            self.finish_model(&run_id, end_ts, Some("Run ended without a terminal event"))?;
            let run = self
                .runs
                .remove(&run_id)
                .context("Muse run disappeared while finalizing")?;
            self.emit(
                "Stop",
                end_ts,
                json!({
                    "session_id": self.session_id,
                    "hook_event_name": "Stop",
                    "turn_id": run_id,
                    "last_assistant_message": run.last_assistant,
                    "error": "Run ended without a terminal event",
                    "source": "muse_export_v1",
                }),
            )?;
        }
        self.emit("SessionEnd", end_ts, json!({
            "session_id": self.session_id,
            "hook_event_name": "SessionEnd",
            "reason": current_session_ended.then(|| self.session.session_end.as_ref().and_then(|end| end.exit_reason.as_deref())).flatten(),
            "error": if !current_session_ended {
                Some("Session has no terminal end after its latest activity")
            } else if self.terminated_abnormally {
                Some("Session terminated abnormally")
            } else {
                None
            },
            "source": "muse_export_v1",
        }))?;
        Ok(current_session_ended)
    }
}

#[cfg(test)]
pub(crate) fn envelopes(path: &Path) -> Result<Vec<Envelope>> {
    let header = read_header(path)?;
    let session = single_session(&header, path)?;
    envelopes_for_session(path, &session.session_id)
}

pub(crate) fn session_id(path: &Path) -> Result<String> {
    let header = read_header(path)?;
    Ok(single_session(&header, path)?.session_id.clone())
}

#[cfg(test)]
pub(crate) fn envelopes_for_session(path: &Path, expected_id: &str) -> Result<Vec<Envelope>> {
    let mut events = Vec::new();
    for_each_envelope_for_session(path, expected_id, |event| {
        events.push(event);
        Ok(())
    })?;
    Ok(events)
}

#[cfg(test)]
pub(crate) fn for_each_envelope_for_session<F>(
    path: &Path,
    expected_id: &str,
    emit: F,
) -> Result<()>
where
    F: FnMut(Envelope) -> Result<()>,
{
    for_each_snapshot_envelope_for_session(path, expected_id, false, emit).map(|_| ())
}

/// Parse a complete export without manufacturing terminal events for an
/// active attach snapshot. Returns whether Muse actually ended the session.
pub(crate) fn for_each_snapshot_envelope_for_session<F>(
    path: &Path,
    expected_id: &str,
    attach_snapshot: bool,
    emit: F,
) -> Result<bool>
where
    F: FnMut(Envelope) -> Result<()>,
{
    validate_session_id(expected_id)?;
    let header = read_header(path)?;
    if header.export_schema_version != 1 {
        bail!(
            "unsupported Muse export schema {} in {}; expected version 1",
            header.export_schema_version,
            path.display()
        );
    }
    let session = single_session(&header, path)?.clone();
    if session.session_id != expected_id {
        bail!(
            "Muse export {} contains session {}, expected {expected_id}",
            path.display(),
            session.session_id
        );
    }
    let mut reader = Reader {
        path,
        session,
        session_id: expected_id.to_owned(),
        version: header.exporter_version.map(Version::into_string),
        terminated_abnormally: header.session_terminated_abnormally,
        emit,
        runs: HashMap::new(),
        first_ts: None,
        last_ts: None,
        current_session_ended: false,
        activity_count: 0,
        metadata: json!({}),
        tools: HashMap::new(),
        tool_errors: HashMap::new(),
        tool_tasks: HashMap::new(),
        run_metadata: HashMap::new(),
    };
    let mut de = serde_json::Deserializer::from_reader(BufReader::new(
        File::open(path).with_context(|| format!("read Muse export {}", path.display()))?,
    ));
    EventsSeed(&mut reader)
        .deserialize(&mut de)
        .with_context(|| format!("parse Muse export {}", path.display()))?;
    de.end()
        .with_context(|| format!("parse Muse export {}", path.display()))?;
    reader.finish(attach_snapshot)
}

fn read_header(path: &Path) -> Result<Header> {
    serde_json::from_reader(BufReader::new(
        File::open(path).with_context(|| format!("read Muse export {}", path.display()))?,
    ))
    .with_context(|| format!("parse Muse export header {}", path.display()))
}

fn single_session<'a>(header: &'a Header, path: &Path) -> Result<&'a Session> {
    if header.sessions.len() != 1 {
        bail!(
            "Muse export {} must contain exactly one session, found {}",
            path.display(),
            header.sessions.len()
        );
    }
    let session = &header.sessions[0];
    validate_session_id(&session.session_id)?;
    Ok(session)
}

struct EventsSeed<'a, 'b, F>(&'a mut Reader<'b, F>);

impl<'de, F> DeserializeSeed<'de> for EventsSeed<'_, '_, F>
where
    F: FnMut(Envelope) -> Result<()>,
{
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(EventsVisitor(self.0))
    }
}

struct EventsVisitor<'a, 'b, F>(&'a mut Reader<'b, F>);

impl<'de, F> Visitor<'de> for EventsVisitor<'_, '_, F>
where
    F: FnMut(Envelope) -> Result<()>,
{
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a Muse export object")
    }

    fn visit_map<M>(self, mut map: M) -> std::result::Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut found_events = false;
        while let Some(key) = map.next_key::<String>()? {
            if key == "events" {
                if found_events {
                    return Err(serde::de::Error::custom("duplicate Muse events array"));
                }
                found_events = true;
                map.next_value_seed(RecordsSeed(self.0))?;
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }
        if !found_events {
            return Err(serde::de::Error::custom("Muse export has no events array"));
        }
        Ok(())
    }
}

struct RecordsSeed<'a, 'b, F>(&'a mut Reader<'b, F>);

impl<'de, F> DeserializeSeed<'de> for RecordsSeed<'_, '_, F>
where
    F: FnMut(Envelope) -> Result<()>,
{
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(RecordsVisitor(self.0))
    }
}

struct RecordsVisitor<'a, 'b, F>(&'a mut Reader<'b, F>);

impl<'de, F> Visitor<'de> for RecordsVisitor<'_, '_, F>
where
    F: FnMut(Envelope) -> Result<()>,
{
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("Muse event records")
    }

    fn visit_seq<S>(self, mut seq: S) -> std::result::Result<Self::Value, S::Error>
    where
        S: SeqAccess<'de>,
    {
        let mut index = 0;
        while let Some(item) = seq.next_element::<ExportRecord>()? {
            self.0.process(item).map_err(|error| {
                serde::de::Error::custom(format!("event {}: {error:#}", index + 1))
            })?;
            index += 1;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_export(value: Value) -> (tempfile::TempDir, std::path::PathBuf) {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("export.json");
        std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
        (temp, path)
    }

    #[test]
    fn real_export_shapes_preserve_usage_terminal_and_session_end_time() {
        let (_temp, path) = write_export(json!({
            "export_schema_version": 1,
            "exporter_version": {"semver":"1.1.1"},
            "sessions": [{"session_id":"session-1", "trajectory_id":"t", "root_session_id":"session-1", "session_end":{"exit_reason":"clean"}}],
            "events": [
                {"recorded_at": 1_000_000, "envelope":{"payload":{"kind":"run","run_id":"run-1","event":{"kind":"started","prompt":"hello"}}}},
                {"recorded_at": 1_001_000, "envelope":{"payload":{"kind":"run","run_id":"run-1","event":{"kind":"model_input_trace_recorded","request_record_id":"request-1","bounded":{"message":"hello"}}}}},
                {"recorded_at": 1_002_000, "envelope":{"payload":{"kind":"run","run_id":"run-1","event":{"kind":"model_completed","usage":{"input_tokens":1}}}}},
                {"recorded_at": 1_003_000, "envelope":{"payload":{"kind":"run","run_id":"run-1","event":{"kind":"assistant_message_committed","text":"world"}}}},
                {"recorded_at": 1_004_000, "envelope":{"payload":{"kind":"run","run_id":"run-1","event":{"kind":"terminal","terminal":"completed"}}}},
                {"recorded_at": 1_009_000, "envelope":{"payload":{"kind":"session_end"}}}
            ]
        }));
        let events = envelopes_for_session(&path, "session-1").unwrap();
        assert_eq!(
            events
                .iter()
                .map(|event| event.event.as_str())
                .collect::<Vec<_>>(),
            [
                "SessionStart",
                "UserPromptSubmit",
                "PreLLMCall",
                "PostLLMCall",
                "Stop",
                "SessionEnd"
            ]
        );
        assert_eq!(events[3].ts_ms, 1002);
        assert_eq!(events[3].payload["usage"]["input_tokens"], 1);
        assert_eq!(events[3].payload["output_text_preview"], "world");
        assert_eq!(events[5].ts_ms, 1009);
    }

    #[test]
    fn native_model_steps_match_live_hook_identity_and_distinguish_retries() {
        let (_temp, path) = write_export(json!({
            "export_schema_version":1,
            "sessions":[{"session_id":"session-1"}],
            "events":[
                {"recorded_at":999000,"envelope":{"payload":{"kind":"metadata","record":{"model_id":"native-model","provider_id":"native-provider","workspace_root":"/workspace"}}}},
                {"recorded_at":1000000,"envelope":{"payload":{"kind":"run","run_id":"run-1","event":{"kind":"started","prompt":"hello"}}}},
                {"recorded_at":1001000,"envelope":{"payload":{"kind":"run","run_id":"run-1","event":{"kind":"model_input_trace_recorded","model_step":0,"request_record_id":"uuid-a","bounded":{"request_message_count":2}}}}},
                {"recorded_at":1002000,"envelope":{"payload":{"kind":"run","run_id":"run-1","event":{"kind":"model_input_trace_recorded","model_step":0,"request_record_id":"uuid-b"}}}},
                {"recorded_at":1003000,"envelope":{"payload":{"kind":"run","run_id":"run-1","event":{"kind":"assistant_message_committed","text":"hello","response_id":"provider-response"}}}},
                {"recorded_at":1004000,"envelope":{"payload":{"kind":"run","run_id":"run-1","event":{"kind":"terminal","terminal":"completed"}}}}
            ]
        }));
        let events = envelopes(&path).unwrap();
        let starts: Vec<_> = events.iter().filter(|e| e.event == "PreLLMCall").collect();
        assert_eq!(starts[0].payload["model"], "native-model");
        assert_eq!(starts[0].payload["provider"], "native-provider");
        assert_eq!(events[0].payload["cwd"], "/workspace");
        assert_eq!(starts[0].payload["request_id"], "run-1:0:1");
        assert_eq!(starts[1].payload["request_id"], "run-1:0:2");
        assert_eq!(starts[0].payload["request_record_id"], "uuid-a");
        assert!(starts[0].payload["messages"].is_null());
        assert_eq!(
            starts[0].payload["input_summary"]["request_message_count"],
            2
        );
        let post = events.iter().rfind(|e| e.event == "PostLLMCall").unwrap();
        assert_eq!(post.payload["response_id"], "provider-response");
    }

    #[test]
    fn captured_modern_export_imports_native_tools_models_and_usage() {
        // Reduced native 1.4.11 export captured against a deterministic Responses
        // endpoint. Session/turn/task ids, paths and timestamps are sanitized.
        let (_temp, path) = write_export(
            serde_json::from_str(include_str!("../../tests/fixtures/muse-export-1.4.11.json"))
                .unwrap(),
        );
        let events = envelopes(&path).unwrap();
        let pre = events.iter().find(|e| e.event == "PreToolUse").unwrap();
        let post = events.iter().find(|e| e.event == "PostToolUse").unwrap();
        assert_eq!(pre.payload["tool_use_id"], "call_muse_1");
        assert_eq!(pre.payload["tool_input"]["command"], "printf MUSE_TOOL_OK");
        assert_eq!(post.payload["tool_response"], "MUSE_TOOL_OK");
        assert!(pre.ts_ms < post.ts_ms);
        let registry = crate::translate::Registry::default_agents();
        let mut translator = registry.create("muse", "muse-export-fixture");
        let ctx = crate::translate::SessionCtx {
            session_id: "muse-export-fixture".into(),
            config: None,
        };
        let mut rows = Vec::new();
        for event in &events {
            rows.extend(translator.handle(event, &ctx).unwrap());
        }
        assert_eq!(rows.iter().filter(|op|matches!(op,crate::translate::SpanOp::Insert(row) if row.span_type == crate::translate::SpanType::Tool)).count(),1);
        assert_eq!(rows.iter().filter(|op|matches!(op,crate::translate::SpanOp::Insert(row) if row.span_type == crate::translate::SpanType::Llm)).count(),2);
        let root = crate::ids::span_id(
            &crate::ids::session_namespace("muse", "muse-export-fixture"),
            "root",
        );
        assert!(rows.iter().any(|op|matches!(op,crate::translate::SpanOp::Merge(row) if row.span_id == root && row.metrics.as_ref().is_some_and(|m|m["prompt_tokens"] == 20 && m["completion_tokens"] == 10 && m["tokens"] == 30))));
        assert!(rows.iter().any(|op|matches!(op,crate::translate::SpanOp::Merge(row) if row.output == Some(json!("MUSE_TOOL_OK")) && row.metadata.as_ref().is_some_and(|m|m["tool_outcome"] == "success"))));
    }

    #[test]
    fn captured_failed_tool_task_preserves_reason_and_output() {
        let (_temp, path) = write_export(
            serde_json::from_str(include_str!(
                "../../tests/fixtures/muse-export-tool-failure-1.4.11.json"
            ))
            .unwrap(),
        );
        let events = envelopes(&path).unwrap();
        let failed = events
            .iter()
            .find(|e| e.event == "PostToolUseFailure")
            .expect("native failed task has a failure envelope");
        assert_eq!(failed.payload["tool_use_id"], "call_muse_failure");
        assert_eq!(
            failed.payload["error"],
            "exit status: 7\nstdout:\nMUSE_TOOL_FAILURE"
        );
        assert_eq!(
            failed.payload["tool_response"],
            "tool failed: exit status: 7\nstdout:\nMUSE_TOOL_FAILURE"
        );
    }

    #[test]
    fn rejected_tool_and_model_failures_keep_explicit_native_reasons() {
        let (_temp, path) = write_export(
            json!({"export_schema_version":1,"sessions":[{"session_id":"failure-fixture"}],"events":[
                {"recorded_at":1000000,"envelope":{"payload":{"kind":"run","run_id":"turn","event":{"kind":"started","prompt":"hello"}}}},
                {"recorded_at":1001000,"envelope":{"payload":{"kind":"run","run_id":"turn","event":{"kind":"model_input_trace_recorded","model_step":0,"request_record_id":"uuid"}}}},
                {"recorded_at":1002000,"envelope":{"payload":{"kind":"run","run_id":"turn","event":{"kind":"model_failure_terminal_intent","reason":"provider connection failed"}}}},
                {"recorded_at":1003000,"envelope":{"payload":{"kind":"run","run_id":"turn","event":{"kind":"assistant_tool_calls_committed","tool_calls":[{"call_id":"call","name":"unknown","args":"{}"}]}}}},
                {"recorded_at":1004000,"envelope":{"payload":{"kind":"task","run_id":"turn","event":{"kind":"rejected","origin":{"tool_call_id":"call"},"reason":"unknown tool `unknown`"}}}},
                {"recorded_at":1005000,"envelope":{"payload":{"kind":"run","run_id":"turn","event":{"kind":"tool_result_batch_committed","results":[{"tool_call_id":"call","text":"unavailable"}]}}}},
                {"recorded_at":1006000,"envelope":{"payload":{"kind":"run","run_id":"turn","event":{"kind":"terminal","terminal":"failed","reason":"provider connection failed"}}}}
            ]}),
        );
        let events = envelopes(&path).unwrap();
        assert!(events.iter().any(
            |e| e.event == "PostLLMCall" && e.payload["error"] == "provider connection failed"
        ));
        assert!(events
            .iter()
            .any(|e| e.event == "PostToolUseFailure"
                && e.payload["error"] == "unknown tool `unknown`"));
    }

    #[test]
    fn sparse_export_has_deterministic_model_and_failure() {
        let (_temp, path) = write_export(json!({
            "export_schema_version": 1,
            "sessions": [{"session_id":"session-1"}],
            "events": [
                {"recorded_at": 1_001_000, "envelope":{"payload":{"kind":"run","run_id":"run-1","event":{"kind":"assistant_message_committed","text":"partial"}}}},
                {"recorded_at": 1_002_000, "envelope":{"payload":{"kind":"run","run_id":"run-1","event":{"kind":"resource_usage_sampled","usage":{"output_tokens":2}}}}},
                {"recorded_at": 1_003_000, "envelope":{"payload":{"kind":"run","run_id":"run-1","event":{"kind":"terminal","terminal":"failed","reason":"provider failed"}}}}
            ]
        }));
        let events = envelopes(&path).unwrap();
        assert_eq!(events[2].payload["request_id"], "run-1:model:1");
        assert_eq!(events[3].payload["usage"]["output_tokens"], 2);
        assert_eq!(events[3].payload["error"], "provider failed");
        assert_eq!(events[4].payload["error"], "provider failed");
    }

    #[test]
    fn identity_and_malformed_recognized_records_fail_with_context() {
        let (_temp, path) = write_export(json!({
            "export_schema_version": 1,
            "sessions": [{"session_id":"session-1"}],
            "events": [{"recorded_at":1_000_000,"envelope":{"payload":{"kind":"run","event":{"kind":"started"}}}}]
        }));
        assert!(envelopes_for_session(&path, "session-2")
            .unwrap_err()
            .to_string()
            .contains("expected session-2"));
        let error = envelopes(&path).unwrap_err();
        assert!(format!("{error:#}").contains("event 1: Muse started record has no run_id"));
    }

    #[test]
    fn resumed_active_export_does_not_reuse_an_old_session_end() {
        let (_temp, path) = write_export(json!({
            "export_schema_version": 1,
            "sessions": [{"session_id":"session-1", "session_end":{"exit_reason":"clean"}}],
            "events": [
                {"recorded_at": 1_000_000, "envelope":{"payload":{"kind":"run","run_id":"run-1","event":{"kind":"started","prompt":"first"}}}},
                {"recorded_at": 1_001_000, "envelope":{"payload":{"kind":"run","run_id":"run-1","event":{"kind":"terminal","terminal":"completed"}}}},
                {"recorded_at": 1_002_000, "envelope":{"payload":{"kind":"session_end"}}},
                {"recorded_at": 1_003_000, "envelope":{"payload":{"kind":"run","run_id":"run-2","event":{"kind":"started","prompt":"second"}}}}
            ]
        }));
        let events = envelopes(&path).unwrap();
        let end = events.last().unwrap();
        assert_eq!(end.event, "SessionEnd");
        assert_eq!(end.ts_ms, 1003);
        assert_eq!(end.payload["reason"], Value::Null);
        assert_eq!(
            end.payload["error"],
            "Session has no terminal end after its latest activity"
        );
    }

    #[test]
    fn active_snapshot_does_not_invent_terminal_events() {
        let (_temp, path) = write_export(json!({
            "export_schema_version": 1,
            "sessions": [{"session_id":"session-1"}],
            "events": [
                {"recorded_at": 1_000_000, "envelope":{"payload":{"kind":"run","run_id":"run-1","event":{"kind":"started","prompt":"hello"}}}},
                {"recorded_at": 1_001_000, "envelope":{"payload":{"kind":"run","run_id":"run-1","event":{"kind":"model_input_trace_recorded","request_record_id":"request-1"}}}}
            ]
        }));
        let mut events = Vec::new();
        let ended = for_each_snapshot_envelope_for_session(&path, "session-1", true, |event| {
            events.push(event.event);
            Ok(())
        })
        .unwrap();
        assert!(!ended);
        assert_eq!(events, ["SessionStart", "UserPromptSubmit", "PreLLMCall"]);
    }

    #[test]
    fn empty_active_snapshot_waits_for_first_run() {
        let (_temp, path) = write_export(json!({
            "export_schema_version": 1,
            "sessions": [{"session_id":"session-1"}],
            "events": []
        }));
        let mut events = Vec::new();
        let ended = for_each_snapshot_envelope_for_session(&path, "session-1", true, |event| {
            events.push(event);
            Ok(())
        })
        .unwrap();
        assert!(!ended);
        assert!(events.is_empty());
    }
}
