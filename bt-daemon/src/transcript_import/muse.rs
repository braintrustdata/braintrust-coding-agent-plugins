//! Streaming Muse export reader and correlation policy. Wire DTOs live in
//! muse::export_v1; synthesized hooks use the same typed events as live capture.
use super::{envelope, validate_session_id};
use crate::muse::export_v1::*;
use crate::muse::hooks::*;
use crate::muse::usage::{TokenUsage, UsageScope};
use crate::wire::Envelope;
use anyhow::{anyhow, bail, Context, Result};
use serde::de::{DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
#[cfg(test)]
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

#[derive(Default)]
struct RunState {
    active_model: Option<ModelState>,
    last_assistant: Option<String>,
    model_ordinal: u64,
    step_attempts: HashMap<u64, u64>,
}
struct ModelState {
    request_id: String,
    usage: Option<TokenUsage>,
    output: Option<ModelOutput>,
    response_id: Option<String>,
    completed_at: Option<i64>,
}
struct PendingTool {
    turn_id: String,
    tool: ToolStarted,
    completed: bool,
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
    metadata: ExportMetadata,
    tools: HashMap<String, PendingTool>,
    tool_errors: HashMap<String, String>,
    tool_tasks: HashMap<String, String>,
    run_metadata: HashMap<String, ExportMetadata>,
}
impl<F: FnMut(Envelope) -> Result<()>> Reader<'_, F> {
    fn context(&self, turn: Option<&str>, request: Option<&str>) -> HookContext {
        HookContext {
            session_id: Some(self.session_id.clone()),
            turn_id: turn.map(Into::into),
            request_id: request.map(Into::into),
            source: Some("muse_export_v1".into()),
            ..Default::default()
        }
    }
    fn emit(&mut self, ts: i64, event: MuseEvent) -> Result<()> {
        (self.emit)(envelope(
            "muse",
            self.version.clone(),
            &self.session_id,
            event.name().as_str(),
            ts,
            event.payload()?,
        ))
    }
    fn ensure_start(&mut self, ts: i64) -> Result<()> {
        if self.first_ts.is_none() {
            self.first_ts = Some(ts);
            let mut context = self.context(None, None);
            context.transcript_path = Some(self.path.to_string_lossy().into_owned());
            context.session = SessionMetadata {
                trajectory_id: self.session.trajectory_id.clone().into(),
                root_session_id: self.session.root_session_id.clone().into(),
                model: self.metadata.model_id.clone().into(),
                provider: self.metadata.provider_id.clone().into(),
                cwd: self.metadata.workspace_root.clone().into(),
                ..Default::default()
            };
            self.emit(
                ts,
                MuseEvent::SessionStart(Hook {
                    context,
                    body: Empty {},
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
        bounded: Option<InputSummary>,
        model_step: Option<u64>,
    ) -> Result<()> {
        let metadata = self
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
            output: None,
            response_id: None,
            completed_at: None,
        });
        let mut context = self.context(Some(run_id), Some(&request_id));
        context.session.model = metadata.model_id.into();
        context.session.provider = metadata.provider_id.into();
        context.session.cwd = self.metadata.workspace_root.clone().into();
        self.emit(
            ts,
            MuseEvent::PreLLMCall(Hook {
                context,
                body: ModelStarted {
                    request_record_id: native_request_id.into(),
                    details: ModelDetails {
                        step: model_step.into(),
                        ..Default::default()
                    },
                    input_summary: bounded.into(),
                    ..Default::default()
                },
            }),
        )
    }
    fn ensure_run(&mut self, run_id: &str, ts: i64) -> Result<()> {
        if self.runs.contains_key(run_id) {
            return Ok(());
        }
        self.runs.insert(run_id.to_owned(), RunState::default());
        self.emit(
            ts,
            MuseEvent::UserPromptSubmit(Hook {
                context: self.context(Some(run_id), None),
                body: PromptSubmitted { prompt: None },
            }),
        )
    }
    fn finish_model(&mut self, run_id: &str, ts: i64, error: Option<&str>) -> Result<()> {
        let Some(model) = self
            .runs
            .get_mut(run_id)
            .and_then(|r| r.active_model.take())
        else {
            return Ok(());
        };
        self.emit(
            model.completed_at.unwrap_or(ts),
            MuseEvent::PostLLMCall(Hook {
                context: self.context(Some(run_id), Some(&model.request_id)),
                body: ModelFinished {
                    usage: model.usage,
                    usage_scope: UsageScope::Model,
                    output_text_preview: model.output,
                    response_id: model.response_id.into(),
                    outcome: NativeOutcome {
                        error: if model.completed_at.is_some() {
                            None
                        } else {
                            error.map(|e| ErrorDetail::Text(e.into()))
                        },
                        ..Default::default()
                    },
                    ..Default::default()
                },
            }),
        )
    }
    fn process_task(&mut self, task: TaskEvent, at: Option<UnixMicros>) -> Result<()> {
        if let (Some(id), Some(call)) = (
            task.task_id.as_ref(),
            task.idempotency_key
                .as_deref()
                .and_then(|s| s.strip_prefix("tool:")),
        ) {
            self.tool_tasks.insert(id.clone(), call.into());
        }
        if matches!(task.kind, TaskEventKind::Rejected) {
            if let (Some(id), Some(reason)) = (
                task.origin.and_then(|o| o.tool_call_id),
                task.reason.as_ref(),
            ) {
                self.tool_errors.insert(id, reason.clone());
            }
        }
        if let Some(call) = task
            .task_id
            .as_ref()
            .and_then(|id| self.tool_tasks.get(id))
            .cloned()
        {
            match task.kind {
                TaskEventKind::Started => {
                    if let Some(tool) = self.tools.get(&call) {
                        let event = MuseEvent::PreToolUse(Hook {
                            context: self.context(Some(&tool.turn_id), None),
                            body: tool.tool.clone(),
                        });
                        self.emit(
                            at.context("Muse tool task has no recorded_at")?.millis(),
                            event,
                        )?;
                    }
                }
                TaskEventKind::Failed | TaskEventKind::Cancelled => {
                    self.tool_errors.insert(
                        call,
                        task.reason
                            .unwrap_or_else(|| "Muse tool task failed".into()),
                    );
                    if let Some(id) = task.task_id {
                        self.tool_tasks.remove(&id);
                    }
                }
                TaskEventKind::Completed => {
                    if let Some(tool) = self.tools.get_mut(&call) {
                        tool.completed = true;
                    }
                    if let Some(id) = task.task_id {
                        self.tool_tasks.remove(&id);
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
    fn process(&mut self, item: ExportRecord) -> Result<()> {
        let Some(native) = item.envelope else {
            return Ok(());
        };
        let at = item.recorded_at.or(native.recorded_at);
        let Some(payload) = native.payload else {
            return Ok(());
        };
        let (run_id, event) = match payload {
            NativePayload::RunModel { record } => {
                if let Some(record) = record {
                    self.run_metadata
                        .insert(record.run_stream.id, record.metadata);
                }
                return Ok(());
            }
            NativePayload::Metadata { record } => {
                if let Some(record) = record {
                    self.metadata = record;
                }
                return Ok(());
            }
            NativePayload::Task { event } => {
                if let Some(event) = event {
                    self.process_task(event, at)?;
                }
                return Ok(());
            }
            NativePayload::SessionEnd => {
                let ts = at
                    .context("Muse session_end record has no recorded_at timestamp")?
                    .millis();
                self.ensure_start(ts)?;
                self.current_session_ended = true;
                return Ok(());
            }
            NativePayload::Run {
                event: RunEvent::Ignored,
                ..
            }
            | NativePayload::Ignored => return Ok(()),
            NativePayload::Run { run_id, event } => (run_id, event),
        };
        if run_id.is_empty() {
            bail!("Muse run record has no run_id")
        }
        let ts = at
            .context("Muse run record has no recorded_at timestamp")?
            .millis();
        self.ensure_start(ts)?;
        self.current_session_ended = false;
        self.activity_count += 1;
        if !matches!(event, RunEvent::Started { .. }) {
            self.ensure_run(&run_id, ts)?;
        }
        match event {
            RunEvent::Started { prompt } => {
                if self.runs.contains_key(&run_id) {
                    bail!("Muse run {run_id} has duplicate started event")
                }
                self.runs.insert(run_id.clone(), RunState::default());
                self.emit(
                    ts,
                    MuseEvent::UserPromptSubmit(Hook {
                        context: self.context(Some(&run_id), None),
                        body: PromptSubmitted { prompt },
                    }),
                )?;
            }
            RunEvent::ModelInputTraceRecorded {
                request_record_id,
                model_step,
                bounded,
            } => {
                if self
                    .runs
                    .get(&run_id)
                    .and_then(|r| r.active_model.as_ref())
                    .is_some()
                {
                    self.finish_model(&run_id, ts, None)?;
                }
                self.start_model(&run_id, ts, Some(&request_record_id), bounded, model_step)?;
            }
            RunEvent::AssistantToolCallsCommitted {
                tool_calls,
                response_id,
            } => {
                let mut output_calls = Vec::new();
                for call in tool_calls {
                    if call.call_id.is_empty() {
                        bail!("Muse committed tool call has no call_id")
                    }
                    let args = call
                        .args
                        .unwrap_or(ToolArguments::Json(serde_json::Value::Null));
                    self.tools.insert(
                        call.call_id.clone(),
                        PendingTool {
                            turn_id: run_id.clone(),
                            tool: ToolStarted {
                                tool_use_id: Some(call.call_id.clone().into()),
                                tool_name: Some(call.name.clone()),
                                tool_input: Some(args.input()),
                            },
                            completed: false,
                        },
                    );
                    output_calls.push(AssistantToolCall {
                        id: call.call_id,
                        call_type: FunctionCallType::Function,
                        function: FunctionCall {
                            name: call.name,
                            arguments: args,
                        },
                    });
                }
                if let Some(model) = self
                    .runs
                    .get_mut(&run_id)
                    .and_then(|r| r.active_model.as_mut())
                {
                    model.output = Some(ModelOutput::Messages(vec![NativeMessage {
                        role: MessageRole::Assistant,
                        content: None,
                        tool_calls: Some(output_calls),
                        tool_call_id: None,
                        name: None,
                        extensions: BTreeMap::new(),
                    }]));
                    model.response_id = response_id;
                }
            }
            RunEvent::ToolResultBatchCommitted { results } => {
                for result in results {
                    if result.tool_call_id.is_empty() {
                        bail!("Muse tool result has no tool_call_id")
                    }
                    let tool = self
                        .tools
                        .remove(&result.tool_call_id)
                        .unwrap_or(PendingTool {
                            turn_id: run_id.clone(),
                            tool: ToolStarted {
                                tool_use_id: Some(result.tool_call_id.clone().into()),
                                ..Default::default()
                            },
                            completed: false,
                        });
                    let error = self.tool_errors.remove(&result.tool_call_id);
                    let context = self.context(Some(&tool.turn_id), None);
                    let body = ToolFinished {
                        tool: tool.tool,
                        tool_response: result.text,
                        outcome: NativeOutcome {
                            error: error.clone().map(ErrorDetail::Text),
                            ..Default::default()
                        },
                        ..Default::default()
                    };
                    let event = if error.is_some() {
                        MuseEvent::PostToolUseFailure(Hook { context, body })
                    } else if tool.completed {
                        MuseEvent::PostToolUse(Hook { context, body })
                    } else {
                        MuseEvent::PostToolBatch(Hook {
                            context,
                            body: ToolBatchFinished {
                                tool_calls: vec![body],
                            },
                        })
                    };
                    self.emit(ts, event)?;
                }
            }
            RunEvent::ModelFailureTerminalIntent { reason } => self.finish_model(
                &run_id,
                ts,
                Some(reason.as_deref().unwrap_or("Muse model failed")),
            )?,
            RunEvent::ModelResponseCreated { response_id } => {
                if let Some(model) = self
                    .runs
                    .get_mut(&run_id)
                    .and_then(|r| r.active_model.as_mut())
                {
                    model.response_id = response_id;
                }
            }
            RunEvent::ModelCompleted { usage } => {
                if self
                    .runs
                    .get(&run_id)
                    .and_then(|r| r.active_model.as_ref())
                    .is_none()
                {
                    self.start_model(&run_id, ts, None, None, None)?;
                }
                if let Some(model) = self
                    .runs
                    .get_mut(&run_id)
                    .and_then(|r| r.active_model.as_mut())
                {
                    model.usage = usage;
                    model.completed_at = Some(ts);
                }
            }
            RunEvent::ResourceUsageSampled => {}
            RunEvent::AssistantMessageCommitted { text, response_id } => {
                if self
                    .runs
                    .get(&run_id)
                    .and_then(|r| r.active_model.as_ref())
                    .is_none()
                {
                    self.start_model(&run_id, ts, None, None, None)?;
                }
                let run = self
                    .runs
                    .get_mut(&run_id)
                    .context("Muse model start did not create a run")?;
                run.last_assistant = Some(text.clone());
                let model = run
                    .active_model
                    .as_mut()
                    .context("Muse run has no active model")?;
                model.output = Some(ModelOutput::Text(text));
                model.response_id = response_id;
            }
            RunEvent::Terminal { terminal, reason } => {
                let error = (terminal != "completed").then(|| {
                    reason
                        .as_ref()
                        .and_then(ErrorDetail::render)
                        .unwrap_or_else(|| terminal.clone())
                });
                self.finish_model(&run_id, ts, error.as_deref())?;
                let run = self.runs.remove(&run_id);
                self.run_metadata.remove(&run_id);
                self.emit(
                    ts,
                    MuseEvent::Stop(Hook {
                        context: self.context(Some(&run_id), None),
                        body: TurnFinished {
                            last_assistant_message: run
                                .and_then(|r| r.last_assistant)
                                .map(ModelOutput::Text),
                            terminal: Some(terminal),
                            outcome: NativeOutcome {
                                error: error.map(ErrorDetail::Text),
                                ..Default::default()
                            },
                        },
                    }),
                )?;
            }
            RunEvent::Ignored => {}
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
        let ended = self.current_session_ended;
        if attach_snapshot && !ended {
            return Ok(false);
        }
        for run_id in self.runs.keys().cloned().collect::<Vec<_>>() {
            self.finish_model(&run_id, end_ts, Some("Run ended without a terminal event"))?;
            let run = self
                .runs
                .remove(&run_id)
                .context("Muse run disappeared while finalizing")?;
            self.emit(
                end_ts,
                MuseEvent::Stop(Hook {
                    context: self.context(Some(&run_id), None),
                    body: TurnFinished {
                        last_assistant_message: run.last_assistant.map(ModelOutput::Text),
                        outcome: NativeOutcome {
                            error: Some(ErrorDetail::Text(
                                "Run ended without a terminal event".into(),
                            )),
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                }),
            )?;
        }
        let error = if !ended {
            Some("Session has no terminal end after its latest activity")
        } else if self.terminated_abnormally {
            Some("Session terminated abnormally")
        } else {
            None
        };
        self.emit(
            end_ts,
            MuseEvent::SessionEnd(Hook {
                context: self.context(None, None),
                body: SessionFinished {
                    reason: (if ended {
                        self.session
                            .session_end
                            .as_ref()
                            .and_then(|e| e.exit_reason.clone())
                            .map(ErrorDetail::Text)
                    } else {
                        None
                    })
                    .into(),
                    outcome: NativeOutcome {
                        error: error.map(|e| ErrorDetail::Text(e.into())),
                        ..Default::default()
                    },
                },
            }),
        )?;
        Ok(ended)
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
        metadata: ExportMetadata::default(),
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
        .map_err(|error| anyhow!("parse Muse export {}: {error}", path.display()))?;
    de.end()
        .map_err(|error| anyhow!("parse Muse export {}: {error}", path.display()))?;
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
        loop {
            let item = seq.next_element::<ExportRecord>().map_err(|error| {
                serde::de::Error::custom(format!("event {}: {error}", index + 1))
            })?;
            let Some(item) = item else { break };
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
                {"recorded_at": 1_002_000, "envelope":{"payload":{"kind":"run","run_id":"run-1","event":{"kind":"resource_usage_sampled","usage":{"cpu_time_ms":2,"peak_rss_bytes":4096}}}}},
                {"recorded_at": 1_003_000, "envelope":{"payload":{"kind":"run","run_id":"run-1","event":{"kind":"terminal","terminal":"failed","reason":"provider failed"}}}}
            ]
        }));
        let events = envelopes(&path).unwrap();
        assert_eq!(events[2].payload["request_id"], "run-1:model:1");
        assert!(events[3].payload["usage"].is_null());
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
        assert!(format!("{error:#}").contains("event 1: missing field `run_id`"));
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
