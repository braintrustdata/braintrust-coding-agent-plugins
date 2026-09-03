//! Google Antigravity hook and transcript translator.
//!
//! Native hooks own lifecycle timing and correlation (`invocationNum` and
//! `stepIdx`). The append-only full transcript supplies the actual user/model
//! messages and tool details. Hook capture records transcript byte boundaries,
//! so replay observes exactly the records that existed when each hook fired.

use super::git::GitMetadataCache;
use super::tool::{with_tool_approval, ToolApproval};
use super::{AgentTranslator, SessionCtx, SpanOp, SpanRow, SpanType, TranslatorFactory};
use crate::ids;
use crate::wire::Envelope;
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::io::{BufRead, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;

pub struct AntigravityTranslatorFactory {
    git: Arc<GitMetadataCache>,
}

impl AntigravityTranslatorFactory {
    pub(super) fn new(git: Arc<GitMetadataCache>) -> Self {
        Self { git }
    }
}

impl TranslatorFactory for AntigravityTranslatorFactory {
    fn source(&self) -> &str {
        "antigravity"
    }

    fn create(&self, session_id: &str) -> Box<dyn AgentTranslator> {
        Box::new(AntigravityTranslator::new(session_id, self.git.clone()))
    }
}

struct Turn {
    span_id: String,
    number: u32,
    start_ms: i64,
    last_output: Option<Value>,
}

struct Invocation {
    span_id: String,
    parent_span_id: String,
    history_start: usize,
    record_start: usize,
}

struct PendingTool {
    span_id: String,
    parent_span_id: String,
    name: String,
}

struct AntigravityTranslator {
    session_id: String,
    session_span_id: String,
    root_span_id: String,
    root_open: bool,
    root_ended: bool,
    turn: Option<Turn>,
    turn_count: u32,
    transcript_offsets: HashMap<String, u64>,
    records: Vec<Value>,
    records_by_step: HashMap<i64, Value>,
    history: Vec<Value>,
    invocations: HashMap<i64, Invocation>,
    tools: HashMap<i64, PendingTool>,
    last_ts_ms: i64,
    git: Arc<GitMetadataCache>,
}

impl AntigravityTranslator {
    fn new(session_id: &str, git: Arc<GitMetadataCache>) -> Self {
        let root = ids::span_id(session_id, "root");
        Self {
            session_id: session_id.to_string(),
            session_span_id: root.clone(),
            root_span_id: root,
            root_open: false,
            root_ended: false,
            turn: None,
            turn_count: 0,
            transcript_offsets: HashMap::new(),
            records: Vec::new(),
            records_by_step: HashMap::new(),
            history: Vec::new(),
            invocations: HashMap::new(),
            tools: HashMap::new(),
            last_ts_ms: 0,
            git,
        }
    }

    fn ensure_root(&mut self, event: &Envelope, ctx: &SessionCtx, ops: &mut Vec<SpanOp>) {
        if self.root_open {
            return;
        }
        self.root_open = true;
        let (parent_span_id, external_root_span_id) = ctx
            .config
            .as_ref()
            .map(|config| config.attached_span_ids())
            .unwrap_or_default();
        if let Some(external_root) = external_root_span_id {
            self.root_span_id = external_root;
        }

        let workspace = event
            .payload
            .get("workspacePaths")
            .and_then(Value::as_array)
            .and_then(|paths| paths.first())
            .and_then(Value::as_str);
        let mut metadata = ctx
            .config
            .as_ref()
            .and_then(|config| config.additional_metadata.clone())
            .and_then(|value| value.as_object().cloned())
            .unwrap_or_default();
        metadata.retain(|key, _| !key.starts_with("_bt_"));
        metadata.insert("session_id".into(), json!(ctx.session_id));
        metadata.insert("conversation_id".into(), json!(ctx.session_id));
        metadata.insert("source".into(), json!("antigravity"));
        if let Some(model) = string_field(&event.payload, "modelName") {
            metadata.insert("model".into(), json!(model));
        }
        if let Some(workspaces) = event.payload.get("workspacePaths") {
            metadata.insert("workspace_paths".into(), workspaces.clone());
        }
        if let Some(path) = string_field(&event.payload, "artifactDirectoryPath") {
            metadata.insert("artifact_directory_path".into(), json!(path));
        }
        if let Some(version) = &event.source_version {
            metadata.insert("antigravity_version".into(), json!(version));
        }

        let label = workspace
            .and_then(|path| Path::new(path).file_name())
            .and_then(|name| name.to_str())
            .filter(|name| !name.is_empty())
            .unwrap_or("session");
        ops.push(SpanOp::Insert(SpanRow {
            span_id: self.session_span_id.clone(),
            root_span_id: self.root_span_id.clone(),
            parent_span_ids: parent_span_id.into_iter().collect(),
            name: format!("Antigravity: {label}"),
            span_type: SpanType::Task,
            start_ms: Some(event.ts_ms),
            metadata: Some(Value::Object(metadata)),
            ..Default::default()
        }));
    }

    fn tail_transcript(&mut self, event: &Envelope, ops: &mut Vec<SpanOp>) {
        let Some(source) = transcript_source(event) else {
            return;
        };
        let offset = self
            .transcript_offsets
            .entry(source.path.clone())
            .or_default();
        let records = if let Some(contents) = source.snapshot {
            read_snapshot_records(contents, offset, source.through)
        } else {
            read_file_records(&source.path, offset, source.through)
        };
        for record in records {
            self.observe_record(record, event.ts_ms, ops);
        }
    }

    fn observe_record(&mut self, record: Value, ts_ms: i64, ops: &mut Vec<SpanOp>) {
        let record_type = normalized_record_type(&record);
        let source = string_field(&record, "source").unwrap_or_default();
        if let Some(step) =
            integer_field(&record, "step_index").or_else(|| integer_field(&record, "stepIndex"))
        {
            self.records_by_step.insert(step, record.clone());
        }

        if record_type == "USER_INPUT"
            && (source.is_empty() || source == "USER_EXPLICIT" || source == "USER")
        {
            self.start_turn(clean_user_input(record_content(&record)), ts_ms, ops);
        }

        if let Some(message) = transcript_message(&record, &record_type, &source) {
            if record_type == "PLANNER_RESPONSE" {
                if let Some(turn) = &mut self.turn {
                    turn.last_output = message.get("content").cloned();
                }
            }
            self.history.push(message);
        }
        self.records.push(record);
    }

    fn start_turn(&mut self, input: Value, ts_ms: i64, ops: &mut Vec<SpanOp>) {
        self.close_turn(ts_ms, None, ops);
        self.turn_count += 1;
        let span_id = ids::span_id(&self.session_id, &format!("turn:{}", self.turn_count));
        ops.push(SpanOp::Insert(SpanRow {
            span_id: span_id.clone(),
            root_span_id: self.root_span_id.clone(),
            parent_span_ids: vec![self.session_span_id.clone()],
            name: format!("Turn {}", self.turn_count),
            span_type: SpanType::Task,
            start_ms: Some(ts_ms),
            input: nonempty_value(input),
            metadata: Some(json!({"turn_number": self.turn_count})),
            ..Default::default()
        }));
        self.turn = Some(Turn {
            span_id,
            number: self.turn_count,
            start_ms: ts_ms,
            last_output: None,
        });
    }

    fn ensure_turn(&mut self, ts_ms: i64, ops: &mut Vec<SpanOp>) -> String {
        if self.turn.is_none() {
            self.start_turn(Value::Null, ts_ms, ops);
        }
        self.turn
            .as_ref()
            .map(|turn| turn.span_id.clone())
            .unwrap_or_else(|| self.session_span_id.clone())
    }

    fn pre_invocation(&mut self, event: &Envelope, ops: &mut Vec<SpanOp>) {
        let Some(invocation_num) = integer_field(&event.payload, "invocationNum") else {
            return;
        };
        if self.invocations.contains_key(&invocation_num) {
            return;
        }
        let parent = self.ensure_turn(event.ts_ms, ops);
        // Antigravity's invocation counter is process-local and resets to zero
        // when `--conversation` resumes an existing conversation. Include the
        // stable transcript-derived turn number so resumed invocations do not
        // merge into an earlier turn's LLM span.
        let turn_number = self.turn.as_ref().map(|turn| turn.number).unwrap_or(0);
        let span_id = ids::span_id(
            &self.session_id,
            &format!("turn:{turn_number}:invocation:{invocation_num}"),
        );
        let model = string_field(&event.payload, "modelName")
            .unwrap_or_else(|| "Antigravity model".to_string());
        let mut metadata = json!({
            "invocation_num": invocation_num,
            "initial_num_steps": integer_field(&event.payload, "initialNumSteps"),
            "turn_number": turn_number,
            "model": model
        });
        remove_null_fields(&mut metadata);
        ops.push(SpanOp::Insert(SpanRow {
            span_id: span_id.clone(),
            root_span_id: self.root_span_id.clone(),
            parent_span_ids: vec![parent.clone()],
            name: model,
            span_type: SpanType::Llm,
            start_ms: Some(event.ts_ms),
            input: Some(Value::Array(self.history.clone())),
            metadata: Some(metadata),
            ..Default::default()
        }));
        self.invocations.insert(
            invocation_num,
            Invocation {
                span_id,
                parent_span_id: parent,
                history_start: self.history.len(),
                record_start: self.records.len(),
            },
        );
    }

    fn post_invocation(&mut self, event: &Envelope, ops: &mut Vec<SpanOp>) {
        let Some(invocation_num) = integer_field(&event.payload, "invocationNum") else {
            return;
        };
        let Some(invocation) = self.invocations.remove(&invocation_num) else {
            return;
        };
        let output = self.history[invocation.history_start.min(self.history.len())..]
            .iter()
            .filter(|message| message.get("role").and_then(Value::as_str) == Some("assistant"))
            .cloned()
            .collect::<Vec<_>>();
        let metrics =
            token_metrics(&self.records[invocation.record_start.min(self.records.len())..]);
        ops.push(SpanOp::Merge(SpanRow {
            span_id: invocation.span_id,
            root_span_id: self.root_span_id.clone(),
            parent_span_ids: vec![invocation.parent_span_id],
            end_ms: Some(event.ts_ms),
            output: (!output.is_empty()).then_some(Value::Array(output)),
            metrics,
            ..Default::default()
        }));
    }

    fn pre_tool(&mut self, event: &Envelope, ops: &mut Vec<SpanOp>) {
        let Some(step) = integer_field(&event.payload, "stepIdx") else {
            return;
        };
        if self.tools.contains_key(&step) {
            return;
        }
        let parent = self.ensure_turn(event.ts_ms, ops);
        let call = event.payload.get("toolCall").unwrap_or(&Value::Null);
        let name = string_field(call, "name").unwrap_or_else(|| format!("Tool step {step}"));
        let input = call.get("args").cloned();
        let span_id = ids::span_id(&self.session_id, &format!("tool:{step}"));
        ops.push(SpanOp::Insert(SpanRow {
            span_id: span_id.clone(),
            root_span_id: self.root_span_id.clone(),
            parent_span_ids: vec![parent.clone()],
            name: name.clone(),
            span_type: SpanType::Tool,
            start_ms: Some(event.ts_ms),
            input,
            metadata: Some(json!({"step_index": step, "tool_name": name})),
            ..Default::default()
        }));
        self.tools.insert(
            step,
            PendingTool {
                span_id,
                parent_span_id: parent,
                name,
            },
        );
    }

    fn post_tool(&mut self, event: &Envelope, ops: &mut Vec<SpanOp>) {
        let Some(step) = integer_field(&event.payload, "stepIdx") else {
            return;
        };
        let transcript = self.records_by_step.get(&step);
        let mut details = transcript.map(tool_details).unwrap_or_default();
        if let Some(call) = event.payload.get("toolCall") {
            if let Some(name) = string_field(call, "name") {
                details.name = Some(name);
            }
            if let Some(input) = call.get("args") {
                details.input = Some(input.clone());
            }
        }
        let recovered_start_ms = planned_tool_start(&self.records, details.name.as_deref(), step)
            .map(|start| {
                self.turn
                    .as_ref()
                    .map(|turn| start.max(turn.start_ms))
                    .unwrap_or(start)
            });
        let pending = self.tools.remove(&step);
        let was_pending = pending.is_some();
        let parent = pending
            .as_ref()
            .map(|tool| tool.parent_span_id.clone())
            .unwrap_or_else(|| self.ensure_turn(event.ts_ms, ops));
        let name = pending
            .as_ref()
            .map(|tool| tool.name.clone())
            .or(details.name)
            .unwrap_or_else(|| format!("Tool step {step}"));
        let span_id = pending
            .map(|tool| tool.span_id)
            .unwrap_or_else(|| ids::span_id(&self.session_id, &format!("tool:{step}")));
        let error = string_field(&event.payload, "error")
            .filter(|error| !error.is_empty())
            .or(details.error);
        if !was_pending {
            ops.push(SpanOp::Insert(SpanRow {
                span_id: span_id.clone(),
                root_span_id: self.root_span_id.clone(),
                parent_span_ids: vec![parent.clone()],
                name: name.clone(),
                span_type: SpanType::Tool,
                start_ms: Some(recovered_start_ms.unwrap_or(event.ts_ms)),
                input: details.input.clone(),
                metadata: Some(json!({
                    "step_index": step,
                    "tool_name": name,
                    "recovered_from_transcript": true
                })),
                ..Default::default()
            }));
        }
        ops.push(SpanOp::Merge(SpanRow {
            span_id,
            root_span_id: self.root_span_id.clone(),
            parent_span_ids: vec![parent],
            name,
            span_type: SpanType::Tool,
            end_ms: Some(event.ts_ms),
            input: details.input,
            output: details.output,
            metadata: Some(with_tool_approval(
                json!({
                    "step_index": step,
                }),
                tool_approval_from_event(event).or(Some(ToolApproval::Approved)),
            )),
            error,
            ..Default::default()
        }));
    }

    fn stop(&mut self, event: &Envelope, ops: &mut Vec<SpanOp>) {
        let error = string_field(&event.payload, "error").filter(|error| !error.is_empty());
        self.close_pending(event.ts_ms, error.clone(), ops);
        self.close_turn(event.ts_ms, error.clone(), ops);
        if event
            .payload
            .get("fullyIdle")
            .and_then(Value::as_bool)
            .unwrap_or(true)
        {
            self.close_root(event.ts_ms, error, ops);
        }
    }

    fn close_pending(&mut self, ts_ms: i64, error: Option<String>, ops: &mut Vec<SpanOp>) {
        for (_, invocation) in self.invocations.drain() {
            ops.push(SpanOp::Merge(SpanRow {
                span_id: invocation.span_id,
                root_span_id: self.root_span_id.clone(),
                parent_span_ids: vec![invocation.parent_span_id],
                end_ms: Some(ts_ms),
                output: self.turn.as_ref().and_then(|turn| {
                    turn.last_output
                        .clone()
                        .map(|content| json!([{"role":"assistant","content":content}]))
                }),
                error: error.clone(),
                ..Default::default()
            }));
        }
        for (step, tool) in self.tools.drain() {
            ops.push(SpanOp::Merge(SpanRow {
                span_id: tool.span_id,
                root_span_id: self.root_span_id.clone(),
                parent_span_ids: vec![tool.parent_span_id],
                name: tool.name,
                span_type: SpanType::Tool,
                end_ms: Some(ts_ms),
                metadata: Some(with_tool_approval(
                    json!({"step_index":step}),
                    Some(ToolApproval::Approved),
                )),
                error: error.clone(),
                ..Default::default()
            }));
        }
    }

    fn close_turn(&mut self, ts_ms: i64, error: Option<String>, ops: &mut Vec<SpanOp>) {
        if let Some(turn) = self.turn.take() {
            ops.push(SpanOp::Merge(SpanRow {
                span_id: turn.span_id,
                root_span_id: self.root_span_id.clone(),
                end_ms: Some(ts_ms),
                output: turn.last_output,
                metadata: Some(json!({"turn_number":turn.number})),
                error,
                ..Default::default()
            }));
        }
    }

    fn close_root(&mut self, ts_ms: i64, error: Option<String>, ops: &mut Vec<SpanOp>) {
        if self.root_ended {
            return;
        }
        self.root_ended = true;
        ops.push(SpanOp::Merge(SpanRow {
            span_id: self.session_span_id.clone(),
            root_span_id: self.root_span_id.clone(),
            end_ms: Some(ts_ms),
            error,
            ..Default::default()
        }));
    }
}

impl AgentTranslator for AntigravityTranslator {
    fn handle(&mut self, event: &Envelope, ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        self.last_ts_ms = self.last_ts_ms.max(event.ts_ms);
        let mut ops = Vec::new();
        // A later Antigravity process can resume the same conversation after a
        // fully-idle Stop. Reopen the logical root so the resumed Stop extends
        // its duration through all subsequent turns.
        if self.root_ended && event.event != "Stop" {
            self.root_ended = false;
        }
        self.ensure_root(event, ctx, &mut ops);
        self.tail_transcript(event, &mut ops);
        match event.event.as_str() {
            "PreInvocation" => self.pre_invocation(event, &mut ops),
            "PostInvocation" => self.post_invocation(event, &mut ops),
            "PreToolUse" => self.pre_tool(event, &mut ops),
            "PostToolUse" => self.post_tool(event, &mut ops),
            "Stop" => self.stop(event, &mut ops),
            _ => {}
        }
        let cwd = event
            .payload
            .get("workspacePaths")
            .and_then(Value::as_array)
            .and_then(|paths| paths.first())
            .and_then(Value::as_str);
        self.git.enrich_rows(cwd, &mut ops);
        Ok(ops)
    }

    fn finalize(&mut self, _ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        let mut ops = Vec::new();
        self.close_pending(self.last_ts_ms, None, &mut ops);
        self.close_turn(self.last_ts_ms, None, &mut ops);
        self.close_root(self.last_ts_ms, None, &mut ops);
        Ok(ops)
    }
}

fn tool_approval_from_event(event: &Envelope) -> Option<ToolApproval> {
    match string_field(&event.payload, "toolApproval").as_deref() {
        Some("approved") => Some(ToolApproval::Approved),
        Some("denied") => Some(ToolApproval::Denied),
        _ => None,
    }
}

struct TranscriptSource<'a> {
    path: String,
    through: u64,
    snapshot: Option<&'a str>,
}

fn transcript_source(event: &Envelope) -> Option<TranscriptSource<'_>> {
    let observation = event.payload.get("_bt_transcript_observation");
    let compact_path = observation
        .and_then(|value| value.get("path"))
        .and_then(Value::as_str)
        .or_else(|| event.payload.get("transcriptPath").and_then(Value::as_str))?;
    let full_path = observation
        .and_then(|value| value.get("full_path"))
        .and_then(Value::as_str);
    let (path, through) = if let (Some(path), Some(through)) = (
        full_path,
        observation
            .and_then(|value| value.get("full_observed_bytes"))
            .and_then(Value::as_u64),
    ) {
        (path, through)
    } else {
        let through = observation
            .and_then(|value| value.get("observed_bytes"))
            .and_then(Value::as_u64)
            .or_else(|| {
                std::fs::metadata(compact_path)
                    .ok()
                    .map(|metadata| metadata.len())
            })?;
        (compact_path, through)
    };
    let snapshot = event
        .payload
        .get("_bt_transcript_snapshot")
        .filter(|value| value.get("path").and_then(Value::as_str) == Some(path))
        .and_then(|value| value.get("contents"))
        .and_then(Value::as_str);
    Some(TranscriptSource {
        path: path.to_string(),
        through,
        snapshot,
    })
}

fn read_file_records(path: &str, offset: &mut u64, through: u64) -> Vec<Value> {
    let Ok(mut file) = std::fs::File::open(path) else {
        return Vec::new();
    };
    let len = file.metadata().map(|metadata| metadata.len()).unwrap_or(0);
    if *offset > len {
        *offset = 0;
    }
    if file.seek(SeekFrom::Start(*offset)).is_err() {
        return Vec::new();
    }
    read_records(&mut std::io::BufReader::new(file), offset, through.min(len))
}

fn read_snapshot_records(contents: &str, offset: &mut u64, through: u64) -> Vec<Value> {
    if *offset > contents.len() as u64 {
        *offset = 0;
    }
    let mut reader = std::io::BufReader::new(std::io::Cursor::new(contents.as_bytes()));
    if reader.seek(SeekFrom::Start(*offset)).is_err() {
        return Vec::new();
    }
    read_records(&mut reader, offset, through.min(contents.len() as u64))
}

fn read_records<R: std::io::Read>(
    reader: &mut std::io::BufReader<R>,
    offset: &mut u64,
    through: u64,
) -> Vec<Value> {
    let mut records = Vec::new();
    let mut line = String::new();
    while *offset < through {
        line.clear();
        let start = *offset;
        let Ok(read) = reader.read_line(&mut line) else {
            break;
        };
        if read == 0 || start + read as u64 > through {
            break;
        }
        *offset += read as u64;
        if let Ok(value) = serde_json::from_str::<Value>(line.trim()) {
            records.push(value);
        }
    }
    records
}

fn normalized_record_type(record: &Value) -> String {
    let record_type = string_field(record, "type").unwrap_or_default();
    record_type
        .strip_prefix("CORTEX_STEP_TYPE_")
        .unwrap_or(&record_type)
        .to_string()
}

fn transcript_message(record: &Value, record_type: &str, source: &str) -> Option<Value> {
    let role = match (record_type, source) {
        ("USER_INPUT", _) | (_, "USER_EXPLICIT") | (_, "USER") => "user",
        ("PLANNER_RESPONSE", _) => "assistant",
        (_, "SYSTEM") => "system",
        _ if record.get("content").is_some() => "tool",
        _ => return None,
    };
    let mut message = Map::new();
    let content = match role {
        "user" => clean_user_input(record_content(record)),
        "tool" => clean_tool_content(record_content(record)),
        _ => record_content(record),
    };
    if role == "system" && content.is_null() {
        return None;
    }
    message.insert("role".into(), json!(role));
    message.insert("content".into(), content);
    if let Some(tool_calls) = record.get("tool_calls").or_else(|| record.get("toolCalls")) {
        message.insert("tool_calls".into(), tool_calls.clone());
    }
    if role == "tool" {
        message.insert("name".into(), json!(record_type.to_ascii_lowercase()));
    }
    message.insert("step_type".into(), json!(record_type));
    Some(Value::Object(message))
}

fn record_content(record: &Value) -> Value {
    record
        .get("content")
        .or_else(|| record.get("message"))
        .or_else(|| record.get("text"))
        .cloned()
        .unwrap_or(Value::Null)
}

fn clean_user_input(content: Value) -> Value {
    let Value::String(text) = content else {
        return content;
    };
    let Some(start) = text.find("<USER_REQUEST>") else {
        return Value::String(text);
    };
    let body_start = start + "<USER_REQUEST>".len();
    let Some(relative_end) = text[body_start..].find("</USER_REQUEST>") else {
        return Value::String(text);
    };
    Value::String(
        text[body_start..body_start + relative_end]
            .trim()
            .to_string(),
    )
}

#[derive(Default)]
struct ToolDetails {
    name: Option<String>,
    input: Option<Value>,
    output: Option<Value>,
    error: Option<String>,
}

fn tool_details(record: &Value) -> ToolDetails {
    let call = record
        .get("tool_calls")
        .or_else(|| record.get("toolCalls"))
        .and_then(|calls| {
            calls
                .as_array()
                .and_then(|calls| calls.first())
                .or(Some(calls))
        });
    let record_type = normalized_record_type(record);
    let name = call
        .and_then(|call| {
            string_field(call, "name")
                .or_else(|| string_field(call, "tool_name"))
                .or_else(|| string_field(call, "toolName"))
        })
        .or_else(|| {
            (!matches!(record_type.as_str(), "USER_INPUT" | "PLANNER_RESPONSE"))
                .then(|| record_type.to_ascii_lowercase())
        });
    let input = call.and_then(|call| {
        call.get("args")
            .or_else(|| call.get("arguments"))
            .or_else(|| call.get("input"))
            .cloned()
    });
    let output = call
        .and_then(|call| call.get("output").or_else(|| call.get("result")))
        .cloned()
        .or_else(|| nonempty_value(clean_tool_content(record_content(record))));
    let error = string_field(record, "error")
        .filter(|error| !error.is_empty())
        .or_else(|| {
            (record_type == "ERROR_MESSAGE")
                .then(|| record_content(record))
                .and_then(|content| content.as_str().map(str::to_owned))
        })
        .or_else(|| {
            string_field(record, "status")
                .filter(|status| matches!(status.to_ascii_lowercase().as_str(), "error" | "failed"))
                .map(|status| match record_content(record) {
                    Value::String(content) if !content.is_empty() => content,
                    _ => status,
                })
        });
    ToolDetails {
        name,
        input,
        output,
        error,
    }
}

fn clean_tool_content(content: Value) -> Value {
    let Value::String(text) = content else {
        return content;
    };
    let mut lines = text.lines();
    let first = lines.next();
    let second = lines.next();
    if first.is_some_and(|line| line.starts_with("Created At:"))
        && second.is_some_and(|line| line.starts_with("Completed At:"))
    {
        return Value::String(lines.collect::<Vec<_>>().join("\n"));
    }
    Value::String(text)
}

fn planned_tool_start(records: &[Value], name: Option<&str>, step: i64) -> Option<i64> {
    let name = name?;
    records.iter().rev().find_map(|record| {
        let record_step =
            integer_field(record, "step_index").or_else(|| integer_field(record, "stepIndex"))?;
        if record_step >= step {
            return None;
        }
        let calls = record
            .get("tool_calls")
            .or_else(|| record.get("toolCalls"))?
            .as_array()?;
        calls
            .iter()
            .any(|call| string_field(call, "name").as_deref() == Some(name))
            .then(|| parse_created_at(record))
            .flatten()
    })
}

fn parse_created_at(record: &Value) -> Option<i64> {
    let timestamp =
        string_field(record, "created_at").or_else(|| string_field(record, "createdAt"))?;
    chrono::DateTime::parse_from_rfc3339(&timestamp)
        .ok()
        .map(|timestamp| timestamp.timestamp_millis())
}

fn token_metrics(records: &[Value]) -> Option<Value> {
    let mut found = HashMap::<&'static str, f64>::new();
    for record in records {
        collect_token_metrics(record, &mut found);
    }
    if found.is_empty() {
        return None;
    }
    let mut metrics = Map::new();
    for (key, value) in found {
        metrics.insert(key.to_string(), json!(value));
    }
    Some(Value::Object(metrics))
}

fn collect_token_metrics(value: &Value, found: &mut HashMap<&'static str, f64>) {
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                if let Some(number) = value.as_f64() {
                    let metric = match key.as_str() {
                        "input_tokens" | "inputTokens" | "prompt_tokens" | "promptTokens" => {
                            Some("prompt_tokens")
                        }
                        "output_tokens" | "outputTokens" | "completion_tokens"
                        | "completionTokens" => Some("completion_tokens"),
                        "total_tokens" | "totalTokens" => Some("tokens"),
                        "thinking_tokens" | "thinkingTokens" => Some("thinking_tokens"),
                        "cache_read_tokens" | "cacheReadTokens" => Some("prompt_cached_tokens"),
                        _ => None,
                    };
                    if let Some(metric) = metric {
                        found.insert(metric, number);
                    }
                }
                collect_token_metrics(value, found);
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_token_metrics(value, found);
            }
        }
        _ => {}
    }
}

fn string_field(value: &Value, field: &str) -> Option<String> {
    value.get(field).and_then(Value::as_str).map(str::to_owned)
}

fn integer_field(value: &Value, field: &str) -> Option<i64> {
    value
        .get(field)
        .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
}

fn nonempty_value(value: Value) -> Option<Value> {
    match &value {
        Value::Null => None,
        Value::String(text) if text.is_empty() => None,
        Value::Array(values) if values.is_empty() => None,
        Value::Object(object) if object.is_empty() => None,
        _ => Some(value),
    }
}

fn remove_null_fields(value: &mut Value) {
    if let Some(object) = value.as_object_mut() {
        object.retain(|_, value| !value.is_null());
    }
}
