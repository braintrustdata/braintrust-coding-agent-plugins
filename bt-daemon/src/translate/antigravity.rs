//! Google Antigravity hook and transcript translator.
//!
//! Native hooks own lifecycle timing and correlation (`invocationNum` and
//! `stepIdx`). The append-only full transcript supplies the actual user/model
//! messages and tool details. Hook capture records transcript byte boundaries,
//! so replay observes exactly the records that existed when each hook fired.

use super::git::GitMetadataCache;
use super::tool::{with_tool_approval, without_nulls, ToolApproval};
use super::{
    local_username, AgentTranslator, SessionCtx, SpanOp, SpanRow, SpanType, TranslatorFactory,
};
use crate::agents::antigravity::schema::{optional_integer, ToolCall, ToolCalls, TranscriptRecord};
use crate::agents::usage::UsageMetrics;
use crate::ids;
use crate::transcript_mirror::TranscriptPayload;
use crate::wire::Envelope;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{BufRead, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct HookPayload {
    workspace_paths: Option<Vec<String>>,
    model_name: Option<String>,
    artifact_directory_path: Option<String>,
    #[serde(default, deserialize_with = "optional_integer")]
    invocation_num: Option<i64>,
    #[serde(default, deserialize_with = "optional_integer")]
    initial_num_steps: Option<i64>,
    #[serde(default, deserialize_with = "optional_integer")]
    step_idx: Option<i64>,
    tool_call: Option<ToolCall>,
    error: Option<String>,
    fully_idle: Option<bool>,
    tool_approval: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
struct Message {
    role: String,
    content: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_calls: Option<ToolCalls>,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    step_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    message_type: Option<String>,
}

impl TranslatorFactory for crate::agents::Antigravity {
    fn create(&self, session_id: &str) -> Box<dyn AgentTranslator> {
        Box::new(AntigravityTranslator::new(
            session_id,
            GitMetadataCache::shared(),
        ))
    }
}
#[derive(Serialize, Deserialize)]
struct Turn {
    span_id: String,
    number: u32,
    start_ms: i64,
    last_output: Option<Value>,
}
#[derive(Serialize, Deserialize)]
struct Invocation {
    span_id: String,
    parent_span_id: String,
    history_start: usize,
    record_start: usize,
}
#[derive(Serialize, Deserialize)]
struct PendingTool {
    span_id: String,
    parent_span_id: String,
    name: String,
}
#[derive(Serialize, Deserialize)]
struct AntigravityTranslator {
    session_id: String,
    session_span_id: String,
    root_span_id: String,
    session_parent_span_ids: Vec<String>,
    root_open: bool,
    root_ended: bool,
    turn: Option<Turn>,
    turn_count: u32,
    transcript_offsets: HashMap<String, u64>,
    records: Vec<TranscriptRecord>,
    records_by_step: HashMap<i64, TranscriptRecord>,
    history: Vec<Message>,
    invocations: HashMap<i64, Invocation>,
    tools: HashMap<i64, PendingTool>,
    last_ts_ms: i64,
    #[serde(skip, default = "GitMetadataCache::shared")]
    git: Arc<GitMetadataCache>,
}

impl AntigravityTranslator {
    fn new(session_id: &str, git: Arc<GitMetadataCache>) -> Self {
        let root = ids::span_id(session_id, "root");
        Self {
            session_id: session_id.to_string(),
            session_span_id: root.clone(),
            root_span_id: root,
            session_parent_span_ids: Vec::new(),
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

    fn ensure_root(
        &mut self,
        event: &Envelope,
        hook: &HookPayload,
        ctx: &SessionCtx,
        ops: &mut Vec<SpanOp>,
    ) {
        if self.root_open {
            return;
        }
        self.root_open = true;
        let workspace = hook
            .workspace_paths
            .as_ref()
            .and_then(|paths| paths.first())
            .map(String::as_str);
        let mut metadata = serde_json::Map::new();
        metadata.retain(|key, _| !key.starts_with("_bt_"));
        metadata.insert("session_id".into(), json!(ctx.session_id));
        metadata.insert("conversation_id".into(), json!(ctx.session_id));
        metadata.insert("source".into(), json!("antigravity"));
        metadata.insert("_bt_defer_root".into(), json!(true));
        metadata.insert("username".into(), json!(local_username()));
        if let Some(model) = hook.model_name.clone() {
            metadata.insert("model".into(), json!(model));
        }
        if let Some(workspaces) = hook.workspace_paths.as_ref() {
            metadata.insert("workspace_paths".into(), json!(workspaces));
        }
        if let Some(path) = hook.artifact_directory_path.clone() {
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
            parent_span_ids: self.session_parent_span_ids.clone(),
            name: format!("Antigravity: {label}"),
            span_type: SpanType::Task,
            start_ms: Some(event.ts_ms),
            metadata: Some(Value::Object(metadata)),
            tags: None,
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
            read_snapshot_records(&contents, offset, source.through)
        } else {
            read_file_records(&source.path, offset, source.through)
        };
        for record in records {
            self.observe_record(record, event.ts_ms, ops);
        }
    }

    fn observe_record(&mut self, record: TranscriptRecord, ts_ms: i64, ops: &mut Vec<SpanOp>) {
        let record_type = record.normalized_kind().to_owned();
        let source = &record.source;
        if let Some(step) = record.step_index {
            self.records_by_step.insert(step, record.clone());
        }

        if record_type == "USER_INPUT"
            && (source.is_empty() || source == "USER_EXPLICIT" || source == "USER")
        {
            self.start_turn(clean_user_input(record.message_content()), ts_ms, ops);
        }

        if record_type == "CHECKPOINT" {
            if let Some(mut message) = transcript_message(&record, &record_type, source) {
                message.message_type = Some("compaction_summary".into());
                self.history.clear();
                self.history.push(message);
                // A checkpoint can be observed while an invocation is open. Its
                // replacement window becomes the new origin for output slicing.
                for invocation in self.invocations.values_mut() {
                    invocation.history_start = 0;
                }
            }
        } else if let Some(message) = transcript_message(&record, &record_type, source) {
            if record_type == "PLANNER_RESPONSE" {
                if let Some(turn) = &mut self.turn {
                    turn.last_output = Some(message.content.clone());
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
            is_turn: true,
            start_ms: Some(ts_ms),
            input: nonempty_value(input),
            metadata: Some(json!({"turn_number": self.turn_count})),
            turn_span_id: Some(span_id.clone()),
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

    fn pre_invocation(&mut self, event: &Envelope, hook: &HookPayload, ops: &mut Vec<SpanOp>) {
        let Some(invocation_num) = hook.invocation_num else {
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
        let model = hook
            .model_name
            .clone()
            .unwrap_or_else(|| "Antigravity model".to_string());
        let mut metadata = json!({
            "invocation_num": invocation_num,
            "initial_num_steps": hook.initial_num_steps,
            "turn_number": turn_number,
            "model": model
        });
        metadata = without_nulls(metadata);
        ops.push(SpanOp::Insert(SpanRow {
            span_id: span_id.clone(),
            root_span_id: self.root_span_id.clone(),
            parent_span_ids: vec![parent.clone()],
            turn_span_id: Some(parent.clone()),
            name: model,
            span_type: SpanType::Llm,
            start_ms: Some(event.ts_ms),
            input: Some(json!(self.history)),
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

    fn post_invocation(&mut self, event: &Envelope, hook: &HookPayload, ops: &mut Vec<SpanOp>) {
        let Some(invocation_num) = hook.invocation_num else {
            return;
        };
        let Some(invocation) = self.invocations.remove(&invocation_num) else {
            return;
        };
        let output = self.history[invocation.history_start.min(self.history.len())..]
            .iter()
            .filter(|message| message.role == "assistant")
            .cloned()
            .collect::<Vec<_>>();
        let metrics =
            token_metrics(&self.records[invocation.record_start.min(self.records.len())..]);
        ops.push(SpanOp::Merge(SpanRow {
            span_id: invocation.span_id,
            root_span_id: self.root_span_id.clone(),
            parent_span_ids: vec![invocation.parent_span_id],
            end_ms: Some(event.ts_ms),
            output: (!output.is_empty()).then(|| json!(output)),
            metrics,
            ..Default::default()
        }));
    }

    fn pre_tool(&mut self, event: &Envelope, hook: &HookPayload, ops: &mut Vec<SpanOp>) {
        let Some(step) = hook.step_idx else {
            return;
        };
        if self.tools.contains_key(&step) {
            return;
        }
        let parent = self.ensure_turn(event.ts_ms, ops);
        let call = hook.tool_call.as_ref();
        let name = call
            .and_then(|call| call.name.clone())
            .unwrap_or_else(|| format!("Tool step {step}"));
        let input = call.and_then(|call| call.args.clone());
        let span_id = ids::span_id(&self.session_id, &format!("tool:{step}"));
        ops.push(SpanOp::Insert(SpanRow {
            span_id: span_id.clone(),
            root_span_id: self.root_span_id.clone(),
            parent_span_ids: vec![parent.clone()],
            turn_span_id: Some(parent.clone()),
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

    fn post_tool(&mut self, event: &Envelope, hook: &HookPayload, ops: &mut Vec<SpanOp>) {
        let Some(step) = hook.step_idx else {
            return;
        };
        let transcript = self.records_by_step.get(&step);
        let mut details = transcript.map(tool_details).unwrap_or_default();
        if let Some(call) = hook.tool_call.as_ref() {
            if let Some(name) = call.name.clone() {
                details.name = Some(name);
            }
            if let Some(input) = call.args.as_ref() {
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
        let error = hook
            .error
            .clone()
            .filter(|error| !error.is_empty())
            .or(details.error);
        if !was_pending {
            ops.push(SpanOp::Insert(SpanRow {
                span_id: span_id.clone(),
                root_span_id: self.root_span_id.clone(),
                parent_span_ids: vec![parent.clone()],
                turn_span_id: Some(parent.clone()),
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
                tool_approval_from_hook(hook).or(Some(ToolApproval::Approved)),
            )),
            error,
            ..Default::default()
        }));
    }

    fn stop(&mut self, event: &Envelope, hook: &HookPayload, ops: &mut Vec<SpanOp>) {
        let error = hook.error.clone().filter(|error| !error.is_empty());
        self.close_pending(event.ts_ms, error.clone(), ops);
        self.close_turn(event.ts_ms, error.clone(), ops);
        if hook.fully_idle.unwrap_or(true) {
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
                parent_span_ids: vec![self.session_span_id.clone()],
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
            parent_span_ids: self.session_parent_span_ids.clone(),
            end_ms: Some(ts_ms),
            error,
            ..Default::default()
        }));
    }
}

impl AgentTranslator for AntigravityTranslator {
    fn handle(&mut self, event: &Envelope, ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        let hook: HookPayload = serde::Deserialize::deserialize(&event.payload)?;
        self.last_ts_ms = self.last_ts_ms.max(event.ts_ms);
        let mut ops = Vec::new();
        // A later Antigravity process can resume the same conversation after a
        // fully-idle Stop. Reopen the logical root so the resumed Stop extends
        // its duration through all subsequent turns.
        if self.root_ended && event.event != "Stop" {
            self.root_ended = false;
        }
        self.ensure_root(event, &hook, ctx, &mut ops);
        self.tail_transcript(event, &mut ops);
        match event.event.as_str() {
            "PreInvocation" => self.pre_invocation(event, &hook, &mut ops),
            "PostInvocation" => self.post_invocation(event, &hook, &mut ops),
            "PreToolUse" => self.pre_tool(event, &hook, &mut ops),
            "PostToolUse" => self.post_tool(event, &hook, &mut ops),
            "Stop" => self.stop(event, &hook, &mut ops),
            _ => {}
        }
        let cwd = hook
            .workspace_paths
            .as_ref()
            .and_then(|paths| paths.first())
            .map(String::as_str);
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

fn tool_approval_from_hook(hook: &HookPayload) -> Option<ToolApproval> {
    match hook.tool_approval.as_deref() {
        Some("approved") => Some(ToolApproval::Approved),
        Some("denied") => Some(ToolApproval::Denied),
        _ => None,
    }
}

struct TranscriptSource {
    path: String,
    through: u64,
    snapshot: Option<String>,
}

fn transcript_source(event: &Envelope) -> Option<TranscriptSource> {
    let fields = TranscriptPayload::decode(&event.payload).ok()?;
    let observation = fields.observation.as_ref();
    let compact_path = observation
        .and_then(|value| value.path.as_deref())
        .or(fields.transcript_path.as_deref())?;
    let full_path = observation.and_then(|value| value.full_path.as_deref());
    let (path, through) = if let (Some(path), Some(through)) = (
        full_path,
        observation.and_then(|value| value.full_observed_bytes),
    ) {
        (path, through)
    } else {
        let through = observation
            .and_then(|value| value.observed_bytes)
            .or_else(|| {
                std::fs::metadata(compact_path)
                    .ok()
                    .map(|metadata| metadata.len())
            })?;
        (compact_path, through)
    };
    let snapshot = fields
        .snapshot
        .filter(|snapshot| snapshot.path == path)
        .map(|snapshot| snapshot.contents);
    Some(TranscriptSource {
        path: path.to_owned(),
        through,
        snapshot,
    })
}

fn read_file_records(path: &str, offset: &mut u64, through: u64) -> Vec<TranscriptRecord> {
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

fn read_snapshot_records(contents: &str, offset: &mut u64, through: u64) -> Vec<TranscriptRecord> {
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
) -> Vec<TranscriptRecord> {
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
        if let Ok(value) = serde_json::from_str::<TranscriptRecord>(line.trim()) {
            records.push(value);
        }
    }
    records
}

fn transcript_message(
    record: &TranscriptRecord,
    record_type: &str,
    source: &str,
) -> Option<Message> {
    let role = match (record_type, source) {
        ("USER_INPUT", _) | (_, "USER_EXPLICIT") | (_, "USER") => "user",
        ("PLANNER_RESPONSE", _) => "assistant",
        (_, "SYSTEM") => "system",
        _ if record.content.is_some() => "tool",
        _ => return None,
    };
    let content = match role {
        "user" => clean_user_input(record.message_content()),
        "tool" => clean_tool_content(record.message_content()),
        _ => record.message_content(),
    };
    if role == "system" && content.is_null() {
        return None;
    }
    Some(Message {
        role: role.into(),
        content,
        tool_calls: record.tool_calls.clone(),
        name: (role == "tool").then(|| record_type.to_ascii_lowercase()),
        step_type: record_type.into(),
        message_type: None,
    })
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

#[derive(Default, Serialize, Deserialize)]
struct ToolDetails {
    name: Option<String>,
    input: Option<Value>,
    output: Option<Value>,
    error: Option<String>,
}

fn tool_details(record: &TranscriptRecord) -> ToolDetails {
    let call = record
        .tool_calls
        .as_ref()
        .and_then(|calls| calls.iter().next());
    let record_type = record.normalized_kind().to_owned();
    let name = call.and_then(|call| call.name.clone()).or_else(|| {
        (!matches!(record_type.as_str(), "USER_INPUT" | "PLANNER_RESPONSE"))
            .then(|| record_type.to_ascii_lowercase())
    });
    let input = call.and_then(|call| call.args.clone());
    let output = call
        .and_then(|call| call.output.clone())
        .or_else(|| nonempty_value(clean_tool_content(record.message_content())));
    let error = record
        .error
        .clone()
        .filter(|error| !error.is_empty())
        .or_else(|| {
            (record_type == "ERROR_MESSAGE")
                .then(|| record.message_content())
                .and_then(|content| content.as_str().map(str::to_owned))
        })
        .or_else(|| {
            record
                .status
                .as_ref()
                .filter(|status| matches!(status.to_ascii_lowercase().as_str(), "error" | "failed"))
                .map(|status| match record.message_content() {
                    Value::String(content) if !content.is_empty() => content,
                    _ => status.clone(),
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

fn planned_tool_start(records: &[TranscriptRecord], name: Option<&str>, step: i64) -> Option<i64> {
    let name = name?;
    records.iter().rev().find_map(|record| {
        if record.step_index? >= step {
            return None;
        }
        record
            .tool_calls
            .as_ref()?
            .iter()
            .any(|call| call.name.as_deref() == Some(name))
            .then(|| record.created_at_ms())
            .flatten()
    })
}

fn token_metrics(records: &[TranscriptRecord]) -> Option<Value> {
    let mut metrics = UsageMetrics::default();
    for usage in records.iter().filter_map(|record| record.usage.as_ref()) {
        metrics.observe(usage.metrics());
    }
    (!metrics.is_empty()).then(|| json!(metrics))
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

#[cfg(test)]
mod usage_tests {
    use super::*;

    #[test]
    fn only_explicit_usage_fields_become_metrics() {
        let records: Vec<TranscriptRecord> = serde_json::from_value(json!([
            {"usage":{"inputTokens":10,"outputTokens":2,"totalTokens":12},
             "content":{"inputTokens":10000},
             "future":{"input_tokens":20000},
             "tool_calls":[{"name":"test","args":{"totalTokens":30000}}]},
            {"content":{"outputTokens":40000}},
            {"usage":{"vendor":{"inputTokens":50000}}}
        ]))
        .unwrap();
        assert_eq!(
            token_metrics(&records).unwrap(),
            json!({"prompt_tokens":10.0,"completion_tokens":2.0,"tokens":12.0})
        );
        assert!(token_metrics(&records[1..]).is_none());
    }

    #[test]
    fn usage_snapshots_merge_known_counts_without_summing_or_inventing_totals() {
        let records: Vec<TranscriptRecord> = serde_json::from_value(json!([
            {"usage":{"inputTokens":10,"cacheReadTokens":3}},
            {"usage":{"outputTokens":2}},
            {"usage":{"inputTokens":12}}
        ]))
        .unwrap();
        assert_eq!(
            token_metrics(&records).unwrap(),
            json!({"prompt_tokens":12.0,"completion_tokens":2.0,"prompt_cached_tokens":3.0})
        );
    }
}
