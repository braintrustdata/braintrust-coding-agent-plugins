//! Hook-led Cursor tracing. Native tool IDs are authoritative; generation IDs
//! from tool hooks are deliberately not treated as user-turn IDs (CLI drift).
//! Transcripts add observable messages only. Model inputs/times are reconstructed,
//! and neither missing tool contents nor unverified token counts are invented.

use super::git::GitMetadataCache;
use super::recent::{RecentMap, RecentSet};
use super::{
    local_username, root_tags, AgentTranslator, SessionCtx, SpanOp, SpanRow, SpanType,
    TranslatorFactory,
};
use crate::{ids, wire::Envelope};
use serde_json::{json, Value};
use std::collections::{BTreeMap, VecDeque};
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

const MAX_HISTORY_BYTES: usize = 2 * 1024 * 1024;
const MAX_HISTORY_MESSAGES: usize = 256;
const MAX_RECORD_BYTES: usize = 1024 * 1024;
const BYTE_BUDGET: u64 = 64 * 1024;
const MAX_OPEN_TOOLS: usize = 256;

// Automatic metadata allowlist. Explicit user destination metadata is handled
// separately. Keep this list aligned with docs/cursor-tracing-audit.md.
const CURSOR_METADATA_FIELDS: &[&str] = &[
    "source",
    "session_id",
    "username",
    "os",
    "workspace",
    "trace_cursor_version",
    "trace_plugin_version",
    "git_origin_url",
    "git_branch",
    "git_commit_sha",
    "model",
    "model_params",
    "input_reconstructed",
    "start_time_estimated",
    "history_truncated",
    "output_truncated",
    "token_usage_scope",
    "status",
    "result_completeness",
    "pre_read_content",
    "failure_type",
    "is_interrupt",
    "tool_approval",
    "subagent_type",
    "recursive_activity_verified",
    "observation_only",
    "completion_observed",
    "replacement_context_available",
];

pub struct CursorTranslatorFactory {
    git: Arc<GitMetadataCache>,
}
impl CursorTranslatorFactory {
    pub(super) fn new(git: Arc<GitMetadataCache>) -> Self {
        Self { git }
    }
}
impl TranslatorFactory for CursorTranslatorFactory {
    fn source(&self) -> &str {
        "cursor"
    }
    fn create(&self, session_id: &str) -> Box<dyn AgentTranslator> {
        Box::new(CursorTranslator::new(session_id, self.git.clone()))
    }
}

#[derive(Default)]
struct History {
    messages: VecDeque<Value>,
    bytes: usize,
    truncated: bool,
}
impl History {
    fn push(&mut self, message: Value) {
        let size = message.to_string().len();
        if size > MAX_HISTORY_BYTES {
            self.truncated = true;
            return;
        }
        self.bytes += size;
        self.messages.push_back(message);
        while self.messages.len() > MAX_HISTORY_MESSAGES || self.bytes > MAX_HISTORY_BYTES {
            if let Some(old) = self.messages.pop_front() {
                self.bytes -= old.to_string().len();
                self.truncated = true;
            }
        }
    }
    fn value(&self) -> Value {
        json!(self.messages)
    }
}

#[derive(Clone)]
struct Turn {
    ordinal: u64,
    id: String,
    prompt: Option<String>,
    output: Option<String>,
    start: i64,
    end: Option<i64>,
    boundary: i64,
    model_seq: u64,
    emitted_text: String,
    usage: serde_json::Map<String, Value>,
}
struct ModelStep {
    row: SpanRow,
    content: Vec<Value>,
    text: String,
    bytes: usize,
    truncated: bool,
}
impl ModelStep {
    fn push(&mut self, value: Value) {
        if let Some(text) = value
            .get("text")
            .and_then(Value::as_str)
            .filter(|_| value.get("type").and_then(Value::as_str) == Some("text"))
        {
            // afterAgentResponse repeats the concatenation of transcript text
            // fragments. Retain one authoritative copy of that exact content.
            if !self.text.is_empty() && self.text == text {
                self.content
                    .retain(|v| v.get("type").and_then(Value::as_str) != Some("text"));
                self.text.clear();
                self.bytes = self.content.iter().map(|v| v.to_string().len()).sum();
            }
        }
        if self.content.contains(&value) {
            return;
        }
        let size = value.to_string().len();
        if self.bytes + size > MAX_HISTORY_BYTES || self.content.len() >= MAX_HISTORY_MESSAGES {
            self.truncated = true;
            return;
        }
        self.bytes += size;
        if value.get("type").and_then(Value::as_str) == Some("text") {
            if let Some(text) = value.get("text").and_then(Value::as_str) {
                self.text.push_str(text);
            }
        }
        self.content.push(value);
    }
}
#[derive(Clone)]
struct Tool {
    row: SpanRow,
    turn: String,
    call_id: String,
    ended: bool,
}
#[derive(Default)]
struct TranscriptCursor {
    path: String,
    offset: u64,
    partial: Vec<u8>,
    oversize: bool,
    assistant_records: u64,
}
impl TranscriptCursor {
    fn read_batch(&mut self, path: &str, through: u64) -> anyhow::Result<(Vec<Value>, bool, bool)> {
        let Ok(mut file) = std::fs::File::open(path) else {
            return Ok((Vec::new(), false, true));
        };
        file.seek(SeekFrom::Start(self.offset))?;
        let mut bytes = Vec::new();
        file.take((through - self.offset).min(BYTE_BUDGET))
            .read_to_end(&mut bytes)?;
        if bytes.is_empty() {
            return Ok((Vec::new(), false, true));
        }
        self.offset += bytes.len() as u64;
        let mut records = Vec::new();
        let mut truncated = false;
        for byte in bytes {
            if byte == b'\n' {
                if !self.oversize {
                    match serde_json::from_slice::<Value>(&self.partial) {
                        Ok(record) => records.push(record),
                        Err(_) => truncated = true,
                    }
                }
                self.partial.clear();
                self.oversize = false;
            } else if !self.oversize {
                if self.partial.len() >= MAX_RECORD_BYTES {
                    self.partial.clear();
                    self.oversize = true;
                    truncated = true;
                } else {
                    self.partial.push(byte);
                }
            }
        }
        Ok((records, truncated, self.offset >= through))
    }
}

struct CursorTranslator {
    namespace: String,
    root: String,
    trace_root: String,
    root_parents: Vec<String>,
    started: bool,
    last_ms: i64,
    cwd: Option<String>,
    model: Option<Value>,
    model_params: Option<Value>,
    git: Arc<GitMetadataCache>,
    turn_seq: u64,
    turn: Option<Turn>,
    model_step: Option<ModelStep>,
    history: History,
    tools: BTreeMap<String, Tool>,
    open_tool_order: VecDeque<String>,
    completed: RecentMap<String, Tool>,
    seen: RecentSet<String>,
    prompt_generations: RecentMap<String, Turn>,
    transcript: TranscriptCursor,
    transcript_users: u64,
    transcript_owner: Option<String>,
    transcript_prompts: RecentMap<u64, (String, String)>,
    pending: Option<Envelope>,
    subagents: BTreeMap<String, SpanRow>,
    compact_seq: u64,
}
impl CursorTranslator {
    fn new(namespace: &str, git: Arc<GitMetadataCache>) -> Self {
        let root = ids::span_id(namespace, "session");
        Self {
            namespace: namespace.into(),
            trace_root: root.clone(),
            root,
            root_parents: Vec::new(),
            started: false,
            last_ms: 0,
            cwd: None,
            model: None,
            model_params: None,
            git,
            turn_seq: 0,
            turn: None,
            model_step: None,
            history: History::default(),
            tools: BTreeMap::new(),
            open_tool_order: VecDeque::new(),
            completed: RecentMap::default(),
            seen: RecentSet::default(),
            prompt_generations: RecentMap::default(),
            transcript: TranscriptCursor::default(),
            transcript_users: 0,
            transcript_owner: None,
            transcript_prompts: RecentMap::default(),
            pending: None,
            subagents: BTreeMap::new(),
            compact_seq: 0,
        }
    }
    fn row(&self, id: String, parent: String, name: &str, kind: SpanType) -> SpanRow {
        SpanRow {
            span_id: id,
            root_span_id: self.trace_root.clone(),
            parent_span_ids: vec![parent],
            name: name.into(),
            span_type: kind,
            ..Default::default()
        }
    }
    fn root(&mut self, e: &Envelope, ctx: &SessionCtx, ops: &mut Vec<SpanOp>) {
        self.last_ms = self.last_ms.max(e.ts_ms);
        let previous_cwd = self.cwd.clone();
        if let Some(cwd) = e
            .payload
            .get("cwd")
            .and_then(Value::as_str)
            .filter(|cwd| !cwd.is_empty())
            .or_else(|| e.payload.get("workspace_roots")?.get(0)?.as_str())
        {
            self.cwd = Some(cwd.into());
        }
        if let Some(model) = e.payload.get("model_id").or_else(|| e.payload.get("model")) {
            if self.model.as_ref() != Some(model) {
                self.model_params = None;
            }
            self.model = Some(model.clone());
        }
        if e.event == "beforeSubmitPrompt" {
            self.model_params = None;
        }
        if let Some(params) = e.payload.get("model_params") {
            self.model_params = Some(params.clone());
        }
        if self.started {
            if previous_cwd != self.cwd {
                ops.push(SpanOp::Merge(SpanRow {
                    span_id: self.root.clone(),
                    root_span_id: self.trace_root.clone(),
                    parent_span_ids: self.root_parents.clone(),
                    name: "Cursor session".into(),
                    span_type: SpanType::Task,
                    metadata: Some(json!({"workspace":self.cwd})),
                    ..Default::default()
                }));
            }
            return;
        }
        self.started = true;
        let (parent, root) = ctx
            .config
            .as_ref()
            .map(|c| c.attached_span_ids())
            .unwrap_or_default();
        self.trace_root = root.unwrap_or_else(|| self.root.clone());
        self.root_parents = parent.into_iter().collect();
        let metadata = json!({"source":"cursor", "session_id":ctx.session_id,
            "username":local_username(), "os":std::env::consts::OS, "workspace":self.cwd,
            "trace_cursor_version":e.source_version, "trace_plugin_version":e.plugin_version});
        ops.push(SpanOp::Insert(SpanRow {
            span_id: self.root.clone(),
            root_span_id: self.trace_root.clone(),
            parent_span_ids: self.root_parents.clone(),
            name: "Cursor session".into(),
            span_type: SpanType::Task,
            start_ms: Some(e.ts_ms),
            metadata: Some(metadata),
            tags: root_tags(ctx),
            ..Default::default()
        }));
    }
    fn ensure_turn(
        &mut self,
        ts: i64,
        prompt: Option<String>,
        generation: Option<&str>,
        origin: &str,
        ops: &mut Vec<SpanOp>,
    ) {
        if self.turn.as_ref().is_some_and(|t| t.end.is_none()) {
            return;
        }
        self.turn_seq += 1;
        let key = generation
            .map(|g| format!("turn:prompt:{g}"))
            .unwrap_or_else(|| format!("turn:observed:{}", self.turn_seq));
        let id = ids::span_id(&self.namespace, &key);
        let mut row = self.row(
            id.clone(),
            self.root.clone(),
            &format!("Turn {}", self.turn_seq),
            SpanType::Task,
        );
        row.start_ms = Some(ts);
        row.input = prompt.as_ref().map(|p| json!(p));
        row.metadata = Some(
            json!({"turn_boundary_source":origin, "start_time_estimated":origin != "beforeSubmitPrompt", "generation_id":generation}),
        );
        ops.push(SpanOp::Insert(row));
        let turn = Turn {
            ordinal: self.turn_seq,
            id,
            prompt: prompt.clone(),
            output: None,
            start: ts,
            end: None,
            boundary: ts,
            model_seq: 0,
            emitted_text: String::new(),
            usage: serde_json::Map::new(),
        };
        if let Some(g) = generation {
            self.prompt_generations.insert(g.into(), turn.clone());
        }
        self.turn = Some(turn);
        if let Some(p) = prompt {
            self.history.push(json!({"role":"user","content":p}));
        }
    }
    fn prompt(&mut self, e: &Envelope, ops: &mut Vec<SpanOp>) {
        let Some(prompt) = e.payload.get("prompt").and_then(Value::as_str) else {
            return;
        };
        let generation = e.payload.get("generation_id").and_then(Value::as_str);
        if generation.is_some_and(|g| self.prompt_generations.get(g).is_some()) {
            return;
        }
        let has_native_prompt = self.turn.as_ref().is_some_and(|current| {
            self.prompt_generations
                .iter()
                .any(|(_, t)| t.id == current.id)
        });
        // A delayed native prompt can enrich the transcript/first-hook turn.
        if let Some(turn) = self.turn.as_mut().filter(|t| {
            !has_native_prompt
                && t.end.is_none()
                && (t.prompt.is_none() || t.prompt.as_deref() == Some(prompt))
        }) {
            let add = turn.prompt.is_none();
            turn.prompt = Some(prompt.into());
            let copy = turn.clone();
            if let Some(g) = generation {
                self.prompt_generations.insert(g.into(), copy.clone());
            }
            let mut row = self.row(
                copy.id,
                self.root.clone(),
                &format!("Turn {}", self.turn_seq),
                SpanType::Task,
            );
            row.input = Some(json!(prompt));
            row.metadata = Some(
                json!({"turn_boundary_source":"beforeSubmitPrompt","generation_id":generation}),
            );
            ops.push(SpanOp::Merge(row));
            if add {
                self.history.push(json!({"role":"user","content":prompt}));
            }
            return;
        }
        self.close_turn(e.ts_ms, Some("superseded"), ops);
        self.ensure_turn(
            e.ts_ms,
            Some(prompt.into()),
            generation,
            "beforeSubmitPrompt",
            ops,
        );
    }
    fn ensure_model(&mut self, ts: i64, ops: &mut Vec<SpanOp>) {
        if self.model_step.is_some() {
            return;
        }
        if self.turn.is_none() {
            self.ensure_turn(ts, None, None, "hook_order", ops);
        }
        let turn = self.turn.as_mut().unwrap();
        turn.model_seq += 1;
        let parent = turn.id.clone();
        let start = turn.boundary;
        let seq = turn.model_seq;
        let mut row = self.row(
            ids::span_id(&self.namespace, &format!("llm:{parent}:{seq}")),
            parent,
            "Cursor completion",
            SpanType::Llm,
        );
        row.start_ms = Some(start);
        row.input = Some(self.history.value());
        row.metadata = Some(
            json!({"input_reconstructed":true,"start_time_estimated":true,"model_call_grouping":"observable_output_and_tool_boundaries",
            "model":self.model,"model_params":self.model_params,"history_truncated":self.history.truncated,"usage_scope":"unverified"}),
        );
        self.model_step = Some(ModelStep {
            row,
            content: Vec::new(),
            text: String::new(),
            bytes: 0,
            truncated: false,
        });
    }
    fn mark_history_truncated(&mut self) {
        self.history.truncated = true;
        if let Some(metadata) = self
            .model_step
            .as_mut()
            .and_then(|step| step.row.metadata.as_mut())
        {
            metadata["history_truncated"] = json!(true);
        }
    }
    fn model_content(&mut self, content: Value, ts: i64, ops: &mut Vec<SpanOp>) {
        if let Some(turn) = self.turn.as_mut().filter(|turn| turn.end.is_some()) {
            // Late output belongs to the closed turn. Its receipt time does
            // not establish another model request or extend model latency.
            if let Some(text) = content
                .get("text")
                .and_then(Value::as_str)
                .filter(|_| content.get("type").and_then(Value::as_str) == Some("text"))
            {
                self.history
                    .push(json!({"role":"assistant","content":text}));
                self.seen.insert(format!(
                    "output-text:{}:{}",
                    turn.id,
                    ids::span_id(&self.namespace, text)
                ));
                if turn.emitted_text.len() + text.len() <= MAX_HISTORY_BYTES {
                    turn.emitted_text.push_str(text);
                } else {
                    self.history.truncated = true;
                }
            }
            return;
        }
        self.ensure_model(ts, ops);
        self.model_step.as_mut().unwrap().push(content);
    }
    fn close_model(&mut self, ts: i64, ops: &mut Vec<SpanOp>) {
        let Some(mut step) = self.model_step.take() else {
            return;
        };
        step.row.end_ms = Some(ts.max(step.row.start_ms.unwrap_or(ts)));
        if let Some(turn) = self
            .turn
            .as_mut()
            .filter(|t| step.row.parent_span_ids.first() == Some(&t.id))
        {
            let text = step
                .content
                .iter()
                .filter(|v| v.get("type").and_then(Value::as_str) == Some("text"))
                .filter_map(|v| v.get("text").and_then(Value::as_str))
                .collect::<String>();
            if turn.emitted_text.len() + text.len() <= MAX_HISTORY_BYTES {
                turn.emitted_text.push_str(&text);
            }
        }
        if let Some(owner) = step.row.parent_span_ids.first() {
            for text in step
                .content
                .iter()
                .filter(|v| v.get("type").and_then(Value::as_str) == Some("text"))
                .filter_map(|v| v.get("text").and_then(Value::as_str))
            {
                self.seen.insert(format!(
                    "output-text:{owner}:{}",
                    ids::span_id(&self.namespace, text)
                ));
            }
        }
        let message = assistant_message(&step.content);
        // Tool requests establish a tool-call boundary. Other hooks do not
        // report the provider's finish reason, so leave that value unknown.
        let finish_reason = message.get("tool_calls").map(|_| "tool_calls");
        step.row.output = Some(json!([{
            "index": 0, "finish_reason": finish_reason, "message": message,
        }]));
        if let Some(metadata) = step.row.metadata.as_mut().and_then(Value::as_object_mut) {
            metadata.insert("output_truncated".into(), json!(step.truncated));
        }
        self.history.push(message);
        ops.push(SpanOp::Insert(step.row));
        if let Some(turn) = &mut self.turn {
            turn.boundary = ts.max(turn.boundary);
        }
    }
    fn close_turn(&mut self, ts: i64, status: Option<&str>, ops: &mut Vec<SpanOp>) {
        self.close_model(ts, ops);
        let Some(turn) = self.turn.as_mut().filter(|t| t.end.is_none()) else {
            return;
        };
        turn.end = Some(ts.max(turn.start));
        let copy = turn.clone();
        let mut row = self.row(
            copy.id,
            self.root.clone(),
            &format!("Turn {}", self.turn_seq),
            SpanType::Task,
        );
        row.end_ms = copy.end;
        row.output = copy.output.map(|s| json!(s));
        row.metadata = Some(json!({"status":status}));
        if matches!(status, Some("error" | "aborted")) {
            row.error = Some(status.unwrap().into());
        }
        let turn_id = row.span_id.clone();
        ops.push(SpanOp::Merge(row));
        let stranded: Vec<String> = self
            .tools
            .iter()
            .filter(|(_, tool)| tool.turn == turn_id)
            .map(|(id, _)| id.clone())
            .collect();
        for id in stranded {
            if let Some(tool) = self.remove_open_tool(&id) {
                self.incomplete_tool(tool, ts, ops);
            }
        }
    }
    fn tool(&mut self, e: &Envelope, ops: &mut Vec<SpanOp>) {
        let Some(call_id) = e.payload.get("tool_use_id").and_then(Value::as_str) else {
            return;
        };
        let key = call_id.to_string();
        let terminal = e.event != "preToolUse";
        let late_incomplete = self
            .completed
            .get(&key)
            .filter(|t| {
                t.row
                    .metadata
                    .as_ref()
                    .and_then(|m| m.get("status"))
                    .and_then(Value::as_str)
                    == Some("incomplete")
            })
            .cloned();
        if self.completed.get(&key).is_some() && (late_incomplete.is_none() || !terminal) {
            return;
        }
        if let Some(tool) = late_incomplete {
            self.completed.remove(&key);
            self.insert_open_tool(key.clone(), tool, e.ts_ms, ops);
        }

        if !self.tools.contains_key(&key) {
            let native_turn = e
                .payload
                .get("generation_id")
                .and_then(Value::as_str)
                .and_then(|g| self.prompt_generations.get(g))
                .map(|t| t.id.clone());
            if native_turn.is_none() {
                self.ensure_turn(e.ts_ms, None, None, "hook_order", ops);
            }
            let turn = native_turn.unwrap_or_else(|| self.turn.as_ref().unwrap().id.clone());
            let name = e
                .payload
                .get("tool_name")
                .and_then(Value::as_str)
                .unwrap_or("Cursor tool");
            let mut row = self.row(
                ids::span_id(&self.namespace, &format!("tool:{call_id}")),
                turn.clone(),
                name,
                SpanType::Tool,
            );
            let duration = e
                .payload
                .get("duration")
                .and_then(Value::as_f64)
                .filter(|n| n.is_finite() && *n >= 0.0);
            row.start_ms = Some(if terminal {
                e.ts_ms
                    .saturating_sub(duration.unwrap_or(0.0).min(i64::MAX as f64) as i64)
            } else {
                e.ts_ms
            });
            row.input = e.payload.get("tool_input").cloned();
            row.metadata = Some(
                json!({"tool_use_id":call_id,"generation_id":e.payload.get("generation_id"),"turn_attribution":"active_prompt_boundary",
                "start_time_estimated":terminal,"result_completeness":"unknown","parent_tool_call_id":e.payload.get("parent_tool_call_id")}),
            );
            ops.push(SpanOp::Insert(row.clone()));
            self.insert_open_tool(
                key.clone(),
                Tool {
                    row,
                    turn,
                    call_id: call_id.into(),
                    ended: false,
                },
                e.ts_ms,
                ops,
            );
        }
        if !terminal {
            let tool = self.tools.get(&key).unwrap();
            if self
                .turn
                .as_ref()
                .is_none_or(|t| t.id != tool.turn || t.end.is_some())
            {
                return;
            }
            self.model_content(
                json!({"type":"tool_use","id":call_id,"name":tool.row.name,"input":tool.row.input}),
                e.ts_ms,
                ops,
            );
            self.close_model(e.ts_ms, ops);
            return;
        }
        let current = self.turn.as_ref().is_some_and(|t| {
            self.tools.get(&key).is_some_and(|tool| t.id == tool.turn) && t.end.is_none()
        });
        if current {
            let terminal_only = self.tools.get(&key).and_then(|tool| {
                tool.row
                    .metadata
                    .as_ref()
                    .is_some_and(|metadata| {
                        metadata
                            .get("start_time_estimated")
                            .and_then(Value::as_bool)
                            == Some(true)
                    })
                    .then(|| {
                        (
                            tool.row.start_ms.unwrap_or(e.ts_ms),
                            tool.call_id.clone(),
                            tool.row.name.clone(),
                            tool.row.input.clone(),
                        )
                    })
            });
            if let Some((boundary, call_id, name, input)) = terminal_only {
                // Without preToolUse, the duration is the only available
                // tool-request boundary. Tool execution is not model latency.
                self.model_content(
                    json!({
                        "type":"tool_use",
                        "id":call_id,
                        "name":name,
                        "input":input,
                    }),
                    boundary,
                    ops,
                );
                self.close_model(boundary, ops);
            }
        }
        let mut tool = self.remove_open_tool(&key).unwrap();
        tool.ended = true;
        tool.row.end_ms = Some(e.ts_ms.max(tool.row.start_ms.unwrap_or(e.ts_ms)));
        if tool
            .row
            .metadata
            .as_ref()
            .is_some_and(|m| m.get("result_source").is_some())
        {
            tool.row.metadata.as_mut().unwrap()["native_tool_result"] =
                e.payload.get("tool_output").cloned().unwrap_or(Value::Null);
        } else {
            tool.row.output = e.payload.get("tool_output").cloned();
        }
        let failed = e.event == "postToolUseFailure";
        let mut metadata = tool.row.metadata.take().unwrap_or(json!({}));
        metadata["status"] = json!(if failed { "error" } else { "completed" });
        if let Some(duration) = e
            .payload
            .get("duration")
            .and_then(Value::as_f64)
            .filter(|n| n.is_finite() && *n >= 0.0)
        {
            metadata["native_duration_ms"] = json!(duration);
        }
        if failed {
            tool.row.error = Some(
                e.payload
                    .get("error_message")
                    .and_then(Value::as_str)
                    .unwrap_or("Cursor tool failed")
                    .into(),
            );
            metadata["failure_type"] = e
                .payload
                .get("failure_type")
                .cloned()
                .unwrap_or(Value::Null);
            metadata["is_interrupt"] = e
                .payload
                .get("is_interrupt")
                .cloned()
                .unwrap_or(Value::Null);
            if e.payload.get("failure_type").and_then(Value::as_str) == Some("permission_denied") {
                metadata["tool_approval"] = json!("denied");
            }
        }
        tool.row.metadata = Some(metadata);
        tool.row.late_merge_key = Some(format!(
            "cursor-tool-terminal:{call_id}:{}",
            ids::span_id(&self.namespace, &e.payload.to_string())
        ));
        ops.push(SpanOp::Merge(tool.row.clone()));
        if current {
            if let Some(output) = tool.row.output.as_ref() {
                self.history
                    .push(json!({"role":"tool", "tool_call_id":call_id,
                    "content":tool_result_content(Some(output))}));
            } else {
                // Do not invent an empty result when the hook omitted the
                // content that the model received.
                self.history.truncated = true;
            }
        }
        if let Some(t) = self.turn.as_mut().filter(|t| t.id == tool.turn) {
            t.boundary = t.boundary.max(e.ts_ms);
        }
        self.completed.insert(key, tool);
    }
    fn incomplete_tool(&mut self, mut tool: Tool, ts: i64, ops: &mut Vec<SpanOp>) {
        tool.row.end_ms = Some(ts.max(tool.row.start_ms.unwrap_or(ts)));
        tool.row.metadata.as_mut().unwrap()["status"] = json!("incomplete");
        tool.ended = true;
        ops.push(SpanOp::Merge(tool.row.clone()));
        self.completed.insert(tool.call_id.clone(), tool);
    }
    fn insert_open_tool(&mut self, key: String, tool: Tool, ts: i64, ops: &mut Vec<SpanOp>) {
        if self.tools.contains_key(&key) {
            self.open_tool_order.retain(|candidate| candidate != &key);
        }
        while self.tools.len() >= MAX_OPEN_TOOLS {
            let Some(oldest) = self.open_tool_order.pop_front() else {
                break;
            };
            if let Some(old) = self.tools.remove(&oldest) {
                self.incomplete_tool(old, ts, ops);
            }
        }
        self.open_tool_order.push_back(key.clone());
        self.tools.insert(key, tool);
    }
    fn remove_open_tool(&mut self, key: &str) -> Option<Tool> {
        self.open_tool_order.retain(|candidate| candidate != key);
        self.tools.remove(key)
    }
    fn specialized(&mut self, e: &Envelope, ops: &mut Vec<SpanOp>) {
        if !matches!(
            e.event.as_str(),
            "afterShellExecution" | "afterMCPExecution" | "afterFileEdit" | "beforeReadFile"
        ) {
            return;
        }
        let explicit_id = e.payload.get("tool_use_id").and_then(Value::as_str);
        let candidates: Vec<String> = self
            .tools
            .iter()
            .chain(self.completed.iter())
            .filter_map(|(id, tool)| {
                let compatible = match e.event.as_str() {
                    "afterShellExecution" => tool.row.name == "Shell",
                    "afterMCPExecution" => {
                        tool.row.name.strip_prefix("MCP:").unwrap_or(&tool.row.name)
                            == e.payload
                                .get("tool_name")
                                .and_then(Value::as_str)
                                .unwrap_or("")
                    }
                    "beforeReadFile" => tool.row.name == "Read",
                    "afterFileEdit" => tool.row.name == "Write",
                    _ => false,
                };
                if !compatible {
                    return None;
                }
                if let Some(native) = explicit_id {
                    return (native == id).then(|| id.clone());
                }
                if self.turn.as_ref().is_none_or(|t| t.id != tool.turn) {
                    return None;
                }
                let input = tool.row.input.as_ref()?;
                let matches = match e.event.as_str() {
                    "afterShellExecution" => {
                        input.get("command") == e.payload.get("command")
                            && e.payload.get("command").is_some()
                    }
                    "afterMCPExecution" => {
                        tool.row.name.strip_prefix("MCP:").unwrap_or(&tool.row.name)
                            == e.payload
                                .get("tool_name")
                                .and_then(Value::as_str)
                                .unwrap_or("")
                            && input
                                == &parse_json_string(
                                    e.payload.get("tool_input").cloned().unwrap_or(Value::Null),
                                )
                    }
                    "beforeReadFile" | "afterFileEdit" => {
                        input.get("file_path").or_else(|| input.get("path"))
                            == e.payload.get("file_path")
                            && e.payload.get("file_path").is_some()
                    }
                    _ => false,
                };
                matches.then(|| id.clone())
            })
            .collect();
        if candidates.len() != 1 {
            return;
        }
        let id = &candidates[0];
        let mut copy = self
            .tools
            .get(id)
            .cloned()
            .or_else(|| self.completed.get(id).cloned())
            .unwrap();
        if e.event == "beforeReadFile" {
            if let Some(content) = e.payload.get("content") {
                copy.row.metadata.as_mut().unwrap()["pre_read_content"] = content.clone();
                copy.row.metadata.as_mut().unwrap()["pre_read_content_source"] =
                    json!("beforeReadFile");
                ops.push(SpanOp::Merge(copy.row.clone()));
                if copy.ended {
                    self.completed.insert(id.clone(), copy);
                } else {
                    self.tools.insert(id.clone(), copy);
                }
            }
            return;
        }
        let output = e
            .payload
            .get("output")
            .or_else(|| e.payload.get("result_json"))
            .or_else(|| e.payload.get("edits"));
        if let Some(output) = output {
            copy.row.output = Some(output.clone());
            copy.row.metadata.as_mut().unwrap()["result_source"] = json!(e.event);
            copy.row.metadata.as_mut().unwrap()["result_completeness"] =
                json!("native_specialized_payload");
            copy.row.late_merge_key = Some(format!("cursor-specialized:{}:{}", id, e.event));
            ops.push(SpanOp::Merge(copy.row.clone()));
            if copy.ended {
                for message in &mut self.history.messages {
                    if message.get("tool_call_id").and_then(Value::as_str) == Some(id) {
                        message["content"] = json!(tool_result_content(Some(output)));
                    }
                }
                self.history.bytes = self
                    .history
                    .messages
                    .iter()
                    .map(|m| m.to_string().len())
                    .sum();
                while self.history.bytes > MAX_HISTORY_BYTES {
                    if let Some(old) = self.history.messages.pop_front() {
                        self.history.bytes -= old.to_string().len();
                        self.history.truncated = true;
                    } else {
                        break;
                    }
                }
            }
            if copy.ended {
                self.completed.insert(id.clone(), copy);
            } else {
                self.tools.insert(id.clone(), copy);
            }
        }
    }
    fn transcript_batch(&mut self, e: &Envelope, ops: &mut Vec<SpanOp>) -> anyhow::Result<bool> {
        let Some(reference) = e.payload.get("_bt_transcript_mirror") else {
            return Ok(true);
        };
        let (Some(path), Some(through)) = (
            reference.get("mirror").and_then(Value::as_str),
            reference.get("through").and_then(Value::as_u64),
        ) else {
            return Ok(true);
        };
        if self.transcript.path != path {
            self.transcript = TranscriptCursor {
                path: path.into(),
                ..Default::default()
            };
            self.transcript_users = 0;
            self.transcript_owner = None;
        }
        if self.transcript.offset >= through {
            return Ok(true);
        }
        let (records, truncated, complete) = self.transcript.read_batch(path, through)?;
        self.history.truncated |= truncated;
        for record in records {
            let role = record.get("role").and_then(Value::as_str);
            if role == Some("user") {
                self.transcript.assistant_records = 0;
                // User ownership must be resolved again after a rewrite:
                // a truncated view may put a later native turn at line 1.
                self.transcript_record(&record, e.ts_ms, ops);
            } else {
                if role == Some("assistant") {
                    self.transcript.assistant_records += 1;
                }
                let key = format!(
                    "transcript:{:?}:{}:{}:{}",
                    role,
                    self.transcript_owner.as_deref().unwrap_or("unattributed"),
                    self.transcript.assistant_records,
                    ids::span_id(&self.namespace, &record.to_string())
                );
                if self.seen.insert(key) {
                    self.transcript_record(&record, e.ts_ms, ops);
                }
            }
        }
        Ok(complete)
    }
    fn transcript_record(&mut self, record: &Value, ts: i64, ops: &mut Vec<SpanOp>) {
        let role = record.get("role").and_then(Value::as_str).unwrap_or("");
        let content = record.get("message").and_then(|m| m.get("content"));
        if role == "user" {
            if content.is_none() {
                self.history.truncated = true;
                return;
            }
            self.transcript_users += 1;
            let full_prompt = content.map(text_content).unwrap_or_default();
            let prompt = transcript_prompt(&full_prompt).to_string();
            if let Some((known_prompt, id)) =
                self.transcript_prompts.get(&self.transcript_users).cloned()
            {
                if known_prompt.trim() == prompt.trim() {
                    self.transcript_owner = Some(id);
                    return;
                }
            }
            let native_match = self
                .prompt_generations
                .iter()
                .find(|(_, t)| {
                    t.ordinal == self.transcript_users
                        && t.prompt
                            .as_deref()
                            .is_some_and(|p| p.trim() == prompt.trim())
                })
                .map(|(_, t)| t.id.clone());
            if let Some(id) = native_match {
                self.transcript_prompts
                    .insert(self.transcript_users, (prompt.clone(), id.clone()));
                self.transcript_owner = Some(id);
                return;
            }
            let matching = self.turn.as_ref().is_some_and(|t| {
                t.end.is_none()
                    && (t.prompt.is_none()
                        || t.prompt
                            .as_deref()
                            .is_some_and(|p| p.trim() == prompt.trim()))
            });
            if matching {
                let turn = self.turn.as_mut().unwrap();
                let add = turn.prompt.is_none();
                turn.prompt = Some(prompt.clone());
                let id = turn.id.clone();
                let mut row = self.row(
                    id.clone(),
                    self.root.clone(),
                    &format!("Turn {}", self.turn_seq),
                    SpanType::Task,
                );
                row.input = Some(json!(prompt));
                ops.push(SpanOp::Merge(row));
                if add {
                    self.history.push(json!({"role":"user","content":prompt}));
                }
                self.transcript_prompts
                    .insert(self.transcript_users, (prompt.clone(), id));
            } else {
                self.close_turn(ts, Some("transcript_boundary"), ops);
                self.ensure_turn(ts, Some(prompt), None, "transcript_user_message", ops);
                self.transcript_prompts.insert(
                    self.transcript_users,
                    (
                        self.turn
                            .as_ref()
                            .unwrap()
                            .prompt
                            .clone()
                            .unwrap_or_default(),
                        self.turn.as_ref().unwrap().id.clone(),
                    ),
                );
            }
            self.transcript_owner = self
                .transcript_prompts
                .get(&self.transcript_users)
                .map(|(_, id)| id.clone());
        } else if role == "assistant" {
            if let Some(content) = content {
                let text = text_content(content);
                if text.is_empty() {
                    return;
                }
                if let Some(owner) = self
                    .transcript_owner
                    .as_ref()
                    .or_else(|| self.turn.as_ref().map(|t| &t.id))
                {
                    let key = format!(
                        "output-text:{owner}:{}",
                        ids::span_id(&self.namespace, &text)
                    );
                    if self.seen.contains(&key) {
                        return;
                    }
                }
                if let Some(owner) = self
                    .transcript_owner
                    .clone()
                    .filter(|id| self.turn.as_ref().is_none_or(|t| &t.id != id))
                {
                    // Delayed history cannot be assigned to an individual model
                    // request. Enrich its known user turn without reparenting the
                    // active model step or inventing a historical call.
                    let mut row = self.row(owner, self.root.clone(), "", SpanType::Task);
                    row.output = Some(json!(text));
                    row.metadata = Some(
                        json!({"late_transcript_enrichment":true,"historical_model_grouping_available":false}),
                    );
                    row.late_merge_key = Some(format!(
                        "cursor-transcript:{}",
                        ids::span_id(&self.namespace, &record.to_string())
                    ));
                    ops.push(SpanOp::Merge(row));
                    self.mark_history_truncated();
                    return;
                }
                if self
                    .turn
                    .as_ref()
                    .is_some_and(|t| t.output.as_deref() == Some(&text))
                {
                    return;
                }
                self.model_content(json!({"type":"text","text":text}), ts, ops);
                if let Some(t) = &mut self.turn {
                    t.output = Some(text);
                }
                self.merge_late_answer(ops);
                // Tool inputs have no IDs and may differ from hook arguments;
                // they do not create a second copy of the native tool span.
            }
        }
    }
    fn merge_late_answer(&mut self, ops: &mut Vec<SpanOp>) {
        let Some(turn) = self.turn.as_ref().filter(|t| t.end.is_some()).cloned() else {
            return;
        };
        let mut row = self.row(turn.id, self.root.clone(), "", SpanType::Task);
        row.output = turn.output.map(|s| json!(s));
        row.metadata = Some(json!({"late_output_enrichment":true}));
        row.late_merge_key = Some(format!(
            "cursor-late-answer:{}:{}",
            row.span_id,
            ids::span_id(
                &self.namespace,
                &row.output.as_ref().unwrap_or(&Value::Null).to_string()
            )
        ));
        ops.push(SpanOp::Merge(row));
    }
    fn subagent(&mut self, e: &Envelope, ops: &mut Vec<SpanOp>) {
        let Some(id) = e.payload.get("subagent_id").and_then(Value::as_str) else {
            return;
        };
        if e.event == "subagentStart" {
            if self.subagents.contains_key(id) {
                return;
            }
            self.ensure_turn(e.ts_ms, None, None, "hook_order", ops);
            let spawn = e.payload.get("tool_call_id").and_then(Value::as_str);
            let parent = spawn
                .and_then(|s| self.tools.get(s).or_else(|| self.completed.get(s)))
                .map(|t| t.turn.clone())
                .unwrap_or_else(|| self.turn.as_ref().unwrap().id.clone());
            let mut row = self.row(
                ids::span_id(&self.namespace, &format!("subagent:{id}")),
                parent,
                "Cursor subagent",
                SpanType::Task,
            );
            row.start_ms = Some(e.ts_ms);
            row.input = e.payload.get("task").cloned();
            row.metadata = Some(
                json!({"subagent_id":id,"spawning_tool_call_id":spawn,"model":e.payload.get("subagent_model"),"subagent_type":e.payload.get("subagent_type"),"recursive_activity_verified":false}),
            );
            if self.subagents.len() >= MAX_OPEN_TOOLS {
                return;
            }
            self.subagents.insert(id.into(), row.clone());
            ops.push(SpanOp::Insert(row));
        } else if let Some(mut row) = self.subagents.remove(id) {
            row.end_ms = Some(e.ts_ms.max(row.start_ms.unwrap_or(e.ts_ms)));
            row.output = e.payload.get("summary").cloned();
            row.metadata.as_mut().unwrap()["status"] =
                e.payload.get("status").cloned().unwrap_or(Value::Null);
            if matches!(
                e.payload.get("status").and_then(Value::as_str),
                Some("error" | "aborted")
            ) {
                row.error = Some(e.payload["status"].as_str().unwrap().into());
            }
            ops.push(SpanOp::Merge(row));
        }
    }
    fn turn_usage(&mut self, e: &Envelope, ops: &mut Vec<SpanOp>) {
        let generation = e.payload.get("generation_id").and_then(Value::as_str);
        // Verified interactive CLI/desktop emissions are totals for one user
        // generation, repeated on response and stop. Merge, never sum.
        let Some((generation, mut turn)) =
            generation.and_then(|g| Some((g, self.prompt_generations.get(g)?.clone())))
        else {
            return;
        };
        let mut metrics = serde_json::Map::new();
        for (native, metric) in [
            ("input_tokens", "prompt_tokens"),
            ("output_tokens", "completion_tokens"),
            ("cache_read_tokens", "prompt_cached_tokens"),
            ("cache_write_tokens", "prompt_cache_creation_tokens"),
        ] {
            if let Some(count) = e.payload.get(native).and_then(Value::as_u64) {
                metrics.insert(metric.into(), json!(count));
            }
        }
        if metrics.is_empty() {
            return;
        }
        // Response and stop can report partial snapshots for the same turn.
        // Merge known counts rather than fabricating missing values or summing
        // repeated observations. Cache counts remain subsets of prompt usage.
        turn.usage.extend(metrics);
        if let Some(total) = turn
            .usage
            .get("prompt_tokens")
            .and_then(Value::as_u64)
            .zip(turn.usage.get("completion_tokens").and_then(Value::as_u64))
            .and_then(|(prompt, completion)| prompt.checked_add(completion))
        {
            turn.usage.insert("tokens".into(), json!(total));
        }
        let mut row = self.row(
            turn.id.clone(),
            self.root.clone(),
            &format!("Turn {}", turn.ordinal),
            SpanType::Task,
        );
        row.metrics = Some(Value::Object(turn.usage.clone()));
        row.metadata = Some(json!({"token_usage_scope":"turn"}));
        self.prompt_generations.insert(generation.into(), turn);
        // A stop/response can enrich a row already closed by a transcript boundary.
        row.late_merge_key = Some(format!(
            "cursor-turn-usage:{}:{}",
            row.span_id,
            ids::span_id(&self.namespace, &row.metrics.as_ref().unwrap().to_string())
        ));
        ops.push(SpanOp::Merge(row));
    }
    fn reduce_hook(&mut self, e: &Envelope, ops: &mut Vec<SpanOp>) {
        let mut native = e.payload.clone();
        if let Some(map) = native.as_object_mut() {
            map.retain(|k, _| !k.starts_with("_bt_"));
        }
        let generation = native.get("generation_id").and_then(Value::as_str);
        let scope = if native.get("tool_use_id").is_some() {
            "native_tool".to_string()
        } else if matches!(
            e.event.as_str(),
            "beforeSubmitPrompt" | "afterAgentResponse" | "stop"
        ) {
            generation
                .map(str::to_string)
                .unwrap_or_else(|| format!("capture:{}", e.ts_ms))
        } else if matches!(
            e.event.as_str(),
            "sessionStart"
                | "sessionEnd"
                | "beforeReadFile"
                | "afterShellExecution"
                | "afterMCPExecution"
                | "afterFileEdit"
        ) {
            format!("capture:{}", e.ts_ms)
        } else {
            self.turn.as_ref().map(|t| t.id.clone()).unwrap_or_default()
        };
        let fingerprint = format!(
            "hook:{}:{scope}:{}",
            e.event,
            ids::span_id(&self.namespace, &native.to_string())
        );
        if !self.seen.insert(fingerprint) {
            return;
        }
        match e.event.as_str() {
            "beforeSubmitPrompt" => self.prompt(e, ops),
            "preToolUse" | "postToolUse" | "postToolUseFailure" => self.tool(e, ops),
            "afterAgentThought" => {
                if let Some(text) = e.payload.get("text") {
                    self.model_content(json!({"type":"thinking","text":text}), e.ts_ms, ops);
                }
            }
            "afterAgentResponse" => {
                let owner = e
                    .payload
                    .get("generation_id")
                    .and_then(Value::as_str)
                    .and_then(|g| self.prompt_generations.get(g))
                    .map(|t| t.id.clone());
                if let Some(owner) =
                    owner.filter(|id| self.turn.as_ref().is_none_or(|t| &t.id != id))
                {
                    let mut row = self.row(owner, self.root.clone(), "", SpanType::Task);
                    row.output = e.payload.get("text").cloned();
                    row.metadata = Some(
                        json!({"late_output_enrichment":true,"historical_model_grouping_available":false}),
                    );
                    row.late_merge_key = Some(format!(
                        "cursor-response:{}",
                        ids::span_id(&self.namespace, &native.to_string())
                    ));
                    ops.push(SpanOp::Merge(row));
                    self.mark_history_truncated();
                    self.turn_usage(e, ops);
                } else {
                    if let Some(text) = e.payload.get("text").and_then(Value::as_str) {
                        if self
                            .turn
                            .as_ref()
                            .is_none_or(|t| t.output.as_deref() != Some(text))
                        {
                            let unseen = self
                                .turn
                                .as_ref()
                                .and_then(|t| text.strip_prefix(&t.emitted_text))
                                .unwrap_or(text);
                            if !unseen.is_empty() {
                                self.model_content(
                                    json!({"type":"text","text":unseen}),
                                    e.ts_ms,
                                    ops,
                                );
                            }
                            if let Some(t) = &mut self.turn {
                                t.output = Some(text.into());
                            }
                        }
                    }
                    self.turn_usage(e, ops);
                    self.close_model(e.ts_ms, ops);
                    self.merge_late_answer(ops);
                }
            }
            "stop" => {
                self.turn_usage(e, ops);
                let prior = e
                    .payload
                    .get("generation_id")
                    .and_then(Value::as_str)
                    .and_then(|g| self.prompt_generations.get(g))
                    .filter(|t| self.turn.as_ref().is_none_or(|current| current.id != t.id))
                    .cloned();
                if let Some(prior) = prior {
                    let mut row = self.row(prior.id, self.root.clone(), "", SpanType::Task);
                    row.end_ms = Some(e.ts_ms.max(prior.start));
                    row.metadata = Some(json!({"status":e.payload.get("status")}));
                    if matches!(
                        e.payload.get("status").and_then(Value::as_str),
                        Some("error" | "aborted")
                    ) {
                        row.error = Some(e.payload["status"].as_str().unwrap().into());
                    }
                    row.late_merge_key = Some(format!(
                        "cursor-stop:{}",
                        ids::span_id(&self.namespace, &native.to_string())
                    ));
                    ops.push(SpanOp::Merge(row));
                } else {
                    self.close_turn(
                        e.ts_ms,
                        e.payload.get("status").and_then(Value::as_str),
                        ops,
                    );
                }
            }
            "sessionEnd" => {
                self.close_turn(
                    e.ts_ms,
                    e.payload.get("reason").and_then(Value::as_str),
                    ops,
                );
                let mut row = self.row(
                    self.root.clone(),
                    self.root.clone(),
                    "Cursor session",
                    SpanType::Task,
                );
                row.parent_span_ids = self.root_parents.clone();
                row.end_ms = Some(e.ts_ms);
                row.metadata = Some(
                    json!({"status":e.payload.get("reason"),"final_status":e.payload.get("final_status")}),
                );
                row.output = self
                    .turn
                    .as_ref()
                    .and_then(|t| t.output.as_ref())
                    .map(|s| json!(s));
                if e.payload.get("reason").and_then(Value::as_str) == Some("error") {
                    row.error = Some(
                        e.payload
                            .get("error_message")
                            .and_then(Value::as_str)
                            .unwrap_or("Cursor session failed")
                            .into(),
                    );
                }
                ops.push(SpanOp::Merge(row));
            }
            "subagentStart" | "subagentStop" => self.subagent(e, ops),
            "preCompact" => {
                self.close_model(e.ts_ms, ops);
                self.compact_seq += 1;
                let mut row = self.row(
                    ids::span_id(&self.namespace, &format!("compaction:{}", self.compact_seq)),
                    self.root.clone(),
                    "Cursor compaction requested",
                    SpanType::Task,
                );
                row.start_ms = Some(e.ts_ms);
                row.input = Some(native);
                row.end_ms = Some(e.ts_ms);
                row.tags = Some(vec!["compaction".into()]);
                row.metadata = Some(
                    json!({"observation_only":true,"completion_observed":false,"replacement_context_available":false}),
                );
                ops.push(SpanOp::Insert(row));
                // No observable replacement context: do not carry stale precompact
                // history forward as if it were the new model request.
                self.history = History {
                    truncated: true,
                    ..Default::default()
                };
            }
            _ => self.specialized(e, ops),
        }
    }
    fn enrich(&self, ops: &mut [SpanOp], ctx: &SessionCtx) {
        self.git.enrich_rows(self.cwd.as_deref(), ops);
        for op in ops {
            let root_insert = matches!(op, SpanOp::Insert(row) if row.span_id == self.root);
            let (SpanOp::Insert(row) | SpanOp::Merge(row)) = op;
            if let Some(metadata) = row.metadata.as_mut().and_then(Value::as_object_mut) {
                // Only the reviewed Cursor fields reach span metadata. Raw
                // correlation/enrichment fields remain in the durable journal.
                metadata.retain(|key, value| {
                    CURSOR_METADATA_FIELDS.contains(&key.as_str())
                        && !value.is_null()
                        && !(matches!(
                            key.as_str(),
                            "start_time_estimated"
                                | "history_truncated"
                                | "output_truncated"
                                | "is_interrupt"
                        ) && value == &json!(false))
                });
                if root_insert {
                    // Destination metadata is an explicit user customization,
                    // rather than additional automatically captured hook data.
                    if let Some(additional) = ctx
                        .config
                        .as_ref()
                        .and_then(|c| c.additional_metadata.as_ref())
                        .and_then(Value::as_object)
                    {
                        for (key, value) in additional {
                            if !key.starts_with("_bt_") {
                                metadata.entry(key.clone()).or_insert_with(|| value.clone());
                            }
                        }
                    }
                }
            }
        }
    }
}
impl AgentTranslator for CursorTranslator {
    fn handle(&mut self, e: &Envelope, ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        anyhow::ensure!(
            self.pending.is_none(),
            "Cursor pending transcript work must be drained before the next hook"
        );
        let mut ops = Vec::new();
        self.root(e, ctx, &mut ops);
        if self.transcript_batch(e, &mut ops)? {
            self.reduce_hook(e, &mut ops);
        } else {
            self.pending = Some(e.clone());
        }
        self.enrich(&mut ops, ctx);
        Ok(ops)
    }
    fn drain_pending(&mut self, ctx: &SessionCtx) -> anyhow::Result<Option<Vec<SpanOp>>> {
        let Some(e) = self.pending.take() else {
            return Ok(None);
        };
        let mut ops = Vec::new();
        if self.transcript_batch(&e, &mut ops)? {
            self.reduce_hook(&e, &mut ops);
        } else {
            self.pending = Some(e);
        }
        self.enrich(&mut ops, ctx);
        Ok(Some(ops))
    }
    fn finalize(&mut self, ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        let mut ops = Vec::new();
        self.close_turn(self.last_ms, Some("capture_ended"), &mut ops);
        let tools = std::mem::take(&mut self.tools);
        self.open_tool_order.clear();
        for (_, tool) in tools {
            self.incomplete_tool(tool, self.last_ms, &mut ops);
        }
        for (_, mut row) in std::mem::take(&mut self.subagents) {
            row.end_ms = Some(self.last_ms.max(row.start_ms.unwrap_or(self.last_ms)));
            row.metadata.as_mut().unwrap()["status"] = json!("incomplete");
            ops.push(SpanOp::Merge(row));
        }
        if self.started {
            ops.push(SpanOp::Merge(SpanRow {
                span_id: self.root.clone(),
                root_span_id: self.trace_root.clone(),
                parent_span_ids: self.root_parents.clone(),
                name: "Cursor session".into(),
                span_type: SpanType::Task,
                end_ms: Some(self.last_ms),
                ..Default::default()
            }));
        }
        self.enrich(&mut ops, ctx);
        Ok(ops)
    }
}
fn assistant_message(content: &[Value]) -> Value {
    let mut text = String::new();
    let mut reasoning = Vec::new();
    let mut calls = Vec::new();
    for part in content {
        match part.get("type").and_then(Value::as_str) {
            Some("text") => {
                if let Some(value) = part.get("text").and_then(Value::as_str) {
                    text.push_str(value);
                }
            }
            Some("thinking") => {
                if let Some(value) = part.get("text").and_then(Value::as_str) {
                    reasoning.push(json!({"type":"summary_text", "text":value}));
                }
            }
            Some("tool_use") => calls.push(json!({
                "id": part.get("id"), "type": "function",
                "function": {"name": part.get("name"),
                    "arguments": tool_result_content(part.get("input"))},
            })),
            _ => {}
        }
    }
    let mut message = json!({"role":"assistant", "content":text});
    if !reasoning.is_empty() {
        message["reasoning"] = json!([{"type":"reasoning", "summary":reasoning}]);
    }
    if !calls.is_empty() {
        if text.is_empty() {
            message["content"] = Value::Null;
        }
        message["tool_calls"] = Value::Array(calls);
    }
    message
}

fn tool_result_content(value: Option<&Value>) -> String {
    match value {
        Some(Value::String(text)) => text.clone(),
        Some(value) => value.to_string(),
        None => String::new(),
    }
}

fn parse_json_string(value: Value) -> Value {
    value
        .as_str()
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or(value)
}
fn text_content(content: &Value) -> String {
    if let Some(text) = content.as_str() {
        return text.into();
    }
    content
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|b| b.get("text").and_then(Value::as_str))
        .collect::<String>()
}

/// CLI saved messages wrap the submitted prompt in native timestamp/query tags.
/// Use the exact observed wrapper only for correlation with the prompt hook.
fn transcript_prompt(text: &str) -> &str {
    if text.starts_with("<timestamp>") {
        if let Some((_, query)) = text.split_once("</timestamp>\n<user_query>\n") {
            if let Some(query) = query.strip_suffix("\n</user_query>") {
                return query;
            }
        }
    }
    text
}
