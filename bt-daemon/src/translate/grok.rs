//! Grok transcript-authoritative translator.
//!
//! Hooks are wake-up and flush signals. `updates.jsonl` is the authoritative
//! trace data source; `events.jsonl` is independently tailed, best-effort tool
//! enrichment. Both are mirrored into daemon-owned storage at hook boundaries.

use super::recent::{RecentMap, RecentSet};
use super::{AgentTranslator, SessionCtx, SpanOp, SpanRow, SpanType, TranslatorFactory};
use crate::ids;
use crate::wire::Envelope;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom};

const CATCH_UP_BYTE_BUDGET: u64 = 64 * 1024;
const CATCH_UP_RECORD_BUDGET: usize = 256;
const MAX_RECORD_BYTES: usize = 1024 * 1024;
const CURSOR_TAIL_BYTES: usize = 128;
const MAX_OPEN_TOOLS: usize = 256;
const MAX_OUTPUT_CHUNKS: usize = 2_048;
const MAX_OUTPUT_BYTES: usize = 2 * 1024 * 1024;
const MAX_SYSTEM_PROMPT_BYTES: u64 = 2 * 1024 * 1024;

pub struct GrokTranslatorFactory;

impl TranslatorFactory for GrokTranslatorFactory {
    fn source(&self) -> &str {
        "grok"
    }

    fn create(&self, session_id: &str) -> Box<dyn AgentTranslator> {
        Box::new(GrokTranslator::new(session_id))
    }
}

#[derive(Default)]
struct BoundedOutput {
    values: Vec<Value>,
    bytes: usize,
    omitted_chunks: u64,
    omitted_bytes: u64,
}

impl BoundedOutput {
    fn push(&mut self, value: Value) {
        let bytes = serde_json::to_vec(&value)
            .map(|encoded| encoded.len())
            .unwrap_or(MAX_OUTPUT_BYTES.saturating_add(1));
        if self.values.len() >= MAX_OUTPUT_CHUNKS
            || self.bytes.saturating_add(bytes) > MAX_OUTPUT_BYTES
        {
            self.omitted_chunks = self.omitted_chunks.saturating_add(1);
            self.omitted_bytes = self.omitted_bytes.saturating_add(bytes as u64);
            return;
        }
        self.bytes += bytes;
        self.values.push(value);
    }

    fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    fn add_truncation_metadata(&self, metadata: &mut Map<String, Value>, prefix: &str) {
        if self.omitted_chunks == 0 {
            return;
        }
        metadata.insert(format!("{prefix}_truncated"), json!(true));
        metadata.insert(
            format!("{prefix}_omitted_chunks"),
            json!(self.omitted_chunks),
        );
        metadata.insert(format!("{prefix}_omitted_bytes"), json!(self.omitted_bytes));
    }

    fn into_value(self) -> Value {
        Value::Array(self.values)
    }

    fn into_message_content(self) -> Value {
        if self.values.iter().all(Value::is_string) {
            let mut text = String::with_capacity(self.bytes);
            for value in self.values {
                if let Value::String(chunk) = value {
                    text.push_str(&chunk);
                }
            }
            Value::String(text)
        } else if self.values.len() == 1 {
            self.values.into_iter().next().unwrap_or(Value::Null)
        } else {
            Value::Array(self.values)
        }
    }
}

struct OpenTurn {
    span_id: String,
    key: String,
    model: Option<String>,
    prompt_id: Option<String>,
    llm_span_ids: Vec<String>,
    assistant_output: BoundedOutput,
}

struct OpenLlm {
    span_id: String,
    prompt_id: Option<String>,
    stream_start_ms: Option<i64>,
    output: BoundedOutput,
    reasoning: BoundedOutput,
    start_ms: i64,
    last_ms: i64,
}

struct OpenTool {
    span_id: String,
    start_ms: i64,
}

struct CompletedTool {
    span_id: String,
    end_ms: i64,
}

#[derive(Clone, Default)]
struct TranscriptCursor {
    path: Option<String>,
    offset: u64,
    partial: Vec<u8>,
    discarding_oversize: bool,
    tail: Vec<u8>,
}

impl TranscriptCursor {
    fn reset(&mut self, path: &str) {
        self.path = Some(path.to_string());
        self.offset = 0;
        self.partial.clear();
        self.discarding_oversize = false;
        self.tail.clear();
    }

    fn remember(&mut self, bytes: &[u8]) {
        self.tail.extend_from_slice(bytes);
        if self.tail.len() > CURSOR_TAIL_BYTES {
            self.tail.drain(..self.tail.len() - CURSOR_TAIL_BYTES);
        }
    }
}

struct TranscriptBatch {
    cursor: TranscriptCursor,
    records: Vec<Value>,
    complete: bool,
}

#[derive(Clone)]
struct PendingWork {
    event: Envelope,
}

struct GrokTranslator {
    session_id: String,
    session_span_id: String,
    root_span_id: String,
    root_open: bool,
    root_closed: bool,
    turn_seq: u32,
    current_turn: Option<OpenTurn>,
    open_llm: Option<OpenLlm>,
    open_tools: BTreeMap<String, OpenTool>,
    completed_tools: RecentMap<String, CompletedTool>,
    emitted_turns: RecentSet<String>,
    emitted_chunks: RecentSet<String>,
    updates: TranscriptCursor,
    events: TranscriptCursor,
    system_prompt: Option<String>,
    first_llm_span_id: Option<String>,
    first_llm_user_input: Option<Value>,
    pending: Option<PendingWork>,
    last_ts_ms: i64,
}

impl GrokTranslator {
    fn new(session_id: &str) -> Self {
        let root = ids::span_id(session_id, "session");
        Self {
            session_id: session_id.to_string(),
            session_span_id: root.clone(),
            root_span_id: root,
            root_open: false,
            root_closed: false,
            turn_seq: 0,
            current_turn: None,
            open_llm: None,
            open_tools: BTreeMap::new(),
            completed_tools: RecentMap::default(),
            emitted_turns: RecentSet::default(),
            emitted_chunks: RecentSet::default(),
            updates: TranscriptCursor::default(),
            events: TranscriptCursor::default(),
            system_prompt: None,
            first_llm_span_id: None,
            first_llm_user_input: None,
            pending: None,
            last_ts_ms: 0,
        }
    }

    fn ensure_root(
        &mut self,
        ts_ms: i64,
        event: &Envelope,
        ctx: &SessionCtx,
        ops: &mut Vec<SpanOp>,
    ) {
        if self.root_open || self.root_closed {
            return;
        }
        let attached = ctx
            .config
            .as_ref()
            .map(|config| config.attached_span_ids())
            .unwrap_or_default();
        self.root_span_id = attached.1.unwrap_or_else(|| self.session_span_id.clone());
        self.root_open = true;

        let mut metadata = ctx
            .config
            .as_ref()
            .and_then(|config| config.additional_metadata.as_ref())
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        metadata.retain(|key, _| !key.starts_with("_bt_"));
        metadata.insert("source".into(), json!("grok"));
        metadata.insert("session_id".into(), json!(self.session_id));
        metadata.insert("trace_source".into(), json!("session_transcript"));
        for field in ["cwd", "workspaceRoot", "permissionMode", "transcriptPath"] {
            if let Some(value) = event.payload.get(field) {
                metadata.insert(field.into(), value.clone());
            }
        }
        if let Some(version) = &event.source_version {
            metadata.insert("grok_version".into(), json!(version));
        }
        if let Some(version) = &event.plugin_version {
            metadata.insert("plugin_version".into(), json!(version));
        }
        ops.push(SpanOp::Insert(SpanRow {
            span_id: self.session_span_id.clone(),
            root_span_id: self.root_span_id.clone(),
            parent_span_ids: attached.0.into_iter().collect(),
            name: "Grok".into(),
            span_type: SpanType::Task,
            start_ms: Some(ts_ms),
            metadata: Some(Value::Object(metadata)),
            ..Default::default()
        }));
    }

    fn process_update(
        &mut self,
        record: &Value,
        event: &Envelope,
        ctx: &SessionCtx,
        ops: &mut Vec<SpanOp>,
    ) {
        let Some(params) = record.get("params") else {
            return;
        };
        let Some(update) = params.get("update") else {
            return;
        };
        let Some(kind) = update.get("sessionUpdate").and_then(Value::as_str) else {
            return;
        };
        let ts_ms = record_ts_ms(record).unwrap_or(event.ts_ms);

        if matches!(kind, "agent_thought_chunk" | "agent_message_chunk") {
            if self.current_turn.is_none() {
                return;
            }
            let serialized = serde_json::to_string(record).unwrap_or_default();
            let chunk_key = ids::span_id(&self.session_id, &format!("chunk:{serialized}"));
            if !self.emitted_chunks.insert(chunk_key) {
                return;
            }
        }
        self.last_ts_ms = self.last_ts_ms.max(ts_ms);

        match kind {
            "user_message_chunk" => {
                let meta = update
                    .get("_meta")
                    .or_else(|| params.get("_meta"))
                    .and_then(Value::as_object);
                let next_number = self.turn_seq.saturating_add(1);
                let turn_key = turn_key(meta, next_number);
                if self.emitted_turns.contains(&turn_key) {
                    return;
                }
                self.emitted_turns.insert(turn_key.clone());
                self.close_incomplete_turn(ts_ms, "new_turn", None, false, ops);
                self.ensure_root(ts_ms, event, ctx, ops);
                self.turn_seq = next_number;
                let span_id = ids::span_id(&self.session_id, &format!("turn:{turn_key}"));
                let model = meta
                    .and_then(|m| m.get("modelId"))
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                let prompt_id = meta
                    .and_then(|m| m.get("promptId"))
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                let user_input = update.get("content").map(content_value);
                if self.first_llm_span_id.is_none() {
                    self.first_llm_user_input = user_input.clone();
                }
                ops.push(SpanOp::Insert(SpanRow {
                    span_id: span_id.clone(),
                    root_span_id: self.root_span_id.clone(),
                    parent_span_ids: vec![self.session_span_id.clone()],
                    name: format!("Turn {}", self.turn_seq),
                    span_type: SpanType::Task,
                    start_ms: Some(ts_ms),
                    input: user_input,
                    metadata: Some(json!({"transcript_turn_key": turn_key})),
                    ..Default::default()
                }));
                self.current_turn = Some(OpenTurn {
                    span_id,
                    key: turn_key,
                    model,
                    prompt_id,
                    llm_span_ids: Vec::new(),
                    assistant_output: BoundedOutput::default(),
                });
            }
            "agent_thought_chunk" | "agent_message_chunk" => {
                if self.current_turn.is_none() {
                    return;
                }
                let meta = params
                    .get("_meta")
                    .or_else(|| update.get("_meta"))
                    .and_then(Value::as_object);
                let prompt_id = meta
                    .and_then(|m| m.get("promptId"))
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                let stream_start_ms = meta
                    .and_then(|m| m.get("streamStartMs"))
                    .and_then(Value::as_i64);
                let needs_new = self.open_llm.as_ref().is_none_or(|llm| {
                    identities_differ(
                        llm.prompt_id.as_deref(),
                        llm.stream_start_ms,
                        prompt_id.as_deref(),
                        stream_start_ms,
                    )
                });
                if needs_new {
                    let boundary_ms = stream_start_ms.unwrap_or(ts_ms);
                    self.close_llm(boundary_ms, "new_stream", ops);
                    let turn = self.current_turn.as_ref().expect("turn checked above");
                    let sequence = turn.llm_span_ids.len() + 1;
                    let turn_span_id = turn.span_id.clone();
                    let turn_key = turn.key.clone();
                    let model = turn.model.clone().unwrap_or_else(|| "Grok".into());
                    let identity_prompt = prompt_id.as_deref().unwrap_or("unknown");
                    let identity_stream = stream_start_ms
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "unknown".into());
                    let span_id = ids::span_id(
                        &self.session_id,
                        &format!(
                            "turn:{turn_key}:llm:{identity_prompt}:{identity_stream}:{sequence}"
                        ),
                    );
                    let is_first_llm = self.first_llm_span_id.is_none();
                    let input = if is_first_llm {
                        self.first_llm_span_id = Some(span_id.clone());
                        first_llm_input(
                            self.system_prompt.as_deref(),
                            self.first_llm_user_input.as_ref(),
                        )
                    } else {
                        None
                    };
                    let mut metadata = Map::new();
                    metadata.insert("model".into(), json!(model.clone()));
                    metadata.insert("trace_source".into(), json!("session_transcript"));
                    metadata.insert("input_unavailable".into(), json!(true));
                    metadata.insert("llm_sequence".into(), json!(sequence));
                    metadata.insert("transcript_turn_key".into(), json!(turn_key));
                    if is_first_llm {
                        add_first_llm_input_metadata(
                            &mut metadata,
                            self.system_prompt.is_some(),
                            self.first_llm_user_input.is_some(),
                        );
                    }
                    if let Some(prompt_id) = &prompt_id {
                        metadata.insert("prompt_id".into(), json!(prompt_id));
                    }
                    if let Some(stream_start_ms) = stream_start_ms {
                        metadata.insert("stream_start_ms".into(), json!(stream_start_ms));
                        metadata.insert("boundary_source".into(), json!("streamStartMs"));
                    }
                    ops.push(SpanOp::Insert(SpanRow {
                        span_id: span_id.clone(),
                        root_span_id: self.root_span_id.clone(),
                        parent_span_ids: vec![turn_span_id],
                        name: format!("{model} call {sequence}"),
                        span_type: SpanType::Llm,
                        start_ms: Some(boundary_ms),
                        input,
                        metadata: Some(Value::Object(metadata)),
                        ..Default::default()
                    }));
                    self.current_turn
                        .as_mut()
                        .expect("turn checked above")
                        .llm_span_ids
                        .push(span_id.clone());
                    self.open_llm = Some(OpenLlm {
                        span_id,
                        prompt_id: prompt_id.clone(),
                        stream_start_ms,
                        output: BoundedOutput::default(),
                        reasoning: BoundedOutput::default(),
                        start_ms: boundary_ms,
                        last_ms: ts_ms.max(boundary_ms),
                    });
                } else if let Some(llm) = self.open_llm.as_mut() {
                    if llm.prompt_id.is_none() {
                        llm.prompt_id = prompt_id.clone();
                    }
                    if llm.stream_start_ms.is_none() {
                        llm.stream_start_ms = stream_start_ms;
                    }
                }
                if let (Some(prompt_id), Some(turn)) = (prompt_id, self.current_turn.as_mut()) {
                    turn.prompt_id.get_or_insert(prompt_id);
                }
                let is_reasoning = kind == "agent_thought_chunk";
                if let Some(content) = update.get("content") {
                    if let Some(llm) = self.open_llm.as_mut() {
                        llm.last_ms = llm.last_ms.max(ts_ms);
                        if is_reasoning {
                            llm.reasoning.push(content_value(content));
                        } else {
                            llm.output.push(content_value(content));
                        }
                    }
                    if !is_reasoning {
                        self.current_turn
                            .as_mut()
                            .expect("turn checked above")
                            .assistant_output
                            .push(content.clone());
                    }
                }
            }
            "tool_call" => {
                self.close_llm(ts_ms, "tool_call", ops);
                let Some(turn_span_id) =
                    self.current_turn.as_ref().map(|turn| turn.span_id.clone())
                else {
                    return;
                };
                let Some(call_id) = update.get("toolCallId").and_then(Value::as_str) else {
                    return;
                };
                if self.open_tools.contains_key(call_id) {
                    return;
                }
                let call_key = call_id.to_string();
                if let Some(completed) = self.completed_tools.remove(&call_key) {
                    self.completed_tools.insert(call_key, completed);
                    return;
                }
                self.evict_open_tool_if_needed(ts_ms, ops);
                let span_id = ids::span_id(&self.session_id, &format!("tool:{call_id}"));
                let name = update
                    .get("title")
                    .and_then(Value::as_str)
                    .unwrap_or("Tool")
                    .to_string();
                ops.push(SpanOp::Insert(SpanRow {
                    span_id: span_id.clone(),
                    root_span_id: self.root_span_id.clone(),
                    parent_span_ids: vec![turn_span_id],
                    name,
                    span_type: SpanType::Tool,
                    start_ms: Some(ts_ms),
                    input: update.get("rawInput").cloned(),
                    metadata: Some(json!({"tool_call_id": call_id})),
                    ..Default::default()
                }));
                self.open_tools.insert(
                    call_key,
                    OpenTool {
                        span_id,
                        start_ms: ts_ms,
                    },
                );
            }
            "tool_call_update" => {
                self.close_llm(ts_ms, "tool_update", ops);
                let Some(call_id) = update.get("toolCallId").and_then(Value::as_str) else {
                    return;
                };
                let status = update.get("status").and_then(Value::as_str);
                if !matches!(status, Some("completed" | "failed" | "error" | "cancelled")) {
                    return;
                }
                let tool = if let Some(tool) = self.open_tools.remove(call_id) {
                    tool
                } else {
                    let Some(turn) = self.current_turn.as_ref() else {
                        return;
                    };
                    let span_id = ids::span_id(&self.session_id, &format!("tool:{call_id}"));
                    ops.push(SpanOp::Insert(SpanRow {
                        span_id: span_id.clone(),
                        root_span_id: self.root_span_id.clone(),
                        parent_span_ids: vec![turn.span_id.clone()],
                        name: update
                            .get("title")
                            .and_then(Value::as_str)
                            .unwrap_or("Tool")
                            .to_string(),
                        span_type: SpanType::Tool,
                        start_ms: Some(ts_ms),
                        input: update.get("rawInput").cloned(),
                        metadata: Some(json!({
                            "tool_call_id": call_id,
                            "missing_start": true
                        })),
                        ..Default::default()
                    }));
                    OpenTool {
                        span_id,
                        start_ms: ts_ms,
                    }
                };
                let end_ms = ts_ms.max(tool.start_ms);
                self.completed_tools.insert(
                    call_id.to_string(),
                    CompletedTool {
                        span_id: tool.span_id.clone(),
                        end_ms,
                    },
                );
                let cancelled = status == Some("cancelled");
                let failed = matches!(status, Some("failed" | "error"));
                let mut metadata = Map::new();
                if let Some(kind) = update.get("kind") {
                    metadata.insert("kind".into(), kind.clone());
                }
                if let Some(status) = status {
                    metadata.insert("status".into(), json!(status));
                }
                if cancelled {
                    metadata.insert("cancelled".into(), json!(true));
                }
                ops.push(SpanOp::Merge(SpanRow {
                    span_id: tool.span_id,
                    root_span_id: self.root_span_id.clone(),
                    end_ms: Some(end_ms),
                    output: update
                        .get("rawOutput")
                        .or_else(|| update.get("content"))
                        .cloned(),
                    error: failed.then(|| format!("Grok tool {}", status.unwrap_or("failed"))),
                    metadata: (!metadata.is_empty()).then_some(Value::Object(metadata)),
                    ..Default::default()
                }));
            }
            "turn_completed" => {
                let Some(turn) = self.current_turn.as_ref() else {
                    return;
                };
                let usage = update.get("usage");
                let metrics = usage.map(usage_metrics);
                let native_model_calls = usage
                    .and_then(|value| value.get("modelCalls"))
                    .and_then(Value::as_u64);
                let reconstructed_model_calls = turn.llm_span_ids.len() as u64;
                if let (Some(llm_span_id), Some(metrics)) = (turn.llm_span_ids.last(), &metrics) {
                    ops.push(SpanOp::Merge(SpanRow {
                        span_id: llm_span_id.clone(),
                        root_span_id: self.root_span_id.clone(),
                        metrics: Some(metrics.clone()),
                        metadata: Some(json!({
                            "usage_scope": "turn",
                            "usage_attribution": "last_llm"
                        })),
                        ..Default::default()
                    }));
                }
                let mut metadata = Map::new();
                if let Some(prompt_id) = update.get("prompt_id") {
                    metadata.insert("prompt_id".into(), prompt_id.clone());
                }
                if let Some(stop_reason) = update.get("stop_reason") {
                    metadata.insert("stop_reason".into(), stop_reason.clone());
                }
                if let Some(model_usage) = usage.and_then(|value| value.get("modelUsage")) {
                    metadata.insert("model_usage".into(), model_usage.clone());
                }
                if native_model_calls.is_some_and(|native| native != reconstructed_model_calls) {
                    metadata.insert(
                        "model_call_count_mismatch".into(),
                        json!({
                            "native": native_model_calls,
                            "reconstructed": reconstructed_model_calls
                        }),
                    );
                }
                if let (Some(expected), Some(actual)) = (
                    turn.prompt_id.as_deref(),
                    update.get("prompt_id").and_then(Value::as_str),
                ) {
                    if expected != actual {
                        metadata.insert(
                            "prompt_id_mismatch".into(),
                            json!({"observed": expected, "completed": actual}),
                        );
                    }
                }
                self.close_turn(ts_ms, "turn_completed", metadata, metrics, None, ops);
            }
            _ => {}
        }
    }

    fn process_event(&mut self, record: &Value, ops: &mut Vec<SpanOp>) {
        if record.get("type").and_then(Value::as_str) != Some("tool_completed") {
            return;
        }
        let Some(call_id) = record.get("tool_call_id").and_then(Value::as_str) else {
            return;
        };
        let key = call_id.to_string();
        let Some(completed) = self.completed_tools.remove(&key) else {
            return;
        };
        let native_end_ms = record_ts_ms(record).unwrap_or(completed.end_ms);
        let end_ms = native_end_ms.max(completed.end_ms);
        let span_id = completed.span_id.clone();
        self.completed_tools.insert(
            key,
            CompletedTool {
                span_id: completed.span_id,
                end_ms,
            },
        );
        let outcome = record.get("outcome").and_then(Value::as_str);
        let cancelled = matches!(outcome, Some("cancelled" | "canceled"));
        let failed = !cancelled && !matches!(outcome, Some("success") | None);
        let mut metadata = Map::new();
        if let Some(duration) = record.get("duration_ms") {
            metadata.insert("duration_ms".into(), duration.clone());
        }
        if let Some(outcome) = record.get("outcome") {
            metadata.insert("outcome".into(), outcome.clone());
        }
        if cancelled {
            metadata.insert("cancelled".into(), json!(true));
        }
        ops.push(SpanOp::Merge(SpanRow {
            span_id,
            root_span_id: self.root_span_id.clone(),
            end_ms: Some(end_ms),
            metadata: Some(Value::Object(metadata)),
            error: failed.then(|| format!("Grok tool outcome: {}", outcome.unwrap_or("error"))),
            allow_late_merge: true,
            ..Default::default()
        }));
    }

    fn close_llm(&mut self, end_ms: i64, reason: &str, ops: &mut Vec<SpanOp>) {
        let Some(llm) = self.open_llm.take() else {
            return;
        };
        let mut metadata = Map::new();
        metadata.insert("close_reason".into(), json!(reason));
        llm.output.add_truncation_metadata(&mut metadata, "output");
        llm.reasoning
            .add_truncation_metadata(&mut metadata, "reasoning");
        let has_reasoning = !llm.reasoning.is_empty();
        let mut output = json!({
            "role": "assistant",
            "content": llm.output.into_message_content(),
        });
        if has_reasoning {
            output["reasoning"] = json!([{
                "id": "reasoning",
                "content": llm.reasoning.into_message_content(),
            }]);
        }
        ops.push(SpanOp::Merge(SpanRow {
            span_id: llm.span_id,
            root_span_id: self.root_span_id.clone(),
            end_ms: Some(end_ms.max(llm.last_ms).max(llm.start_ms)),
            output: Some(Value::Array(vec![output])),
            metadata: Some(Value::Object(metadata)),
            ..Default::default()
        }));
    }

    fn evict_open_tool_if_needed(&mut self, end_ms: i64, ops: &mut Vec<SpanOp>) {
        if self.open_tools.len() < MAX_OPEN_TOOLS {
            return;
        }
        let Some(call_id) = self.open_tools.keys().next().cloned() else {
            return;
        };
        let Some(tool) = self.open_tools.remove(&call_id) else {
            return;
        };
        let end_ms = end_ms.max(tool.start_ms);
        self.completed_tools.insert(
            call_id,
            CompletedTool {
                span_id: tool.span_id.clone(),
                end_ms,
            },
        );
        ops.push(SpanOp::Merge(SpanRow {
            span_id: tool.span_id,
            root_span_id: self.root_span_id.clone(),
            end_ms: Some(end_ms),
            metadata: Some(json!({
                "incomplete": true,
                "close_reason": "open_tool_limit"
            })),
            ..Default::default()
        }));
    }

    fn close_tools(&mut self, end_ms: i64, reason: &str, ops: &mut Vec<SpanOp>) {
        for (call_id, tool) in std::mem::take(&mut self.open_tools) {
            let end_ms = end_ms.max(tool.start_ms);
            self.completed_tools.insert(
                call_id,
                CompletedTool {
                    span_id: tool.span_id.clone(),
                    end_ms,
                },
            );
            ops.push(SpanOp::Merge(SpanRow {
                span_id: tool.span_id,
                root_span_id: self.root_span_id.clone(),
                end_ms: Some(end_ms),
                metadata: Some(json!({"incomplete": true, "close_reason": reason})),
                ..Default::default()
            }));
        }
    }

    fn close_turn(
        &mut self,
        end_ms: i64,
        reason: &str,
        mut metadata: Map<String, Value>,
        metrics: Option<Value>,
        error: Option<String>,
        ops: &mut Vec<SpanOp>,
    ) {
        let end_ms = end_ms.max(self.last_ts_ms);
        self.close_llm(end_ms, reason, ops);
        self.close_tools(end_ms, reason, ops);
        let Some(turn) = self.current_turn.take() else {
            return;
        };
        metadata.insert("close_reason".into(), json!(reason));
        turn.assistant_output
            .add_truncation_metadata(&mut metadata, "assistant_output");
        ops.push(SpanOp::Merge(SpanRow {
            span_id: turn.span_id,
            root_span_id: self.root_span_id.clone(),
            end_ms: Some(end_ms),
            output: Some(turn.assistant_output.into_value()),
            metadata: Some(Value::Object(metadata)),
            metrics,
            error,
            ..Default::default()
        }));
    }

    fn close_incomplete_turn(
        &mut self,
        end_ms: i64,
        reason: &str,
        error: Option<String>,
        cancelled: bool,
        ops: &mut Vec<SpanOp>,
    ) {
        if self.current_turn.is_none() {
            return;
        }
        let mut metadata = Map::new();
        metadata.insert("incomplete".into(), json!(true));
        if cancelled {
            metadata.insert("cancelled".into(), json!(true));
        }
        self.close_turn(end_ms, reason, metadata, None, error, ops);
    }

    fn process_system_prompt(&mut self, reference: Option<&Value>, ops: &mut Vec<SpanOp>) {
        if self.system_prompt.is_some() {
            return;
        }
        let prompt = match read_text_snapshot(reference) {
            Ok(prompt) => prompt,
            Err(error) => {
                tracing::debug!(
                    session_id = %self.session_id,
                    error = %error,
                    "Grok system prompt snapshot unavailable"
                );
                return;
            }
        };
        let Some(prompt) = prompt else {
            return;
        };
        let input = first_llm_input(Some(&prompt), self.first_llm_user_input.as_ref())
            .expect("system prompt produces LLM input");
        self.system_prompt = Some(prompt);
        if let Some(span_id) = self.first_llm_span_id.clone() {
            let mut metadata = Map::new();
            add_first_llm_input_metadata(&mut metadata, true, self.first_llm_user_input.is_some());
            ops.push(SpanOp::Merge(SpanRow {
                span_id,
                root_span_id: self.root_span_id.clone(),
                input: Some(input),
                metadata: Some(Value::Object(metadata)),
                ..Default::default()
            }));
        }
    }

    fn process_event_enrichment(&mut self, reference: Option<&Value>, ops: &mut Vec<SpanOp>) {
        let Some(reference) = reference else {
            return;
        };
        let events = match read_new_records(Some(reference), &self.events) {
            Ok(events) => events,
            Err(error) => {
                tracing::debug!(
                    session_id = %self.session_id,
                    error = %error,
                    "Grok events transcript enrichment unavailable"
                );
                return;
            }
        };
        self.events = events.cursor;
        for record in events.records {
            self.process_event(&record, ops);
        }
    }

    fn process_transcripts(
        &mut self,
        event: &Envelope,
        ctx: &SessionCtx,
    ) -> anyhow::Result<(Vec<SpanOp>, bool)> {
        let mut ops = Vec::new();
        let Some(mirrors) = event.payload.get("_bt_grok_transcript_mirrors") else {
            return Ok((ops, true));
        };

        // Updates are authoritative: their read and cursor must succeed before
        // any translator state advances. Events can enrich completed tools but
        // cannot delay update catch-up, terminal handling, or flush.
        let updates = read_new_records(mirrors.get("updates"), &self.updates)?;
        let complete = updates.complete;
        self.updates = updates.cursor;
        self.process_system_prompt(mirrors.get("system_prompt"), &mut ops);
        for record in updates.records {
            self.process_update(&record, event, ctx, &mut ops);
        }
        self.process_event_enrichment(mirrors.get("events"), &mut ops);
        Ok((ops, complete))
    }

    fn finish_hook(&mut self, event: &Envelope, ops: &mut Vec<SpanOp>) {
        let terminal_ms = self.last_ts_ms.max(event.ts_ms);
        match event.event.as_str() {
            // Transcript `turn_completed` is authoritative. A passive stop hook
            // can race the final transcript write, so closing here would make
            // later catch-up records lose their turn parent.
            "Stop" | "stop" => {}
            "StopFailure" | "stop_failure" => {
                self.last_ts_ms = terminal_ms;
                self.close_incomplete_turn(
                    terminal_ms,
                    event.event.as_str(),
                    Some("Grok turn failed".into()),
                    false,
                    ops,
                );
            }
            "StopCancelled" | "stop_cancelled" => {
                self.last_ts_ms = terminal_ms;
                self.close_incomplete_turn(terminal_ms, event.event.as_str(), None, true, ops);
            }
            "SessionEnd" | "session_end" => {
                self.last_ts_ms = terminal_ms;
                self.close_incomplete_turn(terminal_ms, event.event.as_str(), None, false, ops);
                if self.root_open {
                    self.root_open = false;
                    self.root_closed = true;
                    ops.push(SpanOp::Merge(SpanRow {
                        span_id: self.session_span_id.clone(),
                        root_span_id: self.root_span_id.clone(),
                        end_ms: Some(terminal_ms),
                        ..Default::default()
                    }));
                }
            }
            _ => {}
        }
    }
}

impl AgentTranslator for GrokTranslator {
    fn handle(&mut self, event: &Envelope, ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        anyhow::ensure!(
            self.pending.is_none(),
            "Grok translator has pending catch-up work; drain it before handling another event"
        );
        let (mut ops, complete) = self.process_transcripts(event, ctx)?;
        if complete {
            self.finish_hook(event, &mut ops);
        } else {
            self.pending = Some(PendingWork {
                event: event.clone(),
            });
        }
        Ok(ops)
    }

    fn drain_pending(&mut self, ctx: &SessionCtx) -> anyhow::Result<Option<Vec<SpanOp>>> {
        let Some(pending) = self.pending.clone() else {
            return Ok(None);
        };
        let (mut ops, complete) = self.process_transcripts(&pending.event, ctx)?;
        if complete {
            self.pending = None;
            self.finish_hook(&pending.event, &mut ops);
        }
        Ok(Some(ops))
    }

    fn flush(&mut self, _ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        anyhow::ensure!(
            self.pending.is_none(),
            "Grok translator has pending catch-up work; drain it before flushing"
        );
        let mut ops = Vec::new();
        self.close_incomplete_turn(self.last_ts_ms, "flush", None, false, &mut ops);
        if self.root_open {
            self.root_open = false;
            self.root_closed = true;
            ops.push(SpanOp::Merge(SpanRow {
                span_id: self.session_span_id.clone(),
                root_span_id: self.root_span_id.clone(),
                end_ms: Some(self.last_ts_ms),
                ..Default::default()
            }));
        }
        Ok(ops)
    }
}

fn first_llm_input(system_prompt: Option<&str>, user_message: Option<&Value>) -> Option<Value> {
    let mut messages = Vec::with_capacity(2);
    if let Some(system_prompt) = system_prompt {
        messages.push(json!({"role": "system", "content": system_prompt}));
    }
    if let Some(user_message) = user_message {
        messages.push(json!({"role": "user", "content": user_message}));
    }
    (!messages.is_empty()).then_some(Value::Array(messages))
}

fn add_first_llm_input_metadata(
    metadata: &mut Map<String, Value>,
    has_system_prompt: bool,
    has_user_message: bool,
) {
    if has_system_prompt {
        metadata.insert("system_prompt_included".into(), json!(true));
    }
    if has_user_message {
        metadata.insert("user_message_included".into(), json!(true));
    }
    let scope = match (has_system_prompt, has_user_message) {
        (true, true) => Some("system_and_user"),
        (true, false) => Some("system_prompt_only"),
        (false, true) => Some("user_message_only"),
        (false, false) => None,
    };
    if let Some(scope) = scope {
        metadata.insert("input_scope".into(), json!(scope));
    }
}

fn read_text_snapshot(reference: Option<&Value>) -> anyhow::Result<Option<String>> {
    let Some(reference) = reference else {
        return Ok(None);
    };
    let Some(path) = reference
        .get("mirror")
        .and_then(Value::as_str)
        .or_else(|| reference.get("path").and_then(Value::as_str))
    else {
        return Ok(None);
    };
    let mut file = std::fs::File::open(path)?;
    let file_len = file.metadata()?.len();
    let through = reference
        .get("through")
        .and_then(Value::as_u64)
        .unwrap_or(file_len);
    anyhow::ensure!(
        file_len >= through,
        "Grok system prompt mirror {path} is shorter than captured boundary {through}"
    );
    anyhow::ensure!(
        through <= MAX_SYSTEM_PROMPT_BYTES,
        "Grok system prompt exceeds {MAX_SYSTEM_PROMPT_BYTES} bytes"
    );
    let mut bytes = vec![0; through as usize];
    file.read_exact(&mut bytes)?;
    let prompt = String::from_utf8(bytes)?;
    Ok((!prompt.is_empty()).then_some(prompt))
}

fn read_new_records(
    reference: Option<&Value>,
    cursor: &TranscriptCursor,
) -> anyhow::Result<TranscriptBatch> {
    let Some(reference) = reference else {
        return Ok(TranscriptBatch {
            cursor: cursor.clone(),
            records: Vec::new(),
            complete: true,
        });
    };
    let Some(path) = reference
        .get("mirror")
        .and_then(Value::as_str)
        .or_else(|| reference.get("path").and_then(Value::as_str))
    else {
        return Ok(TranscriptBatch {
            cursor: cursor.clone(),
            records: Vec::new(),
            complete: true,
        });
    };

    let captured_through = reference.get("through").and_then(Value::as_u64);
    let mut file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error)
            if error.kind() == std::io::ErrorKind::NotFound && captured_through == Some(0) =>
        {
            let mut next = cursor.clone();
            if next.path.as_deref() != Some(path) || next.offset != 0 {
                next.reset(path);
            }
            return Ok(TranscriptBatch {
                cursor: next,
                records: Vec::new(),
                complete: true,
            });
        }
        Err(error) => return Err(error.into()),
    };
    let file_len = file.metadata()?.len();
    let through = captured_through.unwrap_or(file_len);
    anyhow::ensure!(
        file_len >= through,
        "Grok transcript mirror {path} is shorter than captured boundary {through}"
    );

    let mut next = cursor.clone();
    if next.path.as_deref() != Some(path)
        || next.offset > through
        || !cursor_tail_matches(&mut file, &next)?
    {
        next.reset(path);
    }
    if next.offset == through {
        return Ok(TranscriptBatch {
            cursor: next,
            records: Vec::new(),
            complete: true,
        });
    }

    file.seek(SeekFrom::Start(next.offset))?;
    let read_len = (through - next.offset).min(CATCH_UP_BYTE_BUDGET) as usize;
    let mut bytes = vec![0; read_len];
    file.read_exact(&mut bytes)?;

    let mut records = Vec::new();
    let mut completed_lines = 0usize;
    let mut consumed = 0usize;
    for byte in bytes.iter().copied() {
        consumed += 1;
        if next.discarding_oversize {
            if byte == b'\n' {
                next.discarding_oversize = false;
                completed_lines += 1;
            }
        } else if byte == b'\n' {
            if next.partial.last() == Some(&b'\r') {
                next.partial.pop();
            }
            if let Ok(record) = serde_json::from_slice(&next.partial) {
                records.push(record);
            }
            next.partial.clear();
            completed_lines += 1;
        } else if next.partial.len() < MAX_RECORD_BYTES {
            next.partial.push(byte);
        } else {
            next.partial.clear();
            next.discarding_oversize = true;
        }

        if completed_lines >= CATCH_UP_RECORD_BUDGET {
            break;
        }
    }
    next.offset = next.offset.saturating_add(consumed as u64);
    next.remember(&bytes[..consumed]);

    Ok(TranscriptBatch {
        complete: next.offset == through,
        cursor: next,
        records,
    })
}

fn cursor_tail_matches(
    file: &mut std::fs::File,
    cursor: &TranscriptCursor,
) -> anyhow::Result<bool> {
    if cursor.offset == 0 || cursor.tail.is_empty() {
        return Ok(true);
    }
    if cursor.offset < cursor.tail.len() as u64 {
        return Ok(false);
    }
    file.seek(SeekFrom::Start(cursor.offset - cursor.tail.len() as u64))?;
    let mut observed = vec![0; cursor.tail.len()];
    if file.read_exact(&mut observed).is_err() {
        return Ok(false);
    }
    Ok(observed == cursor.tail)
}

fn turn_key(meta: Option<&Map<String, Value>>, sequence: u32) -> String {
    if let Some(prompt_id) = meta
        .and_then(|value| value.get("promptId"))
        .and_then(Value::as_str)
    {
        return format!("prompt:{prompt_id}");
    }
    if let Some(prompt_index) = meta.and_then(|value| value.get("promptIndex")) {
        return format!("prompt-index:{prompt_index}");
    }
    format!("sequence:{sequence}")
}

fn identities_differ(
    open_prompt_id: Option<&str>,
    open_stream_start_ms: Option<i64>,
    prompt_id: Option<&str>,
    stream_start_ms: Option<i64>,
) -> bool {
    matches!((open_prompt_id, prompt_id), (Some(open), Some(next)) if open != next)
        || matches!(
            (open_stream_start_ms, stream_start_ms),
            (Some(open), Some(next)) if open != next
        )
}

fn record_ts_ms(record: &Value) -> Option<i64> {
    record
        .pointer("/params/_meta/agentTimestampMs")
        .and_then(Value::as_i64)
        .or_else(|| {
            record
                .get("ts")
                .and_then(Value::as_str)
                .and_then(|ts| chrono::DateTime::parse_from_rfc3339(ts).ok())
                .map(|ts| ts.timestamp_millis())
        })
        .or_else(|| {
            record
                .get("timestamp")
                .and_then(Value::as_i64)
                .map(|seconds| seconds.saturating_mul(1000))
        })
}

fn content_value(content: &Value) -> Value {
    content
        .get("text")
        .cloned()
        .unwrap_or_else(|| content.clone())
}

fn usage_metrics(usage: &Value) -> Value {
    let mut metrics = Map::new();
    for (native, common) in [
        ("inputTokens", "prompt_tokens"),
        ("outputTokens", "completion_tokens"),
        ("totalTokens", "tokens"),
        ("cachedReadTokens", "prompt_cached_tokens"),
        ("cacheCreationTokens", "prompt_cache_creation_tokens"),
        ("reasoningTokens", "completion_reasoning_tokens"),
        ("apiDurationMs", "api_duration_ms"),
        ("costUsdTicks", "cost_usd_ticks"),
        ("modelCalls", "model_calls"),
    ] {
        if let Some(value) = usage.get(native) {
            metrics.insert(common.into(), value.clone());
        }
    }
    Value::Object(metrics)
}
