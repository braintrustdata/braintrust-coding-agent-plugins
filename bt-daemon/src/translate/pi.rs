//! Pi coding-agent translator. Pi forwards native extension callbacks and this
//! state machine owns all span construction and recovery.

use super::git::GitMetadataCache;
use super::tool::{error_text, with_tool_approval, ToolApproval};
use super::{AgentTranslator, SessionCtx, SpanOp, SpanRow, SpanType, TranslatorFactory};
use crate::ids;
use crate::wire::Envelope;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

pub struct PiTranslatorFactory {
    git: Arc<GitMetadataCache>,
}
impl PiTranslatorFactory {
    pub(super) fn new(git: Arc<GitMetadataCache>) -> Self {
        Self { git }
    }
}
impl TranslatorFactory for PiTranslatorFactory {
    fn source(&self) -> &str {
        "pi"
    }
    fn create(&self, session_id: &str) -> Box<dyn AgentTranslator> {
        Box::new(PiTranslator {
            session_id: session_id.into(),
            root_span_id: ids::span_id(session_id, "root"),
            effective_root_span_id: String::new(),
            external_parent: None,
            opened: false,
            turn: None,
            turn_seq: 0,
            llm_seq: 0,
            total_tools: 0,
            pending_llms: Vec::new(),
            tools: HashMap::new(),
            compaction: None,
            branch_summary: None,
            last_ts: 0,
            thinking_level: None,
            git: self.git.clone(),
        })
    }
}

struct PendingLlm {
    start_ms: i64,
    input: Value,
    first_token_ms: Option<i64>,
    provider: Option<Value>,
}
#[derive(Clone)]
struct ToolStart {
    start_ms: i64,
    name: String,
    args: Value,
}
struct PiTranslator {
    session_id: String,
    root_span_id: String,
    effective_root_span_id: String,
    external_parent: Option<String>,
    opened: bool,
    turn: Option<(String, Value)>,
    turn_seq: u32,
    llm_seq: u32,
    total_tools: u32,
    pending_llms: Vec<PendingLlm>,
    tools: HashMap<String, ToolStart>,
    compaction: Option<(String, i64, Value)>,
    branch_summary: Option<(String, i64, Value)>,
    last_ts: i64,
    thinking_level: Option<String>,
    git: Arc<GitMetadataCache>,
}

impl AgentTranslator for PiTranslator {
    fn handle(&mut self, envelope: &Envelope, ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        self.last_ts = self.last_ts.max(envelope.ts_ms);
        let event = envelope.payload.get("event").unwrap_or(&envelope.payload);
        let mut ops = self.ensure_root(envelope, ctx);
        match envelope.event.as_str() {
            "before_agent_start" => ops.extend(self.start_turn(event, envelope.ts_ms)),
            "context" => self.capture_context(event, envelope.ts_ms),
            "before_provider_request" => self.provider_request(event),
            "message_update" => self.streaming_update(event, envelope.ts_ms),
            "thinking_level_select" => {
                self.thinking_level = event
                    .get("level")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            }
            "message_end" => ops.extend(self.message_end(event, envelope.ts_ms)),
            "tool_execution_start" => ops.extend(self.tool_start(event, envelope.ts_ms)),
            "tool_execution_end" => ops.extend(self.tool_end(event, envelope.ts_ms)),
            "agent_end" if event.get("willRetry").and_then(Value::as_bool) != Some(true) => {
                ops.extend(self.close_turn(envelope.ts_ms, None));
            }
            "session_before_compact" => {
                self.compaction = Some((
                    ids::span_id(&self.session_id, &format!("compaction:{}", envelope.ts_ms)),
                    envelope.ts_ms,
                    event.clone(),
                ))
            }
            "session_compact" => {
                ops.extend(self.finish_special("Compaction", true, event, envelope.ts_ms))
            }
            "session_before_tree"
                if event
                    .pointer("/preparation/userWantsSummary")
                    .and_then(Value::as_bool)
                    == Some(true) =>
            {
                self.branch_summary = Some((
                    ids::span_id(
                        &self.session_id,
                        &format!("branch-summary:{}", envelope.ts_ms),
                    ),
                    envelope.ts_ms,
                    event.clone(),
                ))
            }
            "session_tree"
                if event.get("summaryEntry").is_some() || self.branch_summary.is_some() =>
            {
                ops.extend(self.finish_special("Branch Summary", false, event, envelope.ts_ms))
            }
            "session_shutdown" => {
                ops.extend(self.close_turn(envelope.ts_ms, None));
                ops.push(self.close_root(envelope.ts_ms));
            }
            _ => {}
        }
        let cwd = envelope.payload.get("cwd").and_then(Value::as_str);
        self.git.enrich_rows(cwd, &mut ops);
        Ok(ops)
    }

    fn finalize(&mut self, _ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        let error = "Interrupted before completion";
        let mut ops = self.close_dangling(self.last_ts, error);
        ops.extend(self.close_turn(self.last_ts, Some(error.into())));
        if self.opened {
            ops.push(self.close_root(self.last_ts));
        }
        Ok(ops)
    }
}

impl PiTranslator {
    fn close_dangling(&mut self, ts: i64, error: &str) -> Vec<SpanOp> {
        let Some((turn, _)) = &self.turn else {
            self.pending_llms.clear();
            self.tools.clear();
            return Vec::new();
        };
        let mut ops = Vec::new();
        for pending in self.pending_llms.drain(..) {
            self.llm_seq += 1;
            ops.push(SpanOp::Insert(SpanRow {
                span_id: ids::span_id(
                    &self.session_id,
                    &format!("llm:{}:{}", self.turn_seq, self.llm_seq),
                ),
                root_span_id: self.effective_root_span_id.clone(),
                parent_span_ids: vec![turn.clone()],
                name: "llm".into(),
                span_type: SpanType::Llm,
                start_ms: Some(pending.start_ms),
                end_ms: Some(ts),
                input: Some(pending.input),
                metadata: pending.provider,
                error: Some(error.into()),
                ..Default::default()
            }));
        }
        for (call, tool) in self.tools.drain() {
            self.total_tools += 1;
            ops.push(SpanOp::Insert(SpanRow {
                span_id: ids::span_id(&self.session_id, &format!("tool:{}:{call}", self.turn_seq)),
                root_span_id: self.effective_root_span_id.clone(),
                parent_span_ids: vec![turn.clone()],
                name: tool.name.clone(),
                span_type: SpanType::Tool,
                start_ms: Some(tool.start_ms),
                end_ms: Some(ts),
                input: Some(tool.args),
                metadata: Some(with_tool_approval(
                    json!({
                        "tool_name": tool.name,
                        "tool_call_id": call,
                    }),
                    Some(ToolApproval::Approved),
                )),
                error: Some(error.into()),
                ..Default::default()
            }));
        }
        ops
    }

    fn ensure_root(&mut self, envelope: &Envelope, ctx: &SessionCtx) -> Vec<SpanOp> {
        if self.opened {
            return Vec::new();
        }
        self.opened = true;
        let attached = ctx
            .config
            .as_ref()
            .map(|c| c.attached_span_ids())
            .unwrap_or_default();
        self.external_parent = attached.0;
        self.effective_root_span_id = attached
            .1
            .or_else(|| self.external_parent.clone())
            .unwrap_or_else(|| self.root_span_id.clone());
        let mut metadata = ctx
            .config
            .as_ref()
            .and_then(|c| c.additional_metadata.as_ref())
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        metadata.insert("session_id".into(), json!(ctx.session_id));
        metadata.insert("source".into(), json!("pi"));
        metadata.insert("pi_version".into(), json!(envelope.source_version));
        metadata.insert(
            "extension_version".into(),
            envelope
                .payload
                .get("extension_version")
                .cloned()
                .unwrap_or(Value::Null),
        );
        metadata.insert(
            "native_session_id".into(),
            envelope
                .payload
                .get("native_session_id")
                .cloned()
                .unwrap_or(Value::Null),
        );
        metadata.insert(
            "session_file".into(),
            envelope
                .payload
                .get("session_file")
                .cloned()
                .unwrap_or(Value::Null),
        );
        vec![SpanOp::Insert(SpanRow {
            span_id: self.root_span_id.clone(),
            root_span_id: self.effective_root_span_id.clone(),
            parent_span_ids: self.external_parent.clone().into_iter().collect(),
            name: "Pi".into(),
            span_type: SpanType::Task,
            start_ms: Some(envelope.ts_ms),
            metadata: Some(Value::Object(metadata)),
            ..Default::default()
        })]
    }

    fn start_turn(&mut self, event: &Value, ts: i64) -> Vec<SpanOp> {
        let mut ops = self.close_turn(ts, None);
        self.turn_seq += 1;
        self.llm_seq = 0;
        self.pending_llms.clear();
        self.tools.clear();
        let id = ids::span_id(&self.session_id, &format!("turn:{}", self.turn_seq));
        let input = event.get("prompt").cloned().unwrap_or(Value::Null);
        let skills = event
            .get("prompt")
            .and_then(Value::as_str)
            .map(explicit_skills)
            .unwrap_or_default();
        self.turn = Some((id.clone(), input.clone()));
        ops.push(SpanOp::Insert(SpanRow {
            span_id: id,
            root_span_id: self.effective_root_span_id.clone(),
            parent_span_ids: vec![self.root_span_id.clone()],
            name: format!("Turn {}", self.turn_seq),
            span_type: SpanType::Task,
            start_ms: Some(ts),
            input: Some(input),
            metadata: Some(json!({
                "turn_number": self.turn_seq,
                "loaded_skill_names": skills,
                "thinking_level": self.thinking_level,
            })),
            ..Default::default()
        }));
        ops
    }
    fn capture_context(&mut self, event: &Value, ts: i64) {
        let input = event.get("messages").cloned().unwrap_or_else(|| json!([]));
        self.pending_llms.push(PendingLlm {
            start_ms: ts,
            input,
            first_token_ms: None,
            provider: None,
        });
    }
    fn provider_request(&mut self, event: &Value) {
        if let Some(call) = self.pending_llms.last_mut() {
            call.provider = Some(event.clone())
        }
    }
    fn streaming_update(&mut self, event: &Value, ts: i64) {
        if let Some(call) = self.pending_llms.last_mut() {
            let kind = event
                .pointer("/assistantMessageEvent/type")
                .or_else(|| event.get("type"))
                .and_then(Value::as_str)
                .unwrap_or("");
            if matches!(kind, "text_delta" | "thinking_delta" | "text" | "thinking")
                && call.first_token_ms.is_none()
            {
                call.first_token_ms = Some(ts)
            }
        }
    }
    fn message_end(&mut self, event: &Value, ts: i64) -> Vec<SpanOp> {
        let message = event.get("message").unwrap_or(event);
        if message.get("role").and_then(Value::as_str) != Some("assistant") {
            return vec![];
        };
        let Some((turn, _)) = &self.turn else {
            return vec![];
        };
        self.llm_seq += 1;
        let pending = if self.pending_llms.is_empty() {
            PendingLlm {
                start_ms: ts,
                input: json!([]),
                first_token_ms: None,
                provider: None,
            }
        } else {
            self.pending_llms.remove(0)
        };
        let model = response_model(message);
        let usage = message.get("usage").cloned().unwrap_or_else(|| json!({}));
        let prompt = num(&usage, "input")
            + num(&usage, "cacheRead")
            + num(&usage, "cacheWrite")
            + num(&usage, "cacheWrite1h");
        let completion = num(&usage, "output");
        let reasoning = num(&usage, "reasoning");
        let total = usage
            .get("totalTokens")
            .and_then(Value::as_i64)
            .unwrap_or(prompt + completion + reasoning);
        let output = normalize_assistant(message);
        let error = message
            .get("errorMessage")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .filter(|_| {
                matches!(
                    message.get("stopReason").and_then(Value::as_str),
                    Some("error" | "aborted")
                )
            });
        let ttft = pending
            .first_token_ms
            .map(|first| (first - pending.start_ms) as f64 / 1000.0);
        vec![SpanOp::Insert(SpanRow {
            span_id: ids::span_id(
                &self.session_id,
                &format!("llm:{}:{}", self.turn_seq, self.llm_seq),
            ),
            root_span_id: self.effective_root_span_id.clone(),
            parent_span_ids: vec![turn.clone()],
            name: model.clone().unwrap_or_else(|| "llm".into()),
            span_type: SpanType::Llm,
            start_ms: Some(pending.start_ms),
            end_ms: Some(ts),
            input: Some(pending.input),
            output: Some(json!([output])),
            metadata: Some(json!({
                "model": model,
                "provider": message.get("provider"),
                "api": message.get("api"),
                "stop_reason": message.get("stopReason"),
                "thinking_level": self.thinking_level,
                "provider_request": pending.provider,
                "response_id": message.get("responseId"),
            })),
            metrics: Some(json!({
                "prompt_tokens": prompt,
                "completion_tokens": completion,
                "reasoning_tokens": reasoning,
                "tokens": total,
                "prompt_cached_tokens": num(&usage, "cacheRead"),
                "prompt_cache_creation_tokens": num(&usage, "cacheWrite")
                    + num(&usage, "cacheWrite1h"),
                "time_to_first_token": ttft,
                "cost": usage.get("cost"),
            })),
            error,
            ..Default::default()
        })]
    }
    fn tool_start(&mut self, event: &Value, ts: i64) -> Vec<SpanOp> {
        let Some(id) = event.get("toolCallId").and_then(Value::as_str) else {
            return vec![];
        };
        let Some((turn, _)) = &self.turn else {
            return vec![];
        };
        let name = event
            .get("toolName")
            .and_then(Value::as_str)
            .unwrap_or("tool")
            .to_string();
        let args = event.get("args").cloned().unwrap_or(Value::Null);
        if self.tools.contains_key(id) {
            return vec![];
        }
        self.tools.insert(
            id.into(),
            ToolStart {
                start_ms: ts,
                name: name.clone(),
                args: args.clone(),
            },
        );
        vec![SpanOp::Insert(SpanRow {
            span_id: ids::span_id(&self.session_id, &format!("tool:{}:{id}", self.turn_seq)),
            root_span_id: self.effective_root_span_id.clone(),
            parent_span_ids: vec![turn.clone()],
            name,
            span_type: SpanType::Tool,
            start_ms: Some(ts),
            input: Some(args),
            metadata: Some(with_tool_approval(
                json!({
                    "tool_name": event.get("toolName").and_then(Value::as_str).unwrap_or("tool"),
                    "tool_call_id": id,
                }),
                Some(ToolApproval::Approved),
            )),
            ..Default::default()
        })]
    }
    fn tool_end(&mut self, event: &Value, ts: i64) -> Vec<SpanOp> {
        let Some((turn, _)) = &self.turn else {
            return vec![];
        };
        let call = event
            .get("toolCallId")
            .and_then(Value::as_str)
            .unwrap_or("");
        let pending = self.tools.remove(call);
        let tracked = pending.clone().unwrap_or(ToolStart {
            start_ms: ts,
            name: event
                .get("toolName")
                .and_then(Value::as_str)
                .unwrap_or("tool")
                .into(),
            args: Value::Null,
        });
        self.total_tools += 1;
        let failed = event.get("isError").and_then(Value::as_bool) == Some(true);
        let skill = skill_from_read(&tracked.name, &tracked.args);
        let name = skill
            .as_ref()
            .map(|s| format!("skill: {s}"))
            .unwrap_or_else(|| tracked.name.clone());
        let row = SpanRow {
            span_id: ids::span_id(&self.session_id, &format!("tool:{}:{call}", self.turn_seq)),
            root_span_id: self.effective_root_span_id.clone(),
            parent_span_ids: vec![turn.clone()],
            name,
            span_type: SpanType::Tool,
            start_ms: pending.is_none().then_some(tracked.start_ms),
            end_ms: Some(ts),
            input: pending.is_none().then_some(tracked.args),
            output: event.get("result").cloned(),
            metadata: Some(with_tool_approval(
                json!({
                    "tool_name": if skill.is_some() { "skill" } else { &tracked.name },
                    "original_tool_name": tracked.name,
                    "tool_call_id": call,
                    "skill_name": skill,
                }),
                Some(ToolApproval::Approved),
            )),
            error: failed.then(|| format_error(event.get("result"))),
            ..Default::default()
        };
        vec![if pending.is_some() {
            SpanOp::Merge(row)
        } else {
            SpanOp::Insert(row)
        }]
    }
    fn finish_special(
        &mut self,
        name: &str,
        compaction: bool,
        event: &Value,
        ts: i64,
    ) -> Vec<SpanOp> {
        let active = if compaction {
            self.compaction.take()
        } else {
            self.branch_summary.take()
        };
        let (id, start, input) = active.unwrap_or_else(|| {
            (
                ids::span_id(&self.session_id, &format!("special:{name}:{ts}")),
                ts,
                Value::Null,
            )
        });
        vec![SpanOp::Insert(SpanRow {
            span_id: id,
            root_span_id: self.effective_root_span_id.clone(),
            parent_span_ids: vec![self.root_span_id.clone()],
            name: name.into(),
            span_type: SpanType::Task,
            start_ms: Some(start),
            end_ms: Some(ts),
            input: Some(input),
            output: Some(event.clone()),
            metadata: Some(
                json!({"event_type":if compaction{"session_compact"}else{"session_tree"}}),
            ),
            tags: Some(vec![if compaction {
                "compaction".into()
            } else {
                "branch-summary".into()
            }]),
            ..Default::default()
        })]
    }
    fn close_turn(&mut self, ts: i64, error: Option<String>) -> Vec<SpanOp> {
        let turn = self.turn.take();
        // Native context/tool payloads are only correlation state for the active
        // turn. The journal can rebuild them if a retired session later resumes.
        self.pending_llms.clear();
        self.tools.clear();
        let Some((id, _)) = turn else {
            return vec![];
        };
        vec![SpanOp::Merge(SpanRow {
            span_id: id,
            root_span_id: self.effective_root_span_id.clone(),
            parent_span_ids: vec![self.root_span_id.clone()],
            end_ms: Some(ts),
            error,
            ..Default::default()
        })]
    }
    fn close_root(&mut self, ts: i64) -> SpanOp {
        self.opened = false;
        self.compaction = None;
        self.branch_summary = None;
        SpanOp::Merge(SpanRow {
            span_id: self.root_span_id.clone(),
            root_span_id: self.effective_root_span_id.clone(),
            parent_span_ids: self.external_parent.clone().into_iter().collect(),
            end_ms: Some(ts),
            metadata: Some(
                json!({"total_turns":self.turn_seq,"total_tool_calls":self.total_tools}),
            ),
            ..Default::default()
        })
    }
}

fn num(v: &Value, key: &str) -> i64 {
    v.get(key).and_then(Value::as_i64).unwrap_or(0)
}
fn response_model(v: &Value) -> Option<String> {
    for key in [
        "responseModel",
        "routedModel",
        "resolvedModel",
        "actualModel",
        "concreteModel",
        "outputModel",
        "model",
    ] {
        if let Some(s) = v.get(key).and_then(Value::as_str) {
            return Some(s.into());
        }
    }
    None
}
fn normalize_assistant(v: &Value) -> Value {
    let mut text = String::new();
    let mut reasoning = String::new();
    let mut calls = Vec::new();
    for part in v
        .get("content")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        match part.get("type").and_then(Value::as_str) {
            Some("text") => {
                if let Some(s) = part.get("text").and_then(Value::as_str) {
                    text.push_str(s);
                }
            }
            Some("thinking") => {
                if let Some(s) = part.get("thinking").and_then(Value::as_str) {
                    reasoning.push_str(s);
                }
            }
            Some("toolCall") => calls.push(json!({
                "id": part.get("id"),
                "type": "function",
                "function": {
                    "name": part.get("name"),
                    "arguments": serde_json::to_string(
                        part.get("arguments").unwrap_or(&Value::Null),
                    )
                    .unwrap_or_default(),
                },
            })),
            _ => {}
        }
    }
    let mut out = json!({"role":"assistant","content":text});
    if !reasoning.is_empty() {
        out["reasoning"] = json!([{"id":"reasoning","content":reasoning}])
    }
    if !calls.is_empty() {
        out["tool_calls"] = Value::Array(calls)
    }
    out
}
fn skill_from_read(tool: &str, args: &Value) -> Option<String> {
    if tool != "read" {
        return None;
    }
    let path = args
        .get("path")
        .or_else(|| args.get("filePath"))
        .or_else(|| args.get("file_path"))
        .and_then(Value::as_str)?;
    if !path.to_ascii_lowercase().ends_with("/skill.md") {
        return None;
    }
    path.rsplit('/').nth(1).map(str::to_owned)
}
fn explicit_skills(input: &str) -> Vec<String> {
    input
        .split_whitespace()
        .filter_map(|s| s.strip_prefix("/skill:"))
        .map(|s| {
            s.trim_matches(|c: char| matches!(c, ',' | ')' | '.' | ';'))
                .to_string()
        })
        .filter(|s| !s.is_empty())
        .collect()
}
fn format_error(v: Option<&Value>) -> String {
    error_text(v, "Tool execution failed")
}
