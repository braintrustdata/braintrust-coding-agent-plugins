//! Pi coding-agent translator. Pi forwards native extension callbacks and this
//! state machine owns all span construction and recovery.

use super::git::GitMetadataCache;
use super::tool::{error_text, with_tool_approval, ToolApproval};
use super::{
    local_username, root_tags, AgentTranslator, SessionCtx, SpanOp, SpanRow, SpanType,
    TranslatorFactory,
};
use crate::ids;
use crate::wire::Envelope;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Map, Value};
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
            active_compaction_message: None,
            branch_summary: None,
            last_ts: 0,
            thinking_level: None,
            git: self.git.clone(),
        })
    }
}

// These are deliberately partial native event types. Serde ignores fields we
// do not model, while `Value` remains at the user-controlled JSON boundaries
// that we forward without interpreting (messages, tool arguments, results).
// Keeping this vocabulary local lets the raw journal remain forward-compatible
// and makes each translator reducer explicit about the fields it consumes.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BeforeAgentStart {
    #[serde(default)]
    prompt: Option<Value>,
}

#[derive(Deserialize)]
struct ContextEvent {
    #[serde(default)]
    messages: Vec<Value>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProviderRequest {
    #[serde(flatten)]
    fields: Map<String, Value>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MessageUpdate {
    #[serde(default)]
    assistant_message_event: Option<MessageDelta>,
    #[serde(default, rename = "type")]
    kind: Option<String>,
}

#[derive(Deserialize)]
struct MessageDelta {
    #[serde(default, rename = "type")]
    kind: Option<String>,
}

#[derive(Deserialize)]
struct ThinkingLevel {
    #[serde(default)]
    level: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AssistantMessage {
    #[serde(default)]
    role: Option<String>,
    #[serde(default)]
    provider: Option<String>,
    #[serde(default)]
    api: Option<String>,
    #[serde(default)]
    response_model: Option<String>,
    #[serde(default)]
    routed_model: Option<String>,
    #[serde(default)]
    resolved_model: Option<String>,
    #[serde(default)]
    actual_model: Option<String>,
    #[serde(default)]
    concrete_model: Option<String>,
    #[serde(default)]
    output_model: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    usage: Usage,
    #[serde(default)]
    content: Vec<AssistantContent>,
    #[serde(default)]
    error_message: Option<String>,
    #[serde(default)]
    stop_reason: Option<String>,
    #[serde(default)]
    response_id: Option<String>,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Usage {
    #[serde(default)]
    input: i64,
    #[serde(default)]
    output: i64,
    #[serde(default)]
    reasoning: i64,
    #[serde(default)]
    cache_read: i64,
    #[serde(default)]
    cache_write: i64,
    #[serde(default)]
    cache_write1h: i64,
    #[serde(default)]
    total_tokens: Option<i64>,
    #[serde(default)]
    cost: Option<Value>,
}

#[derive(Deserialize)]
#[serde(tag = "type")]
enum AssistantContent {
    #[serde(rename = "text")]
    Text {
        #[serde(default)]
        text: Option<String>,
    },
    #[serde(rename = "thinking")]
    Thinking {
        #[serde(default)]
        thinking: Option<String>,
    },
    #[serde(rename = "toolCall")]
    ToolCall {
        #[serde(default)]
        id: Option<Value>,
        #[serde(default)]
        name: Option<Value>,
        #[serde(default)]
        arguments: Option<Value>,
    },
    #[serde(other)]
    Other,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ToolExecutionStart {
    tool_call_id: String,
    #[serde(default)]
    tool_name: Option<String>,
    #[serde(default)]
    args: Value,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ToolExecutionEnd {
    #[serde(default)]
    tool_call_id: Option<String>,
    #[serde(default)]
    tool_name: Option<String>,
    #[serde(default)]
    result: Option<Value>,
    #[serde(default)]
    is_error: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AgentEnd {
    #[serde(default)]
    will_retry: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SessionCompact {
    #[serde(default)]
    compaction_entry: Option<CompactionEntry>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CompactionEntry {
    #[serde(default)]
    summary: Option<Value>,
    #[serde(default)]
    tokens_before: Option<Value>,
    #[serde(default)]
    timestamp: Option<Value>,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Preparation {
    #[serde(default)]
    tokens_before: Option<Value>,
    #[serde(default)]
    first_kept_entry_id: Option<Value>,
    #[serde(default)]
    is_split_turn: Option<Value>,
    #[serde(default)]
    messages_to_summarize: Option<Vec<Value>>,
    #[serde(default)]
    turn_prefix_messages: Option<Vec<Value>>,
    #[serde(default)]
    target_id: Option<Value>,
    #[serde(default)]
    old_leaf_id: Option<Value>,
    #[serde(default)]
    common_ancestor_id: Option<Value>,
    #[serde(default)]
    user_wants_summary: Option<bool>,
    #[serde(default)]
    custom_instructions: Option<Value>,
    #[serde(default)]
    replace_instructions: Option<Value>,
    #[serde(default)]
    label: Option<Value>,
    #[serde(default)]
    entries_to_summarize: Option<Vec<Value>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BeforeCompact {
    #[serde(default)]
    reason: Option<Value>,
    #[serde(default)]
    will_retry: Option<Value>,
    #[serde(default)]
    custom_instructions: Option<Value>,
    #[serde(default)]
    preparation: Option<Preparation>,
    #[serde(default)]
    branch_entries: Option<Vec<Value>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BeforeTree {
    #[serde(default)]
    preparation: Option<Preparation>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SessionTree {
    #[serde(default)]
    summary_entry: Option<Value>,
}

fn decode<T: DeserializeOwned>(value: &Value) -> Option<T> {
    serde_json::from_value(value.clone()).ok()
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
    active_compaction_message: Option<Value>,
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
            "before_agent_start" => {
                if let Some(event) = decode(event) {
                    ops.extend(self.start_turn(event, envelope.ts_ms));
                }
            }
            "context" => {
                if let Some(event) = decode(event) {
                    self.capture_context(event, envelope.ts_ms);
                }
            }
            "before_provider_request" => {
                if let Some(event) = decode(event) {
                    self.provider_request(event);
                }
            }
            "message_update" => {
                if let Some(event) = decode(event) {
                    self.streaming_update(event, envelope.ts_ms);
                }
            }
            "thinking_level_select" => {
                if let Some(event) = decode::<ThinkingLevel>(event) {
                    self.thinking_level = event.level;
                }
            }
            "message_end" => ops.extend(self.message_end(event, envelope.ts_ms)),
            "tool_execution_start" => {
                if let Some(event) = decode(event) {
                    ops.extend(self.tool_start(event, envelope.ts_ms));
                }
            }
            "tool_execution_end" => {
                if let Some(event) = decode(event) {
                    ops.extend(self.tool_end(event, envelope.ts_ms));
                }
            }
            "agent_end" if decode::<AgentEnd>(event).is_none_or(|event| !event.will_retry) => {
                ops.extend(self.close_turn(envelope.ts_ms, None));
            }
            "session_before_compact" => {
                if let Some(event) = decode(event) {
                    self.compaction = Some((
                        ids::span_id(&self.session_id, &format!("compaction:{}", envelope.ts_ms)),
                        envelope.ts_ms,
                        compaction_input(&event),
                    ));
                }
            }
            "session_compact" => {
                if let Some(typed) = decode::<SessionCompact>(event) {
                    self.active_compaction_message = compaction_message(&typed);
                }
                ops.extend(self.finish_special("Compaction", true, event, envelope.ts_ms))
            }
            "session_before_tree" => {
                if let Some(event) = decode::<BeforeTree>(event) {
                    if event
                        .preparation
                        .as_ref()
                        .and_then(|preparation| preparation.user_wants_summary)
                        == Some(true)
                    {
                        self.branch_summary = Some((
                            ids::span_id(
                                &self.session_id,
                                &format!("branch-summary:{}", envelope.ts_ms),
                            ),
                            envelope.ts_ms,
                            branch_summary_input(&event),
                        ));
                    }
                }
            }
            "session_tree" => {
                // Tree navigation can select a branch with a different (or no)
                // active compaction. The next native context event is
                // authoritative for that branch.
                self.active_compaction_message = None;
                if decode::<SessionTree>(event)
                    .and_then(|event| event.summary_entry)
                    .is_some()
                    || self.branch_summary.is_some()
                {
                    ops.extend(self.finish_special("Branch Summary", false, event, envelope.ts_ms))
                }
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
        metadata.insert("username".into(), json!(local_username()));
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
            tags: root_tags(ctx),
            ..Default::default()
        })]
    }

    fn start_turn(&mut self, event: BeforeAgentStart, ts: i64) -> Vec<SpanOp> {
        let mut ops = self.close_turn(ts, None);
        self.turn_seq += 1;
        self.llm_seq = 0;
        self.pending_llms.clear();
        self.tools.clear();
        let id = ids::span_id(&self.session_id, &format!("turn:{}", self.turn_seq));
        let input = event.prompt.unwrap_or(Value::Null);
        let skills = input.as_str().map(explicit_skills).unwrap_or_default();
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
    fn capture_context(&mut self, event: ContextEvent, ts: i64) {
        let mut messages = event.messages;
        let native_compaction = messages.iter().position(|message| {
            message.get("role").and_then(Value::as_str) == Some("compactionSummary")
        });
        match native_compaction {
            Some(0) => self.active_compaction_message = messages.first().cloned(),
            Some(index) => {
                let message = messages.remove(index);
                self.active_compaction_message = Some(message.clone());
                messages.insert(0, message);
            }
            None => {
                if let Some(compaction) = &self.active_compaction_message {
                    messages.insert(0, compaction.clone());
                }
            }
        }
        let input = Value::Array(messages);
        self.pending_llms.push(PendingLlm {
            start_ms: ts,
            input,
            first_token_ms: None,
            provider: None,
        });
    }
    fn provider_request(&mut self, event: ProviderRequest) {
        if let Some(call) = self.pending_llms.last_mut() {
            let mut provider = event.fields;
            if let Some(payload) = provider.get_mut("payload").and_then(Value::as_object_mut) {
                // The authoritative provider-visible messages are already the
                // LLM span input captured by the preceding context event.
                payload.remove("messages");
            }
            call.provider = Some(Value::Object(provider))
        }
    }
    fn streaming_update(&mut self, event: MessageUpdate, ts: i64) {
        if let Some(call) = self.pending_llms.last_mut() {
            let kind = event
                .assistant_message_event
                .and_then(|event| event.kind)
                .or(event.kind);
            let kind = kind.as_deref().unwrap_or("");
            if matches!(kind, "text_delta" | "thinking_delta" | "text" | "thinking")
                && call.first_token_ms.is_none()
            {
                call.first_token_ms = Some(ts)
            }
        }
    }
    fn message_end(&mut self, event: &Value, ts: i64) -> Vec<SpanOp> {
        let message = event.get("message").unwrap_or(event);
        let Some(message) = decode::<AssistantMessage>(message) else {
            return vec![];
        };
        if message.role.as_deref() != Some("assistant") {
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
        let model = response_model(&message);
        let prompt = message.usage.input
            + message.usage.cache_read
            + message.usage.cache_write
            + message.usage.cache_write1h;
        let completion = message.usage.output;
        let reasoning = message.usage.reasoning;
        let total = message
            .usage
            .total_tokens
            .unwrap_or(prompt + completion + reasoning);
        let output = normalize_assistant(&message);
        let error = message
            .error_message
            .clone()
            .filter(|_| matches!(message.stop_reason.as_deref(), Some("error" | "aborted")));
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
                "provider": message.provider,
                "api": message.api,
                "stop_reason": message.stop_reason,
                "thinking_level": self.thinking_level,
                "provider_request": pending.provider,
                "response_id": message.response_id,
            })),
            metrics: Some(json!({
                "prompt_tokens": prompt,
                "completion_tokens": completion,
                "reasoning_tokens": reasoning,
                "tokens": total,
                "prompt_cached_tokens": message.usage.cache_read,
                "prompt_cache_creation_tokens": message.usage.cache_write
                    + message.usage.cache_write1h,
                "time_to_first_token": ttft,
                "cost": message.usage.cost,
            })),
            error,
            ..Default::default()
        })]
    }
    fn tool_start(&mut self, event: ToolExecutionStart, ts: i64) -> Vec<SpanOp> {
        let id = event.tool_call_id;
        let Some((turn, _)) = &self.turn else {
            return vec![];
        };
        let name = event.tool_name.unwrap_or_else(|| "tool".into());
        let args = event.args;
        if self.tools.contains_key(&id) {
            return vec![];
        }
        self.tools.insert(
            id.clone(),
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
            name: name.clone(),
            span_type: SpanType::Tool,
            start_ms: Some(ts),
            input: Some(args),
            metadata: Some(with_tool_approval(
                json!({
                    "tool_name": name,
                    "tool_call_id": id,
                }),
                Some(ToolApproval::Approved),
            )),
            ..Default::default()
        })]
    }
    fn tool_end(&mut self, event: ToolExecutionEnd, ts: i64) -> Vec<SpanOp> {
        let Some((turn, _)) = &self.turn else {
            return vec![];
        };
        let call = event.tool_call_id.unwrap_or_default();
        let pending = self.tools.remove(&call);
        let tracked = pending.clone().unwrap_or(ToolStart {
            start_ms: ts,
            name: event.tool_name.unwrap_or_else(|| "tool".into()),
            args: Value::Null,
        });
        self.total_tools += 1;
        let failed = event.is_error;
        let skill = skill_from_read(&tracked.name, &tracked.args);
        let name = skill
            .as_ref()
            .map(|s| format!("skill: {s}"))
            .unwrap_or_else(|| tracked.name.clone());
        let row = SpanRow {
            span_id: ids::span_id(&self.session_id, &format!("tool:{}:{call}", self.turn_seq)),
            root_span_id: self.effective_root_span_id.clone(),
            parent_span_ids: pending
                .is_none()
                .then(|| turn.clone())
                .into_iter()
                .collect(),
            name,
            span_type: SpanType::Tool,
            start_ms: pending.is_none().then_some(tracked.start_ms),
            end_ms: Some(ts),
            input: pending.is_none().then_some(tracked.args),
            output: event.result.clone(),
            metadata: Some(with_tool_approval(
                json!({
                    "tool_name": if skill.is_some() { "skill" } else { &tracked.name },
                    "original_tool_name": tracked.name,
                    "tool_call_id": call,
                    "skill_name": skill,
                }),
                Some(ToolApproval::Approved),
            )),
            error: failed.then(|| format_error(event.result.as_ref())),
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
            end_ms: Some(ts),
            error,
            ..Default::default()
        })]
    }
    fn close_root(&mut self, ts: i64) -> SpanOp {
        self.opened = false;
        self.compaction = None;
        self.active_compaction_message = None;
        self.branch_summary = None;
        SpanOp::Merge(SpanRow {
            span_id: self.root_span_id.clone(),
            root_span_id: self.effective_root_span_id.clone(),
            end_ms: Some(ts),
            metadata: Some(
                json!({"total_turns":self.turn_seq,"total_tool_calls":self.total_tools}),
            ),
            ..Default::default()
        })
    }
}

fn compaction_message(event: &SessionCompact) -> Option<Value> {
    let entry = event.compaction_entry.as_ref()?;
    let summary = entry.summary.clone()?;
    Some(json!({
        "role": "compactionSummary",
        "summary": summary,
        "tokensBefore": entry.tokens_before.clone().unwrap_or(Value::Null),
        "timestamp": entry.timestamp.clone().unwrap_or(Value::Null),
    }))
}

fn compaction_input(event: &BeforeCompact) -> Value {
    let preparation = event.preparation.as_ref();
    json!({
        "reason": event.reason.clone(),
        "willRetry": event.will_retry.clone(),
        "customInstructions": event.custom_instructions.clone(),
        "tokensBefore": preparation.and_then(|value| value.tokens_before.clone()),
        "firstKeptEntryId": preparation.and_then(|value| value.first_kept_entry_id.clone()),
        "isSplitTurn": preparation.and_then(|value| value.is_split_turn.clone()),
        "messagesToSummarizeCount": preparation
            .and_then(|value| value.messages_to_summarize.as_ref())
            .map(Vec::len),
        "turnPrefixMessagesCount": preparation
            .and_then(|value| value.turn_prefix_messages.as_ref())
            .map(Vec::len),
        "branchEntryCount": event.branch_entries.as_ref().map(Vec::len),
    })
}

fn branch_summary_input(event: &BeforeTree) -> Value {
    let preparation = event.preparation.as_ref();
    json!({
        "targetId": preparation.and_then(|value| value.target_id.clone()),
        "oldLeafId": preparation.and_then(|value| value.old_leaf_id.clone()),
        "commonAncestorId": preparation.and_then(|value| value.common_ancestor_id.clone()),
        "userWantsSummary": preparation.and_then(|value| value.user_wants_summary),
        "customInstructions": preparation.and_then(|value| value.custom_instructions.clone()),
        "replaceInstructions": preparation.and_then(|value| value.replace_instructions.clone()),
        "label": preparation.and_then(|value| value.label.clone()),
        "entriesToSummarizeCount": preparation
            .and_then(|value| value.entries_to_summarize.as_ref())
            .map(Vec::len),
    })
}

fn response_model(message: &AssistantMessage) -> Option<String> {
    [
        &message.response_model,
        &message.routed_model,
        &message.resolved_model,
        &message.actual_model,
        &message.concrete_model,
        &message.output_model,
        &message.model,
    ]
    .into_iter()
    .flatten()
    .next()
    .cloned()
}
fn normalize_assistant(message: &AssistantMessage) -> Value {
    let mut text = String::new();
    let mut reasoning = String::new();
    let mut calls = Vec::new();
    for part in &message.content {
        match part {
            AssistantContent::Text {
                text: Some(content),
            } => text.push_str(content),
            AssistantContent::Thinking {
                thinking: Some(content),
            } => reasoning.push_str(content),
            AssistantContent::ToolCall {
                id,
                name,
                arguments,
            } => calls.push(json!({
                "id": id,
                "type": "function",
                "function": {
                    "name": name,
                    "arguments": serde_json::to_string(
                        arguments.as_ref().unwrap_or(&Value::Null),
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
