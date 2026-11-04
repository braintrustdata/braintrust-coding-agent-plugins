//! OpenCode 2 native event translation.
mod event;
mod span;

use self::event::*;
use self::span::*;
use super::{
    git::GitMetadataCache, local_username, recent::RecentSet, tool::ToolApproval, AgentTranslator,
    SessionCtx, SpanOp, SpanRow, SpanType,
};
use crate::{ids, wire::Envelope};
use serde::Serialize;
use serde_json::Value;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

// SpanRow exposes JSON at the sink boundary; all data before this conversion
// uses the concrete types in `span` (apart from native, open-ended values).
fn span_value(value: impl Serialize) -> Value {
    match serde_json::to_value(value) {
        Ok(value) => value,
        Err(error) => {
            tracing::warn!(%error, "could not serialize OpenCode span data");
            Value::Null
        }
    }
}

#[derive(Default)]
struct SessionState {
    root: String,
    trace_root: String,
    parents: Vec<String>,
    parent_session: Option<String>,
    owner_turn: Option<String>,
    turn: Option<String>,
    turn_number: u32,
    tool_count: u32,
    idle: bool,
    output: Option<String>,
    history: Vec<HistoryEntry>,
    native_window: Option<NativeWindow>,
    calls: HashMap<String, Vec<ToolCall>>,
    results: HashMap<String, Vec<Message>>,
    tools: HashMap<String, ToolState>,
    denied_tools: HashSet<String>,
    completed_messages: RecentSet<String>,
}
struct ToolState {
    name: String,
    skill_name: Option<String>,
    message_id: String,
    span_id: String,
    parent: String,
    denied: bool,
}
struct Step {
    started: i64,
    model: Option<Model>,
    context: Option<Context>,
}
struct Permission {
    session_id: String,
    call_id: Option<String>,
    action: String,
}
struct HistoryEntry {
    id: String,
    messages: Vec<Message>,
}
struct NativeWindow {
    model: Option<Model>,
    messages: Vec<Message>,
    history_boundary: usize,
}
struct Compaction {
    started: i64,
    input_id: Option<String>,
}
struct CompactionResult<'a> {
    session_id: &'a str,
    id: &'a str,
    text: String,
    model: Option<Model>,
    tokens: Option<Tokens>,
    started: i64,
    ended: i64,
    error: Option<String>,
    keep_id: Option<String>,
    input_id: Option<String>,
    native_messages: Option<NativeMessages>,
}

pub(super) struct OpenCodeV2Translator {
    daemon_session_id: String,
    sessions: HashMap<String, SessionState>,
    pending_inputs: HashMap<(String, String), Prompt>,
    tool_names: HashMap<(String, String), String>,
    steps: HashMap<(String, String), Step>,
    pending_context: HashMap<String, Context>,
    text_parts: HashMap<(String, String, bool), BTreeMap<u64, String>>,
    permissions: HashMap<String, Permission>,
    compactions: HashMap<String, Compaction>,
    seen: RecentSet<String>,
    last_ts_ms: i64,
    git: Arc<GitMetadataCache>,
}
impl OpenCodeV2Translator {
    pub(super) fn new(session_id: &str) -> Self {
        Self {
            daemon_session_id: session_id.into(),
            sessions: HashMap::new(),
            pending_inputs: HashMap::new(),
            tool_names: HashMap::new(),
            steps: HashMap::new(),
            pending_context: HashMap::new(),
            text_parts: HashMap::new(),
            permissions: HashMap::new(),
            compactions: HashMap::new(),
            seen: RecentSet::default(),
            last_ts_ms: 0,
            git: GitMetadataCache::shared(),
        }
    }
    fn open(
        &mut self,
        sid: &str,
        parent_id: Option<&str>,
        title: Option<&str>,
        ts: i64,
    ) -> Vec<SpanOp> {
        if self.sessions.contains_key(sid) {
            return vec![];
        }
        let root = ids::span_id(&self.daemon_session_id, &format!("session:{sid}"));
        let parent = parent_id.and_then(|p| self.sessions.get(p));
        let trace_root = parent
            .map(|p| p.trace_root.clone())
            .unwrap_or_else(|| root.clone());
        let parents = parent
            .and_then(|p| p.turn.clone())
            .into_iter()
            .collect::<Vec<_>>();
        let owner = parent.and_then(|p| p.owner_turn.clone().or_else(|| p.turn.clone()));
        let name = if parent_id.is_some() {
            let title = title.unwrap_or("Subagent");
            if let Some((description, agent)) = title.split_once(" (@") {
                format!(
                    "{}: {description}",
                    agent.strip_suffix(" subagent)").unwrap_or(agent)
                )
            } else {
                title.to_owned()
            }
        } else {
            "OpenCode".into()
        };
        let metadata = RootMetadata {
            session_id: sid,
            source: "opencode",
            defer_root: true,
            username: local_username(),
            parent_session_id: parent_id,
            is_subagent: parent_id.map(|_| true),
        };
        self.sessions.insert(
            sid.into(),
            SessionState {
                root: root.clone(),
                trace_root: trace_root.clone(),
                parents: parents.clone(),
                parent_session: parent_id.map(str::to_owned),
                owner_turn: owner.clone(),
                ..Default::default()
            },
        );
        vec![SpanOp::Insert(SpanRow {
            span_id: root,
            root_span_id: trace_root,
            parent_span_ids: parents,
            turn_span_id: owner,
            name,
            span_type: SpanType::Task,
            start_ms: Some(ts),
            metadata: Some(span_value(metadata)),
            ..Default::default()
        })]
    }
    fn turn(&mut self, sid: &str, id: &str, prompt: Prompt, ts: i64) -> Vec<SpanOp> {
        let mut ops = self.open(sid, None, None, ts);
        let Some(s) = self.sessions.get_mut(sid) else {
            return ops;
        };
        if s.idle {
            s.idle = false;
            ops.push(SpanOp::Merge(SpanRow {
                span_id: s.root.clone(),
                root_span_id: s.trace_root.clone(),
                parent_span_ids: s.parents.clone(),
                ..Default::default()
            }));
        }
        if let Some(turn) = s.turn.take() {
            ops.push(SpanOp::Merge(SpanRow {
                span_id: turn,
                root_span_id: s.trace_root.clone(),
                parent_span_ids: vec![s.root.clone()],
                end_ms: Some(ts),
                output: s.output.take().map(Value::String),
                ..Default::default()
            }));
        }
        s.turn_number += 1;
        let turn = ids::span_id(&self.daemon_session_id, &format!("turn:{sid}:{id}"));
        let mut content = vec![ContentPart::text(prompt.text.clone())];
        for file in prompt.files {
            if file.mime.starts_with("image/") {
                content.push(ContentPart::image(format!(
                    "data:{};base64,{}",
                    file.mime, file.data
                )));
            }
        }
        let input = if content.len() == 1 {
            Content::Text(prompt.text.clone())
        } else {
            Content::Parts(content)
        };
        let has_input = !prompt.text.is_empty() || matches!(input, Content::Parts(_));
        s.history.push(HistoryEntry {
            id: id.into(),
            messages: has_input
                .then(|| Message::new("user", input.clone()))
                .into_iter()
                .collect(),
        });
        let mut skills = prompt
            .text
            .split_whitespace()
            .filter_map(|s| {
                s.strip_prefix("/skills:")
                    .or_else(|| s.strip_prefix("/skills"))
            })
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
            .collect::<Vec<_>>();
        for skill in prompt.skills {
            if !skills.contains(&skill.name) {
                skills.push(skill.name);
            }
        }
        s.turn = Some(turn.clone());
        ops.push(SpanOp::Insert(SpanRow {
            span_id: turn.clone(),
            root_span_id: s.trace_root.clone(),
            parent_span_ids: vec![s.root.clone()],
            turn_span_id: Some(s.owner_turn.clone().unwrap_or(turn)),
            name: format!("Turn {}", s.turn_number),
            span_type: SpanType::Task,
            is_turn: true,
            start_ms: Some(ts),
            input: has_input.then(|| span_value(input)),
            metadata: Some(span_value(TurnMetadata {
                turn_number: s.turn_number,
                model: None,
                loaded_skill_names: skills,
            })),
            ..Default::default()
        }));
        ops
    }
    fn finish(
        &mut self,
        sid: &str,
        ts: i64,
        permanent: bool,
        error: Option<String>,
    ) -> Vec<SpanOp> {
        let Some(mut s) = self.sessions.remove(sid) else {
            return vec![];
        };
        let mut ops = Vec::new();
        for (_, tool) in s.tools.drain() {
            if !tool.denied {
                ops.push(SpanOp::Merge(SpanRow {
                    span_id: tool.span_id,
                    root_span_id: s.trace_root.clone(),
                    parent_span_ids: vec![tool.parent],
                    end_ms: Some(ts),
                    error: Some("Interrupted before tool completion".into()),
                    ..Default::default()
                }));
            }
        }
        if let Some(turn) = s.turn.take() {
            ops.push(SpanOp::Merge(SpanRow {
                span_id: turn,
                root_span_id: s.trace_root.clone(),
                parent_span_ids: vec![s.root.clone()],
                end_ms: Some(ts),
                output: s.output.take().map(Value::String),
                error: error.clone(),
                ..Default::default()
            }));
        }
        ops.push(SpanOp::Merge(SpanRow {
            span_id: s.root.clone(),
            root_span_id: s.trace_root.clone(),
            parent_span_ids: s.parents.clone(),
            end_ms: Some(ts),
            metadata: Some(span_value(RootTotals {
                total_turns: s.turn_number,
                total_tool_calls: s.tool_count,
            })),
            error,
            ..Default::default()
        }));
        if !permanent && s.parent_session.is_none() {
            s.idle = true;
            self.sessions.insert(sid.into(), s);
        }
        ops
    }
}
impl AgentTranslator for OpenCodeV2Translator {
    fn handle(&mut self, event: &Envelope, _ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        self.last_ts_ms = self.last_ts_ms.max(event.ts_ms);
        let mut payload = event.payload.clone();
        let Some(object) = payload.as_object_mut() else {
            return Ok(vec![]);
        };
        object.insert("type".into(), event.event.clone().into());
        let Ok(native) = serde_json::from_value::<NativeEnvelope>(payload) else {
            return Ok(vec![]);
        };
        if let Some(id) = &native.id {
            if !self.seen.insert(id.clone()) {
                return Ok(vec![]);
            }
        }
        let directory = native
            .location
            .as_ref()
            .and_then(|location| location.directory.as_deref());
        let ts = event.ts_ms;
        let mut ops = match native.event {
            NativeEvent::Created(data) => self.open(
                &data.session_id,
                data.parent_id.as_deref(),
                data.title.as_deref(),
                ts,
            ),
            NativeEvent::Enqueued(data) => {
                if let InboxItem::User { payload } = data.item {
                    self.pending_inputs
                        .insert((data.session_id, data.inbox_id), payload);
                }
                vec![]
            }
            NativeEvent::Delivered(data) => {
                match self
                    .pending_inputs
                    .remove(&(data.session_id.clone(), data.inbox_id.clone()))
                {
                    Some(prompt) => self.turn(&data.session_id, &data.inbox_id, prompt, ts),
                    None => vec![],
                }
            }
            NativeEvent::Cancelled(data) => {
                self.pending_inputs
                    .remove(&(data.session_id, data.inbox_id));
                vec![]
            }
            NativeEvent::Context(data) => {
                self.pending_context.insert(data.session_id.clone(), data);
                vec![]
            }
            NativeEvent::StepStarted(data) => {
                let context = self.pending_context.remove(&data.session_id);
                self.steps.insert(
                    (data.session_id, data.message_id),
                    Step {
                        started: data.started.unwrap_or(ts),
                        model: data.model,
                        context,
                    },
                );
                vec![]
            }
            NativeEvent::ToolInputStarted(data) => {
                self.tool_names
                    .insert((data.session_id, data.id), data.name);
                vec![]
            }
            NativeEvent::ToolCalled(data) => {
                let name = self
                    .tool_names
                    .get(&(data.session_id.clone(), data.id.clone()))
                    .cloned()
                    .unwrap_or_else(|| "tool".into());
                let Some(s) = self.sessions.get_mut(&data.session_id) else {
                    return Ok(vec![]);
                };
                let Some(parent) = s.turn.clone() else {
                    return Ok(vec![]);
                };
                if s.tools.contains_key(&data.id) {
                    return Ok(vec![]);
                }
                if s.denied_tools.contains(&data.id) {
                    return Ok(vec![]);
                }
                let span_id = ids::span_id(
                    &self.daemon_session_id,
                    &format!("tool:{}:{}", data.session_id, data.id),
                );
                let span_name = if name == "skill" {
                    data.input
                        .get("name")
                        .and_then(Value::as_str)
                        .map(|skill| format!("skill: {skill}"))
                        .unwrap_or_else(|| name.clone())
                } else {
                    name.clone()
                };
                let skill_name = (name == "skill")
                    .then(|| {
                        data.input
                            .get("name")
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                    })
                    .flatten();
                s.calls
                    .entry(data.message_id.clone())
                    .or_default()
                    .push(ToolCall::new(data.id.clone(), name.clone(), &data.input));
                s.tools.insert(
                    data.id.clone(),
                    ToolState {
                        name: name.clone(),
                        skill_name,
                        message_id: data.message_id,
                        span_id: span_id.clone(),
                        parent: parent.clone(),
                        denied: false,
                    },
                );
                vec![SpanOp::Insert(SpanRow {
                    span_id,
                    root_span_id: s.trace_root.clone(),
                    parent_span_ids: vec![parent],
                    turn_span_id: s.owner_turn.clone().or_else(|| s.turn.clone()),
                    name: span_name,
                    span_type: SpanType::Tool,
                    start_ms: Some(ts),
                    input: Some(data.input),
                    metadata: Some(span_value(ToolMetadata::new(&name, &data.id))),
                    ..Default::default()
                })]
            }
            NativeEvent::ToolSuccess(data) => {
                let output = normalize_tool_content(data.content.unwrap_or_default());
                self.finish_tool(
                    &data.session_id,
                    &data.message_id,
                    &data.id,
                    Some(output),
                    None,
                    ts,
                )
            }
            NativeEvent::ToolFailed(data) => {
                let error = data.error.message;
                self.finish_tool(
                    &data.session_id,
                    &data.message_id,
                    &data.id,
                    None,
                    Some(error),
                    ts,
                )
            }
            NativeEvent::PermissionAsked(data) => {
                self.permissions.insert(
                    data.id,
                    Permission {
                        session_id: data.session_id,
                        call_id: data.source.map(|source| source.id),
                        action: data.action,
                    },
                );
                vec![]
            }
            NativeEvent::PermissionReplied(data) => {
                let Some(p) = self.permissions.remove(&data.request_id) else {
                    return Ok(vec![]);
                };
                if p.session_id != data.session_id {
                    return Ok(vec![]);
                }
                if !matches!(data.reply.as_str(), "reject" | "denied" | "deny") {
                    return Ok(vec![]);
                }
                let Some(call) = p.call_id else {
                    return Ok(vec![]);
                };
                let Some(s) = self.sessions.get_mut(&p.session_id) else {
                    return Ok(vec![]);
                };
                let tool_name = self
                    .tool_names
                    .get(&(p.session_id.clone(), call.clone()))
                    .cloned()
                    .unwrap_or_else(|| p.action.clone());
                let existing = s.tools.get_mut(&call);
                let (span_id, parent, had_start) = if let Some(tool) = existing {
                    tool.denied = true;
                    (tool.span_id.clone(), tool.parent.clone(), true)
                } else {
                    let Some(parent) = s.turn.clone() else {
                        return Ok(vec![]);
                    };
                    (
                        ids::span_id(
                            &self.daemon_session_id,
                            &format!("tool:{}:{call}", p.session_id),
                        ),
                        parent,
                        false,
                    )
                };
                s.denied_tools.insert(call.clone());
                let mut metadata = ToolMetadata::new(&tool_name, &call);
                metadata.tool_approval = Some(ToolApproval::Denied);
                metadata.permission_id = Some(&data.request_id);
                metadata.permission_type = Some(&p.action);
                let row = SpanRow {
                    span_id,
                    root_span_id: s.trace_root.clone(),
                    parent_span_ids: vec![parent],
                    turn_span_id: (!had_start)
                        .then(|| s.owner_turn.clone().or_else(|| s.turn.clone()))
                        .flatten(),
                    name: if had_start {
                        String::new()
                    } else {
                        tool_name.clone()
                    },
                    span_type: SpanType::Tool,
                    start_ms: (!had_start).then_some(ts),
                    end_ms: Some(ts),
                    metadata: Some(span_value(metadata)),
                    ..Default::default()
                };
                vec![if had_start {
                    SpanOp::Merge(row)
                } else {
                    SpanOp::Insert(row)
                }]
            }
            NativeEvent::TextEnded(data) | NativeEvent::ReasoningEnded(data) => {
                let reasoning = event.event == "session.reasoning.ended";
                self.text_parts
                    .entry((data.session_id, data.message_id, reasoning))
                    .or_default()
                    .insert(data.ordinal.unwrap_or(0), data.text);
                vec![]
            }
            NativeEvent::StepEnded(data) | NativeEvent::StepFailed(data) => {
                self.finish_step(data, ts)
            }
            NativeEvent::CompactionStarted(data) => {
                self.compactions.insert(
                    data.session_id,
                    Compaction {
                        started: ts,
                        input_id: data.input_id,
                    },
                );
                vec![]
            }
            NativeEvent::CompactionEnded(data) => {
                let start = self
                    .compactions
                    .remove(&data.session_id)
                    .unwrap_or(Compaction {
                        started: ts,
                        input_id: None,
                    });
                self.compact(CompactionResult {
                    session_id: &data.session_id,
                    id: native.id.as_deref().unwrap_or("compaction-summary"),
                    text: data.text,
                    model: data.model,
                    tokens: data.tokens,
                    started: start.started,
                    ended: ts,
                    error: None,
                    keep_id: native.metadata.and_then(|m| m.keep_id),
                    input_id: start.input_id,
                    native_messages: data
                        .native
                        .and_then(|n| n.messages)
                        .or_else(|| data.provider_context.and_then(|n| n.messages)),
                })
            }
            NativeEvent::CompactionFailed(data) => {
                let start = self
                    .compactions
                    .remove(&data.session_id)
                    .unwrap_or(Compaction {
                        started: ts,
                        input_id: None,
                    });
                self.compact(CompactionResult {
                    session_id: &data.session_id,
                    id: native.id.as_deref().unwrap_or("compaction-failed"),
                    text: String::new(),
                    model: None,
                    tokens: data.tokens,
                    started: start.started,
                    ended: ts,
                    error: Some(data.error.message),
                    keep_id: None,
                    input_id: start.input_id,
                    native_messages: None,
                })
            }
            NativeEvent::Succeeded(data) => self.finish(&data.session_id, ts, false, None),
            NativeEvent::Failed(data) => {
                self.finish(&data.session_id, ts, true, Some(data.error.message))
            }
            NativeEvent::Interrupted(data) => self.finish(
                &data.session_id,
                ts,
                true,
                Some(format!(
                    "OpenCode session interrupted ({})",
                    data.reason.as_deref().unwrap_or("unknown")
                )),
            ),
            NativeEvent::Deleted(data) => self.finish(&data.session_id, ts, true, None),
            NativeEvent::Other => vec![],
        };
        self.git.enrich_rows(directory, &mut ops);
        Ok(ops)
    }
    fn finalize(&mut self, _ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        let ids: Vec<_> = self.sessions.keys().cloned().collect();
        Ok(ids
            .iter()
            .flat_map(|id| self.finish(id, self.last_ts_ms, true, None))
            .collect())
    }
}
impl OpenCodeV2Translator {
    fn finish_tool(
        &mut self,
        sid: &str,
        message_id: &str,
        call: &str,
        output: Option<Vec<ContentPart>>,
        error: Option<String>,
        ts: i64,
    ) -> Vec<SpanOp> {
        self.tool_names.remove(&(sid.into(), call.into()));
        let Some(s) = self.sessions.get_mut(sid) else {
            return vec![];
        };
        if s.denied_tools.remove(call) {
            s.tools.remove(call);
            return vec![];
        }
        if !s
            .tools
            .get(call)
            .is_some_and(|tool| tool.message_id == message_id)
        {
            return vec![];
        }
        let Some(tool) = s.tools.remove(call) else {
            return vec![];
        };
        if tool.denied {
            return vec![];
        }
        s.tool_count += 1;
        let result = if let Some(error) = &error {
            Content::Text(error.clone())
        } else {
            Content::Parts(output.clone().unwrap_or_default())
        };
        s.results
            .entry(tool.message_id)
            .or_default()
            .push(Message::tool(call.into(), result));
        let mut metadata = ToolMetadata::new(&tool.name, call);
        metadata.tool_approval = Some(ToolApproval::Approved);
        if tool.name == "skill" {
            metadata.tool_kind = Some("skill");
            metadata.skill_name = Some(tool.skill_name.as_deref());
        }
        vec![SpanOp::Merge(SpanRow {
            span_id: tool.span_id,
            root_span_id: s.trace_root.clone(),
            parent_span_ids: vec![tool.parent],
            end_ms: Some(ts),
            output: Some(span_value(output)),
            error,
            metadata: Some(span_value(metadata)),
            ..Default::default()
        })]
    }
    fn finish_step(&mut self, data: StepEnded, ts: i64) -> Vec<SpanOp> {
        let key = (data.session_id.clone(), data.message_id.clone());
        let step = self.steps.remove(&key).unwrap_or(Step {
            started: ts,
            model: None,
            context: None,
        });
        let text = self
            .text_parts
            .remove(&(data.session_id.clone(), data.message_id.clone(), false))
            .map(|parts| parts.into_values().collect::<String>())
            .unwrap_or_default();
        let reasoning = self
            .text_parts
            .remove(&(data.session_id.clone(), data.message_id.clone(), true))
            .map(|parts| parts.into_values().collect::<String>());
        let Some(s) = self.sessions.get_mut(&data.session_id) else {
            return vec![];
        };
        if !s.completed_messages.insert(data.message_id.clone()) {
            return vec![];
        }
        let Some(parent) = s.turn.clone() else {
            return vec![];
        };
        let mut assistant = Message::new("assistant", Content::Text(text.clone()));
        let calls = s.calls.remove(&data.message_id).unwrap_or_default();
        if !calls.is_empty() {
            assistant.tool_calls = calls.clone();
        }
        if let Some(reasoning) = reasoning {
            assistant.reasoning = Some(vec![Reasoning {
                id: "reasoning",
                content: reasoning,
            }]);
        }
        let input = step.context.map(normalize_context).unwrap_or_else(|| {
            let window = s.native_window.as_ref().filter(|window| {
                match (window.model.as_ref(), step.model.as_ref()) {
                    (Some(a), Some(b)) => {
                        a.id.is_some()
                            && a.provider_id.is_some()
                            && a.id == b.id
                            && a.provider_id == b.provider_id
                    }
                    _ => false,
                }
            });
            let (mut input, start) = window.map_or_else(
                || (Vec::new(), 0),
                |window| (window.messages.clone(), window.history_boundary),
            );
            input.extend(
                s.history[start..]
                    .iter()
                    .flat_map(|entry| entry.messages.iter().cloned()),
            );
            input
        });
        if !text.is_empty() {
            s.output = Some(text.clone());
        }
        let mut messages = Vec::new();
        if !text.is_empty() || !calls.is_empty() {
            messages.push(assistant.clone());
        }
        messages.extend(s.results.remove(&data.message_id).unwrap_or_default());
        s.history.push(HistoryEntry {
            id: data.message_id.clone(),
            messages,
        });
        let metrics = Metrics::from_tokens(data.tokens.unwrap_or_default());
        let provider = step
            .model
            .as_ref()
            .and_then(|m| m.provider_id.as_deref())
            .unwrap_or("unknown");
        let model = step
            .model
            .as_ref()
            .and_then(|m| m.id.as_deref())
            .unwrap_or("unknown");
        let metadata = LlmMetadata {
            provider,
            model,
            message_id: &data.message_id,
            variant: step.model.as_ref().and_then(|m| m.variant.as_deref()),
        };
        vec![SpanOp::Insert(SpanRow {
            span_id: ids::span_id(
                &self.daemon_session_id,
                &format!("llm:{}:{}", data.session_id, data.message_id),
            ),
            root_span_id: s.trace_root.clone(),
            parent_span_ids: vec![parent.clone()],
            turn_span_id: Some(s.owner_turn.clone().unwrap_or(parent)),
            name: format!("{provider}/{model}"),
            span_type: SpanType::Llm,
            start_ms: Some(step.started),
            end_ms: Some(ts),
            input: Some(span_value(input)),
            output: Some(span_value([assistant])),
            metadata: Some(span_value(metadata)),
            metrics: Some(span_value(metrics)),
            error: data.error.map(|e| e.message),
            ..Default::default()
        })]
    }
    fn compact(&mut self, result: CompactionResult<'_>) -> Vec<SpanOp> {
        let CompactionResult {
            session_id: sid,
            id,
            text,
            model,
            tokens,
            started,
            ended: ts,
            error,
            keep_id,
            input_id,
            native_messages,
        } = result;
        let Some(s) = self.sessions.get_mut(sid) else {
            return vec![];
        };
        let parent = s.turn.clone().unwrap_or_else(|| s.root.clone());
        if let Some(input_id) = input_id {
            if let Some(window) = &mut s.native_window {
                let removed = s.history[..window.history_boundary]
                    .iter()
                    .filter(|entry| entry.id == input_id)
                    .count();
                window.history_boundary -= removed;
            }
            s.history.retain(|entry| entry.id != input_id);
        }
        let tail = keep_id
            .as_deref()
            .and_then(|id| s.history.iter().position(|entry| entry.id == id))
            .unwrap_or(s.history.len());
        let input: Vec<_> = s.history[..tail]
            .iter()
            .flat_map(|entry| entry.messages.iter().cloned())
            .collect();
        let output = match native_messages {
            Some(NativeMessages::Messages(messages)) => {
                let messages = normalize_context(Context {
                    session_id: sid.to_owned(),
                    system: Vec::new(),
                    messages: messages
                        .into_iter()
                        .map(|message| ContextMessage {
                            role: message.role,
                            content: message.content,
                        })
                        .collect(),
                });
                if error.is_none() {
                    s.native_window = Some(NativeWindow {
                        model: model.clone(),
                        messages: messages.clone(),
                        history_boundary: s.history.len(),
                    });
                }
                span_value(messages)
            }
            Some(NativeMessages::Opaque(messages)) => {
                if error.is_none() {
                    s.native_window = None;
                }
                messages
            }
            None => {
                if error.is_none() {
                    let kept = s.history.drain(tail..).collect::<Vec<_>>();
                    s.history.clear();
                    s.history.push(HistoryEntry {
                        id: id.into(),
                        messages: vec![Message::summary(text.clone())],
                    });
                    s.history.extend(kept);
                    s.native_window = None;
                }
                span_value([Message::new("assistant", Content::Text(text))])
            }
        };
        let variant = model.as_ref().and_then(|m| m.variant.as_deref());
        let provider = model
            .as_ref()
            .and_then(|m| m.provider_id.as_deref())
            .unwrap_or("unknown");
        let model = model
            .as_ref()
            .and_then(|m| m.id.as_deref())
            .unwrap_or("unknown");
        let metrics = Metrics::from_tokens(tokens.unwrap_or_default());
        vec![SpanOp::Insert(SpanRow {
            span_id: ids::span_id(&self.daemon_session_id, &format!("llm:{sid}:{id}")),
            root_span_id: s.trace_root.clone(),
            parent_span_ids: vec![parent],
            turn_span_id: s.owner_turn.clone().or_else(|| s.turn.clone()),
            name: format!("{provider}/{model}"),
            span_type: SpanType::Llm,
            start_ms: Some(started),
            end_ms: Some(ts),
            input: Some(span_value(input)),
            output: Some(output),
            metadata: Some(span_value(LlmMetadata {
                provider,
                model,
                message_id: id,
                variant,
            })),
            metrics: Some(span_value(metrics)),
            error,
            tags: Some(vec!["compaction".into()]),
            ..Default::default()
        })]
    }
}
fn normalize_context(context: Context) -> Vec<Message> {
    let mut messages = Vec::new();
    let system = context.system.join("\n\n");
    if !system.is_empty() {
        messages.push(Message::new("system", Content::Text(system)));
    }
    for message in context.messages {
        match message.content {
            MessageContent::Text(text) => {
                messages.push(Message::new(message.role, Content::Text(text)))
            }
            MessageContent::Other(content) => {
                messages.push(Message::new(message.role, Content::Dynamic(content)))
            }
            MessageContent::Parts(parts) => {
                let mut content = Vec::new();
                let mut calls = Vec::new();
                let mut single_text = None;
                for part in parts {
                    match part {
                        ContextPart::Known(KnownContextPart::Text { text }) => {
                            single_text = Some(text.clone());
                            content.push(ContentPart::text(text));
                        }
                        ContextPart::Known(KnownContextPart::Media {
                            media_type,
                            data,
                            media,
                            extra,
                        }) => {
                            let source = media.as_ref().and_then(|media| media.source.as_ref());
                            let mime = source
                                .and_then(|source| source.media_type.as_deref())
                                .or(media_type.as_deref());
                            let bytes = source
                                .and_then(|source| source.data.as_deref())
                                .or(data.as_deref());
                            if let Some((mime, bytes)) = mime
                                .zip(bytes)
                                .filter(|(mime, _)| mime.starts_with("image/"))
                            {
                                content.push(ContentPart::image(format!(
                                    "data:{mime};base64,{bytes}"
                                )));
                            } else {
                                content.push(ContentPart::Native(KnownContextPart::Media {
                                    media_type,
                                    data,
                                    media,
                                    extra,
                                }));
                            }
                        }
                        ContextPart::Known(KnownContextPart::ToolCall { id, name, input }) => {
                            calls.push(ToolCall::new(id, name, &input))
                        }
                        ContextPart::Known(KnownContextPart::ToolResult { id, result }) => {
                            let value = match serde_json::from_value::<Vec<ToolContent>>(
                                result.value.clone(),
                            ) {
                                Ok(parts) => Content::Parts(normalize_tool_content(parts)),
                                Err(_) => Content::Dynamic(result.value),
                            };
                            messages.push(Message::tool(id, value));
                        }
                        ContextPart::Other(value) => content.push(ContentPart::Dynamic(value)),
                    }
                }
                if message.role == "tool" {
                    continue;
                }
                let value = if content.is_empty() {
                    Content::Text(String::new())
                } else if content.len() == 1 && single_text.is_some() {
                    Content::Text(single_text.unwrap_or_default())
                } else {
                    Content::Parts(content)
                };
                let mut normalized = Message::new(message.role, value);
                normalized.tool_calls = calls;
                messages.push(normalized);
            }
        }
    }
    messages
}

fn normalize_tool_content(parts: Vec<ToolContent>) -> Vec<ContentPart> {
    parts
        .into_iter()
        .map(|part| match part {
            ToolContent::Known(KnownToolContent::File { mime, uri, .. })
                if mime.starts_with("image/") =>
            {
                ContentPart::image(uri)
            }
            other => ContentPart::Tool(other),
        })
        .collect()
}
