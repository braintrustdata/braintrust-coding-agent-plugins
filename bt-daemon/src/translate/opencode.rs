//! OpenCode translator. The JavaScript package forwards native hook payloads;
//! this module owns span construction, correlation, and recovery.

use super::git::GitMetadataCache;
use super::{AgentTranslator, SessionCtx, SpanOp, SpanRow, SpanType, TranslatorFactory};
use crate::ids;
use crate::wire::Envelope;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub struct OpenCodeTranslatorFactory {
    git: Arc<GitMetadataCache>,
}

impl OpenCodeTranslatorFactory {
    pub(super) fn new(git: Arc<GitMetadataCache>) -> Self {
        Self { git }
    }
}

impl TranslatorFactory for OpenCodeTranslatorFactory {
    fn source(&self) -> &str {
        "opencode"
    }

    fn create(&self, session_id: &str) -> Box<dyn AgentTranslator> {
        Box::new(OpenCodeTranslator {
            daemon_session_id: session_id.to_string(),
            sessions: HashMap::new(),
            git: self.git.clone(),
            last_ts_ms: 0,
        })
    }
}

#[derive(Default)]
struct NativeSession {
    root_span_id: String,
    effective_root_span_id: String,
    parent_session_id: Option<String>,
    current_turn_span_id: Option<String>,
    turn_number: u32,
    tool_call_count: u32,
    current_input: Option<String>,
    current_output: Option<String>,
    system_prompt: Option<String>,
    output_parts: HashMap<String, String>,
    reasoning_parts: HashMap<String, String>,
    tool_calls: HashMap<String, Vec<Value>>,
    tool_starts: HashMap<String, i64>,
    tool_args: HashMap<String, Value>,
    tool_outputs: HashMap<String, Value>,
    tool_errors: HashMap<String, String>,
    tool_message_ids: HashMap<String, String>,
    denied_tools: HashSet<String>,
    completed_messages: HashSet<String>,
}

struct OpenCodeTranslator {
    daemon_session_id: String,
    sessions: HashMap<String, NativeSession>,
    git: Arc<GitMetadataCache>,
    last_ts_ms: i64,
}

impl AgentTranslator for OpenCodeTranslator {
    fn handle(&mut self, event: &Envelope, ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        self.last_ts_ms = self.last_ts_ms.max(event.ts_ms);
        let mut ops = match event.event.as_str() {
            "session.created" => self.session_created(event, ctx),
            "chat.message" => self.chat_message(event, ctx),
            "experimental.chat.system.transform" => self.system_prompt(event),
            "message.part.updated" => self.part_updated(event),
            "message.updated" => self.message_updated(event),
            "tool.execute.before" => self.tool_before(event, ctx),
            "tool.execute.after" => self.tool_after(event),
            "permission.asked" | "permission.replied" => self.permission(event),
            "session.idle" => self.finish_session_event(event, false, None),
            "session.deleted" => self.finish_session_event(event, true, None),
            "session.error" => {
                let error = format_error(
                    event
                        .payload
                        .pointer("/properties/error")
                        .or_else(|| event.payload.get("error")),
                );
                self.finish_session_event(event, true, Some(error))
            }
            _ => Vec::new(),
        };
        let cwd = event
            .payload
            .get("directory")
            .and_then(Value::as_str)
            .or_else(|| event.payload.get("worktree").and_then(Value::as_str));
        self.git.enrich_rows(cwd, &mut ops);
        Ok(ops)
    }

    fn flush(&mut self, _ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        let now = self.last_ts_ms;
        let ids: Vec<String> = self.sessions.keys().cloned().collect();
        let mut ops = Vec::new();
        for id in ids {
            ops.extend(self.close(&id, now, true, None));
        }
        Ok(ops)
    }
}

impl OpenCodeTranslator {
    fn session_created(&mut self, event: &Envelope, ctx: &SessionCtx) -> Vec<SpanOp> {
        let info = event
            .payload
            .pointer("/properties/info")
            .or_else(|| event.payload.get("info"));
        let Some(native_id) = info
            .and_then(|v| v.get("id"))
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or_else(|| native_session_id(&event.payload))
        else {
            return Vec::new();
        };
        if self.sessions.contains_key(&native_id) {
            return Vec::new();
        }
        let parent_id = info
            .and_then(|v| v.get("parentID"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        self.open_session(&native_id, parent_id.as_deref(), info, event.ts_ms, ctx)
    }

    fn open_session(
        &mut self,
        native_id: &str,
        parent_id: Option<&str>,
        info: Option<&Value>,
        ts: i64,
        ctx: &SessionCtx,
    ) -> Vec<SpanOp> {
        if self.sessions.contains_key(native_id) {
            return Vec::new();
        }
        let root_span_id = ids::span_id(&self.daemon_session_id, &format!("session:{native_id}"));
        let (effective_root_span_id, parent_span_ids, name) = if let Some(parent_id) = parent_id {
            if let Some(parent) = self.sessions.get(parent_id) {
                let title = info
                    .and_then(|v| v.get("title"))
                    .and_then(Value::as_str)
                    .unwrap_or("Subagent");
                (
                    parent.effective_root_span_id.clone(),
                    parent.current_turn_span_id.clone().into_iter().collect(),
                    subagent_name(title),
                )
            } else {
                (root_span_id.clone(), Vec::new(), "Subagent".to_string())
            }
        } else {
            let external = ctx
                .config
                .as_ref()
                .map(|c| c.attached_span_ids())
                .unwrap_or_default();
            (
                external.1.unwrap_or_else(|| root_span_id.clone()),
                external.0.into_iter().collect(),
                "OpenCode".to_string(),
            )
        };
        let mut metadata = ctx
            .config
            .as_ref()
            .and_then(|c| c.additional_metadata.as_ref())
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        metadata.insert("session_id".into(), Value::String(native_id.to_string()));
        metadata.insert("source".into(), Value::String("opencode".into()));
        if let Some(parent) = parent_id {
            metadata.insert("parent_session_id".into(), Value::String(parent.into()));
            metadata.insert("is_subagent".into(), Value::Bool(true));
        }
        self.sessions.insert(
            native_id.to_string(),
            NativeSession {
                root_span_id: root_span_id.clone(),
                effective_root_span_id: effective_root_span_id.clone(),
                parent_session_id: parent_id.map(str::to_owned),
                ..Default::default()
            },
        );
        vec![SpanOp::Insert(SpanRow {
            span_id: root_span_id,
            root_span_id: effective_root_span_id,
            parent_span_ids,
            name,
            span_type: SpanType::Task,
            start_ms: Some(ts),
            metadata: Some(Value::Object(metadata)),
            ..Default::default()
        })]
    }

    fn chat_message(&mut self, event: &Envelope, ctx: &SessionCtx) -> Vec<SpanOp> {
        let Some(sid) = native_session_id(&event.payload) else {
            return Vec::new();
        };
        let mut ops = self.open_session(&sid, None, None, event.ts_ms, ctx);
        let Some(state) = self.sessions.get_mut(&sid) else {
            return ops;
        };
        if let Some(turn) = state.current_turn_span_id.take() {
            ops.push(SpanOp::Merge(SpanRow {
                span_id: turn,
                root_span_id: state.effective_root_span_id.clone(),
                end_ms: Some(event.ts_ms),
                output: state.current_output.take().map(Value::String),
                ..Default::default()
            }));
        }
        state.turn_number += 1;
        let turn_id = ids::span_id(
            &self.daemon_session_id,
            &format!("turn:{sid}:{}", state.turn_number),
        );
        let output = event.payload.get("output").unwrap_or(&event.payload);
        let input = output
            .get("parts")
            .and_then(Value::as_array)
            .map(|parts| {
                parts
                    .iter()
                    .filter(|p| p.get("type").and_then(Value::as_str) == Some("text"))
                    .filter_map(|p| p.get("text").and_then(Value::as_str))
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .unwrap_or_default();
        state.current_input = Some(input.clone());
        state.current_turn_span_id = Some(turn_id.clone());
        let model = event
            .payload
            .pointer("/input/model/modelID")
            .or_else(|| event.payload.pointer("/model/modelID"))
            .and_then(Value::as_str);
        let skills = explicit_skills(&input);
        ops.push(SpanOp::Insert(SpanRow {
            span_id: turn_id,
            root_span_id: state.effective_root_span_id.clone(),
            parent_span_ids: vec![state.root_span_id.clone()],
            name: format!("Turn {}", state.turn_number),
            span_type: SpanType::Task,
            start_ms: Some(event.ts_ms),
            input: (!input.is_empty()).then_some(Value::String(input)),
            metadata: Some(
                json!({"turn_number":state.turn_number,"model":model,"loaded_skill_names":skills}),
            ),
            ..Default::default()
        }));
        ops
    }

    fn system_prompt(&mut self, event: &Envelope) -> Vec<SpanOp> {
        if let Some(sid) = native_session_id(&event.payload) {
            if let Some(state) = self.sessions.get_mut(&sid) {
                state.system_prompt = event
                    .payload
                    .pointer("/output/system")
                    .and_then(Value::as_array)
                    .map(|p| {
                        p.iter()
                            .filter_map(Value::as_str)
                            .collect::<Vec<_>>()
                            .join("\n\n")
                    });
            }
        }
        Vec::new()
    }

    fn part_updated(&mut self, event: &Envelope) -> Vec<SpanOp> {
        let part = event
            .payload
            .pointer("/properties/part")
            .or_else(|| event.payload.get("part"));
        let Some(part) = part else { return Vec::new() };
        let Some(sid) = part.get("sessionID").and_then(Value::as_str) else {
            return Vec::new();
        };
        let Some(state) = self.sessions.get_mut(sid) else {
            return Vec::new();
        };
        let message_id = part.get("messageID").and_then(Value::as_str).unwrap_or("");
        match part.get("type").and_then(Value::as_str) {
            Some("text") => {
                if let Some(text) = part.get("text").and_then(Value::as_str) {
                    state.output_parts.insert(message_id.into(), text.into());
                    if part.pointer("/time/end").is_some() {
                        state.current_output = Some(text.into());
                    }
                }
            }
            Some("reasoning") => {
                if let Some(text) = part.get("text").and_then(Value::as_str) {
                    state.reasoning_parts.insert(message_id.into(), text.into());
                }
            }
            Some("tool") => {
                let call_id = part.get("callID").and_then(Value::as_str).unwrap_or("");
                let tool = part.get("tool").and_then(Value::as_str).unwrap_or("tool");
                if let Some(input) = part.pointer("/state/input") {
                    let call = json!({"id":call_id,"type":"function","function":{"name":tool,"arguments":serde_json::to_string(input).unwrap_or_default()}});
                    let calls = state.tool_calls.entry(message_id.into()).or_default();
                    if let Some(i) = calls
                        .iter()
                        .position(|v| v.get("id").and_then(Value::as_str) == Some(call_id))
                    {
                        calls[i] = call
                    } else {
                        calls.push(call)
                    };
                    state
                        .tool_message_ids
                        .insert(call_id.into(), message_id.into());
                }
                match part.pointer("/state/status").and_then(Value::as_str) {
                    Some("completed") => {
                        if let Some(v) = part.pointer("/state/output") {
                            state.tool_outputs.insert(call_id.into(), v.clone());
                        }
                    }
                    Some("error") => {
                        state
                            .tool_errors
                            .insert(call_id.into(), format_error(part.pointer("/state/error")));
                    }
                    _ => {}
                }
            }
            _ => {}
        }
        Vec::new()
    }

    fn message_updated(&mut self, event: &Envelope) -> Vec<SpanOp> {
        let info = event
            .payload
            .pointer("/properties/info")
            .or_else(|| event.payload.get("info"));
        let Some(info) = info else { return Vec::new() };
        if info.get("role").and_then(Value::as_str) != Some("assistant")
            || info.pointer("/time/completed").is_none()
        {
            return Vec::new();
        }
        let Some(sid) = info.get("sessionID").and_then(Value::as_str) else {
            return Vec::new();
        };
        let Some(mid) = info.get("id").and_then(Value::as_str) else {
            return Vec::new();
        };
        let Some(state) = self.sessions.get_mut(sid) else {
            return Vec::new();
        };
        if !state.completed_messages.insert(mid.into()) {
            return Vec::new();
        }
        let Some(turn) = state.current_turn_span_id.clone() else {
            return Vec::new();
        };
        let cache_read = num(info, "/tokens/cache/read");
        let cache_write = num(info, "/tokens/cache/write");
        let prompt = num(info, "/tokens/input") + cache_read + cache_write;
        let completion = num(info, "/tokens/output");
        let reasoning = num(info, "/tokens/reasoning");
        let mut assistant = json!({"role":"assistant","content":state.output_parts.get(mid).cloned().unwrap_or_default()});
        if let Some(calls) = state.tool_calls.get(mid) {
            assistant["tool_calls"] = Value::Array(calls.clone())
        }
        if let Some(reason) = state.reasoning_parts.get(mid) {
            assistant["reasoning"] = json!([{"id":"reasoning","content":reason}])
        }
        let mut input = Vec::new();
        if let Some(system) = &state.system_prompt {
            input.push(json!({"role":"system","content":system}))
        }
        if let Some(user) = &state.current_input {
            input.push(json!({"role":"user","content":user}))
        }
        let provider = info
            .get("providerID")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let model = info
            .get("modelID")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        vec![SpanOp::Insert(SpanRow {
            span_id: ids::span_id(&self.daemon_session_id, &format!("llm:{sid}:{mid}")),
            root_span_id: state.effective_root_span_id.clone(),
            parent_span_ids: vec![turn],
            name: format!("{provider}/{model}"),
            span_type: SpanType::Llm,
            start_ms: info
                .pointer("/time/created")
                .and_then(Value::as_i64)
                .or(Some(event.ts_ms)),
            end_ms: info
                .pointer("/time/completed")
                .and_then(Value::as_i64)
                .or(Some(event.ts_ms)),
            input: Some(Value::Array(input)),
            output: Some(Value::Array(vec![assistant])),
            metadata: Some(json!({"model":model,"provider":provider,"message_id":mid})),
            metrics: Some(
                json!({"prompt_tokens":prompt,"completion_tokens":completion,"tokens":prompt+completion+reasoning,"prompt_cached_tokens":cache_read,"prompt_cache_creation_tokens":cache_write,"reasoning_tokens":reasoning}),
            ),
            error: info.get("error").map(|e| format_error(Some(e))),
            ..Default::default()
        })]
    }

    fn tool_before(&mut self, event: &Envelope, _ctx: &SessionCtx) -> Vec<SpanOp> {
        let Some(sid) = native_session_id(&event.payload) else {
            return vec![];
        };
        let Some(call) = event
            .payload
            .pointer("/input/callID")
            .or_else(|| event.payload.get("callID"))
            .and_then(Value::as_str)
        else {
            return vec![];
        };
        if let Some(s) = self.sessions.get_mut(&sid) {
            s.tool_starts.insert(call.into(), event.ts_ms);
            if let Some(a) = event.payload.pointer("/output/args") {
                s.tool_args.insert(call.into(), a.clone());
            }
        }
        vec![]
    }

    fn tool_after(&mut self, event: &Envelope) -> Vec<SpanOp> {
        let Some(sid) = native_session_id(&event.payload) else {
            return vec![];
        };
        let call = event
            .payload
            .pointer("/input/callID")
            .or_else(|| event.payload.get("callID"))
            .and_then(Value::as_str)
            .unwrap_or("");
        let tool = event
            .payload
            .pointer("/input/tool")
            .or_else(|| event.payload.get("tool"))
            .and_then(Value::as_str)
            .unwrap_or("tool");
        let Some(s) = self.sessions.get_mut(&sid) else {
            return vec![];
        };
        if s.denied_tools.remove(call) {
            return vec![];
        }
        let Some(turn) = s.current_turn_span_id.clone() else {
            return vec![];
        };
        s.tool_call_count += 1;
        let output = s
            .tool_outputs
            .remove(call)
            .or_else(|| event.payload.pointer("/result/output").cloned())
            .or_else(|| event.payload.get("output").cloned());
        let error = s.tool_errors.remove(call);
        let args = s.tool_args.remove(call);
        let name = if tool == "skill" {
            args.as_ref()
                .and_then(|v| v.get("name"))
                .and_then(Value::as_str)
                .map(|n| format!("skill: {n}"))
                .unwrap_or_else(|| "skill".into())
        } else {
            event
                .payload
                .pointer("/result/title")
                .and_then(Value::as_str)
                .unwrap_or(tool)
                .to_string()
        };
        let mut metadata = json!({"tool_name":tool,"call_id":call,"tool_outcome":if error.is_some(){"error"}else{"success"}});
        if tool == "skill" {
            metadata["tool_kind"] = json!("skill");
            metadata["skill_name"] = args
                .as_ref()
                .and_then(|v| v.get("name"))
                .cloned()
                .unwrap_or(Value::Null)
        }
        vec![SpanOp::Insert(SpanRow {
            span_id: ids::span_id(&self.daemon_session_id, &format!("tool:{sid}:{call}")),
            root_span_id: s.effective_root_span_id.clone(),
            parent_span_ids: vec![turn],
            name,
            span_type: SpanType::Tool,
            start_ms: s.tool_starts.remove(call).or(Some(event.ts_ms)),
            end_ms: Some(event.ts_ms),
            input: args,
            output,
            error,
            metadata: Some(metadata),
            ..Default::default()
        })]
    }

    fn permission(&mut self, event: &Envelope) -> Vec<SpanOp> {
        let props = event.payload.get("properties").unwrap_or(&event.payload);
        let Some(sid) = native_session_id(props) else {
            return vec![];
        };
        let call = props
            .get("callID")
            .or_else(|| props.get("id"))
            .and_then(Value::as_str)
            .unwrap_or("");
        let reply = props
            .get("response")
            .or_else(|| props.get("status"))
            .and_then(Value::as_str)
            .unwrap_or("");
        if matches!(reply, "reject" | "denied" | "deny") {
            let Some(s) = self.sessions.get_mut(&sid) else {
                return vec![];
            };
            let Some(turn) = s.current_turn_span_id.clone() else {
                return vec![];
            };
            s.denied_tools.insert(call.into());
            let tool = props.get("tool").and_then(Value::as_str).unwrap_or("tool");
            return vec![SpanOp::Insert(SpanRow {
                span_id: ids::span_id(&self.daemon_session_id, &format!("tool:{sid}:{call}")),
                root_span_id: s.effective_root_span_id.clone(),
                parent_span_ids: vec![turn],
                name: tool.into(),
                span_type: SpanType::Tool,
                start_ms: s.tool_starts.remove(call).or(Some(event.ts_ms)),
                end_ms: Some(event.ts_ms),
                input: s.tool_args.remove(call),
                metadata: Some(
                    json!({"tool_name":tool,"call_id":call,"tool_approval":"denied","tool_outcome":"denied"}),
                ),
                error: Some("Permission denied".into()),
                ..Default::default()
            })];
        }
        vec![]
    }

    fn finish_session_event(
        &mut self,
        event: &Envelope,
        close_root: bool,
        error: Option<String>,
    ) -> Vec<SpanOp> {
        let Some(sid) = native_session_id(&event.payload) else {
            return vec![];
        };
        self.close(&sid, event.ts_ms, close_root, error)
    }
    fn close(
        &mut self,
        sid: &str,
        ts: i64,
        close_root: bool,
        error: Option<String>,
    ) -> Vec<SpanOp> {
        let Some(mut s) = self.sessions.remove(sid) else {
            return vec![];
        };
        let mut ops = vec![];
        if let Some(turn) = s.current_turn_span_id.take() {
            ops.push(SpanOp::Merge(SpanRow {
                span_id: turn,
                root_span_id: s.effective_root_span_id.clone(),
                end_ms: Some(ts),
                output: s.current_output.take().map(Value::String),
                error: error.clone(),
                ..Default::default()
            }));
        }
        if close_root || s.parent_session_id.is_some() {
            ops.push(SpanOp::Merge(SpanRow {
                span_id: s.root_span_id,
                root_span_id: s.effective_root_span_id,
                end_ms: Some(ts),
                metadata: Some(
                    json!({"total_turns":s.turn_number,"total_tool_calls":s.tool_call_count}),
                ),
                error,
                ..Default::default()
            }));
        } else {
            self.sessions.insert(sid.into(), s);
        }
        ops
    }
}

fn native_session_id(v: &Value) -> Option<String> {
    v.pointer("/input/sessionID")
        .or_else(|| v.get("sessionID"))
        .or_else(|| v.pointer("/properties/sessionID"))
        .or_else(|| v.pointer("/properties/info/id"))
        .or_else(|| v.pointer("/part/sessionID"))
        .and_then(Value::as_str)
        .map(str::to_owned)
}
fn num(v: &Value, p: &str) -> i64 {
    v.pointer(p).and_then(Value::as_i64).unwrap_or(0)
}
fn format_error(v: Option<&Value>) -> String {
    let Some(v) = v else {
        return "UnknownError".into();
    };
    if let Some(s) = v.as_str() {
        return s.lines().next().unwrap_or(s).into();
    }
    let name = v
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("UnknownError");
    let msg = v
        .pointer("/data/message")
        .or_else(|| v.get("message"))
        .and_then(Value::as_str)
        .unwrap_or(name);
    format!("{msg}\n\ntype: {name}")
}
fn subagent_name(title: &str) -> String {
    let Some((description, tail)) = title.split_once(" (@") else {
        return title.into();
    };
    let agent = tail.strip_suffix(" subagent)").unwrap_or(tail);
    format!("{agent}: {description}")
}
fn explicit_skills(input: &str) -> Vec<String> {
    input
        .split_whitespace()
        .filter_map(|s| {
            s.strip_prefix("/skills:")
                .or_else(|| s.strip_prefix("/skills"))
        })
        .map(|s| {
            s.trim_matches(|c: char| matches!(c, ',' | ')' | '.' | ';'))
                .to_string()
        })
        .filter(|s| !s.is_empty())
        .collect()
}
