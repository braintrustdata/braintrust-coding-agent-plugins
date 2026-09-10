//! Muse Code hook translator.
//!
//! Muse hooks deliberately remain a thin, fail-open transport.  This state
//! machine only relies on fields observed in Muse 1.1's documented hook
//! payloads.  An MSP/export reader can feed the same event names later without
//! changing trace construction.

use super::git::GitMetadataCache;
use super::{
    local_username, AgentTranslator, SessionCtx, SpanOp, SpanRow, SpanType, TranslatorFactory,
};
use crate::ids;
use crate::wire::Envelope;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

pub struct MuseTranslatorFactory {
    git: Arc<GitMetadataCache>,
}
impl MuseTranslatorFactory {
    pub(super) fn new(git: Arc<GitMetadataCache>) -> Self {
        Self { git }
    }
}
impl TranslatorFactory for MuseTranslatorFactory {
    fn source(&self) -> &str {
        "muse"
    }
    fn create(&self, session_id: &str) -> Box<dyn AgentTranslator> {
        Box::new(MuseTranslator::new(session_id, self.git.clone()))
    }
}

#[derive(Clone)]
struct OpenOp {
    id: String,
    parents: Vec<String>,
    name: String,
    start_ms: i64,
    input: Value,
    kind: SpanType,
}

struct MuseTranslator {
    session_id: String,
    root_id: String,
    effective_root_id: String,
    external_parent: Option<String>,
    opened: bool,
    turns: HashMap<String, OpenOp>,
    llms: HashMap<String, OpenOp>,
    tools: HashMap<String, OpenOp>,
    special: HashMap<String, OpenOp>,
    last_ts: i64,
    git: Arc<GitMetadataCache>,
}

impl MuseTranslator {
    fn new(session_id: &str, git: Arc<GitMetadataCache>) -> Self {
        Self {
            session_id: session_id.into(),
            root_id: ids::span_id(session_id, "root"),
            effective_root_id: String::new(),
            external_parent: None,
            opened: false,
            turns: HashMap::new(),
            llms: HashMap::new(),
            tools: HashMap::new(),
            special: HashMap::new(),
            last_ts: 0,
            git,
        }
    }
    fn ensure_root(&mut self, e: &Envelope, ctx: &SessionCtx) -> Vec<SpanOp> {
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
        self.effective_root_id = attached
            .1
            .or_else(|| self.external_parent.clone())
            .unwrap_or_else(|| self.root_id.clone());
        let mut metadata = ctx
            .config
            .as_ref()
            .and_then(|c| c.additional_metadata.as_ref())
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        metadata.insert("source".into(), json!("muse"));
        metadata.insert("session_id".into(), json!(ctx.session_id));
        metadata.insert("username".into(), json!(local_username()));
        metadata.insert("muse_version".into(), json!(e.source_version));
        for key in [
            "cwd",
            "model",
            "permission_mode",
            "trajectory_id",
            "root_session_id",
        ] {
            if let Some(value) = e.payload.get(key) {
                metadata.insert(key.into(), value.clone());
            }
        }
        vec![SpanOp::Insert(SpanRow {
            span_id: self.root_id.clone(),
            root_span_id: self.effective_root_id.clone(),
            parent_span_ids: self.external_parent.clone().into_iter().collect(),
            name: "Muse Code".into(),
            span_type: SpanType::Task,
            start_ms: Some(e.ts_ms),
            metadata: Some(Value::Object(metadata)),
            ..Default::default()
        })]
    }
    fn close(
        root_span_id: &str,
        map: &mut HashMap<String, OpenOp>,
        key: &str,
        ts: i64,
        output: Option<Value>,
        metrics: Option<Value>,
        error: Option<String>,
    ) -> Vec<SpanOp> {
        let Some(open) = map.remove(key) else {
            return Vec::new();
        };
        vec![SpanOp::Insert(SpanRow {
            span_id: open.id,
            root_span_id: root_span_id.into(),
            parent_span_ids: open.parents,
            name: open.name,
            span_type: open.kind,
            start_ms: Some(open.start_ms),
            end_ms: Some(ts),
            input: Some(open.input),
            output,
            metrics,
            error,
            ..Default::default()
        })]
    }
    fn close_all(&mut self, ts: i64, error: Option<&str>) -> Vec<SpanOp> {
        let mut out = Vec::new();
        for map in [
            &mut self.llms,
            &mut self.tools,
            &mut self.special,
            &mut self.turns,
        ] {
            for (_, open) in std::mem::take(map) {
                out.push(SpanOp::Insert(SpanRow {
                    span_id: open.id,
                    root_span_id: self.effective_root_id.clone(),
                    parent_span_ids: open.parents,
                    name: open.name,
                    span_type: open.kind,
                    start_ms: Some(open.start_ms),
                    end_ms: Some(ts),
                    input: Some(open.input),
                    error: error.map(str::to_owned),
                    ..Default::default()
                }));
            }
        }
        out
    }
}

impl AgentTranslator for MuseTranslator {
    fn handle(&mut self, e: &Envelope, ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        self.last_ts = self.last_ts.max(e.ts_ms);
        let mut out = self.ensure_root(e, ctx);
        let p = &e.payload;
        let turn_id = p.get("turn_id").and_then(Value::as_str);
        match e.event.as_str() {
            "UserPromptSubmit" => {
                let key = turn_id.unwrap_or("unknown");
                if !self.turns.contains_key(key) {
                    self.turns.insert(
                        key.into(),
                        OpenOp {
                            id: ids::span_id(&self.session_id, &format!("turn:{key}")),
                            parents: vec![self.root_id.clone()],
                            name: "Turn".into(),
                            start_ms: e.ts_ms,
                            input: p.get("prompt").cloned().unwrap_or(Value::Null),
                            kind: SpanType::Task,
                        },
                    );
                }
            }
            "PreLLMCall" => {
                let key = p
                    .get("request_id")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown");
                self.llms.entry(key.into()).or_insert_with(|| OpenOp { id: ids::span_id(&self.session_id, &format!("llm:{key}")), parents: vec![turn_id.and_then(|id| self.turns.get(id)).map(|op| op.id.clone()).unwrap_or_else(|| self.root_id.clone())], name: "llm".into(), start_ms: e.ts_ms, input: json!({"messages": p.get("messages"), "tools": p.get("tools"), "provider": p.get("provider")}), kind: SpanType::Llm });
            }
            "PostLLMCall" => {
                let key = p
                    .get("request_id")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown");
                let metrics = p.get("usage").cloned();
                let output = p.get("output_text_preview").cloned();
                let error = p.get("error").and_then(Value::as_str).map(str::to_owned);
                out.extend(Self::close(
                    &self.effective_root_id,
                    &mut self.llms,
                    key,
                    e.ts_ms,
                    output,
                    metrics,
                    error,
                ));
            }
            "PreToolUse" => {
                let key = p
                    .get("tool_use_id")
                    .or_else(|| p.get("tool_call_id"))
                    .and_then(Value::as_str)
                    .unwrap_or("unknown");
                self.tools.entry(key.into()).or_insert_with(|| OpenOp {
                    id: ids::span_id(&self.session_id, &format!("tool:{key}")),
                    parents: vec![turn_id
                        .and_then(|id| self.turns.get(id))
                        .map(|op| op.id.clone())
                        .unwrap_or_else(|| self.root_id.clone())],
                    name: p
                        .get("tool_name")
                        .and_then(Value::as_str)
                        .unwrap_or("tool")
                        .into(),
                    start_ms: e.ts_ms,
                    input: p
                        .get("tool_input")
                        .or_else(|| p.get("input"))
                        .cloned()
                        .unwrap_or(Value::Null),
                    kind: SpanType::Tool,
                });
            }
            "PostToolUse" => {
                let key = p
                    .get("tool_use_id")
                    .or_else(|| p.get("tool_call_id"))
                    .and_then(Value::as_str)
                    .unwrap_or("unknown");
                out.extend(Self::close(
                    &self.effective_root_id,
                    &mut self.tools,
                    key,
                    e.ts_ms,
                    p.get("tool_response").or_else(|| p.get("output")).cloned(),
                    None,
                    p.get("error").and_then(Value::as_str).map(str::to_owned),
                ));
            }
            "PermissionRequest" | "PreCompact" | "SubagentStart" => {
                let key = format!(
                    "{}:{}",
                    e.event,
                    p.get("request_id")
                        .or_else(|| p.get("subagent_id"))
                        .and_then(Value::as_str)
                        .unwrap_or("unknown")
                );
                self.special.entry(key.clone()).or_insert_with(|| OpenOp {
                    id: ids::span_id(&self.session_id, &format!("special:{key}")),
                    parents: vec![turn_id
                        .and_then(|id| self.turns.get(id))
                        .map(|op| op.id.clone())
                        .unwrap_or_else(|| self.root_id.clone())],
                    name: e.event.clone(),
                    start_ms: e.ts_ms,
                    input: p.clone(),
                    kind: SpanType::Task,
                });
            }
            "PostCompact" | "SubagentStop" => {
                let start = if e.event == "PostCompact" {
                    "PreCompact"
                } else {
                    "SubagentStart"
                };
                let key = format!(
                    "{}:{}",
                    start,
                    p.get("request_id")
                        .or_else(|| p.get("subagent_id"))
                        .and_then(Value::as_str)
                        .unwrap_or("unknown")
                );
                out.extend(Self::close(
                    &self.effective_root_id,
                    &mut self.special,
                    &key,
                    e.ts_ms,
                    Some(p.clone()),
                    None,
                    None,
                ));
            }
            "Stop" => {
                if let Some(id) = turn_id {
                    out.extend(Self::close(
                        &self.effective_root_id,
                        &mut self.turns,
                        id,
                        e.ts_ms,
                        p.get("last_assistant_message").cloned(),
                        None,
                        None,
                    ));
                }
            }
            "SessionEnd" => {
                out.extend(self.close_all(e.ts_ms, None));
                if self.opened {
                    out.push(SpanOp::Insert(SpanRow {
                        span_id: self.root_id.clone(),
                        root_span_id: self.effective_root_id.clone(),
                        end_ms: Some(e.ts_ms),
                        ..Default::default()
                    }));
                    self.opened = false;
                }
            }
            _ => {}
        }
        self.git
            .enrich_rows(p.get("cwd").and_then(Value::as_str), &mut out);
        Ok(out)
    }
    fn finalize(&mut self, _ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        let mut out = self.close_all(self.last_ts, Some("Interrupted before completion"));
        if self.opened {
            out.push(SpanOp::Insert(SpanRow {
                span_id: self.root_id.clone(),
                root_span_id: self.effective_root_id.clone(),
                end_ms: Some(self.last_ts),
                error: Some("Interrupted before completion".into()),
                ..Default::default()
            }));
            self.opened = false;
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::translate::Registry;
    use crate::wire::Envelope;
    fn event(name: &str, payload: Value) -> Envelope {
        Envelope {
            source: "muse".into(),
            source_version: Some("1.1.1".into()),
            plugin_version: None,
            session_id: "session-1".into(),
            event: name.into(),
            ts_ms: 1000,
            managed_run_id: None,
            capture: None,
            payload,
            route: None,
            config: None,
        }
    }
    #[test]
    fn registry_and_llm_pairing_are_stable() {
        let registry = Registry::default_agents();
        assert_eq!(registry.canonical_source("muse-code"), Some("muse"));
        let mut translator = registry.create("muse", "session-1");
        let ctx = SessionCtx {
            session_id: "session-1".into(),
            config: None,
        };
        translator
            .handle(
                &event(
                    "UserPromptSubmit",
                    json!({"session_id":"session-1","turn_id":"turn-1","prompt":"hello"}),
                ),
                &ctx,
            )
            .unwrap();
        translator.handle(&event("PreLLMCall", json!({"session_id":"session-1","turn_id":"turn-1","request_id":"req-1","messages":[]})), &ctx).unwrap();
        let rows = translator.handle(&event("PostLLMCall", json!({"session_id":"session-1","turn_id":"turn-1","request_id":"req-1","output_text_preview":"hi","usage":{"input_tokens":1}})), &ctx).unwrap();
        assert!(rows.iter().any(|op| matches!(op, SpanOp::Insert(row) if row.name == "llm" && row.output == Some(json!("hi")))));
    }
}
