//! Muse Code hook translator.
//!
//! Muse hooks remain a thin transport. The session, turn and model payloads
//! used here were captured from Muse Code 1.1.1-R2514.1. Import feeds the
//! same event names through this translator.

use super::git::GitMetadataCache;
use super::{
    local_username, root_tags, AgentTranslator, SessionCtx, SpanOp, SpanRow, SpanType,
    TranslatorFactory,
};
use crate::ids;
use crate::wire::Envelope;
use serde::{Deserialize, Deserializer};
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};
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
}

#[derive(Default, Deserialize)]
struct HookIds {
    #[serde(default, deserialize_with = "string_or_none")]
    turn_id: Option<String>,
    #[serde(default, deserialize_with = "string_or_none")]
    request_id: Option<String>,
}

fn string_or_none<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Value::deserialize(deserializer)?
        .as_str()
        .filter(|value| !value.is_empty())
        .map(str::to_owned))
}

impl HookIds {
    fn turn(&self) -> Option<&str> {
        self.turn_id.as_deref().filter(|id| !id.is_empty())
    }
    fn request(&self) -> Option<&str> {
        self.request_id.as_deref().filter(|id| !id.is_empty())
    }
}

struct MuseTranslator {
    session_id: String,
    root_id: String,
    effective_root_id: String,
    external_parent: Option<String>,
    opened: bool,
    session_metadata: Map<String, Value>,
    session_version: Option<String>,
    turns: HashMap<String, OpenOp>,
    llms: HashMap<String, OpenOp>,
    closed_turns: HashSet<String>,
    closed_llms: HashSet<String>,
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
            session_metadata: Map::new(),
            session_version: None,
            turns: HashMap::new(),
            llms: HashMap::new(),
            closed_turns: HashSet::new(),
            closed_llms: HashSet::new(),
            last_ts: 0,
            git,
        }
    }
    fn remember_session(&mut self, e: &Envelope) {
        if let Some(version) = &e.source_version {
            self.session_version = Some(version.clone());
        }
        for key in [
            "cwd",
            "model",
            "permission_mode",
            "trajectory_id",
            "root_session_id",
        ] {
            if let Some(value) = e.payload.get(key) {
                self.session_metadata.insert(key.into(), value.clone());
            }
        }
    }

    fn starts_trace(event: &str) -> bool {
        matches!(event, "UserPromptSubmit" | "PreLLMCall")
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
        if let Some(version) = &self.session_version {
            metadata.insert("muse_version".into(), json!(version));
        }
        if let Some(version) = &e.plugin_version {
            metadata.insert("adapter_version".into(), json!(version));
        }
        for (key, value) in &self.session_metadata {
            metadata.insert(key.clone(), value.clone());
        }
        vec![SpanOp::Insert(SpanRow {
            span_id: self.root_id.clone(),
            root_span_id: self.effective_root_id.clone(),
            parent_span_ids: self.external_parent.clone().into_iter().collect(),
            name: "Muse Code".into(),
            span_type: SpanType::Task,
            start_ms: Some(e.ts_ms),
            metadata: Some(Value::Object(metadata)),
            tags: root_tags(ctx),
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
        vec![SpanOp::Merge(SpanRow {
            span_id: open.id,
            root_span_id: root_span_id.into(),
            parent_span_ids: open.parents,
            end_ms: Some(ts),
            output,
            metrics,
            error,
            ..Default::default()
        })]
    }
    fn close_all(&mut self, ts: i64, error: Option<&str>) -> Vec<SpanOp> {
        let mut out = Vec::new();
        for (map, closed) in [
            (&mut self.llms, &mut self.closed_llms),
            (&mut self.turns, &mut self.closed_turns),
        ] {
            for (key, open) in std::mem::take(map) {
                closed.insert(key);
                out.push(SpanOp::Merge(SpanRow {
                    span_id: open.id,
                    root_span_id: self.effective_root_id.clone(),
                    parent_span_ids: open.parents,
                    end_ms: Some(ts),
                    error: error.map(str::to_owned),
                    ..Default::default()
                }));
            }
        }
        out
    }

    fn parent(&self, ids: &HookIds) -> String {
        ids.turn()
            .and_then(|id| self.turns.get(id))
            .map(|op| op.id.clone())
            .unwrap_or_else(|| self.root_id.clone())
    }

    fn insert_open(map: &mut HashMap<String, OpenOp>, key: &str, row: SpanRow) -> Vec<SpanOp> {
        if map.contains_key(key) {
            return Vec::new();
        }
        map.insert(
            key.into(),
            OpenOp {
                id: row.span_id.clone(),
                parents: row.parent_span_ids.clone(),
            },
        );
        vec![SpanOp::Insert(row)]
    }

    fn unpaired(&self, e: &Envelope, reason: &str) -> SpanOp {
        // A malformed event must not share an `unknown` ID with another event.
        let identity = format!("unpaired:{}:{}:{}", e.event, e.ts_ms, e.payload);
        SpanOp::Insert(SpanRow {
            span_id: ids::span_id(&self.session_id, &identity),
            root_span_id: self.effective_root_id.clone(),
            parent_span_ids: vec![self.root_id.clone()],
            name: e.event.clone(),
            span_type: SpanType::Task,
            start_ms: Some(e.ts_ms),
            end_ms: Some(e.ts_ms),
            metadata: Some(json!({"unpaired": true})),
            error: Some(reason.into()),
            ..Default::default()
        })
    }
}

impl AgentTranslator for MuseTranslator {
    fn handle(&mut self, e: &Envelope, ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        self.last_ts = self.last_ts.max(e.ts_ms);
        self.remember_session(e);
        let ids = match serde_json::from_value::<HookIds>(e.payload.clone()) {
            Ok(ids) => ids,
            Err(error) => {
                // Unknown or malformed optional fields must not terminate a session.
                tracing::warn!(source = "muse", event = %e.event, %error, "invalid Muse hook identifiers");
                HookIds::default()
            }
        };
        if (e.event == "UserPromptSubmit"
            && ids.turn().is_some_and(|id| self.closed_turns.contains(id)))
            || (e.event == "PreLLMCall"
                && ids
                    .request()
                    .is_some_and(|id| self.closed_llms.contains(id)))
        {
            return Ok(Vec::new());
        }
        let mut out = if Self::starts_trace(&e.event) {
            self.ensure_root(e, ctx)
        } else {
            Vec::new()
        };
        let p = &e.payload;
        match e.event.as_str() {
            "UserPromptSubmit" => {
                if let Some(id) = ids.turn() {
                    out.extend(Self::insert_open(
                        &mut self.turns,
                        id,
                        SpanRow {
                            span_id: ids::span_id(&self.session_id, &format!("turn:{id}")),
                            root_span_id: self.effective_root_id.clone(),
                            parent_span_ids: vec![self.root_id.clone()],
                            name: "Turn".into(),
                            span_type: SpanType::Task,
                            start_ms: Some(e.ts_ms),
                            input: p.get("prompt").cloned(),
                            ..Default::default()
                        },
                    ));
                } else {
                    out.push(self.unpaired(e, "Muse UserPromptSubmit has no turn_id"));
                }
            }
            "PreLLMCall" => {
                if let Some(id) = ids.request() {
                    let mut metadata = Map::new();
                    for key in [
                        "model",
                        "provider",
                        "step",
                        "attempt",
                        "message_count",
                        "tool_count",
                    ] {
                        if let Some(value) = p.get(key) {
                            metadata.insert(key.into(), value.clone());
                        }
                    }
                    let input = json!({"messages": p.get("messages"), "tools": p.get("tools")});
                    let parent = self.parent(&ids);
                    out.extend(Self::insert_open(
                        &mut self.llms,
                        id,
                        SpanRow {
                            span_id: ids::span_id(&self.session_id, &format!("llm:{id}")),
                            root_span_id: self.effective_root_id.clone(),
                            parent_span_ids: vec![parent],
                            name: p
                                .get("model")
                                .and_then(Value::as_str)
                                .filter(|s| !s.is_empty() && *s != "unknown")
                                .unwrap_or("llm")
                                .into(),
                            span_type: SpanType::Llm,
                            start_ms: Some(e.ts_ms),
                            input: Some(input),
                            metadata: Some(Value::Object(metadata)),
                            ..Default::default()
                        },
                    ));
                } else {
                    out.push(self.unpaired(e, "Muse PreLLMCall has no request_id"));
                }
            }
            "PostLLMCall" => {
                if let Some(id) = ids.request() {
                    let error = event_error(p);
                    let closed = Self::close(
                        &self.effective_root_id,
                        &mut self.llms,
                        id,
                        e.ts_ms,
                        p.get("output_text_preview").cloned(),
                        p.get("usage").map(usage_metrics),
                        error,
                    );
                    if !closed.is_empty() {
                        self.closed_llms.insert(id.into());
                        out.extend(closed);
                    }
                } else if self.opened {
                    out.push(self.unpaired(e, "Muse PostLLMCall has no request_id"));
                }
            }
            "Stop" => {
                if let Some(id) = ids.turn() {
                    let closed = Self::close(
                        &self.effective_root_id,
                        &mut self.turns,
                        id,
                        e.ts_ms,
                        p.get("last_assistant_message").cloned(),
                        None,
                        event_error(p),
                    );
                    if !closed.is_empty() {
                        self.closed_turns.insert(id.into());
                        out.extend(closed);
                    }
                } else if self.opened {
                    out.push(self.unpaired(e, "Muse Stop has no turn_id"));
                }
            }
            "SessionEnd" => {
                out.extend(self.close_all(e.ts_ms, Some("Interrupted before completion")));
                if self.opened {
                    out.push(SpanOp::Merge(SpanRow {
                        span_id: self.root_id.clone(),
                        root_span_id: self.effective_root_id.clone(),
                        end_ms: Some(e.ts_ms),
                        metadata: p.get("reason").map(|reason| json!({"end_reason":reason})),
                        error: event_error(p),
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
            out.push(SpanOp::Merge(SpanRow {
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

fn event_error(payload: &Value) -> Option<String> {
    payload
        .get("error")
        .and_then(|error| {
            error
                .as_str()
                .map(str::to_owned)
                .or_else(|| (!error.is_null()).then(|| error.to_string()))
        })
        .or_else(|| {
            payload
                .get("status")
                .and_then(Value::as_str)
                .filter(|status| !matches!(*status, "success" | "completed"))
                .map(|status| format!("Muse operation {status}"))
        })
}

fn usage_metrics(usage: &Value) -> Value {
    let mut metrics = Map::new();
    for (native, standard) in [
        ("input_tokens", "prompt_tokens"),
        ("output_tokens", "completion_tokens"),
        ("cached_tokens", "prompt_cached_tokens"),
        ("reasoning_tokens", "completion_reasoning_tokens"),
    ] {
        if let Some(value) = usage.get(native).and_then(Value::as_u64) {
            metrics.insert(standard.into(), json!(value));
        }
    }
    if let (Some(input), Some(output)) = (
        usage.get("input_tokens").and_then(Value::as_u64),
        usage.get("output_tokens").and_then(Value::as_u64),
    ) {
        metrics.insert("tokens".into(), json!(input.saturating_add(output)));
    }
    Value::Object(metrics)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::translate::Registry;

    fn event(name: &str, payload: Value, ts_ms: i64) -> Envelope {
        Envelope {
            source: "muse".into(),
            source_version: Some("1.1.1-R2514.1".into()),
            plugin_version: None,
            session_id: "muse-fixture-session".into(),
            event: name.into(),
            ts_ms,
            managed_run_id: None,
            capture: None,
            payload,
            route: None,
            config: None,
        }
    }

    fn context() -> SessionCtx {
        SessionCtx {
            session_id: "muse-fixture-session".into(),
            config: None,
        }
    }

    fn fixture() -> Vec<Envelope> {
        include_str!("../../tests/fixtures/muse-hooks-1.1.1.jsonl")
            .lines()
            .enumerate()
            .map(|(index, line)| {
                let payload: Value = serde_json::from_str(line).unwrap();
                let name = payload["hook_event_name"].as_str().unwrap();
                event(name, payload.clone(), 1000 + index as i64)
            })
            .collect()
    }

    #[test]
    fn captured_muse_1_1_1_lifecycle_has_visible_starts_and_merges() {
        let registry = Registry::default_agents();
        assert_eq!(registry.canonical_source("muse-code"), Some("muse"));
        let mut translator = registry.create("muse", "muse-fixture-session");
        let ctx = context();
        let events = fixture();
        assert!(translator.handle(&events[0], &ctx).unwrap().is_empty());

        let turn_start = translator.handle(&events[1], &ctx).unwrap();
        let root_id = turn_start
            .iter()
            .find_map(|op| match op {
                SpanOp::Insert(row) if row.name == "Muse Code" => Some(row.span_id.clone()),
                _ => None,
            })
            .expect("session root");
        assert!(turn_start.iter().any(|op| matches!(op, SpanOp::Insert(row) if row.name == "Muse Code" && row.metadata.as_ref().and_then(|m| m.get("muse_version")) == Some(&json!("1.1.1-R2514.1")))));
        assert!(turn_start.iter().any(
            |op| matches!(op, SpanOp::Insert(row) if row.name == "Turn" && row.end_ms.is_none())
        ));
        let llm_start = translator.handle(&events[2], &ctx).unwrap();
        let llm_id = llm_start
            .iter()
            .find_map(|op| match op {
                SpanOp::Insert(row) if row.span_type == SpanType::Llm => Some(row.span_id.clone()),
                _ => None,
            })
            .expect("LLM start span");
        let llm_end = translator.handle(&events[3], &ctx).unwrap();
        assert!(llm_end.iter().any(|op| matches!(op, SpanOp::Merge(row) if row.span_id == llm_id && row.output == Some(json!("echo: hello hook probe")) && row.metrics.as_ref().and_then(|m| m.get("prompt_tokens")) == Some(&json!(0)))));
        let turn_end = translator.handle(&events[4], &ctx).unwrap();
        assert!(turn_end.iter().any(|op| matches!(op, SpanOp::Merge(row) if row.output == Some(json!("echo: hello hook probe")))));
        assert!(translator.handle(&events[1], &ctx).unwrap().is_empty());
        assert!(translator.handle(&events[2], &ctx).unwrap().is_empty());
        assert!(translator.handle(&events[3], &ctx).unwrap().is_empty());
        let session_end = translator.handle(&events[5], &ctx).unwrap();
        assert!(session_end.iter().any(|op| matches!(op, SpanOp::Merge(row) if row.span_id == root_id && row.end_ms == Some(1005))));
        assert!(translator.finalize(&ctx).unwrap().is_empty());
    }

    #[test]
    fn passive_and_unknown_hooks_do_not_create_a_trace() {
        let mut translator = MuseTranslator::new(
            "muse-fixture-session",
            Arc::new(GitMetadataCache::default()),
        );
        for name in ["SessionStart", "SessionEnd", "UnrecognizedEvent", "Stop"] {
            assert!(translator
                .handle(&event(name, json!({}), 1000), &context())
                .unwrap()
                .is_empty());
        }
        assert!(translator.finalize(&context()).unwrap().is_empty());
    }

    #[test]
    fn missing_ids_cannot_pair_distinct_operations_and_failure_is_visible() {
        let mut translator = MuseTranslator::new(
            "muse-fixture-session",
            Arc::new(GitMetadataCache::default()),
        );
        let ctx = context();
        let first = translator
            .handle(
                &event(
                    "PreLLMCall",
                    json!({"messages":["a"],"request_id":42}),
                    1000,
                ),
                &ctx,
            )
            .unwrap();
        let second = translator
            .handle(&event("PreLLMCall", json!({"messages":["b"]}), 1001), &ctx)
            .unwrap();
        let first_id = first
            .iter()
            .find_map(|op| match op {
                SpanOp::Insert(row) if row.error.is_some() => Some(row.span_id.clone()),
                _ => None,
            })
            .unwrap();
        let second_id = second
            .iter()
            .find_map(|op| match op {
                SpanOp::Insert(row) if row.error.is_some() => Some(row.span_id.clone()),
                _ => None,
            })
            .unwrap();
        assert_ne!(first_id, second_id);
        assert!(translator.llms.is_empty());

        let start = event(
            "PreLLMCall",
            json!({"request_id":"r1","turn_id":"t1"}),
            1002,
        );
        translator.handle(&start, &ctx).unwrap();
        let completed = translator.handle(&event("PostLLMCall", json!({"request_id":"r1","status":"failed","usage":{"input_tokens":3,"output_tokens":1,"cached_tokens":2,"reasoning_tokens":1}}), 1003), &ctx).unwrap();
        assert!(completed.iter().any(|op| matches!(op, SpanOp::Merge(row) if row.error.as_deref() == Some("Muse operation failed") && row.metrics.as_ref().and_then(|m| m.get("tokens")) == Some(&json!(4)) && row.metrics.as_ref().and_then(|m| m.get("prompt_cached_tokens")) == Some(&json!(2)))));
    }

    #[test]
    fn unsupported_hook_is_ignored_and_pending_work_is_interrupted() {
        let mut translator = MuseTranslator::new(
            "muse-fixture-session",
            Arc::new(GitMetadataCache::default()),
        );
        let ctx = context();
        translator
            .handle(
                &event(
                    "UserPromptSubmit",
                    json!({"turn_id":"t1","prompt":"x"}),
                    1000,
                ),
                &ctx,
            )
            .unwrap();
        let unsupported = translator
            .handle(
                &event(
                    "PermissionRequest",
                    json!({"turn_id":"t1","request_id":"p1"}),
                    1001,
                ),
                &ctx,
            )
            .unwrap();
        assert!(unsupported.is_empty());
        translator
            .handle(
                &event(
                    "PreLLMCall",
                    json!({"turn_id":"t1","request_id":"r1"}),
                    1002,
                ),
                &ctx,
            )
            .unwrap();
        let ended = translator
            .handle(
                &event(
                    "SessionEnd",
                    json!({"reason":"other","error":"Session terminated abnormally"}),
                    1003,
                ),
                &ctx,
            )
            .unwrap();
        assert!(ended.iter().any(|op| matches!(op, SpanOp::Merge(row) if row.error.as_deref() == Some("Interrupted before completion"))));
        assert!(ended.iter().any(|op| matches!(op, SpanOp::Merge(row) if row.error.as_deref() == Some("Session terminated abnormally"))));
    }
}
