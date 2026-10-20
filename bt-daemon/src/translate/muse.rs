//! Muse Code hook translator.
//!
//! Muse hooks remain a thin transport. The session, turn and model payloads
//! used here were captured from Muse Code 1.1.1-R2514.1. Import feeds the
//! same event names through this translator.

use super::git::GitMetadataCache;
use super::recent::{RecentMap, RecentSet};

#[path = "muse_events.rs"]
mod muse_events;
use super::{
    local_username, AgentTranslator, SessionCtx, SpanOp, SpanRow, SpanType, TranslatorFactory,
};
use crate::ids;
use crate::wire::Envelope;
use serde::{Deserialize, Deserializer};
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::sync::Arc;

impl TranslatorFactory for crate::agents::Muse {
    fn create(&self, session_id: &str) -> Box<dyn AgentTranslator> {
        Box::new(MuseTranslator::new(session_id, GitMetadataCache::shared()))
    }
}

#[derive(Clone)]
struct OpenOp {
    id: String,
    parents: Vec<String>,
    turn_span_id: Option<String>,
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
    closed_turns: RecentSet<String>,
    closed_llms: RecentSet<String>,
    root_created: bool,
    events: muse_events::Events,
    total_metrics: Map<String, Value>,
    model_metrics: RecentMap<String, Value>,
    turn_usage: RecentMap<String, Value>,
    last_completed_turn: Option<String>,
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
            closed_turns: RecentSet::default(),
            closed_llms: RecentSet::default(),
            root_created: false,
            events: muse_events::Events::default(),
            total_metrics: Map::new(),
            model_metrics: RecentMap::default(),
            turn_usage: RecentMap::default(),
            last_completed_turn: None,
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
            "provider",
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
        matches!(
            event,
            "UserPromptSubmit"
                | "PreLLMCall"
                | "PreToolUse"
                | "PermissionRequest"
                | "PostToolUse"
                | "PostToolUseFailure"
                | "PostToolBatch"
                | "SubagentStart"
                | "PreCompact"
        )
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
        if self.root_created {
            return Vec::new();
        }
        self.root_created = true;
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
        vec![SpanOp::Merge(SpanRow {
            span_id: open.id,
            root_span_id: root_span_id.into(),
            parent_span_ids: open.parents,
            turn_span_id: open.turn_span_id,
            end_ms: Some(ts),
            output,
            metrics,
            error,
            late_merge_key: Some(format!("muse:complete:{key}:{ts}")),
            ..Default::default()
        })]
    }
    fn close_all(&mut self, ts: i64, error: Option<&str>) -> Vec<SpanOp> {
        let mut out = self.close_events(ts, error);
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
                    turn_span_id: open.turn_span_id,
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
                turn_span_id: row.turn_span_id.clone(),
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
            && ids
                .turn()
                .is_some_and(|id| self.closed_turns.contains(&id.to_owned())))
            || (e.event == "PreLLMCall"
                && ids
                    .request()
                    .is_some_and(|id| self.closed_llms.contains(&id.to_owned())))
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
                            is_turn: true,
                            turn_span_id: Some(ids::span_id(
                                &self.session_id,
                                &format!("turn:{id}"),
                            )),
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
                        "options",
                        "request_record_id",
                        "input_summary",
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
                            parent_span_ids: vec![parent.clone()],
                            turn_span_id: (parent != self.root_id).then_some(parent),
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
                    let metrics = p.get("usage").filter(|v| v.is_object()).map(|usage| {
                        // Native hook usage is cumulative within a turn (verified
                        // against per-model durable records in Muse 1.4.11).
                        // Export envelopes explicitly carry per-model usage.
                        let mut per_model = usage.clone();
                        if p.get("usage_scope").and_then(Value::as_str) != Some("model")
                            && self.llms.contains_key(id)
                        {
                            if let Some(turn) = ids.turn() {
                                if let Some(previous) = self.turn_usage.get(turn) {
                                    if let Some(fields) = per_model.as_object_mut() {
                                        for (key, value) in fields {
                                            if let (Some(current), Some(previous)) = (
                                                value.as_u64(),
                                                previous.get(key).and_then(Value::as_u64),
                                            ) {
                                                // A reset starts a new native counter window.
                                                *value = json!(current
                                                    .checked_sub(previous)
                                                    .unwrap_or(current));
                                            }
                                        }
                                    }
                                }
                                self.turn_usage.insert(turn.to_owned(), usage.clone());
                            }
                        }
                        usage_metrics(&per_model)
                    });
                    if let Some(metrics) = &metrics {
                        if self.llms.contains_key(id) && self.model_metrics.get(id).is_none() {
                            for (key, value) in metrics.as_object().into_iter().flatten() {
                                if let Some(value) = value.as_u64() {
                                    let previous = self
                                        .total_metrics
                                        .get(key)
                                        .and_then(Value::as_u64)
                                        .unwrap_or(0);
                                    self.total_metrics
                                        .insert(key.clone(), json!(previous.saturating_add(value)));
                                }
                            }
                            self.model_metrics.insert(id.to_owned(), metrics.clone());
                        }
                    }
                    let mut closed = Self::close(
                        &self.effective_root_id,
                        &mut self.llms,
                        id,
                        e.ts_ms,
                        p.get("output_text_preview").cloned(),
                        metrics,
                        error,
                    );
                    for op in &mut closed {
                        let (SpanOp::Merge(row) | SpanOp::Insert(row)) = op;
                        let metadata: Map<String, Value> = [
                            "finish_reason",
                            "response_id",
                            "tool_call_count",
                            "status",
                            "options",
                        ]
                        .into_iter()
                        .filter_map(|key| p.get(key).map(|v| (key.to_owned(), v.clone())))
                        .collect();
                        row.metadata = Some(Value::Object(metadata));
                    }
                    if !closed.is_empty() {
                        self.closed_llms.insert(id.into());
                        out.extend(closed);
                    }
                } else if self.opened {
                    out.push(self.unpaired(e, "Muse PostLLMCall has no request_id"));
                }
            }
            "Stop" | "StopFailure" => {
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
                        self.last_completed_turn = Some(id.to_owned());
                        out.extend(closed);
                        out.push(SpanOp::Merge(SpanRow {
                            span_id: self.root_id.clone(),
                            root_span_id: self.effective_root_id.clone(),
                            output: p
                                .get("last_assistant_message")
                                .cloned()
                                .filter(|v| !v.is_null()),
                            metrics: Some(Value::Object(self.total_metrics.clone())),
                            error: event_error(p),
                            late_merge_key: Some(format!("muse:turn:{id}:{}", e.ts_ms)),
                            late_merge_version: Some(super::LateMergeVersion {
                                ts_ms: e.ts_ms,
                                native_id: Some(id.to_owned()),
                            }),
                            ..Default::default()
                        }));
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
                        metrics: Some(Value::Object(self.total_metrics.clone())),
                        late_merge_key: Some(format!("muse:session:end:{}", e.ts_ms)),
                        late_merge_version: Some(super::LateMergeVersion {
                            ts_ms: e.ts_ms,
                            native_id: self.last_completed_turn.clone(),
                        }),
                        ..Default::default()
                    }));
                    self.opened = false;
                }
            }
            _ => out.extend(self.handle_events(e, &ids)),
        }
        // A native export can enrich a previously delivered preview at the
        // same timestamp. Deduplicate the material completion, not only time.
        for op in &mut out {
            let (SpanOp::Insert(row) | SpanOp::Merge(row)) = op;
            if let Some(key) = &row.late_merge_key {
                let content = json!({"output":row.output,"metrics":row.metrics,"error":row.error,"metadata":row.metadata});
                row.late_merge_key = Some(format!(
                    "{key}:{}",
                    ids::span_id(&self.session_id, &content.to_string())
                ));
            }
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
    let render = |value: &Value| match value {
        Value::Null => None,
        Value::String(text) => (!text.is_empty()).then(|| text.clone()),
        value => Some(value.to_string()),
    };
    let error = payload.get("error").and_then(render).or_else(|| {
        payload
            .get("status")
            .and_then(Value::as_str)
            // Native model completions include successful statuses such as
            // `tool_calls`. Unknown future statuses are not evidence of failure.
            .filter(|status| {
                matches!(
                    *status,
                    "failed"
                        | "failure"
                        | "error"
                        | "cancelled"
                        | "canceled"
                        | "interrupted"
                        | "timeout"
                        | "timed_out"
                        | "denied"
                        | "rejected"
                )
            })
            .map(|status| format!("Muse operation {status}"))
    });
    match (error, payload.get("error_details").and_then(render)) {
        (Some(error), Some(details)) => Some(format!("{error}: {details}")),
        (Some(error), None) => Some(error),
        (None, details) => details,
    }
}

fn usage_metrics(usage: &Value) -> Value {
    let mut metrics = Map::new();
    for (native, standard) in [
        ("input_tokens", "prompt_tokens"),
        ("output_tokens", "completion_tokens"),
        ("cache_read_tokens", "prompt_cached_tokens"),
        ("cached_tokens", "prompt_cached_tokens"),
        ("cache_write_tokens", "prompt_cache_creation_tokens"),
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
        let turn_id = turn_start
            .iter()
            .find_map(|op| match op {
                SpanOp::Insert(row) if row.is_turn => Some(row.span_id.clone()),
                _ => None,
            })
            .expect("native user turn marker");
        for op in turn_start.iter().chain(llm_start.iter()) {
            let (SpanOp::Insert(row) | SpanOp::Merge(row)) = op;
            if row.span_id != root_id {
                assert_eq!(
                    row.metadata
                        .as_ref()
                        .and_then(|metadata| metadata.get(super::super::TURN_SPAN_ID_KEY)),
                    Some(&json!(turn_id))
                );
            }
        }
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
    fn resumed_session_keeps_one_root_and_publishes_latest_output_and_totals() {
        let mut translator = MuseTranslator::new(
            "muse-fixture-session",
            Arc::new(GitMetadataCache::default()),
        );
        let ctx = context();
        let mut rows = Vec::new();
        for (turn, ts) in [("t1", 1), ("t2", 10)] {
            for event in [
                event(
                    "UserPromptSubmit",
                    json!({"turn_id":turn,"prompt":"hello"}),
                    ts,
                ),
                event(
                    "PreLLMCall",
                    json!({"turn_id":turn,"request_id":format!("{turn}:0:1"),"options":{"temperature":0.2}}),
                    ts + 1,
                ),
                event(
                    "PostLLMCall",
                    json!({"turn_id":turn,"request_id":format!("{turn}:0:1"),"finish_reason":"stop","response_id":"native-response","tool_call_count":0,"usage":{"input_tokens":3,"output_tokens":2}}),
                    ts + 2,
                ),
                event(
                    "Stop",
                    json!({"turn_id":turn,"last_assistant_message":turn}),
                    ts + 3,
                ),
                event("SessionEnd", json!({}), ts + 4),
            ] {
                rows.extend(translator.handle(&event, &ctx).unwrap());
            }
        }
        assert_eq!(
            rows.iter()
                .filter(|op| matches!(op, SpanOp::Insert(row) if row.name == "Muse Code"))
                .count(),
            1
        );
        let root = ids::span_id("muse-fixture-session", "root");
        assert!(rows.iter().any(|op| matches!(op, SpanOp::Merge(row) if row.span_id == root && row.output == Some(json!("t2")) && row.metrics.as_ref().unwrap()["tokens"] == 10 && row.late_merge_key.is_some())));
        assert!(rows.iter().any(|op| matches!(op, SpanOp::Merge(row) if row.metadata.as_ref().is_some_and(|m| m["response_id"] == "native-response"))));
    }

    #[test]
    fn stop_failure_details_reach_turn_and_root() {
        let mut translator = MuseTranslator::new(
            "muse-fixture-session",
            Arc::new(GitMetadataCache::default()),
        );
        translator
            .handle(
                &event(
                    "UserPromptSubmit",
                    json!({"turn_id":"turn","prompt":"hello"}),
                    1,
                ),
                &context(),
            )
            .unwrap();
        let rows = translator.handle(&event("StopFailure", json!({"turn_id":"turn","error":"provider_error","error_details":"rate limited"}), 2), &context()).unwrap();
        assert_eq!(rows.iter().filter(|op| matches!(op, SpanOp::Merge(row) if row.error.as_deref() == Some("provider_error: rate limited"))).count(), 2);
    }

    #[test]
    fn native_cumulative_hook_usage_becomes_per_model_metrics_without_overcounting_root() {
        // Muse 1.4.11 deterministic two-model native run: live PostLLMCall
        // reports 10/5 then 20/10, while each durable model_completed is 10/5.
        let mut translator = MuseTranslator::new(
            "muse-fixture-session",
            Arc::new(GitMetadataCache::default()),
        );
        let ctx = context();
        translator
            .handle(
                &event(
                    "UserPromptSubmit",
                    json!({"turn_id":"turn","prompt":"hello"}),
                    1,
                ),
                &ctx,
            )
            .unwrap();
        for (step, input, output) in [(0, 10, 5), (1, 20, 10)] {
            let request = format!("turn:{step}:1");
            translator
                .handle(
                    &event(
                        "PreLLMCall",
                        json!({"turn_id":"turn","request_id":request}),
                        step + 2,
                    ),
                    &ctx,
                )
                .unwrap();
            let complete = event(
                "PostLLMCall",
                json!({"turn_id":"turn","request_id":request,"usage":{"input_tokens":input,"output_tokens":output}}),
                step + 3,
            );
            let rows = translator.handle(&complete, &ctx).unwrap();
            assert!(rows.iter().any(|op|matches!(op,SpanOp::Merge(row) if row.metrics.as_ref().is_some_and(|m|m["prompt_tokens"] == 10 && m["completion_tokens"] == 5))));
            assert!(translator.handle(&complete, &ctx).unwrap().is_empty());
        }
        let rows = translator
            .handle(
                &event(
                    "Stop",
                    json!({"turn_id":"turn","last_assistant_message":"done"}),
                    10,
                ),
                &ctx,
            )
            .unwrap();
        let root = ids::span_id("muse-fixture-session", "root");
        assert!(rows.iter().any(|op|matches!(op,SpanOp::Merge(row) if row.span_id == root && row.metrics.as_ref().is_some_and(|m|m["prompt_tokens"] == 20 && m["completion_tokens"] == 10 && m["tokens"] == 30))));
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
                    "UnrecognizedEvent",
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
