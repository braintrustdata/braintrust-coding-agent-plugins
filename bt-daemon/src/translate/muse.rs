//! Muse hook reducer. Native hook decoding and export DTOs live in crate::muse;
//! this module owns span correlation, ordering, and replay policy.
use super::git::GitMetadataCache;
use super::recent::RecentSet;
#[path = "muse_events.rs"]
mod muse_events;
use super::{
    local_username, AgentTranslator, SessionCtx, SpanOp, SpanRow, SpanType, TranslatorFactory,
};
use crate::ids;
use crate::muse::hooks::*;
use crate::muse::metadata::*;
#[path = "muse_usage.rs"]
mod muse_usage;
use crate::wire::Envelope;
use muse_usage::UsageTracker;
#[cfg(test)]
use serde_json::json;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

type Result<T> = anyhow::Result<T>;
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
struct MuseTranslator {
    session_id: String,
    root_id: String,
    effective_root_id: String,
    external_parent: Option<String>,
    opened: bool,
    root_created: bool,
    session_metadata: SessionMetadata,
    session_version: Option<String>,
    turns: HashMap<String, OpenOp>,
    llms: HashMap<String, OpenOp>,
    closed_turns: RecentSet<String>,
    closed_llms: RecentSet<String>,
    events: muse_events::Events,
    usage: UsageTracker,
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
            session_metadata: SessionMetadata::default(),
            session_version: None,
            turns: HashMap::new(),
            llms: HashMap::new(),
            closed_turns: RecentSet::default(),
            closed_llms: RecentSet::default(),
            root_created: false,
            events: muse_events::Events::default(),
            usage: UsageTracker::default(),
            last_completed_turn: None,
            last_ts: 0,
            git,
        }
    }
    fn ensure_root(&mut self, e: &Envelope, ctx: &SessionCtx) -> Result<Vec<SpanOp>> {
        if self.opened {
            return Ok(Vec::new());
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
        if self.root_created {
            return Ok(Vec::new());
        }
        let authored = serde_json::to_value(RootMetadata {
            source: "muse",
            session_id: &ctx.session_id,
            username: local_username(),
            muse_version: self.session_version.as_deref(),
            adapter_version: e.plugin_version.as_deref(),
            session: &self.session_metadata,
        })?;
        // Arbitrary user metadata is an intentional JSON merge boundary.
        let mut metadata = ctx
            .config
            .as_ref()
            .and_then(|c| c.additional_metadata.as_ref())
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        if let Value::Object(authored) = authored {
            metadata.extend(authored);
        }
        self.root_created = true;
        Ok(vec![SpanOp::Insert(SpanRow {
            span_id: self.root_id.clone(),
            root_span_id: self.effective_root_id.clone(),
            parent_span_ids: self.external_parent.clone().into_iter().collect(),
            name: "Muse Code".into(),
            span_type: SpanType::Task,
            start_ms: Some(e.ts_ms),
            metadata: Some(Value::Object(metadata)),
            ..Default::default()
        })])
    }
    // Values here are already serialized SpanRow fields, never native payloads.
    fn close(
        root: &str,
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
            root_span_id: root.into(),
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
    fn parent(&self, ids: &HookContext) -> String {
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
    fn unpaired(&self, e: &Envelope, reason: &str) -> Result<SpanOp> {
        // Raw payload is used only as an identity fingerprint, not inspected.
        let identity = format!("unpaired:{}:{}:{}", e.event, e.ts_ms, e.payload);
        Ok(SpanOp::Insert(SpanRow {
            span_id: ids::span_id(&self.session_id, &identity),
            root_span_id: self.effective_root_id.clone(),
            parent_span_ids: vec![self.root_id.clone()],
            name: e.event.clone(),
            span_type: SpanType::Task,
            start_ms: Some(e.ts_ms),
            end_ms: Some(e.ts_ms),
            metadata: Some(serde_json::to_value(UnpairedMetadata { unpaired: true })?),
            error: Some(reason.into()),
            ..Default::default()
        }))
    }
    fn start_model(&mut self, e: &Envelope, hook: Hook<ModelStarted>) -> Result<Vec<SpanOp>> {
        let Some(id) = hook.context.request() else {
            return Ok(vec![self.unpaired(e, "Muse PreLLMCall has no request_id")?]);
        };
        let parent = self.parent(&hook.context);
        let model = hook.context.session.model.as_deref();
        Ok(Self::insert_open(
            &mut self.llms,
            id,
            SpanRow {
                span_id: ids::span_id(&self.session_id, &format!("llm:{id}")),
                root_span_id: self.effective_root_id.clone(),
                parent_span_ids: vec![parent.clone()],
                turn_span_id: (parent != self.root_id).then_some(parent),
                name: model
                    .filter(|s| !s.is_empty() && *s != "unknown")
                    .unwrap_or("llm")
                    .into(),
                span_type: SpanType::Llm,
                start_ms: Some(e.ts_ms),
                input: Some(serde_json::to_value(ModelInput {
                    messages: &hook.body.messages,
                    tools: &hook.body.tools,
                })?),
                metadata: Some(serde_json::to_value(ModelStartMetadata {
                    model: &hook.context.session.model,
                    provider: &hook.context.session.provider,
                    details: &hook.body.details,
                    request_record_id: &hook.body.request_record_id,
                    input_summary: &hook.body.input_summary,
                })?),
                ..Default::default()
            },
        ))
    }
    fn finish_model(&mut self, e: &Envelope, hook: Hook<ModelFinished>) -> Result<Vec<SpanOp>> {
        let Some(id) = hook.context.request() else {
            return if self.opened {
                Ok(vec![self.unpaired(e, "Muse PostLLMCall has no request_id")?])
            } else {
                Ok(Vec::new())
            };
        };
        let metrics = hook.body.usage.as_ref().map(|usage| {
            self.usage.record(
                hook.context.turn(),
                id,
                usage,
                hook.body.usage_scope,
                self.llms.contains_key(id),
            )
        });
        let mut closed = Self::close(
            &self.effective_root_id,
            &mut self.llms,
            id,
            e.ts_ms,
            hook.body
                .output_text_preview
                .as_ref()
                .map(serde_json::to_value)
                .transpose()?,
            metrics.map(serde_json::to_value).transpose()?,
            hook.body.outcome.error_message(),
        );
        for op in &mut closed {
            let (SpanOp::Insert(row) | SpanOp::Merge(row)) = op;
            row.metadata = Some(serde_json::to_value(ModelEndMetadata {
                finish_reason: &hook.body.finish_reason,
                response_id: &hook.body.response_id,
                tool_call_count: &hook.body.tool_call_count,
                status: &hook.body.outcome.status,
                options: &hook.body.options,
            })?);
        }
        if !closed.is_empty() {
            self.closed_llms.insert(id.into());
        }
        Ok(closed)
    }
    fn finish_turn(&mut self, e: &Envelope, hook: Hook<TurnFinished>) -> Result<Vec<SpanOp>> {
        let Some(id) = hook.context.turn() else {
            return if self.opened {
                Ok(vec![self.unpaired(e, "Muse Stop has no turn_id")?])
            } else {
                Ok(Vec::new())
            };
        };
        let output = hook
            .body
            .last_assistant_message
            .map(serde_json::to_value)
            .transpose()?;
        let error = hook.body.outcome.error_message();
        let mut out = Self::close(
            &self.effective_root_id,
            &mut self.turns,
            id,
            e.ts_ms,
            output.clone(),
            None,
            error.clone(),
        );
        if !out.is_empty() {
            self.closed_turns.insert(id.into());
            self.last_completed_turn = Some(id.into());
            out.push(SpanOp::Merge(SpanRow {
                span_id: self.root_id.clone(),
                root_span_id: self.effective_root_id.clone(),
                output,
                metrics: Some(serde_json::to_value(&self.usage.totals)?),
                error,
                late_merge_key: Some(format!("muse:turn:{id}:{}", e.ts_ms)),
                late_merge_version: Some(super::LateMergeVersion {
                    ts_ms: e.ts_ms,
                    native_id: Some(id.into()),
                }),
                ..Default::default()
            }));
        }
        Ok(out)
    }
}
impl AgentTranslator for MuseTranslator {
    fn handle(&mut self, e: &Envelope, ctx: &SessionCtx) -> Result<Vec<SpanOp>> {
        self.last_ts = self.last_ts.max(e.ts_ms);
        let event = match MuseEvent::decode(e) {
            Ok(Some(event)) => event,
            Ok(None) => return Ok(Vec::new()),
            Err(error) => {
                tracing::warn!(source = "muse", event = %e.event, %error, "invalid Muse hook payload");
                return Ok(Vec::new());
            }
        };
        if let Some(version) = &e.source_version {
            self.session_version = Some(version.clone());
        }
        self.session_metadata.update(&event.context().session);
        let kind = event.name();
        let ids = event.context();
        if (kind == HookName::UserPromptSubmit
            && ids
                .turn()
                .is_some_and(|id| self.closed_turns.contains(&id.to_owned())))
            || (kind == HookName::PreLLMCall
                && ids
                    .request()
                    .is_some_and(|id| self.closed_llms.contains(&id.to_owned())))
        {
            return Ok(Vec::new());
        }
        let mut out = if matches!(
            kind,
            HookName::UserPromptSubmit
                | HookName::PreLLMCall
                | HookName::PreToolUse
                | HookName::PermissionRequest
                | HookName::PostToolUse
                | HookName::PostToolUseFailure
                | HookName::PostToolBatch
                | HookName::SubagentStart
                | HookName::PreCompact
        ) {
            self.ensure_root(e, ctx)?
        } else {
            Vec::new()
        };
        let cwd = event.context().session.cwd.clone();
        match event {
            MuseEvent::UserPromptSubmit(hook) => {
                if let Some(id) = hook.context.turn() {
                    let turn_span_id = ids::span_id(&self.session_id, &format!("turn:{id}"));
                    out.extend(Self::insert_open(
                        &mut self.turns,
                        id,
                        SpanRow {
                            span_id: turn_span_id.clone(),
                            root_span_id: self.effective_root_id.clone(),
                            parent_span_ids: vec![self.root_id.clone()],
                            name: "Turn".into(),
                            span_type: SpanType::Task,
                            start_ms: Some(e.ts_ms),
                            input: hook.body.prompt.map(serde_json::to_value).transpose()?,
                            is_turn: true,
                            turn_span_id: Some(turn_span_id),
                            ..Default::default()
                        },
                    ));
                } else {
                    out.push(self.unpaired(e, "Muse UserPromptSubmit has no turn_id")?);
                }
            }
            MuseEvent::PreLLMCall(hook) => out.extend(self.start_model(e, hook)?),
            MuseEvent::PostLLMCall(hook) => out.extend(self.finish_model(e, hook)?),
            MuseEvent::Stop(hook) | MuseEvent::StopFailure(hook) => {
                out.extend(self.finish_turn(e, hook)?)
            }
            MuseEvent::SessionEnd(hook) => {
                out.extend(self.close_all(e.ts_ms, Some("Interrupted before completion")));
                if self.opened {
                    out.push(SpanOp::Merge(SpanRow {
                        span_id: self.root_id.clone(),
                        root_span_id: self.effective_root_id.clone(),
                        end_ms: Some(e.ts_ms),
                        metadata: if hook.body.reason.is_missing() {
                            None
                        } else {
                            Some(serde_json::to_value(SessionEndMetadata {
                                end_reason: &hook.body.reason,
                            })?)
                        },
                        error: hook.body.outcome.error_message(),
                        metrics: Some(serde_json::to_value(&self.usage.totals)?),
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
            event => out.extend(self.handle_events(e, event)?),
        }
        for op in &mut out {
            let (SpanOp::Insert(row) | SpanOp::Merge(row)) = op;
            if let Some(key) = &row.late_merge_key {
                let content = serde_json::to_value(CompletionFingerprint {
                    output: &row.output,
                    metrics: &row.metrics,
                    error: &row.error,
                    metadata: &row.metadata,
                })?;
                row.late_merge_key = Some(format!(
                    "{key}:{}",
                    ids::span_id(&self.session_id, &content.to_string())
                ));
            }
        }
        self.git.enrich_rows(cwd.as_deref(), &mut out);
        Ok(out)
    }
    fn finalize(&mut self, _ctx: &SessionCtx) -> Result<Vec<SpanOp>> {
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
#[cfg(test)]
fn event_error(payload: &Value) -> Option<String> {
    serde_json::from_value::<NativeOutcome>(payload.clone())
        .unwrap()
        .error_message()
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
    fn completion_metadata_preserves_absent_null_and_native_status_aliases() {
        let mut translator = MuseTranslator::new(
            "muse-fixture-session",
            Arc::new(GitMetadataCache::default()),
        );
        translator
            .handle(
                &event("PreLLMCall", json!({"request_id":"r"}), 1),
                &context(),
            )
            .unwrap();
        let completed = translator
            .handle(
                &event(
                    "PostLLMCall",
                    json!({
                        "request_id":"r", "status":"error", "response_id":null,
                        "finish_reason":null, "options":null,
                    }),
                    2,
                ),
                &context(),
            )
            .unwrap();
        let row = completed
            .iter()
            .find_map(|op| match op {
                SpanOp::Merge(row) => Some(row),
                _ => None,
            })
            .unwrap();
        assert_eq!(
            row.metadata,
            Some(json!({"status":"error","response_id":null,"finish_reason":null,"options":null}))
        );
        assert_eq!(row.error.as_deref(), Some("Muse operation error"));
        let ended = translator
            .handle(&event("SessionEnd", json!({"reason":null}), 3), &context())
            .unwrap();
        assert!(ended.iter().any(|op| matches!(op, SpanOp::Merge(row) if row.metadata == Some(json!({"end_reason":null})))));
    }

    #[test]
    fn malformed_known_hook_is_a_noop_and_does_not_lose_open_state() {
        let mut translator = MuseTranslator::new(
            "muse-fixture-session",
            Arc::new(GitMetadataCache::default()),
        );
        translator
            .handle(
                &event("PreLLMCall", json!({"request_id":"r"}), 1),
                &context(),
            )
            .unwrap();
        assert!(translator
            .handle(
                &event(
                    "PostLLMCall",
                    json!({"request_id":"r","usage":{"input_tokens":"wrong"}}),
                    2
                ),
                &context()
            )
            .unwrap()
            .is_empty());
        assert!(translator.llms.contains_key("r"));
        assert!(translator
            .handle(
                &event(
                    "PostLLMCall",
                    json!({"request_id":"r","usage":{"input_tokens":1}}),
                    3
                ),
                &context()
            )
            .unwrap()
            .iter()
            .any(|op| matches!(op, SpanOp::Merge(_))));
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
                    json!({"messages":[{"role":"user","content":"a"}],"request_id":42}),
                    1000,
                ),
                &ctx,
            )
            .unwrap();
        let second = translator
            .handle(
                &event(
                    "PreLLMCall",
                    json!({"messages":[{"role":"user","content":"b"}]}),
                    1001,
                ),
                &ctx,
            )
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
