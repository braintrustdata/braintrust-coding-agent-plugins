//! Typed tool, observation, subagent, and compaction reducers.
use super::*;
#[derive(Default)]
pub(super) struct Events {
    open: HashMap<String, OpenOp>,
    closed: RecentSet<String>,
}
impl MuseTranslator {
    pub(super) fn handle_events(&mut self, e: &Envelope, event: MuseEvent) -> Result<Vec<SpanOp>> {
        if !self.opened {
            return Ok(Vec::new());
        }
        match event {
            MuseEvent::PreToolUse(hook) => self.start_tool(e, &hook.context, &hook.body, false),
            MuseEvent::PostToolUse(hook) => {
                let outcome = hook
                    .body
                    .outcome
                    .error_message()
                    .map(ToolOutcome::Failure)
                    .unwrap_or(ToolOutcome::Success);
                self.finish_tool(e, &hook.context, hook.body, outcome)
            }
            MuseEvent::PostToolUseFailure(hook) => {
                let outcome = ToolOutcome::Failure(
                    hook.body
                        .outcome
                        .error_message()
                        .unwrap_or_else(|| "Muse tool failed".into()),
                );
                self.finish_tool(e, &hook.context, hook.body, outcome)
            }
            MuseEvent::PostToolBatch(hook) => {
                let mut out = Vec::new();
                for call in hook.body.tool_calls {
                    let outcome = call
                        .outcome
                        .error_message()
                        .map(ToolOutcome::Failure)
                        .unwrap_or(ToolOutcome::Unknown);
                    out.extend(self.finish_tool(e, &hook.context, call, outcome)?);
                }
                Ok(out)
            }
            MuseEvent::PermissionRequest(hook) => {
                let metadata = serde_json::to_value(PermissionMetadata {
                    tool_name: &hook.body.tool_name,
                })?;
                self.observe(
                    e,
                    &hook.context,
                    hook.body.tool_input.map(|v| v.0),
                    metadata,
                    "permission-request",
                )
            }
            MuseEvent::Notification(hook) => {
                let metadata = serde_json::to_value(NotificationMetadata {
                    tool_name: &hook.body.tool.tool_name,
                    notification_type: &hook.body.notification_type,
                    title: &hook.body.title,
                    message: &hook.body.message,
                })?;
                self.observe(
                    e,
                    &hook.context,
                    hook.body.tool.tool_input.map(|v| v.0),
                    metadata,
                    "notification",
                )
            }
            MuseEvent::SubagentStart(hook) => {
                let Some(child) = hook
                    .body
                    .child_session_id
                    .as_deref()
                    .filter(|s| !s.is_empty())
                else {
                    return Ok(vec![
                        self.unpaired(e, "Muse subagent event has no child identity")?
                    ]);
                };
                // subagent_id names an agent; only child_session_id identifies an invocation.
                let key = format!("subagent:{child}");
                let mut row = self.event_row(e, &hook.context, &key);
                row.name = hook
                    .body
                    .subagent_id
                    .as_deref()
                    .unwrap_or("Subagent")
                    .into();
                row.metadata = Some(serde_json::to_value(SubagentMetadata {
                    subagent_id: &hook.body.subagent_id,
                    child_session_id: &hook.body.child_session_id,
                })?);
                Ok(self.open_event(&key, row))
            }
            MuseEvent::SubagentStop(hook) => {
                let Some(child) = hook
                    .body
                    .child
                    .child_session_id
                    .as_deref()
                    .filter(|s| !s.is_empty())
                else {
                    return Ok(vec![
                        self.unpaired(e, "Muse subagent event has no child identity")?
                    ]);
                };
                Ok(self.finish_event(
                    &format!("subagent:{child}"),
                    e.ts_ms,
                    hook.body
                        .last_assistant_message
                        .map(serde_json::to_value)
                        .transpose()?,
                    hook.body.outcome.error_message(),
                    None,
                ))
            }
            MuseEvent::PreCompact(hook) => {
                let key = format!(
                    "compact:{}:{}:{}",
                    hook.context.turn().unwrap_or("session"),
                    hook.body.trigger.as_deref().unwrap_or("unknown"),
                    e.ts_ms
                );
                let mut row = self.event_row(e, &hook.context, &key);
                row.name = "Context compaction".into();
                row.tags = Some(vec!["compaction".into()]);
                row.metadata = Some(serde_json::to_value(CompactMetadata {
                    trigger: &hook.body.trigger,
                    native_operation_id_available: false,
                })?);
                Ok(self.open_event(&key, row))
            }
            MuseEvent::PostCompact(hook) => {
                let prefix = format!(
                    "compact:{}:{}:",
                    hook.context.turn().unwrap_or("session"),
                    hook.body.trigger.as_deref().unwrap_or("unknown")
                );
                // There is no native operation id. Pair only a uniquely matching start.
                let matches: Vec<_> = self
                    .events
                    .open
                    .keys()
                    .filter(|key| key.starts_with(&prefix))
                    .cloned()
                    .collect();
                Ok(if matches.len() == 1 {
                    self.finish_event(
                        &matches[0],
                        e.ts_ms,
                        None,
                        hook.body.outcome.error_message(),
                        None,
                    )
                } else {
                    Vec::new()
                })
            }
            _ => Ok(Vec::new()),
        }
    }
    fn observe(
        &mut self,
        e: &Envelope,
        context: &HookContext,
        input: Option<Value>,
        metadata: Value,
        tag: &str,
    ) -> Result<Vec<SpanOp>> {
        // Native observation has neither a decision nor an operation duration.
        let key = ids::span_id(
            &self.session_id,
            &format!("observation:{}:{}:{}", e.event, e.ts_ms, e.payload),
        );
        if !self.events.closed.insert(key.clone()) {
            return Ok(Vec::new());
        }
        let mut row = self.event_row(e, context, &key);
        row.name = e.event.clone();
        row.end_ms = Some(e.ts_ms);
        row.input = input;
        row.metadata = Some(metadata);
        row.tags = Some(vec![tag.into()]);
        Ok(vec![SpanOp::Insert(row)])
    }
    fn event_row(&self, e: &Envelope, context: &HookContext, key: &str) -> SpanRow {
        let parent = self.parent(context);
        SpanRow {
            span_id: ids::span_id(&self.session_id, key),
            root_span_id: self.effective_root_id.clone(),
            parent_span_ids: vec![parent.clone()],
            turn_span_id: (parent != self.root_id).then_some(parent),
            start_ms: Some(e.ts_ms),
            ..Default::default()
        }
    }
    fn open_event(&mut self, key: &str, row: SpanRow) -> Vec<SpanOp> {
        if self.events.closed.contains(&key.to_owned()) {
            return Vec::new();
        }
        Self::insert_open(&mut self.events.open, key, row)
    }
    fn finish_event(
        &mut self,
        key: &str,
        ts: i64,
        output: Option<Value>,
        error: Option<String>,
        metadata: Option<Value>,
    ) -> Vec<SpanOp> {
        let mut out = Self::close(
            &self.effective_root_id,
            &mut self.events.open,
            key,
            ts,
            output,
            None,
            error,
        );
        if !out.is_empty() {
            self.events.closed.insert(key.into());
            for op in &mut out {
                let (SpanOp::Merge(row) | SpanOp::Insert(row)) = op;
                row.metadata = metadata.clone();
            }
        }
        out
    }
    fn start_tool(
        &mut self,
        e: &Envelope,
        context: &HookContext,
        tool: &ToolStarted,
        missing_start: bool,
    ) -> Result<Vec<SpanOp>> {
        let Some(id) = tool.tool_use_id.as_deref().filter(|s| !s.is_empty()) else {
            return Ok(vec![self.unpaired(e, "Muse tool event has no tool_use_id")?]);
        };
        let key = format!("tool:{id}");
        let mut row = self.event_row(e, context, &key);
        row.name = tool
            .tool_name
            .as_deref()
            .filter(|s| !s.is_empty())
            .unwrap_or("tool")
            .into();
        row.span_type = SpanType::Tool;
        row.input = tool.tool_input.as_ref().map(|v| v.0.clone());
        row.metadata = Some(serde_json::to_value(ToolStartMetadata {
            tool_name: &row.name,
            native_start_missing: missing_start,
        })?);
        Ok(self.open_event(&key, row))
    }
    fn finish_tool(
        &mut self,
        e: &Envelope,
        context: &HookContext,
        tool: ToolFinished,
        outcome: ToolOutcome,
    ) -> Result<Vec<SpanOp>> {
        let Some(id) = tool.tool.tool_use_id.as_deref().filter(|s| !s.is_empty()) else {
            return Ok(vec![
                self.unpaired(e, "Muse tool result has no tool_use_id")?
            ]);
        };
        let key = format!("tool:{id}");
        if self.events.closed.contains(&key) {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        if !self.events.open.contains_key(&key) {
            out.extend(self.start_tool(e, context, &tool.tool, true)?);
            if let Some(SpanOp::Insert(row)) = out.first_mut() {
                row.start_ms = Some(
                    tool.duration_ms
                        .map(|d| e.ts_ms.saturating_sub(i64::try_from(d).unwrap_or(i64::MAX)))
                        .unwrap_or(e.ts_ms),
                );
            }
        }
        let (error, label) = match outcome {
            ToolOutcome::Success => (None, Some(ToolOutcomeLabel::Success)),
            ToolOutcome::Failure(error) => (Some(error), Some(ToolOutcomeLabel::Error)),
            ToolOutcome::Unknown => (None, None),
        };
        out.extend(
            self.finish_event(
                &key,
                e.ts_ms,
                tool.tool_response.map(|v| v.0),
                error,
                label
                    .map(|tool_outcome| serde_json::to_value(ToolEndMetadata { tool_outcome }))
                    .transpose()?,
            ),
        );
        Ok(out)
    }
    pub(super) fn close_events(&mut self, ts: i64, error: Option<&str>) -> Vec<SpanOp> {
        let keys: Vec<_> = self.events.open.keys().cloned().collect();
        keys.into_iter()
            .flat_map(|key| self.finish_event(&key, ts, None, error.map(str::to_owned), None))
            .collect()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn model_tool_calls_status_is_success_and_batch_only_outcomes_stay_unknown() {
        for status in [
            "success",
            "completed",
            "tool_calls",
            "stop",
            "future_status",
        ] {
            assert_eq!(event_error(&json!({"status":status,"error":null})), None);
        }
        assert_eq!(event_error(&json!({"status":"success","error":""})), None);
        assert_eq!(
            event_error(&json!({"status":"failed","error_details":"network unavailable"})),
            Some("Muse operation failed: network unavailable".into())
        );
        let mut t = translator();
        let rows = t.handle(&event("PostToolBatch",json!({"turn_id":"t1","tool_calls":[{"tool_name":"bash","tool_use_id":"unavailable","tool_input":{},"tool_response":"tool unavailable: unknown tool `bash`"},{"tool_name":"shell","tool_use_id":"explicit-error","tool_input":{},"tool_response":"failed","error":"access denied"}]}),1010), &context()).unwrap();
        assert!(rows.iter().any(|op| matches!(op, SpanOp::Merge(row) if row.span_id == ids::span_id("muse-fixture-session","tool:unavailable") && row.error.is_none() && row.metadata.is_none())));
        assert!(rows.iter().any(|op| matches!(op, SpanOp::Merge(row) if row.span_id == ids::span_id("muse-fixture-session","tool:explicit-error") && row.error.as_deref() == Some("access denied") && row.metadata.as_ref().and_then(|m|m.get("tool_outcome")) == Some(&json!("error")))));
    }
    #[test]
    fn captured_modern_hooks_keep_native_child_sessions_distinct() {
        // Captured from Muse Code 1.4.10-R6427.1 with a managed `cat` hook,
        // `muse exec --provider echo --no-session-log modern hook fixture`.
        // Session/turn ids and the scratch workspace path are sanitized.
        // Builtin reminder child hooks contain no parent session/turn identity.
        let registry = super::super::super::Registry::default_agents();
        let mut translators = HashMap::new();
        let mut child_starts = 0;
        let mut llms = 0;
        for (index, line) in include_str!("../../tests/fixtures/muse-hooks-1.4.10.jsonl")
            .lines()
            .enumerate()
        {
            let payload: Value = serde_json::from_str(line).unwrap();
            let session = payload["session_id"].as_str().unwrap();
            let name = payload["hook_event_name"].as_str().unwrap();
            if name == "SubagentStart" {
                child_starts += 1;
                assert_eq!(payload["session_id"], payload["child_session_id"]);
                assert!(payload.get("parent_session_id").is_none());
            }
            let mut e = event(name, payload.clone(), 1000 + index as i64);
            e.session_id = session.into();
            let ctx = SessionCtx {
                session_id: session.into(),
                config: None,
            };
            let translator = translators
                .entry(session.to_owned())
                .or_insert_with(|| registry.create("muse", session));
            let rows = translator.handle(&e, &ctx).unwrap();
            for op in rows {
                if let SpanOp::Insert(row) = op {
                    if row.span_type == SpanType::Llm {
                        llms += 1;
                        assert_eq!(
                            row.root_span_id,
                            ids::span_id(&ids::session_namespace("muse", session), "root")
                        );
                    }
                }
            }
        }
        assert!(child_starts > 0);
        assert!(llms >= 2);
    }
    fn context() -> SessionCtx {
        SessionCtx {
            session_id: "muse-fixture-session".into(),
            config: None,
        }
    }
    fn event(name: &str, payload: Value, ts_ms: i64) -> Envelope {
        Envelope {
            source: "muse".into(),
            source_version: Some("1.4.10-R6427.1".into()),
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
    fn translator() -> MuseTranslator {
        let mut t = MuseTranslator::new(
            "muse-fixture-session",
            Arc::new(GitMetadataCache::default()),
        );
        t.handle(
            &event(
                "UserPromptSubmit",
                json!({"turn_id":"t1","prompt":"x"}),
                1000,
            ),
            &context(),
        )
        .unwrap();
        t
    }
    #[test]
    fn parallel_tools_pair_by_id_and_batch_results_do_not_duplicate() {
        let mut t = translator();
        for id in ["a", "b"] {
            let rows = t.handle(&event("PreToolUse", json!({"turn_id":"t1","tool_use_id":id,"tool_name":"read_file","tool_input":{"path":id}}), 1010), &context()).unwrap();
            assert!(rows.iter().any(|op| matches!(op, SpanOp::Insert(row) if row.span_type == SpanType::Tool && row.end_ms.is_none() && row.turn_span_id.is_some())));
        }
        let rows = t
            .handle(
                &event(
                    "PostToolUse",
                    json!({"turn_id":"t1","tool_use_id":"b","tool_response":{"text":"b"}}),
                    1020,
                ),
                &context(),
            )
            .unwrap();
        assert!(rows.iter().any(|op| matches!(op, SpanOp::Merge(row) if row.span_id == ids::span_id("muse-fixture-session", "tool:b") && row.metadata.as_ref().and_then(|m|m.get("tool_outcome")) == Some(&json!("success")))));
        let rows = t
            .handle(
                &event(
                    "PostToolUseFailure",
                    json!({"turn_id":"t1","tool_use_id":"a","error":"file absent"}),
                    1021,
                ),
                &context(),
            )
            .unwrap();
        assert!(rows.iter().any(|op| matches!(op, SpanOp::Merge(row) if row.error.as_deref() == Some("file absent") && row.metadata.as_ref().and_then(|m|m.get("tool_outcome")) == Some(&json!("error")))));
        assert!(t.handle(&event("PostToolBatch",json!({"turn_id":"t1","tool_calls":[{"tool_use_id":"a","tool_response":"ignored"},{"tool_use_id":"b","tool_response":"ignored"}]}),1022), &context()).unwrap().is_empty());
        assert!(t
            .handle(
                &event(
                    "PreToolUse",
                    json!({"turn_id":"t1","tool_use_id":"a"}),
                    1023
                ),
                &context()
            )
            .unwrap()
            .is_empty());
    }
    #[test]
    fn permission_is_an_observation_and_compaction_pairs_only_one_start() {
        let mut t = translator();
        let rows = t.handle(&event("PermissionRequest",json!({"turn_id":"t1","tool_name":"shell","tool_input":{"command":"echo hello"}}),1010), &context()).unwrap();
        assert!(rows.iter().any(|op| matches!(op, SpanOp::Insert(row) if row.start_ms == row.end_ms && row.error.is_none() && row.metadata.as_ref().and_then(|m|m.get("tool_outcome")).is_none())));
        let notification = t
            .handle(
                &event(
                    "Notification",
                    json!({"turn_id":"t1","notification_type":"future_notice"}),
                    1011,
                ),
                &context(),
            )
            .unwrap();
        assert!(notification.iter().any(|op| matches!(op, SpanOp::Insert(row) if row.tags.as_deref() == Some(&["notification".into()]))));
        for (name, ts) in [("PreCompact", 1020), ("PostCompact", 1030)] {
            assert_eq!(
                t.handle(
                    &event(name, json!({"turn_id":"t1","trigger":"auto"}), ts),
                    &context()
                )
                .unwrap()
                .len(),
                1
            );
        }
        assert!(t
            .handle(
                &event(
                    "PostCompact",
                    json!({"turn_id":"t1","trigger":"auto"}),
                    1031
                ),
                &context()
            )
            .unwrap()
            .is_empty());
    }
    #[test]
    fn subagent_lifecycle_and_pending_tool_close_once() {
        let mut t = translator();
        let p = json!({"turn_id":"t1","subagent_id":"worker","child_session_id":"child"});
        let rows = t
            .handle(&event("SubagentStart", p.clone(), 1010), &context())
            .unwrap();
        assert!(rows.iter().any(|op| matches!(op, SpanOp::Insert(row) if row.metadata.as_ref().and_then(|m|m.get("child_session_id")) == Some(&json!("child")))));
        assert!(t
            .handle(&event("SubagentStop", p, 1020), &context())
            .unwrap()
            .iter()
            .any(|op| matches!(op, SpanOp::Merge(row) if row.end_ms == Some(1020))));
        t.handle(
            &event(
                "PreToolUse",
                json!({"turn_id":"t1","tool_use_id":"pending"}),
                1030,
            ),
            &context(),
        )
        .unwrap();
        let rows = t.finalize(&context()).unwrap();
        assert_eq!(rows.iter().filter(|op| matches!(op, SpanOp::Merge(row) if row.span_id == ids::span_id("muse-fixture-session","tool:pending") && row.error.is_some())).count(),1);
        assert!(t.finalize(&context()).unwrap().is_empty());
    }
    #[test]
    fn orphan_failure_preserves_duration_and_missing_ids_cannot_collide() {
        let mut t = translator();
        let rows = t.handle(&event("PostToolUseFailure",json!({"turn_id":"t1","tool_use_id":"orphan","error":"failed","duration_ms":20}),1050), &context()).unwrap();
        assert!(rows
            .iter()
            .any(|op| matches!(op, SpanOp::Insert(row) if row.start_ms == Some(1030))));
        let a = t
            .handle(
                &event("PreToolUse", json!({"turn_id":"t1","tool_name":"a"}), 1060),
                &context(),
            )
            .unwrap();
        let b = t
            .handle(
                &event("PreToolUse", json!({"turn_id":"t1","tool_name":"b"}), 1060),
                &context(),
            )
            .unwrap();
        let id = |rows: Vec<SpanOp>| match rows.into_iter().next().unwrap() {
            SpanOp::Insert(row) | SpanOp::Merge(row) => row.span_id,
        };
        assert_ne!(id(a), id(b));
    }
}
