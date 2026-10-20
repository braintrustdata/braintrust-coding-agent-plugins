//! Additional native Muse lifecycle events. Keep hook forwarding free of trace policy.
use super::*;

#[derive(Default)]
pub(super) struct Events {
    open: HashMap<String, OpenOp>,
    closed: RecentSet<String>,
}

impl MuseTranslator {
    pub(super) fn handle_events(&mut self, e: &Envelope, hook_ids: &HookIds) -> Vec<SpanOp> {
        if !self.opened {
            return Vec::new();
        }
        let p = &e.payload;
        match e.event.as_str() {
            "PreToolUse" | "ToolUseStart" => self.start_tool(e, hook_ids, p),
            "PostToolUse" | "PostToolUseFailure" => self.finish_tool(e, hook_ids, p),
            "PostToolBatch" => p
                .get("tool_calls")
                .and_then(Value::as_array)
                .map(|calls| {
                    calls
                        .iter()
                        .flat_map(|call| self.finish_tool(e, hook_ids, call))
                        .collect()
                })
                .unwrap_or_default(),
            "PermissionRequest" | "Notification" => {
                // Muse provides no permission operation id or decision event. Record the
                // observation without claiming to know its duration or approval result.
                let identity = format!("observation:{}:{}:{}", e.event, e.ts_ms, p);
                let key = ids::span_id(&self.session_id, &identity);
                if !self.events.closed.insert(key.clone()) {
                    return Vec::new();
                }
                let mut row = self.event_row(e, hook_ids, &key);
                row.name = e.event.clone();
                row.end_ms = Some(e.ts_ms);
                row.input = p.get("tool_input").cloned();
                row.metadata = Some(selected(
                    p,
                    &["tool_name", "notification_type", "title", "message"],
                ));
                row.tags = Some(vec![if e.event == "Notification" {
                    "notification".into()
                } else {
                    "permission-request".into()
                }]);
                vec![SpanOp::Insert(row)]
            }
            "SubagentStart" | "SubagentStop" => {
                // subagent_id is an agent name (e.g. "skill-reminder"), so
                // reusing it must not conflate multiple child sessions.
                let Some(child) = string(p, "child_session_id") else {
                    return vec![self.unpaired(e, "Muse subagent event has no child identity")];
                };
                let key = format!("subagent:{child}");
                if e.event == "SubagentStart" {
                    let mut row = self.event_row(e, hook_ids, &key);
                    row.name = string(p, "subagent_id").unwrap_or("Subagent").into();
                    row.metadata = Some(selected(p, &["subagent_id", "child_session_id"]));
                    self.open_event(&key, row)
                } else {
                    self.finish_event(
                        &key,
                        e.ts_ms,
                        p.get("last_assistant_message").cloned(),
                        event_error(p),
                        None,
                    )
                }
            }
            "PreCompact" | "PostCompact" => {
                // Compaction is serialized within a turn but has no native operation id.
                // Pair by turn and trigger and retain that limitation on the span.
                let key = format!(
                    "compact:{}:{}",
                    hook_ids.turn().unwrap_or("session"),
                    string(p, "trigger").unwrap_or("unknown")
                );
                if e.event == "PreCompact" {
                    // A later compaction in the same turn is a distinct operation.
                    let key = format!("{key}:{}", e.ts_ms);
                    let mut row = self.event_row(e, hook_ids, &key);
                    row.name = "Context compaction".into();
                    row.tags = Some(vec!["compaction".into()]);
                    row.metadata = Some(
                        json!({"trigger":p.get("trigger"), "native_operation_id_available":false}),
                    );
                    self.open_event(&key, row)
                } else {
                    // Choose only a uniquely matching open start; never guess across
                    // concurrent/malformed starts.
                    let matches: Vec<_> = self
                        .events
                        .open
                        .keys()
                        .filter(|candidate| candidate.starts_with(&format!("{key}:")))
                        .cloned()
                        .collect();
                    if matches.len() == 1 {
                        self.finish_event(&matches[0], e.ts_ms, None, event_error(p), None)
                    } else {
                        Vec::new()
                    }
                }
            }
            _ => Vec::new(),
        }
    }

    fn event_row(&self, e: &Envelope, hook_ids: &HookIds, key: &str) -> SpanRow {
        let parent = self.parent(hook_ids);
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

    fn start_tool(&mut self, e: &Envelope, hook_ids: &HookIds, p: &Value) -> Vec<SpanOp> {
        let Some(id) = string(p, "tool_use_id") else {
            return vec![self.unpaired(e, "Muse tool event has no tool_use_id")];
        };
        let key = format!("tool:{id}");
        let mut row = self.event_row(e, hook_ids, &key);
        row.name = string(p, "tool_name").unwrap_or("tool").into();
        row.span_type = SpanType::Tool;
        row.input = p.get("tool_input").cloned();
        row.metadata = Some(json!({"tool_name":row.name}));
        self.open_event(&key, row)
    }

    fn finish_tool(&mut self, e: &Envelope, hook_ids: &HookIds, p: &Value) -> Vec<SpanOp> {
        let Some(id) = string(p, "tool_use_id") else {
            return vec![self.unpaired(e, "Muse tool result has no tool_use_id")];
        };
        let key = format!("tool:{id}");
        if self.events.closed.contains(&key) {
            return Vec::new();
        }
        let failed = e.event == "PostToolUseFailure";
        let error = event_error(p).or_else(|| failed.then(|| "Muse tool failed".into()));
        let outcome = if error.is_some() {
            Some("error")
        } else if e.event == "PostToolUse" {
            Some("success")
        } else {
            // Batch results may contain unavailable/denied tools without a
            // machine-readable failure field. Their prose is not an outcome API.
            None
        };
        let mut out = Vec::new();
        if !self.events.open.contains_key(&key) {
            out.extend(self.start_tool(e, hook_ids, p));
            if let Some(SpanOp::Insert(row)) = out.first_mut() {
                row.start_ms = p
                    .get("duration_ms")
                    .and_then(Value::as_i64)
                    .filter(|duration| *duration >= 0)
                    .map(|duration| e.ts_ms.saturating_sub(duration))
                    .or(Some(e.ts_ms));
                row.metadata = Some(json!({"tool_name":row.name, "native_start_missing":true}));
            }
        }
        out.extend(self.finish_event(
            &key,
            e.ts_ms,
            p.get("tool_response").cloned(),
            error,
            outcome.map(|outcome| json!({"tool_outcome":outcome})),
        ));
        out
    }

    pub(super) fn close_events(&mut self, ts: i64, error: Option<&str>) -> Vec<SpanOp> {
        let keys: Vec<_> = self.events.open.keys().cloned().collect();
        keys.into_iter()
            .flat_map(|key| self.finish_event(&key, ts, None, error.map(str::to_owned), None))
            .collect()
    }
}

fn string<'a>(p: &'a Value, key: &str) -> Option<&'a str> {
    p.get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
}
fn selected(p: &Value, keys: &[&str]) -> Value {
    Value::Object(
        keys.iter()
            .filter_map(|key| p.get(key).map(|value| ((*key).into(), value.clone())))
            .collect(),
    )
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
