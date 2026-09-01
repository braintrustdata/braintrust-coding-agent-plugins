use bt_daemon::wire::{BackendAuth, Envelope, SessionRoute};
use bt_daemon::{Registry, SessionCtx, SpanOp, SpanRow, SpanType};
use serde_json::json;
use std::collections::HashMap;

fn event(name: &str, ts_ms: i64, payload: serde_json::Value) -> Envelope {
    Envelope {
        source: "opencode".into(),
        source_version: Some("1.1.14".into()),
        plugin_version: Some("0.1.0".into()),
        session_id: "root-session".into(),
        event: name.into(),
        ts_ms,
        managed_run_id: None,
        payload,
        route: None,
        config: None,
        capture: None,
    }
}

fn reduce(ops: Vec<SpanOp>) -> HashMap<String, SpanRow> {
    let mut rows = HashMap::new();
    for op in ops {
        match op {
            SpanOp::Insert(row) => {
                rows.insert(row.span_id.clone(), row);
            }
            SpanOp::Merge(update) => {
                let row: &mut SpanRow = rows.entry(update.span_id.clone()).or_default();
                if update.end_ms.is_some() {
                    row.end_ms = update.end_ms;
                }
                if update.output.is_some() {
                    row.output = update.output;
                }
                if update.error.is_some() {
                    row.error = update.error;
                }
                if update.metadata.is_some() {
                    row.metadata = update.metadata;
                }
            }
        }
    }
    rows
}

#[test]
fn opencode_builds_turn_llm_tool_and_closes_the_session() {
    let registry = Registry::default_agents();
    assert!(registry.sources().contains(&"opencode".to_string()));
    let mut translator = registry.create("opencode", "root-session");
    let ctx = SessionCtx {
        session_id: "root-session".into(),
        config: None,
    };
    let events = vec![
        event(
            "session.created",
            1,
            json!({"properties":{"info":{"id":"native"}}}),
        ),
        event(
            "chat.message",
            2,
            json!({"input":{"sessionID":"native","model":{"modelID":"gpt-5"}},"output":{"parts":[{"type":"text","text":"hello"}]}}),
        ),
        event(
            "tool.execute.before",
            3,
            json!({"input":{"sessionID":"native","callID":"call-1","tool":"read"},"output":{"args":{"path":"README.md"}}}),
        ),
        event(
            "message.part.updated",
            4,
            json!({"properties":{"part":{"sessionID":"native","messageID":"m1","type":"text","text":"world","time":{"end":4}}}}),
        ),
        event(
            "message.updated",
            5,
            json!({"properties":{"info":{"id":"m1","sessionID":"native","role":"assistant","providerID":"openai","modelID":"gpt-5","time":{"created":2,"completed":5},"tokens":{"input":5,"output":2,"reasoning":1,"cache":{"read":3,"write":4}}}}}),
        ),
        event(
            "tool.execute.after",
            6,
            json!({"input":{"sessionID":"native","callID":"call-1","tool":"read"},"result":{"title":"Read","output":"contents"}}),
        ),
        event(
            "session.deleted",
            7,
            json!({"properties":{"sessionID":"native"}}),
        ),
    ];
    let mut ops = Vec::new();
    for event in events {
        ops.extend(translator.handle(&event, &ctx).unwrap());
    }
    let rows = reduce(ops);
    assert_eq!(
        rows.values()
            .filter(|r| r.span_type == SpanType::Task)
            .count(),
        2
    );
    let llm = rows
        .values()
        .find(|r| r.span_type == SpanType::Llm)
        .unwrap();
    assert_eq!(llm.metrics.as_ref().unwrap()["prompt_tokens"], 12);
    assert_eq!(llm.output.as_ref().unwrap()[0]["content"], "world");
    let tool = rows
        .values()
        .find(|r| r.span_type == SpanType::Tool)
        .unwrap();
    assert_eq!(tool.metadata.as_ref().unwrap()["tool_approval"], "approved");
    assert!(rows
        .values()
        .filter(|r| r.span_type == SpanType::Task)
        .all(|r| r.end_ms.is_some()));
}

#[test]
fn opencode_child_sessions_share_the_parent_trace_root() {
    let registry = Registry::default_agents();
    let mut translator = registry.create("opencode", "root-session");
    let ctx = SessionCtx {
        session_id: "root-session".into(),
        config: None,
    };
    let mut ops = translator
        .handle(
            &event(
                "session.created",
                1,
                json!({"properties":{"info":{"id":"parent"}}}),
            ),
            &ctx,
        )
        .unwrap();
    ops.extend(translator.handle(&event("chat.message", 2, json!({"input":{"sessionID":"parent"},"output":{"parts":[{"type":"text","text":"delegate"}]}})), &ctx).unwrap());
    ops.extend(translator.handle(&event("session.created", 3, json!({"properties":{"info":{"id":"child","parentID":"parent","title":"find docs (@research subagent)"}}})), &ctx).unwrap());
    let rows = reduce(ops);
    let parent = rows.values().find(|r| r.name == "OpenCode").unwrap();
    let child = rows
        .values()
        .find(|r| r.name == "research: find docs")
        .unwrap();
    assert_eq!(child.root_span_id, parent.root_span_id);
    assert_eq!(child.parent_span_ids.len(), 1);
}

#[test]
fn opencode_additional_metadata_reaches_roots_without_overriding_session_fields() {
    let registry = Registry::default_agents();
    let mut translator = registry.create("opencode", "root-session");
    let ctx = SessionCtx {
        session_id: "root-session".into(),
        config: Some(
            SessionRoute {
                additional_metadata: Some(json!({"team": "platform", "source": "custom"})),
                ..SessionRoute::default()
            }
            .with_auth(BackendAuth {
                token: "test".into(),
                api_url: None,
                app_url: None,
                org_name: None,
                org_id: None,
            }),
        ),
    };
    let rows = reduce(
        translator
            .handle(
                &event(
                    "session.created",
                    1,
                    json!({"properties":{"info":{"id":"native"}}}),
                ),
                &ctx,
            )
            .unwrap(),
    );
    let root = rows.values().next().unwrap();
    assert_eq!(root.metadata.as_ref().unwrap()["team"], "platform");
    assert_eq!(root.metadata.as_ref().unwrap()["source"], "opencode");
}

#[test]
fn opencode_checkpoint_preserves_session_state_for_later_turns() {
    let registry = Registry::default_agents();
    let mut translator = registry.create("opencode", "root-session");
    let ctx = SessionCtx {
        session_id: "root-session".into(),
        config: None,
    };
    translator
        .handle(
            &event(
                "session.created",
                1,
                json!({"properties":{"info":{"id":"native"}}}),
            ),
            &ctx,
        )
        .unwrap();
    translator
        .handle(
            &event("chat.message", 2, json!({"input":{"sessionID":"native"}})),
            &ctx,
        )
        .unwrap();

    assert!(translator.checkpoint(&ctx).unwrap().is_empty());
    let ops = translator
        .handle(
            &event("chat.message", 3, json!({"input":{"sessionID":"native"}})),
            &ctx,
        )
        .unwrap();
    assert!(ops
        .iter()
        .any(|op| matches!(op, SpanOp::Insert(row) if row.name == "Turn 2")));
    assert!(ops
        .iter()
        .all(|op| !matches!(op, SpanOp::Insert(row) if row.name == "OpenCode")));
}

#[test]
fn opencode_finalization_closes_a_missing_tool_completion() {
    let registry = Registry::default_agents();
    let mut translator = registry.create("opencode", "root-session");
    let ctx = SessionCtx {
        session_id: "root-session".into(),
        config: None,
    };
    for envelope in [
        event("chat.message", 1, json!({"input":{"sessionID":"native"}})),
        event(
            "tool.execute.before",
            2,
            json!({"input":{"sessionID":"native","callID":"call","tool":"read"},"output":{"args":{"path":"x"}}}),
        ),
    ] {
        translator.handle(&envelope, &ctx).unwrap();
    }
    let ops = translator.finalize(&ctx).unwrap();
    assert!(ops.iter().any(|op| matches!(
        op,
        SpanOp::Merge(row) if row.end_ms == Some(2) && row.error.is_some()
    )));
}

#[test]
fn opencode_distinguishes_denied_tools_from_failed_executions() {
    let registry = Registry::default_agents();
    let mut translator = registry.create("opencode", "root-session");
    let ctx = SessionCtx {
        session_id: "root-session".into(),
        config: None,
    };
    let events = [
        event(
            "session.created",
            1,
            json!({"properties":{"info":{"id":"native"}}}),
        ),
        event("chat.message", 2, json!({"input":{"sessionID":"native"}})),
        event(
            "tool.execute.before",
            3,
            json!({"input":{"sessionID":"native","callID":"denied","tool":"read"},"output":{"args":{"path":"secret"}}}),
        ),
        event(
            "permission.asked",
            4,
            json!({"properties":{"permission":{"id":"perm-1","sessionID":"native","title":"Read secret","type":"tool","tool":{"callID":"denied","name":"read","input":{"path":"secret"}}}}}),
        ),
        event(
            "permission.replied",
            5,
            json!({"properties":{"id":"perm-1","reply":"reject"}}),
        ),
        event(
            "tool.execute.after",
            6,
            json!({"input":{"sessionID":"native","callID":"denied","tool":"read"}}),
        ),
        event(
            "tool.execute.before",
            7,
            json!({"input":{"sessionID":"native","callID":"failed","tool":"read"},"output":{"args":{"path":"missing"}}}),
        ),
        event(
            "message.part.updated",
            8,
            json!({"properties":{"part":{"sessionID":"native","messageID":"m","type":"tool","callID":"failed","tool":"read","state":{"status":"error","error":"not found"}}}}),
        ),
        event(
            "tool.execute.after",
            9,
            json!({"input":{"sessionID":"native","callID":"failed","tool":"read"}}),
        ),
    ];
    let mut ops = Vec::new();
    for event in events {
        ops.extend(translator.handle(&event, &ctx).unwrap());
    }
    let rows = reduce(ops);
    let denied = rows
        .values()
        .find(|row| {
            row.metadata
                .as_ref()
                .is_some_and(|m| m["call_id"] == "denied")
        })
        .unwrap();
    assert_eq!(denied.metadata.as_ref().unwrap()["tool_approval"], "denied");
    assert_eq!(denied.metadata.as_ref().unwrap()["permission_id"], "perm-1");
    assert!(denied.error.is_none());
    let failed = rows
        .values()
        .find(|row| {
            row.metadata
                .as_ref()
                .is_some_and(|m| m["call_id"] == "failed")
        })
        .unwrap();
    assert_eq!(
        failed.metadata.as_ref().unwrap()["tool_approval"],
        "approved"
    );
    assert_eq!(failed.error.as_deref(), Some("not found"));
}
