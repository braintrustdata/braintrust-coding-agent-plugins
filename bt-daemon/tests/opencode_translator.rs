use bt_daemon::wire::Envelope;
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
    assert_eq!(tool.metadata.as_ref().unwrap()["tool_outcome"], "success");
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
