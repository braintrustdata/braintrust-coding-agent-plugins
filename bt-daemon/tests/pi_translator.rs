use bt_daemon::wire::Envelope;
use bt_daemon::{Registry, SessionCtx, SpanOp, SpanRow, SpanType};
use serde_json::json;
use std::collections::HashMap;

fn event(name: &str, ts_ms: i64, native: serde_json::Value) -> Envelope {
    Envelope {
        source: "pi".into(),
        source_version: Some("0.80.10".into()),
        session_id: "pi-session".into(),
        event: name.into(),
        ts_ms,
        payload: json!({"event":native,"extension_version":"0.10.0","cwd":"."}),
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
                if update.metadata.is_some() {
                    row.metadata = update.metadata;
                }
                if update.error.is_some() {
                    row.error = update.error;
                }
            }
        }
    }
    rows
}

#[test]
fn pi_builds_turn_llm_tool_compaction_and_shutdown_spans() {
    let registry = Registry::default_agents();
    assert!(registry.sources().contains(&"pi".to_string()));
    let mut translator = registry.create("pi", "pi-session");
    let ctx = SessionCtx {
        session_id: "pi-session".into(),
        config: None,
    };
    let events = vec![
        event("session_start", 1, json!({"reason":"new"})),
        event(
            "before_agent_start",
            2,
            json!({"prompt":"/skill:review inspect"}),
        ),
        event(
            "context",
            3,
            json!({"messages":[{"role":"user","content":"inspect"}]}),
        ),
        event(
            "message_update",
            4,
            json!({"assistantMessageEvent":{"type":"text_delta"}}),
        ),
        event(
            "message_end",
            5,
            json!({"message":{"role":"assistant","provider":"openai","model":"gpt-5","content":[{"type":"thinking","thinking":"reason"},{"type":"text","text":"done"}],"usage":{"input":5,"output":2,"cacheRead":3,"reasoning":1,"totalTokens":11}}}),
        ),
        event(
            "tool_execution_start",
            6,
            json!({"toolCallId":"call-1","toolName":"read","args":{"path":"skills/review/SKILL.md"}}),
        ),
        event(
            "tool_execution_end",
            7,
            json!({"toolCallId":"call-1","toolName":"read","result":"contents","isError":false}),
        ),
        event(
            "session_before_compact",
            8,
            json!({"preparation":{"tokensBefore":100}}),
        ),
        event(
            "session_compact",
            9,
            json!({"compactionEntry":{"summary":"short"}}),
        ),
        event("agent_end", 10, json!({"messages":[]})),
        event("session_shutdown", 11, json!({"reason":"quit"})),
    ];
    let mut ops = Vec::new();
    for event in events {
        ops.extend(translator.handle(&event, &ctx).unwrap());
    }
    let rows = reduce(ops);
    assert_eq!(
        rows.values()
            .filter(|r| r.span_type == SpanType::Llm)
            .count(),
        1
    );
    let llm = rows
        .values()
        .find(|r| r.span_type == SpanType::Llm)
        .unwrap();
    assert_eq!(llm.metrics.as_ref().unwrap()["prompt_tokens"], 8);
    assert_eq!(llm.metrics.as_ref().unwrap()["time_to_first_token"], 0.001);
    let tool = rows
        .values()
        .find(|r| r.span_type == SpanType::Tool)
        .unwrap();
    assert_eq!(tool.name, "skill: review");
    assert_eq!(tool.metadata.as_ref().unwrap()["tool_outcome"], "success");
    assert!(rows.values().any(|r| r.name == "Compaction"));
    assert!(rows
        .values()
        .filter(|r| r.span_type == SpanType::Task)
        .all(|r| r.end_ms.is_some()));
}
