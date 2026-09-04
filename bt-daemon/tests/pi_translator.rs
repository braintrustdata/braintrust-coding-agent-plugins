use bt_daemon::wire::{BackendAuth, Envelope, SessionRoute};
use bt_daemon::{Registry, SessionCtx, SpanOp, SpanRow, SpanType};
use serde_json::json;
use std::collections::HashMap;

fn event(name: &str, ts_ms: i64, native: serde_json::Value) -> Envelope {
    Envelope {
        source: "pi".into(),
        source_version: Some("0.80.10".into()),
        plugin_version: Some("1.0.0".into()),
        session_id: "pi-session".into(),
        event: name.into(),
        ts_ms,
        managed_run_id: None,
        payload: json!({"event":native,"extension_version":"1.0.0","cwd":"."}),
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
                if !update.name.is_empty() {
                    row.name = update.name;
                }
                if update.end_ms.is_some() {
                    row.end_ms = update.end_ms;
                }
                if update.output.is_some() {
                    row.output = update.output;
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
            "before_provider_request",
            3,
            json!({"payload":{"model":"gpt-5","messages":[{"role":"user","content":"inspect"}]}}),
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
            "tool_execution_start",
            8,
            json!({"toolCallId":"call-2","toolName":"write","args":{"path":"readonly"}}),
        ),
        event(
            "tool_execution_end",
            9,
            json!({"toolCallId":"call-2","toolName":"write","result":{"stderr":"permission denied"},"isError":true}),
        ),
        event(
            "session_before_compact",
            10,
            json!({
                "preparation":{
                    "tokensBefore":100,
                    "firstKeptEntryId":"kept-1",
                    "messagesToSummarize":[{"role":"user","content":"large history"}],
                    "turnPrefixMessages":[{"role":"assistant","content":"prefix"}]
                },
                "branchEntries":[{"type":"message","message":{"role":"user","content":"large history"}}]
            }),
        ),
        event(
            "session_compact",
            11,
            json!({"compactionEntry":{"summary":"short","tokensBefore":100,"timestamp":"2026-01-01T00:00:11Z"}}),
        ),
        event("agent_end", 12, json!({"messages":[]})),
        event("before_agent_start", 13, json!({"prompt":"continue"})),
        // Pi normally puts compactionSummary first itself. Omitting it here
        // verifies that session_compact still establishes the active base.
        event(
            "context",
            14,
            json!({"messages":[{"role":"user","content":"continue"}]}),
        ),
        event(
            "message_end",
            15,
            json!({"message":{"role":"assistant","provider":"openai","model":"gpt-5","content":[{"type":"text","text":"continued"}],"usage":{"input":3,"output":1,"totalTokens":4}}}),
        ),
        event("agent_end", 16, json!({"messages":[]})),
        event("session_shutdown", 17, json!({"reason":"quit"})),
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
        2
    );
    let compacted_llm = rows
        .values()
        .find(|r| {
            r.span_type == SpanType::Llm
                && r.input.as_ref().is_some_and(|input| {
                    input
                        .as_array()
                        .and_then(|messages| messages.first())
                        .and_then(|message| message.get("role"))
                        == Some(&json!("compactionSummary"))
                })
        })
        .unwrap();
    assert_eq!(compacted_llm.input.as_ref().unwrap()[0]["summary"], "short");
    assert_eq!(
        compacted_llm.input.as_ref().unwrap()[1]["content"],
        "continue"
    );
    let first_llm = rows
        .values()
        .find(|r| {
            r.span_type == SpanType::Llm
                && r.input
                    .as_ref()
                    .is_some_and(|input| input[0]["content"] == "inspect")
        })
        .unwrap();
    assert_eq!(first_llm.metrics.as_ref().unwrap()["prompt_tokens"], 8);
    assert_eq!(
        first_llm.metrics.as_ref().unwrap()["time_to_first_token"],
        0.001
    );
    assert_eq!(
        first_llm.metadata.as_ref().unwrap()["provider_request"]["payload"]["model"],
        "gpt-5"
    );
    assert!(
        first_llm.metadata.as_ref().unwrap()["provider_request"]["payload"]
            .get("messages")
            .is_none()
    );
    let tool = rows.values().find(|r| r.name == "skill: review").unwrap();
    assert_eq!(tool.name, "skill: review");
    assert_eq!(tool.metadata.as_ref().unwrap()["tool_approval"], "approved");
    let failed_tool = rows.values().find(|r| r.name == "write").unwrap();
    assert_eq!(failed_tool.error.as_deref(), Some("permission denied"));
    assert!(rows.values().any(|r| r.name == "Compaction"));
    let compaction = rows.values().find(|r| r.name == "Compaction").unwrap();
    assert_eq!(
        compaction.input.as_ref().unwrap()["messagesToSummarizeCount"],
        1
    );
    assert_eq!(
        compaction.input.as_ref().unwrap()["turnPrefixMessagesCount"],
        1
    );
    assert_eq!(compaction.input.as_ref().unwrap()["branchEntryCount"], 1);
    assert!(compaction
        .input
        .as_ref()
        .unwrap()
        .get("preparation")
        .is_none());
    assert!(compaction
        .input
        .as_ref()
        .unwrap()
        .get("branchEntries")
        .is_none());
    assert!(rows
        .values()
        .filter(|r| r.span_type == SpanType::Task)
        .all(|r| r.end_ms.is_some()));
}

#[test]
fn pi_additional_metadata_reaches_roots_without_overriding_session_fields() {
    let registry = Registry::default_agents();
    let mut translator = registry.create("pi", "pi-session");
    let ctx = SessionCtx {
        session_id: "pi-session".into(),
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
    let mut event = event("session_start", 1, json!({"reason":"new"}));
    event.payload["trace_settings"] = json!({
        "additional_metadata": {"team": "payload"},
        "parent_span_id": "payload-parent",
        "root_span_id": "payload-root",
    });
    let rows = reduce(translator.handle(&event, &ctx).unwrap());
    let root = rows.values().next().unwrap();
    assert_eq!(root.metadata.as_ref().unwrap()["team"], "platform");
    assert_eq!(root.metadata.as_ref().unwrap()["source"], "pi");
    assert!(root.parent_span_ids.is_empty());
    assert_eq!(root.root_span_id, root.span_id);
}

#[test]
fn pi_checkpoint_preserves_the_open_session_and_turn() {
    let registry = Registry::default_agents();
    let mut translator = registry.create("pi", "pi-session");
    let ctx = SessionCtx {
        session_id: "pi-session".into(),
        config: None,
    };
    translator
        .handle(&event("session_start", 1, json!({"reason":"new"})), &ctx)
        .unwrap();
    translator
        .handle(
            &event("before_agent_start", 2, json!({"prompt":"first"})),
            &ctx,
        )
        .unwrap();

    assert!(translator.checkpoint(&ctx).unwrap().is_empty());
    let ops = translator
        .handle(&event("agent_end", 3, json!({"messages":[]})), &ctx)
        .unwrap();
    assert!(ops.iter().any(
        |op| matches!(op, SpanOp::Merge(row) if row.name.is_empty() && row.end_ms == Some(3))
    ));
    assert!(ops.iter().all(|op| !matches!(
        op,
        SpanOp::Merge(row)
            if row
                .metadata
                .as_ref()
                .is_some_and(|metadata| metadata.get("total_turns").is_some())
    )));
}

#[test]
fn pi_finalization_closes_missing_llm_and_tool_events() {
    let registry = Registry::default_agents();
    let mut translator = registry.create("pi", "pi-session");
    let ctx = SessionCtx {
        session_id: "pi-session".into(),
        config: None,
    };
    for envelope in [
        event("before_agent_start", 1, json!({"prompt":"work"})),
        event("context", 2, json!({"messages":[]})),
        event(
            "tool_execution_start",
            3,
            json!({"toolCallId":"call","toolName":"read","args":{"path":"x"}}),
        ),
    ] {
        translator.handle(&envelope, &ctx).unwrap();
    }
    let ops = translator.finalize(&ctx).unwrap();
    assert!(ops.iter().any(|op| matches!(
        op,
        SpanOp::Insert(row) if row.span_type == SpanType::Llm && row.error.is_some()
    )));
    assert!(ops.iter().any(|op| matches!(
        op,
        SpanOp::Insert(row) if row.span_type == SpanType::Tool && row.error.is_some()
    )));
}
