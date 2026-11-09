#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "Integration test assertions and fixtures fail immediately on unexpected values."
)]

#[path = "support/span_identity.rs"]
mod span_identity;

use braintrust_sdk_rust::{SpanComponents, SpanObjectType};
use bt_daemon::wire::{BackendAuth, Envelope, SessionRoute, TraceDestination};
use bt_daemon::{Registry, SessionCtx, SpanOp, SpanRow, SpanType};
use serde_json::json;
use span_identity::{assert_merges_preserve_insert_identity, assert_turn_lineage};
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
    assert_merges_preserve_insert_identity(&ops);
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
            json!({"properties":{"info":{"id":"m1","sessionID":"native","role":"assistant","providerID":"openai","modelID":"gpt-5","variant":"high","time":{"created":2,"completed":5},"tokens":{"input":5,"output":2,"reasoning":1,"cache":{"read":3,"write":4}}}}}),
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
    assert!(assert_turn_lineage(&ops, |row| row.name.starts_with("Turn ")) >= 3);
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
    assert_eq!(llm.metadata.as_ref().unwrap()["variant"], "high");
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
fn opencode_merges_late_tool_result_attachments_into_closed_tool_span() {
    let mut translator = Registry::default_agents().create("opencode", "root-session");
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
        event(
            "chat.message",
            2,
            json!({"input":{"sessionID":"native"},"output":{"parts":[]}}),
        ),
        event(
            "tool.execute.before",
            3,
            json!({"input":{"sessionID":"native","callID":"call-1","tool":"read"},"output":{"args":{"path":"image.png"}}}),
        ),
        event(
            "tool.execute.after",
            4,
            json!({"input":{"sessionID":"native","callID":"call-1","tool":"read"},"result":{"output":"image contents"}}),
        ),
        event(
            "message.part.updated",
            5,
            json!({"properties":{"part":{"sessionID":"native","messageID":"assistant-1","type":"tool","callID":"call-1","tool":"read","state":{"status":"completed","input":{"path":"image.png"},"output":"image contents","attachments":[{"mime":"image/png","url":"file:///tmp/image.png"}]}}}}),
        ),
    ];

    let ops = events
        .iter()
        .flat_map(|event| translator.handle(event, &ctx).unwrap())
        .collect::<Vec<_>>();
    let tool_id = ops
        .iter()
        .find_map(|op| match op {
            SpanOp::Merge(row) if row.span_type == SpanType::Tool => Some(row.span_id.clone()),
            _ => None,
        })
        .unwrap();
    let late_attachment_merge = ops.iter().find(|op| {
        matches!(op, SpanOp::Merge(row) if row.span_id == tool_id && row.output.as_ref().is_some_and(|output| output.to_string().contains("image_url")))
    });
    assert!(late_attachment_merge.is_some());

    let rows = reduce(ops);
    assert_eq!(
        rows[&tool_id].output,
        Some(json!([
            {"type":"text","text":"image contents"},
            {"type":"image_url","image_url":{"url":"file:///tmp/image.png"}}
        ]))
    );
    assert_eq!(rows[&tool_id].end_ms, Some(4));
}

#[test]
fn opencode_idle_updates_a_resumable_session_root() {
    let registry = Registry::default_agents();
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
            json!({"input":{"sessionID":"native"},"output":{"parts":[{"type":"text","text":"first"}]}}),
        ),
        event(
            "message.part.updated",
            3,
            json!({"properties":{"part":{"sessionID":"native","messageID":"one","type":"text","text":"one"}}}),
        ),
        event(
            "message.updated",
            4,
            json!({"properties":{"info":{"id":"one","sessionID":"native","role":"assistant","providerID":"openai","modelID":"gpt-5","time":{"created":2,"completed":4},"tokens":{}}}}),
        ),
        event(
            "session.idle",
            5,
            json!({"properties":{"sessionID":"native"}}),
        ),
        event(
            "chat.message",
            10,
            json!({"input":{"sessionID":"native"},"output":{"parts":[{"type":"text","text":"second"}]}}),
        ),
        event(
            "message.part.updated",
            11,
            json!({"properties":{"part":{"sessionID":"native","messageID":"two","type":"text","text":"two"}}}),
        ),
        event(
            "message.updated",
            12,
            json!({"properties":{"info":{"id":"two","sessionID":"native","role":"assistant","providerID":"openai","modelID":"gpt-5","time":{"created":10,"completed":12},"tokens":{}}}}),
        ),
        event(
            "session.idle",
            13,
            json!({"properties":{"sessionID":"native"}}),
        ),
    ];
    let mut ops = Vec::new();
    for event in events {
        ops.extend(translator.handle(&event, &ctx).unwrap());
    }
    let reopened_roots = ops
        .iter()
        .filter(|op| matches!(op, SpanOp::Merge(row) if row.name.is_empty() && row.end_ms.is_none() && row.parent_span_ids.is_empty()))
        .count();
    assert_eq!(reopened_roots, 1);
    let rows = reduce(ops);
    assert!(rows
        .values()
        .filter(|row| row.span_type == SpanType::Llm)
        .all(|row| row.metadata.as_ref().unwrap().get("variant").is_none()));
    let root = rows.values().find(|row| row.name == "OpenCode").unwrap();
    assert_eq!(root.metadata.as_ref().unwrap()["total_turns"], 2);
    assert_eq!(root.end_ms, Some(13));
    assert_eq!(
        rows.values()
            .filter(|row| row.name.starts_with("Turn "))
            .count(),
        2
    );
}

#[test]
fn opencode_compaction_replaces_old_prefix_and_preserves_native_tail() {
    let registry = Registry::default_agents();
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
            json!({"input":{"sessionID":"native"},"output":{"message":{"id":"old-user"},"parts":[{"type":"text","text":"old question"}]}}),
        ),
        event(
            "message.part.updated",
            3,
            json!({"properties":{"part":{"sessionID":"native","messageID":"old-assistant","type":"text","text":"old answer","time":{"end":3}}}}),
        ),
        event(
            "message.updated",
            4,
            json!({"properties":{"info":{"id":"old-assistant","sessionID":"native","role":"assistant","providerID":"openai","modelID":"gpt-5","time":{"created":2,"completed":4},"tokens":{}}}}),
        ),
        event(
            "chat.message",
            5,
            json!({"input":{"sessionID":"native"},"output":{"message":{"id":"recent-user"},"parts":[{"type":"text","text":"recent question"}]}}),
        ),
        event(
            "message.part.updated",
            6,
            json!({"properties":{"part":{"sessionID":"native","messageID":"recent-assistant","type":"text","text":"recent answer"}}}),
        ),
        event(
            "message.part.updated",
            7,
            json!({"properties":{"part":{"sessionID":"native","messageID":"recent-assistant","type":"tool","callID":"tool-1","tool":"read","state":{"status":"completed","input":{"path":"README.md"},"output":"contents"}}}}),
        ),
        event(
            "message.updated",
            8,
            json!({"properties":{"info":{"id":"recent-assistant","sessionID":"native","role":"assistant","providerID":"openai","modelID":"gpt-5","time":{"created":5,"completed":8},"tokens":{}}}}),
        ),
        event(
            "message.updated",
            9,
            json!({"properties":{"info":{"id":"compact-trigger","sessionID":"native","role":"user","time":{"created":9}}}}),
        ),
        event(
            "message.part.updated",
            10,
            json!({"properties":{"part":{"id":"compaction-part","sessionID":"native","messageID":"compact-trigger","type":"compaction","auto":true,"overflow":false,"tail_start_id":"recent-user"}}}),
        ),
        event(
            "message.part.updated",
            11,
            json!({"properties":{"part":{"sessionID":"native","messageID":"compact-summary","type":"text","text":"summary of old work","time":{"end":11}}}}),
        ),
        event(
            "message.updated",
            12,
            json!({"properties":{"info":{"id":"compact-summary","parentID":"compact-trigger","sessionID":"native","role":"assistant","mode":"compaction","summary":true,"providerID":"openai","modelID":"gpt-5","time":{"created":10,"completed":12},"tokens":{}}}}),
        ),
        event(
            "session.compacted",
            13,
            json!({"properties":{"sessionID":"native"}}),
        ),
        event(
            "message.updated",
            14,
            json!({"properties":{"info":{"id":"continue-user","sessionID":"native","role":"user","time":{"created":14}}}}),
        ),
        event(
            "message.part.updated",
            15,
            json!({"properties":{"part":{"sessionID":"native","messageID":"continue-user","type":"text","text":"Continue if there are next steps","synthetic":true}}}),
        ),
        event(
            "message.part.updated",
            16,
            json!({"properties":{"part":{"sessionID":"native","messageID":"new-assistant","type":"text","text":"new answer","time":{"end":16}}}}),
        ),
        event(
            "message.updated",
            17,
            json!({"properties":{"info":{"id":"new-assistant","sessionID":"native","role":"assistant","providerID":"openai","modelID":"gpt-5","time":{"created":15,"completed":17},"tokens":{}}}}),
        ),
    ];
    let mut ops = Vec::new();
    for event in events {
        ops.extend(translator.handle(&event, &ctx).unwrap());
    }
    let rows = reduce(ops);
    let llm = rows
        .values()
        .find(|row| {
            row.span_type == SpanType::Llm
                && row.metadata.as_ref().unwrap()["message_id"] == "new-assistant"
        })
        .unwrap();
    let messages = llm.input.as_ref().unwrap().as_array().unwrap();
    assert_eq!(messages.len(), 5);
    assert_eq!(messages[0]["message_type"], "compaction_summary");
    assert_eq!(messages[0]["content"], "summary of old work");
    assert_eq!(messages[1]["content"], "recent question");
    assert_eq!(messages[2]["content"], "recent answer");
    assert_eq!(messages[2]["tool_calls"][0]["id"], "tool-1");
    assert_eq!(messages[3]["role"], "tool");
    assert_eq!(messages[3]["content"], "contents");
    assert_eq!(messages[4]["content"], "Continue if there are next steps");
    assert!(!messages.iter().any(|message| {
        matches!(
            message.get("content").and_then(serde_json::Value::as_str),
            Some("old question" | "old answer")
        )
    }));
}

#[test]
fn opencode_child_sessions_share_the_parent_trace_root() {
    let registry = Registry::default_agents();
    let mut translator = registry.create("opencode", "root-session");
    let ctx = SessionCtx {
        session_id: "root-session".into(),
        config: Some(
            SessionRoute {
                tags: vec!["nested".into()],
                additional_metadata: Some(
                    json!({"team":"pipeline", "source":"override", "_bt_private":true}),
                ),
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
    ops.extend(
        translator
            .handle(
                &event(
                    "session.deleted",
                    4,
                    json!({"properties":{"sessionID":"child"}}),
                ),
                &ctx,
            )
            .unwrap(),
    );
    assert!(assert_turn_lineage(&ops, |row| row.name.starts_with("Turn ")) >= 2);
    let roots: Vec<_> = ops
        .iter()
        .filter_map(|op| match op {
            SpanOp::Insert(row) if row.span_type == SpanType::Task && !row.is_turn => Some(row),
            _ => None,
        })
        .collect();
    assert_eq!(roots.len(), 2);
    for root in roots {
        let metadata = root.metadata.as_ref().unwrap();
        assert_eq!(metadata["team"], "pipeline");
        assert_eq!(metadata["source"], "opencode");
        assert!(metadata.get("_bt_private").is_none());
    }
    let rows = reduce(ops);
    let parent = rows.values().find(|r| r.name == "OpenCode").unwrap();
    let child = rows
        .values()
        .find(|r| r.name == "research: find docs")
        .unwrap();
    assert_eq!(child.root_span_id, parent.root_span_id);
    assert_eq!(child.parent_span_ids.len(), 1);
    for root in [parent, child] {
        assert_eq!(root.tags.as_ref().unwrap(), &["nested"]);
    }
}

#[test]
fn opencode_additional_metadata_reaches_roots_without_overriding_session_fields() {
    let registry = Registry::default_agents();
    let mut translator = registry.create("opencode", "root-session");
    let mut components = SpanComponents::new(SpanObjectType::ProjectLogs);
    components.span_id = Some("external-parent".into());
    components.root_span_id = Some("external-root".into());
    let ctx = SessionCtx {
        session_id: "root-session".into(),
        config: Some(
            SessionRoute {
                destination: Some(TraceDestination::ParentSpan { components }),
                additional_metadata: Some(json!({"team": "platform", "source": "custom"})),
                tags: vec!["ci".into(), "docs".into()],
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
    let mut ops = translator
        .handle(
            &event(
                "session.created",
                1,
                json!({"properties":{"info":{"id":"native"}}}),
            ),
            &ctx,
        )
        .unwrap();
    ops.extend(
        translator
            .handle(
                &event(
                    "session.deleted",
                    2,
                    json!({"properties":{"sessionID":"native"}}),
                ),
                &ctx,
            )
            .unwrap(),
    );
    let inserted_root = ops
        .iter()
        .find_map(|op| match op {
            SpanOp::Insert(row) if row.name == "OpenCode" => Some(row),
            _ => None,
        })
        .unwrap();
    assert_eq!(inserted_root.metadata.as_ref().unwrap()["team"], "platform");
    assert_eq!(
        inserted_root.metadata.as_ref().unwrap()["source"],
        "opencode"
    );
    assert!(inserted_root
        .metadata
        .as_ref()
        .unwrap()
        .get("username")
        .is_some());
    let rows = reduce(ops);
    let root = rows.values().next().unwrap();
    assert_eq!(root.root_span_id, "external-root");
    assert_eq!(root.parent_span_ids, ["external-parent"]);
    assert_eq!(root.tags, Some(vec!["ci".into(), "docs".into()]));
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
    let mut ops = Vec::new();
    for envelope in [
        event("chat.message", 1, json!({"input":{"sessionID":"native"}})),
        event(
            "tool.execute.before",
            2,
            json!({"input":{"sessionID":"native","callID":"call","tool":"read"},"output":{"args":{"path":"x"}}}),
        ),
        event("chat.message", 3, json!({"input":{"sessionID":"native"}})),
    ] {
        ops.extend(translator.handle(&envelope, &ctx).unwrap());
    }
    ops.extend(translator.finalize(&ctx).unwrap());
    assert_merges_preserve_insert_identity(&ops);
    assert!(ops.iter().any(|op| matches!(
        op,
        SpanOp::Merge(row) if row.end_ms == Some(3) && row.error.is_some()
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
            json!({"properties":{"requestID":"perm-1","sessionID":"native","title":"Read secret","permission":"read","tool":{"callID":"denied","input":{"path":"secret"}}}}),
        ),
        event(
            "permission.replied",
            5,
            json!({"properties":{"requestID":"perm-1","reply":"reject"}}),
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
    assert_eq!(denied.metadata.as_ref().unwrap()["tool_name"], "read");
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

#[test]
fn opencode_permission_dtos_accept_wrapped_request_and_info_reply() {
    let registry = Registry::default_agents();
    let mut translator = registry.create("opencode", "root-session");
    let ctx = SessionCtx {
        session_id: "root-session".into(),
        config: None,
    };
    let mut ops = Vec::new();
    for envelope in [
        event(
            "session.created",
            1,
            json!({"properties":{"info":{"id":"native"}}}),
        ),
        event("chat.message", 2, json!({"input":{"sessionID":"native"}})),
        event(
            "permission.asked",
            3,
            json!({"properties":{"permission":{"id":"perm","sessionID":"native","tool":{"callID":"call","name":"read","input":{"path":"secret"}},"permissionType":"filesystem","futurePermissionField":true}}}),
        ),
        event(
            "permission.replied",
            4,
            json!({"properties":{"info":{"id":"perm"},"response":"denied"}}),
        ),
    ] {
        ops.extend(translator.handle(&envelope, &ctx).unwrap());
    }
    let rows = reduce(ops);
    let denied = rows
        .values()
        .find(|row| {
            row.metadata
                .as_ref()
                .is_some_and(|metadata| metadata["permission_id"] == "perm")
        })
        .unwrap();
    assert_eq!(denied.metadata.as_ref().unwrap()["tool_approval"], "denied");
    assert_eq!(
        denied.metadata.as_ref().unwrap()["permission_type"],
        "filesystem"
    );
    assert_eq!(denied.input.as_ref().unwrap()["path"], "secret");
}

#[test]
fn opencode_ignores_unknown_fields_at_typed_reducer_boundaries() {
    let registry = Registry::default_agents();
    let mut translator = registry.create("opencode", "root-session");
    let ctx = SessionCtx {
        session_id: "root-session".into(),
        config: None,
    };
    let mut ops = Vec::new();
    for envelope in [
        event(
            "session.created",
            1,
            json!({"properties":{"info":{"id":"native"}}}),
        ),
        event(
            "chat.message",
            2,
            json!({"input":{"sessionID":"native"},"output":{"parts":[{"type":"text","text":"hello"}]}}),
        ),
        event(
            "tool.execute.before",
            3,
            json!({"input":{"sessionID":"native","callID":"call","tool":"read","futureToolField":true},"output":{"args":{"path":"README.md","futureArgField":true}},"futureEnvelopeField":true}),
        ),
        event(
            "message.part.updated",
            4,
            json!({"properties":{"part":{"sessionID":"native","messageID":"message","type":"text","text":"done","time":{"end":4},"futurePartField":true},"futurePropertiesField":true}}),
        ),
        event(
            "message.updated",
            5,
            json!({"properties":{"info":{"id":"message","sessionID":"native","role":"assistant","modelID":"gpt-5","time":{"completed":5},"tokens":{"input":1,"output":1,"futureTokenField":true},"futureInfoField":true}}}),
        ),
        event(
            "tool.execute.after",
            6,
            json!({"input":{"sessionID":"native","callID":"call","tool":"read"},"result":{"output":"ok","futureResultField":true}}),
        ),
    ] {
        ops.extend(translator.handle(&envelope, &ctx).unwrap());
    }
    let rows = reduce(ops);
    assert!(rows.values().any(|row| row.span_type == SpanType::Llm));
    assert!(rows.values().any(|row| row.span_type == SpanType::Tool));
}

#[test]
fn opencode_ignores_malformed_typed_events_without_failing_the_session() {
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
            &event(
                "chat.message",
                2,
                json!({"input":{"sessionID":"native"},"output":{"parts":[]}}),
            ),
            &ctx,
        )
        .unwrap();

    let ops = translator
        .handle(
            &event(
                "tool.execute.before",
                3,
                json!({"input":{"sessionID":"native","callID":42,"tool":"read"}}),
            ),
            &ctx,
        )
        .unwrap();
    assert!(ops
        .iter()
        .all(|op| !matches!(op, SpanOp::Insert(row) if row.span_type == SpanType::Tool)));
    assert!(translator
        .handle(
            &event(
                "session.deleted",
                4,
                json!({"properties":{"sessionID":"native"}})
            ),
            &ctx,
        )
        .unwrap()
        .iter()
        .any(|op| matches!(op, SpanOp::Merge(row) if row.end_ms == Some(4))));
}

#[test]
fn opencode_accepts_flattened_tool_hook_payloads() {
    let registry = Registry::default_agents();
    let mut translator = registry.create("opencode", "root-session");
    let ctx = SessionCtx {
        session_id: "root-session".into(),
        config: None,
    };
    let mut ops = Vec::new();
    for envelope in [
        event(
            "session.created",
            1,
            json!({"properties":{"info":{"id":"native"}}}),
        ),
        event(
            "chat.message",
            2,
            json!({"input":{"sessionID":"native"},"output":{"parts":[]}}),
        ),
        event(
            "tool.execute.before",
            3,
            json!({"sessionID":"native","callID":"call","tool":"read","output":{"args":{"path":"README.md"}}}),
        ),
        event(
            "tool.execute.after",
            4,
            json!({"sessionID":"native","callID":"call","tool":"read","output":"contents"}),
        ),
    ] {
        ops.extend(translator.handle(&envelope, &ctx).unwrap());
    }
    let rows = reduce(ops);
    let tool = rows
        .values()
        .find(|row| row.span_type == SpanType::Tool)
        .unwrap();
    assert_eq!(tool.input.as_ref().unwrap()["path"], "README.md");
    assert_eq!(tool.output.as_ref().unwrap(), "contents");
    assert_eq!(tool.end_ms, Some(4));
}

/// `session.idle` fires after every turn and merges the root summary while the
/// session stays resumable. That merge must keep the attached parent — the
/// sibling close-root arm already does.
#[test]
fn opencode_idle_root_merge_keeps_external_parent() {
    let registry = Registry::default_agents();
    let mut translator = registry.create("opencode", "root-session");
    let mut components = SpanComponents::new(SpanObjectType::ProjectLogs);
    components.span_id = Some("external-parent".into());
    components.root_span_id = Some("external-root".into());
    let ctx = SessionCtx {
        session_id: "root-session".into(),
        config: Some(
            SessionRoute {
                destination: Some(TraceDestination::ParentSpan { components }),
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
    let mut ops = translator
        .handle(
            &event(
                "session.created",
                1,
                json!({"properties":{"info":{"id":"native"}}}),
            ),
            &ctx,
        )
        .unwrap();
    ops.extend(
        translator
            .handle(
                &event("chat.message", 2, json!({"input":{"sessionID":"native"}})),
                &ctx,
            )
            .unwrap(),
    );
    // Idle, not deleted: the session stays resumable for the next turn.
    ops.extend(
        translator
            .handle(
                &event(
                    "session.idle",
                    3,
                    json!({"properties":{"sessionID":"native"}}),
                ),
                &ctx,
            )
            .unwrap(),
    );

    let root_span_id = ops
        .iter()
        .find_map(|op| match op {
            SpanOp::Insert(row) if row.name == "OpenCode" => Some(row.span_id.clone()),
            _ => None,
        })
        .expect("root insert");
    let idle_merge = ops
        .iter()
        .filter_map(|op| match op {
            SpanOp::Merge(row) if row.span_id == root_span_id => Some(row),
            _ => None,
        })
        .next_back()
        .expect("idle root merge");
    assert_eq!(
        idle_merge.parent_span_ids,
        ["external-parent"],
        "idle root merge dropped the external parent"
    );
    assert_merges_preserve_insert_identity(&ops);
}

fn v2_translator() -> (Box<dyn bt_daemon::AgentTranslator>, SessionCtx) {
    (
        Registry::default_agents().create("opencode", "root-session"),
        SessionCtx {
            session_id: "root-session".into(),
            config: None,
        },
    )
}

#[test]
fn opencode_v2_reduces_native_text_session_without_session_created() {
    let (mut translator, ctx) = v2_translator();
    // Shapes observed from OpenCode 2.2.5 with a local mock model.
    let events = [
        (
            "session.inbox.enqueued",
            1,
            json!({"sessionID":"native", "inboxID":"user-1", "item":{"type":"user", "payload":{"text":"hello", "files":[],"skills":[{"id":"s1","name":"review"}]}, "delivery":"steer"}}),
        ),
        (
            "session.inbox.delivered",
            2,
            json!({"sessionID":"native", "inboxID":"user-1"}),
        ),
        (
            "session.step.started",
            3,
            json!({"sessionID":"native", "assistantMessageID":"assistant-1", "model":{"id":"mock-model", "providerID":"mock","variant":"high"}, "started":2}),
        ),
        (
            "session.text.ended",
            4,
            json!({"sessionID":"native", "assistantMessageID":"assistant-1", "ordinal":0, "text":"world"}),
        ),
        (
            "session.text.ended",
            5,
            json!({"sessionID":"native", "assistantMessageID":"assistant-1", "ordinal":1, "text":"!"}),
        ),
        (
            "session.step.ended",
            6,
            json!({"sessionID":"native", "assistantMessageID":"assistant-1", "finish":"stop", "tokens":{"input":3,"output":2,"reasoning":0,"cache":{"read":0,"write":0}}}),
        ),
        (
            "session.execution.succeeded",
            7,
            json!({"sessionID":"native"}),
        ),
    ];
    let mut ops = Vec::new();
    for (name, ts, data) in events {
        let mut e = event(
            name,
            ts,
            json!({"id":format!("event-{ts}"),"type":name,"data":data}),
        );
        e.source_version = Some("2.2.5".into());
        ops.extend(translator.handle(&e, &ctx).unwrap());
        assert!(translator.handle(&e, &ctx).unwrap().is_empty());
    }
    let rows = reduce(ops);
    let turn = rows.values().find(|r| r.name == "Turn 1").unwrap();
    assert_eq!(turn.input, Some(json!("hello")));
    assert_eq!(
        turn.metadata.as_ref().unwrap()["loaded_skill_names"],
        json!(["review"])
    );
    let llm = rows
        .values()
        .find(|r| r.span_type == SpanType::Llm)
        .unwrap();
    assert_eq!(llm.name, "mock/mock-model");
    assert_eq!(llm.metadata.as_ref().unwrap()["variant"], "high");
    assert_eq!(
        llm.output,
        Some(json!([{"role":"assistant","content":"world!"}]))
    );
    assert_eq!(llm.metrics.as_ref().unwrap()["prompt_tokens"], 3);
    assert_eq!(llm.parent_span_ids, vec![turn.span_id.clone()]);
}

#[test]
fn opencode_v2_emits_tool_span_from_native_events() {
    let (mut translator, ctx) = v2_translator();
    let events = [
        (
            "session.inbox.enqueued",
            json!({"sessionID":"native","inboxID":"u","item":{"type":"user","payload":{"text":"read"}}}),
        ),
        (
            "session.inbox.delivered",
            json!({"sessionID":"native","inboxID":"u"}),
        ),
        (
            "session.tool.input.started",
            json!({"sessionID":"native","assistantMessageID":"m","id":"call","name":"read"}),
        ),
        (
            "session.tool.called",
            json!({"sessionID":"native","assistantMessageID":"m","id":"call","input":{"path":"README.md"},"executed":false}),
        ),
        (
            "session.tool.success",
            json!({"sessionID":"native","assistantMessageID":"m","id":"call","content":[{"type":"text","text":"contents"}],"executed":true}),
        ),
    ];
    let mut ops = Vec::new();
    for (i, (name, data)) in events.into_iter().enumerate() {
        let e = v2_event(name, i as i64 + 1, data);
        ops.extend(translator.handle(&e, &ctx).unwrap());
    }
    let rows = reduce(ops);
    let tool = rows
        .values()
        .find(|r| r.span_type == SpanType::Tool)
        .unwrap();
    assert_eq!(tool.name, "read");
    assert_eq!(tool.input, Some(json!({"path":"README.md"})));
    assert_eq!(
        tool.output,
        Some(json!([{"type":"text","text":"contents"}]))
    );
    assert!(tool.end_ms.is_some());
}

#[test]
fn opencode_v2_preserves_attached_images_in_turn_and_model_input() {
    let (mut translator, ctx) = v2_translator();
    let events = [
        (
            "session.inbox.enqueued",
            json!({"sessionID":"native","inboxID":"u","item":{"type":"user","payload":{"text":"describe","files":[{"mime":"image/png","data":"YWJj","source":{"type":"inline"}}]}}}),
        ),
        (
            "session.inbox.delivered",
            json!({"sessionID":"native","inboxID":"u"}),
        ),
        (
            "session.step.started",
            json!({"sessionID":"native","assistantMessageID":"m","model":{"id":"model","providerID":"mock"},"started":3}),
        ),
        (
            "session.text.ended",
            json!({"sessionID":"native","assistantMessageID":"m","ordinal":0,"text":"red"}),
        ),
        (
            "session.step.ended",
            json!({"sessionID":"native","assistantMessageID":"m","tokens":{"input":1,"output":1}}),
        ),
    ];
    let mut ops = Vec::new();
    for (i, (name, data)) in events.into_iter().enumerate() {
        let e = v2_event(name, i as i64 + 1, data);
        ops.extend(translator.handle(&e, &ctx).unwrap());
    }
    let rows = reduce(ops);
    let turn = rows.values().find(|r| r.name == "Turn 1").unwrap();
    let expected = json!([{"type":"text","text":"describe"},{"type":"image_url","image_url":{"url":"data:image/png;base64,YWJj"}}]);
    assert_eq!(turn.input, Some(expected.clone()));
    let llm = rows
        .values()
        .find(|r| r.span_type == SpanType::Llm)
        .unwrap();
    assert_eq!(llm.input.as_ref().unwrap()[0]["content"], expected);
}

#[test]
fn opencode_v2_resume_keeps_the_first_native_event_after_synthetic_start() {
    let (mut translator, ctx) = v2_translator();
    let mut ops = Vec::new();
    for (i, name, data) in [
        (
            1,
            "session.inbox.enqueued",
            json!({"sessionID":"native","inboxID":"u1","item":{"type":"user","payload":{"text":"first"}}}),
        ),
        (
            2,
            "session.inbox.delivered",
            json!({"sessionID":"native","inboxID":"u1"}),
        ),
        (
            3,
            "session.execution.succeeded",
            json!({"sessionID":"native"}),
        ),
        (
            4,
            "session_start",
            json!({"sessionID":"native","inboxID":"u2","item":{"type":"user","payload":{"text":"second"}}}),
        ),
        (
            4,
            "session.inbox.enqueued",
            json!({"sessionID":"native","inboxID":"u2","item":{"type":"user","payload":{"text":"second"}}}),
        ),
        (
            5,
            "session.inbox.delivered",
            json!({"sessionID":"native","inboxID":"u2"}),
        ),
    ] {
        let mut e = event(name, i, json!({"id":format!("event-{i}"),"data":data}));
        e.source_version = Some("2.2.5".into());
        ops.extend(translator.handle(&e, &ctx).unwrap());
    }
    let rows = reduce(ops);
    assert!(rows
        .values()
        .any(|row| row.name == "Turn 2" && row.input == Some(json!("second"))));
}

#[test]
fn opencode_v2_compaction_preserves_native_model_context_after_resume() {
    let (mut translator, ctx) = v2_translator();
    let events = [
        (
            1,
            "session.inbox.enqueued",
            json!({"sessionID":"native","inboxID":"u1","item":{"type":"user","payload":{"text":"old"}}}),
        ),
        (
            2,
            "session.inbox.delivered",
            json!({"sessionID":"native","inboxID":"u1"}),
        ),
        (
            3,
            "session.compaction.started",
            json!({"sessionID":"native","reason":"manual","inputID":"c1","recent":""}),
        ),
        (
            4,
            "session.compaction.ended",
            json!({"sessionID":"native","reason":"manual","model":{"providerID":"mock","id":"model"},"text":"summary","recent":""}),
        ),
        (
            5,
            "session.execution.succeeded",
            json!({"sessionID":"native"}),
        ),
        (
            6,
            "session_start",
            json!({"sessionID":"native","inboxID":"u2","item":{"type":"user","payload":{"text":"next"}}}),
        ),
        (
            6,
            "session.inbox.enqueued",
            json!({"sessionID":"native","inboxID":"u2","item":{"type":"user","payload":{"text":"next"}}}),
        ),
        (
            7,
            "session.inbox.delivered",
            json!({"sessionID":"native","inboxID":"u2"}),
        ),
        (
            8,
            "model.context",
            json!({"sessionID":"native","system":["system"],"messages":[{"role":"user","content":[{"type":"text","text":"summary"}]},{"role":"user","content":[{"type":"text","text":"next"}]}]}),
        ),
        (
            9,
            "session.step.started",
            json!({"sessionID":"native","assistantMessageID":"m2","model":{"providerID":"mock","id":"model"},"started":9}),
        ),
        (
            10,
            "session.text.ended",
            json!({"sessionID":"native","assistantMessageID":"m2","ordinal":0,"text":"answer"}),
        ),
        (
            11,
            "session.step.ended",
            json!({"sessionID":"native","assistantMessageID":"m2","tokens":{"input":2,"output":1}}),
        ),
    ];
    let mut ops = Vec::new();
    for (i, name, data) in events {
        let mut e = event(name, i, json!({"id":format!("event-{i}"),"data":data}));
        e.source_version = Some("2.0.0".into());
        ops.extend(translator.handle(&e, &ctx).unwrap());
    }
    let rows = reduce(ops);
    let llm = rows
        .values()
        .find(|row| {
            row.metadata
                .as_ref()
                .is_some_and(|metadata| metadata["message_id"] == "m2")
        })
        .unwrap();
    assert_eq!(
        llm.input,
        Some(
            json!([{"role":"system","content":"system"},{"role":"user","content":"summary"},{"role":"user","content":"next"}])
        )
    );
}

#[test]
fn opencode_v2_distinguishes_denied_and_failed_tools() {
    let (mut translator, ctx) = v2_translator();
    let events = [
        (
            "session.inbox.enqueued",
            json!({"sessionID":"native","inboxID":"u","item":{"type":"user","payload":{"text":"run"}}}),
        ),
        (
            "session.inbox.delivered",
            json!({"sessionID":"native","inboxID":"u"}),
        ),
        (
            "session.tool.input.started",
            json!({"sessionID":"native","assistantMessageID":"m","id":"denied","name":"read"}),
        ),
        (
            "session.tool.called",
            json!({"sessionID":"native","assistantMessageID":"m","id":"denied","input":{"path":"secret"}}),
        ),
        (
            "permission.asked",
            json!({"sessionID":"native","id":"p1","action":"read","source":{"id":"denied"}}),
        ),
        (
            "permission.replied",
            json!({"sessionID":"native","requestID":"p1","reply":"reject"}),
        ),
        (
            "session.tool.failed",
            json!({"sessionID":"native","assistantMessageID":"m","id":"denied","error":{"type":"permission.rejected","message":"denied"}}),
        ),
        (
            "session.tool.input.started",
            json!({"sessionID":"native","assistantMessageID":"m","id":"failed","name":"read"}),
        ),
        (
            "session.tool.called",
            json!({"sessionID":"native","assistantMessageID":"m","id":"failed","input":{"path":"missing"}}),
        ),
        (
            "session.tool.failed",
            json!({"sessionID":"native","assistantMessageID":"m","id":"failed","error":{"type":"error","message":"not found"}}),
        ),
    ];
    let mut ops = Vec::new();
    for (i, (name, data)) in events.into_iter().enumerate() {
        let e = v2_event(name, i as i64 + 1, data);
        ops.extend(translator.handle(&e, &ctx).unwrap());
    }
    let rows = reduce(ops);
    let tools: Vec<_> = rows
        .values()
        .filter(|row| row.span_type == SpanType::Tool)
        .collect();
    assert_eq!(tools.len(), 2);
    let denied = tools
        .iter()
        .find(|row| {
            row.metadata
                .as_ref()
                .is_some_and(|m| m["call_id"] == "denied")
        })
        .unwrap();
    assert_eq!(denied.metadata.as_ref().unwrap()["tool_approval"], "denied");
    let failed = tools
        .iter()
        .find(|row| {
            row.metadata
                .as_ref()
                .is_some_and(|m| m["call_id"] == "failed")
        })
        .unwrap();
    assert_eq!(failed.error.as_deref(), Some("not found"));
    assert_eq!(
        failed.metadata.as_ref().unwrap()["tool_approval"],
        "approved"
    );
}

#[test]
fn opencode_v2_compaction_keeps_native_tail_without_context_hook() {
    let (mut translator, ctx) = v2_translator();
    let events = [
        (
            "session.inbox.enqueued",
            json!({"sessionID":"s","inboxID":"old","item":{"type":"user","payload":{"text":"old"}}}),
        ),
        (
            "session.inbox.delivered",
            json!({"sessionID":"s","inboxID":"old"}),
        ),
        (
            "session.inbox.enqueued",
            json!({"sessionID":"s","inboxID":"keep","item":{"type":"user","payload":{"text":"keep"}}}),
        ),
        (
            "session.inbox.delivered",
            json!({"sessionID":"s","inboxID":"keep"}),
        ),
        (
            "session.compaction.started",
            json!({"sessionID":"s","inputID":"compact"}),
        ),
        (
            "session.compaction.ended",
            json!({"sessionID":"s","text":"summary"}),
        ),
        (
            "session.step.started",
            json!({"sessionID":"s","assistantMessageID":"m","model":{"id":"model","providerID":"test"}}),
        ),
        (
            "session.text.ended",
            json!({"sessionID":"s","assistantMessageID":"m","ordinal":0,"text":"answer"}),
        ),
        (
            "session.step.ended",
            json!({"sessionID":"s","assistantMessageID":"m"}),
        ),
    ];
    let mut ops = Vec::new();
    for (i, (name, data)) in events.into_iter().enumerate() {
        let mut envelope = event(
            name,
            i as i64 + 1,
            json!({"id":format!("tail-{i}"),"metadata":{"keepID":"keep"},"data":data}),
        );
        envelope.source_version = Some("2.2.5".into());
        ops.extend(translator.handle(&envelope, &ctx).unwrap());
    }
    let rows = reduce(ops);
    let llm = rows
        .values()
        .find(|row| {
            row.metadata
                .as_ref()
                .is_some_and(|m| m["message_id"] == "m")
        })
        .unwrap();
    assert_eq!(
        llm.input,
        Some(
            json!([{"role":"assistant","content":"summary","message_type":"compaction_summary"},{"role":"user","content":"keep"}])
        )
    );
    let compact = rows
        .values()
        .find(|row| {
            row.tags
                .as_ref()
                .is_some_and(|tags| tags.iter().any(|tag| tag == "compaction"))
        })
        .unwrap();
    assert_eq!(
        compact.input,
        Some(json!([{"role":"user","content":"old"}]))
    );
}

#[test]
fn opencode_v2_permission_denied_before_tool_call_creates_one_tool_span() {
    let (mut translator, ctx) = v2_translator();
    let events = [
        (
            "session.inbox.enqueued",
            json!({"sessionID":"s","inboxID":"u","item":{"type":"user","payload":{"text":"read"}}}),
        ),
        (
            "session.inbox.delivered",
            json!({"sessionID":"s","inboxID":"u"}),
        ),
        (
            "session.tool.input.started",
            json!({"sessionID":"s","assistantMessageID":"m","id":"call","name":"read"}),
        ),
        (
            "permission.asked",
            json!({"sessionID":"s","id":"p","action":"read","source":{"id":"call"}}),
        ),
        (
            "permission.replied",
            json!({"sessionID":"s","requestID":"p","reply":"reject"}),
        ),
        (
            "session.tool.failed",
            json!({"sessionID":"s","assistantMessageID":"m","id":"call","error":{"type":"permission.rejected","message":"denied"}}),
        ),
    ];
    let mut ops = Vec::new();
    for (i, (name, data)) in events.into_iter().enumerate() {
        let e = v2_event(name, i as i64 + 1, data);
        ops.extend(translator.handle(&e, &ctx).unwrap());
    }
    let rows = reduce(ops);
    let tools: Vec<_> = rows
        .values()
        .filter(|row| row.span_type == SpanType::Tool)
        .collect();
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0].name, "read");
    assert_eq!(
        tools[0].metadata.as_ref().unwrap()["tool_approval"],
        "denied"
    );
}

#[test]
fn opencode_v2_preserves_provider_defined_context_and_tool_content() {
    let (mut translator, ctx) = v2_translator();
    let events = [
        (
            "session.inbox.enqueued",
            json!({"sessionID":"s","inboxID":"u","item":{"type":"user","payload":{"text":"inspect"}}}),
        ),
        (
            "session.inbox.delivered",
            json!({"sessionID":"s","inboxID":"u"}),
        ),
        (
            "model.context",
            json!({"sessionID":"s","system":[],"messages":[{"role":"user","content":[{"type":"provider-specific","payload":{"key":"value"}}]}]}),
        ),
        (
            "session.step.started",
            json!({"sessionID":"s","assistantMessageID":"m","model":{"id":"model","providerID":"test"}}),
        ),
        (
            "session.tool.input.started",
            json!({"sessionID":"s","assistantMessageID":"m","id":"call","name":"custom"}),
        ),
        (
            "session.tool.called",
            json!({"sessionID":"s","assistantMessageID":"m","id":"call","input":{"nested":{"key":42}}}),
        ),
        (
            "session.tool.success",
            json!({"sessionID":"s","assistantMessageID":"m","id":"call","content":[{"type":"vendor-content","payload":{"value":7}}]}),
        ),
        (
            "session.step.ended",
            json!({"sessionID":"s","assistantMessageID":"m"}),
        ),
    ];
    let mut ops = Vec::new();
    for (i, (name, data)) in events.into_iter().enumerate() {
        let envelope = v2_event(name, i as i64 + 1, data);
        ops.extend(translator.handle(&envelope, &ctx).unwrap());
    }
    let rows = reduce(ops);
    let llm = rows
        .values()
        .find(|row| row.span_type == SpanType::Llm)
        .unwrap();
    assert_eq!(
        llm.input.as_ref().unwrap()[0]["content"],
        json!([{"type":"provider-specific","payload":{"key":"value"}}])
    );
    let tool = rows
        .values()
        .find(|row| row.span_type == SpanType::Tool)
        .unwrap();
    assert_eq!(tool.input, Some(json!({"nested":{"key":42}})));
    assert_eq!(
        tool.output,
        Some(json!([{"type":"vendor-content","payload":{"value":7}}]))
    );
}

#[test]
fn opencode_v2_native_compaction_window_is_scoped_to_its_model() {
    for (field, model, expected) in [
        ("native", "same", "replacement"),
        ("native", "different", "original"),
        ("providerContext", "same", "replacement"),
    ] {
        let mut translator = Registry::default_agents().create("opencode", "root-session");
        let ctx = SessionCtx {
            session_id: "root-session".into(),
            config: None,
        };
        let mut compact =
            json!({"sessionID":"s","text":"","model":{"id":"same","providerID":"test"}});
        compact[field] = json!({"messages":[{"role":"user","content":"replacement"}]});
        let events = [
            (
                "session.inbox.enqueued",
                json!({"sessionID":"s","inboxID":"old","item":{"type":"user","payload":{"text":"original"}}}),
            ),
            (
                "session.inbox.delivered",
                json!({"sessionID":"s","inboxID":"old"}),
            ),
            ("session.compaction.started", json!({"sessionID":"s"})),
            ("session.compaction.ended", compact),
            (
                "session.step.started",
                json!({"sessionID":"s","assistantMessageID":"m","model":{"id":model,"providerID":"test"}}),
            ),
            (
                "session.text.ended",
                json!({"sessionID":"s","assistantMessageID":"m","ordinal":0,"text":"answer"}),
            ),
            (
                "session.step.ended",
                json!({"sessionID":"s","assistantMessageID":"m"}),
            ),
        ];
        let mut ops = Vec::new();
        for (i, (name, data)) in events.into_iter().enumerate() {
            let envelope = v2_event(name, i as i64 + 1, data);
            ops.extend(translator.handle(&envelope, &ctx).unwrap());
        }
        let rows = reduce(ops);
        let llm = rows
            .values()
            .find(|row| {
                row.metadata
                    .as_ref()
                    .is_some_and(|m| m["message_id"] == "m")
            })
            .unwrap();
        assert_eq!(llm.input, Some(json!([{"role":"user","content":expected}])));
        let compact = rows
            .values()
            .find(|row| {
                row.tags
                    .as_ref()
                    .is_some_and(|tags| tags.iter().any(|tag| tag == "compaction"))
            })
            .unwrap();
        assert_eq!(
            compact.output,
            Some(json!([{"role":"user","content":"replacement"}]))
        );
    }
}

#[test]
fn opencode_v2_preserves_opaque_provider_compaction_output() {
    let (mut translator, ctx) = v2_translator();
    let native =
        json!([{"role":"assistant","content":null,"provider_specific":{"continuation":"opaque"}}]);
    let events = [
        (
            "session.inbox.enqueued",
            json!({"sessionID":"s","inboxID":"i","item":{"type":"user","payload":{"text":"hello"}}}),
        ),
        (
            "session.inbox.delivered",
            json!({"sessionID":"s","inboxID":"i"}),
        ),
        (
            "session.compaction.ended",
            json!({"sessionID":"s","text":"","native":{"messages":native}}),
        ),
    ];
    let mut ops = Vec::new();
    for (i, (name, data)) in events.into_iter().enumerate() {
        let envelope = v2_event(name, i as i64 + 1, data);
        ops.extend(translator.handle(&envelope, &ctx).unwrap());
    }
    let rows = reduce(ops);
    let compact = rows
        .values()
        .find(|row| {
            row.tags
                .as_ref()
                .is_some_and(|tags| tags.iter().any(|tag| tag == "compaction"))
        })
        .unwrap();
    assert_eq!(compact.output, Some(native));
}

fn v2_event(name: &str, ts: i64, data: serde_json::Value) -> Envelope {
    let mut envelope = event(name, ts, json!({"id":format!("parity-{ts}"),"data":data}));
    envelope.source_version = Some("2.0.0".into());
    envelope
}

#[test]
fn opencode_v2_checkpoint_resume_and_finalize_preserve_session_and_close_missing_tool() {
    let (mut translator, ctx) = v2_translator();
    let mut ops = Vec::new();
    for e in [
        v2_event(
            "session.inbox.enqueued",
            1,
            json!({"sessionID":"s","inboxID":"first","item":{"type":"user","payload":{"text":"first"}}}),
        ),
        v2_event(
            "session.inbox.delivered",
            2,
            json!({"sessionID":"s","inboxID":"first"}),
        ),
        v2_event("session.execution.succeeded", 3, json!({"sessionID":"s"})),
        v2_event(
            "session.inbox.enqueued",
            4,
            json!({"sessionID":"s","inboxID":"second","item":{"type":"user","payload":{"text":"second"}}}),
        ),
    ] {
        ops.extend(translator.handle(&e, &ctx).unwrap());
    }
    assert!(translator.checkpoint(&ctx).unwrap().is_empty());
    for e in [
        v2_event(
            "session.inbox.delivered",
            5,
            json!({"sessionID":"s","inboxID":"second"}),
        ),
        v2_event(
            "session.tool.input.started",
            6,
            json!({"sessionID":"s","id":"call","name":"read"}),
        ),
        v2_event(
            "session.tool.called",
            7,
            json!({"sessionID":"s","assistantMessageID":"m","id":"call","input":{"path":"x"}}),
        ),
    ] {
        ops.extend(translator.handle(&e, &ctx).unwrap());
    }
    ops.extend(translator.finalize(&ctx).unwrap());
    let rows = reduce(ops);
    assert_eq!(rows.values().filter(|r| r.name == "OpenCode").count(), 1);
    assert_eq!(
        rows.values()
            .filter(|r| r.name.starts_with("Turn "))
            .count(),
        2
    );
    let root = rows.values().find(|r| r.name == "OpenCode").unwrap();
    assert_eq!(root.metadata.as_ref().unwrap()["total_turns"], 2);
    assert_eq!(root.end_ms, Some(7));
    let tool = rows
        .values()
        .find(|r| r.span_type == SpanType::Tool)
        .unwrap();
    assert_eq!(tool.end_ms, Some(7));
    assert_eq!(
        tool.error.as_deref(),
        Some("Interrupted before tool completion")
    );
}

#[test]
fn opencode_v2_records_reasoning_usage_timing_and_error() {
    let (mut translator, ctx) = v2_translator();
    let events = [
        v2_event(
            "session.inbox.enqueued",
            1,
            json!({"sessionID":"s","inboxID":"i","item":{"type":"user","payload":{"text":"think"}}}),
        ),
        v2_event(
            "session.inbox.delivered",
            2,
            json!({"sessionID":"s","inboxID":"i"}),
        ),
        v2_event(
            "session.step.started",
            3,
            json!({"sessionID":"s","assistantMessageID":"m","started":2,"model":{"id":"model","providerID":"test","variant":"high"}}),
        ),
        v2_event(
            "session.reasoning.ended",
            4,
            json!({"sessionID":"s","assistantMessageID":"m","ordinal":1,"text":"second"}),
        ),
        v2_event(
            "session.reasoning.ended",
            5,
            json!({"sessionID":"s","assistantMessageID":"m","ordinal":0,"text":"first "}),
        ),
        v2_event(
            "session.text.ended",
            6,
            json!({"sessionID":"s","assistantMessageID":"m","text":"answer"}),
        ),
        v2_event(
            "session.step.failed",
            7,
            json!({"sessionID":"s","assistantMessageID":"m","tokens":{"input":10,"output":3,"reasoning":2,"cache":{"read":5,"write":4}},"error":{"type":"provider","message":"provider failure"}}),
        ),
        v2_event(
            "session.execution.failed",
            8,
            json!({"sessionID":"s","error":{"type":"provider","message":"provider failure"}}),
        ),
    ];
    let ops = events
        .iter()
        .flat_map(|e| translator.handle(e, &ctx).unwrap())
        .collect();
    let rows = reduce(ops);
    let llm = rows
        .values()
        .find(|r| r.span_type == SpanType::Llm)
        .unwrap();
    assert_eq!((llm.start_ms, llm.end_ms), (Some(2), Some(7)));
    assert_eq!(llm.error.as_deref(), Some("provider failure"));
    assert_eq!(
        llm.output.as_ref().unwrap()[0]["reasoning"][0]["content"],
        "first second"
    );
    assert_eq!(
        llm.metrics,
        Some(
            json!({"prompt_tokens":19,"completion_tokens":3,"tokens":24,"prompt_cached_tokens":5,"prompt_cache_creation_tokens":4,"reasoning_tokens":2})
        )
    );
    let root = rows.values().find(|r| r.name == "OpenCode").unwrap();
    assert_eq!(root.error.as_deref(), Some("provider failure"));
}

#[test]
fn opencode_v2_ignores_malformed_unknown_cancelled_and_replayed_events() {
    let (mut translator, ctx) = v2_translator();
    for e in [
        v2_event(
            "session.inbox.enqueued",
            1,
            json!({"sessionID":"s","inboxID":"cancelled","item":{"type":"user","payload":{"text":"not delivered"}}}),
        ),
        v2_event(
            "session.inbox.cancelled",
            2,
            json!({"sessionID":"s","inboxID":"cancelled"}),
        ),
        v2_event(
            "session.inbox.delivered",
            3,
            json!({"sessionID":"s","inboxID":"cancelled"}),
        ),
        v2_event("session.step.ended", 4, json!({"sessionID":42})),
        v2_event(
            "session.future.event",
            5,
            json!({"sessionID":"s","new":"shape"}),
        ),
    ] {
        assert!(translator.handle(&e, &ctx).unwrap().is_empty());
    }
    let enqueued = v2_event(
        "session.inbox.enqueued",
        6,
        json!({"sessionID":"s","inboxID":"real","item":{"type":"user","payload":{"text":"delivered"}}}),
    );
    let delivered = v2_event(
        "session.inbox.delivered",
        7,
        json!({"sessionID":"s","inboxID":"real"}),
    );
    assert!(translator.handle(&enqueued, &ctx).unwrap().is_empty());
    assert!(translator.handle(&enqueued, &ctx).unwrap().is_empty());
    let ops = translator.handle(&delivered, &ctx).unwrap();
    assert_eq!(
        ops.iter()
            .filter(|op| matches!(op, SpanOp::Insert(row) if row.name == "Turn 1"))
            .count(),
        1
    );
    assert!(translator.handle(&delivered, &ctx).unwrap().is_empty());
}

#[test]
fn opencode_v2_routes_resumed_nested_sessions_under_external_parent() {
    let mut translator = Registry::default_agents().create("opencode", "root-session");
    let mut components = SpanComponents::new(SpanObjectType::ProjectLogs);
    components.span_id = Some("external-parent".into());
    components.root_span_id = Some("external-root".into());
    let ctx = SessionCtx {
        session_id: "root-session".into(),
        config: Some(
            SessionRoute {
                destination: Some(TraceDestination::ParentSpan { components }),
                additional_metadata: Some(
                    json!({"team":"pipeline","source":"override","_bt_private":true}),
                ),
                tags: vec!["nested".into()],
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
    let events = [
        v2_event("session.created", 1, json!({"sessionID":"parent"})),
        v2_event(
            "session.inbox.enqueued",
            2,
            json!({"sessionID":"parent","inboxID":"i","item":{"type":"user","payload":{"text":"delegate"}}}),
        ),
        v2_event(
            "session.inbox.delivered",
            3,
            json!({"sessionID":"parent","inboxID":"i"}),
        ),
        v2_event(
            "session.created",
            4,
            json!({"sessionID":"child","parentID":"parent","title":"find docs (@research subagent)"}),
        ),
        v2_event(
            "session.execution.succeeded",
            5,
            json!({"sessionID":"child"}),
        ),
        v2_event(
            "session.execution.succeeded",
            6,
            json!({"sessionID":"parent"}),
        ),
        v2_event(
            "session.inbox.enqueued",
            7,
            json!({"sessionID":"parent","inboxID":"j","item":{"type":"user","payload":{"text":"resume"}}}),
        ),
        v2_event(
            "session.inbox.delivered",
            8,
            json!({"sessionID":"parent","inboxID":"j"}),
        ),
    ];
    let ops = events
        .iter()
        .flat_map(|e| translator.handle(e, &ctx).unwrap())
        .collect::<Vec<_>>();
    assert_merges_preserve_insert_identity(&ops);
    let root_insert = ops
        .iter()
        .find_map(|op| match op {
            SpanOp::Insert(row) if row.name == "OpenCode" => Some(row),
            _ => None,
        })
        .unwrap();
    assert_eq!(root_insert.parent_span_ids, ["external-parent"]);
    let reopen = ops
        .iter()
        .find_map(|op| match op {
            SpanOp::Merge(row) if row.span_id == root_insert.span_id && row.end_ms.is_none() => {
                Some(row)
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(reopen.parent_span_ids, ["external-parent"]);
    let child_insert = ops
        .iter()
        .find_map(|op| match op {
            SpanOp::Insert(row) if row.name == "research: find docs" => Some(row),
            _ => None,
        })
        .unwrap();
    assert_eq!(root_insert.root_span_id, "external-root");
    assert_eq!(child_insert.root_span_id, "external-root");
    assert_ne!(child_insert.parent_span_ids, ["external-parent"]);
    for row in [root_insert, child_insert] {
        assert_eq!(row.metadata.as_ref().unwrap()["team"], "pipeline");
        assert_eq!(row.metadata.as_ref().unwrap()["source"], "opencode");
        assert!(row.metadata.as_ref().unwrap().get("_bt_private").is_none());
        assert_eq!(row.tags, Some(vec!["nested".into()]));
    }
}

#[test]
fn opencode_v2_reused_tool_call_ids_are_scoped_to_the_assistant_step() {
    let mut t = Registry::default_agents().create("opencode", "root-session");
    let ctx = SessionCtx {
        session_id: "root-session".into(),
        config: None,
    };
    let events = [
        v2_event(
            "session.inbox.enqueued",
            1,
            json!({"sessionID":"s","inboxID":"a","item":{"type":"user","payload":{"text":"hi"}}}),
        ),
        v2_event(
            "session.inbox.delivered",
            2,
            json!({"sessionID":"s","inboxID":"a"}),
        ),
        v2_event(
            "session.tool.input.started",
            3,
            json!({"sessionID":"s","assistantMessageID":"m1","id":"call","name":"read"}),
        ),
        v2_event(
            "session.tool.called",
            4,
            json!({"sessionID":"s","assistantMessageID":"m1","id":"call","input":{"path":"first"}}),
        ),
        v2_event(
            "session.tool.success",
            5,
            json!({"sessionID":"s","assistantMessageID":"m1","id":"call","content":[]}),
        ),
        v2_event(
            "session.tool.input.started",
            6,
            json!({"sessionID":"s","assistantMessageID":"m2","id":"call","name":"read"}),
        ),
        v2_event(
            "session.tool.called",
            7,
            json!({"sessionID":"s","assistantMessageID":"m2","id":"call","input":{"path":"second"}}),
        ),
        v2_event(
            "session.tool.success",
            8,
            json!({"sessionID":"s","assistantMessageID":"m2","id":"call","content":[]}),
        ),
    ];
    let ops = events
        .iter()
        .flat_map(|e| t.handle(e, &ctx).unwrap())
        .collect::<Vec<_>>();
    let tools = ops
        .iter()
        .filter_map(|op| match op {
            SpanOp::Insert(row) if row.span_type == SpanType::Tool => Some(row),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(tools.len(), 2);
    assert_ne!(tools[0].span_id, tools[1].span_id);
}

#[test]
fn opencode_v2_reuses_session_state_after_failure_interruption_and_child_success() {
    for terminal in [
        "session.execution.interrupted",
        "session.execution.failed",
        "session.execution.succeeded",
    ] {
        let mut t = Registry::default_agents().create("opencode", "root-session");
        let ctx = SessionCtx {
            session_id: "root-session".into(),
            config: None,
        };
        let events = [
            v2_event(
                "session.inbox.enqueued",
                1,
                json!({"sessionID":"p","inboxID":"p1","item":{"type":"user","payload":{"text":"parent"}}}),
            ),
            v2_event(
                "session.inbox.delivered",
                2,
                json!({"sessionID":"p","inboxID":"p1"}),
            ),
            v2_event(
                "session.created",
                3,
                json!({"sessionID":"child","parentID":"p","title":"Child"}),
            ),
            v2_event(
                "session.inbox.enqueued",
                4,
                json!({"sessionID":"child","inboxID":"c1","item":{"type":"user","payload":{"text":"child"}}}),
            ),
            v2_event(
                "session.inbox.delivered",
                5,
                json!({"sessionID":"child","inboxID":"c1"}),
            ),
            v2_event(
                terminal,
                6,
                json!({"sessionID":"child","reason":"user","error":{"type":"test","message":"error"}}),
            ),
            v2_event(
                "session.inbox.enqueued",
                7,
                json!({"sessionID":"child","inboxID":"c2","item":{"type":"user","payload":{"text":"continue"}}}),
            ),
            v2_event(
                "session.inbox.delivered",
                8,
                json!({"sessionID":"child","inboxID":"c2"}),
            ),
        ];
        let ops = events
            .iter()
            .flat_map(|e| t.handle(e, &ctx).unwrap())
            .collect::<Vec<_>>();
        let roots = ops
            .iter()
            .filter_map(|op| match op {
                SpanOp::Insert(row)
                    if row
                        .metadata
                        .as_ref()
                        .is_some_and(|m| m["session_id"] == "child") =>
                {
                    Some(row)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(roots.len(), 1, "{terminal}");
        assert!(!roots[0].parent_span_ids.is_empty());
        assert!(ops.iter().any(|op| matches!(op, SpanOp::Insert(row) if row.name == "Turn 2" && row.root_span_id == roots[0].root_span_id)), "{terminal}");
    }
}

#[test]
fn opencode_v2_preserves_tool_text_and_model_metadata() {
    let mut t = Registry::default_agents().create("opencode", "root-session");
    let ctx = SessionCtx {
        session_id: "root-session".into(),
        config: None,
    };
    let events = [
        v2_event(
            "session.inbox.enqueued",
            1,
            json!({"sessionID":"s","inboxID":"a","item":{"type":"user","payload":{"text":"hi"}}}),
        ),
        v2_event(
            "session.inbox.delivered",
            2,
            json!({"sessionID":"s","inboxID":"a"}),
        ),
        v2_event(
            "model.context",
            3,
            json!({"sessionID":"s","messages":[{"role":"tool","content":[{"type":"text","text":"tool payload"}]}]}),
        ),
        v2_event(
            "session.step.started",
            4,
            json!({"sessionID":"s","assistantMessageID":"m","model":{"id":"model","providerID":"p"}}),
        ),
        v2_event(
            "session.text.ended",
            5,
            json!({"sessionID":"s","assistantMessageID":"m","text":"answer"}),
        ),
        v2_event(
            "session.step.ended",
            6,
            json!({"sessionID":"s","assistantMessageID":"m"}),
        ),
    ];
    let rows = reduce(
        events
            .iter()
            .flat_map(|e| t.handle(e, &ctx).unwrap())
            .collect(),
    );
    let llm = rows
        .values()
        .find(|row| row.span_type == SpanType::Llm)
        .unwrap();
    assert!(llm
        .input
        .as_ref()
        .unwrap()
        .to_string()
        .contains("tool payload"));
    let turn = rows.values().find(|row| row.name == "Turn 1").unwrap();
    assert_eq!(turn.metadata.as_ref().unwrap()["model"], "model");
}

#[test]
fn opencode_v2_repeated_native_compaction_uses_replacement_window_as_input() {
    let mut t = Registry::default_agents().create("opencode", "root-session");
    let ctx = SessionCtx {
        session_id: "root-session".into(),
        config: None,
    };
    let events = [
        v2_event(
            "session.inbox.enqueued",
            1,
            json!({"sessionID":"s","inboxID":"a","item":{"type":"user","payload":{"text":"old"}}}),
        ),
        v2_event(
            "session.inbox.delivered",
            2,
            json!({"sessionID":"s","inboxID":"a"}),
        ),
        v2_event(
            "session.compaction.ended",
            3,
            json!({"sessionID":"s","text":"","model":{"id":"m","providerID":"p"},"native":{"messages":[{"role":"user","content":"replacement"}]}}),
        ),
        v2_event(
            "session.inbox.enqueued",
            4,
            json!({"sessionID":"s","inboxID":"b","item":{"type":"user","payload":{"text":"new"}}}),
        ),
        v2_event(
            "session.inbox.delivered",
            5,
            json!({"sessionID":"s","inboxID":"b"}),
        ),
        v2_event(
            "session.compaction.ended",
            6,
            json!({"sessionID":"s","text":"second summary","model":{"id":"m","providerID":"p"}}),
        ),
    ];
    let ops = events
        .iter()
        .flat_map(|e| t.handle(e, &ctx).unwrap())
        .collect::<Vec<_>>();
    let second = ops
        .iter()
        .find_map(|op| match op {
            SpanOp::Insert(row)
                if row
                    .output
                    .as_ref()
                    .is_some_and(|v| v.to_string().contains("second summary")) =>
            {
                Some(row)
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(
        second.input,
        Some(json!([{"role":"user","content":"replacement"},{"role":"user","content":"new"}]))
    );
}

#[test]
fn opencode_v2_permission_denial_is_scoped_to_the_assistant_step() {
    let mut t = Registry::default_agents().create("opencode", "root-session");
    let ctx = SessionCtx {
        session_id: "root-session".into(),
        config: None,
    };
    let events = [
        v2_event(
            "session.inbox.enqueued",
            1,
            json!({"sessionID":"s","inboxID":"a","item":{"type":"user","payload":{"text":"hi"}}}),
        ),
        v2_event(
            "session.inbox.delivered",
            2,
            json!({"sessionID":"s","inboxID":"a"}),
        ),
        v2_event(
            "session.tool.input.started",
            3,
            json!({"sessionID":"s","assistantMessageID":"m1","id":"call","name":"read"}),
        ),
        v2_event(
            "permission.asked",
            4,
            json!({"sessionID":"s","id":"p","action":"read","source":{"id":"call","messageID":"m1"}}),
        ),
        v2_event(
            "permission.replied",
            5,
            json!({"sessionID":"s","requestID":"p","reply":"reject"}),
        ),
        v2_event(
            "session.tool.failed",
            6,
            json!({"sessionID":"s","assistantMessageID":"m1","id":"call","error":{"type":"permission.rejected","message":"denied"}}),
        ),
        v2_event(
            "session.tool.input.started",
            7,
            json!({"sessionID":"s","assistantMessageID":"m2","id":"call","name":"read"}),
        ),
        v2_event(
            "session.tool.called",
            8,
            json!({"sessionID":"s","assistantMessageID":"m2","id":"call","input":{"path":"allowed"}}),
        ),
        v2_event(
            "session.tool.success",
            9,
            json!({"sessionID":"s","assistantMessageID":"m2","id":"call","content":[]}),
        ),
    ];
    let rows = reduce(
        events
            .iter()
            .flat_map(|e| t.handle(e, &ctx).unwrap())
            .collect(),
    );
    let tools = rows
        .values()
        .filter(|r| r.span_type == SpanType::Tool)
        .collect::<Vec<_>>();
    assert_eq!(tools.len(), 2);
    assert!(tools.iter().any(|r| r
        .metadata
        .as_ref()
        .is_some_and(|m| m["tool_approval"] == "denied")));
    assert!(tools.iter().any(|r| r
        .metadata
        .as_ref()
        .is_some_and(|m| m["tool_approval"] == "approved")));
}

#[test]
fn opencode_v2_top_level_resume_after_interruption_keeps_turn_numbers() {
    let mut t = Registry::default_agents().create("opencode", "root-session");
    let ctx = SessionCtx {
        session_id: "root-session".into(),
        config: None,
    };
    let events = [
        v2_event(
            "session.inbox.enqueued",
            1,
            json!({"sessionID":"s","inboxID":"a","item":{"type":"user","payload":{"text":"first"}}}),
        ),
        v2_event(
            "session.inbox.delivered",
            2,
            json!({"sessionID":"s","inboxID":"a"}),
        ),
        v2_event(
            "session.execution.interrupted",
            3,
            json!({"sessionID":"s","reason":"user"}),
        ),
        v2_event(
            "session.inbox.enqueued",
            4,
            json!({"sessionID":"s","inboxID":"b","item":{"type":"user","payload":{"text":"second"}}}),
        ),
        v2_event(
            "session.inbox.delivered",
            5,
            json!({"sessionID":"s","inboxID":"b"}),
        ),
    ];
    let ops = events
        .iter()
        .flat_map(|e| t.handle(e, &ctx).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        ops.iter()
            .filter(|op| matches!(op, SpanOp::Insert(row) if row.name == "OpenCode"))
            .count(),
        1
    );
    assert!(ops
        .iter()
        .any(|op| matches!(op, SpanOp::Insert(row) if row.name == "Turn 2")));
}

#[test]
fn opencode_v2_snapshot_recovers_unseen_step_and_replays_without_duplicates() {
    let (mut translator, ctx) = v2_translator();
    let events = [
        v2_event(
            "session.inbox.enqueued",
            1,
            json!({"sessionID":"s","inboxID":"u","item":{"type":"user","payload":{"text":"hello"}}}),
        ),
        v2_event(
            "session.inbox.delivered",
            2,
            json!({"sessionID":"s","inboxID":"u"}),
        ),
        v2_event(
            "model.context",
            3,
            json!({"sessionID":"s","system":["system"],"messages":[{"role":"user","content":"hello"}]}),
        ),
    ];
    let mut ops = Vec::new();
    for event in events {
        ops.extend(translator.handle(&event, &ctx).unwrap());
    }
    ops.extend(
        translator
            .handle(
                &v2_event("session.execution.succeeded", 5, json!({"sessionID":"s"})),
                &ctx,
            )
            .unwrap(),
    );
    let snapshot = v2_event(
        "session.snapshot",
        6,
        json!({"sessionID":"s","idle_ms":6,"outcome":"succeeded","messages":[{
            "id":"m", "kind":{"type":"assistant","data":{"time":{"created":4,"completed":5}, "model":{"id":"mock","providerID":"test"},
            "content":[{"type":"text","text":"answer"}],"tokens":{"input":2,"output":1}}}
        }]}),
    );
    ops.extend(translator.handle(&snapshot, &ctx).unwrap());
    let replay = v2_event("session.snapshot", 7, snapshot.payload["data"].clone());
    assert!(translator.handle(&replay, &ctx).unwrap().is_empty());
    let rows = reduce(ops);
    let llm = rows
        .values()
        .find(|row| row.span_type == SpanType::Llm)
        .unwrap();
    assert_eq!(
        llm.output,
        Some(json!([{"role":"assistant","content":"answer"}]))
    );
    assert_eq!(llm.metrics.as_ref().unwrap()["prompt_tokens"], 2);
    assert!(llm.input.as_ref().unwrap().to_string().contains("hello"));
    assert_eq!(
        rows.values()
            .find(|row| row.name == "Turn 1")
            .unwrap()
            .output,
        Some(json!("answer"))
    );
}

#[test]
fn opencode_v2_snapshot_recovers_tool_output_without_inventing_approval() {
    let (mut translator, ctx) = v2_translator();
    let mut ops = Vec::new();
    for event in [
        v2_event(
            "session.inbox.enqueued",
            1,
            json!({"sessionID":"s","inboxID":"u","item":{"type":"user","payload":{"text":"hello"}}}),
        ),
        v2_event(
            "session.inbox.delivered",
            2,
            json!({"sessionID":"s","inboxID":"u"}),
        ),
        v2_event(
            "session.snapshot",
            7,
            json!({"sessionID":"s","idle_ms":6,"outcome":"interrupted","messages":[{
                "id":"m", "kind":{"type":"assistant","data":{"time":{"created":3,"completed":5}, "model":{"id":"mock","providerID":"test"},
                "content":[{"type":"tool","id":"call","name":"shell","state":{"status":"error","input":{"command":"false"},"error":{"type":"aborted","message":"stopped"}}}]}}
            }]}),
        ),
    ] {
        ops.extend(translator.handle(&event, &ctx).unwrap());
    }
    let rows = reduce(ops);
    let tool = rows
        .values()
        .find(|row| row.span_type == SpanType::Tool)
        .unwrap();
    assert_eq!(tool.name, "shell");
    assert_eq!(tool.error.as_deref(), Some("stopped"));
    assert!(tool
        .metadata
        .as_ref()
        .unwrap()
        .get("tool_approval")
        .is_none());
}

#[test]
fn opencode_v2_snapshot_recovers_a_turn_without_live_events() {
    let (mut translator, ctx) = v2_translator();
    let snapshot = v2_event(
        "session.snapshot",
        6,
        json!({"sessionID":"s","idle_ms":6,"outcome":"succeeded","messages":[
            {"id":"u","kind":{"type":"user","data":{"time":{"created":1},"text":"hello","files":[]}}},
            {"id":"m","kind":{"type":"assistant","data":{"time":{"created":4,"completed":5},"model":{"id":"mock","providerID":"test"},"content":[{"type":"text","text":"answer"}]}}}
        ]}),
    );
    let rows = reduce(translator.handle(&snapshot, &ctx).unwrap());
    let turn = rows.values().find(|row| row.name == "Turn 1").unwrap();
    assert_eq!(turn.input, Some(json!("hello")));
    assert_eq!(turn.output, Some(json!("answer")));
    assert_eq!(
        rows.values()
            .filter(|row| row.span_type == SpanType::Llm)
            .count(),
        1
    );
}

#[test]
fn opencode_v2_snapshot_recovers_compaction_and_its_following_context() {
    let (mut translator, ctx) = v2_translator();
    let events = [
        v2_event(
            "session.inbox.enqueued",
            1,
            json!({"sessionID":"s","inboxID":"u","item":{"type":"user","payload":{"text":"hello"}}}),
        ),
        v2_event(
            "session.inbox.delivered",
            2,
            json!({"sessionID":"s","inboxID":"u"}),
        ),
        v2_event(
            "session.snapshot",
            8,
            json!({"sessionID":"s","idle_ms":8,"outcome":"succeeded","messages":[
                {"id":"c","seq":3,"updated_ms":5,"kind":{"type":"compaction","data":{"time":{"created":3},"status":"completed","summary":"summary","model":{"id":"mock","providerID":"test"},"providerContext":{"messages":[{"role":"user","content":"native compacted context"}]}}}},
                {"id":"m","seq":6,"kind":{"type":"assistant","data":{"time":{"created":6,"completed":7},"model":{"id":"mock","providerID":"test"},"content":[{"type":"text","text":"answer"}]}}}
            ]}),
        ),
    ];
    let mut ops = Vec::new();
    for event in events {
        ops.extend(translator.handle(&event, &ctx).unwrap());
    }
    let rows = reduce(ops);
    assert_eq!(
        rows.values()
            .filter(|row| row
                .tags
                .as_ref()
                .is_some_and(|tags| tags.iter().any(|tag| tag == "compaction")))
            .count(),
        1
    );
    let llm = rows
        .values()
        .find(|row| row.span_type == SpanType::Llm && row.tags.is_none())
        .unwrap();
    assert!(llm
        .input
        .as_ref()
        .unwrap()
        .to_string()
        .contains("native compacted context"));
    assert_eq!(
        llm.output,
        Some(json!([{"role":"assistant","content":"answer"}]))
    );
}

#[test]
fn opencode_v2_snapshot_does_not_duplicate_live_compaction() {
    let (mut translator, ctx) = v2_translator();
    for name in ["session.compaction.started", "session.compaction.ended"] {
        let mut e = event(
            name,
            3,
            json!({"id":if name.ends_with("started") {"evt_c"} else {"evt_done"},"type":name,"durable":{"seq":if name.ends_with("started") {3} else {5}},"data":{"sessionID":"s","text":"summary"}}),
        );
        e.source_version = Some("2.0.0".into());
        if name.ends_with("started") {
            let _ = translator
                .handle(
                    &v2_event("session.created", 1, json!({"sessionID":"s"})),
                    &ctx,
                )
                .unwrap();
        }
        let _ = translator.handle(&e, &ctx).unwrap();
    }
    let snapshot = v2_event(
        "session.snapshot",
        7,
        json!({"sessionID":"s","messages":[{"id":"msg_c","seq":3,"updated_ms":5,"kind":{"type":"compaction","data":{"time":{"created":3},"status":"completed","summary":"summary"}}}]}),
    );
    assert!(translator.handle(&snapshot, &ctx).unwrap().is_empty());
}

#[test]
fn opencode_v2_snapshot_preserves_parent_child_turn_lineage() {
    let (mut translator, ctx) = v2_translator();
    let mut ops = Vec::new();
    for event in [
        v2_event(
            "session.snapshot",
            1,
            json!({"sessionID":"parent","messages":[{"id":"u1","kind":{"type":"user","data":{"time":{"created":1},"text":"parent"}}}]}),
        ),
        v2_event(
            "session.snapshot",
            2,
            json!({"sessionID":"child","parent_id":"parent","title":"Child","messages":[{"id":"u2","kind":{"type":"user","data":{"time":{"created":2},"text":"child"}}},{"id":"m","kind":{"type":"assistant","data":{"time":{"created":3,"completed":4},"model":{"id":"mock","providerID":"test"},"content":[{"type":"text","text":"child answer"}]}}}]}),
        ),
        v2_event(
            "session.snapshot",
            5,
            json!({"sessionID":"child","messages":[],"outcome":"succeeded","idle_ms":5}),
        ),
        v2_event(
            "session.snapshot",
            6,
            json!({"sessionID":"parent","messages":[],"outcome":"succeeded","idle_ms":6}),
        ),
    ] {
        ops.extend(translator.handle(&event, &ctx).unwrap());
    }
    let rows = reduce(ops);
    let parent = rows.values().find(|row| row.name == "OpenCode").unwrap();
    let parent_turn = rows
        .values()
        .find(|row| row.input == Some(json!("parent")))
        .unwrap();
    let child = rows.values().find(|row| row.name == "Child").unwrap();
    let child_turn = rows
        .values()
        .find(|row| row.input == Some(json!("child")))
        .unwrap();
    assert_eq!(child.root_span_id, parent.root_span_id);
    assert_eq!(
        child.parent_span_ids,
        std::slice::from_ref(&parent_turn.span_id)
    );
    assert_eq!(
        child_turn.parent_span_ids,
        std::slice::from_ref(&child.span_id)
    );
}
