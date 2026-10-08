#![allow(
    clippy::disallowed_methods,
    reason = "Test fixtures intentionally launch raw children."
)]

#[path = "support/span_identity.rs"]
mod span_identity;

use braintrust_sdk_rust::{SpanComponents, SpanObjectType};
use bt_daemon::wire::{BackendAuth, Envelope, SessionRoute, TraceDestination};
use bt_daemon::{Registry, SessionCtx, SpanOp, SpanRow, SpanType};
use serde_json::{json, Value};
use span_identity::{assert_merges_preserve_insert_identity, assert_turn_lineage};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/claude")
        .join(name)
}

fn claude_event(session_id: &str, event: &str, ts_ms: i64, payload: Value) -> Envelope {
    Envelope {
        source: "claude-code".into(),
        source_version: None,
        plugin_version: None,
        session_id: session_id.into(),
        event: event.into(),
        ts_ms,
        managed_run_id: None,
        payload,
        route: None,
        config: None,
        capture: None,
    }
}

/// How a replayed event points at its transcript bytes.
#[derive(Clone, Copy, PartialEq)]
enum Source {
    /// The daemon's append-only mirror plus a high-water offset (current).
    Mirror,
    /// The whole transcript inlined into the event (pre-mirror journals).
    Snapshot,
}

fn replay(name: &str) -> Vec<SpanOp> {
    replay_from(name, Source::Mirror)
}

fn replay_from(name: &str, source: Source) -> Vec<SpanOp> {
    let dir = fixture(name);
    let contents = std::fs::read_to_string(dir.join("events.ndjson")).unwrap();
    let first: Value = serde_json::from_str(contents.lines().next().unwrap()).unwrap();
    let session_id = first["payload"]["session_id"].as_str().unwrap();
    let registry = Registry::default_agents();
    let mut translator = registry.create("claude-code", session_id);
    let ctx = SessionCtx {
        session_id: session_id.to_string(),
        config: None,
    };
    let mut ops = Vec::new();

    for line in contents.lines() {
        let record: Value = serde_json::from_str(line).unwrap();
        let mut payload = record["payload"].clone();
        for field in ["transcript_path", "agent_transcript_path"] {
            let Some(original) = payload.get(field).and_then(Value::as_str) else {
                continue;
            };
            let basename = Path::new(original).file_name().unwrap();
            let local = dir.join("transcripts").join(basename);
            if local.exists() {
                payload[field] = json!(local.to_str().unwrap());
                let contents = std::fs::read_to_string(&local).unwrap();
                payload[match source {
                    Source::Mirror => "_bt_transcript_mirror",
                    Source::Snapshot => "_bt_transcript_snapshot",
                }] = match source {
                    // The mirror is byte-identical to the source prefix, so
                    // the fixture file stands in for it directly.
                    Source::Mirror => json!({
                        "path": local.to_str().unwrap(),
                        "mirror": local.to_str().unwrap(),
                        "through": contents.len() as u64,
                    }),
                    Source::Snapshot => json!({
                        "path": local.to_str().unwrap(),
                        "contents": contents,
                    }),
                };
            }
        }
        let ts_ms = chrono::DateTime::parse_from_rfc3339(record["ts"].as_str().unwrap())
            .unwrap()
            .timestamp_millis();
        let env = Envelope {
            source: "claude-code".into(),
            source_version: None,
            plugin_version: None,
            session_id: session_id.into(),
            event: record["hook"].as_str().unwrap().into(),
            ts_ms,
            managed_run_id: None,
            payload,
            route: None,
            config: None,
            capture: None,
        };
        ops.extend(translator.handle(&env, &ctx).unwrap());
        while let Some(batch) = translator.drain_pending(&ctx).unwrap() {
            ops.extend(batch);
        }
    }
    ops.extend(translator.flush(&ctx).unwrap());
    while let Some(batch) = translator.drain_pending(&ctx).unwrap() {
        ops.extend(batch);
    }
    ops
}

fn reduce(ops: Vec<SpanOp>) -> HashMap<String, SpanRow> {
    assert_merges_preserve_insert_identity(&ops);
    let mut rows = HashMap::<String, SpanRow>::new();
    for op in ops {
        match op {
            SpanOp::Insert(row) => {
                rows.insert(row.span_id.clone(), row);
            }
            SpanOp::Merge(update) => {
                let row = rows.entry(update.span_id.clone()).or_default();
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

/// Recorded sessions, including subagents and compaction, attribute every span
/// under a user turn to that turn so its cost can be aggregated flatly.
#[test]
fn every_span_under_a_turn_carries_its_turn_span_id() {
    for name in ["test-fixture", "example-simple", "subagent-compact"] {
        let ops = replay_from(name, Source::Mirror);
        let llm_spans = ops
            .iter()
            .filter(|op| matches!(op, SpanOp::Insert(row) if row.span_type == SpanType::Llm))
            .count();
        let stamped = assert_turn_lineage(&ops, |row| {
            row.name.starts_with("Turn ") || row.name.starts_with("Continuation")
        });
        assert!(
            stamped > llm_spans,
            "{name}: expected turns, LLM calls, and tools"
        );
    }
}

/// Journals recorded before transcript mirroring inline the whole transcript.
/// Both forms must translate identically, so upgrading the daemon neither
/// changes live output nor breaks recovery from an existing journal.
#[test]
fn mirror_and_inline_snapshot_transcripts_translate_identically() {
    for name in ["test-fixture", "example-simple", "subagent-compact"] {
        let mirrored = reduce(replay_from(name, Source::Mirror));
        let inlined = reduce(replay_from(name, Source::Snapshot));
        assert_eq!(
            mirrored.len(),
            inlined.len(),
            "{name}: span count differs between mirror and inline snapshot"
        );
        for (span_id, row) in &mirrored {
            let other = inlined
                .get(span_id)
                .unwrap_or_else(|| panic!("{name}: {span_id} missing from the inline replay"));
            assert_eq!(
                serde_json::to_value(row).unwrap(),
                serde_json::to_value(other).unwrap(),
                "{name}: {span_id} differs between mirror and inline snapshot"
            );
        }
    }
}

#[test]
fn claude_real_fixture_matches_session_turn_tool_and_token_contract() {
    let rows = reduce(replay("test-fixture"));
    let roots: Vec<_> = rows
        .values()
        .filter(|row| row.name.starts_with("Claude Code:"))
        .collect();
    let turns: Vec<_> = rows
        .values()
        .filter(|row| row.name.starts_with("Turn "))
        .collect();
    let tools: Vec<_> = rows
        .values()
        .filter(|row| row.span_type == SpanType::Tool)
        .collect();
    let llms: Vec<_> = rows
        .values()
        .filter(|row| row.span_type == SpanType::Llm)
        .collect();

    assert_eq!(roots.len(), 1);
    assert_eq!(turns.len(), 4);
    assert_eq!(tools.len(), 13);
    assert_eq!(llms.len(), 7, "one LLM span per unique requestId");
    assert!(turns.iter().all(|turn| turn.end_ms.is_some()));
    assert!(tools.iter().all(|tool| {
        tool.metadata.as_ref().and_then(|m| m.get("tool_approval")) == Some(&json!("approved"))
    }));

    let total = |key: &str| -> u64 {
        llms.iter()
            .map(|row| {
                row.metrics
                    .as_ref()
                    .and_then(|m| m.get(key))
                    .and_then(Value::as_u64)
                    .unwrap_or(0)
            })
            .sum()
    };
    assert_eq!(total("prompt_tokens"), 187_216);
    assert_eq!(total("completion_tokens"), 1_867);
    assert_eq!(total("prompt_cached_tokens"), 165_784);
    assert_eq!(total("tokens"), 189_083);

    let mut llms_per_turn = turns
        .iter()
        .map(|turn| {
            (
                turn.name.clone(),
                llms.iter()
                    .filter(|llm| llm.parent_span_ids.first() == Some(&turn.span_id))
                    .count(),
            )
        })
        .collect::<Vec<_>>();
    llms_per_turn.sort();
    assert_eq!(
        llms_per_turn,
        vec![
            ("Turn 1".into(), 2),
            ("Turn 2".into(), 1),
            ("Turn 3".into(), 3),
            ("Turn 4".into(), 1),
        ],
        "late transcript rows must remain attached to the turn that produced them"
    );
    assert!(llms.iter().any(|llm| {
        let roles = llm
            .input
            .as_ref()
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|message| message.get("role").and_then(Value::as_str))
            .collect::<Vec<_>>();
        roles.contains(&"assistant") && roles.contains(&"tool")
    }));
    assert!(llms.iter().any(|llm| {
        llm.output
            .as_ref()
            .and_then(|output| output.get("tool_calls"))
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .any(|call| {
                call.pointer("/function/arguments")
                    .is_some_and(Value::is_string)
            })
    }));
}

#[test]
fn claude_additional_metadata_reaches_roots_without_overriding_session_fields() {
    let registry = Registry::default_agents();
    let mut translator = registry.create("claude-code", "session");
    let mut components = SpanComponents::new(SpanObjectType::ProjectLogs);
    components.span_id = Some("external-parent".into());
    components.root_span_id = Some("external-root".into());
    let ctx = SessionCtx {
        session_id: "session".into(),
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
    let event = |name: &str, ts_ms: i64| Envelope {
        source: "claude-code".into(),
        source_version: None,
        plugin_version: None,
        session_id: "session".into(),
        event: name.into(),
        ts_ms,
        managed_run_id: None,
        payload: json!({"session_id":"session","cwd":"/workspace","prompt":"go"}),
        route: None,
        config: None,
        capture: None,
    };
    let mut ops = translator
        .handle(&event("UserPromptSubmit", 1), &ctx)
        .unwrap();
    ops.extend(translator.handle(&event("SessionEnd", 2), &ctx).unwrap());
    let rows = reduce(ops);
    let root = rows
        .values()
        .find(|row| row.name.starts_with("Claude Code:"))
        .unwrap();
    assert_eq!(root.metadata.as_ref().unwrap()["team"], "platform");
    assert_eq!(root.metadata.as_ref().unwrap()["source"], "claude-code");
    assert_eq!(root.tags, Some(vec!["ci".into(), "docs".into()]));
    assert_eq!(root.root_span_id, "external-root");
    assert_eq!(root.parent_span_ids, ["external-parent"]);
}

#[test]
fn claude_passive_hooks_do_not_create_blank_session_traces() {
    let registry = Registry::default_agents();
    let mut translator = registry.create("claude-code", "idle-session");
    let ctx = SessionCtx {
        session_id: "idle-session".into(),
        config: None,
    };
    let event = |name: &str, ts_ms: i64, payload: Value| Envelope {
        source: "claude-code".into(),
        source_version: None,
        plugin_version: None,
        session_id: "idle-session".into(),
        event: name.into(),
        ts_ms,
        managed_run_id: None,
        payload,
        route: None,
        config: None,
        capture: None,
    };

    let mut ops = Vec::new();
    for event in [
        event(
            "SessionStart",
            1,
            json!({
                "cwd":"/workspace/demo",
                "source":"resume",
                "model":"claude-test",
                "permission_mode":"plan",
                "system_prompt":"You are a precise coding assistant."
            }),
        ),
        event(
            "Notification",
            2,
            json!({"cwd":"/workspace/demo", "notification_type":"idle_prompt"}),
        ),
        event(
            "TeammateIdle",
            3,
            json!({"cwd":"/workspace/demo", "teammate_name":"researcher"}),
        ),
        event("SessionEnd", 4, json!({"cwd":"/workspace/demo"})),
    ] {
        ops.extend(translator.handle(&event, &ctx).unwrap());
    }
    assert!(ops.is_empty(), "passive Claude hooks must not emit a trace");

    let ops = translator
        .handle(
            &event(
                "UserPromptSubmit",
                5,
                json!({"cwd":"/workspace/demo", "prompt":"trace this"}),
            ),
            &ctx,
        )
        .unwrap();
    let root = ops
        .into_iter()
        .find_map(|op| match op {
            SpanOp::Insert(row) if row.name == "Claude Code: demo" => Some(row),
            _ => None,
        })
        .expect("a user prompt starts a trace");
    let metadata = root.metadata.as_ref().unwrap();
    assert_eq!(metadata["session_source"], "resume");
    assert_eq!(metadata["model"], "claude-test");
    assert!(metadata.get("permission_mode").is_none());
    assert_eq!(
        metadata["system_prompt"],
        "You are a precise coding assistant."
    );
}

#[test]
fn claude_turns_capture_the_effective_permission_mode() {
    let registry = Registry::default_agents();
    let mut translator = registry.create("claude-code", "permission-session");
    let ctx = SessionCtx {
        session_id: "permission-session".into(),
        config: None,
    };
    let event = |name: &str, ts_ms: i64, payload: Value| Envelope {
        source: "claude-code".into(),
        source_version: None,
        plugin_version: None,
        session_id: "permission-session".into(),
        event: name.into(),
        ts_ms,
        managed_run_id: None,
        payload,
        route: None,
        config: None,
        capture: None,
    };

    let mut ops = Vec::new();
    for event in [
        event(
            "SessionStart",
            1,
            json!({"cwd":"/workspace/demo", "permission_mode":"plan"}),
        ),
        event(
            "UserPromptSubmit",
            2,
            json!({"cwd":"/workspace/demo", "prompt":"plan this", "permission_mode":"plan"}),
        ),
        event(
            "Stop",
            3,
            json!({"cwd":"/workspace/demo", "permission_mode":"plan"}),
        ),
        event(
            "UserPromptSubmit",
            4,
            json!({"cwd":"/workspace/demo", "prompt":"implement it", "permission_mode":"acceptEdits"}),
        ),
        event(
            "SessionEnd",
            5,
            json!({"cwd":"/workspace/demo", "permission_mode":"acceptEdits"}),
        ),
    ] {
        ops.extend(translator.handle(&event, &ctx).unwrap());
    }
    let rows = reduce(ops);
    let turn_metadata = |name: &str| {
        rows.values()
            .find(|row| row.name == name)
            .and_then(|row| row.metadata.as_ref())
            .and_then(|metadata| metadata.get("permission_mode"))
            .cloned()
    };
    assert_eq!(turn_metadata("Turn 1"), Some(json!("plan")));
    assert_eq!(turn_metadata("Turn 2"), Some(json!("acceptEdits")));
}

#[test]
fn claude_subagent_routing_tolerates_malformed_optional_metadata() {
    let registry = Registry::default_agents();
    let mut translator = registry.create("claude-code", "subagent-session");
    let ctx = SessionCtx {
        session_id: "subagent-session".into(),
        config: None,
    };
    let event = Envelope {
        source: "claude-code".into(),
        source_version: None,
        plugin_version: None,
        session_id: "subagent-session".into(),
        event: "SubagentStart".into(),
        ts_ms: 1,
        managed_run_id: None,
        payload: json!({
            "cwd": "/workspace/demo",
            "agent_id": "agent-1",
            "agent_type": { "future": "object shape" },
            "future_field": true,
        }),
        route: None,
        config: None,
        capture: None,
    };

    let rows = reduce(translator.handle(&event, &ctx).unwrap());
    assert!(rows.values().any(|row| row.name == "Claude Code: demo"));
    let subagent = rows
        .values()
        .find(|row| row.name == "subagent: agent")
        .expect("valid agent_id must create a subagent span");
    assert_eq!(subagent.metadata.as_ref().unwrap()["agent_id"], "agent-1");
}

#[test]
fn claude_ignores_empty_subagent_identifier() {
    let registry = Registry::default_agents();
    let mut translator = registry.create("claude-code", "empty-subagent-session");
    let ctx = SessionCtx {
        session_id: "empty-subagent-session".into(),
        config: None,
    };
    let event = Envelope {
        source: "claude-code".into(),
        source_version: None,
        plugin_version: None,
        session_id: "empty-subagent-session".into(),
        event: "SubagentStart".into(),
        ts_ms: 1,
        managed_run_id: None,
        payload: json!({
            "cwd": "/workspace/demo",
            "agent_id": "",
            "agent_type": "reviewer",
        }),
        route: None,
        config: None,
        capture: None,
    };

    let rows = reduce(translator.handle(&event, &ctx).unwrap());
    assert!(rows.values().all(|row| !row.name.starts_with("subagent:")));
}

#[test]
fn claude_ignores_untracked_empty_type_subagent_stops() {
    let registry = Registry::default_agents();
    let mut translator = registry.create("claude-code", "internal-completions");
    let ctx = SessionCtx {
        session_id: "internal-completions".into(),
        config: None,
    };
    let event = |name, ts_ms, payload| claude_event(&ctx.session_id, name, ts_ms, payload);
    let internal_stop = |agent_id| {
        json!({
            "agent_id": agent_id,
            "agent_type": "",
            "agent_transcript_path": "/unavailable/internal-agent.jsonl",
            "last_assistant_message": "Suggested next prompt"
        })
    };

    // Internal completion alone must not manufacture a session trace.
    assert!(translator
        .handle(
            &event("SubagentStop", 10, internal_stop("idle-agent")),
            &ctx,
        )
        .unwrap()
        .is_empty());
    let mut ops = Vec::new();
    for envelope in [
        event("UserPromptSubmit", 20, json!({"prompt": "hello"})),
        event("Stop", 30, json!({"last_assistant_message": "Hello!"})),
        event("SubagentStop", 40, internal_stop("suggestion-agent")),
    ] {
        ops.extend(translator.handle(&envelope, &ctx).unwrap());
    }
    let rows = reduce(ops);
    assert_eq!(
        rows.len(),
        2,
        "only the session and user turn should remain"
    );
    let turn = rows.values().find(|row| row.name == "Turn 1").unwrap();
    assert_eq!(turn.end_ms, Some(30));
    assert_eq!(turn.output, Some(json!("Hello!")));
}

#[test]
fn claude_subagent_stops_preserve_lifecycle_and_missing_start_recovery() {
    let registry = Registry::default_agents();
    let mut translator = registry.create("claude-code", "subagent-recovery");
    let ctx = SessionCtx {
        session_id: "subagent-recovery".into(),
        config: None,
    };
    let event = |name, ts_ms, payload| claude_event(&ctx.session_id, name, ts_ms, payload);
    let mut ops = Vec::new();
    for envelope in [
        event("UserPromptSubmit", 10, json!({"prompt": "delegate"})),
        event(
            "SubagentStart",
            20,
            json!({"agent_id": "started", "agent_type": "worker"}),
        ),
        event(
            "SubagentStop",
            30,
            json!({"agent_id": "started", "agent_type": ""}),
        ),
        event(
            "PreToolUse",
            40,
            json!({
                "agent_id": "tool-recovered", "tool_name": "Read",
                "tool_use_id": "read-call", "tool_input": {"file_path": "example.txt"}
            }),
        ),
        event(
            "SubagentStop",
            50,
            json!({"agent_id": "tool-recovered", "agent_type": ""}),
        ),
        event(
            "SubagentStop",
            60,
            json!({
                "agent_id": "stop-recovered", "agent_type": "reviewer",
                "last_assistant_message": "Review complete"
            }),
        ),
    ] {
        ops.extend(translator.handle(&envelope, &ctx).unwrap());
    }
    let rows = reduce(ops);
    let subagents: HashMap<_, _> = rows
        .values()
        .filter(|row| row.name.starts_with("subagent:"))
        .map(|row| {
            (
                row.metadata.as_ref().unwrap()["agent_id"].as_str().unwrap(),
                row,
            )
        })
        .collect();
    assert_eq!(subagents.len(), 3);
    for (agent_id, start, end) in [
        ("started", 20, 30),
        ("tool-recovered", 40, 50),
        ("stop-recovered", 60, 60),
    ] {
        let agent = subagents[agent_id];
        assert_eq!(agent.start_ms, Some(start));
        assert_eq!(agent.end_ms, Some(end));
    }
    assert_eq!(
        subagents["stop-recovered"].output,
        Some(json!("Review complete"))
    );
    let tool = rows
        .values()
        .find(|row| row.span_type == SpanType::Tool)
        .unwrap();
    assert_eq!(
        tool.parent_span_ids,
        vec![subagents["tool-recovered"].span_id.clone()]
    );
    assert_eq!(tool.end_ms, Some(50));
}

#[test]
fn claude_subagent_fixture_builds_nested_subagent_llms() {
    let rows = reduce(replay("subagent-compact"));
    let subagents: Vec<_> = rows
        .values()
        .filter(|row| row.name.starts_with("subagent:"))
        .collect();
    assert_eq!(
        subagents.len(),
        2,
        "only the two delegated agents should produce task spans"
    );
    assert!(subagents.iter().all(|row| row.end_ms.is_some()));

    let subagent_ids: Vec<_> = subagents.iter().map(|row| row.span_id.as_str()).collect();
    let nested_llms: Vec<_> = rows
        .values()
        .filter(|row| {
            row.span_type == SpanType::Llm
                && row
                    .parent_span_ids
                    .first()
                    .is_some_and(|parent| subagent_ids.contains(&parent.as_str()))
        })
        .collect();
    assert!(
        !nested_llms.is_empty(),
        "subagent transcripts should produce LLM children"
    );
    let nested_tools = rows
        .values()
        .filter(|row| {
            row.span_type == SpanType::Tool
                && row
                    .parent_span_ids
                    .first()
                    .is_some_and(|parent| subagent_ids.contains(&parent.as_str()))
        })
        .count();
    assert!(
        nested_tools >= 20,
        "subagent hook tools should be children of their subagent task"
    );
}

#[test]
fn claude_permission_denied_and_failed_tools_are_first_class_spans() {
    let temp = tempfile::tempdir().unwrap();
    let repo = temp.path().join("repo");
    std::fs::create_dir(&repo).unwrap();
    let git = |args: &[&str]| {
        assert!(std::process::Command::new("git")
            .arg("-C")
            .arg(&repo)
            .args(args)
            .status()
            .unwrap()
            .success());
    };
    git(&["init", "-b", "main"]);
    git(&["config", "user.email", "test@example.com"]);
    git(&["config", "user.name", "Test"]);
    std::fs::write(repo.join("README.md"), "test").unwrap();
    git(&["add", "README.md"]);
    git(&["commit", "-m", "initial"]);
    git(&[
        "remote",
        "add",
        "origin",
        "https://example.com/acme/app.git",
    ]);
    let registry = Registry::default_agents();
    let mut translator = registry.create("claude-code", "s");
    let ctx = SessionCtx {
        session_id: "s".into(),
        config: None,
    };
    let event = |name: &str, payload: Value| claude_event("s", name, 1, payload);
    let mut ops = translator
        .handle(
            &event(
                "UserPromptSubmit",
                json!({"session_id":"s","cwd":repo,"prompt":"go"}),
            ),
            &ctx,
        )
        .unwrap();
    ops.extend(
        translator
            .handle(
                &event(
                    "PermissionDenied",
                    json!({
                        "session_id":"s",
                        "tool_name":"Bash",
                        "tool_use_id":"a",
                        "tool_input":{"command":"no"},
                        "permission":{"id":"p1","type":"tool","title":"Run command"},
                        "error":"The user denied this tool request"
                    }),
                ),
                &ctx,
            )
            .unwrap(),
    );
    ops.extend(
        translator
            .handle(
                &event(
                    "PostToolUse",
                    json!({"session_id":"s","tool_name":"Write","tool_use_id":"c","tool_input":{"file_path":"x"},"tool_response":{"is_error":true,"stderr":"\ndisk full"}}),
                ),
                &ctx,
            )
            .unwrap(),
    );
    ops.extend(
        translator
            .handle(
                &event(
                    "PostToolUse",
                    json!({"session_id":"s","tool_name":"mcp__github__issue_read","tool_use_id":"d","tool_input":{},"tool_response":{"isError":true,"content":[{"type":"text","text":"Could not resolve issue 355"}]}}),
                ),
                &ctx,
            )
            .unwrap(),
    );
    ops.extend(
        translator
            .handle(
                &event(
                    "PostToolUseFailure",
                    json!({"session_id":"s","tool_name":"Read","tool_use_id":"b","tool_input":{"file_path":"x"},"error":"missing"}),
                ),
                &ctx,
            )
            .unwrap(),
    );
    let rows = reduce(ops);
    let tools: Vec<_> = rows
        .values()
        .filter(|row| row.span_type == SpanType::Tool)
        .collect();
    assert_eq!(tools.len(), 4);
    assert!(tools
        .iter()
        .any(|row| { row.metadata.as_ref().unwrap()["tool_approval"] == json!("denied") }));
    let denied = tools
        .iter()
        .find(|row| row.metadata.as_ref().unwrap()["tool_approval"] == json!("denied"))
        .unwrap();
    assert_eq!(
        denied.metadata.as_ref().unwrap()["permission_id"],
        json!("p1")
    );
    assert_eq!(
        denied.metadata.as_ref().unwrap()["permission_title"],
        json!("Run command")
    );
    assert_eq!(denied.error, None, "permission denial is not tool failure");
    assert!(tools
        .iter()
        .any(|row| row.error.as_deref() == Some("missing")));
    assert!(tools
        .iter()
        .any(|row| row.error.as_deref() == Some("disk full")));
    assert!(tools
        .iter()
        .any(|row| row.error.as_deref() == Some("Could not resolve issue 355")));
    assert!(rows.values().all(|row| {
        let metadata = row.metadata.as_ref().and_then(Value::as_object).unwrap();
        metadata.get("git_origin_url") == Some(&json!("https://example.com/acme/app.git"))
            && metadata.get("git_branch") == Some(&json!("main"))
            && metadata.contains_key("git_commit_sha")
    }));
}

#[test]
fn claude_successful_tool_outputs_do_not_populate_error() {
    let registry = Registry::default_agents();
    let mut translator = registry.create("claude-code", "successful-tools");
    let ctx = SessionCtx {
        session_id: "successful-tools".into(),
        config: None,
    };
    let event = |name: &str, ts_ms: i64, payload: Value| {
        claude_event("successful-tools", name, ts_ms, payload)
    };
    let mut ops = translator
        .handle(
            &event(
                "UserPromptSubmit",
                10,
                json!({"session_id":"successful-tools","prompt":"run tools"}),
            ),
            &ctx,
        )
        .unwrap();
    for envelope in [
        event(
            "PostToolUse",
            20,
            json!({
                "session_id":"successful-tools",
                "tool_name":"Bash",
                "tool_use_id":"bash-1",
                "tool_input":{"command":"pwd"},
                "tool_response":{
                    "stdout":"/tmp",
                    "stderr":"\nShell cwd was reset to /tmp",
                    "interrupted":false
                }
            }),
        ),
        event(
            "PostToolUse",
            30,
            json!({
                "session_id":"successful-tools",
                "tool_name":"TaskStop",
                "tool_use_id":"stop-1",
                "tool_input":{"task_id":"abc"},
                "tool_response":{
                    "task_id":"abc",
                    "message":"Successfully stopped task: abc"
                }
            }),
        ),
    ] {
        ops.extend(translator.handle(&envelope, &ctx).unwrap());
    }

    let rows = reduce(ops);
    let tools = rows
        .values()
        .filter(|row| row.span_type == SpanType::Tool)
        .collect::<Vec<_>>();
    assert_eq!(tools.len(), 2);
    assert!(tools.iter().all(|row| row.error.is_none()));
    assert!(tools.iter().any(|row| {
        row.metadata.as_ref().unwrap()["tool_name"] == json!("Bash")
            && row.output.as_ref().unwrap()["stdout"] == json!("/tmp")
    }));
    assert!(tools.iter().any(|row| {
        row.metadata.as_ref().unwrap()["tool_name"] == json!("TaskStop")
            && row.output.as_ref().unwrap()["message"] == json!("Successfully stopped task: abc")
    }));
}

#[test]
fn claude_recovered_tool_results_determine_outcome_without_boundary_errors() {
    let base = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
        .unwrap()
        .timestamp_millis();
    let transcript = tempfile::NamedTempFile::new().unwrap();
    let transcript_path = transcript.path().to_str().unwrap();
    let records = [
        json!({
            "type":"user",
            "timestamp":"2026-01-01T00:00:01Z",
            "message":{"role":"user","content":"run tools"}
        }),
        json!({
            "type":"assistant",
            "timestamp":"2026-01-01T00:00:02Z",
            "message":{
                "id":"request-1",
                "model":"claude-test",
                "role":"assistant",
                "content":[
                    {"type":"tool_use","id":"success","name":"Bash","input":{"command":"pwd"}},
                    {"type":"tool_use","id":"success-denial-text","name":"Bash","input":{"command":"cat audit.log"}},
                    {"type":"tool_use","id":"success-cancellation-text","name":"Bash","input":{"command":"cat worker.log"}},
                    {"type":"tool_use","id":"task-stop","name":"TaskStop","input":{"task_id":"abc"}},
                    {"type":"tool_use","id":"denied","name":"Write","input":{"file_path":"secret"}},
                    {"type":"tool_use","id":"denied-permission","name":"Bash","input":{"command":"git commit"}},
                    {"type":"tool_use","id":"failed","name":"Bash","input":{"command":"exit 7"}},
                    {"type":"tool_use","id":"cancelled","name":"Bash","input":{"command":"sleep 10"}}
                ],
                "usage":{"input_tokens":1,"output_tokens":1}
            }
        }),
        json!({
            "type":"user",
            "timestamp":"2026-01-01T00:00:03Z",
            "message":{"role":"user","content":[
                {"type":"tool_result","tool_use_id":"success","content":"/tmp","is_error":false},
                {"type":"tool_result","tool_use_id":"success-denial-text","content":"audit log: permission request was denied yesterday","is_error":false},
                {"type":"tool_result","tool_use_id":"success-cancellation-text","content":"worker log: operation was aborted and retried","is_error":false},
                {"type":"tool_result","tool_use_id":"task-stop","content":"Successfully stopped task: abc","is_error":false},
                {"type":"tool_result","tool_use_id":"denied","content":"The user doesn't want to proceed with this tool use. The tool use was rejected.","is_error":true},
                {"type":"tool_result","tool_use_id":"denied-permission","content":"Permission to use Bash with command git commit has been denied.","is_error":true},
                {"type":"tool_result","tool_use_id":"failed","content":"Exit code 7","is_error":true},
                {"type":"tool_result","tool_use_id":"cancelled","content":"Request interrupted by user","is_error":true}
            ]}
        }),
    ];
    let contents = records
        .iter()
        .map(Value::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(transcript.path(), &contents).unwrap();

    let registry = Registry::default_agents();
    let mut translator = registry.create("claude-code", "recovered-outcomes");
    let ctx = SessionCtx {
        session_id: "recovered-outcomes".into(),
        config: None,
    };
    let event = |name: &str, ts_ms: i64, payload: Value| {
        claude_event("recovered-outcomes", name, ts_ms, payload)
    };
    let mut ops = translator
        .handle(
            &event(
                "UserPromptSubmit",
                base + 1_000,
                json!({"session_id":"recovered-outcomes","prompt":"run tools"}),
            ),
            &ctx,
        )
        .unwrap();
    for (call_id, tool_name, input) in [
        ("success", "Bash", json!({"command":"pwd"})),
        (
            "success-denial-text",
            "Bash",
            json!({"command":"cat audit.log"}),
        ),
        (
            "success-cancellation-text",
            "Bash",
            json!({"command":"cat worker.log"}),
        ),
        ("task-stop", "TaskStop", json!({"task_id":"abc"})),
        ("denied", "Write", json!({"file_path":"secret"})),
        ("denied-permission", "Bash", json!({"command":"git commit"})),
        ("failed", "Bash", json!({"command":"exit 7"})),
        ("cancelled", "Bash", json!({"command":"sleep 10"})),
    ] {
        ops.extend(
            translator
                .handle(
                    &event(
                        "PreToolUse",
                        base + 2_000,
                        json!({
                            "session_id":"recovered-outcomes",
                            "tool_name":tool_name,
                            "tool_use_id":call_id,
                            "tool_input":input
                        }),
                    ),
                    &ctx,
                )
                .unwrap(),
        );
    }
    ops.extend(
        translator
            .handle(
                &event(
                    "Stop",
                    base + 4_000,
                    json!({
                        "session_id":"recovered-outcomes",
                        "transcript_path":transcript_path,
                        "_bt_transcript_snapshot":{"path":transcript_path,"contents":contents}
                    }),
                ),
                &ctx,
            )
            .unwrap(),
    );
    while let Some(batch) = translator.drain_pending(&ctx).unwrap() {
        ops.extend(batch);
    }

    let rows = reduce(ops);
    let by_call = |call_id: &str| {
        rows.values()
            .find(|row| {
                row.span_type == SpanType::Tool
                    && row.metadata.as_ref().unwrap()["tool_call_id"] == call_id
            })
            .unwrap()
    };
    for call_id in [
        "success",
        "success-denial-text",
        "success-cancellation-text",
        "task-stop",
    ] {
        let tool = by_call(call_id);
        assert_eq!(tool.metadata.as_ref().unwrap()["tool_approval"], "approved");
        assert_eq!(tool.error, None, "{call_id} should remain successful");
    }
    for (call_id, marker) in [("denied", "rejected"), ("denied-permission", "denied")] {
        let denied = by_call(call_id);
        assert_eq!(denied.metadata.as_ref().unwrap()["tool_approval"], "denied");
        assert_eq!(denied.error, None, "a denial is not an execution failure");
        assert!(denied
            .output
            .as_ref()
            .unwrap()
            .as_str()
            .unwrap()
            .contains(marker));
    }
    let failed = by_call("failed");
    assert_eq!(
        failed.metadata.as_ref().unwrap()["tool_approval"],
        "approved"
    );
    assert_eq!(failed.error.as_deref(), Some("Exit code 7"));
    let cancelled = by_call("cancelled");
    assert!(cancelled
        .metadata
        .as_ref()
        .unwrap()
        .get("tool_approval")
        .is_none());
    assert_eq!(cancelled.metadata.as_ref().unwrap()["cancelled"], true);
    assert_eq!(
        cancelled.error, None,
        "cancellation is not an execution failure"
    );
}

#[test]
fn claude_unfinished_tools_are_cancelled_at_explicit_boundaries() {
    let registry = Registry::default_agents();
    let mut translator = registry.create("claude-code", "boundary-cancellation");
    let ctx = SessionCtx {
        session_id: "boundary-cancellation".into(),
        config: None,
    };
    let event = |name: &str, ts_ms: i64, payload: Value| {
        claude_event("boundary-cancellation", name, ts_ms, payload)
    };
    let mut ops = Vec::new();
    for envelope in [
        event("UserPromptSubmit", 10, json!({"prompt":"go"})),
        event(
            "PreToolUse",
            20,
            json!({"tool_name":"Bash","tool_use_id":"turn","tool_input":{"command":"sleep 10"}}),
        ),
        event(
            "SubagentStart",
            30,
            json!({"agent_id":"agent-1","agent_type":"worker"}),
        ),
        event(
            "PreToolUse",
            40,
            json!({"agent_id":"agent-1","tool_name":"Bash","tool_use_id":"subagent","tool_input":{"command":"sleep 10"}}),
        ),
        event(
            "SubagentStop",
            50,
            json!({"agent_id":"agent-1","agent_type":"worker"}),
        ),
        event("Stop", 60, json!({})),
        event("UserPromptSubmit", 70, json!({"prompt":"again"})),
        event(
            "PreToolUse",
            80,
            json!({"tool_name":"Bash","tool_use_id":"session","tool_input":{"command":"sleep 10"}}),
        ),
        event("SessionEnd", 90, json!({})),
    ] {
        ops.extend(translator.handle(&envelope, &ctx).unwrap());
    }

    let rows = reduce(ops);
    for call_id in ["turn", "subagent", "session"] {
        let tool = rows
            .values()
            .find(|row| {
                row.span_type == SpanType::Tool
                    && row.metadata.as_ref().unwrap()["tool_call_id"] == call_id
            })
            .unwrap();
        assert_eq!(tool.metadata.as_ref().unwrap()["cancelled"], true);
        assert_eq!(tool.error, None, "{call_id} should be a cancellation");
    }
}

#[test]
fn claude_pairs_tool_lifecycle_and_marks_explicit_skills_and_stop_failures() {
    let registry = Registry::default_agents();
    let mut translator = registry.create("claude-code", "lifecycle");
    let ctx = SessionCtx {
        session_id: "lifecycle".into(),
        config: None,
    };
    let event = |name: &str, ts_ms: i64, payload: Value| {
        let mut event = claude_event("lifecycle", name, ts_ms, payload);
        event.source_version = Some("2.0.0".into());
        event
    };
    let mut ops = Vec::new();
    for envelope in [
        event(
            "UserPromptSubmit",
            10,
            json!({"session_id":"lifecycle","cwd":"/tmp/x","prompt":"go"}),
        ),
        event(
            "PreToolUse",
            20,
            json!({"session_id":"lifecycle","tool_name":"Skill","tool_use_id":"skill-1","tool_input":{"skill":"review"}}),
        ),
        event(
            "PostToolUse",
            30,
            json!({"session_id":"lifecycle","tool_name":"Skill","tool_use_id":"skill-1","tool_input":{"skill":"review"},"tool_response":{"output":"loaded"}}),
        ),
        event(
            "StopFailure",
            40,
            json!({"session_id":"lifecycle","message":"model process exited"}),
        ),
    ] {
        ops.extend(translator.handle(&envelope, &ctx).unwrap());
    }
    let rows = reduce(ops);
    let skill = rows
        .values()
        .find(|row| row.span_type == SpanType::Tool)
        .unwrap();
    assert_eq!(skill.start_ms, Some(20));
    assert_eq!(skill.end_ms, Some(30));
    assert_eq!(skill.error, None);
    assert_eq!(
        skill.metadata.as_ref().unwrap()["skill_load_trigger"],
        json!("explicit")
    );
    let turn = rows.values().find(|row| row.name == "Turn 1").unwrap();
    assert_eq!(turn.error.as_deref(), Some("model process exited"));
}

#[test]
fn claude_groups_streamed_rows_and_reads_late_final_output_at_session_end() {
    let base = chrono::DateTime::parse_from_rfc3339("2026-07-28T16:00:00Z")
        .unwrap()
        .timestamp_millis();
    let dir = tempfile::tempdir().unwrap();
    let transcript = dir.path().join("session.jsonl");
    let usage = json!({
        "input_tokens": 10,
        "output_tokens": 5,
        "cache_creation_input_tokens": 0,
        "cache_read_input_tokens": 0
    });
    let records = [
        json!({
            "type": "user",
            "uuid": "user-1",
            "timestamp": "2026-07-28T16:00:00Z",
            "message": {"role": "user", "content": "run it"}
        }),
        json!({
            "type": "assistant",
            "uuid": "assistant-thinking-row",
            "timestamp": "2026-07-28T16:00:01Z",
            "message": {
                "id": "msg-native-request",
                "model": "claude-test",
                "role": "assistant",
                "content": [{"type": "thinking", "thinking": ""}],
                "usage": usage
            }
        }),
        json!({
            "type": "assistant",
            "uuid": "assistant-tool-row",
            "timestamp": "2026-07-28T16:00:02Z",
            "message": {
                "id": "msg-native-request",
                "model": "claude-test",
                "role": "assistant",
                "content": [{
                    "type": "tool_use",
                    "id": "tool-1",
                    "name": "Bash",
                    "input": {"command": "true"}
                }],
                "usage": usage
            }
        }),
    ];
    std::fs::write(
        &transcript,
        records
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n"),
    )
    .unwrap();

    let registry = Registry::default_agents();
    let mut translator = registry.create("claude-code", "streamed");
    let ctx = SessionCtx {
        session_id: "streamed".into(),
        config: None,
    };
    let event = |name: &str, ts_ms: i64, payload: Value| Envelope {
        source: "claude-code".into(),
        source_version: None,
        plugin_version: None,
        session_id: "streamed".into(),
        event: name.into(),
        ts_ms,
        managed_run_id: None,
        payload,
        route: None,
        config: None,
        capture: None,
    };
    let mut ops = translator
        .handle(
            &event(
                "UserPromptSubmit",
                base,
                json!({"session_id":"streamed","cwd":"/tmp/x","prompt":"run it"}),
            ),
            &ctx,
        )
        .unwrap();
    ops.extend(
        translator
            .handle(
                &event(
                    "Stop",
                    base + 2_500,
                    json!({
                        "session_id": "streamed",
                        "transcript_path": transcript,
                        "last_assistant_message": ""
                    }),
                ),
                &ctx,
            )
            .unwrap(),
    );

    let final_record = json!({
        "type": "assistant",
        "uuid": "assistant-final-row",
        "timestamp": "2026-07-28T16:00:03Z",
        "message": {
            "id": "msg-final-request",
            "model": "claude-test",
            "role": "assistant",
            "content": [{"type": "text", "text": "done"}],
            "usage": {
                "input_tokens": 11,
                "output_tokens": 1,
                "cache_creation_input_tokens": 0,
                "cache_read_input_tokens": 0
            }
        }
    });
    let previous = std::fs::read_to_string(&transcript).unwrap();
    std::fs::write(&transcript, format!("{previous}\n{final_record}")).unwrap();
    ops.extend(
        translator
            .handle(
                &event(
                    "SessionEnd",
                    base + 4_000,
                    json!({
                        "session_id": "streamed",
                        "transcript_path": transcript
                    }),
                ),
                &ctx,
            )
            .unwrap(),
    );

    let rows = reduce(ops);
    let llms = rows
        .values()
        .filter(|row| row.span_type == SpanType::Llm)
        .collect::<Vec<_>>();
    assert_eq!(llms.len(), 2);
    let streamed = llms
        .iter()
        .find(|row| row.metadata.as_ref().unwrap()["request_id"] == json!("msg-native-request"))
        .unwrap();
    assert_eq!(
        streamed.output.as_ref().unwrap()["tool_calls"][0]["id"],
        json!("tool-1")
    );
    let final_output = llms
        .iter()
        .find(|row| row.metadata.as_ref().unwrap()["request_id"] == json!("msg-final-request"))
        .unwrap();
    assert_eq!(
        final_output.output.as_ref().unwrap()["content"],
        json!("done")
    );
}

fn replay_continuation_events(
    events: Vec<(&str, i64, Value)>,
    finalize: bool,
) -> HashMap<String, SpanRow> {
    let session = "continuation";
    let registry = Registry::default_agents();
    let mut translator = registry.create("claude-code", session);
    let ctx = SessionCtx {
        session_id: session.into(),
        config: None,
    };
    let mut ops = Vec::new();
    for (name, ts, payload) in events {
        ops.extend(
            translator
                .handle(&claude_event(session, name, ts, payload), &ctx)
                .unwrap(),
        );
        while let Some(batch) = translator.drain_pending(&ctx).unwrap() {
            ops.extend(batch);
        }
    }
    if finalize {
        ops.extend(translator.finalize(&ctx).unwrap());
    }
    reduce(ops)
}

#[test]
fn claude_handbacks_continue_the_originating_human_turn_with_real_model_work() {
    let notification = "<task-notification>\n<task-id>fork-1</task-id>\n<status>completed</status>\n<summary>Agent finished</summary>\n<result>Done</result>\n</task-notification>";
    let message = "<agent-message from=\"fork-1\">Done</agent-message>";
    let transcript = json!({
        "type": "assistant",
        "timestamp": "2026-10-01T16:48:31Z",
        "message": {
            "id": "notification-response",
            "model": "claude-test",
            "content": [{"type": "text", "text": "The fork finished."}],
            "usage": {"input_tokens": 10, "output_tokens": 5}
        }
    });
    let base = 1_790_873_300_000;
    for payload in [
        json!({"prompt": notification}),
        json!({"prompt": message}),
        json!({
            "prompt": format!("Native prefix\n{notification}\nNative suffix"),
            "origin": {"kind": "peer", "handback": true, "senderTaskId": "fork-1"}
        }),
        json!({
            "prompt": format!("Native prefix\n{message}\nNative suffix"),
            "origin": {"kind": "peer", "handback": true, "from": "fork-1"}
        }),
    ] {
        for child_stops_first in [true, false] {
            let mut events = vec![
                ("UserPromptSubmit", base, json!({"prompt": "delegate"})),
                (
                    "SubagentStart",
                    base + 100,
                    json!({"agent_id": "fork-1", "agent_type": "fork"}),
                ),
                (
                    "Stop",
                    base + 1_000,
                    json!({"last_assistant_message": "Working in background"}),
                ),
                (
                    "UserPromptSubmit",
                    base + 4_000,
                    json!({"prompt": "unrelated question"}),
                ),
                (
                    "Stop",
                    base + 5_000,
                    json!({"last_assistant_message": "Unrelated answer"}),
                ),
            ];
            let child_stop = (
                "SubagentStop",
                base + 8_000 + if child_stops_first { -43 } else { 43 },
                json!({
                    "agent_id": "fork-1", "agent_type": "fork",
                    "last_assistant_message": "Done"
                }),
            );
            if child_stops_first {
                events.push(child_stop.clone());
            }
            events.push(("UserPromptSubmit", base + 8_000, payload.clone()));
            if !child_stops_first {
                events.push(child_stop);
            }
            events.extend([
                (
                    "Stop",
                    base + 12_000,
                    json!({
                        "last_assistant_message": "The fork finished.",
                        "transcript_path": "C:\\sessions\\main.jsonl",
                        "_bt_transcript_snapshot": {
                            "path": "C:\\sessions\\main.jsonl",
                            "contents": format!("{transcript}\n")
                        }
                    }),
                ),
                (
                    "UserPromptSubmit",
                    base + 13_000,
                    json!({"prompt": "thanks"}),
                ),
                ("Stop", base + 14_000, json!({})),
            ]);
            let rows = replay_continuation_events(events, false);
            let original = rows.values().find(|row| row.name == "Turn 1").unwrap();
            let unrelated = rows.values().find(|row| row.name == "Turn 2").unwrap();
            let next = rows.values().find(|row| row.name == "Turn 3").unwrap();
            assert_eq!(original.input, Some(json!("delegate")));
            assert_eq!(unrelated.input, Some(json!("unrelated question")));
            assert_eq!(next.input, Some(json!("thanks")));
            assert_eq!(
                rows.values()
                    .filter(|row| row.name.starts_with("Turn "))
                    .count(),
                3
            );
            let continuation = rows
                .values()
                .find(|row| row.input.as_ref() == Some(&payload["prompt"]))
                .unwrap();
            assert_eq!(continuation.span_type, SpanType::Task);
            assert_eq!(continuation.parent_span_ids, vec![original.span_id.clone()]);
            assert_eq!(continuation.output, Some(json!("The fork finished.")));
            assert_eq!(continuation.end_ms, Some(base + 12_000));
            assert_eq!(original.end_ms, continuation.end_ms);
            assert_eq!(original.output, Some(json!("Working in background")));
            assert_eq!(unrelated.end_ms, Some(base + 5_000));
            let child = rows
                .values()
                .find(|row| row.output == Some(json!("Done")))
                .unwrap();
            assert_eq!(child.parent_span_ids, vec![original.span_id.clone()]);
            let llms: Vec<_> = rows
                .values()
                .filter(|row| row.span_type == SpanType::Llm)
                .collect();
            assert_eq!(llms.len(), 1);
            let llm = llms[0];
            assert_eq!(llm.parent_span_ids, vec![continuation.span_id.clone()]);
            assert_eq!(
                llm.output.as_ref().unwrap()["content"],
                "The fork finished."
            );
            assert_eq!(llm.metrics.as_ref().unwrap()["prompt_tokens"], 10);
            assert_eq!(llm.metrics.as_ref().unwrap()["completion_tokens"], 5);
        }
    }
}

#[test]
fn claude_unknown_handbacks_are_session_children_but_quoted_tags_are_human_turns() {
    let notification =
        "<task-notification><task-id>unknown</task-id><result>Done</result></task-notification>";
    let message = "<agent-message from=\"unknown\">Done</agent-message>";
    let quoted_notification = format!("Explain this format: {notification}");
    let quoted_message = format!("{message}\nExplain the tag above.");
    let rows = replay_continuation_events(
        vec![
            ("UserPromptSubmit", 10, json!({"prompt": "hello"})),
            ("Stop", 20, json!({})),
            ("UserPromptSubmit", 30, json!({"prompt": notification})),
            ("Stop", 40, json!({"last_assistant_message": "received"})),
            ("UserPromptSubmit", 50, json!({"prompt": message})),
            (
                "Stop",
                60,
                json!({"last_assistant_message": "received again"}),
            ),
            (
                "UserPromptSubmit",
                70,
                json!({"prompt": quoted_notification}),
            ),
            ("Stop", 80, json!({})),
            ("UserPromptSubmit", 90, json!({"prompt": quoted_message})),
            ("Stop", 100, json!({})),
        ],
        false,
    );
    let root = rows
        .values()
        .find(|row| row.parent_span_ids.is_empty())
        .unwrap();
    for prompt in [notification, message] {
        let continuation = rows
            .values()
            .find(|row| row.input == Some(json!(prompt)))
            .unwrap();
        assert_eq!(continuation.span_type, SpanType::Task);
        assert!(!continuation.name.starts_with("Turn "));
        assert_eq!(continuation.parent_span_ids, vec![root.span_id.clone()]);
    }
    for (name, prompt) in [
        ("Turn 1", "hello"),
        ("Turn 2", quoted_notification.as_str()),
        ("Turn 3", quoted_message.as_str()),
    ] {
        let human = rows.values().find(|row| row.name == name).unwrap();
        assert_eq!(human.input, Some(json!(prompt)));
        assert_eq!(human.parent_span_ids, vec![root.span_id.clone()]);
    }
    assert_eq!(
        rows.values()
            .find(|row| row.name == "Turn 1")
            .unwrap()
            .end_ms,
        Some(20)
    );
    assert_eq!(
        rows.values()
            .filter(|row| row.name.starts_with("Turn "))
            .count(),
        3
    );
}

#[test]
fn claude_repeated_handbacks_retain_distinct_work_without_consuming_human_numbers() {
    let prompt = "<agent-message from=\"fork-1\">Done</agent-message>";
    let rows = replay_continuation_events(
        vec![
            ("UserPromptSubmit", 10, json!({"prompt": "delegate"})),
            (
                "SubagentStart",
                20,
                json!({"agent_id": "fork-1", "agent_type": "fork"}),
            ),
            ("Stop", 30, json!({})),
            (
                "SubagentStop",
                40,
                json!({"agent_id": "fork-1", "agent_type": "fork"}),
            ),
            ("UserPromptSubmit", 50, json!({"prompt": prompt})),
            (
                "Stop",
                60,
                json!({"last_assistant_message": "first return"}),
            ),
            ("UserPromptSubmit", 70, json!({"prompt": prompt})),
            (
                "Stop",
                80,
                json!({"last_assistant_message": "second return"}),
            ),
            ("UserPromptSubmit", 90, json!({"prompt": "next question"})),
            ("Stop", 100, json!({})),
        ],
        false,
    );
    let original = rows.values().find(|row| row.name == "Turn 1").unwrap();
    let mut returns: Vec<_> = rows
        .values()
        .filter(|row| row.input == Some(json!(prompt)))
        .collect();
    returns.sort_by_key(|row| row.start_ms);
    assert_eq!(returns.len(), 2);
    assert_ne!(returns[0].span_id, returns[1].span_id);
    assert_eq!(returns[0].output, Some(json!("first return")));
    assert_eq!(returns[1].output, Some(json!("second return")));
    assert!(returns
        .iter()
        .all(|row| row.parent_span_ids == vec![original.span_id.clone()]));
    assert_eq!(original.end_ms, Some(80));
    assert_eq!(
        rows.values()
            .find(|row| row.name == "Turn 2")
            .unwrap()
            .input,
        Some(json!("next question"))
    );
    assert_eq!(
        rows.values()
            .filter(|row| row.name.starts_with("Turn "))
            .count(),
        2
    );
}

#[test]
fn claude_active_continuation_tool_skill_and_terminal_merges_preserve_causal_parent() {
    let prompt =
        "<task-notification><task-id>fork-1</task-id><result>Done</result></task-notification>";
    for terminal in ["Stop", "SessionEnd", "finalize"] {
        let rows = replay_continuation_events(
            vec![
                ("UserPromptSubmit", 10, json!({"prompt": "delegate"})),
                (
                    "SubagentStart",
                    20,
                    json!({"agent_id": "fork-1", "agent_type": "fork"}),
                ),
                ("Stop", 30, json!({"last_assistant_message": "background"})),
                (
                    "SubagentStop",
                    40,
                    json!({"agent_id": "fork-1", "agent_type": "fork"}),
                ),
                ("UserPromptSubmit", 50, json!({"prompt": prompt})),
                (
                    "UserPromptExpansion",
                    60,
                    json!({"expansion_type": "slash_command", "skill_name": "review"}),
                ),
                (
                    "PreToolUse",
                    70,
                    json!({"tool_name": "Skill", "tool_use_id": "skill-1", "tool_input": {"skill": "review"}}),
                ),
                (
                    "PostToolUse",
                    80,
                    json!({"tool_name": "Skill", "tool_use_id": "skill-1", "tool_response": {"output": "loaded"}}),
                ),
                (
                    "PreToolUse",
                    90,
                    json!({"tool_name": "Bash", "tool_use_id": "unfinished", "tool_input": {"command": "sleep 10"}}),
                ),
                (
                    if terminal == "finalize" {
                        "Notification"
                    } else {
                        terminal
                    },
                    100,
                    json!({}),
                ),
            ],
            terminal == "finalize",
        );
        let original = rows.values().find(|row| row.name == "Turn 1").unwrap();
        let continuation = rows
            .values()
            .find(|row| row.input == Some(json!(prompt)))
            .unwrap();
        assert_eq!(continuation.parent_span_ids, vec![original.span_id.clone()]);
        assert_eq!(continuation.end_ms, Some(100), "{terminal}");
        assert_eq!(original.end_ms, continuation.end_ms, "{terminal}");
        assert_eq!(original.output, Some(json!("background")));
        let tools: Vec<_> = rows
            .values()
            .filter(|row| row.span_type == SpanType::Tool)
            .collect();
        assert_eq!(tools.len(), 2);
        assert!(tools
            .iter()
            .all(|row| row.parent_span_ids == vec![continuation.span_id.clone()]));
        let completed = tools.iter().find(|row| row.start_ms == Some(70)).unwrap();
        assert_eq!(completed.end_ms, Some(80));
        assert_eq!(completed.output, Some(json!({"output": "loaded"})));
        let unfinished = tools.iter().find(|row| row.start_ms == Some(90)).unwrap();
        assert_eq!(unfinished.end_ms, Some(100), "{terminal}");
    }
}

#[test]
fn claude_background_child_completion_extends_the_originating_human_turn() {
    let rows = replay_continuation_events(
        vec![
            ("UserPromptSubmit", 10, json!({"prompt": "delegate"})),
            (
                "SubagentStart",
                20,
                json!({"agent_id": "fork-1", "agent_type": "fork"}),
            ),
            ("Stop", 30, json!({"last_assistant_message": "background"})),
            ("UserPromptSubmit", 40, json!({"prompt": "unrelated"})),
            ("Stop", 50, json!({})),
            (
                "SubagentStop",
                100,
                json!({"agent_id": "fork-1", "agent_type": "fork", "last_assistant_message": "child result"}),
            ),
        ],
        false,
    );
    let original = rows.values().find(|row| row.name == "Turn 1").unwrap();
    let unrelated = rows.values().find(|row| row.name == "Turn 2").unwrap();
    let child = rows
        .values()
        .find(|row| row.output == Some(json!("child result")))
        .unwrap();
    assert_eq!(child.parent_span_ids, vec![original.span_id.clone()]);
    assert_eq!(child.end_ms, Some(100));
    assert_eq!(original.end_ms, child.end_ms);
    assert_eq!(original.output, Some(json!("background")));
    assert_eq!(unrelated.end_ms, Some(50));
}

#[test]
fn claude_later_turns_extend_an_already_closed_session_root() {
    let session = "resumed-root";
    let registry = Registry::default_agents();
    let mut translator = registry.create("claude-code", session);
    let ctx = SessionCtx {
        session_id: session.into(),
        config: None,
    };
    let mut ops = Vec::new();
    for (name, ts, payload) in [
        ("UserPromptSubmit", 10, json!({"prompt": "first"})),
        ("Stop", 20, json!({"last_assistant_message": "done"})),
        ("SessionEnd", 25, json!({})),
        ("SessionStart", 30, json!({"source": "resume"})),
        ("UserPromptSubmit", 40, json!({"prompt": "thank you"})),
        (
            "Stop",
            50,
            json!({"last_assistant_message": "You're welcome"}),
        ),
    ] {
        ops.extend(
            translator
                .handle(&claude_event(session, name, ts, payload), &ctx)
                .unwrap(),
        );
    }
    let rows = reduce(ops.clone());
    let root = rows
        .values()
        .find(|row| row.parent_span_ids.is_empty())
        .unwrap();
    assert_eq!(root.start_ms, Some(10));
    assert_eq!(root.end_ms, Some(50));
    assert!(rows
        .values()
        .filter(|row| row.name.starts_with("Turn "))
        .all(|row| row.end_ms <= root.end_ms));
    // A delayed terminal hook must not shrink the root behind completed work.
    ops.extend(
        translator
            .handle(&claude_event(session, "SessionEnd", 45, json!({})), &ctx)
            .unwrap(),
    );
    let rows = reduce(ops);
    assert_eq!(
        rows.values()
            .find(|row| row.parent_span_ids.is_empty())
            .unwrap()
            .end_ms,
        Some(50)
    );
}

#[test]
fn claude_fork_inherits_parent_history_without_recounting_parent_work() {
    // The child can finish before the parent's buffered transcript is emitted.
    for parent_stops_first in [false, true] {
        let session = "fork-ownership";
        let base = 1_790_873_300_000;
        let parent = json!({
            "type": "assistant", "timestamp": "2026-10-01T16:48:21Z",
            "message": {
                "id": "parent-request", "model": "claude-test",
                "content": [{"type":"tool_use","id":"spawn-fork","name":"Agent","input":{"subagent_type":"fork"}}],
                "usage": {"input_tokens": 20, "output_tokens": 10}
            }
        });
        let result = json!({
            "type": "user", "timestamp": "2026-10-01T16:48:22Z",
            "message": {"content": [{"type":"tool_result","tool_use_id":"spawn-fork","content":"Fork started"}]}
        });
        let child = json!({
            "type": "assistant", "timestamp": "2026-10-01T16:48:23Z",
            "message": {
                "id": "child-request", "model": "claude-test",
                "content": [{"type":"text","text":"Analyzed"}],
                "usage": {"input_tokens": 30, "output_tokens": 5}
            }
        });
        let dir = tempfile::tempdir().unwrap();
        let main_path = dir.path().join("main.jsonl");
        std::fs::write(&main_path, format!("{parent}\n{result}\n")).unwrap();
        let registry = Registry::default_agents();
        let mut translator = registry.create("claude-code", session);
        let ctx = SessionCtx {
            session_id: session.into(),
            config: None,
        };
        let mut events = vec![
            claude_event(
                session,
                "UserPromptSubmit",
                base,
                json!({"prompt":"delegate","transcript_path":main_path}),
            ),
            claude_event(
                session,
                "SubagentStart",
                base + 2_000,
                json!({"agent_id":"fork-1","agent_type":"fork"}),
            ),
        ];
        let stop = claude_event(
            session,
            "Stop",
            base + 2_500,
            json!({"last_assistant_message":"Working in background"}),
        );
        if parent_stops_first {
            events.push(stop.clone());
        }
        events.push(claude_event(session, "SubagentStop", base + 4_000, json!({
            "agent_id":"fork-1","agent_type":"fork","agent_transcript_path":"C:\\sessions\\fork.jsonl",
            "last_assistant_message":"Analyzed",
            "_bt_transcript_snapshot": {
                "path":"C:\\sessions\\fork.jsonl",
                "contents":format!("{{\"type\":\"fork-context-ref\"}}\n{parent}\n{result}\n{child}\n")
            }
        })));
        if !parent_stops_first {
            events.push(stop);
        }
        let mut ops = Vec::new();
        for event in events {
            ops.extend(translator.handle(&event, &ctx).unwrap());
            while let Some(batch) = translator.drain_pending(&ctx).unwrap() {
                ops.extend(batch);
            }
        }
        let rows = reduce(ops);
        let llms: Vec<_> = rows
            .values()
            .filter(|row| row.span_type == SpanType::Llm)
            .collect();
        assert_eq!(
            llms.len(),
            2,
            "inherited parent request is not a new child call"
        );
        assert_eq!(
            llms.iter()
                .map(|row| row.metrics.as_ref().unwrap()["completion_tokens"]
                    .as_u64()
                    .unwrap())
                .sum::<u64>(),
            15
        );
        let turn = rows.values().find(|row| row.name == "Turn 1").unwrap();
        let fork = rows
            .values()
            .find(|row| row.name == "subagent: fork")
            .unwrap();
        let parent_call = llms
            .iter()
            .find(|row| row.metadata.as_ref().unwrap()["request_id"] == "parent-request")
            .unwrap();
        assert_eq!(parent_call.parent_span_ids, vec![turn.span_id.clone()]);
        let child_call = llms
            .iter()
            .find(|row| row.metadata.as_ref().unwrap()["request_id"] == "child-request")
            .unwrap();
        assert_eq!(child_call.parent_span_ids, vec![fork.span_id.clone()]);
        let input = child_call.input.as_ref().unwrap().as_array().unwrap();
        assert_eq!(input[0]["tool_calls"][0]["id"], "spawn-fork");
        assert_eq!(input[1]["content"], "Fork started");
        let tools: Vec<_> = rows
            .values()
            .filter(|row| row.span_type == SpanType::Tool)
            .collect();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].parent_span_ids, vec![turn.span_id.clone()]);
        assert_eq!(tools[0].output, Some(json!("Fork started")));
    }
}

#[test]
fn claude_large_catch_up_emits_one_historical_snapshot_per_batch() {
    const CALLS: usize = 24;
    const MESSAGE_BYTES: usize = 64 * 1024;
    let transcript = tempfile::NamedTempFile::new().unwrap();
    let transcript_path = transcript.path().to_str().unwrap();
    let mut records = Vec::with_capacity(CALLS * 2);
    for index in 0..CALLS {
        records.push(json!({
            "type": "user",
            "timestamp": "2026-07-28T16:00:00Z",
            "message": {"role":"user", "content": "x".repeat(MESSAGE_BYTES)}
        }));
        records.push(json!({
            "type": "assistant",
            "timestamp": "2026-07-28T16:00:01Z",
            "message": {
                "id": format!("request-{index}"),
                "model": "claude-test",
                "role": "assistant",
                "content": [{"type":"text", "text":format!("answer-{index}")}],
                "usage": {"input_tokens":1,"output_tokens":1}
            }
        }));
    }
    let contents = records
        .iter()
        .map(Value::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(transcript.path(), &contents).unwrap();

    let registry = Registry::default_agents();
    let mut translator = registry.create("claude-code", "bounded");
    let ctx = SessionCtx {
        session_id: "bounded".into(),
        config: None,
    };
    let envelope = |name: &str, payload: Value| Envelope {
        source: "claude-code".into(),
        source_version: None,
        plugin_version: None,
        session_id: "bounded".into(),
        event: name.into(),
        ts_ms: 2_000_000_000_000,
        managed_run_id: None,
        payload,
        route: None,
        config: None,
        capture: None,
    };
    translator
        .handle(
            &envelope(
                "UserPromptSubmit",
                json!({"session_id":"bounded","prompt":"go"}),
            ),
            &ctx,
        )
        .unwrap();
    let mut batch = translator
        .handle(
            &envelope(
                "Stop",
                json!({
                    "session_id":"bounded",
                    "transcript_path":transcript_path,
                    "_bt_transcript_snapshot":{"path":transcript_path,"contents":contents}
                }),
            ),
            &ctx,
        )
        .unwrap();
    let mut llm_count = 0;
    loop {
        let batch_llms = batch
            .iter()
            .filter(|op| matches!(op, SpanOp::Insert(row) if row.span_type == SpanType::Llm))
            .count();
        assert!(
            batch_llms <= 1,
            "catch-up batch materialized {batch_llms} LLM inputs"
        );
        llm_count += batch_llms;
        let Some(next) = translator.drain_pending(&ctx).unwrap() else {
            break;
        };
        batch = next;
    }
    assert_eq!(llm_count, CALLS);
}

#[test]
fn claude_post_compact_replaces_old_prefix_and_preserves_recent_window() {
    let base = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
        .unwrap()
        .timestamp_millis();
    let transcript = tempfile::NamedTempFile::new().unwrap();
    let transcript_path = transcript.path().to_str().unwrap();
    let initial = [
        json!({
            "type":"user",
            "uuid":"old-user",
            "timestamp":"2026-01-01T00:00:01Z",
            "message":{"role":"user","content":"old question"}
        }),
        json!({
            "type":"assistant",
            "uuid":"old-assistant",
            "timestamp":"2026-01-01T00:00:02Z",
            "message":{"id":"old-request","model":"claude-test","role":"assistant","content":[{"type":"text","text":"old answer"}],"usage":{"input_tokens":1,"output_tokens":1}}
        }),
        json!({
            "type":"user",
            "uuid":"recent-user",
            "timestamp":"2026-01-01T00:00:02.100Z",
            "message":{"role":"user","content":"recent question"}
        }),
        json!({
            "type":"assistant",
            "uuid":"recent-assistant-text",
            "timestamp":"2026-01-01T00:00:02.200Z",
            "message":{"id":"recent-request","model":"claude-test","role":"assistant","content":[{"type":"text","text":"recent answer"}],"usage":{"input_tokens":1,"output_tokens":1}}
        }),
        json!({
            "type":"assistant",
            "uuid":"recent-assistant-tool",
            "timestamp":"2026-01-01T00:00:02.300Z",
            "message":{"id":"recent-request","model":"claude-test","role":"assistant","content":[{"type":"tool_use","id":"tool-1","name":"Read","input":{"file_path":"README.md"}}],"usage":{"input_tokens":1,"output_tokens":1}}
        }),
        json!({
            "type":"user",
            "uuid":"recent-tool-result",
            "timestamp":"2026-01-01T00:00:02.400Z",
            "message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"tool-1","content":"contents"}]}
        }),
        json!({
            "type":"attachment",
            "uuid":"recent-attachment",
            "timestamp":"2026-01-01T00:00:02.500Z",
            "message":{"role":"user","content":"attachment metadata"}
        }),
    ];
    std::fs::write(
        transcript.path(),
        initial
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n"),
    )
    .unwrap();

    let registry = Registry::default_agents();
    let mut translator = registry.create("claude-code", "compacted");
    let ctx = SessionCtx {
        session_id: "compacted".into(),
        config: None,
    };
    let envelope = |event: &str, ts_ms: i64, prompt: Option<&str>| Envelope {
        source: "claude-code".into(),
        source_version: None,
        plugin_version: None,
        session_id: "compacted".into(),
        event: event.into(),
        ts_ms,
        managed_run_id: None,
        payload: json!({
            "session_id":"compacted",
            "transcript_path":transcript_path,
            "prompt":prompt,
        }),
        route: None,
        config: None,
        capture: None,
    };
    let mut ops = Vec::new();
    for event in [
        envelope("UserPromptSubmit", base + 1_000, Some("old question")),
        envelope("Stop", base + 2_000, None),
    ] {
        ops.extend(translator.handle(&event, &ctx).unwrap());
        while let Some(batch) = translator.drain_pending(&ctx).unwrap() {
            ops.extend(batch);
        }
    }

    let compacted = [
        json!({
            "type":"system",
            "subtype":"compact_boundary",
            "timestamp":"2026-01-01T00:00:03Z",
            "compactMetadata":{
                "trigger":"manual",
                "preservedSegment":{
                    "headUuid":"recent-user",
                    "anchorUuid":"compact-summary",
                    "tailUuid":"recent-attachment"
                },
                "preservedMessages":{
                    "anchorUuid":"compact-summary",
                    "uuids":[
                        "recent-user",
                        "recent-assistant-text",
                        "recent-assistant-tool",
                        "recent-tool-result",
                        "recent-attachment"
                    ],
                    "allUuids":[
                        "recent-user",
                        "recent-assistant-text",
                        "recent-assistant-tool",
                        "recent-tool-result",
                        "internal-queue-record",
                        "recent-attachment"
                    ]
                }
            }
        }),
        json!({
            "type":"user",
            "uuid":"compact-summary",
            "isCompactSummary":true,
            "timestamp":"2026-01-01T00:00:04Z",
            "message":{"role":"user","content":"compact summary"}
        }),
    ];
    let previous = std::fs::read_to_string(transcript.path()).unwrap();
    std::fs::write(
        transcript.path(),
        format!(
            "{previous}\n{}",
            compacted
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n")
        ),
    )
    .unwrap();
    ops.extend(
        translator
            .handle(&envelope("PostCompact", base + 4_000, None), &ctx)
            .unwrap(),
    );
    while let Some(batch) = translator.drain_pending(&ctx).unwrap() {
        ops.extend(batch);
    }

    let post_compact = [
        json!({
            "type":"user",
            "timestamp":"2026-01-01T00:00:05Z",
            "message":{"role":"user","content":"new question"}
        }),
        json!({
            "type":"assistant",
            "timestamp":"2026-01-01T00:00:06Z",
            "message":{"id":"new-request","model":"claude-test","role":"assistant","content":[{"type":"text","text":"new answer"}],"usage":{"input_tokens":2,"output_tokens":1}}
        }),
    ];
    let previous = std::fs::read_to_string(transcript.path()).unwrap();
    std::fs::write(
        transcript.path(),
        format!(
            "{previous}\n{}",
            post_compact
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n")
        ),
    )
    .unwrap();
    for event in [
        envelope("UserPromptSubmit", base + 5_000, Some("new question")),
        envelope("Stop", base + 6_000, None),
    ] {
        ops.extend(translator.handle(&event, &ctx).unwrap());
        while let Some(batch) = translator.drain_pending(&ctx).unwrap() {
            ops.extend(batch);
        }
    }

    let rows = reduce(ops);
    let llm = rows
        .values()
        .find(|row| {
            row.span_type == SpanType::Llm
                && row.metadata.as_ref().unwrap()["request_id"] == "new-request"
        })
        .unwrap();
    let messages = llm.input.as_ref().unwrap().as_array().unwrap();
    assert_eq!(messages.len(), 5);
    assert_eq!(messages[0]["message_type"], "compaction_summary");
    assert_eq!(messages[0]["content"], "compact summary");
    assert_eq!(messages[1]["role"], "user");
    assert_eq!(messages[1]["content"], "recent question");
    assert_eq!(messages[2]["role"], "assistant");
    assert_eq!(messages[2]["content"], "recent answer");
    assert_eq!(messages[2]["tool_calls"][0]["id"], "tool-1");
    assert_eq!(messages[3]["role"], "tool");
    assert_eq!(messages[3]["tool_call_id"], "tool-1");
    assert_eq!(messages[3]["content"], "contents");
    assert_eq!(messages[4]["content"], "new question");
    assert!(!messages.iter().any(|message| {
        matches!(
            message.get("content").and_then(Value::as_str),
            Some("old question" | "old answer" | "attachment metadata")
        )
    }));
}

#[test]
fn claude_late_model_records_keep_prompt_ownership_across_turns() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("main.jsonl");
    let records = [
        json!({"type":"user","uuid":"user-a","promptId":"prompt-a","message":{"content":"first"}}),
        json!({"type":"assistant","uuid":"answer-a","parentUuid":"user-a","message":{"id":"request-a","model":"claude-test","content":[{"type":"text","text":"first answer"}],"usage":{"input_tokens":11,"output_tokens":3}}}),
        json!({"type":"user","uuid":"user-b","parentUuid":"answer-a","promptId":"prompt-b","message":{"content":"second"}}),
        json!({"type":"assistant","uuid":"answer-b","parentUuid":"user-b","message":{"id":"request-b","model":"claude-test","content":[{"type":"text","text":"second answer"}],"usage":{"input_tokens":17,"output_tokens":5}}}),
        // Neither a repeated native row nor a queue-only copy may consume the
        // third submission, even though its prompt text repeats the first.
        json!({"type":"user","uuid":"user-a","promptId":"prompt-a","message":{"content":"first"}}),
        json!({"type":"user","uuid":"queued-c","queueTranscriptOnly":true,"message":{"content":"first"}}),
        json!({"type":"user","uuid":"user-c","parentUuid":"answer-b","message":{"content":[{"type":"text","text":"first"}]}}),
        json!({"type":"assistant","uuid":"answer-c","parentUuid":"user-c","message":{"id":"request-c","model":"claude-test","content":[{"type":"text","text":"third answer"}],"usage":{"input_tokens":19,"output_tokens":7}}}),
        // A late descendant of the first response must follow ancestry, not
        // the most recently encountered prompt. Its tool has no live hook.
        json!({"type":"assistant","uuid":"late-a","parentUuid":"answer-a","message":{"id":"request-late-a","model":"claude-test","content":[{"type":"text","text":"late first answer"},{"type":"tool_use","id":"tool-a","name":"Read","input":{"file_path":"a.txt"}}],"usage":{"input_tokens":23,"output_tokens":9}}}),
        json!({"type":"user","uuid":"result-a","parentUuid":"late-a","message":{"content":[{"type":"tool_result","tool_use_id":"tool-a","content":"file contents"}]}}),
    ];
    let contents = records.iter().map(|r| format!("{r}\n")).collect::<String>();
    std::fs::write(&path, &contents).unwrap();
    let frozen = |through| {
        json!({
            "transcript_path":path,
            "_bt_claude_transcript_mirrors": {
                path.to_str().unwrap(): {"path":path,"mirror":path,"through":through}
            }
        })
    };
    let mut first = frozen(0);
    first["prompt"] = json!("first");
    let mut second = frozen(0);
    second["prompt"] = json!("second");
    let rows = replay_continuation_events(
        vec![
            ("UserPromptSubmit", 10, first.clone()),
            ("Stop", 20, frozen(0)),
            ("UserPromptSubmit", 30, second),
            ("Stop", 40, frozen(0)),
            ("UserPromptSubmit", 50, first),
            ("Stop", 60, frozen(contents.len())),
        ],
        false,
    );
    for (request, turn, answer, tokens) in [
        ("request-a", "Turn 1", "first answer", 3),
        ("request-b", "Turn 2", "second answer", 5),
        ("request-c", "Turn 3", "third answer", 7),
        ("request-late-a", "Turn 1", "late first answer", 9),
    ] {
        let owner = rows.values().find(|r| r.name == turn).unwrap();
        let llm = rows
            .values()
            .find(|r| {
                r.metadata.as_ref().and_then(|m| m.get("request_id")) == Some(&json!(request))
            })
            .unwrap();
        assert_eq!(
            llm.parent_span_ids,
            vec![owner.span_id.clone()],
            "{request}"
        );
        assert_eq!(llm.output.as_ref().unwrap()["content"], answer);
        assert_eq!(llm.metrics.as_ref().unwrap()["completion_tokens"], tokens);
    }
    let first = rows.values().find(|r| r.name == "Turn 1").unwrap();
    let tool = rows
        .values()
        .find(|r| r.span_type == SpanType::Tool)
        .unwrap();
    assert_eq!(tool.parent_span_ids, vec![first.span_id.clone()]);
    assert_eq!(tool.output, Some(json!("file contents")));
}

#[test]
fn claude_late_native_handback_records_belong_to_the_continuation() {
    let handback =
        "<task-notification><task-id>worker</task-id><summary>Done</summary></task-notification>";
    let records = [
        json!({"type":"user","uuid":"delegate","promptId":"p1","message":{"content":"delegate"}}),
        json!({"type":"assistant","uuid":"answer-1","parentUuid":"delegate","message":{"id":"request-1","content":[{"type":"text","text":"working"}]}}),
        json!({"type":"user","uuid":"unrelated","promptId":"p2","message":{"content":"unrelated"}}),
        json!({"type":"assistant","uuid":"answer-2","parentUuid":"unrelated","message":{"id":"request-2","content":[{"type":"text","text":"other answer"}]}}),
        json!({"type":"user","uuid":"queued-handback","queueTranscriptOnly":true,"message":{"content":handback}}),
        json!({"type":"user","uuid":"handback","promptId":"p3","parentUuid":"answer-2","message":{"content":handback}}),
        json!({"type":"assistant","uuid":"answer-3","parentUuid":"handback","message":{"id":"request-3","content":[{"type":"text","text":"worker result"}]}}),
    ];
    let contents = records.iter().map(|r| format!("{r}\n")).collect::<String>();
    let rows = replay_continuation_events(
        vec![
            ("UserPromptSubmit", 10, json!({"prompt":"delegate"})),
            (
                "SubagentStart",
                11,
                json!({"agent_id":"worker","agent_type":"Explore"}),
            ),
            ("Stop", 20, json!({})),
            ("UserPromptSubmit", 30, json!({"prompt":"unrelated"})),
            ("Stop", 40, json!({})),
            ("UserPromptSubmit", 50, json!({"prompt":handback})),
            ("Stop", 60, json!({})),
            (
                "TranscriptUpdate",
                70,
                json!({
                    "transcript_path":"main.jsonl",
                    "_bt_transcript_snapshot":{"path":"main.jsonl","contents":contents}
                }),
            ),
        ],
        false,
    );
    let first = rows.values().find(|r| r.name == "Turn 1").unwrap();
    let continuation = rows
        .values()
        .find(|r| r.name == "Continuation: subagent result")
        .unwrap();
    assert_eq!(continuation.parent_span_ids, vec![first.span_id.clone()]);
    assert_eq!(
        rows.values()
            .filter(|r| r.name.starts_with("Turn "))
            .count(),
        2
    );
    for (request, owner) in [
        ("request-1", first),
        (
            "request-2",
            rows.values().find(|r| r.name == "Turn 2").unwrap(),
        ),
        ("request-3", continuation),
    ] {
        let llm = rows
            .values()
            .find(|r| {
                r.metadata.as_ref().and_then(|m| m.get("request_id")) == Some(&json!(request))
            })
            .unwrap();
        assert_eq!(
            llm.parent_span_ids,
            vec![owner.span_id.clone()],
            "{request}"
        );
    }
}

#[test]
fn claude_frozen_empty_capture_does_not_read_future_native_bytes() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("late.jsonl");
    std::fs::write(&path, format!("{}\n",json!({"type":"assistant","message":{"id":"future","content":[{"type":"text","text":"not captured"}]}}))).unwrap();
    for boundary in [
        json!({"_bt_claude_transcript_mirrors":{}}),
        json!({"_bt_transcript_replay":true}),
    ] {
        let mut payload = boundary;
        payload["transcript_path"] = json!(path);
        payload["prompt"] = json!("hello");
        let rows = replay_continuation_events(
            vec![
                ("UserPromptSubmit", 10, payload.clone()),
                ("Stop", 20, payload),
            ],
            true,
        );
        assert!(!rows.values().any(|row| row.span_type == SpanType::Llm));
    }
}

#[test]
fn claude_finalization_drains_observed_late_response() {
    let transcript = format!(
        "{}\n",
        json!({
            "type":"assistant","message":{"id":"late-final","model":"claude-test",
            "content":[{"type":"text","text":"late answer"}],"usage":{"input_tokens":13,"output_tokens":7}}
        })
    );
    let rows = replay_continuation_events(
        vec![
            ("UserPromptSubmit", 10, json!({"prompt":"hello"})),
            ("Stop", 20, json!({})),
            (
                "Notification",
                30,
                json!({
                    "transcript_path":"late.jsonl",
                    "_bt_transcript_snapshot":{"path":"late.jsonl","contents":transcript}
                }),
            ),
        ],
        true,
    );
    let turn = rows.values().find(|r| r.name == "Turn 1").unwrap();
    let llm = rows
        .values()
        .find(|r| r.span_type == SpanType::Llm)
        .unwrap();
    assert_eq!(llm.parent_span_ids, vec![turn.span_id.clone()]);
    assert_eq!(llm.output.as_ref().unwrap()["content"], "late answer");
    assert_eq!(llm.metrics.as_ref().unwrap()["completion_tokens"], 7);
}
