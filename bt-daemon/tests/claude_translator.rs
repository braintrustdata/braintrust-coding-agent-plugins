use bt_daemon::wire::Envelope;
use bt_daemon::{Registry, SessionCtx, SpanOp, SpanRow, SpanType};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/claude")
        .join(name)
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
fn claude_subagent_fixture_builds_nested_subagent_llms() {
    let rows = reduce(replay("subagent-compact"));
    let subagents: Vec<_> = rows
        .values()
        .filter(|row| row.name.starts_with("subagent:"))
        .collect();
    assert!(subagents.len() >= 2);
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
    let event = |name: &str, payload: Value| Envelope {
        source: "claude-code".into(),
        source_version: None,
        plugin_version: None,
        session_id: "s".into(),
        event: name.into(),
        ts_ms: 1,
        managed_run_id: None,
        payload,
        route: None,
        config: None,
    };
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
                        "permission":{"id":"p1","type":"tool","title":"Run command"}
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
    assert_eq!(tools.len(), 2);
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
    assert!(tools
        .iter()
        .any(|row| row.error.as_deref() == Some("missing")));
    assert!(rows.values().all(|row| {
        let metadata = row.metadata.as_ref().and_then(Value::as_object).unwrap();
        metadata.get("git_origin_url") == Some(&json!("https://example.com/acme/app.git"))
            && metadata.get("git_branch") == Some(&json!("main"))
            && metadata.contains_key("git_commit_sha")
    }));
}

#[test]
fn claude_pairs_tool_lifecycle_and_marks_explicit_skills_and_stop_failures() {
    let registry = Registry::default_agents();
    let mut translator = registry.create("claude-code", "lifecycle");
    let ctx = SessionCtx {
        session_id: "lifecycle".into(),
        config: None,
    };
    let event = |name: &str, ts_ms: i64, payload: Value| Envelope {
        source: "claude-code".into(),
        source_version: Some("2.0.0".into()),
        plugin_version: None,
        session_id: "lifecycle".into(),
        event: name.into(),
        ts_ms,
        managed_run_id: None,
        payload,
        route: None,
        config: None,
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
            json!({"session_id":"lifecycle","error":"model process exited"}),
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
