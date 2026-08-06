//! Phase 3 core: the Codex translator turns a transcript ("rollout" JSONL) plus
//! hook triggers into a session → turn → {llm, tool} span tree. Mirrors the
//! happy-path shape of the TS `event-processor` tests.

use bt_daemon::wire::{BackendAuth, Envelope, FlushMode, SessionConfig};
use bt_daemon::{Registry, SessionCtx, SpanOp, SpanRow, SpanType};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::Write;

fn line(v: Value) -> String {
    serde_json::to_string(&v).unwrap()
}

/// Write the full happy-path transcript to `path`.
fn write_transcript(path: &std::path::Path) {
    let records = vec![
        json!({ "timestamp": "2026-01-01T00:00:01Z", "type": "session_meta",
                "payload": { "id": "session-1", "cwd": "/whatever/myapp", "cli_version": "1.2.3" } }),
        json!({ "timestamp": "2026-01-01T00:00:02Z", "type": "turn_context",
                "payload": { "model": "gpt-5.5" } }),
        json!({ "timestamp": "2026-01-01T00:00:03Z", "type": "event_msg",
                "payload": { "type": "task_started", "turn_id": "t1" } }),
        json!({ "timestamp": "2026-01-01T00:00:04Z", "type": "event_msg",
                "payload": { "type": "user_message", "message": "list the files" } }),
        json!({ "timestamp": "2026-01-01T00:00:05Z", "type": "response_item",
                "payload": { "type": "reasoning",
                             "summary": [{ "type": "summary_text", "text": "I'll run ls" }],
                             "encrypted_content": "opaque" } }),
        json!({ "timestamp": "2026-01-01T00:00:06Z", "type": "response_item",
                "payload": { "type": "message", "role": "assistant",
                             "content": [{ "type": "output_text", "text": "Running ls." }] } }),
        json!({ "timestamp": "2026-01-01T00:00:07Z", "type": "response_item",
                "payload": { "type": "function_call", "call_id": "c1", "name": "shell",
                             "arguments": "{\"command\":\"ls\"}", "metadata": { "turn_id": "t1" } } }),
        json!({ "timestamp": "2026-01-01T00:00:08Z", "type": "event_msg",
                "payload": { "type": "token_count",
                             "info": { "last_token_usage": { "input_tokens": 100, "output_tokens": 20, "total_tokens": 120 } } } }),
        json!({ "timestamp": "2026-01-01T00:00:09Z", "type": "response_item",
                "payload": { "type": "function_call_output", "call_id": "c1", "output": "README.md\nsrc" } }),
        json!({ "timestamp": "2026-01-01T00:00:10Z", "type": "event_msg",
                "payload": { "type": "task_complete", "last_agent_message": "Here are the files." } }),
    ];
    let mut f = std::fs::File::create(path).unwrap();
    for r in records {
        writeln!(f, "{}", line(r)).unwrap();
    }
}

fn envelope(session: &str, event: &str, transcript_path: &str, extra: Value) -> Envelope {
    let mut payload = json!({ "session_id": session, "hook_event_name": event, "transcript_path": transcript_path });
    if let (Value::Object(p), Value::Object(e)) = (&mut payload, &extra) {
        for (k, v) in e {
            p.insert(k.clone(), v.clone());
        }
    }
    Envelope {
        source: "codex".into(),
        source_version: None,
        session_id: session.into(),
        event: event.into(),
        ts_ms: 0,
        payload,
        route: None,
        config: None,
    }
}

/// Reduce a stream of span ops into final rows, applying merges by span_id.
fn reduce(ops: Vec<SpanOp>) -> HashMap<String, SpanRow> {
    let mut map: HashMap<String, SpanRow> = HashMap::new();
    for op in ops {
        match op {
            SpanOp::Insert(r) => {
                map.insert(r.span_id.clone(), r);
            }
            SpanOp::Merge(r) => {
                let e = map.entry(r.span_id.clone()).or_insert_with(|| r.clone());
                if r.end_ms.is_some() {
                    e.end_ms = r.end_ms;
                }
                if r.output.is_some() {
                    e.output = r.output.clone();
                }
                if r.input.is_some() {
                    e.input = r.input.clone();
                }
                if r.metrics.is_some() {
                    e.metrics = r.metrics.clone();
                }
                if r.error.is_some() {
                    e.error = r.error.clone();
                }
                if !r.parent_span_ids.is_empty() {
                    e.parent_span_ids = r.parent_span_ids.clone();
                }
                if !r.name.is_empty() {
                    e.name = r.name.clone();
                }
                if let Some(t) = &r.tags {
                    e.tags = Some(t.clone());
                }
                // Merge metadata objects key-by-key.
                if let Some(Value::Object(incoming)) = &r.metadata {
                    let base = match e.metadata.take() {
                        Some(Value::Object(m)) => m,
                        _ => serde_json::Map::new(),
                    };
                    let mut merged = base;
                    for (k, v) in incoming {
                        merged.insert(k.clone(), v.clone());
                    }
                    e.metadata = Some(Value::Object(merged));
                }
            }
        }
    }
    map
}

fn find<'a>(rows: &'a HashMap<String, SpanRow>, ty: SpanType, name: &str) -> &'a SpanRow {
    rows.values()
        .find(|r| r.span_type == ty && r.name == name)
        .unwrap_or_else(|| {
            panic!(
                "no {ty:?} span named {name:?}; have: {:?}",
                rows.values()
                    .map(|r| (&r.name, r.span_type))
                    .collect::<Vec<_>>()
            )
        })
}

#[test]
fn codex_happy_path_builds_session_turn_llm_tool_tree() {
    let tmp = tempfile::tempdir().unwrap();
    let transcript = tmp.path().join("rollout.jsonl");
    write_transcript(&transcript);
    let tpath = transcript.to_str().unwrap();

    let reg = Registry::default_agents();
    let mut tr = reg.create("codex", "sess-1");
    let ctx = SessionCtx {
        session_id: "sess-1".into(),
        config: None,
    };

    let mut ops = Vec::new();
    // SessionStart carries source/permission_mode and triggers the first read.
    ops.extend(
        tr.handle(
            &envelope(
                "sess-1",
                "SessionStart",
                tpath,
                json!({ "source": "startup", "permission_mode": "auto" }),
            ),
            &ctx,
        )
        .unwrap(),
    );
    // A later trigger (Stop) — nothing new in the transcript here.
    ops.extend(
        tr.handle(&envelope("sess-1", "Stop", tpath, json!({})), &ctx)
            .unwrap(),
    );
    ops.extend(tr.flush(&ctx).unwrap());

    let rows = reduce(ops);

    // Root (session).
    let root = find(&rows, SpanType::Task, "codex: myapp");
    assert!(
        root.parent_span_ids.is_empty(),
        "root should have no parent"
    );
    let md = root.metadata.as_ref().unwrap();
    assert_eq!(md["session_id"], json!("session-1"));
    assert_eq!(
        md["model"],
        json!("gpt-5.5"),
        "model backfilled from turn_context"
    );
    assert_eq!(md["source"], json!("startup"));
    assert_eq!(md["permission_mode"], json!("auto"));

    // Turn.
    let turn = find(&rows, SpanType::Task, "turn: t1");
    assert_eq!(turn.parent_span_ids, vec![root.span_id.clone()]);
    assert_eq!(turn.input, Some(json!("list the files")));
    assert_eq!(turn.output, Some(json!("Here are the files.")));
    assert!(
        turn.end_ms.is_some(),
        "turn should be closed by task_complete"
    );

    // LLM span under the turn, with token metrics.
    let llm = find(&rows, SpanType::Llm, "gpt-5.5");
    assert_eq!(llm.parent_span_ids, vec![turn.span_id.clone()]);
    assert!(llm.end_ms.is_some(), "llm closed by token_count");
    let m = llm.metrics.as_ref().unwrap();
    assert_eq!(m["prompt_tokens"], json!(100.0));
    assert_eq!(m["completion_tokens"], json!(20.0));
    assert_eq!(m["tokens"], json!(120.0));
    assert_eq!(
        llm.output.as_ref().unwrap()[0]["summary"][0],
        json!({ "type": "summary_text", "text": "I'll run ls" })
    );

    // Tool span under the turn.
    let tool = find(&rows, SpanType::Tool, "shell");
    assert_eq!(tool.parent_span_ids, vec![turn.span_id.clone()]);
    assert_eq!(tool.input, Some(json!("{\"command\":\"ls\"}")));
    assert_eq!(tool.output, Some(json!("README.md\nsrc")));
    assert!(tool.end_ms.is_some(), "tool closed by function_call_output");

    // Exactly one of each in this trace.
    assert_eq!(
        rows.values()
            .filter(|r| r.span_type == SpanType::Task)
            .count(),
        2
    );
    assert_eq!(
        rows.values()
            .filter(|r| r.span_type == SpanType::Llm)
            .count(),
        1
    );
    assert_eq!(
        rows.values()
            .filter(|r| r.span_type == SpanType::Tool)
            .count(),
        1
    );
}

#[test]
fn codex_incremental_reads_advance_offset() {
    // Two reads: the second only sees records appended after the first.
    let tmp = tempfile::tempdir().unwrap();
    let transcript = tmp.path().join("rollout.jsonl");
    let tpath = transcript.to_str().unwrap();

    let reg = Registry::default_agents();
    let mut tr = reg.create("codex", "s");
    let ctx = SessionCtx {
        session_id: "s".into(),
        config: None,
    };

    let mut f = std::fs::File::create(&transcript).unwrap();
    writeln!(f, "{}", line(json!({ "timestamp": "2026-01-01T00:00:01Z", "type": "session_meta", "payload": { "id": "s", "cwd": "/x/app" } }))).unwrap();
    f.flush().unwrap();

    let first = tr
        .handle(&envelope("s", "SessionStart", tpath, json!({})), &ctx)
        .unwrap();
    assert_eq!(first.len(), 1, "first read: just the root insert");

    writeln!(f, "{}", line(json!({ "timestamp": "2026-01-01T00:00:02Z", "type": "event_msg", "payload": { "type": "task_started", "turn_id": "t1" } }))).unwrap();
    f.flush().unwrap();

    let second = tr
        .handle(&envelope("s", "UserPromptSubmit", tpath, json!({})), &ctx)
        .unwrap();
    assert_eq!(second.len(), 1, "second read: only the new turn insert");
    match &second[0] {
        SpanOp::Insert(r) => assert_eq!(r.name, "turn: t1"),
        _ => panic!("expected a turn insert"),
    }
}

#[test]
fn codex_import_checkpoints_preserve_native_turn_boundaries() {
    let tmp = tempfile::tempdir().unwrap();
    let transcript = tmp.path().join("rollout.jsonl");
    let tpath = transcript.to_str().unwrap();
    for value in [
        json!({ "timestamp": "2026-01-01T00:00:01Z", "type": "session_meta", "payload": { "id": "s", "cwd": "/x/app" } }),
        json!({ "timestamp": "2026-01-01T00:00:02Z", "type": "event_msg", "payload": { "type": "task_started", "turn_id": "t1" } }),
        json!({ "timestamp": "2026-01-01T00:00:03Z", "type": "event_msg", "payload": { "type": "task_complete", "turn_id": "t1" } }),
        json!({ "timestamp": "2026-01-01T00:00:04Z", "type": "event_msg", "payload": { "type": "task_started", "turn_id": "t2" } }),
    ] {
        append(&transcript, value);
    }

    let reg = Registry::default_agents();
    let mut translator = reg.create("codex", "s");
    let ctx = SessionCtx {
        session_id: "s".into(),
        config: None,
    };
    let first = translator
        .handle(
            &envelope(
                "s",
                "ImportCheckpoint",
                tpath,
                json!({ "_bt_import_through_ms": 1_767_225_603_000_i64 }),
            ),
            &ctx,
        )
        .unwrap();
    assert!(first
        .iter()
        .any(|op| matches!(op, SpanOp::Insert(row) if row.name == "turn: t1")));
    assert!(!first
        .iter()
        .any(|op| matches!(op, SpanOp::Insert(row) if row.name == "turn: t2")));

    let second = translator
        .handle(
            &envelope(
                "s",
                "ImportCheckpoint",
                tpath,
                json!({ "_bt_import_through_ms": 1_767_225_604_000_i64 }),
            ),
            &ctx,
        )
        .unwrap();
    assert!(second
        .iter()
        .any(|op| matches!(op, SpanOp::Insert(row) if row.name == "turn: t2")));
}

#[test]
fn codex_stop_closes_turn_before_late_task_complete() {
    let tmp = tempfile::tempdir().unwrap();
    let transcript = tmp.path().join("rollout.jsonl");
    let tpath = transcript.to_str().unwrap();
    for v in [
        json!({ "timestamp": "2026-01-01T00:00:01Z", "type": "session_meta",
                "payload": { "id": "s", "cwd": "/x/app" } }),
        json!({ "timestamp": "2026-01-01T00:00:02Z", "type": "event_msg",
                "payload": { "type": "task_started", "turn_id": "t1" } }),
        json!({ "timestamp": "2026-01-01T00:00:03Z", "type": "event_msg",
                "payload": { "type": "user_message", "message": "say done" } }),
    ] {
        append(&transcript, v);
    }

    let reg = Registry::default_agents();
    let mut tr = reg.create("codex", "s");
    let ctx = SessionCtx {
        session_id: "s".into(),
        config: None,
    };
    let mut ops = tr
        .handle(
            &envelope(
                "s",
                "Stop",
                tpath,
                json!({ "last_assistant_message": "done" }),
            ),
            &ctx,
        )
        .unwrap();
    ops.extend(tr.flush(&ctx).unwrap());
    let rows = reduce(ops);

    let turn = find(&rows, SpanType::Task, "turn: t1");
    assert_eq!(turn.end_ms, Some(0), "Stop hook closes the active turn");
    assert_eq!(turn.output, Some(json!("done")));
}

// ---- compaction & subagent coverage --------------------------------------

fn append(path: &std::path::Path, v: Value) {
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    writeln!(f, "{}", line(v)).unwrap();
}

fn configured_ctx(session_id: &str, additional_metadata: Value) -> SessionCtx {
    SessionCtx {
        session_id: session_id.into(),
        config: Some(SessionConfig {
            auth: BackendAuth {
                token: "test-token".into(),
                api_url: None,
                app_url: None,
                org_name: None,
                org_id: None,
            },
            destination: Some(bt_daemon::wire::TraceDestination::ProjectLogs {
                project_id: None,
                project_name: Some("team-project".into()),
            }),
            flush_mode: FlushMode::FireAndForget,
            additional_metadata: Some(additional_metadata),
        }),
    }
}

#[test]
fn late_task_complete_is_correlated_by_turn_id() {
    let tmp = tempfile::tempdir().unwrap();
    let transcript = tmp.path().join("rollout.jsonl");
    let path = transcript.to_str().unwrap();
    for record in [
        json!({ "timestamp": "2026-01-01T00:00:01Z", "type": "session_meta",
                "payload": { "id": "s", "cwd": "/x/app" } }),
        json!({ "timestamp": "2026-01-01T00:00:02Z", "type": "event_msg",
                "payload": { "type": "task_started", "turn_id": "t1" } }),
    ] {
        append(&transcript, record);
    }

    let reg = Registry::default_agents();
    let mut translator = reg.create("codex", "s");
    let ctx = SessionCtx {
        session_id: "s".into(),
        config: None,
    };
    let mut ops = translator
        .handle(&envelope("s", "SessionStart", path, json!({})), &ctx)
        .unwrap();
    ops.extend(
        translator
            .handle(
                &envelope(
                    "s",
                    "Stop",
                    path,
                    json!({ "turn_id": "t1", "last_assistant_message": "one" }),
                ),
                &ctx,
            )
            .unwrap(),
    );

    // t2 begins before Codex appends t1's delayed task_complete.
    append(
        &transcript,
        json!({ "timestamp": "2026-01-01T00:00:03Z", "type": "event_msg",
                "payload": { "type": "task_started", "turn_id": "t2" } }),
    );
    append(
        &transcript,
        json!({ "timestamp": "2026-01-01T00:00:04Z", "type": "event_msg",
                "payload": { "type": "task_complete", "turn_id": "t1",
                             "last_agent_message": "one" } }),
    );
    ops.extend(
        translator
            .handle(&envelope("s", "UserPromptSubmit", path, json!({})), &ctx)
            .unwrap(),
    );

    let rows = reduce(ops);
    let t1 = find(&rows, SpanType::Task, "turn: t1");
    let t2 = find(&rows, SpanType::Task, "turn: t2");
    assert!(t1.end_ms.is_some());
    assert_eq!(t1.output, Some(json!("one")));
    assert_eq!(t2.end_ms, None, "late t1 completion must not close t2");
}

#[test]
fn root_preserves_config_input_and_git_metadata() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().join("repo");
    std::fs::create_dir(&repo).unwrap();
    let git = |args: &[&str]| {
        let status = std::process::Command::new("git")
            .arg("-C")
            .arg(&repo)
            .args(args)
            .status()
            .unwrap();
        assert!(status.success(), "git command failed: {args:?}");
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
        "https://secret@example.com/acme/app.git",
    ]);
    let commit = std::process::Command::new("git")
        .arg("-C")
        .arg(&repo)
        .args(["rev-parse", "HEAD"])
        .output()
        .unwrap();
    let commit = String::from_utf8(commit.stdout).unwrap().trim().to_string();

    let transcript = tmp.path().join("rollout.jsonl");
    for record in [
        json!({ "timestamp": "2026-01-01T00:00:01Z", "type": "session_meta",
                "payload": { "id": "s", "cwd": repo, "cli_version": "1.2.3" } }),
        json!({ "timestamp": "2026-01-01T00:00:02Z", "type": "turn_context",
                "payload": { "model": "gpt-5.5", "cwd": repo } }),
        json!({ "timestamp": "2026-01-01T00:00:03Z", "type": "event_msg",
                "payload": { "type": "task_started", "turn_id": "t1" } }),
        json!({ "timestamp": "2026-01-01T00:00:04Z", "type": "response_item",
                "payload": { "type": "message", "role": "assistant",
                             "content": [{ "type": "output_text", "text": "working" }] } }),
        json!({ "timestamp": "2026-01-01T00:00:05Z", "type": "event_msg",
                "payload": { "type": "token_count", "info": { "last_token_usage": {
                    "input_tokens": 1, "output_tokens": 1
                } } } }),
    ] {
        append(&transcript, record);
    }

    let reg = Registry::default_agents();
    let mut translator = reg.create("codex", "s");
    let ctx = configured_ctx("s", json!({ "team": "platform", "model": "wrong" }));
    let rows = reduce(
        translator
            .handle(
                &envelope(
                    "s",
                    "SessionStart",
                    transcript.to_str().unwrap(),
                    json!({ "source": "resume", "permission_mode": "acceptEdits" }),
                ),
                &ctx,
            )
            .unwrap(),
    );
    let root = find(&rows, SpanType::Task, "codex: repo");
    let metadata = root.metadata.as_ref().unwrap();
    assert_eq!(metadata["team"], json!("platform"));
    assert_eq!(metadata["model"], json!("gpt-5.5"));
    assert_eq!(metadata["project"], json!("team-project"));
    assert_eq!(
        metadata["git_origin_url"],
        json!("https://example.com/acme/app.git")
    );
    assert_eq!(metadata["git_branch"], json!("main"));
    assert_eq!(metadata["git_commit_sha"], json!(commit));
    assert_eq!(root.input.as_ref().unwrap()["model"], json!("gpt-5.5"));
    assert_eq!(root.input.as_ref().unwrap()["source"], json!("resume"));
    assert_eq!(root.input.as_ref().unwrap()["cwd"], json!(repo));
    assert!(rows.values().all(|row| {
        let metadata = row.metadata.as_ref().and_then(Value::as_object).unwrap();
        metadata.get("git_origin_url") == Some(&json!("https://example.com/acme/app.git"))
            && metadata.get("git_branch") == Some(&json!("main"))
            && metadata.get("git_commit_sha") == Some(&json!(commit))
    }));
}

#[test]
fn tool_and_llm_payloads_preserve_original_contract() {
    let tmp = tempfile::tempdir().unwrap();
    let transcript = tmp.path().join("rollout.jsonl");
    let path = transcript.to_str().unwrap();
    for record in [
        json!({ "timestamp": "2026-01-01T00:00:01Z", "type": "session_meta",
                "payload": { "id": "s", "cwd": "/x/app" } }),
        json!({ "timestamp": "2026-01-01T00:00:02Z", "type": "turn_context",
                "payload": { "model": "gpt-5.5" } }),
        json!({ "timestamp": "2026-01-01T00:00:03Z", "type": "event_msg",
                "payload": { "type": "task_started", "turn_id": "t1" } }),
        json!({ "timestamp": "2026-01-01T00:00:04Z", "type": "event_msg",
                "payload": { "type": "user_message", "message": "$review inspect this" } }),
        json!({ "timestamp": "2026-01-01T00:00:04Z", "type": "response_item",
                "payload": { "type": "message", "role": "user",
                             "content": [{ "type": "input_text", "text": "$review inspect this" }] } }),
        json!({ "timestamp": "2026-01-01T00:00:05Z", "type": "response_item",
                "payload": { "type": "function_call", "call_id": "c1", "name": "exec_command",
                             "arguments": "{\"cmd\":\"cat /tmp/review/SKILL.md\",\"sandbox_permissions\":\"require_escalated\",\"justification\":\"Need access\",\"prefix_rule\":[\"cat\"]}",
                             "metadata": { "turn_id": "t1" } } }),
        json!({ "timestamp": "2026-01-01T00:00:06Z", "type": "event_msg",
                "payload": { "type": "token_count", "info": { "last_token_usage": {
                    "input_tokens": 10, "output_tokens": 2, "cost": 0.25
                } } } }),
        json!({ "timestamp": "2026-01-01T00:00:07Z", "type": "response_item",
                "payload": { "type": "function_call_output", "call_id": "c1",
                             "output": { "status": "failed", "error": "boom" } } }),
        json!({ "timestamp": "2026-01-01T00:00:08Z", "type": "response_item",
                "payload": { "type": "message", "role": "assistant",
                             "content": [{ "type": "output_text", "text": "Recovered" }] } }),
        json!({ "timestamp": "2026-01-01T00:00:09Z", "type": "event_msg",
                "payload": { "type": "token_count", "info": { "last_token_usage": {} } } }),
        json!({ "timestamp": "2026-01-01T00:00:10Z", "type": "event_msg",
                "payload": { "type": "task_complete", "turn_id": "t1",
                             "last_agent_message": "done" } }),
    ] {
        append(&transcript, record);
    }

    let reg = Registry::default_agents();
    let mut translator = reg.create("codex", "s");
    let ctx = SessionCtx {
        session_id: "s".into(),
        config: None,
    };
    let rows = reduce(
        translator
            .handle(&envelope("s", "SessionStart", path, json!({})), &ctx)
            .unwrap(),
    );

    let turn = find(&rows, SpanType::Task, "turn: t1");
    assert_eq!(turn.input, Some(json!("$review inspect this")));
    assert_eq!(
        turn.metadata.as_ref().unwrap()["loaded_skill_names"],
        json!(["review"])
    );

    let tool = find(&rows, SpanType::Tool, "skill: review");
    assert!(tool.input.as_ref().unwrap().is_string());
    let metadata = tool.metadata.as_ref().unwrap();
    assert_eq!(metadata["tool_name"], json!("exec_command"));
    assert_eq!(metadata["call_id"], json!("c1"));
    assert_eq!(metadata["turn_id"], json!("t1"));
    assert_eq!(metadata["tool_kind"], json!("skill"));
    assert_eq!(metadata["skill_name"], json!("review"));
    assert_eq!(metadata["skill_path"], json!("/tmp/review/SKILL.md"));
    assert_eq!(metadata["skill_load_trigger"], json!("explicit"));
    assert_eq!(
        metadata["permission"]["sandbox_permissions"],
        json!("require_escalated")
    );
    assert_eq!(
        metadata["permission"]["justification"],
        json!("Need access")
    );
    assert_eq!(metadata["permission"]["prefix_rule"], json!(["cat"]));
    assert_eq!(metadata["tool_approval"], json!("approved"));
    assert_eq!(tool.tags, Some(vec!["permission-request".into()]));
    assert_eq!(tool.error.as_deref(), Some("boom"));

    let mut llms: Vec<&SpanRow> = rows
        .values()
        .filter(|row| row.span_type == SpanType::Llm)
        .collect();
    llms.sort_by_key(|row| row.start_ms);
    assert_eq!(llms.len(), 2);
    assert_eq!(
        llms[0].output.as_ref().unwrap()["tool_calls"][0]["function"]["arguments"],
        json!("{\"cmd\":\"cat /tmp/review/SKILL.md\",\"sandbox_permissions\":\"require_escalated\",\"justification\":\"Need access\",\"prefix_rule\":[\"cat\"]}")
    );
    assert_eq!(llms[0].metrics.as_ref().unwrap()["cost"], json!(0.25));
    assert_eq!(
        llms[0].metrics.as_ref().unwrap()["estimated_cost"],
        json!(0.25)
    );
    let second_input = llms[1].input.as_ref().unwrap().as_array().unwrap();
    assert_eq!(second_input.last().unwrap()["role"], json!("tool"));
    assert_eq!(second_input.last().unwrap()["tool_call_id"], json!("c1"));
    assert_eq!(
        llms[1].metadata.as_ref().unwrap()["usage_unavailable_reason"],
        json!("codex_token_count_missing_usage")
    );
}

#[test]
fn missing_tool_output_is_an_error() {
    let tmp = tempfile::tempdir().unwrap();
    let transcript = tmp.path().join("rollout.jsonl");
    for record in [
        json!({ "timestamp": "2026-01-01T00:00:01Z", "type": "session_meta",
                "payload": { "id": "s", "cwd": "/x/app" } }),
        json!({ "timestamp": "2026-01-01T00:00:02Z", "type": "event_msg",
                "payload": { "type": "task_started", "turn_id": "t1" } }),
        json!({ "timestamp": "2026-01-01T00:00:03Z", "type": "response_item",
                "payload": { "type": "function_call", "call_id": "c1", "name": "shell",
                             "arguments": "{}", "metadata": { "turn_id": "t1" } } }),
        json!({ "timestamp": "2026-01-01T00:00:04Z", "type": "event_msg",
                "payload": { "type": "task_complete", "turn_id": "t1",
                             "last_agent_message": "done" } }),
    ] {
        append(&transcript, record);
    }
    let reg = Registry::default_agents();
    let mut translator = reg.create("codex", "s");
    let ctx = SessionCtx {
        session_id: "s".into(),
        config: None,
    };
    let rows = reduce(
        translator
            .handle(
                &envelope("s", "SessionStart", transcript.to_str().unwrap(), json!({})),
                &ctx,
            )
            .unwrap(),
    );
    let tool = find(&rows, SpanType::Tool, "shell");
    assert_eq!(
        tool.error.as_deref(),
        Some("Tool output missing before turn ended")
    );
    assert_eq!(
        tool.metadata.as_ref().unwrap()["tool_approval"],
        json!("approved")
    );
}

#[test]
fn codex_compaction_relabels_turn_and_adds_compaction_llm() {
    let tmp = tempfile::tempdir().unwrap();
    let t = tmp.path().join("rollout.jsonl");
    let tpath = t.to_str().unwrap();
    for v in [
        json!({ "timestamp": "2026-01-01T00:00:01Z", "type": "session_meta", "payload": { "id": "s", "cwd": "/x/app" } }),
        json!({ "timestamp": "2026-01-01T00:00:02Z", "type": "turn_context", "payload": { "model": "gpt-5.5" } }),
        json!({ "timestamp": "2026-01-01T00:00:03Z", "type": "event_msg", "payload": { "type": "task_started", "turn_id": "t1" } }),
        json!({ "timestamp": "2026-01-01T00:00:04Z", "type": "compacted", "payload": {
            "window_id": "w1",
            "replacement_history": [
                { "role": "user", "content": "kept" },
                { "type": "compaction", "encrypted_content": "opaque" }
            ]
        } }),
        json!({ "timestamp": "2026-01-01T00:00:05Z", "type": "event_msg", "payload": { "type": "token_count", "info": { "last_token_usage": { "input_tokens": 5000, "output_tokens": 50 } } } }),
    ] {
        append(&t, v);
    }

    let reg = Registry::default_agents();
    let mut tr = reg.create("codex", "s");
    let ctx = SessionCtx {
        session_id: "s".into(),
        config: None,
    };

    let mut ops = Vec::new();
    ops.extend(
        tr.handle(&envelope("s", "SessionStart", tpath, json!({})), &ctx)
            .unwrap(),
    );
    // PostCompact closes the compaction turn and supplies the trigger.
    ops.extend(
        tr.handle(
            &envelope(
                "s",
                "PostCompact",
                tpath,
                json!({ "turn_id": "t1", "trigger": "auto" }),
            ),
            &ctx,
        )
        .unwrap(),
    );
    let rows = reduce(ops);

    let compaction = find(&rows, SpanType::Task, "compaction");
    assert!(compaction
        .tags
        .as_ref()
        .unwrap()
        .contains(&"compaction".to_string()));
    assert_eq!(
        compaction.metadata.as_ref().unwrap()["compaction"]["trigger"],
        json!("auto")
    );
    assert!(
        compaction.end_ms.is_some(),
        "compaction turn closed by PostCompact"
    );

    // The synthetic compaction llm span carries before/after context + metrics.
    let llm = find(&rows, SpanType::Llm, "gpt-5.5");
    assert_eq!(llm.parent_span_ids, vec![compaction.span_id.clone()]);
    assert!(llm.output.as_ref().unwrap()["kept_messages"].is_array());
    assert_eq!(
        llm.output.as_ref().unwrap()["summary"],
        json!("[summary unavailable — encrypted by Codex]")
    );
    assert_eq!(
        llm.output.as_ref().unwrap()["kept_messages"]
            .as_array()
            .unwrap()
            .len(),
        1,
        "encrypted compaction entry should not be exposed as a kept message"
    );
    assert_eq!(
        llm.metrics.as_ref().unwrap()["prompt_tokens"],
        json!(5000.0)
    );
    assert!(llm.end_ms.is_some());
}

#[test]
fn codex_compaction_replaces_history_for_following_llms() {
    let tmp = tempfile::tempdir().unwrap();
    let transcript = tmp.path().join("rollout.jsonl");
    for record in [
        json!({ "timestamp": "2026-01-01T00:00:01Z", "type": "session_meta", "payload": { "id": "s", "cwd": "/x/app" } }),
        json!({ "timestamp": "2026-01-01T00:00:02Z", "type": "turn_context", "payload": { "model": "gpt-5.5" } }),
        json!({ "timestamp": "2026-01-01T00:00:03Z", "type": "event_msg", "payload": { "type": "task_started", "turn_id": "t1" } }),
        json!({ "timestamp": "2026-01-01T00:00:04Z", "type": "response_item", "payload": { "type": "message", "role": "assistant", "content": [{ "type": "output_text", "text": "discard me" }] } }),
        json!({ "timestamp": "2026-01-01T00:00:05Z", "type": "event_msg", "payload": { "type": "token_count", "info": { "last_token_usage": { "input_tokens": 10, "output_tokens": 2 } } } }),
        json!({ "timestamp": "2026-01-01T00:00:06Z", "type": "compacted", "payload": { "replacement_history": [{ "role": "user", "content": "compacted context" }] } }),
        json!({ "timestamp": "2026-01-01T00:00:07Z", "type": "event_msg", "payload": { "type": "token_count", "info": { "last_token_usage": { "input_tokens": 5, "output_tokens": 1 } } } }),
        json!({ "timestamp": "2026-01-01T00:00:08Z", "type": "event_msg", "payload": { "type": "task_complete", "turn_id": "t1" } }),
        json!({ "timestamp": "2026-01-01T00:00:09Z", "type": "event_msg", "payload": { "type": "task_started", "turn_id": "t2" } }),
        json!({ "timestamp": "2026-01-01T00:00:10Z", "type": "response_item", "payload": { "type": "message", "role": "assistant", "content": [{ "type": "output_text", "text": "after" }] } }),
        json!({ "timestamp": "2026-01-01T00:00:11Z", "type": "event_msg", "payload": { "type": "token_count", "info": { "last_token_usage": { "input_tokens": 6, "output_tokens": 1 } } } }),
    ] {
        append(&transcript, record);
    }

    let registry = Registry::default_agents();
    let mut translator = registry.create("codex", "s");
    let ctx = SessionCtx {
        session_id: "s".into(),
        config: None,
    };
    let rows = reduce(
        translator
            .handle(
                &envelope("s", "SessionStart", transcript.to_str().unwrap(), json!({})),
                &ctx,
            )
            .unwrap(),
    );
    let following = rows
        .values()
        .find(|row| {
            row.span_type == SpanType::Llm
                && row
                    .metadata
                    .as_ref()
                    .is_some_and(|metadata| metadata["turn_id"] == json!("t2"))
        })
        .unwrap();
    let input = following.input.as_ref().unwrap().as_array().unwrap();
    assert_eq!(
        input,
        &[json!({ "role": "user", "content": "compacted context" })]
    );
}

#[test]
fn codex_subagent_nests_under_spawning_turn() {
    let tmp = tempfile::tempdir().unwrap();
    let main_t = tmp.path().join("main.jsonl");
    let sub_t = tmp.path().join("sub.jsonl");
    let main_p = main_t.to_str().unwrap();
    let sub_p = sub_t.to_str().unwrap();

    // Main session opens a turn and runs a spawn_agent tool.
    for v in [
        json!({ "timestamp": "2026-01-01T00:00:01Z", "type": "session_meta", "payload": { "id": "s", "cwd": "/x/app" } }),
        json!({ "timestamp": "2026-01-01T00:00:02Z", "type": "turn_context", "payload": { "model": "gpt-5.5" } }),
        json!({ "timestamp": "2026-01-01T00:00:03Z", "type": "event_msg", "payload": { "type": "task_started", "turn_id": "t1" } }),
        json!({ "timestamp": "2026-01-01T00:00:04Z", "type": "response_item", "payload": { "type": "function_call", "call_id": "c1", "name": "spawn_agent", "arguments": "{}" } }),
    ] {
        append(&main_t, v);
    }

    let reg = Registry::default_agents();
    let mut tr = reg.create("codex", "s");
    let ctx = SessionCtx {
        session_id: "s".into(),
        config: None,
    };

    let mut ops = Vec::new();
    // The spawn_agent transcript record is first observed on this same
    // PostToolUse. Catch-up must establish call -> turn before mapping agent_id.
    ops.extend(
        tr.handle(
            &envelope("s", "PostToolUse", main_p, json!({ "tool_name": "spawn_agent", "tool_use_id": "c1", "tool_response": { "agent_id": "a1" } })),
            &ctx,
        )
        .unwrap(),
    );
    // SubagentStart registers the subagent scope (its own transcript).
    ops.extend(
        tr.handle(
            &envelope(
                "s",
                "SubagentStart",
                main_p,
                json!({ "agent_id": "a1", "transcript_path": sub_p, "agent_type": "reviewer" }),
            ),
            &ctx,
        )
        .unwrap(),
    );

    // The subagent runs and writes its own transcript.
    for v in [
        json!({ "timestamp": "2026-01-01T00:00:05Z", "type": "session_meta", "payload": { "id": "a1", "cwd": "/x/app" } }),
        json!({ "timestamp": "2026-01-01T00:00:06Z", "type": "turn_context", "payload": { "model": "gpt-5.5-mini" } }),
        json!({ "timestamp": "2026-01-01T00:00:07Z", "type": "event_msg", "payload": { "type": "task_started", "turn_id": "st1" } }),
        json!({ "timestamp": "2026-01-01T00:00:08Z", "type": "response_item", "payload": { "type": "message", "role": "assistant", "content": [{ "type": "output_text", "text": "reviewed" }] } }),
        json!({ "timestamp": "2026-01-01T00:00:09Z", "type": "event_msg", "payload": { "type": "token_count", "info": { "last_token_usage": { "input_tokens": 10, "output_tokens": 3 } } } }),
        json!({ "timestamp": "2026-01-01T00:00:10Z", "type": "event_msg", "payload": { "type": "task_complete", "last_agent_message": "done" } }),
    ] {
        append(&sub_t, v);
    }

    // A subagent-scoped event (carries agent_id + its transcript_path) drives
    // the subagent catch-up, then SubagentStop closes it.
    ops.extend(
        tr.handle(
            &envelope(
                "s",
                "PostToolUse",
                main_p,
                json!({ "agent_id": "a1", "transcript_path": sub_p }),
            ),
            &ctx,
        )
        .unwrap(),
    );
    ops.extend(
        tr.handle(
            &envelope(
                "s",
                "SubagentStop",
                main_p,
                json!({ "agent_id": "a1", "agent_transcript_path": sub_p }),
            ),
            &ctx,
        )
        .unwrap(),
    );

    let rows = reduce(ops);

    let root = find(&rows, SpanType::Task, "codex: app");
    let main_turn = find(&rows, SpanType::Task, "turn: t1");
    let subagent = find(&rows, SpanType::Task, "subagent: a1");
    let sub_turn = find(&rows, SpanType::Task, "turn: st1");

    // Whole thing is one trace under the main root.
    for r in [main_turn, subagent, sub_turn] {
        assert_eq!(
            r.root_span_id, root.span_id,
            "span {:?} not in main trace",
            r.name
        );
    }
    // subagent root is a sibling of the spawn_agent tool, under the spawning turn.
    assert_eq!(subagent.parent_span_ids, vec![main_turn.span_id.clone()]);
    assert_eq!(
        subagent.metadata.as_ref().unwrap()["agent_type"],
        json!("reviewer")
    );
    assert!(
        subagent.end_ms.is_some(),
        "subagent root closed by SubagentStop"
    );
    // subagent turn hangs under the subagent root.
    assert_eq!(sub_turn.parent_span_ids, vec![subagent.span_id.clone()]);
    assert_eq!(sub_turn.output, Some(json!("done")));
    // subagent's llm is its own model, under its turn.
    let sub_llm = find(&rows, SpanType::Llm, "gpt-5.5-mini");
    assert_eq!(sub_llm.parent_span_ids, vec![sub_turn.span_id.clone()]);
}
