use bt_daemon::wire::Envelope;
use bt_daemon::{AgentTranslator, Registry, SessionCtx, SpanOp, SpanType};
use serde_json::json;
use std::path::{Path, PathBuf};

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/grok/transcript")
        .join(name)
}

fn envelope(updates_through: u64, events_through: u64) -> Envelope {
    Envelope {
        source: "grok".into(),
        source_version: Some("1.0.13".into()),
        plugin_version: Some("0.1.0".into()),
        session_id: "grok-session".into(),
        event: "Wake".into(),
        ts_ms: 1_600,
        managed_run_id: None,
        capture: None,
        payload: json!({
            "cwd": "/repo",
            "workspaceRoot": "/repo",
            "transcriptPath": "/native/session/chat_history.jsonl",
            "_bt_grok_transcript_mirrors": {
                "updates": {
                    "mirror": fixture("updates.jsonl"),
                    "through": updates_through
                },
                "events": {
                    "mirror": fixture("events.jsonl"),
                    "through": events_through
                },
                "system_prompt": {
                    "mirror": fixture("system_prompt.txt"),
                    "through": std::fs::metadata(fixture("system_prompt.txt")).unwrap().len()
                }
            }
        }),
        route: None,
        config: None,
    }
}

fn ctx() -> SessionCtx {
    SessionCtx {
        session_id: "grok-session".into(),
        config: None,
    }
}
fn expected_username() -> String {
    std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .unwrap_or_default()
}

fn point_at(event: &mut Envelope, updates: &Path, events: &Path) {
    event.payload["_bt_grok_transcript_mirrors"]["updates"]["mirror"] = json!(updates);
    event.payload["_bt_grok_transcript_mirrors"]["updates"]["through"] =
        json!(std::fs::metadata(updates).unwrap().len());
    event.payload["_bt_grok_transcript_mirrors"]["events"]["mirror"] = json!(events);
    event.payload["_bt_grok_transcript_mirrors"]["events"]["through"] =
        json!(std::fs::metadata(events).unwrap().len());
}

fn drain(translator: &mut dyn AgentTranslator) -> Vec<SpanOp> {
    let mut ops = Vec::new();
    while let Some(batch) = translator.drain_pending(&ctx()).unwrap() {
        ops.extend(batch);
    }
    ops
}

#[test]
fn grok_transcript_builds_turn_llm_and_tool_spans_with_aggregate_usage() {
    let updates = std::fs::metadata(fixture("updates.jsonl")).unwrap().len();
    let events = std::fs::metadata(fixture("events.jsonl")).unwrap().len();
    let registry = Registry::default_agents();
    assert!(registry.sources().contains(&"grok".to_string()));
    let mut translator = registry.create("grok", "grok-session");
    let ops = translator
        .handle(&envelope(updates, events), &ctx())
        .unwrap();

    let inserts: Vec<_> = ops
        .iter()
        .filter_map(|op| match op {
            SpanOp::Insert(row) => Some(row),
            _ => None,
        })
        .collect();
    assert_eq!(inserts.len(), 5);
    let root = inserts.iter().find(|row| row.name == "Grok").unwrap();
    assert_eq!(root.span_type, SpanType::Task);
    assert_eq!(
        root.metadata.as_ref().unwrap()["trace_source"],
        "session_transcript"
    );

    let turn = inserts.iter().find(|row| row.name == "Turn 1").unwrap();
    assert_eq!(turn.input, Some(json!("inspect the fixture")));
    assert_eq!(turn.parent_span_ids, vec![root.span_id.clone()]);

    let llms: Vec<_> = inserts
        .iter()
        .filter(|row| row.span_type == SpanType::Llm)
        .collect();
    assert_eq!(llms.len(), 2);
    assert_eq!(llms[0].name, "grok-4.6-build call 1");
    assert_eq!(llms[1].name, "grok-4.6-build call 2");
    assert!(llms
        .iter()
        .all(|row| row.parent_span_ids == vec![turn.span_id.clone()]));
    let system_prompt = std::fs::read_to_string(fixture("system_prompt.txt")).unwrap();
    assert_eq!(
        llms[0].input,
        Some(json!([
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": "inspect the fixture"}
        ]))
    );
    assert!(llms[1].input.is_none());
    assert!(llms.iter().all(|row| {
        row.metadata.as_ref().is_some_and(|metadata| {
            metadata["trace_source"] == "session_transcript"
                && metadata["input_unavailable"] == true
                && metadata["boundary_source"] == "streamStartMs"
        })
    }));
    assert_eq!(
        llms[0].metadata.as_ref().unwrap()["system_prompt_included"],
        true
    );
    assert_eq!(
        llms[0].metadata.as_ref().unwrap()["user_message_included"],
        true
    );
    assert_eq!(
        llms[0].metadata.as_ref().unwrap()["input_scope"],
        "system_and_user"
    );
    assert!(llms[1]
        .metadata
        .as_ref()
        .unwrap()
        .get("system_prompt_included")
        .is_none());
    let first_llm_close = ops
        .iter()
        .find_map(|op| match op {
            SpanOp::Merge(row) if row.span_id == llms[0].span_id && row.output.is_some() => {
                Some(row)
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(
        first_llm_close.output,
        Some(json!([{
            "role": "assistant",
            "content": "Reading the fixture.",
            "reasoning": [{
                "id": "reasoning",
                "content": "I will read it."
            }]
        }]))
    );

    let tool = inserts
        .iter()
        .find(|row| row.span_type == SpanType::Tool)
        .unwrap();
    assert_eq!(tool.name, "read_file");
    assert_eq!(tool.input, Some(json!({"path": "fixture.txt"})));
    assert_eq!(tool.parent_span_ids, vec![turn.span_id.clone()]);

    let tool_merges: Vec<_> = ops
        .iter()
        .filter_map(|op| match op {
            SpanOp::Merge(row) if row.span_id == tool.span_id => Some(row),
            _ => None,
        })
        .collect();
    assert_eq!(tool_merges.len(), 2);
    assert_eq!(tool_merges[0].output, Some(json!("fixture contents")));
    assert_eq!(
        tool_merges[1].metadata.as_ref().unwrap()["duration_ms"],
        100
    );

    let turn_merge = ops
        .iter()
        .find_map(|op| match op {
            SpanOp::Merge(row) if row.span_id == turn.span_id => Some(row),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        turn_merge.output,
        Some(json!([
            {"type": "text", "text": "Reading the fixture."},
            {"type": "text", "text": "Done."}
        ]))
    );
    let metrics = turn_merge.metrics.as_ref().unwrap();
    assert_eq!(metrics["prompt_tokens"], 100);
    assert_eq!(metrics["completion_tokens"], 20);
    assert_eq!(metrics["prompt_cached_tokens"], 40);
    assert_eq!(metrics["model_calls"], 2);
    assert_eq!(metrics["cost_usd_ticks"], 12);
    assert!(metrics.get("estimated_cost").is_none());
    assert!(!ops.iter().any(
        |op| matches!(op, SpanOp::Merge(row) if row.span_id == llms[0].span_id && row.metrics.is_some()),
    ));
    let last_llm_usage = ops
        .iter()
        .find_map(|op| match op {
            SpanOp::Merge(row) if row.span_id == llms[1].span_id && row.metrics.is_some() => {
                Some(row)
            }
            _ => None,
        })
        .expect("turn aggregate usage must be attributed to the final LLM");
    assert_eq!(last_llm_usage.metrics.as_ref(), Some(metrics));
    assert_eq!(
        last_llm_usage.metadata.as_ref().unwrap(),
        &json!({
            "usage_scope": "turn",
            "usage_attribution": "last_llm"
        })
    );
}
#[test]
fn grok_enriches_emitted_spans_from_the_captured_working_directory() {
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
        "https://secret@example.com/acme/app.git",
    ]);
    let commit = std::process::Command::new("git")
        .arg("-C")
        .arg(&repo)
        .args(["rev-parse", "HEAD"])
        .output()
        .unwrap();
    let commit = String::from_utf8(commit.stdout).unwrap().trim().to_string();

    let updates = std::fs::metadata(fixture("updates.jsonl")).unwrap().len();
    let events = std::fs::metadata(fixture("events.jsonl")).unwrap().len();
    let mut event = envelope(updates, events);
    event.payload["cwd"] = json!(repo);
    let registry = Registry::default_agents();
    let mut translator = registry.create("grok", "grok-session");
    let ops = translator.handle(&event, &ctx()).unwrap();

    let inserts: Vec<_> = ops
        .iter()
        .filter_map(|op| match op {
            SpanOp::Insert(row) => Some(row),
            SpanOp::Merge(_) => None,
        })
        .collect();
    let root = inserts.iter().find(|row| row.name == "Grok").unwrap();
    assert_eq!(
        root.metadata.as_ref().unwrap()["username"],
        json!(expected_username())
    );
    assert!(inserts.iter().all(|row| {
        row.metadata.as_ref().is_some_and(|metadata| {
            metadata["git_origin_url"] == "https://example.com/acme/app.git"
                && metadata["git_branch"] == "main"
                && metadata["git_commit_sha"] == commit
        })
    }));
}

#[test]
fn grok_transcript_reads_incrementally_and_replay_is_deterministic() {
    let updates_path = fixture("updates.jsonl");
    let bytes = std::fs::read(&updates_path).unwrap();
    let first_through = bytes
        .iter()
        .enumerate()
        .filter(|(_, byte)| **byte == b'\n')
        .nth(4)
        .map(|(index, _)| index as u64 + 1)
        .unwrap();
    let full_through = bytes.len() as u64;
    let events = std::fs::metadata(fixture("events.jsonl")).unwrap().len();

    let registry = Registry::default_agents();
    let mut incremental = registry.create("grok", "grok-session");
    let first = incremental
        .handle(&envelope(first_through, 0), &ctx())
        .unwrap();
    let second = incremental
        .handle(&envelope(full_through, events), &ctx())
        .unwrap();
    assert!(incremental
        .handle(&envelope(full_through, events), &ctx())
        .unwrap()
        .is_empty());

    let mut replay = registry.create("grok", "grok-session");
    let all = replay
        .handle(&envelope(full_through, events), &ctx())
        .unwrap();
    let incremental_json =
        serde_json::to_value(first.into_iter().chain(second).collect::<Vec<_>>()).unwrap();
    assert_eq!(incremental_json, serde_json::to_value(all).unwrap());
}

#[test]
fn grok_single_call_usage_and_missing_tool_start_are_recovered() {
    let temp = tempfile::tempdir().unwrap();
    let updates_path = temp.path().join("updates.jsonl");
    let events_path = temp.path().join("events.jsonl");
    std::fs::write(
        &updates_path,
        concat!(
            "{\"params\":{\"update\":{\"sessionUpdate\":\"user_message_chunk\",\"content\":{\"text\":\"hello\"},\"_meta\":{\"modelId\":\"grok-4.6\",\"promptIndex\":0}},\"_meta\":{\"agentTimestampMs\":1000}}}\n",
            "{\"params\":{\"update\":{\"sessionUpdate\":\"agent_message_chunk\",\"content\":{\"text\":\"working\"}},\"_meta\":{\"promptId\":\"p1\",\"streamStartMs\":1100,\"agentTimestampMs\":1110}}}\n",
            "{\"params\":{\"update\":{\"sessionUpdate\":\"tool_call_update\",\"toolCallId\":\"missing\",\"title\":\"read_file\",\"rawInput\":{\"path\":\"x\"},\"rawOutput\":\"y\",\"status\":\"completed\"},\"_meta\":{\"agentTimestampMs\":1200}}}\n",
            "{\"params\":{\"update\":{\"sessionUpdate\":\"turn_completed\",\"prompt_id\":\"p1\",\"stop_reason\":\"end_turn\",\"usage\":{\"inputTokens\":10,\"outputTokens\":2,\"totalTokens\":12,\"modelCalls\":1,\"costUsdTicks\":25000000,\"modelUsage\":{\"grok-4.6-build\":{\"inputTokens\":10,\"outputTokens\":2,\"totalTokens\":12,\"modelCalls\":1,\"costUsdTicks\":25000000}}}},\"_meta\":{\"agentTimestampMs\":1300}}}\n"
        ),
    )
    .unwrap();
    std::fs::write(&events_path, "").unwrap();
    let mut event = envelope(0, 0);
    point_at(&mut event, &updates_path, &events_path);

    let registry = Registry::default_agents();
    let mut translator = registry.create("grok", "grok-session");
    let ops = translator.handle(&event, &ctx()).unwrap();
    let llm = ops
        .iter()
        .find_map(|op| match op {
            SpanOp::Insert(row) if row.span_type == SpanType::Llm => Some(row),
            _ => None,
        })
        .unwrap();
    assert_eq!(llm.name, "grok-4.6 call 1");
    assert!(ops.iter().any(|op| {
        matches!(op, SpanOp::Merge(row) if row.span_id == llm.span_id
            && row.metrics.as_ref().is_some_and(|metrics| metrics["tokens"] == 12
                && metrics["cost_usd_ticks"] == 25000000
                && metrics.get("estimated_cost").is_none()))
    }));
    assert!(ops.iter().any(|op| {
        matches!(op, SpanOp::Insert(row) if row.span_type == SpanType::Tool && row.metadata.as_ref().is_some_and(|metadata| metadata["missing_start"] == true))
    }));
}

#[test]
fn grok_edge_fixture_preserves_turns_skips_malformed_records_and_marks_mismatches() {
    let updates = fixture("edge-updates.jsonl");
    let events = fixture("edge-events.jsonl");
    let mut event = envelope(0, 0);
    point_at(&mut event, &updates, &events);
    let registry = Registry::default_agents();
    let mut translator = registry.create("grok", "grok-session");
    let ops = translator.handle(&event, &ctx()).unwrap();

    let turns: Vec<_> = ops
        .iter()
        .filter_map(|op| match op {
            SpanOp::Insert(row) if row.name.starts_with("Turn ") => Some(row),
            _ => None,
        })
        .collect();
    assert_eq!(
        turns.len(),
        2,
        "duplicate user chunk must not mutate turn state"
    );
    assert!(!ops.iter().any(|op| {
        matches!(op, SpanOp::Merge(row)
            if row.span_id == turns[0].span_id && row.input == Some(json!("firstfirst")))
    }));
    let llms: Vec<_> = ops
        .iter()
        .filter_map(|op| match op {
            SpanOp::Insert(row) if row.span_type == SpanType::Llm => Some(row),
            _ => None,
        })
        .collect();
    assert_eq!(llms.len(), 3);
    assert!(llms[0]
        .metadata
        .as_ref()
        .is_some_and(|metadata| metadata.get("boundary_source").is_none()));

    let first_turn_merge = ops
        .iter()
        .find_map(|op| match op {
            SpanOp::Merge(row) if row.span_id == turns[0].span_id && row.end_ms.is_some() => {
                Some(row)
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(
        first_turn_merge.metadata.as_ref().unwrap()["model_call_count_mismatch"],
        json!({"native": 3, "reconstructed": 2})
    );
    let first_turn_llms: Vec<_> = llms
        .iter()
        .filter(|llm| llm.parent_span_ids == vec![turns[0].span_id.clone()])
        .collect();
    assert_eq!(first_turn_llms.len(), 2);
    assert!(!ops.iter().any(
        |op| matches!(op, SpanOp::Merge(row) if row.span_id == first_turn_llms[0].span_id && row.metrics.is_some()),
    ));
    assert!(ops.iter().any(|op| {
        matches!(op, SpanOp::Merge(row) if row.span_id == first_turn_llms[1].span_id
        && row.metrics.as_ref().is_some_and(|metrics| metrics["model_calls"] == 3)
        && row.metadata.as_ref().is_some_and(|metadata| {
            metadata["usage_scope"] == "turn"
                && metadata["usage_attribution"] == "last_llm"
        }))
    }));
    let second_llm = llms
        .iter()
        .find(|llm| llm.parent_span_ids == vec![turns[1].span_id.clone()])
        .unwrap();
    assert!(ops.iter().any(|op| {
        matches!(op, SpanOp::Merge(row) if row.span_id == second_llm.span_id
            && row.metrics.as_ref().is_some_and(|metrics| metrics["model_calls"] == 1))
    }));
    let cancelled_merges: Vec<_> = ops
        .iter()
        .filter_map(|op| match op {
            SpanOp::Merge(row)
                if row
                    .metadata
                    .as_ref()
                    .is_some_and(|metadata| metadata["cancelled"] == true) =>
            {
                Some(row)
            }
            _ => None,
        })
        .collect();
    assert!(!cancelled_merges.is_empty());
    assert!(cancelled_merges.iter().all(|row| row.error.is_none()));
}

#[test]
fn grok_partial_records_are_retried_and_complete_malformed_lines_do_not_stall() {
    let temp = tempfile::tempdir().unwrap();
    let updates_path = temp.path().join("updates.jsonl");
    let events_path = temp.path().join("events.jsonl");
    let initial = std::fs::read(fixture("partial-updates.jsonl")).unwrap();
    std::fs::write(&updates_path, &initial).unwrap();
    std::fs::write(&events_path, "").unwrap();
    let mut first_event = envelope(0, 0);
    point_at(&mut first_event, &updates_path, &events_path);

    let registry = Registry::default_agents();
    let mut translator = registry.create("grok", "grok-session");
    let first = translator.handle(&first_event, &ctx()).unwrap();
    assert!(!first
        .iter()
        .any(|op| matches!(op, SpanOp::Insert(row) if row.span_type == SpanType::Llm)));

    let mut completed = initial;
    completed.extend_from_slice(
        b",\"streamStartMs\":1100,\"agentTimestampMs\":1110,\"chunkId\":1}}}\nnot-json\n{\"params\":{\"update\":{\"sessionUpdate\":\"turn_completed\",\"prompt_id\":\"partial-prompt\",\"usage\":{\"modelCalls\":1}},\"_meta\":{\"agentTimestampMs\":1200}}}\n",
    );
    std::fs::write(&updates_path, completed).unwrap();
    let mut second_event = envelope(0, 0);
    point_at(&mut second_event, &updates_path, &events_path);
    let second = translator.handle(&second_event, &ctx()).unwrap();
    assert!(second
        .iter()
        .any(|op| matches!(op, SpanOp::Insert(row) if row.span_type == SpanType::Llm)));
    assert!(second.iter().any(|op| matches!(op, SpanOp::Merge(row) if row.metrics.as_ref().is_some_and(|metrics| metrics["model_calls"] == 1))));
}

#[test]
fn grok_processes_a_complete_terminal_record_without_a_trailing_newline() {
    let temp = tempfile::tempdir().unwrap();
    let updates_path = temp.path().join("updates.jsonl");
    let events_path = temp.path().join("events.jsonl");
    let updates = concat!(
        "{\"params\":{\"update\":{\"sessionUpdate\":\"user_message_chunk\",\"content\":{\"text\":\"finish\"}},\"_meta\":{\"promptIndex\":0,\"agentTimestampMs\":1000}}}\n",
        "{\"params\":{\"update\":{\"sessionUpdate\":\"agent_message_chunk\",\"content\":{\"text\":\"done\"}},\"_meta\":{\"promptId\":\"p1\",\"streamStartMs\":1100,\"agentTimestampMs\":1110,\"chunkId\":1}}}\n",
        "{\"params\":{\"update\":{\"sessionUpdate\":\"turn_completed\",\"prompt_id\":\"p1\",\"usage\":{\"modelCalls\":1,\"totalTokens\":7}},\"_meta\":{\"agentTimestampMs\":1200}}}"
    );
    std::fs::write(&updates_path, updates).unwrap();
    std::fs::write(&events_path, "").unwrap();
    let mut event = envelope(0, 0);
    event.event = "SessionEnd".into();
    point_at(&mut event, &updates_path, &events_path);

    let registry = Registry::default_agents();
    let mut translator = registry.create("grok", "grok-session");
    let ops = translator.handle(&event, &ctx()).unwrap();

    assert!(ops.iter().any(|op| {
        matches!(op, SpanOp::Merge(row)
            if row.metrics.as_ref().is_some_and(|metrics| metrics["tokens"] == 7))
    }));
    assert!(ops.iter().any(|op| {
        matches!(op, SpanOp::Merge(row)
            if row.metadata.as_ref().is_some_and(|metadata| metadata["close_reason"] == "turn_completed"))
    }));
}

#[test]
fn grok_events_failure_does_not_block_updates_and_recovers_enrichment() {
    let temp = tempfile::tempdir().unwrap();
    let updates_path = temp.path().join("updates.jsonl");
    let events_path = temp.path().join("events.jsonl");
    let update_body = concat!(
        "{\"params\":{\"update\":{\"sessionUpdate\":\"user_message_chunk\",\"content\":{\"text\":\"retry\"}},\"_meta\":{\"promptIndex\":0,\"agentTimestampMs\":1000}}}\n",
        "{\"params\":{\"update\":{\"sessionUpdate\":\"tool_call\",\"toolCallId\":\"retry-tool\",\"title\":\"read\"},\"_meta\":{\"agentTimestampMs\":1100}}}\n",
        "{\"params\":{\"update\":{\"sessionUpdate\":\"tool_call_update\",\"toolCallId\":\"retry-tool\",\"status\":\"completed\"},\"_meta\":{\"agentTimestampMs\":1200}}}\n"
    );
    let event_body = "{\"ts\":\"1970-01-01T00:00:01.200Z\",\"type\":\"tool_completed\",\"tool_call_id\":\"retry-tool\",\"duration_ms\":100,\"outcome\":\"success\"}\n";
    std::fs::write(&updates_path, update_body).unwrap();

    let mut event = envelope(update_body.len() as u64, event_body.len() as u64);
    event.payload["_bt_grok_transcript_mirrors"]["updates"]["mirror"] = json!(updates_path);
    event.payload["_bt_grok_transcript_mirrors"]["events"]["mirror"] = json!(events_path);
    let registry = Registry::default_agents();
    let mut translator = registry.create("grok", "grok-session");

    let primary = translator.handle(&event, &ctx()).unwrap();
    assert_eq!(
        primary
            .iter()
            .filter(|op| matches!(op, SpanOp::Insert(row) if row.name == "Turn 1"))
            .count(),
        1
    );
    assert!(primary
        .iter()
        .any(|op| matches!(op, SpanOp::Insert(row) if row.span_type == SpanType::Tool)));
    assert!(!primary.iter().any(|op| {
        matches!(op, SpanOp::Merge(row) if row.metadata.as_ref().is_some_and(|metadata| metadata.get("outcome").is_some()))
    }));

    std::fs::write(&events_path, event_body).unwrap();
    let enrichment = translator.handle(&event, &ctx()).unwrap();
    assert!(!enrichment.iter().any(|op| matches!(op, SpanOp::Insert(_))));
    let enriched_tools: Vec<_> = enrichment
        .iter()
        .filter_map(|op| match op {
            SpanOp::Merge(row)
                if row.metadata.as_ref().is_some_and(|metadata| {
                    metadata["duration_ms"] == 100 && metadata["outcome"] == "success"
                }) =>
            {
                Some(row)
            }
            _ => None,
        })
        .collect();
    assert_eq!(enriched_tools.len(), 1);
    assert!(translator.handle(&event, &ctx()).unwrap().is_empty());
}

#[test]
fn grok_buffers_tool_events_until_the_matching_update_arrives() {
    let temp = tempfile::tempdir().unwrap();
    let updates_path = temp.path().join("updates.jsonl");
    let events_path = temp.path().join("events.jsonl");
    let initial_updates = concat!(
        "{\"params\":{\"update\":{\"sessionUpdate\":\"user_message_chunk\",\"content\":{\"text\":\"retry\"}},\"_meta\":{\"promptIndex\":0,\"agentTimestampMs\":1000}}}\n",
        "{\"params\":{\"update\":{\"sessionUpdate\":\"tool_call\",\"toolCallId\":\"early-tool\",\"title\":\"read\"},\"_meta\":{\"agentTimestampMs\":1100}}}\n"
    );
    let completed_update = "{\"params\":{\"update\":{\"sessionUpdate\":\"tool_call_update\",\"toolCallId\":\"early-tool\",\"status\":\"completed\",\"rawOutput\":\"done\"},\"_meta\":{\"agentTimestampMs\":1200}}}\n";
    let event_body = "{\"ts\":\"1970-01-01T00:00:01.200Z\",\"type\":\"tool_completed\",\"tool_call_id\":\"early-tool\",\"duration_ms\":100,\"outcome\":\"success\"}\n";
    std::fs::write(&updates_path, initial_updates).unwrap();
    std::fs::write(&events_path, event_body).unwrap();
    let mut first_event = envelope(0, 0);
    point_at(&mut first_event, &updates_path, &events_path);
    let registry = Registry::default_agents();
    let mut translator = registry.create("grok", "grok-session");

    let first = translator.handle(&first_event, &ctx()).unwrap();
    assert!(!first.iter().any(|op| {
        matches!(op, SpanOp::Merge(row)
            if row.metadata.as_ref().is_some_and(|metadata| metadata.get("outcome").is_some()))
    }));

    std::fs::write(
        &updates_path,
        format!("{initial_updates}{completed_update}"),
    )
    .unwrap();
    let mut second_event = envelope(0, 0);
    point_at(&mut second_event, &updates_path, &events_path);
    let second = translator.handle(&second_event, &ctx()).unwrap();
    assert!(second.iter().any(|op| {
        matches!(op, SpanOp::Merge(row)
            if row.output == Some(json!("done"))
                && row.metadata.as_ref().is_some_and(|metadata| metadata["status"] == "completed"))
    }));
    assert!(second.iter().any(|op| {
        matches!(op, SpanOp::Merge(row)
        if row.metadata.as_ref().is_some_and(|metadata| {
            metadata["duration_ms"] == 100 && metadata["outcome"] == "success"
        }))
    }));
}

#[test]
fn grok_drains_every_captured_event_batch_before_finishing_the_hook() {
    let temp = tempfile::tempdir().unwrap();
    let updates_path = temp.path().join("updates.jsonl");
    let events_path = temp.path().join("events.jsonl");
    let updates = concat!(
        "{\"params\":{\"update\":{\"sessionUpdate\":\"user_message_chunk\",\"content\":{\"text\":\"many events\"}},\"_meta\":{\"promptIndex\":0,\"agentTimestampMs\":1000}}}\n",
        "{\"params\":{\"update\":{\"sessionUpdate\":\"tool_call_update\",\"toolCallId\":\"repeated-tool\",\"status\":\"completed\"},\"_meta\":{\"agentTimestampMs\":1100}}}\n"
    );
    let event =
        "{\"ts\":\"1970-01-01T00:00:01.200Z\",\"type\":\"tool_completed\",\"tool_call_id\":\"repeated-tool\",\"duration_ms\":100,\"outcome\":\"success\"}\n";
    std::fs::write(&updates_path, updates).unwrap();
    std::fs::write(&events_path, event.repeat(257)).unwrap();
    let mut envelope = envelope(0, 0);
    point_at(&mut envelope, &updates_path, &events_path);
    let registry = Registry::default_agents();
    let mut translator = registry.create("grok", "grok-session");

    let mut ops = translator.handle(&envelope, &ctx()).unwrap();
    ops.extend(drain(translator.as_mut()));

    assert_eq!(
        ops.iter()
            .filter(|op| {
                matches!(op, SpanOp::Merge(row)
                if row.metadata.as_ref().is_some_and(|metadata| {
                    metadata["duration_ms"] == 100 && metadata["outcome"] == "success"
                }))
            })
            .count(),
        257
    );
}

#[test]
fn grok_updates_failure_keeps_primary_state_retryable() {
    let temp = tempfile::tempdir().unwrap();
    let updates_path = temp.path().join("updates.jsonl");
    let events_path = temp.path().join("events.jsonl");
    let update_body = concat!(
        "{\"params\":{\"update\":{\"sessionUpdate\":\"user_message_chunk\",\"content\":{\"text\":\"retry\"}},\"_meta\":{\"promptIndex\":0,\"agentTimestampMs\":1000}}}\n",
        "{\"params\":{\"update\":{\"sessionUpdate\":\"tool_call\",\"toolCallId\":\"retry-tool\",\"title\":\"read\"},\"_meta\":{\"agentTimestampMs\":1100}}}\n",
        "{\"params\":{\"update\":{\"sessionUpdate\":\"tool_call_update\",\"toolCallId\":\"retry-tool\",\"status\":\"completed\"},\"_meta\":{\"agentTimestampMs\":1200}}}\n"
    );
    let event_body = "{\"ts\":\"1970-01-01T00:00:01.200Z\",\"type\":\"tool_completed\",\"tool_call_id\":\"retry-tool\",\"duration_ms\":100,\"outcome\":\"success\"}\n";
    std::fs::write(&events_path, event_body).unwrap();

    let mut event = envelope(update_body.len() as u64, event_body.len() as u64);
    event.payload["_bt_grok_transcript_mirrors"]["updates"]["mirror"] = json!(updates_path);
    event.payload["_bt_grok_transcript_mirrors"]["events"]["mirror"] = json!(events_path);
    let registry = Registry::default_agents();
    let mut translator = registry.create("grok", "grok-session");
    assert!(translator.handle(&event, &ctx()).is_err());

    std::fs::write(&updates_path, update_body).unwrap();
    let ops = translator.handle(&event, &ctx()).unwrap();
    assert_eq!(
        ops.iter()
            .filter(|op| matches!(op, SpanOp::Insert(row) if row.name == "Turn 1"))
            .count(),
        1
    );
    assert!(ops.iter().any(|op| {
        matches!(op, SpanOp::Merge(row) if row.metadata.as_ref().is_some_and(|metadata| {
            metadata["duration_ms"] == 100 && metadata["outcome"] == "success"
        }))
    }));
}

#[test]
fn grok_resets_semantic_state_when_the_transcript_is_replaced() {
    let temp = tempfile::tempdir().unwrap();
    let updates_path = temp.path().join("updates.jsonl");
    let events_path = temp.path().join("events.jsonl");
    std::fs::write(&events_path, "").unwrap();
    let first = concat!(
        "{\"params\":{\"update\":{\"sessionUpdate\":\"user_message_chunk\",\"content\":{\"text\":\"old\"}},\"_meta\":{\"promptIndex\":0,\"agentTimestampMs\":1000}}}\n",
        "{\"params\":{\"update\":{\"sessionUpdate\":\"agent_message_chunk\",\"content\":{\"text\":\"old answer\"}},\"_meta\":{\"promptId\":\"p1\",\"streamStartMs\":1100,\"agentTimestampMs\":1110}}}\n",
        "{\"params\":{\"update\":{\"sessionUpdate\":\"tool_call\",\"toolCallId\":\"old-tool\",\"title\":\"read\"},\"_meta\":{\"agentTimestampMs\":1120}}}\n"
    );
    std::fs::write(&updates_path, first).unwrap();
    let mut first_event = envelope(0, 0);
    point_at(&mut first_event, &updates_path, &events_path);
    let registry = Registry::default_agents();
    let mut translator = registry.create("grok", "grok-session");
    translator.handle(&first_event, &ctx()).unwrap();

    let replacement = concat!(
        "{\"params\":{\"update\":{\"sessionUpdate\":\"user_message_chunk\",\"content\":{\"text\":\"new\"}},\"_meta\":{\"promptIndex\":0,\"agentTimestampMs\":2000}}}\n",
        "{\"params\":{\"update\":{\"sessionUpdate\":\"agent_message_chunk\",\"content\":{\"text\":\"new answer\"}},\"_meta\":{\"promptId\":\"p1\",\"streamStartMs\":1100,\"agentTimestampMs\":2010}}}\n"
    );
    std::fs::write(&updates_path, replacement).unwrap();
    let mut replacement_event = envelope(0, 0);
    point_at(&mut replacement_event, &updates_path, &events_path);
    let ops = translator.handle(&replacement_event, &ctx()).unwrap();

    assert!(ops.iter().any(|op| {
        matches!(op, SpanOp::Insert(row)
            if row.name == "Turn 1" && row.input == Some(json!("new")))
    }));
    assert_eq!(
        ops.iter()
            .filter(|op| matches!(op, SpanOp::Insert(row) if row.span_type == SpanType::Llm && row.name == "Grok call 1"))
            .count(),
        1
    );
    assert!(!ops
        .iter()
        .any(|op| matches!(op, SpanOp::Insert(row) if row.name == "Grok call 2")));
}

#[test]
fn grok_supports_native_and_documented_terminal_events() {
    let temp = tempfile::tempdir().unwrap();
    let updates_path = temp.path().join("updates.jsonl");
    let events_path = temp.path().join("events.jsonl");
    std::fs::write(&events_path, "").unwrap();
    std::fs::write(
        &updates_path,
        "{\"params\":{\"update\":{\"sessionUpdate\":\"user_message_chunk\",\"content\":{\"text\":\"terminal\"}},\"_meta\":{\"promptIndex\":0,\"agentTimestampMs\":2000}}}\n",
    )
    .unwrap();
    let mut base = envelope(0, 0);
    point_at(&mut base, &updates_path, &events_path);
    base.ts_ms = 1_500;
    let registry = Registry::default_agents();

    for event_name in ["session_end", "SessionEnd"] {
        let mut translator = registry.create("grok", "grok-session");
        let mut event = base.clone();
        event.event = event_name.into();
        let ops = translator.handle(&event, &ctx()).unwrap();
        let root_id = ops
            .iter()
            .find_map(|op| match op {
                SpanOp::Insert(row) if row.name == "Grok" => Some(row.span_id.clone()),
                _ => None,
            })
            .unwrap();
        assert!(ops.iter().any(
            |op| matches!(op, SpanOp::Merge(row) if row.span_id == root_id && row.end_ms == Some(2000))
        ));
    }

    for (event_name, should_error, cancelled) in [
        ("stop_failure", true, false),
        ("StopFailure", true, false),
        ("stop_cancelled", false, true),
        ("StopCancelled", false, true),
    ] {
        let mut translator = registry.create("grok", "grok-session");
        let mut event = base.clone();
        event.event = event_name.into();
        let ops = translator.handle(&event, &ctx()).unwrap();
        let turn_id = ops
            .iter()
            .find_map(|op| match op {
                SpanOp::Insert(row) if row.name == "Turn 1" => Some(row.span_id.clone()),
                _ => None,
            })
            .unwrap();
        let close = ops
            .iter()
            .find_map(|op| match op {
                SpanOp::Merge(row) if row.span_id == turn_id => Some(row),
                _ => None,
            })
            .unwrap();
        assert_eq!(close.error.is_some(), should_error);
        assert_eq!(
            close
                .metadata
                .as_ref()
                .is_some_and(|metadata| metadata["cancelled"] == true),
            cancelled
        );
    }
}

#[test]
fn grok_resume_extends_and_recloses_the_existing_session_root() {
    let temp = tempfile::tempdir().unwrap();
    let updates_path = temp.path().join("updates.jsonl");
    let events_path = temp.path().join("events.jsonl");
    std::fs::write(&events_path, "").unwrap();
    let first_turn = concat!(
        "{\"params\":{\"update\":{\"sessionUpdate\":\"user_message_chunk\",\"content\":{\"text\":\"first\"}},\"_meta\":{\"promptIndex\":0,\"agentTimestampMs\":1000}}}\n",
        "{\"params\":{\"update\":{\"sessionUpdate\":\"turn_completed\",\"usage\":{\"modelCalls\":0}},\"_meta\":{\"agentTimestampMs\":1100}}}\n"
    );
    std::fs::write(&updates_path, first_turn).unwrap();
    let mut first_end = envelope(0, 0);
    first_end.event = "SessionEnd".into();
    first_end.ts_ms = 1_200;
    point_at(&mut first_end, &updates_path, &events_path);
    let registry = Registry::default_agents();
    let mut translator = registry.create("grok", "grok-session");

    let first = translator.handle(&first_end, &ctx()).unwrap();
    let root_id = first
        .iter()
        .find_map(|op| match op {
            SpanOp::Insert(row) if row.name == "Grok" => Some(row.span_id.clone()),
            _ => None,
        })
        .unwrap();
    assert!(first.iter().any(|op| {
        matches!(op, SpanOp::Merge(row)
            if row.span_id == root_id
                && row.late_merge_key.as_deref() == Some("session:terminal:0"))
    }));

    let resumed_turn = concat!(
        "{\"params\":{\"update\":{\"sessionUpdate\":\"user_message_chunk\",\"content\":{\"text\":\"second\"}},\"_meta\":{\"promptIndex\":1,\"agentTimestampMs\":3000}}}\n",
        "{\"params\":{\"update\":{\"sessionUpdate\":\"turn_completed\",\"usage\":{\"modelCalls\":0}},\"_meta\":{\"agentTimestampMs\":3100}}}\n"
    );
    std::fs::write(&updates_path, format!("{first_turn}{resumed_turn}")).unwrap();
    let mut resumed = envelope(0, 0);
    resumed.ts_ms = 3_100;
    point_at(&mut resumed, &updates_path, &events_path);
    let second = translator.handle(&resumed, &ctx()).unwrap();
    assert!(second.iter().any(|op| {
        matches!(op, SpanOp::Merge(row)
            if row.span_id == root_id
                && row.end_ms == Some(3000)
                && row.late_merge_key.as_deref() == Some("session:resume:1"))
    }));

    let mut second_end = resumed;
    second_end.event = "session_end".into();
    second_end.ts_ms = 3_200;
    let third = translator.handle(&second_end, &ctx()).unwrap();
    assert!(third.iter().any(|op| {
        matches!(op, SpanOp::Merge(row)
            if row.span_id == root_id
                && row.end_ms == Some(3200)
                && row.late_merge_key.as_deref() == Some("session:terminal:1"))
    }));
}

#[test]
fn grok_catch_up_and_open_state_bounds_drain_without_losing_later_records() {
    let temp = tempfile::tempdir().unwrap();
    let updates_path = temp.path().join("updates.jsonl");
    let events_path = temp.path().join("events.jsonl");
    let mut body = String::from("{\"params\":{\"update\":{\"sessionUpdate\":\"user_message_chunk\",\"content\":{\"text\":\"bounded\"}},\"_meta\":{\"promptIndex\":0,\"agentTimestampMs\":1000}}}\n");
    for index in 0..270 {
        body.push_str(&format!("{{\"params\":{{\"update\":{{\"sessionUpdate\":\"tool_call\",\"toolCallId\":\"tool-{index}\",\"title\":\"tool\"}},\"_meta\":{{\"agentTimestampMs\":{}}}}}}}\n", 1100 + index));
    }
    body.push_str("{\"params\":{\"update\":{\"sessionUpdate\":\"turn_completed\",\"usage\":{\"modelCalls\":0}},\"_meta\":{\"agentTimestampMs\":2000}}}\n");
    std::fs::write(&updates_path, body).unwrap();
    std::fs::write(&events_path, "").unwrap();
    let mut event = envelope(0, 0);
    point_at(&mut event, &updates_path, &events_path);
    let registry = Registry::default_agents();
    let mut translator = registry.create("grok", "grok-session");
    let mut ops = translator.handle(&event, &ctx()).unwrap();
    ops.extend(drain(translator.as_mut()));
    assert_eq!(
        ops.iter()
            .filter(|op| matches!(op, SpanOp::Insert(row) if row.span_type == SpanType::Tool))
            .count(),
        270
    );
    assert!(ops.iter().any(|op| matches!(op, SpanOp::Merge(row) if row.metadata.as_ref().is_some_and(|metadata| metadata["close_reason"] == "open_tool_limit"))));
}

#[test]
fn grok_finalize_closes_open_spans() {
    let temp = tempfile::tempdir().unwrap();
    let updates_path = temp.path().join("updates.jsonl");
    let events_path = temp.path().join("events.jsonl");
    std::fs::write(
        &updates_path,
        concat!(
            "{\"params\":{\"update\":{\"sessionUpdate\":\"user_message_chunk\",\"content\":{\"text\":\"unfinished\"}},\"_meta\":{\"promptIndex\":0,\"agentTimestampMs\":1000}}}\n",
            "{\"params\":{\"update\":{\"sessionUpdate\":\"agent_message_chunk\",\"content\":{\"text\":\"partial\"}},\"_meta\":{\"promptId\":\"p1\",\"streamStartMs\":1100,\"agentTimestampMs\":1110,\"chunkId\":1}}}\n"
        ),
    )
    .unwrap();
    std::fs::write(&events_path, "").unwrap();
    let mut event = envelope(0, 0);
    point_at(&mut event, &updates_path, &events_path);
    let registry = Registry::default_agents();
    let mut translator = registry.create("grok", "grok-session");
    let handled = translator.handle(&event, &ctx()).unwrap();
    let root_id = handled
        .iter()
        .find_map(|op| match op {
            SpanOp::Insert(row) if row.name == "Grok" => Some(row.span_id.clone()),
            _ => None,
        })
        .unwrap();
    let turn_id = handled
        .iter()
        .find_map(|op| match op {
            SpanOp::Insert(row) if row.name == "Turn 1" => Some(row.span_id.clone()),
            _ => None,
        })
        .unwrap();
    let llm_id = handled
        .iter()
        .find_map(|op| match op {
            SpanOp::Insert(row) if row.span_type == SpanType::Llm => Some(row.span_id.clone()),
            _ => None,
        })
        .unwrap();

    let finalized = translator.finalize(&ctx()).unwrap();
    assert!(finalized.iter().any(
        |op| matches!(op, SpanOp::Merge(row) if row.span_id == root_id && row.end_ms == Some(1110))
    ));
    assert!(finalized.iter().any(|op| {
        matches!(op, SpanOp::Merge(row) if row.span_id == turn_id
        && row.end_ms == Some(1110)
        && row.metadata.as_ref().is_some_and(|metadata| {
            metadata["incomplete"] == true && metadata["close_reason"] == "finalize"
        }))
    }));
    assert!(finalized.iter().any(|op| {
        matches!(op, SpanOp::Merge(row) if row.span_id == llm_id
        && row.end_ms == Some(1110)
        && row.metadata.as_ref().is_some_and(|metadata| {
            metadata["close_reason"] == "finalize"
        }))
    }));
}

#[test]
fn grok_assembles_user_and_identical_assistant_chunks() {
    let temp = tempfile::tempdir().unwrap();
    let updates_path = temp.path().join("updates.jsonl");
    let events_path = temp.path().join("events.jsonl");
    let assistant_chunk = "{\"params\":{\"update\":{\"sessionUpdate\":\"agent_message_chunk\",\"content\":{\"text\":\"ha\"}},\"_meta\":{\"promptId\":\"p1\",\"streamStartMs\":1100,\"agentTimestampMs\":1110}}}\n";
    std::fs::write(
        &updates_path,
        format!(
            "{}{}{}{}{}",
            "{\"params\":{\"update\":{\"sessionUpdate\":\"user_message_chunk\",\"content\":{\"text\":\"hel\"}},\"_meta\":{\"promptIndex\":0,\"agentTimestampMs\":1000}}}\n",
            "{\"params\":{\"update\":{\"sessionUpdate\":\"user_message_chunk\",\"content\":{\"text\":\"lo\"}},\"_meta\":{\"promptIndex\":0,\"agentTimestampMs\":1001}}}\n",
            assistant_chunk,
            assistant_chunk,
            "{\"params\":{\"update\":{\"sessionUpdate\":\"turn_completed\",\"prompt_id\":\"p1\",\"usage\":{\"modelCalls\":1}},\"_meta\":{\"agentTimestampMs\":1200}}}\n"
        ),
    )
    .unwrap();
    std::fs::write(&events_path, "").unwrap();
    let mut event = envelope(0, 0);
    point_at(&mut event, &updates_path, &events_path);
    let registry = Registry::default_agents();
    let mut translator = registry.create("grok", "grok-session");
    let ops = translator.handle(&event, &ctx()).unwrap();
    let turn_id = ops
        .iter()
        .find_map(|op| match op {
            SpanOp::Insert(row) if row.name == "Turn 1" => Some(row.span_id.clone()),
            _ => None,
        })
        .unwrap();
    assert!(ops.iter().any(
        |op| matches!(op, SpanOp::Merge(row) if row.span_id == turn_id && row.input == Some(json!("hello")))
    ));
    let llm = ops
        .iter()
        .find_map(|op| match op {
            SpanOp::Insert(row) if row.span_type == SpanType::Llm => Some(row),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        llm.input,
        Some(json!([
            {
                "role": "system",
                "content": std::fs::read_to_string(fixture("system_prompt.txt")).unwrap()
            },
            {"role": "user", "content": "hello"}
        ]))
    );
    assert!(ops.iter().any(|op| {
        matches!(op, SpanOp::Merge(row) if row.span_id == llm.span_id
            && row.output == Some(json!([{"role": "assistant", "content": "haha"}])))
    }));
}

#[test]
fn grok_late_update_completes_an_evicted_tool_without_reinserting_it() {
    let temp = tempfile::tempdir().unwrap();
    let updates_path = temp.path().join("updates.jsonl");
    let events_path = temp.path().join("events.jsonl");
    let mut body = String::from(
        "{\"params\":{\"update\":{\"sessionUpdate\":\"user_message_chunk\",\"content\":{\"text\":\"tools\"}},\"_meta\":{\"promptIndex\":0,\"agentTimestampMs\":1000}}}\n",
    );
    for index in 0..=256 {
        body.push_str(&format!(
            "{{\"params\":{{\"update\":{{\"sessionUpdate\":\"tool_call\",\"toolCallId\":\"tool-{index}\",\"title\":\"tool\"}},\"_meta\":{{\"agentTimestampMs\":{}}}}}}}\n",
            1100 + index
        ));
    }
    body.push_str(
        "{\"params\":{\"update\":{\"sessionUpdate\":\"tool_call_update\",\"toolCallId\":\"tool-0\",\"status\":\"completed\",\"rawOutput\":\"done\"},\"_meta\":{\"agentTimestampMs\":1500}}}\n",
    );
    std::fs::write(&updates_path, body).unwrap();
    std::fs::write(&events_path, "").unwrap();
    let mut event = envelope(0, 0);
    point_at(&mut event, &updates_path, &events_path);
    let registry = Registry::default_agents();
    let mut translator = registry.create("grok", "grok-session");
    let mut ops = translator.handle(&event, &ctx()).unwrap();
    ops.extend(drain(translator.as_mut()));
    let tool_id = ops
        .iter()
        .find_map(|op| match op {
            SpanOp::Insert(row)
                if row.span_type == SpanType::Tool
                    && row
                        .metadata
                        .as_ref()
                        .is_some_and(|metadata| metadata["tool_call_id"] == "tool-0") =>
            {
                Some(row.span_id.clone())
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(
        ops.iter()
            .filter(|op| matches!(op, SpanOp::Insert(row) if row.span_id == tool_id))
            .count(),
        1
    );
    assert!(ops.iter().any(|op| {
        matches!(op, SpanOp::Merge(row) if row.span_id == tool_id
            && row.output == Some(json!("done"))
            && row.late_merge_key.as_deref() == Some("tool:terminal_update")
            && row.metadata.as_ref().is_some_and(|metadata| metadata["incomplete"] == false))
    }));
}
