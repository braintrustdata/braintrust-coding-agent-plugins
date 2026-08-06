use bt_daemon::{import_transcript, DebugSinkFactory, ImportSource, Registry, ServeOptions};
use serde_json::{json, Value};
use std::io::Write;
use std::sync::Arc;

fn write_jsonl(path: &std::path::Path, records: &[Value]) {
    let mut file = std::fs::File::create(path).unwrap();
    for record in records {
        writeln!(file, "{}", serde_json::to_string(record).unwrap()).unwrap();
    }
}

fn options(output: &std::path::Path) -> ServeOptions {
    ServeOptions {
        version: "test".into(),
        translators: Arc::new(Registry::default_agents()),
        sink_factory: Arc::new(DebugSinkFactory { dir: output.into() }),
        auth_provider: None,
    }
}

fn rows(path: &std::path::Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn inserted(rows: &[Value], span_type: &str) -> usize {
    rows.iter()
        .filter(|row| row.pointer("/Insert/span_type").and_then(Value::as_str) == Some(span_type))
        .count()
}

#[tokio::test]
async fn imports_native_codex_rollout_through_codex_translator() {
    let tmp = tempfile::tempdir().unwrap();
    let transcript = tmp.path().join("rollout.jsonl");
    write_jsonl(
        &transcript,
        &[
            json!({"timestamp":"2026-01-01T00:00:01Z","type":"session_meta","payload":{"id":"codex-past","cwd":"/tmp/demo","cli_version":"1.2.3"}}),
            json!({"timestamp":"2026-01-01T00:00:02Z","type":"turn_context","payload":{"model":"gpt-test"}}),
            json!({"timestamp":"2026-01-01T00:00:03Z","type":"event_msg","payload":{"type":"task_started","turn_id":"turn-1"}}),
            json!({"timestamp":"2026-01-01T00:00:04Z","type":"event_msg","payload":{"type":"user_message","message":"list files"}}),
            json!({"timestamp":"2026-01-01T00:00:05Z","type":"response_item","payload":{"type":"function_call","call_id":"call-1","name":"shell","arguments":"{\"command\":\"ls\"}"}}),
            json!({"timestamp":"2026-01-01T00:00:06Z","type":"response_item","payload":{"type":"function_call_output","call_id":"call-1","output":"README.md"}}),
            json!({"timestamp":"2026-01-01T00:00:07Z","type":"event_msg","payload":{"type":"task_complete","last_agent_message":"Done"}}),
            json!({"timestamp":"2026-01-01T00:00:08Z","type":"turn_context","payload":{"model":"gpt-test"}}),
            json!({"timestamp":"2026-01-01T00:00:09Z","type":"event_msg","payload":{"type":"task_started","turn_id":"turn-2"}}),
            json!({"timestamp":"2026-01-01T00:00:10Z","type":"event_msg","payload":{"type":"user_message","message":"show status"}}),
            json!({"timestamp":"2026-01-01T00:00:11Z","type":"event_msg","payload":{"type":"task_complete","turn_id":"turn-2","last_agent_message":"Clean"}}),
        ],
    );

    let output = tmp.path().join("spans");
    import_transcript(
        &transcript,
        ImportSource::Codex,
        options(&output),
        None,
        false,
    )
    .await
    .unwrap();

    let rows = rows(&output.join("codex-past.ndjson"));
    assert_eq!(inserted(&rows, "task"), 3, "session and two turns");
    assert_eq!(inserted(&rows, "tool"), 1);
    assert!(rows.iter().any(|row| row
        .pointer("/Insert/metadata/source")
        .and_then(Value::as_str)
        == Some("import")));
    let turn_ids = rows
        .iter()
        .filter_map(|row| {
            row.pointer("/Insert/metadata/turn_id")
                .and_then(Value::as_str)
        })
        .collect::<Vec<_>>();
    assert!(turn_ids.contains(&"turn-1"));
    assert!(turn_ids.contains(&"turn-2"));
}

#[tokio::test]
async fn imports_native_claude_transcript_with_multiple_turns_and_tools() {
    let tmp = tempfile::tempdir().unwrap();
    let transcript = tmp.path().join("claude.jsonl");
    write_jsonl(
        &transcript,
        &[
            json!({"type":"user","timestamp":"2026-01-01T00:00:01Z","sessionId":"claude-past","cwd":"/tmp/demo","version":"2.0.0","message":{"role":"user","content":"run it"}}),
            json!({"type":"assistant","timestamp":"2026-01-01T00:00:02Z","sessionId":"claude-past","requestId":"req-1","message":{"id":"msg-1","model":"claude-test","role":"assistant","content":[{"type":"tool_use","id":"tool-1","name":"Bash","input":{"command":"true"}}],"usage":{"input_tokens":10,"output_tokens":3}}}),
            json!({"type":"user","timestamp":"2026-01-01T00:00:03Z","sessionId":"claude-past","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"tool-1","content":"ok","is_error":false}]}}),
            json!({"type":"assistant","timestamp":"2026-01-01T00:00:04Z","sessionId":"claude-past","requestId":"req-2","message":{"id":"msg-2","model":"claude-test","role":"assistant","content":[{"type":"text","text":"done"}],"usage":{"input_tokens":12,"output_tokens":2}}}),
            json!({"type":"user","timestamp":"2026-01-01T00:00:05Z","sessionId":"claude-past","message":{"role":"user","content":"again"}}),
            json!({"type":"assistant","timestamp":"2026-01-01T00:00:06Z","sessionId":"claude-past","requestId":"req-3","message":{"id":"msg-3","model":"claude-test","role":"assistant","content":[{"type":"text","text":"again done"}],"usage":{"input_tokens":14,"output_tokens":2}}}),
        ],
    );

    let output = tmp.path().join("spans");
    import_transcript(
        &transcript,
        ImportSource::Claude,
        options(&output),
        None,
        false,
    )
    .await
    .unwrap();

    let rows = rows(&output.join("claude-past.ndjson"));
    assert_eq!(inserted(&rows, "task"), 3, "session and two turns");
    assert_eq!(inserted(&rows, "llm"), 3);
    assert_eq!(inserted(&rows, "tool"), 1);
    assert!(rows.iter().any(|row| {
        row.pointer("/Insert/metadata/recovered_from_transcript")
            .and_then(Value::as_bool)
            == Some(true)
    }));
}

#[tokio::test]
async fn imports_non_monotonic_claude_records_into_their_native_turns() {
    let tmp = tempfile::tempdir().unwrap();
    let transcript = tmp.path().join("claude.jsonl");
    write_jsonl(
        &transcript,
        &[
            json!({"type":"user","uuid":"user-1","timestamp":"2026-01-01T00:00:01Z","sessionId":"claude-non-monotonic","cwd":"/tmp/demo","version":"2.0.0","message":{"role":"user","content":"first"}}),
            json!({"type":"assistant","uuid":"assistant-1","parentUuid":"user-1","timestamp":"2026-01-01T00:00:02Z","sessionId":"claude-non-monotonic","isApiErrorMessage":true,"apiErrorStatus":404,"error":"model_not_found","message":{"id":"msg-1","model":"<synthetic>","role":"assistant","content":[{"type":"text","text":"model unavailable"}],"usage":{"input_tokens":0,"output_tokens":0}}}),
            // Claude may append queue bookkeeping before conversation records
            // whose native timestamps are earlier.
            json!({"type":"queue-operation","operation":"enqueue","timestamp":"2026-01-01T00:00:10Z","sessionId":"claude-non-monotonic","content":"third"}),
            json!({"type":"queue-operation","operation":"dequeue","timestamp":"2026-01-01T00:00:10Z","sessionId":"claude-non-monotonic"}),
            json!({"type":"user","uuid":"user-2","parentUuid":"assistant-1","timestamp":"2026-01-01T00:00:05Z","sessionId":"claude-non-monotonic","message":{"role":"user","content":[{"type":"text","text":"second"}]}}),
            json!({"type":"assistant","uuid":"assistant-2","parentUuid":"user-2","timestamp":"2026-01-01T00:00:06Z","sessionId":"claude-non-monotonic","isApiErrorMessage":false,"message":{"id":"msg-2","model":"claude-test","role":"assistant","content":[{"type":"text","text":"second response"}],"usage":{"input_tokens":4,"output_tokens":2}}}),
            json!({"type":"user","uuid":"user-3","parentUuid":"assistant-2","timestamp":"2026-01-01T00:00:11Z","sessionId":"claude-non-monotonic","message":{"role":"user","content":"third"}}),
            json!({"type":"assistant","uuid":"assistant-3","parentUuid":"user-3","timestamp":"2026-01-01T00:00:12Z","sessionId":"claude-non-monotonic","isApiErrorMessage":true,"apiErrorStatus":429,"error":"rate_limit_error","message":{"id":"msg-3","model":"<synthetic>","role":"assistant","content":[{"type":"text","text":"rate limited"}],"usage":{"input_tokens":0,"output_tokens":0}}}),
        ],
    );

    let output = tmp.path().join("spans");
    import_transcript(
        &transcript,
        ImportSource::Claude,
        options(&output),
        None,
        false,
    )
    .await
    .unwrap();

    let rows = rows(&output.join("claude-non-monotonic.ndjson"));
    assert_eq!(inserted(&rows, "task"), 4, "session and three turns");
    assert_eq!(inserted(&rows, "llm"), 3);

    let insert_named = |name: &str| {
        rows.iter()
            .filter_map(|op| op.get("Insert"))
            .find(|row| row.get("name").and_then(Value::as_str) == Some(name))
            .unwrap()
    };
    let turn_1 = insert_named("Turn 1");
    let turn_2 = insert_named("Turn 2");
    let turn_3 = insert_named("Turn 3");
    let merged_turn = |turn: &Value| {
        let span_id = turn.get("span_id").and_then(Value::as_str).unwrap();
        rows.iter()
            .filter_map(|op| op.get("Merge"))
            .find(|row| row.get("span_id").and_then(Value::as_str) == Some(span_id))
            .unwrap()
    };
    assert_eq!(
        merged_turn(turn_1).get("end_ms").and_then(Value::as_i64),
        Some(1_767_225_602_000)
    );
    assert_eq!(
        merged_turn(turn_1).get("error").and_then(Value::as_str),
        Some("model_not_found")
    );
    assert_eq!(
        merged_turn(turn_2).get("end_ms").and_then(Value::as_i64),
        Some(1_767_225_606_000)
    );
    assert!(merged_turn(turn_2).get("error").is_none());
    assert_eq!(
        merged_turn(turn_3).get("error").and_then(Value::as_str),
        Some("rate_limit_error")
    );
    let llm = |request_id: &str| {
        rows.iter()
            .filter_map(|op| op.get("Insert"))
            .find(|row| {
                row.pointer("/metadata/request_id").and_then(Value::as_str) == Some(request_id)
            })
            .unwrap()
    };
    for (request_id, turn) in [("msg-1", turn_1), ("msg-2", turn_2), ("msg-3", turn_3)] {
        assert_eq!(
            llm(request_id)
                .pointer("/parent_span_ids/0")
                .and_then(Value::as_str),
            turn.get("span_id").and_then(Value::as_str)
        );
    }
    assert_eq!(
        llm("msg-1").get("error").and_then(Value::as_str),
        Some("model_not_found")
    );
    assert_eq!(
        llm("msg-1")
            .pointer("/metadata/api_error_status")
            .and_then(Value::as_u64),
        Some(404)
    );
    assert_eq!(
        llm("msg-3").get("error").and_then(Value::as_str),
        Some("rate_limit_error")
    );
}
