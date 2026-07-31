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
        ],
    );

    let output = tmp.path().join("spans");
    import_transcript(&transcript, ImportSource::Codex, options(&output), None)
        .await
        .unwrap();

    let rows = rows(&output.join("codex-past.ndjson"));
    assert_eq!(inserted(&rows, "task"), 2, "session and turn");
    assert_eq!(inserted(&rows, "tool"), 1);
    assert!(rows.iter().any(|row| row
        .pointer("/Insert/metadata/source")
        .and_then(Value::as_str)
        == Some("import")));
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
    import_transcript(&transcript, ImportSource::Claude, options(&output), None)
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
