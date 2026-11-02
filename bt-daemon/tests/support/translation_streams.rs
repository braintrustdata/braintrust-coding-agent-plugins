// Native event streams shared by reducer and storage-equivalence tests.
use bt_daemon as fixture_sdk;
#[allow(dead_code)]
mod distributed {
    use super::fixture_sdk as bt_daemon;
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/support/distributed.rs"
    ));
}
use bt_daemon::wire::Envelope;
use distributed::{AgentKind, DistributedFixtures, ProcessTree};
use serde_json::{json, Value};
fn event(source: &str, name: &str, ts: i64, payload: Value) -> Envelope {
    Envelope {
        source: source.into(),
        source_version: None,
        plugin_version: None,
        session_id: "checkpoint".into(),
        event: name.into(),
        ts_ms: ts,
        managed_run_id: None,
        capture: None,
        payload,
        route: None,
        config: None,
    }
}
// Keep the fixtures alive while consuming a stream: Codex events refer to
// transcript files owned by this guard.
pub type NativeStream = (String, Vec<Envelope>, Option<DistributedFixtures>);

pub fn native_streams() -> Vec<NativeStream> {
    let mut cases = Vec::<NativeStream>::new();
    {
        for kind in AgentKind::ALL {
            let mut fixtures = DistributedFixtures::new();
            let tree = ProcessTree::root(1234);
            let mut events =
                fixtures.start_turn(kind, "checkpoint", &tree, "first", 1_700_000_000_000);
            events.push(fixtures.open_tool(
                kind,
                "checkpoint",
                &tree,
                "call",
                "work",
                1_700_000_000_010,
            ));
            events.push(fixtures.close_tool(
                kind,
                "checkpoint",
                &tree,
                "call",
                "work",
                "result",
                1_700_000_000_020,
            ));
            events.push(fixtures.close_session(
                kind,
                "checkpoint",
                &tree,
                "complete",
                1_700_000_000_030,
            ));
            events.extend(fixtures.start_turn(
                kind,
                "checkpoint",
                &tree,
                "second",
                1_700_000_000_040,
            ));
            let source = events[0].source.clone();
            cases.push((source.clone(), events, Some(fixtures)));
        }
    }
    {
        let events = include_str!("../fixtures/muse-hooks-1.1.1.jsonl")
            .lines()
            .enumerate()
            .map(|(i, line)| {
                let payload: Value = serde_json::from_str(line).unwrap();
                let name = payload["hook_event_name"].as_str().unwrap().to_owned();
                event("muse", &name, i as i64 + 1, payload)
            })
            .collect();
        cases.push(("muse".to_owned(), events, None));
    }
    {
        let events = [
        ("sessionStart", json!({})),
        ("beforeSubmitPrompt", json!({"prompt":"work"})),
        (
            "preToolUse",
            json!({"tool_name":"Shell","tool_use_id":"call","tool_input":{"command":"echo hello"}}),
        ),
        (
            "postToolUse",
            json!({"tool_name":"Shell","tool_use_id":"call","tool_output":"hello","tool_input":{"command":"echo hello"},"duration":1}),
        ),
        ("afterAgentResponse", json!({"text":"done"})),
        ("stop", json!({"status":"completed"})),
        ("beforeSubmitPrompt", json!({"prompt":"next"})),
    ]
    .into_iter()
    .enumerate()
    .map(|(i, (name, mut payload))| {
        payload["hook_event_name"] = name.into();
        payload["conversation_id"] = "checkpoint".into();
        payload["generation_id"] = format!("generation-{i}").into();
        payload["model"] = "test".into();
        payload["workspace_roots"] = json!(["/workspace"]);
        event("cursor", name, i as i64 + 1, payload)
    })
    .collect();
        cases.push(("cursor".to_owned(), events, None));
    }
    {
        let records = [
            json!({"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"COMPLETED","content":"List files"}),
            json!({"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","status":"COMPLETED","content":"Inspecting","tool_calls":[{"name":"run_command","args":{"CommandLine":"ls"}}],"usage":{"inputTokens":12,"outputTokens":7,"totalTokens":19}}),
            json!({"step_index":2,"source":"MODEL","type":"CORTEX_STEP_TYPE_RUN_COMMAND","status":"COMPLETED","content":"README","tool_calls":[{"name":"run_command","args":{"CommandLine":"ls"},"result":"README"}]}),
        ];
        let mut contents = String::new();
        let mut boundaries = Vec::new();
        for record in records {
            contents.push_str(&serde_json::to_string(&record).unwrap());
            contents.push('\n');
            boundaries.push(contents.len());
        }
        let events = [("PreInvocation", boundaries[0]), ("PostInvocation", boundaries[1]), ("PostToolUse", boundaries[2]), ("Stop", boundaries[2])].into_iter().enumerate().map(|(i, (name, through))| {
        event("antigravity", name, i as i64 + 1, json!({"conversationId":"checkpoint","invocationNum":0,"initialNumSteps":1,"stepIdx":2,"fullyIdle":true,
            "transcriptPath":"/tmp/checkpoint/transcript.jsonl",
            "_bt_transcript_observation":{"path":"/tmp/checkpoint/transcript.jsonl","observed_bytes":through,"full_path":"/tmp/checkpoint/transcript_full.jsonl","full_observed_bytes":through},
            "_bt_transcript_snapshot":{"path":"/tmp/checkpoint/transcript_full.jsonl","contents":contents}}))
    }).collect();
        cases.push(("antigravity".to_owned(), events, None));
    }
    {
        let root =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/grok/transcript");
        let updates = root.join("updates.jsonl");
        let events = root.join("events.jsonl");
        let mut offset = 0;
        let stream = std::fs::read(&updates).unwrap().split_inclusive(|byte| *byte == b'\n').map(|line| {
        offset += line.len() as u64;
        event("grok", "Wake", 1600, json!({"cwd":"/repo","_bt_grok_transcript_mirrors":{"updates":{"mirror":updates,"through":offset},"events":{"mirror":events,"through":std::fs::metadata(&events).unwrap().len()}}}))
    }).collect();
        cases.push(("grok".to_owned(), stream, None));
    }
    cases.push((
        "debug".into(),
        vec![
            event("debug", "event", 1, json!({"value":1})),
            event("debug", "event", 2, json!({"value":2})),
        ],
        None,
    ));
    cases
}

// Include reducer-only correlation fields omitted by public SpanOp JSON.
pub fn comparable(ops: Vec<bt_daemon::SpanOp>) -> Vec<String> {
    let mut rows = ops
        .into_iter()
        .map(|op| {
            let (bt_daemon::SpanOp::Insert(row) | bt_daemon::SpanOp::Merge(row)) = &op;
            serde_json::to_string(&(
                &op,
                &row.turn_span_id,
                row.is_turn,
                &row.late_merge_key,
                &row.late_merge_version,
            ))
            .unwrap()
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}
