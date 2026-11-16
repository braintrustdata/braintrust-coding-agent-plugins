// Native event streams for storage and recovery equivalence tests.
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
    serde_json::from_value(json!({"source":source,"session_id":"checkpoint","event":name,"ts_ms":ts,"payload":payload})).unwrap()
}
// Keep the fixtures alive while consuming a stream: Codex events refer to
// transcript files owned by this guard.
pub type NativeStream = (String, Vec<Envelope>, Option<DistributedFixtures>);

pub fn native_streams() -> Vec<NativeStream> {
    let mut cases = Vec::<NativeStream>::new();
    for kind in AgentKind::ALL {
        let mut fixtures = DistributedFixtures::new();
        let tree = ProcessTree::root(1234);
        let id = "checkpoint";
        let t = 1_700_000_000_000;
        let mut events = fixtures.start_turn(kind, id, &tree, "first", t);
        events.push(fixtures.open_tool(kind, id, &tree, "call", "work", t + 10));
        events.push(fixtures.close_tool(kind, id, &tree, "call", "work", "result", t + 20));
        events.push(fixtures.close_session(kind, id, &tree, "complete", t + 30));
        events.extend(fixtures.start_turn(kind, id, &tree, "second", t + 40));
        cases.push((events[0].source.clone(), events, Some(fixtures)));
    }
    let v2 = [
        ("session.inbox.enqueued", json!({"sessionID":"native", "inboxID":"inbox", "item":{"type":"user","payload":{"text":"hello","files":[],"skills":[]},"delivery":"steer"}})),
        ("session.inbox.delivered", json!({"sessionID":"native","inboxID":"inbox"})),
        ("session.step.started", json!({"sessionID":"native","assistantMessageID":"step","started":2,"model":{"id":"mock","providerID":"mock"}})),
        ("session.text.ended", json!({"sessionID":"native","assistantMessageID":"step","ordinal":0,"text":"answer"})),
        ("session.step.ended", json!({"sessionID":"native","assistantMessageID":"step","finish":"stop"})),
        ("session.execution.succeeded", json!({"sessionID":"native"})),
    ].into_iter().enumerate().map(|(i,(name,data))| {
        let mut envelope = event("opencode", name, i as i64 + 1, json!({"id":format!("v2-{i}"),"type":name,"data":data}));
        envelope.source_version = Some("2.2.5".into());
        envelope
    }).collect();
    cases.push(("opencode".into(), v2, None));
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
    let events =
        include_str!("../fixtures/cursor/real-cli/interactive-multi-turn-success/hooks.ndjson")
            .lines()
            .map(|line| {
                let capture: Value = serde_json::from_str(line).unwrap();
                let payload = capture["payload"].clone();
                event(
                    "cursor",
                    payload["hook_event_name"].as_str().unwrap(),
                    capture["received_at_ms"].as_i64().unwrap(),
                    payload.clone(),
                )
            })
            .collect();
    cases.push(("cursor".into(), events, None));
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
