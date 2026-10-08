//! Independent reducer checks. Real fixtures retain native receipt times and IDs;
//! explicitly synthetic cases exercise documented schemas and recovery edges.
#[path = "support/span_identity.rs"]
mod span_identity;

use braintrust_sdk_rust::{SpanComponents, SpanObjectType};
use bt_daemon::wire::{BackendAuth, Envelope, SessionRoute, TraceDestination};
use bt_daemon::{AgentTranslator, Registry, SessionCtx, SpanOp, SpanRow, SpanType};
use serde_json::{json, Value};
use span_identity::IdentityLedger;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// A Cursor hook envelope. Fields the CLI sends with every agent hook are
/// filled in when the test does not set them; a test that needs a hook to
/// share a generation with an earlier one sets `generation_id` explicitly.
fn event(kind: &str, ts: i64, mut payload: Value) -> Envelope {
    payload["hook_event_name"] = json!(kind);
    payload["conversation_id"] = json!("test-session");
    if !kind.starts_with("_bt_") {
        let object = payload.as_object_mut().unwrap();
        object
            .entry("generation_id")
            .or_insert_with(|| json!(format!("generation-{kind}-{ts}")));
        object.entry("model").or_insert_with(|| json!("default"));
        object
            .entry("workspace_roots")
            .or_insert_with(|| json!(["/workspace"]));
    }
    Envelope {
        source: "cursor".into(),
        source_version: Some("2026.10.01-14929f9".into()),
        plugin_version: Some("0.1.0".into()),
        session_id: "test-session".into(),
        event: kind.into(),
        ts_ms: ts,
        managed_run_id: None,
        capture: None,
        payload,
        route: None,
        config: None,
    }
}
struct Harness {
    translator: Box<dyn AgentTranslator>,
    ctx: SessionCtx,
    ops: Vec<SpanOp>,
    ledger: IdentityLedger,
}
impl Harness {
    fn new(session: &str) -> Self {
        Self {
            translator: Registry::default_agents().create("cursor", session),
            ctx: SessionCtx {
                session_id: session.into(),
                config: None,
            },
            ops: Vec::new(),
            ledger: IdentityLedger::default(),
        }
    }
    fn handle(&mut self, event: &Envelope) {
        let ops = self.translator.handle(event, &self.ctx).unwrap();
        self.ledger.check(&ops);
        self.ops.extend(ops);
        let mut batches = 0;
        while let Some(ops) = self.translator.drain_pending(&self.ctx).unwrap() {
            self.ledger.check(&ops);
            self.ops.extend(ops);
            batches += 1;
            assert!(
                batches < 1024,
                "transcript draining did not make bounded progress"
            );
        }
    }
    fn finish(&mut self) {
        let ops = self.translator.finalize(&self.ctx).unwrap();
        self.ledger.check(&ops);
        self.ops.extend(ops);
    }
    fn inserted(&self, kind: SpanType) -> Vec<&SpanRow> {
        self.ops
            .iter()
            .filter_map(|op| match op {
                SpanOp::Insert(row) if row.span_type == kind => Some(row),
                _ => None,
            })
            .collect()
    }
    fn turns(&self) -> Vec<&SpanRow> {
        self.inserted(SpanType::Task)
            .into_iter()
            .filter(|r| r.name.starts_with("Turn "))
            .collect()
    }
    fn rows(&self) -> HashMap<String, Value> {
        let mut rows: HashMap<String, Value> = HashMap::new();
        for op in &self.ops {
            let row = match op {
                SpanOp::Insert(r) | SpanOp::Merge(r) => r,
            };
            let value = serde_json::to_value(row).unwrap();
            match op {
                SpanOp::Insert(_) => {
                    rows.insert(row.span_id.clone(), value);
                }
                SpanOp::Merge(_) => {
                    let prior = rows.get_mut(&row.span_id).unwrap().as_object_mut().unwrap();
                    for (key, value) in value.as_object().unwrap() {
                        if matches!(key.as_str(), "metadata" | "metrics") {
                            prior
                                .entry(key.clone())
                                .or_insert_with(|| json!({}))
                                .as_object_mut()
                                .unwrap()
                                .extend(value.as_object().unwrap().clone());
                        } else {
                            prior.insert(key.clone(), value.clone());
                        }
                    }
                }
            }
        }
        rows
    }
}
fn fixture(scenario: &str, file: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/cursor/real-cli")
        .join(scenario)
        .join(file)
}
fn real_capture(scenario: &str) -> Harness {
    let contents = std::fs::read_to_string(fixture(scenario, "hooks.ndjson")).unwrap();
    let records: Vec<Value> = contents
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let session = records[0]["payload"]["conversation_id"]
        .as_str()
        .unwrap()
        .to_string();
    let mut harness = Harness::new(&session);
    for record in records {
        let payload = record["payload"].clone();
        let mut envelope = event(
            payload["hook_event_name"].as_str().unwrap(),
            record["received_at_ms"].as_i64().unwrap(),
            payload.clone(),
        );
        envelope.payload = payload;
        envelope.session_id = session.clone();
        harness.handle(&envelope);
        // Duplicate ingress must not create additional turns/model calls/tools.
        harness.handle(&envelope);
    }
    harness.finish();
    harness
}
#[test]
fn real_interactive_error_capture_has_two_turns_one_read_and_turn_scoped_usage() {
    let h = real_capture("interactive-multi-turn-error");
    assert_eq!(h.turns().len(), 2);
    assert_eq!(h.inserted(SpanType::Tool).len(), 1);
    let rows = h.rows();
    for (index, expected) in [(0, (12554, 5)), (1, (12823, 30))] {
        let turn = &rows[&h.turns()[index].span_id];
        assert_eq!(turn["error"], "error");
        assert_eq!(turn["metrics"]["prompt_tokens"], expected.0);
        assert_eq!(turn["metrics"]["completion_tokens"], expected.1);
        assert_eq!(turn["metadata"]["token_usage_scope"], "turn");
    }
    let tool = &rows[&h.inserted(SpanType::Tool)[0].span_id];
    assert_eq!(tool["parent_span_ids"], json!([h.turns()[1].span_id]));
    assert!(tool["metadata"]["pre_read_content"]
        .as_str()
        .unwrap()
        .contains("audit"));
    assert!(tool["metadata"].get("pre_read_content_source").is_none());
    // The pre-read content is evidence, not proof of the generic result's completeness.
    assert_eq!(tool["metadata"]["result_completeness"], "unknown");
    assert!(h
        .inserted(SpanType::Llm)
        .iter()
        .all(|llm| llm.metrics.is_none()));
}
#[test]
fn real_headless_generation_drift_keeps_one_turn_and_correlates_parallel_tools() {
    let h = real_capture("headless-tools-error");
    assert_eq!(h.turns().len(), 1);
    assert_eq!(h.inserted(SpanType::Tool).len(), 2);
    let rows = h.rows();
    for tool in h.inserted(SpanType::Tool) {
        assert_eq!(tool.parent_span_ids, vec![h.turns()[0].span_id.clone()]);
        assert!(rows[&tool.span_id]["end_ms"].is_number());
    }
    let shell = h
        .inserted(SpanType::Tool)
        .into_iter()
        .find(|r| r.name == "Shell")
        .unwrap();
    assert!(rows[&shell.span_id]["metadata"]
        .get("result_source")
        .is_none());
    assert!(rows[&shell.span_id]["output"]
        .as_str()
        .unwrap()
        .contains("audit-shell-ok"));
}
#[test]
fn synthetic_success_is_parented_and_duplicate_response_stop_usage_is_not_summed() {
    let mut h = Harness::new("test-session");
    h.handle(&event(
        "beforeSubmitPrompt",
        100,
        json!({"generation_id":"turn-a","prompt":"read"}),
    ));
    h.handle(&event(
        "afterAgentThought",
        110,
        json!({"generation_id":"request-a","text":"thinking"}),
    ));
    h.handle(&event("preToolUse",120,json!({"generation_id":"unrelated","tool_name":"Read","tool_use_id":"r1","tool_input":{"file_path":"/tmp/a"}})));
    let completions = h.inserted(SpanType::Llm);
    assert_eq!(completions.len(), 1);
    assert_eq!(completions[0].start_ms, Some(100));
    assert_eq!(completions[0].end_ms, Some(120));
    h.handle(&event(
        "postToolUse",
        130,
        json!({"tool_name":"Read","tool_use_id":"r1","tool_input":{"file_path":"/tmp/a"},"tool_output":"metadata only","duration":10}),
    ));
    let response = event(
        "afterAgentResponse",
        140,
        json!({"generation_id":"turn-a","text":"done","input_tokens":20,"output_tokens":4,"cache_read_tokens":7,"cache_write_tokens":0}),
    );
    h.handle(&response);
    h.handle(&response);
    h.handle(&event(
        "stop",
        150,
        json!({"generation_id":"turn-a","status":"completed","input_tokens":20,"output_tokens":4}),
    ));
    h.finish();
    assert_eq!(h.turns().len(), 1);
    assert_eq!(h.inserted(SpanType::Llm).len(), 2);
    let rows = h.rows();
    let turn = &rows[&h.turns()[0].span_id];
    assert_eq!(turn["output"], "done");
    assert_eq!(turn["metrics"]["prompt_tokens"], 20);
    assert_eq!(turn["metrics"]["completion_tokens"], 4);
    assert_eq!(turn["metrics"]["tokens"], 24);
    let llms = h.inserted(SpanType::Llm);
    assert_eq!(llms[0].end_ms, Some(120));
    assert_eq!(llms[1].start_ms, Some(130));
    let first = llms[0].output.as_ref().unwrap();
    assert_eq!(first[0]["finish_reason"], "tool_calls");
    assert_eq!(first[0]["message"]["role"], "assistant");
    assert_eq!(first[0]["message"]["content"], Value::Null);
    assert_eq!(
        first[0]["message"]["reasoning"][0]["summary"][0]["text"],
        "thinking"
    );
    assert_eq!(
        first[0]["message"]["tool_calls"],
        json!([{
            "id":"r1", "type":"function", "function":{
                "name":"Read", "arguments":"{\"file_path\":\"/tmp/a\"}"
            }
        }])
    );
    let second_input = llms[1].input.as_ref().unwrap().as_array().unwrap();
    assert_eq!(second_input[1], first[0]["message"]);
    assert_eq!(
        second_input[2],
        json!({
            "role":"tool", "tool_call_id":"r1", "content":"metadata only"
        })
    );
    assert_eq!(
        llms[1].output.as_ref().unwrap()[0]["message"]["content"],
        "done"
    );
    assert!(llms[1].output.as_ref().unwrap()[0]["finish_reason"].is_null());
    for child in h
        .inserted(SpanType::Tool)
        .into_iter()
        .chain(h.inserted(SpanType::Llm))
    {
        assert_eq!(child.parent_span_ids, vec![h.turns()[0].span_id.clone()]);
    }
    assert!(h
        .inserted(SpanType::Llm)
        .iter()
        .all(|r| r.metadata.as_ref().unwrap()["input_reconstructed"] == true));
}
#[test]
fn terminal_tool_hook_does_not_close_a_later_completion() {
    let mut h = Harness::new("test-session");
    h.handle(&event(
        "beforeSubmitPrompt",
        100,
        json!({"prompt":"run tools"}),
    ));
    h.handle(&event(
        "preToolUse",
        120,
        json!({"tool_name":"Shell","tool_use_id":"slow","tool_input":{"command":"sleep 1"}}),
    ));
    h.handle(&event(
        "afterAgentThought",
        130,
        json!({"text":"working while the tool runs"}),
    ));
    h.handle(&event(
        "postToolUse",
        150,
        json!({"tool_name":"Shell","tool_use_id":"slow","tool_input":{"command":"sleep 1"},"tool_output":"done","duration":30}),
    ));
    assert_eq!(h.inserted(SpanType::Llm).len(), 1);
    h.handle(&event("stop", 160, json!({"status":"completed"})));
    let llms = h.inserted(SpanType::Llm);
    assert_eq!(llms.len(), 2);
    assert_eq!(llms[0].end_ms, Some(120));
    assert_eq!(llms[1].end_ms, Some(160));
}

#[test]
fn missing_pre_tool_hook_uses_duration_to_end_completion_before_execution() {
    let mut h = Harness::new("test-session");
    h.handle(&event(
        "beforeSubmitPrompt",
        100,
        json!({"prompt":"run a tool"}),
    ));
    h.handle(&event(
        "afterAgentThought",
        110,
        json!({"text":"running the command"}),
    ));
    h.handle(&event(
        "postToolUse",
        150,
        json!({"tool_name":"Shell","tool_use_id":"missing-pre","tool_input":{"command":"echo done"},"tool_output":"done","duration":30}),
    ));
    h.handle(&event(
        "afterAgentThought",
        160,
        json!({"text":"the command returned"}),
    ));
    h.finish();
    let llms = h.inserted(SpanType::Llm);
    assert_eq!(llms.len(), 2);
    assert_eq!(llms[0].end_ms, Some(120));
    assert_eq!(h.inserted(SpanType::Tool)[0].start_ms, Some(120));
    assert_eq!(
        llms[0].output.as_ref().unwrap()[0]["message"]["tool_calls"],
        json!([{
            "id":"missing-pre", "type":"function", "function":{
                "name":"Shell", "arguments":"{\"command\":\"echo done\"}"
            }
        }])
    );
    let next_input = llms[1].input.as_ref().unwrap().as_array().unwrap();
    assert_eq!(next_input[1]["tool_calls"][0]["id"], "missing-pre");
    assert_eq!(next_input[2]["role"], "tool");
    assert_eq!(next_input[2]["tool_call_id"], "missing-pre");
    assert_eq!(next_input[2]["content"], "done");
}

#[test]
fn synthetic_denial_and_missing_tool_end_preserve_native_outcome() {
    let mut h = Harness::new("test-session");
    h.handle(&event(
        "beforeSubmitPrompt",
        100,
        json!({"generation_id":"t","prompt":"run"}),
    ));
    h.handle(&event(
        "preToolUse",
        110,
        json!({"tool_name":"Shell","tool_use_id":"denied","tool_input":{"command":"rm x"}}),
    ));
    h.handle(&event("postToolUseFailure",120,json!({"tool_name":"Shell","tool_use_id":"denied","tool_input":{"command":"rm x"},"failure_type":"permission_denied","error_message":"Denied by policy","is_interrupt":false,"duration":0})));
    h.handle(&event(
        "preToolUse",
        130,
        json!({"tool_name":"Shell","tool_use_id":"lost","tool_input":{"command":"pwd"}}),
    ));
    h.finish();
    let rows = h.rows();
    for tool in h.inserted(SpanType::Tool) {
        let row = &rows[&tool.span_id];
        if tool.input.as_ref().unwrap()["command"] == "rm x" {
            assert_eq!(row["error"], "Denied by policy");
            assert_eq!(row["metadata"]["tool_approval"], "denied");
        } else {
            assert_eq!(row["metadata"]["status"], "incomplete");
        }
    }
}
#[test]
fn synthetic_attached_session_preserves_external_identity_across_all_merges() {
    let mut h = Harness::new("test-session");
    let mut components = SpanComponents::new(SpanObjectType::ProjectLogs);
    components.span_id = Some("external-parent".into());
    components.root_span_id = Some("external-root".into());
    h.ctx.config = Some(
        SessionRoute {
            destination: Some(TraceDestination::ParentSpan { components }),
            tags: vec!["custom-tag".into()],
            additional_metadata: Some(json!({"team":"audit","_bt_secret":"hidden"})),
            ..Default::default()
        }
        .with_auth(BackendAuth {
            token: "private-token".into(),
            api_url: None,
            app_url: None,
            org_name: None,
            org_id: None,
        }),
    );
    h.handle(&event(
        "beforeSubmitPrompt",
        100,
        json!({"generation_id":"t","prompt":"hello","cwd":"","workspace_roots":["/audit/workspace"],
            "model_id":"audit-model","model_params":[{"id":"effort","value":"high"}]}),
    ));
    h.handle(&event(
        "afterAgentResponse",
        110,
        json!({"generation_id":"t","text":"hi","model":"audit-model","workspace_roots":["/audit/second-workspace"]}),
    ));
    h.handle(&event(
        "sessionEnd",
        120,
        json!({"reason":"completed","final_status":"completed","model":"audit-model","workspace_roots":["/audit/second-workspace"]}),
    ));
    h.finish();
    let root = h
        .inserted(SpanType::Task)
        .into_iter()
        .find(|r| r.name == "Cursor session")
        .unwrap();
    assert_eq!(root.parent_span_ids, vec!["external-parent"]);
    assert!(h.ops.iter().all(|op| match op {
        SpanOp::Insert(r) | SpanOp::Merge(r) => r.root_span_id == "external-root",
    }));
    assert_eq!(root.metadata.as_ref().unwrap()["team"], "audit");
    assert_eq!(
        root.metadata.as_ref().unwrap()["workspace"],
        "/audit/workspace"
    );
    assert_eq!(
        h.rows()[&root.span_id]["metadata"]["workspace"],
        "/audit/second-workspace"
    );
    let llm = h.inserted(SpanType::Llm)[0];
    assert_eq!(llm.metadata.as_ref().unwrap()["model"], "audit-model");
    assert_eq!(
        llm.metadata.as_ref().unwrap()["model_params"],
        json!([{"id":"effort","value":"high"}])
    );
    assert!(root.metadata.as_ref().unwrap().get("_bt_secret").is_none());
    assert!(!serde_json::to_string(&h.ops)
        .unwrap()
        .contains("private-token"));
}

#[test]
fn imported_session_root_marks_its_start_timestamp_as_estimated() {
    let mut h = Harness::new("test-session");
    h.handle(&event(
        "_bt_importStart",
        100,
        json!({"start_time_estimated":true}),
    ));
    let root = h
        .inserted(SpanType::Task)
        .into_iter()
        .find(|row| row.name == "Cursor session")
        .unwrap();
    assert_eq!(
        root.metadata.as_ref().unwrap()["start_time_estimated"],
        true
    );
}

#[test]
fn imported_terminal_error_message_is_preserved_on_the_turn_span() {
    let dir = tempfile::tempdir().unwrap();
    let transcript = dir.path().join("transcript.jsonl");
    std::fs::write(
        &transcript,
        format!(
            "{}\n",
            json!({"role":"user","message":{"content":[{"type":"text","text":"hello"}]}})
        ),
    )
    .unwrap();
    let through = std::fs::metadata(&transcript).unwrap().len();
    let mut h = Harness::new("test-session");
    h.handle(&event(
        "_bt_importStart",
        100,
        json!({"start_time_estimated":true}),
    ));
    h.handle(&mirrored(
        "_bt_importCheckpoint",
        110,
        &transcript,
        through,
        json!({}),
    ));
    h.handle(&mirrored(
        "_bt_importStop",
        120,
        &transcript,
        through,
        json!({"status":"error","error_message":"WritableIterable is closed"}),
    ));
    let turn = h.turns().into_iter().next().unwrap();
    assert_eq!(
        h.rows()[&turn.span_id]["error"],
        "WritableIterable is closed"
    );
}

#[test]
fn transcript_replacement_keeps_turn_span_ids_monotonic() {
    let temp = tempfile::tempdir().unwrap();
    let first_path = temp.path().join("first-snapshot.jsonl");
    let next_path = temp.path().join("replacement-snapshot.jsonl");
    let first = "{\"role\":\"user\",\"message\":{\"content\":\"first prompt\"}}\n{\"role\":\"assistant\",\"message\":{\"content\":\"first answer\"}}\n";
    let next = "{\"role\":\"user\",\"message\":{\"content\":\"second prompt\"}}\n{\"role\":\"assistant\",\"message\":{\"content\":\"second answer\"}}\n";
    std::fs::write(&first_path, first).unwrap();
    std::fs::write(&next_path, next).unwrap();

    let mut h = Harness::new("test-session");
    h.handle(&event("sessionStart", 100, json!({})));
    h.handle(&mirrored(
        "ImportCheckpoint",
        110,
        &first_path,
        first.len() as u64,
        json!({}),
    ));
    let first_turn = h.turns().into_iter().next().unwrap().span_id.clone();
    h.handle(&mirrored(
        "ImportCheckpoint",
        120,
        &next_path,
        next.len() as u64,
        json!({}),
    ));

    let turns = h.turns();
    assert_eq!(turns.len(), 2);
    assert_eq!(turns[0].name, "Turn 1");
    assert_eq!(turns[0].span_id, first_turn);
    assert_eq!(turns[1].name, "Turn 2");
    assert_ne!(turns[0].span_id, turns[1].span_id);
}

fn mirrored(kind: &str, ts: i64, path: &Path, through: u64, payload: Value) -> Envelope {
    let mut e = event(kind, ts, payload);
    e.payload["_bt_transcript_mirror"] = json!({"mirror":path,"through":through});
    e
}
#[test]
fn synthetic_transcript_partial_records_and_large_batches_are_bounded_and_optional() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("transcript.jsonl");
    let prompt = json!({"role":"user","message":{"content":[{"type":"text","text":"hello"}]}})
        .to_string()
        + "\n";
    let assistant=json!({"role":"assistant","message":{"content":[{"type":"text","text":"x".repeat(80_000)}]}}).to_string()+"\n";
    std::fs::write(&path, format!("{prompt}{assistant}")).unwrap();
    let mut h = Harness::new("test-session");
    h.handle(&mirrored(
        "sessionStart",
        100,
        &path,
        (prompt.len() - 1) as u64,
        json!({}),
    ));
    assert!(
        h.turns().is_empty(),
        "an unterminated transcript record is not evidence yet"
    );
    h.handle(&mirrored(
        "stop",
        110,
        &path,
        std::fs::metadata(&path).unwrap().len(),
        json!({"status":"completed"}),
    ));
    assert_eq!(h.turns().len(), 1);
    assert_eq!(h.inserted(SpanType::Llm).len(), 1);
    h.handle(&event(
        "sessionEnd",
        120,
        json!({"reason":"completed","final_status":"completed","transcript_path":"/unavailable"}),
    ));
    h.finish();
    assert_eq!(h.turns().len(), 1);
}
#[test]
fn repeated_transcript_prompt_after_stop_creates_a_new_turn_without_a_prompt_hook() {
    for first_prompt_hook in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("repeated-prompts.jsonl");
        let first =
            transcript_record("user", "continue") + &transcript_record("assistant", "first answer");
        let second = transcript_record("user", "continue")
            + &transcript_record("assistant", "second answer");
        std::fs::write(&path, format!("{first}{second}")).unwrap();
        let mut h = Harness::new("test-session");
        if first_prompt_hook {
            h.handle(&event(
                "beforeSubmitPrompt",
                100,
                json!({"generation_id":"t1","prompt":"continue"}),
            ));
        }
        h.handle(&mirrored(
            "stop",
            200,
            &path,
            first.len() as u64,
            json!({"status":"completed"}),
        ));
        h.handle(&mirrored(
            "stop",
            300,
            &path,
            (first.len() + second.len()) as u64,
            json!({"status":"completed"}),
        ));
        h.finish();
        let turns = h.turns();
        assert_eq!(turns.len(), 2);
        assert_ne!(turns[0].span_id, turns[1].span_id);
        let rows = h.rows();
        for (turn, answer, end) in [
            (turns[0], "first answer", 200),
            (turns[1], "second answer", 300),
        ] {
            assert_eq!(rows[&turn.span_id]["input"], "continue");
            assert_eq!(rows[&turn.span_id]["output"], answer);
            assert_eq!(rows[&turn.span_id]["end_ms"], end);
        }
        let llms = h.inserted(SpanType::Llm);
        assert_eq!(llms.len(), 2);
        for (llm, turn) in llms.into_iter().zip(turns) {
            assert_eq!(llm.parent_span_ids, vec![turn.span_id.clone()]);
        }
    }
}

#[test]
fn synthetic_delayed_transcript_after_closed_native_turn_does_not_create_phantom_turn() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("transcript.jsonl");
    std::fs::write(
        &path,
        format!(
            "{}\n{}\n",
            json!({"role":"user","message":{"content":[{"type":"text","text":"hello"}]}}),
            json!({"role":"assistant","message":{"content":[{"type":"text","text":"hi"}]}})
        ),
    )
    .unwrap();
    let mut h = Harness::new("test-session");
    h.handle(&event(
        "beforeSubmitPrompt",
        100,
        json!({"generation_id":"t","prompt":"hello"}),
    ));
    h.handle(&event(
        "afterAgentResponse",
        110,
        json!({"generation_id":"t","text":"hi"}),
    ));
    h.handle(&event(
        "stop",
        120,
        json!({"generation_id":"t","status":"completed"}),
    ));
    h.handle(&mirrored(
        "sessionEnd",
        130,
        &path,
        std::fs::metadata(&path).unwrap().len(),
        json!({"reason":"completed","final_status":"completed"}),
    ));
    h.finish();
    assert_eq!(
        h.turns().len(),
        1,
        "mirror enrichment must reuse its native turn"
    );
    assert_eq!(
        h.inserted(SpanType::Llm).len(),
        1,
        "identical transcript output should not duplicate native output"
    );
}
#[test]
fn synthetic_late_tool_completion_retains_old_turn_and_does_not_split_new_model() {
    let mut h = Harness::new("test-session");
    h.handle(&event(
        "beforeSubmitPrompt",
        100,
        json!({"generation_id":"t1","prompt":"first"}),
    ));
    h.handle(&event(
        "preToolUse",
        110,
        json!({"tool_name":"Shell","tool_use_id":"late","tool_input":{"command":"slow"}}),
    ));
    h.handle(&event(
        "stop",
        120,
        json!({"generation_id":"t1","status":"aborted"}),
    ));
    h.handle(&event(
        "beforeSubmitPrompt",
        130,
        json!({"generation_id":"t2","prompt":"second"}),
    ));
    h.handle(&event(
        "afterAgentThought",
        140,
        json!({"generation_id":"t2","text":"second thinking"}),
    ));
    h.handle(&event(
        "postToolUse",
        150,
        json!({"tool_name":"Shell","tool_use_id":"late","tool_input":{"command":"slow"},"tool_output":"old result","duration":0}),
    ));
    h.handle(&event(
        "afterAgentResponse",
        160,
        json!({"generation_id":"t2","text":"second answer"}),
    ));
    h.handle(&event(
        "stop",
        170,
        json!({"generation_id":"t2","status":"completed"}),
    ));
    h.finish();
    assert_eq!(h.turns().len(), 2);
    assert_eq!(
        h.inserted(SpanType::Tool)[0].parent_span_ids,
        vec![h.turns()[0].span_id.clone()]
    );
    let second: Vec<_> = h
        .inserted(SpanType::Llm)
        .into_iter()
        .filter(|r| r.parent_span_ids == vec![h.turns()[1].span_id.clone()])
        .collect();
    assert_eq!(
        second.len(),
        1,
        "an old tool completion is not a second-turn model boundary"
    );
}

#[test]
fn real_successful_interactive_capture_reuses_stopped_turn_for_late_response() {
    let h = real_capture("interactive-multi-turn-success");
    assert_eq!(
        h.turns().len(),
        2,
        "native stop precedes response; response must enrich that turn"
    );
    let rows = h.rows();
    assert!(rows[&h.turns()[0].span_id]["output"]
        .as_str()
        .unwrap()
        .contains("audit-shell-ok"));
    assert_eq!(rows[&h.turns()[1].span_id]["output"], "AUDIT_RESUMED");
    for (turn, (input, output)) in h.turns().into_iter().zip([(27376, 174), (27866, 117)]) {
        assert_eq!(rows[&turn.span_id]["metadata"]["status"], "completed");
        assert_eq!(rows[&turn.span_id]["metrics"]["prompt_tokens"], input);
        assert_eq!(rows[&turn.span_id]["metrics"]["completion_tokens"], output);
    }
    for llm in h.inserted(SpanType::Llm) {
        assert!(h
            .turns()
            .iter()
            .any(|t| llm.parent_span_ids == vec![t.span_id.clone()]));
        assert!(llm.metrics.is_none(), "native totals are turn-scoped");
        let parent = &rows[&llm.parent_span_ids[0]];
        assert!(llm.end_ms.unwrap() <= parent["end_ms"].as_i64().unwrap());
    }
}
#[test]
fn response_after_stop_enriches_turn_and_history_without_opening_a_completion() {
    for observed_thought in [false, true] {
        let mut h = Harness::new("test-session");
        h.handle(&event(
            "beforeSubmitPrompt",
            100,
            json!({"generation_id":"t1","prompt":"first","transcript_path":null}),
        ));
        if observed_thought {
            h.handle(&event("afterAgentThought", 110, json!({"text":"thinking"})));
        }
        h.handle(&event(
            "stop",
            120,
            json!({"generation_id":"t1","status":"completed","transcript_path":null}),
        ));
        let before = serde_json::to_value(h.inserted(SpanType::Llm)).unwrap();
        let response = event(
            "afterAgentResponse",
            5000,
            json!({"generation_id":"t1","text":"late answer","input_tokens":20,"output_tokens":4}),
        );
        h.handle(&response);
        h.handle(&response);
        assert_eq!(
            serde_json::to_value(h.inserted(SpanType::Llm)).unwrap(),
            before
        );
        let first_id = h.turns()[0].span_id.clone();
        let rows = h.rows();
        assert_eq!(rows[&first_id]["end_ms"], 120);
        assert_eq!(rows[&first_id]["output"], "late answer");
        assert_eq!(rows[&first_id]["metrics"]["tokens"], 24);
        h.handle(&event(
            "beforeSubmitPrompt",
            6000,
            json!({"generation_id":"t2","prompt":"second"}),
        ));
        h.handle(&event(
            "afterAgentResponse",
            6010,
            json!({"generation_id":"t2","text":"second answer"}),
        ));
        h.finish();
        let llms = h.inserted(SpanType::Llm);
        assert_eq!(llms.len(), usize::from(observed_thought) + 1);
        let input = llms
            .last()
            .unwrap()
            .input
            .as_ref()
            .unwrap()
            .as_array()
            .unwrap();
        assert_eq!(
            input
                .iter()
                .filter(|message| **message == json!({"role":"assistant","content":"late answer"}))
                .count(),
            1
        );
    }
}

#[test]
fn transcript_first_seen_after_stop_enriches_closed_turn_without_a_completion() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("late.jsonl");
    let transcript = transcript_record("user", "hello")
        + &transcript_record("assistant", "fragment A")
        + &transcript_record("assistant", " + fragment B");
    std::fs::write(&path, &transcript).unwrap();
    let mut h = Harness::new("test-session");
    h.handle(&event(
        "beforeSubmitPrompt",
        100,
        json!({"generation_id":"t1","prompt":"hello","transcript_path":null}),
    ));
    h.handle(&event(
        "stop",
        120,
        json!({"generation_id":"t1","status":"completed"}),
    ));
    let response = mirrored(
        "afterAgentResponse",
        5000,
        &path,
        transcript.len() as u64,
        json!({"generation_id":"t1","text":"fragment A + fragment B"}),
    );
    h.handle(&response);
    h.handle(&response);
    assert!(h.inserted(SpanType::Llm).is_empty());
    let rows = h.rows();
    let turn = &rows[&h.turns()[0].span_id];
    assert_eq!(turn["end_ms"], 120);
    assert_eq!(turn["output"], "fragment A + fragment B");
    h.handle(&event(
        "beforeSubmitPrompt",
        6000,
        json!({"generation_id":"t2","prompt":"continue"}),
    ));
    h.handle(&event(
        "afterAgentResponse",
        6010,
        json!({"generation_id":"t2","text":"done"}),
    ));
    h.finish();
    let llms = h.inserted(SpanType::Llm);
    assert_eq!(llms.len(), 1);
    let prior_text = llms[0]
        .input
        .as_ref()
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .filter(|message| message["role"] == "assistant")
        .filter_map(|message| message["content"].as_str())
        .collect::<String>();
    assert_eq!(prior_text, "fragment A + fragment B");
}

#[test]
fn real_successful_headless_capture_correlates_tools_and_records_completion() {
    let h = real_capture("headless-tools-success");
    assert_eq!(h.turns().len(), 1);
    assert_eq!(h.inserted(SpanType::Tool).len(), 2);
    for row in h.inserted(SpanType::Tool) {
        assert_eq!(row.parent_span_ids, vec![h.turns()[0].span_id.clone()]);
        assert!(h.rows()[&row.span_id]["end_ms"].is_number());
    }
    assert_eq!(
        h.rows()[&h.turns()[0].span_id]["metadata"]["status"],
        "completed"
    );
}
#[test]
fn synthetic_specialized_native_call_id_wins_over_ambiguous_arguments() {
    let mut h = Harness::new("test-session");
    h.handle(&event(
        "beforeSubmitPrompt",
        100,
        json!({"generation_id":"t","prompt":"run twice"}),
    ));
    for (ts, call) in [(110, "shell-1"), (120, "shell-2")] {
        h.handle(&event(
            "preToolUse",
            ts,
            json!({"tool_name":"Shell","tool_use_id":call,"tool_input":{"command":"pwd"}}),
        ));
    }
    h.handle(&event(
        "afterShellExecution",
        130,
        json!({"tool_use_id":"shell-1","command":"pwd","output":"first output"}),
    ));
    h.handle(&event(
        "postToolUse",
        140,
        json!({"tool_name":"Shell","tool_use_id":"shell-1","tool_input":{"command":"pwd"},"tool_output":"generic metadata","duration":0}),
    ));
    h.finish();
    let target = h
        .inserted(SpanType::Tool)
        .into_iter()
        .find(|r| r.start_ms == Some(110))
        .unwrap();
    assert_eq!(
        h.rows()[&target.span_id]["output"],
        "first output",
        "native tool identity must outrank heuristic ambiguity"
    );
}
#[test]
fn synthetic_specialized_event_cannot_enrich_an_incompatible_tool_type() {
    let mut h = Harness::new("test-session");
    h.handle(&event(
        "beforeSubmitPrompt",
        100,
        json!({"generation_id":"t","prompt":"read a file"}),
    ));
    h.handle(&event(
        "preToolUse",
        110,
        json!({"tool_name":"Read","tool_use_id":"read-1","tool_input":{"file_path":"/tmp/a"}}),
    ));
    h.handle(&event(
        "afterShellExecution",
        120,
        json!({"tool_use_id":"read-1","command":"pwd","output":"wrong tool result"}),
    ));
    h.finish();
    let read = h
        .inserted(SpanType::Tool)
        .into_iter()
        .find(|r| r.name == "Read")
        .unwrap();
    assert_eq!(h.rows()[&read.span_id].get("output"), None);
}
#[test]
fn open_tool_limit_evicts_the_oldest_tool_not_the_smallest_id() {
    let mut h = Harness::new("test-session");
    h.handle(&event(
        "beforeSubmitPrompt",
        100,
        json!({"generation_id":"t","prompt":"many tools"}),
    ));
    for index in (0..=256).rev() {
        let id = format!("tool-{index:03}");
        h.handle(&event(
            "preToolUse",
            110 + (256 - index),
            json!({"tool_name":"Shell","tool_use_id":id,"tool_input":{"command":id}}),
        ));
    }
    let rows = h.rows();
    let tools = h.inserted(SpanType::Tool);
    let oldest = tools
        .iter()
        .find(|r| r.name == "Shell" && r.start_ms == Some(110))
        .unwrap();
    let newest = tools
        .iter()
        .find(|r| r.input == Some(json!({"command":"tool-000"})))
        .unwrap();
    assert_eq!(rows[&oldest.span_id]["metadata"]["status"], "incomplete");
    assert_eq!(rows[&newest.span_id]["metadata"].get("status"), None);
}
#[test]
fn synthetic_usage_without_verified_prompt_generation_stays_unavailable() {
    let mut h = Harness::new("test-session");
    h.handle(&event(
        "afterAgentResponse",
        100,
        json!({"generation_id":"request-id","text":"hi","input_tokens":40,"output_tokens":2}),
    ));
    h.handle(&event("stop", 110, json!({"generation_id":"request-id","status":"completed","input_tokens":40,"output_tokens":2})));
    h.finish();
    assert!(h.ops.iter().all(|op| match op {
        SpanOp::Insert(r) | SpanOp::Merge(r) => r.metrics.is_none(),
    }));
}

#[test]
fn synthetic_subagents_parent_to_spawning_turn_and_unmatched_stop_stays_incomplete() {
    let mut h = Harness::new("test-session");
    h.handle(&event(
        "beforeSubmitPrompt",
        100,
        json!({"generation_id":"t","prompt":"delegate"}),
    ));
    h.handle(&event(
        "preToolUse",
        110,
        json!({"tool_name":"Task","tool_use_id":"spawn","tool_input":{"task":"explore"}}),
    ));
    for (ts, id) in [(120, "child-a"), (130, "child-b")] {
        h.handle(&event("subagentStart", ts, json!({"subagent_id":id,"tool_call_id":"spawn","subagent_type":"explore","subagent_model":"auto","task":"explore"})));
    }
    h.handle(&event(
        "subagentStop",
        140,
        json!({"subagent_id":"child-a","subagent_type":"explore","status":"completed","summary":"available summary"}),
    ));
    h.handle(&event(
        "sessionEnd",
        160,
        json!({"reason":"completed","final_status":"completed"}),
    ));
    h.finish();
    let children: Vec<_> = h
        .inserted(SpanType::Task)
        .into_iter()
        .filter(|r| r.name == "Cursor subagent")
        .collect();
    assert_eq!(children.len(), 2);
    let rows = h.rows();
    for child in children {
        assert_eq!(child.parent_span_ids, vec![h.turns()[0].span_id.clone()]);
        assert!(child
            .metadata
            .as_ref()
            .unwrap()
            .get("spawning_tool_call_id")
            .is_none());
        assert_eq!(
            child.metadata.as_ref().unwrap()["recursive_activity_verified"],
            false
        );
        let row = &rows[&child.span_id];
        if child.start_ms == Some(120) {
            assert_eq!(row["output"], "available summary");
            assert_eq!(row["metadata"]["status"], "completed");
        } else {
            assert!(row.get("output").is_none());
            assert_eq!(row["metadata"]["status"], "incomplete");
        }
    }
    assert_eq!(
        rows[&h.inserted(SpanType::Tool)[0].span_id]["metadata"]["status"],
        "incomplete"
    );
}

#[test]
fn terminal_only_subagent_stop_synthesizes_estimated_span() {
    let mut h = Harness::new("test-session");
    h.handle(&event(
        "beforeSubmitPrompt",
        100,
        json!({"generation_id":"t","prompt":"delegate"}),
    ));
    h.handle(&event(
        "subagentStop",
        140,
        json!({
            "subagent_id":"child-terminal-only",
            "status":"completed",
            "summary":"finished work",
            "subagent_type":"explore"
        }),
    ));
    h.finish();

    let child = h
        .inserted(SpanType::Task)
        .into_iter()
        .find(|row| row.name == "Cursor subagent")
        .unwrap();
    assert_eq!(child.start_ms, Some(140));
    assert_eq!(child.end_ms, Some(140));
    assert_eq!(child.output, Some(json!("finished work")));
    let metadata = child.metadata.as_ref().unwrap();
    assert_eq!(metadata["start_time_estimated"], true);
    assert_eq!(metadata["result_completeness"], "terminal_observation_only");
    assert_eq!(metadata["status"], "completed");
    assert_eq!(metadata["subagent_type"], "explore");
    // subagentStop does not report the subagent's model.
    assert!(metadata.get("model").is_none());
    assert_eq!(child.parent_span_ids, vec![h.turns()[0].span_id.clone()]);
}
#[test]
fn synthetic_compaction_is_observation_only_and_clears_unavailable_context() {
    let mut h = Harness::new("test-session");
    h.handle(&event(
        "beforeSubmitPrompt",
        100,
        json!({"generation_id":"t","prompt":"stale context"}),
    ));
    h.handle(&event(
        "afterAgentThought",
        110,
        json!({"generation_id":"t","text":"before"}),
    ));
    h.handle(&event("preCompact",120,json!({"trigger":"manual","context_tokens":120000,"context_usage_percent":90,"context_window_size":128000,"message_count":20,"messages_to_compact":10})));
    h.handle(&event(
        "afterAgentResponse",
        130,
        json!({"generation_id":"t","text":"after"}),
    ));
    h.handle(&event(
        "stop",
        140,
        json!({"generation_id":"t","status":"completed"}),
    ));
    h.finish();
    let observation = h
        .inserted(SpanType::Task)
        .into_iter()
        .find(|r| r.name == "Cursor compaction requested")
        .unwrap();
    assert_eq!(observation.input.as_ref().unwrap()["trigger"], "manual");
    assert_eq!(
        observation.input.as_ref().unwrap()["context_tokens"],
        120000
    );
    assert_eq!(
        observation.metadata.as_ref().unwrap()["completion_observed"],
        false
    );
    assert_eq!(
        observation.metadata.as_ref().unwrap()["replacement_context_available"],
        false
    );
    assert!(observation.output.is_none());
    let after = h
        .inserted(SpanType::Llm)
        .into_iter()
        .find(|r| r.start_ms == Some(120))
        .unwrap();
    assert_eq!(after.input.as_ref().unwrap(), &json!([]));
    assert_eq!(after.metadata.as_ref().unwrap()["history_truncated"], true);
}
#[test]
fn synthetic_identical_thought_payload_in_later_turn_is_distinct_evidence() {
    let mut h = Harness::new("test-session");
    for (ts, generation, prompt) in [(100, "t1", "first"), (200, "t2", "second")] {
        h.handle(&event(
            "beforeSubmitPrompt",
            ts,
            json!({"generation_id":generation,"prompt":prompt}),
        ));
        h.handle(&event(
            "afterAgentThought",
            ts + 10,
            json!({"generation_id":"session-native-drift","text":"identical thought"}),
        ));
        h.handle(&event(
            "stop",
            ts + 20,
            json!({"generation_id":generation,"status":"completed"}),
        ));
    }
    h.finish();
    assert_eq!(h.turns().len(), 2);
    assert_eq!(h.inserted(SpanType::Llm).len(), 2);
    for llm in h.inserted(SpanType::Llm) {
        assert!(llm
            .output
            .as_ref()
            .unwrap()
            .to_string()
            .contains("identical thought"));
    }
}
#[test]
fn synthetic_late_old_turn_response_cannot_end_current_turn_model() {
    let mut h = Harness::new("test-session");
    h.handle(&event(
        "beforeSubmitPrompt",
        100,
        json!({"generation_id":"t1","prompt":"first"}),
    ));
    h.handle(&event(
        "stop",
        110,
        json!({"generation_id":"t1","status":"completed"}),
    ));
    h.handle(&event(
        "beforeSubmitPrompt",
        120,
        json!({"generation_id":"t2","prompt":"second"}),
    ));
    h.handle(&event(
        "afterAgentThought",
        130,
        json!({"generation_id":"t2","text":"second thinking"}),
    ));
    h.handle(&event(
        "afterAgentResponse",
        140,
        json!({"generation_id":"t1","text":"first answer","input_tokens":20,"output_tokens":2}),
    ));
    assert!(h.inserted(SpanType::Llm).is_empty());
    h.handle(&event(
        "preToolUse",
        145,
        json!({"generation_id":"t2","tool_name":"Read","tool_use_id":"r1","tool_input":{"file_path":"/tmp/a"}}),
    ));
    h.handle(&event(
        "postToolUse",
        146,
        json!({"generation_id":"t2","tool_name":"Read","tool_use_id":"r1","tool_input":{"file_path":"/tmp/a"},"tool_output":"file content","duration":0}),
    ));
    h.handle(&event(
        "afterAgentResponse",
        150,
        json!({"generation_id":"t2","text":"second answer"}),
    ));
    h.handle(&event(
        "stop",
        160,
        json!({"generation_id":"t2","status":"completed"}),
    ));
    h.finish();
    assert_eq!(h.turns().len(), 2);
    let rows = h.rows();
    assert_eq!(rows[&h.turns()[0].span_id]["output"], "first answer");
    assert_eq!(rows[&h.turns()[1].span_id]["output"], "second answer");
    let second: Vec<_> = h
        .inserted(SpanType::Llm)
        .into_iter()
        .filter(|r| r.parent_span_ids == vec![h.turns()[1].span_id.clone()])
        .collect();
    assert_eq!(second.len(), 2);
    assert_eq!(second[0].end_ms, Some(145));
    assert_eq!(second[1].start_ms, Some(146));
    for completion in &second {
        assert_eq!(
            completion.metadata.as_ref().unwrap()["history_truncated"],
            true
        );
        assert!(!completion
            .input
            .as_ref()
            .unwrap()
            .to_string()
            .contains("first answer"));
    }
    assert!(second[0]
        .output
        .as_ref()
        .unwrap()
        .to_string()
        .contains("second thinking"));
    assert!(second[1]
        .output
        .as_ref()
        .unwrap()
        .to_string()
        .contains("second answer"));
}
#[test]
fn real_native_capture_replay_is_deterministic() {
    let first = real_capture("interactive-multi-turn-success");
    let replay = real_capture("interactive-multi-turn-success");
    assert_eq!(
        serde_json::to_value(first.ops).unwrap(),
        serde_json::to_value(replay.ops).unwrap()
    );
}

fn assert_transcript_fragments_and_aggregate_emit_once(response_after_stop: bool) {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("fragments.jsonl");
    let records = [
        json!({"role":"user","message":{"content":[{"type":"text","text":"hello"}]}}),
        json!({"role":"assistant","message":{"content":[{"type":"text","text":"fragment A"}]}}),
        json!({"role":"assistant","message":{"content":[{"type":"text","text":" + fragment B"}]}}),
    ];
    let transcript = records.iter().map(|r| format!("{r}\n")).collect::<String>();
    std::fs::write(&path, &transcript).unwrap();
    let through = transcript.len() as u64;
    let mut h = Harness::new("test-session");
    h.handle(&event(
        "beforeSubmitPrompt",
        100,
        json!({"generation_id":"t","prompt":"hello"}),
    ));
    let response = json!({"generation_id":"t","text":"fragment A + fragment B","input_tokens":20,"output_tokens":4});
    let stop =
        json!({"generation_id":"t","status":"completed","input_tokens":20,"output_tokens":4});
    if response_after_stop {
        h.handle(&mirrored("stop", 110, &path, through, stop));
        h.handle(&mirrored(
            "afterAgentResponse",
            120,
            &path,
            through,
            response,
        ));
    } else {
        h.handle(&mirrored(
            "afterAgentResponse",
            110,
            &path,
            through,
            response,
        ));
        h.handle(&mirrored("stop", 120, &path, through, stop));
    }
    h.finish();
    assert_eq!(h.turns().len(), 1);
    let llms = h.inserted(SpanType::Llm);
    assert_eq!(
        llms.len(),
        1,
        "an aggregate native response enriches its existing model output"
    );
    let emitted = llms
        .iter()
        .filter_map(|r| r.output.as_ref().unwrap()[0]["message"]["content"].as_str())
        .collect::<String>();
    assert_eq!(
        emitted, "fragment A + fragment B",
        "native aggregate output must not repeat mirrored fragments"
    );
    let rows = h.rows();
    assert_eq!(
        rows[&h.turns()[0].span_id]["output"],
        "fragment A + fragment B"
    );
    assert_eq!(rows[&h.turns()[0].span_id]["metrics"]["prompt_tokens"], 20);
}
#[test]
fn synthetic_transcript_fragments_and_native_aggregate_emit_once_in_either_order() {
    for response_after_stop in [false, true] {
        assert_transcript_fragments_and_aggregate_emit_once(response_after_stop);
    }
}
#[test]
fn synthetic_late_old_turn_stop_cannot_close_current_model_or_turn() {
    let mut h = Harness::new("test-session");
    h.handle(&event(
        "beforeSubmitPrompt",
        100,
        json!({"generation_id":"t1","prompt":"first"}),
    ));
    h.handle(&event(
        "stop",
        110,
        json!({"generation_id":"t1","status":"completed"}),
    ));
    h.handle(&event(
        "beforeSubmitPrompt",
        120,
        json!({"generation_id":"t2","prompt":"second"}),
    ));
    h.handle(&event(
        "afterAgentThought",
        130,
        json!({"generation_id":"t2","text":"second thinking"}),
    ));
    h.handle(&event(
        "stop",
        140,
        json!({"generation_id":"t1","status":"aborted","input_tokens":10,"output_tokens":1}),
    ));
    h.handle(&event(
        "afterAgentResponse",
        150,
        json!({"generation_id":"t2","text":"second answer"}),
    ));
    h.handle(&event(
        "stop",
        160,
        json!({"generation_id":"t2","status":"completed"}),
    ));
    h.finish();
    assert_eq!(h.turns().len(), 2);
    let rows = h.rows();
    assert_eq!(rows[&h.turns()[0].span_id]["error"], "aborted");
    let current = &rows[&h.turns()[1].span_id];
    assert_eq!(current["metadata"]["status"], "completed");
    assert_eq!(current["end_ms"], 160);
    assert!(current.get("error").is_none());
    assert!(
        current.get("metrics").is_none(),
        "old-turn counts cannot migrate to the current turn"
    );
    let second: Vec<_> = h
        .inserted(SpanType::Llm)
        .into_iter()
        .filter(|r| r.parent_span_ids == vec![h.turns()[1].span_id.clone()])
        .collect();
    assert_eq!(
        second.len(),
        1,
        "a stop for another native generation is not a model boundary"
    );
    let output = second[0].output.as_ref().unwrap().to_string();
    assert!(output.contains("second thinking") && output.contains("second answer"));
}

#[test]
fn real_daemon_two_turn_capture_correlates_three_tools_and_keeps_native_usage_totals() {
    let h = real_capture("daemon-two-turn-success");
    let roots: Vec<_> = h
        .inserted(SpanType::Task)
        .into_iter()
        .filter(|r| r.name == "Cursor session")
        .collect();
    assert_eq!(
        roots.len(),
        1,
        "prompt-before-sessionStart still belongs to one logical session"
    );
    assert_eq!(h.turns().len(), 2);
    assert_eq!(h.inserted(SpanType::Tool).len(), 3);
    let rows = h.rows();
    for (turn, (input, output, cached)) in h
        .turns()
        .into_iter()
        .zip([(27377, 178, 17408), (27874, 119, 27520)])
    {
        let row = &rows[&turn.span_id];
        assert!(row["metadata"].get("generation_id").is_none());
        assert!(row["metadata"].get("turn_boundary_source").is_none());
        assert!(row["metadata"].get("start_time_estimated").is_none());
        assert_eq!(row["metadata"]["token_usage_scope"], "turn");
        assert_eq!(row["metadata"]["status"], "completed");
        assert_eq!(row["metrics"]["prompt_tokens"], input);
        assert_eq!(row["metrics"]["completion_tokens"], output);
        assert_eq!(row["metrics"]["tokens"], input + output);
        assert_eq!(row["metrics"]["prompt_cached_tokens"], cached);
        assert!(row.get("error").is_none());
    }
    for (index, tool) in h.inserted(SpanType::Tool).into_iter().enumerate() {
        let owner = if index == 2 {
            h.turns()[1]
        } else {
            h.turns()[0]
        };
        assert_eq!(tool.parent_span_ids, vec![owner.span_id.clone()]);
        assert!(tool
            .metadata
            .as_ref()
            .unwrap()
            .get("generation_id")
            .is_none());
        assert!(rows[&tool.span_id]["metadata"]
            .get("turn_attribution")
            .is_none());
        assert_eq!(rows[&tool.span_id]["metadata"]["status"], "completed");
    }
    assert_eq!(rows[&h.turns()[1].span_id]["output"], "AUDIT_RESUMED");
    assert!(h
        .inserted(SpanType::Llm)
        .iter()
        .all(|r| r.metrics.is_none()));
}
#[test]
fn real_default_plugin_headless_resume_retains_session_and_observed_turn_boundaries() {
    let h = real_capture("plugin-headless-resume-success");
    let roots: Vec<_> = h
        .inserted(SpanType::Task)
        .into_iter()
        .filter(|r| r.name == "Cursor session")
        .collect();
    assert_eq!(roots.len(), 1);
    assert_eq!(
        h.turns().len(),
        2,
        "tool activity after a native sessionEnd is a resumed observed turn"
    );
    assert_eq!(h.inserted(SpanType::Tool).len(), 3);
    let rows = h.rows();
    assert_eq!(rows[&roots[0].span_id]["metadata"]["status"], "completed");
    for turn in h.turns() {
        let row = &rows[&turn.span_id];
        assert!(row["metadata"].get("turn_boundary_source").is_none());
        assert_eq!(row["metadata"]["status"], "completed");
        assert_eq!(row["metadata"]["start_time_estimated"], true);
        assert!(
            row.get("input").is_none(),
            "headless capture supplied no prompt hook or mirrored transcript"
        );
        assert!(
            row.get("metrics").is_none(),
            "headless hooks supplied no native usage totals"
        );
    }
    for (index, tool) in h.inserted(SpanType::Tool).into_iter().enumerate() {
        let owner = if index == 2 {
            h.turns()[1]
        } else {
            h.turns()[0]
        };
        assert_eq!(tool.parent_span_ids, vec![owner.span_id.clone()]);
        assert_eq!(rows[&tool.span_id]["metadata"]["status"], "completed");
    }
    assert!(h.ops.iter().all(|op| match op {
        SpanOp::Insert(r) | SpanOp::Merge(r) => r.metrics.is_none(),
    }));
}

#[test]
fn synthetic_distinct_native_prompt_generations_separate_identical_prompts() {
    let mut h = Harness::new("identical-prompts");
    h.handle(&event(
        "beforeSubmitPrompt",
        100,
        json!({"prompt":"try again","generation_id":"t1"}),
    ));
    h.handle(&event(
        "beforeSubmitPrompt",
        200,
        json!({"prompt":"try again","generation_id":"t2"}),
    ));
    h.handle(&event(
        "stop",
        300,
        json!({"generation_id":"t2","status":"completed"}),
    ));
    h.finish();
    assert_eq!(h.turns().len(), 2);
    assert_ne!(h.turns()[0].span_id, h.turns()[1].span_id);
    let rows = h.rows();
    assert_eq!(
        rows[&h.turns()[0].span_id]["metadata"]["status"],
        "superseded"
    );
    assert_eq!(
        rows[&h.turns()[1].span_id]["metadata"]["status"],
        "completed"
    );
}

fn transcript_record(role: &str, text: &str) -> String {
    format!(
        "{}\n",
        json!({"role":role,"message":{"content":[{"type":"text","text":text}]}})
    )
}
#[test]
fn synthetic_truncated_mirror_after_stop_maps_its_first_user_record_to_native_turn() {
    let temporary = tempfile::tempdir().unwrap();
    let mirror = temporary.path().join("generation-2.jsonl");
    let second = transcript_record("user", "second prompt")
        + &transcript_record("assistant", "second answer");
    std::fs::write(&mirror, &second).unwrap();
    let mut h = Harness::new("test-session");
    h.handle(&event(
        "beforeSubmitPrompt",
        100,
        json!({"generation_id":"t1","prompt":"first prompt"}),
    ));
    h.handle(&event(
        "stop",
        110,
        json!({"generation_id":"t1","status":"completed"}),
    ));
    h.handle(&event(
        "beforeSubmitPrompt",
        120,
        json!({"generation_id":"t2","prompt":"second prompt"}),
    ));
    h.handle(&event(
        "stop",
        130,
        json!({"generation_id":"t2","status":"completed"}),
    ));
    // The truncated mirror is first observed after the owning native turn has
    // already stopped; its physical first-user ordinal is not native ordinal 1.
    h.handle(&mirrored(
        "sessionEnd",
        140,
        &mirror,
        second.len() as u64,
        json!({"reason":"completed","final_status":"completed"}),
    ));
    h.finish();
    assert_eq!(
        h.turns().len(),
        2,
        "physical user ordinal one is not a stable native turn identity after truncation"
    );
    let rows = h.rows();
    assert!(rows[&h.turns()[0].span_id].get("output").is_none());
    assert_eq!(rows[&h.turns()[1].span_id]["output"], "second answer");
    assert!(h.inserted(SpanType::Llm).is_empty());
}
#[test]
fn ambiguous_rewritten_prompt_does_not_overwrite_either_identical_native_turn() {
    let temporary = tempfile::tempdir().unwrap();
    let mirror = temporary.path().join("rewritten.jsonl");
    let transcript = transcript_record("user", "try again")
        + &transcript_record("assistant", "rewritten answer");
    std::fs::write(&mirror, &transcript).unwrap();
    let mut h = Harness::new("test-session");
    for (generation, start, answer) in [("t1", 100, "answer one"), ("t2", 200, "answer two")] {
        h.handle(&event(
            "beforeSubmitPrompt",
            start,
            json!({"generation_id":generation,"prompt":"try again"}),
        ));
        h.handle(&event(
            "afterAgentResponse",
            start + 10,
            json!({"generation_id":generation,"text":answer}),
        ));
        h.handle(&event(
            "stop",
            start + 20,
            json!({"generation_id":generation,"status":"completed"}),
        ));
    }
    h.handle(&mirrored(
        "sessionEnd",
        300,
        &mirror,
        transcript.len() as u64,
        json!({"reason":"completed","final_status":"completed"}),
    ));
    h.finish();
    let rows = h.rows();
    let turns = h.turns();
    assert_eq!(turns.len(), 2);
    assert_eq!(rows[&turns[0].span_id]["output"], "answer one");
    assert_eq!(rows[&turns[1].span_id]["output"], "answer two");
}
#[test]
fn synthetic_rewrite_removing_terminal_marker_does_not_reemit_shifted_assistant_records() {
    let temporary = tempfile::tempdir().unwrap();
    let original_mirror = temporary.path().join("original.jsonl");
    let rewritten_mirror = temporary.path().join("rewritten.jsonl");
    let first =
        transcript_record("user", "first prompt") + &transcript_record("assistant", "first answer");
    let terminal = format!("{}\n", json!({"type":"turn_ended","status":"completed"}));
    let second = transcript_record("user", "second prompt")
        + &transcript_record("assistant", "second answer");
    let original = first.clone() + &terminal + &second;
    let rewritten = first.clone() + &second;
    std::fs::write(&original_mirror, &original).unwrap();
    std::fs::write(&rewritten_mirror, &rewritten).unwrap();
    let mut h = Harness::new("test-session");
    h.handle(&event(
        "beforeSubmitPrompt",
        100,
        json!({"generation_id":"t1","prompt":"first prompt"}),
    ));
    h.handle(&mirrored(
        "stop",
        110,
        &original_mirror,
        (first.len() + terminal.len()) as u64,
        json!({"generation_id":"t1","status":"completed"}),
    ));
    h.handle(&event(
        "beforeSubmitPrompt",
        120,
        json!({"generation_id":"t2","prompt":"second prompt"}),
    ));
    h.handle(&mirrored(
        "stop",
        130,
        &original_mirror,
        original.len() as u64,
        json!({"generation_id":"t2","status":"completed"}),
    ));
    h.handle(&mirrored(
        "sessionEnd",
        140,
        &rewritten_mirror,
        rewritten.len() as u64,
        json!({"reason":"completed","final_status":"completed"}),
    ));
    h.finish();
    assert_eq!(h.turns().len(), 2);
    let llms = h.inserted(SpanType::Llm);
    assert_eq!(
        llms.len(),
        2,
        "rewriting record positions cannot invent another model operation"
    );
    let emitted = llms
        .iter()
        .filter_map(|r| r.output.as_ref().unwrap()[0]["message"]["content"].as_str())
        .collect::<String>();
    assert_eq!(emitted, "first answersecond answer");
    let rows = h.rows();
    assert_eq!(rows[&h.turns()[0].span_id]["output"], "first answer");
    assert_eq!(rows[&h.turns()[1].span_id]["output"], "second answer");
}

#[test]
fn synthetic_partial_usage_merges_known_counts_without_inventing_missing_values() {
    let mut h = Harness::new("test-session");
    h.handle(&event(
        "beforeSubmitPrompt",
        100,
        json!({
            "generation_id":"t", "prompt":"hello", "model":"composer-2.5"
        }),
    ));
    h.handle(&event(
        "afterAgentResponse",
        110,
        json!({
            "generation_id":"t", "text":"hello", "input_tokens":20,
            "cache_read_tokens":7, "model":"composer-2.5"
        }),
    ));
    let id = h.turns()[0].span_id.clone();
    let rows = h.rows();
    assert!(rows[&id]["metrics"].get("completion_tokens").is_none());
    assert!(rows[&id]["metrics"].get("tokens").is_none());
    assert!(rows[&id]["metrics"]
        .get("prompt_cache_creation_tokens")
        .is_none());
    let stop = event(
        "stop",
        120,
        json!({
            "generation_id":"t", "status":"completed", "output_tokens":4,
            "model":"composer-2.5"
        }),
    );
    h.handle(&stop);
    h.handle(&stop);
    h.finish();
    let rows = h.rows();
    assert_eq!(
        rows[&id]["metrics"],
        json!({
            "prompt_tokens":20, "completion_tokens":4, "tokens":24, "prompt_cached_tokens":7
        })
    );
    let llm = h.inserted(SpanType::Llm)[0];
    assert!(llm.metrics.is_none());
    assert!(llm.metadata.as_ref().unwrap().get("provider").is_none());
    assert_eq!(llm.metadata.as_ref().unwrap()["model"], "composer-2.5");
}

#[test]
fn synthetic_failed_tool_without_result_does_not_invent_an_empty_model_input_message() {
    let mut h = Harness::new("test-session");
    h.handle(&event(
        "beforeSubmitPrompt",
        100,
        json!({
            "generation_id":"t", "prompt":"read"
        }),
    ));
    h.handle(&event(
        "preToolUse",
        110,
        json!({
            "tool_use_id":"r", "tool_name":"Read", "tool_input":{"file_path":"/tmp/a"}
        }),
    ));
    h.handle(&event(
        "postToolUseFailure",
        120,
        json!({
            "tool_use_id":"r", "tool_name":"Read", "tool_input":{"file_path":"/tmp/a"},
            "error_message":"read failed", "failure_type":"error", "is_interrupt":false,
            "duration":5
        }),
    ));
    h.handle(&event(
        "afterAgentResponse",
        130,
        json!({"generation_id":"t", "text":"done"}),
    ));
    h.finish();
    let llms = h.inserted(SpanType::Llm);
    assert_eq!(llms.len(), 2);
    let input = llms[1].input.as_ref().unwrap().as_array().unwrap();
    assert!(input.iter().all(|message| message["role"] != "tool"));
    assert_eq!(
        llms[1].metadata.as_ref().unwrap()["history_truncated"],
        true
    );
}

#[test]
fn malformed_hooks_fail_the_event_instead_of_dropping_fields() {
    let ctx = SessionCtx {
        session_id: "test-session".into(),
        config: None,
    };
    for (kind, payload) in [
        (
            "afterAgentResponse",
            json!({"generation_id":"t", "text":"hello", "input_tokens":-1}),
        ),
        (
            "preToolUse",
            json!({"tool_name":5, "tool_use_id":"call-1", "tool_input":{}}),
        ),
        ("stop", json!({"status":{"code":"completed"}})),
        ("sessionStart", json!({"workspace_roots":"/not/a/list"})),
        // Fields the CLI always sends are required.
        ("beforeSubmitPrompt", json!({})),
        ("preToolUse", json!({"tool_name":"Shell", "tool_input":{}})),
        ("sessionEnd", json!({"reason":"completed"})),
        (
            "subagentStop",
            json!({"subagent_type":"explore", "status":"completed"}),
        ),
    ] {
        let mut translator = Registry::default_agents().create("cursor", "test-session");
        let error = translator
            .handle(&event(kind, 100, payload), &ctx)
            .unwrap_err()
            .to_string();
        assert!(
            error.starts_with(&format!("unexpected cursor {kind} hook format"))
                || error.starts_with(&format!("unsupported shape for {kind}")),
            "{error}"
        );
    }
}

#[test]
fn unexpected_transcript_record_types_fail_the_event() {
    let dir = tempfile::tempdir().unwrap();
    let transcript = dir.path().join("transcript.jsonl");
    std::fs::write(
        &transcript,
        format!("{}\n", json!({"role":"user","message":{"content":42}})),
    )
    .unwrap();
    let through = std::fs::metadata(&transcript).unwrap().len();
    let ctx = SessionCtx {
        session_id: "test-session".into(),
        config: None,
    };
    let mut translator = Registry::default_agents().create("cursor", "test-session");
    let error = translator
        .handle(
            &mirrored(
                "beforeSubmitPrompt",
                100,
                &transcript,
                through,
                json!({"prompt":"hi"}),
            ),
            &ctx,
        )
        .unwrap_err()
        .to_string();
    assert!(
        error.starts_with("unexpected cursor transcript record format"),
        "{error}"
    );
}
