use bt_daemon::wire::Envelope;
use bt_daemon::{Registry, SessionCtx, SpanOp, SpanRow, SpanType};
use serde_json::{json, Value};
use std::collections::HashMap;

fn jsonl(records: &[Value]) -> (String, Vec<u64>) {
    let mut contents = String::new();
    let mut boundaries = Vec::new();
    for record in records {
        contents.push_str(&serde_json::to_string(record).unwrap());
        contents.push('\n');
        boundaries.push(contents.len() as u64);
    }
    (contents, boundaries)
}

fn event(
    name: &str,
    ts_ms: i64,
    transcript_path: &str,
    transcript: &str,
    through: u64,
    extra: Value,
) -> Envelope {
    let full_path = transcript_path.replace("transcript.jsonl", "transcript_full.jsonl");
    let mut payload = json!({
        "conversationId": "conversation-1",
        "workspacePaths": ["/workspace/demo"],
        "transcriptPath": transcript_path,
        "artifactDirectoryPath": "/tmp/artifacts",
        "modelName": "gemini-3.1-pro",
        "_bt_transcript_observation": {
            "path": transcript_path,
            "observed_bytes": through,
            "full_path": full_path,
            "full_observed_bytes": through
        },
        "_bt_transcript_snapshot": {
            "path": full_path,
            "contents": transcript
        }
    });
    if let (Value::Object(payload), Value::Object(extra)) = (&mut payload, extra) {
        payload.extend(extra);
    }
    Envelope {
        source: "antigravity".into(),
        source_version: Some("1.1.12".into()),
        plugin_version: None,
        session_id: "conversation-1".into(),
        event: name.into(),
        ts_ms,
        payload,
        route: None,
        config: None,
        managed_run_id: None,
        capture: None,
    }
}

fn reduce(ops: Vec<SpanOp>) -> HashMap<String, SpanRow> {
    let mut rows: HashMap<String, SpanRow> = HashMap::new();
    for op in ops {
        match op {
            SpanOp::Insert(row) => {
                rows.insert(row.span_id.clone(), row);
            }
            SpanOp::Merge(row) => {
                let existing = rows.entry(row.span_id.clone()).or_default();
                if !row.root_span_id.is_empty() {
                    existing.root_span_id = row.root_span_id;
                }
                if !row.parent_span_ids.is_empty() {
                    existing.parent_span_ids = row.parent_span_ids;
                }
                if !row.name.is_empty() {
                    existing.name = row.name;
                }
                if row.start_ms.is_some() {
                    existing.start_ms = row.start_ms;
                }
                if row.end_ms.is_some() {
                    existing.end_ms = row.end_ms;
                }
                if row.input.is_some() {
                    existing.input = row.input;
                }
                if row.output.is_some() {
                    existing.output = row.output;
                }
                if row.metrics.is_some() {
                    existing.metrics = row.metrics;
                }
                if row.error.is_some() {
                    existing.error = row.error;
                }
                if let Some(Value::Object(incoming)) = row.metadata {
                    let mut metadata = existing
                        .metadata
                        .take()
                        .and_then(|value| value.as_object().cloned())
                        .unwrap_or_default();
                    metadata.extend(incoming);
                    existing.metadata = Some(Value::Object(metadata));
                }
            }
        }
    }
    rows
}

#[test]
fn hooks_and_full_transcript_build_model_and_tool_spans() {
    let records = vec![
        json!({
            "step_index": 0,
            "source": "USER_EXPLICIT",
            "type": "USER_INPUT",
            "status": "COMPLETED",
            "content": "List the files"
        }),
        json!({
            "step_index": 1,
            "source": "MODEL",
            "type": "PLANNER_RESPONSE",
            "status": "COMPLETED",
            "content": "I'll inspect the directory.",
            "tool_calls": [{"name":"run_command","args":{"CommandLine":"ls"}}],
            "usage": {"inputTokens": 12, "outputTokens": 7, "totalTokens": 19}
        }),
        json!({
            "step_index": 2,
            "source": "MODEL",
            "type": "CORTEX_STEP_TYPE_RUN_COMMAND",
            "status": "COMPLETED",
            "content": "README.md\nsrc",
            "tool_calls": [{
                "name": "run_command",
                "args": {"CommandLine":"ls"},
                "result": "README.md\nsrc"
            }]
        }),
    ];
    let (transcript, boundary) = jsonl(&records);
    let path = "/tmp/conversation/transcript.jsonl";
    let registry = Registry::default_agents();
    let mut translator = registry.create("antigravity", "conversation-1");
    let ctx = SessionCtx {
        session_id: "conversation-1".into(),
        config: None,
    };
    let mut ops = Vec::new();
    ops.extend(
        translator
            .handle(
                &event(
                    "PreInvocation",
                    100,
                    path,
                    &transcript,
                    boundary[0],
                    json!({"invocationNum":0,"initialNumSteps":1}),
                ),
                &ctx,
            )
            .unwrap(),
    );
    ops.extend(
        translator
            .handle(
                &event(
                    "PostInvocation",
                    200,
                    path,
                    &transcript,
                    boundary[1],
                    json!({"invocationNum":0,"initialNumSteps":1}),
                ),
                &ctx,
            )
            .unwrap(),
    );
    // The safe default plugin uses PostToolUse only: transcript stepIdx
    // recovery provides the tool name, arguments, and output without changing
    // Antigravity's permission behavior via a PreToolUse decision.
    ops.extend(
        translator
            .handle(
                &event(
                    "PostToolUse",
                    300,
                    path,
                    &transcript,
                    boundary[2],
                    json!({"stepIdx":2,"error":""}),
                ),
                &ctx,
            )
            .unwrap(),
    );
    ops.extend(
        translator
            .handle(
                &event(
                    "Stop",
                    400,
                    path,
                    &transcript,
                    boundary[2],
                    json!({
                        "executionNum": 0,
                        "terminationReason": "model_stop",
                        "error": "",
                        "fullyIdle": true
                    }),
                ),
                &ctx,
            )
            .unwrap(),
    );

    let rows = reduce(ops);
    let root = rows
        .values()
        .find(|row| row.name == "Antigravity: demo")
        .unwrap();
    assert_eq!(root.metadata.as_ref().unwrap()["source"], "antigravity");
    assert!(root.end_ms.is_some());

    let turn = rows
        .values()
        .find(|row| row.span_type == SpanType::Task && row.name == "Turn 1")
        .unwrap();
    assert_eq!(turn.input, Some(json!("List the files")));
    assert_eq!(turn.output, Some(json!("I'll inspect the directory.")));
    assert_eq!(turn.parent_span_ids, vec![root.span_id.clone()]);

    let llm = rows
        .values()
        .find(|row| row.span_type == SpanType::Llm)
        .unwrap();
    assert_eq!(llm.name, "gemini-3.1-pro");
    assert_eq!(llm.parent_span_ids, vec![turn.span_id.clone()]);
    assert_eq!(llm.input.as_ref().unwrap()[0]["role"], "user");
    assert_eq!(llm.output.as_ref().unwrap()[0]["role"], "assistant");
    assert_eq!(llm.metrics.as_ref().unwrap()["prompt_tokens"], 12.0);
    assert_eq!(llm.metrics.as_ref().unwrap()["completion_tokens"], 7.0);
    assert_eq!(llm.metrics.as_ref().unwrap()["tokens"], 19.0);

    let tool = rows
        .values()
        .find(|row| row.span_type == SpanType::Tool)
        .unwrap();
    assert_eq!(tool.name, "run_command");
    assert_eq!(tool.input.as_ref().unwrap()["CommandLine"], "ls");
    assert_eq!(tool.output, Some(json!("README.md\nsrc")));
    assert_eq!(tool.metadata.as_ref().unwrap()["tool_outcome"], "success");
    assert_eq!(tool.parent_span_ids, vec![turn.span_id.clone()]);
}

#[test]
fn pre_tool_pair_preserves_start_time_and_reports_failure() {
    let records = vec![json!({
        "step_index": 4,
        "source": "MODEL",
        "type": "CORTEX_STEP_TYPE_RUN_COMMAND",
        "status": "FAILED",
        "content": "permission denied"
    })];
    let (transcript, boundary) = jsonl(&records);
    let path = "/tmp/conversation/transcript.jsonl";
    let registry = Registry::default_agents();
    let mut translator = registry.create("antigravity", "conversation-2");
    let ctx = SessionCtx {
        session_id: "conversation-2".into(),
        config: None,
    };
    let mut ops = translator
        .handle(
            &event(
                "PreToolUse",
                10,
                path,
                &transcript,
                0,
                json!({
                    "stepIdx": 4,
                    "toolCall": {"name":"run_command","args":{"CommandLine":"secret"}}
                }),
            ),
            &ctx,
        )
        .unwrap();
    ops.extend(
        translator
            .handle(
                &event(
                    "PostToolUse",
                    20,
                    path,
                    &transcript,
                    boundary[0],
                    json!({"stepIdx":4,"error":"permission denied"}),
                ),
                &ctx,
            )
            .unwrap(),
    );
    let rows = reduce(ops);
    let tool = rows
        .values()
        .find(|row| row.span_type == SpanType::Tool)
        .unwrap();
    assert_eq!(tool.start_ms, Some(10));
    assert_eq!(tool.end_ms, Some(20));
    assert_eq!(tool.error.as_deref(), Some("permission denied"));
    assert_eq!(tool.metadata.as_ref().unwrap()["tool_outcome"], "error");
}

#[test]
fn real_cli_schema_recovers_messages_and_post_only_tool() {
    let records = vec![
        json!({
            "step_index": 0,
            "created_at": "2026-08-11T17:46:20Z",
            "source": "USER_EXPLICIT",
            "type": "USER_INPUT",
            "status": "DONE",
            "content": "<USER_REQUEST>\nList /tmp\n</USER_REQUEST>\n<ADDITIONAL_METADATA>\nignored\n</ADDITIONAL_METADATA>"
        }),
        json!({
            "step_index": 1,
            "created_at": "2026-08-11T17:46:20Z",
            "source": "SYSTEM",
            "type": "CONVERSATION_HISTORY",
            "status": "DONE"
        }),
        json!({
            "step_index": 2,
            "created_at": "2026-08-11T17:46:20Z",
            "source": "MODEL",
            "type": "PLANNER_RESPONSE",
            "status": "DONE",
            "tool_calls": [{
                "name": "list_dir",
                "args": {"DirectoryPath":"/tmp","toolSummary":"List /tmp"}
            }]
        }),
        json!({
            "step_index": 3,
            "created_at": "2026-08-11T17:46:21Z",
            "source": "MODEL",
            "type": "LIST_DIRECTORY",
            "status": "DONE",
            "content": "Created At: 2026-08-12T01:46:21+08:00\nCompleted At: 2026-08-12T01:46:21+08:00\n{\"name\":\"sample\"}"
        }),
        json!({
            "step_index": 4,
            "created_at": "2026-08-11T17:46:21Z",
            "source": "SYSTEM",
            "type": "CHECKPOINT",
            "status": "DONE",
            "content": "checkpoint"
        }),
        json!({
            "step_index": 5,
            "created_at": "2026-08-11T17:46:21Z",
            "source": "MODEL",
            "type": "PLANNER_RESPONSE",
            "status": "DONE",
            "content": "done"
        }),
    ];
    let (transcript, boundary) = jsonl(&records);
    let path = "/tmp/conversation/transcript_full.jsonl";
    let registry = Registry::default_agents();
    let mut translator = registry.create("antigravity", "real-conversation");
    let ctx = SessionCtx {
        session_id: "real-conversation".into(),
        config: None,
    };
    let mut ops = Vec::new();
    ops.extend(
        translator
            .handle(
                &event(
                    "PreInvocation",
                    100,
                    path,
                    &transcript,
                    boundary[1],
                    json!({"invocationNum":0,"initialNumSteps":1}),
                ),
                &ctx,
            )
            .unwrap(),
    );
    ops.extend(
        translator
            .handle(
                &event(
                    "PostInvocation",
                    200,
                    path,
                    &transcript,
                    boundary[3],
                    json!({"invocationNum":0,"initialNumSteps":1}),
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
                    210,
                    path,
                    &transcript,
                    boundary[3],
                    json!({
                        "stepIdx":3,
                        "error":"",
                        "toolCall":{"name":"list_dir","args":{"DirectoryPath":"/tmp"}}
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
                    "PreInvocation",
                    220,
                    path,
                    &transcript,
                    boundary[3],
                    json!({"invocationNum":1,"initialNumSteps":5}),
                ),
                &ctx,
            )
            .unwrap(),
    );
    ops.extend(
        translator
            .handle(
                &event(
                    "PostInvocation",
                    300,
                    path,
                    &transcript,
                    boundary[5],
                    json!({"invocationNum":1,"initialNumSteps":5}),
                ),
                &ctx,
            )
            .unwrap(),
    );
    ops.extend(
        translator
            .handle(
                &event(
                    "Stop",
                    310,
                    path,
                    &transcript,
                    boundary[5],
                    json!({"executionNum":0,"terminationReason":"NO_TOOL_CALL","fullyIdle":true}),
                ),
                &ctx,
            )
            .unwrap(),
    );

    let rows = reduce(ops);
    let turn = rows.values().find(|row| row.name == "Turn 1").unwrap();
    assert_eq!(turn.input, Some(json!("List /tmp")));
    assert_eq!(turn.output, Some(json!("done")));

    let mut llms = rows
        .values()
        .filter(|row| row.span_type == SpanType::Llm)
        .collect::<Vec<_>>();
    llms.sort_by_key(|row| row.start_ms);
    assert_eq!(llms.len(), 2);
    assert_eq!(
        llms[0].output.as_ref().unwrap()[0]["tool_calls"][0]["name"],
        "list_dir"
    );
    assert!(llms[1]
        .input
        .as_ref()
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .any(|message| message["role"] == "tool"));
    assert_eq!(llms[1].output.as_ref().unwrap()[0]["content"], "done");

    let tool = rows
        .values()
        .find(|row| row.span_type == SpanType::Tool)
        .unwrap();
    assert_eq!(tool.name, "list_dir");
    assert_eq!(tool.start_ms, Some(1_786_470_380_000));
    assert_eq!(tool.output, Some(json!("{\"name\":\"sample\"}")));
    assert_eq!(tool.metadata.as_ref().unwrap()["tool_outcome"], "success");
}

#[test]
fn resumed_process_reuses_invocation_zero_without_reparenting_to_turn_one() {
    let records = vec![
        json!({
            "step_index": 0,
            "source": "USER_EXPLICIT",
            "type": "USER_INPUT",
            "status": "DONE",
            "content": "Use a tool"
        }),
        json!({
            "step_index": 1,
            "source": "MODEL",
            "type": "PLANNER_RESPONSE",
            "status": "DONE",
            "tool_calls": [{
                "name": "list_dir",
                "args": {"DirectoryPath":"/workspace/demo"}
            }]
        }),
        json!({
            "step_index": 2,
            "source": "MODEL",
            "type": "LIST_DIRECTORY",
            "status": "DONE",
            "content": "README.md"
        }),
        json!({
            "step_index": 3,
            "source": "MODEL",
            "type": "PLANNER_RESPONSE",
            "status": "DONE",
            "content": "first-turn-complete"
        }),
        json!({
            "step_index": 4,
            "source": "USER_EXPLICIT",
            "type": "USER_INPUT",
            "status": "DONE",
            "content": "This is turn two"
        }),
        json!({
            "step_index": 5,
            "source": "MODEL",
            "type": "PLANNER_RESPONSE",
            "status": "DONE",
            "content": "second-turn-complete"
        }),
    ];
    let (transcript, boundary) = jsonl(&records);
    let path = "/tmp/resumed-conversation/transcript_full.jsonl";
    let registry = Registry::default_agents();
    let mut translator = registry.create("antigravity", "resumed-conversation");
    let ctx = SessionCtx {
        session_id: "resumed-conversation".into(),
        config: None,
    };
    let mut ops = Vec::new();

    // First process: invocation zero asks for a tool, invocation one consumes
    // its result, then a fully-idle Stop closes the root.
    for hook in [
        event(
            "PreInvocation",
            100,
            path,
            &transcript,
            boundary[0],
            json!({"invocationNum":0,"initialNumSteps":1}),
        ),
        event(
            "PostInvocation",
            200,
            path,
            &transcript,
            boundary[1],
            json!({"invocationNum":0,"initialNumSteps":1}),
        ),
        event(
            "PostToolUse",
            210,
            path,
            &transcript,
            boundary[2],
            json!({"stepIdx":2,"error":""}),
        ),
        event(
            "PreInvocation",
            220,
            path,
            &transcript,
            boundary[2],
            json!({"invocationNum":1,"initialNumSteps":3}),
        ),
        event(
            "PostInvocation",
            300,
            path,
            &transcript,
            boundary[3],
            json!({"invocationNum":1,"initialNumSteps":3}),
        ),
        event(
            "Stop",
            310,
            path,
            &transcript,
            boundary[3],
            json!({"executionNum":0,"fullyIdle":true}),
        ),
        // A new process resumes the conversation. Antigravity resets
        // invocationNum to zero while the transcript step index continues.
        event(
            "PreInvocation",
            400,
            path,
            &transcript,
            boundary[4],
            json!({"invocationNum":0,"initialNumSteps":5}),
        ),
        event(
            "PostInvocation",
            500,
            path,
            &transcript,
            boundary[5],
            json!({"invocationNum":0,"initialNumSteps":5}),
        ),
        event(
            "Stop",
            510,
            path,
            &transcript,
            boundary[5],
            json!({"executionNum":0,"fullyIdle":true}),
        ),
    ] {
        ops.extend(translator.handle(&hook, &ctx).unwrap());
    }

    let rows = reduce(ops);
    let root = rows
        .values()
        .find(|row| row.name == "Antigravity: demo")
        .unwrap();
    assert_eq!(root.end_ms, Some(510));

    let turn_one = rows.values().find(|row| row.name == "Turn 1").unwrap();
    let turn_two = rows.values().find(|row| row.name == "Turn 2").unwrap();
    assert_eq!(turn_one.output, Some(json!("first-turn-complete")));
    assert_eq!(turn_two.input, Some(json!("This is turn two")));
    assert_eq!(turn_two.output, Some(json!("second-turn-complete")));

    let llms = rows
        .values()
        .filter(|row| row.span_type == SpanType::Llm)
        .collect::<Vec<_>>();
    assert_eq!(llms.len(), 3);
    let resumed = llms
        .iter()
        .find(|row| row.metadata.as_ref().unwrap()["turn_number"] == 2)
        .unwrap();
    assert_eq!(resumed.parent_span_ids, vec![turn_two.span_id.clone()]);
    assert_eq!(
        resumed.output.as_ref().unwrap()[0]["content"],
        "second-turn-complete"
    );
    let invocation_zero = llms
        .iter()
        .filter(|row| row.metadata.as_ref().unwrap()["invocation_num"] == 0)
        .collect::<Vec<_>>();
    assert_eq!(invocation_zero.len(), 2);
    assert_ne!(invocation_zero[0].span_id, invocation_zero[1].span_id);

    let tool = rows
        .values()
        .find(|row| row.span_type == SpanType::Tool)
        .unwrap();
    assert_eq!(tool.name, "list_directory");
    assert_eq!(tool.parent_span_ids, vec![turn_one.span_id.clone()]);
    assert_eq!(tool.metadata.as_ref().unwrap()["tool_outcome"], "success");
}
