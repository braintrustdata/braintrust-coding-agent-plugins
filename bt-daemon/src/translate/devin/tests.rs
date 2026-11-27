use crate::translate::{AgentTranslator, Registry, SessionCtx, SpanOp, SpanType};
use crate::wire::Envelope;
use serde_json::{json, Value};

fn send(
    translator: &mut dyn AgentTranslator,
    event: &str,
    ts_ms: i64,
    fields: Value,
) -> Vec<SpanOp> {
    let mut payload = json!({"hook_event_name": event, "session_id": "devin-fixture"});
    payload
        .as_object_mut()
        .unwrap()
        .extend(fields.as_object().unwrap().clone());
    translator
        .handle(
            &Envelope {
                source: "devin".into(),
                source_version: Some("3000.15.14".into()),
                session_id: "devin-fixture".into(),
                event: event.into(),
                ts_ms,
                payload,
                plugin_version: None,
                managed_run_id: None,
                capture: None,
                route: None,
                config: None,
            },
            &SessionCtx {
                session_id: "devin-fixture".into(),
                config: None,
            },
        )
        .unwrap()
}

#[test]
fn correlates_overlapping_tools_and_captures_the_final_reply_after_restore() {
    let registry = Registry::default_agents();
    let mut translator = registry.create("devin", "devin-fixture");
    let start = send(
        &mut *translator,
        "SessionStart",
        10,
        json!({"source": "startup"}),
    );
    let root = start[0].row();
    let submit = send(
        &mut *translator,
        "UserPromptSubmit",
        20,
        json!({"prompt_id": "p", "prompt": "Read both files", "source": "user"}),
    );
    let turn = submit[0].row();
    assert_eq!(turn.parent_span_ids, [root.span_id.as_str()]);
    assert_eq!(
        turn.input,
        Some(json!([{"role": "user", "content": "Read both files"}]))
    );

    let pre = |id: &str, command: &str| {
        json!({
            "prompt_id": "p", "tool_name": "exec", "tool_input": {"command": command}, "tool_use_id": id
        })
    };
    let first = send(
        &mut *translator,
        "PreToolUse",
        30,
        pre("exec:0#a", "cat a.txt"),
    );
    let second = send(
        &mut *translator,
        "PreToolUse",
        31,
        pre("exec:1#b", "cat missing.txt"),
    );
    assert_ne!(first[0].row().span_id, second[0].row().span_id);
    assert_eq!(second[0].row().parent_span_ids, [turn.span_id.as_str()]);
    assert_eq!(second[0].row().span_type, SpanType::Tool);

    let state = translator.save_state().unwrap();
    let mut restored = registry.create("devin", "devin-fixture");
    restored.restore_state(&state).unwrap();

    // Devin reports successful tool transport even when the shell command fails.
    let mut post = pre("exec:1#b", "cat missing.txt");
    post["tool_response"] =
        json!({"success": true, "output": "cat: missing.txt\nExit code: 1", "error": null});
    let finish = send(&mut *restored, "PostToolUse", 40, post.clone());
    assert!(matches!(finish.as_slice(), [SpanOp::Merge(_)]));
    assert_eq!(finish[0].row().span_id, second[0].row().span_id);
    assert_eq!(
        finish[0].row().output,
        Some(json!("cat: missing.txt\nExit code: 1"))
    );
    assert!(finish[0].row().error.is_none());
    assert!(send(&mut *restored, "PostToolUse", 41, post).is_empty());

    let stop = send(
        &mut *restored,
        "Stop",
        50,
        json!({"prompt_id": "p", "stop_hook_active": false, "last_assistant_message": "One file is missing"}),
    );
    assert_eq!(stop[0].row().span_id, turn.span_id);
    assert_eq!(
        stop[0].row().output,
        Some(json!([{"role": "assistant", "content": "One file is missing"}]))
    );
    assert_eq!(stop[0].row().end_ms, Some(50));

    let end = send(
        &mut *restored,
        "SessionEnd",
        60,
        json!({"prompt_id": "p", "reason": "other"}),
    );
    assert_eq!(end.len(), 2);
    let incomplete = end[0].row();
    assert_eq!(incomplete.span_id, first[0].row().span_id);
    assert_eq!(
        incomplete.turn_span_id.as_deref(),
        Some(turn.span_id.as_str())
    );
    assert_eq!(incomplete.metadata.as_ref().unwrap()["incomplete"], true);
    assert!(incomplete.error.is_none());
    assert_eq!(end[1].row().span_id, root.span_id);
    assert_eq!(end[1].row().end_ms, Some(60));
    let resume = send(
        &mut *restored,
        "SessionStart",
        70,
        json!({"source": "resume"}),
    );
    assert_eq!(resume[0].row().span_id, root.span_id);
    assert_eq!(resume[0].row().end_ms, Some(70));
    let resumed = send(
        &mut *restored,
        "UserPromptSubmit",
        80,
        json!({"prompt_id": "next", "prompt": "Try again"}),
    );
    assert_eq!(resumed[0].row().span_id, root.span_id);
    assert_eq!(resumed[0].row().end_ms, Some(80));
    assert_eq!(resumed[1].row().parent_span_ids, [root.span_id.as_str()]);
    assert_ne!(resumed[1].row().span_id, turn.span_id);
}

#[test]
fn preserves_permission_requests_and_missing_tool_boundaries() {
    let mut translator = Registry::default_agents().create("devin", "devin-fixture");
    let mut tool = json!({"prompt_id": "p", "tool_name": "exec", "tool_use_id": "exec:0#a", "tool_input": {"command": "printf test"}});
    let permission = send(&mut *translator, "PermissionRequest", 10, tool.clone());
    let request = permission.last().unwrap().row();
    assert_eq!(request.tags, Some(vec!["permission-request".into()]));
    assert!(request.error.is_none());

    tool["tool_response"] = json!({"success": false, "output": null, "error": "tool failed"});
    let post = send(&mut *translator, "PostToolUse", 20, tool.clone());
    assert!(matches!(post.as_slice(), [SpanOp::Insert(_)]));
    assert!(post[0].row().start_ms.is_none());
    assert_eq!(post[0].row().error.as_deref(), Some("tool failed"));
    let pre = send(&mut *translator, "PreToolUse", 15, tool);
    assert!(matches!(pre.as_slice(), [SpanOp::Merge(_)]));
    assert_eq!(pre[0].row().span_id, post[0].row().span_id);
    assert_eq!(pre[0].row().start_ms, Some(15));
    assert!(send(
        &mut *translator,
        "PreToolUse",
        30,
        json!({"prompt_id": "p", "tool_name": "exec", "tool_input": {}})
    )
    .is_empty());
    assert!(send(&mut *translator, "FutureEvent", 40, json!({})).is_empty());
}

#[test]
fn closes_pending_turn_and_tool_on_shutdown_or_finalization() {
    let registry = Registry::default_agents();
    let ctx = SessionCtx {
        session_id: "devin-fixture".into(),
        config: None,
    };
    for explicit_end in [true, false] {
        let mut translator = registry.create("devin", "devin-fixture");
        let start = send(
            &mut *translator,
            "UserPromptSubmit",
            10,
            json!({"prompt_id": "p", "prompt": "Read a file"}),
        );
        let turn = start.last().unwrap().row();
        let mut tool = json!({"prompt_id": "p", "tool_name": "read", "tool_use_id": "read:1"});
        let pre = send(&mut *translator, "PreToolUse", 20, tool.clone());
        let state = translator.save_state().unwrap();
        let mut translator = registry.create("devin", "devin-fixture");
        translator.restore_state(&state).unwrap();

        let closed = if explicit_end {
            send(
                &mut *translator,
                "SessionEnd",
                30,
                json!({"prompt_id": "p"}),
            )
        } else {
            translator.finalize(&ctx).unwrap()
        };
        for opened in [turn, pre[0].row()] {
            let row = closed
                .iter()
                .find(|op| op.row().span_id == opened.span_id)
                .unwrap()
                .row();
            assert_eq!(row.end_ms, Some(if explicit_end { 30 } else { 20 }));
            assert_eq!(row.metadata.as_ref().unwrap()["incomplete"], true);
            assert_eq!(row.turn_span_id, opened.turn_span_id);
            assert!(row.output.is_none());
            assert!(row.error.is_none());
        }
        assert!(translator.finalize(&ctx).unwrap().is_empty());

        tool["tool_response"] = json!({"output": "file contents"});
        let late = send(&mut *translator, "PostToolUse", 35, tool);
        let completed = late.last().unwrap().row();
        assert_eq!(completed.span_id, pre[0].row().span_id);
        assert_eq!(completed.output, Some(json!("file contents")));
        assert_eq!(completed.metadata.as_ref().unwrap()["incomplete"], false);
        assert!(completed.error.is_none());
    }
}

#[test]
fn accepts_null_tool_success() {
    let mut translator = Registry::default_agents().create("devin", "devin-fixture");
    let ops = send(
        &mut *translator,
        "PostToolUse",
        20,
        json!({
            "tool_name": "read", "tool_use_id": "read:1",
            "tool_response": {"success": null, "output": "file contents"}
        }),
    );
    let tool = ops.last().unwrap().row();
    assert_eq!(tool.output, Some(json!("file contents")));
    assert_eq!(tool.end_ms, Some(20));
    assert!(tool.error.is_none());
}
