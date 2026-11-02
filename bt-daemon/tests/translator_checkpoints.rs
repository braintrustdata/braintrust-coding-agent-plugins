#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "Checkpoint regression fixtures fail immediately."
)]
#[path = "support/translation_streams.rs"]
mod streams;
use bt_daemon::wire::Envelope;
use bt_daemon::{AgentTranslator, Registry, SessionCtx, SpanOp};

fn collect(translator: &mut dyn AgentTranslator, env: &Envelope, ctx: &SessionCtx) -> Vec<SpanOp> {
    let mut ops = translator.handle(env, ctx).unwrap();
    while let Some(batch) = translator.drain_pending(ctx).unwrap() {
        ops.extend(batch);
    }
    ops
}
fn comparable(ops: Vec<SpanOp>) -> Vec<String> {
    let mut values = ops
        .into_iter()
        .map(|op| {
            let (SpanOp::Insert(row) | SpanOp::Merge(row)) = &op;
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
    values.sort();
    values
}
fn assert_stream(source: &str, events: Vec<Envelope>) {
    let registry = Registry::default_agents();
    let session = events
        .first()
        .map(|env| env.session_id.as_str())
        .unwrap_or("checkpoint");
    let ctx = SessionCtx {
        session_id: session.into(),
        config: None,
    };
    for split in 0..=events.len() {
        let mut original = registry.create(source, session);
        for env in &events[..split] {
            collect(original.as_mut(), env, &ctx);
        }
        let state = original
            .save_state()
            .unwrap()
            .expect("every translator must checkpoint");
        let mut restored = registry.create(source, session);
        restored.restore_state(&state).unwrap();
        for env in &events[split..] {
            assert_eq!(
                comparable(collect(original.as_mut(), env, &ctx)),
                comparable(collect(restored.as_mut(), env, &ctx)),
                "{source} split {split} event {}",
                env.event
            );
        }
        assert_eq!(
            comparable(original.finalize(&ctx).unwrap()),
            comparable(restored.finalize(&ctx).unwrap()),
            "{source} split {split} finalization"
        );
    }
}
#[test]
fn checkpoints_preserve_native_turns_tools_transcripts_and_later_turn_ids() {
    for (source, events, _fixtures) in streams::native_streams() {
        if ["claude-code", "codex", "pi", "opencode"].contains(&source.as_str()) {
            assert_stream(&source, events);
        }
    }
}
#[test]
fn muse_checkpoints_preserve_open_operations_and_cumulative_usage() {
    for (source, events, _fixtures) in streams::native_streams() {
        if source == "muse" {
            assert_stream(&source, events);
        }
    }
}
#[test]
fn cursor_checkpoints_preserve_open_tools_and_internal_span_fields() {
    for (source, events, _fixtures) in streams::native_streams() {
        if source == "cursor" {
            assert_stream(&source, events);
        }
    }
}
#[test]
fn antigravity_checkpoints_preserve_invocations_across_native_boundaries() {
    for (source, events, _fixtures) in streams::native_streams() {
        if source == "antigravity" {
            assert_stream(&source, events);
        }
    }
}
#[test]
fn grok_checkpoints_preserve_transcript_cursors_and_pending_model_work() {
    for (source, events, _fixtures) in streams::native_streams() {
        if source == "grok" {
            assert_stream(&source, events);
        }
    }
}
