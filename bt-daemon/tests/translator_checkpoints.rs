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
                streams::comparable(collect(original.as_mut(), env, &ctx)),
                streams::comparable(collect(restored.as_mut(), env, &ctx)),
                "{source} split {split} event {}",
                env.event
            );
        }
        assert_eq!(
            streams::comparable(original.finalize(&ctx).unwrap()),
            streams::comparable(restored.finalize(&ctx).unwrap()),
            "{source} split {split} finalization"
        );
    }
}
#[test]
fn every_translator_preserves_native_state_at_every_event_boundary() {
    let mut covered = Vec::new();
    for (source, events, _fixtures) in streams::native_streams() {
        covered.push(source.clone());
        assert_stream(&source, events);
    }
    covered.sort();
    assert_eq!(covered, Registry::default_agents().sources());
}
