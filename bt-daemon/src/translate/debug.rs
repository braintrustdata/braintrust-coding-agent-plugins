//! A pass-through translator used by the prototype and tests. It builds a
//! minimal but real span tree — one session root plus one `tool`-typed span
//! per event — so the end-to-end pipeline (transport → dispatch → journal →
//! translate → sink) can be exercised before any agent-specific translator
//! exists.

use super::{AgentTranslator, SessionCtx, SpanOp, SpanRow, SpanType, TranslatorFactory};
use crate::ids;
use crate::wire::Envelope;

pub struct DebugTranslatorFactory;

impl TranslatorFactory for DebugTranslatorFactory {
    fn source(&self) -> &str {
        "debug"
    }
    fn create(&self, session_id: &str) -> Box<dyn AgentTranslator> {
        Box::new(DebugTranslator {
            root_span_id: ids::span_id(session_id, "root"),
            root_emitted: false,
            event_seq: 0,
        })
    }
}

struct DebugTranslator {
    root_span_id: String,
    root_emitted: bool,
    event_seq: u64,
}

impl AgentTranslator for DebugTranslator {
    fn handle(&mut self, event: &Envelope, ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        let mut ops = Vec::new();

        if !self.root_emitted {
            self.root_emitted = true;
            ops.push(SpanOp::Insert(SpanRow {
                span_id: self.root_span_id.clone(),
                root_span_id: self.root_span_id.clone(),
                parent_span_ids: Vec::new(),
                name: format!("{}: {}", event.source, ctx.session_id),
                span_type: SpanType::Task,
                start_ms: Some(event.ts_ms),
                end_ms: None,
                input: None,
                output: None,
                metadata: Some(serde_json::json!({ "session_id": ctx.session_id })),
                metrics: None,
                error: None,
                tags: None,
            }));
        }

        let seq = self.event_seq;
        self.event_seq += 1;
        let span_id = ids::span_id(&ctx.session_id, &format!("event:{seq}"));
        ops.push(SpanOp::Insert(SpanRow {
            span_id,
            root_span_id: self.root_span_id.clone(),
            parent_span_ids: vec![self.root_span_id.clone()],
            name: event.event.clone(),
            span_type: SpanType::Tool,
            start_ms: Some(event.ts_ms),
            end_ms: Some(event.ts_ms),
            input: Some(event.payload.clone()),
            output: None,
            metadata: Some(serde_json::json!({ "seq": seq, "source": event.source })),
            metrics: None,
            error: None,
            tags: None,
        }));

        Ok(ops)
    }

    fn flush(&mut self, _ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        Ok(Vec::new())
    }
}
