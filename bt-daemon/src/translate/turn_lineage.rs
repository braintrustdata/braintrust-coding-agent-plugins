//! Publishes the user turn that owns each span.
//!
//! Translators already track which native turn every operation belongs to in
//! order to parent it, so they record the owning user turn's span id on each
//! row ([`SpanRow::turn_span_id`]). Nested agent activity (subagents, child
//! sessions, continuation turns) records the user turn that spawned it, so a
//! turn's cost can be aggregated with a flat filter instead of a tree walk.
//!
//! This wrapper copies that value into `metadata.turn_span_id` after the
//! translator has applied its own metadata allowlist. It keeps no state.

use super::{AgentTranslator, SessionCtx, SpanOp, SpanRow};
use serde_json::{Map, Value};

pub const TURN_SPAN_ID_KEY: &str = "turn_span_id";

pub(crate) struct TurnLineage {
    inner: Box<dyn AgentTranslator>,
}

impl TurnLineage {
    pub(crate) fn new(inner: Box<dyn AgentTranslator>) -> Self {
        Self { inner }
    }
}

fn stamp(mut ops: Vec<SpanOp>) -> Vec<SpanOp> {
    for op in &mut ops {
        let (SpanOp::Insert(row) | SpanOp::Merge(row)) = op;
        publish_turn(row);
    }
    ops
}

fn publish_turn(row: &mut SpanRow) {
    let Some(turn) = row.turn_span_id.clone() else {
        return;
    };
    let metadata = row
        .metadata
        .get_or_insert_with(|| Value::Object(Map::new()));
    if let Some(metadata) = metadata.as_object_mut() {
        metadata.insert(TURN_SPAN_ID_KEY.into(), Value::String(turn));
    }
}

impl AgentTranslator for TurnLineage {
    fn handle(
        &mut self,
        event: &crate::wire::Envelope,
        ctx: &SessionCtx,
    ) -> anyhow::Result<Vec<SpanOp>> {
        Ok(stamp(self.inner.handle(event, ctx)?))
    }

    fn drain_pending(&mut self, ctx: &SessionCtx) -> anyhow::Result<Option<Vec<SpanOp>>> {
        Ok(self.inner.drain_pending(ctx)?.map(stamp))
    }

    fn checkpoint(&mut self, ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        Ok(stamp(self.inner.checkpoint(ctx)?))
    }

    fn finalize(&mut self, ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        Ok(stamp(self.inner.finalize(ctx)?))
    }

    fn flush(&mut self, ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        Ok(stamp(self.inner.flush(ctx)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn row(turn: Option<&str>, metadata: Option<Value>) -> SpanOp {
        SpanOp::Insert(SpanRow {
            span_id: "span".into(),
            root_span_id: "root".into(),
            turn_span_id: turn.map(str::to_owned),
            metadata,
            ..Default::default()
        })
    }

    fn metadata(op: &SpanOp) -> Option<&Value> {
        let (SpanOp::Insert(row) | SpanOp::Merge(row)) = op;
        row.metadata.as_ref()
    }

    #[test]
    fn publishes_the_translator_recorded_turn_alongside_existing_metadata() {
        let ops = stamp(vec![row(Some("turn"), Some(json!({"model": "m"})))]);
        assert_eq!(
            metadata(&ops[0]),
            Some(&json!({"model": "m", "turn_span_id": "turn"}))
        );
    }

    #[test]
    fn rows_without_a_turn_are_left_untouched() {
        let ops = stamp(vec![row(None, None), row(None, Some(json!({"a": 1})))]);
        assert_eq!(metadata(&ops[0]), None);
        assert_eq!(metadata(&ops[1]), Some(&json!({"a": 1})));
    }
}
