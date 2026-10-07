//! Stamps every span with the user turn that owns it.
//!
//! Translators only mark the span that opens a turn ([`SpanRow::turn_root`]).
//! This wrapper copies that span's id into `metadata.turn_span_id` on the turn
//! and on every descendant, including nested subagent activity, so a turn's
//! cost can be aggregated with a flat filter instead of a tree walk.
//!
//! The outermost turn wins: a subagent's own turns roll up to the user turn
//! that spawned them. A row whose ancestry is not yet known is remembered and
//! re-emitted as a late merge once its parent resolves.
//!
//! Translator state is rebuilt by journal replay, which also replays through
//! this wrapper, so the lineage maps need no persistence of their own.

use super::{AgentTranslator, SessionCtx, SpanOp, SpanRow};
use serde_json::{Map, Value};
use std::collections::HashMap;

pub const TURN_SPAN_ID_KEY: &str = "turn_span_id";
const LATE_MERGE_KEY: &str = "turn_lineage";

struct Unresolved {
    root_span_id: String,
    parent_span_ids: Vec<String>,
}

pub(crate) struct TurnLineage {
    inner: Box<dyn AgentTranslator>,
    turn_of: HashMap<String, String>,
    parent_of: HashMap<String, String>,
    unresolved_children: HashMap<String, HashMap<String, Unresolved>>,
}

impl TurnLineage {
    pub(crate) fn new(inner: Box<dyn AgentTranslator>) -> Self {
        Self {
            inner,
            turn_of: HashMap::new(),
            parent_of: HashMap::new(),
            unresolved_children: HashMap::new(),
        }
    }

    fn stamp(&mut self, ops: Vec<SpanOp>) -> Vec<SpanOp> {
        let mut out = Vec::with_capacity(ops.len());
        for mut op in ops {
            let is_insert = matches!(op, SpanOp::Insert(_));
            let (SpanOp::Insert(row) | SpanOp::Merge(row)) = &mut op;
            let span_id = row.span_id.clone();
            let already_stamped = self.turn_of.contains_key(&span_id);
            let turn = self.resolve(row);
            // Merges keep earlier metadata, so only rows that create or replace
            // a span, or the first row to resolve it, need the key.
            if let Some(turn) = turn.as_ref().filter(|_| is_insert || !already_stamped) {
                set_turn(row, turn);
            }
            out.push(op);
            if let Some(turn) = turn {
                self.release_children(&span_id, &turn, &mut out);
            }
        }
        out
    }

    fn resolve(&mut self, row: &SpanRow) -> Option<String> {
        if let Some(parent) = row.parent_span_ids.first() {
            self.parent_of
                .entry(row.span_id.clone())
                .or_insert_with(|| parent.clone());
        }
        if let Some(turn) = self.turn_of.get(&row.span_id) {
            return Some(turn.clone());
        }
        let parent = self.parent_of.get(&row.span_id).cloned();
        let inherited = parent
            .as_ref()
            .and_then(|parent| self.turn_of.get(parent))
            .cloned();
        let Some(turn) = inherited.or_else(|| row.turn_root.then(|| row.span_id.clone())) else {
            if let Some(parent) = parent {
                self.unresolved_children
                    .entry(parent)
                    .or_default()
                    .entry(row.span_id.clone())
                    .or_insert_with(|| Unresolved {
                        root_span_id: row.root_span_id.clone(),
                        parent_span_ids: row.parent_span_ids.clone(),
                    });
            }
            return None;
        };
        self.turn_of.insert(row.span_id.clone(), turn.clone());
        Some(turn)
    }

    fn release_children(&mut self, span_id: &str, turn: &str, out: &mut Vec<SpanOp>) {
        let mut resolved = vec![span_id.to_owned()];
        while let Some(parent) = resolved.pop() {
            let Some(children) = self.unresolved_children.remove(&parent) else {
                continue;
            };
            for (child, unresolved) in children {
                self.turn_of.insert(child.clone(), turn.to_owned());
                let mut row = SpanRow {
                    span_id: child.clone(),
                    root_span_id: unresolved.root_span_id,
                    parent_span_ids: unresolved.parent_span_ids,
                    late_merge_key: Some(LATE_MERGE_KEY.into()),
                    ..Default::default()
                };
                set_turn(&mut row, turn);
                out.push(SpanOp::Merge(row));
                resolved.push(child);
            }
        }
    }
}

fn set_turn(row: &mut SpanRow, turn: &str) {
    let metadata = row
        .metadata
        .get_or_insert_with(|| Value::Object(Map::new()));
    if let Some(metadata) = metadata.as_object_mut() {
        metadata.insert(TURN_SPAN_ID_KEY.into(), Value::String(turn.to_owned()));
    }
}

impl AgentTranslator for TurnLineage {
    fn handle(
        &mut self,
        event: &crate::wire::Envelope,
        ctx: &SessionCtx,
    ) -> anyhow::Result<Vec<SpanOp>> {
        let ops = self.inner.handle(event, ctx)?;
        Ok(self.stamp(ops))
    }

    fn drain_pending(&mut self, ctx: &SessionCtx) -> anyhow::Result<Option<Vec<SpanOp>>> {
        let ops = self.inner.drain_pending(ctx)?;
        Ok(ops.map(|ops| self.stamp(ops)))
    }

    fn checkpoint(&mut self, ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        let ops = self.inner.checkpoint(ctx)?;
        Ok(self.stamp(ops))
    }

    fn finalize(&mut self, ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        let ops = self.inner.finalize(ctx)?;
        Ok(self.stamp(ops))
    }

    fn flush(&mut self, ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        let ops = self.inner.flush(ctx)?;
        Ok(self.stamp(ops))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::translate::SpanType;
    use crate::wire::Envelope;
    use std::collections::VecDeque;

    struct Scripted(VecDeque<Vec<SpanOp>>);

    impl AgentTranslator for Scripted {
        fn handle(&mut self, _event: &Envelope, _ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
            Ok(self.0.pop_front().unwrap_or_default())
        }
    }

    fn row(span_id: &str, parent: Option<&str>, span_type: SpanType) -> SpanRow {
        SpanRow {
            span_id: span_id.into(),
            root_span_id: "root".into(),
            parent_span_ids: parent.map(|p| vec![p.to_owned()]).unwrap_or_default(),
            name: span_id.into(),
            span_type,
            ..Default::default()
        }
    }

    fn turn(span_id: &str, parent: &str) -> SpanRow {
        SpanRow {
            turn_root: true,
            ..row(span_id, Some(parent), SpanType::Task)
        }
    }

    fn run(batches: Vec<Vec<SpanOp>>) -> Vec<SpanOp> {
        let count = batches.len();
        let mut lineage = TurnLineage::new(Box::new(Scripted(batches.into())));
        let ctx = SessionCtx {
            session_id: "s".into(),
            config: None,
        };
        let event: Envelope = serde_json::from_value(serde_json::json!({
            "v": 1, "source": "debug", "session_id": "s", "event": "e", "ts_ms": 0, "payload": {}
        }))
        .unwrap();
        (0..count)
            .flat_map(|_| lineage.handle(&event, &ctx).unwrap())
            .collect()
    }

    fn turn_of(ops: &[SpanOp], span_id: &str) -> Vec<Option<String>> {
        ops.iter()
            .map(|op| match op {
                SpanOp::Insert(row) | SpanOp::Merge(row) => row,
            })
            .filter(|row| row.span_id == span_id)
            .map(|row| {
                row.metadata
                    .as_ref()
                    .and_then(|metadata| metadata.get(TURN_SPAN_ID_KEY))
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .collect()
    }

    #[test]
    fn stamps_turn_and_all_descendants() {
        let ops = run(vec![vec![
            SpanOp::Insert(row("root", None, SpanType::Task)),
            SpanOp::Insert(turn("turn", "root")),
            SpanOp::Insert(row("llm", Some("turn"), SpanType::Llm)),
            SpanOp::Insert(row("subagent", Some("turn"), SpanType::Task)),
            SpanOp::Insert(row("nested-llm", Some("subagent"), SpanType::Llm)),
            SpanOp::Merge(SpanRow {
                span_id: "llm".into(),
                root_span_id: "root".into(),
                ..Default::default()
            }),
        ]]);
        assert_eq!(turn_of(&ops, "root"), [None]);
        assert_eq!(turn_of(&ops, "turn"), [Some("turn".into())]);
        assert_eq!(turn_of(&ops, "llm"), [Some("turn".into()), None]);
        assert_eq!(turn_of(&ops, "nested-llm"), [Some("turn".into())]);
    }

    #[test]
    fn outermost_turn_wins_for_nested_agent_turns() {
        let ops = run(vec![vec![
            SpanOp::Insert(turn("user-turn", "root")),
            SpanOp::Insert(row("spawn", Some("user-turn"), SpanType::Tool)),
            SpanOp::Insert(turn("subagent-turn", "spawn")),
            SpanOp::Insert(row("subagent-llm", Some("subagent-turn"), SpanType::Llm)),
        ]]);
        assert_eq!(turn_of(&ops, "subagent-turn"), [Some("user-turn".into())]);
        assert_eq!(turn_of(&ops, "subagent-llm"), [Some("user-turn".into())]);
    }

    #[test]
    fn late_parent_resolution_emits_late_merges_for_waiting_descendants() {
        let ops = run(vec![
            vec![
                SpanOp::Insert(row("llm", Some("turn"), SpanType::Llm)),
                SpanOp::Insert(row("child", Some("llm"), SpanType::Tool)),
            ],
            vec![SpanOp::Insert(turn("turn", "root"))],
        ]);
        assert_eq!(turn_of(&ops, "llm"), [None, Some("turn".into())]);
        assert_eq!(turn_of(&ops, "child"), [None, Some("turn".into())]);
        let late: Vec<_> = ops
            .iter()
            .filter_map(|op| match op {
                SpanOp::Merge(row) => row.late_merge_key.as_deref(),
                SpanOp::Insert(_) => None,
            })
            .collect();
        assert_eq!(late, [LATE_MERGE_KEY, LATE_MERGE_KEY]);
    }

    #[test]
    fn spans_outside_any_turn_are_left_untouched() {
        let ops = run(vec![vec![
            SpanOp::Insert(row("root", None, SpanType::Task)),
            SpanOp::Insert(row("session-compaction", Some("root"), SpanType::Task)),
        ]]);
        assert_eq!(turn_of(&ops, "session-compaction"), [None]);
        let SpanOp::Insert(row) = &ops[1] else {
            panic!("expected insert");
        };
        assert!(row.metadata.is_none());
    }
}
