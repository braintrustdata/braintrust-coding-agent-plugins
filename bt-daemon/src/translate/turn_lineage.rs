//! Stamps every span with the user turn that owns it.
//!
//! Translators only mark the span that opens a turn ([`SpanRow::turn_root`]).
//! This wrapper copies that span's id into `metadata.turn_span_id` on the turn
//! and on every descendant, including nested subagent activity, so a turn's
//! cost can be aggregated with a flat filter instead of a tree walk.
//!
//! A span's turn is always derived from what is known so far: its parent's
//! turn if the parent has one, otherwise itself if it is a turn root. The
//! outermost turn therefore wins, so a subagent's own turns roll up to the user
//! turn that spawned them. Rows can arrive in any order; whenever a span's turn
//! changes, its affected descendants are corrected with late merges.
//!
//! Translator state is rebuilt by journal replay, which also replays through
//! this wrapper, so the lineage maps need no persistence of their own.

use super::{AgentTranslator, SessionCtx, SpanOp, SpanRow};
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};

pub const TURN_SPAN_ID_KEY: &str = "turn_span_id";
const LATE_MERGE_KEY_PREFIX: &str = "turn_lineage";

struct Identity {
    root_span_id: String,
    parent_span_ids: Vec<String>,
}

pub(crate) struct TurnLineage {
    inner: Box<dyn AgentTranslator>,
    identity: HashMap<String, Identity>,
    parent_of: HashMap<String, String>,
    children: HashMap<String, Vec<String>>,
    turn_roots: HashSet<String>,
    turn_of: HashMap<String, String>,
}

impl TurnLineage {
    pub(crate) fn new(inner: Box<dyn AgentTranslator>) -> Self {
        Self {
            inner,
            identity: HashMap::new(),
            parent_of: HashMap::new(),
            children: HashMap::new(),
            turn_roots: HashSet::new(),
            turn_of: HashMap::new(),
        }
    }

    fn stamp(&mut self, ops: Vec<SpanOp>) -> Vec<SpanOp> {
        let mut out = Vec::with_capacity(ops.len());
        for mut op in ops {
            let is_insert = matches!(op, SpanOp::Insert(_));
            let (SpanOp::Insert(row) | SpanOp::Merge(row)) = &mut op;
            let span_id = row.span_id.clone();
            self.observe(row);
            let previous = self.turn_of.get(&span_id).cloned();
            let turn = self.derive(&span_id);
            // Merges keep earlier metadata, so only rows that create or replace
            // a span, or that change its turn, need the key.
            if let Some(turn) = turn
                .as_ref()
                .filter(|turn| is_insert || previous.as_ref() != Some(*turn))
            {
                set_turn(row, turn);
                self.turn_of.insert(span_id.clone(), turn.clone());
            }
            out.push(op);
            if turn.is_some() && turn != previous {
                self.correct_descendants(&span_id, &mut out);
            }
        }
        out
    }

    fn observe(&mut self, row: &SpanRow) {
        if row.turn_root {
            self.turn_roots.insert(row.span_id.clone());
        }
        let identity = self
            .identity
            .entry(row.span_id.clone())
            .or_insert_with(|| Identity {
                root_span_id: row.root_span_id.clone(),
                parent_span_ids: Vec::new(),
            });
        if identity.parent_span_ids.is_empty() {
            identity.parent_span_ids = row.parent_span_ids.clone();
        }
        if let Some(parent) = row.parent_span_ids.first() {
            if !self.parent_of.contains_key(&row.span_id) {
                self.parent_of.insert(row.span_id.clone(), parent.clone());
                self.children
                    .entry(parent.clone())
                    .or_default()
                    .push(row.span_id.clone());
            }
        }
    }

    fn derive(&self, span_id: &str) -> Option<String> {
        let inherited = self
            .parent_of
            .get(span_id)
            .and_then(|parent| self.turn_of.get(parent));
        inherited.cloned().or_else(|| {
            self.turn_roots
                .contains(span_id)
                .then(|| span_id.to_owned())
        })
    }

    fn correct_descendants(&mut self, span_id: &str, out: &mut Vec<SpanOp>) {
        let mut changed = vec![span_id.to_owned()];
        while let Some(parent) = changed.pop() {
            let Some(children) = self.children.get(&parent).cloned() else {
                continue;
            };
            for child in children {
                let Some(turn) = self.derive(&child) else {
                    continue;
                };
                if self.turn_of.get(&child) == Some(&turn) {
                    continue;
                }
                self.turn_of.insert(child.clone(), turn.clone());
                let Some(identity) = self.identity.get(&child) else {
                    continue;
                };
                let mut row = SpanRow {
                    span_id: child.clone(),
                    root_span_id: identity.root_span_id.clone(),
                    parent_span_ids: identity.parent_span_ids.clone(),
                    late_merge_key: Some(format!("{LATE_MERGE_KEY_PREFIX}:{turn}")),
                    ..Default::default()
                };
                set_turn(&mut row, &turn);
                out.push(SpanOp::Merge(row));
                changed.push(child);
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

    fn late_merge_keys(ops: &[SpanOp]) -> Vec<&str> {
        ops.iter()
            .filter_map(|op| match op {
                SpanOp::Merge(row) => row.late_merge_key.as_deref(),
                SpanOp::Insert(_) => None,
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
        assert_eq!(
            late_merge_keys(&ops),
            ["turn_lineage:turn", "turn_lineage:turn"]
        );
    }

    #[test]
    fn subagent_turn_emitted_before_its_spawn_span_is_corrected_to_the_user_turn() {
        let ops = run(vec![
            vec![SpanOp::Insert(turn("user-turn", "root"))],
            vec![
                SpanOp::Insert(turn("subagent-turn", "spawn")),
                SpanOp::Insert(row("subagent-llm", Some("subagent-turn"), SpanType::Llm)),
            ],
            vec![SpanOp::Insert(row(
                "spawn",
                Some("user-turn"),
                SpanType::Tool,
            ))],
        ]);
        assert_eq!(
            turn_of(&ops, "subagent-turn"),
            [Some("subagent-turn".into()), Some("user-turn".into())]
        );
        assert_eq!(
            turn_of(&ops, "subagent-llm"),
            [Some("subagent-turn".into()), Some("user-turn".into())]
        );
    }

    #[test]
    fn each_correction_of_a_span_gets_its_own_late_merge_key() {
        let ops = run(vec![
            vec![SpanOp::Insert(turn("inner", "middle"))],
            vec![SpanOp::Insert(turn("middle", "outer"))],
            vec![SpanOp::Insert(turn("outer", "root"))],
        ]);
        assert_eq!(
            turn_of(&ops, "inner"),
            [
                Some("inner".into()),
                Some("middle".into()),
                Some("outer".into())
            ]
        );
        assert_eq!(
            late_merge_keys(&ops),
            [
                "turn_lineage:middle",
                "turn_lineage:outer",
                "turn_lineage:outer"
            ]
        );
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
