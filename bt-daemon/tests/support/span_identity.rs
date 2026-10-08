use bt_daemon::{SpanOp, SpanRow, TURN_SPAN_ID_KEY};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

/// Remembers the identity each span was inserted with, so merges emitted in a
/// later batch than their insert can still be checked.
#[derive(Default)]
pub(crate) struct IdentityLedger {
    identities: HashMap<String, (String, Vec<String>)>,
}

impl IdentityLedger {
    /// Record this batch's inserts, then assert its merges repeat the identity
    /// their span was inserted with.
    pub(crate) fn check(&mut self, ops: &[SpanOp]) {
        for op in ops {
            match op {
                SpanOp::Insert(row) => {
                    self.identities.insert(
                        row.span_id.clone(),
                        (row.root_span_id.clone(), row.parent_span_ids.clone()),
                    );
                }
                SpanOp::Merge(row) => {
                    let (root_span_id, parent_span_ids) = self
                        .identities
                        .get(&row.span_id)
                        .unwrap_or_else(|| panic!("merge missing insert for span {}", row.span_id));
                    assert_eq!(
                        &row.root_span_id, root_span_id,
                        "merge changed root identity for span {}",
                        row.span_id
                    );
                    assert_eq!(
                        &row.parent_span_ids, parent_span_ids,
                        "merge dropped parent identity for span {}",
                        row.span_id
                    );
                }
            }
        }
    }
}

/// Stateless merges must carry the same hierarchy as their original inserts.
#[allow(dead_code)]
pub(crate) fn assert_merges_preserve_insert_identity(ops: &[SpanOp]) {
    IdentityLedger::default().check(ops);
}

/// Every span with a turn ancestor (or that is a turn) ends up carrying the
/// outermost such turn's id, and spans outside any turn carry none. A later
/// row may correct an earlier stamp, so the last value written wins. Returns
/// how many spans were stamped so callers can assert the fixture exercised
/// lineage.
#[allow(dead_code)]
pub(crate) fn assert_turn_lineage(ops: &[SpanOp], is_turn: impl Fn(&SpanRow) -> bool) -> usize {
    let mut parent_of = HashMap::<String, String>::new();
    let mut turns = HashSet::<String>::new();
    let mut stamped = HashMap::<String, String>::new();
    let mut spans = HashSet::<String>::new();
    for op in ops {
        let (SpanOp::Insert(row) | SpanOp::Merge(row)) = op;
        spans.insert(row.span_id.clone());
        if let Some(parent) = row.parent_span_ids.first() {
            parent_of
                .entry(row.span_id.clone())
                .or_insert_with(|| parent.clone());
        }
        if matches!(op, SpanOp::Insert(_)) && is_turn(row) {
            turns.insert(row.span_id.clone());
        }
        let turn = row
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get(TURN_SPAN_ID_KEY))
            .and_then(Value::as_str);
        if let Some(turn) = turn {
            stamped.insert(row.span_id.clone(), turn.to_owned());
        }
    }
    for span in &spans {
        let mut expected = None;
        let mut current = Some(span);
        for _ in 0..=spans.len() {
            let Some(id) = current else { break };
            if turns.contains(id) {
                expected = Some(id);
            }
            current = parent_of.get(id);
        }
        assert_eq!(
            stamped.get(span),
            expected,
            "span {span} has the wrong {TURN_SPAN_ID_KEY}"
        );
    }
    stamped.len()
}
