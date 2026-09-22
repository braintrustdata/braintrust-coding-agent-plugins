use bt_daemon::SpanOp;
use std::collections::HashMap;

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
