use bt_daemon::SpanOp;
use std::collections::HashMap;

/// Stateless merges must carry the same hierarchy as their original inserts.
pub(crate) fn assert_merges_preserve_insert_identity(ops: &[SpanOp]) {
    let mut identities: HashMap<String, (String, Vec<String>)> = HashMap::new();
    for op in ops {
        match op {
            SpanOp::Insert(row) => {
                identities.insert(
                    row.span_id.clone(),
                    (row.root_span_id.clone(), row.parent_span_ids.clone()),
                );
            }
            SpanOp::Merge(row) => {
                let (root_span_id, parent_span_ids) = identities
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
