//! Root candidates are translation state, not unfinished sink deliveries.
use super::Stage;
use crate::translate::SpanOp;
use std::collections::BTreeMap;

#[derive(Default)]
pub(crate) struct Roots {
    pending: BTreeMap<String, Vec<SpanOp>>,
}

impl Roots {
    pub fn restore(pending: BTreeMap<String, Vec<SpanOp>>) -> Self {
        Self { pending }
    }

    pub fn pending(&self) -> &BTreeMap<String, Vec<SpanOp>> {
        &self.pending
    }
}

#[async_trait::async_trait]
impl Stage for Roots {
    type Input = Vec<SpanOp>;
    type Outcome = Vec<SpanOp>;

    async fn process(&mut self, ops: &Vec<SpanOp>) -> Vec<SpanOp> {
        let mut ready = Vec::new();
        for op in ops {
            let (SpanOp::Insert(row) | SpanOp::Merge(row)) = op;
            if let Some(root) = self.pending.get_mut(&row.span_id) {
                root.push(op.clone());
                continue;
            }
            for parent in &row.parent_span_ids {
                if let Some(root) = self.pending.remove(parent) {
                    for mut op in root {
                        let (SpanOp::Insert(row) | SpanOp::Merge(row)) = &mut op;
                        if let Some(metadata) =
                            row.metadata.as_mut().and_then(|v| v.as_object_mut())
                        {
                            metadata.remove("_bt_defer_root");
                        }
                        ready.push(op);
                    }
                }
            }
            if matches!(op, SpanOp::Insert(_))
                && row.metadata.as_ref().and_then(|v| v.get("_bt_defer_root"))
                    == Some(&serde_json::Value::Bool(true))
            {
                self.pending.insert(row.span_id.clone(), vec![op.clone()]);
            } else {
                ready.push(op.clone());
            }
        }
        ready
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::translate::SpanRow;
    use serde_json::json;

    fn root(id: &str) -> SpanOp {
        SpanOp::Insert(SpanRow {
            span_id: id.into(),
            root_span_id: id.into(),
            metadata: Some(json!({"_bt_defer_root": true})),
            ..Default::default()
        })
    }

    fn child(id: &str, parent: &str) -> SpanOp {
        SpanOp::Insert(SpanRow {
            span_id: id.into(),
            root_span_id: parent.into(),
            parent_span_ids: vec![parent.into()],
            ..Default::default()
        })
    }

    #[tokio::test]
    async fn unused_root_does_not_hold_other_roots_turns() {
        let mut roots = Roots::default();
        assert!(roots
            .process(&vec![root("unused"), root("active")])
            .await
            .is_empty());
        let output = roots.process(&vec![child("turn", "active")]).await;
        assert_eq!(output.len(), 2);
        let (SpanOp::Insert(row) | SpanOp::Merge(row)) = &output[0];
        assert_eq!(row.span_id, "active");
        assert!(row
            .metadata
            .as_ref()
            .unwrap()
            .get("_bt_defer_root")
            .is_none());
        assert!(roots.pending.contains_key("unused"));
        // An arbitrary number of later turns leaves just the unrelated root,
        // without retaining their values or completion obligations.
        for index in 0..1000 {
            assert_eq!(
                roots
                    .process(&vec![child(&format!("turn-{index}"), "active")])
                    .await
                    .len(),
                1
            );
        }
        assert_eq!(roots.pending.len(), 1);
        assert_eq!(roots.pending["unused"].len(), 1);
    }

    #[tokio::test]
    async fn restored_root_updates_are_reported_before_its_first_child() {
        let mut roots = Roots::default();
        let closed = SpanOp::Merge(SpanRow {
            span_id: "root".into(),
            end_ms: Some(2),
            ..Default::default()
        });
        assert!(roots.process(&vec![root("root"), closed]).await.is_empty());
        let mut restored = Roots::restore(roots.pending);
        let output = restored.process(&vec![child("turn", "root")]).await;
        assert_eq!(output.len(), 3);
        assert!(matches!(&output[0], SpanOp::Insert(row) if row.span_id == "root"));
        assert!(matches!(&output[1], SpanOp::Merge(row) if row.end_ms == Some(2)));
        assert!(matches!(&output[2], SpanOp::Insert(row) if row.span_id == "turn"));
        assert!(restored.pending.is_empty());
    }
}
