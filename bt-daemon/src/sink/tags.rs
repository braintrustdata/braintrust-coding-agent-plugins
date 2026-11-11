//! Deliver each span's tags once, however many sources contributed them.
use super::Sink;
use crate::translate::{CowSpanOp, SpanOp};
use crate::wire::SessionConfig;
use std::collections::HashSet;

pub(crate) struct UniqueTags(pub Box<dyn Sink>);

#[async_trait::async_trait]
impl Sink for UniqueTags {
    fn configure(&mut self, config: &SessionConfig) {
        self.0.configure(config);
    }

    async fn emit(&mut self, ops: &[SpanOp]) -> anyhow::Result<u64> {
        self.emit_shared(&ops.iter().cloned().map(CowSpanOp::new).collect::<Vec<_>>())
            .await
    }

    async fn emit_shared(&mut self, ops: &[CowSpanOp]) -> anyhow::Result<u64> {
        let mut ops = ops.to_vec();
        for op in &mut ops {
            let (SpanOp::Insert(row) | SpanOp::Merge(row)) = op.as_ref();
            let duplicated = row.tags.as_ref().is_some_and(|tags| {
                let mut seen = HashSet::with_capacity(tags.len());
                !tags.iter().all(|tag| seen.insert(tag))
            });
            if duplicated {
                // Copy only a row that changes; others keep sharing their payload.
                let (SpanOp::Insert(row) | SpanOp::Merge(row)) = op.make_mut();
                if let Some(tags) = &mut row.tags {
                    let mut seen = HashSet::with_capacity(tags.len());
                    tags.retain(|tag| seen.insert(tag.clone()));
                }
            }
        }
        self.0.emit_shared(&ops).await
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        self.0.flush().await
    }

    fn has_pending_delivery(&self) -> bool {
        self.0.has_pending_delivery()
    }

    fn permalink(&self) -> Option<String> {
        self.0.permalink()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::translate::SpanRow;
    use parking_lot::Mutex;
    use std::sync::Arc;

    struct Recording(Arc<Mutex<Vec<SpanOp>>>);
    #[async_trait::async_trait]
    impl Sink for Recording {
        async fn emit(&mut self, ops: &[SpanOp]) -> anyhow::Result<u64> {
            self.0.lock().extend_from_slice(ops);
            Ok(ops.len() as u64)
        }
        async fn flush(&mut self) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn deduplicates_tags_without_changing_the_shared_operation() {
        let emitted = Arc::new(Mutex::new(Vec::new()));
        let mut sink = UniqueTags(Box::new(Recording(emitted.clone())));
        let original = CowSpanOp::new(SpanOp::Insert(SpanRow {
            span_id: "span".into(),
            tags: Some(vec!["a".into(), "b".into(), "a".into()]),
            ..Default::default()
        }));
        sink.emit_shared(std::slice::from_ref(&original))
            .await
            .unwrap();
        let SpanOp::Insert(row) = &emitted.lock()[0] else {
            panic!("expected insert")
        };
        assert_eq!(
            row.tags.as_deref(),
            Some(["a".into(), "b".into()].as_slice())
        );
        let SpanOp::Insert(row) = original.as_ref() else {
            panic!("expected insert")
        };
        assert_eq!(row.tags.as_ref().unwrap().len(), 3);
    }
}
