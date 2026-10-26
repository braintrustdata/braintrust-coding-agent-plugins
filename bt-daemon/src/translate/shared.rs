//! Owned copy-on-write span operations for asynchronous route fan-out.
use super::SpanOp;
use std::sync::{Arc, Weak};

/// A route's independently mutable reference to a canonical span operation.
/// Clones share immutable data; mutation copies only when another owner exists.
#[derive(Debug, Clone)]
pub struct CowSpanOp(Arc<SpanOp>);
impl CowSpanOp {
    pub fn new(op: SpanOp) -> Self {
        Self(Arc::new(op))
    }
    pub fn make_mut(&mut self) -> &mut SpanOp {
        Arc::make_mut(&mut self.0)
    }
    pub(crate) fn downgrade(&self) -> Weak<SpanOp> {
        Arc::downgrade(&self.0)
    }
    pub(crate) fn from_arc(op: Arc<SpanOp>) -> Self {
        Self(op)
    }
}
impl From<SpanOp> for CowSpanOp {
    fn from(op: SpanOp) -> Self {
        Self::new(op)
    }
}
impl AsRef<SpanOp> for CowSpanOp {
    fn as_ref(&self) -> &SpanOp {
        &self.0
    }
}
impl std::ops::Deref for CowSpanOp {
    type Target = SpanOp;
    fn deref(&self) -> &SpanOp {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::translate::SpanRow;
    #[tokio::test]
    async fn concurrent_routes_share_until_one_mutates() {
        let canonical = CowSpanOp::new(SpanOp::Insert(SpanRow {
            input: Some(serde_json::json!({"history":"original"})),
            ..Default::default()
        }));
        let mut first = canonical.clone();
        let second = canonical.clone();
        assert!(Arc::ptr_eq(&first.0, &second.0));
        let (first, second) = tokio::join!(
            async move {
                let (SpanOp::Insert(row) | SpanOp::Merge(row)) = first.make_mut();
                row.input = Some(serde_json::json!({"history":"changed"}));
                first
            },
            async move { second }
        );
        assert!(!Arc::ptr_eq(&first.0, &second.0));
        let (SpanOp::Insert(row) | SpanOp::Merge(row)) = second.as_ref();
        assert_eq!(row.input.as_ref().unwrap()["history"], "original");
        assert!(Arc::ptr_eq(&canonical.0, &second.0));
        let weak = first.downgrade();
        drop(first);
        assert!(
            weak.upgrade().is_none(),
            "no route cache pins a finished payload"
        );
    }
}
