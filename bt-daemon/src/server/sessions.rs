//! Delivery keys and capture-side locks.

use super::*;

/// One delivery route of a source session, keyed by the exact route carried
/// by its hook or import envelope.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct DeliveryKey {
    pub(super) source: String,
    pub(super) session_id: String,
    pub(super) route: String,
}

impl DeliveryKey {
    pub(super) fn new(
        source: &str,
        session_id: &str,
        route: &SessionRoute,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            source: source.to_string(),
            session_id: session_id.to_string(),
            route: serde_json::to_string(route)?,
        })
    }

    pub(super) fn partition(&self) -> PartitionId {
        PartitionId::new(&self.source, &self.session_id)
    }
}

impl Daemon {
    pub(super) fn session_lock(
        &self,
        source: &str,
        session_id: &str,
    ) -> Arc<tokio::sync::Mutex<()>> {
        let storage_key = crate::ids::session_namespace(source, session_id);
        self.session_locks
            .lock()
            .unwrap()
            .entry(storage_key)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }
}
