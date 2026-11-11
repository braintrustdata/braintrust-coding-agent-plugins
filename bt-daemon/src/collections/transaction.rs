//! Atomic mutations of independently typed collections sharing one SQLite file.
use super::{insert_encoded, Collection};
use rusqlite::{params, Connection};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    ops::RangeToInclusive,
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
};

type AfterCommit = Option<Box<dyn FnOnce() + Send>>;
type Mutation = Box<dyn FnOnce(&rusqlite::Transaction<'_>) -> anyhow::Result<AfterCommit> + Send>;

/// An ID becomes observable only after its transaction commits successfully.
#[derive(Clone, Default)]
pub(crate) struct Insertion(Arc<OnceLock<u64>>);
impl Insertion {
    pub fn id(&self) -> Option<u64> {
        self.0.get().copied()
    }
}
pub(crate) enum Removal {
    Item(u64),
    Through(u64),
}
impl From<u64> for Removal {
    fn from(id: u64) -> Self {
        Self::Item(id)
    }
}
impl From<RangeToInclusive<u64>> for Removal {
    fn from(range: RangeToInclusive<u64>) -> Self {
        Self::Through(range.end)
    }
}

pub(crate) struct Transaction {
    path: Arc<PathBuf>,
    mutations: Vec<Mutation>,
}
impl Transaction {
    pub(super) fn new(path: Arc<PathBuf>) -> Self {
        Self {
            path,
            mutations: Vec::new(),
        }
    }

    pub fn insert<T: Serialize + DeserializeOwned + Send + 'static>(
        &mut self,
        collection: &Collection<T>,
        value: &T,
    ) -> anyhow::Result<Insertion> {
        self.insert_with_identity(collection, value, None)
    }
    pub(super) fn insert_with_identity<T: Serialize + DeserializeOwned + Send + 'static>(
        &mut self,
        collection: &Collection<T>,
        value: &T,
        identity: Option<&T>,
    ) -> anyhow::Result<Insertion> {
        let name = collection.name.clone();
        let value = serde_json::to_vec(value)?;
        let identity = identity.map(serde_json::to_vec).transpose()?;
        let insertion = Insertion::default();
        let committed = insertion.clone();
        self.stage_storage(&collection.path, move |db| {
            let id = insert_encoded(db, &name, value, identity)?;
            Ok(Some(Box::new(move || {
                let _ = committed.0.set(id);
            })))
        })?;
        Ok(insertion)
    }
    pub fn remove<T>(
        &mut self,
        collection: &Collection<T>,
        selection: impl Into<Removal>,
    ) -> anyhow::Result<()> {
        let name = collection.name.clone();
        let selection = selection.into();
        self.stage_storage(&collection.path, move |db| {
            match selection {
                Removal::Item(id) => db.execute(
                    "DELETE FROM collection_records WHERE name=? AND id=?",
                    params![&*name, id],
                )?,
                Removal::Through(through) => db.execute(
                    "DELETE FROM collection_records WHERE name=? AND id<=?",
                    params![&*name, through],
                )?,
            };
            Ok(None)
        })
    }
    pub fn drain<T>(
        &mut self,
        collection: &Collection<T>,
        range: RangeToInclusive<u64>,
    ) -> anyhow::Result<()> {
        self.remove(collection, range)
    }

    // Storage adapters can join the same commit without exposing SQLite to their
    // callers. Notifications are returned, and run only after the final COMMIT.
    pub(crate) fn stage_storage(
        &mut self,
        path: &Path,
        mutation: impl FnOnce(&rusqlite::Transaction<'_>) -> anyhow::Result<AfterCommit>
            + Send
            + 'static,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            path == self.path.as_path(),
            "collections in a transaction must share a database"
        );
        self.mutations.push(Box::new(mutation));
        Ok(())
    }
    pub(super) fn commit(self, db: &mut Connection) -> anyhow::Result<()> {
        let transaction = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let mut notifications = Vec::new();
        for mutation in self.mutations {
            if let Some(notify) = mutation(&transaction)? {
                notifications.push(notify);
            }
        }
        transaction.commit()?;
        for notify in notifications {
            notify();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collections::async_iter::{AsyncIteratorExt, IntoAsyncIterator};
    #[tokio::test]
    async fn transaction_transfers_between_independently_opened_typed_collections() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shared.sqlite");
        let events = Collection::<String>::open(&path, "events").await.unwrap();
        let spans = Collection::<Vec<String>>::open(&path, "spans")
            .await
            .unwrap();
        let checkpoints = Collection::<u64>::open(&path, "checkpoints").await.unwrap();
        let event = events.insert(&"event".into()).await.unwrap();
        let insertion = events
            .transaction(|tx| {
                tx.remove(&events, event)?;
                let insertion = tx.insert(&spans, &vec!["span".into()])?;
                assert!(
                    insertion.id().is_none(),
                    "an uncommitted ID is not published"
                );
                tx.insert(&checkpoints, &event)?;
                Ok(insertion)
            })
            .await
            .unwrap();
        let span = insertion.id().unwrap();
        drop((events, spans, checkpoints));
        let events = Collection::<String>::open(&path, "events").await.unwrap();
        let spans = Collection::<Vec<String>>::open(&path, "spans")
            .await
            .unwrap();
        let checkpoints = Collection::<u64>::open(&path, "checkpoints").await.unwrap();
        assert!((&events).into_async_iter().next().await.is_none());
        assert_eq!(spans.get(span).await.unwrap(), Some(vec!["span".into()]));
        assert_eq!(
            (&checkpoints)
                .into_async_iter()
                .next()
                .await
                .unwrap()
                .unwrap()
                .value,
            event
        );
    }

    #[tokio::test]
    async fn failed_transaction_rolls_back_every_collection_tail_and_notification() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shared.sqlite");
        let events = Collection::<String>::open(&path, "events").await.unwrap();
        let spans = Collection::<String>::open(&path, "spans").await.unwrap();
        let checkpoints = Collection::<u64>::open(&path, "checkpoints").await.unwrap();
        let event = events.insert(&"event".into()).await.unwrap();
        let db = Connection::open(&path).unwrap();
        db.execute_batch("CREATE TRIGGER reject_checkpoint BEFORE INSERT ON collection_records WHEN NEW.name='checkpoints' BEGIN SELECT RAISE(FAIL,'checkpoint failure'); END;").unwrap();
        let notified = Arc::new(AtomicBool::new(false));
        let notification = notified.clone();
        let mut insertion = None;
        assert!(events
            .transaction(|tx| {
                tx.drain(&events, ..=event)?;
                insertion = Some(tx.insert(&spans, &"span".into())?);
                tx.stage_storage(&events.path, move |_| {
                    Ok(Some(Box::new(move || {
                        notification.store(true, Ordering::SeqCst);
                    })))
                })?;
                tx.insert(&checkpoints, &event)?;
                Ok(())
            })
            .await
            .is_err());
        assert_eq!(events.get(event).await.unwrap().as_deref(), Some("event"));
        assert_eq!(spans.tail().await.unwrap(), 0);
        assert_eq!(checkpoints.tail().await.unwrap(), 0);
        assert!((&spans).into_async_iter().next().await.is_none());
        assert!((&checkpoints).into_async_iter().next().await.is_none());
        assert!(insertion.unwrap().id().is_none());
        assert!(!notified.load(Ordering::SeqCst));
        db.execute_batch("DROP TRIGGER reject_checkpoint").unwrap();
        events
            .transaction(|tx| {
                tx.drain(&events, ..=event)?;
                tx.insert(&spans, &"span".into())?;
                tx.insert(&checkpoints, &event)?;
                Ok(())
            })
            .await
            .unwrap();
        assert!(events.get(event).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn transactions_reject_other_databases_and_abort_failed_builders() {
        let dir = tempfile::tempdir().unwrap();
        let events = Collection::<String>::open(&dir.path().join("events.sqlite"), "events")
            .await
            .unwrap();
        let elsewhere = Collection::<u64>::open(&dir.path().join("other.sqlite"), "other")
            .await
            .unwrap();
        let event = events.insert(&"keep".into()).await.unwrap();
        assert!(events
            .transaction(|tx| {
                tx.remove(&events, event)?;
                tx.insert(&elsewhere, &event)?;
                Ok(())
            })
            .await
            .is_err());
        let aborted: anyhow::Result<()> = events
            .transaction(|tx| {
                tx.remove(&events, event)?;
                anyhow::bail!("builder failed")
            })
            .await;
        assert!(aborted.is_err());
        assert_eq!(events.get(event).await.unwrap().as_deref(), Some("keep"));
        assert_eq!(elsewhere.tail().await.unwrap(), 0);
    }
}
