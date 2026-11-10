//! Typed, lazy persistent collections. Reading never changes durable membership.
pub(crate) mod async_iter;
pub(crate) mod view;

use self::async_iter::{AsyncIterator, IntoAsyncIterator};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    collections::VecDeque,
    marker::PhantomData,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::task::JoinHandle;

type ReadJob = JoinHandle<anyhow::Result<(u64, Vec<Entry<Vec<u8>>>)>>;

#[derive(Debug)]
pub(crate) struct Entry<T> {
    pub id: u64,
    pub value: T,
}

pub(crate) struct Collection<T> {
    database: Arc<Mutex<Connection>>,
    name: Arc<str>,
    marker: PhantomData<fn() -> T>,
}
impl<T> Clone for Collection<T> {
    fn clone(&self) -> Self {
        Self {
            database: self.database.clone(),
            name: self.name.clone(),
            marker: PhantomData,
        }
    }
}

impl<T: Serialize + DeserializeOwned + Send + 'static> Collection<T> {
    pub async fn open(path: &Path, name: &str) -> anyhow::Result<Self> {
        let path = path.to_owned();
        let name: Arc<str> = Arc::from(name);
        let owned_name = name.clone();
        let db = tokio::task::spawn_blocking(move || {
            if let Some(parent) = path.parent() { std::fs::create_dir_all(parent)?; }
            let db = Connection::open(path)?;
            db.busy_timeout(std::time::Duration::from_secs(5))?;
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            loop {
                match db.execute_batch("PRAGMA auto_vacuum=INCREMENTAL; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA journal_size_limit=1048576; BEGIN IMMEDIATE;") {
                    Ok(()) => break,
                    Err(error) if error.sqlite_error_code() == Some(rusqlite::ErrorCode::DatabaseBusy)
                        && std::time::Instant::now() < deadline => std::thread::sleep(std::time::Duration::from_millis(10)),
                    Err(error) => return Err(error.into()),
                }
            }
            db.execute_batch("CREATE TABLE IF NOT EXISTS collection_state(name TEXT PRIMARY KEY,tail INTEGER NOT NULL DEFAULT 0,path TEXT);
                CREATE TABLE IF NOT EXISTS collection_records(name TEXT NOT NULL,id INTEGER NOT NULL,value BLOB NOT NULL,PRIMARY KEY(name,id));
                CREATE TABLE IF NOT EXISTS collection_identity(name TEXT PRIMARY KEY,position INTEGER NOT NULL,value BLOB NOT NULL);")?;
            db.execute("INSERT OR IGNORE INTO collection_state(name) VALUES(?)", [&*owned_name])?;
            db.execute_batch("COMMIT")?;
            Ok::<_,anyhow::Error>(db)
        }).await??;
        Ok(Self {
            database: Arc::new(Mutex::new(db)),
            name,
            marker: PhantomData,
        })
    }

    async fn with_db<R: Send + 'static>(
        &self,
        action: impl FnOnce(&mut Connection, &str) -> anyhow::Result<R> + Send + 'static,
    ) -> anyhow::Result<R> {
        let database = self.database.clone();
        let name = self.name.clone();
        tokio::task::spawn_blocking(move || {
            let mut db = database
                .lock()
                .map_err(|_| anyhow::anyhow!("collection storage lock poisoned"))?;
            action(&mut db, &name)
        })
        .await?
    }

    pub async fn exists(path: &Path, name: &str) -> anyhow::Result<bool> {
        if !tokio::fs::try_exists(path).await? {
            return Ok(false);
        }
        let path = path.to_owned();
        let name = name.to_owned();
        tokio::task::spawn_blocking(move || {
            let db = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
            let table: bool = db.query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='collection_state')",
                [],
                |r| r.get(0),
            )?;
            if !table {
                return Ok(false);
            }
            Ok(db.query_row(
                "SELECT EXISTS(SELECT 1 FROM collection_state WHERE name=?)",
                [name],
                |r| r.get(0),
            )?)
        })
        .await?
    }

    pub async fn set_path(&self, path: &Path) -> anyhow::Result<()> {
        let path = path
            .file_name()
            .and_then(|v| v.to_str())
            .ok_or_else(|| anyhow::anyhow!("journal path is not UTF-8"))?
            .to_owned();
        self.with_db(move |db, name| {
            db.execute(
                "UPDATE collection_state SET path=? WHERE name=? AND path IS NOT ?",
                params![path, name, path],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn set_identity(&self, value: &T, position: u64) -> anyhow::Result<()> {
        let value = serde_json::to_vec(value)?;
        self.with_db(move |db, name| {
            db.execute(
                "INSERT INTO collection_identity VALUES(?,?,?) ON CONFLICT(name) DO UPDATE SET position=excluded.position,value=excluded.value WHERE excluded.position>=collection_identity.position",
                params![name,position,value],
            )?;
            Ok(())
        })
        .await
    }
    pub async fn identity(&self) -> anyhow::Result<Option<T>> {
        self.with_db(|db, name| {
            let bytes: Option<Vec<u8>> = db
                .query_row(
                    "SELECT value FROM collection_identity WHERE name=?",
                    [name],
                    |r| r.get(0),
                )
                .optional()?;
            bytes.map(|v| Ok(serde_json::from_slice(&v)?)).transpose()
        })
        .await
    }

    pub async fn reserve(&self, through: u64) -> anyhow::Result<()> {
        anyhow::ensure!(
            through <= i64::MAX as u64,
            "legacy position exceeds SQLite integer range"
        );
        self.with_db(move |db, name| {
            db.execute(
                "UPDATE collection_state SET tail=max(tail,?) WHERE name=?",
                params![through, name],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn tail(&self) -> anyhow::Result<u64> {
        self.with_db(|db, name| {
            Ok(db.query_row(
                "SELECT tail FROM collection_state WHERE name=?",
                [name],
                |r| r.get(0),
            )?)
        })
        .await
    }

    /// Stable IDs retain the original byte-boundary numbering during migration.
    /// New entries reserve their encoded size; deletion never shifts later IDs.
    pub async fn insert(&self, value: &T) -> anyhow::Result<u64> {
        self.insert_with_identity(value, None).await
    }

    pub async fn insert_with_identity(
        &self,
        value: &T,
        identity: Option<&T>,
    ) -> anyhow::Result<u64> {
        let value = serde_json::to_vec(value)?;
        let identity = identity.map(serde_json::to_vec).transpose()?;
        self.with_db(move |db, name| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let tail: u64 = tx.query_row(
                "SELECT tail FROM collection_state WHERE name=?",
                [name],
                |r| r.get(0),
            )?;
            let id = tail
                .checked_add(value.len() as u64 + 1)
                .filter(|id| *id <= i64::MAX as u64)
                .ok_or_else(|| anyhow::anyhow!("collection ID overflow"))?;
            tx.execute(
                "INSERT INTO collection_records VALUES(?,?,?)",
                params![name, id, value],
            )?;
            tx.execute(
                "UPDATE collection_state SET tail=? WHERE name=?",
                params![id, name],
            )?;
            if let Some(identity) = identity {
                tx.execute(
                    "INSERT INTO collection_identity VALUES(?,?,?) ON CONFLICT(name) DO UPDATE SET position=excluded.position,value=excluded.value WHERE excluded.position>=collection_identity.position",
                    params![name,id,identity],
                )?;
            }
            tx.commit()?;
            Ok(id)
        })
        .await
    }

    #[cfg(test)]
    pub async fn get(&self, id: u64) -> anyhow::Result<Option<T>> {
        self.with_db(move |db, name| {
            let bytes: Option<Vec<u8>> = db
                .query_row(
                    "SELECT value FROM collection_records WHERE name=? AND id=?",
                    params![name, id],
                    |r| r.get(0),
                )
                .optional()?;
            bytes
                .map(|bytes| Ok(serde_json::from_slice(&bytes)?))
                .transpose()
        })
        .await
    }

    #[cfg(test)]
    pub async fn remove(&self, id: u64) -> anyhow::Result<bool> {
        self.with_db(move |db, name| {
            Ok(db.execute(
                "DELETE FROM collection_records WHERE name=? AND id=?",
                params![name, id],
            )? != 0)
        })
        .await
    }

    /// Import a legacy record at its original boundary. Retrying is idempotent.
    pub async fn import(&self, id: u64, value: &T) -> anyhow::Result<()> {
        let value = serde_json::to_vec(value)?;
        self.with_db(move |db, name| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let prior: Option<Vec<u8>> = tx
                .query_row(
                    "SELECT value FROM collection_records WHERE name=? AND id=?",
                    params![name, id],
                    |r| r.get(0),
                )
                .optional()?;
            if let Some(prior) = prior {
                anyhow::ensure!(prior == value, "legacy record changed during migration");
            } else {
                tx.execute(
                    "INSERT INTO collection_records VALUES(?,?,?)",
                    params![name, id, value],
                )?;
            }
            tx.execute(
                "UPDATE collection_state SET tail=max(tail,?) WHERE name=?",
                params![id, name],
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
    }

    pub fn range(&self, after: u64, through: u64) -> Iter<T> {
        Iter {
            collection: self.clone(),
            after,
            through: Some(through),
            buffer: VecDeque::new(),
            task: None,
            done: false,
        }
    }
}

pub(crate) struct Iter<T> {
    collection: Collection<T>,
    after: u64,
    through: Option<u64>,
    buffer: VecDeque<Entry<Vec<u8>>>,
    task: Option<ReadJob>,
    done: bool,
}
impl<T> Unpin for Iter<T> {}
impl<T: Serialize + DeserializeOwned + Send + 'static> IntoAsyncIterator for &Collection<T> {
    type Item = anyhow::Result<Entry<T>>;
    type IntoAsyncIter = Iter<T>;
    fn into_async_iter(self) -> Self::IntoAsyncIter {
        Iter {
            collection: self.clone(),
            after: 0,
            through: None,
            buffer: VecDeque::new(),
            task: None,
            done: false,
        }
    }
}
impl<T: Serialize + DeserializeOwned + Send + 'static> AsyncIterator for Iter<T> {
    type Item = anyhow::Result<Entry<T>>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use std::future::Future;
        let this = self.get_mut();
        loop {
            if let Some(entry) = this.buffer.pop_front() {
                this.after = entry.id;
                let value = serde_json::from_slice(&entry.value).map_err(anyhow::Error::from);
                if value.is_err() {
                    this.done = true;
                    this.buffer.clear();
                }
                return Poll::Ready(Some(value.map(|value| Entry {
                    id: entry.id,
                    value,
                })));
            }
            if this.done {
                return Poll::Ready(None);
            }
            if this.task.is_none() {
                let collection = this.collection.clone();
                let after = this.after;
                let through = this.through;
                this.task = Some(tokio::spawn(async move {
                    collection.with_db(move |db,name| {
                        let upper: u64 = match through { Some(upper)=>upper, None=>db.query_row("SELECT tail FROM collection_state WHERE name=?",[name],|r|r.get(0))? };
                        let mut query = db.prepare("SELECT id,value FROM collection_records WHERE name=? AND id>? AND id<=? ORDER BY id LIMIT 32")?;
                        let mut rows = query.query(params![name,after,upper])?;
                        let mut records = Vec::new();
                        let mut bytes = 0;
                        while let Some(row) = rows.next()? {
                            let value: Vec<u8> = row.get(1)?;
                            if !records.is_empty() && bytes + value.len() > 256*1024 { break; }
                            bytes += value.len();
                            records.push(Entry { id: row.get(0)?, value });
                        }
                        Ok((upper, records))
                    }).await
                }));
            }
            let Some(task) = this.task.as_mut() else {
                return Poll::Pending;
            };
            match Pin::new(task).poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => {
                    this.task = None;
                    match result.map_err(anyhow::Error::from).and_then(|v| v) {
                        Ok((upper, records)) => {
                            this.through = Some(upper);
                            this.done = records.is_empty();
                            this.buffer = records.into();
                        }
                        Err(error) => {
                            this.done = true;
                            return Poll::Ready(Some(Err(error)));
                        }
                    }
                }
            }
        }
    }
}

/// Canonical pipeline path for source-qualified legacy journals; other callers
/// (including legacy fixtures) get an adjacent private SQLite file.
pub(crate) fn database_path(journal: &Path) -> PathBuf {
    let id = journal
        .file_stem()
        .and_then(|v| v.to_str())
        .and_then(|v| v.rsplit_once("--").map(|(_, id)| id));
    if let Some(id) = id.filter(|id| uuid::Uuid::parse_str(id).is_ok()) {
        if let Some(data) = journal.parent().and_then(Path::parent) {
            return data.join("pipeline").join(format!("{id}.sqlite"));
        }
    }
    journal.with_extension("events.sqlite")
}

pub(crate) async fn journal_paths(data_dir: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut paths = std::collections::BTreeSet::new();
    if let Ok(mut entries) = tokio::fs::read_dir(data_dir.join("journal")).await {
        while let Some(entry) = entries.next_entry().await? {
            if entry.path().extension().is_some_and(|v| v == "ndjson") {
                paths.insert(entry.path());
            }
        }
    }
    if let Ok(mut entries) = tokio::fs::read_dir(data_dir.join("pipeline")).await {
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().is_none_or(|v| v != "sqlite") {
                continue;
            }
            let display_path = path.clone();
            let found = tokio::task::spawn_blocking(move || -> anyhow::Result<Option<String>> {
                let db =
                    Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
                let exists: bool = db.query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='collection_state')",
                    [],
                    |r| r.get(0),
                )?;
                if !exists {
                    return Ok(None);
                }
                Ok(db
                    .query_row(
                        "SELECT path FROM collection_state WHERE name='events'",
                        [],
                        |r| r.get(0),
                    )
                    .optional()?
                    .flatten())
            })
            .await?;
            let found = match found {
                Ok(found) => found,
                Err(error) => {
                    tracing::warn!(%error,path=%display_path.display(),"journal catalog could not inspect session storage");
                    continue;
                }
            };
            if let Some(path) = found {
                let path = Path::new(&path)
                    .file_name()
                    .ok_or_else(|| anyhow::anyhow!("invalid stored journal name"))?;
                paths.insert(data_dir.join("journal").join(path));
            }
        }
    }
    Ok(paths.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::async_iter::{AsyncIteratorExt, IntoAsyncIterator};
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn independent_iterators_and_explicit_durable_removal() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store.sqlite");
        let collection = Collection::<serde_json::Value>::open(&path, "events")
            .await
            .unwrap();
        let first = collection.insert(&json!({"one": 1})).await.unwrap();
        let second = collection.insert(&json!({"two": 2})).await.unwrap();
        let mut a = (&collection).into_async_iter();
        let mut b = (&collection).into_async_iter();
        assert_eq!(a.next().await.unwrap().unwrap().id, first);
        assert_eq!(b.next().await.unwrap().unwrap().id, first);
        drop(a);
        drop(b);
        assert!(collection.get(first).await.unwrap().is_some());
        assert!(collection.remove(first).await.unwrap());
        assert!(!collection.remove(first).await.unwrap());
        let mut iter = (&collection).into_async_iter();
        assert_eq!(iter.next().await.unwrap().unwrap().id, second);
        let third = collection.insert(&json!({"three": 3})).await.unwrap();
        assert!(third > second);
        assert!(iter.next().await.is_none());
        let reopened = Collection::<serde_json::Value>::open(&path, "events")
            .await
            .unwrap();
        assert!(reopened.get(first).await.unwrap().is_none());
        assert_eq!(reopened.get(second).await.unwrap(), Some(json!({"two": 2})));
        assert_eq!(reopened.tail().await.unwrap(), third);
    }

    #[tokio::test]
    async fn valid_prefix_survives_later_corruption_and_failed_removal() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store.sqlite");
        let collection = Collection::<String>::open(&path, "events").await.unwrap();
        let first = collection.insert(&"first".into()).await.unwrap();
        let second = collection.insert(&"second".into()).await.unwrap();
        let db = Connection::open(&path).unwrap();
        db.execute(
            "UPDATE collection_records SET value=x'78' WHERE id=?",
            [second],
        )
        .unwrap();
        db.execute_batch("CREATE TRIGGER reject_delete BEFORE DELETE ON collection_records BEGIN SELECT RAISE(FAIL,'injected removal failure'); END;").unwrap();
        assert!(collection.remove(first).await.is_err());
        assert_eq!(
            collection.get(first).await.unwrap().as_deref(),
            Some("first")
        );
        let mut iter = (&collection).into_async_iter();
        assert_eq!(iter.next().await.unwrap().unwrap().value, "first");
        assert!(iter.next().await.unwrap().is_err());
        assert!(iter.next().await.is_none());
    }

    #[tokio::test]
    async fn legacy_import_is_idempotent_and_ids_are_never_recycled() {
        let dir = tempfile::tempdir().unwrap();
        let collection = Collection::<String>::open(&dir.path().join("store.sqlite"), "events")
            .await
            .unwrap();
        collection.import(100, &"old".into()).await.unwrap();
        collection.import(100, &"old".into()).await.unwrap();
        assert!(collection.import(100, &"changed".into()).await.is_err());
        collection.remove(100).await.unwrap();
        assert!(collection.insert(&"new".into()).await.unwrap() > 100);
    }
}
