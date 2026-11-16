//! The only owner of SQLite connections. Every collection stored in one session
//! database shares one connection, so a multi-collection transfer is a single
//! local transaction and closing the last handle releases the file.
use parking_lot::Mutex;
use rusqlite::{
    types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef},
    Connection, Params,
};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, LazyLock, Weak},
};

/// Schema 1-6 belong to the original SpanOp channel. Schema 7 adds the event
/// collection, replacing NDJSON capture, and one session row that makes each
/// event-to-SpanOp handoff a single transaction.
pub(super) const SCHEMA: u32 = 7;

/// The current time in milliseconds, as SQL.
pub(super) const NOW_MS: &str = "CAST(unixepoch('subsec') * 1000 AS INTEGER)";

static OPEN: LazyLock<Mutex<HashMap<PathBuf, Weak<Inner>>>> = LazyLock::new(Default::default);
/// Files whose last handle is still compacting and closing its connection.
static CLOSING: LazyLock<Mutex<HashMap<PathBuf, usize>>> = LazyLock::new(Default::default);

struct Inner {
    path: PathBuf,
    connection: std::sync::Mutex<Option<Connection>>,
    channel: super::span_ops::Channel,
}

impl Drop for Inner {
    fn drop(&mut self) {
        *CLOSING.lock().entry(self.path.clone()).or_default() += 1;
        self.close();
        let mut closing = CLOSING.lock();
        if let Some(count) = closing.get_mut(&self.path) {
            *count -= 1;
            if *count == 0 {
                closing.remove(&self.path);
            }
        }
    }
}

impl Inner {
    fn close(&mut self) {
        let connection = match self.connection.get_mut() {
            Ok(connection) => connection.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        };
        if let Some(connection) = connection {
            // Reclaim a bounded number of free pages and leave no WAL behind.
            if let Err(error) = connection
                .execute_batch("PRAGMA incremental_vacuum(256); PRAGMA wal_checkpoint(TRUNCATE)")
            {
                tracing::warn!(%error, path = %self.path.display(), "session storage could not be compacted on close");
            }
            if let Err((_, error)) = connection.close() {
                tracing::warn!(%error, path = %self.path.display(), "session storage did not close cleanly");
            }
        }
    }
}

#[derive(Clone)]
pub(super) struct Store(Arc<Inner>);

impl Store {
    /// Open (or share) the database at `path`, creating and upgrading its schema.
    pub async fn open(path: &Path) -> anyhow::Result<Self> {
        let path = normalize(path).await?;
        if let Some(store) = Self::shared(&path) {
            return Ok(store);
        }
        let opened = path.clone();
        let (connection, tail) = tokio::task::spawn_blocking(move || {
            let db = open_connection(&opened)?;
            let tail = db.one("SELECT op_tail FROM session WHERE id=1", [])?;
            anyhow::Ok((db, tail))
        })
        .await??;
        let mut open = OPEN.lock();
        // A concurrent opener may have published the same file meanwhile.
        if let Some(inner) = open.get(&path).and_then(Weak::upgrade) {
            drop(connection);
            return Ok(Self(inner));
        }
        let inner = Arc::new(Inner {
            path: path.clone(),
            connection: std::sync::Mutex::new(Some(connection)),
            channel: super::span_ops::Channel::new(tail),
        });
        open.retain(|_, inner| inner.strong_count() > 0);
        open.insert(path, Arc::downgrade(&inner));
        Ok(Self(inner))
    }

    /// Live delivery state shared by every handle on this database.
    pub fn channel(&self) -> &super::span_ops::Channel {
        &self.0.channel
    }

    fn shared(path: &Path) -> Option<Self> {
        OPEN.lock().get(path).and_then(Weak::upgrade).map(Self)
    }

    pub async fn run<R: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut Connection) -> anyhow::Result<R> + Send + 'static,
    ) -> anyhow::Result<R> {
        let inner = self.0.clone();
        tokio::task::spawn_blocking(move || {
            let mut connection = inner
                .connection
                .lock()
                .map_err(|_| anyhow::anyhow!("session storage lock poisoned"))?;
            operation(
                connection
                    .as_mut()
                    .ok_or_else(|| anyhow::anyhow!("session storage is closed"))?,
            )
        })
        .await?
    }

    #[cfg(test)]
    pub fn lock(&self) -> std::sync::MutexGuard<'_, Option<Connection>> {
        match self.0.connection.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// One immediate transaction: writers acquire the lock before reading.
    pub async fn write<R: Send + 'static>(
        &self,
        operation: impl FnOnce(&rusqlite::Transaction<'_>) -> anyhow::Result<R> + Send + 'static,
    ) -> anyhow::Result<R> {
        self.run(move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let result = operation(&tx)?;
            tx.commit()?;
            Ok(result)
        })
        .await
    }
}

/// Delete a database and its sidecars only if no handle is open in this
/// process. Holding the registry lock keeps a concurrent open from racing it.
pub(super) async fn remove(path: &Path) -> anyhow::Result<bool> {
    let path = normalize(path).await?;
    let open = OPEN.lock();
    if open
        .get(&path)
        .is_some_and(|inner| inner.strong_count() > 0)
        || CLOSING.lock().contains_key(&path)
    {
        return Ok(false);
    }
    for suffix in ["", "-wal", "-shm"] {
        let mut file = path.clone().into_os_string();
        file.push(suffix);
        match std::fs::remove_file(&file) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(true)
}

async fn normalize(path: &Path) -> anyhow::Result<PathBuf> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("storage path has no parent"))?;
    let name = path
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("storage path has no file name"))?;
    tokio::fs::create_dir_all(parent).await?;
    Ok(tokio::fs::canonicalize(parent).await?.join(name))
}

pub(super) fn open_connection(path: &Path) -> anyhow::Result<Connection> {
    let db = Connection::open(path)?;
    db.busy_timeout(std::time::Duration::from_secs(5))?;
    // Concurrent WAL transitions can return BUSY without invoking SQLite's
    // busy handler. Retry initialization before any schema transaction starts.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        match db.execute_batch(
            "PRAGMA auto_vacuum=INCREMENTAL; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
             PRAGMA journal_size_limit=1048576; BEGIN IMMEDIATE;",
        ) {
            Ok(()) => break,
            Err(error)
                if error.sqlite_error_code() == Some(rusqlite::ErrorCode::DatabaseBusy)
                    && std::time::Instant::now() < deadline =>
            {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            Err(error) => return Err(error.into()),
        }
    }
    match super::upgrade::migrate(&db) {
        Ok(()) => db.execute_batch("COMMIT")?,
        Err(error) => {
            let _ = db.execute_batch("ROLLBACK");
            return Err(error);
        }
    }
    Ok(db)
}

/// A column holding a JSON-encoded value, stored as text.
pub(super) struct Json<T>(pub T);

impl<T: serde::de::DeserializeOwned> FromSql for Json<T> {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        serde_json::from_slice(value.as_bytes()?)
            .map(Json)
            .map_err(|error| FromSqlError::Other(Box::new(error)))
    }
}

impl<T: serde::Serialize> ToSql for Json<T> {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        serde_json::to_string(&self.0)
            .map(ToSqlOutput::from)
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
    }
}

/// Read the first column of the one row a query selects.
pub(super) trait One {
    fn one<T: FromSql>(&self, sql: &str, params: impl Params) -> rusqlite::Result<T>;
}

impl One for Connection {
    fn one<T: FromSql>(&self, sql: &str, params: impl Params) -> rusqlite::Result<T> {
        self.query_row(sql, params, |row| row.get(0))
    }
}

/// Storage errors that will not succeed on retry without changing the data.
pub(crate) fn is_corruption(error: &anyhow::Error) -> bool {
    error.downcast_ref::<rusqlite::Error>().is_some_and(|error| {
        matches!(error, rusqlite::Error::SqliteFailure(code, _)
            if matches!(code.code, rusqlite::ErrorCode::DatabaseCorrupt | rusqlite::ErrorCode::NotADatabase))
    })
}

/// Record pipeline progress. Opening or inspecting storage never counts as
/// activity, so retention measures how long the session has been cold.
pub(super) fn touch(tx: &Connection) -> rusqlite::Result<()> {
    tx.execute(
        &format!("UPDATE session SET active_ms={NOW_MS} WHERE id=1"),
        [],
    )
    .map(|_| ())
}

/// Read records in order until they reach `budget` bytes or `window` elapses,
/// always returning at least one. Column `value` of `sql` holds each record's
/// encoded bytes; `decode` builds the record from its row.
pub(super) fn read_batch<R>(
    db: &Connection,
    sql: &str,
    params: impl Params,
    value: usize,
    budget: usize,
    window: std::time::Duration,
    mut decode: impl FnMut(&rusqlite::Row<'_>, &[u8]) -> anyhow::Result<R>,
) -> anyhow::Result<Vec<R>> {
    let mut statement = db.prepare_cached(sql)?;
    let mut rows = statement.query(params)?;
    let started = std::time::Instant::now();
    let mut records = Vec::new();
    let mut bytes = 0;
    while let Some(row) = rows.next()? {
        let encoded = row.get_ref(value)?.as_blob()?;
        if !records.is_empty() && bytes + encoded.len() > budget {
            break;
        }
        bytes += encoded.len();
        records.push(decode(row, encoded)?);
        if bytes >= budget || started.elapsed() >= window {
            break;
        }
    }
    Ok(records)
}
