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
    sync::{Arc, LazyLock, OnceLock, Weak},
};

/// Schema 1-6 belong to the original SpanOp channel. Schema 7 adds the event
/// collection, replacing NDJSON capture, and the translation state that makes
/// each event-to-SpanOp handoff a single transaction.
pub(super) const SCHEMA: u32 = 7;

/// The current time in milliseconds, as SQL.
pub(super) const NOW_MS: &str = "CAST(unixepoch('subsec') * 1000 AS INTEGER)";

static OPEN: LazyLock<Mutex<HashMap<PathBuf, Weak<Inner>>>> = LazyLock::new(Default::default);
/// Files whose last handle is still compacting and closing its connection.
static CLOSING: LazyLock<Mutex<HashMap<PathBuf, usize>>> = LazyLock::new(Default::default);

struct Inner {
    path: PathBuf,
    connection: std::sync::Mutex<Option<Connection>>,
    channel: OnceLock<Arc<super::span_ops::Channel>>,
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
        let connection = tokio::task::spawn_blocking(move || open_connection(&opened)).await??;
        let mut open = OPEN.lock();
        // A concurrent opener may have published the same file meanwhile.
        if let Some(inner) = open.get(&path).and_then(Weak::upgrade) {
            drop(connection);
            return Ok(Self(inner));
        }
        let inner = Arc::new(Inner {
            path: path.clone(),
            connection: std::sync::Mutex::new(Some(connection)),
            channel: OnceLock::new(),
        });
        open.retain(|_, inner| inner.strong_count() > 0);
        open.insert(path, Arc::downgrade(&inner));
        Ok(Self(inner))
    }

    /// Live delivery state shared by every handle on this database.
    pub fn channel(&self) -> &OnceLock<Arc<super::span_ops::Channel>> {
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
    match migrate(&db) {
        Ok(()) => db.execute_batch("COMMIT")?,
        Err(error) => {
            let _ = db.execute_batch("ROLLBACK");
            return Err(error);
        }
    }
    Ok(db)
}

// Read the version under the same write transaction as every upgrade, so a
// concurrent opener cannot run a stale migration plan.
fn migrate(db: &Connection) -> anyhow::Result<()> {
    let version: u32 = db.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    anyhow::ensure!(
        version <= SCHEMA,
        "unsupported session storage schema {version}"
    );
    if version < 3 {
        db.execute_batch(
            "CREATE TABLE IF NOT EXISTS batches (
                input INTEGER PRIMARY KEY, committed INTEGER NOT NULL DEFAULT 0,
                observation INTEGER NOT NULL DEFAULT 1);
            CREATE TABLE IF NOT EXISTS ops (
                position INTEGER PRIMARY KEY AUTOINCREMENT,
                input INTEGER NOT NULL, value BLOB NOT NULL);
            CREATE INDEX IF NOT EXISTS ops_input ON ops(input);
            CREATE TABLE IF NOT EXISTS heads (
                route TEXT PRIMARY KEY, position INTEGER NOT NULL DEFAULT 0,
                failure TEXT);
            CREATE TABLE IF NOT EXISTS recovery (id INTEGER PRIMARY KEY CHECK(id=1),failure TEXT);",
        )?;
        if version == 1 {
            db.execute_batch(
                "ALTER TABLE batches ADD COLUMN observation INTEGER NOT NULL DEFAULT 1",
            )?;
        }
        db.execute_batch("ALTER TABLE heads ADD COLUMN binding TEXT")?;
    }
    if version < 4 {
        db.execute_batch(
            "CREATE TABLE root_candidates (span_id TEXT PRIMARY KEY, value BLOB NOT NULL)",
        )?;
    }
    if version < 5 {
        db.execute_batch(
            "ALTER TABLE heads ADD COLUMN settings TEXT;
             ALTER TABLE heads ADD COLUMN settings_through INTEGER NOT NULL DEFAULT 0",
        )?;
    }
    if version < 6 {
        db.execute_batch(
            "DELETE FROM ops WHERE input IN (SELECT input FROM batches WHERE committed=0);
            CREATE TABLE receipts (input INTEGER NOT NULL, ordinal INTEGER NOT NULL, PRIMARY KEY(input,ordinal));
            CREATE TABLE publication (id INTEGER PRIMARY KEY CHECK(id=1), tail INTEGER NOT NULL, floor INTEGER NOT NULL);
            INSERT INTO publication VALUES(1,(SELECT coalesce(max(position),0) FROM ops),0);
            CREATE TABLE origins (span_id TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE span_state (span_id TEXT PRIMARY KEY, kind TEXT NOT NULL, value BLOB NOT NULL, end_ms INTEGER);",
        )?;
    }
    if version < 7 {
        // Translation state replaces per-input batches: an input is handed
        // over in one transaction, so only ops a route could already see
        // survive. A partly published input is translated again.
        db.execute_batch(
            "CREATE TABLE translation(id INTEGER PRIMARY KEY CHECK(id=1),
                through INTEGER NOT NULL DEFAULT 0, tail INTEGER NOT NULL DEFAULT 0,
                continuation BLOB, roots BLOB, failure TEXT);
            INSERT INTO translation(id,through,tail,failure) VALUES(1,
                (SELECT coalesce(max(input),0) FROM batches WHERE committed=1),
                (SELECT tail FROM publication WHERE id=1),
                (SELECT failure FROM recovery WHERE id=1));
            DELETE FROM ops WHERE position>(SELECT tail FROM translation);",
        )?;
        super::span_ops::migrate_roots(db)?;
        // Metadata kept for a live session expires once it goes unused.
        db.execute_batch(&format!(
            "ALTER TABLE origins ADD COLUMN ended_ms INTEGER;
            CREATE INDEX origins_ended ON origins(ended_ms);
            ALTER TABLE span_state ADD COLUMN touched_ms INTEGER NOT NULL DEFAULT 0;
            UPDATE span_state SET touched_ms={NOW_MS};
            ALTER TABLE heads ADD COLUMN progress_ms INTEGER NOT NULL DEFAULT 0;
            ALTER TABLE heads ADD COLUMN subscribed INTEGER NOT NULL DEFAULT 0;
            UPDATE heads SET progress_ms={NOW_MS}, subscribed=1;"
        ))?;
        if version < 6 {
            // One bounded pass extracts the metadata a schema-5 channel kept
            // only inside its stored ops.
            super::span_ops::extract_metadata(db)?;
        }
        db.execute_batch(
            "DROP TABLE batches; DROP TABLE receipts; DROP TABLE root_candidates;
            DROP TABLE publication; DROP TABLE recovery;
            DROP INDEX ops_input; ALTER TABLE ops DROP COLUMN input;
            CREATE TABLE events (id INTEGER PRIMARY KEY, value BLOB NOT NULL);
            CREATE TABLE captured (digest BLOB PRIMARY KEY, at_ms INTEGER NOT NULL) WITHOUT ROWID;
            CREATE INDEX captured_at ON captured(at_ms);
            CREATE TABLE event_state (id INTEGER PRIMARY KEY CHECK(id=1),
                source TEXT, session_id TEXT, tail INTEGER NOT NULL DEFAULT 0,
                identity BLOB, legacy_namespace INTEGER NOT NULL DEFAULT 0,
                active_ms INTEGER NOT NULL DEFAULT 0);
            INSERT INTO event_state(id, active_ms) VALUES(1, CAST(unixepoch('subsec') * 1000 AS INTEGER));",
        )?;
    }
    if version < SCHEMA {
        // Opening an up-to-date store writes nothing, so it keeps its age.
        db.execute_batch(&format!("PRAGMA user_version={SCHEMA}"))?;
    }
    Ok(())
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
        &format!("UPDATE event_state SET active_ms={NOW_MS} WHERE id=1"),
        [],
    )
    .map(|_| ())
}
