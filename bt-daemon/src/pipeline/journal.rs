//! Concrete durable channel: one ordered SpanOp log and a head per route.
use super::{
    delivery::InputPort,
    recovery::{Boundary, Failure, Recovery, RecoveryOutcome},
    Completion, Delivery, Stage,
};
use crate::{
    translate::SpanOp,
    wire::{RecoveryInfo, SessionConfig, SessionRoute},
};
use parking_lot::Mutex;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex as StorageMutex,
    },
};
use tokio::sync::watch;

#[derive(Debug)]
pub(crate) struct BindingChanged;
impl std::fmt::Display for BindingChanged {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            "credential resolution changed the durable head's pinned backend or organization",
        )
    }
}
impl std::error::Error for BindingChanged {}

#[derive(Clone)]
pub(crate) struct SpanJournal {
    connection: Arc<StorageMutex<Option<Connection>>>,
    changed: watch::Sender<u64>,
}

/// Explicit codec preserves fields intentionally omitted from wire/plugin JSON.
#[derive(Serialize, Deserialize)]
struct StoredOp {
    version: u32,
    op: SpanOp,
    late_merge_key: Option<String>,
    turn_span_id: Option<String>,
    is_turn: bool,
}

impl StoredOp {
    fn encode(op: &SpanOp) -> anyhow::Result<Vec<u8>> {
        let (SpanOp::Insert(row) | SpanOp::Merge(row)) = op;
        Ok(serde_json::to_vec(&Self {
            version: 1,
            op: op.clone(),
            late_merge_key: row.late_merge_key.clone(),
            turn_span_id: row.turn_span_id.clone(),
            is_turn: row.is_turn,
        })?)
    }
    fn decode(bytes: &[u8]) -> anyhow::Result<SpanOp> {
        let mut stored: Self = serde_json::from_slice(bytes)?;
        anyhow::ensure!(
            stored.version == 1,
            "unsupported SpanOp codec {}",
            stored.version
        );
        let (SpanOp::Insert(row) | SpanOp::Merge(row)) = &mut stored.op;
        row.late_merge_key = stored.late_merge_key;
        row.turn_span_id = stored.turn_span_id;
        row.is_turn = stored.is_turn;
        Ok(stored.op)
    }
}

impl SpanJournal {
    pub async fn open(path: &Path) -> anyhow::Result<Self> {
        let path = path.to_owned();
        let connection = tokio::task::spawn_blocking(move || -> anyhow::Result<(Connection, u64)> {
            let parent = path.parent().ok_or_else(|| anyhow::anyhow!("SpanOp journal path has no parent"))?;
            std::fs::create_dir_all(parent)?;
            let db = Connection::open(path)?;
            db.busy_timeout(std::time::Duration::from_secs(5))?;
            let version: u32 = db.query_row("PRAGMA user_version", [], |row| row.get(0))?;
            anyhow::ensure!(version <= 4, "unsupported SpanOp journal schema {version}");
            db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")?;
            if version < 3 {
                db.execute_batch(
                    "BEGIN IMMEDIATE;
                    CREATE TABLE IF NOT EXISTS batches (
                        input INTEGER PRIMARY KEY, committed INTEGER NOT NULL DEFAULT 0,
                        observation INTEGER NOT NULL DEFAULT 1);
                    CREATE TABLE IF NOT EXISTS ops (
                        position INTEGER PRIMARY KEY AUTOINCREMENT,
                        input INTEGER NOT NULL, value BLOB NOT NULL);
                    CREATE INDEX IF NOT EXISTS ops_input ON ops(input);
                    CREATE TABLE IF NOT EXISTS heads (
                        route TEXT PRIMARY KEY, position INTEGER NOT NULL DEFAULT 0,
                        failure TEXT);
                    CREATE TABLE IF NOT EXISTS recovery (id INTEGER PRIMARY KEY CHECK(id=1),failure TEXT);
                    ",
                )?;
                if version == 1 {
                    db.execute_batch(
                        "ALTER TABLE batches ADD COLUMN observation INTEGER NOT NULL DEFAULT 1",
                    )?;
                }
                db.execute_batch("ALTER TABLE heads ADD COLUMN binding TEXT")?;
                db.execute_batch("PRAGMA user_version=3; COMMIT")?;
            }
            if version < 4 {
                db.execute_batch("BEGIN IMMEDIATE; CREATE TABLE root_candidates (span_id TEXT PRIMARY KEY, value BLOB NOT NULL); PRAGMA user_version=4; COMMIT")?;
            }
            let tail = db.query_row("SELECT coalesce(max(position),0) FROM ops JOIN batches USING(input) WHERE committed=1", [], |row| row.get(0))?;
            Ok((db, tail))
        })
        .await??;
        let (connection, tail) = connection;
        let (changed, _) = watch::channel(tail);
        Ok(Self {
            connection: Arc::new(StorageMutex::new(Some(connection))),
            changed,
        })
    }

    async fn with_db<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut Connection) -> anyhow::Result<T> + Send + 'static,
    ) -> anyhow::Result<T> {
        let db = self.connection.clone();
        tokio::task::spawn_blocking(move || {
            let mut db = db
                .lock()
                .map_err(|_| anyhow::anyhow!("SpanOp storage lock poisoned"))?;
            operation(
                db.as_mut()
                    .ok_or_else(|| anyhow::anyhow!("SpanOp journal is closed"))?,
            )
        })
        .await?
    }

    /// Closing the shared cell also covers cancelled reads whose blocking
    /// jobs can outlive their actor. Queued jobs cannot reopen the connection.
    pub async fn close(&self) -> anyhow::Result<()> {
        let connection = self.connection.clone();
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            let database = connection
                .lock()
                .map_err(|_| anyhow::anyhow!("SpanOp storage lock poisoned"))?
                .take();
            if let Some(database) = database {
                database.close().map_err(|(_, error)| error)?;
            }
            Ok(())
        })
        .await?
    }

    pub async fn translated_through(&self) -> anyhow::Result<u64> {
        self.with_db(|db| {
            Ok(db.query_row(
                "SELECT coalesce(max(input),0) FROM batches WHERE committed=1",
                [],
                |row| row.get(0),
            )?)
        })
        .await
    }

    /// Restart abandons only invisible chunks; a committed identity is never rewritten.
    pub async fn begin(&self, input: u64, observation: bool) -> anyhow::Result<bool> {
        self.with_db(move |db| {
            let tx = db.transaction()?;
            let done: Option<bool> = tx
                .query_row(
                    "SELECT committed FROM batches WHERE input=?",
                    [input],
                    |row| row.get(0),
                )
                .optional()?;
            if done == Some(true) {
                return Ok(false);
            }
            tx.execute("DELETE FROM ops WHERE input=?", [input])?;
            tx.execute(
                "INSERT OR REPLACE INTO batches(input,committed,observation) VALUES (?,0,?)",
                params![input, observation],
            )?;
            tx.commit()?;
            Ok(true)
        })
        .await
    }

    pub async fn append(&self, input: u64, ops: &[SpanOp]) -> anyhow::Result<()> {
        let encoded = ops
            .iter()
            .map(StoredOp::encode)
            .collect::<anyhow::Result<Vec<_>>>()?;
        self.with_db(move |db| {
            let tx = db.transaction()?;
            let done: bool = tx.query_row(
                "SELECT committed FROM batches WHERE input=?",
                [input],
                |r| r.get(0),
            )?;
            anyhow::ensure!(!done, "cannot change a committed translation input");
            for value in encoded {
                tx.execute(
                    "INSERT INTO ops(input,value) VALUES (?,?)",
                    params![input, value],
                )?;
            }
            tx.commit()?;
            Ok(())
        })
        .await
    }

    pub async fn commit(&self, input: u64) -> anyhow::Result<u64> {
        self.commit_output(input, None).await
    }

    /// Root candidates and translated output share the consumed-input boundary.
    pub async fn commit_with_roots(
        &self,
        input: u64,
        roots: &BTreeMap<String, Vec<SpanOp>>,
    ) -> anyhow::Result<u64> {
        let roots = roots
            .iter()
            .map(|(id, ops)| {
                let ops = ops
                    .iter()
                    .map(StoredOp::encode)
                    .collect::<anyhow::Result<Vec<_>>>()?;
                Ok((id.clone(), serde_json::to_vec(&ops)?))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        self.commit_output(input, Some(roots)).await
    }

    pub async fn root_candidates(&self) -> anyhow::Result<BTreeMap<String, Vec<SpanOp>>> {
        self.with_db(|db| {
            let mut query = db.prepare("SELECT span_id,value FROM root_candidates")?;
            let roots = query
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
                })?
                .map(|row| {
                    let (id, bytes) = row?;
                    let ops: Vec<Vec<u8>> = serde_json::from_slice(&bytes)?;
                    Ok((
                        id,
                        ops.iter()
                            .map(|bytes| StoredOp::decode(bytes))
                            .collect::<anyhow::Result<Vec<_>>>()?,
                    ))
                })
                .collect();
            roots
        })
        .await
    }

    async fn commit_output(
        &self,
        input: u64,
        roots: Option<Vec<(String, Vec<u8>)>>,
    ) -> anyhow::Result<u64> {
        let tail = self
            .with_db(move |db| {
                let tx = db.transaction()?;
                let previous: u64 = tx.query_row(
                    "SELECT coalesce(max(input),0) FROM batches WHERE committed=1",
                    [],
                    |r| r.get(0),
                )?;
                anyhow::ensure!(input >= previous, "translation commits must be ordered");
                let earlier: u64 = tx.query_row(
                    "SELECT count(*) FROM batches WHERE committed=0 AND input<?",
                    [input],
                    |row| row.get(0),
                )?;
                anyhow::ensure!(
                    earlier == 0,
                    "translation cannot publish past an unfinished input"
                );
                anyhow::ensure!(
                    tx.execute("UPDATE batches SET committed=1 WHERE input=?", [input])? == 1,
                    "missing translation batch"
                );
                if let Some(roots) = roots {
                    tx.execute("DELETE FROM root_candidates", [])?;
                    for (id, value) in roots {
                        tx.execute(
                            "INSERT INTO root_candidates(span_id,value) VALUES (?,?)",
                            params![id, value],
                        )?;
                    }
                }
                let tail = tx.query_row(
                    "SELECT coalesce(max(position),0) FROM ops WHERE input<=?",
                    [input],
                    |r| r.get(0),
                )?;
                tx.commit()?;
                Ok(tail)
            })
            .await?;
        self.changed.send_replace(tail);
        Ok(tail)
    }

    pub async fn subscribe(
        &self,
        route: &SessionRoute,
        legacy_through: u64,
    ) -> anyhow::Result<Cursor> {
        let route = serde_json::to_string(route)?;
        let key = route.clone();
        let position = self
            .with_db(move |db| {
                let tx = db.transaction()?;
                // Seed only a new head, using released event checkpoints where provable.
                let legacy: u64 = tx.query_row(
                    "SELECT coalesce(max(position),0) FROM ops JOIN batches USING(input)
                WHERE input<=? AND committed=1",
                    [legacy_through],
                    |r| r.get(0),
                )?;
                tx.execute(
                    "INSERT OR IGNORE INTO heads(route,position) VALUES (?,?)",
                    params![key, legacy],
                )?;
                let position =
                    tx.query_row("SELECT position FROM heads WHERE route=?", [key], |r| {
                        r.get(0)
                    })?;
                tx.commit()?;
                Ok(position)
            })
            .await?;
        let (progress, _) = watch::channel(position);
        Ok(Cursor {
            journal: self.clone(),
            route,
            read: position,
            completed: position,
            changes: self.changed.subscribe(),
            progress,
            abandoned: Arc::new(AtomicBool::new(false)),
            settings: None,
            version: String::new(),
            restored: false,
            error: Arc::new(Mutex::new(None)),
            recovery: Arc::new(Mutex::new(None)),
        })
    }

    pub async fn read_after(&self, after: u64, limit: usize) -> anyhow::Result<Vec<(u64, SpanOp)>> {
        self.with_db(move |db| {
            let mut statement = db.prepare("SELECT position,value FROM ops JOIN batches USING(input) WHERE committed=1 AND observation=1 AND position>? ORDER BY position LIMIT ?")?;
            let rows = statement.query_map(params![after,limit as u64], |r| Ok((r.get::<_,u64>(0)?,r.get::<_,Vec<u8>>(1)?)))?;
            rows.map(|row| { let (position,bytes)=row?; Ok((position,StoredOp::decode(&bytes)?)) }).collect()
        }).await
    }

    pub async fn tail(&self) -> anyhow::Result<u64> {
        self.with_db(|db| {
            Ok(db.query_row(
                "SELECT coalesce(max(position),0) FROM ops JOIN batches USING(input)
            WHERE committed=1",
                [],
                |r| r.get(0),
            )?)
        })
        .await
    }

    /// A completed subscription can remain dormant until its source has new work.
    pub async fn route_complete(&self, route: &SessionRoute, input: u64) -> anyhow::Result<bool> {
        let route = route.clone();
        self.with_db(move |db| {
            let committed: u64 = db.query_row("SELECT coalesce(max(input),0) FROM batches WHERE committed=1",[],|r|r.get(0))?;
            if committed < input {return Ok(false);}
            let tail: u64 = db.query_row("SELECT coalesce(max(position),0) FROM ops JOIN batches USING(input) WHERE committed=1",[],|r|r.get(0))?;
            let mut query = db.prepare("SELECT route FROM heads WHERE failure IS NULL AND position>=?")?;
            for stored in query.query_map([tail],|r|r.get::<_,String>(0))? {
                let stored: SessionRoute = serde_json::from_str(&stored?)?;
                if stored.same_route(&route) {return Ok(true);}
            }
            Ok(false)
        }).await
    }

    /// Tokens can change; a durable head's backend and organization cannot.
    pub async fn bind(&self, route: &str, config: &SessionConfig) -> anyhow::Result<()> {
        let route = route.to_owned();
        let binding = serde_json::to_string(&serde_json::json!({
            "api_url": config.auth.api_url.as_deref().unwrap_or("https://api.braintrust.dev"),
            "org_id":config.auth.org_id,"org_name":config.auth.org_name,
        }))?;
        self.with_db(move |db| {
            let previous: Option<String> =
                db.query_row("SELECT binding FROM heads WHERE route=?", [&route], |r| {
                    r.get(0)
                })?;
            anyhow::ensure!(
                previous
                    .as_ref()
                    .is_none_or(|previous| previous == &binding),
                BindingChanged
            );
            if previous.is_none() {
                db.execute(
                    "UPDATE heads SET binding=? WHERE route=?",
                    params![binding, route],
                )?;
            }
            Ok(())
        })
        .await
    }
    pub async fn routes(&self) -> anyhow::Result<Vec<SessionRoute>> {
        self.with_db(|db| {
            let mut query = db.prepare("SELECT route FROM heads ORDER BY route")?;
            let rows = query
                .query_map([], |r| r.get::<_, String>(0))?
                .map(|route| Ok(serde_json::from_str(&route?)?))
                .collect();
            rows
        })
        .await
    }
    /// Whole-partition retention only; an empty subscriber set is not completion.
    pub async fn collectible(&self, input: u64) -> anyhow::Result<bool> {
        self.with_db(move |db| Ok(db.query_row("SELECT EXISTS(SELECT 1 FROM heads)
            AND NOT EXISTS(SELECT 1 FROM heads WHERE failure IS NOT NULL OR position < (SELECT coalesce(max(position),0) FROM ops JOIN batches USING(input) WHERE committed=1))
            AND NOT EXISTS(SELECT 1 FROM recovery WHERE failure IS NOT NULL)
            AND NOT EXISTS(SELECT 1 FROM batches WHERE committed=0)
            AND (SELECT coalesce(max(input),0) FROM batches WHERE committed=1)>=?",[input],|r|r.get(0))?)).await
    }
    pub async fn close_storage(&self) -> anyhow::Result<()> {
        self.with_db(|db| {
            db.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;
            Ok(())
        })
        .await
    }

    pub async fn source_failure(&self) -> anyhow::Result<Option<String>> {
        self.with_db(|db| {
            Ok(db
                .query_row("SELECT failure FROM recovery WHERE id=1", [], |row| {
                    row.get(0)
                })
                .optional()?
                .flatten())
        })
        .await
    }
    pub async fn set_source_failure(&self, failure: Option<String>) -> anyhow::Result<()> {
        self.with_db(move |db| {
            db.execute(
                "INSERT OR REPLACE INTO recovery(id,failure) VALUES(1,?)",
                [failure],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn failure(&self, route: &str) -> anyhow::Result<Option<String>> {
        let route = route.to_owned();
        self.with_db(move |db| {
            Ok(
                db.query_row("SELECT failure FROM heads WHERE route=?", [route], |r| {
                    r.get(0)
                })?,
            )
        })
        .await
    }

    pub async fn set_failure(&self, route: &str, failure: Option<String>) -> anyhow::Result<()> {
        let route = route.to_owned();
        self.with_db(move |db| {
            db.execute(
                "UPDATE heads SET failure=? WHERE route=?",
                params![failure, route],
            )?;
            Ok(())
        })
        .await
    }
}

pub(crate) struct Cursor {
    journal: SpanJournal,
    pub route: String,
    read: u64,
    completed: u64,
    changes: watch::Receiver<u64>,
    progress: watch::Sender<u64>,
    abandoned: Arc<AtomicBool>,
    settings: Option<watch::Receiver<SessionConfig>>,
    version: String,
    restored: bool,
    error: Arc<Mutex<Option<String>>>,
    recovery: Arc<Mutex<Option<RecoveryInfo>>>,
}

/// The composition layer keeps this status handle; downstream stages only get
/// the InputPort and managed deliveries.
#[derive(Clone)]
pub(crate) struct Progress {
    completed: watch::Receiver<u64>,
    published: watch::Receiver<u64>,
}
#[cfg(test)]
impl Default for Progress {
    fn default() -> Self {
        Self {
            completed: watch::channel(0).1,
            published: watch::channel(0).1,
        }
    }
}
impl Progress {
    pub fn completed(&self) -> u64 {
        *self.completed.borrow()
    }
    pub fn pending(&self) -> u64 {
        self.published.borrow().saturating_sub(self.completed())
    }

    pub async fn through(&self, target: u64, error: &Mutex<Option<String>>) -> Result<(), String> {
        let mut completed = self.completed.clone();
        loop {
            if *completed.borrow_and_update() >= target {
                return Ok(());
            }
            if let Some(error) = error.lock().clone() {
                return Err(error);
            }
            tokio::select! {
                changed = completed.changed() => changed.map_err(|_| "journal input is gone".to_owned())?,
                _ = tokio::time::sleep(std::time::Duration::from_millis(250)) => {},
            }
        }
    }
}

pub(crate) struct Subscription {
    pub input: Box<dyn InputPort<Item = Vec<SpanOp>>>,
    pub progress: Progress,
}

impl Cursor {
    pub fn into_input(
        mut self,
        settings: watch::Receiver<SessionConfig>,
        version: String,
        error: Arc<Mutex<Option<String>>>,
        recovery: Arc<Mutex<Option<RecoveryInfo>>>,
    ) -> Subscription {
        let progress = Progress {
            completed: self.progress.subscribe(),
            published: self.changes.clone(),
        };
        self.settings = Some(settings);
        self.version = version;
        self.error = error;
        self.recovery = recovery;
        Subscription {
            input: Box::new(self),
            progress,
        }
    }

    pub async fn next_batch(
        &mut self,
        limit: usize,
    ) -> anyhow::Result<Option<Delivery<Vec<SpanOp>>>> {
        anyhow::ensure!(limit > 0, "cursor batch limit must be nonzero");
        let after = self.read;
        let rows = self
            .journal
            .with_db(move |db| {
                let mut statement = db.prepare(
                    "SELECT position,value FROM ops JOIN batches USING(input)
                WHERE committed=1 AND position>? ORDER BY position LIMIT ?",
                )?;
                let rows = statement.query_map(params![after, limit as u64], |r| {
                    Ok((r.get::<_, u64>(0)?, r.get::<_, Vec<u8>>(1)?))
                })?;
                Ok(rows.collect::<Result<Vec<_>, _>>()?)
            })
            .await?;
        let Some(&(through, _)) = rows.last() else {
            return Ok(None);
        };
        let ops = rows
            .into_iter()
            .map(|(_, bytes)| StoredOp::decode(&bytes))
            .collect::<anyhow::Result<Vec<_>>>()?;
        self.read = through;
        Ok(Some(Delivery::new(
            ops,
            HeadCompletion {
                journal: self.journal.clone(),
                route: self.route.clone(),
                expected: after,
                through,
                progress: self.progress.clone(),
                abandoned: self.abandoned.clone(),
                acknowledged: false,
                error: self.error.clone(),
                recovery: self.recovery.clone(),
            },
        )))
    }

    #[cfg(test)]
    pub fn completed_position(&self) -> u64 {
        self.completed
    }

    pub async fn rewind(&mut self) -> anyhow::Result<()> {
        let key = self.route.clone();
        self.completed = self
            .journal
            .with_db(move |db| {
                Ok(
                    db.query_row("SELECT position FROM heads WHERE route=?", [key], |r| {
                        r.get(0)
                    })?,
                )
            })
            .await?;
        self.read = self.completed;
        self.progress.send_replace(self.completed);
        Ok(())
    }

    #[cfg(test)]
    pub async fn changed(&mut self) {
        let _ = self.changes.changed().await;
    }
}

#[async_trait::async_trait]
impl InputPort for Cursor {
    type Item = Vec<SpanOp>;
    async fn receive(&mut self) -> anyhow::Result<Option<Delivery<Self::Item>>> {
        let result = self.receive_managed().await;
        match result {
            Ok(delivery) => Ok(Some(delivery)),
            Err(error) => {
                let error = match error.downcast::<Failure>() {
                    Ok(failure) => return Err(anyhow::Error::new(failure)),
                    Err(error) => error,
                };
                let failure = Failure::storage(&error, &self.version, 0).at(self.completed + 1);
                // An input failure has no delivery receipt to report through.
                // Its source still owns persistence of the recovery evidence.
                if let Err(error) = self
                    .journal
                    .set_failure(&self.route, Some(serde_json::to_string(&failure)?))
                    .await
                {
                    tracing::warn!(%error, "input recovery evidence could not be persisted");
                }
                Err(anyhow::Error::new(failure))
            }
        }
    }
}

impl Cursor {
    async fn receive_managed(&mut self) -> anyhow::Result<Delivery<Vec<SpanOp>>> {
        let mut acknowledgements = self.progress.subscribe();
        loop {
            if self.abandoned.load(Ordering::Acquire) {
                self.rewind().await?;
                self.abandoned.store(false, Ordering::Release);
            }
            self.completed = *self.progress.borrow();
            self.read = self.read.max(self.completed);
            let config = self
                .settings
                .as_ref()
                .map(|settings| settings.borrow().clone());
            if !self.restored {
                if let Some(raw) = self.journal.failure(&self.route).await? {
                    let failure = match serde_json::from_str::<Failure>(&raw) {
                        Ok(failure) => failure,
                        Err(error) => match &config {
                            Some(config) => Failure::legacy(raw, self.completed + 1, config),
                            None => return Err(error.into()),
                        },
                    };
                    if matches!(failure.boundary, Boundary::Completion)
                        && self.completed >= failure.through.unwrap_or(failure.position)
                    {
                        self.journal.set_failure(&self.route, None).await?;
                    } else {
                        *self.error.lock() = Some(failure.message());
                        *self.recovery.lock() = Some(failure.info(false));
                        if matches!(
                            (Recovery {
                                config: config.as_ref(),
                                version: &self.version
                            })
                            .process(&failure)
                            .await,
                            RecoveryOutcome::Waiting
                        ) {
                            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                            continue;
                        }
                        *self.recovery.lock() = Some(failure.info(true));
                    }
                }
            }
            if let Some(config) = &config {
                if let Err(error) = self.journal.bind(&self.route, config).await {
                    if error.downcast_ref::<BindingChanged>().is_some() {
                        let failure =
                            Failure::legacy(error.to_string(), self.completed + 1, config);
                        self.journal
                            .set_failure(&self.route, Some(serde_json::to_string(&failure)?))
                            .await?;
                        return Err(anyhow::Error::new(failure));
                    }
                    return Err(error);
                }
            }
            if let Some(delivery) = self.next_batch(128).await? {
                if self.abandoned.load(Ordering::Acquire) {
                    drop(delivery);
                    continue;
                }
                self.restored = true;
                return Ok(delivery);
            }
            tokio::select! {
                _ = self.changes.changed() => {},
                _ = acknowledgements.changed() => {},
            }
        }
    }
}

struct HeadCompletion {
    journal: SpanJournal,
    route: String,
    expected: u64,
    through: u64,
    progress: watch::Sender<u64>,
    abandoned: Arc<AtomicBool>,
    acknowledged: bool,
    error: Arc<Mutex<Option<String>>>,
    recovery: Arc<Mutex<Option<RecoveryInfo>>>,
}
impl Drop for HeadCompletion {
    fn drop(&mut self) {
        if !self.acknowledged {
            self.abandoned.store(true, Ordering::Release);
            self.progress.send_modify(|_| {});
        }
    }
}
#[async_trait::async_trait]
impl Completion for HeadCompletion {
    async fn complete(&mut self) -> anyhow::Result<()> {
        let (route, expected, through) = (self.route.clone(), self.expected, self.through);
        self.journal
            .with_db(move |db| {
                let tx = db.transaction()?;
                let head: u64 =
                    tx.query_row("SELECT position FROM heads WHERE route=?", [&route], |r| {
                        r.get(0)
                    })?;
                if head >= through {
                    return Ok(head);
                }
                anyhow::ensure!(head >= expected, "completion cannot skip unresolved input");
                tx.execute(
                    "UPDATE heads SET position=?, failure=NULL WHERE route=?",
                    params![through, route],
                )?;
                tx.commit()?;
                Ok(through)
            })
            .await
            .map(|head| {
                self.acknowledged = true;
                self.progress.send_replace(head);
                *self.error.lock() = None;
                *self.recovery.lock() = None;
            })
    }

    async fn failed(&mut self, failure: &mut Failure) -> anyhow::Result<()> {
        failure.position = self.expected + 1;
        failure.through = Some(self.through);
        *self.error.lock() = Some(failure.message());
        *self.recovery.lock() = Some(failure.info(false));
        self.journal
            .set_failure(&self.route, Some(serde_json::to_string(failure)?))
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::translate::{OriginSnapshot, SpanRow};

    fn op(name: &str) -> SpanOp {
        SpanOp::Insert(SpanRow {
            span_id: name.into(),
            name: name.into(),
            late_merge_key: Some("late".into()),
            turn_span_id: Some("turn".into()),
            is_turn: true,
            origin: Some(OriginSnapshot {
                plugin_version: Some("capture".into()),
                source_version: None,
                bt_version: "test".into(),
            }),
            ..SpanRow::default()
        })
    }
    fn route(name: &str) -> SessionRoute {
        SessionRoute {
            tags: vec![name.into()],
            ..SessionRoute::default()
        }
    }

    #[tokio::test]
    async fn root_candidates_commit_and_release_atomically() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("roots.sqlite");
        let journal = SpanJournal::open(&path).await.unwrap();
        let mut roots = crate::pipeline::roots::Roots::default();
        let mut root = op("root");
        let (SpanOp::Insert(row) | SpanOp::Merge(row)) = &mut root;
        row.metadata = Some(serde_json::json!({"_bt_defer_root": true}));
        assert!(roots.process(&vec![root]).await.is_empty());
        journal.begin(10, true).await.unwrap();
        let db = Connection::open(&path).unwrap();
        db.execute_batch("CREATE TRIGGER fail_roots BEFORE INSERT ON root_candidates BEGIN SELECT RAISE(FAIL,'root write'); END;").unwrap();
        assert!(journal
            .commit_with_roots(10, roots.pending())
            .await
            .is_err());
        assert_eq!(journal.translated_through().await.unwrap(), 0);
        assert!(journal.root_candidates().await.unwrap().is_empty());
        db.execute_batch("DROP TRIGGER fail_roots").unwrap();
        journal
            .commit_with_roots(10, roots.pending())
            .await
            .unwrap();
        journal.close().await.unwrap();

        let journal = SpanJournal::open(&path).await.unwrap();
        let candidates = journal.root_candidates().await.unwrap();
        let (SpanOp::Insert(row) | SpanOp::Merge(row)) = &candidates["root"][0];
        assert_eq!(row.late_merge_key.as_deref(), Some("late"));
        assert_eq!(
            row.origin.as_ref().unwrap().plugin_version.as_deref(),
            Some("capture")
        );
        let mut roots = crate::pipeline::roots::Roots::restore(candidates);
        let child = SpanOp::Insert(SpanRow {
            span_id: "turn".into(),
            parent_span_ids: vec!["root".into()],
            ..Default::default()
        });
        let ready = roots.process(&vec![child]).await;
        assert_eq!(ready.len(), 2);
        journal.begin(20, true).await.unwrap();
        journal.append(20, &ready).await.unwrap();
        db.execute_batch("CREATE TRIGGER fail_release BEFORE DELETE ON root_candidates BEGIN SELECT RAISE(FAIL,'release'); END;").unwrap();
        assert!(journal
            .commit_with_roots(20, roots.pending())
            .await
            .is_err());
        assert_eq!(journal.translated_through().await.unwrap(), 10);
        assert_eq!(journal.tail().await.unwrap(), 0);
        assert_eq!(journal.root_candidates().await.unwrap().len(), 1);
        db.execute_batch("DROP TRIGGER fail_release").unwrap();
        journal
            .commit_with_roots(20, roots.pending())
            .await
            .unwrap();
        assert_eq!(journal.tail().await.unwrap(), 2);
        assert!(journal.root_candidates().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn input_port_replays_abandoned_delivery_without_cursor_control() {
        use crate::pipeline::delivery::InputPort;
        let tmp = tempfile::tempdir().unwrap();
        let journal = SpanJournal::open(&tmp.path().join("spans.sqlite"))
            .await
            .unwrap();
        journal.begin(1, true).await.unwrap();
        journal.append(1, &[op("one")]).await.unwrap();
        journal.commit(1).await.unwrap();
        let mut input: Box<dyn InputPort<Item = Vec<SpanOp>>> =
            Box::new(journal.subscribe(&route("a"), 0).await.unwrap());
        let delivery = input.receive().await.unwrap().unwrap();
        drop(delivery);
        // Cancel a receive while its storage read is pending, as a concurrent
        // configuration update does. The replay obligation must survive it.
        let storage = journal.connection.lock();
        let mut cancelled = Box::pin(input.receive());
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(std::future::Future::poll(cancelled.as_mut(), &mut context).is_pending());
        drop(cancelled);
        drop(storage);
        let replay = tokio::time::timeout(std::time::Duration::from_millis(250), input.receive())
            .await
            .expect("unacknowledged input must be replayed")
            .unwrap()
            .unwrap();
        let (SpanOp::Insert(row) | SpanOp::Merge(row)) = &replay.value()[0];
        assert_eq!(row.name, "one");
        replay.into_completed().await.ok().unwrap();
        assert!(journal.route_complete(&route("a"), 1).await.unwrap());
        assert!(!journal.route_complete(&route("b"), 1).await.unwrap());
    }

    #[tokio::test]
    async fn preparatory_schema_upgrades_without_changing_committed_positions() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("old.sqlite");
        let db = Connection::open(&path).unwrap();
        db.execute_batch("CREATE TABLE batches(input INTEGER PRIMARY KEY,committed INTEGER NOT NULL DEFAULT 0); INSERT INTO batches VALUES(10,1); PRAGMA user_version=1;").unwrap();
        drop(db);
        let journal = SpanJournal::open(&path).await.unwrap();
        assert_eq!(journal.translated_through().await.unwrap(), 10);
        journal.begin(20, false).await.unwrap();
        journal.append(20, &[op("retirement")]).await.unwrap();
        journal.commit(20).await.unwrap();
        assert!(
            journal.read_after(0, 8).await.unwrap().is_empty(),
            "retirement output is not a native correlation observation"
        );
        let mut route = journal.subscribe(&route("a"), 0).await.unwrap();
        assert_eq!(
            route.next_batch(8).await.unwrap().unwrap().value().len(),
            1,
            "all published output still reaches durable delivery subscribers"
        );
    }

    #[tokio::test]
    async fn invisible_chunks_empty_commit_and_restart_preserve_the_boundary() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("spans.sqlite");
        let journal = SpanJournal::open(&path).await.unwrap();
        journal.begin(10, true).await.unwrap();
        journal.append(10, &[op("abandoned")]).await.unwrap();
        let mut reader = journal.subscribe(&route("a"), 0).await.unwrap();
        assert!(reader.next_batch(8).await.unwrap().is_none());
        drop(reader);
        drop(journal);
        let journal = SpanJournal::open(&path).await.unwrap();
        assert_eq!(journal.translated_through().await.unwrap(), 0);
        journal.begin(10, true).await.unwrap();
        journal.append(10, &[op("retained")]).await.unwrap();
        journal.commit(10).await.unwrap();
        assert!(!journal.begin(10, true).await.unwrap());
        journal.begin(20, true).await.unwrap();
        journal.commit(20).await.unwrap();
        assert_eq!(journal.translated_through().await.unwrap(), 20);
        let mut reader = journal.subscribe(&route("a"), 0).await.unwrap();
        let message = reader.next_batch(8).await.unwrap().unwrap();
        let (SpanOp::Insert(row) | SpanOp::Merge(row)) = &message.value()[0];
        assert_eq!(row.name, "retained");
        assert_eq!(row.late_merge_key.as_deref(), Some("late"));
        assert!(row.is_turn);
        assert_eq!(
            row.origin.as_ref().unwrap().plugin_version.as_deref(),
            Some("capture")
        );
    }

    #[tokio::test]
    async fn independent_heads_and_dropped_deliveries_resume_without_skipping() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("spans.sqlite");
        let journal = SpanJournal::open(&path).await.unwrap();
        journal.begin(10, true).await.unwrap();
        journal.append(10, &[op("one"), op("two")]).await.unwrap();
        journal.commit(10).await.unwrap();
        let mut a = journal.subscribe(&route("a"), 0).await.unwrap();
        let mut b = journal.subscribe(&route("b"), 0).await.unwrap();
        a.next_batch(2)
            .await
            .unwrap()
            .unwrap()
            .into_completed()
            .await
            .ok()
            .unwrap();
        drop(b.next_batch(2).await.unwrap().unwrap());
        a.rewind().await.unwrap();
        b.rewind().await.unwrap();
        assert!(a.next_batch(2).await.unwrap().is_none());
        assert_eq!(b.next_batch(2).await.unwrap().unwrap().value().len(), 2);
        let mut c = journal.subscribe(&route("c"), 0).await.unwrap();
        let first = c.next_batch(1).await.unwrap().unwrap();
        let second = c.next_batch(1).await.unwrap().unwrap();
        assert!(
            second.into_completed().await.is_err(),
            "later work cannot release an earlier obligation"
        );
        first.into_completed().await.ok().unwrap();
        c.rewind().await.unwrap();
        assert_eq!(c.next_batch(2).await.unwrap().unwrap().value().len(), 1);
    }
    #[tokio::test]
    async fn head_write_failure_retains_work_and_commit_wakes_readers() {
        let tmp = tempfile::tempdir().unwrap();
        let journal = SpanJournal::open(&tmp.path().join("spans.sqlite"))
            .await
            .unwrap();
        let mut reader = journal.subscribe(&route("a"), 0).await.unwrap();
        journal.begin(10, true).await.unwrap();
        journal.append(10, &[op("one")]).await.unwrap();
        journal.commit(10).await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), reader.changed())
            .await
            .unwrap();
        assert_eq!(journal.tail().await.unwrap(), 1);
        journal
            .set_failure(&reader.route, Some("held".into()))
            .await
            .unwrap();
        journal.with_db(|db| {
            db.execute_batch("CREATE TRIGGER fail_head BEFORE UPDATE OF position ON heads BEGIN SELECT RAISE(FAIL,'injected head failure'); END;")?;
            Ok(())
        }).await.unwrap();
        let pending = reader.next_batch(8).await.unwrap().unwrap();
        let failed = pending.into_completed().await.err().unwrap();
        assert!(failed.error.to_string().contains("injected head failure"));
        assert_eq!(
            journal.failure(&reader.route).await.unwrap().as_deref(),
            Some("held")
        );
        reader.rewind().await.unwrap();
        assert!(reader.next_batch(8).await.unwrap().is_some());
        journal
            .with_db(|db| {
                db.execute_batch("DROP TRIGGER fail_head;")?;
                Ok(())
            })
            .await
            .unwrap();
        failed.delivery.into_completed().await.ok().unwrap();
        assert!(journal.failure(&reader.route).await.unwrap().is_none());
        reader.rewind().await.unwrap();
        assert!(reader.next_batch(8).await.unwrap().is_none());
    }
    #[tokio::test]
    async fn a_completed_canonical_head_also_covers_its_legacy_named_route() {
        let tmp = tempfile::tempdir().unwrap();
        let journal = SpanJournal::open(&tmp.path().join("spans.sqlite"))
            .await
            .unwrap();
        journal.begin(10, true).await.unwrap();
        journal.append(10, &[op("one")]).await.unwrap();
        journal.commit(10).await.unwrap();
        let mut legacy = route("work");
        legacy.auth.profile = Some("work".into());
        let mut canonical = legacy.clone();
        canonical.auth.profile_id = Some("profile-id".into());
        let mut head = journal.subscribe(&canonical, 0).await.unwrap();
        head.next_batch(8)
            .await
            .unwrap()
            .unwrap()
            .into_completed()
            .await
            .ok()
            .unwrap();
        assert!(journal.route_complete(&legacy, 10).await.unwrap());
        let mut other_profile = canonical.clone();
        other_profile.auth.profile_id = Some("different-id".into());
        assert!(!journal.route_complete(&other_profile, 10).await.unwrap());
        legacy.tags.push("different route".into());
        assert!(!journal.route_complete(&legacy, 10).await.unwrap());
    }

    #[tokio::test]
    async fn schema_two_heads_upgrade_and_keep_a_nonsecret_backend_binding() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("old.sqlite");
        let db = Connection::open(&path).unwrap();
        db.execute_batch("CREATE TABLE heads(route TEXT PRIMARY KEY,position INTEGER NOT NULL DEFAULT 0,failure TEXT); PRAGMA user_version=2;").unwrap();
        let route = route("canonical");
        let key = serde_json::to_string(&route).unwrap();
        db.execute("INSERT INTO heads(route,position) VALUES(?,7)", [&key])
            .unwrap();
        drop(db);
        let journal = SpanJournal::open(&path).await.unwrap();
        let mut config = route.with_auth(crate::wire::BackendAuth {
            token: "secret-a".into(),
            api_url: Some("https://one.test".into()),
            app_url: None,
            org_id: Some("org".into()),
            org_name: None,
        });
        journal.bind(&key, &config).await.unwrap();
        config.auth.token = "secret-b".into();
        journal.bind(&key, &config).await.unwrap();
        config.auth.api_url = Some("https://other.test".into());
        assert!(journal
            .bind(&key, &config)
            .await
            .unwrap_err()
            .downcast_ref::<BindingChanged>()
            .is_some());
        let restored = journal.subscribe(&route, 0).await.unwrap();
        assert_eq!(restored.completed_position(), 7);
        journal
            .with_db(|db| {
                let binding: String =
                    db.query_row("SELECT binding FROM heads", [], |r| r.get(0))?;
                assert!(!binding.contains("secret-"));
                Ok(())
            })
            .await
            .unwrap();
    }
    #[tokio::test]
    async fn close_releases_storage_even_when_a_blocking_read_was_cancelled() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("spans.sqlite");
        let journal = SpanJournal::open(&path).await.unwrap();
        let reader = journal.clone();
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        let read = tokio::spawn(async move {
            reader
                .with_db(move |_| {
                    let _ = entered.send(());
                    blocked.recv().unwrap();
                    Ok(())
                })
                .await
        });
        started.await.unwrap();
        read.abort();
        assert!(read.await.unwrap_err().is_cancelled());
        let closing = journal.clone();
        let close = tokio::spawn(async move { closing.close().await });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!close.is_finished());
        release.send(()).unwrap();
        close.await.unwrap().unwrap();
        assert!(journal.tail().await.is_err());
        std::fs::remove_file(path).unwrap();
    }
}
