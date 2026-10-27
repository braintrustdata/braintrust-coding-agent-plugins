//! Concrete durable channel: one ordered SpanOp log and a head per route.
use super::live::{Multiplexer, Receiver, Record, SharedOp, BYTE_BUDGET, WINDOW};
use super::{
    delivery::InputPort,
    recovery::{Boundary, Failure, Recovery, RecoveryOutcome},
    Completion, Delivery, Stage,
};
use crate::{
    translate::{OriginSnapshot, SpanOp, SpanRow, SpanType},
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

type ReplayWindow = Vec<(u64, std::sync::Weak<SpanOp>, usize)>;

#[derive(Clone)]
pub(crate) struct SpanJournal {
    connection: Arc<StorageMutex<Option<Connection>>>,
    changed: watch::Sender<u64>,
    live: Arc<Multiplexer>,
    replay: Arc<tokio::sync::Mutex<ReplayWindow>>,
    #[cfg(test)]
    payload_reads: Arc<std::sync::atomic::AtomicU64>,
}

/// Explicit codec preserves fields intentionally omitted from wire/plugin JSON.
#[derive(Serialize, Deserialize)]
struct StoredOp {
    version: u32,
    op: SpanOp,
    late_merge_key: Option<String>,
    #[serde(default)]
    late_merge_version: Option<crate::translate::LateMergeVersion>,
    turn_span_id: Option<String>,
    is_turn: bool,
}

impl StoredOp {
    fn encode(op: &SpanOp) -> anyhow::Result<Vec<u8>> {
        let (SpanOp::Insert(row) | SpanOp::Merge(row)) = op;
        #[derive(Serialize)]
        struct Borrowed<'a> {
            version: u32,
            op: &'a SpanOp,
            late_merge_key: &'a Option<String>,
            late_merge_version: &'a Option<crate::translate::LateMergeVersion>,
            turn_span_id: &'a Option<String>,
            is_turn: bool,
        }
        Ok(serde_json::to_vec(&Borrowed {
            version: 1,
            op,
            late_merge_key: &row.late_merge_key,
            late_merge_version: &row.late_merge_version,
            turn_span_id: &row.turn_span_id,
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
        row.late_merge_version = stored.late_merge_version;
        row.turn_span_id = stored.turn_span_id;
        row.is_turn = stored.is_turn;
        Ok(stored.op)
    }
}

/// Metadata needed to restore correlation and provenance without retaining
/// conversation input/output after all delivery obligations are complete.
pub(super) fn metadata_only(op: &SpanOp) -> SpanOp {
    let (SpanOp::Insert(row) | SpanOp::Merge(row)) = op;
    let row = SpanRow {
        span_id: row.span_id.clone(),
        root_span_id: row.root_span_id.clone(),
        parent_span_ids: row.parent_span_ids.clone(),
        name: row.name.clone(),
        span_type: row.span_type,
        start_ms: row.start_ms,
        end_ms: row.end_ms,
        metadata: row.metadata.clone(),
        origin: row.origin.clone(),
        is_turn: row.is_turn,
        ..Default::default()
    };
    match op {
        SpanOp::Insert(_) => SpanOp::Insert(row),
        SpanOp::Merge(_) => SpanOp::Merge(row),
    }
}
fn save_metadata(db: &Connection, op: &SpanOp, observation: bool) -> anyhow::Result<()> {
    let (SpanOp::Insert(row) | SpanOp::Merge(row)) = op;
    if let Some(origin) = &row.origin {
        db.execute(
            "INSERT OR IGNORE INTO origins(span_id,value) VALUES (?,?)",
            params![row.span_id, serde_json::to_string(origin)?],
        )?;
    }
    if !observation {
        return Ok(());
    }
    if matches!(op, SpanOp::Insert(_))
        && (row.span_type == SpanType::Tool
            || (row.span_type == SpanType::Task && row.span_id == row.root_span_id))
    {
        let mut birth = metadata_only(op);
        let (SpanOp::Insert(birth_row) | SpanOp::Merge(birth_row)) = &mut birth;
        birth_row.end_ms = None;
        db.execute(
            "INSERT OR REPLACE INTO span_state(span_id,kind,value,end_ms) VALUES (?,?,?,?)",
            params![
                row.span_id,
                if row.span_type == SpanType::Tool {
                    "tool"
                } else {
                    "root"
                },
                StoredOp::encode(&birth)?,
                row.end_ms
            ],
        )?;
    } else {
        db.execute(
            "UPDATE span_state SET end_ms=? WHERE span_id=?",
            params![row.end_ms, row.span_id],
        )?;
    }
    db.execute(
        "DELETE FROM span_state WHERE span_id=? AND kind='tool' AND end_ms IS NOT NULL",
        [&row.span_id],
    )?;
    Ok(())
}
fn prune(db: &Connection) -> anyhow::Result<()> {
    let through: Option<u64> = db.query_row("SELECT min(position) FROM heads", [], |r| r.get(0))?;
    if let Some(through) = through {
        db.execute("DELETE FROM ops WHERE position<=?", [through])?;
        db.execute(
            "UPDATE publication SET floor=max(floor,?) WHERE id=1",
            [through],
        )?;
        db.execute("DELETE FROM receipts WHERE EXISTS(SELECT 1 FROM batches WHERE batches.input=receipts.input AND committed=1) AND NOT EXISTS(SELECT 1 FROM ops WHERE ops.input=receipts.input)", [])?;
    }
    Ok(())
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
            anyhow::ensure!(version <= 6, "unsupported SpanOp journal schema {version}");
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
            if version < 5 {
                db.execute_batch("BEGIN IMMEDIATE; ALTER TABLE heads ADD COLUMN settings TEXT; ALTER TABLE heads ADD COLUMN settings_through INTEGER NOT NULL DEFAULT 0; PRAGMA user_version=5; COMMIT")?;
            }
            if version < 6 {
                db.execute_batch("BEGIN IMMEDIATE;
                    DELETE FROM ops WHERE input IN (SELECT input FROM batches WHERE committed=0);
                    CREATE TABLE receipts (input INTEGER NOT NULL, ordinal INTEGER NOT NULL, PRIMARY KEY(input,ordinal));
                    CREATE TABLE publication (id INTEGER PRIMARY KEY CHECK(id=1), tail INTEGER NOT NULL, floor INTEGER NOT NULL);
                    INSERT INTO publication VALUES(1,(SELECT coalesce(max(position),0) FROM ops),0);
                    CREATE TABLE origins (span_id TEXT PRIMARY KEY, value TEXT NOT NULL);
                    CREATE TABLE span_state (span_id TEXT PRIMARY KEY, kind TEXT NOT NULL, value BLOB NOT NULL, end_ms INTEGER);
                    ")?;
                // One bounded upgrade pass extracts small recovery metadata.
                let mut after = 0;
                loop {
                    let record: Option<(u64,Vec<u8>,bool)> = db.query_row("SELECT position,value,observation FROM ops JOIN batches USING(input) WHERE position>? ORDER BY position LIMIT 1", [after], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
                    let Some((position,value,observation)) = record else { break; };
                    save_metadata(&db, &StoredOp::decode(&value)?, observation)?;
                    after = position;
                }
                prune(&db)?;
                db.execute_batch("PRAGMA user_version=6; COMMIT")?;
            }
            let tail = db.query_row("SELECT tail FROM publication WHERE id=1", [], |row| row.get(0))?;
            Ok((db, tail))
        })
        .await??;
        let (connection, tail) = connection;
        let (changed, _) = watch::channel(tail);
        Ok(Self {
            connection: Arc::new(StorageMutex::new(Some(connection))),
            changed,
            live: Arc::new(Multiplexer::default()),
            replay: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            #[cfg(test)]
            payload_reads: Arc::default(),
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

    /// Restart abandons only invisible receipts; a committed identity is never rewritten.
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
            let published: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM receipts WHERE input=?)",
                [input],
                |r| r.get(0),
            )?;
            if !published {
                tx.execute("DELETE FROM ops WHERE input=?", [input])?;
            }
            tx.execute(
                "INSERT OR REPLACE INTO batches(input,committed,observation) VALUES (?,0,?)",
                params![input, observation],
            )?;
            tx.commit()?;
            Ok(true)
        })
        .await
    }

    /// Journal available operations and hand the original instances to routes.
    /// Per-input operation ordinals survive a crash halfway through translation,
    /// so rebuilding translator state does not publish an accepted prefix twice.
    pub async fn append_live(
        &self,
        input: u64,
        ordinal: &mut u64,
        ops: &mut Vec<SpanOp>,
    ) -> anyhow::Result<()> {
        while !ops.is_empty() {
            let started = std::time::Instant::now();
            let mut encoded = Vec::new();
            let mut bytes = 0;
            for op in ops.iter() {
                let value = StoredOp::encode(op)?;
                if !encoded.is_empty() && bytes + value.len() > BYTE_BUDGET {
                    break;
                }
                bytes += value.len();
                encoded.push(value);
                if bytes >= BYTE_BUDGET || started.elapsed() >= WINDOW {
                    break;
                }
            }
            let sizes = encoded.iter().map(Vec::len).collect::<Vec<_>>();
            let metadata = ops
                .iter()
                .take(encoded.len())
                .map(metadata_only)
                .collect::<Vec<_>>();
            let first = *ordinal;
            let positions = self
                .with_db(move |db| {
                    let tx = db.transaction()?;
                    let mut positions = Vec::new();
                    let (committed, observation): (bool, bool) = tx.query_row(
                        "SELECT committed,observation FROM batches WHERE input=?",
                        [input],
                        |r| Ok((r.get(0)?, r.get(1)?)),
                    )?;
                    for (index, value) in encoded.into_iter().enumerate() {
                        let ordinal = first + index as u64;
                        let exists = tx.query_row(
                            "SELECT EXISTS(SELECT 1 FROM receipts WHERE input=? AND ordinal=?)",
                            params![input, ordinal],
                            |r| r.get::<_, bool>(0),
                        )?;
                        if exists {
                            positions.push(None);
                            continue;
                        }
                        anyhow::ensure!(!committed, "cannot change a completed translation input");
                        save_metadata(&tx, &metadata[index], observation)?;
                        tx.execute(
                            "INSERT INTO ops(input,value) VALUES (?,?)",
                            params![input, value],
                        )?;
                        let position = tx.last_insert_rowid() as u64;
                        positions.push(Some(position));
                        tx.execute("UPDATE publication SET tail=? WHERE id=1", [position])?;
                        tx.execute(
                            "INSERT INTO receipts(input,ordinal) VALUES (?,?)",
                            params![input, ordinal],
                        )?;
                    }
                    tx.commit()?;
                    Ok(positions)
                })
                .await?;
            let count = positions.len();
            *ordinal += count as u64;
            for ((position, bytes), op) in positions.into_iter().zip(sizes).zip(ops.drain(..count))
            {
                if let Some(position) = position {
                    self.live
                        .publish(Record {
                            position,
                            bytes,
                            op: SharedOp::new(op),
                        })
                        .await;
                    self.changed.send_replace(position);
                }
            }
        }
        Ok(())
    }

    /// One serialized, weakly held recovery window is shared by all routes.
    /// No decoded payload remains alive solely because of the replay cache.
    async fn recover_after(&self, after: u64) -> anyhow::Result<Vec<Record>> {
        let mut cache = self.replay.lock().await;
        if cache
            .first()
            .is_some_and(|(position, _, _)| *position <= after + 1)
            && cache
                .last()
                .is_some_and(|(position, _, _)| *position > after)
        {
            let shared = cache
                .iter()
                .filter(|(position, _, _)| *position > after)
                .map(|(position, op, bytes)| {
                    Some(Record {
                        position: *position,
                        op: SharedOp::from_arc(op.upgrade()?),
                        bytes: *bytes,
                    })
                })
                .collect::<Option<Vec<_>>>();
            if let Some(shared) = shared {
                return Ok(shared);
            }
        }
        let records = self.with_db(move |db| {
            let mut statement = db.prepare("SELECT position,value FROM ops JOIN batches USING(input) WHERE (committed=1 OR EXISTS(SELECT 1 FROM receipts WHERE receipts.input=ops.input)) AND position>? ORDER BY position")?;
            let mut rows = statement.query([after])?;
            let started = std::time::Instant::now();
            let mut records = Vec::new();
            let mut bytes = 0;
            while let Some(row) = rows.next()? {
                let value = row.get_ref(1)?.as_blob()?;
                if !records.is_empty() && bytes + value.len() > BYTE_BUDGET { break; }
                bytes += value.len();
                records.push(Record { position:row.get(0)?, op:SharedOp::new(StoredOp::decode(value)?), bytes:value.len() });
                if bytes >= BYTE_BUDGET || started.elapsed() >= WINDOW { break; }
            }
            Ok(records)
        }).await?;
        #[cfg(test)]
        self.payload_reads
            .fetch_add(records.len() as u64, Ordering::Relaxed);
        *cache = records
            .iter()
            .map(|record| (record.position, record.op.downgrade(), record.bytes))
            .collect();
        Ok(records)
    }

    #[cfg(test)]
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
                save_metadata(&tx, &StoredOp::decode(&value)?, true)?;
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
                let tail: u64 = tail;
                tx.execute("UPDATE publication SET tail=max(tail,?) WHERE id=1", [tail])?;
                let tail =
                    tx.query_row("SELECT tail FROM publication WHERE id=1", [], |r| r.get(0))?;
                prune(&tx)?;
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
        let live = self.live.subscribe();
        let route = serde_json::to_string(route)?;
        let key = route.clone();
        let position = self
            .with_db(move |db| {
                let tx = db.transaction()?;
                // Auth can register a named-profile obligation before learning
                // its stable ID. Move that unresolved placeholder to the resolved
                // identity instead of leaving a second head that can never run.
                let candidate: SessionRoute = serde_json::from_str(&key)?;
                if candidate.auth.profile_id.is_some() {
                    let aliases = {
                        let mut query = tx.prepare("SELECT route FROM heads WHERE binding IS NULL AND route<>?")?;
                        let rows = query.query_map([&key], |r|r.get::<_,String>(0))?;
                        rows.collect::<Result<Vec<_>,_>>()?
                    };
                    for alias in aliases {
                        let previous: SessionRoute = serde_json::from_str(&alias)?;
                        if previous.auth.profile_id.is_none() && previous.same_route(&candidate) {
                            tx.execute("UPDATE OR IGNORE heads SET route=? WHERE route=?", params![key,alias])?;
                            tx.execute("DELETE FROM heads WHERE route=?",[alias])?;
                        }
                    }
                }
                // Seed only a new head, using released event checkpoints where provable.
                let legacy: u64 = tx.query_row(
                    "SELECT coalesce(max(position),0) FROM ops JOIN batches USING(input)
                WHERE input<=? AND committed=1",
                    [legacy_through],
                    |r| r.get(0),
                )?;
                tx.execute(
                    "INSERT OR IGNORE INTO heads(route,position) VALUES (?,max(?,(SELECT floor FROM publication WHERE id=1)))",
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
            drain: watch::channel(position).0,
            abandoned: Arc::new(AtomicBool::new(false)),
            settings: None,
            version: String::new(),
            restored: false,
            live,
            queued: None,
            error: Arc::new(Mutex::new(None)),
            recovery: Arc::new(Mutex::new(None)),
        })
    }

    #[cfg(test)]
    pub async fn read_after(&self, after: u64, limit: usize) -> anyhow::Result<Vec<(u64, SpanOp)>> {
        self.with_db(move |db| {
            let mut statement = db.prepare("SELECT position,value FROM ops JOIN batches USING(input) WHERE (committed=1 OR EXISTS(SELECT 1 FROM receipts WHERE receipts.input=ops.input)) AND observation=1 AND position>? ORDER BY position LIMIT ?")?;
            let rows = statement.query_map(params![after,limit as u64], |r| Ok((r.get::<_,u64>(0)?,r.get::<_,Vec<u8>>(1)?)))?;
            rows.map(|row| { let (position,bytes)=row?; Ok((position,StoredOp::decode(&bytes)?)) }).collect()
        }).await
    }

    pub async fn tail(&self) -> anyhow::Result<u64> {
        self.with_db(|db| {
            Ok(db.query_row("SELECT tail FROM publication WHERE id=1", [], |r| r.get(0))?)
        })
        .await
    }

    pub async fn origins_for(
        &self,
        spans: Vec<String>,
    ) -> anyhow::Result<BTreeMap<String, Arc<OriginSnapshot>>> {
        if spans.is_empty() {
            return Ok(BTreeMap::new());
        }
        self.with_db(move |db| {
            let mut query = db.prepare_cached("SELECT value FROM origins WHERE span_id=?")?;
            let mut origins = BTreeMap::new();
            for span in spans {
                if let Some(value) = query
                    .query_row([&span], |r| r.get::<_, String>(0))
                    .optional()?
                {
                    origins.insert(span, serde_json::from_str(&value)?);
                }
            }
            Ok(origins)
        })
        .await
    }

    #[cfg(test)]
    pub async fn origins(&self) -> anyhow::Result<BTreeMap<String, OriginSnapshot>> {
        self.with_db(|db| {
            let mut query = db.prepare("SELECT span_id,value FROM origins")?;
            let rows =
                query.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
            rows.map(|row| {
                let (id, value) = row?;
                Ok((id, serde_json::from_str(&value)?))
            })
            .collect()
        })
        .await
    }

    pub async fn observations(&self) -> anyhow::Result<Vec<SpanOp>> {
        self.with_db(|db| {
            let mut query = db.prepare("SELECT value,end_ms FROM span_state ORDER BY rowid")?;
            let rows = query.query_map([], |r| {
                Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Option<i64>>(1)?))
            })?;
            let mut result = Vec::new();
            for row in rows {
                let (bytes, end) = row?;
                let op = StoredOp::decode(&bytes)?;
                let (SpanOp::Insert(birth) | SpanOp::Merge(birth)) = &op;
                if let Some(end_ms) = end {
                    let mut terminal = birth.clone();
                    terminal.end_ms = Some(end_ms);
                    result.push(op);
                    result.push(SpanOp::Merge(terminal));
                } else {
                    result.push(op);
                }
            }
            Ok(result)
        })
        .await
    }

    pub async fn register(&self, route: &SessionRoute) -> anyhow::Result<()> {
        let route = serde_json::to_string(route)?;
        self.with_db(move |db| {
            let candidate: SessionRoute = serde_json::from_str(&route)?;
            let existing = {
                let mut query = db.prepare("SELECT route FROM heads")?;
                let rows = query.query_map([], |r|r.get::<_,String>(0))?;
                let mut existing = false;
                for row in rows { if serde_json::from_str::<SessionRoute>(&row?)?.same_route(&candidate) { existing = true; break; } }
                existing
            };
            if !existing { db.execute("INSERT OR IGNORE INTO heads(route,position) VALUES (?,(SELECT floor FROM publication WHERE id=1))",[route])?; }
            Ok(())
        }).await
    }

    /// A completed subscription can remain dormant until its source has new work.
    pub async fn route_complete(&self, route: &SessionRoute, input: u64) -> anyhow::Result<bool> {
        let route = route.clone();
        self.with_db(move |db| {
            let committed: u64 = db.query_row(
                "SELECT coalesce(max(input),0) FROM batches WHERE committed=1",
                [],
                |r| r.get(0),
            )?;
            if committed < input {
                return Ok(false);
            }
            let tail: u64 =
                db.query_row("SELECT tail FROM publication WHERE id=1", [], |r| r.get(0))?;
            let mut query =
                db.prepare("SELECT route FROM heads WHERE failure IS NULL AND position>=?")?;
            for stored in query.query_map([tail], |r| r.get::<_, String>(0))? {
                let stored: SessionRoute = serde_json::from_str(&stored?)?;
                if stored.same_route(&route) {
                    return Ok(true);
                }
            }
            Ok(false)
        })
        .await
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
    /// Keep the original subscription and backend binding while correcting settings
    /// that can unblock an explicitly configuration-held delivery.
    pub async fn reconfigure_held(
        &self,
        route: &SessionRoute,
        through: u64,
    ) -> anyhow::Result<Vec<(SessionRoute, SessionRoute)>> {
        let route = route.clone();
        self.with_db(move |db| {
            let tx = db.transaction()?;
            let updates = {
                let mut query = tx.prepare(
                    "SELECT route,failure,settings FROM heads WHERE failure IS NOT NULL AND settings_through<?",
                )?;
                let rows = query.query_map([through], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                })?;
                let mut updates = Vec::new();
                for row in rows {
                    let (key, raw, settings) = row?;
                    let stored: SessionRoute = serde_json::from_str(&key)?;
                    let Ok(failure) = serde_json::from_str::<Failure>(&raw) else {
                        continue;
                    };
                    if !matches!(
                        failure.condition,
                        super::recovery::Condition::Configuration { .. } | super::recovery::Condition::Plugin { .. }
                    ) || !stored.same_destination(&route)
                    {
                        continue;
                    }
                    let previous: SessionRoute = settings
                        .as_deref()
                        .map(serde_json::from_str)
                        .transpose()?
                        .unwrap_or_else(|| stored.clone());
                    if previous.same_route(&route) {
                        continue;
                    }
                    let mut corrected = route.clone();
                    // Preserve the subscription's pinned credential selection.
                    corrected.auth = stored.auth.clone();
                    updates.push((key, stored, corrected));
                }
                updates
            };
            for (key, _, corrected) in &updates {
                tx.execute(
                    "UPDATE heads SET settings=?, settings_through=? WHERE route=?",
                    params![serde_json::to_string(corrected)?, through, key],
                )?;
            }
            tx.commit()?;
            Ok(updates
                .into_iter()
                .map(|(_, stored, corrected)| (stored, corrected))
                .collect())
        })
        .await
    }

    pub async fn route_settings(&self, route: &SessionRoute) -> anyhow::Result<SessionRoute> {
        let fallback = route.clone();
        let key = serde_json::to_string(route)?;
        self.with_db(move |db| {
            let settings: Option<String> = db
                .query_row("SELECT settings FROM heads WHERE route=?", [key], |row| {
                    row.get(0)
                })
                .optional()?
                .flatten();
            Ok(settings
                .as_deref()
                .map(serde_json::from_str)
                .transpose()?
                .unwrap_or(fallback))
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
    /// Remaining metadata can expire once input and every route are complete.
    /// An empty subscriber set is not completion.
    pub async fn collectible(&self, input: u64) -> anyhow::Result<bool> {
        self.with_db(move |db| Ok(db.query_row("SELECT EXISTS(SELECT 1 FROM heads)
            AND NOT EXISTS(SELECT 1 FROM heads WHERE failure IS NOT NULL OR position < (SELECT tail FROM publication WHERE id=1))
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
    drain: watch::Sender<u64>,
    abandoned: Arc<AtomicBool>,
    settings: Option<watch::Receiver<SessionConfig>>,
    version: String,
    restored: bool,
    live: Receiver,
    queued: Option<Record>,
    error: Arc<Mutex<Option<String>>>,
    recovery: Arc<Mutex<Option<RecoveryInfo>>>,
}

/// The composition layer keeps this status handle; downstream stages only get
/// the InputPort and managed deliveries.
#[derive(Clone)]
pub(crate) struct Progress {
    completed: watch::Receiver<u64>,
    published: watch::Receiver<u64>,
    drain: watch::Sender<u64>,
}
#[cfg(test)]
impl Default for Progress {
    fn default() -> Self {
        Self {
            completed: watch::channel(0).1,
            published: watch::channel(0).1,
            drain: watch::channel(0).0,
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
        // Flush callers already know the publication boundary they need. Wake
        // collection and keep draining promptly until that boundary is reported.
        self.drain.send_if_modified(|requested| {
            if target > *requested {
                *requested = target;
                true
            } else {
                false
            }
        });
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
    pub input: Box<dyn InputPort<Item = Vec<SharedOp>>>,
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
            drain: self.drain.clone(),
        };
        self.settings = Some(settings);
        self.version = version;
        self.error = error;
        self.recovery = recovery;
        Subscription {
            input: Box::new(SharedCursor(self)),
            progress,
        }
    }

    #[cfg(test)]
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
                WHERE (committed=1 OR EXISTS(SELECT 1 FROM receipts WHERE receipts.input=ops.input)) AND position>? ORDER BY position LIMIT ?",
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
                live: self.live.control(),
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

    async fn next_shared(&mut self) -> anyhow::Result<Option<Delivery<Vec<SharedOp>>>> {
        let published = *self.changes.borrow();
        if self.read >= published {
            self.live.resume();
            return Ok(None);
        }
        let after = self.read;
        let first = self.queued.take().or_else(|| self.live.try_recv());
        let mut records = Vec::new();
        if let Some(first) = first {
            if first.position == after + 1 {
                records.push(first);
            }
        }
        if records.is_empty() {
            // Restart, abandoned delivery, a late subscription, or a bounded
            // live-queue overrun. Healthy live delivery never enters this path.
            records = self.journal.recover_after(after).await?;
            self.live.clear();
        } else {
            let deadline = tokio::time::Instant::now() + WINDOW;
            let mut drain = self.drain.subscribe();
            let mut bytes = records[0].bytes;
            while bytes < BYTE_BUDGET {
                let next = if *drain.borrow_and_update() > after {
                    match self.live.try_recv() {
                        Some(record) => record,
                        None => break,
                    }
                } else {
                    tokio::select! {
                        next = tokio::time::timeout_at(deadline, self.live.recv()) => match next {
                            Ok(Some(record)) => record,
                            _ => break,
                        },
                        _ = drain.changed() => continue,
                    }
                };
                if next.position != records.last().map_or(after, |record| record.position) + 1
                    || bytes + next.bytes > BYTE_BUDGET
                {
                    self.queued = Some(next);
                    break;
                }
                bytes += next.bytes;
                records.push(next);
            }
        }
        let Some(last) = records.last() else {
            return Ok(None);
        };
        let through = last.position;
        self.read = through;
        Ok(Some(Delivery::new(
            records.into_iter().map(|record| record.op).collect(),
            HeadCompletion {
                journal: self.journal.clone(),
                live: self.live.control(),
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
        self.queued = None;
        self.live.clear();
        self.progress.send_replace(self.completed);
        Ok(())
    }

    #[cfg(test)]
    pub async fn changed(&mut self) {
        let _ = self.changes.changed().await;
    }
}

struct SharedCursor(Cursor);

#[async_trait::async_trait]
impl InputPort for SharedCursor {
    type Item = Vec<SharedOp>;
    async fn receive(&mut self) -> anyhow::Result<Option<Delivery<Self::Item>>> {
        let result = self.0.receive_managed().await;
        match result {
            Ok(delivery) => Ok(Some(delivery)),
            Err(error) => {
                self.0.live.control().pause();
                let error = match error.downcast::<Failure>() {
                    Ok(failure) => return Err(anyhow::Error::new(failure)),
                    Err(error) => error,
                };
                let failure = Failure::storage(&error, &self.0.version, 0).at(self.0.completed + 1);
                // An input failure has no delivery receipt to report through.
                // Its source still owns persistence of the recovery evidence.
                if let Err(error) = self
                    .0
                    .journal
                    .set_failure(&self.0.route, Some(serde_json::to_string(&failure)?))
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
    async fn receive_managed(&mut self) -> anyhow::Result<Delivery<Vec<SharedOp>>> {
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
            if let Some(delivery) = self.next_shared().await? {
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

#[cfg(test)]
#[async_trait::async_trait]
impl InputPort for Cursor {
    type Item = Vec<SpanOp>;
    async fn receive(&mut self) -> anyhow::Result<Option<Delivery<Self::Item>>> {
        Ok(Some(self.receive_managed().await?.map(|ops| {
            ops.into_iter().map(|op| op.as_ref().clone()).collect()
        })))
    }
}

struct HeadCompletion {
    journal: SpanJournal,
    live: super::live::Control,
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
            self.live.pause();
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
                prune(&tx)?;
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
        self.live.pause();
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
            origin: Some(
                OriginSnapshot {
                    plugin_version: Some("capture".into()),
                    source_version: None,
                    bt_version: "test".into(),
                }
                .into(),
            ),
            ..SpanRow::default()
        })
    }
    #[test]
    fn codec_preserves_aggregate_watermarks_and_reads_older_records() {
        let mut op = op("root");
        let (SpanOp::Insert(row) | SpanOp::Merge(row)) = &mut op;
        let version = crate::translate::LateMergeVersion {
            ts_ms: 123,
            native_id: Some("turn".into()),
        };
        row.late_merge_version = Some(version.clone());
        let encoded = StoredOp::encode(&op).unwrap();
        let decoded = StoredOp::decode(&encoded).unwrap();
        let (SpanOp::Insert(row) | SpanOp::Merge(row)) = decoded;
        assert_eq!(row.late_merge_version, Some(version));
        let mut legacy: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        legacy.as_object_mut().unwrap().remove("late_merge_version");
        let decoded = StoredOp::decode(&serde_json::to_vec(&legacy).unwrap()).unwrap();
        let (SpanOp::Insert(row) | SpanOp::Merge(row)) = decoded;
        assert!(row.late_merge_version.is_none());
        assert!(serde_json::to_value(&op).unwrap()["Insert"]
            .get("late_merge_version")
            .is_none());
    }

    fn route(name: &str) -> SessionRoute {
        SessionRoute {
            tags: vec![name.into()],
            ..SessionRoute::default()
        }
    }

    #[tokio::test]
    async fn schema_five_upgrade_reclaims_acknowledged_payloads_without_losing_provenance() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("upgrade.sqlite");
        let journal = SpanJournal::open(&path).await.unwrap();
        journal.register(&route("old")).await.unwrap();
        journal.begin(10, true).await.unwrap();
        journal.append(10, &[op("old")]).await.unwrap();
        journal.commit(10).await.unwrap();
        journal.close().await.unwrap();
        let db = Connection::open(&path).unwrap();
        db.execute_batch("UPDATE heads SET position=1; DROP TABLE receipts; DROP TABLE publication; DROP TABLE origins; DROP TABLE span_state; PRAGMA user_version=5").unwrap();
        drop(db);
        let upgraded = SpanJournal::open(&path).await.unwrap();
        assert_eq!(upgraded.tail().await.unwrap(), 1);
        assert_eq!(upgraded.origins().await.unwrap()["old"].bt_version, "test");
        assert_eq!(
            upgraded
                .with_db(|db| Ok(
                    db.query_row("SELECT count(*) FROM ops", [], |r| r.get::<_, u64>(0))?
                ))
                .await
                .unwrap(),
            0
        );
        assert!(upgraded.collectible(10).await.unwrap());
    }

    #[tokio::test]
    async fn resolved_profile_completes_the_pre_authentication_placeholder() {
        let tmp = tempfile::tempdir().unwrap();
        let journal = SpanJournal::open(&tmp.path().join("auth.sqlite"))
            .await
            .unwrap();
        let mut named = route("profile");
        named.auth.profile = Some("work".into());
        journal.register(&named).await.unwrap();
        journal.begin(10, true).await.unwrap();
        journal
            .append_live(10, &mut 0, &mut vec![op("one")])
            .await
            .unwrap();
        journal.commit(10).await.unwrap();
        let mut resolved = named.clone();
        resolved.auth.profile_id = Some("stable-id".into());
        let mut cursor = journal.subscribe(&resolved, 0).await.unwrap();
        assert_eq!(journal.routes().await.unwrap().len(), 1);
        cursor
            .next_shared()
            .await
            .unwrap()
            .unwrap()
            .into_completed()
            .await
            .ok()
            .unwrap();
        assert_eq!(
            journal
                .with_db(|db| Ok(
                    db.query_row("SELECT count(*) FROM ops", [], |r| r.get::<_, u64>(0))?
                ))
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn recovery_routes_at_different_heads_share_the_same_decode_window() {
        let tmp = tempfile::tempdir().unwrap();
        let journal = SpanJournal::open(&tmp.path().join("shared-recovery.sqlite"))
            .await
            .unwrap();
        journal.begin(10, true).await.unwrap();
        journal
            .append_live(10, &mut 0, &mut vec![op("one"), op("two"), op("three")])
            .await
            .unwrap();
        let mut a = journal.subscribe(&route("a"), 0).await.unwrap();
        let mut b = journal.subscribe(&route("b"), 0).await.unwrap();
        b.read = 1;
        let first = a.next_shared().await.unwrap().unwrap();
        let second = b.next_shared().await.unwrap().unwrap();
        assert_eq!(first.value().len(), 3);
        assert_eq!(second.value().len(), 2);
        assert!(std::ptr::eq(
            first.value()[1].as_ref(),
            second.value()[0].as_ref()
        ));
        assert_eq!(journal.payload_reads.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn all_route_acknowledgements_reclaim_payloads_and_preserve_watermarks() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("cleanup.sqlite");
        let journal = SpanJournal::open(&path).await.unwrap();
        let mut a = journal.subscribe(&route("a"), 0).await.unwrap();
        let mut b = journal.subscribe(&route("b"), 0).await.unwrap();
        journal.begin(10, true).await.unwrap();
        journal
            .append_live(10, &mut 0, &mut vec![op("one")])
            .await
            .unwrap();
        journal.commit(10).await.unwrap();
        let first = a.next_shared().await.unwrap().unwrap();
        let second = b.next_shared().await.unwrap().unwrap();
        first.into_completed().await.ok().unwrap();
        assert_eq!(
            journal
                .with_db(|db| Ok(
                    db.query_row("SELECT count(*) FROM ops", [], |r| r.get::<_, u64>(0))?
                ))
                .await
                .unwrap(),
            1
        );
        second.into_completed().await.ok().unwrap();
        assert_eq!(
            journal
                .with_db(|db| Ok(
                    db.query_row("SELECT count(*) FROM ops", [], |r| r.get::<_, u64>(0))?
                ))
                .await
                .unwrap(),
            0
        );
        assert_eq!(journal.tail().await.unwrap(), 1);
        assert_eq!(journal.origins().await.unwrap()["one"].bt_version, "test");
        journal.close().await.unwrap();
        let restored = SpanJournal::open(&path).await.unwrap();
        assert_eq!(restored.tail().await.unwrap(), 1);
        assert_eq!(restored.translated_through().await.unwrap(), 10);
        let mut late = restored.subscribe(&route("new"), 0).await.unwrap();
        assert!(late.next_shared().await.unwrap().is_none());
        restored.begin(20, true).await.unwrap();
        restored
            .append_live(20, &mut 0, &mut vec![op("two")])
            .await
            .unwrap();
        assert_eq!(restored.tail().await.unwrap(), 2);
        assert!(late.next_shared().await.unwrap().is_some());
    }

    #[tokio::test]
    async fn explicit_flush_interrupts_live_batch_collection() {
        let tmp = tempfile::tempdir().unwrap();
        let journal = SpanJournal::open(&tmp.path().join("flush.sqlite"))
            .await
            .unwrap();
        let mut cursor = journal.subscribe(&route("a"), 0).await.unwrap();
        let progress = Progress {
            completed: cursor.progress.subscribe(),
            published: cursor.changes.clone(),
            drain: cursor.drain.clone(),
        };
        journal.begin(10, true).await.unwrap();
        journal
            .append_live(10, &mut 0, &mut vec![op("one")])
            .await
            .unwrap();
        let mut receiving = Box::pin(cursor.next_shared());
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut receiving)
                .await
                .is_err(),
            "a sparse batch should wait for more operations"
        );
        let error = Mutex::new(None);
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            let ((), result) = tokio::join!(
                async {
                    receiving
                        .await
                        .unwrap()
                        .unwrap()
                        .into_completed()
                        .await
                        .ok()
                        .unwrap();
                },
                progress.through(1, &error)
            );
            result.unwrap();
        })
        .await
        .expect("explicit flush must bypass the 30-second window");
        assert_eq!(journal.payload_reads.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn live_routes_share_original_payload_without_sqlite_reads() {
        let tmp = tempfile::tempdir().unwrap();
        let journal = SpanJournal::open(&tmp.path().join("live.sqlite"))
            .await
            .unwrap();
        let mut first = journal.subscribe(&route("first"), 0).await.unwrap();
        let mut second = journal.subscribe(&route("second"), 0).await.unwrap();
        let payload = "x".repeat(BYTE_BUDGET * 2);
        let pointer = payload.as_ptr();
        let mut ops = vec![SpanOp::Insert(SpanRow {
            input: Some(serde_json::Value::String(payload)),
            ..Default::default()
        })];
        journal.begin(10, true).await.unwrap();
        journal.append_live(10, &mut 0, &mut ops).await.unwrap();
        assert!(ops.is_empty());
        // Live delivery is possible before the native event has finished.
        let a = first.next_shared().await.unwrap().unwrap();
        let b = second.next_shared().await.unwrap().unwrap();
        assert!(std::ptr::eq(a.value()[0].as_ref(), b.value()[0].as_ref()));
        let (SpanOp::Insert(row) | SpanOp::Merge(row)) = a.value()[0].as_ref();
        assert_eq!(
            row.input.as_ref().unwrap().as_str().unwrap().as_ptr(),
            pointer
        );
        assert_eq!(journal.payload_reads.load(Ordering::Relaxed), 0);
        let weak = a.value()[0].downgrade();
        drop(a.into_completed().await.ok().unwrap());
        drop(b.into_completed().await.ok().unwrap());
        assert!(weak.upgrade().is_none());
    }

    #[tokio::test]
    async fn recovery_decodes_once_for_concurrent_routes_and_keeps_only_weak_references() {
        let tmp = tempfile::tempdir().unwrap();
        let journal = SpanJournal::open(&tmp.path().join("recovery.sqlite"))
            .await
            .unwrap();
        journal.begin(10, true).await.unwrap();
        journal
            .append_live(10, &mut 0, &mut vec![op("recover")])
            .await
            .unwrap();
        let mut first = journal.subscribe(&route("first"), 0).await.unwrap();
        let mut second = journal.subscribe(&route("second"), 0).await.unwrap();
        let a = first.next_shared().await.unwrap().unwrap();
        let b = second.next_shared().await.unwrap().unwrap();
        assert!(std::ptr::eq(a.value()[0].as_ref(), b.value()[0].as_ref()));
        assert_eq!(journal.payload_reads.load(Ordering::Relaxed), 1);
        let weak = a.value()[0].downgrade();
        drop(a);
        drop(b);
        assert!(weak.upgrade().is_none());
    }

    #[tokio::test]
    async fn partial_event_restart_preserves_published_operations_and_route_heads() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("partial.sqlite");
        let journal = SpanJournal::open(&path).await.unwrap();
        let mut first = journal.subscribe(&route("first"), 0).await.unwrap();
        journal.begin(10, true).await.unwrap();
        journal
            .append_live(10, &mut 0, &mut vec![op("one")])
            .await
            .unwrap();
        first
            .next_shared()
            .await
            .unwrap()
            .unwrap()
            .into_completed()
            .await
            .ok()
            .unwrap();
        assert_eq!(journal.translated_through().await.unwrap(), 0);
        journal.close().await.unwrap();
        let journal = SpanJournal::open(&path).await.unwrap();
        journal.begin(10, true).await.unwrap();
        let mut ordinal = 0;
        journal
            .append_live(10, &mut ordinal, &mut vec![op("one"), op("two")])
            .await
            .unwrap();
        journal.commit(10).await.unwrap();
        let mut restored = journal.subscribe(&route("first"), 0).await.unwrap();
        let delivery = restored.next_shared().await.unwrap().unwrap();
        assert_eq!(delivery.value().len(), 1);
        let (SpanOp::Insert(row) | SpanOp::Merge(row)) = delivery.value()[0].as_ref();
        assert_eq!(row.name, "two");
        assert_eq!(journal.tail().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn paused_route_recovers_without_blocking_a_healthy_live_route() {
        let tmp = tempfile::tempdir().unwrap();
        let journal = SpanJournal::open(&tmp.path().join("lag.sqlite"))
            .await
            .unwrap();
        let mut fast = journal.subscribe(&route("fast"), 0).await.unwrap();
        let mut slow = journal.subscribe(&route("slow"), 0).await.unwrap();
        slow.live.control().pause();
        journal.begin(10, true).await.unwrap();
        let mut ordinal = 0;
        for index in 0..3 {
            journal
                .append_live(
                    10,
                    &mut ordinal,
                    &mut vec![SpanOp::Insert(SpanRow {
                        name: index.to_string(),
                        input: Some(serde_json::json!("x".repeat(BYTE_BUDGET))),
                        ..Default::default()
                    })],
                )
                .await
                .unwrap();
            fast.next_shared()
                .await
                .unwrap()
                .unwrap()
                .into_completed()
                .await
                .ok()
                .unwrap();
        }
        assert_eq!(journal.payload_reads.load(Ordering::Relaxed), 0);
        journal.commit(10).await.unwrap();
        for _ in 0..3 {
            let delivery = slow.next_shared().await.unwrap().unwrap();
            assert_eq!(
                delivery.value().len(),
                1,
                "an oversized operation travels alone"
            );
            delivery.into_completed().await.ok().unwrap();
        }
        slow.rewind().await.unwrap();
        assert_eq!(slow.completed_position(), 3);
        assert_eq!(journal.payload_reads.load(Ordering::Relaxed), 3);
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
    async fn legacy_uncommitted_output_empty_commit_and_restart_preserve_the_boundary() {
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
    async fn corrected_settings_survive_restart_without_advancing_or_retargeting_heads() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("spans.sqlite");
        let journal = SpanJournal::open(&path).await.unwrap();
        let original = SessionRoute {
            destination: Some(crate::wire::TraceDestination::ProjectLogs {
                project_id: Some("test-project".into()),
                project_name: None,
            }),
            ..route("a")
        };
        let cursor = journal.subscribe(&original, 0).await.unwrap();
        let failure = Failure {
            boundary: Boundary::Sink,
            position: 1,
            through: None,
            reason: "rejected row".into(),
            attempt: 0,
            condition: super::super::recovery::Condition::Configuration {
                signature: "original".into(),
                recheck_ms: None,
            },
        };
        let raw = serde_json::to_string(&failure).unwrap();
        journal
            .set_failure(&cursor.route, Some(raw.clone()))
            .await
            .unwrap();
        let mut corrected = original.clone();
        corrected.tags.push("corrected".into());
        for unrelated in [
            SessionRoute {
                destination: Some(crate::wire::TraceDestination::ProjectLogs {
                    project_id: Some("other-project".into()),
                    project_name: None,
                }),
                ..corrected.clone()
            },
            SessionRoute {
                auth: crate::wire::AuthSelection {
                    org_name: Some("other-org".into()),
                    ..Default::default()
                },
                ..corrected.clone()
            },
        ] {
            assert!(journal
                .reconfigure_held(&unrelated, 20)
                .await
                .unwrap()
                .is_empty());
        }
        assert_eq!(
            journal
                .reconfigure_held(&corrected, 20)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(cursor.completed, 0);
        assert_eq!(journal.failure(&cursor.route).await.unwrap(), Some(raw));
        // A captured prefix must not revert a newer correction during startup replay.
        assert!(journal
            .reconfigure_held(&original, 10)
            .await
            .unwrap()
            .is_empty());
        journal.close().await.unwrap();
        let restored = SpanJournal::open(&path).await.unwrap();
        assert_eq!(
            restored.route_settings(&original).await.unwrap().tags,
            corrected.tags
        );
        assert_eq!(restored.routes().await.unwrap().len(), 1);
        assert_eq!(restored.subscribe(&original, 0).await.unwrap().completed, 0);
        restored.set_failure(&cursor.route, None).await.unwrap();
        let mut newer = corrected.clone();
        newer.tags.push("independent".into());
        assert!(
            restored
                .reconfigure_held(&newer, 30)
                .await
                .unwrap()
                .is_empty(),
            "healthy independent subscriptions retain their settings"
        );
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
