//! The daemon-wide SQLite database: span and backfill journals, consumer
//! heads, receipts, failure records, branch state, the delivery ledger, and
//! the partition index.
//!
//! One writer connection serializes every write; a small pool serves reads.
//! All calls run on Tokio's blocking pool, never on a worker thread. SQL never
//! leaves this module.

use super::codec::{decode_span_entry, encode_span_entry};
use super::types::{NodeId, PartitionId, SpanEntry};
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS partitions (
    pkey TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    session_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    created_ms INTEGER NOT NULL,
    last_activity_ms INTEGER NOT NULL,
    finalized INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS span_nodes (
    jkey TEXT NOT NULL,
    node INTEGER NOT NULL,
    entry BLOB NOT NULL,
    PRIMARY KEY (jkey, node)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS journal_tails (
    jkey TEXT PRIMARY KEY,
    last_node INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS heads (
    jkey TEXT NOT NULL,
    consumer TEXT NOT NULL,
    next_node INTEGER NOT NULL,
    PRIMARY KEY (jkey, consumer)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS failures (
    jkey TEXT NOT NULL,
    consumer TEXT NOT NULL,
    record TEXT NOT NULL,
    PRIMARY KEY (jkey, consumer)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS branches (
    pkey TEXT NOT NULL,
    route TEXT NOT NULL,
    state TEXT NOT NULL,
    PRIMARY KEY (pkey, route)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS ledger (
    pkey TEXT NOT NULL,
    route TEXT NOT NULL,
    kind INTEGER NOT NULL,
    id TEXT NOT NULL,
    PRIMARY KEY (pkey, route, kind, id)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS origins (
    pkey TEXT NOT NULL,
    route TEXT NOT NULL,
    span_id TEXT NOT NULL,
    snapshot TEXT NOT NULL,
    PRIMARY KEY (pkey, route, span_id)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS backfills (
    pkey TEXT NOT NULL,
    route TEXT NOT NULL,
    state TEXT NOT NULL,
    PRIMARY KEY (pkey, route)
) WITHOUT ROWID;
"#;

/// Ledger row kinds.
pub(crate) const LEDGER_COMPLETED: i64 = 0;
pub(crate) const LEDGER_LATE_MERGE: i64 = 1;

const READ_POOL: usize = 4;

pub(crate) struct Store {
    path: PathBuf,
    writer: Mutex<Connection>,
    readers: Mutex<Vec<Connection>>,
}

fn open_connection(path: &Path) -> rusqlite::Result<Connection> {
    let conn = Connection::open(path)?;
    conn.busy_timeout(std::time::Duration::from_secs(10))?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "FULL")?;
    Ok(conn)
}

impl Store {
    pub(crate) fn open(path: &Path) -> anyhow::Result<Arc<Self>> {
        if let Some(parent) = path.parent() {
            crate::paths::ensure_private_dir(parent)?;
        }
        let writer = open_connection(path)?;
        writer.execute_batch(SCHEMA)?;
        Ok(Arc::new(Self {
            path: path.to_path_buf(),
            writer: Mutex::new(writer),
            readers: Mutex::new(Vec::new()),
        }))
    }

    /// Run `f` in one immediate write transaction. Either every write in `f`
    /// commits or none does.
    pub(crate) async fn write<R, F>(self: &Arc<Self>, f: F) -> anyhow::Result<R>
    where
        R: Send + 'static,
        F: FnOnce(&Transaction<'_>) -> anyhow::Result<R> + Send + 'static,
    {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.write_blocking(f))
            .await
            .map_err(|error| anyhow::anyhow!("database task failed: {error}"))?
    }

    pub(crate) fn write_blocking<R, F>(&self, f: F) -> anyhow::Result<R>
    where
        F: FnOnce(&Transaction<'_>) -> anyhow::Result<R>,
    {
        let mut conn = self.writer.lock().unwrap_or_else(|e| e.into_inner());
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let result = f(&tx)?;
        tx.commit()?;
        Ok(result)
    }

    pub(crate) async fn read<R, F>(self: &Arc<Self>, f: F) -> anyhow::Result<R>
    where
        R: Send + 'static,
        F: FnOnce(&Connection) -> anyhow::Result<R> + Send + 'static,
    {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.read_blocking(f))
            .await
            .map_err(|error| anyhow::anyhow!("database task failed: {error}"))?
    }

    pub(crate) fn read_blocking<R, F>(&self, f: F) -> anyhow::Result<R>
    where
        F: FnOnce(&Connection) -> anyhow::Result<R>,
    {
        let conn = {
            let mut pool = self.readers.lock().unwrap_or_else(|e| e.into_inner());
            pool.pop()
        };
        let conn = match conn {
            Some(conn) => conn,
            None => open_connection(&self.path)?,
        };
        let result = f(&conn);
        let mut pool = self.readers.lock().unwrap_or_else(|e| e.into_inner());
        if pool.len() < READ_POOL {
            pool.push(conn);
        }
        result
    }
}

// --- meta -------------------------------------------------------------------

pub(crate) fn get_meta(conn: &Connection, key: &str) -> anyhow::Result<Option<String>> {
    Ok(conn
        .query_row("SELECT value FROM meta WHERE key = ?1", [key], |row| {
            row.get(0)
        })
        .optional()?)
}

pub(crate) fn set_meta(tx: &Transaction<'_>, key: &str, value: &str) -> anyhow::Result<()> {
    tx.execute(
        "INSERT INTO meta (key, value) VALUES (?1, ?2)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        params![key, value],
    )?;
    Ok(())
}

// --- partitions -------------------------------------------------------------

#[derive(Debug, Clone)]
pub(crate) struct PartitionRow {
    pub(crate) partition: PartitionId,
    pub(crate) namespace: String,
    pub(crate) last_activity_ms: i64,
}

/// Record that a partition was modified, restarting its retention window.
pub(crate) fn touch_partition(
    tx: &Transaction<'_>,
    partition: &PartitionId,
    now_ms: i64,
) -> anyhow::Result<()> {
    tx.execute(
        "UPDATE partitions SET last_activity_ms = ?2 WHERE pkey = ?1 AND last_activity_ms < ?2",
        params![partition.key(), now_ms],
    )?;
    Ok(())
}

pub(crate) fn ensure_partition(
    tx: &Transaction<'_>,
    partition: &PartitionId,
    namespace: &str,
    now_ms: i64,
) -> anyhow::Result<()> {
    tx.execute(
        "INSERT INTO partitions (pkey, source, session_id, namespace, created_ms, last_activity_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?5)
         ON CONFLICT(pkey) DO UPDATE SET last_activity_ms = excluded.last_activity_ms",
        params![
            partition.key(),
            partition.source,
            partition.session_id,
            namespace,
            now_ms
        ],
    )?;
    Ok(())
}

pub(crate) fn partition(
    conn: &Connection,
    partition: &PartitionId,
) -> anyhow::Result<Option<PartitionRow>> {
    Ok(conn
        .query_row(
            "SELECT namespace, last_activity_ms FROM partitions WHERE pkey = ?1",
            [partition.key()],
            |row| {
                Ok(PartitionRow {
                    partition: partition.clone(),
                    namespace: row.get(0)?,
                    last_activity_ms: row.get(1)?,
                })
            },
        )
        .optional()?)
}

pub(crate) fn partitions(conn: &Connection) -> anyhow::Result<Vec<PartitionRow>> {
    let mut statement =
        conn.prepare("SELECT source, session_id, namespace, last_activity_ms FROM partitions")?;
    let rows = statement
        .query_map([], |row| {
            Ok(PartitionRow {
                partition: PartitionId::new(row.get::<_, String>(0)?, row.get::<_, String>(1)?),
                namespace: row.get(2)?,
                last_activity_ms: row.get(3)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// Delete every row belonging to a partition. Retention only.
pub(crate) fn delete_partition(
    tx: &Transaction<'_>,
    partition: &PartitionId,
) -> anyhow::Result<()> {
    let pkey = partition.key();
    for table in ["span_nodes", "journal_tails", "heads", "failures"] {
        tx.execute(
            &format!("DELETE FROM {table} WHERE jkey LIKE ?1 ESCAPE '\\'"),
            [escape_like(&pkey) + "\u{1f}%"],
        )?;
    }
    for table in ["branches", "ledger", "origins", "backfills", "partitions"] {
        tx.execute(&format!("DELETE FROM {table} WHERE pkey = ?1"), [&pkey])?;
    }
    Ok(())
}

fn escape_like(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

// --- span and backfill journals ----------------------------------------------

/// The last node ever appended to a SQLite journal, or 0.
pub(crate) fn journal_tail(conn: &Connection, jkey: &str) -> anyhow::Result<NodeId> {
    Ok(conn
        .query_row(
            "SELECT last_node FROM journal_tails WHERE jkey = ?1",
            [jkey],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .unwrap_or(0) as NodeId)
}

/// The first node not yet deleted, or `tail + 1` when the journal is empty.
pub(crate) fn retained_start(conn: &Connection, jkey: &str) -> anyhow::Result<NodeId> {
    let first: Option<i64> = conn
        .query_row(
            "SELECT MIN(node) FROM span_nodes WHERE jkey = ?1",
            [jkey],
            |row| row.get(0),
        )
        .optional()?
        .flatten();
    match first {
        Some(first) => Ok(first as NodeId),
        None => Ok(journal_tail(conn, jkey)? + 1),
    }
}

/// Append entries, returning the inclusive node range assigned.
pub(crate) fn append_span_entries(
    tx: &Transaction<'_>,
    jkey: &str,
    entries: &[SpanEntry],
) -> anyhow::Result<Option<(NodeId, NodeId)>> {
    if entries.is_empty() {
        return Ok(None);
    }
    let tail = journal_tail(tx, jkey)?;
    let mut statement =
        tx.prepare_cached("INSERT INTO span_nodes (jkey, node, entry) VALUES (?1, ?2, ?3)")?;
    for (offset, entry) in entries.iter().enumerate() {
        let node = tail + 1 + offset as NodeId;
        statement.execute(params![jkey, node as i64, encode_span_entry(entry)])?;
    }
    let last = tail + entries.len() as NodeId;
    tx.execute(
        "INSERT INTO journal_tails (jkey, last_node) VALUES (?1, ?2)
         ON CONFLICT(jkey) DO UPDATE SET last_node = excluded.last_node",
        params![jkey, last as i64],
    )?;
    Ok(Some((tail + 1, last)))
}

pub(crate) fn read_span_entries(
    conn: &Connection,
    jkey: &str,
    from: NodeId,
    limit: usize,
) -> anyhow::Result<Vec<(NodeId, SpanEntry)>> {
    let mut statement = conn.prepare_cached(
        "SELECT node, entry FROM span_nodes WHERE jkey = ?1 AND node >= ?2 ORDER BY node LIMIT ?3",
    )?;
    let rows = statement
        .query_map(params![jkey, from as i64, limit as i64], |row| {
            Ok((row.get::<_, i64>(0)? as NodeId, row.get::<_, Vec<u8>>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    rows.into_iter()
        .map(|(node, bytes)| Ok((node, decode_span_entry(&bytes)?)))
        .collect()
}

pub(crate) fn delete_span_entries_before(
    tx: &Transaction<'_>,
    jkey: &str,
    boundary: NodeId,
) -> anyhow::Result<usize> {
    Ok(tx.execute(
        "DELETE FROM span_nodes WHERE jkey = ?1 AND node < ?2",
        params![jkey, boundary as i64],
    )?)
}

pub(crate) fn delete_journal(tx: &Transaction<'_>, jkey: &str) -> anyhow::Result<()> {
    for table in ["span_nodes", "journal_tails", "heads", "failures"] {
        tx.execute(&format!("DELETE FROM {table} WHERE jkey = ?1"), [jkey])?;
    }
    Ok(())
}

// --- heads ------------------------------------------------------------------

pub(crate) fn head(
    conn: &Connection,
    jkey: &str,
    consumer: &str,
) -> anyhow::Result<Option<NodeId>> {
    Ok(conn
        .query_row(
            "SELECT next_node FROM heads WHERE jkey = ?1 AND consumer = ?2",
            [jkey, consumer],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .map(|node| node as NodeId))
}

/// Advance a head. Heads only move forward; a stale or conflicting value is an
/// internal error.
pub(crate) fn advance_head(
    tx: &Transaction<'_>,
    jkey: &str,
    consumer: &str,
    next: NodeId,
) -> anyhow::Result<()> {
    let current = head(tx, jkey, consumer)?.unwrap_or(1);
    anyhow::ensure!(
        next >= current,
        "head for {consumer} on {jkey} cannot move back from {current} to {next}"
    );
    tx.execute(
        "INSERT INTO heads (jkey, consumer, next_node) VALUES (?1, ?2, ?3)
         ON CONFLICT(jkey, consumer) DO UPDATE SET next_node = excluded.next_node",
        params![jkey, consumer, next as i64],
    )?;
    Ok(())
}

/// Register a consumer at `next` unless it already exists. Returns its head.
pub(crate) fn register_head(
    tx: &Transaction<'_>,
    jkey: &str,
    consumer: &str,
    next: NodeId,
) -> anyhow::Result<NodeId> {
    if let Some(existing) = head(tx, jkey, consumer)? {
        return Ok(existing);
    }
    tx.execute(
        "INSERT INTO heads (jkey, consumer, next_node) VALUES (?1, ?2, ?3)",
        params![jkey, consumer, next as i64],
    )?;
    Ok(next)
}

pub(crate) fn heads_for(conn: &Connection, jkey: &str) -> anyhow::Result<Vec<(String, NodeId)>> {
    let mut statement =
        conn.prepare_cached("SELECT consumer, next_node FROM heads WHERE jkey = ?1")?;
    let rows = statement
        .query_map([jkey], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as NodeId))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

// --- failures ---------------------------------------------------------------

pub(crate) fn failure(
    conn: &Connection,
    jkey: &str,
    consumer: &str,
) -> anyhow::Result<Option<String>> {
    Ok(conn
        .query_row(
            "SELECT record FROM failures WHERE jkey = ?1 AND consumer = ?2",
            [jkey, consumer],
            |row| row.get(0),
        )
        .optional()?)
}

pub(crate) fn set_failure(
    tx: &Transaction<'_>,
    jkey: &str,
    consumer: &str,
    record: &str,
) -> anyhow::Result<()> {
    tx.execute(
        "INSERT INTO failures (jkey, consumer, record) VALUES (?1, ?2, ?3)
         ON CONFLICT(jkey, consumer) DO UPDATE SET record = excluded.record",
        params![jkey, consumer, record],
    )?;
    Ok(())
}

pub(crate) fn clear_failure(
    tx: &Transaction<'_>,
    jkey: &str,
    consumer: &str,
) -> anyhow::Result<()> {
    tx.execute(
        "DELETE FROM failures WHERE jkey = ?1 AND consumer = ?2",
        [jkey, consumer],
    )?;
    Ok(())
}

pub(crate) fn failures(conn: &Connection) -> anyhow::Result<Vec<(String, String, String)>> {
    let mut statement = conn.prepare("SELECT jkey, consumer, record FROM failures")?;
    let rows = statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

// --- branches and backfills -------------------------------------------------

pub(crate) fn branch_state(
    conn: &Connection,
    pkey: &str,
    route: &str,
) -> anyhow::Result<Option<String>> {
    Ok(conn
        .query_row(
            "SELECT state FROM branches WHERE pkey = ?1 AND route = ?2",
            [pkey, route],
            |row| row.get(0),
        )
        .optional()?)
}

pub(crate) fn set_branch_state(
    tx: &Transaction<'_>,
    pkey: &str,
    route: &str,
    state: &str,
) -> anyhow::Result<()> {
    tx.execute(
        "INSERT INTO branches (pkey, route, state) VALUES (?1, ?2, ?3)
         ON CONFLICT(pkey, route) DO UPDATE SET state = excluded.state",
        params![pkey, route, state],
    )?;
    Ok(())
}

pub(crate) fn branches(conn: &Connection, pkey: &str) -> anyhow::Result<Vec<(String, String)>> {
    let mut statement = conn.prepare_cached("SELECT route, state FROM branches WHERE pkey = ?1")?;
    let rows = statement
        .query_map([pkey], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

pub(crate) fn backfill_state(
    conn: &Connection,
    pkey: &str,
    route: &str,
) -> anyhow::Result<Option<String>> {
    Ok(conn
        .query_row(
            "SELECT state FROM backfills WHERE pkey = ?1 AND route = ?2",
            [pkey, route],
            |row| row.get(0),
        )
        .optional()?)
}

pub(crate) fn set_backfill_state(
    tx: &Transaction<'_>,
    pkey: &str,
    route: &str,
    state: &str,
) -> anyhow::Result<()> {
    tx.execute(
        "INSERT INTO backfills (pkey, route, state) VALUES (?1, ?2, ?3)
         ON CONFLICT(pkey, route) DO UPDATE SET state = excluded.state",
        params![pkey, route, state],
    )?;
    Ok(())
}

pub(crate) fn remove_backfill(tx: &Transaction<'_>, pkey: &str, route: &str) -> anyhow::Result<()> {
    tx.execute(
        "DELETE FROM backfills WHERE pkey = ?1 AND route = ?2",
        [pkey, route],
    )?;
    Ok(())
}

pub(crate) fn backfills(conn: &Connection, pkey: &str) -> anyhow::Result<Vec<(String, String)>> {
    let mut statement =
        conn.prepare_cached("SELECT route, state FROM backfills WHERE pkey = ?1")?;
    let rows = statement
        .query_map([pkey], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

// --- delivery ledger --------------------------------------------------------

#[derive(Debug, Default, Clone)]
pub(crate) struct LedgerRows {
    pub(crate) completed: std::collections::HashSet<String>,
    pub(crate) late_merges: std::collections::HashSet<String>,
    pub(crate) origins: std::collections::HashMap<String, String>,
}

pub(crate) fn ledger(conn: &Connection, pkey: &str, route: &str) -> anyhow::Result<LedgerRows> {
    let mut rows = LedgerRows::default();
    let mut statement =
        conn.prepare_cached("SELECT kind, id FROM ledger WHERE pkey = ?1 AND route = ?2")?;
    for item in statement.query_map([pkey, route], |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
    })? {
        let (kind, id) = item?;
        if kind == LEDGER_LATE_MERGE {
            rows.late_merges.insert(id);
        } else {
            rows.completed.insert(id);
        }
    }
    let mut statement = conn
        .prepare_cached("SELECT span_id, snapshot FROM origins WHERE pkey = ?1 AND route = ?2")?;
    for item in statement.query_map([pkey, route], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })? {
        let (span_id, snapshot) = item?;
        rows.origins.insert(span_id, snapshot);
    }
    Ok(rows)
}

pub(crate) fn add_ledger_ids(
    tx: &Transaction<'_>,
    pkey: &str,
    route: &str,
    kind: i64,
    ids: &[String],
) -> anyhow::Result<()> {
    let mut statement = tx.prepare_cached(
        "INSERT OR IGNORE INTO ledger (pkey, route, kind, id) VALUES (?1, ?2, ?3, ?4)",
    )?;
    for id in ids {
        statement.execute(params![pkey, route, kind, id])?;
    }
    Ok(())
}

pub(crate) fn add_origins(
    tx: &Transaction<'_>,
    pkey: &str,
    route: &str,
    origins: &[(String, String)],
) -> anyhow::Result<()> {
    let mut statement = tx.prepare_cached(
        "INSERT OR IGNORE INTO origins (pkey, route, span_id, snapshot) VALUES (?1, ?2, ?3, ?4)",
    )?;
    for (span_id, snapshot) in origins {
        statement.execute(params![pkey, route, span_id, snapshot])?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::types::JournalId;

    fn store() -> (tempfile::TempDir, Arc<Store>) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("pipeline.sqlite")).unwrap();
        (dir, store)
    }

    #[test]
    fn appends_reads_and_deletes_span_nodes() {
        let (_dir, store) = store();
        let partition = PartitionId::new("codex", "s");
        let jkey = JournalId::spans(&partition).key();
        let entries = vec![
            SpanEntry::Barrier {
                id: "a".into(),
                finalize: false,
            },
            SpanEntry::BackfillComplete,
        ];
        let range = store
            .write_blocking(|tx| append_span_entries(tx, &jkey, &entries))
            .unwrap();
        assert_eq!(range, Some((1, 2)));
        let read = store
            .read_blocking(|conn| read_span_entries(conn, &jkey, 2, 10))
            .unwrap();
        assert_eq!(read.len(), 1);
        store
            .write_blocking(|tx| delete_span_entries_before(tx, &jkey, 3))
            .unwrap();
        assert_eq!(
            store
                .read_blocking(|conn| retained_start(conn, &jkey))
                .unwrap(),
            3
        );
        // Node ids are never reused after deletion.
        let range = store
            .write_blocking(|tx| append_span_entries(tx, &jkey, &entries[..1]))
            .unwrap();
        assert_eq!(range, Some((3, 3)));
    }

    #[test]
    fn heads_only_move_forward() {
        let (_dir, store) = store();
        store
            .write_blocking(|tx| {
                assert_eq!(register_head(tx, "j", "c", 1)?, 1);
                advance_head(tx, "j", "c", 4)?;
                assert_eq!(register_head(tx, "j", "c", 1)?, 4);
                Ok(())
            })
            .unwrap();
        assert!(store
            .write_blocking(|tx| advance_head(tx, "j", "c", 2))
            .is_err());
    }

    #[test]
    fn failed_transactions_write_nothing() {
        let (_dir, store) = store();
        let result: anyhow::Result<()> = store.write_blocking(|tx| {
            advance_head(tx, "j", "c", 5)?;
            anyhow::bail!("injected")
        });
        assert!(result.is_err());
        assert_eq!(
            store.read_blocking(|conn| head(conn, "j", "c")).unwrap(),
            None
        );
    }

    #[test]
    fn touching_a_partition_only_moves_its_activity_forward() {
        let (_dir, store) = store();
        let partition = PartitionId::new("codex", "touched");
        let activity = |store: &Store| {
            store
                .read_blocking(|conn| Ok(partitions(conn)?.remove(0).last_activity_ms))
                .unwrap()
        };
        store
            .write_blocking(|tx| ensure_partition(tx, &partition, "ns", 1_000))
            .unwrap();
        store
            .write_blocking(|tx| touch_partition(tx, &partition, 5_000))
            .unwrap();
        assert_eq!(activity(&store), 5_000);
        store
            .write_blocking(|tx| touch_partition(tx, &partition, 2_000))
            .unwrap();
        assert_eq!(activity(&store), 5_000);
    }

    #[test]
    fn deletes_one_partition_only() {
        let (_dir, store) = store();
        let kept = PartitionId::new("codex", "kept");
        let gone = PartitionId::new("codex", "gone");
        store
            .write_blocking(|tx| {
                for partition in [&kept, &gone] {
                    ensure_partition(tx, partition, "ns", 1)?;
                    let jkey = JournalId::spans(partition).key();
                    append_span_entries(tx, &jkey, &[SpanEntry::BackfillComplete])?;
                    advance_head(tx, &jkey, "c", 2)?;
                }
                delete_partition(tx, &gone)
            })
            .unwrap();
        let rows = store.read_blocking(partitions).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].partition, kept);
        assert_eq!(
            store
                .read_blocking(|conn| journal_tail(conn, &JournalId::spans(&kept).key()))
                .unwrap(),
            1
        );
    }
}
