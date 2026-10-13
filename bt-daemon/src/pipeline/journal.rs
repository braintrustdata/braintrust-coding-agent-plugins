//! Concrete durable channel: one ordered SpanOp log and a head per route.
use super::{Completion, Delivery};
use crate::{
    translate::{OriginSnapshot, SpanOp},
    wire::SessionRoute,
};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::{
    path::Path,
    sync::{Arc, Mutex},
};
use tokio::sync::watch;

#[derive(Clone)]
pub(crate) struct SpanJournal {
    connection: Arc<Mutex<Connection>>,
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
        let connection = tokio::task::spawn_blocking(move || -> anyhow::Result<Connection> {
            std::fs::create_dir_all(path.parent().expect("journal parent"))?;
            let db = Connection::open(path)?;
            db.busy_timeout(std::time::Duration::from_secs(5))?;
            let version: u32 = db.query_row("PRAGMA user_version", [], |row| row.get(0))?;
            anyhow::ensure!(version <= 1, "unsupported SpanOp journal schema {version}");
            db.execute_batch(
                "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
                CREATE TABLE IF NOT EXISTS batches (
                    input INTEGER PRIMARY KEY, committed INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS ops (
                    position INTEGER PRIMARY KEY AUTOINCREMENT,
                    input INTEGER NOT NULL, value BLOB NOT NULL);
                CREATE INDEX IF NOT EXISTS ops_input ON ops(input);
                CREATE TABLE IF NOT EXISTS heads (
                    route TEXT PRIMARY KEY, position INTEGER NOT NULL DEFAULT 0,
                    failure TEXT);
                PRAGMA user_version=1;",
            )?;
            Ok(db)
        })
        .await??;
        let (changed, _) = watch::channel(0);
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
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
            operation(&mut db)
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
    pub async fn begin(&self, input: u64) -> anyhow::Result<bool> {
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
                "INSERT OR REPLACE INTO batches(input,committed) VALUES (?,0)",
                [input],
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
        let tail = self
            .with_db(move |db| {
                let tx = db.transaction()?;
                let previous: u64 = tx.query_row(
                    "SELECT coalesce(max(input),0) FROM batches WHERE committed=1",
                    [],
                    |r| r.get(0),
                )?;
                anyhow::ensure!(input >= previous, "translation commits must be ordered");
                anyhow::ensure!(
                    tx.execute("UPDATE batches SET committed=1 WHERE input=?", [input])? == 1,
                    "missing translation batch"
                );
                let tail = tx.query_row(
                    "SELECT coalesce(max(position),0) FROM ops WHERE input<=?",
                    [input],
                    |r| r.get(0),
                )?;
                tx.commit()?;
                Ok(tail)
            })
            .await?;
        self.changed
            .send_modify(|version| *version = version.wrapping_add(1));
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
        Ok(Cursor {
            journal: self.clone(),
            route,
            read: position,
            completed: position,
            changes: self.changed.subscribe(),
        })
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
}

impl Cursor {
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
        if rows.is_empty() {
            return Ok(None);
        }
        let through = rows.last().expect("nonempty rows").0;
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
            },
        )))
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
        Ok(())
    }

    pub async fn changed(&mut self) {
        let _ = self.changes.changed().await;
    }
}

struct HeadCompletion {
    journal: SpanJournal,
    route: String,
    expected: u64,
    through: u64,
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
                    return Ok(());
                }
                anyhow::ensure!(head == expected, "completion cannot skip unresolved input");
                tx.execute(
                    "UPDATE heads SET position=?, failure=NULL WHERE route=?",
                    params![through, route],
                )?;
                tx.commit()?;
                Ok(())
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::translate::SpanRow;

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
    async fn invisible_chunks_empty_commit_and_restart_preserve_the_boundary() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("spans.sqlite");
        let journal = SpanJournal::open(&path).await.unwrap();
        journal.begin(10).await.unwrap();
        journal.append(10, &[op("abandoned")]).await.unwrap();
        let mut reader = journal.subscribe(&route("a"), 0).await.unwrap();
        assert!(reader.next_batch(8).await.unwrap().is_none());
        drop(reader);
        drop(journal);
        let journal = SpanJournal::open(&path).await.unwrap();
        assert_eq!(journal.translated_through().await.unwrap(), 0);
        journal.begin(10).await.unwrap();
        journal.append(10, &[op("retained")]).await.unwrap();
        journal.commit(10).await.unwrap();
        assert!(!journal.begin(10).await.unwrap());
        journal.begin(20).await.unwrap();
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
        journal.begin(10).await.unwrap();
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
        journal.begin(10).await.unwrap();
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
}
