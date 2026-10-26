//! Disk index for birth metadata and completed IDs. The append-only ledger remains authoritative.
use super::LedgerFile;
use crate::translate::OriginSnapshot;
use parking_lot::Mutex;
use rusqlite::{Connection, OptionalExtension};
use std::io::BufRead;
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
};

pub(super) struct OriginIndex(Arc<Mutex<Connection>>);
impl OriginIndex {
    pub async fn open(path: PathBuf) -> anyhow::Result<Self> {
        tokio::task::spawn_blocking(move || {
            let db = Connection::open(path)?;
            db.busy_timeout(std::time::Duration::from_secs(5))?;
            // This index contains only small metadata, never SpanOp payloads.
            // Bound SQLite's own cache too; the ledger rebuilds a missing index.
            db.execute_batch(
                "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA cache_size=-64;
                CREATE TABLE IF NOT EXISTS origins(span_id TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS completed(kind INTEGER NOT NULL, id TEXT NOT NULL,
                    PRIMARY KEY(kind,id)) WITHOUT ROWID",
            )?;
            Ok(Self(Arc::new(Mutex::new(db))))
        })
        .await?
    }

    // Rebuild membership under the same cross-process ledger lock used by writers.
    // The checkpoint and append-only log remain authoritative after a crash.
    pub async fn rebuild_completed(
        &self,
        spans: HashSet<String>,
        late_merges: HashSet<String>,
        journal: PathBuf,
    ) -> anyhow::Result<()> {
        let db = self.0.clone();
        tokio::task::spawn_blocking(move || {
            let mut db = db.lock();
            let tx = db.transaction()?;
            tx.execute("DELETE FROM completed", [])?;
            {
                let mut insert =
                    tx.prepare_cached("INSERT OR IGNORE INTO completed(kind,id) VALUES (?,?)")?;
                let mut remember =
                    |spans: HashSet<String>, late_merges: HashSet<String>| -> anyhow::Result<()> {
                        for (kind, ids) in [(0, spans), (1, late_merges)] {
                            for id in ids {
                                insert.execute(rusqlite::params![kind, id])?;
                            }
                        }
                        Ok(())
                    };
                remember(spans, late_merges)?;
                match std::fs::File::open(journal) {
                    Ok(file) => {
                        let mut reader = std::io::BufReader::new(file);
                        let mut line = Vec::new();
                        loop {
                            line.clear();
                            if reader.read_until(b'\n', &mut line)? == 0
                                || line.last() != Some(&b'\n')
                            {
                                break;
                            }
                            let update: LedgerFile = serde_json::from_slice(&line)?;
                            remember(update.completed_span_ids, update.late_merge_ids)?;
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
            }
            tx.commit()?;
            Ok(())
        })
        .await?
    }

    pub async fn remember_completed(
        &self,
        spans: HashSet<String>,
        late_merges: HashSet<String>,
    ) -> anyhow::Result<()> {
        if spans.is_empty() && late_merges.is_empty() {
            return Ok(());
        }
        let db = self.0.clone();
        tokio::task::spawn_blocking(move || {
            let mut db = db.lock();
            let tx = db.transaction()?;
            {
                let mut insert =
                    tx.prepare_cached("INSERT OR IGNORE INTO completed(kind,id) VALUES (?,?)")?;
                for (kind, ids) in [(0, spans), (1, late_merges)] {
                    for id in ids {
                        insert.execute(rusqlite::params![kind, id])?;
                    }
                }
            }
            tx.commit()?;
            Ok(())
        })
        .await?
    }

    // Return only membership requested by the current operation, never the
    // session's complete history. No SpanOp payload is stored or read here.
    pub async fn completed(
        &self,
        spans: Vec<String>,
        late_merges: Vec<String>,
    ) -> anyhow::Result<(HashSet<String>, HashSet<String>)> {
        if spans.is_empty() && late_merges.is_empty() {
            return Ok(Default::default());
        }
        let db = self.0.clone();
        tokio::task::spawn_blocking(move || {
            let db = db.lock();
            let mut query = db.prepare_cached("SELECT 1 FROM completed WHERE kind=? AND id=?")?;
            let mut found = [HashSet::new(), HashSet::new()];
            for (kind, ids) in [(0, spans), (1, late_merges)] {
                for id in ids {
                    if query.exists(rusqlite::params![kind, &id])? {
                        found[kind].insert(id);
                    }
                }
            }
            let [spans, late_merges] = found;
            Ok((spans, late_merges))
        })
        .await?
    }

    pub async fn remember(
        &self,
        origins: HashMap<String, Arc<OriginSnapshot>>,
    ) -> anyhow::Result<()> {
        self.write(origins, false).await
    }

    pub async fn rebuild(
        &self,
        origins: HashMap<String, Arc<OriginSnapshot>>,
    ) -> anyhow::Result<()> {
        self.write(origins, true).await
    }

    async fn write(
        &self,
        origins: HashMap<String, Arc<OriginSnapshot>>,
        replace: bool,
    ) -> anyhow::Result<()> {
        if origins.is_empty() && !replace {
            return Ok(());
        }
        let db = self.0.clone();
        tokio::task::spawn_blocking(move || {
            let mut db = db.lock();
            let tx = db.transaction()?;
            if replace {
                tx.execute("DELETE FROM origins", [])?;
            }
            for (span, origin) in origins {
                tx.execute(
                    "INSERT OR IGNORE INTO origins(span_id,value) VALUES (?,?)",
                    rusqlite::params![span, serde_json::to_string(&origin)?],
                )?;
            }
            tx.commit()?;
            Ok(())
        })
        .await?
    }

    pub async fn find(
        &self,
        spans: Vec<String>,
    ) -> anyhow::Result<HashMap<String, Arc<OriginSnapshot>>> {
        let db = self.0.clone();
        tokio::task::spawn_blocking(move || {
            let db = db.lock();
            let mut query = db.prepare_cached("SELECT value FROM origins WHERE span_id=?")?;
            let mut origins = HashMap::new();
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
        .await?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn membership_queries_return_only_requested_ids_and_keep_namespaces_separate() {
        let temp = tempfile::tempdir().unwrap();
        let index = OriginIndex::open(temp.path().join("index.sqlite"))
            .await
            .unwrap();
        index
            .remember_completed(
                (0..50_000).map(|i| format!("span-{i}")).collect(),
                HashSet::from(["late-only".into()]),
            )
            .await
            .unwrap();
        let (spans, merges) = index
            .completed(
                vec!["span-49999".into(), "late-only".into(), "missing".into()],
                vec!["span-49999".into(), "late-only".into()],
            )
            .await
            .unwrap();
        assert_eq!(spans, HashSet::from(["span-49999".into()]));
        assert_eq!(merges, HashSet::from(["late-only".into()]));
    }

    #[tokio::test]
    async fn failed_rebuild_preserves_membership_visible_to_existing_writers() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("index.sqlite");
        let index = OriginIndex::open(path.clone()).await.unwrap();
        index
            .remember_completed(HashSet::from(["accepted".into()]), HashSet::new())
            .await
            .unwrap();
        let peer = OriginIndex::open(path).await.unwrap();
        let journal = temp.path().join("ledger.ndjson");
        std::fs::write(&journal, b"{invalid}\n").unwrap();
        assert!(index
            .rebuild_completed(HashSet::new(), HashSet::new(), journal.clone())
            .await
            .is_err());
        assert!(peer
            .completed(vec!["accepted".into()], vec![])
            .await
            .unwrap()
            .0
            .contains("accepted"));
        std::fs::write(&journal, b"{\"completed_span_ids\":[\"new\"]}\n{partial").unwrap();
        index
            .rebuild_completed(HashSet::new(), HashSet::new(), journal)
            .await
            .unwrap();
        assert_eq!(
            peer.completed(vec!["accepted".into(), "new".into()], vec![])
                .await
                .unwrap()
                .0,
            HashSet::from(["new".into()])
        );
    }
}
