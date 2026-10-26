//! Disk index for birth metadata. The append-only ledger remains authoritative.
use crate::translate::OriginSnapshot;
use parking_lot::Mutex;
use rusqlite::{Connection, OptionalExtension};
use std::{collections::HashMap, path::PathBuf, sync::Arc};

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
                CREATE TABLE IF NOT EXISTS origins(span_id TEXT PRIMARY KEY, value TEXT NOT NULL)",
            )?;
            Ok(Self(Arc::new(Mutex::new(db))))
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
