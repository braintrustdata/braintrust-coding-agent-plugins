//! Test support: inspect session storage and inject storage faults through a
//! typed interface, so tests outside this module never contain SQL.
#![allow(dead_code, reason = "Different test suites use different inspectors.")]
use rusqlite::{Connection, OptionalExtension};
use serde_json::Value;
use std::path::{Path, PathBuf};

/// A storage write that can be made to fail.
#[derive(Clone, Copy, Debug)]
pub enum Fault {
    /// The handoff of an input's translation and continuation.
    Handoff,
    InsertOps,
    AdvanceHead,
    RemoveEvents,
}

impl Fault {
    fn trigger(self) -> (&'static str, &'static str) {
        match self {
            Self::Handoff => ("fault_handoff", "BEFORE UPDATE ON translation"),
            Self::InsertOps => ("fault_insert_ops", "BEFORE INSERT ON ops"),
            Self::AdvanceHead => ("fault_head", "BEFORE UPDATE OF position ON heads"),
            Self::RemoveEvents => ("fault_remove_events", "BEFORE DELETE ON events"),
        }
    }
}

/// Stored record kinds that can be counted.
#[derive(Clone, Copy, Debug)]
pub enum Table {
    Events,
    Ops,
    Heads,
    Origins,
}

impl Table {
    fn name(self) -> &'static str {
        match self {
            Self::Events => "events",
            Self::Ops => "ops",
            Self::Heads => "heads",
            Self::Origins => "origins",
        }
    }
}

/// One route's durable delivery head.
#[derive(Debug, Clone)]
pub struct Head {
    pub position: u64,
    pub failure: Option<String>,
    /// Corrected settings recorded for a held head.
    pub settings: Option<Value>,
}

/// The database file holding one source session.
pub fn session_path(data_dir: &Path, source: &str, session_id: &str) -> PathBuf {
    super::events::session_path(data_dir, source, session_id)
}

/// A separate connection to a session database, independent of the daemon's.
pub struct Inspector(Connection);

impl Inspector {
    /// Open (creating at the current schema if needed) a session database.
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        Ok(Self(super::store::open_connection(path)?))
    }

    pub fn session(data_dir: &Path, source: &str, session_id: &str) -> anyhow::Result<Self> {
        Self::open(&session_path(data_dir, source, session_id))
    }

    /// Open without creating or upgrading anything; `None` if it is absent.
    pub fn existing(path: &Path) -> Option<Self> {
        let db =
            Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).ok()?;
        db.busy_timeout(std::time::Duration::from_secs(1)).ok()?;
        Some(Self(db))
    }

    pub fn count(&self, table: Table) -> u64 {
        self.0
            .query_row(&format!("SELECT count(*) FROM {}", table.name()), [], |r| {
                r.get(0)
            })
            .unwrap_or(0)
    }

    /// Event records awaiting translation, decoded as JSON.
    pub fn events(&self) -> Vec<Value> {
        let Ok(mut query) = self.0.prepare("SELECT value FROM events ORDER BY id") else {
            return Vec::new();
        };
        let Ok(rows) = query.query_map([], |r| r.get::<_, Vec<u8>>(0)) else {
            return Vec::new();
        };
        rows.filter_map(|row| serde_json::from_slice(&row.ok()?).ok())
            .collect()
    }

    /// Encoded SpanOp records in publication order.
    pub fn op_records(&self) -> Vec<Vec<u8>> {
        let Ok(mut query) = self.0.prepare("SELECT value FROM ops ORDER BY position") else {
            return Vec::new();
        };
        let Ok(rows) = query.query_map([], |r| r.get::<_, Vec<u8>>(0)) else {
            return Vec::new();
        };
        rows.filter_map(Result::ok).collect()
    }

    pub fn event_tail(&self) -> u64 {
        self.0
            .query_row("SELECT tail FROM event_state WHERE id=1", [], |r| r.get(0))
            .unwrap_or(0)
    }

    pub fn identity(&self) -> Option<Value> {
        let bytes: Vec<u8> = self
            .0
            .query_row("SELECT identity FROM event_state WHERE id=1", [], |r| {
                r.get(0)
            })
            .ok()?;
        serde_json::from_slice(&bytes).ok()
    }

    /// The translator continuation and the event ID it covers.
    pub fn checkpoint(&self) -> Option<(u64, Value)> {
        let (through, bytes): (u64, Vec<u8>) = self
            .0
            .query_row(
                "SELECT through,continuation FROM translation WHERE id=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
            .ok()??;
        Some((through, serde_json::from_slice(&bytes).ok()?))
    }

    pub fn heads(&self) -> Vec<Head> {
        let Ok(mut query) = self
            .0
            .prepare("SELECT position,failure,settings FROM heads")
        else {
            return Vec::new();
        };
        let Ok(rows) = query.query_map([], |r| {
            Ok(Head {
                position: r.get(0)?,
                failure: r.get(1)?,
                settings: r
                    .get::<_, Option<String>>(2)?
                    .and_then(|settings| serde_json::from_str(&settings).ok()),
            })
        }) else {
            return Vec::new();
        };
        rows.filter_map(Result::ok).collect()
    }

    /// The position of the last published SpanOp.
    pub fn published_tail(&self) -> u64 {
        self.0
            .query_row("SELECT tail FROM translation WHERE id=1", [], |r| r.get(0))
            .unwrap_or(0)
    }

    /// Heads that are failed or behind the published tail.
    pub fn held_heads(&self) -> u64 {
        self.0
            .query_row(
                "SELECT count(*) FROM heads WHERE failure IS NOT NULL
                 OR position<(SELECT tail FROM translation WHERE id=1)",
                [],
                |r| r.get(0),
            )
            .unwrap_or(0)
    }

    /// All captured input is translated and every head has delivered.
    pub fn delivered(&self) -> bool {
        self.checkpoint()
            .is_some_and(|(through, _)| through >= self.event_tail())
            && self.held_heads() == 0
    }

    pub fn inject(&self, fault: Fault) -> anyhow::Result<()> {
        let (name, when) = fault.trigger();
        self.0.execute_batch(&format!(
            "CREATE TRIGGER IF NOT EXISTS {name} {when} BEGIN SELECT RAISE(FAIL,'injected {name}'); END;"
        ))?;
        Ok(())
    }

    pub fn clear(&self, fault: Fault) -> anyhow::Result<()> {
        self.0
            .execute_batch(&format!("DROP TRIGGER IF EXISTS {}", fault.trigger().0))?;
        Ok(())
    }

    /// Make event records in `(after, through]` undecodable.
    pub fn corrupt_events(&self, after: u64, through: u64) -> anyhow::Result<()> {
        self.0.execute(
            "UPDATE events SET value=x'78' WHERE id>? AND id<=?",
            [after, through],
        )?;
        Ok(())
    }

    pub fn corrupt_checkpoint(&self) -> anyhow::Result<()> {
        self.0
            .execute_batch("UPDATE translation SET continuation=x'78' WHERE id=1")?;
        Ok(())
    }

    /// Make the session's last recorded activity `by` older.
    pub fn age(&self, by: std::time::Duration) -> anyhow::Result<()> {
        self.0.execute(
            "UPDATE event_state SET active_ms=active_ms-? WHERE id=1",
            [i64::try_from(by.as_millis())?],
        )?;
        Ok(())
    }

    /// Register a delivery head at the start, as a route subscribed before
    /// any output existed.
    pub fn add_head(&self, route: &impl serde::Serialize) -> anyhow::Result<()> {
        self.0.execute(
            "INSERT OR IGNORE INTO heads(route,position) VALUES (?,0)",
            [serde_json::to_string(route)?],
        )?;
        Ok(())
    }

    /// Return every head to the start and clear failures.
    pub fn rewind_heads(&self) -> anyhow::Result<()> {
        self.0
            .execute("UPDATE heads SET position=0,failure=NULL", [])?;
        Ok(())
    }

    /// Mark the database as written by a different storage schema.
    pub fn set_schema_version(&self, version: u32) -> anyhow::Result<()> {
        self.0
            .execute_batch(&format!("PRAGMA user_version={version}"))?;
        Ok(())
    }

    /// Undo [`Self::set_schema_version`].
    pub fn restore_schema_version(&self) -> anyhow::Result<()> {
        self.set_schema_version(super::store::SCHEMA)
    }

    pub fn checkpoint_wal(&self) -> anyhow::Result<()> {
        self.0.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;
        Ok(())
    }

    /// Copy the database, as a crash-consistent snapshot, to `to`.
    pub fn backup(&self, to: &Path) -> anyhow::Result<()> {
        self.0.execute(
            "VACUUM INTO ?",
            [to.to_str()
                .ok_or_else(|| anyhow::anyhow!("backup path is not UTF-8"))?],
        )?;
        Ok(())
    }
}
