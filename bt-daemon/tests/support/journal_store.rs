#![allow(
    dead_code,
    reason = "Shared integration-test inspection helpers are used by different suites."
)]
//! Inspect recovery ownership at the SQLite boundary in integration tests.
use rusqlite::{Connection, OptionalExtension};
use serde_json::Value;
use std::path::{Path, PathBuf};

pub fn path(data: &Path, source: &str, session: &str) -> PathBuf {
    let journal = bt_daemon::source_journal_path(data, source, session);
    let name = journal.file_stem().unwrap().to_str().unwrap();
    data.join("pipeline")
        .join(format!("{}.sqlite", name.rsplit("--").next().unwrap()))
}
fn saved(db: &Connection) -> Option<Value> {
    let exists: bool = db
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='source_checkpoint')",
            [],
            |r| r.get(0),
        )
        .unwrap();
    if !exists {
        return None;
    }
    let bytes: Option<Vec<u8>> = db
        .query_row("SELECT value FROM source_checkpoint WHERE id=1", [], |r| {
            r.get(0)
        })
        .optional()
        .unwrap();
    bytes.map(|v| serde_json::from_slice(&v).unwrap())
}
pub fn checkpoint(data: &Path, source: &str, session: &str) -> Option<Value> {
    saved(&Connection::open(path(data, source, session)).unwrap())
}
pub fn identity(data: &Path, source: &str, session: &str) -> Value {
    let db = Connection::open(path(data, source, session)).unwrap();
    let bytes: Vec<u8> = db
        .query_row(
            "SELECT value FROM collection_identity WHERE name='events'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}
pub fn pending(data: &Path, source: &str, session: &str) -> Vec<Value> {
    let db = Connection::open(path(data, source, session)).unwrap();
    let mut query = db
        .prepare("SELECT value FROM collection_records WHERE name='events' ORDER BY id")
        .unwrap();
    query
        .query_map([], |r| r.get::<_, Vec<u8>>(0))
        .unwrap()
        .map(|r| serde_json::from_slice(&r.unwrap()).unwrap())
        .collect()
}
pub fn accepted(data: &Path, source: &str, session: &str) -> u64 {
    let mut db = Connection::open(path(data, source, session)).unwrap();
    let tx = db.transaction().unwrap();
    let count = saved(&tx)
        .and_then(|s| s["native"]["event_seq"].as_u64())
        .unwrap_or(0);
    let mut query = tx
        .prepare("SELECT value FROM collection_records WHERE name='events'")
        .unwrap();
    let pending = query
        .query_map([], |r| r.get::<_, Vec<u8>>(0))
        .unwrap()
        .map(|r| serde_json::from_slice::<Value>(&r.unwrap()).unwrap())
        .filter(|v| v.get("event").is_some())
        .count();
    count + pending as u64
}
pub fn block_translation(data: &Path, source: &str, session: &str) {
    let path = path(data, source, session);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let db = Connection::open(path).unwrap();
    db.execute_batch("CREATE TABLE IF NOT EXISTS batches(input INTEGER PRIMARY KEY,committed INTEGER NOT NULL DEFAULT 0,observation INTEGER NOT NULL DEFAULT 1); CREATE TRIGGER block_translation BEFORE INSERT ON batches BEGIN SELECT RAISE(FAIL,'injected translation storage failure'); END;").unwrap();
}
pub fn unblock_translation(data: &Path, source: &str, session: &str) {
    Connection::open(path(data, source, session))
        .unwrap()
        .execute_batch("DROP TRIGGER block_translation")
        .unwrap();
}

// A native completion can still be queued, or already transferred into durable
// continuation state while its RPC acknowledgement is held by the test proxy.
pub fn has_completed_opencode_capture(data: &Path) -> bool {
    let Ok(entries) = std::fs::read_dir(data.join("pipeline")) else {
        return false;
    };
    for entry in entries.flatten() {
        if entry.path().extension().is_none_or(|ext| ext != "sqlite") {
            continue;
        }
        let Ok(db) =
            Connection::open_with_flags(entry.path(), rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        else {
            continue;
        };
        let _ = db.busy_timeout(std::time::Duration::from_secs(1));
        let queued = (|| -> rusqlite::Result<bool> {
            let mut query = db.prepare(
                "SELECT value FROM collection_records WHERE name='events'
                 UNION ALL SELECT value FROM collection_identity WHERE name='events'",
            )?;
            for bytes in query.query_map([], |row| row.get::<_, Vec<u8>>(0))? {
                let Ok(bytes) = bytes else {
                    continue;
                };
                if serde_json::from_slice::<Value>(&bytes).is_ok_and(|value| {
                    value["source"] == "opencode" && value["event"] == "session.execution.succeeded"
                }) {
                    return Ok(true);
                }
            }
            Ok(false)
        })()
        .unwrap_or(false);
        if queued {
            return true;
        }
        // The schema can be incomplete during the first capture transaction.
        let state = db
            .query_row(
                "SELECT value FROM source_checkpoint WHERE id=1",
                [],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok());
        if state.is_some_and(|value| {
            value["source"] == "opencode"
                && value["native"]["v2"]["sessions"]
                    .as_object()
                    .is_some_and(|sessions| {
                        sessions.values().any(|session| session["idle"] == true)
                    })
        }) {
            return true;
        }
    }
    false
}
