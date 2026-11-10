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
