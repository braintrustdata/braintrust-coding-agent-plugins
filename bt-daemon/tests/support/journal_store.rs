#![allow(
    dead_code,
    reason = "Shared integration-test inspection helpers are used by different suites."
)]
//! Inspect recovery ownership in session storage from integration tests.
use bt_daemon::storage_inspect::{Fault, Inspector};
use serde_json::Value;
use std::path::{Path, PathBuf};

pub fn path(data: &Path, source: &str, session: &str) -> PathBuf {
    bt_daemon::storage_inspect::session_path(data, source, session)
}
fn existing(data: &Path, source: &str, session: &str) -> Option<Inspector> {
    Inspector::existing(&path(data, source, session))
}
/// The translator continuation for a session, if one has been saved.
pub fn checkpoint(data: &Path, source: &str, session: &str) -> Option<Value> {
    existing(data, source, session)?
        .checkpoint()
        .map(|(_, value)| value)
}
pub fn identity(data: &Path, source: &str, session: &str) -> Value {
    existing(data, source, session)
        .and_then(|db| db.identity())
        .unwrap_or(Value::Null)
}
/// Captured events that translation has not yet consumed.
pub fn pending(data: &Path, source: &str, session: &str) -> Vec<Value> {
    existing(data, source, session)
        .map(|db| db.events())
        .unwrap_or_default()
}
/// Debug-source events accepted: consumed into the continuation, or pending.
pub fn accepted(data: &Path, source: &str, session: &str) -> u64 {
    let Some(db) = existing(data, source, session) else {
        return 0;
    };
    let count = db
        .checkpoint()
        .and_then(|(_, s)| s["native"]["event_seq"].as_u64())
        .unwrap_or(0);
    count
        + db.events()
            .iter()
            .filter(|v| v.get("event").is_some())
            .count() as u64
}
pub fn block_translation(data: &Path, source: &str, session: &str) {
    Inspector::session(data, source, session)
        .unwrap()
        .inject(Fault::Handoff)
        .unwrap();
}
pub fn unblock_translation(data: &Path, source: &str, session: &str) {
    Inspector::session(data, source, session)
        .unwrap()
        .clear(Fault::Handoff)
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
        let Some(db) = Inspector::existing(&entry.path()) else {
            continue;
        };
        let succeeded = |value: &Value| {
            value["source"] == "opencode" && value["event"] == "session.execution.succeeded"
        };
        if db.events().iter().any(succeeded) || db.identity().is_some_and(|v| succeeded(&v)) {
            return true;
        }
        if db.checkpoint().is_some_and(|(_, value)| {
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
