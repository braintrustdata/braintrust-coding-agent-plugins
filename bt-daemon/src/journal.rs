//! On-disk naming for session state, and managed-run acceptance records.
//!
//! Captured events live in [`crate::collections`]. NDJSON journals written by
//! earlier daemons are only read there, to migrate them, and are then deleted.
//! Managed-run acceptance records remain separate so flush can discover
//! pipelines after the daemon that accepted them has restarted or idle-exited.

use crate::wire::{BackendAuth, Envelope, RedactedEnvelope, SessionRoute};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;

pub fn journal_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("journal")
}

pub(crate) fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Longest single path component, in bytes, that common filesystems accept.
pub(crate) const MAX_FILE_NAME_BYTES: usize = 255;

pub fn journal_path(data_dir: &Path, session_id: &str) -> PathBuf {
    journal_dir(data_dir).join(format!("{}.ndjson", sanitize(session_id)))
}

/// Source-qualified legacy journal path. The stable suffix prevents sanitized native
/// ids such as `a/b` and `a_b` from aliasing the same file, so a session id
/// too long for one file name keeps only its tail (Pi ids embed the session
/// file path). Names that fit are unchanged, so existing journals still match.
pub fn source_journal_path(data_dir: &Path, source: &str, session_id: &str) -> PathBuf {
    let prefix = format!("{}--", sanitize(source));
    let suffix = format!(
        "--{}.ndjson",
        crate::ids::session_storage_id(source, session_id)
    );
    let session = sanitize(session_id);
    let budget = MAX_FILE_NAME_BYTES.saturating_sub(prefix.len() + suffix.len());
    // `sanitize` emits ASCII only, so any byte offset is a char boundary.
    let session = &session[session.len().saturating_sub(budget)..];
    journal_dir(data_dir).join(format!("{prefix}{session}{suffix}"))
}

pub fn managed_run_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("managed-runs")
}

pub fn managed_run_path(data_dir: &Path, managed_run_id: &str) -> PathBuf {
    managed_run_dir(data_dir).join(format!("{}.ndjson", sanitize(managed_run_id)))
}

/// One delivery pipeline accepted from a managed child process tree.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManagedRunKey {
    /// Missing in records written before source-qualified delivery keys.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    pub session_id: String,
    pub route: SessionRoute,
}

/// Append one accepted delivery pipeline to the managed run's record.
pub async fn append_managed_run_key(
    data_dir: &Path,
    managed_run_id: &str,
    key: &ManagedRunKey,
) -> anyhow::Result<()> {
    let dir = managed_run_dir(data_dir);
    tokio::fs::create_dir_all(&dir).await?;
    let mut line = serde_json::to_vec(key)?;
    line.push(b'\n');
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(managed_run_path(data_dir, managed_run_id))
        .await?;
    file.write_all(&line).await?;
    file.flush().await?;
    Ok(())
}

/// Read a managed run's accepted delivery pipelines back, deduplicated.
/// A missing record means the run never produced an accepted event.
pub async fn read_managed_run_keys(data_dir: &Path, managed_run_id: &str) -> Vec<ManagedRunKey> {
    let Ok(data) = tokio::fs::read_to_string(managed_run_path(data_dir, managed_run_id)).await
    else {
        return Vec::new();
    };
    let mut keys = Vec::new();
    let mut seen = HashSet::new();
    for line in data.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if seen.insert(line.to_string()) {
            if let Ok(key) = serde_json::from_str::<ManagedRunKey>(line) {
                keys.push(key);
            }
        }
    }
    keys
}

/// Recover the source omitted by pre-source-qualified managed-run records.
/// Route matching avoids selecting an unrelated delivery pipeline when an old
/// session contained more than one destination. The source that owned the
/// legacy session namespace wins when several match.
pub async fn legacy_journal_source(
    data_dir: &Path,
    session_id: &str,
    route: &SessionRoute,
) -> Option<String> {
    let mut found = None;
    for storage in crate::collections::sessions(data_dir).await.ok()? {
        let Ok(events) = storage.events().await else {
            continue;
        };
        let Ok(Some((source, session))) = events.owner().await else {
            continue;
        };
        if session != session_id {
            continue;
        }
        let recorded = events
            .identity()
            .await
            .ok()
            .flatten()
            .and_then(|identity| identity.route)
            .is_some_and(|candidate| candidate.same_route(route));
        let registered = match storage.span_ops().await {
            Ok(ops) => ops
                .routes()
                .await
                .unwrap_or_default()
                .iter()
                .any(|candidate| candidate.same_route(route)),
            Err(_) => false,
        };
        if recorded || registered {
            if events.legacy_namespace().await.unwrap_or(false) {
                return Some(source);
            }
            found.get_or_insert(source);
        }
    }
    found
}

/// Best-effort age-based collection of managed-run records, mirroring journal
/// GC.
pub async fn gc_old_managed_runs(data_dir: &Path, max_age: std::time::Duration) {
    let dir = managed_run_dir(data_dir);
    let Ok(mut entries) = tokio::fs::read_dir(&dir).await else {
        return;
    };
    let now = std::time::SystemTime::now();
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if path.extension().and_then(|v| v.to_str()) != Some("ndjson") {
            continue;
        }
        let old = entry
            .metadata()
            .await
            .ok()
            .and_then(|m| m.modified().ok())
            .and_then(|modified| now.duration_since(modified).ok())
            .is_some_and(|age| age > max_age);
        if old {
            let _ = tokio::fs::remove_file(&path).await;
        }
    }
}

/// Reconstruct a translator-usable [`Envelope`] from a redacted journal entry.
/// The live token is gone (redacted), so `auth.token` is empty — fine for
/// rebuilding translator state; the sink must be re-supplied live credentials
/// if replay needs to actually deliver.
pub fn envelope_from_redacted(r: RedactedEnvelope) -> Envelope {
    let route = r.route;
    let config = route.as_ref().map(|route| {
        route.with_auth(BackendAuth {
            token: String::new(),
            api_url: None,
            app_url: None,
            org_name: route.auth.org_name.clone(),
            org_id: None,
        })
    });
    Envelope {
        source: r.source,
        source_version: r.source_version,
        plugin_version: r.plugin_version,
        session_id: r.session_id,
        event: r.event,
        ts_ms: r.ts_ms,
        managed_run_id: r.managed_run_id,
        capture: r.capture,
        payload: r.payload,
        route,
        config,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pi_journaled_request_configuration_replays_unchanged() {
        let schema = serde_json::json!({"type": "object"});
        for payload in [
            serde_json::json!({
                "max_tokens": 1024,
                "tools": [
                    {"name": "read", "input_schema": schema, "strict": true},
                    {"type": "web_search_20250305", "name": "web_search"},
                ],
                "tool_choice": {"type": "tool", "name": "read", "disable_parallel_tool_use": true},
            }),
            serde_json::json!({"config": {
                "temperature": 0.5,
                "tools": [{"functionDeclarations": [{"name": "read", "parameters": schema}]}, {"googleSearch": {}}],
                "toolConfig": {"functionCallingConfig": {"mode": "ANY"}},
            }}),
            serde_json::json!({
                "toolConfig": {"tools": [{"toolSpec": {"name": "read", "inputSchema": {"json": schema}}}]},
                "inferenceConfig": {"maxTokens": 512},
            }),
        ] {
            let journaled =
                serde_json::Value::Object(crate::translate::pi_request_config(&payload));
            assert_eq!(
                serde_json::Value::Object(crate::translate::pi_request_config(&journaled)),
                journaled
            );
        }
    }

    #[test]
    fn long_session_ids_get_distinct_bounded_journal_names() {
        let temp = tempfile::tempdir().unwrap();
        let shared = "x".repeat(400);
        let first = source_journal_path(temp.path(), "pi", &format!("a{shared}"));
        let second = source_journal_path(temp.path(), "pi", &format!("b{shared}"));
        assert_ne!(first, second);
        for path in [&first, &second] {
            assert!(path.file_name().unwrap().len() <= MAX_FILE_NAME_BYTES);
        }
        // Names that already fit keep their original layout so existing
        // journals are still found after an upgrade.
        assert_eq!(
            source_journal_path(temp.path(), "pi", "short/id"),
            journal_dir(temp.path()).join(format!(
                "pi--short_id--{}.ndjson",
                crate::ids::session_storage_id("pi", "short/id")
            ))
        );
    }

    #[tokio::test]
    async fn legacy_managed_run_records_recover_source_from_the_old_journal() {
        let temp = tempfile::tempdir().unwrap();
        let route = SessionRoute::default();
        let path = journal_path(temp.path(), "legacy");
        tokio::fs::create_dir_all(path.parent().unwrap())
            .await
            .unwrap();
        let mut lines = String::new();
        for (source, ts_ms) in [("codex", 1), ("claude", 2)] {
            let env = Envelope {
                source: source.into(),
                source_version: None,
                plugin_version: None,
                session_id: "legacy".into(),
                event: "SessionStart".into(),
                ts_ms,
                managed_run_id: None,
                payload: serde_json::json!({}),
                route: Some(route.clone()),
                config: None,
                capture: None,
            };
            lines.push_str(&serde_json::to_string(&env.redacted()).unwrap());
            lines.push('\n');
        }
        tokio::fs::write(&path, lines).await.unwrap();
        assert_eq!(
            legacy_journal_source(temp.path(), "legacy", &route).await,
            Some("codex".into())
        );
        assert!(!path.exists());
        for (source, owner) in [("codex", true), ("claude-code", false)] {
            let events = crate::collections::EventCollection::open(temp.path(), source, "legacy")
                .await
                .unwrap();
            assert_eq!(events.legacy_namespace().await.unwrap(), owner);
        }
    }
}
