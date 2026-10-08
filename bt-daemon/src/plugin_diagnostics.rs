//! Bounded, persistent diagnostics for span-plugin failures.

use crate::wire::SessionRoute;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_DIAGNOSTICS: usize = 128;
const FILE_NAME: &str = "span-plugin-errors.json";

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct PluginDiagnostic {
    pub source: String,
    pub plugin_path: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plugin_digest: Option<String>,
    /// The unmodified QuickJS or host exception, including its stack when
    /// QuickJS provides one.
    pub exception: String,
    pub first_seen_ms: i64,
    pub last_seen_ms: i64,
    pub occurrences: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub span_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pipeline_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub span_cursor: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_revisions: Option<u64>,
}

#[derive(Default, Serialize, Deserialize)]
struct DiagnosticStore {
    #[serde(default)]
    entries: Vec<PluginDiagnostic>,
}

pub fn record(
    data_dir: &Path,
    source: &str,
    plugin_path: &Path,
    exception: &str,
) -> anyhow::Result<()> {
    let path = diagnostics_path(data_dir);
    ensure_diagnostics_dir(&path)?;
    crate::settings::with_settings_lock(&path, || {
        let mut store = read_unlocked(&path)?;
        let now = now_ms();
        let digest = plugin_digest(plugin_path);
        if let Some(existing) = store.entries.iter_mut().find(|entry| {
            entry.source == source
                && entry.plugin_path == plugin_path
                && entry.plugin_digest == digest
                && entry.exception == exception
                && entry.session_id.is_none()
        }) {
            existing.last_seen_ms = now;
            existing.occurrences = existing.occurrences.saturating_add(1);
        } else {
            store.entries.push(PluginDiagnostic {
                source: source.to_owned(),
                plugin_path: plugin_path.to_path_buf(),
                plugin_digest: digest,
                exception: exception.to_owned(),
                first_seen_ms: now,
                last_seen_ms: now,
                occurrences: 1,
                session_id: None,
                span_id: None,
                operation: None,
                pipeline_id: None,
                span_cursor: None,
                pending_revisions: None,
            });
        }
        trim(&mut store);
        write_unlocked(&path, &store)
    })
}

pub fn pipeline_id(route: &SessionRoute) -> anyhow::Result<String> {
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(route)?)))
}

pub struct PipelineFailure<'a> {
    pub source: &'a str,
    pub session_id: &'a str,
    pub route: &'a SessionRoute,
    pub span_id: &'a str,
    pub operation: &'a str,
    pub plugin_path: &'a Path,
    pub exception: &'a str,
    pub span_cursor: u64,
    pub span_through: u64,
}

pub fn record_pipeline_failure(
    data_dir: &Path,
    failure: PipelineFailure<'_>,
) -> anyhow::Result<()> {
    let path = diagnostics_path(data_dir);
    ensure_diagnostics_dir(&path)?;
    let id = pipeline_id(failure.route)?;
    crate::settings::with_settings_lock(&path, || {
        let mut store = read_unlocked(&path)?;
        let now = now_ms();
        let digest = plugin_digest(failure.plugin_path);
        let existing = store.entries.iter_mut().find(|entry| {
            entry.source == failure.source
                && entry.session_id.as_deref() == Some(failure.session_id)
                && entry.pipeline_id.as_deref() == Some(id.as_str())
        });
        if let Some(entry) = existing {
            entry.plugin_path = failure.plugin_path.to_path_buf();
            entry.plugin_digest = digest;
            entry.exception = failure.exception.to_owned();
            entry.span_id = Some(failure.span_id.to_owned());
            entry.operation = Some(failure.operation.to_owned());
            entry.span_cursor = Some(failure.span_cursor);
            entry.pending_revisions =
                Some(failure.span_through.saturating_sub(failure.span_cursor));
            entry.last_seen_ms = now;
            entry.occurrences = entry.occurrences.saturating_add(1);
        } else {
            store.entries.push(PluginDiagnostic {
                source: failure.source.to_owned(),
                plugin_path: failure.plugin_path.to_path_buf(),
                plugin_digest: digest,
                exception: failure.exception.to_owned(),
                first_seen_ms: now,
                last_seen_ms: now,
                occurrences: 1,
                session_id: Some(failure.session_id.to_owned()),
                span_id: Some(failure.span_id.to_owned()),
                operation: Some(failure.operation.to_owned()),
                pipeline_id: Some(id),
                span_cursor: Some(failure.span_cursor),
                pending_revisions: Some(failure.span_through.saturating_sub(failure.span_cursor)),
            });
        }
        trim(&mut store);
        write_unlocked(&path, &store)
    })
}

fn trim(store: &mut DiagnosticStore) {
    store
        .entries
        .sort_by_key(|diagnostic| diagnostic.last_seen_ms);
    let excess = store.entries.len().saturating_sub(MAX_DIAGNOSTICS);
    store.entries.drain(..excess);
}

pub fn read(data_dir: &Path) -> anyhow::Result<Vec<PluginDiagnostic>> {
    let path = diagnostics_path(data_dir);
    ensure_diagnostics_dir(&path)?;
    crate::settings::with_settings_lock(&path, || Ok(read_unlocked(&path)?.entries))
}

pub fn merge(from_data_dir: &Path, into_data_dir: &Path) -> anyhow::Result<()> {
    let incoming = read(from_data_dir)?;
    if incoming.is_empty() {
        return Ok(());
    }
    let path = diagnostics_path(into_data_dir);
    ensure_diagnostics_dir(&path)?;
    crate::settings::with_settings_lock(&path, || {
        let mut store = read_unlocked(&path)?;
        for diagnostic in incoming {
            if let Some(existing) = store.entries.iter_mut().find(|entry| {
                entry.source == diagnostic.source
                    && entry.plugin_path == diagnostic.plugin_path
                    && entry.plugin_digest == diagnostic.plugin_digest
                    && entry.exception == diagnostic.exception
            }) {
                existing.first_seen_ms = existing.first_seen_ms.min(diagnostic.first_seen_ms);
                existing.last_seen_ms = existing.last_seen_ms.max(diagnostic.last_seen_ms);
                existing.occurrences = existing.occurrences.saturating_add(diagnostic.occurrences);
            } else {
                store.entries.push(diagnostic);
            }
        }
        trim(&mut store);
        write_unlocked(&path, &store)
    })
}

fn diagnostics_path(data_dir: &Path) -> PathBuf {
    data_dir.join("diagnostics").join(FILE_NAME)
}

fn ensure_diagnostics_dir(path: &Path) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("plugin diagnostics path has no parent"))?;
    crate::paths::ensure_private_dir(parent)?;
    Ok(())
}

fn read_unlocked(path: &Path) -> anyhow::Result<DiagnosticStore> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok(DiagnosticStore::default())
        }
        Err(error) => Err(error.into()),
    }
}

fn write_unlocked(path: &Path, store: &DiagnosticStore) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("plugin diagnostics path has no parent"))?;
    let mut encoded = serde_json::to_string_pretty(store)?;
    encoded.push('\n');
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(encoded.as_bytes())?;
    temporary.persist(path).map_err(|error| error.error)?;
    Ok(())
}

pub fn plugin_digest(path: &Path) -> Option<String> {
    let bytes = std::fs::read(path).ok()?;
    Some(format!("{:x}", Sha256::digest(bytes)))
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deduplicates_identical_raw_exceptions() {
        let temp = tempfile::tempdir().unwrap();
        let plugin = temp.path().join("redact.mjs");
        std::fs::write(&plugin, "export default span => span").unwrap();
        let exception = "Error: secret value\n    at redact (redact.mjs:1)";

        record(temp.path(), "codex", &plugin, exception).unwrap();
        record(temp.path(), "codex", &plugin, exception).unwrap();

        let diagnostics = read(temp.path()).unwrap();
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].exception, exception);
        assert_eq!(diagnostics[0].occurrences, 2);
    }
}
