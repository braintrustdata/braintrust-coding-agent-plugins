//! Bounded, persistent diagnostics for span-plugin failures.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_DIAGNOSTICS: usize = 128;
const FILE_NAME: &str = "span-plugin-errors.json";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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
            });
        }
        store
            .entries
            .sort_by_key(|diagnostic| diagnostic.last_seen_ms);
        let excess = store.entries.len().saturating_sub(MAX_DIAGNOSTICS);
        if excess > 0 {
            store.entries.drain(..excess);
        }
        write_unlocked(&path, &store)
    })
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
        store
            .entries
            .sort_by_key(|diagnostic| diagnostic.last_seen_ms);
        let excess = store.entries.len().saturating_sub(MAX_DIAGNOSTICS);
        if excess > 0 {
            store.entries.drain(..excess);
        }
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

fn plugin_digest(path: &Path) -> Option<String> {
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
