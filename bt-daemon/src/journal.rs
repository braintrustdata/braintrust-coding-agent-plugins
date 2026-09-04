//! Per-session write-ahead journal. Every accepted event is appended
//! (auth-redacted) before the caller is acked, so a restarted daemon can
//! rebuild session state by replaying the journal through the translator.
//!
//! Format: one [`RedactedEnvelope`] JSON value per line in
//! `<data_dir>/journal/<source>-<session>-<stable-id>.ndjson`.
//!
//! Managed-run acceptance records live alongside the journals so a flush can
//! still tell which delivery pipelines a managed child produced after the
//! daemon that accepted them has restarted or idle-exited.

use crate::wire::{BackendAuth, Envelope, RedactedEnvelope, SessionRoute};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};

pub fn journal_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("journal")
}

fn sanitize(s: &str) -> String {
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

pub fn journal_path(data_dir: &Path, session_id: &str) -> PathBuf {
    journal_dir(data_dir).join(format!("{}.ndjson", sanitize(session_id)))
}

/// Source-qualified journal path. The stable suffix prevents sanitized native
/// ids such as `a/b` and `a_b` from aliasing the same file.
pub fn source_journal_path(data_dir: &Path, source: &str, session_id: &str) -> PathBuf {
    journal_dir(data_dir).join(format!(
        "{}--{}--{}.ndjson",
        sanitize(source),
        sanitize(session_id),
        crate::ids::session_storage_id(source, session_id)
    ))
}

/// Return the source-qualified journal, copying the legacy session-only file
/// on first use so an upgrade retains replay history. The legacy file remains
/// untouched for rollback and is no longer appended after migration.
pub async fn ensure_source_journal(
    data_dir: &Path,
    source: &str,
    session_id: &str,
) -> anyhow::Result<PathBuf> {
    let path = source_journal_path(data_dir, source, session_id);
    if tokio::fs::metadata(&path).await.is_ok() {
        return Ok(path);
    }
    let legacy = journal_path(data_dir, session_id);
    match tokio::fs::metadata(&legacy).await {
        Ok(_) => {
            tokio::fs::create_dir_all(journal_dir(data_dir)).await?;
            tokio::fs::copy(&legacy, &path).await?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(path)
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
/// session journal contains more than one destination.
pub async fn legacy_journal_source(
    data_dir: &Path,
    session_id: &str,
    route: &SessionRoute,
) -> Option<String> {
    let path = journal_path(data_dir, session_id);
    let through = JournalReader::recorded_len(&path).await;
    let mut reader = JournalReader::open(&path, through).await.ok().flatten()?;
    while let Ok(Some(entry)) = reader.next_entry().await {
        if entry
            .route
            .as_ref()
            .is_some_and(|candidate| candidate.same_route(route))
        {
            return Some(entry.source);
        }
    }
    None
}

/// Whether this source owned the session actor recorded by a pre-qualification
/// daemon. Only the first source recorded for a native session keeps the old
/// span-id namespace: the legacy daemon had one actor and one namespace per
/// native session even if its journal later received mixed-source events.
pub async fn legacy_journal_has_session(
    data_dir: &Path,
    canonical_source: &str,
    session_id: &str,
) -> bool {
    let path = journal_path(data_dir, session_id);
    let through = JournalReader::recorded_len(&path).await;
    let Ok(Some(mut reader)) = JournalReader::open(&path, through).await else {
        return false;
    };
    while let Ok(Some(entry)) = reader.next_entry().await {
        let recorded_source = match entry.source.as_str() {
            "claude" => "claude-code",
            "open-code" => "opencode",
            source => source,
        };
        if entry.session_id == session_id {
            return recorded_source == canonical_source;
        }
    }
    false
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

/// Append-only journal writer for one session.
pub struct JournalWriter {
    file: tokio::fs::File,
    position: u64,
}

impl JournalWriter {
    pub async fn open_path(path: &Path) -> anyhow::Result<Self> {
        if let Some(dir) = path.parent() {
            tokio::fs::create_dir_all(dir).await?;
        }
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;
        let position = file.metadata().await?.len();
        Ok(Self { file, position })
    }

    pub(crate) fn position(&self) -> u64 {
        self.position
    }

    /// Append one event in redacted form and flush to the OS. Not fsync'd per
    /// event (that would dominate hook latency); an OS crash can lose the last
    /// few lines, which replay tolerates.
    ///
    /// The journal is a durability record and is never capped or truncated:
    /// dropping entries would silently cost recovery fidelity. Its size is
    /// bounded instead by writing each transcript byte once (see
    /// [`crate::transcript_mirror`]) and by age-based GC, and replay reads it
    /// as a stream so a large journal never becomes a large allocation.
    pub async fn append(&mut self, env: &Envelope) -> anyhow::Result<u64> {
        let mut line = serde_json::to_vec(&env.redacted())?;
        line.push(b'\n');
        self.file.write_all(&line).await?;
        self.file.flush().await?;
        self.position += line.len() as u64;
        Ok(self.position)
    }

    /// Record that the sink durably accepted every event through this byte
    /// offset for one route. It lives in the event journal so a cold worker
    /// restores delivery knowledge together with translator state.
    pub async fn append_delivery_checkpoint(
        &mut self,
        route: &SessionRoute,
        through: u64,
    ) -> anyhow::Result<()> {
        let mut line = serde_json::to_vec(&DeliveryCheckpointRecord {
            record_type: "delivery_checkpoint".into(),
            route: route.clone(),
            through,
        })?;
        line.push(b'\n');
        self.file.write_all(&line).await?;
        self.file.flush().await?;
        self.position += line.len() as u64;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeliveryCheckpointRecord {
    #[serde(rename = "_bt_record_type")]
    record_type: String,
    route: SessionRoute,
    through: u64,
}

#[derive(Debug)]
pub enum JournalRecord {
    Event(RedactedEnvelope),
    DeliveryCheckpoint { route: SessionRoute, through: u64 },
}

#[derive(Debug)]
pub struct JournalRecordEntry {
    pub record: JournalRecord,
    /// Exclusive byte offset after this record in the journal.
    pub through: u64,
}

/// Streaming reader over one session's journal.
///
/// Replay must never materialize a whole journal: the file is read line by
/// line so peak memory is one entry, not the entire recorded session.
pub struct JournalReader {
    reader: tokio::io::BufReader<tokio::io::Take<tokio::fs::File>>,
    path: PathBuf,
    line_no: usize,
    position: u64,
}

impl JournalReader {
    /// Open a journal for streaming, reading at most `through` bytes.
    ///
    /// That bound is what keeps replay from consuming the very event that
    /// triggered the session's creation: the caller records the journal's
    /// length before appending, so the actor replays strictly what was
    /// already recovered state, never the live event still on its way to the
    /// queue. `Ok(None)` means the session has no journal yet.
    pub async fn open(path: &Path, through: u64) -> anyhow::Result<Option<Self>> {
        let file = match tokio::fs::File::open(path).await {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        Ok(Some(Self {
            reader: tokio::io::BufReader::new(file.take(through)),
            path: path.to_path_buf(),
            line_no: 0,
            position: 0,
        }))
    }

    /// The journal's current length, which is the bound a session created now
    /// should replay through. A missing journal replays nothing.
    pub async fn recorded_len(path: &Path) -> u64 {
        tokio::fs::metadata(path)
            .await
            .map(|meta| meta.len())
            .unwrap_or(0)
    }

    /// The next entry, or `None` at end of file.
    pub async fn next_entry(&mut self) -> anyhow::Result<Option<RedactedEnvelope>> {
        while let Some(entry) = self.next_record().await? {
            if let JournalRecord::Event(env) = entry.record {
                return Ok(Some(env));
            }
        }
        Ok(None)
    }

    /// The next journal record, including delivery checkpoints written by a
    /// previous daemon generation.
    pub async fn next_record(&mut self) -> anyhow::Result<Option<JournalRecordEntry>> {
        loop {
            let mut bytes = Vec::new();
            let read = self.reader.read_until(b'\n', &mut bytes).await?;
            if read == 0 {
                return Ok(None);
            }
            self.position += read as u64;
            self.line_no += 1;
            let line = std::str::from_utf8(&bytes)
                .map_err(|error| {
                    anyhow::anyhow!("journal {}:{}: {error}", self.path.display(), self.line_no)
                })?
                .trim();
            if line.is_empty() {
                continue;
            }
            let value: serde_json::Value = serde_json::from_str(line).map_err(|error| {
                anyhow::anyhow!("journal {}:{}: {error}", self.path.display(), self.line_no)
            })?;
            let record = if value
                .get("_bt_record_type")
                .and_then(serde_json::Value::as_str)
                == Some("delivery_checkpoint")
            {
                let checkpoint: DeliveryCheckpointRecord =
                    serde_json::from_value(value).map_err(|error| {
                        anyhow::anyhow!("journal {}:{}: {error}", self.path.display(), self.line_no)
                    })?;
                JournalRecord::DeliveryCheckpoint {
                    route: checkpoint.route,
                    through: checkpoint.through,
                }
            } else {
                JournalRecord::Event(serde_json::from_value(value).map_err(|error| {
                    anyhow::anyhow!("journal {}:{}: {error}", self.path.display(), self.line_no)
                })?)
            };
            return Ok(Some(JournalRecordEntry {
                record,
                through: self.position,
            }));
        }
    }

    /// The latest acknowledged event offset for `route` in the portion of the
    /// journal being recovered. Older journals contain no checkpoints and
    /// therefore retain their existing replay behavior.
    pub async fn acknowledged_through(path: &Path, through: u64, route: &SessionRoute) -> u64 {
        let Ok(Some(mut reader)) = Self::open(path, through).await else {
            return 0;
        };
        let mut acknowledged = 0;
        while let Ok(Some(entry)) = reader.next_record().await {
            if let JournalRecord::DeliveryCheckpoint {
                route: candidate,
                through,
            } = entry.record
            {
                if candidate.same_route(route) {
                    acknowledged = acknowledged.max(through);
                }
            }
        }
        acknowledged
    }
}

/// Best-effort age-based journal collection. A failed stat/remove is logged
/// and ignored; stale state must never prevent the daemon from serving hooks.
pub async fn gc_old_journals(data_dir: &Path, max_age: std::time::Duration) {
    let dir = journal_dir(data_dir);
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
            if let Err(e) = tokio::fs::remove_file(&path).await {
                tracing::warn!(path = %path.display(), "failed to remove stale journal: {e}");
            }
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

    #[tokio::test]
    async fn source_journals_are_distinct_and_migrate_legacy_history() {
        let temp = tempfile::tempdir().unwrap();
        let legacy = journal_path(temp.path(), "same/session");
        tokio::fs::create_dir_all(legacy.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&legacy, b"legacy\n").await.unwrap();

        let codex = ensure_source_journal(temp.path(), "codex", "same/session")
            .await
            .unwrap();
        let claude = ensure_source_journal(temp.path(), "claude-code", "same/session")
            .await
            .unwrap();
        assert_ne!(codex, claude);
        assert_eq!(tokio::fs::read(&codex).await.unwrap(), b"legacy\n");
        assert_eq!(tokio::fs::read(&claude).await.unwrap(), b"legacy\n");
        assert_eq!(tokio::fs::read(&legacy).await.unwrap(), b"legacy\n");
    }

    #[tokio::test]
    async fn legacy_managed_run_records_recover_source_from_the_old_journal() {
        let temp = tempfile::tempdir().unwrap();
        let route = SessionRoute::default();
        let mut writer = JournalWriter::open_path(&journal_path(temp.path(), "legacy"))
            .await
            .unwrap();
        writer
            .append(&Envelope {
                source: "codex".into(),
                source_version: None,
                plugin_version: None,
                session_id: "legacy".into(),
                event: "SessionStart".into(),
                ts_ms: 1,
                managed_run_id: None,
                payload: serde_json::json!({}),
                route: Some(route.clone()),
                config: None,
                capture: None,
            })
            .await
            .unwrap();
        writer
            .append(&Envelope {
                source: "claude".into(),
                source_version: None,
                plugin_version: None,
                session_id: "legacy".into(),
                event: "SessionStart".into(),
                ts_ms: 2,
                managed_run_id: None,
                payload: serde_json::json!({}),
                route: Some(route.clone()),
                config: None,
                capture: None,
            })
            .await
            .unwrap();
        assert_eq!(
            legacy_journal_source(temp.path(), "legacy", &route).await,
            Some("codex".into())
        );
        assert!(legacy_journal_has_session(temp.path(), "codex", "legacy").await);
        assert!(!legacy_journal_has_session(temp.path(), "claude-code", "legacy").await);
    }
}
