//! Per-session write-ahead journal. Every accepted event is appended
//! (auth-redacted) before the caller is acked, so a restarted daemon can
//! rebuild session state by replaying the journal through the translator.
//!
//! Format: one [`RedactedEnvelope`] JSON value per line in
//! `<data_dir>/journal/<session_id>.ndjson`.
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

pub fn managed_run_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("managed-runs")
}

pub fn managed_run_path(data_dir: &Path, managed_run_id: &str) -> PathBuf {
    managed_run_dir(data_dir).join(format!("{}.ndjson", sanitize(managed_run_id)))
}

/// One delivery pipeline accepted from a managed child process tree.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManagedRunKey {
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
}

impl JournalWriter {
    pub async fn open(data_dir: &Path, session_id: &str) -> anyhow::Result<Self> {
        let dir = journal_dir(data_dir);
        tokio::fs::create_dir_all(&dir).await?;
        let path = journal_path(data_dir, session_id);
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;
        Ok(Self { file })
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
    pub async fn append(&mut self, env: &Envelope) -> anyhow::Result<()> {
        let mut line = serde_json::to_vec(&env.redacted())?;
        line.push(b'\n');
        self.file.write_all(&line).await?;
        self.file.flush().await?;
        Ok(())
    }
}

/// Streaming reader over one session's journal.
///
/// Replay must never materialize a whole journal: the file is read line by
/// line so peak memory is one entry, not the entire recorded session.
pub struct JournalReader {
    lines: tokio::io::Lines<tokio::io::BufReader<tokio::io::Take<tokio::fs::File>>>,
    path: PathBuf,
    line_no: usize,
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
            lines: tokio::io::BufReader::new(file.take(through)).lines(),
            path: path.to_path_buf(),
            line_no: 0,
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
        while let Some(line) = self.lines.next_line().await? {
            self.line_no += 1;
            if line.trim().is_empty() {
                continue;
            }
            let env: RedactedEnvelope = serde_json::from_str(&line).map_err(|error| {
                anyhow::anyhow!("journal {}:{}: {error}", self.path.display(), self.line_no)
            })?;
            return Ok(Some(env));
        }
        Ok(None)
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
        payload: r.payload,
        plugin_env: std::collections::BTreeMap::new(),
        route,
        config,
    }
}
