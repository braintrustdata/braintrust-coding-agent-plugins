//! Per-session write-ahead journal. Every accepted event is appended
//! (auth-redacted) before the caller is acked, so a restarted daemon can
//! rebuild session state by replaying the journal through the translator.
//!
//! Format: one [`RedactedEnvelope`] JSON value per line in
//! `<data_dir>/journal/<session_id>.ndjson`.

use crate::wire::{BackendAuth, Envelope, RedactedEnvelope};
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;

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
    pub async fn append(&mut self, env: &Envelope) -> anyhow::Result<()> {
        let mut line = serde_json::to_vec(&env.redacted())?;
        line.push(b'\n');
        self.file.write_all(&line).await?;
        self.file.flush().await?;
        Ok(())
    }
}

/// Read a journal file back into redacted envelopes (for replay/rebuild).
pub async fn read_journal(path: &Path) -> anyhow::Result<Vec<RedactedEnvelope>> {
    let data = tokio::fs::read_to_string(path).await?;
    let mut out = Vec::new();
    for (i, line) in data.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let env: RedactedEnvelope = serde_json::from_str(line)
            .map_err(|e| anyhow::anyhow!("journal {}:{}: {e}", path.display(), i + 1))?;
        out.push(env);
    }
    Ok(out)
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
        session_id: r.session_id,
        event: r.event,
        ts_ms: r.ts_ms,
        payload: r.payload,
        route,
        config,
    }
}
