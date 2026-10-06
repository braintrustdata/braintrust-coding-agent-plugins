//! Per-session write-ahead journal. Every accepted event is appended
//! (auth-redacted) before the caller is acked, so a restarted daemon can
//! rebuild session state by replaying the journal through the translator.
//!
//! Format: one [`RedactedEnvelope`] JSON value per line in
//! `<data_dir>/journal/<source>--<session>--<stable-id>.ndjson`, where an
//! overlong sanitized session id keeps only its tail to fit one file name.
//!
//! Managed-run acceptance records live alongside the journals so a flush can
//! still tell which delivery pipelines a managed child produced after the
//! daemon that accepted them has restarted or idle-exited.

use crate::wire::{BackendAuth, Envelope, RedactedEnvelope, SessionRoute};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

pub fn journal_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("journal")
}

fn checkpoint_path(journal_path: &Path) -> PathBuf {
    journal_path
        .parent()
        .and_then(Path::parent)
        .unwrap_or_else(|| Path::new("."))
        .join("journal-control")
        .join(journal_path.file_name().unwrap_or_default())
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

pub(crate) const MAX_FILE_NAME_BYTES: usize = 240;

pub fn journal_path(data_dir: &Path, session_id: &str) -> PathBuf {
    journal_dir(data_dir).join(format!("{}.ndjson", sanitize(session_id)))
}

/// Source-qualified journal path. The stable suffix prevents sanitized native
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
    // A legacy name too long for the filesystem was never written.
    if legacy.file_name().map_or(0, |name| name.len()) > MAX_FILE_NAME_BYTES {
        return Ok(path);
    }
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
        let recorded_source = crate::translate::canonical_source_name(&entry.source);
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
    path: PathBuf,
    position: u64,
}

impl JournalWriter {
    pub async fn open_path(path: &Path) -> anyhow::Result<Self> {
        if let Some(dir) = path.parent() {
            crate::paths::ensure_private_dir(dir).map_err(|error| {
                anyhow::anyhow!("create journal directory {}: {error}", dir.display())
            })?;
        }
        truncate_incomplete_tail(path)
            .map_err(|error| anyhow::anyhow!("prepare journal {}: {error}", path.display()))?;
        let mut options = tokio::fs::OpenOptions::new();
        options.create(true).append(true);
        #[cfg(unix)]
        options.mode(0o600);
        let file = options
            .open(path)
            .await
            .map_err(|error| anyhow::anyhow!("open journal {}: {error}", path.display()))?;
        crate::paths::restrict_file_to_owner(path)?;
        let position = file.metadata().await?.len();
        Ok(Self {
            file,
            path: path.to_path_buf(),
            position,
        })
    }

    pub(crate) fn position(&self) -> u64 {
        self.position
    }

    /// Append one event and durably commit it before acknowledging capture.
    ///
    /// The journal is a durability record and is never capped or truncated:
    /// dropping entries would silently cost recovery fidelity. Replay reads it
    /// as a stream so a large journal never becomes a large allocation.
    pub async fn append(&mut self, env: &Envelope) -> anyhow::Result<u64> {
        let redacted = env.redacted();
        // Native observations are the WAL's source of truth. In particular,
        // Pi context snapshots must stay intact even when a previous snapshot
        // contains the same prefix: a record should stand on its own.
        let mut value = serde_json::to_value(&redacted)?;
        let payload = serde_json::to_vec(&value)?;
        let object = value
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("journal envelope is not an object"))?;
        object.insert("_bt_wal_version".into(), serde_json::json!(1));
        object.insert("_bt_wal_offset".into(), serde_json::json!(self.position));
        object.insert("_bt_wal_length".into(), serde_json::json!(payload.len()));
        object.insert(
            "_bt_wal_sha256".into(),
            serde_json::json!(format!("{:x}", Sha256::digest(&payload))),
        );
        let mut line = serde_json::to_vec(&value)?;
        line.push(b'\n');
        self.file
            .write_all(&line)
            .await
            .map_err(|error| anyhow::anyhow!("write journal {}: {error}", self.path.display()))?;
        self.file
            .flush()
            .await
            .map_err(|error| anyhow::anyhow!("flush journal {}: {error}", self.path.display()))?;
        self.file
            .sync_data()
            .await
            .map_err(|error| anyhow::anyhow!("sync journal {}: {error}", self.path.display()))?;
        self.position += line.len() as u64;
        Ok(self.position)
    }

    /// Record route delivery progress in a separate control stream. Native
    /// event records remain a faithful WAL independent of sink activity.
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
        let path = checkpoint_path(&self.path);
        crate::paths::ensure_private_dir(path.parent().expect("checkpoint path has parent"))?;
        let mut options = tokio::fs::OpenOptions::new();
        options.create(true).append(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = options.open(&path).await?;
        crate::paths::restrict_file_to_owner(&path)?;
        file.write_all(&line).await?;
        file.flush().await?;
        file.sync_data().await?;
        Ok(())
    }
}

/// Keep the last complete record after a crash so the next append starts at
/// a fresh frame boundary. A legacy newline journal has the same framing.
fn truncate_incomplete_tail(path: &Path) -> anyhow::Result<()> {
    let mut file = match std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let mut end = file.metadata()?.len();
    if end == 0 {
        return Ok(());
    }
    let mut chunk = [0u8; 4096];
    while end > 0 {
        let start = end.saturating_sub(chunk.len() as u64);
        let count = (end - start) as usize;
        file.seek(SeekFrom::Start(start))?;
        file.read_exact(&mut chunk[..count])?;
        if let Some(index) = chunk[..count].iter().rposition(|byte| *byte == b'\n') {
            let complete = start + index as u64 + 1;
            if complete < file.metadata()?.len() {
                file.set_len(complete)?;
                file.sync_data()?;
            }
            return Ok(());
        }
        end = start;
    }
    file.set_len(0)?;
    file.sync_data()?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeliveryCheckpointRecord {
    #[serde(rename = "_bt_record_type")]
    record_type: String,
    route: SessionRoute,
    through: u64,
}

#[derive(Debug)]
// Keep event records inline on the journal read path; the added capture
// completeness bit makes this enum just exceed Clippy's size threshold.
#[allow(clippy::large_enum_variant)]
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
    pi_context: Vec<serde_json::Value>,
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
        Self::open_from(path, 0, through).await
    }

    pub async fn open_from(path: &Path, start: u64, through: u64) -> anyhow::Result<Option<Self>> {
        let mut file = match tokio::fs::File::open(path).await {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        file.seek(std::io::SeekFrom::Start(start)).await?;
        Ok(Some(Self {
            reader: tokio::io::BufReader::new(file.take(through.saturating_sub(start))),
            path: path.to_path_buf(),
            line_no: 0,
            position: start,
            pi_context: Vec::new(),
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
            // An interrupted append can leave an unterminated final record.
            // Earlier complete records remain valid and replayable.
            if bytes.last() != Some(&b'\n') {
                return Ok(None);
            }
            self.line_no += 1;
            let line = std::str::from_utf8(&bytes)
                .map_err(|error| {
                    anyhow::anyhow!("journal {}:{}: {error}", self.path.display(), self.line_no)
                })?
                .trim();
            if line.is_empty() {
                continue;
            }
            let mut value: serde_json::Value = serde_json::from_str(line).map_err(|error| {
                anyhow::anyhow!("journal {}:{}: {error}", self.path.display(), self.line_no)
            })?;
            if let Some(object) = value.as_object_mut() {
                if let Some(version) = object.remove("_bt_wal_version") {
                    anyhow::ensure!(version == 1, "unsupported journal record version {version}");
                    let offset = object.remove("_bt_wal_offset").and_then(|v| v.as_u64());
                    let length = object.remove("_bt_wal_length").and_then(|v| v.as_u64());
                    let checksum = object
                        .remove("_bt_wal_sha256")
                        .and_then(|v| v.as_str().map(str::to_owned));
                    let payload = serde_json::to_vec(object)?;
                    anyhow::ensure!(
                        offset == Some(self.position - read as u64)
                            && length == Some(payload.len() as u64)
                            && checksum.as_deref()
                                == Some(&format!("{:x}", Sha256::digest(&payload))),
                        "journal {}:{}: record checksum or position mismatch",
                        self.path.display(),
                        self.line_no,
                    );
                }
            }
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
                let mut event = serde_json::from_value(value).map_err(|error| {
                    anyhow::anyhow!("journal {}:{}: {error}", self.path.display(), self.line_no)
                })?;
                expand_pi_payload(&mut event, &mut self.pi_context).map_err(|error| {
                    anyhow::anyhow!("journal {}:{}: {error}", self.path.display(), self.line_no)
                })?;
                JournalRecord::Event(event)
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
        for (candidate, offset) in read_checkpoint_state(path).await {
            if candidate.same_route(route) {
                acknowledged = acknowledged.max(offset);
            }
        }
        acknowledged
    }
}

/// Control records live beside, rather than inside, the native event WAL.
/// Legacy inline checkpoints remain readable through JournalReader.
pub async fn read_checkpoint_state(path: &Path) -> Vec<(SessionRoute, u64)> {
    let data = match tokio::fs::read(checkpoint_path(path)).await {
        Ok(data) => data,
        Err(_) => return Vec::new(),
    };
    data.split(|byte| *byte == b'\n')
        .filter_map(|line| serde_json::from_slice::<DeliveryCheckpointRecord>(line).ok())
        .map(|record| (record.route, record.through))
        .collect()
}

const PI_MESSAGES_DELTA: &str = "_bt_messages_delta";

// Older journals used Pi context deltas. Keep decoding those records while
// all new captures retain the complete native observation.
fn expand_pi_payload(
    event: &mut RedactedEnvelope,
    previous: &mut Vec<serde_json::Value>,
) -> anyhow::Result<()> {
    if event.source != "pi" || event.event != "context" {
        return Ok(());
    }
    let Some(native) = event
        .payload
        .get_mut("event")
        .and_then(serde_json::Value::as_object_mut)
    else {
        return Ok(());
    };
    let Some(delta) = native.remove(PI_MESSAGES_DELTA) else {
        if let Some(messages) = native.get("messages").and_then(serde_json::Value::as_array) {
            *previous = messages.clone();
        }
        return Ok(());
    };
    let common_prefix = delta
        .get("common_prefix")
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| anyhow::anyhow!("invalid Pi context common prefix"))?;
    anyhow::ensure!(
        common_prefix <= previous.len(),
        "Pi context common prefix exceeds prior context"
    );
    let suffix = delta
        .get("suffix")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("invalid Pi context suffix"))?;
    previous.truncate(common_prefix);
    previous.extend(suffix.iter().cloned());
    native.insert(
        "messages".into(),
        serde_json::Value::Array(previous.clone()),
    );
    Ok(())
}

/// Native events remain the complete historical record. Collection is inert
/// until a lossless archive policy is implemented.
pub async fn gc_old_journals(data_dir: &Path, max_age: std::time::Duration) {
    // Accepted native events are the historical WAL. Retain them for the
    // lifetime of the data directory. This scheduled maintenance hook is
    // intentionally inert until a lossless archive policy is implemented.
    let _ = (data_dir, max_age);
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
    async fn accepted_wal_is_retained_as_the_historical_event_record() {
        let temp = tempfile::tempdir().unwrap();
        let path = source_journal_path(temp.path(), "claude", "session");
        tokio::fs::create_dir_all(path.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&path, b"event\n").await.unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(std::time::SystemTime::now() - std::time::Duration::from_secs(86_400))
            .unwrap();
        gc_old_journals(temp.path(), std::time::Duration::from_secs(1)).await;
        assert!(path.exists());
    }

    #[tokio::test]
    async fn wal_is_retained_after_an_incident_is_resolved() {
        let temp = tempfile::tempdir().unwrap();
        let path = source_journal_path(temp.path(), "pi", "held-session");
        tokio::fs::create_dir_all(path.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&path, b"event\n").await.unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(std::time::SystemTime::now() - std::time::Duration::from_secs(86_400))
            .unwrap();
        let scope = crate::recovery::WorkScope::SourceSession {
            source: "pi".into(),
            session_id: "held-session".into(),
        };
        crate::recovery::pause(
            temp.path(),
            scope.clone(),
            0,
            0,
            crate::recovery::FailureCause::InputShape {
                event: "tool_execution_start".into(),
                translator_revision: "old".into(),
            },
            "missing toolCallId".into(),
            None,
        )
        .unwrap();
        gc_old_journals(temp.path(), std::time::Duration::from_secs(1)).await;
        assert!(path.exists());
        crate::recovery::resolve(temp.path(), &scope).unwrap();
        gc_old_journals(temp.path(), std::time::Duration::from_secs(1)).await;
        assert!(path.exists());
    }

    fn pi_context(messages: Vec<serde_json::Value>, ts_ms: i64) -> Envelope {
        Envelope {
            source: "pi".into(),
            source_version: None,
            plugin_version: None,
            session_id: "pi-session".into(),
            event: "context".into(),
            ts_ms,
            managed_run_id: None,
            capture: None,
            payload: serde_json::json!({"event":{"type":"context","messages":messages}}),
            route: None,
            config: None,
        }
    }

    #[tokio::test]
    async fn pi_context_history_is_preserved_and_replayed() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("pi.ndjson");
        let mut writer = JournalWriter::open_path(&path).await.unwrap();
        let mut messages = Vec::new();
        let mut naive_bytes = 0usize;
        for index in 0..20 {
            messages.push(serde_json::json!({
                "role":"user",
                "content": format!("{index}:{}", "x".repeat(4096)),
            }));
            let event = pi_context(messages.clone(), index);
            naive_bytes += serde_json::to_vec(&event.redacted()).unwrap().len() + 1;
            writer.append(&event).await.unwrap();
        }
        let compacted = vec![
            serde_json::json!({"role":"compactionSummary","summary":"bounded"}),
            serde_json::json!({"role":"user","content":"after"}),
        ];
        let event = pi_context(compacted.clone(), 20);
        naive_bytes += serde_json::to_vec(&event.redacted()).unwrap().len() + 1;
        writer.append(&event).await.unwrap();
        drop(writer);

        let stored = tokio::fs::read(&path).await.unwrap();
        assert!(stored.len() >= naive_bytes);
        assert!(String::from_utf8_lossy(&stored).contains("\"messages\""));

        let through = stored.len() as u64;
        let mut reader = JournalReader::open(&path, through).await.unwrap().unwrap();
        let mut contexts = Vec::new();
        while let Some(entry) = reader.next_entry().await.unwrap() {
            contexts.push(
                entry
                    .payload
                    .pointer("/event/messages")
                    .and_then(serde_json::Value::as_array)
                    .cloned()
                    .unwrap(),
            );
        }
        assert_eq!(contexts.len(), 21);
        assert_eq!(contexts[19].len(), 20);
        assert_eq!(contexts[20], compacted);
    }

    #[tokio::test]
    async fn wal_recovers_torn_tail_and_detects_corruption() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("events.ndjson");
        let mut writer = JournalWriter::open_path(&path).await.unwrap();
        writer.append(&pi_context(vec![], 1)).await.unwrap();
        let first = writer.position();
        drop(writer);
        // Tokio's Windows file wrapper closes through its blocking worker.
        // Give that close a turn before simulating a process restart with a
        // second handle to the same file.
        tokio::task::yield_now().await;
        let mut file = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .await
            .unwrap();
        file.write_all(b"{\"partial\":true").await.unwrap();
        file.sync_all().await.unwrap();
        drop(file);
        tokio::task::yield_now().await;
        let mut writer = JournalWriter::open_path(&path).await.unwrap();
        assert_eq!(writer.position(), first);
        writer.append(&pi_context(vec![], 2)).await.unwrap();
        drop(writer);
        let mut reader = JournalReader::open(&path, u64::MAX).await.unwrap().unwrap();
        assert_eq!(reader.next_entry().await.unwrap().unwrap().ts_ms, 1);
        assert_eq!(reader.next_entry().await.unwrap().unwrap().ts_ms, 2);
        assert!(reader.next_entry().await.unwrap().is_none());

        let mut bytes = tokio::fs::read(&path).await.unwrap();
        let changed = bytes.iter().position(|byte| *byte == b'1').unwrap();
        bytes[changed] = b'9';
        tokio::fs::write(&path, bytes).await.unwrap();
        let mut reader = JournalReader::open(&path, u64::MAX).await.unwrap().unwrap();
        assert!(reader.next_entry().await.is_err());
    }

    #[tokio::test]
    async fn pi_streaming_updates_replay_complete_native_payload() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("pi.ndjson");
        let mut writer = JournalWriter::open_path(&path).await.unwrap();
        let marker = "growing-partial-marker".repeat(4096);
        let event = Envelope {
            source: "pi".into(),
            source_version: None,
            plugin_version: None,
            session_id: "pi-session".into(),
            event: "message_update".into(),
            ts_ms: 1,
            managed_run_id: None,
            capture: None,
            payload: serde_json::json!({
                "event": {
                    "type": "message_update",
                    "assistantMessageEvent": {
                        "type": "text_delta",
                        "partial": marker,
                    },
                    "message": {"role": "assistant", "content": marker},
                }
            }),
            route: None,
            config: None,
        };
        writer.append(&event).await.unwrap();
        drop(writer);

        let stored = tokio::fs::read(&path).await.unwrap();
        assert!(String::from_utf8_lossy(&stored).contains("growing-partial-marker"));

        let mut reader = JournalReader::open(&path, stored.len() as u64)
            .await
            .unwrap()
            .unwrap();
        let replayed = reader.next_entry().await.unwrap().unwrap();
        assert_eq!(replayed.payload, event.payload);
    }

    #[tokio::test]
    async fn pi_provider_requests_replay_complete_native_payload() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("pi.ndjson");
        let mut writer = JournalWriter::open_path(&path).await.unwrap();
        let event = Envelope {
            source: "pi".into(),
            source_version: None,
            plugin_version: None,
            session_id: "pi-session".into(),
            event: "before_provider_request".into(),
            ts_ms: 1,
            managed_run_id: None,
            capture: None,
            payload: serde_json::json!({
                "event": {
                    "type": "before_provider_request",
                    "payload": {
                        "system": "system-prompt-marker",
                        "messages": [{"role": "user", "content": "message-marker"}],
                        "tools": [{"name": "read", "input_schema": {"type": "object"}}],
                        "max_tokens": 1024,
                    },
                }
            }),
            route: None,
            config: None,
        };
        writer.append(&event).await.unwrap();
        drop(writer);

        let stored = tokio::fs::read(&path).await.unwrap();
        let stored_text = String::from_utf8_lossy(&stored);
        assert!(stored_text.contains("system-prompt-marker"));
        assert!(stored_text.contains("message-marker"));

        let mut reader = JournalReader::open(&path, stored.len() as u64)
            .await
            .unwrap()
            .unwrap();
        let replayed = reader.next_entry().await.unwrap().unwrap();
        assert_eq!(replayed.payload, event.payload);
    }

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
    async fn long_session_ids_get_distinct_bounded_journals() {
        let temp = tempfile::tempdir().unwrap();
        let shared = "x".repeat(400);
        let first = ensure_source_journal(temp.path(), "pi", &format!("a{shared}"))
            .await
            .unwrap();
        let second = ensure_source_journal(temp.path(), "pi", &format!("b{shared}"))
            .await
            .unwrap();
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
