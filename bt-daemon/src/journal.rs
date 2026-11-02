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

mod codec;
use crate::pipeline::input::SourcePosition;
pub use crate::pipeline::input::{ControlKind, PipelineControl};
pub(crate) use codec::DecoderContext;

use crate::wire::{BackendAuth, Envelope, RedactedEnvelope, SessionRoute};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

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
    position: u64,
    payloads: codec::PayloadCache,
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
        Ok(Self {
            file,
            position,
            payloads: codec::PayloadCache::default(),
        })
    }

    pub(crate) fn position(&self) -> u64 {
        self.position
    }

    /// Append one redacted event and confirm its durable write before capture acknowledges it.
    ///
    /// The journal is a durability record and is never capped or truncated:
    /// dropping entries would silently cost recovery fidelity. Its size is
    /// managed by age-based GC. Native event payloads are preserved, including
    /// repeated message snapshots. External transcript mirrors capture each
    /// byte once, and replay streams records without loading the whole log.
    #[cfg(test)]
    pub async fn append(&mut self, env: &Envelope) -> anyhow::Result<u64> {
        self.append_ordered(env, None)
            .await
            .map(|position| position.through)
    }

    pub(crate) async fn append_ordered(
        &mut self,
        env: &Envelope,
        sequence: Option<u64>,
    ) -> anyhow::Result<SourcePosition> {
        #[derive(Serialize)]
        struct Captured<'a> {
            #[serde(flatten)]
            envelope: &'a RedactedEnvelope,
            _bt_record_type: &'static str,
            #[serde(skip_serializing_if = "Option::is_none")]
            _bt_capture_sequence: Option<u64>,
        }
        let mut redacted = env.redacted();
        let (json, delta) = self.payloads.encode(&env.event, &redacted.payload)?;
        let record_type = if let Some(delta) = delta {
            redacted.payload = serde_json::to_value(delta)?;
            codec::DELTA_RECORD
        } else {
            "event"
        };
        let mut line = serde_json::to_vec(&Captured {
            envelope: &redacted,
            _bt_record_type: record_type,
            _bt_capture_sequence: sequence,
        })?;
        line.push(b'\n');
        self.file.write_all(&line).await?;
        self.file.flush().await?;
        self.file.sync_data().await?;
        let before = self.position;
        self.position += line.len() as u64;
        self.payloads
            .record(&env.event, self.position, json.clone());
        Ok(SourcePosition {
            before,
            through: self.position,
            decoded_bytes: (line.len() as u64)
                .max(json.as_ref().map_or(0, |json| json.len() as u64)),
            decoder_base: json,
        })
    }

    #[cfg(test)]
    pub(crate) async fn append_control(
        &mut self,
        control: &PipelineControl,
    ) -> anyhow::Result<u64> {
        self.append_control_record(control)
            .await
            .map(|position| position.through)
    }

    pub(crate) async fn append_control_record(
        &mut self,
        control: &PipelineControl,
    ) -> anyhow::Result<SourcePosition> {
        let mut value = serde_json::to_value(control)?;
        value["_bt_record_type"] = "pipeline_control".into();
        let mut line = serde_json::to_vec(&value)?;
        line.push(b'\n');
        self.file.write_all(&line).await?;
        self.file.flush().await?;
        self.file.sync_data().await?;
        let before = self.position;
        self.position += line.len() as u64;
        Ok(SourcePosition {
            before,
            through: self.position,
            decoded_bytes: line.len() as u64,
            decoder_base: None,
        })
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
// Keep event records inline on the journal read path; the added capture
// completeness bit makes this enum just exceed Clippy's size threshold.
#[allow(clippy::large_enum_variant)]
pub enum JournalRecord {
    Event(RedactedEnvelope),
    Control(PipelineControl),
    DeliveryCheckpoint { route: SessionRoute, through: u64 },
}

#[derive(Debug)]
pub struct JournalRecordEntry {
    pub record: JournalRecord,
    /// Exclusive byte offset after this record in the journal.
    pub through: u64,
    /// Decoded work, which can be much larger than a compressed record.
    pub(crate) decoded_bytes: u64,
    /// Daemon-local capture order. Kept outside the native event payload.
    pub(crate) capture_sequence: Option<u64>,
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
    decoder: DecoderContext,
}

impl JournalReader {
    pub(crate) fn decoder_context(&self) -> &DecoderContext {
        &self.decoder
    }

    /// Resume any source journal at a committed byte offset with its saved
    /// lossless payload cache (and, for old Pi logs, their legacy message base).
    pub(crate) async fn resume(
        path: &Path,
        position: u64,
        through: u64,
        decoder_context: DecoderContext,
    ) -> anyhow::Result<Self> {
        let mut file = tokio::fs::File::open(path).await?;
        anyhow::ensure!(
            position <= through && through <= file.metadata().await?.len(),
            "checkpoint is beyond the source journal"
        );
        if position > 0 {
            file.seek(std::io::SeekFrom::Start(position - 1)).await?;
            anyhow::ensure!(
                file.read_u8().await? == b'\n',
                "checkpoint is not at a journal record boundary"
            );
        }
        file.seek(std::io::SeekFrom::Start(position)).await?;
        Ok(Self {
            reader: tokio::io::BufReader::new(file.take(through - position)),
            path: path.to_owned(),
            line_no: 0,
            position,
            decoder: decoder_context,
        })
    }
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
            decoder: DecoderContext::default(),
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
            let capture_sequence = value
                .get("_bt_capture_sequence")
                .and_then(serde_json::Value::as_u64);
            let mut decoded_bytes = read as u64;
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
            } else if value
                .get("_bt_record_type")
                .and_then(serde_json::Value::as_str)
                == Some("pipeline_control")
            {
                JournalRecord::Control(serde_json::from_value(value)?)
            } else {
                let record_type = value
                    .get("_bt_record_type")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned);
                let mut event: RedactedEnvelope =
                    serde_json::from_value(value).map_err(|error| {
                        anyhow::anyhow!("journal {}:{}: {error}", self.path.display(), self.line_no)
                    })?;
                match record_type.as_deref() {
                    Some("event") | Some(codec::DELTA_RECORD) => {
                        self.decoder.legacy_pi.clear();
                        let json = if record_type.as_deref() == Some(codec::DELTA_RECORD) {
                            let (payload, json) =
                                self.decoder.payloads.decode(&event.event, event.payload)?;
                            decoded_bytes = decoded_bytes.max(json.len() as u64);
                            event.payload = payload;
                            Some(json)
                        } else {
                            codec::cache_json(&event.event, &event.payload)?
                        };
                        self.decoder
                            .payloads
                            .record(&event.event, self.position, json);
                    }
                    None => {
                        self.decoder.payloads = codec::PayloadCache::default();
                        expand_legacy_pi_payload(&mut event, &mut self.decoder.legacy_pi).map_err(
                            |error| {
                                anyhow::anyhow!(
                                    "journal {}:{}: {error}",
                                    self.path.display(),
                                    self.line_no
                                )
                            },
                        )?;
                    }
                    Some(kind) => anyhow::bail!("unsupported journal record type {kind:?}"),
                }
                JournalRecord::Event(event)
            };
            return Ok(Some(JournalRecordEntry {
                record,
                through: self.position,
                decoded_bytes,
                capture_sequence,
            }));
        }
    }

    pub(crate) fn extend(&mut self, through: u64) {
        let buffered = self.reader.buffer().len() as u64;
        self.reader
            .get_mut()
            .set_limit(through.saturating_sub(self.position + buffered));
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

const LEGACY_PI_MESSAGES_DELTA: &str = "_bt_messages_delta";

// Read compatibility for journals written before native payload preservation.
// New event records are tagged explicitly and never enter this decoder.
fn expand_legacy_pi_payload(
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
    let Some(delta) = native.remove(LEGACY_PI_MESSAGES_DELTA) else {
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
        let partition = path
            .file_stem()
            .and_then(|name| name.to_str())
            .and_then(|name| name.rsplit("--").next());
        let protected = match partition {
            Some(id) => {
                tokio::fs::try_exists(data_dir.join("pipeline").join(format!("{id}.sqlite")))
                    .await
                    .unwrap_or(true)
            }
            None => false,
        };
        if old && !protected && legacy_collectible(data_dir, &path).await {
            if let Err(e) = tokio::fs::remove_file(&path).await {
                tracing::warn!(path = %path.display(), "failed to remove stale journal: {e}");
            }
        }
    }
}

// Pre-SpanOp journals can be collected only with explicit completion evidence.
// Unknown or uncheckpointed history stays available for migration/reconstruction.
async fn legacy_collectible(data_dir: &Path, path: &Path) -> bool {
    let Ok(Some(mut reader)) =
        JournalReader::open(path, JournalReader::recorded_len(path).await).await
    else {
        return false;
    };
    let mut observed = std::collections::HashMap::<String, u64>::new();
    let mut completed = std::collections::HashMap::<String, u64>::new();
    loop {
        match reader.next_record().await {
            Ok(Some(entry)) => match entry.record {
                JournalRecord::Event(event) => {
                    let partition = data_dir.join("pipeline").join(format!(
                        "{}.sqlite",
                        crate::ids::session_storage_id(&event.source, &event.session_id)
                    ));
                    if tokio::fs::try_exists(partition).await.unwrap_or(true) {
                        return false;
                    }
                    let Some(route) = event.route else {
                        return false;
                    };
                    observed.insert(
                        serde_json::to_string(&route).unwrap_or_default(),
                        entry.through,
                    );
                }
                JournalRecord::DeliveryCheckpoint { route, through } => {
                    completed
                        .entry(serde_json::to_string(&route).unwrap_or_default())
                        .and_modify(|previous| *previous = (*previous).max(through))
                        .or_insert(through);
                }
                JournalRecord::Control(_) => return false,
            },
            Ok(None) => break,
            Err(_) => return false,
        }
    }
    !observed.is_empty()
        && observed
            .iter()
            .all(|(route, through)| completed.get(route).copied().unwrap_or(0) >= *through)
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
    async fn every_source_journals_complete_payloads_in_event_order() {
        for source in crate::translate::Registry::default_agents().sources() {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("events.ndjson");
            let mut writer = JournalWriter::open_path(&path).await.unwrap();
            let mut expected = Vec::new();
            for (index, name) in [
                "context",
                "before_provider_request",
                "message_update",
                "agent_end",
            ]
            .into_iter()
            .enumerate()
            {
                let mut event = pi_context(
                    vec![serde_json::json!({"role":"user","content":"entire history"})],
                    index as i64,
                );
                event.source = source.clone();
                event.event = name.into();
                event.payload["event"]["type"] = name.into();
                event.payload["event"]["message"] =
                    serde_json::json!({"role":"assistant","content":"growing message"});
                event.payload["event"]["assistantMessageEvent"] = serde_json::json!({"type":"text_delta","delta":"token","partial":"partial message"});
                event.payload["event"]["payload"] = serde_json::json!({"system":"system prompt","messages":[{"role":"user","content":"provider messages"}],"unknown_provider_field":{"preserve":true}});
                writer.append(&event).await.unwrap();
                expected.push(event);
            }
            drop(writer);
            let stored = tokio::fs::read_to_string(&path).await.unwrap();
            let records = stored
                .lines()
                .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                .collect::<Vec<_>>();
            let mut reader = JournalReader::open(&path, stored.len() as u64)
                .await
                .unwrap()
                .unwrap();
            for (index, event) in expected.iter().enumerate() {
                assert_eq!(
                    records[index]["payload"], event.payload,
                    "{source}: {} was changed before persistence",
                    event.event
                );
                let replayed = reader.next_entry().await.unwrap().unwrap();
                assert_eq!(replayed.event, event.event);
                assert_eq!(replayed.payload, event.payload);
                assert!(
                    reader.decoder_context().legacy_pi.is_empty(),
                    "new {source} records must not require the legacy Pi message decoder"
                );
            }
            assert!(reader.next_entry().await.unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn every_source_deduplicates_repeated_payloads_and_resumes_losslessly() {
        for source in crate::translate::Registry::default_agents().sources() {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("events.ndjson");
            let mut writer = JournalWriter::open_path(&path).await.unwrap();
            let mut expected = Vec::new();
            let mut messages = Vec::new();
            let mut raw_bytes = 0;
            for index in 0..20 {
                messages.push(serde_json::json!({"role":"user","content":format!("{index}:{}", "x".repeat(4096))}));
                let mut event = pi_context(messages.clone(), index);
                event.source = source.clone();
                raw_bytes += serde_json::to_vec(&event.redacted()).unwrap().len() + 1;
                writer.append(&event).await.unwrap();
                expected.push(event);
            }
            drop(writer);
            let stored = tokio::fs::read_to_string(&path).await.unwrap();
            assert!(
                stored.len() < raw_bytes / 3,
                "{source} should benefit from the shared payload codec"
            );
            assert!(stored.contains(codec::DELTA_RECORD));
            let mut reader = JournalReader::open(&path, stored.len() as u64)
                .await
                .unwrap()
                .unwrap();
            let mut boundary = 0;
            for (index, event) in expected.iter().enumerate() {
                let entry = reader.next_record().await.unwrap().unwrap();
                let JournalRecord::Event(replayed) = entry.record else {
                    panic!("expected event")
                };
                assert_eq!(replayed.payload, event.payload, "{source} event {index}");
                boundary = entry.through;
                // Restore directly in the middle of a delta chain, with no
                // prefix replay and without assumptions about agent schemas.
                if index == 9 {
                    let saved = serde_json::to_vec(reader.decoder_context()).unwrap();
                    let context = serde_json::from_slice(&saved).unwrap();
                    reader = JournalReader::resume(&path, boundary, stored.len() as u64, context)
                        .await
                        .unwrap();
                }
            }
            assert_eq!(boundary, stored.len() as u64);
        }
    }

    #[tokio::test]
    async fn legacy_pi_deltas_recover_and_mix_with_complete_new_events() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("pi.ndjson");
        let first = pi_context(
            vec![serde_json::json!({"role":"user","content":"old history"})],
            1,
        );
        let mut delta = pi_context(Vec::new(), 2).redacted();
        delta.payload["event"]
            .as_object_mut()
            .unwrap()
            .remove("messages");
        delta.payload["event"][LEGACY_PI_MESSAGES_DELTA] = serde_json::json!({"common_prefix":1,"suffix":[{"role":"assistant","content":"legacy delta"}]});
        let first_line = format!("{}\n", serde_json::to_string(&first.redacted()).unwrap());
        let old_log = format!("{}{}\n", first_line, serde_json::to_string(&delta).unwrap());
        tokio::fs::write(&path, old_log.as_bytes()).await.unwrap();
        let mut prefix = JournalReader::open(&path, first_line.len() as u64)
            .await
            .unwrap()
            .unwrap();
        prefix.next_entry().await.unwrap().unwrap();
        let decoder = prefix.decoder_context().clone();
        let mut writer = JournalWriter::open_path(&path).await.unwrap();
        let mut native = pi_context(
            vec![serde_json::json!({"role":"user","content":"new native snapshot"})],
            3,
        );
        // Even an identically named field inside a native payload must be
        // preserved. Only untagged old records use the compatibility codec.
        native.payload["event"][LEGACY_PI_MESSAGES_DELTA] =
            serde_json::json!({"native_field":"preserve"});
        let tail = writer.append(&native).await.unwrap();
        drop(writer);
        let mut reader = JournalReader::resume(&path, first_line.len() as u64, tail, decoder)
            .await
            .unwrap();
        let expanded = reader.next_entry().await.unwrap().unwrap();
        assert_eq!(
            expanded.payload["event"]["messages"],
            serde_json::json!([{"role":"user","content":"old history"},{"role":"assistant","content":"legacy delta"}])
        );
        assert_eq!(
            reader.next_entry().await.unwrap().unwrap().payload,
            native.payload
        );
        assert!(reader.decoder_context().legacy_pi.is_empty());
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
