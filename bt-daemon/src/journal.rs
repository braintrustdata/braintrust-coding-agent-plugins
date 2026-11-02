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
pub(crate) use codec::CodecState;

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
    codec: CodecState,
    failed_write: bool,
}

/// Ignore only an unterminated trailing write. Complete records, including
/// malformed ones, remain visible so corruption cannot silently discard data.
async fn complete_position(file: &mut tokio::fs::File) -> std::io::Result<u64> {
    let len = file.metadata().await?.len();
    if len == 0 {
        return Ok(0);
    }
    file.seek(std::io::SeekFrom::Start(len - 1)).await?;
    if file.read_u8().await? == b'\n' {
        return Ok(len);
    }
    let mut end = len;
    let mut buffer = vec![0; 8192];
    while end > 0 {
        let start = end.saturating_sub(buffer.len() as u64);
        let bytes = &mut buffer[..(end - start) as usize];
        file.seek(std::io::SeekFrom::Start(start)).await?;
        file.read_exact(bytes).await?;
        if let Some(index) = bytes.iter().rposition(|byte| *byte == b'\n') {
            return Ok(start + index as u64 + 1);
        }
        end = start;
    }
    Ok(0)
}

impl JournalWriter {
    pub async fn open_path(path: &Path) -> anyhow::Result<Self> {
        if let Some(dir) = path.parent() {
            tokio::fs::create_dir_all(dir).await?;
        }
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)
            .await?;
        let position = complete_position(&mut file).await?;
        if position < file.metadata().await?.len() {
            file.set_len(position).await?;
        }
        file.sync_data().await?;
        Ok(Self {
            file,
            position,
            codec: CodecState::default(),
            failed_write: false,
        })
    }

    pub(crate) fn position(&self) -> u64 {
        self.position
    }

    async fn append_bytes(&mut self, line: &[u8]) -> anyhow::Result<u64> {
        if self.failed_write {
            self.file.set_len(self.position).await?;
            self.file.sync_data().await?;
            self.failed_write = false;
        }
        if let Err(error) = async {
            self.file.write_all(line).await?;
            self.file.flush().await?;
            self.file.sync_data().await
        }
        .await
        {
            self.failed_write = true;
            return Err(error.into());
        }
        let before = self.position;
        self.position += line.len() as u64;
        Ok(before)
    }

    /// Append one redacted event and confirm its durable write before capture acknowledges it.
    ///
    /// Complete journal records are never capped or truncated. An unfinished
    /// trailing write is discarded on reopen, before new appends. Its size is
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
        let (json, delta) = self.codec.encode(&redacted.payload)?;
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
        let before = self.append_bytes(&line).await?;
        self.codec.record(self.position, json.clone());
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
        let before = self.append_bytes(&line).await?;
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
    decoder: CodecState,
}

impl JournalReader {
    pub(crate) fn position(&self) -> u64 {
        self.position
    }

    /// The source consumer owns decoding state between recovery reads.
    pub(crate) fn swap_decoder_context(&mut self, context: &mut CodecState) {
        std::mem::swap(&mut self.decoder, context);
    }

    /// Resume any source journal at a committed byte offset with its saved
    /// lossless payload cache (and, for old Pi logs, their legacy message base).
    pub(crate) async fn resume(
        path: &Path,
        position: u64,
        through: u64,
        decoder_context: CodecState,
    ) -> anyhow::Result<Self> {
        decoder_context.validate()?;
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
            decoder: CodecState::default(),
        }))
    }

    /// The journal's current length, which is the bound a session created now
    /// should replay through. A missing journal replays nothing.
    pub async fn recorded_len(path: &Path) -> u64 {
        let Ok(mut file) = tokio::fs::File::open(path).await else {
            return 0;
        };
        complete_position(&mut file).await.unwrap_or(0)
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
            anyhow::ensure!(
                bytes.last() == Some(&b'\n'),
                "unterminated source journal record"
            );
            let path = self.path.clone();
            let line_no = self.line_no;
            let position = self.position;
            let mut decoder = std::mem::take(&mut self.decoder);
            // Expanding and parsing a large history can be CPU-heavy even
            // when file reads are async. Never run it on the hook IPC thread.
            let (decoder, decoded) = tokio::task::spawn_blocking(move || {
                let decoded = decode_record(&bytes, &path, line_no, position, &mut decoder);
                (decoder, decoded)
            })
            .await?;
            self.decoder = decoder;
            if let Some(entry) = decoded? {
                return Ok(Some(entry));
            }
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
        if through == 0 {
            return 0;
        }
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

fn decode_record(
    bytes: &[u8],
    path: &Path,
    line_no: usize,
    position: u64,
    decoder: &mut CodecState,
) -> anyhow::Result<Option<JournalRecordEntry>> {
    let line = std::str::from_utf8(bytes)
        .map_err(|error| anyhow::anyhow!("journal {}:{}: {error}", path.display(), line_no))?
        .trim();
    if line.is_empty() {
        return Ok(None);
    }
    let value: serde_json::Value = serde_json::from_str(line)
        .map_err(|error| anyhow::anyhow!("journal {}:{}: {error}", path.display(), line_no))?;
    let capture_sequence = value
        .get("_bt_capture_sequence")
        .and_then(serde_json::Value::as_u64);
    let mut decoded_bytes = bytes.len() as u64;
    let record = if value
        .get("_bt_record_type")
        .and_then(serde_json::Value::as_str)
        == Some("delivery_checkpoint")
    {
        let checkpoint: DeliveryCheckpointRecord = serde_json::from_value(value)
            .map_err(|error| anyhow::anyhow!("journal {}:{}: {error}", path.display(), line_no))?;
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
        let mut event: RedactedEnvelope = serde_json::from_value(value)
            .map_err(|error| anyhow::anyhow!("journal {}:{}: {error}", path.display(), line_no))?;
        match record_type.as_deref() {
            Some("event") | Some(codec::DELTA_RECORD) => {
                decoder.legacy_pi.clear();
                let json = if record_type.as_deref() == Some(codec::DELTA_RECORD) {
                    let (payload, json) = decoder.decode(event.payload)?;
                    decoded_bytes = decoded_bytes.max(json.len() as u64);
                    event.payload = payload;
                    Some(json)
                } else {
                    #[derive(Deserialize)]
                    struct StoredPayload<'a> {
                        #[serde(borrow)]
                        payload: &'a serde_json::value::RawValue,
                    }
                    let stored: StoredPayload<'_> = serde_json::from_str(line)?;
                    codec::cache_serialized(stored.payload.get())
                };
                decoder.record(position, json);
            }
            None => {
                decoder.base = None;
                expand_legacy_pi_payload(&mut event, &mut decoder.legacy_pi).map_err(|error| {
                    anyhow::anyhow!("journal {}:{}: {error}", path.display(), line_no)
                })?;
            }
            Some(kind) => anyhow::bail!("unsupported journal record type {kind:?}"),
        }
        JournalRecord::Event(event)
    };
    Ok(Some(JournalRecordEntry {
        record,
        through: position,
        decoded_bytes,
        capture_sequence,
    }))
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
    async fn delta_recovery_uses_original_json_bytes_not_reader_serialization() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("original-json.ndjson");
        // Key order and number formatting may differ across serde feature
        // sets/versions. Dictionaries use the writer's exact JSON.
        let raw = format!(r#"{{"z":"{}","a":1e-3}}"#, "x".repeat(4096));
        let first = format!("{{\"_bt_record_type\":\"event\",\"source\":\"debug\",\"session_id\":\"bytes\",\"event\":\"native\",\"ts_ms\":1,\"payload\":{raw}}}\n");
        let mut cache = CodecState::default();
        cache.record(first.len() as u64, Some(raw.clone().into()));
        let (_, delta) = cache
            .encode(&serde_json::json!({"z":"x".repeat(4096),"a":0.002}))
            .unwrap();
        let delta = serde_json::json!({
            "_bt_record_type":codec::DELTA_RECORD, "source":"debug", "session_id":"bytes", "event":"native", "ts_ms":2,
            "payload":delta.unwrap()
        });
        tokio::fs::write(&path, format!("{first}{delta}\n"))
            .await
            .unwrap();
        let through = JournalReader::recorded_len(&path).await;
        let mut reader = JournalReader::open(&path, through).await.unwrap().unwrap();
        assert_eq!(
            reader.next_entry().await.unwrap().unwrap().payload["a"],
            0.001
        );
        let next = reader.next_entry().await.unwrap().unwrap();
        assert_eq!(next.payload["a"], 0.002);
        assert_eq!(next.payload["z"], "x".repeat(4096));
    }

    #[tokio::test]
    async fn partial_writes_recover_on_retry_and_reopen_without_losing_payloads() {
        for reopen in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("events.ndjson");
            let mut writer = JournalWriter::open_path(&path).await.unwrap();
            let first = pi_context(
                vec![serde_json::json!({"content":"history".repeat(1024)})],
                1,
            );
            let through = writer.append(&first).await.unwrap();
            writer.file.write_all(b"{unfinished").await.unwrap();
            writer.file.flush().await.unwrap();
            assert_eq!(JournalReader::recorded_len(&path).await, through);
            if reopen {
                writer = JournalWriter::open_path(&path).await.unwrap();
            } else {
                writer.failed_write = true;
            }
            let mut second = first.clone();
            second.payload["tail"] = "after recovery".into();
            let position = writer.append_ordered(&second, None).await.unwrap();
            assert_eq!(position.before, through);
            let mut reader = JournalReader::open(&path, position.through)
                .await
                .unwrap()
                .unwrap();
            for event in [first, second] {
                assert_eq!(
                    reader.next_entry().await.unwrap().unwrap().payload,
                    event.payload
                );
            }
            assert!(reader.next_entry().await.unwrap().is_none());
        }
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
