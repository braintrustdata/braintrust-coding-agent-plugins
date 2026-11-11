//! Durable per-source input collection in the session's pipeline SQLite database.
//! Capture persists redacted events before acknowledging them. Translation atomically
//! saves its continuation and output before removing consumed input records.
//! Legacy NDJSON journals are imported at their original logical positions on open.
//!
//! Managed-run acceptance records remain separate so flush can discover pipelines
//! after the daemon that accepted them has restarted or idle-exited.

use crate::collections::{async_iter::AsyncIteratorExt, Collection};
use crate::wire::{BackendAuth, Envelope, RedactedEnvelope, SessionRoute};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};

pub(crate) async fn paths(data_dir: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut paths = std::collections::BTreeSet::new();
    for path in crate::collections::journal_paths(data_dir).await? {
        let qualified = path
            .file_stem()
            .and_then(|v| v.to_str())
            .and_then(|v| v.rsplit_once("--").map(|(_, id)| id))
            .is_some_and(|id| uuid::Uuid::parse_str(id).is_ok());
        if qualified {
            paths.insert(path);
            continue;
        }
        match migrate_session_journal(data_dir, &path).await {
            Ok(migrated) => paths.extend(migrated),
            Err(error) => {
                tracing::warn!(%error,path=%path.display(),"legacy journal discovery held; retaining input")
            }
        }
    }
    Ok(paths.into_iter().collect())
}

async fn migrate_session_journal(data_dir: &Path, path: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let Some(mut reader) = JournalReader::open_legacy(path, u64::MAX).await? else {
        return Ok(Vec::new());
    };
    let mut sessions = std::collections::BTreeSet::new();
    while let Some(record) = reader.next_entry().await? {
        sessions.insert((
            crate::translate::canonical_source_name(&record.source).to_owned(),
            record.session_id,
        ));
    }
    drop(reader);
    let mut complete = !sessions.is_empty();
    let mut paths = Vec::new();
    for (source, session) in sessions {
        let qualified = ensure_source_journal(data_dir, &source, &session).await?;
        drop(JournalWriter::open_path(&qualified).await?);
        complete &= !tokio::fs::try_exists(&qualified).await?;
        paths.push(qualified);
    }
    if complete {
        tokio::fs::remove_file(path).await?;
    }
    Ok(paths)
}

pub(crate) async fn identity(path: &Path) -> anyhow::Result<Option<RedactedEnvelope>> {
    let database = crate::collections::database_path(path);
    if !Collection::<serde_json::Value>::exists(&database, "events").await? {
        return Ok(None);
    }
    let records = Collection::<serde_json::Value>::open(&database, "events").await?;
    records
        .identity()
        .await?
        .map(|value| Ok(serde_json::from_value(value)?))
        .transpose()
}

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
/// until all source-qualified copies have been imported successfully.
pub async fn ensure_source_journal(
    data_dir: &Path,
    source: &str,
    session_id: &str,
) -> anyhow::Result<PathBuf> {
    let path = source_journal_path(data_dir, source, session_id);
    if Collection::<serde_json::Value>::exists(&crate::collections::database_path(&path), "events")
        .await?
    {
        return Ok(path);
    }
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
            let owner = legacy_journal_has_session(data_dir, source, session_id).await;
            let records = Collection::<serde_json::Value>::open(
                &crate::collections::database_path(&path),
                "events",
            )
            .await?;
            records
                .set_identity(&serde_json::json!({"_bt_legacy_namespace":owner}), 0)
                .await?;
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
    if let Ok(Some(mut reader)) = JournalReader::open(&path, through).await {
        while let Ok(Some(entry)) = reader.next_entry().await {
            if entry
                .route
                .as_ref()
                .is_some_and(|candidate| candidate.same_route(route))
            {
                return Some(entry.source);
            }
        }
    }
    for path in paths(data_dir).await.ok()? {
        let Some(identity) = identity(&path).await.ok().flatten() else {
            continue;
        };
        if identity.session_id != session_id {
            continue;
        }
        if identity
            .route
            .as_ref()
            .is_some_and(|candidate| candidate.same_route(route))
        {
            return Some(identity.source);
        }
        if let Ok(partition) =
            crate::pipeline::journal::SpanJournal::open(&crate::collections::database_path(&path))
                .await
        {
            if partition
                .routes()
                .await
                .ok()?
                .iter()
                .any(|candidate| candidate.same_route(route))
            {
                return Some(identity.source);
            }
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
    let qualified = source_journal_path(data_dir, canonical_source, session_id);
    let database = crate::collections::database_path(&qualified);
    if Collection::<serde_json::Value>::exists(&database, "events")
        .await
        .unwrap_or(false)
    {
        if let Ok(records) = Collection::<serde_json::Value>::open(&database, "events").await {
            if let Ok(Some(identity)) = records.identity().await {
                if let Some(owner) = identity
                    .get("_bt_legacy_namespace")
                    .and_then(|v| v.as_bool())
                {
                    return owner;
                }
            }
        }
    }
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

/// Durable capture writer for one source session.
pub struct JournalWriter {
    records: Collection<serde_json::Value>,
    position: u64,
}

impl JournalWriter {
    pub async fn open_path(path: &Path) -> anyhow::Result<Self> {
        let records = Collection::open(&crate::collections::database_path(path), "events").await?;
        records.set_path(path).await?;
        if let Ok(metadata) = tokio::fs::metadata(path).await {
            records.reserve(metadata.len()).await?;
        }
        let expected_source = path
            .file_stem()
            .and_then(|v| v.to_str())
            .and_then(|v| v.split_once("--"))
            .map(|(source, _)| source.to_owned());
        let migration = async {
            let mut routes = Vec::<SessionRoute>::new();
            if let Some(mut legacy) = JournalReader::open_legacy(path, u64::MAX).await? {
                while let Some(entry) = legacy.next_record().await? {
                    let value = match entry.record {
                        JournalRecord::Event(event) => {
                            if expected_source.as_deref().is_some_and(|source| {
                                crate::translate::canonical_source_name(&event.source) != source
                            }) {
                                continue;
                            }
                            if let Some(route) = &event.route {
                                routes.push(route.clone());
                            }
                            serde_json::to_value(event)?
                        }
                        JournalRecord::Control(control) => {
                            let mut value = serde_json::to_value(control)?;
                            value["_bt_record_type"] = "pipeline_control".into();
                            value
                        }
                        JournalRecord::DeliveryCheckpoint { route, through } => {
                            if expected_source.is_some()
                                && !routes.iter().any(|known| known.same_route(&route))
                            {
                                continue;
                            }
                            serde_json::json!({"_bt_record_type":"delivery_checkpoint","route":route,"through":through})
                        }
                    };
                    records.import(entry.through, &value).await?;
                    if let Some(payload) = value.get("payload") {
                        let mut identity = value.clone();
                        let mut kept = serde_json::Map::new();
                        for key in ["transcript_path", "agent_transcript_path", "_bt_transcript_mirror"] {
                            if let Some(v) = payload.get(key) {
                                kept.insert(key.into(), v.clone());
                            }
                        }
                        identity["payload"] = kept.into();
                        if let Some(previous) = records.identity().await? {
                            if let Some(owner) = previous.get("_bt_legacy_namespace") {
                                identity["_bt_legacy_namespace"] = owner.clone();
                            }
                        }
                        records.set_identity(&identity, entry.through).await?;
                    }
                }
                drop(legacy);
                tokio::fs::remove_file(path).await?;
            }
            Ok::<_, anyhow::Error>(())
        }.await;
        if let Err(error) = migration {
            tracing::warn!(%error,path=%path.display(),"legacy journal migration is held; existing SpanOps remain deliverable");
        }
        let position = records.tail().await?;
        Ok(Self { records, position })
    }

    pub(crate) fn collection(&self) -> Collection<serde_json::Value> {
        self.records.clone()
    }

    pub(crate) fn position(&self) -> u64 {
        self.position
    }

    /// Append one redacted event and confirm its durable write before capture acknowledges it.
    ///
    /// The journal is a durability record and is never capped or truncated:
    /// dropping entries would silently cost recovery fidelity. Its size is
    /// bounded instead by writing each transcript byte once (see
    /// [`crate::transcript_mirror`]) and by age-based GC, and replay reads it
    /// as a stream so a large journal never becomes a large allocation.
    pub async fn append(&mut self, env: &Envelope) -> anyhow::Result<u64> {
        let mut identity = env.redacted();
        let mut payload = serde_json::Map::new();
        for key in [
            "transcript_path",
            "agent_transcript_path",
            "_bt_transcript_mirror",
        ] {
            if let Some(value) = identity.payload.get(key) {
                payload.insert(key.into(), value.clone());
            }
        }
        if let Some(previous) = self.records.identity().await? {
            for key in [
                "transcript_path",
                "agent_transcript_path",
                "_bt_transcript_mirror",
            ] {
                if !payload.contains_key(key) {
                    if let Some(value) = previous.get("payload").and_then(|v| v.get(key)) {
                        payload.insert(key.into(), value.clone());
                    }
                }
            }
        }
        identity.payload = serde_json::Value::Object(payload);
        let mut identity = serde_json::to_value(identity)?;
        if let Some(previous) = self.records.identity().await? {
            if let Some(owner) = previous.get("_bt_legacy_namespace") {
                identity["_bt_legacy_namespace"] = owner.clone();
            }
        }
        let mut redacted = env.redacted();
        // New rows are independently decodable: context deltas start at zero.
        // Other Pi admission filters still discard unused streaming/request data.
        compact_pi_payload(&mut redacted, &mut Vec::new());
        self.position = self
            .records
            .insert_with_identity(&serde_json::to_value(redacted)?, Some(&identity))
            .await?;
        Ok(self.position)
    }

    pub(crate) async fn append_control(
        &mut self,
        control: &PipelineControl,
    ) -> anyhow::Result<u64> {
        let mut value = serde_json::to_value(control)?;
        value["_bt_record_type"] = "pipeline_control".into();
        self.position = self.records.insert(&value).await?;
        Ok(self.position)
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlKind {
    Checkpoint,
    Finalize,
    Retire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PipelineControl {
    pub kind: ControlKind,
    pub bounds: std::collections::HashMap<String, u64>,
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
    reader: Option<tokio::io::BufReader<tokio::io::Take<tokio::fs::File>>>,
    records: Option<Collection<serde_json::Value>>,
    through: u64,
    path: PathBuf,
    line_no: usize,
    position: u64,
    pi_context: Vec<serde_json::Value>,
}

impl JournalReader {
    pub(crate) fn position(&self) -> u64 {
        self.position
    }

    /// Rebuild decoder context before continuing at a saved record boundary.
    pub(crate) async fn resume(path: &Path, position: u64, through: u64) -> anyhow::Result<Self> {
        anyhow::ensure!(position <= through, "invalid recovery range");
        let mut reader = Self::open(path, through)
            .await?
            .ok_or_else(|| anyhow::anyhow!("recovery journal is missing"))?;
        if reader.records.is_some() {
            reader.position = position;
            return Ok(reader);
        }
        while reader.position < position {
            anyhow::ensure!(
                reader.next_record().await?.is_some(),
                "recovery journal ended before its boundary"
            );
        }
        anyhow::ensure!(reader.position == position, "invalid recovery boundary");
        Ok(reader)
    }

    /// Open a journal for streaming, reading at most `through` bytes.
    ///
    /// That bound is what keeps replay from consuming the very event that
    /// triggered the session's creation: the caller records the journal's
    /// length before appending, so the actor replays strictly what was
    /// already recovered state, never the live event still on its way to the
    /// queue. `Ok(None)` means the session has no journal yet.
    pub async fn open(path: &Path, through: u64) -> anyhow::Result<Option<Self>> {
        let database = crate::collections::database_path(path);
        if Collection::<serde_json::Value>::exists(&database, "events").await? {
            return Ok(Some(Self {
                reader: None,
                records: Some(Collection::open(&database, "events").await?),
                through,
                path: path.to_owned(),
                line_no: 0,
                position: 0,
                pi_context: Vec::new(),
            }));
        }
        Self::open_legacy(path, through).await
    }

    async fn open_legacy(path: &Path, through: u64) -> anyhow::Result<Option<Self>> {
        let file = match tokio::fs::File::open(path).await {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        Ok(Some(Self {
            reader: Some(tokio::io::BufReader::new(file.take(through))),
            records: None,
            through,
            path: path.to_path_buf(),
            line_no: 0,
            position: 0,
            pi_context: Vec::new(),
        }))
    }

    /// The journal's current length, which is the bound a session created now
    /// should replay through. A missing journal replays nothing.
    pub async fn recorded_len(path: &Path) -> u64 {
        let database = crate::collections::database_path(path);
        if Collection::<serde_json::Value>::exists(&database, "events")
            .await
            .unwrap_or(false)
        {
            if let Ok(records) = Collection::<serde_json::Value>::open(&database, "events").await {
                return records.tail().await.unwrap_or(0);
            }
        }
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
            let read = if let Some(records) = &self.records {
                let Some(entry) = records
                    .range(self.position, self.through)
                    .next()
                    .await
                    .transpose()?
                else {
                    return Ok(None);
                };
                bytes = serde_json::to_vec(&entry.value)?;
                let read = entry.id - self.position;
                usize::try_from(read)?
            } else if let Some(reader) = &mut self.reader {
                reader.read_until(b'\n', &mut bytes).await?
            } else {
                return Ok(None);
            };
            if read == 0 {
                return Ok(None);
            }
            self.position += read as u64;
            self.line_no += 1;
            let path = self.path.clone();
            let line_no = self.line_no;
            let position = self.position;
            let mut pi_context = std::mem::take(&mut self.pi_context);
            let (pi_context, record) = tokio::task::spawn_blocking(move || {
                let record = decode_record(&bytes, &path, line_no, position, &mut pi_context);
                (pi_context, record)
            })
            .await?;
            self.pi_context = pi_context;
            if let Some(record) = record? {
                return Ok(Some(record));
            }
        }
    }

    pub(crate) fn extend(&mut self, through: u64) {
        self.through = through;
        if let Some(reader) = &mut self.reader {
            let buffered = reader.buffer().len() as u64;
            reader
                .get_mut()
                .set_limit(through.saturating_sub(self.position + buffered));
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

fn decode_record(
    bytes: &[u8],
    path: &Path,
    line_no: usize,
    position: u64,
    pi_context: &mut Vec<serde_json::Value>,
) -> anyhow::Result<Option<JournalRecordEntry>> {
    let line = std::str::from_utf8(bytes)
        .map_err(|error| anyhow::anyhow!("journal {}:{}: {error}", path.display(), line_no))?
        .trim();
    if line.is_empty() {
        return Ok(None);
    }
    let value: serde_json::Value = serde_json::from_str(line)
        .map_err(|error| anyhow::anyhow!("journal {}:{}: {error}", path.display(), line_no))?;
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
        let mut event = serde_json::from_value(value)
            .map_err(|error| anyhow::anyhow!("journal {}:{}: {error}", path.display(), line_no))?;
        expand_pi_payload(&mut event, pi_context)
            .map_err(|error| anyhow::anyhow!("journal {}:{}: {error}", path.display(), line_no))?;
        JournalRecord::Event(event)
    };
    Ok(Some(JournalRecordEntry {
        record,
        through: position,
    }))
}

const PI_MESSAGES_DELTA: &str = "_bt_messages_delta";

fn compact_pi_payload(event: &mut RedactedEnvelope, previous: &mut Vec<serde_json::Value>) {
    if event.source != "pi" {
        return;
    }
    let Some(native) = event
        .payload
        .get_mut("event")
        .and_then(serde_json::Value::as_object_mut)
    else {
        return;
    };
    match event.event.as_str() {
        "context" => {
            let Some(messages) = native
                .remove("messages")
                .and_then(|messages| messages.as_array().cloned())
            else {
                return;
            };
            let common_prefix = previous
                .iter()
                .zip(&messages)
                .take_while(|(left, right)| left == right)
                .count();
            native.insert(
                PI_MESSAGES_DELTA.into(),
                serde_json::json!({
                    "common_prefix": common_prefix,
                    "suffix": messages[common_prefix..],
                }),
            );
            *previous = messages;
        }
        "before_provider_request" => {
            // The translator records only the allowlisted request
            // configuration and tool definitions, never the prompt or messages.
            if let Some(payload) = native.get_mut("payload") {
                *payload = serde_json::Value::Object(crate::translate::pi_request_config(payload));
            }
        }
        "agent_end" => {
            // The translator only consumes willRetry from this lifecycle event.
            native.remove("messages");
        }
        "message_update" => {
            // Pi repeats the progressively growing assistant message on every
            // streaming update. Translation only needs the update type to mark
            // time-to-first-token; the completed message arrives separately in
            // message_end.
            native.remove("message");
            if let Some(update) = native
                .get_mut("assistantMessageEvent")
                .and_then(serde_json::Value::as_object_mut)
            {
                update.retain(|key, _| key == "type");
            }
            native.retain(|key, _| key == "type" || key == "assistantMessageEvent");
        }
        _ => {}
    }
}

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
    async fn stored(path: &Path) -> anyhow::Result<Vec<u8>> {
        use crate::collections::async_iter::IntoAsyncIterator;
        let collection = Collection::<serde_json::Value>::open(
            &crate::collections::database_path(path),
            "events",
        )
        .await?;
        let mut iter = (&collection).into_async_iter();
        let mut bytes = Vec::new();
        while let Some(entry) = iter.next().await {
            bytes.extend(serde_json::to_vec(&entry?.value)?);
            bytes.push(b'\n');
        }
        Ok(bytes)
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
    async fn pi_context_history_is_delta_encoded_and_replayed() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("pi.ndjson");
        let mut legacy = Vec::new();
        let mut previous = Vec::new();
        let mut messages = Vec::new();
        let mut naive_bytes = 0usize;
        for index in 0..20 {
            messages.push(serde_json::json!({
                "role":"user",
                "content": format!("{index}:{}", "x".repeat(4096)),
            }));
            let event = pi_context(messages.clone(), index);
            naive_bytes += serde_json::to_vec(&event.redacted()).unwrap().len() + 1;
            let mut event = event.redacted();
            compact_pi_payload(&mut event, &mut previous);
            legacy.extend(serde_json::to_vec(&event).unwrap());
            legacy.push(b'\n');
        }
        let compacted = vec![
            serde_json::json!({"role":"compactionSummary","summary":"bounded"}),
            serde_json::json!({"role":"user","content":"after"}),
        ];
        let event = pi_context(compacted.clone(), 20);
        naive_bytes += serde_json::to_vec(&event.redacted()).unwrap().len() + 1;
        let mut event = event.redacted();
        compact_pi_payload(&mut event, &mut previous);
        legacy.extend(serde_json::to_vec(&event).unwrap());
        legacy.push(b'\n');
        tokio::fs::write(&path, &legacy).await.unwrap();
        let stored = legacy;
        assert!(
            stored.len() < naive_bytes / 3,
            "Pi journal did not eliminate cumulative context copies: stored={} naive={naive_bytes}",
            stored.len()
        );
        assert!(!String::from_utf8_lossy(&stored).contains("\"messages\""));

        let writer = JournalWriter::open_path(&path).await.unwrap();
        let through = writer.position();
        assert!(!path.exists(), "cutover removes the legacy journal");
        drop(writer);
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
    async fn pi_streaming_updates_do_not_persist_growing_partial_messages() {
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

        let stored = stored(&path).await.unwrap();
        assert!(!String::from_utf8_lossy(&stored).contains("growing-partial-marker"));

        let mut reader = JournalReader::open(&path, stored.len() as u64)
            .await
            .unwrap()
            .unwrap();
        let replayed = reader.next_entry().await.unwrap().unwrap();
        assert_eq!(
            replayed
                .payload
                .pointer("/event/assistantMessageEvent/type"),
            Some(&serde_json::json!("text_delta"))
        );
    }

    #[tokio::test]
    async fn pi_provider_requests_persist_only_request_configuration() {
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

        let stored = stored(&path).await.unwrap();
        let stored_text = String::from_utf8_lossy(&stored);
        assert!(!stored_text.contains("system-prompt-marker"));
        assert!(!stored_text.contains("message-marker"));

        let mut reader = JournalReader::open(&path, stored.len() as u64)
            .await
            .unwrap()
            .unwrap();
        let replayed = reader.next_entry().await.unwrap().unwrap();
        assert_eq!(
            replayed.payload.pointer("/event/payload"),
            Some(&serde_json::json!({
                "max_tokens": 1024,
                "tools": [{"type": "function", "function": {
                    "name": "read",
                    "parameters": {"type": "object"},
                }}],
            }))
        );
    }

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

    #[tokio::test]
    async fn interrupted_import_preserves_new_capture_and_original_positions() {
        let temp = tempfile::tempdir().unwrap();
        let path = source_journal_path(temp.path(), "pi", "pi-session");
        tokio::fs::create_dir_all(path.parent().unwrap())
            .await
            .unwrap();
        let old = pi_context(vec![serde_json::json!({"role":"user","content":"old"})], 1);
        let line = serde_json::to_vec(&old.redacted()).unwrap();
        let mut broken = line.clone();
        broken.extend_from_slice(b"\n{broken}\n");
        tokio::fs::write(&path, &broken).await.unwrap();
        let mut writer = JournalWriter::open_path(&path).await.unwrap();
        assert!(path.exists(), "failed import keeps its source");
        let new = pi_context(vec![serde_json::json!({"role":"user","content":"new"})], 2);
        let newest = writer.append(&new).await.unwrap();
        assert!(newest > broken.len() as u64);
        drop(writer);
        // Repair the legacy tail without moving its valid prefix.
        tokio::fs::write(&path, [line.as_slice(), b"\n"].concat())
            .await
            .unwrap();
        let writer = JournalWriter::open_path(&path).await.unwrap();
        assert_eq!(writer.position(), newest);
        assert!(!path.exists());
        assert_eq!(identity(&path).await.unwrap().unwrap().ts_ms, 2);
        let mut reader = JournalReader::open(&path, newest).await.unwrap().unwrap();
        let first = reader.next_record().await.unwrap().unwrap();
        assert_eq!(first.through, line.len() as u64 + 1);
        assert_eq!(reader.next_record().await.unwrap().unwrap().through, newest);
        assert!(reader.next_record().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn startup_imports_session_only_journal_without_resurrecting_consumed_records() {
        let temp = tempfile::tempdir().unwrap();
        let path = journal_path(temp.path(), "pi-session");
        tokio::fs::create_dir_all(path.parent().unwrap())
            .await
            .unwrap();
        let mut first = pi_context(vec![], 1);
        let mut second = first.clone();
        second.source = "codex".into();
        second.ts_ms = 2;
        first.route = Some(SessionRoute::default());
        let bytes = format!(
            "{}\n{}\n",
            serde_json::to_string(&first.redacted()).unwrap(),
            serde_json::to_string(&second.redacted()).unwrap()
        );
        tokio::fs::write(&path, bytes).await.unwrap();
        let migrated = paths(temp.path()).await.unwrap();
        assert_eq!(migrated.len(), 2);
        assert!(!path.exists());
        for (source, ts) in [("pi", 1), ("codex", 2)] {
            let qualified = source_journal_path(temp.path(), source, "pi-session");
            assert_eq!(identity(&qualified).await.unwrap().unwrap().ts_ms, ts);
            let records = Collection::<serde_json::Value>::open(
                &crate::collections::database_path(&qualified),
                "events",
            )
            .await
            .unwrap();
            let mut entries = records.range(0, records.tail().await.unwrap());
            let entry = entries.next().await.unwrap().unwrap();
            assert_eq!(entry.value["source"], source);
            assert!(entries.next().await.is_none());
            records.remove(entry.id).await.unwrap();
            drop(
                JournalWriter::open_path(
                    &ensure_source_journal(temp.path(), source, "pi-session")
                        .await
                        .unwrap(),
                )
                .await
                .unwrap(),
            );
            assert!(records
                .range(0, records.tail().await.unwrap())
                .next()
                .await
                .is_none());
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
