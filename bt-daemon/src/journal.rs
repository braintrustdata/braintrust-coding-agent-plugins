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
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};

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
}

const PI_MESSAGES_DELTA: &str = "_bt_messages_delta";

pub(crate) fn compact_pi_payload(
    event: &mut RedactedEnvelope,
    previous: &mut Vec<serde_json::Value>,
) {
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

pub(crate) fn expand_pi_payload(
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
    fn long_session_ids_get_distinct_bounded_journals() {
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
        let mut lines = Vec::new();
        for envelope in [
            Envelope {
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
            },
            Envelope {
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
            },
        ] {
            lines.extend(serde_json::to_vec(&envelope.redacted()).unwrap());
            lines.push(b'\n');
        }
        tokio::fs::write(&path, lines).await.unwrap();
        assert_eq!(
            legacy_journal_source(temp.path(), "legacy", &route).await,
            Some("codex".into())
        );
        assert!(legacy_journal_has_session(temp.path(), "codex", "legacy").await);
        assert!(!legacy_journal_has_session(temp.path(), "claude-code", "legacy").await);
    }
}
