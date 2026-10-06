use super::{envelope, validate_session_id};
use crate::translate::cursor::{IMPORT_CHECKPOINT, IMPORT_START, IMPORT_STOP};
use crate::wire::Envelope;
use anyhow::{bail, Context};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

/// Cursor stores transcript conversations under
/// ~/.cursor/projects/<workspace>/agent-transcripts/<id>/<id>.jsonl.
/// The directory and filename are the only session identity in this format.
pub(super) fn roots(home: &Path) -> Vec<PathBuf> {
    vec![home.join(".cursor/projects")]
}

pub(super) fn transcript_session_id(path: &Path) -> Option<String> {
    let session_id = path.file_stem()?.to_str()?;
    // Cursor writes child agent transcripts with an `agent-` identifier.
    // They do not carry a recoverable parent tool-call link, so they are not
    // selected as independent user sessions or claimed as recursive imports.
    if session_id.starts_with("agent-") {
        return None;
    }
    validate_session_id(session_id).ok()?;
    filename_matches(path, session_id).then(|| session_id.to_owned())
}

pub(super) fn filename_matches(path: &Path, session_id: &str) -> bool {
    !session_id.starts_with("agent-")
        && validate_session_id(session_id).is_ok()
        && path.extension().and_then(|value| value.to_str()) == Some("jsonl")
        && path.file_stem().and_then(|value| value.to_str()) == Some(session_id)
        && path
            .parent()
            .and_then(Path::file_name)
            .and_then(|value| value.to_str())
            == Some(session_id)
        && path
            .parent()
            .and_then(Path::parent)
            .and_then(Path::file_name)
            .and_then(|value| value.to_str())
            == Some("agent-transcripts")
}

fn is_conversation_record(record: &Value) -> bool {
    matches!(
        record.get("role").and_then(Value::as_str),
        Some("user" | "assistant")
    ) && record
        .get("message")
        .and_then(Value::as_object)
        .is_some_and(|message| message.get("content").is_some())
}

#[derive(Default)]
pub(super) struct Tail {
    started: bool,
    last_len: u64,
    stopped: bool,
}

impl Tail {
    pub(super) fn poll(
        &mut self,
        events: Vec<Envelope>,
        len: u64,
        finalize: bool,
    ) -> anyhow::Result<Vec<Envelope>> {
        if events.len() != 3 {
            bail!("Cursor import did not produce session boundary events");
        }
        let mut out = Vec::new();
        if !self.started {
            out.push(events[0].clone());
            self.started = true;
        }
        if len != self.last_len && len > 0 {
            out.push(events[1].clone());
        }
        if finalize && !self.stopped {
            out.push(events[2].clone());
            self.stopped = true;
        }
        self.last_len = len;
        Ok(out)
    }
}

pub(super) fn envelopes(
    path: &Path,
    through: u64,
    records: &[Value],
) -> anyhow::Result<Vec<Envelope>> {
    envelopes_with_snapshot(path, path, through, records)
}

pub(super) fn envelopes_with_snapshot(
    path: &Path,
    snapshot: &Path,
    through: u64,
    records: &[Value],
) -> anyhow::Result<Vec<Envelope>> {
    let session_id = transcript_session_id(path)
        .ok_or_else(|| anyhow::anyhow!("invalid Cursor transcript path {}", path.display()))?;
    if records.is_empty() {
        bail!("Cursor transcript {} is empty", path.display());
    }
    for (index, record) in records.iter().enumerate() {
        let supported = is_conversation_record(record);
        let terminal = record.get("type").and_then(Value::as_str) == Some("turn_ended");
        if !supported && !terminal {
            bail!(
                "unsupported Cursor transcript record in {} at record {}",
                path.display(),
                index + 1
            );
        }
    }

    // Cursor's transcript records have no per-record timestamps. File mtime
    // is the only available clock and is explicitly an estimate for import.
    let ts_ms = std::fs::metadata(path)
        .with_context(|| format!("read Cursor transcript metadata {}", path.display()))?
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0);
    let path_text = path.to_string_lossy().into_owned();
    let snapshot_text = snapshot.to_string_lossy().into_owned();
    // A previous turn's marker can remain at the end of the file while a
    // resumed turn is being appended. Do not use that status to close the new
    // turn unless no conversation records follow the latest marker.
    let final_terminal = records
        .iter()
        .rposition(|record| record.get("type").and_then(Value::as_str) == Some("turn_ended"))
        .filter(|marker| !records[*marker + 1..].iter().any(is_conversation_record))
        .map(|marker| &records[marker]);
    let boundary = |event: &str, through: Option<u64>| {
        let mut payload = json!({
            "session_id": session_id,
            "hook_event_name": event,
            "transcript_path": path_text,
            "source": "import",
            "start_time_estimated": true,
            "historical_fidelity": "user_and_assistant_text_and_terminal_status"
        });
        if event == IMPORT_STOP {
            if let Some(status) = final_terminal.and_then(|record| record.get("status")) {
                payload["status"] = status.clone();
            }
            if let Some(error) = final_terminal
                .and_then(|record| record.get("error"))
                .filter(|error| error.is_string())
            {
                payload["error_message"] = error.clone();
            }
        }
        if let Some(through) = through {
            payload["_bt_transcript_mirror"] = json!({
                "mirror": snapshot_text,
                "through": through
            });
        }
        envelope("cursor", None, &session_id, event, ts_ms, payload)
    };
    Ok(vec![
        boundary(IMPORT_START, None),
        boundary(IMPORT_CHECKPOINT, Some(through)),
        boundary(IMPORT_STOP, Some(through)),
    ])
}
