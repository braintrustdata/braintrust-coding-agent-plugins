use super::{envelope, validate_session_id};
use crate::wire::Envelope;
use anyhow::bail;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

const TRANSCRIPT_NAME: &str = "transcript_full.jsonl";

#[derive(Default)]
pub(super) struct Tail {
    started: bool,
    reported_tools: HashSet<i64>,
    last_len: u64,
}

impl Tail {
    pub(super) fn poll(
        &mut self,
        events: Vec<Envelope>,
        len: u64,
        finalize: bool,
    ) -> anyhow::Result<Vec<Envelope>> {
        if events.len() < 3 {
            bail!("Antigravity import did not produce session boundary events");
        }
        let mut out = Vec::new();
        if !self.started {
            out.push(events[0].clone());
            self.started = true;
        } else if len != self.last_len {
            let mut checkpoint = events[0].clone();
            checkpoint.event = "ImportCheckpoint".into();
            out.push(checkpoint);
        }
        let end = events.len() - 2;
        for event in &events[1..end] {
            let step = event.payload.get("stepIdx").and_then(Value::as_i64);
            if step.is_some_and(|step| self.reported_tools.insert(step)) {
                out.push(event.clone());
            }
        }
        if finalize {
            out.extend_from_slice(&events[end..]);
        }
        self.last_len = len;
        Ok(out)
    }
}

pub(super) fn roots(home: &Path) -> Vec<PathBuf> {
    vec![home.join(".gemini/antigravity-cli/brain")]
}

pub(super) fn transcript_session_id(path: &Path) -> Option<String> {
    if path.file_name().and_then(|name| name.to_str()) != Some(TRANSCRIPT_NAME) {
        return None;
    }
    let session_id = path
        .parent()? // logs
        .parent()? // .system_generated
        .parent()? // conversation directory
        .file_name()?
        .to_str()?;
    validate_session_id(session_id).ok()?;
    is_transcript_path(path, session_id).then(|| session_id.to_owned())
}

pub(super) fn filename_matches(path: &Path, session_id: &str) -> bool {
    validate_session_id(session_id).is_ok() && is_transcript_path(path, session_id)
}

fn is_transcript_path(path: &Path, session_id: &str) -> bool {
    path.file_name().and_then(|name| name.to_str()) == Some(TRANSCRIPT_NAME)
        && path
            .parent()
            .and_then(Path::parent)
            .and_then(Path::parent)
            .and_then(Path::file_name)
            .and_then(|name| name.to_str())
            == Some(session_id)
}

pub(super) fn envelopes(path: &Path, records: &[Value]) -> anyhow::Result<Vec<Envelope>> {
    let session_id = transcript_session_id(path)
        .ok_or_else(|| anyhow::anyhow!("invalid Antigravity transcript path {}", path.display()))?;
    if records.is_empty() {
        bail!("Antigravity transcript {} is empty", path.display());
    }
    let (start_ms, end_ms) = timestamp_bounds_created(records);
    let transcript_path = path.to_string_lossy();
    let mut events = vec![envelope(
        "antigravity",
        None,
        &session_id,
        "PreInvocation",
        start_ms,
        json!({"conversationId": session_id, "invocationNum": 0, "initialNumSteps": 0,
            "transcriptPath": transcript_path}),
    )];
    for record in records.iter().filter(|record| is_tool_result(record)) {
        let Some(step) = record
            .get("step_index")
            .or_else(|| record.get("stepIndex"))
            .and_then(Value::as_i64)
        else {
            continue;
        };
        events.push(envelope(
            "antigravity", None, &session_id, "PostToolUse", created_at_ms(record).unwrap_or(end_ms),
            json!({"conversationId": session_id, "stepIdx": step, "transcriptPath": transcript_path}),
        ));
    }
    events.push(envelope(
        "antigravity",
        None,
        &session_id,
        "PostInvocation",
        end_ms,
        json!({"conversationId": session_id, "invocationNum": 0, "initialNumSteps": 0,
            "transcriptPath": transcript_path}),
    ));
    events.push(envelope(
        "antigravity", None, &session_id, "Stop", end_ms.saturating_add(1),
        json!({"conversationId": session_id, "fullyIdle": true, "terminationReason": "transcript_import",
            "transcriptPath": transcript_path}),
    ));
    Ok(events)
}

fn is_tool_result(record: &Value) -> bool {
    !matches!(
        record.get("type").and_then(Value::as_str),
        Some("USER_INPUT" | "PLANNER_RESPONSE" | "CONVERSATION_HISTORY" | "CHECKPOINT")
    ) && record
        .get("step_index")
        .or_else(|| record.get("stepIndex"))
        .is_some()
}

fn timestamp_bounds_created(records: &[Value]) -> (i64, i64) {
    let timestamps = records.iter().filter_map(created_at_ms).collect::<Vec<_>>();
    timestamps
        .iter()
        .copied()
        .fold((0, 0), |(min, max), value| {
            if min == 0 {
                (value, value)
            } else {
                (min.min(value), max.max(value))
            }
        })
}

fn created_at_ms(record: &Value) -> Option<i64> {
    record
        .get("created_at")
        .or_else(|| record.get("createdAt"))
        .and_then(Value::as_str)
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.timestamp_millis())
}
