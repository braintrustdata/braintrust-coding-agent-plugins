use super::{envelope, validate_session_id};
use crate::wire::Envelope;
use anyhow::bail;
use serde_json::{json, Value};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};

const TRANSCRIPT_NAME: &str = "transcript_full.jsonl";

#[derive(Default)]
pub(super) struct Tail {
    emitted: usize,
    stopped: bool,
}

impl Tail {
    pub(super) fn poll(
        &mut self,
        events: Vec<Envelope>,
        _len: u64,
        finalize: bool,
    ) -> anyhow::Result<Vec<Envelope>> {
        let Some((stop, active)) = events.split_last() else {
            bail!("Antigravity import did not produce a session boundary event");
        };
        if self.emitted > active.len() {
            bail!("Antigravity import event stream shrank without a reset");
        }
        let mut out = active[self.emitted..].to_vec();
        self.emitted = active.len();
        if finalize && !self.stopped {
            out.push(stop.clone());
            self.stopped = true;
        }
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

pub(super) fn envelopes(
    path: &Path,
    records: &[Value],
    record_end_offsets: &[u64],
) -> anyhow::Result<Vec<Envelope>> {
    if records.len() != record_end_offsets.len() {
        bail!("Antigravity transcript record offsets do not match parsed records");
    }
    let session_id = transcript_session_id(path)
        .ok_or_else(|| anyhow::anyhow!("invalid Antigravity transcript path {}", path.display()))?;
    if records.is_empty() {
        bail!("Antigravity transcript {} is empty", path.display());
    }
    let (start_ms, end_ms) = timestamp_bounds_created(records);
    let transcript_path = path.to_string_lossy().into_owned();
    let mut events = Vec::new();
    let mut planned_calls = VecDeque::new();
    let mut invocation_num = 0i64;
    for (index, record) in records.iter().enumerate() {
        let record_type = record.get("type").and_then(Value::as_str);
        let step = record
            .get("step_index")
            .or_else(|| record.get("stepIndex"))
            .and_then(Value::as_i64)
            .unwrap_or(index as i64);
        let ts = created_at_ms(record).unwrap_or(start_ms);
        if record_type == Some("PLANNER_RESPONSE") {
            let before = index
                .checked_sub(1)
                .and_then(|index| record_end_offsets.get(index))
                .copied()
                .unwrap_or(0);
            events.push(envelope(
                "antigravity",
                None,
                &session_id,
                "PreInvocation",
                ts,
                bounded_payload(
                    &session_id,
                    &transcript_path,
                    before,
                    json!({"invocationNum": invocation_num, "initialNumSteps": step}),
                ),
            ));
            events.push(envelope(
                "antigravity",
                None,
                &session_id,
                "PostInvocation",
                ts,
                bounded_payload(
                    &session_id,
                    &transcript_path,
                    record_end_offsets[index],
                    json!({"invocationNum": invocation_num, "initialNumSteps": step}),
                ),
            ));
            invocation_num += 1;
        }
        if let Some(calls) = record
            .get("tool_calls")
            .or_else(|| record.get("toolCalls"))
            .and_then(Value::as_array)
        {
            planned_calls.extend(calls.iter().cloned());
        }
        if !is_tool_result(record) {
            continue;
        }
        let mut extra = json!({"stepIdx": step});
        if let Some(call) = planned_calls.pop_front() {
            extra["toolCall"] = call;
        }
        if is_denied(record) {
            extra["toolApproval"] = json!("denied");
        }
        events.push(envelope(
            "antigravity",
            None,
            &session_id,
            "PostToolUse",
            ts,
            bounded_payload(
                &session_id,
                &transcript_path,
                record_end_offsets[index],
                extra,
            ),
        ));
    }
    events.push(envelope(
        "antigravity",
        None,
        &session_id,
        "Stop",
        end_ms.saturating_add(1),
        bounded_payload(
            &session_id,
            &transcript_path,
            record_end_offsets.last().copied().unwrap_or(0),
            json!({"fullyIdle": true, "terminationReason": "transcript_import"}),
        ),
    ));
    Ok(events)
}

fn bounded_payload(session_id: &str, transcript_path: &str, through: u64, extra: Value) -> Value {
    let mut payload = json!({
        "conversationId": session_id,
        "transcriptPath": transcript_path,
        "_bt_transcript_observation": {
            "path": transcript_path,
            "observed_bytes": through,
        }
    });
    if let (Value::Object(payload), Value::Object(extra)) = (&mut payload, extra) {
        payload.extend(extra);
    }
    payload
}

fn is_denied(record: &Value) -> bool {
    record
        .get("content")
        .and_then(Value::as_str)
        .is_some_and(|content| content.to_ascii_lowercase().contains("denied"))
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
