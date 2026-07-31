use crate::wire::Envelope;
use crate::ReplaySource;
use anyhow::{bail, Context};
use serde_json::{json, Value};
use std::path::Path;

pub(crate) fn transcript_envelopes(
    path: &Path,
    source: Option<ReplaySource>,
) -> anyhow::Result<Vec<Envelope>> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("read transcript {}", path.display()))?;
    let records: Vec<Value> = contents
        .lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty())
        .map(|(index, line)| {
            serde_json::from_str(line)
                .with_context(|| format!("parse transcript {} line {}", path.display(), index + 1))
        })
        .collect::<anyhow::Result<_>>()?;
    if records.is_empty() {
        bail!("transcript {} is empty", path.display());
    }
    match source.unwrap_or_else(|| detect_source(&records)) {
        ReplaySource::Codex => codex_envelopes(path, &records),
        ReplaySource::Claude => claude_envelopes(path, &records),
    }
}

fn detect_source(records: &[Value]) -> ReplaySource {
    if records.iter().any(|record| {
        matches!(
            record.get("type").and_then(Value::as_str),
            Some("session_meta" | "turn_context" | "event_msg" | "response_item" | "compacted")
        ) && record.get("payload").is_some()
    }) {
        ReplaySource::Codex
    } else {
        ReplaySource::Claude
    }
}

fn codex_envelopes(path: &Path, records: &[Value]) -> anyhow::Result<Vec<Envelope>> {
    let meta = records
        .iter()
        .find(|record| record.get("type").and_then(Value::as_str) == Some("session_meta"));
    let session_id = meta
        .and_then(|record| record.pointer("/payload/id"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| file_session_id(path));
    let source_version = meta
        .and_then(|record| record.pointer("/payload/cli_version"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let (start_ms, end_ms) = timestamp_bounds(records);
    let transcript_path = path.to_string_lossy();
    let last_message = records.iter().rev().find_map(|record| {
        (record.pointer("/payload/type").and_then(Value::as_str) == Some("task_complete"))
            .then(|| record.pointer("/payload/last_agent_message").cloned())
            .flatten()
    });
    Ok(vec![
        envelope(
            "codex",
            source_version.clone(),
            &session_id,
            "SessionStart",
            start_ms,
            json!({
                "session_id": session_id,
                "hook_event_name": "SessionStart",
                "transcript_path": transcript_path,
                "source": "replay"
            }),
        ),
        envelope(
            "codex",
            source_version,
            &session_id,
            "Stop",
            end_ms,
            json!({
                "session_id": session_id,
                "hook_event_name": "Stop",
                "transcript_path": transcript_path,
                "last_agent_message": last_message
            }),
        ),
    ])
}

fn claude_envelopes(path: &Path, records: &[Value]) -> anyhow::Result<Vec<Envelope>> {
    let session_id = records
        .iter()
        .find_map(|record| string_at(record, "/sessionId"))
        .unwrap_or_else(|| file_session_id(path));
    let source_version = records
        .iter()
        .find_map(|record| string_at(record, "/version"));
    let cwd = records.iter().find_map(|record| string_at(record, "/cwd"));
    let (start_ms, end_ms) = timestamp_bounds(records);
    let transcript_path = path.to_string_lossy().into_owned();
    let mut events = vec![envelope(
        "claude-code",
        source_version.clone(),
        &session_id,
        "SessionStart",
        start_ms.saturating_sub(1),
        json!({
            "session_id": session_id,
            "hook_event_name": "SessionStart",
            "transcript_path": transcript_path,
            "cwd": cwd,
            "source": "replay"
        }),
    )];

    let user_indexes: Vec<usize> = records
        .iter()
        .enumerate()
        .filter_map(|(index, record)| is_real_user(record).then_some(index))
        .collect();
    for (turn, &index) in user_indexes.iter().enumerate() {
        let next = user_indexes.get(turn + 1).copied().unwrap_or(records.len());
        let segment = &records[index..next];
        let turn_start = timestamp_ms(&records[index]).unwrap_or(start_ms);
        let turn_end = segment
            .iter()
            .filter_map(timestamp_ms)
            .max()
            .unwrap_or(turn_start);
        let prompt = records[index]
            .pointer("/message/content")
            .cloned()
            .unwrap_or(Value::Null);
        events.push(envelope(
            "claude-code",
            source_version.clone(),
            &session_id,
            "UserPromptSubmit",
            turn_start,
            json!({
                "session_id": session_id,
                "hook_event_name": "UserPromptSubmit",
                "transcript_path": transcript_path,
                "cwd": cwd,
                "prompt": prompt
            }),
        ));
        events.push(envelope(
            "claude-code",
            source_version.clone(),
            &session_id,
            "Stop",
            turn_end,
            json!({
                "session_id": session_id,
                "hook_event_name": "Stop",
                "transcript_path": transcript_path,
                "cwd": cwd,
                "last_assistant_message": last_assistant_text(segment)
            }),
        ));
    }
    events.push(envelope(
        "claude-code",
        source_version,
        &session_id,
        "SessionEnd",
        end_ms.saturating_add(1),
        json!({
            "session_id": session_id,
            "hook_event_name": "SessionEnd",
            "transcript_path": transcript_path,
            "cwd": cwd,
            "reason": "transcript_replay"
        }),
    ));
    Ok(events)
}

fn envelope(
    source: &str,
    source_version: Option<String>,
    session_id: &str,
    event: &str,
    ts_ms: i64,
    payload: Value,
) -> Envelope {
    Envelope {
        source: source.into(),
        source_version,
        session_id: session_id.into(),
        event: event.into(),
        ts_ms,
        payload,
        config: None,
    }
}

fn is_real_user(record: &Value) -> bool {
    if record.get("type").and_then(Value::as_str) != Some("user") {
        return false;
    }
    !record
        .pointer("/message/content")
        .and_then(Value::as_array)
        .is_some_and(|blocks| {
            blocks
                .iter()
                .any(|block| block.get("type").and_then(Value::as_str) == Some("tool_result"))
        })
}

fn last_assistant_text(records: &[Value]) -> Option<Value> {
    records.iter().rev().find_map(|record| {
        if record.get("type").and_then(Value::as_str) != Some("assistant") {
            return None;
        }
        let content = record.pointer("/message/content")?;
        if let Some(text) = content.as_str() {
            return Some(json!(text));
        }
        let text = content
            .as_array()?
            .iter()
            .filter_map(|block| {
                (block.get("type").and_then(Value::as_str) == Some("text"))
                    .then(|| block.get("text").and_then(Value::as_str))
                    .flatten()
            })
            .collect::<Vec<_>>()
            .join("\n");
        (!text.is_empty()).then(|| json!(text))
    })
}

fn timestamp_bounds(records: &[Value]) -> (i64, i64) {
    let mut timestamps = records.iter().filter_map(timestamp_ms);
    let Some(first) = timestamps.next() else {
        return (0, 0);
    };
    timestamps.fold((first, first), |(min, max), value| {
        (min.min(value), max.max(value))
    })
}

fn timestamp_ms(record: &Value) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(record.get("timestamp")?.as_str()?)
        .ok()
        .map(|timestamp| timestamp.timestamp_millis())
}

fn string_at(record: &Value, pointer: &str) -> Option<String> {
    record.pointer(pointer)?.as_str().map(str::to_owned)
}

fn file_session_id(path: &Path) -> String {
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .filter(|stem| !stem.is_empty())
        .unwrap_or("replayed-session")
        .to_owned()
}
