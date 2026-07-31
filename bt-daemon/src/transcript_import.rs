use crate::wire::Envelope;
use crate::ImportSource;
use anyhow::{bail, Context};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

pub(crate) fn resolve_transcript(
    session_id: &str,
    source: ImportSource,
) -> anyhow::Result<PathBuf> {
    validate_session_id(session_id)?;
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    let roots = match source {
        ImportSource::Codex => {
            let codex_home = std::env::var_os("CODEX_HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|| home.join(".codex"));
            vec![
                codex_home.join("sessions"),
                codex_home.join("archived_sessions"),
            ]
        }
        ImportSource::Claude => {
            let claude_home = std::env::var_os("CLAUDE_CONFIG_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| home.join(".claude"));
            vec![claude_home.join("projects")]
        }
    };
    resolve_transcript_in(session_id, source, &roots)
}

fn resolve_transcript_in(
    session_id: &str,
    source: ImportSource,
    roots: &[PathBuf],
) -> anyhow::Result<PathBuf> {
    validate_session_id(session_id)?;
    let expected = match source {
        ImportSource::Codex => format!("{session_id}.jsonl"),
        ImportSource::Claude => format!("{session_id}.jsonl"),
    };
    let mut matches = Vec::new();
    for root in roots {
        find_matching_files(root, &expected, source, &mut matches);
    }
    matches.sort();
    matches.dedup();
    match matches.as_slice() {
        [path] => Ok(path.clone()),
        [] => {
            let locations = roots
                .iter()
                .map(|root| root.display().to_string())
                .collect::<Vec<_>>()
                .join(", ");
            bail!(
                "no {} transcript found for session {session_id}; searched {locations}",
                source_name(source)
            )
        }
        paths => {
            let locations = paths
                .iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>()
                .join(", ");
            bail!(
                "multiple {} transcripts found for session {session_id}: {locations}",
                source_name(source)
            )
        }
    }
}

fn find_matching_files(
    directory: &Path,
    expected_suffix: &str,
    source: ImportSource,
    matches: &mut Vec<PathBuf>,
) {
    let Ok(entries) = std::fs::read_dir(directory) else {
        return;
    };
    for entry in entries.flatten() {
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if file_type.is_symlink() {
            continue;
        }
        let path = entry.path();
        if file_type.is_dir() {
            find_matching_files(&path, expected_suffix, source, matches);
            continue;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let is_match = match source {
            // Codex prefixes rollout files with their timestamp. Claude names
            // the main transcript exactly after the session id.
            ImportSource::Codex => name.ends_with(expected_suffix),
            ImportSource::Claude => name == expected_suffix,
        };
        if is_match {
            matches.push(path);
        }
    }
}

fn validate_session_id(session_id: &str) -> anyhow::Result<()> {
    if session_id.is_empty()
        || !session_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        bail!("invalid session id {session_id:?}");
    }
    Ok(())
}

fn source_name(source: ImportSource) -> &'static str {
    match source {
        ImportSource::Codex => "Codex",
        ImportSource::Claude => "Claude Code",
    }
}

pub(crate) fn transcript_envelopes(
    path: &Path,
    source: ImportSource,
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
    match source {
        ImportSource::Codex => codex_envelopes(path, &records),
        ImportSource::Claude => claude_envelopes(path, &records),
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
    let mut events = vec![envelope(
        "codex",
        source_version.clone(),
        &session_id,
        "SessionStart",
        start_ms,
        json!({
            "session_id": session_id,
            "hook_event_name": "SessionStart",
            "transcript_path": transcript_path,
            "source": "import",
            "_bt_import_through_ms": start_ms
        }),
    )];
    let mut checkpoints = records
        .iter()
        .filter(|record| {
            matches!(
                record.pointer("/payload/type").and_then(Value::as_str),
                Some("task_started" | "task_complete" | "turn_aborted")
            )
        })
        .filter_map(timestamp_ms)
        .collect::<Vec<_>>();
    checkpoints.sort_unstable();
    checkpoints.dedup();
    for checkpoint_ms in checkpoints {
        if checkpoint_ms <= start_ms || checkpoint_ms >= end_ms {
            continue;
        }
        events.push(envelope(
            "codex",
            source_version.clone(),
            &session_id,
            "ImportCheckpoint",
            checkpoint_ms,
            json!({
                "session_id": session_id,
                "hook_event_name": "ImportCheckpoint",
                "transcript_path": transcript_path,
                "_bt_import_through_ms": checkpoint_ms
            }),
        ));
    }
    events.push(envelope(
        "codex",
        source_version,
        &session_id,
        "Stop",
        end_ms,
        json!({
            "session_id": session_id,
            "hook_event_name": "Stop",
            "transcript_path": transcript_path,
            "last_agent_message": last_message,
            "_bt_import_through_ms": end_ms
        }),
    ));
    Ok(events)
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
            "source": "import"
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
            "reason": "transcript_import"
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
        .unwrap_or("imported-session")
        .to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_codex_rollout_by_session_suffix() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("sessions");
        let transcript = root
            .join("2026/07/31")
            .join("rollout-2026-07-31T12-00-00-session-123.jsonl");
        std::fs::create_dir_all(transcript.parent().unwrap()).unwrap();
        std::fs::write(&transcript, "{}\n").unwrap();

        assert_eq!(
            resolve_transcript_in("session-123", ImportSource::Codex, &[root]).unwrap(),
            transcript
        );
    }

    #[test]
    fn finds_only_exact_claude_session_filename() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("projects");
        let project = root.join("-tmp-project");
        std::fs::create_dir_all(&project).unwrap();
        std::fs::write(project.join("prefix-session-123.jsonl"), "{}\n").unwrap();
        let transcript = project.join("session-123.jsonl");
        std::fs::write(&transcript, "{}\n").unwrap();

        assert_eq!(
            resolve_transcript_in("session-123", ImportSource::Claude, &[root]).unwrap(),
            transcript
        );
    }

    #[test]
    fn rejects_unsafe_session_ids() {
        let error = resolve_transcript_in(
            "../session",
            ImportSource::Codex,
            &[PathBuf::from("unused")],
        )
        .unwrap_err();
        assert!(error.to_string().contains("invalid session id"));
    }

    #[test]
    fn reports_missing_and_ambiguous_sessions() {
        let temp = tempfile::tempdir().unwrap();
        let first = temp.path().join("first");
        let second = temp.path().join("second");
        std::fs::create_dir_all(&first).unwrap();
        std::fs::create_dir_all(&second).unwrap();

        let missing = resolve_transcript_in(
            "missing",
            ImportSource::Claude,
            &[first.clone(), second.clone()],
        )
        .unwrap_err();
        assert!(missing.to_string().contains("no Claude Code transcript"));

        std::fs::write(first.join("duplicate.jsonl"), "{}\n").unwrap();
        std::fs::write(second.join("duplicate.jsonl"), "{}\n").unwrap();
        let ambiguous =
            resolve_transcript_in("duplicate", ImportSource::Claude, &[first, second]).unwrap_err();
        assert!(ambiguous
            .to_string()
            .contains("multiple Claude Code transcripts"));
    }

    #[test]
    fn codex_import_adds_native_turn_checkpoints() {
        let records = vec![
            json!({"timestamp":"2026-01-01T00:00:01Z","type":"session_meta","payload":{"id":"session-123"}}),
            json!({"timestamp":"2026-01-01T00:00:02Z","type":"event_msg","payload":{"type":"task_started","turn_id":"turn-1"}}),
            json!({"timestamp":"2026-01-01T00:00:03Z","type":"event_msg","payload":{"type":"task_complete","turn_id":"turn-1"}}),
            json!({"timestamp":"2026-01-01T00:00:04Z","type":"event_msg","payload":{"type":"task_started","turn_id":"turn-2"}}),
            json!({"timestamp":"2026-01-01T00:00:05Z","type":"event_msg","payload":{"type":"task_complete","turn_id":"turn-2"}}),
        ];
        let events = codex_envelopes(Path::new("rollout.jsonl"), &records).unwrap();

        assert_eq!(events.first().unwrap().event, "SessionStart");
        assert_eq!(events.last().unwrap().event, "Stop");
        assert_eq!(
            events
                .iter()
                .filter(|event| event.event == "ImportCheckpoint")
                .count(),
            3
        );
    }
}
