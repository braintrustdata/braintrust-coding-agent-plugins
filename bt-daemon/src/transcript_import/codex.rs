use super::{
    envelope, file_session_id, find_jsonl_files, read_jsonl_records, string_at, timestamp_bounds,
    timestamp_ms, validate_session_id,
};
use crate::wire::Envelope;
use anyhow::bail;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::io::BufRead;
use std::path::{Path, PathBuf};

#[derive(Default)]
pub(super) struct Tail {
    started: bool,
    checkpoints: usize,
    last_len: u64,
}

impl Tail {
    pub(super) fn poll(
        &mut self,
        events: Vec<Envelope>,
        len: u64,
        finalize: bool,
    ) -> anyhow::Result<Vec<Envelope>> {
        if events.len() < 2 {
            bail!("Codex import did not produce session boundary events");
        }
        let mut out = Vec::new();
        if !self.started {
            out.push(events[0].clone());
            self.started = true;
        }
        let middle = &events[1..events.len() - 1];
        let checkpoints = middle
            .iter()
            .filter(|event| event.event == "ImportCheckpoint")
            .collect::<Vec<_>>();
        out.extend(
            checkpoints
                .iter()
                .skip(self.checkpoints)
                .map(|event| (*event).clone()),
        );
        self.checkpoints = checkpoints.len();
        let mut tail = events.last().cloned().unwrap();
        if finalize {
            out.extend(
                middle
                    .iter()
                    .filter(|event| event.event != "ImportCheckpoint")
                    .cloned(),
            );
            out.push(tail);
        } else if len != self.last_len {
            tail.event = "ImportCheckpoint".into();
            if let Some(payload) = tail.payload.as_object_mut() {
                payload.insert("hook_event_name".into(), json!("ImportCheckpoint"));
            }
            out.push(tail);
        }
        self.last_len = len;
        Ok(out)
    }
}

pub(super) fn transcript_session_id(path: &Path) -> Option<String> {
    transcript_session_id_with_subagents(path, false)
}

fn transcript_session_id_with_subagents(path: &Path, include_subagents: bool) -> Option<String> {
    let file = std::fs::File::open(path).ok()?;
    for line in std::io::BufReader::new(file).lines().map_while(Result::ok) {
        let Ok(record) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        if record.get("type").and_then(Value::as_str) != Some("session_meta") {
            continue;
        }
        if !include_subagents && record.pointer("/payload/source/subagent").is_some() {
            return None;
        }
        let session_id = record.pointer("/payload/id").and_then(Value::as_str)?;
        if validate_session_id(session_id).is_err() {
            return None;
        }
        return filename_matches(path, session_id).then(|| session_id.to_owned());
    }
    None
}

pub(super) fn roots(home: &Path) -> Vec<PathBuf> {
    let codex_home = std::env::var_os("CODEX_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".codex"));
    vec![
        codex_home.join("sessions"),
        codex_home.join("archived_sessions"),
    ]
}

pub(super) fn filename_matches(path: &Path, session_id: &str) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.ends_with(&format!("{session_id}.jsonl")))
}

pub(super) fn envelopes(path: &Path, records: &[Value]) -> anyhow::Result<Vec<Envelope>> {
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
    let last_message = last_message(records);
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
    let mut visited = HashSet::new();
    visited.insert(session_id.clone());
    append_subagent_events(
        &mut events,
        path,
        records,
        None,
        &session_id,
        source_version.clone(),
        &mut visited,
    )?;
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

fn append_subagent_events(
    events: &mut Vec<Envelope>,
    parent_path: &Path,
    parent_records: &[Value],
    parent_agent_id: Option<&str>,
    root_session_id: &str,
    source_version: Option<String>,
    visited: &mut HashSet<String>,
) -> anyhow::Result<()> {
    let calls = spawn_calls(parent_records);
    if calls.is_empty() {
        return Ok(());
    }
    let search_root = transcript_search_root(parent_path);
    for call in calls {
        if !visited.insert(call.agent_id.clone()) {
            continue;
        }
        let Some(child_path) = find_transcript_by_id(&search_root, &call.agent_id)? else {
            continue;
        };
        let child_records = read_jsonl_records(&child_path)?;
        let (child_start, child_end) = timestamp_bounds(&child_records);
        let parent_transcript_path = parent_path.to_string_lossy().into_owned();
        let child_transcript_path = child_path.to_string_lossy().into_owned();
        let mut post_payload = json!({
            "session_id": root_session_id,
            "hook_event_name": "PostToolUse",
            "transcript_path": parent_transcript_path,
            "tool_name": "spawn_agent",
            "tool_use_id": call.call_id,
            "tool_response": { "agent_id": call.agent_id },
            "_bt_import_through_ms": call.result_ms
        });
        if let (Some(parent_agent_id), Some(payload)) =
            (parent_agent_id, post_payload.as_object_mut())
        {
            payload.insert("agent_id".into(), json!(parent_agent_id));
        }
        events.push(envelope(
            "codex",
            source_version.clone(),
            root_session_id,
            "PostToolUse",
            call.result_ms,
            post_payload,
        ));
        events.push(envelope(
            "codex",
            source_version.clone(),
            root_session_id,
            "SubagentStart",
            child_start.min(call.start_ms),
            json!({
                "session_id": root_session_id,
                "hook_event_name": "SubagentStart",
                "agent_id": call.agent_id,
                "agent_type": call.agent_type,
                "transcript_path": child_transcript_path,
                "_bt_import_through_ms": child_start
            }),
        ));
        append_subagent_events(
            events,
            &child_path,
            &child_records,
            Some(&call.agent_id),
            root_session_id,
            source_version.clone(),
            visited,
        )?;
        events.push(envelope(
            "codex",
            source_version.clone(),
            root_session_id,
            "SubagentStop",
            child_end.max(call.result_ms),
            json!({
                "session_id": root_session_id,
                "hook_event_name": "SubagentStop",
                "agent_id": call.agent_id,
                "agent_transcript_path": child_transcript_path,
                "last_agent_message": last_message(&child_records),
                "_bt_import_through_ms": child_end
            }),
        ));
    }
    Ok(())
}

struct SpawnCall {
    call_id: String,
    agent_id: String,
    agent_type: Option<String>,
    start_ms: i64,
    result_ms: i64,
}

fn spawn_calls(records: &[Value]) -> Vec<SpawnCall> {
    let mut calls = HashMap::<String, (Option<String>, i64)>::new();
    let mut spawned = Vec::new();
    for record in records {
        let Some(payload) = record.get("payload") else {
            continue;
        };
        let Some(call_id) = payload.get("call_id").and_then(Value::as_str) else {
            continue;
        };
        if payload.get("type").and_then(Value::as_str) == Some("function_call")
            && payload.get("name").and_then(Value::as_str) == Some("spawn_agent")
        {
            let arguments = payload
                .get("arguments")
                .and_then(Value::as_str)
                .and_then(|value| serde_json::from_str::<Value>(value).ok());
            let agent_type = arguments
                .as_ref()
                .and_then(|value| string_at(value, "/agent_type"));
            calls.insert(
                call_id.to_owned(),
                (agent_type, timestamp_ms(record).unwrap_or(0)),
            );
            continue;
        }
        if payload.get("type").and_then(Value::as_str) != Some("function_call_output") {
            continue;
        }
        let Some((agent_type, start_ms)) = calls.get(call_id).cloned() else {
            continue;
        };
        let output = payload
            .get("output")
            .and_then(Value::as_str)
            .and_then(|value| serde_json::from_str::<Value>(value).ok());
        let Some(agent_id) = output
            .as_ref()
            .and_then(|value| string_at(value, "/agent_id"))
        else {
            continue;
        };
        spawned.push(SpawnCall {
            call_id: call_id.to_owned(),
            agent_id,
            agent_type,
            start_ms,
            result_ms: timestamp_ms(record).unwrap_or(start_ms),
        });
    }
    spawned
}

fn transcript_search_root(path: &Path) -> PathBuf {
    path.ancestors()
        .find(|ancestor| {
            matches!(
                ancestor.file_name().and_then(|name| name.to_str()),
                Some("sessions" | "archived_sessions")
            )
        })
        .map(Path::to_path_buf)
        .or_else(|| path.parent().map(Path::to_path_buf))
        .unwrap_or_else(|| PathBuf::from("."))
}

fn find_transcript_by_id(root: &Path, session_id: &str) -> anyhow::Result<Option<PathBuf>> {
    let mut candidates = Vec::new();
    find_jsonl_files(root, &mut candidates);
    candidates.sort();
    let matches = candidates
        .into_iter()
        .filter(|path| {
            transcript_session_id_with_subagents(path, true).as_deref() == Some(session_id)
        })
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [] => Ok(None),
        [path] => Ok(Some(path.clone())),
        paths => {
            let locations = paths
                .iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>()
                .join(", ");
            bail!("multiple Codex transcripts found for subagent {session_id}: {locations}")
        }
    }
}

fn last_message(records: &[Value]) -> Option<Value> {
    records.iter().rev().find_map(|record| {
        (record.pointer("/payload/type").and_then(Value::as_str) == Some("task_complete"))
            .then(|| record.pointer("/payload/last_agent_message").cloned())
            .flatten()
    })
}
