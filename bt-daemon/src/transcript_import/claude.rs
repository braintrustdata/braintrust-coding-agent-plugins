use super::{
    envelope, file_session_id, find_jsonl_files, read_complete_jsonl_records, read_jsonl_records,
    string_at, timestamp_bounds, timestamp_ms, validate_session_id,
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
    completed_turns: usize,
    active_turn: Option<usize>,
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
            bail!("Claude import did not produce session boundary events");
        }
        let mut out = Vec::new();
        if !self.started {
            out.push(events[0].clone());
            self.started = true;
        }
        let middle = &events[1..events.len() - 1];
        let turn_starts = middle
            .iter()
            .enumerate()
            .filter_map(|(index, event)| (event.event == "UserPromptSubmit").then_some(index))
            .collect::<Vec<_>>();
        let turn_count = turn_starts.len();
        let completed_target = if finalize {
            turn_count
        } else {
            turn_count.saturating_sub(1)
        };
        while self.completed_turns < completed_target {
            let turn = self.completed_turns;
            let start = turn_starts[turn];
            let end = turn_starts.get(turn + 1).copied().unwrap_or(middle.len());
            let skip = usize::from(self.active_turn == Some(turn));
            out.extend(middle[start + skip..end].iter().cloned());
            self.completed_turns += 1;
            self.active_turn = None;
        }
        if !finalize && turn_count > 0 {
            let active = turn_count - 1;
            if self.active_turn != Some(active) {
                out.push(middle[turn_starts[active]].clone());
                self.active_turn = Some(active);
            }
            if len != self.last_len {
                let mut checkpoint = events.last().cloned().unwrap();
                checkpoint.event = "ImportCheckpoint".into();
                if let Some(payload) = checkpoint.payload.as_object_mut() {
                    payload.insert("hook_event_name".into(), json!("ImportCheckpoint"));
                }
                out.push(checkpoint);
            }
        }
        if finalize {
            out.push(events.last().cloned().unwrap());
        }
        self.last_len = len;
        Ok(out)
    }
}

pub(super) fn transcript_session_id(path: &Path) -> Option<String> {
    let file = std::fs::File::open(path).ok()?;
    for line in std::io::BufReader::new(file).lines().map_while(Result::ok) {
        let Ok(record) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let Some(session_id) = record.get("sessionId").and_then(Value::as_str) else {
            continue;
        };
        if validate_session_id(session_id).is_err() {
            return None;
        }
        return filename_matches(path, session_id).then(|| session_id.to_owned());
    }
    None
}

pub(super) fn roots(home: &Path) -> Vec<PathBuf> {
    let claude_home = std::env::var_os("CLAUDE_CONFIG_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".claude"));
    vec![claude_home.join("projects")]
}

pub(super) fn filename_matches(path: &Path, session_id: &str) -> bool {
    path.file_name().and_then(|name| name.to_str()) == Some(&format!("{session_id}.jsonl"))
}

pub(super) fn envelopes(
    path: &Path,
    records: &[Value],
    record_end_offsets: &[u64],
    transcript_len: u64,
) -> anyhow::Result<Vec<Envelope>> {
    if records.len() != record_end_offsets.len() {
        bail!("Claude transcript record offsets do not match parsed records");
    }
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
    let subagents = subagents(path, records)?;
    let mut events = vec![import_envelope(
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
        0,
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
        // Claude can append queue bookkeeping ahead of older conversation
        // records. Only message rows define the native turn's duration.
        let turn_end = segment
            .iter()
            .filter(|record| {
                matches!(
                    record.get("type").and_then(Value::as_str),
                    Some("user" | "assistant")
                )
            })
            .filter_map(timestamp_ms)
            .max()
            .unwrap_or(turn_start);
        let turn_cwd = segment
            .iter()
            .find_map(|record| string_at(record, "/cwd"))
            .or_else(|| cwd.clone());
        let prompt = records[index]
            .pointer("/message/content")
            .cloned()
            .unwrap_or(Value::Null);
        events.push(import_envelope(
            source_version.clone(),
            &session_id,
            "UserPromptSubmit",
            turn_start,
            json!({
                "session_id": session_id,
                "hook_event_name": "UserPromptSubmit",
                "transcript_path": transcript_path,
                "cwd": turn_cwd,
                "prompt": prompt
            }),
            record_end_offsets[index],
        ));
        for subagent in subagents
            .iter()
            .filter(|subagent| subagent.record_index >= index && subagent.record_index < next)
        {
            events.push(import_envelope(
                source_version.clone(),
                &session_id,
                "SubagentStart",
                subagent.start_ms,
                json!({
                    "session_id": session_id,
                    "hook_event_name": "SubagentStart",
                    "transcript_path": transcript_path,
                    "agent_id": subagent.agent_id,
                    "agent_type": subagent.agent_type,
                    "agent_transcript_path": subagent.path
                }),
                record_end_offsets[subagent.start_index],
            ));
            events.push(import_envelope(
                source_version.clone(),
                &session_id,
                "SubagentStop",
                subagent.end_ms,
                json!({
                    "session_id": session_id,
                    "hook_event_name": "SubagentStop",
                    "transcript_path": transcript_path,
                    "agent_id": subagent.agent_id,
                    "agent_type": subagent.agent_type,
                    "agent_transcript_path": subagent.path,
                    "last_assistant_message": subagent.last_assistant_message
                }),
                record_end_offsets[subagent.record_index],
            ));
        }
        let error = last_assistant_error(segment);
        let stop_event = if error.is_some() {
            "StopFailure"
        } else {
            "Stop"
        };
        events.push(import_envelope(
            source_version.clone(),
            &session_id,
            stop_event,
            turn_end,
            json!({
                "session_id": session_id,
                "hook_event_name": stop_event,
                "transcript_path": transcript_path,
                "cwd": turn_cwd,
                "last_assistant_message": last_assistant_text(segment),
                "error": error
            }),
            record_end_offsets[next.saturating_sub(1)],
        ));
    }
    events.push(import_envelope(
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
        transcript_len,
    ));
    Ok(events)
}

struct Subagent {
    agent_id: String,
    agent_type: Option<String>,
    path: String,
    start_index: usize,
    record_index: usize,
    start_ms: i64,
    end_ms: i64,
    last_assistant_message: Option<Value>,
}

fn subagents(path: &Path, records: &[Value]) -> anyhow::Result<Vec<Subagent>> {
    let Some(stem) = path.file_stem() else {
        return Ok(Vec::new());
    };
    let directory = path.with_file_name(stem).join("subagents");
    let mut paths = Vec::new();
    find_jsonl_files(&directory, &mut paths);
    paths.sort();

    let mut result_records = HashMap::<String, (usize, Option<String>)>::new();
    let mut calls = HashMap::<String, (usize, Option<String>, Option<String>)>::new();
    for (index, record) in records.iter().enumerate() {
        if let Some(agent_id) = string_at(record, "/toolUseResult/agentId") {
            let call_id = record
                .pointer("/message/content")
                .and_then(Value::as_array)
                .and_then(|blocks| {
                    blocks.iter().find_map(|block| {
                        (block.get("type").and_then(Value::as_str) == Some("tool_result"))
                            .then(|| string_at(block, "/tool_use_id"))
                            .flatten()
                    })
                });
            result_records.insert(agent_id, (index, call_id));
        }
        let Some(blocks) = record.pointer("/message/content").and_then(Value::as_array) else {
            continue;
        };
        for block in blocks {
            if block.get("type").and_then(Value::as_str) != Some("tool_use")
                || block.get("name").and_then(Value::as_str) != Some("Agent")
            {
                continue;
            }
            let Some(call_id) = string_at(block, "/id") else {
                continue;
            };
            calls.insert(
                call_id,
                (
                    index,
                    string_at(block, "/input/subagent_type"),
                    string_at(block, "/input/prompt"),
                ),
            );
        }
    }

    let completed_calls = result_records
        .values()
        .filter_map(|(_, call_id)| call_id.as_deref())
        .collect::<HashSet<_>>();
    let mut unmatched_calls = calls
        .iter()
        .filter(|(call_id, _)| !completed_calls.contains(call_id.as_str()))
        .map(|(call_id, (index, agent_type, prompt))| {
            (
                call_id.clone(),
                *index,
                timestamp_ms(&records[*index]).unwrap_or_default(),
                agent_type.clone(),
                prompt.clone(),
            )
        })
        .collect::<Vec<_>>();
    unmatched_calls.sort_by_key(|(_, index, _, _, _)| *index);

    let mut children = Vec::new();
    for path in paths {
        let Some(agent_id) = path
            .file_stem()
            .and_then(|name| name.to_str())
            .and_then(|name| name.strip_prefix("agent-"))
            .map(str::to_owned)
        else {
            continue;
        };
        let records = if result_records.contains_key(&agent_id) {
            read_jsonl_records(&path)?
        } else {
            read_complete_jsonl_records(&path)?
        };
        let start_ms = timestamp_bounds(&records).0;
        children.push((path, agent_id, records, start_ms));
    }
    children.sort_by(|left, right| left.3.cmp(&right.3).then_with(|| left.0.cmp(&right.0)));

    let subagents = children
        .into_iter()
        .filter_map(|(subagent_path, agent_id, subagent_records, child_start)| {
            // A completed tool result is the strongest parent/child link. If
            // the session was interrupted first, Claude still leaves both the
            // Agent call and a sidechain whose initial prompt matches that call.
            // Files with neither anchor are auxiliary or stale and must not
            // become synthetic `subagent: agent` outputs on the final turn.
            let (record_index, call_id) = if let Some(link) = result_records.get(&agent_id) {
                link.clone()
            } else {
                let child_prompt = subagent_records.iter().find_map(|record| {
                    (record.get("isSidechain").and_then(Value::as_bool) == Some(true)
                        && string_at(record, "/agentId").as_deref() == Some(&agent_id))
                    .then(|| string_at(record, "/message/content"))
                    .flatten()
                })?;
                let child_type = subagent_records
                    .iter()
                    .find_map(|record| string_at(record, "/attributionAgent"));
                let call_index = unmatched_calls
                    .iter()
                    .enumerate()
                    .filter(|(_, (_, _, call_start, agent_type, prompt))| {
                        prompt.as_deref() == Some(child_prompt.as_str())
                            && *call_start <= child_start
                            && match (agent_type.as_deref(), child_type.as_deref()) {
                                (Some(expected), Some(actual)) => expected == actual,
                                _ => true,
                            }
                    })
                    .min_by_key(|(_, (_, _, call_start, _, _))| child_start - *call_start)
                    .map(|(index, _)| index)?;
                let (call_id, index, _, _, _) = unmatched_calls.remove(call_index);
                (index, Some(call_id))
            };
            let (start_index, agent_type) = call_id
                .as_ref()
                .and_then(|call_id| calls.get(call_id))
                .map(|(index, agent_type, _)| (*index, agent_type.clone()))
                .unwrap_or((record_index, None));
            let (_, child_end) = timestamp_bounds(&subagent_records);
            let start_ms = timestamp_ms(&records[start_index])
                .unwrap_or(child_start)
                .min(child_start);
            let end_ms = timestamp_ms(&records[record_index])
                .unwrap_or(child_end)
                .max(child_end);
            Some(Subagent {
                agent_id,
                agent_type,
                path: subagent_path.to_string_lossy().into_owned(),
                start_index,
                record_index,
                start_ms,
                end_ms,
                last_assistant_message: last_assistant_text(&subagent_records),
            })
        })
        .collect::<Vec<_>>();
    Ok(subagents)
}

fn import_envelope(
    source_version: Option<String>,
    session_id: &str,
    event: &str,
    ts_ms: i64,
    mut payload: Value,
    through_offset: u64,
) -> Envelope {
    if let Some(payload) = payload.as_object_mut() {
        payload.insert("_bt_import_through_offset".into(), json!(through_offset));
    }
    envelope(
        "claude-code",
        source_version,
        session_id,
        event,
        ts_ms,
        payload,
    )
}

fn is_real_user(record: &Value) -> bool {
    if record.get("type").and_then(Value::as_str) != Some("user") {
        return false;
    }
    if record.get("isCompactSummary").and_then(Value::as_bool) == Some(true) {
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

fn last_assistant_error(records: &[Value]) -> Option<String> {
    records.iter().rev().find_map(|record| {
        if record.get("type").and_then(Value::as_str) != Some("assistant")
            || record.get("isApiErrorMessage").and_then(Value::as_bool) != Some(true)
        {
            return None;
        }
        string_at(record, "/error")
            .or_else(|| {
                last_assistant_text(std::slice::from_ref(record))?
                    .as_str()
                    .map(str::to_owned)
            })
            .or_else(|| Some("Claude API error".into()))
    })
}
