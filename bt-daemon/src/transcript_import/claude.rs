use super::{
    envelope, file_session_id, find_jsonl_files, read_complete_jsonl_records, read_jsonl_records,
    string_at, timestamp_bounds, timestamp_ms, validate_session_id, IncrementalRecords,
    TailSession, TranscriptImport, TranscriptLayout,
};
use crate::translate::{
    claude_task_notification_id, is_background_agent_launch, ClaudeSubagentHook,
    ClaudeUserPromptHook,
};
use crate::translate::{cli_fork_history, CliForkHistory};
use crate::wire::Envelope;
use anyhow::bail;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::io::BufRead;
use std::path::{Path, PathBuf};

impl TranscriptImport for crate::agents::Claude {
    fn discover(&self, home: &Path) -> anyhow::Result<Vec<PathBuf>> {
        layout(roots(home)).discover()
    }

    fn find(&self, home: &Path, session_id: &str) -> anyhow::Result<PathBuf> {
        layout(roots(home)).find(session_id)
    }

    fn tail(&self) -> Box<dyn TailSession> {
        Box::new(Tail::default())
    }
}

#[derive(Default)]
pub(super) struct Tail {
    started: bool,
    completed_turns: usize,
    active_turn: Option<usize>,
    last_len: u64,
    pending_background_stops: HashSet<String>,
}

impl TailSession for Tail {
    fn envelopes(
        &mut self,
        path: &Path,
        records: &IncrementalRecords,
    ) -> anyhow::Result<Vec<Envelope>> {
        envelopes(
            path,
            &records.values,
            &records.end_offsets,
            records.read_offset,
        )
    }

    fn poll(
        &mut self,
        events: Vec<Envelope>,
        len: u64,
        finalize: bool,
    ) -> anyhow::Result<Vec<Envelope>> {
        if events.is_empty() {
            return Ok(events);
        }
        if events.len() < 2 {
            bail!("Claude import did not produce session boundary events");
        }
        // A cold historical import already has the complete child transcript.
        // During attach, async launch results are links, not completion events.
        let attaching = !finalize || self.started;
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
            for event in &middle[start + skip..end] {
                if attaching && event.event == "SubagentStop" {
                    if let Ok(hook) = ClaudeSubagentHook::deserialize(&event.payload) {
                        if hook.import_background {
                            self.pending_background_stops.insert(hook.agent_id);
                            continue;
                        }
                    }
                }
                out.push(event.clone());
            }
            self.completed_turns += 1;
            self.active_turn = None;
        }
        let notifications = turn_starts
            .iter()
            .filter_map(|&index| {
                let hook = ClaudeUserPromptHook::deserialize(&middle[index].payload).ok()?;
                claude_task_notification_id(hook.text()?).map(str::to_owned)
            })
            .collect::<HashSet<_>>();
        for event in middle.iter().filter(|event| event.event == "SubagentStop") {
            let Ok(hook) = ClaudeSubagentHook::deserialize(&event.payload) else {
                continue;
            };
            let id = hook.agent_id.as_str();
            if (finalize || notifications.contains(id)) && self.pending_background_stops.remove(id)
            {
                out.push(event.clone());
            }
        }
        if !finalize && turn_count > 0 {
            let active = turn_count - 1;
            if self.active_turn != Some(active) {
                out.push(middle[turn_starts[active]].clone());
                self.active_turn = Some(active);
            }
            if len != self.last_len {
                let mut checkpoint = events.last().cloned().ok_or_else(|| {
                    anyhow::anyhow!("transcript import is missing its terminal envelope")
                })?;
                checkpoint.event = "ImportCheckpoint".into();
                if let Some(payload) = checkpoint.payload.as_object_mut() {
                    payload.insert("hook_event_name".into(), json!("ImportCheckpoint"));
                }
                out.push(checkpoint);
            }
        }
        if finalize {
            out.push(events.last().cloned().ok_or_else(|| {
                anyhow::anyhow!("transcript import is missing its terminal envelope")
            })?);
        }
        self.last_len = len;
        Ok(out)
    }
}

fn transcript_session_id(path: &Path) -> Option<String> {
    let file = std::fs::File::open(path).ok()?;
    for line in std::io::BufReader::new(file).lines().map_while(Result::ok) {
        let Ok(record) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let Some(session_id) = record.get("sessionId").and_then(Value::as_str) else {
            continue;
        };
        if validate_session_id(session_id).is_ok() && filename_matches(path, session_id) {
            return Some(session_id.to_owned());
        }
    }
    None
}

/// The transcript layout under these roots.
pub(super) fn layout(roots: Vec<PathBuf>) -> TranscriptLayout {
    TranscriptLayout {
        display_name: crate::agents::Agent::identity(&crate::agents::Claude).display_name,
        roots,
        session_id: transcript_session_id,
        filename_matches,
    }
}

fn roots(home: &Path) -> Vec<PathBuf> {
    let claude_home = std::env::var_os("CLAUDE_CONFIG_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".claude"));
    vec![claude_home.join("projects")]
}

fn filename_matches(path: &Path, session_id: &str) -> bool {
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
    let filename_id = file_session_id(path);
    let own_start = records.iter().position(|record| {
        record.get("sessionId").and_then(Value::as_str) == Some(filename_id.as_str())
    });
    if uuid::Uuid::parse_str(&filename_id).is_ok()
        && own_start.is_none()
        && records
            .iter()
            .any(|record| string_at(record, "/sessionId").is_some())
    {
        bail!(
            "Claude transcript {} does not contain its filename session id",
            path.display()
        );
    }
    let session_id = own_start.map(|_| filename_id).unwrap_or_else(|| {
        records
            .iter()
            .find_map(|record| string_at(record, "/sessionId"))
            .unwrap_or_else(|| file_session_id(path))
    });
    let inherited_prefix = match own_start.filter(|&index| index > 0) {
        Some(index) => index,
        None => match cli_fork_history(records) {
            CliForkHistory::Absent => 0,
            // Wait to emit SessionStart until its offset includes the whole
            // inherited prefix. Copied prompts must never advance turn indices.
            CliForkHistory::AwaitingPrompt => return Ok(Vec::new()),
            CliForkHistory::EndsAt(index) => index,
        },
    };
    let own_records = &records[inherited_prefix..];
    let source_version = own_records
        .iter()
        .find_map(|record| string_at(record, "/version"));
    let cwd = own_records
        .iter()
        .find_map(|record| string_at(record, "/cwd"));
    let (start_ms, end_ms) = timestamp_bounds(own_records);
    let transcript_path = path.to_string_lossy().into_owned();
    let subagents = subagents(path, own_records)?;
    let mut subagent_events = subagents
        .iter()
        .flat_map(|subagent| {
            [
                (subagent.start_index + inherited_prefix, true, subagent),
                (subagent.record_index + inherited_prefix, false, subagent),
            ]
        })
        .collect::<Vec<_>>();
    // Lifecycle boundaries follow native file order, not wall-clock order.
    subagent_events.sort_by_key(|(index, start, agent)| {
        (
            *index,
            if *start { 0 } else { 1 },
            if *start {
                agent.depth
            } else {
                usize::MAX - agent.depth
            },
        )
    });
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
            "source": "import",
            "_bt_import_inherited_history": inherited_prefix > 0
        }),
        inherited_prefix
            .checked_sub(1)
            .map_or(0, |index| record_end_offsets[index]),
    )];

    let user_indexes: Vec<usize> = records
        .iter()
        .enumerate()
        .filter_map(|(index, record)| {
            (index >= inherited_prefix && is_real_user(record)).then_some(index)
        })
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
                "prompt": prompt,
                "prompt_id": records[index].get("promptId"),
                "origin": records[index].get("origin")
            }),
            record_end_offsets[index],
        ));
        for &(record_index, is_start, subagent) in subagent_events
            .iter()
            .filter(|(record_index, _, _)| *record_index >= index && *record_index < next)
        {
            let (event, timestamp) = if is_start {
                ("SubagentStart", subagent.start_ms)
            } else {
                ("SubagentStop", subagent.end_ms)
            };
            let mut payload = json!({
                "session_id": session_id,
                "hook_event_name": event,
                "transcript_path": transcript_path,
                "agent_id": subagent.agent_id,
                "agent_type": subagent.agent_type,
                "agent_transcript_path": subagent.path,
                "_bt_import_parent_agent_id": subagent.parent_agent_id,
                "_bt_import_parent_tool_id": subagent.parent_tool_id,
                "_bt_import_background": subagent.background
            });
            if !is_start {
                payload["last_assistant_message"] = json!(subagent
                    .handback_message
                    .clone()
                    .or_else(|| subagent.last_assistant_message.clone()));
            }
            events.push(import_envelope(
                source_version.clone(),
                &session_id,
                event,
                timestamp,
                payload,
                record_end_offsets[record_index],
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
    handback_message: Option<Value>,
    parent_agent_id: Option<String>,
    parent_tool_id: Option<String>,
    depth: usize,
    background: bool,
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
        // API request blobs can repeat an earlier assistant message after the
        // tool result. Only the original assistant record is a launch event.
        if record.get("type").and_then(Value::as_str) != Some("assistant") {
            continue;
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

    let mut subagents: Vec<Subagent> = children
        .iter()
        .cloned()
        .filter_map(|(subagent_path, agent_id, subagent_records, child_start)| {
            // Nested and workflow transcripts require their own native links.
            if subagent_path.parent() != Some(directory.as_path())
                || std::fs::read(subagent_path.with_extension("meta.json"))
                    .ok()
                    .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                    .and_then(|meta| string_at(&meta, "/parentAgentId"))
                    .is_some()
            {
                return None;
            }
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
                handback_message: crate::translate::successful_subagent_handback_message(
                    &subagent_records,
                ),
                parent_agent_id: None,
                parent_tool_id: None,
                depth: 0,
                background: is_background_agent_launch(&records[record_index]),
            })
        })
        .collect::<Vec<_>>();
    // Workflow agents are anchored by the native Workflow result's run ID.
    let mut workflows = HashMap::new();
    for (index, record) in records.iter().enumerate() {
        let Some(run_id) = string_at(record, "/toolUseResult/runId") else {
            continue;
        };
        let Some(call_id) = record
            .pointer("/message/content")
            .and_then(Value::as_array)
            .and_then(|blocks| {
                blocks.iter().find_map(|block| {
                    (block.get("type").and_then(Value::as_str) == Some("tool_result"))
                        .then(|| string_at(block, "/tool_use_id"))
                        .flatten()
                })
            })
        else {
            continue;
        };
        let start = records.iter().position(|row| {
            row.get("type").and_then(Value::as_str) == Some("assistant")
                && row
                    .pointer("/message/content")
                    .and_then(Value::as_array)
                    .is_some_and(|blocks| {
                        blocks.iter().any(|block| {
                            block.get("id").and_then(Value::as_str) == Some(call_id.as_str())
                                && block.get("name").and_then(Value::as_str) == Some("Workflow")
                        })
                    })
        });
        if let Some(start) = start {
            workflows.insert(run_id, (start, index, call_id));
        }
    }
    let mut linked: HashSet<String> = subagents
        .iter()
        .map(|agent| agent.agent_id.clone())
        .collect();
    // Repeated passes admit only descendants of an already linked agent.
    loop {
        let mut added = false;
        for (path, agent_id, child_records, child_start) in &children {
            if linked.contains(agent_id) {
                continue;
            }
            let meta = std::fs::read(path.with_extension("meta.json"))
                .ok()
                .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                .unwrap_or(Value::Null);
            let parent_id = string_at(&meta, "/parentAgentId");
            let nested = parent_id.as_ref().and_then(|id| {
                let parent = subagents.iter().find(|agent| &agent.agent_id == id)?;
                let call_id = string_at(&meta, "/toolUseId")?;
                let parent_records = children
                    .iter()
                    .find(|(_, child_id, _, _)| child_id == id)
                    .map(|(_, _, records, _)| records)?;
                let found = parent_records.iter().any(|record| {
                    record.get("type").and_then(Value::as_str) == Some("assistant")
                        && record
                            .pointer("/message/content")
                            .and_then(Value::as_array)
                            .is_some_and(|blocks| {
                                blocks.iter().any(|block| {
                                    block.get("id").and_then(Value::as_str)
                                        == Some(call_id.as_str())
                                        && block.get("name").and_then(Value::as_str)
                                            == Some("Agent")
                                })
                            })
                });
                found.then(|| {
                    (
                        parent.start_index,
                        parent.record_index,
                        parent.depth + 1,
                        None,
                    )
                })
            });
            let workflow = parent_id
                .is_none()
                .then(|| path.parent())
                .flatten()
                .and_then(|run| {
                    if run.parent()? != directory.join("workflows") {
                        return None;
                    }
                    let run_id = run.file_name()?.to_str()?;
                    let (start, end, call_id) = workflows.get(run_id)?;
                    Some((*start, *end, 0, Some(call_id.clone())))
                });
            let Some((start_index, record_index, depth, parent_tool_id)) = nested.or(workflow)
            else {
                continue;
            };
            let (_, child_end) = timestamp_bounds(child_records);
            subagents.push(Subagent {
                agent_id: agent_id.clone(),
                agent_type: string_at(&meta, "/agentType"),
                path: path.to_string_lossy().into_owned(),
                start_index,
                record_index,
                start_ms: *child_start,
                end_ms: child_end,
                last_assistant_message: last_assistant_text(child_records),
                handback_message: crate::translate::successful_subagent_handback_message(
                    child_records,
                ),
                parent_agent_id: parent_id,
                parent_tool_id,
                depth,
                background: is_background_agent_launch(&records[record_index]),
            });
            linked.insert(agent_id.clone());
            added = true;
        }
        if !added {
            break;
        }
    }
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
