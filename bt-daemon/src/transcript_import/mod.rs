use crate::wire::Envelope;
use crate::ImportSource;
use anyhow::{bail, Context};
use serde_json::Value;
use std::collections::BTreeMap;
use std::io::BufRead;
use std::path::{Path, PathBuf};

mod claude;
mod codex;

pub(crate) fn resolve_transcripts(
    session_ids: &[String],
    all: bool,
    source: ImportSource,
) -> anyhow::Result<Vec<PathBuf>> {
    match (all, session_ids.is_empty()) {
        (true, true) => discover_transcripts(source),
        (false, false) => session_ids
            .iter()
            .map(|session_id| resolve_transcript(session_id, source))
            .collect(),
        (true, false) => bail!("--all cannot be combined with explicit session ids"),
        (false, true) => bail!("provide at least one session id or use --all"),
    }
}

pub(crate) fn resolve_transcript(
    session_id: &str,
    source: ImportSource,
) -> anyhow::Result<PathBuf> {
    validate_session_id(session_id)?;
    resolve_transcript_in(session_id, source, &transcript_roots(source))
}

fn transcript_roots(source: ImportSource) -> Vec<PathBuf> {
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    match source {
        ImportSource::Codex => codex::roots(&home),
        ImportSource::Claude => claude::roots(&home),
    }
}

fn discover_transcripts(source: ImportSource) -> anyhow::Result<Vec<PathBuf>> {
    let roots = transcript_roots(source);
    discover_transcripts_in(source, &roots)
}

fn discover_transcripts_in(
    source: ImportSource,
    roots: &[PathBuf],
) -> anyhow::Result<Vec<PathBuf>> {
    let mut candidates = Vec::new();
    for root in roots {
        find_jsonl_files(root, &mut candidates);
    }
    candidates.sort();
    candidates.dedup();

    let mut sessions = BTreeMap::<String, Vec<PathBuf>>::new();
    for path in candidates {
        let Some(session_id) = transcript_session_id(&path, source) else {
            continue;
        };
        sessions.entry(session_id).or_default().push(path);
    }
    if sessions.is_empty() {
        let locations = roots
            .iter()
            .map(|root| root.display().to_string())
            .collect::<Vec<_>>()
            .join(", ");
        bail!(
            "no {} transcripts found; searched {locations}",
            source_name(source)
        );
    }

    let mut resolved = Vec::with_capacity(sessions.len());
    for (session_id, paths) in sessions {
        match paths.as_slice() {
            [path] => resolved.push(path.clone()),
            paths => {
                let locations = paths
                    .iter()
                    .map(|path| path.display().to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                bail!(
                    "multiple {} transcripts found for session {session_id}: {locations}",
                    source_name(source)
                );
            }
        }
    }
    Ok(resolved)
}

fn find_jsonl_files(directory: &Path, matches: &mut Vec<PathBuf>) {
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
            find_jsonl_files(&path, matches);
        } else if path.extension().and_then(|extension| extension.to_str()) == Some("jsonl") {
            matches.push(path);
        }
    }
}

fn transcript_session_id(path: &Path, source: ImportSource) -> Option<String> {
    match source {
        ImportSource::Codex => codex::transcript_session_id(path),
        ImportSource::Claude => claude::transcript_session_id(path),
    }
}

fn resolve_transcript_in(
    session_id: &str,
    source: ImportSource,
    roots: &[PathBuf],
) -> anyhow::Result<PathBuf> {
    validate_session_id(session_id)?;
    let mut matches = Vec::new();
    for root in roots {
        let mut candidates = Vec::new();
        find_jsonl_files(root, &mut candidates);
        matches.extend(candidates.into_iter().filter(|path| match source {
            ImportSource::Codex => codex::filename_matches(path, session_id),
            ImportSource::Claude => claude::filename_matches(path, session_id),
        }));
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
    let file =
        std::fs::File::open(path).with_context(|| format!("read transcript {}", path.display()))?;
    let mut reader = std::io::BufReader::new(file);
    let mut records = Vec::new();
    let mut record_end_offsets = Vec::new();
    let mut offset = 0_u64;
    let mut line = String::new();
    let mut index = 0_usize;
    while reader.read_line(&mut line)? != 0 {
        index += 1;
        offset += line.len() as u64;
        if !line.trim().is_empty() {
            records.push(
                serde_json::from_str(&line).with_context(|| {
                    format!("parse transcript {} line {}", path.display(), index)
                })?,
            );
            record_end_offsets.push(offset);
        }
        line.clear();
    }
    if records.is_empty() {
        bail!("transcript {} is empty", path.display());
    }
    match source {
        ImportSource::Codex => codex::envelopes(path, &records),
        ImportSource::Claude => claude::envelopes(path, &records, &record_end_offsets, offset),
    }
}

/// Incrementally converts a growing native transcript into synthetic hook
/// events for one persistent translator. The final poll closes the active
/// turn/session; ordinary polls keep the newest turn open.
pub(crate) struct TranscriptTail {
    path: PathBuf,
    source: ImportSource,
    state: TailState,
    observed_file: Option<(u64, Option<std::time::SystemTime>)>,
}

enum TailState {
    Codex(codex::Tail),
    Claude(claude::Tail),
}

impl TranscriptTail {
    pub(crate) fn new(path: PathBuf, source: ImportSource) -> Self {
        Self {
            path,
            source,
            state: match source {
                ImportSource::Codex => TailState::Codex(codex::Tail::default()),
                ImportSource::Claude => TailState::Claude(claude::Tail::default()),
            },
            observed_file: None,
        }
    }

    pub(crate) fn poll(&mut self, finalize: bool) -> anyhow::Result<Vec<Envelope>> {
        let metadata = match std::fs::metadata(&self.path) {
            Ok(metadata) => metadata,
            Err(_) if !finalize => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        };
        let len = metadata.len();
        let observed_file = (len, metadata.modified().ok());
        if !finalize && self.observed_file == Some(observed_file) {
            return Ok(Vec::new());
        }
        let events = match transcript_envelopes(&self.path, self.source) {
            Ok(events) => events,
            Err(_) if !finalize => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        self.observed_file = Some(observed_file);
        match &mut self.state {
            TailState::Codex(state) => state.poll(events, len, finalize),
            TailState::Claude(state) => state.poll(events, len, finalize),
        }
    }
}

fn read_jsonl_records(path: &Path) -> anyhow::Result<Vec<Value>> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("read transcript {}", path.display()))?;
    contents
        .lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty())
        .map(|(index, line)| {
            serde_json::from_str(line)
                .with_context(|| format!("parse transcript {} line {}", path.display(), index + 1))
        })
        .collect()
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
        plugin_version: None,
        session_id: session_id.into(),
        event: event.into(),
        ts_ms,
        managed_run_id: None,
        capture: None,
        payload,
        route: None,
        config: None,
    }
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
    use serde_json::json;

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
    fn discovers_only_top_level_codex_transcripts_in_stable_order() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("sessions");
        let first = root.join("2026/01/01/rollout-first-session-a.jsonl");
        let second = root.join("2026/01/02/rollout-second-session-b.jsonl");
        std::fs::create_dir_all(first.parent().unwrap()).unwrap();
        std::fs::create_dir_all(second.parent().unwrap()).unwrap();
        std::fs::write(
            &first,
            "{\"type\":\"event_msg\"}\n{\"type\":\"session_meta\",\"payload\":{\"id\":\"session-a\"}}\n",
        )
        .unwrap();
        std::fs::write(
            &second,
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"session-b\"}}\n",
        )
        .unwrap();
        let subagent = root.join("2026/01/02/rollout-agent-c.jsonl");
        std::fs::write(
            subagent,
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"agent-c\",\"source\":{\"subagent\":{\"thread_spawn\":{\"parent_thread_id\":\"session-b\"}}}}}\n",
        )
        .unwrap();
        std::fs::write(root.join("not-a-transcript.jsonl"), "{}\n").unwrap();

        assert_eq!(
            discover_transcripts_in(ImportSource::Codex, &[root]).unwrap(),
            vec![first, second]
        );
    }

    #[test]
    fn claude_all_selects_parent_while_subagents_are_related_content() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("projects");
        let project = root.join("-tmp-project");
        let transcript = project.join("session-a.jsonl");
        let subagent = project.join("session-a/subagents/agent-1.jsonl");
        std::fs::create_dir_all(subagent.parent().unwrap()).unwrap();
        std::fs::write(
            &transcript,
            "{\"type\":\"user\",\"sessionId\":\"session-a\"}\n",
        )
        .unwrap();
        std::fs::write(
            &subagent,
            "{\"type\":\"assistant\",\"sessionId\":\"session-a\"}\n",
        )
        .unwrap();
        std::fs::write(project.join("notes.jsonl"), "{}\n").unwrap();

        assert_eq!(
            discover_transcripts_in(ImportSource::Claude, &[root]).unwrap(),
            vec![transcript]
        );
    }

    #[test]
    fn claude_import_emits_related_subagent_lifecycle_inside_parent_turn() {
        let temp = tempfile::tempdir().unwrap();
        let transcript = temp.path().join("session-a.jsonl");
        let subagent = temp.path().join("session-a/subagents/agent-child-a.jsonl");
        std::fs::create_dir_all(subagent.parent().unwrap()).unwrap();
        let records = [
            json!({"type":"user","sessionId":"session-a","timestamp":"2026-01-01T00:00:01Z","message":{"content":"delegate"}}),
            json!({"type":"assistant","sessionId":"session-a","timestamp":"2026-01-01T00:00:02Z","message":{"content":[{"type":"tool_use","id":"call-a","name":"Agent","input":{"subagent_type":"reviewer"}}]}}),
            json!({"type":"user","sessionId":"session-a","timestamp":"2026-01-01T00:00:05Z","toolUseResult":{"agentId":"child-a"},"message":{"content":[{"type":"tool_result","tool_use_id":"call-a","content":"done"}]}}),
            json!({"type":"assistant","sessionId":"session-a","timestamp":"2026-01-01T00:00:06Z","message":{"content":[{"type":"text","text":"complete"}]}}),
        ];
        std::fs::write(
            &transcript,
            records
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n"),
        )
        .unwrap();
        std::fs::write(
            &subagent,
            json!({"type":"assistant","sessionId":"session-a","timestamp":"2026-01-01T00:00:04Z","message":{"id":"sub-message","content":[{"type":"text","text":"reviewed"}]}}).to_string(),
        )
        .unwrap();

        let events = transcript_envelopes(&transcript, ImportSource::Claude).unwrap();
        let names = events
            .iter()
            .map(|event| event.event.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec![
                "SessionStart",
                "UserPromptSubmit",
                "SubagentStart",
                "SubagentStop",
                "Stop",
                "SessionEnd"
            ]
        );
        assert_eq!(events[2].payload["agent_id"], json!("child-a"));
        assert_eq!(events[2].payload["agent_type"], json!("reviewer"));
        assert_eq!(
            Path::new(events[3].payload["agent_transcript_path"].as_str().unwrap()),
            subagent
        );
    }

    #[test]
    fn codex_import_discovers_spawned_rollout_and_emits_lifecycle() {
        let temp = tempfile::tempdir().unwrap();
        let parent = temp.path().join("rollout-parent.jsonl");
        let child = temp.path().join("rollout-child-a.jsonl");
        let records = vec![
            json!({"timestamp":"2026-01-01T00:00:01Z","type":"session_meta","payload":{"id":"parent"}}),
            json!({"timestamp":"2026-01-01T00:00:02Z","type":"response_item","payload":{"type":"function_call","call_id":"call-a","name":"spawn_agent","arguments":"{\"agent_type\":\"reviewer\"}"}}),
            json!({"timestamp":"2026-01-01T00:00:03Z","type":"response_item","payload":{"type":"function_call_output","call_id":"call-a","output":"{\"agent_id\":\"child-a\"}"}}),
        ];
        std::fs::write(
            &parent,
            records
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n"),
        )
        .unwrap();
        std::fs::write(
            &child,
            [
                json!({"timestamp":"2026-01-01T00:00:02Z","type":"session_meta","payload":{"id":"child-a","source":{"subagent":{"thread_spawn":{"parent_thread_id":"parent"}}}}}),
                json!({"timestamp":"2026-01-01T00:00:04Z","type":"event_msg","payload":{"type":"task_complete","last_agent_message":"reviewed"}}),
            ]
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n"),
        )
        .unwrap();

        let events = codex::envelopes(&parent, &records).unwrap();
        assert!(events.iter().any(|event| {
            event.event == "SubagentStart" && event.payload["agent_id"] == json!("child-a")
        }));
        assert!(events.iter().any(|event| {
            event.event == "SubagentStop"
                && event.payload["agent_transcript_path"] == json!(child.to_string_lossy())
        }));
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
        let events = codex::envelopes(Path::new("rollout.jsonl"), &records).unwrap();

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

    #[test]
    fn codex_tail_keeps_session_open_until_final_poll() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session-123.jsonl");
        let mut records = vec![
            json!({"timestamp":"2026-01-01T00:00:01Z","type":"session_meta","payload":{"id":"session-123"}}),
            json!({"timestamp":"2026-01-01T00:00:02Z","type":"event_msg","payload":{"type":"task_started","turn_id":"turn-1"}}),
            json!({"timestamp":"2026-01-01T00:00:03Z","type":"response_item","payload":{"type":"message","role":"assistant"}}),
        ];
        let write = |records: &[Value]| {
            std::fs::write(
                &path,
                records
                    .iter()
                    .map(Value::to_string)
                    .collect::<Vec<_>>()
                    .join("\n"),
            )
            .unwrap();
        };
        write(&records);
        let mut tail = TranscriptTail::new(path.clone(), ImportSource::Codex);
        let first = tail.poll(false).unwrap();
        assert_eq!(first.first().unwrap().event, "SessionStart");
        assert_eq!(first.last().unwrap().event, "ImportCheckpoint");
        assert!(first.iter().all(|event| event.event != "Stop"));

        records.push(json!({"timestamp":"2026-01-01T00:00:04Z","type":"event_msg","payload":{"type":"task_complete","turn_id":"turn-1"}}));
        write(&records);
        let second = tail.poll(false).unwrap();
        assert!(second.iter().all(|event| event.event != "SessionStart"));
        assert_eq!(second.last().unwrap().event, "ImportCheckpoint");
        assert_eq!(tail.poll(true).unwrap().last().unwrap().event, "Stop");
    }

    #[test]
    fn claude_tail_closes_only_completed_turns() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session-123.jsonl");
        let mut records = vec![
            json!({"type":"user","sessionId":"session-123","timestamp":"2026-01-01T00:00:01Z","message":{"content":"one"}}),
            json!({"type":"assistant","sessionId":"session-123","timestamp":"2026-01-01T00:00:02Z","message":{"content":"answer one"}}),
        ];
        let write = |records: &[Value]| {
            std::fs::write(
                &path,
                records
                    .iter()
                    .map(Value::to_string)
                    .collect::<Vec<_>>()
                    .join("\n"),
            )
            .unwrap();
        };
        write(&records);
        let mut tail = TranscriptTail::new(path.clone(), ImportSource::Claude);
        let first = tail.poll(false).unwrap();
        assert_eq!(
            first
                .iter()
                .map(|event| event.event.as_str())
                .collect::<Vec<_>>(),
            vec!["SessionStart", "UserPromptSubmit", "ImportCheckpoint"]
        );

        records.extend([
            json!({"type":"user","sessionId":"session-123","timestamp":"2026-01-01T00:00:03Z","message":{"content":"two"}}),
            json!({"type":"assistant","sessionId":"session-123","timestamp":"2026-01-01T00:00:04Z","message":{"content":"answer two"}}),
        ]);
        write(&records);
        let second = tail.poll(false).unwrap();
        assert_eq!(
            second
                .iter()
                .map(|event| event.event.as_str())
                .collect::<Vec<_>>(),
            vec!["Stop", "UserPromptSubmit", "ImportCheckpoint"]
        );
        assert_eq!(
            tail.poll(true)
                .unwrap()
                .iter()
                .map(|event| event.event.as_str())
                .collect::<Vec<_>>(),
            vec!["Stop", "SessionEnd"]
        );
    }
}
