use crate::agents::{registrar, Agent};
use crate::wire::Envelope;
use crate::ImportSource;
use anyhow::{bail, Context};
use serde_json::Value;
use std::collections::BTreeMap;
use std::io::{BufRead, Read, Seek};
use std::path::{Path, PathBuf};

mod antigravity;
mod claude;
mod codex;
mod cursor;
mod pipeline;

pub use pipeline::{import_transcript, import_transcripts, run_import};

/// Importing one agent's native transcripts. Implemented in each agent's
/// module here and registered in [`crate::agents`].
pub(crate) trait TranscriptImport: Agent {
    /// Every session transcript the agent has saved under `home`.
    fn discover(&self, home: &Path) -> anyhow::Result<Vec<PathBuf>>;
    /// The saved transcript for one session under `home`.
    fn find(&self, home: &Path, session_id: &str) -> anyhow::Result<PathBuf>;
    /// State for following one growing transcript.
    fn tail(&self) -> Box<dyn TailSession>;
}

/// Where an agent saves its transcripts and how they are named. Each agent
/// builds one privately; this holds the search shared by all of them.
pub(super) struct TranscriptLayout {
    pub display_name: &'static str,
    /// Directories searched recursively for `.jsonl` transcripts.
    pub roots: Vec<PathBuf>,
    /// The session a transcript belongs to, if the path is one.
    pub session_id: fn(&Path) -> Option<String>,
    /// Whether a path is named for a session id.
    pub filename_matches: fn(&Path, &str) -> bool,
}

/// Follows one growing transcript, turning the records read so far into
/// synthetic hook events and emitting only those not emitted before.
pub(crate) trait TailSession: Send {
    /// Synthetic hook events for every record read so far.
    fn envelopes(
        &mut self,
        path: &Path,
        records: &IncrementalRecords,
    ) -> anyhow::Result<Vec<Envelope>>;

    /// The events from `events` to emit now. `finalize` ends the session.
    fn poll(
        &mut self,
        events: Vec<Envelope>,
        len: u64,
        finalize: bool,
    ) -> anyhow::Result<Vec<Envelope>>;

    /// Whether a rewritten transcript continues with the same translator
    /// instead of starting a new one. If so, the fresh tail that replaces this
    /// one receives [`Self::rewritten`].
    fn keeps_translator_on_rewrite(&self) -> bool {
        false
    }

    /// Called on a fresh tail after a rewrite, with the records from before
    /// and after it, when [`Self::keeps_translator_on_rewrite`] is true.
    fn rewritten(&mut self, _previous: &[Value], _current: &[Value]) {}

    /// Whether an attached import may stop at a final record that is still
    /// being written when it shuts down.
    fn tolerates_incomplete_final_record(&self) -> bool {
        false
    }
}

fn importer(source: ImportSource) -> &'static dyn TranscriptImport {
    let name = source.identity().id;
    registrar()
        .import
        .get(name)
        .unwrap_or_else(|| panic!("no transcript import is registered for {name}"))
}

pub(crate) fn resolve_transcripts(
    session_ids: &[String],
    all: bool,
    source: ImportSource,
) -> anyhow::Result<Vec<PathBuf>> {
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    let importer = importer(source);
    match (all, session_ids.is_empty()) {
        (true, true) => importer.discover(&home),
        (false, false) => session_ids
            .iter()
            .map(|session_id| importer.find(&home, session_id))
            .collect(),
        (true, false) => bail!("--all cannot be combined with explicit session ids"),
        (false, true) => bail!("provide at least one session id or use --all"),
    }
}

impl TranscriptLayout {
    /// One transcript per session found under the roots, in path order.
    pub fn discover(&self) -> anyhow::Result<Vec<PathBuf>> {
        let mut sessions = BTreeMap::<String, Vec<PathBuf>>::new();
        for path in self.candidates() {
            if let Some(session_id) = (self.session_id)(&path) {
                sessions.entry(session_id).or_default().push(path);
            }
        }
        if sessions.is_empty() {
            bail!(
                "no {} transcripts found; searched {}",
                self.display_name,
                self.locations()
            );
        }
        sessions
            .into_iter()
            .map(|(session_id, paths)| self.single(&session_id, paths))
            .collect()
    }

    /// The one transcript named for `session_id`.
    pub fn find(&self, session_id: &str) -> anyhow::Result<PathBuf> {
        validate_session_id(session_id)?;
        let matches: Vec<_> = self
            .candidates()
            .into_iter()
            .filter(|path| (self.filename_matches)(path, session_id))
            .collect();
        if matches.is_empty() {
            bail!(
                "no {} transcript found for session {session_id}; searched {}",
                self.display_name,
                self.locations()
            );
        }
        self.single(session_id, matches)
    }

    fn candidates(&self) -> Vec<PathBuf> {
        let mut candidates = Vec::new();
        for root in &self.roots {
            find_jsonl_files(root, &mut candidates);
        }
        candidates.sort();
        candidates.dedup();
        candidates
    }

    fn single(&self, session_id: &str, paths: Vec<PathBuf>) -> anyhow::Result<PathBuf> {
        match <[PathBuf; 1]>::try_from(paths) {
            Ok([path]) => Ok(path),
            Err(paths) => bail!(
                "multiple {} transcripts found for session {session_id}: {}",
                self.display_name,
                paths
                    .iter()
                    .map(|path| path.display().to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }
    }

    fn locations(&self) -> String {
        self.roots
            .iter()
            .map(|root| root.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    }
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

#[cfg(test)]
pub(crate) fn transcript_envelopes(
    path: &Path,
    source: ImportSource,
) -> anyhow::Result<Vec<Envelope>> {
    let mut records = IncrementalRecords::default();
    records.refresh_with_final_partial(path, true, false)?;
    importer(source).tail().envelopes(path, &records)
}

/// The records of a transcript read so far, re-read incrementally as it
/// grows.
#[derive(Default)]
pub(crate) struct IncrementalRecords {
    values: Vec<Value>,
    end_offsets: Vec<u64>,
    read_offset: u64,
    line_count: usize,
    modified: Option<std::time::SystemTime>,
    anchor: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Refresh {
    Unchanged,
    Appended,
    Reset,
}

impl IncrementalRecords {
    fn refresh_with_final_partial(
        &mut self,
        path: &Path,
        finalize: bool,
        allow_incomplete_final_record: bool,
    ) -> anyhow::Result<Refresh> {
        let metadata = std::fs::metadata(path)
            .with_context(|| format!("read transcript metadata {}", path.display()))?;
        let len = metadata.len();
        let modified = metadata.modified().ok();
        let prefix_changed = self.read_offset > 0
            && len >= self.read_offset
            && read_anchor(path, self.read_offset, self.anchor.len())? != self.anchor;
        let reset = self.read_offset > 0
            && (len < self.read_offset
                || prefix_changed
                || (len == self.read_offset && modified != self.modified));
        if reset {
            *self = Self::default();
        }
        if len == self.read_offset {
            self.modified = modified;
            if self.values.is_empty() && finalize {
                bail!("transcript {} is empty", path.display());
            }
            return Ok(if reset {
                Refresh::Reset
            } else {
                Refresh::Unchanged
            });
        }

        let start_offset = self.read_offset;
        let mut file = std::fs::File::open(path)
            .with_context(|| format!("read transcript {}", path.display()))?;
        file.seek(std::io::SeekFrom::Start(self.read_offset))?;
        let mut reader = std::io::BufReader::new(file);
        let mut values = Vec::new();
        let mut end_offsets = Vec::new();
        let mut offset = self.read_offset;
        let mut parsed_through = self.read_offset;
        let mut line_count = self.line_count;
        let mut parsed_line_count = self.line_count;
        let mut line = String::new();
        loop {
            let bytes = reader.read_line(&mut line)?;
            if bytes == 0 {
                break;
            }
            line_count += 1;
            offset += bytes as u64;
            if !line.trim().is_empty() {
                match serde_json::from_str(&line) {
                    Ok(value) => {
                        values.push(value);
                        end_offsets.push(offset);
                    }
                    Err(_)
                        if (allow_incomplete_final_record || !finalize)
                            && offset == len
                            && !line.ends_with('\n') =>
                    {
                        break
                    }
                    Err(error) => {
                        return Err(error).with_context(|| {
                            format!("parse transcript {} line {}", path.display(), line_count)
                        });
                    }
                }
            }
            parsed_through = offset;
            parsed_line_count = line_count;
            line.clear();
        }

        self.values.extend(values);
        self.end_offsets.extend(end_offsets);
        self.read_offset = parsed_through;
        self.line_count = parsed_line_count;
        self.modified = std::fs::metadata(path)
            .ok()
            .and_then(|metadata| metadata.modified().ok());
        self.anchor = read_anchor(path, parsed_through, 64)?;
        if self.values.is_empty() && finalize {
            bail!("transcript {} is empty", path.display());
        }
        Ok(if reset {
            Refresh::Reset
        } else if parsed_through > start_offset {
            Refresh::Appended
        } else {
            Refresh::Unchanged
        })
    }
}

fn read_anchor(path: &Path, through: u64, max_len: usize) -> anyhow::Result<Vec<u8>> {
    let len = usize::try_from(through.min(max_len as u64)).unwrap_or(max_len);
    if len == 0 {
        return Ok(Vec::new());
    }
    let mut file =
        std::fs::File::open(path).with_context(|| format!("read transcript {}", path.display()))?;
    file.seek(std::io::SeekFrom::Start(through - len as u64))?;
    let mut anchor = vec![0; len];
    file.read_exact(&mut anchor)?;
    Ok(anchor)
}

/// Incrementally converts a growing native transcript into synthetic hook
/// events for one persistent translator. The final poll closes the active
/// turn/session; ordinary polls keep the newest turn open.
pub(crate) struct TranscriptTail {
    path: PathBuf,
    importer: &'static dyn TranscriptImport,
    session: Box<dyn TailSession>,
    records: IncrementalRecords,
    retry_envelopes: bool,
    translator_reset: bool,
    allow_incomplete_final_record: bool,
}

impl TranscriptTail {
    pub(crate) fn new(path: PathBuf, source: ImportSource) -> Self {
        let importer = importer(source);
        Self {
            path,
            importer,
            session: importer.tail(),
            records: IncrementalRecords::default(),
            retry_envelopes: false,
            translator_reset: false,
            allow_incomplete_final_record: false,
        }
    }

    pub(crate) fn allow_incomplete_final_record_on_shutdown(&mut self) {
        self.allow_incomplete_final_record = true;
    }

    pub(crate) fn poll(&mut self, finalize: bool) -> anyhow::Result<Vec<Envelope>> {
        let keeps_translator = self.session.keeps_translator_on_rewrite();
        let previous_records = if keeps_translator {
            self.records.values.clone()
        } else {
            Vec::new()
        };
        let refresh = match self.records.refresh_with_final_partial(
            &self.path,
            finalize,
            self.session.tolerates_incomplete_final_record() && self.allow_incomplete_final_record,
        ) {
            Ok(refresh) => refresh,
            Err(_) if !finalize => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        if refresh == Refresh::Unchanged && !finalize && !self.retry_envelopes {
            return Ok(Vec::new());
        }
        if refresh == Refresh::Reset {
            self.session = self.importer.tail();
            if keeps_translator {
                self.session
                    .rewritten(&previous_records, &self.records.values);
            } else {
                self.translator_reset = true;
            }
        }
        let events = match self.session.envelopes(&self.path, &self.records) {
            Ok(events) => events,
            Err(_) if !finalize => {
                self.retry_envelopes = true;
                return Ok(Vec::new());
            }
            Err(error) => return Err(error),
        };
        self.retry_envelopes = false;
        let len = std::fs::metadata(&self.path)
            .with_context(|| format!("read transcript metadata {}", self.path.display()))?
            .len();
        self.session.poll(events, len, finalize)
    }

    pub(crate) fn take_translator_reset(&mut self) -> bool {
        std::mem::take(&mut self.translator_reset)
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

fn read_complete_jsonl_records(path: &Path) -> anyhow::Result<Vec<Value>> {
    let contents =
        std::fs::read(path).with_context(|| format!("read transcript {}", path.display()))?;
    let lines = contents.split(|byte| *byte == b'\n').collect::<Vec<_>>();
    let mut records = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        match serde_json::from_slice(line) {
            Ok(record) => records.push(record),
            Err(_) if index + 1 == lines.len() => break,
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("parse transcript {} line {}", path.display(), index + 1)
                });
            }
        }
    }
    Ok(records)
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
            codex::layout(vec![root]).find("session-123").unwrap(),
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
            claude::layout(vec![root]).find("session-123").unwrap(),
            transcript
        );
    }

    #[test]
    fn finds_only_exact_antigravity_conversation_directory() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("brain");
        let transcript = root
            .join("conversation-123")
            .join(".system_generated/logs/transcript_full.jsonl");
        std::fs::create_dir_all(transcript.parent().unwrap()).unwrap();
        std::fs::write(&transcript, "{}\n").unwrap();
        let unrelated = root
            .join("conversation-123-other")
            .join(".system_generated/logs/transcript_full.jsonl");
        std::fs::create_dir_all(unrelated.parent().unwrap()).unwrap();
        std::fs::write(unrelated, "{}\n").unwrap();

        assert_eq!(
            antigravity::layout(vec![root])
                .find("conversation-123")
                .unwrap(),
            transcript
        );
    }

    #[test]
    fn finds_cursor_transcripts_by_nested_session_identity_and_ignores_symlinks() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("projects");
        let transcript = root
            .join("workspace")
            .join("agent-transcripts/session-123/session-123.jsonl");
        std::fs::create_dir_all(transcript.parent().unwrap()).unwrap();
        std::fs::write(&transcript, r#"{"role":"user","message":{"content":"hi"}}"#).unwrap();
        let subagent = root
            .join("workspace")
            .join("agent-transcripts/agent-session-123/agent-session-123.jsonl");
        std::fs::create_dir_all(subagent.parent().unwrap()).unwrap();
        std::fs::write(&subagent, "{}\n").unwrap();

        assert_eq!(
            cursor::layout(vec![root.clone()])
                .find("session-123")
                .unwrap(),
            transcript
        );
        assert_eq!(
            cursor::layout(vec![root]).discover().unwrap(),
            vec![transcript]
        );
    }

    #[test]
    fn cursor_import_emits_only_supported_transcript_enrichment_and_boundaries() {
        let temp = tempfile::tempdir().unwrap();
        let transcript = temp
            .path()
            .join("workspace/agent-transcripts/session-123/session-123.jsonl");
        std::fs::create_dir_all(transcript.parent().unwrap()).unwrap();
        let contents = concat!(
            "{\"role\":\"user\",\"message\":{\"content\":[{\"type\":\"text\",\"text\":\"question\"}]}}\n",
            "{\"role\":\"assistant\",\"message\":{\"content\":[{\"type\":\"tool_use\",\"name\":\"Read\",\"input\":{\"path\":\"a.txt\"}}]}}\n",
            "{\"role\":\"assistant\",\"message\":{\"content\":[{\"type\":\"text\",\"text\":\"answer\"}]}}\n",
            "{\"type\":\"turn_ended\",\"status\":\"success\"}\n"
        );
        std::fs::write(&transcript, contents).unwrap();
        let events = transcript_envelopes(&transcript, ImportSource::Cursor).unwrap();

        assert_eq!(
            events
                .iter()
                .map(|event| event.event.as_str())
                .collect::<Vec<_>>(),
            vec!["_bt_importStart", "_bt_importCheckpoint", "_bt_importStop"]
        );
        assert!(events.iter().all(|event| event.source == "cursor"));
        assert_eq!(
            events[1].payload["_bt_transcript_mirror"]["through"],
            contents.len() as u64
        );
        assert_eq!(
            events[1].payload["historical_fidelity"],
            "user_and_assistant_text_and_terminal_status"
        );
        assert_eq!(events[2].payload["status"], "success");
        assert_eq!(events[1].payload["start_time_estimated"], true);
    }

    #[test]
    fn cursor_import_preserves_terminal_error_message() {
        let temp = tempfile::tempdir().unwrap();
        let transcript = temp
            .path()
            .join("workspace/agent-transcripts/session-123/session-123.jsonl");
        std::fs::create_dir_all(transcript.parent().unwrap()).unwrap();
        std::fs::write(
            &transcript,
            "{\"role\":\"user\",\"message\":{\"content\":\"question\"}}\n{\"type\":\"turn_ended\",\"status\":\"error\",\"error\":\"WritableIterable is closed\"}\n",
        )
        .unwrap();

        let events = transcript_envelopes(&transcript, ImportSource::Cursor).unwrap();
        let stop = events
            .iter()
            .find(|event| event.event == "_bt_importStop")
            .unwrap();
        assert_eq!(stop.payload["status"], "error");
        assert_eq!(stop.payload["error_message"], "WritableIterable is closed");
    }

    #[test]
    fn cursor_attach_tails_growth_without_repeating_boundaries_or_checkpoints() {
        let temp = tempfile::tempdir().unwrap();
        let transcript = temp
            .path()
            .join("workspace/agent-transcripts/session-123/session-123.jsonl");
        std::fs::create_dir_all(transcript.parent().unwrap()).unwrap();
        let first_record = b"{\"role\":\"user\",\"message\":{\"content\":\"one\"}}\n";
        std::fs::write(&transcript, first_record).unwrap();
        let mut tail = TranscriptTail::new(transcript.clone(), ImportSource::Cursor);
        let first = tail.poll(false).unwrap();
        assert_eq!(
            first
                .iter()
                .map(|event| event.event.as_str())
                .collect::<Vec<_>>(),
            vec!["_bt_importStart", "_bt_importCheckpoint"]
        );
        let snapshot = first[1].payload["_bt_transcript_mirror"]["mirror"]
            .as_str()
            .unwrap()
            .to_owned();
        assert_ne!(PathBuf::from(&snapshot), transcript);
        assert_eq!(
            first[1].payload["_bt_transcript_mirror"]["through"],
            first_record.len() as u64
        );
        assert!(tail.poll(false).unwrap().is_empty());

        use std::io::Write;
        std::fs::OpenOptions::new()
            .append(true)
            .open(&transcript)
            .unwrap()
            .write_all(b"{\"role\":\"assistant\",\"message\":{\"content\":\"two\"}}\n")
            .unwrap();
        let grown = tail.poll(false).unwrap();
        assert_eq!(
            grown
                .iter()
                .map(|event| event.event.as_str())
                .collect::<Vec<_>>(),
            vec!["_bt_importCheckpoint"]
        );
        assert_eq!(
            grown[0].payload["_bt_transcript_mirror"]["mirror"],
            snapshot
        );
        assert_eq!(
            std::fs::read_to_string(&snapshot).unwrap().lines().count(),
            2
        );
        assert_eq!(
            tail.poll(true)
                .unwrap()
                .iter()
                .map(|event| event.event.as_str())
                .collect::<Vec<_>>(),
            vec!["_bt_importStop"]
        );
        std::fs::write(&transcript, b"replacement transcript\n").unwrap();
        let snapshot_contents = std::fs::read_to_string(&snapshot).unwrap();
        assert!(snapshot_contents.contains("\"content\":\"one\""));
        assert!(snapshot_contents.contains("\"content\":\"two\""));
    }

    #[test]
    fn cursor_transcript_replacement_rotates_snapshot_without_resetting_translator() {
        let temp = tempfile::tempdir().unwrap();
        let transcript = temp
            .path()
            .join("workspace/agent-transcripts/session-123/session-123.jsonl");
        std::fs::create_dir_all(transcript.parent().unwrap()).unwrap();
        std::fs::write(
            &transcript,
            "{\"role\":\"user\",\"message\":{\"content\":\"the original longer prompt\"}}\n",
        )
        .unwrap();
        let mut tail = TranscriptTail::new(transcript.clone(), ImportSource::Cursor);
        let first = tail.poll(false).unwrap();
        let first_snapshot = first[1].payload["_bt_transcript_mirror"]["mirror"]
            .as_str()
            .unwrap()
            .to_owned();

        std::fs::write(
            &transcript,
            "{\"role\":\"user\",\"message\":{\"content\":\"latest\"}}\n",
        )
        .unwrap();
        let replacement = tail.poll(false).unwrap();
        assert_eq!(replacement[0].event, "_bt_importStart");
        assert_eq!(replacement[1].event, "_bt_importCheckpoint");
        let replacement_snapshot = replacement[1].payload["_bt_transcript_mirror"]["mirror"]
            .as_str()
            .unwrap();
        assert_ne!(first_snapshot, replacement_snapshot);
        assert!(!tail.take_translator_reset());
    }

    #[test]
    fn cursor_attach_finalizes_after_an_incomplete_trailing_record() {
        let temp = tempfile::tempdir().unwrap();
        let transcript = temp
            .path()
            .join("workspace/agent-transcripts/session-123/session-123.jsonl");
        std::fs::create_dir_all(transcript.parent().unwrap()).unwrap();
        std::fs::write(
            &transcript,
            "{\"role\":\"user\",\"message\":{\"content\":\"complete\"}}\n",
        )
        .unwrap();

        let mut tail = TranscriptTail::new(transcript.clone(), ImportSource::Cursor);
        tail.allow_incomplete_final_record_on_shutdown();
        assert_eq!(
            tail.poll(false).unwrap().last().unwrap().event,
            "_bt_importCheckpoint"
        );
        use std::io::Write;
        std::fs::OpenOptions::new()
            .append(true)
            .open(&transcript)
            .unwrap()
            .write_all(b"{\"role\":\"assistant\",\"message\":")
            .unwrap();

        let final_events = tail.poll(true).unwrap();
        assert_eq!(
            final_events
                .iter()
                .map(|event| event.event.as_str())
                .collect::<Vec<_>>(),
            vec!["_bt_importCheckpoint", "_bt_importStop"]
        );
    }

    #[test]
    fn completed_import_still_rejects_an_incomplete_trailing_record() {
        let temp = tempfile::tempdir().unwrap();
        let transcript = temp.path().join("transcript.jsonl");
        std::fs::write(&transcript, "{\"type\":\"complete\"}\n{\"partial\"").unwrap();

        let error = IncrementalRecords::default()
            .refresh_with_final_partial(&transcript, true, false)
            .unwrap_err();
        assert!(error.to_string().contains("parse transcript"));
    }

    #[test]
    fn rejects_unsafe_session_ids() {
        let error = codex::layout(vec![PathBuf::from("unused")])
            .find("../session")
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

        let missing = claude::layout(vec![first.clone(), second.clone()])
            .find("missing")
            .unwrap_err();
        assert!(missing.to_string().contains("no Claude Code transcript"));

        std::fs::write(first.join("duplicate.jsonl"), "{}\n").unwrap();
        std::fs::write(second.join("duplicate.jsonl"), "{}\n").unwrap();
        let ambiguous = claude::layout(vec![first, second])
            .find("duplicate")
            .unwrap_err();
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
            codex::layout(vec![root]).discover().unwrap(),
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
            claude::layout(vec![root]).discover().unwrap(),
            vec![transcript]
        );
    }

    #[test]
    fn claude_import_keeps_delayed_child_and_native_handback_under_launching_turn() {
        use crate::translate::{Registry, SessionCtx, SpanOp, SpanType};

        let temp = tempfile::tempdir().unwrap();
        let transcript = temp.path().join("session-a.jsonl");
        let subagent = temp.path().join("session-a/subagents/agent-child-a.jsonl");
        std::fs::create_dir_all(subagent.parent().unwrap()).unwrap();
        let records = [
            json!({"type":"user","sessionId":"session-a","timestamp":"2026-01-01T00:00:01Z","message":{"content":"delegate"}}),
            json!({"type":"assistant","sessionId":"session-a","timestamp":"2026-01-01T00:00:02Z","message":{"content":[{"type":"tool_use","id":"call-a","name":"Agent","input":{"subagent_type":"reviewer"}}]}}),
            json!({"type":"user","sessionId":"session-a","timestamp":"2026-01-01T00:00:03Z","message":{"content":"unrelated question"}}),
            json!({"type":"user","sessionId":"session-a","timestamp":"2026-01-01T00:00:05Z","toolUseResult":{"agentId":"child-a"},"message":{"content":[{"type":"tool_result","tool_use_id":"call-a","content":"done"}]}}),
            json!({"type":"assistant","sessionId":"session-a","timestamp":"2026-01-01T00:00:06Z","message":{"content":[{"type":"text","text":"complete"}]}}),
            json!({"type":"user","sessionId":"session-a","timestamp":"2026-01-01T00:00:07Z","origin":{"kind":"peer","handback":true,"senderTaskId":"child-a","from":"child-a","name":"reviewer"},"message":{"content":"Another Claude session sent a message:\n<agent-message from=\"child-a\">reviewed</agent-message>\nThis agent is working inside the same session."}}),
            json!({"type":"assistant","sessionId":"session-a","timestamp":"2026-01-01T00:00:08Z","message":{"id":"handback-response","content":[{"type":"text","text":"accepted review"}]}}),
            json!({"type":"user","sessionId":"session-a","timestamp":"2026-01-01T00:00:09Z","message":{"content":"next human question"}}),
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
        let context = SessionCtx {
            session_id: "session-a".into(),
            config: None,
        };
        let mut translator = Registry::default_agents().create("claude-code", "session-a");
        let mut ops = Vec::new();
        for event in &events {
            ops.extend(translator.handle(event, &context).unwrap());
            while let Some(pending) = translator.drain_pending(&context).unwrap() {
                ops.extend(pending);
            }
        }
        ops.extend(translator.finalize(&context).unwrap());
        while let Some(pending) = translator.drain_pending(&context).unwrap() {
            ops.extend(pending);
        }
        let inserts = ops
            .iter()
            .filter_map(|op| match op {
                SpanOp::Insert(row) => Some(row),
                SpanOp::Merge(_) => None,
            })
            .collect::<Vec<_>>();
        let turns = inserts
            .iter()
            .filter(|row| row.name.starts_with("Turn "))
            .collect::<Vec<_>>();
        assert_eq!(
            turns
                .iter()
                .map(|row| row.name.as_str())
                .collect::<Vec<_>>(),
            ["Turn 1", "Turn 2", "Turn 3"]
        );
        let launch = turns[0];
        let child = inserts
            .iter()
            .find(|row| {
                row.span_type == SpanType::Task
                    && row
                        .metadata
                        .as_ref()
                        .and_then(|value| value.get("agent_id"))
                        == Some(&json!("child-a"))
            })
            .unwrap();
        assert_eq!(child.parent_span_ids, std::slice::from_ref(&launch.span_id));
        let continuation = inserts
            .iter()
            .find(|row| {
                row.metadata
                    .as_ref()
                    .and_then(|value| value.get("turn_trigger"))
                    == Some(&json!("agent_message"))
            })
            .unwrap();
        assert_eq!(
            continuation.parent_span_ids,
            std::slice::from_ref(&launch.span_id)
        );
        assert!(ops.iter().any(|op| match op {
            SpanOp::Insert(row) | SpanOp::Merge(row) => {
                row.span_id == continuation.span_id
                    && row.output.as_ref() == Some(&json!("accepted review"))
            }
        }));
    }

    #[test]
    fn claude_import_rejects_partial_completed_subagent_transcript() {
        let temp = tempfile::tempdir().unwrap();
        let transcript = temp.path().join("session-a.jsonl");
        let subagent = temp.path().join("session-a/subagents/agent-child-a.jsonl");
        std::fs::create_dir_all(subagent.parent().unwrap()).unwrap();
        let records = [
            json!({"type":"user","sessionId":"session-a","timestamp":"2026-01-01T00:00:01Z","message":{"content":"delegate"}}),
            json!({"type":"assistant","sessionId":"session-a","timestamp":"2026-01-01T00:00:02Z","message":{"content":[{"type":"tool_use","id":"call-a","name":"Agent","input":{"prompt":"review this change"}}]}}),
            json!({"type":"user","sessionId":"session-a","timestamp":"2026-01-01T00:00:05Z","toolUseResult":{"agentId":"child-a"},"message":{"content":[{"type":"tool_result","tool_use_id":"call-a","content":"done"}]}}),
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
            format!(
                "{}\n{{\"type\":",
                json!({"type":"assistant","sessionId":"session-a","timestamp":"2026-01-01T00:00:04Z","message":{"content":[{"type":"text","text":"reviewed"}]}})
            ),
        )
        .unwrap();

        let error = transcript_envelopes(&transcript, ImportSource::Claude).unwrap_err();
        assert!(format!("{error:#}").contains("line 2"));
    }

    #[test]
    fn claude_import_keeps_interrupted_subagent_with_matching_agent_call() {
        let temp = tempfile::tempdir().unwrap();
        let transcript = temp.path().join("session-a.jsonl");
        let subagent = temp.path().join("session-a/subagents/agent-child-a.jsonl");
        std::fs::create_dir_all(subagent.parent().unwrap()).unwrap();
        let records = [
            json!({"type":"user","sessionId":"session-a","timestamp":"2026-01-01T00:00:01Z","message":{"content":"delegate"}}),
            json!({"type":"assistant","sessionId":"session-a","timestamp":"2026-01-01T00:00:02Z","message":{"content":[{"type":"tool_use","id":"call-a","name":"Agent","input":{"subagent_type":"reviewer","prompt":"review this change"}}]}}),
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
            format!(
                "{}\n{{\"type\":",
                json!({"type":"user","isSidechain":true,"agentId":"child-a","sessionId":"session-a","timestamp":"2026-01-01T00:00:03Z","message":{"content":"review this change"}})
            ),
        )
        .unwrap();

        let events = transcript_envelopes(&transcript, ImportSource::Claude).unwrap();
        assert!(events.iter().any(|event| {
            event.event == "SubagentStart"
                && event.payload["agent_id"] == json!("child-a")
                && event.payload["agent_type"] == json!("reviewer")
        }));
        assert!(events.iter().any(|event| {
            event.event == "SubagentStop"
                && event.payload["agent_transcript_path"]
                    .as_str()
                    .is_some_and(|path| Path::new(path) == subagent)
        }));
    }

    #[test]
    fn claude_import_matches_interrupted_subagents_by_native_time() {
        let temp = tempfile::tempdir().unwrap();
        let transcript = temp.path().join("session-a.jsonl");
        let child_dir = temp.path().join("session-a/subagents");
        std::fs::create_dir_all(&child_dir).unwrap();
        let records = [
            json!({"type":"user","sessionId":"session-a","timestamp":"2026-01-01T00:00:01Z","message":{"content":"delegate"}}),
            json!({"type":"assistant","sessionId":"session-a","timestamp":"2026-01-01T00:00:02Z","message":{"content":[{"type":"tool_use","id":"call-early","name":"Agent","input":{"subagent_type":"reviewer","prompt":"same prompt"}}]}}),
            json!({"type":"assistant","sessionId":"session-a","timestamp":"2026-01-01T00:00:10Z","message":{"content":[{"type":"tool_use","id":"call-late","name":"Agent","input":{"subagent_type":"reviewer","prompt":"same prompt"}}]}}),
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
        for (agent_id, timestamp) in [
            ("z-early", "2026-01-01T00:00:03Z"),
            ("a-late", "2026-01-01T00:00:11Z"),
        ] {
            std::fs::write(
                child_dir.join(format!("agent-{agent_id}.jsonl")),
                json!({"type":"user","isSidechain":true,"agentId":agent_id,"sessionId":"session-a","timestamp":timestamp,"message":{"content":"same prompt"}}).to_string(),
            )
            .unwrap();
        }

        let events = transcript_envelopes(&transcript, ImportSource::Claude).unwrap();
        let starts = events
            .iter()
            .filter(|event| event.event == "SubagentStart")
            .map(|event| (event.payload["agent_id"].as_str().unwrap(), event.ts_ms))
            .collect::<Vec<_>>();
        assert_eq!(
            starts,
            vec![
                ("z-early", 1_767_225_602_000),
                ("a-late", 1_767_225_610_000)
            ]
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
    fn antigravity_tail_keeps_session_open_and_reports_new_records_once() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp
            .path()
            .join("conversation-123/.system_generated/logs/transcript_full.jsonl");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut records = vec![
            json!({"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","created_at":"2026-01-01T00:00:01Z","content":"one"}),
            json!({"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","created_at":"2026-01-01T00:00:02Z","content":"answer"}),
        ];
        let write = |records: &[Value]| {
            std::fs::write(
                &path,
                records
                    .iter()
                    .map(Value::to_string)
                    .collect::<Vec<_>>()
                    .join("\n")
                    + "\n",
            )
            .unwrap()
        };
        write(&records);
        let mut tail = TranscriptTail::new(path.clone(), ImportSource::Antigravity);
        assert_eq!(
            tail.poll(false)
                .unwrap()
                .iter()
                .map(|event| event.event.as_str())
                .collect::<Vec<_>>(),
            vec!["PreInvocation", "PostInvocation"]
        );

        records.push(json!({"step_index":2,"source":"MODEL","type":"LIST_DIRECTORY","created_at":"2026-01-01T00:00:03Z","content":"result"}));
        write(&records);
        assert_eq!(
            tail.poll(false)
                .unwrap()
                .iter()
                .map(|event| event.event.as_str())
                .collect::<Vec<_>>(),
            vec!["PostToolUse"]
        );
        assert!(tail.poll(false).unwrap().is_empty());
        assert_eq!(
            tail.poll(true)
                .unwrap()
                .iter()
                .map(|event| event.event.as_str())
                .collect::<Vec<_>>(),
            vec!["Stop"]
        );
    }

    #[test]
    fn incremental_reader_handles_partial_appends_and_replacement() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session.jsonl");
        std::fs::write(&path, "{\"type\":\"session_meta\"").unwrap();
        let mut records = IncrementalRecords::default();
        assert_eq!(
            records
                .refresh_with_final_partial(&path, false, false)
                .unwrap(),
            Refresh::Unchanged
        );
        assert!(records.values.is_empty());

        std::fs::write(
            &path,
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"session\"}}\n",
        )
        .unwrap();
        assert_eq!(
            records
                .refresh_with_final_partial(&path, false, false)
                .unwrap(),
            Refresh::Appended
        );
        assert_eq!(records.values.len(), 1);

        std::fs::write(&path, "{\"type\":\"event_msg\"}\n").unwrap();
        assert_eq!(
            records
                .refresh_with_final_partial(&path, false, false)
                .unwrap(),
            Refresh::Reset
        );
        assert_eq!(records.values, vec![json!({"type":"event_msg"})]);

        std::fs::write(
            &path,
            "{\"type\":\"replacement_with_a_longer_prefix\"}\n{\"type\":\"second\"}\n",
        )
        .unwrap();
        assert_eq!(
            records
                .refresh_with_final_partial(&path, false, false)
                .unwrap(),
            Refresh::Reset
        );
        assert_eq!(
            records.values,
            vec![
                json!({"type":"replacement_with_a_longer_prefix"}),
                json!({"type":"second"})
            ]
        );
    }

    #[test]
    fn transcript_replacement_requests_a_persistent_translator_reset() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session.jsonl");
        std::fs::write(
            &path,
            "{\"timestamp\":\"2026-01-01T00:00:01Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"session\"}}\n",
        )
        .unwrap();
        let mut tail = TranscriptTail::new(path.clone(), ImportSource::Codex);
        assert!(!tail.poll(false).unwrap().is_empty());
        assert!(!tail.take_translator_reset());

        std::fs::write(
            &path,
            "{\"timestamp\":\"2026-01-01T00:00:01Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"session\",\"cwd\":\"/replacement\"}}\n",
        )
        .unwrap();
        let events = tail.poll(false).unwrap();
        assert_eq!(events.first().unwrap().event, "SessionStart");
        assert!(tail.take_translator_reset());
        assert!(!tail.take_translator_reset());
    }

    #[test]
    fn nonfinal_poll_retries_transient_related_transcript_errors() {
        let temp = tempfile::tempdir().unwrap();
        let parent = temp.path().join("rollout-parent.jsonl");
        let child = temp.path().join("rollout-child.jsonl");
        let parent_records = [
            json!({"timestamp":"2026-01-01T00:00:01Z","type":"session_meta","payload":{"id":"parent"}}),
            json!({"timestamp":"2026-01-01T00:00:02Z","type":"response_item","payload":{"type":"function_call","call_id":"call-1","name":"spawn_agent","arguments":"{}"}}),
            json!({"timestamp":"2026-01-01T00:00:03Z","type":"response_item","payload":{"type":"function_call_output","call_id":"call-1","output":"{\"agent_id\":\"child\"}"}}),
        ];
        std::fs::write(
            &parent,
            parent_records
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n")
                + "\n",
        )
        .unwrap();
        std::fs::write(&child, "{\"timestamp\":").unwrap();

        let mut tail = TranscriptTail::new(parent, ImportSource::Codex);
        assert!(tail.poll(false).unwrap().is_empty());
        assert!(tail.retry_envelopes);

        std::fs::write(
            child,
            "{\"timestamp\":\"2026-01-01T00:00:02Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"child\",\"source\":{\"subagent\":true}}}\n",
        )
        .unwrap();
        let events = tail.poll(false).unwrap();
        assert!(!events.is_empty());
        assert!(!tail.retry_envelopes);
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
