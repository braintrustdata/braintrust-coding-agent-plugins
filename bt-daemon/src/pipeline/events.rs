//! The event journal store: one append-only file per partition.
//!
//! Each line is `<node>\t<checksum>\t<json>\n`, where the checksum covers the
//! JSON bytes exactly as written. Appends are synced before returning, so an
//! acknowledged hook is never lost. Reopening truncates a torn final record;
//! corruption anywhere before it is a storage failure, never a skip.
//!
//! Pi `context` events repeat their entire, growing message history. As on
//! `main`, they are stored as a lossless delta from the previous context and
//! expanded again by readers, which therefore always read from the start.

use super::types::{NodeId, PartitionId, TranslationInput};
use crate::journal::{compact_pi_payload, expand_pi_payload, sanitize, MAX_FILE_NAME_BYTES};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io::{BufRead, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

pub(crate) fn events_dir(pipeline_dir: &Path) -> PathBuf {
    pipeline_dir.join("events")
}

/// The event journal file of one partition.
pub(crate) fn event_path(pipeline_dir: &Path, partition: &PartitionId) -> PathBuf {
    let prefix = format!("{}--", sanitize(&partition.source));
    let suffix = format!(
        "--{}.ndjson",
        crate::ids::session_storage_id(&partition.source, &partition.session_id)
    );
    let session = sanitize(&partition.session_id);
    let budget = MAX_FILE_NAME_BYTES.saturating_sub(prefix.len() + suffix.len());
    let session = &session[session.len().saturating_sub(budget)..];
    events_dir(pipeline_dir).join(format!("{prefix}{session}{suffix}"))
}

fn checksum(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

struct Line {
    node: NodeId,
    input: TranslationInput,
}

fn parse_line(path: &Path, line: &[u8]) -> anyhow::Result<Line> {
    let text = std::str::from_utf8(line)
        .map_err(|error| anyhow::anyhow!("{}: invalid UTF-8: {error}", path.display()))?;
    let mut parts = text.splitn(3, '\t');
    let (Some(node), Some(sum), Some(json)) = (parts.next(), parts.next(), parts.next()) else {
        anyhow::bail!("{}: malformed event record", path.display());
    };
    let node: NodeId = node
        .parse()
        .map_err(|error| anyhow::anyhow!("{}: invalid node id: {error}", path.display()))?;
    anyhow::ensure!(
        checksum(json.as_bytes()) == sum,
        "{}: checksum mismatch at node {node}",
        path.display()
    );
    let input = serde_json::from_str(json)
        .map_err(|error| anyhow::anyhow!("{}: node {node}: {error}", path.display()))?;
    Ok(Line { node, input })
}

fn expand(
    input: &mut TranslationInput,
    pi_context: &mut Vec<serde_json::Value>,
) -> anyhow::Result<()> {
    if let TranslationInput::Native(env) = input {
        expand_pi_payload(env, pi_context)?;
    }
    Ok(())
}

/// The writer side of one partition's event journal.
struct EventFile {
    file: std::fs::File,
    last_node: NodeId,
    pi_context: Vec<serde_json::Value>,
    changed: watch::Sender<NodeId>,
}

impl EventFile {
    fn open(path: &Path) -> anyhow::Result<Self> {
        if let Some(dir) = path.parent() {
            crate::paths::ensure_private_dir(dir)?;
        }
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;
        let mut last_node = 0;
        let mut pi_context = Vec::new();
        let mut good_len = 0u64;
        {
            file.seek(std::io::SeekFrom::Start(0))?;
            let mut reader = std::io::BufReader::new(&file);
            let mut offset = 0u64;
            loop {
                let mut bytes = Vec::new();
                let read = reader.read_until(b'\n', &mut bytes)?;
                if read == 0 {
                    break;
                }
                if bytes.last() != Some(&b'\n') {
                    // A crash tears an append mid-record: only an unterminated
                    // final record is dropped. A complete record that does not
                    // verify is corruption, however late in the file.
                    break;
                }
                let mut line = parse_line(path, &bytes[..bytes.len() - 1])?;
                anyhow::ensure!(
                    line.node == last_node + 1,
                    "{}: expected node {}, found {}",
                    path.display(),
                    last_node + 1,
                    line.node
                );
                expand(&mut line.input, &mut pi_context)?;
                last_node = line.node;
                offset += read as u64;
                good_len = offset;
            }
        }
        if file.metadata()?.len() > good_len {
            tracing::warn!(path = %path.display(), last_node, "truncating torn event journal tail");
            // The append handle cannot truncate on every platform.
            let truncate = std::fs::OpenOptions::new().write(true).open(path)?;
            truncate.set_len(good_len)?;
            truncate.sync_all()?;
        }
        let (changed, _) = watch::channel(last_node);
        Ok(Self {
            file,
            last_node,
            pi_context,
            changed,
        })
    }

    fn append(&mut self, input: &TranslationInput) -> anyhow::Result<NodeId> {
        let mut stored = input.clone();
        if let TranslationInput::Native(env) = &mut stored {
            compact_pi_payload(env, &mut self.pi_context);
        }
        let json = serde_json::to_string(&stored)?;
        let node = self.last_node + 1;
        let line = format!("{node}\t{}\t{json}\n", checksum(json.as_bytes()));
        self.file.write_all(line.as_bytes())?;
        self.file.sync_data()?;
        self.last_node = node;
        self.changed.send_replace(node);
        Ok(node)
    }
}

/// All event journals of this daemon.
pub(crate) struct EventStore {
    dir: PathBuf,
    files: Mutex<HashMap<PartitionId, Arc<Mutex<EventFile>>>>,
}

impl EventStore {
    pub(crate) fn new(pipeline_dir: &Path) -> Arc<Self> {
        Arc::new(Self {
            dir: pipeline_dir.to_path_buf(),
            files: Mutex::new(HashMap::new()),
        })
    }

    pub(crate) fn path(&self, partition: &PartitionId) -> PathBuf {
        event_path(&self.dir, partition)
    }

    fn file(&self, partition: &PartitionId) -> anyhow::Result<Arc<Mutex<EventFile>>> {
        let mut files = self.files.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(file) = files.get(partition) {
            return Ok(file.clone());
        }
        let file = Arc::new(Mutex::new(EventFile::open(&self.path(partition))?));
        files.insert(partition.clone(), file.clone());
        Ok(file)
    }

    /// Durably append one input and return its node id.
    pub(crate) async fn append(
        self: &Arc<Self>,
        partition: &PartitionId,
        input: TranslationInput,
    ) -> anyhow::Result<NodeId> {
        let store = self.clone();
        let partition = partition.clone();
        tokio::task::spawn_blocking(move || {
            let file = store.file(&partition)?;
            let mut file = file.lock().unwrap_or_else(|e| e.into_inner());
            file.append(&input)
        })
        .await
        .map_err(|error| anyhow::anyhow!("event append task failed: {error}"))?
    }

    /// The last node appended to a partition's journal, or 0.
    pub(crate) async fn tail(self: &Arc<Self>, partition: &PartitionId) -> anyhow::Result<NodeId> {
        let store = self.clone();
        let partition = partition.clone();
        tokio::task::spawn_blocking(move || {
            let file = store.file(&partition)?;
            let file = file.lock().unwrap_or_else(|e| e.into_inner());
            Ok(file.last_node)
        })
        .await
        .map_err(|error| anyhow::anyhow!("event tail task failed: {error}"))?
    }

    pub(crate) fn subscribe(
        &self,
        partition: &PartitionId,
    ) -> anyhow::Result<watch::Receiver<NodeId>> {
        let file = self.file(partition)?;
        let file = file.lock().unwrap_or_else(|e| e.into_inner());
        Ok(file.changed.subscribe())
    }

    /// Delete a partition's journal. Retention only.
    pub(crate) fn delete(&self, partition: &PartitionId) -> anyhow::Result<()> {
        self.files
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(partition);
        match std::fs::remove_file(self.path(partition)) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    /// Release the open file handle of an idle partition.
    pub(crate) fn release(&self, partition: &PartitionId) {
        self.files
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(partition);
    }

    /// Partitions that have an event journal on disk but are not in `known`.
    /// The journal names its own partition in its first native event, so a
    /// lost index can be rebuilt from the journals alone.
    pub(crate) fn unindexed_partitions(&self, known: &[PartitionId]) -> Vec<PartitionId> {
        let known: std::collections::HashSet<PathBuf> =
            known.iter().map(|partition| self.path(partition)).collect();
        let Ok(entries) = std::fs::read_dir(events_dir(&self.dir)) else {
            return Vec::new();
        };
        let mut found = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("ndjson")
                || known.contains(&path)
            {
                continue;
            }
            let mut cursor = EventCursor {
                path: path.clone(),
                reader: None,
                next_node: 1,
                pi_context: Vec::new(),
            };
            while let Ok(Some((_, input))) = cursor.read_one() {
                if let TranslationInput::Native(env) = input {
                    let partition = PartitionId::new(&env.source, &env.session_id);
                    if self.path(&partition) == path {
                        found.push(partition);
                    }
                    break;
                }
            }
        }
        found
    }

    pub(crate) fn cursor(&self, partition: &PartitionId) -> EventCursor {
        EventCursor {
            path: self.path(partition),
            reader: None,
            next_node: 1,
            pi_context: Vec::new(),
        }
    }
}

/// A sequential reader over one event journal. Pi deltas are expanded in
/// order, so seeking backwards restarts from the beginning.
pub(crate) struct EventCursor {
    path: PathBuf,
    reader: Option<std::io::BufReader<std::fs::File>>,
    next_node: NodeId,
    pi_context: Vec<serde_json::Value>,
}

impl EventCursor {
    /// Position the cursor so the next read returns `node`.
    pub(crate) fn seek(&mut self, node: NodeId) -> anyhow::Result<()> {
        if node < self.next_node {
            self.reader = None;
            self.next_node = 1;
            self.pi_context.clear();
        }
        while self.next_node < node {
            if self.read_one()?.is_none() {
                break;
            }
        }
        Ok(())
    }

    /// Read the next complete record, or `None` when caught up.
    pub(crate) fn read_one(&mut self) -> anyhow::Result<Option<(NodeId, TranslationInput)>> {
        if self.reader.is_none() {
            let file = match std::fs::File::open(&self.path) {
                Ok(file) => file,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(error) => return Err(error.into()),
            };
            self.reader = Some(std::io::BufReader::new(file));
        }
        let reader = self.reader.as_mut().expect("reader opened");
        let position = reader.stream_position()?;
        let mut bytes = Vec::new();
        let read = reader.read_until(b'\n', &mut bytes)?;
        if read == 0 || bytes.last() != Some(&b'\n') {
            // A partial line is an append in progress; read it again later.
            reader.seek(std::io::SeekFrom::Start(position))?;
            return Ok(None);
        }
        let mut line = parse_line(&self.path, &bytes[..bytes.len() - 1])?;
        anyhow::ensure!(
            line.node == self.next_node,
            "{}: expected node {}, found {}",
            self.path.display(),
            self.next_node,
            line.node
        );
        expand(&mut line.input, &mut self.pi_context)?;
        self.next_node += 1;
        Ok(Some((line.node, line.input)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::types::{FinalizeKind, TranslationControl};
    use crate::wire::RedactedEnvelope;

    fn native(event: &str) -> TranslationInput {
        TranslationInput::Native(Box::new(RedactedEnvelope {
            source: "codex".into(),
            source_version: None,
            plugin_version: None,
            session_id: "s".into(),
            event: event.into(),
            ts_ms: 1,
            managed_run_id: None,
            capture: None,
            payload: serde_json::json!({"event": event}),
            route: None,
        }))
    }

    #[tokio::test]
    async fn appends_and_reads_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let store = EventStore::new(dir.path());
        let partition = PartitionId::new("codex", "s");
        assert_eq!(store.append(&partition, native("a")).await.unwrap(), 1);
        assert_eq!(
            store
                .append(
                    &partition,
                    TranslationInput::Control(TranslationControl::Finalize {
                        barrier: "b".into(),
                        kind: FinalizeKind::Retire,
                    })
                )
                .await
                .unwrap(),
            2
        );
        let mut cursor = store.cursor(&partition);
        cursor.seek(2).unwrap();
        let (node, input) = cursor.read_one().unwrap().unwrap();
        assert_eq!(node, 2);
        assert!(matches!(input, TranslationInput::Control(_)));
        assert!(cursor.read_one().unwrap().is_none());
        cursor.seek(1).unwrap();
        assert_eq!(cursor.read_one().unwrap().unwrap().0, 1);
    }

    #[tokio::test]
    async fn truncates_only_a_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let partition = PartitionId::new("codex", "torn");
        let path = event_path(dir.path(), &partition);
        {
            let store = EventStore::new(dir.path());
            store.append(&partition, native("a")).await.unwrap();
        }
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        file.write_all(b"2\tdeadbeef\t{\"nat").unwrap();
        drop(file);
        let store = EventStore::new(dir.path());
        assert_eq!(store.tail(&partition).await.unwrap(), 1);
        assert_eq!(store.append(&partition, native("b")).await.unwrap(), 2);

        // Corruption before the final record is never skipped.
        let text = std::fs::read_to_string(&path).unwrap();
        std::fs::write(&path, text.replacen("\"a\"", "\"x\"", 1)).unwrap();
        let store = EventStore::new(dir.path());
        assert!(store.tail(&partition).await.is_err());
    }

    #[tokio::test]
    async fn a_complete_corrupt_final_record_is_corruption_not_a_torn_write() {
        let dir = tempfile::tempdir().unwrap();
        let partition = PartitionId::new("codex", "corrupt-tail");
        let path = event_path(dir.path(), &partition);
        {
            let store = EventStore::new(dir.path());
            store.append(&partition, native("a")).await.unwrap();
            store.append(&partition, native("b")).await.unwrap();
        }
        let before = std::fs::read_to_string(&path).unwrap();
        for corrupt in [
            before.replacen("\"b\"", "\"x\"", 1),
            format!("{}3\tdeadbeef\t{{}}\n", before),
            before.replacen("2\t", "5\t", 1),
        ] {
            std::fs::write(&path, &corrupt).unwrap();
            let store = EventStore::new(dir.path());
            assert!(store.tail(&partition).await.is_err(), "{corrupt}");
            assert_eq!(
                std::fs::read_to_string(&path).unwrap(),
                corrupt,
                "a corrupt journal is reported, never truncated"
            );
        }
    }

    fn pi_input(event: &str, ts_ms: i64, payload: serde_json::Value) -> TranslationInput {
        TranslationInput::Native(Box::new(RedactedEnvelope {
            source: "pi".into(),
            source_version: None,
            plugin_version: None,
            session_id: "pi-session".into(),
            event: event.into(),
            ts_ms,
            managed_run_id: None,
            capture: None,
            payload,
            route: None,
        }))
    }

    fn read_payloads(store: &Arc<EventStore>, partition: &PartitionId) -> Vec<serde_json::Value> {
        let mut cursor = store.cursor(partition);
        let mut payloads = Vec::new();
        while let Some((_, input)) = cursor.read_one().unwrap() {
            let TranslationInput::Native(env) = input else {
                panic!("expected a native event");
            };
            payloads.push(env.payload);
        }
        payloads
    }

    #[tokio::test]
    async fn pi_context_history_is_delta_encoded_and_replayed() {
        let dir = tempfile::tempdir().unwrap();
        let store = EventStore::new(dir.path());
        let partition = PartitionId::new("pi", "pi-session");
        let context = |messages: &[serde_json::Value]| serde_json::json!({"event": {"type": "context", "messages": messages}});
        let mut messages = Vec::new();
        let mut naive_bytes = 0usize;
        for index in 0..20 {
            messages.push(serde_json::json!({
                "role": "user",
                "content": format!("{index}:{}", "x".repeat(4096)),
            }));
            let payload = context(&messages);
            naive_bytes += serde_json::to_vec(&payload).unwrap().len() + 1;
            store
                .append(&partition, pi_input("context", index, payload))
                .await
                .unwrap();
        }
        let compacted = vec![
            serde_json::json!({"role": "compactionSummary", "summary": "bounded"}),
            serde_json::json!({"role": "user", "content": "after"}),
        ];
        let payload = context(&compacted);
        naive_bytes += serde_json::to_vec(&payload).unwrap().len() + 1;
        store
            .append(&partition, pi_input("context", 20, payload))
            .await
            .unwrap();

        let stored = std::fs::read(store.path(&partition)).unwrap();
        assert!(
            stored.len() < naive_bytes / 3,
            "Pi journal did not eliminate cumulative context copies: stored={} naive={naive_bytes}",
            stored.len()
        );
        assert!(!String::from_utf8_lossy(&stored).contains("\"messages\""));

        let contexts: Vec<_> = read_payloads(&store, &partition)
            .into_iter()
            .map(|payload| {
                payload
                    .pointer("/event/messages")
                    .and_then(serde_json::Value::as_array)
                    .cloned()
                    .unwrap()
            })
            .collect();
        assert_eq!(contexts.len(), 21);
        assert_eq!(contexts[19].len(), 20);
        assert_eq!(contexts[20], compacted);
    }

    #[tokio::test]
    async fn pi_streaming_updates_do_not_persist_growing_partial_messages() {
        let dir = tempfile::tempdir().unwrap();
        let store = EventStore::new(dir.path());
        let partition = PartitionId::new("pi", "pi-session");
        let marker = "growing-partial-marker".repeat(4096);
        store
            .append(
                &partition,
                pi_input(
                    "message_update",
                    1,
                    serde_json::json!({
                        "event": {
                            "type": "message_update",
                            "assistantMessageEvent": {"type": "text_delta", "partial": marker},
                            "message": {"role": "assistant", "content": marker},
                        }
                    }),
                ),
            )
            .await
            .unwrap();
        let stored = std::fs::read(store.path(&partition)).unwrap();
        assert!(!String::from_utf8_lossy(&stored).contains("growing-partial-marker"));
        let replayed = read_payloads(&store, &partition);
        assert_eq!(
            replayed[0].pointer("/event/assistantMessageEvent/type"),
            Some(&serde_json::json!("text_delta"))
        );
    }

    #[tokio::test]
    async fn pi_provider_requests_persist_only_request_configuration() {
        let dir = tempfile::tempdir().unwrap();
        let store = EventStore::new(dir.path());
        let partition = PartitionId::new("pi", "pi-session");
        store
            .append(
                &partition,
                pi_input(
                    "before_provider_request",
                    1,
                    serde_json::json!({
                        "event": {
                            "type": "before_provider_request",
                            "payload": {
                                "system": "system-prompt-marker",
                                "messages": [{"role": "user", "content": "message-marker"}],
                                "tools": [{"name": "read", "input_schema": {"type": "object"}}],
                                "max_tokens": 1024,
                            },
                        }
                    }),
                ),
            )
            .await
            .unwrap();
        let stored = std::fs::read(store.path(&partition)).unwrap();
        let stored_text = String::from_utf8_lossy(&stored);
        assert!(!stored_text.contains("system-prompt-marker"));
        assert!(!stored_text.contains("message-marker"));
        let replayed = read_payloads(&store, &partition);
        assert_eq!(
            replayed[0].pointer("/event/payload"),
            Some(&serde_json::json!({
                "max_tokens": 1024,
                "tools": [{"type": "function", "function": {
                    "name": "read",
                    "parameters": {"type": "object"},
                }}],
            }))
        );
    }
}
