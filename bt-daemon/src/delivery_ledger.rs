//! Destination-scoped completed-span delivery ledger.
//!
//! Hook journals prevent a daemon recovery from re-emitting already flushed
//! observations. Imports are a second ingress, though: they synthesize the
//! same source session without using that journal. This ledger records which
//! completed span rows a destination has already accepted, so every ingress can
//! avoid re-reporting any later partial merge for a completed span while a new
//! destination still receives the full trace.

use crate::sink::Sink;
use crate::translate::{OriginSnapshot, SpanOp, SpanRow};
use crate::wire::SessionConfig;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, AsyncWriteExt};

#[derive(serde::Serialize, serde::Deserialize, Default)]
struct LedgerFile {
    #[serde(
        default,
        alias = "terminal_span_ids",
        skip_serializing_if = "HashSet::is_empty"
    )]
    completed_span_ids: HashSet<String>,
    #[serde(
        default,
        alias = "late_merge_span_ids",
        skip_serializing_if = "HashSet::is_empty"
    )]
    late_merge_ids: HashSet<String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    span_origins: HashMap<String, OriginSnapshot>,
}

// Closing this file releases the OS lock, including on cancellation or errors.
struct LedgerLock {
    _file: std::fs::File,
}

struct DeliveryLedger {
    path: PathBuf,
    journal_len: u64,
    known: HashSet<String>,
    pending: HashSet<String>,
    known_late_merges: HashSet<String>,
    pending_late_merges: HashSet<String>,
}

impl DeliveryLedger {
    async fn load(
        data_dir: &Path,
        source: &str,
        session_id: &str,
        config: &SessionConfig,
    ) -> anyhow::Result<(Self, HashMap<String, OriginSnapshot>)> {
        let fingerprint = serde_json::json!({
            "api_url": config.auth.api_url,
            "org_id": config.auth.org_id,
            "org_name": config.auth.org_name,
            "destination": config.destination,
        });
        let digest = Sha256::digest(serde_json::to_vec(&fingerprint)?);
        let fingerprint_id = format!("{digest:x}");
        let path = data_dir.join("delivery-ledger").join(format!(
            "{}--{}.json",
            crate::ids::session_storage_id(source, session_id),
            &fingerprint_id[..32]
        ));
        // Retain the old atomic JSON checkpoint as the base for upgrades.
        let mut persisted = match tokio::fs::read(&path).await {
            Ok(bytes) => serde_json::from_slice::<LedgerFile>(&bytes)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => LedgerFile::default(),
            Err(error) => return Err(error.into()),
        };
        let mut ledger = Self {
            path: path.with_extension("ndjson"),
            journal_len: 0,
            known: persisted.completed_span_ids,
            pending: HashSet::new(),
            known_late_merges: persisted.late_merge_ids,
            pending_late_merges: HashSet::new(),
        };
        let lock = ledger.lock().await?;
        ledger
            .refresh(&mut persisted.span_origins, &mut HashMap::new(), &lock)
            .await?;
        Ok((ledger, persisted.span_origins))
    }

    async fn lock(&self) -> anyhow::Result<LedgerLock> {
        // The stable sidecar protects reads, tail repair, and writes together.
        // Acquire on the blocking pool so a competing process cannot block a
        // Tokio executor thread. Keep ledger I/O on separate handles for Windows.
        let path = self.path.with_extension("lock");
        tokio::task::spawn_blocking(move || {
            let parent = path
                .parent()
                .ok_or_else(|| anyhow::anyhow!("ledger lock has no parent"))?;
            std::fs::create_dir_all(parent)?;
            let file = std::fs::OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(false)
                .open(path)?;
            fs2::FileExt::lock_exclusive(&file)?;
            Ok(LedgerLock { _file: file })
        })
        .await?
    }

    async fn refresh(
        &mut self,
        origins: &mut HashMap<String, OriginSnapshot>,
        pending_origins: &mut HashMap<String, OriginSnapshot>,
        _lock: &LedgerLock,
    ) -> anyhow::Result<()> {
        let file = match tokio::fs::File::open(&self.path).await {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && self.journal_len == 0 => {
                return Ok(())
            }
            Err(error) => return Err(error.into()),
        };
        anyhow::ensure!(
            file.metadata().await?.len() >= self.journal_len,
            "delivery ledger was truncated outside its lock"
        );
        let mut reader = tokio::io::BufReader::new(file);
        reader
            .seek(std::io::SeekFrom::Start(self.journal_len))
            .await?;
        let mut line = Vec::new();
        loop {
            line.clear();
            let read = reader.read_until(b'\n', &mut line).await?;
            if read == 0 || line.last() != Some(&b'\n') {
                // An incomplete record is safe to repair while holding the lock:
                // it cannot belong to another writer's in-progress append.
                break;
            }
            let update: LedgerFile = serde_json::from_slice(&line)?;
            self.known.extend(update.completed_span_ids);
            self.known_late_merges.extend(update.late_merge_ids);
            for (span, origin) in update.span_origins {
                if pending_origins.remove(&span).is_some() {
                    // Another writer persisted first while this writer's
                    // candidate snapshot was waiting to be retried.
                    origins.insert(span, origin);
                } else {
                    origins.entry(span).or_insert(origin);
                }
            }
            self.journal_len += read as u64;
        }
        Ok(())
    }

    fn should_emit(&self, op: &SpanOp) -> bool {
        let row = match op {
            SpanOp::Insert(row) | SpanOp::Merge(row) => row,
        };
        if let Some(key) = &row.late_merge_key {
            let id = late_merge_id(row, key);
            !self.known_late_merges.contains(&id) && !self.pending_late_merges.contains(&id)
        } else {
            !self.known.contains(&row.span_id) && !self.pending.contains(&row.span_id)
        }
    }

    fn filter(&self, ops: &[SpanOp]) -> Vec<SpanOp> {
        ops.iter()
            .filter(|op| self.should_emit(op))
            .cloned()
            .collect()
    }

    fn record_emitted(&mut self, ops: &[SpanOp]) {
        for op in ops {
            let row = match op {
                SpanOp::Insert(row) | SpanOp::Merge(row) => row,
            };
            if row.end_ms.is_some() {
                self.pending.insert(row.span_id.clone());
            }
            if let Some(key) = &row.late_merge_key {
                self.pending_late_merges.insert(late_merge_id(row, key));
            }
        }
    }

    async fn commit(&mut self, lock: &LedgerLock) -> anyhow::Result<()> {
        self.pending.retain(|id| !self.known.contains(id));
        self.pending_late_merges
            .retain(|id| !self.known_late_merges.contains(id));
        if self.pending.is_empty() && self.pending_late_merges.is_empty() {
            return Ok(());
        }
        // Only new acknowledgements are serialized. Retain pending state until
        // the write succeeds so a failed append can be retried on the next flush.
        self.append(
            &LedgerFile {
                completed_span_ids: self.pending.clone(),
                late_merge_ids: self.pending_late_merges.clone(),
                ..Default::default()
            },
            lock,
        )
        .await?;
        self.known.extend(self.pending.drain());
        self.known_late_merges
            .extend(self.pending_late_merges.drain());
        Ok(())
    }

    async fn append(&mut self, update: &LedgerFile, _lock: &LedgerLock) -> anyhow::Result<()> {
        let parent = self.path.parent().ok_or_else(|| {
            anyhow::anyhow!(
                "delivery ledger path has no parent: {}",
                self.path.display()
            )
        })?;
        tokio::fs::create_dir_all(parent).await?;
        let mut line = serde_json::to_vec(update)?;
        line.push(b'\n');
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&self.path)
            .await?;
        // Remove an incomplete tail from recovery or a failed write before
        // retrying. Successful prior records are never rewritten.
        file.set_len(self.journal_len).await?;
        file.seek(std::io::SeekFrom::Start(self.journal_len))
            .await?;
        file.write_all(&line).await?;
        // Match the event journal's process-crash durability: flush to the OS
        // before delivery without fsync on every hook event.
        file.flush().await?;
        self.journal_len += line.len() as u64;
        Ok(())
    }
}

fn late_merge_id(row: &SpanRow, key: &str) -> String {
    format!("{}\u{1f}{key}", row.span_id)
}

/// The backend accepted delivery; only the local ledger write needs retrying.
#[derive(Debug)]
pub(crate) struct CommitFailure(pub anyhow::Error);
impl std::fmt::Display for CommitFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for CommitFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

/// Wrap a sink with destination-scoped terminal-span suppression. If durable
/// ledger state cannot be loaded, retain input until its recovery condition is met.
pub(crate) struct LedgerSink {
    inner: Box<dyn Sink>,
    ledger: Option<DeliveryLedger>,
    span_origins: HashMap<String, OriginSnapshot>,
    pending_origins: HashMap<String, OriginSnapshot>,
    plugin_version: Option<String>,
    source_version: Option<String>,
    bt_version: String,
    attached_parent: Option<String>,
}

impl LedgerSink {
    pub(crate) async fn new(
        inner: Box<dyn Sink>,
        data_dir: Option<&Path>,
        source: &str,
        session_id: &str,
        config: Option<&SessionConfig>,
        bt_version: &str,
    ) -> anyhow::Result<Self> {
        let loaded = match (data_dir, config) {
            (Some(data_dir), Some(config)) if config.destination.is_some() => {
                Some(DeliveryLedger::load(data_dir, source, session_id, config).await?)
            }
            _ => None,
        };
        let (ledger, span_origins) = match loaded {
            Some((ledger, origins)) => (Some(ledger), origins),
            None => (None, HashMap::new()),
        };
        Ok(Self {
            inner,
            ledger,
            span_origins,
            pending_origins: HashMap::new(),
            plugin_version: None,
            source_version: None,
            bt_version: bt_version.to_owned(),
            attached_parent: config.and_then(|config| config.attached_span_ids().0),
        })
    }
}

#[async_trait::async_trait]
impl Sink for LedgerSink {
    fn configure(&mut self, config: &SessionConfig) {
        self.inner.configure(config);
        self.attached_parent = config.attached_span_ids().0;
    }

    fn set_capture_versions(&mut self, plugin_version: Option<&str>, source_version: Option<&str>) {
        if self.plugin_version.as_deref() != plugin_version {
            self.plugin_version = plugin_version.map(str::to_owned);
        }
        if self.source_version.as_deref() != source_version {
            self.source_version = source_version.map(str::to_owned);
        }
    }

    async fn emit(&mut self, ops: &[SpanOp]) -> anyhow::Result<u64> {
        let mut filtered = self
            .ledger
            .as_ref()
            .map(|ledger| ledger.filter(ops))
            .unwrap_or_else(|| ops.to_vec());
        if filtered.is_empty() {
            return Ok(0);
        }
        for op in &mut filtered {
            let is_insert = matches!(op, SpanOp::Insert(_));
            let row = match op {
                SpanOp::Insert(row) | SpanOp::Merge(row) => row,
            };
            // Every span emitted by the integration has plugin provenance,
            // including LLM, tool, compaction, and subagent spans. Never stamp
            // an external parent that the session merely attaches to.
            if is_insert && self.attached_parent.as_deref() != Some(row.span_id.as_str()) {
                if let std::collections::hash_map::Entry::Vacant(entry) =
                    self.span_origins.entry(row.span_id.clone())
                {
                    let origin = row.origin.clone().unwrap_or_else(|| OriginSnapshot {
                        plugin_version: self.plugin_version.clone(),
                        bt_version: self.bt_version.clone(),
                        source_version: self.source_version.clone(),
                    });
                    if self.ledger.is_some() {
                        self.pending_origins
                            .insert(row.span_id.clone(), origin.clone());
                    }
                    entry.insert(origin);
                }
            }
        }
        // Retain this event's candidate versions even if persistence fails, but
        // reconcile them with authoritative snapshots before any row is sent.
        let lock = if let Some(ledger) = &mut self.ledger {
            let lock = ledger.lock().await?;
            ledger
                .refresh(&mut self.span_origins, &mut self.pending_origins, &lock)
                .await?;
            self.pending_origins.retain(|span, _| {
                if ledger.known.contains(span) {
                    // A peer may have acknowledged a legacy span without an
                    // origin. Do not invent provenance for an already-saved row.
                    self.span_origins.remove(span);
                    false
                } else {
                    true
                }
            });
            filtered.retain(|op| ledger.should_emit(op));
            Some(lock)
        } else {
            None
        };
        if filtered.is_empty() {
            return Ok(0);
        }
        for op in &mut filtered {
            let row = match op {
                SpanOp::Insert(row) | SpanOp::Merge(row) => row,
            };
            // Repeat each span's birth snapshot, including stateless updates.
            // This also discards provenance supplied by a span-processing plugin.
            row.origin = self.span_origins.get(&row.span_id).cloned();
        }
        if !self.pending_origins.is_empty() {
            // Persist birth versions before the SDK can queue a span. A crash
            // between delivery and its checkpoint must not change its provenance.
            // Do not acknowledge pending span deliveries before the sink flushes.
            if let (Some(ledger), Some(lock)) = (&mut self.ledger, &lock) {
                ledger
                    .append(
                        &LedgerFile {
                            span_origins: self.pending_origins.clone(),
                            ..Default::default()
                        },
                        lock,
                    )
                    .await?;
            }
            self.pending_origins.clear();
        }
        // Never hold a cross-process file lock over SDK/network delivery.
        drop(lock);
        let emitted = self.inner.emit(&filtered).await?;
        // A deferred lifecycle root has not reached the backend yet. Do not
        // record it as delivered: a subsequent cold-worker replay needs to
        // retain the root until descendant work makes it exportable.
        if emitted > 0 {
            if let Some(ledger) = &mut self.ledger {
                ledger.record_emitted(&filtered);
            }
        }
        Ok(emitted)
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        self.inner.flush().await?;
        if let Some(ledger) = &mut self.ledger {
            async {
                let lock = ledger.lock().await?;
                ledger
                    .refresh(&mut self.span_origins, &mut self.pending_origins, &lock)
                    .await?;
                ledger.commit(&lock).await
            }
            .await
            .map_err(|error| anyhow::Error::new(CommitFailure(error)))?;
        }
        Ok(())
    }

    fn has_pending_delivery(&self) -> bool {
        self.inner.has_pending_delivery()
    }

    fn permalink(&self) -> Option<String> {
        self.inner.permalink()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::translate::{SpanRow, SpanType};
    use crate::wire::{BackendAuth, FlushMode, TraceDestination};
    use parking_lot::Mutex;
    use std::sync::Arc;

    #[derive(Default)]
    struct RecordingSink {
        emitted: Arc<Mutex<Vec<SpanOp>>>,
    }

    #[async_trait::async_trait]
    impl Sink for RecordingSink {
        async fn emit(&mut self, ops: &[SpanOp]) -> anyhow::Result<u64> {
            self.emitted.lock().extend_from_slice(ops);
            Ok(ops.len() as u64)
        }

        async fn flush(&mut self) -> anyhow::Result<()> {
            Ok(())
        }
    }

    fn config(project_id: &str) -> SessionConfig {
        SessionConfig {
            auth: BackendAuth {
                token: "test".into(),
                api_url: Some("https://api.example.test".into()),
                app_url: None,
                org_name: Some("test-org".into()),
                org_id: Some("org-id".into()),
            },
            destination: Some(TraceDestination::ProjectLogs {
                project_id: Some(project_id.into()),
                project_name: None,
            }),
            flush_mode: FlushMode::FireAndForget,
            additional_metadata: None,
            tags: Vec::new(),
            span_plugins: Vec::new(),
        }
    }

    fn terminal(span_id: &str) -> SpanOp {
        SpanOp::Insert(SpanRow {
            span_id: span_id.into(),
            root_span_id: "root".into(),
            end_ms: Some(1),
            ..Default::default()
        })
    }

    fn partial_merge(span_id: &str) -> SpanOp {
        SpanOp::Merge(SpanRow {
            span_id: span_id.into(),
            root_span_id: "root".into(),
            ..Default::default()
        })
    }

    fn late_merge(span_id: &str, key: &str) -> SpanOp {
        SpanOp::Merge(SpanRow {
            span_id: span_id.into(),
            root_span_id: "root".into(),
            late_merge_key: Some(key.into()),
            ..Default::default()
        })
    }

    fn late_terminal_merge(span_id: &str, key: &str, end_ms: i64) -> SpanOp {
        SpanOp::Merge(SpanRow {
            span_id: span_id.into(),
            root_span_id: "root".into(),
            end_ms: Some(end_ms),
            late_merge_key: Some(key.into()),
            ..Default::default()
        })
    }

    #[tokio::test]
    async fn post_acceptance_ledger_failures_retain_pending_completion() {
        for phase in ["lock", "refresh", "append"] {
            let tmp = tempfile::tempdir().unwrap();
            let config = config("accepted");
            let mut sink = LedgerSink::new(
                Box::new(RecordingSink::default()),
                Some(tmp.path()),
                "debug",
                "test",
                Some(&config),
                "test",
            )
            .await
            .unwrap();
            sink.emit(&[terminal("one")]).await.unwrap();
            let path = sink.ledger.as_ref().unwrap().path.clone();
            let original = std::fs::read(&path).unwrap();
            let permissions = std::fs::metadata(&path).unwrap().permissions();
            match phase {
                "lock" => {
                    std::fs::remove_file(path.with_extension("lock")).unwrap();
                    std::fs::create_dir(path.with_extension("lock")).unwrap();
                }
                "refresh" => {
                    use std::io::Write;
                    std::fs::OpenOptions::new()
                        .append(true)
                        .open(&path)
                        .unwrap()
                        .write_all(b"invalid JSON\n")
                        .unwrap();
                }
                "append" => {
                    let mut readonly = permissions.clone();
                    readonly.set_readonly(true);
                    std::fs::set_permissions(&path, readonly).unwrap();
                }
                _ => unreachable!(),
            }
            let error = sink.flush().await.unwrap_err();
            assert!(
                error.downcast_ref::<CommitFailure>().is_some(),
                "{phase}: {error:#}"
            );
            assert!(sink.ledger.as_ref().unwrap().pending.contains("one"));
            match phase {
                "lock" => std::fs::remove_dir(path.with_extension("lock")).unwrap(),
                "refresh" => std::fs::write(&path, &original).unwrap(),
                "append" => std::fs::set_permissions(&path, permissions).unwrap(),
                _ => unreachable!(),
            }
            sink.flush().await.unwrap();
            let (restored, _) = DeliveryLedger::load(tmp.path(), "debug", "test", &config)
                .await
                .unwrap();
            assert!(restored.known.contains("one"), "{phase}");
        }
    }

    #[tokio::test]
    async fn open_span_snapshots_survive_recovery_before_delivery_checkpoint() {
        let temp = tempfile::tempdir().unwrap();
        let config = config("project-a");
        let mut first = LedgerSink::new(
            Box::new(RecordingSink::default()),
            Some(temp.path()),
            "codex",
            "session-1",
            Some(&config),
            "bt1",
        )
        .await
        .unwrap();
        first.set_capture_versions(None, Some("native1"));
        let turn = SpanRow {
            span_id: "turn-1".into(),
            root_span_id: "root".into(),
            parent_span_ids: vec!["root".into()],
            is_turn: true,
            ..Default::default()
        };
        let llm = SpanRow {
            span_id: "llm".into(),
            span_type: SpanType::Llm,
            is_turn: false,
            ..turn.clone()
        };
        let tool = SpanRow {
            span_id: "tool".into(),
            span_type: SpanType::Tool,
            ..llm.clone()
        };
        first
            .emit(&[
                SpanOp::Insert(turn.clone()),
                SpanOp::Insert(llm.clone()),
                SpanOp::Insert(tool.clone()),
            ])
            .await
            .unwrap();
        let ledger_path = first.ledger.as_ref().unwrap().path.clone();
        let before_crash = tokio::fs::read(&ledger_path).await.unwrap();
        // Lose in-memory state before delivery checkpointing and interrupt the
        // next append. Recovery must retain complete records and repair the tail.
        drop(first);
        let mut interrupted = before_crash.clone();
        interrupted.extend_from_slice(b"{\"span_origins\":");
        tokio::fs::write(&ledger_path, interrupted).await.unwrap();

        let output = Arc::new(Mutex::new(Vec::new()));
        let mut recovered = LedgerSink::new(
            Box::new(RecordingSink {
                emitted: output.clone(),
            }),
            Some(temp.path()),
            "codex",
            "session-1",
            Some(&config),
            "bt2",
        )
        .await
        .unwrap();
        recovered.set_capture_versions(Some("v2"), Some("native2"));
        recovered
            .emit(&[
                SpanOp::Insert(turn.clone()),
                partial_merge("turn-1"),
                SpanOp::Insert(llm),
                partial_merge("llm"),
                SpanOp::Insert(tool),
                partial_merge("tool"),
                SpanOp::Insert(SpanRow {
                    span_id: "turn-2".into(),
                    ..turn.clone()
                }),
                SpanOp::Insert(SpanRow {
                    span_id: "subagent-container".into(),
                    is_turn: false,
                    ..turn
                }),
            ])
            .await
            .unwrap();
        let after_recovery = tokio::fs::read(&ledger_path).await.unwrap();
        assert!(after_recovery.starts_with(&before_crash));
        let new_origins: LedgerFile =
            serde_json::from_slice(&after_recovery[before_crash.len()..]).unwrap();
        assert_eq!(new_origins.span_origins.len(), 2);
        assert!(new_origins.span_origins.contains_key("turn-2"));
        assert!(new_origins.span_origins.contains_key("subagent-container"));
        let rows = output.lock();
        assert_eq!(rows.len(), 8);
        for op in rows.iter() {
            let row = match op {
                SpanOp::Insert(row) | SpanOp::Merge(row) => row,
            };
            let expected = match row.span_id.as_str() {
                "turn-1" | "llm" | "tool" => Some(OriginSnapshot {
                    plugin_version: None,
                    source_version: Some("native1".into()),
                    bt_version: "bt1".into(),
                }),
                "turn-2" | "subagent-container" => Some(OriginSnapshot {
                    plugin_version: Some("v2".into()),
                    source_version: Some("native2".into()),
                    bt_version: "bt2".into(),
                }),
                unexpected => panic!("unexpected span: {unexpected}"),
            };
            assert_eq!(row.origin, expected, "{}", row.span_id);
        }
    }

    #[tokio::test]
    async fn ledger_appends_only_new_spans_and_retries_before_delivery() {
        let temp = tempfile::tempdir().unwrap();
        let output = Arc::new(Mutex::new(Vec::new()));
        let config = config("project-a");
        let mut sink = LedgerSink::new(
            Box::new(RecordingSink {
                emitted: output.clone(),
            }),
            Some(temp.path()),
            "codex",
            "session-1",
            Some(&config),
            "bt1",
        )
        .await
        .unwrap();
        sink.set_capture_versions(Some("v1"), Some("native1"));
        let path = sink.ledger.as_ref().unwrap().path.clone();
        // Block persistence: nothing may reach the sink until origins are saved.
        tokio::fs::create_dir_all(&path).await.unwrap();
        assert!(sink.emit(&[terminal("span-0")]).await.is_err());
        assert!(output.lock().is_empty());
        sink.set_capture_versions(Some("v2"), Some("native2"));
        tokio::fs::remove_dir(&path).await.unwrap();
        for i in 0..256 {
            sink.emit(&[terminal(&format!("span-{i}"))]).await.unwrap();
            sink.flush().await.unwrap();
        }
        // Every origin and completion is serialized once, regardless of how
        // many prior spans exist. No growing snapshots or checkpoint rewrites.
        let data = tokio::fs::read_to_string(&path).await.unwrap();
        let records: Vec<LedgerFile> = data
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(records.len(), 512);
        for records in records.as_chunks::<2>().0 {
            assert_eq!(records[0].span_origins.len(), 1);
            assert!(records[0].completed_span_ids.is_empty());
            assert_eq!(records[1].completed_span_ids.len(), 1);
            assert!(records[1].span_origins.is_empty());
        }
        let mut recovered = LedgerSink::new(
            Box::new(RecordingSink::default()),
            Some(temp.path()),
            "codex",
            "session-1",
            Some(&config),
            "bt2",
        )
        .await
        .unwrap();
        assert_eq!(recovered.span_origins.len(), 256);
        assert_eq!(
            recovered.span_origins["span-0"].plugin_version.as_deref(),
            Some("v1")
        );
        assert_eq!(
            recovered.span_origins["span-1"].plugin_version.as_deref(),
            Some("v2")
        );
        assert_eq!(recovered.emit(&[terminal("span-0")]).await.unwrap(), 0);
        // Simulate upgrading a JSON checkpoint with versions and completion IDs.
        let (ledger, origins) = DeliveryLedger::load(temp.path(), "codex", "session-1", &config)
            .await
            .unwrap();
        tokio::fs::write(
            path.with_extension("json"),
            serde_json::to_vec(&LedgerFile {
                completed_span_ids: ledger.known,
                late_merge_ids: ledger.known_late_merges,
                span_origins: origins,
            })
            .unwrap(),
        )
        .await
        .unwrap();
        tokio::fs::remove_file(&path).await.unwrap();
        let (legacy, origins) = DeliveryLedger::load(temp.path(), "codex", "session-1", &config)
            .await
            .unwrap();
        assert_eq!(legacy.known.len(), 256);
        assert_eq!(origins.len(), 256);
        assert_eq!(origins["span-0"].plugin_version.as_deref(), Some("v1"));
    }

    #[tokio::test]
    async fn concurrent_processes_preserve_origins_and_delivery_records() {
        const CHILD_DIR: &str = "BRAINTRUST_TEST_LEDGER_WRITER_DIR";
        const TEST: &str =
            "delivery_ledger::tests::concurrent_processes_preserve_origins_and_delivery_records";
        async fn wait_for(path: &Path) {
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                while !path.exists() {
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("other ledger writer did not reach its barrier");
        }
        let temp = tempfile::tempdir().unwrap();
        let child_dir = std::env::var_os(CHILD_DIR).map(PathBuf::from);
        let dir = child_dir.as_deref().unwrap_or(temp.path());
        let is_child = child_dir.is_some();
        let output = Arc::new(Mutex::new(Vec::new()));
        let config = config("project-a");
        let mut sink = LedgerSink::new(
            Box::new(RecordingSink {
                emitted: output.clone(),
            }),
            Some(dir),
            "codex",
            "session-1",
            Some(&config),
            if is_child { "bt2" } else { "bt1" },
        )
        .await
        .unwrap();
        sink.set_capture_versions(
            Some(if is_child { "v2" } else { "v1" }),
            Some(if is_child { "native2" } else { "native1" }),
        );
        let shared = SpanOp::Insert(SpanRow {
            span_id: "shared".into(),
            root_span_id: "root".into(),
            span_type: SpanType::Tool,
            ..Default::default()
        });
        if is_child {
            // Both writers load the same empty ledger. The parent then persists
            // work before this writer acts on its now-stale length and maps.
            std::fs::write(dir.join("ready"), b"").unwrap();
            wait_for(&dir.join("go")).await;
            sink.emit(&[shared]).await.unwrap();
            {
                let rows = output.lock();
                let SpanOp::Insert(row) = &rows[0] else {
                    panic!("expected shared tool")
                };
                assert_eq!(
                    row.origin.as_ref().unwrap().plugin_version.as_deref(),
                    Some("v1"),
                    "a stale writer must reuse the first persisted origin"
                );
            }
            assert_eq!(sink.emit(&[terminal("parent-only")]).await.unwrap(), 0);
            assert_eq!(
                sink.emit(&[late_merge("parent-only", "usage")])
                    .await
                    .unwrap(),
                0
            );
        } else {
            let command = crate::subprocess::background_command(std::env::current_exe().unwrap());
            let mut child = tokio::process::Command::from(command);
            child
                .args(["--exact", TEST, "--nocapture"])
                .env(CHILD_DIR, dir)
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .kill_on_drop(true);
            let child = child.spawn().unwrap();
            wait_for(&dir.join("ready")).await;
            sink.emit(&[
                shared,
                terminal("parent-only"),
                late_merge("parent-only", "usage"),
            ])
            .await
            .unwrap();
            sink.flush().await.unwrap();
            std::fs::write(dir.join("go"), b"").unwrap();
            // Exercise overlapping OS-level writes, not just two objects sharing
            // an in-process mutex. Each operation must survive either ordering.
            for i in 0..32 {
                sink.emit(&[terminal(&format!("parent-{i}"))])
                    .await
                    .unwrap();
                sink.flush().await.unwrap();
            }
            let child_output =
                tokio::time::timeout(std::time::Duration::from_secs(10), child.wait_with_output())
                    .await
                    .expect("child ledger writer stalled")
                    .unwrap();
            assert!(
                child_output.status.success(),
                "child writer failed: {}\n{}",
                String::from_utf8_lossy(&child_output.stdout),
                String::from_utf8_lossy(&child_output.stderr)
            );
            assert_eq!(
                sink.emit(&[terminal("child-0")]).await.unwrap(),
                0,
                "an already-open parent must observe the child's acknowledgement"
            );
            let (ledger, origins) = DeliveryLedger::load(dir, "codex", "session-1", &config)
                .await
                .unwrap();
            assert_eq!(origins["shared"].plugin_version.as_deref(), Some("v1"));
            assert_eq!(origins["shared"].bt_version, "bt1");
            assert_eq!(origins["shared"].source_version.as_deref(), Some("native1"));
            assert!(ledger.known.contains("parent-only"));
            assert!(ledger.known_late_merges.contains("parent-only\u{1f}usage"));
            for i in 0..32 {
                for (writer, version) in [("parent", "v1"), ("child", "v2")] {
                    let span = format!("{writer}-{i}");
                    assert!(ledger.known.contains(&span), "lost completion: {span}");
                    assert_eq!(origins[&span].plugin_version.as_deref(), Some(version));
                }
            }
            return;
        }
        for i in 0..32 {
            sink.emit(&[terminal(&format!("child-{i}"))]).await.unwrap();
            sink.flush().await.unwrap();
        }
    }

    #[tokio::test]
    async fn a_destination_receives_a_terminal_span_only_once_across_sink_instances() {
        let temp = tempfile::tempdir().unwrap();
        let first_output = Arc::new(Mutex::new(Vec::new()));
        let first = RecordingSink {
            emitted: first_output.clone(),
        };
        let mut first = LedgerSink::new(
            Box::new(first),
            Some(temp.path()),
            "codex",
            "session-1",
            Some(&config("project-a")),
            "test",
        )
        .await
        .unwrap();
        first.emit(&[terminal("span-1")]).await.unwrap();
        first.flush().await.unwrap();
        assert_eq!(first_output.lock().len(), 1);

        let mut same_destination = config("project-a");
        same_destination.additional_metadata = Some(serde_json::json!({"run_id": "new"}));
        let repeated_output = Arc::new(Mutex::new(Vec::new()));
        let repeated = RecordingSink {
            emitted: repeated_output.clone(),
        };
        let mut repeated = LedgerSink::new(
            Box::new(repeated),
            Some(temp.path()),
            "codex",
            "session-1",
            Some(&same_destination),
            "test",
        )
        .await
        .unwrap();
        assert_eq!(repeated.emit(&[terminal("span-1")]).await.unwrap(), 0);
        assert_eq!(repeated.emit(&[partial_merge("span-1")]).await.unwrap(), 0);
        repeated.flush().await.unwrap();
        assert!(repeated_output.lock().is_empty());
    }

    #[tokio::test]
    async fn a_completed_span_receives_each_distinct_late_merge_once() {
        let temp = tempfile::tempdir().unwrap();
        let first = RecordingSink::default();
        let mut first = LedgerSink::new(
            Box::new(first),
            Some(temp.path()),
            "grok",
            "session-1",
            Some(&config("project-a")),
            "test",
        )
        .await
        .unwrap();
        assert_eq!(first.emit(&[terminal("span-1")]).await.unwrap(), 1);
        first.flush().await.unwrap();

        let second = RecordingSink::default();
        let mut second = LedgerSink::new(
            Box::new(second),
            Some(temp.path()),
            "grok",
            "session-1",
            Some(&config("project-a")),
            "test",
        )
        .await
        .unwrap();
        assert_eq!(
            second
                .emit(&[late_merge("span-1", "output")])
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            second.emit(&[late_merge("span-1", "usage")]).await.unwrap(),
            1
        );
        assert_eq!(
            second
                .emit(&[late_merge("span-1", "output")])
                .await
                .unwrap(),
            0
        );
        second.flush().await.unwrap();

        let third = RecordingSink::default();
        let mut third = LedgerSink::new(
            Box::new(third),
            Some(temp.path()),
            "grok",
            "session-1",
            Some(&config("project-a")),
            "test",
        )
        .await
        .unwrap();
        assert_eq!(
            third.emit(&[late_merge("span-1", "output")]).await.unwrap(),
            0
        );
        assert_eq!(
            third.emit(&[late_merge("span-1", "usage")]).await.unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn a_resumed_root_delivers_each_distinct_terminal_refresh_once() {
        let temp = tempfile::tempdir().unwrap();
        let first = RecordingSink::default();
        let mut first = LedgerSink::new(
            Box::new(first),
            Some(temp.path()),
            "codex",
            "session-1",
            Some(&config("project-a")),
            "test",
        )
        .await
        .unwrap();
        assert_eq!(
            first
                .emit(&[late_terminal_merge("root", "session:stop:3", 3)])
                .await
                .unwrap(),
            1
        );
        first.flush().await.unwrap();

        let resumed = RecordingSink::default();
        let mut resumed = LedgerSink::new(
            Box::new(resumed),
            Some(temp.path()),
            "codex",
            "session-1",
            Some(&config("project-a")),
            "test",
        )
        .await
        .unwrap();
        assert_eq!(
            resumed
                .emit(&[late_terminal_merge("root", "session:stop:3", 3)])
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            resumed
                .emit(&[late_terminal_merge("root", "session:stop:5", 5)])
                .await
                .unwrap(),
            1
        );
        resumed.flush().await.unwrap();
    }

    #[tokio::test]
    async fn a_different_destination_replays_the_same_terminal_span() {
        let temp = tempfile::tempdir().unwrap();
        let initial_output = Arc::new(Mutex::new(Vec::new()));
        let initial = RecordingSink {
            emitted: initial_output,
        };
        let mut initial = LedgerSink::new(
            Box::new(initial),
            Some(temp.path()),
            "claude-code",
            "session-1",
            Some(&config("project-a")),
            "test",
        )
        .await
        .unwrap();
        initial.emit(&[terminal("span-1")]).await.unwrap();
        initial.flush().await.unwrap();

        let replay_output = Arc::new(Mutex::new(Vec::new()));
        let replay = RecordingSink {
            emitted: replay_output.clone(),
        };
        let mut replay = LedgerSink::new(
            Box::new(replay),
            Some(temp.path()),
            "claude-code",
            "session-1",
            Some(&config("project-b")),
            "test",
        )
        .await
        .unwrap();
        assert_eq!(replay.emit(&[terminal("span-1")]).await.unwrap(), 1);
        replay.flush().await.unwrap();
        assert_eq!(replay_output.lock().len(), 1);
    }
    #[tokio::test]
    async fn an_authoritative_terminal_merge_records_completion_and_late_delivery() {
        let temp = tempfile::tempdir().unwrap();
        let first = RecordingSink::default();
        let mut first = LedgerSink::new(
            Box::new(first),
            Some(temp.path()),
            "grok",
            "session-1",
            Some(&config("project-a")),
            "test",
        )
        .await
        .unwrap();
        let mut authoritative = terminal("span-1");
        let SpanOp::Insert(row) = &mut authoritative else {
            unreachable!();
        };
        row.late_merge_key = Some("authoritative".into());
        assert_eq!(first.emit(&[authoritative]).await.unwrap(), 1);
        first.flush().await.unwrap();

        let repeated = RecordingSink::default();
        let mut repeated = LedgerSink::new(
            Box::new(repeated),
            Some(temp.path()),
            "grok",
            "session-1",
            Some(&config("project-a")),
            "test",
        )
        .await
        .unwrap();
        assert_eq!(repeated.emit(&[terminal("span-1")]).await.unwrap(), 0);
        assert_eq!(
            repeated
                .emit(&[late_merge("span-1", "authoritative")])
                .await
                .unwrap(),
            0
        );
    }
}
