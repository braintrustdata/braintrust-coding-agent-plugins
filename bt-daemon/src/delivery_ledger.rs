//! Destination-scoped completed-span delivery ledger.
//!
//! Hook journals prevent a daemon recovery from re-emitting already flushed
//! observations. Imports are a second ingress, though: they synthesize the
//! same source session without using that journal. This ledger records which
//! completed span rows a destination has already accepted, so every ingress can
//! avoid re-reporting any later partial merge for a completed span while a new
//! destination still receives the full trace.

use crate::sink::Sink;
use crate::translate::{OriginSnapshot, SpanOp, SpanRow, SpanType};
use crate::wire::SessionConfig;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

#[derive(serde::Serialize, serde::Deserialize, Default)]
struct LedgerFile {
    #[serde(default, alias = "terminal_span_ids")]
    completed_span_ids: HashSet<String>,
    #[serde(default, alias = "late_merge_span_ids")]
    late_merge_ids: HashSet<String>,
    #[serde(default)]
    span_origins: HashMap<String, OriginSnapshot>,
}

struct DeliveryLedger {
    path: PathBuf,
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
        let persisted = match tokio::fs::read(&path).await {
            Ok(bytes) => serde_json::from_slice::<LedgerFile>(&bytes)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => LedgerFile::default(),
            Err(error) => return Err(error.into()),
        };
        Ok((
            Self {
                path,
                known: persisted.completed_span_ids,
                pending: HashSet::new(),
                known_late_merges: persisted.late_merge_ids,
                pending_late_merges: HashSet::new(),
            },
            persisted.span_origins,
        ))
    }

    fn filter(&self, ops: &[SpanOp]) -> Vec<SpanOp> {
        ops.iter()
            .filter(|op| {
                let row = match op {
                    SpanOp::Insert(row) | SpanOp::Merge(row) => row,
                };
                if let Some(key) = &row.late_merge_key {
                    let id = late_merge_id(row, key);
                    !self.known_late_merges.contains(&id) && !self.pending_late_merges.contains(&id)
                } else {
                    !self.known.contains(&row.span_id) && !self.pending.contains(&row.span_id)
                }
            })
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

    async fn commit(
        &mut self,
        span_origins: &HashMap<String, OriginSnapshot>,
    ) -> anyhow::Result<()> {
        if self.pending.is_empty() && self.pending_late_merges.is_empty() {
            return Ok(());
        }
        self.known.extend(self.pending.drain());
        self.known_late_merges
            .extend(self.pending_late_merges.drain());
        self.save(span_origins).await
    }

    async fn save(&self, span_origins: &HashMap<String, OriginSnapshot>) -> anyhow::Result<()> {
        let parent = self.path.parent().expect("ledger path has a parent");
        tokio::fs::create_dir_all(parent).await?;
        let temp = self
            .path
            .with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
        tokio::fs::write(
            &temp,
            serde_json::to_vec(&LedgerFile {
                completed_span_ids: self.known.clone(),
                late_merge_ids: self.known_late_merges.clone(),
                span_origins: span_origins.clone(),
            })?,
        )
        .await?;
        if let Err(first_error) = tokio::fs::rename(&temp, &self.path).await {
            let _ = tokio::fs::remove_file(&self.path).await;
            tokio::fs::rename(&temp, &self.path)
                .await
                .map_err(|error| {
                    anyhow::anyhow!(
                        "replace delivery ledger failed: {first_error}; retry failed: {error}"
                    )
                })?;
        }
        Ok(())
    }
}

fn late_merge_id(row: &SpanRow, key: &str) -> String {
    format!("{}\u{1f}{key}", row.span_id)
}

/// Wrap a sink with destination-scoped terminal-span suppression. If durable
/// ledger state cannot be loaded, callers retain normal delivery rather than
/// risking lost trace data.
pub(crate) struct LedgerSink {
    inner: Box<dyn Sink>,
    ledger: Option<DeliveryLedger>,
    span_origins: HashMap<String, OriginSnapshot>,
    origins_dirty: bool,
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
    ) -> Self {
        let loaded = match (data_dir, config) {
            (Some(data_dir), Some(config)) if config.destination.is_some() => {
                DeliveryLedger::load(data_dir, source, session_id, config)
                    .await
                    .map_err(|error| {
                        tracing::warn!(source, session_id, "delivery ledger unavailable: {error}")
                    })
                    .ok()
            }
            _ => None,
        };
        let (ledger, span_origins) = match loaded {
            Some((ledger, origins)) => (Some(ledger), origins),
            None => (None, HashMap::new()),
        };
        Self {
            inner,
            ledger,
            span_origins,
            origins_dirty: false,
            plugin_version: None,
            source_version: None,
            bt_version: bt_version.to_owned(),
            attached_parent: config.and_then(|config| config.attached_span_ids().0),
        }
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
            let is_root = row.span_type == SpanType::Task
                && match self.attached_parent.as_deref() {
                    Some(parent) => {
                        row.parent_span_ids.len() == 1
                            && row.parent_span_ids[0] == parent
                            && row.span_id != parent
                    }
                    None => row.parent_span_ids.is_empty(),
                };
            if is_insert && (is_root || row.is_turn) {
                if let std::collections::hash_map::Entry::Vacant(entry) =
                    self.span_origins.entry(row.span_id.clone())
                {
                    entry.insert(OriginSnapshot {
                        plugin_version: self.plugin_version.clone(),
                        bt_version: self.bt_version.clone(),
                        source_version: self.source_version.clone(),
                    });
                    self.origins_dirty = true;
                }
            }
            // Stamp only known session roots and turns, including stateless updates.
            // This also discards provenance supplied by a span-processing plugin.
            row.origin = self.span_origins.get(&row.span_id).cloned();
        }
        if self.origins_dirty {
            // Persist birth versions before the SDK can queue a span. A crash
            // between delivery and its checkpoint must not change its provenance.
            // Do not acknowledge pending span deliveries before the sink flushes.
            if let Some(ledger) = &self.ledger {
                ledger.save(&self.span_origins).await?;
            }
            self.origins_dirty = false;
        }
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

    async fn emit_plugin_marker(&mut self, op: &SpanOp) -> anyhow::Result<u64> {
        self.inner.emit_plugin_marker(op).await
    }

    async fn replace_plugin_marker(&mut self, op: &SpanOp) -> anyhow::Result<u64> {
        let emitted = self.inner.replace_plugin_marker(op).await?;
        if emitted > 0 {
            if let Some(ledger) = &mut self.ledger {
                ledger.record_emitted(std::slice::from_ref(op));
            }
        }
        Ok(emitted)
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        self.inner.flush().await?;
        if let Some(ledger) = &mut self.ledger {
            ledger.commit(&self.span_origins).await?;
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
    use crate::translate::SpanRow;
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
    async fn open_turn_snapshot_survives_recovery_before_delivery_checkpoint() {
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
        .await;
        first.set_capture_versions(None, Some("native1"));
        let turn = SpanRow {
            span_id: "turn-1".into(),
            root_span_id: "root".into(),
            parent_span_ids: vec!["root".into()],
            is_turn: true,
            ..Default::default()
        };
        first.emit(&[SpanOp::Insert(turn.clone())]).await.unwrap();
        // Lose in-memory state without flushing/checkpointing delivery.
        drop(first);

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
        .await;
        recovered.set_capture_versions(Some("v2"), Some("native2"));
        recovered
            .emit(&[
                SpanOp::Insert(turn.clone()),
                partial_merge("turn-1"),
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
        let rows = output.lock();
        assert_eq!(rows.len(), 4);
        for op in rows.iter() {
            let row = match op {
                SpanOp::Insert(row) | SpanOp::Merge(row) => row,
            };
            let expected = match row.span_id.as_str() {
                "turn-1" => Some(OriginSnapshot {
                    plugin_version: None,
                    source_version: Some("native1".into()),
                    bt_version: "bt1".into(),
                }),
                "turn-2" => Some(OriginSnapshot {
                    plugin_version: Some("v2".into()),
                    source_version: Some("native2".into()),
                    bt_version: "bt2".into(),
                }),
                "subagent-container" => None,
                unexpected => panic!("unexpected span: {unexpected}"),
            };
            assert_eq!(row.origin, expected, "{}", row.span_id);
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
        .await;
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
        .await;
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
        .await;
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
        .await;
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
        .await;
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
        .await;
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
        .await;
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
        .await;
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
        .await;
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
        .await;
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
        .await;
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
