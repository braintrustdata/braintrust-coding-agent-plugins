//! Destination-scoped checked-delivery and visible-span projection ledger.
//!
//! Hook journals prevent a daemon recovery from re-emitting already flushed
//! observations. Imports are a second ingress, though: they synthesize the
//! same source session without using that journal. This ledger records accepted
//! span images and completed spans, so retries can skip unchanged merges while
//! a new destination still receives the full trace.

use crate::sink::Sink;
use crate::translate::{SpanOp, SpanRow};
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
    span_images: HashMap<String, serde_json::Value>,
}

struct DeliveryLedger {
    path: PathBuf,
    known: HashSet<String>,
    pending: HashSet<String>,
    known_late_merges: HashSet<String>,
    pending_late_merges: HashSet<String>,
    known_images: HashMap<String, serde_json::Value>,
    pending_images: HashMap<String, serde_json::Value>,
}

impl DeliveryLedger {
    async fn load(
        data_dir: &Path,
        source: &str,
        session_id: &str,
        config: &SessionConfig,
    ) -> anyhow::Result<Self> {
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
        crate::paths::ensure_private_dir(path.parent().expect("ledger path has a parent"))?;
        if tokio::fs::metadata(&path).await.is_ok() {
            crate::paths::restrict_file_to_owner(&path)?;
        }
        let persisted = match tokio::fs::read(&path).await {
            Ok(bytes) => serde_json::from_slice::<LedgerFile>(&bytes)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => LedgerFile::default(),
            Err(error) => return Err(error.into()),
        };
        Ok(Self {
            path,
            known: persisted.completed_span_ids,
            pending: HashSet::new(),
            known_late_merges: persisted.late_merge_ids,
            pending_late_merges: HashSet::new(),
            known_images: persisted.span_images,
            pending_images: HashMap::new(),
        })
    }

    fn filter(&self, ops: &[SpanOp]) -> Vec<SpanOp> {
        let mut images = self.known_images.clone();
        images.extend(self.pending_images.clone());
        let mut terminal = self.known.clone();
        terminal.extend(self.pending.clone());
        let mut late = self.known_late_merges.clone();
        late.extend(self.pending_late_merges.clone());
        let mut filtered = Vec::new();
        for op in ops {
            let row = match op {
                SpanOp::Insert(row) | SpanOp::Merge(row) => row,
            };
            if let Some(key) = &row.late_merge_key {
                let id = late_merge_id(row, key);
                if !late.insert(id) {
                    continue;
                }
            } else if terminal.contains(&row.span_id) {
                continue;
            }
            let next = next_image(images.get(&row.span_id), op);
            if images.get(&row.span_id) == Some(&next) {
                continue;
            }
            images.insert(row.span_id.clone(), next);
            if row.end_ms.is_some() {
                terminal.insert(row.span_id.clone());
            }
            filtered.push(op.clone());
        }
        filtered
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
            let current = self
                .pending_images
                .get(&row.span_id)
                .or_else(|| self.known_images.get(&row.span_id));
            self.pending_images
                .insert(row.span_id.clone(), next_image(current, op));
        }
    }

    async fn commit(&mut self) -> anyhow::Result<()> {
        if self.pending.is_empty()
            && self.pending_late_merges.is_empty()
            && self.pending_images.is_empty()
        {
            return Ok(());
        }
        self.known.extend(self.pending.drain());
        self.known_late_merges
            .extend(self.pending_late_merges.drain());
        self.known_images.extend(self.pending_images.drain());
        let parent = self.path.parent().expect("ledger path has a parent");
        crate::paths::ensure_private_dir(parent)?;
        let temp = self
            .path
            .with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
        let bytes = serde_json::to_vec(&LedgerFile {
            completed_span_ids: self.known.clone(),
            late_merge_ids: self.known_late_merges.clone(),
            span_images: self.known_images.clone(),
        })?;
        let mut options = tokio::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = options.open(&temp).await?;
        use tokio::io::AsyncWriteExt;
        file.write_all(&bytes).await?;
        file.flush().await?;
        file.sync_data().await?;
        drop(file);
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
        crate::paths::restrict_file_to_owner(&self.path)?;
        Ok(())
    }
}

/// Project a row operation using the SDK's merge behavior: an insert replaces
/// the prior image, while a merge overwrites only fields present in the row.
fn next_image(previous: Option<&serde_json::Value>, op: &SpanOp) -> serde_json::Value {
    let row = match op {
        SpanOp::Insert(row) | SpanOp::Merge(row) => row,
    };
    let next = serde_json::to_value(row).unwrap_or(serde_json::Value::Null);
    if matches!(op, SpanOp::Insert(_)) {
        return next;
    }
    let mut merged = previous
        .and_then(serde_json::Value::as_object)
        .cloned()
        .unwrap_or_default();
    if let Some(changes) = next.as_object() {
        merged.extend(changes.clone());
    }
    serde_json::Value::Object(merged)
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
}

impl LedgerSink {
    pub(crate) async fn new(
        inner: Box<dyn Sink>,
        data_dir: &Path,
        source: &str,
        session_id: &str,
        config: Option<&SessionConfig>,
    ) -> Self {
        let ledger = match config {
            Some(config) if config.destination.is_some() => {
                DeliveryLedger::load(data_dir, source, session_id, config)
                    .await
                    .map_err(|error| {
                        tracing::warn!(source, session_id, "delivery ledger unavailable: {error}")
                    })
                    .ok()
            }
            _ => None,
        };
        Self { inner, ledger }
    }
}

#[async_trait::async_trait]
impl Sink for LedgerSink {
    fn configure(&mut self, config: &SessionConfig) {
        self.inner.configure(config);
    }

    async fn emit(&mut self, ops: &[SpanOp]) -> anyhow::Result<u64> {
        let filtered = self
            .ledger
            .as_ref()
            .map(|ledger| ledger.filter(ops))
            .unwrap_or_else(|| ops.to_vec());
        if filtered.is_empty() {
            return Ok(0);
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
            ledger.commit().await?;
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
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct RecordingSink {
        emitted: Arc<Mutex<Vec<SpanOp>>>,
    }

    #[async_trait::async_trait]
    impl Sink for RecordingSink {
        async fn emit(&mut self, ops: &[SpanOp]) -> anyhow::Result<u64> {
            self.emitted.lock().unwrap().extend_from_slice(ops);
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
    async fn a_destination_receives_a_terminal_span_only_once_across_sink_instances() {
        let temp = tempfile::tempdir().unwrap();
        let first_output = Arc::new(Mutex::new(Vec::new()));
        let first = RecordingSink {
            emitted: first_output.clone(),
        };
        let mut first = LedgerSink::new(
            Box::new(first),
            temp.path(),
            "codex",
            "session-1",
            Some(&config("project-a")),
        )
        .await;
        first.emit(&[terminal("span-1")]).await.unwrap();
        first.flush().await.unwrap();
        assert_eq!(first_output.lock().unwrap().len(), 1);

        let mut same_destination = config("project-a");
        same_destination.additional_metadata = Some(serde_json::json!({"run_id": "new"}));
        let repeated_output = Arc::new(Mutex::new(Vec::new()));
        let repeated = RecordingSink {
            emitted: repeated_output.clone(),
        };
        let mut repeated = LedgerSink::new(
            Box::new(repeated),
            temp.path(),
            "codex",
            "session-1",
            Some(&same_destination),
        )
        .await;
        assert_eq!(repeated.emit(&[terminal("span-1")]).await.unwrap(), 0);
        assert_eq!(repeated.emit(&[partial_merge("span-1")]).await.unwrap(), 0);
        repeated.flush().await.unwrap();
        assert!(repeated_output.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn identical_late_merges_are_suppressed_even_with_distinct_internal_keys() {
        let temp = tempfile::tempdir().unwrap();
        let first = RecordingSink::default();
        let mut first = LedgerSink::new(
            Box::new(first),
            temp.path(),
            "grok",
            "session-1",
            Some(&config("project-a")),
        )
        .await;
        assert_eq!(first.emit(&[terminal("span-1")]).await.unwrap(), 1);
        first.flush().await.unwrap();

        let second = RecordingSink::default();
        let mut second = LedgerSink::new(
            Box::new(second),
            temp.path(),
            "grok",
            "session-1",
            Some(&config("project-a")),
        )
        .await;
        assert_eq!(
            second
                .emit(&[late_merge("span-1", "output")])
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            second.emit(&[late_merge("span-1", "usage")]).await.unwrap(),
            0
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
            temp.path(),
            "grok",
            "session-1",
            Some(&config("project-a")),
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
            temp.path(),
            "codex",
            "session-1",
            Some(&config("project-a")),
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
            temp.path(),
            "codex",
            "session-1",
            Some(&config("project-a")),
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
            temp.path(),
            "claude-code",
            "session-1",
            Some(&config("project-a")),
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
            temp.path(),
            "claude-code",
            "session-1",
            Some(&config("project-b")),
        )
        .await;
        assert_eq!(replay.emit(&[terminal("span-1")]).await.unwrap(), 1);
        replay.flush().await.unwrap();
        assert_eq!(replay_output.lock().unwrap().len(), 1);
    }
    #[tokio::test]
    async fn an_authoritative_terminal_merge_records_completion_and_late_delivery() {
        let temp = tempfile::tempdir().unwrap();
        let first = RecordingSink::default();
        let mut first = LedgerSink::new(
            Box::new(first),
            temp.path(),
            "grok",
            "session-1",
            Some(&config("project-a")),
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
            temp.path(),
            "grok",
            "session-1",
            Some(&config("project-a")),
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
