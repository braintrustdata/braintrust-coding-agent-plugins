//! Destination-scoped completed-span delivery ledger.
//!
//! Hook journals prevent a daemon recovery from re-emitting already flushed
//! observations. Imports are a second ingress, though: they synthesize the
//! same source session without using that journal. This ledger records which
//! completed span rows a destination has already accepted, so every ingress can
//! avoid re-reporting any later partial merge for a completed span while a new
//! destination still receives the full trace.

use crate::sink::Sink;
use crate::translate::SpanOp;
use crate::wire::SessionConfig;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

#[derive(serde::Serialize, serde::Deserialize, Default)]
struct LedgerFile {
    #[serde(default, alias = "terminal_span_ids")]
    completed_span_ids: HashSet<String>,
    #[serde(default)]
    late_merge_span_ids: HashSet<String>,
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
        let persisted = match tokio::fs::read(&path).await {
            Ok(bytes) => serde_json::from_slice::<LedgerFile>(&bytes)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => LedgerFile::default(),
            Err(error) => return Err(error.into()),
        };
        Ok(Self {
            path,
            known: persisted.completed_span_ids,
            pending: HashSet::new(),
            known_late_merges: persisted.late_merge_span_ids,
            pending_late_merges: HashSet::new(),
        })
    }

    fn filter(&self, ops: &[SpanOp]) -> Vec<SpanOp> {
        ops.iter()
            .filter(|op| {
                let row = match op {
                    SpanOp::Insert(row) | SpanOp::Merge(row) => row,
                };
                if row.allow_late_merge {
                    !self.known_late_merges.contains(&row.span_id)
                        && !self.pending_late_merges.contains(&row.span_id)
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
            if row.allow_late_merge {
                self.pending_late_merges.insert(row.span_id.clone());
            } else if row.end_ms.is_some() {
                self.pending.insert(row.span_id.clone());
            }
        }
    }

    async fn commit(&mut self) -> anyhow::Result<()> {
        if self.pending.is_empty() && self.pending_late_merges.is_empty() {
            return Ok(());
        }
        self.known.extend(self.pending.drain());
        self.known_late_merges
            .extend(self.pending_late_merges.drain());
        let parent = self.path.parent().expect("ledger path has a parent");
        tokio::fs::create_dir_all(parent).await?;
        let temp = self
            .path
            .with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
        tokio::fs::write(
            &temp,
            serde_json::to_vec(&LedgerFile {
                completed_span_ids: self.known.clone(),
                late_merge_span_ids: self.known_late_merges.clone(),
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
        if let Some(ledger) = &mut self.ledger {
            ledger.record_emitted(&filtered);
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

    fn late_merge(span_id: &str) -> SpanOp {
        SpanOp::Merge(SpanRow {
            span_id: span_id.into(),
            root_span_id: "root".into(),
            allow_late_merge: true,
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
    async fn a_completed_span_receives_one_late_merge_across_sink_instances() {
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
        assert_eq!(second.emit(&[late_merge("span-1")]).await.unwrap(), 1);
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
        assert_eq!(third.emit(&[late_merge("span-1")]).await.unwrap(), 0);
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
}
