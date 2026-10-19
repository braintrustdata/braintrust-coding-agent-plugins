//! Routing owns its sink behind a bounded typed port. Session dispatch is its facade.
use super::delivery::{Delivery, InputPort, OutputPort};
use super::plugins::{PluginOutcome, Plugins};
use super::{Completion, Stage};
use crate::{sink::Sink, translate::SpanOp, wire::SessionConfig};
use parking_lot::Mutex;
use std::{path::PathBuf, sync::Arc};
use tokio::sync::{mpsc, oneshot};

struct Request {
    config: SessionConfig,
    versions: (Option<String>, Option<String>),
    operation: Operation,
    reply: oneshot::Sender<Response>,
}

enum Operation {
    Emit(Vec<SpanOp>),
    Flush,
}
struct Response {
    result: anyhow::Result<u64>,
    pending: bool,
    permalink: Option<String>,
}

/// Rows accepted before a mandatory plugin failed still count as emitted.
#[derive(Debug)]
pub(crate) struct EmitFailure {
    pub emitted: u64,
    pub report: bool,
    pub cause: anyhow::Error,
}
impl std::fmt::Display for EmitFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.cause.fmt(f)
    }
}
impl std::error::Error for EmitFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.cause.as_ref())
    }
}

struct Forward;
#[async_trait::async_trait]
impl Completion for Forward {
    async fn complete(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

pub(crate) struct RoutingSink {
    tx: mpsc::Sender<Delivery<Request>>,
    config: Option<SessionConfig>,
    versions: (Option<String>, Option<String>),
    pending: bool,
    permalink: Option<String>,
}

impl RoutingSink {
    pub fn spawn(
        mut sink: Box<dyn Sink>,
        source: String,
        session_id: String,
        data_dir: PathBuf,
        last_error: Arc<Mutex<Option<String>>>,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel::<Delivery<Request>>(16);
        tokio::spawn(async move {
            while let Some(delivery) = InputPort::receive(&mut rx).await {
                let request = delivery.value();
                sink.configure(&request.config);
                sink.set_capture_versions(
                    request.versions.0.as_deref(),
                    request.versions.1.as_deref(),
                );
                let result = match &request.operation {
                    Operation::Emit(ops) => {
                        let mut stage = Plugins {
                            paths: request.config.span_plugins.clone(),
                            source: source.clone(),
                            session_id: session_id.clone(),
                        };
                        let mut prepared = Vec::with_capacity(ops.len());
                        let mut failure = None;
                        let mut report = false;
                        for op in ops {
                            match stage.process(op).await {
                                PluginOutcome::Prepared(op) => prepared.push(*op),
                                PluginOutcome::Filtered => {}
                                PluginOutcome::Failed(error) => {
                                    report |= error.newly_seen;
                                    if error.newly_seen {
                                        if let Err(diagnostic) = crate::plugin_diagnostics::record(
                                            &data_dir,
                                            &source,
                                            &error.path,
                                            &error.message,
                                        ) {
                                            tracing::warn!(%diagnostic, "plugin diagnostic could not be saved");
                                        }
                                    }
                                    failure = Some(anyhow::anyhow!(
                                        "span plugin {} failed; span operations are being discarded: {}",
                                        error.path.display(), error.message));
                                }
                                PluginOutcome::Unavailable(error) => {
                                    report = true;
                                    failure = Some(error);
                                }
                            }
                        }
                        let emitted = if prepared.is_empty() {
                            Ok(0)
                        } else {
                            sink.emit(&prepared).await
                        };
                        match (emitted, failure) {
                            (Err(error), _) => Err(error),
                            (Ok(emitted), Some(cause)) => Err(EmitFailure {
                                emitted,
                                report,
                                cause,
                            }
                            .into()),
                            (Ok(emitted), None) => Ok(emitted),
                        }
                    }
                    Operation::Flush => sink.flush().await.map(|()| 0),
                };
                if let Err(error) = &result {
                    *last_error.lock() = Some(error.to_string());
                }
                let request = match delivery.into_completed().await {
                    Ok(request) => request,
                    Err(failed) => {
                        *last_error.lock() =
                            Some(format!("routing completion failed: {}", failed.error));
                        // Dropping the unfinished request closes its reply; the
                        // caller receives an error instead of a success response.
                        continue;
                    }
                };
                let _ = request.reply.send(Response {
                    result,
                    pending: sink.has_pending_delivery(),
                    permalink: sink.permalink(),
                });
            }
        });
        Self {
            tx,
            config: None,
            versions: (None, None),
            pending: false,
            permalink: None,
        }
    }

    async fn request(&mut self, operation: Operation) -> anyhow::Result<u64> {
        let config = self
            .config
            .clone()
            .ok_or_else(|| anyhow::anyhow!("route is not configured"))?;
        let (reply, receive) = oneshot::channel();
        let delivery = Delivery::new(operation, Forward).map(|operation| Request {
            config,
            versions: self.versions.clone(),
            operation,
            reply,
        });
        // Retain ownership on a failed send; no upstream completion is implied.
        if let Err(undelivered) = OutputPort::send(&mut self.tx, delivery).await {
            let _ = undelivered.value();
            anyhow::bail!("routing actor is gone");
        }
        let response = receive
            .await
            .map_err(|_| anyhow::anyhow!("routing actor dropped its reply"))?;
        self.pending = response.pending;
        self.permalink = response.permalink;
        response.result
    }
}

#[async_trait::async_trait]
impl Sink for RoutingSink {
    fn configure(&mut self, config: &SessionConfig) {
        self.config = Some(config.clone());
    }
    fn set_capture_versions(&mut self, plugin: Option<&str>, source: Option<&str>) {
        self.versions = (plugin.map(str::to_owned), source.map(str::to_owned));
    }
    async fn emit(&mut self, ops: &[SpanOp]) -> anyhow::Result<u64> {
        self.request(Operation::Emit(ops.to_vec())).await
    }
    async fn flush(&mut self) -> anyhow::Result<()> {
        self.request(Operation::Flush).await.map(|_| ())
    }
    fn has_pending_delivery(&self) -> bool {
        self.pending
    }
    fn permalink(&self) -> Option<String> {
        self.permalink.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        translate::SpanRow,
        wire::{BackendAuth, SessionRoute},
    };
    use std::sync::atomic::{AtomicU64, Ordering};

    struct CountingSink(Arc<AtomicU64>);
    #[async_trait::async_trait]
    impl Sink for CountingSink {
        async fn emit(&mut self, ops: &[SpanOp]) -> anyhow::Result<u64> {
            self.0.fetch_add(ops.len() as u64, Ordering::Relaxed);
            Ok(ops.len() as u64)
        }
        async fn flush(&mut self) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn partial_plugin_failure_preserves_count_and_reports_only_once() {
        let tmp = tempfile::tempdir().unwrap();
        let plugin = tmp.path().join("selective.mjs");
        std::fs::write(&plugin, "export default span => { if (span.name === 'reject') throw new Error('rejected'); return span; }").unwrap();
        let emitted = Arc::new(AtomicU64::new(0));
        let error = Arc::new(Mutex::new(None));
        let mut sink = RoutingSink::spawn(
            Box::new(CountingSink(emitted.clone())),
            "debug".into(),
            "partial".into(),
            tmp.path().into(),
            error.clone(),
        );
        sink.configure(
            &SessionRoute {
                span_plugins: vec![plugin],
                ..SessionRoute::default()
            }
            .with_auth(BackendAuth {
                token: "test".into(),
                api_url: None,
                app_url: None,
                org_name: None,
                org_id: None,
            }),
        );
        let op = |name: &str| {
            SpanOp::Insert(SpanRow {
                span_id: name.into(),
                name: name.into(),
                ..SpanRow::default()
            })
        };
        let failure = sink.emit(&[op("accept"), op("reject")]).await.unwrap_err();
        let first = failure.downcast_ref::<EmitFailure>().unwrap();
        assert_eq!(first.emitted, 1);
        assert!(first.report);
        assert_eq!(emitted.load(Ordering::Relaxed), 1);
        let failure = sink.emit(&[op("accept")]).await.unwrap_err();
        let cached = failure.downcast_ref::<EmitFailure>().unwrap();
        assert_eq!(cached.emitted, 0);
        assert!(
            !cached.report,
            "cached failures must not request another warning"
        );
        assert!(error.lock().as_ref().unwrap().contains("rejected"));
        assert_eq!(emitted.load(Ordering::Relaxed), 1);
    }
}
