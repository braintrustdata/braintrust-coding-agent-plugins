//! Routing owns its sink behind a bounded typed port. Session dispatch is its facade.
use super::delivery::{Delivery, InputPort, OutputPort};
use super::plugins::{PluginOutcome, Plugins};
use super::{Completion, Stage};
use crate::{sink::Sink, translate::SpanOp, wire::SessionConfig};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};
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
                        for op in ops {
                            match stage.process(op).await {
                                PluginOutcome::Prepared(op) => prepared.push(*op),
                                PluginOutcome::Filtered => {}
                                PluginOutcome::Failed(error) => {
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
                                PluginOutcome::Unavailable(error) => failure = Some(error),
                            }
                        }
                        let emitted = if prepared.is_empty() {
                            Ok(0)
                        } else {
                            sink.emit(&prepared).await
                        };
                        match failure {
                            Some(error) => Err(error),
                            None => emitted,
                        }
                    }
                    Operation::Flush => sink.flush().await.map(|()| 0),
                };
                if let Err(error) = &result {
                    *last_error.lock().unwrap() = Some(error.to_string());
                }
                let request = delivery
                    .into_completed()
                    .await
                    .ok()
                    .expect("volatile delivery");
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
