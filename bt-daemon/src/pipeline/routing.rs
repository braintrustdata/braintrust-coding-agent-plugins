//! Routing owns its sink behind a bounded typed port. Session dispatch is its facade.
use super::delivery::{Delivery, InputPort, OutputPort};
use super::plugins::{PluginOutcome, Plugins};
use super::prepare::Prepare;
use super::recovery::{RetryCondition, RouteFailure};
use super::submission::{
    Input as SubmissionInput, Operation, Outcome as SubmissionOutcome, Submission,
};
use super::{Completion, Stage};
use crate::{sink::Sink, translate::SpanOp, wire::SessionConfig};
use parking_lot::Mutex;
use std::{path::PathBuf, sync::Arc};
use tokio::sync::{mpsc, oneshot};

struct Request {
    config: SessionConfig,
    operation: Operation,
    reply: oneshot::Sender<Response>,
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
    task: tokio::task::JoinHandle<()>,
    config: Option<SessionConfig>,
    pending: bool,
    permalink: Option<String>,
}

impl RoutingSink {
    pub fn spawn(
        sink: Box<dyn Sink>,
        source: String,
        session_id: String,
        data_dir: PathBuf,
        last_error: Arc<Mutex<Option<String>>>,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel::<Delivery<Request>>(16);
        let task = tokio::spawn(async move {
            let mut plugins = Plugins::new(&source, &session_id);
            let mut prepare = Prepare::new(&source);
            let mut submission = Submission::new(sink);
            while let Ok(Some(delivery)) = InputPort::receive(&mut rx).await {
                let request = delivery.value();
                let (result, state) = match &request.operation {
                    Operation::Emit(ops) => {
                        let mut ops = ops.clone();
                        prepare.apply_shared(&mut ops, &request.config);
                        plugins.configure(&request.config.span_plugins);
                        let mut prepared = Vec::with_capacity(ops.len());
                        let mut failure = None;
                        let mut report = false;
                        for op in &ops {
                            if request.config.span_plugins.is_empty() {
                                prepared.push(op.clone());
                                continue;
                            }
                            match plugins.process(op.as_ref()).await {
                                PluginOutcome::Prepared(op) => {
                                    prepared.push(super::live::SharedOp::new(*op))
                                }
                                PluginOutcome::Filtered => {}
                                PluginOutcome::Failed(error) => {
                                    report |= error.failure.newly_seen;
                                    if error.failure.newly_seen {
                                        if let Err(diagnostic) = crate::plugin_diagnostics::record(
                                            &data_dir,
                                            &source,
                                            &error.failure.path,
                                            &error.failure.message,
                                        ) {
                                            tracing::warn!(%diagnostic, "plugin diagnostic could not be saved");
                                        }
                                    }
                                    failure = Some((
                                        anyhow::Error::new(error.clone()),
                                        RetryCondition::Plugin {
                                            timed_out: error.failure.timed_out,
                                            path: error.failure.path.clone(),
                                            digest: error.digest,
                                            processors: error.processors.clone(),
                                        },
                                    ));
                                }
                                PluginOutcome::Unavailable(error) => {
                                    report = true;
                                    failure = Some((error, RetryCondition::Transient));
                                }
                            }
                        }
                        let submit_input = SubmissionInput {
                            config: request.config.clone(),
                            operation: Operation::Emit(prepared),
                        };
                        let SubmissionOutcome::Emitted {
                            result: emitted,
                            state,
                        } = submission.process(&submit_input).await
                        else {
                            unreachable!("emit request returned a flush outcome")
                        };
                        match (emitted, failure) {
                            (Err(error), _) => (
                                Err(anyhow::Error::new(RouteFailure {
                                    cause: error.cause,
                                    retry: error.retry,
                                    emitted: 0,
                                    report: false,
                                })),
                                state,
                            ),
                            (Ok(emitted), Some((cause, retry))) => (
                                Err(anyhow::Error::new(RouteFailure {
                                    cause,
                                    retry,
                                    emitted,
                                    report,
                                })),
                                state,
                            ),
                            (Ok(emitted), None) => (Ok(emitted), state),
                        }
                    }
                    Operation::Flush => {
                        let submit_input = SubmissionInput {
                            config: request.config.clone(),
                            operation: Operation::Flush,
                        };
                        let SubmissionOutcome::Flushed { result, state } =
                            submission.process(&submit_input).await
                        else {
                            unreachable!("flush request returned an emit outcome")
                        };
                        (
                            result.map(|()| 0).map_err(|error| {
                                anyhow::Error::new(RouteFailure {
                                    cause: error.cause,
                                    retry: error.retry,
                                    emitted: 0,
                                    report: false,
                                })
                            }),
                            state,
                        )
                    }
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
                    pending: state.pending,
                    permalink: state.permalink,
                });
            }
        });
        Self {
            tx,
            task,
            config: None,
            pending: false,
            permalink: None,
        }
    }

    pub async fn shutdown(self) {
        let Self { tx, mut task, .. } = self;
        drop(tx);
        // Join the sink owner so completed shutdown also releases ledger files.
        // A stalled backend cannot keep daemon shutdown waiting indefinitely.
        if tokio::time::timeout(std::time::Duration::from_secs(2), &mut task)
            .await
            .is_err()
        {
            task.abort();
            let _ = task.await;
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
    async fn emit(&mut self, ops: &[SpanOp]) -> anyhow::Result<u64> {
        self.emit_shared(
            &ops.iter()
                .cloned()
                .map(super::live::SharedOp::new)
                .collect::<Vec<_>>(),
        )
        .await
    }
    async fn emit_shared(&mut self, ops: &[super::live::SharedOp]) -> anyhow::Result<u64> {
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
    async fn shutdown_releases_the_owned_sink_before_returning() {
        struct Owned(std::sync::Arc<std::sync::atomic::AtomicBool>);
        impl Drop for Owned {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        #[async_trait::async_trait]
        impl Sink for Owned {
            async fn emit(&mut self, _: &[SpanOp]) -> anyhow::Result<u64> {
                Ok(0)
            }
            async fn flush(&mut self) -> anyhow::Result<()> {
                Ok(())
            }
        }
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let tmp = tempfile::tempdir().unwrap();
        let sink = RoutingSink::spawn(
            Box::new(Owned(dropped.clone())),
            "debug".into(),
            "owned".into(),
            tmp.path().into(),
            Arc::new(Mutex::new(None)),
        );
        sink.shutdown().await;
        assert!(dropped.load(Ordering::SeqCst));
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
        let first = failure.downcast_ref::<RouteFailure>().unwrap();
        assert_eq!(first.emitted, 1);
        assert!(first.report);
        assert_eq!(emitted.load(Ordering::Relaxed), 1);
        let failure = sink.emit(&[op("accept")]).await.unwrap_err();
        let cached = failure.downcast_ref::<RouteFailure>().unwrap();
        assert_eq!(cached.emitted, 0);
        assert!(
            !cached.report,
            "cached failures must not request another warning"
        );
        assert!(error.lock().as_ref().unwrap().contains("rejected"));
        assert_eq!(emitted.load(Ordering::Relaxed), 1);
    }
}
