//! Route processing consumes managed deliveries independently of their source.
use super::{
    delivery::InputPort,
    recovery::{Boundary, Failure, Recovery, RecoveryOutcome},
    routing::RoutingSink,
    Delivery, Stage,
};
use crate::{
    delivery_ledger::{CommitFailure, LedgerSink},
    dispatch::Counters,
    sink::{Sink, SinkFactory},
    translate::SpanOp,
    wire::{RecoveryInfo, SessionConfig, SessionRoute},
    AuthProvider, AuthResolveReason,
};
use parking_lot::Mutex;
use std::{
    collections::VecDeque,
    path::PathBuf,
    sync::{atomic::Ordering, Arc},
};
use tokio::sync::{watch, Notify};

enum Work {
    Resubmit,
    Flush,
    Complete,
}
type Held = Box<(Failure, Work)>;

pub(crate) struct Consumer {
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    config: watch::Sender<SessionConfig>,
    stop: Arc<Notify>,
}
pub(crate) struct ConsumerOptions {
    pub source: String,
    pub session_id: String,
    pub version: String,
    pub data_dir: PathBuf,
    pub route: SessionRoute,
    pub config: watch::Sender<SessionConfig>,
    pub counters: Arc<Counters>,
    pub error: Arc<Mutex<Option<String>>>,
    pub recovery: Arc<Mutex<Option<RecoveryInfo>>>,
    pub permalink: Arc<Mutex<Option<String>>>,
    pub factory: Arc<dyn SinkFactory>,
    pub auth_provider: Option<Arc<dyn AuthProvider>>,
}

impl ConsumerOptions {
    async fn sink(&self, config: &SessionConfig) -> anyhow::Result<RoutingSink> {
        let inner = self.factory.create(&self.session_id, &self.source)?;
        let ledger = LedgerSink::new(
            inner,
            Some(&self.data_dir),
            &self.source,
            &self.session_id,
            Some(config),
            &self.version,
        )
        .await?;
        let mut sink = RoutingSink::spawn(
            Box::new(ledger),
            self.source.clone(),
            self.session_id.clone(),
            self.data_dir.clone(),
            self.error.clone(),
        );
        sink.configure(config);
        Ok(sink)
    }
    async fn hold(&self, pending: &mut VecDeque<Delivery<()>>, failure: &mut Failure) -> bool {
        let result = match pending.front_mut() {
            Some(delivery) => delivery.failed(failure).await,
            None => Ok(()), // Input errors are reported by the source itself.
        };
        *self.error.lock() = Some(failure.message());
        *self.recovery.lock() = Some(failure.info(false));
        if let Err(error) = result {
            tracing::warn!(%error,"delivery recovery evidence could not be reported; original failure retained");
            return false;
        }
        true
    }
    async fn credentials(&self, config: &mut SessionConfig) -> anyhow::Result<()> {
        let provider = self
            .auth_provider
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("daemon host has no auth provider"))?;
        let route = &self.route;
        let lease = crate::server::resolve_route_auth(
            provider.as_ref(),
            &route.auth,
            AuthResolveReason::Unauthorized,
            route.auth.org_name.as_deref(),
        )
        .await?;
        anyhow::ensure!(
            route.auth.profile_id.is_none() || route.auth.profile_id == lease.selection.profile_id,
            "credential refresh changed the pinned profile"
        );
        anyhow::ensure!(
            config.auth.org_id.is_none() || config.auth.org_id == lease.auth.org_id,
            "credential refresh changed the pinned organization"
        );
        anyhow::ensure!(
            config.auth.api_url.is_none() || config.auth.api_url == lease.auth.api_url,
            "credential refresh changed the pinned backend"
        );
        config.auth = lease.auth;
        Ok(())
    }

    fn storage(&self, error: &anyhow::Error, attempt: u32, work: Work) -> Held {
        let mut failure = Failure::storage(error, &self.version, attempt);
        failure.boundary = match work {
            Work::Complete => Boundary::Completion,
            Work::Flush => Boundary::Ledger,
            Work::Resubmit => failure.boundary,
        };
        Box::new((failure, work))
    }

    fn routing(&self, error: &anyhow::Error, config: &SessionConfig, attempt: u32) -> Held {
        if let Some(partial) = error.downcast_ref::<super::recovery::RouteFailure>() {
            self.counters
                .spans_emitted
                .fetch_add(partial.emitted, Ordering::Relaxed);
            if partial.report {
                tracing::warn!(session_id=%self.session_id, %error, "route processing failed");
            }
        }
        Box::new((
            Failure::routing(error, config, attempt, &self.version),
            Work::Resubmit,
        ))
    }

    // Keep the original acknowledgement obligations until checked delivery has
    // resolved every deferred row. Completion failures retry only acknowledgement.
    async fn finish(
        &self,
        pending: &mut VecDeque<Delivery<()>>,
        sink: &mut Option<RoutingSink>,
        config: &SessionConfig,
        attempt: u32,
        work: Work,
    ) -> Result<bool, Held> {
        match work {
            Work::Resubmit => {
                // Dropping unresolved obligations asks their source to replay.
                // Recreate the consumed SDK batch and its in-memory ledger too.
                *sink = None;
                pending.clear();
                return Ok(false);
            }
            Work::Flush => {
                let sink = sink.as_mut().ok_or_else(|| {
                    self.storage(
                        &anyhow::anyhow!("accepted delivery has no sink"),
                        attempt,
                        Work::Flush,
                    )
                })?;
                sink.flush().await.map_err(|error| {
                    if error.chain().any(|cause| cause.is::<CommitFailure>()) {
                        self.storage(&error, attempt, Work::Flush)
                    } else {
                        self.routing(&error, config, attempt)
                    }
                })?;
                *self.permalink.lock() = sink.permalink();
                if sink.has_pending_delivery() {
                    return Ok(false);
                }
            }
            Work::Complete => {}
        }
        while let Some(delivery) = pending.pop_front() {
            if let Err(unfinished) = delivery.into_completed().await {
                pending.push_front(unfinished.delivery);
                return Err(self.storage(&unfinished.error, attempt, Work::Complete));
            }
        }
        Ok(true)
    }
}

impl Consumer {
    pub fn spawn(
        mut input: Box<dyn InputPort<Item = Vec<SpanOp>>>,
        options: ConsumerOptions,
    ) -> Arc<Self> {
        let config = options.config.clone();
        let mut settings = config.subscribe();
        let stop = Arc::new(Notify::new());
        let handle = Arc::new(Self {
            task: Mutex::new(None),
            config: config.clone(),
            stop: stop.clone(),
        });
        let task = tokio::spawn(async move {
            let mut sink = None;
            let mut pending = VecDeque::new();
            let mut held: Option<Held> = None;
            let mut attempts = 0;
            let mut recorded = None;
            let mut record_retry = 0;
            loop {
                if let Some(waiting) = held.take() {
                    let (mut failure, work) = *waiting;
                    let mut reported = failure.clone();
                    if recorded.as_ref() != Some(&reported)
                        && super::recovery::now_ms() >= record_retry
                    {
                        if options.hold(&mut pending, &mut failure).await {
                            reported = failure.clone();
                            recorded = Some(reported.clone());
                        } else {
                            record_retry = super::recovery::now_ms().saturating_add(1_000);
                        }
                    }
                    tokio::select! {
                        _=stop.notified()=>break,
                        _=tokio::time::sleep(std::time::Duration::from_millis(250))=>{},
                        result=settings.changed()=>if result.is_err() { break; },
                    }
                    let mut current = settings.borrow_and_update().clone();
                    if recorded.as_ref() != Some(&reported)
                        || matches!(
                            (Recovery {
                                config: Some(&current),
                                version: &options.version
                            })
                            .process(&failure)
                            .await,
                            RecoveryOutcome::Waiting
                        )
                    {
                        held = Some(Box::new((failure, work)));
                        continue;
                    }
                    *options.recovery.lock() = Some(failure.info(true));
                    attempts = failure.attempt.saturating_add(1);
                    if matches!(failure.boundary, Boundary::Credentials) {
                        if let Err(error) = options.credentials(&mut current).await {
                            held = Some(Box::new((
                                Failure::credentials(error.to_string(), attempts),
                                work,
                            )));
                            continue;
                        }
                        config.send_replace(current.clone());
                    }
                    match options
                        .finish(&mut pending, &mut sink, &current, attempts, work)
                        .await
                    {
                        Ok(completed) => {
                            if completed {
                                *options.error.lock() = None;
                                *options.recovery.lock() = None;
                                recorded = None;
                            }
                        }
                        Err(failure) => held = Some(failure),
                    }
                    continue;
                }
                let attempt = async {
                    let received = tokio::select! {
                        result=input.receive()=>result,
                        result=settings.changed()=> {
                            return if result.is_err() { Ok(None) } else { Ok(Some(false)) };
                        },
                    };
                    let delivery = received.map_err(|error| match error.downcast::<Failure>() {
                        Ok(failure) => Box::new((failure, Work::Resubmit)),
                        Err(error) => options.storage(&error, attempts, Work::Resubmit),
                    })?;
                    let Some(delivery) = delivery else {
                        return Ok(None);
                    };
                    let current = settings.borrow_and_update().clone();
                    let ops = delivery.value().as_slice();
                    let result = async {
                        if sink.is_none() {
                            sink = Some(options.sink(&current).await.map_err(|error| {
                                if error.downcast_ref::<std::io::Error>().is_some()
                                    || error.downcast_ref::<serde_json::Error>().is_some()
                                {
                                    let mut failure =
                                        options.storage(&error, attempts, Work::Resubmit);
                                    failure.0.boundary = Boundary::Ledger;
                                    failure
                                } else {
                                    options.routing(&error, &current, attempts)
                                }
                            })?);
                        }
                        let routing = sink.as_mut().ok_or_else(|| {
                            options.storage(
                                &anyhow::anyhow!("configured sink is missing"),
                                attempts,
                                Work::Resubmit,
                            )
                        })?;
                        routing.configure(&current);
                        routing
                            .emit(ops)
                            .await
                            .map_err(|error| options.routing(&error, &current, attempts))
                    }
                    .await;
                    pending.push_back(delivery.map(|_| ()));
                    let count = result?;
                    options
                        .counters
                        .spans_emitted
                        .fetch_add(count, Ordering::Relaxed);
                    options
                        .finish(&mut pending, &mut sink, &current, attempts, Work::Flush)
                        .await
                        .map(Some)
                };
                let result = tokio::select! {
                    _=stop.notified()=>break,
                    result=attempt=>result,
                };
                match result {
                    Ok(None) => break,
                    Ok(Some(false)) => {}
                    Ok(Some(true)) => {
                        *options.error.lock() = None;
                        *options.recovery.lock() = None;
                        attempts = 0;
                        recorded = None;
                    }
                    Err(failure) => held = Some(failure),
                }
            }
        });
        *handle.task.lock() = Some(task);
        handle
    }
    pub fn configure(&self, config: SessionConfig) {
        self.config.send_if_modified(|current| {
            if serde_json::to_value(&*current).ok() == serde_json::to_value(&config).ok() {
                return false;
            }
            *current = config;
            true
        });
    }
    pub async fn shutdown(&self) {
        self.stop.notify_one();
        let task = self.task.lock().take();
        if let Some(task) = task {
            let _ = task.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        pipeline::journal::SpanJournal,
        translate::{SpanOp, SpanRow},
        wire::BackendAuth,
    };
    use std::sync::atomic::{AtomicBool, AtomicU64};

    #[derive(Default)]
    struct Recording {
        submitted: AtomicU64,
        accepted: AtomicU64,
        reject: AtomicBool,
        unauthorized: AtomicBool,
        commit_fail: AtomicBool,
        defer_until: AtomicU64,
    }
    struct Factory(Arc<Recording>);
    struct CheckedSink {
        record: Arc<Recording>,
        pending: u64,
    }
    impl SinkFactory for Factory {
        fn create(&self, _: &str, _: &str) -> anyhow::Result<Box<dyn Sink>> {
            Ok(Box::new(CheckedSink {
                record: self.0.clone(),
                pending: 0,
            }))
        }
    }
    #[async_trait::async_trait]
    impl Sink for CheckedSink {
        async fn emit(&mut self, ops: &[SpanOp]) -> anyhow::Result<u64> {
            self.pending += ops.len() as u64;
            self.record
                .submitted
                .fetch_add(ops.len() as u64, Ordering::SeqCst);
            Ok(ops.len() as u64)
        }
        async fn flush(&mut self) -> anyhow::Result<()> {
            if self.pending < self.record.defer_until.load(Ordering::SeqCst) {
                return Ok(());
            }
            // The SDK consumes its pending batch even when delivery is rejected.
            let pending = std::mem::take(&mut self.pending);
            if self.record.reject.swap(false, Ordering::SeqCst) {
                return Err(braintrust_sdk_rust::BraintrustError::Api {
                    status: if self.record.unauthorized.load(Ordering::SeqCst) {
                        401
                    } else {
                        503
                    },
                    message: "unavailable".into(),
                }
                .into());
            }
            self.record.accepted.fetch_add(pending, Ordering::SeqCst);
            if self.record.commit_fail.swap(false, Ordering::SeqCst) {
                return Err(CommitFailure(anyhow::anyhow!("injected ledger failure")).into());
            }
            Ok(())
        }
        fn has_pending_delivery(&self) -> bool {
            self.pending > 0
        }
    }
    fn options(
        dir: &std::path::Path,
        route: &SessionRoute,
        record: Arc<Recording>,
    ) -> ConsumerOptions {
        ConsumerOptions {
            source: "debug".into(),
            session_id: "test".into(),
            version: "test".into(),
            data_dir: dir.into(),
            route: route.clone(),
            config: watch::channel(route.with_auth(BackendAuth {
                token: "never-persist-this".into(),
                api_url: None,
                app_url: None,
                org_id: None,
                org_name: None,
            }))
            .0,
            counters: Arc::new(Counters::default()),
            error: Arc::new(Mutex::new(None)),
            recovery: Arc::new(Mutex::new(None)),
            permalink: Arc::new(Mutex::new(None)),
            factory: Arc::new(Factory(record)),
            auth_provider: None,
        }
    }
    fn spawn(
        cursor: crate::pipeline::journal::Cursor,
        options: ConsumerOptions,
    ) -> (
        Arc<Consumer>,
        crate::pipeline::journal::Progress,
        Arc<Mutex<Option<String>>>,
    ) {
        let subscription = cursor.into_input(
            options.config.subscribe(),
            options.version.clone(),
            options.error.clone(),
            options.recovery.clone(),
        );
        let error = options.error.clone();
        (
            Consumer::spawn(subscription.input, options),
            subscription.progress,
            error,
        )
    }
    async fn wait_for(mut predicate: impl FnMut() -> bool) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !predicate() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
    async fn stored(journal: &SpanJournal) {
        journal.begin(10, true).await.unwrap();
        journal
            .append(
                10,
                &[SpanOp::Insert(SpanRow {
                    span_id: "one".into(),
                    name: "one".into(),
                    root_span_id: "one".into(),
                    end_ms: Some(1),
                    ..Default::default()
                })],
            )
            .await
            .unwrap();
        journal.commit(10).await.unwrap();
    }
    #[tokio::test]
    async fn transient_rejection_resubmits_consumed_batch_before_completing() {
        let tmp = tempfile::tempdir().unwrap();
        let journal = SpanJournal::open(&tmp.path().join("spans.sqlite"))
            .await
            .unwrap();
        stored(&journal).await;
        let route = SessionRoute::default();
        let record = Arc::new(Recording::default());
        record.reject.store(true, Ordering::SeqCst);
        let cursor = journal.subscribe(&route, 0).await.unwrap();
        let opts = options(tmp.path(), &route, record.clone());
        let subscription = cursor.into_input(
            opts.config.subscribe(),
            opts.version.clone(),
            opts.error.clone(),
            opts.recovery.clone(),
        );
        let progress = subscription.progress;
        let mut input = subscription.input;
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let forward = tokio::spawn(async move {
            while let Ok(Some(delivery)) = input.receive().await {
                if tx.send(delivery).await.is_err() {
                    break;
                }
            }
        });
        let error = opts.error.clone();
        let consumer = Consumer::spawn(Box::new(rx), opts);
        wait_for(|| record.accepted.load(Ordering::SeqCst) == 1).await;
        wait_for(|| progress.completed() == 1).await;
        assert_eq!(record.submitted.load(Ordering::SeqCst), 2);
        assert!(error.lock().is_none());
        assert!(journal
            .failure(&serde_json::to_string(&route).unwrap())
            .await
            .unwrap()
            .is_none());
        consumer.shutdown().await;
        forward.abort();
        let _ = forward.await;
        journal.close().await.unwrap();
    }

    #[tokio::test]
    async fn transient_plugin_timeout_replays_the_held_operation_without_an_edit() {
        let tmp = tempfile::tempdir().unwrap();
        let plugin = tmp.path().join("redact.mjs");
        std::fs::write(&plugin, "let calls=0; export default span => {if(calls++===0) {while(true){}} return {...span,name:'redacted'};}").unwrap();
        let journal = SpanJournal::open(&tmp.path().join("spans.sqlite"))
            .await
            .unwrap();
        stored(&journal).await;
        let route = SessionRoute {
            span_plugins: vec![plugin],
            ..Default::default()
        };
        let record = Arc::new(Recording::default());
        let opts = options(tmp.path(), &route, record.clone());
        let recovery = opts.recovery.clone();
        let (consumer, progress, error) = spawn(journal.subscribe(&route, 0).await.unwrap(), opts);
        wait_for(|| recovery.lock().is_some()).await;
        assert_eq!(progress.completed(), 0);
        assert_eq!(record.accepted.load(Ordering::SeqCst), 0);
        assert_eq!(recovery.lock().as_ref().unwrap().stage, "plugin");
        wait_for(|| progress.completed() == 1).await;
        assert_eq!(record.accepted.load(Ordering::SeqCst), 1);
        assert!(error.lock().is_none());
        assert!(recovery.lock().is_none());
        assert!(journal
            .failure(&serde_json::to_string(&route).unwrap())
            .await
            .unwrap()
            .is_none());
        consumer.shutdown().await;
        journal.close().await.unwrap();
    }
    struct Acknowledged(Arc<AtomicU64>);
    #[async_trait::async_trait]
    impl crate::pipeline::Completion for Acknowledged {
        async fn complete(&mut self) -> anyhow::Result<()> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn plain_channel_uses_the_same_consumer_and_completion_contract() {
        let tmp = tempfile::tempdir().unwrap();
        let record = Arc::new(Recording::default());
        let acknowledged = Arc::new(AtomicU64::new(0));
        let opts = options(tmp.path(), &SessionRoute::default(), record.clone());
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let consumer = Consumer::spawn(Box::new(rx), opts);
        tx.send(Delivery::new(
            vec![SpanOp::Insert(SpanRow {
                span_id: "one".into(),
                name: "one".into(),
                root_span_id: "one".into(),
                end_ms: Some(1),
                ..Default::default()
            })],
            Acknowledged(acknowledged.clone()),
        ))
        .await
        .unwrap_or_else(|_| panic!("consumer stopped"));
        wait_for(|| acknowledged.load(Ordering::SeqCst) == 1).await;
        assert_eq!(record.accepted.load(Ordering::SeqCst), 1);
        consumer.shutdown().await;
    }

    #[tokio::test]
    async fn deferred_delivery_can_read_ahead_and_acknowledge_original_receipts() {
        let tmp = tempfile::tempdir().unwrap();
        let journal = SpanJournal::open(&tmp.path().join("spans.sqlite"))
            .await
            .unwrap();
        let ops: Vec<_> = (0..129)
            .map(|index| {
                SpanOp::Insert(SpanRow {
                    span_id: index.to_string(),
                    name: index.to_string(),
                    root_span_id: index.to_string(),
                    end_ms: Some(1),
                    ..Default::default()
                })
            })
            .collect();
        journal.begin(1, true).await.unwrap();
        journal.append(1, &ops).await.unwrap();
        journal.commit(1).await.unwrap();
        let route = SessionRoute::default();
        let record = Arc::new(Recording::default());
        record.defer_until.store(129, Ordering::SeqCst);
        let (consumer, progress, _error) = spawn(
            journal.subscribe(&route, 0).await.unwrap(),
            options(tmp.path(), &route, record.clone()),
        );
        wait_for(|| progress.completed() == 129).await;
        assert_eq!(record.submitted.load(Ordering::SeqCst), 129);
        assert_eq!(record.accepted.load(Ordering::SeqCst), 129);
        assert_eq!(progress.pending(), 0);
        consumer.shutdown().await;
    }

    #[tokio::test]
    async fn ledger_recovery_does_not_resubmit_accepted_work() {
        let tmp = tempfile::tempdir().unwrap();
        let journal = SpanJournal::open(&tmp.path().join("spans.sqlite"))
            .await
            .unwrap();
        stored(&journal).await;
        let route = SessionRoute::default();
        let record = Arc::new(Recording::default());
        record.commit_fail.store(true, Ordering::SeqCst);
        let (consumer, progress, error) = spawn(
            journal.subscribe(&route, 0).await.unwrap(),
            options(tmp.path(), &route, record.clone()),
        );
        wait_for(|| error.lock().is_some()).await;
        assert_eq!(progress.completed(), 0);
        assert_eq!(record.accepted.load(Ordering::SeqCst), 1);
        wait_for(|| progress.completed() == 1).await;
        assert_eq!(record.submitted.load(Ordering::SeqCst), 1);
        assert_eq!(record.accepted.load(Ordering::SeqCst), 1);
        consumer.shutdown().await;
    }

    #[tokio::test]
    async fn head_write_recovery_retries_only_completion() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("spans.sqlite");
        let journal = SpanJournal::open(&path).await.unwrap();
        stored(&journal).await;
        let route = SessionRoute::default();
        let cursor = journal.subscribe(&route, 0).await.unwrap();
        let db = rusqlite::Connection::open(&path).unwrap();
        db.execute_batch("CREATE TRIGGER fail_head BEFORE UPDATE OF position ON heads BEGIN SELECT RAISE(FAIL,'injected head failure'); END;").unwrap();
        let record = Arc::new(Recording::default());
        let (consumer, progress, error) =
            spawn(cursor, options(tmp.path(), &route, record.clone()));
        wait_for(|| error.lock().is_some()).await;
        assert_eq!(progress.completed(), 0);
        assert_eq!(record.accepted.load(Ordering::SeqCst), 1);
        db.execute_batch("DROP TRIGGER fail_head").unwrap();
        wait_for(|| progress.completed() == 1).await;
        assert_eq!(record.submitted.load(Ordering::SeqCst), 1);
        assert_eq!(record.accepted.load(Ordering::SeqCst), 1);
        consumer.shutdown().await;
    }
    #[tokio::test]
    async fn plugin_hold_survives_restart_and_file_edit_recovers_without_native_input() {
        let tmp = tempfile::tempdir().unwrap();
        let plugin = tmp.path().join("plugin.mjs");
        std::fs::write(&plugin, "export default () => {throw new Error('bad')}").unwrap();
        let journal = SpanJournal::open(&tmp.path().join("spans.sqlite"))
            .await
            .unwrap();
        stored(&journal).await;
        let route = SessionRoute {
            span_plugins: vec![plugin.clone()],
            ..Default::default()
        };
        let key = serde_json::to_string(&route).unwrap();
        let record = Arc::new(Recording::default());
        let (consumer, _progress, error) = spawn(
            journal.subscribe(&route, 0).await.unwrap(),
            options(tmp.path(), &route, record.clone()),
        );
        wait_for(|| error.lock().is_some()).await;
        consumer.shutdown().await;
        let evidence = journal.failure(&key).await.unwrap().unwrap();
        assert!(!evidence.contains("never-persist-this"));
        // Restart with an ordinary channel between the durable source and the
        // same consumer. Historical failure gating belongs to the source.
        let opts = options(tmp.path(), &route, record.clone());
        let subscription = journal.subscribe(&route, 0).await.unwrap().into_input(
            opts.config.subscribe(),
            opts.version.clone(),
            opts.error.clone(),
            opts.recovery.clone(),
        );
        let progress = subscription.progress;
        let mut input = subscription.input;
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let forward = tokio::spawn(async move {
            while let Ok(Some(delivery)) = input.receive().await {
                if tx.send(delivery).await.is_err() {
                    break;
                }
            }
        });
        let consumer = Consumer::spawn(Box::new(rx), opts);
        tokio::time::sleep(std::time::Duration::from_millis(600)).await;
        assert_eq!(record.submitted.load(Ordering::SeqCst), 0);
        std::fs::write(&plugin, "export default span => span").unwrap();
        wait_for(|| progress.completed() == 1).await;
        assert_eq!(record.accepted.load(Ordering::SeqCst), 1);
        assert!(journal.failure(&key).await.unwrap().is_none());
        consumer.shutdown().await;
        forward.abort();
        let _ = forward.await;
        journal.close().await.unwrap();
    }
    struct RefreshAuth(Arc<AtomicU64>);
    #[async_trait::async_trait]
    impl AuthProvider for RefreshAuth {
        async fn resolve(
            &self,
            selection: &crate::wire::AuthSelection,
            reason: AuthResolveReason,
        ) -> anyhow::Result<crate::AuthLease> {
            assert_eq!(reason, AuthResolveReason::Unauthorized);
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(crate::AuthLease {
                selection: selection.clone(),
                auth: BackendAuth {
                    token: "fresh".into(),
                    api_url: None,
                    app_url: None,
                    org_id: None,
                    org_name: None,
                },
                expires_at_ms: None,
            })
        }
    }
    #[tokio::test]
    async fn unauthorized_delivery_refreshes_credentials_at_the_same_head() {
        let tmp = tempfile::tempdir().unwrap();
        let journal = SpanJournal::open(&tmp.path().join("spans.sqlite"))
            .await
            .unwrap();
        stored(&journal).await;
        let route = SessionRoute::default();
        let record = Arc::new(Recording::default());
        record.reject.store(true, Ordering::SeqCst);
        record.unauthorized.store(true, Ordering::SeqCst);
        let refreshed = Arc::new(AtomicU64::new(0));
        let mut opts = options(tmp.path(), &route, record.clone());
        opts.auth_provider = Some(Arc::new(RefreshAuth(refreshed.clone())));
        let (consumer, progress, _error) = spawn(journal.subscribe(&route, 0).await.unwrap(), opts);
        wait_for(|| progress.completed() == 1).await;
        assert_eq!(refreshed.load(Ordering::SeqCst), 1);
        assert_eq!(consumer.config.borrow().auth.token, "fresh");
        assert_eq!(record.submitted.load(Ordering::SeqCst), 2);
        consumer.shutdown().await;
    }
    #[tokio::test]
    async fn real_sdk_rejection_holds_until_relevant_configuration_changes() {
        use wiremock::{
            matchers::{method, path},
            Mock, MockServer, ResponseTemplate,
        };
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/version"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .mount(&server)
            .await;
        let rejection = Mock::given(method("POST"))
            .and(path("/logs3"))
            .respond_with(ResponseTemplate::new(400).set_body_string("invalid row"))
            .expect(1..)
            .mount_as_scoped(&server)
            .await;
        let tmp = tempfile::tempdir().unwrap();
        let journal = SpanJournal::open(&tmp.path().join("spans.sqlite"))
            .await
            .unwrap();
        stored(&journal).await;
        let route = SessionRoute {
            destination: Some(crate::wire::TraceDestination::ProjectLogs {
                project_id: Some("project".into()),
                project_name: None,
            }),
            ..Default::default()
        };
        let mut opts = options(tmp.path(), &route, Arc::new(Recording::default()));
        opts.config.send_modify(|config| {
            config.auth.api_url = Some(server.uri());
            config.auth.app_url = Some(server.uri());
        });
        opts.factory = Arc::new(crate::BraintrustSinkFactory::new(
            crate::BraintrustSinkConfig::default(),
        ));
        let mut config = opts.config.borrow().clone();
        let (consumer, progress, error) = spawn(journal.subscribe(&route, 0).await.unwrap(), opts);
        wait_for(|| error.lock().is_some()).await;
        assert_eq!(progress.completed(), 0);
        drop(rejection);
        Mock::given(method("POST"))
            .and(path("/logs3"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1..)
            .mount(&server)
            .await;
        // A new credential cannot cure a permanently rejected span body.
        config.auth.token = "replacement-token".into();
        consumer.configure(config.clone());
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        assert_eq!(progress.completed(), 0);
        config.tags.push("corrected".into());
        consumer.configure(config);
        wait_for(|| progress.completed() == 1).await;
        assert!(error.lock().is_none());
        consumer.shutdown().await;
    }

    #[tokio::test]
    async fn editing_a_predecessor_releases_a_later_plugins_hold() {
        let tmp = tempfile::tempdir().unwrap();
        let first = tmp.path().join("first.mjs");
        let last = tmp.path().join("last.mjs");
        std::fs::write(&first, "export default span => ({...span,name:'bad'})").unwrap();
        std::fs::write(&last,"export default span => {if(span.name==='bad') throw new Error('bad input'); return span}").unwrap();
        let last_digest = crate::span_processor::file_digest(&last);
        let journal = SpanJournal::open(&tmp.path().join("spans.sqlite"))
            .await
            .unwrap();
        stored(&journal).await;
        let route = SessionRoute {
            span_plugins: vec![first.clone(), last.clone()],
            ..Default::default()
        };
        let record = Arc::new(Recording::default());
        let (consumer, progress, error) = spawn(
            journal.subscribe(&route, 0).await.unwrap(),
            options(tmp.path(), &route, record.clone()),
        );
        wait_for(|| error.lock().is_some()).await;
        let failure: Failure = serde_json::from_str(
            &journal
                .failure(&serde_json::to_string(&route).unwrap())
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        std::fs::write(
            tmp.path().join("unrelated.mjs"),
            "export default span => span",
        )
        .unwrap();
        let config = consumer.config.borrow().clone();
        assert!(matches!(
            (Recovery {
                config: Some(&config),
                version: "test"
            })
            .process(&failure)
            .await,
            RecoveryOutcome::Waiting
        ));
        std::fs::write(&first, "export default span => ({...span,name:'good'})").unwrap();
        wait_for(|| progress.completed() == 1).await;
        assert_eq!(crate::span_processor::file_digest(&last), last_digest);
        assert_eq!(record.accepted.load(Ordering::SeqCst), 1);
        consumer.shutdown().await;
    }
}
