//! One route owns processing and recovery at its durable SpanOp head.
use super::{
    delivery::InputPort,
    journal::{Cursor, SpanJournal},
    recovery::{Boundary, Failure, Recovery, RecoveryOutcome},
    routing::RoutingSink,
    Delivery, Stage,
};
use crate::{
    delivery_ledger::{CommitFailure, LedgerSink},
    dispatch::Counters,
    sink::{Sink, SinkFactory},
    wire::{RecoveryInfo, SessionConfig, SessionRoute},
    AuthProvider, AuthResolveReason,
};
use parking_lot::Mutex;
use std::{
    path::PathBuf,
    sync::{atomic::Ordering, Arc},
};
use tokio::sync::{watch, Notify};

enum Work {
    Resubmit,
    Flush,
    Complete(Delivery<()>),
}
type Held = Box<(Failure, Work)>;

pub(crate) struct Consumer {
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    config: watch::Sender<SessionConfig>,
    progress: watch::Receiver<u64>,
    stop: Arc<Notify>,
    error: Arc<Mutex<Option<String>>>,
}
pub(crate) struct ConsumerOptions {
    pub source: String,
    pub session_id: String,
    pub version: String,
    pub data_dir: PathBuf,
    pub config: SessionConfig,
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
        .await;
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
    async fn hold(&self, journal: &SpanJournal, cursor: &Cursor, failure: &mut Failure) -> bool {
        failure.through.get_or_insert(cursor.position());
        *self.error.lock() = Some(failure.message());
        *self.recovery.lock() = Some(failure.info(false));
        if let Err(error) = async {
            let encoded = serde_json::to_string(failure)?;
            journal.set_failure(&cursor.route, Some(encoded)).await
        }
        .await
        {
            tracing::warn!(%error,"route recovery evidence could not be persisted; original failure retained");
            return false;
        }
        true
    }
    async fn credentials(&self, route: &str, config: &mut SessionConfig) -> anyhow::Result<()> {
        let provider = self
            .auth_provider
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("daemon host has no auth provider"))?;
        let route: SessionRoute = serde_json::from_str(route)?;
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

    fn storage(&self, error: &anyhow::Error, cursor: &Cursor, attempt: u32, work: Work) -> Held {
        let mut failure = Failure::storage(
            error,
            cursor.completed_position() + 1,
            &self.version,
            attempt,
        );
        failure.boundary = match work {
            Work::Complete(_) => Boundary::Completion,
            Work::Flush => Boundary::Ledger,
            Work::Resubmit => failure.boundary,
        };
        Box::new((failure, work))
    }

    fn routing(
        &self,
        error: &anyhow::Error,
        cursor: &Cursor,
        config: &SessionConfig,
        attempt: u32,
    ) -> Held {
        if let Some(partial) = error.downcast_ref::<super::routing::EmitFailure>() {
            self.counters
                .spans_emitted
                .fetch_add(partial.emitted, Ordering::Relaxed);
            if partial.report {
                tracing::warn!(session_id=%self.session_id, %error, "route processing failed");
            }
        }
        Box::new((
            Failure::routing(error, cursor.completed_position() + 1, config, attempt),
            Work::Resubmit,
        ))
    }

    // Normal delivery and recovery share the same acceptance/commit path. Only a
    // rejected batch is resubmitted; accepted work retains its local retry point.
    async fn finish(
        &self,
        cursor: &mut Cursor,
        sink: &mut Option<RoutingSink>,
        config: &SessionConfig,
        attempt: u32,
        work: Work,
    ) -> Result<bool, Held> {
        let completed = !matches!(work, Work::Resubmit);
        let completion = match work {
            Work::Resubmit => {
                // The SDK consumed the rejected batch. Recreate its ledger too,
                // so in-memory filtering cannot turn a retry into an empty ack.
                *sink = None;
                None
            }
            Work::Complete(delivery) => Some(delivery),
            Work::Flush => {
                let sink = sink.as_mut().ok_or_else(|| {
                    self.storage(
                        &anyhow::anyhow!("accepted delivery has no sink"),
                        cursor,
                        attempt,
                        Work::Flush,
                    )
                })?;
                sink.flush().await.map_err(|error| {
                    if error.downcast_ref::<CommitFailure>().is_some() {
                        self.storage(&error, cursor, attempt, Work::Flush)
                    } else {
                        self.routing(&error, cursor, config, attempt)
                    }
                })?;
                *self.permalink.lock() = sink.permalink();
                // Deferred roots keep their prefix unresolved until a child
                // releases them; the cursor can read ahead without buffering ops.
                if sink.has_pending_delivery() {
                    return Ok(false);
                }
                Some(cursor.pending_prefix())
            }
        };
        if let Some(delivery) = completion {
            delivery.into_completed().await.map_err(|unfinished| {
                self.storage(
                    &unfinished.error,
                    cursor,
                    attempt,
                    Work::Complete(unfinished.delivery),
                )
            })?;
        }
        cursor
            .rewind()
            .await
            .map_err(|error| self.storage(&error, cursor, attempt, Work::Resubmit))?;
        Ok(completed)
    }
}

impl Consumer {
    pub fn spawn(journal: SpanJournal, mut cursor: Cursor, options: ConsumerOptions) -> Arc<Self> {
        let (config, mut settings) = watch::channel(options.config.clone());
        let (progress, receive) = watch::channel(cursor.position());
        let stop = Arc::new(Notify::new());
        let handle = Arc::new(Self {
            task: Mutex::new(None),
            config: config.clone(),
            progress: receive,
            stop: stop.clone(),
            error: options.error.clone(),
        });
        let task = tokio::spawn(async move {
            let mut sink = None;
            let mut held = match journal.failure(&cursor.route).await {
                Ok(Some(raw)) => Some(Box::new((
                    serde_json::from_str(&raw).unwrap_or_else(|_| {
                        Failure::legacy(raw, cursor.completed_position() + 1, &options.config)
                    }),
                    Work::Resubmit,
                ))),
                Ok(None) => None,
                Err(error) => Some(options.storage(&error, &cursor, 0, Work::Resubmit)),
            };
            let mut attempts = 0;
            let mut recorded = None;
            let mut record_retry = 0;
            loop {
                if let Some(pending) = held.take() {
                    let (mut failure, work) = *pending;
                    if matches!(failure.boundary, Boundary::Completion)
                        && cursor.completed_position()
                            >= failure.through.unwrap_or(failure.position)
                        && journal.set_failure(&cursor.route, None).await.is_ok()
                    {
                        *options.error.lock() = None;
                        *options.recovery.lock() = None;
                        continue;
                    }
                    failure.through.get_or_insert(cursor.position());
                    let encoded = match serde_json::to_string(&failure) {
                        Ok(encoded) => encoded,
                        Err(error) => {
                            held = Some(Box::new((
                                Failure::revision(
                                    Boundary::Journal,
                                    failure.position,
                                    format!(
                                        "{}; recovery evidence cannot be encoded: {error}",
                                        failure.message()
                                    ),
                                    &options.version,
                                ),
                                work,
                            )));
                            continue;
                        }
                    };
                    if recorded.as_ref() != Some(&encoded)
                        && super::recovery::now_ms() >= record_retry
                    {
                        if options.hold(&journal, &cursor, &mut failure).await {
                            recorded = Some(encoded.clone());
                        } else {
                            record_retry = super::recovery::now_ms().saturating_add(1_000);
                        }
                    }
                    progress.send_replace(cursor.completed_position());
                    tokio::select! {
                        _=stop.notified()=>break,
                        _=tokio::time::sleep(std::time::Duration::from_millis(250))=>{},
                        result=settings.changed()=>if result.is_err() { break; },
                    }
                    let mut current = settings.borrow_and_update().clone();
                    if recorded.as_ref() != Some(&encoded)
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
                        if let Err(error) = options.credentials(&cursor.route, &mut current).await {
                            held = Some(Box::new((
                                Failure::credentials(
                                    cursor.completed_position() + 1,
                                    error.to_string(),
                                    attempts,
                                ),
                                work,
                            )));
                            continue;
                        }
                        config.send_replace(current.clone());
                    }
                    match options
                        .finish(&mut cursor, &mut sink, &current, attempts, work)
                        .await
                    {
                        Ok(completed) => {
                            progress.send_replace(cursor.completed_position());
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
                    let tail = journal.tail().await.map_err(|error| {
                        options.storage(&error, &cursor, attempts, Work::Resubmit)
                    })?;
                    options.counters.queued.store(
                        tail.saturating_sub(cursor.completed_position()),
                        Ordering::Relaxed,
                    );
                    let received = tokio::select! {
                        result=InputPort::receive(&mut cursor)=>result,
                        result=settings.changed()=> {
                            return if result.is_err() { Ok(None) } else { Ok(Some(false)) };
                        },
                    };
                    let delivery = received.map_err(|error| {
                        options.storage(&error, &cursor, attempts, Work::Resubmit)
                    })?;
                    let Some(delivery) = delivery else {
                        return Ok(None);
                    };
                    let current = settings.borrow_and_update().clone();
                    if sink.is_none() {
                        sink = Some(options.sink(&current).await.map_err(|error| {
                            options.routing(&error, &cursor, &current, attempts)
                        })?);
                    }
                    let routing = sink.as_mut().ok_or_else(|| {
                        options.storage(
                            &anyhow::anyhow!("configured sink is missing"),
                            &cursor,
                            attempts,
                            Work::Resubmit,
                        )
                    })?;
                    routing.configure(&current);
                    let count = routing
                        .emit(delivery.value())
                        .await
                        .map_err(|error| options.routing(&error, &cursor, &current, attempts))?;
                    options
                        .counters
                        .spans_emitted
                        .fetch_add(count, Ordering::Relaxed);
                    drop(delivery);
                    options
                        .finish(&mut cursor, &mut sink, &current, attempts, Work::Flush)
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
                        progress.send_replace(cursor.completed_position());
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
    pub async fn through(&self, tail: u64) -> Result<(), String> {
        let mut progress = self.progress.clone();
        loop {
            if *progress.borrow_and_update() >= tail {
                return Ok(());
            }
            if let Some(error) = self.error.lock().clone() {
                return Err(error);
            }
            progress
                .changed()
                .await
                .map_err(|_| "routing actor is gone".to_owned())?;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
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
            config: route.with_auth(BackendAuth {
                token: "never-persist-this".into(),
                api_url: None,
                app_url: None,
                org_id: None,
                org_name: None,
            }),
            counters: Arc::new(Counters::default()),
            error: Arc::new(Mutex::new(None)),
            recovery: Arc::new(Mutex::new(None)),
            permalink: Arc::new(Mutex::new(None)),
            factory: Arc::new(Factory(record)),
            auth_provider: None,
        }
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
        let consumer = Consumer::spawn(
            journal.clone(),
            cursor,
            options(tmp.path(), &route, record.clone()),
        );
        wait_for(|| record.accepted.load(Ordering::SeqCst) == 1).await;
        wait_for(|| *consumer.progress.borrow() == 1).await;
        assert_eq!(record.submitted.load(Ordering::SeqCst), 2);
        assert!(consumer.error.lock().is_none());
        assert!(journal
            .failure(&serde_json::to_string(&route).unwrap())
            .await
            .unwrap()
            .is_none());
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
        let consumer = Consumer::spawn(
            journal.clone(),
            journal.subscribe(&route, 0).await.unwrap(),
            options(tmp.path(), &route, record.clone()),
        );
        wait_for(|| consumer.error.lock().is_some()).await;
        assert_eq!(*consumer.progress.borrow(), 0);
        assert_eq!(record.accepted.load(Ordering::SeqCst), 1);
        wait_for(|| *consumer.progress.borrow() == 1).await;
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
        let consumer = Consumer::spawn(
            journal.clone(),
            cursor,
            options(tmp.path(), &route, record.clone()),
        );
        wait_for(|| consumer.error.lock().is_some()).await;
        assert_eq!(*consumer.progress.borrow(), 0);
        assert_eq!(record.accepted.load(Ordering::SeqCst), 1);
        db.execute_batch("DROP TRIGGER fail_head").unwrap();
        wait_for(|| *consumer.progress.borrow() == 1).await;
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
        let consumer = Consumer::spawn(
            journal.clone(),
            journal.subscribe(&route, 0).await.unwrap(),
            options(tmp.path(), &route, record.clone()),
        );
        wait_for(|| consumer.error.lock().is_some()).await;
        consumer.shutdown().await;
        let evidence = journal.failure(&key).await.unwrap().unwrap();
        assert!(!evidence.contains("never-persist-this"));
        let consumer = Consumer::spawn(
            journal.clone(),
            journal.subscribe(&route, 0).await.unwrap(),
            options(tmp.path(), &route, record.clone()),
        );
        tokio::time::sleep(std::time::Duration::from_millis(600)).await;
        assert_eq!(record.submitted.load(Ordering::SeqCst), 0);
        std::fs::write(&plugin, "export default span => span").unwrap();
        wait_for(|| *consumer.progress.borrow() == 1).await;
        assert_eq!(record.accepted.load(Ordering::SeqCst), 1);
        assert!(journal.failure(&key).await.unwrap().is_none());
        consumer.shutdown().await;
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
        let consumer = Consumer::spawn(
            journal.clone(),
            journal.subscribe(&route, 0).await.unwrap(),
            opts,
        );
        wait_for(|| *consumer.progress.borrow() == 1).await;
        assert_eq!(refreshed.load(Ordering::SeqCst), 1);
        assert_eq!(consumer.config.borrow().auth.token, "fresh");
        assert_eq!(record.submitted.load(Ordering::SeqCst), 2);
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
        let consumer = Consumer::spawn(
            journal.clone(),
            journal.subscribe(&route, 0).await.unwrap(),
            options(tmp.path(), &route, record.clone()),
        );
        wait_for(|| consumer.error.lock().is_some()).await;
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
        wait_for(|| *consumer.progress.borrow() == 1).await;
        assert_eq!(crate::span_processor::file_digest(&last), last_digest);
        assert_eq!(record.accepted.load(Ordering::SeqCst), 1);
        consumer.shutdown().await;
    }
}
