//! Per-session dispatch. Each session owns an ordered queue and a single actor
//! task that runs its translator + sink serially, so events for one session
//! are processed strictly in arrival order. Different sessions run
//! concurrently.
//!
//! Hook acknowledgement happens before this layer, immediately after the raw
//! event is durably journaled. Translation, correlation, and delivery here are
//! entirely out-of-band from hook execution.

use crate::delivery_ledger::LedgerSink;
use crate::journal::JournalWriter;
use crate::sink::SinkFactory;
use crate::translate::{Registry, SessionCtx};
use crate::wire::{Envelope, SessionRoute};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot};

/// Bound on a session's in-flight queue. Enqueue awaits a slot rather than
/// letting a stalled sink accumulate events without limit; the daemon has
/// already journaled anything waiting here, so backpressure costs latency,
/// never data.
const QUEUE_CAPACITY: usize = 1024;

/// An actor exists from routing information before credentials are resolved.
/// Its initial translation replay cannot submit output yet.
struct UnconfiguredSink;

#[async_trait::async_trait]
impl crate::sink::Sink for UnconfiguredSink {
    async fn emit(&mut self, _ops: &[crate::translate::SpanOp]) -> anyhow::Result<u64> {
        anyhow::bail!("delivery attempted before route authentication")
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

#[derive(Default)]
pub struct Counters {
    pub queued: AtomicU64,
    pub spans_emitted: AtomicU64,
}

enum SessionMsg {
    Event(Box<Envelope>, u64),
    Configure(Box<crate::wire::SessionConfig>, oneshot::Sender<()>),
    AuthFailure(String, u64),
    Barrier(oneshot::Sender<()>),
    Flush(oneshot::Sender<u64>),
    Finalize(oneshot::Sender<u64>),
    Shutdown(oneshot::Sender<()>),
}

/// Where to rebuild a session's translator state from, streamed at startup.
pub struct ReplayPlan {
    /// Replay stops here — the journal's length when this session was
    /// created, so the event creating it is not replayed and then delivered
    /// a second time from the queue.
    pub through: u64,
    /// Events through this offset reached the backend before the prior daemon
    /// stopped. Replay still rebuilds translator state from them, but must not
    /// deliver them again.
    pub acknowledged_through: u64,
}

pub(crate) struct SessionOptions {
    pub session_id: String,
    pub source: String,
    pub plugin_version: Option<String>,
    pub replay: Option<ReplayPlan>,
    pub config: crate::wire::SessionConfig,
    pub correlation_key: String,
    pub route: SessionRoute,
    pub correlation: Arc<crate::correlation::CorrelationRegistry>,
    pub data_dir: PathBuf,
    pub journal: Arc<tokio::sync::Mutex<JournalWriter>>,
    pub correlation_changed: Arc<tokio::sync::Notify>,
    pub auth_provider: Option<Arc<dyn crate::server::AuthProvider>>,
    pub derived: Arc<crate::derived::SourceTranslation>,
}

/// Handle to one live session: its queue plus observable counters/state.
pub struct Session {
    pub source: String,
    tx: mpsc::Sender<SessionMsg>,
    pub counters: Arc<Counters>,
    pub last_error: Arc<Mutex<Option<String>>>,
    pub permalink: Arc<Mutex<Option<String>>>,
    pause: Arc<Mutex<Option<PausedWork>>>,
    last_activity: Mutex<Instant>,
}

#[derive(Clone, Default)]
struct PausedWork {
    marker_span_id: Option<String>,
    marker_emitted: bool,
    marker_cleared: bool,
}

impl Session {
    /// Spawn a session's actor task and return its handle.
    pub fn spawn(
        options: SessionOptions,
        _translators: Arc<Registry>,
        sink_factory: Arc<dyn SinkFactory>,
    ) -> Arc<Session> {
        let SessionOptions {
            session_id,
            source,
            plugin_version,
            replay,
            config,
            correlation_key,
            route,
            correlation,
            data_dir,
            journal,
            correlation_changed,
            auth_provider,
            derived,
        } = options;
        let (tx, rx) = mpsc::channel(QUEUE_CAPACITY);
        let counters = Arc::new(Counters::default());
        let last_error = Arc::new(Mutex::new(None));
        let permalink = Arc::new(Mutex::new(None));
        let pause = Arc::new(Mutex::new(None));

        let actor = SessionActor {
            session_id: session_id.clone(),
            source: source.clone(),
            plugin_version,
            sink_factory,
            counters: counters.clone(),
            last_error: last_error.clone(),
            permalink: permalink.clone(),
            pause: pause.clone(),
            replay,
            config,
            correlation_key,
            route,
            correlation,
            data_dir,
            journal,
            correlation_changed,
            auth_provider,
            derived,
            delivered_sequence: AtomicU64::new(0),
        };
        tokio::spawn(actor.run(rx));

        Arc::new(Session {
            source,
            tx,
            counters,
            last_error,
            permalink,
            pause,
            last_activity: Mutex::new(Instant::now()),
        })
    }

    /// Queue a journaled event without waiting for translation or delivery.
    pub async fn enqueue(&self, env: Envelope, journal_through: u64) -> anyhow::Result<()> {
        self.touch();
        self.counters.queued.fetch_add(1, Ordering::Relaxed);
        self.tx
            .send(SessionMsg::Event(Box::new(env), journal_through))
            .await
            .map_err(|_| anyhow::anyhow!("session actor is gone"))
    }

    fn touch(&self) {
        *self.last_activity.lock().unwrap() = Instant::now();
    }

    /// Wait until events already accepted by this daemon worker have updated
    /// translator and correlation state. Hook capture never calls this.
    pub async fn barrier(&self) {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self.tx.send(SessionMsg::Barrier(reply_tx)).await.is_ok() {
            let _ = reply_rx.await;
        }
    }

    /// How long since this session last saw traffic. Drives idle retirement.
    pub fn idle_for(&self) -> std::time::Duration {
        if self.pause.lock().unwrap().is_some() {
            return std::time::Duration::ZERO;
        }
        self.last_activity.lock().unwrap().elapsed()
    }

    pub fn has_paused_work(&self) -> bool {
        self.pause.lock().unwrap().is_some()
    }

    /// Insert a flush in actor order without waiting for backend delivery.
    pub(crate) async fn enqueue_flush(&self) -> anyhow::Result<oneshot::Receiver<u64>> {
        self.touch();
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(SessionMsg::Flush(reply_tx))
            .await
            .map_err(|_| anyhow::anyhow!("session actor is gone"))?;
        Ok(reply_rx)
    }

    /// Ask the actor to drain and flush its sink, bounded by `timeout`.
    /// Returns `(flushed, pending)`.
    pub async fn flush(&self, timeout: std::time::Duration) -> (bool, u64) {
        let Ok(reply_rx) = self.enqueue_flush().await else {
            return (false, self.counters.queued.load(Ordering::Relaxed));
        };
        match tokio::time::timeout(timeout, reply_rx).await {
            Ok(Ok(pending)) => (pending == 0, pending),
            _ => (false, self.counters.queued.load(Ordering::Relaxed)),
        }
    }

    /// Finalize an invocation-local session and flush its sink. Unlike a
    /// delivery checkpoint, a managed-run completion is a terminal boundary.
    pub async fn finalize(&self, timeout: std::time::Duration) -> (bool, u64) {
        self.touch();
        let (reply_tx, reply_rx) = oneshot::channel();
        if self.tx.send(SessionMsg::Finalize(reply_tx)).await.is_err() {
            return (false, self.counters.queued.load(Ordering::Relaxed));
        }
        match tokio::time::timeout(timeout, reply_rx).await {
            Ok(Ok(pending)) => (pending == 0, pending),
            _ => (false, self.counters.queued.load(Ordering::Relaxed)),
        }
    }

    /// Reconfigure the sink before a refresh-triggered flush. Queue ordering
    /// guarantees that all earlier events are processed first.
    pub async fn configure(&self, config: crate::wire::SessionConfig) -> anyhow::Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(SessionMsg::Configure(Box::new(config), reply_tx))
            .await
            .map_err(|_| anyhow::anyhow!("session actor is gone"))?;
        reply_rx
            .await
            .map_err(|_| anyhow::anyhow!("session actor dropped configuration reply"))
    }

    pub async fn auth_failure(&self, error: String, first_unprocessed: u64) -> anyhow::Result<()> {
        self.tx
            .send(SessionMsg::AuthFailure(error, first_unprocessed))
            .await
            .map_err(|_| anyhow::anyhow!("session actor is gone"))
    }

    /// Drain, flush, and stop the actor (used on daemon shutdown and when an
    /// idle session is retired).
    pub async fn shutdown(&self) {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self.tx.send(SessionMsg::Shutdown(reply_tx)).await.is_ok() {
            let _ = reply_rx.await;
        }
    }
}

/// Agent transcript files are external mutable state. Mirror them into
/// daemon-owned storage at lifecycle boundaries and journal only bounded
/// references, so recovery sees exactly the bytes that live translation
/// observed instead of depending on mutable external paths or copying a full
/// transcript into every event. Capture failures remain fail-open.
pub(crate) async fn hydrate_transcript_reference(data_dir: &std::path::Path, env: &mut Envelope) {
    if env.source == "cursor" {
        hydrate_cursor_transcript_reference(data_dir, env).await;
        return;
    }
    if env.source == "grok" {
        hydrate_grok_transcript_references(data_dir, env).await;
        return;
    }
    if env.source == "claude-code" {
        hydrate_claude_transcript_references(data_dir, env).await;
        return;
    }
    if env.source != "codex" {
        return;
    }
    let field = if env.event == "SubagentStop" {
        "agent_transcript_path"
    } else {
        "transcript_path"
    };
    let Some(path) = env
        .payload
        .get(field)
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
    else {
        return;
    };
    let mirror_session = crate::ids::session_namespace(&env.source, &env.session_id);
    let (mirror, through) =
        match crate::transcript_mirror::capture(data_dir, &mirror_session, &path).await {
            Ok(captured) => captured,
            Err(error) => {
                tracing::debug!(session_id = %env.session_id, %error, "transcript mirror skipped");
                return;
            }
        };
    if let Some(payload) = env.payload.as_object_mut() {
        payload.insert(
            "_bt_transcript_mirror".to_string(),
            serde_json::json!({
                "path": path,
                "mirror": mirror.to_string_lossy(),
                "through": through,
            }),
        );
    }
}

async fn hydrate_cursor_transcript_reference(data_dir: &std::path::Path, env: &mut Envelope) {
    if env.payload.get("_bt_transcript_mirror").is_some() {
        return;
    }
    let path = env
        .payload
        .get("transcript_path")
        .and_then(serde_json::Value::as_str);
    let mut observation = serde_json::json!({});
    if let Some(path) = path.filter(|path| !path.is_empty()) {
        let observed_bytes = env
            .payload
            .get("_bt_transcript_observation")
            .filter(|observation| {
                observation.get("path").and_then(serde_json::Value::as_str) == Some(path)
            })
            .and_then(|observation| observation.get("observed_bytes"))
            .and_then(serde_json::Value::as_u64);
        let mirror_session = crate::ids::session_namespace(&env.source, &env.session_id);
        match crate::transcript_mirror::capture_generation(
            data_dir,
            &mirror_session,
            path,
            observed_bytes,
        )
        .await
        {
            Ok((mirror, through)) => {
                observation =
                    serde_json::json!({"path": path, "mirror": mirror, "through": through});
            }
            Err(error) => {
                tracing::debug!(session_id = %env.session_id, %error, "Cursor transcript mirror skipped");
            }
        }
    }
    if let Some(payload) = env.payload.as_object_mut() {
        // An empty observation is also durable: no transcript was available
        // at this boundary, even if the external path appears during replay.
        payload.insert("_bt_transcript_mirror".to_string(), observation);
    }
}

async fn hydrate_claude_transcript_references(data_dir: &std::path::Path, env: &mut Envelope) {
    // Historical snapshots and newly captured references are observations,
    // not invitations to reread an external file during journal recovery.
    if env.payload.get("_bt_claude_transcript_mirrors").is_some()
        || env.payload.get("_bt_transcript_mirror").is_some()
        || env.payload.get("_bt_transcript_snapshot").is_some()
        || env.payload.get("_bt_transcript_replay").is_some()
    {
        return;
    }
    let mirror_session = crate::ids::session_namespace(&env.source, &env.session_id);
    let mut mirrors = serde_json::Map::new();
    for field in ["transcript_path", "agent_transcript_path"] {
        let Some(path) = env.payload.get(field).and_then(serde_json::Value::as_str) else {
            continue;
        };
        if mirrors.contains_key(path) {
            continue;
        }
        match crate::transcript_mirror::capture(data_dir, &mirror_session, path).await {
            Ok((mirror, through)) => {
                mirrors.insert(
                    path.to_string(),
                    serde_json::json!({"path": path, "mirror": mirror, "through": through}),
                );
            }
            Err(error) => {
                tracing::debug!(session_id = %env.session_id, %error, "Claude transcript mirror skipped");
            }
        }
    }
    if !env.payload.is_object() {
        env.payload = serde_json::json!({});
    }
    env.payload.as_object_mut().unwrap().insert(
        "_bt_claude_transcript_mirrors".to_string(),
        serde_json::Value::Object(mirrors),
    );
}

const GROK_TERMINAL_SNAPSHOT_ATTEMPTS: usize = 4;
const GROK_TERMINAL_SNAPSHOT_QUIET_PERIOD: std::time::Duration =
    std::time::Duration::from_millis(10);

async fn hydrate_grok_transcript_references(data_dir: &std::path::Path, env: &mut Envelope) {
    let Some(transcript) = env
        .payload
        .get("transcriptPath")
        .or_else(|| env.payload.get("transcript_path"))
        .and_then(serde_json::Value::as_str)
        .map(std::path::PathBuf::from)
    else {
        return;
    };
    let Some(session_dir) = transcript.parent() else {
        return;
    };
    let mirror_session = crate::ids::session_namespace(&env.source, &env.session_id);
    let mut mirrors = serde_json::Map::new();
    capture_grok_transcript_pass(
        data_dir,
        session_dir,
        &mirror_session,
        &env.session_id,
        &mut mirrors,
    )
    .await;

    if matches!(env.event.as_str(), "SessionEnd" | "session_end") {
        for _ in 0..GROK_TERMINAL_SNAPSHOT_ATTEMPTS {
            tokio::time::sleep(GROK_TERMINAL_SNAPSHOT_QUIET_PERIOD).await;
            capture_grok_transcript_pass(
                data_dir,
                session_dir,
                &mirror_session,
                &env.session_id,
                &mut mirrors,
            )
            .await;
        }
    }

    if !mirrors.is_empty() {
        if let Some(payload) = env.payload.as_object_mut() {
            payload.insert(
                "_bt_grok_transcript_mirrors".to_string(),
                serde_json::Value::Object(mirrors),
            );
        }
    }
}

async fn capture_grok_transcript_pass(
    data_dir: &std::path::Path,
    session_dir: &std::path::Path,
    mirror_session: &str,
    session_id: &str,
    mirrors: &mut serde_json::Map<String, serde_json::Value>,
) {
    for (name, key) in [
        ("updates.jsonl", "updates"),
        ("events.jsonl", "events"),
        ("system_prompt.txt", "system_prompt"),
    ] {
        let source = session_dir.join(name);
        let Some(source_str) = source.to_str() else {
            continue;
        };
        match crate::transcript_mirror::capture(data_dir, mirror_session, source_str).await {
            Ok((mirror, through)) => {
                mirrors.insert(
                    key.to_string(),
                    serde_json::json!({
                        "path": source,
                        "mirror": mirror,
                        "through": through,
                    }),
                );
            }
            Err(error) => {
                tracing::debug!(%session_id, %error, file = name, "Grok transcript mirror skipped");
            }
        }
    }
}

struct SessionActor {
    session_id: String,
    source: String,
    plugin_version: Option<String>,
    sink_factory: Arc<dyn SinkFactory>,
    counters: Arc<Counters>,
    last_error: Arc<Mutex<Option<String>>>,
    permalink: Arc<Mutex<Option<String>>>,
    pause: Arc<Mutex<Option<PausedWork>>>,
    replay: Option<ReplayPlan>,
    config: crate::wire::SessionConfig,
    correlation_key: String,
    route: SessionRoute,
    correlation: Arc<crate::correlation::CorrelationRegistry>,
    data_dir: PathBuf,
    journal: Arc<tokio::sync::Mutex<JournalWriter>>,
    correlation_changed: Arc<tokio::sync::Notify>,
    auth_provider: Option<Arc<dyn crate::server::AuthProvider>>,
    derived: Arc<crate::derived::SourceTranslation>,
    delivered_sequence: AtomicU64,
}

#[derive(Clone, Copy)]
enum BatchMode {
    Live,
    Replay,
    Checkpoint,
    TerminalFinalize,
}

impl BatchMode {
    fn errors(self) -> (&'static str, &'static str) {
        match self {
            Self::Live => ("translate failed", "sink emit failed"),
            Self::Replay => ("journal replay failed", "sink replay emit failed"),
            Self::Checkpoint => (
                "translate checkpoint failed",
                "sink emit (checkpoint) failed",
            ),
            Self::TerminalFinalize => (
                "translate finalization failed",
                "sink emit (finalization) failed",
            ),
        }
    }

    fn observes_correlation(self) -> bool {
        true
    }
}

impl SessionActor {
    async fn run(self, mut rx: mpsc::Receiver<SessionMsg>) {
        let mut translator = self.derived.route_translator();
        let sink: Box<dyn crate::sink::Sink> = Box::new(UnconfiguredSink);
        let mut auth_ready = false;
        let mut sink: Box<dyn crate::sink::Sink> = Box::new(
            LedgerSink::new(sink, &self.data_dir, &self.source, &self.session_id, None).await,
        );
        let mut ctx = SessionCtx {
            session_id: self.session_id.clone(),
            config: Some(self.config.clone()),
        };
        let recovery_scope =
            crate::recovery::WorkScope::delivery(&self.source, &self.session_id, &self.route).ok();
        if let Some(incident) = recovery_scope.as_ref().and_then(|scope| {
            crate::recovery::active(&self.data_dir, scope)
                .ok()
                .flatten()
        }) {
            *self.pause.lock().unwrap() = Some(PausedWork {
                marker_span_id: incident.marker_span_id.clone(),
                marker_emitted: incident.marker_span_id.is_some(),
                marker_cleared: false,
            });
        }
        let source_scope = crate::recovery::WorkScope::SourceSession {
            source: self.source.clone(),
            session_id: self.session_id.clone(),
        };
        if let Ok(Some(incident)) = crate::recovery::active(&self.data_dir, &source_scope) {
            let _ = incident;
            *self.pause.lock().unwrap() = Some(PausedWork::default());
        }
        if self.pause.lock().unwrap().is_none()
            && recovery_scope.as_ref().is_some_and(|scope| {
                crate::recovery::active(&self.data_dir, scope)
                    .ok()
                    .flatten()
                    .is_some()
            })
        {
            *self.pause.lock().unwrap() = Some(PausedWork::default());
        }
        // Rebuild translator state before accepting the first new event.
        // Stable span ids make this both crash recovery and a complete copy
        // when an existing source session is sent to another destination.
        let mut acknowledged_through = self
            .replay
            .as_ref()
            .map_or(0, |plan| plan.acknowledged_through);
        let mut pending_through = acknowledged_through;
        // A failed sink write makes the contiguous delivery boundary unknown.
        // Keep the journal conservative for this actor generation rather than
        // checkpointing past an event that may need recovery delivery.
        let mut checkpointable = self
            .prepare_and_replay(
                &mut translator,
                &mut sink,
                &ctx,
                self.replay.as_ref(),
                auth_ready,
            )
            .await;
        if auth_ready && checkpointable && self.pause.lock().unwrap().is_some() {
            let tip = self.replay.as_ref().map_or(0, |plan| plan.through);
            let marker_cleared = self
                .pause
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|state| !state.marker_emitted || state.marker_cleared);
            if marker_cleared && self.flush_sink(&mut sink, acknowledged_through).await {
                self.record_delivery_checkpoint(&mut sink, tip, &mut acknowledged_through)
                    .await;
                if acknowledged_through >= tip {
                    if let Some(scope) = &recovery_scope {
                        let _ = crate::recovery::resolve(&self.data_dir, scope);
                    }
                    *self.pause.lock().unwrap() = None;
                    *self.last_error.lock().unwrap() = None;
                }
            }
        }
        let mut last_event_through = self.replay.as_ref().map_or(0, |plan| plan.through);
        let mut replayed_through = 0;
        let mut retry_tick = tokio::time::interval(Duration::from_secs(1));
        retry_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            let msg = tokio::select! {
                msg = rx.recv() => match msg { Some(msg) => msg, None => break },
                _ = retry_tick.tick(), if self.pause.lock().unwrap().is_some() => {
                    let route_incident = recovery_scope.as_ref().and_then(|scope| crate::recovery::active(&self.data_dir, scope).ok().flatten());
                    let source_incident = crate::recovery::active(&self.data_dir, &source_scope).ok().flatten();
                    let now = crate::recovery::now_ms();
                    let eligible = |incident: &crate::recovery::Incident| match &incident.cause {
                        crate::recovery::FailureCause::Credentials { retry_after_ms, .. } => now >= *retry_after_ms,
                        cause => cause.check_local(crate::recovery::translator_revision(), now)
                            == crate::recovery::CheckResult::RetryCandidate,
                    };
                    let retry_source = source_incident.as_ref().is_some_and(eligible);
                    let (mut scope, mut incident) = if retry_source {
                        (Some(&source_scope), source_incident.clone())
                    } else if route_incident.as_ref().is_some_and(eligible) {
                        (recovery_scope.as_ref(), route_incident.clone())
                    } else if source_incident.is_none() && route_incident.is_none() {
                        // Another route may have recovered the shared source.
                        // Drain its newly committed output even without a native event.
                        (recovery_scope.as_ref(), None)
                    } else { continue; };
                    if retry_source {
                        let tip=self.journal.lock().await.position();
                        if let Err(error)=self.derived.ensure_event(tip).await {
                            self.record_translation_failure(&error,Some((0,tip)));
                            self.set_error(format!("span production retry failed: {error}"));continue;
                        }
                        if let Err(error)=self.derived.collect_input(&self.journal).await {self.set_error(format!("event WAL collection failed: {error}"));}
                        let _=crate::recovery::resolve(&self.data_dir,&source_scope);
                        if let Some(route_incident)=&route_incident {
                            if !matches!(route_incident.cause,crate::recovery::FailureCause::Credentials{..}) &&
                                route_incident.cause.check_local(crate::recovery::translator_revision(),now)!=crate::recovery::CheckResult::RetryCandidate {
                                continue;
                            }
                        }
                        scope=recovery_scope.as_ref();
                        incident=route_incident.clone();
                    }
                    if !auth_ready || incident.as_ref().is_some_and(|incident| matches!(incident.cause, crate::recovery::FailureCause::Credentials { .. })) {
                        let Some(provider) = &self.auth_provider else {
                            self.set_error("credential retry unavailable without an auth provider".into());
                            continue;
                        };
                        let lease = crate::server::resolve_route_auth(
                            provider.as_ref(),
                            &self.route.auth,
                            crate::server::AuthResolveReason::Initial,
                            self.route.auth.org_name.as_deref(),
                        ).await;
                        match lease {
                            Ok(lease) => {
                                ctx.config = Some(self.route.with_auth(lease.auth));
                                auth_ready = true;
                            }
                            Err(error) => {
                                if let Some(scope) = scope {
                                    let _ = crate::recovery::pause(
                                        &self.data_dir, scope.clone(),
                                        incident.as_ref().map_or(0, |incident| incident.first_unprocessed),
                                        incident.as_ref().map_or(0, |incident| incident.operation_index),
                                        crate::recovery::FailureCause::Credentials {
                                            selection: self.route.auth.clone(),
                                            retry_after_ms: now + 2_000,
                                        },
                                        error.to_string(),
                                        incident.as_ref().and_then(|incident| incident.marker_span_id.clone()),
                                    );
                                }
                                self.set_error(format!("credential retry failed: {error}"));
                                continue;
                            }
                        }
                    }
                    let changed = incident.as_ref().and_then(|incident| match &incident.cause { crate::recovery::FailureCause::PluginFile { path, .. } => Some(crate::recovery::plugin_digest(path)), _ => None });
                    if let Some(scope) = scope {
                        if let Err(error) = crate::recovery::begin_retry(&self.data_dir, scope) {
                            self.set_error(format!("failed to record recovery attempt: {error}"));
                            continue;
                        }
                    }
                    let next_translator = self.derived.route_translator();
                    let Ok(next_sink) = self.sink_factory.create(
                        &self.session_id, &self.source, self.plugin_version.as_deref(),
                    ) else {
                        self.set_error("plugin recovery could not create sink".into());
                        continue;
                    };
                    let mut next_sink: Box<dyn crate::sink::Sink> = Box::new(
                        LedgerSink::new(next_sink, &self.data_dir, &self.source, &self.session_id,
                            ctx.config.as_ref()).await,
                    );
                    if let Some(config) = &ctx.config { next_sink.configure(config); }
                    let tip = self.journal.lock().await.position();
                    let plan = ReplayPlan {
                        through: tip,
                        acknowledged_through,
                    };
                    let mut next_translator = next_translator;
                    let replay_ok = self.prepare_and_replay(
                        &mut next_translator, &mut next_sink, &ctx, Some(&plan), true,
                    ).await;
                    let flushed = replay_ok && self.checkpoint_and_flush(
                        &mut next_translator, &mut next_sink, &ctx,
                        Some((acknowledged_through, tip)),
                    ).await;
                    let stable = incident.as_ref().is_none_or(|incident| match &incident.cause {
                        crate::recovery::FailureCause::PluginFile { path, .. } => crate::recovery::plugin_digest(path) == changed.flatten(),
                        crate::recovery::FailureCause::InputShape { translator_revision, .. } | crate::recovery::FailureCause::TranslatorFault { translator_revision } => translator_revision != crate::recovery::translator_revision(),
                        _ => true,
                    });
                    let marker_cleared = self.pause.lock().unwrap().as_ref()
                        .is_some_and(|current| !current.marker_emitted || current.marker_cleared);
                    if flushed && stable && marker_cleared {
                        self.record_delivery_checkpoint(&mut next_sink, tip, &mut acknowledged_through).await;
                        if acknowledged_through >= tip {
                            if let Some(scope) = scope {
                                let _ = crate::recovery::resolve(&self.data_dir, scope);
                            }
                            *self.pause.lock().unwrap() = None;
                            *self.last_error.lock().unwrap() = None;
                            translator = next_translator;
                            sink = next_sink;
                            checkpointable = true;
                            pending_through = tip;
                            replayed_through = tip;
                            last_event_through = tip;
                            continue;
                        }
                    }
                    // A new plugin failure records its new digest. A sink
                    // failure retains the old one so a later tick can retry.
                    continue;
                }
            };
            match msg {
                SessionMsg::Event(env, journal_through) => {
                    let paused = self.pause.lock().unwrap().clone();
                    let source_failure = crate::recovery::active(&self.data_dir, &source_scope)
                        .ok()
                        .flatten();
                    let translation_paused = source_failure.is_some_and(|incident| {
                        matches!(
                            incident.cause,
                            crate::recovery::FailureCause::InputShape { .. }
                                | crate::recovery::FailureCause::TranslatorFault { .. }
                                | crate::recovery::FailureCause::LocalStorage { .. }
                        )
                    });
                    let skip_for_pause = translation_paused;
                    if journal_through <= replayed_through || skip_for_pause {
                        last_event_through = last_event_through.max(journal_through);
                        self.counters.queued.fetch_sub(1, Ordering::Relaxed);
                        continue;
                    }
                    let event_start = last_event_through;
                    let event_window = Some((event_start, journal_through));
                    last_event_through = journal_through;
                    let correlation_barrier = is_tool_lifecycle_event(&env.event);
                    if let Some(cfg) = env.config.as_ref().filter(|_| auth_ready) {
                        sink.configure(cfg);
                        ctx.config = Some(cfg.clone());
                        self.refresh_permalink(sink.as_ref());
                    }
                    let produced = self.derived.ensure_event(journal_through).await;
                    let (correlation_changed, delivered) = match produced {
                        Ok(_) => {
                            if let Err(error) = self.derived.collect_input(&self.journal).await {
                                self.set_error(format!("event WAL collection failed: {error}"));
                            }
                            if auth_ready && paused.is_some() {
                                self.counters.queued.fetch_sub(1, Ordering::Relaxed);
                                continue;
                            }
                            self.consume_into(
                                &mut translator,
                                &mut sink,
                                &ctx,
                                auth_ready,
                                BatchMode::Live,
                            )
                            .await
                        }
                        Err(error) => {
                            self.record_translation_failure(&error, event_window);
                            self.set_error(format!("span production failed: {error}"));
                            if let Err(error) = self.derived.collect_input(&self.journal).await {
                                self.set_error(format!("event WAL collection failed: {error}"));
                            }
                            let (changed, _) = self
                                .consume_into(
                                    &mut translator,
                                    &mut sink,
                                    &ctx,
                                    auth_ready,
                                    BatchMode::Live,
                                )
                                .await;
                            (changed, false)
                        }
                    };
                    if correlation_changed || correlation_barrier {
                        if let Err(error) = crate::server::persist_active_parent_snapshot(
                            &self.data_dir,
                            &self.correlation_key,
                            &self.correlation,
                        )
                        .await
                        {
                            self.set_error(error);
                        }
                        self.correlation_changed.notify_one();
                    }
                    self.counters.queued.fetch_sub(1, Ordering::Relaxed);
                    if !delivered && checkpointable && self.pause.lock().unwrap().is_some() {
                        // The current event may have emitted only some of its
                        // bounded batches. Checkpoint strictly before it so
                        // recovery delivers that event again, but does not
                        // resend earlier accepted events.
                        self.record_delivery_checkpoint(
                            &mut sink,
                            event_start,
                            &mut acknowledged_through,
                        )
                        .await;
                    }
                    if delivered && checkpointable && auth_ready {
                        pending_through = pending_through.max(journal_through);
                    } else if !delivered {
                        checkpointable = false;
                    }
                }
                SessionMsg::Configure(config, reply) => {
                    let config = *config;
                    let became_ready = !auth_ready;
                    auth_ready = true;
                    ctx.config = Some(config.clone());
                    let route_retry_allowed = recovery_scope
                        .as_ref()
                        .and_then(|scope| {
                            crate::recovery::active(&self.data_dir, scope)
                                .ok()
                                .flatten()
                        })
                        .is_none_or(|incident| {
                            matches!(
                                incident.cause,
                                crate::recovery::FailureCause::Credentials { .. }
                            ) || incident.cause.check_local(
                                crate::recovery::translator_revision(),
                                crate::recovery::now_ms(),
                            ) == crate::recovery::CheckResult::RetryCandidate
                        });
                    if became_ready && route_retry_allowed {
                        if let Ok(next_sink) = self.sink_factory.create(
                            &self.session_id,
                            &self.source,
                            self.plugin_version.as_deref(),
                        ) {
                            let mut next_translator = self.derived.route_translator();
                            let mut next_sink: Box<dyn crate::sink::Sink> = Box::new(
                                LedgerSink::new(
                                    next_sink,
                                    &self.data_dir,
                                    &self.source,
                                    &self.session_id,
                                    Some(&config),
                                )
                                .await,
                            );
                            next_sink.configure(&config);
                            let tip = last_event_through;
                            let plan = ReplayPlan {
                                through: tip,
                                acknowledged_through,
                            };
                            let replay_ok = self
                                .prepare_and_replay(
                                    &mut next_translator,
                                    &mut next_sink,
                                    &ctx,
                                    Some(&plan),
                                    true,
                                )
                                .await;
                            if replay_ok {
                                if let Some(scope) = &recovery_scope {
                                    if crate::recovery::active(&self.data_dir, scope)
                                        .ok()
                                        .flatten()
                                        .is_some()
                                    {
                                        self.record_delivery_checkpoint(
                                            &mut next_sink,
                                            tip,
                                            &mut acknowledged_through,
                                        )
                                        .await;
                                        if acknowledged_through >= tip {
                                            let _ = crate::recovery::resolve(&self.data_dir, scope);
                                            *self.pause.lock().unwrap() = None;
                                            *self.last_error.lock().unwrap() = None;
                                        }
                                    }
                                }
                                translator = next_translator;
                                sink = next_sink;
                                checkpointable = true;
                                pending_through = tip;
                                replayed_through = tip;
                            }
                        }
                    } else if auth_ready {
                        sink.configure(&config);
                        self.refresh_permalink(sink.as_ref());
                    }
                    let _ = reply.send(());
                }
                SessionMsg::AuthFailure(error, _capture_cursor) => {
                    let first_unprocessed = self
                        .derived
                        .consumer_sequence(&self.route)
                        .await
                        .unwrap_or(0);
                    auth_ready = false;
                    if let Ok(scope) = crate::recovery::WorkScope::delivery(
                        &self.source,
                        &self.session_id,
                        &self.route,
                    ) {
                        let prior = crate::recovery::active(&self.data_dir, &scope)
                            .ok()
                            .flatten();
                        if !prior.as_ref().is_some_and(|incident| {
                            matches!(
                                incident.cause,
                                crate::recovery::FailureCause::PluginFile { .. }
                            )
                        }) {
                            let _ = crate::recovery::pause(
                                &self.data_dir,
                                scope,
                                first_unprocessed,
                                0,
                                crate::recovery::FailureCause::Credentials {
                                    selection: self.route.auth.clone(),
                                    retry_after_ms: crate::recovery::now_ms() + 1_000,
                                },
                                error.clone(),
                                prior.and_then(|incident| incident.marker_span_id),
                            );
                            *self.pause.lock().unwrap() = Some(PausedWork::default());
                        }
                    }
                    self.set_error(format!("session auth failed: {error}"));
                }
                SessionMsg::Barrier(reply) => {
                    let _ = reply.send(());
                }
                SessionMsg::Flush(reply) => {
                    if self.source_paused() && !self.delivery_paused() {
                        let (_, delivered) = self
                            .consume_into(&mut translator, &mut sink, &ctx, true, BatchMode::Replay)
                            .await;
                        if delivered {
                            self.record_delivery_checkpoint(
                                &mut sink,
                                pending_through,
                                &mut acknowledged_through,
                            )
                            .await;
                        }
                        let _ = reply.send(self.counters.queued.load(Ordering::Relaxed).max(1));
                        continue;
                    }
                    if self.pause.lock().unwrap().is_some() {
                        let _ = reply.send(self.counters.queued.load(Ordering::Relaxed).max(1));
                        continue;
                    }
                    checkpointable &= self
                        .checkpoint_and_flush(
                            &mut translator,
                            &mut sink,
                            &ctx,
                            Some((acknowledged_through, last_event_through)),
                        )
                        .await;
                    if checkpointable {
                        self.record_delivery_checkpoint(
                            &mut sink,
                            pending_through,
                            &mut acknowledged_through,
                        )
                        .await;
                    }
                    let pending = self.counters.queued.load(Ordering::Relaxed);
                    let _ = reply.send(if self.pause.lock().unwrap().is_some() {
                        pending.max(1)
                    } else {
                        pending
                    });
                }
                SessionMsg::Finalize(reply) => {
                    if self.pause.lock().unwrap().is_some() {
                        let _ = reply.send(self.counters.queued.load(Ordering::Relaxed).max(1));
                        continue;
                    }
                    checkpointable &= self
                        .finalize_and_flush(
                            &mut translator,
                            &mut sink,
                            &ctx,
                            BatchMode::TerminalFinalize,
                            Some((acknowledged_through, last_event_through)),
                        )
                        .await;
                    if checkpointable {
                        self.record_delivery_checkpoint(
                            &mut sink,
                            pending_through,
                            &mut acknowledged_through,
                        )
                        .await;
                    }
                    let pending = self.counters.queued.load(Ordering::Relaxed);
                    let _ = reply.send(if self.pause.lock().unwrap().is_some() {
                        pending.max(1)
                    } else {
                        pending
                    });
                }
                SessionMsg::Shutdown(reply) => {
                    if self.pause.lock().unwrap().is_some() {
                        let _ = reply.send(());
                        break;
                    }
                    checkpointable &= self
                        .checkpoint_and_flush(
                            &mut translator,
                            &mut sink,
                            &ctx,
                            Some((acknowledged_through, last_event_through)),
                        )
                        .await;
                    if checkpointable {
                        self.record_delivery_checkpoint(
                            &mut sink,
                            pending_through,
                            &mut acknowledged_through,
                        )
                        .await;
                    }
                    let _ = reply.send(());
                    break;
                }
            }
        }
    }

    /// Emit a translator result and every bounded continuation it schedules.
    /// A continuation may be empty (for example, irrelevant rollout rows), so
    /// only `None` signals that the translator is fully caught up.
    async fn emit_translator_batches(
        &self,
        translator: &mut Box<dyn crate::translate::AgentTranslator>,
        sink: &mut Box<dyn crate::sink::Sink>,
        ctx: &SessionCtx,
        first: anyhow::Result<Vec<crate::translate::SpanOp>>,
        mode: BatchMode,
        event_window: Option<(u64, u64)>,
    ) -> (bool, bool) {
        let (translate_error, emit_error) = mode.errors();
        let mut next = match first {
            Ok(ops) => Some(ops),
            Err(e) => {
                self.record_translation_failure(&e, event_window);
                self.set_error(format!("{translate_error}: {e}"));
                return (false, false);
            }
        };
        let event_window = translator
            .ledger_sequence()
            .filter(|seq| *seq > 0)
            .map(|seq| (seq - 1, seq))
            .or(event_window);
        let mut correlation_changed = false;
        let mut delivered = true;
        let mut operation_index = 0u32;
        let resume_at = event_window
            .and_then(|(start, _)| {
                crate::recovery::WorkScope::delivery(&self.source, &self.session_id, &self.route)
                    .ok()
                    .and_then(|scope| {
                        crate::recovery::active(&self.data_dir, &scope)
                            .ok()
                            .flatten()
                    })
                    .filter(|incident| incident.first_unprocessed == start)
                    .map(|incident| incident.operation_index)
            })
            .unwrap_or(0);
        while let Some(ops) = next {
            if !ops.is_empty() {
                if mode.observes_correlation() {
                    let changed = self.correlation.observe_ops(
                        &self.correlation_key,
                        &self.route,
                        ctx.config.as_ref().expect("session config"),
                        &ops,
                    );
                    correlation_changed |= changed;
                    if changed {
                        self.persist_correlation_if_changed(true).await;
                    }
                }
                let plugin_paths = ctx
                    .config
                    .as_ref()
                    .map(|config| config.span_plugins.as_slice())
                    .unwrap_or_default();
                let mut processed = Vec::with_capacity(ops.len());
                for op in &ops {
                    let current_index = operation_index;
                    operation_index = operation_index.saturating_add(1);
                    if current_index < resume_at {
                        continue;
                    }
                    match crate::span_processor::process(
                        plugin_paths,
                        op,
                        &self.source,
                        &self.session_id,
                        &self.correlation_key,
                    ) {
                        Ok(result) => {
                            if let Some(failure) = result.failure {
                                delivered = false;
                                let prefix_accepted = processed.is_empty()
                                    || match sink.emit(&processed).await {
                                        Ok(_) => match sink.flush().await {
                                            Ok(()) => true,
                                            Err(error) => {
                                                self.set_error(format!("{emit_error}: {error}"));
                                                false
                                            }
                                        },
                                        Err(error) => {
                                            self.set_error(format!("{emit_error}: {error}"));
                                            false
                                        }
                                    };
                                self.record_plugin_failure(
                                    sink,
                                    op,
                                    &failure.path,
                                    &failure.message,
                                    event_window,
                                    if prefix_accepted { current_index } else { 0 },
                                )
                                .await;
                                break;
                            }
                            if let Some(processed_op) = result.op {
                                processed.push(processed_op);
                            }
                        }
                        Err(error) => {
                            delivered = false;
                            if let Some(plugin) = plugin_paths.first() {
                                let prefix_accepted = processed.is_empty()
                                    || match sink.emit(&processed).await {
                                        Ok(_) => match sink.flush().await {
                                            Ok(()) => true,
                                            Err(error) => {
                                                self.set_error(format!("{emit_error}: {error}"));
                                                false
                                            }
                                        },
                                        Err(error) => {
                                            self.set_error(format!("{emit_error}: {error}"));
                                            false
                                        }
                                    };
                                self.record_plugin_failure(
                                    sink,
                                    op,
                                    plugin,
                                    &error.to_string(),
                                    event_window,
                                    if prefix_accepted { current_index } else { 0 },
                                )
                                .await;
                            }
                            self.set_error(format!(
                                "span plugin processor failed; span operation discarded: {error}"
                            ));
                            break;
                        }
                    }
                }
                if delivered && !processed.is_empty() {
                    let paused = self.pause.lock().unwrap().clone();
                    let recovery_span = paused
                        .and_then(|pause| pause.marker_span_id.filter(|_| !pause.marker_cleared));
                    let result = if let Some(recovery_span) = recovery_span {
                        let mut emitted = 0;
                        let mut result = Ok(());
                        for op in &processed {
                            let row = match op {
                                crate::translate::SpanOp::Insert(row)
                                | crate::translate::SpanOp::Merge(row) => row,
                            };
                            let replacing = row.span_id == recovery_span;
                            let outcome = if replacing {
                                sink.replace_plugin_marker(op).await
                            } else {
                                sink.emit(std::slice::from_ref(op)).await
                            };
                            match outcome {
                                Ok(n) => {
                                    emitted += n;
                                    if replacing {
                                        if let Some(state) = self.pause.lock().unwrap().as_mut() {
                                            state.marker_cleared = true;
                                        }
                                    }
                                }
                                Err(error) => {
                                    result = Err(error);
                                    break;
                                }
                            }
                        }
                        result.map(|()| emitted)
                    } else {
                        sink.emit(&processed).await
                    };
                    match result {
                        Ok(n) => {
                            self.counters.spans_emitted.fetch_add(n, Ordering::Relaxed);
                        }
                        Err(e) => {
                            delivered = false;
                            self.record_delivery_failure(
                                &e,
                                event_window,
                                operation_index.saturating_sub(processed.len() as u32),
                            );
                            self.set_error(format!("{emit_error}: {e}"));
                        }
                    }
                }
            }
            if !delivered && self.pause.lock().unwrap().is_some() {
                break;
            }
            next = match translator.drain_pending(ctx) {
                Ok(next) => next,
                Err(e) => {
                    self.record_translation_failure(&e, event_window);
                    self.set_error(format!("{translate_error}: {e}"));
                    delivered = false;
                    None
                }
            };
        }
        if delivered {
            if let Some(sequence) = translator.ledger_sequence() {
                self.delivered_sequence
                    .fetch_max(sequence, Ordering::Relaxed);
            }
        }
        (correlation_changed, delivered)
    }

    async fn record_plugin_failure(
        &self,
        sink: &mut Box<dyn crate::sink::Sink>,
        op: &crate::translate::SpanOp,
        plugin: &std::path::Path,
        message: &str,
        event_window: Option<(u64, u64)>,
        operation_index: u32,
    ) {
        let (row, operation) = match op {
            crate::translate::SpanOp::Insert(row) => (row, "insert"),
            crate::translate::SpanOp::Merge(row) => (row, "merge"),
        };
        let (start, through) = event_window.unwrap_or((0, self.derived.tip().await.unwrap_or(0)));
        let marker = crate::translate::SpanOp::Merge(crate::translate::SpanRow {
            span_id: row.span_id.clone(),
            root_span_id: row.root_span_id.clone(),
            parent_span_ids: row.parent_span_ids.clone(),
            name: if row.name.is_empty() {
                String::new()
            } else {
                "Plugin failure".into()
            },
            span_type: row.span_type,
            start_ms: row.start_ms,
            error: Some(format!("plugin failure: {}", plugin.display())),
            ..Default::default()
        });
        let marker_emitted = match sink.emit_plugin_marker(&marker).await {
            Ok(_) => sink.flush().await.is_ok(),
            Err(error) => {
                self.set_error(format!(
                    "plugin failure marker could not be delivered: {error}"
                ));
                false
            }
        };
        *self.pause.lock().unwrap() = Some(PausedWork {
            marker_span_id: marker_emitted.then(|| row.span_id.clone()),
            marker_emitted,
            marker_cleared: false,
        });
        match crate::recovery::WorkScope::delivery(&self.source, &self.session_id, &self.route) {
            Ok(scope) => {
                let plugin_index = self
                    .route
                    .span_plugins
                    .iter()
                    .position(|candidate| candidate == plugin)
                    .unwrap_or(0);
                if let Err(error) = crate::recovery::pause(
                    &self.data_dir,
                    scope,
                    start,
                    operation_index,
                    crate::recovery::FailureCause::PluginFile {
                        path: plugin.to_path_buf(),
                        digest: crate::recovery::plugin_digest(plugin),
                        plugin_index,
                    },
                    message.to_owned(),
                    marker_emitted.then(|| row.span_id.clone()),
                ) {
                    self.set_error(format!("failed to persist recovery incident: {error}"));
                }
            }
            Err(error) => self.set_error(format!("failed to identify recovery route: {error}")),
        }
        if let Err(error) = crate::plugin_diagnostics::record_pipeline_failure(
            &self.data_dir,
            crate::plugin_diagnostics::PipelineFailure {
                source: &self.source,
                session_id: &self.session_id,
                route: &self.route,
                span_id: &row.span_id,
                operation,
                plugin_path: plugin,
                exception: message,
                span_cursor: start,
                span_through: through,
            },
        ) {
            self.set_error(format!("failed to persist span plugin diagnostic: {error}"));
        }
        self.set_error(format!(
            "span plugin {} failed for span {}; session delivery paused: {}",
            plugin.display(),
            row.span_id,
            message,
        ));
    }

    fn record_translation_failure(&self, error: &anyhow::Error, event_window: Option<(u64, u64)>) {
        let Some((mut start, _)) = event_window else {
            return;
        };
        if let Some(position) = error.downcast_ref::<crate::derived::InputFailurePosition>() {
            start = position.start;
        }
        let revision = crate::recovery::translator_revision().to_owned();
        let cause = if crate::derived::retryable_storage(error) {
            crate::recovery::FailureCause::LocalStorage {
                retry_after_ms: crate::recovery::now_ms() + 1_000,
            }
        } else if let Some(shape) = error.downcast_ref::<crate::translate::InputShapeError>() {
            crate::recovery::FailureCause::InputShape {
                event: shape.event.clone(),
                translator_revision: revision.clone(),
            }
        } else {
            crate::recovery::FailureCause::TranslatorFault {
                translator_revision: revision.clone(),
            }
        };
        let scope = crate::recovery::WorkScope::SourceSession {
            source: self.source.clone(),
            session_id: self.session_id.clone(),
        };
        if let Err(persist_error) = crate::recovery::pause(
            &self.data_dir,
            scope,
            start,
            0,
            cause,
            error.to_string(),
            None,
        ) {
            self.set_error(format!(
                "failed to persist translation incident: {persist_error}"
            ));
        }
        if self.pause.lock().unwrap().is_none() {
            *self.pause.lock().unwrap() = Some(PausedWork::default());
        }
    }

    fn record_delivery_failure(
        &self,
        error: &anyhow::Error,
        event_window: Option<(u64, u64)>,
        operation_index: u32,
    ) {
        let Ok(scope) =
            crate::recovery::WorkScope::delivery(&self.source, &self.session_id, &self.route)
        else {
            self.set_error("could not identify failed delivery route".into());
            return;
        };
        let prior = crate::recovery::active(&self.data_dir, &scope)
            .ok()
            .flatten();
        let cause = crate::recovery::classify_delivery_error(
            error,
            &self.route,
            self.config.auth.api_url.as_deref(),
            prior.as_ref().map_or(0, |incident| incident.attempts),
        );
        let first_unprocessed = event_window.map_or(0, |window| window.0);
        if let Err(persist_error) = crate::recovery::pause(
            &self.data_dir,
            scope,
            first_unprocessed,
            operation_index,
            cause,
            error.to_string(),
            prior.and_then(|incident| incident.marker_span_id),
        ) {
            self.set_error(format!(
                "failed to persist delivery incident: {persist_error}"
            ));
        }
        if self.pause.lock().unwrap().is_none() {
            *self.pause.lock().unwrap() = Some(PausedWork::default());
        }
    }

    /// Source translation and route delivery pause independently.
    fn source_paused(&self) -> bool {
        crate::recovery::active(
            &self.data_dir,
            &crate::recovery::WorkScope::SourceSession {
                source: self.source.clone(),
                session_id: self.session_id.clone(),
            },
        )
        .ok()
        .flatten()
        .is_some()
    }
    fn delivery_paused(&self) -> bool {
        crate::recovery::WorkScope::delivery(&self.source, &self.session_id, &self.route)
            .ok()
            .and_then(|scope| {
                crate::recovery::active(&self.data_dir, &scope)
                    .ok()
                    .flatten()
            })
            .is_some()
    }

    async fn prepare_and_replay(
        &self,
        translator: &mut Box<dyn crate::translate::AgentTranslator>,
        sink: &mut Box<dyn crate::sink::Sink>,
        ctx: &SessionCtx,
        plan: Option<&ReplayPlan>,
        deliver: bool,
    ) -> bool {
        if let Some(plan) = plan {
            // Produce outstanding captured input upstream, then consume only the committed span ledger.
            let source_scope = crate::recovery::WorkScope::SourceSession {
                source: self.source.clone(),
                session_id: self.session_id.clone(),
            };
            let can_produce = crate::recovery::active(&self.data_dir, &source_scope)
                .ok()
                .flatten()
                .is_none_or(|incident| {
                    incident.cause.check_local(
                        crate::recovery::translator_revision(),
                        crate::recovery::now_ms(),
                    ) == crate::recovery::CheckResult::RetryCandidate
                });
            if can_produce {
                if let Err(error) = self.derived.ensure_event(plan.through).await {
                    self.record_translation_failure(
                        &error,
                        Some((plan.acknowledged_through, plan.through)),
                    );
                    self.set_error(format!("span production failed: {error}"));
                }
            }
            if let Err(error) = self.derived.collect_input(&self.journal).await {
                self.set_error(format!("event WAL collection failed: {error}"));
                return false;
            }
        }
        self.replay_into(translator, sink, ctx, plan, deliver).await
    }

    async fn replay_into(
        &self,
        translator: &mut Box<dyn crate::translate::AgentTranslator>,
        sink: &mut Box<dyn crate::sink::Sink>,
        ctx: &SessionCtx,
        plan: Option<&ReplayPlan>,
        deliver: bool,
    ) -> bool {
        if plan.is_none() {
            return true;
        }
        match self.derived.consumer_sequence(&self.route).await {
            Ok(sequence) => self.delivered_sequence.store(sequence, Ordering::Relaxed),
            Err(error) => {
                self.record_delivery_failure(
                    &error,
                    Some((
                        self.delivered_sequence.load(Ordering::Relaxed),
                        self.delivered_sequence.load(Ordering::Relaxed),
                    )),
                    0,
                );
                self.set_error(format!("span ledger cursor failed: {error}"));
                return false;
            }
        }
        self.consume_into(translator, sink, ctx, deliver, BatchMode::Replay)
            .await
            .1
    }

    /// Every downstream stage reads only committed span revisions, in output sequence order.
    async fn consume_into(
        &self,
        translator: &mut Box<dyn crate::translate::AgentTranslator>,
        sink: &mut Box<dyn crate::sink::Sink>,
        ctx: &SessionCtx,
        deliver: bool,
        mode: BatchMode,
    ) -> (bool, bool) {
        let tip = match self.derived.tip().await {
            Ok(tip) => tip,
            Err(error) => {
                self.record_delivery_failure(&error, None, 0);
                self.set_error(format!("span ledger tip failed: {error}"));
                return (false, false);
            }
        };
        let mut after = self.delivered_sequence.load(Ordering::Relaxed);
        let mut changed = false;
        loop {
            let revision = match self.derived.next_revision(after, tip).await {
                Ok(Some(revision)) => revision,
                Ok(None) => break,
                Err(error) => {
                    self.record_delivery_failure(&error, Some((after, after)), 0);
                    self.set_error(format!("span ledger replay failed: {error}"));
                    return (changed, false);
                }
            };
            after = revision.sequence;
            let env = revision
                .envelope
                .map(crate::journal::envelope_from_redacted)
                .unwrap_or_else(|| Envelope {
                    source: self.source.clone(),
                    session_id: self.session_id.clone(),
                    event: String::new(),
                    ts_ms: 0,
                    payload: serde_json::Value::Null,
                    source_version: None,
                    plugin_version: None,
                    managed_run_id: None,
                    capture: None,
                    route: None,
                    config: None,
                });
            translator.set_revision_path(&self.derived.revision_path_for_id(&revision.id));
            let translated = translator.handle(&env, ctx);
            if deliver {
                let (revision_changed, delivered) = self
                    .emit_translator_batches(
                        translator,
                        sink,
                        ctx,
                        translated,
                        mode,
                        Some((revision.sequence - 1, revision.sequence)),
                    )
                    .await;
                changed |= revision_changed;
                if !delivered {
                    return (changed, false);
                }
            } else if !self
                .replay_without_delivery(
                    translator,
                    ctx,
                    translated,
                    (revision.sequence - 1, revision.sequence),
                )
                .await
            {
                return (changed, false);
            }
        }
        (changed, true)
    }

    /// Continue a replayed translator exactly as usual but deliberately omit
    /// sink delivery for entries recorded as durably accepted. This lets every
    /// agent recover correlation/open-span state without retriggering backend
    /// side effects such as online scoring.
    async fn replay_without_delivery(
        &self,
        translator: &mut Box<dyn crate::translate::AgentTranslator>,
        ctx: &SessionCtx,
        first: anyhow::Result<Vec<crate::translate::SpanOp>>,
        event_window: (u64, u64),
    ) -> bool {
        let mut next = match first {
            Ok(ops) => Some(ops),
            Err(error) => {
                self.record_translation_failure(&error, Some(event_window));
                self.set_error(format!("journal replay failed: {error}"));
                return false;
            }
        };
        while let Some(ops) = next {
            if !ops.is_empty() {
                let _ = self.correlation.observe_ops(
                    &self.correlation_key,
                    &self.route,
                    ctx.config.as_ref().expect("session config"),
                    &ops,
                );
            }
            next = match translator.drain_pending(ctx) {
                Ok(next) => next,
                Err(error) => {
                    self.record_translation_failure(&error, Some(event_window));
                    self.set_error(format!("journal replay failed: {error}"));
                    return false;
                }
            };
        }
        true
    }

    async fn checkpoint_and_flush(
        &self,
        translator: &mut Box<dyn crate::translate::AgentTranslator>,
        sink: &mut Box<dyn crate::sink::Sink>,
        ctx: &SessionCtx,
        event_window: Option<(u64, u64)>,
    ) -> bool {
        if let Err(error) = self
            .derived
            .ensure_control(
                crate::derived::ControlKind::Checkpoint,
                event_window.map_or(0, |window| window.1),
            )
            .await
        {
            self.record_translation_failure(&error, event_window);
            self.set_error(format!("span production failed: {error}"));
            return false;
        }
        let (changed, delivered) = self
            .consume_into(translator, sink, ctx, true, BatchMode::Checkpoint)
            .await;
        self.persist_correlation_if_changed(changed).await;
        delivered
    }

    async fn finalize_and_flush(
        &self,
        translator: &mut Box<dyn crate::translate::AgentTranslator>,
        sink: &mut Box<dyn crate::sink::Sink>,
        ctx: &SessionCtx,
        mode: BatchMode,
        event_window: Option<(u64, u64)>,
    ) -> bool {
        if let Err(error) = self
            .derived
            .ensure_control(
                crate::derived::ControlKind::Finalize,
                event_window.map_or(0, |window| window.1),
            )
            .await
        {
            self.record_translation_failure(&error, event_window);
            self.set_error(format!("span production failed: {error}"));
            return false;
        }
        let (changed, delivered) = self.consume_into(translator, sink, ctx, true, mode).await;
        self.persist_correlation_if_changed(changed).await;
        delivered
    }

    async fn persist_correlation_if_changed(&self, changed: bool) {
        if !changed {
            return;
        }
        if let Err(error) = crate::server::persist_active_parent_snapshot(
            &self.data_dir,
            &self.correlation_key,
            &self.correlation,
        )
        .await
        {
            self.set_error(error);
        }
    }

    async fn flush_sink(
        &self,
        sink: &mut Box<dyn crate::sink::Sink>,
        _capture_cursor: u64,
    ) -> bool {
        let first_unprocessed = match self.derived.consumer_sequence(&self.route).await {
            Ok(cursor) => cursor,
            Err(error) => {
                self.record_delivery_failure(
                    &error,
                    Some((
                        self.delivered_sequence.load(Ordering::Relaxed),
                        self.delivered_sequence.load(Ordering::Relaxed),
                    )),
                    0,
                );
                self.set_error(format!("span ledger cursor failed: {error}"));
                return false;
            }
        };
        if let Err(e) = sink.flush().await {
            self.record_delivery_failure(&e, Some((first_unprocessed, first_unprocessed)), 0);
            self.set_error(format!("sink flush failed: {e}"));
            return false;
        }
        self.refresh_permalink(sink.as_ref());
        !sink.has_pending_delivery()
    }

    async fn record_delivery_checkpoint(
        &self,
        sink: &mut Box<dyn crate::sink::Sink>,
        _capture_tip: u64,
        acknowledged_through: &mut u64,
    ) {
        if !self.flush_sink(sink, *acknowledged_through).await {
            return;
        }
        match self
            .derived
            .acknowledge(&self.route, self.delivered_sequence.load(Ordering::Relaxed))
            .await
        {
            Ok(through) => *acknowledged_through = through,
            Err(error) => {
                self.record_delivery_failure(
                    &error,
                    Some((
                        self.delivered_sequence.load(Ordering::Relaxed),
                        self.delivered_sequence.load(Ordering::Relaxed),
                    )),
                    0,
                );
                self.set_error(format!("delivery checkpoint failed: {error}"));
            }
        }
    }

    fn refresh_permalink(&self, sink: &dyn crate::sink::Sink) {
        if let Some(link) = sink.permalink() {
            *self.permalink.lock().unwrap() = Some(link);
        }
    }

    fn set_error(&self, msg: String) {
        tracing::warn!(session_id = %self.session_id, "{msg}");
        *self.last_error.lock().unwrap() = Some(msg);
    }
}

pub(crate) fn is_tool_lifecycle_event(event: &str) -> bool {
    matches!(
        event,
        "PreToolUse"
            | "PostToolUse"
            | "PostToolUseFailure"
            | "preToolUse"
            | "postToolUse"
            | "postToolUseFailure"
            | "tool_execution_start"
            | "tool_execution_end"
            | "tool.execute.before"
            | "tool.execute.after"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cursor_hydration_freezes_hook_bytes_and_missing_paths() {
        let tmp = tempfile::tempdir().unwrap();
        let transcript = tmp.path().join("cursor.jsonl");
        std::fs::write(&transcript, b"first\nfuture\n").unwrap();
        let mut env: Envelope = serde_json::from_value(serde_json::json!({
            "source":"cursor", "session_id":"cursor-boundary", "event":"stop", "ts_ms":1,
            "payload": {
                "transcript_path":transcript,
                "_bt_transcript_observation":{"path":transcript,"observed_bytes":6}
            }
        }))
        .unwrap();
        hydrate_transcript_reference(tmp.path(), &mut env).await;
        let observation = env.payload["_bt_transcript_mirror"].clone();
        assert_eq!(observation["through"], 6);
        assert_eq!(
            std::fs::read(observation["mirror"].as_str().unwrap()).unwrap(),
            b"first\n"
        );
        std::fs::write(&transcript, b"replacement\n").unwrap();
        hydrate_transcript_reference(tmp.path(), &mut env).await;
        assert_eq!(env.payload["_bt_transcript_mirror"], observation);

        env.payload = serde_json::json!({"transcript_path":null});
        hydrate_transcript_reference(tmp.path(), &mut env).await;
        assert_eq!(env.payload["_bt_transcript_mirror"], serde_json::json!({}));
        env.payload["transcript_path"] = serde_json::json!(transcript);
        hydrate_transcript_reference(tmp.path(), &mut env).await;
        assert_eq!(env.payload["_bt_transcript_mirror"], serde_json::json!({}));
    }

    #[tokio::test]
    async fn enqueue_flush_preserves_boundary_without_waiting_for_delivery() {
        let (tx, mut rx) = mpsc::channel(4);
        let session = Session {
            source: "pi".into(),
            tx,
            counters: Arc::new(Counters::default()),
            last_error: Arc::new(Mutex::new(None)),
            permalink: Arc::new(Mutex::new(None)),
            pause: Arc::new(Mutex::new(None)),
            last_activity: Mutex::new(Instant::now()),
        };
        let envelope = |event| {
            serde_json::from_value::<Envelope>(serde_json::json!({
                "source": "pi", "session_id": "ordered-turns", "event": event,
                "ts_ms": 1, "payload": {"event": {}}
            }))
            .unwrap()
        };
        session.enqueue(envelope("agent_end"), 1).await.unwrap();
        // No actor is reading yet: enqueue must complete independently of delivery.
        let mut completion =
            tokio::time::timeout(std::time::Duration::from_secs(1), session.enqueue_flush())
                .await
                .expect("enqueue waited for delivery")
                .unwrap();
        session
            .enqueue(envelope("before_agent_start"), 2)
            .await
            .unwrap();
        assert!(matches!(
            completion.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(
            matches!(rx.recv().await, Some(SessionMsg::Event(env, 1)) if env.event == "agent_end")
        );
        let Some(SessionMsg::Flush(reply)) = rx.recv().await else {
            panic!("next turn overtook the boundary flush");
        };
        assert!(
            matches!(rx.recv().await, Some(SessionMsg::Event(env, 2)) if env.event == "before_agent_start")
        );
        reply.send(1).unwrap();
        assert_eq!(completion.await.unwrap(), 1);
        drop(rx);
        assert!(session.enqueue_flush().await.is_err());
    }

    #[tokio::test]
    async fn grok_hydration_mirrors_transcripts_and_system_prompt_at_one_boundary() {
        let tmp = tempfile::tempdir().unwrap();
        let session = tmp.path().join("native-session");
        tokio::fs::create_dir(&session).await.unwrap();
        tokio::fs::write(session.join("chat_history.jsonl"), b"")
            .await
            .unwrap();
        tokio::fs::write(session.join("updates.jsonl"), b"{\"update\":1}\n")
            .await
            .unwrap();
        tokio::fs::write(session.join("events.jsonl"), b"{\"event\":1}\n")
            .await
            .unwrap();
        tokio::fs::write(session.join("system_prompt.txt"), b"You are Grok.")
            .await
            .unwrap();
        let mut env = Envelope {
            source: "grok".into(),
            source_version: None,
            plugin_version: None,
            session_id: "session-1".into(),
            event: "stop".into(),
            ts_ms: 1,
            managed_run_id: None,
            capture: None,
            payload: serde_json::json!({
                "transcriptPath": session.join("chat_history.jsonl")
            }),
            route: None,
            config: None,
        };

        hydrate_transcript_reference(tmp.path(), &mut env).await;

        let mirrors = &env.payload["_bt_grok_transcript_mirrors"];
        for name in ["updates", "events", "system_prompt"] {
            let path = mirrors[name]["mirror"].as_str().unwrap();
            assert!(std::path::Path::new(path).is_file());
            assert!(mirrors[name]["through"].as_u64().unwrap() > 0);
        }
    }

    #[tokio::test]
    async fn terminal_grok_hydration_waits_for_a_stable_transcript_boundary() {
        let tmp = tempfile::tempdir().unwrap();
        let session = tmp.path().join("native-session");
        tokio::fs::create_dir(&session).await.unwrap();
        tokio::fs::write(session.join("chat_history.jsonl"), b"")
            .await
            .unwrap();
        let updates = session.join("updates.jsonl");
        tokio::fs::write(&updates, b"{\"update\":1}\n")
            .await
            .unwrap();
        let writer_path = updates.clone();
        let writer = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            tokio::fs::write(&writer_path, b"{\"update\":1}\n{\"update\":2}\n")
                .await
                .unwrap();
        });
        let mut env = Envelope {
            source: "grok".into(),
            source_version: None,
            plugin_version: None,
            session_id: "session-1".into(),
            event: "session_end".into(),
            ts_ms: 1,
            managed_run_id: None,
            capture: None,
            payload: serde_json::json!({
                "transcriptPath": session.join("chat_history.jsonl")
            }),
            route: None,
            config: None,
        };

        hydrate_transcript_reference(tmp.path(), &mut env).await;
        writer.await.unwrap();

        let update_mirror = &env.payload["_bt_grok_transcript_mirrors"]["updates"];
        assert_eq!(
            update_mirror["through"],
            std::fs::metadata(&updates).unwrap().len()
        );
        assert_eq!(
            std::fs::read(update_mirror["mirror"].as_str().unwrap()).unwrap(),
            std::fs::read(updates).unwrap()
        );
    }
}
