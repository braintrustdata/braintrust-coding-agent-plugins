//! The daemon: owns the session map + shared deps, binds the UDS listener,
//! serves JSON-RPC connections, and shuts down gracefully (idle timeout,
//! `daemon.shutdown`, or SIGINT/SIGTERM).

use crate::dispatch::{hydrate_transcript_reference, ReplayPlan, Session, SessionOptions};
use crate::journal::{self, JournalWriter};
use crate::sink::SinkFactory;
use crate::translate::Registry;
use crate::transport::{self, Listener, ServerStream};
use crate::wire::{
    error_code, method, Capabilities, Envelope, EventLogResult, FlushParams, FlushResult,
    InitializeParams, InitializeResult, ManagedRunFlushParams, Message, Request, Response,
    RpcError, SessionStatus, ShutdownResult, StatusParams, StatusResult, PROTOCOL_VERSION,
};
use crate::wire::{AuthSelection, BackendAuth, SessionRoute};
use crate::{paths, ServeArgs};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, oneshot, Notify};

/// Injected dependencies for `serve`, so `bt` / tests can supply a sink
/// factory (Braintrust in production, debug in tests) and a version string.
pub struct ServeOptions {
    pub version: String,
    pub translators: Arc<Registry>,
    pub sink_factory: Arc<dyn SinkFactory>,
    /// Host-owned access to Braintrust profiles, OAuth, and keychains. The
    /// daemon owns lease timing and session routing; the embedding `bt`
    /// process owns the credential store implementation.
    pub auth_provider: Option<Arc<dyn AuthProvider>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthResolveReason {
    Initial,
    Expiring,
    Unauthorized,
}

/// A live credential lease. Only the canonical non-secret selection is
/// observable; the backend credential remains in daemon memory and is never
/// journaled.
#[derive(Debug, Clone)]
pub struct AuthLease {
    pub selection: AuthSelection,
    pub auth: BackendAuth,
    /// Epoch milliseconds. `None` is appropriate for non-expiring API keys.
    pub expires_at_ms: Option<i64>,
}

#[async_trait]
pub trait AuthProvider: Send + Sync {
    /// Resolve without prompting. On refresh, `selection` is the canonical
    /// source returned by the initial lease, keeping an active session pinned
    /// even if the user's default profile or environment changes.
    async fn resolve(
        &self,
        selection: &AuthSelection,
        reason: AuthResolveReason,
    ) -> anyhow::Result<AuthLease>;
}

#[derive(Clone)]
struct SessionAuthState {
    route: SessionRoute,
    lease: AuthLease,
}

#[derive(Default, Serialize, Deserialize)]
struct PendingSession {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    linked_route: Option<SessionRoute>,
    #[serde(default)]
    events: Vec<PendingEvent>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    candidate_span_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    evidence: Vec<Value>,
}

#[derive(Clone, Serialize)]
struct PendingEvent {
    env: Envelope,
    replay_through: u64,
    journal_through: u64,
}

impl<'de> Deserialize<'de> for PendingEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum StoredPendingEvent {
            Current {
                env: Envelope,
                replay_through: u64,
                journal_through: u64,
            },
            Legacy(Envelope),
        }
        Ok(match StoredPendingEvent::deserialize(deserializer)? {
            StoredPendingEvent::Current {
                env,
                replay_through,
                journal_through,
            } => Self {
                env,
                replay_through,
                journal_through,
            },
            StoredPendingEvent::Legacy(env) => Self {
                env,
                replay_through: 0,
                journal_through: 0,
            },
        })
    }
}

enum IngressMsg {
    Event(Box<PendingEvent>),
    Barrier(oneshot::Sender<()>),
}

/// The hook path never waits for this queue. Once it fills, the append-only
/// journals become the overflow queue and the worker catches them up in place.
const INGRESS_QUEUE_CAPACITY: usize = 64;

/// One independent delivery pipeline for a source session and the exact route
/// carried by its hook or import envelope.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct DeliveryKey {
    source: String,
    session_id: String,
    route: String,
}

impl DeliveryKey {
    fn new(source: &str, session_id: &str, route: &SessionRoute) -> anyhow::Result<Self> {
        Ok(Self {
            source: source.to_string(),
            session_id: session_id.to_string(),
            route: serde_json::to_string(route)?,
        })
    }

    fn correlation_key(&self) -> String {
        format!(
            "{}\u{1f}{}\u{1f}{}",
            self.source, self.session_id, self.route
        )
    }
}

pub struct Daemon {
    version: String,
    data_dir: PathBuf,
    translators: Arc<Registry>,
    sink_factory: Arc<dyn SinkFactory>,
    auth_provider: Option<Arc<dyn AuthProvider>>,
    session_auth: tokio::sync::Mutex<HashMap<DeliveryKey, SessionAuthState>>,
    route_aliases: Mutex<HashMap<DeliveryKey, DeliveryKey>>,
    /// Serializes only capture-side journal appends for a source session.
    session_locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// Serializes daemon-side routing and actor creation independently of
    /// capture, which must never inherit actor or sink backpressure.
    dispatch_locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    journals: Mutex<HashMap<String, Arc<tokio::sync::Mutex<JournalWriter>>>>,
    managed_run_sessions: Mutex<HashMap<String, HashSet<DeliveryKey>>>,
    auth_errors: Mutex<HashMap<DeliveryKey, (String, String)>>,
    sessions: Mutex<HashMap<DeliveryKey, Arc<Session>>>,
    correlation: Arc<crate::correlation::CorrelationRegistry>,
    automatic_links: Mutex<HashMap<String, SessionRoute>>,
    pending_sessions: Mutex<HashMap<String, PendingSession>>,
    pending_reconcile_lock: tokio::sync::Mutex<()>,
    correlation_locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    correlation_changed: Arc<Notify>,
    ingress_tx: mpsc::Sender<IngressMsg>,
    ingress_overflow: AtomicBool,
    ingress_dispatched: Mutex<HashMap<DeliveryKey, u64>>,
    started: Instant,
    last_activity: Mutex<Instant>,
    shutting_down: AtomicBool,
    shutdown: Notify,
}

impl Daemon {
    fn new(opts: ServeOptions, data_dir: PathBuf) -> Arc<Self> {
        let (ingress_tx, ingress_rx) = mpsc::channel(INGRESS_QUEUE_CAPACITY);
        let daemon = Arc::new(Daemon {
            version: opts.version,
            data_dir,
            translators: opts.translators,
            sink_factory: opts.sink_factory,
            auth_provider: opts.auth_provider,
            session_auth: tokio::sync::Mutex::new(HashMap::new()),
            route_aliases: Mutex::new(HashMap::new()),
            session_locks: Mutex::new(HashMap::new()),
            dispatch_locks: Mutex::new(HashMap::new()),
            journals: Mutex::new(HashMap::new()),
            managed_run_sessions: Mutex::new(HashMap::new()),
            auth_errors: Mutex::new(HashMap::new()),
            sessions: Mutex::new(HashMap::new()),
            correlation: Arc::new(crate::correlation::CorrelationRegistry::default()),
            automatic_links: Mutex::new(HashMap::new()),
            pending_sessions: Mutex::new(HashMap::new()),
            pending_reconcile_lock: tokio::sync::Mutex::new(()),
            correlation_locks: Mutex::new(HashMap::new()),
            correlation_changed: Arc::new(Notify::new()),
            ingress_tx,
            ingress_overflow: AtomicBool::new(false),
            ingress_dispatched: Mutex::new(HashMap::new()),
            started: Instant::now(),
            last_activity: Mutex::new(Instant::now()),
            shutting_down: AtomicBool::new(false),
            shutdown: Notify::new(),
        });
        spawn_ingress_worker(daemon.clone(), ingress_rx);
        spawn_pending_reconciler(daemon.clone());
        daemon
    }

    async fn configure_event(&self, env: &mut Envelope) -> anyhow::Result<DeliveryKey> {
        let Some(provider) = &self.auth_provider else {
            anyhow::bail!("daemon host has no Braintrust auth provider");
        };
        let requested_route = env
            .route
            .clone()
            .ok_or_else(|| anyhow::anyhow!("event is missing its session route"))?;
        if requested_route.destination.is_none() {
            anyhow::bail!(
                "session route is missing its trace destination; select a project or destination during `bt trace setup` or `bt trace run`"
            );
        }
        let requested_key = DeliveryKey::new(&env.source, &env.session_id, &requested_route)?;
        let key = self
            .route_aliases
            .lock()
            .unwrap()
            .get(&requested_key)
            .cloned()
            .unwrap_or_else(|| requested_key.clone());
        let (selection, reason, expected_selection) = {
            let states = self.session_auth.lock().await;
            match states.get(&key) {
                Some(state) => {
                    if !lease_is_expiring(&state.lease) {
                        env.config = Some(state.route.with_auth(state.lease.auth.clone()));
                        return Ok(key);
                    }
                    (
                        state.lease.selection.clone(),
                        AuthResolveReason::Expiring,
                        Some(state.lease.selection.clone()),
                    )
                }
                None => (
                    requested_route.auth.clone(),
                    AuthResolveReason::Initial,
                    None,
                ),
            }
        };

        let lease = provider.resolve(&selection, reason).await.map_err(|error| {
            let message = format!(
                "could not resolve Braintrust auth for {}: {error}; run `bt login` or select a profile explicitly",
                env.source
            );
            self.auth_errors.lock().unwrap().insert(
                key.clone(),
                (env.source.clone(), message.clone()),
            );
            anyhow::anyhow!(message)
        })?;
        if let Some(expected) = expected_selection {
            if lease.selection != expected {
                anyhow::bail!(
                    "credential refresh changed auth selection from {expected:?} to {:?}",
                    lease.selection
                );
            }
        }
        if let Some(expected_org) = requested_route.auth.org_name.as_deref() {
            if lease.auth.org_name.as_deref() != Some(expected_org) {
                anyhow::bail!(
                    "profile {:?} resolved organization {:?}, expected {:?}",
                    lease.selection,
                    lease.auth.org_name,
                    expected_org
                );
            }
        }

        let mut route = requested_route;
        if route.auth.effective_source() == crate::wire::AuthSource::SavedProfile
            && route.auth.profile_id.is_none()
            && lease.selection.profile_id.is_some()
        {
            route.auth = lease.selection.clone();
            if let Err(error) = crate::settings::migrate_persisted_route(
                &env.source,
                env.route.as_ref().expect("route checked above"),
                &route,
            ) {
                tracing::warn!(source = %env.source, "could not migrate legacy profile route: {error}");
            }
        }
        let canonical_key = DeliveryKey::new(&env.source, &env.session_id, &route)?;
        if canonical_key != requested_key {
            self.route_aliases
                .lock()
                .unwrap()
                .insert(requested_key, canonical_key.clone());
        }
        env.route = Some(route.clone());
        env.config = Some(route.with_auth(lease.auth.clone()));
        self.session_auth
            .lock()
            .await
            .insert(canonical_key.clone(), SessionAuthState { route, lease });
        self.auth_errors.lock().unwrap().remove(&canonical_key);
        Ok(canonical_key)
    }

    async fn refresh_session_before_flush(&self, key: &DeliveryKey) -> anyhow::Result<()> {
        let Some(provider) = &self.auth_provider else {
            return Ok(());
        };
        let Some(state) = self.session_auth.lock().await.get(key).cloned() else {
            return Ok(());
        };
        if !lease_is_expiring(&state.lease) {
            return Ok(());
        }
        let selection = state.lease.selection.clone();
        let lease = provider
            .resolve(&selection, AuthResolveReason::Expiring)
            .await?;
        if lease.selection != state.lease.selection {
            anyhow::bail!("credential refresh changed the session auth selection");
        }
        let config = state.route.with_auth(lease.auth.clone());
        self.session_auth.lock().await.insert(
            key.clone(),
            SessionAuthState {
                route: state.route,
                lease,
            },
        );
        let session = { self.sessions.lock().unwrap().get(key).cloned() };
        if let Some(session) = session {
            session.configure(config).await?;
        }
        Ok(())
    }

    fn touch(&self) {
        *self.last_activity.lock().unwrap() = Instant::now();
    }

    fn correlation_lock(&self, key: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.correlation_locks.lock().unwrap();
        locks
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    async fn session_for(
        &self,
        env: &Envelope,
        key: &DeliveryKey,
        replay_through: u64,
    ) -> anyhow::Result<Arc<Session>> {
        {
            let map = self.sessions.lock().unwrap();
            if let Some(session) = map.get(key) {
                return Ok(session.clone());
            }
        }
        let route = env
            .route
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("event is missing its session route"))?;
        let config = env.config.clone().ok_or_else(|| {
            anyhow::anyhow!("resolved route is missing its session configuration")
        })?;
        // The actor streams the journal itself, so creating a session stays
        // cheap and allocation-free here no matter how long the recorded
        // session is. Bound it to what is recorded now, before this event is
        // appended, so replay covers prior source observations only. Every
        // destination consumes that shared observation stream, with its own
        // delivery checkpoint controlling which rows reach its sink.
        let journal_path =
            journal::ensure_source_journal(&self.data_dir, &env.source, &env.session_id).await?;
        let translator_session_id =
            if journal::legacy_journal_has_session(&self.data_dir, &env.source, &env.session_id)
                .await
            {
                env.session_id.clone()
            } else {
                crate::ids::session_namespace(&env.source, &env.session_id)
            };
        let replay = ReplayPlan {
            acknowledged_through: journal::JournalReader::acknowledged_through(
                &journal_path,
                replay_through,
                route,
            )
            .await,
            through: replay_through,
            journal_path,
        };
        let journal = self
            .journal_writer_for(&env.source, &env.session_id)
            .await?;
        let mut map = self.sessions.lock().unwrap();
        if let Some(s) = map.get(key) {
            return Ok(s.clone());
        }
        let session = Session::spawn(
            SessionOptions {
                session_id: env.session_id.clone(),
                translator_session_id,
                source: env.source.clone(),
                plugin_version: env.plugin_version.clone(),
                replay: Some(replay),
                config,
                correlation_key: key.correlation_key(),
                route: route.clone(),
                correlation: self.correlation.clone(),
                data_dir: self.data_dir.clone(),
                journal,
                correlation_changed: self.correlation_changed.clone(),
            },
            self.translators.clone(),
            self.sink_factory.clone(),
        );
        map.insert(key.clone(), session.clone());
        Ok(session)
    }

    /// Drop every trace of one delivery pipeline. A session that goes quiet
    /// must not pin its translator state, sink handles, credential lease, or
    /// journal file for the rest of the daemon's life; deterministic span ids
    /// mean a late event simply rebuilds it from the journal.
    async fn retire_session(&self, key: &DeliveryKey) {
        let lock = self.dispatch_lock(&key.source, &key.session_id);
        let _guard = lock.lock().await;

        let session = { self.sessions.lock().unwrap().remove(key) };
        let Some(session) = session else {
            return;
        };
        session.shutdown().await;
        self.correlation.remove_session(&key.correlation_key());

        self.session_auth.lock().await.remove(key);
        self.auth_errors.lock().unwrap().remove(key);
        self.route_aliases
            .lock()
            .unwrap()
            .retain(|alias, target| alias != key && target != key);
        self.managed_run_sessions.lock().unwrap().retain(|_, keys| {
            keys.remove(key);
            !keys.is_empty()
        });

        // Several delivery routes can share one source session, so release
        // its journal writer and lock only after its final route retires.
        let last = !self
            .sessions
            .lock()
            .unwrap()
            .keys()
            .any(|other| other.source == key.source && other.session_id == key.session_id);
        if last {
            let storage_key = crate::ids::session_namespace(&key.source, &key.session_id);
            // Capture can proceed while the actor flushes above. Only take its
            // lock for the brief writer-map cleanup after daemon work ends.
            let capture_lock = self.session_lock(&key.source, &key.session_id);
            let _capture_guard = capture_lock.lock().await;
            self.journals.lock().unwrap().remove(&storage_key);
            self.session_locks.lock().unwrap().remove(&storage_key);
            self.dispatch_locks.lock().unwrap().remove(&storage_key);
            self.ingress_dispatched
                .lock()
                .unwrap()
                .retain(|candidate, _| {
                    candidate.source != key.source || candidate.session_id != key.session_id
                });
        }
        tracing::info!(session_id = %key.session_id, "session retired");
    }

    /// Delivery pipelines with no traffic for `idle_timeout` and nothing left
    /// queued.
    fn idle_sessions(&self, idle_timeout: Duration) -> Vec<DeliveryKey> {
        self.sessions
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, session)| {
                session.idle_for() >= idle_timeout
                    && session.counters.queued.load(Ordering::Relaxed) == 0
            })
            .filter(|(key, _)| !self.correlation.has_active_tools(&key.correlation_key()))
            .map(|(key, _)| key.clone())
            .collect()
    }

    async fn journal_writer_for(
        &self,
        source: &str,
        session_id: &str,
    ) -> anyhow::Result<Arc<tokio::sync::Mutex<JournalWriter>>> {
        let storage_key = crate::ids::session_namespace(source, session_id);
        if let Some(writer) = self.journals.lock().unwrap().get(&storage_key).cloned() {
            return Ok(writer);
        }
        let path = journal::ensure_source_journal(&self.data_dir, source, session_id).await?;
        let writer = Arc::new(tokio::sync::Mutex::new(
            JournalWriter::open_path(&path).await?,
        ));
        Ok(self
            .journals
            .lock()
            .unwrap()
            .entry(storage_key)
            .or_insert_with(|| writer.clone())
            .clone())
    }

    async fn append_to_journal(&self, env: &mut Envelope) -> anyhow::Result<(u64, u64)> {
        hydrate_transcript_reference(&self.data_dir, env).await;
        let writer = self
            .journal_writer_for(&env.source, &env.session_id)
            .await?;
        let mut writer = writer.lock().await;
        let before = writer.position();
        let through = writer.append(env).await?;
        Ok((before, through))
    }

    fn session_lock(&self, source: &str, session_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        let storage_key = crate::ids::session_namespace(source, session_id);
        self.session_locks
            .lock()
            .unwrap()
            .entry(storage_key)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    fn dispatch_lock(&self, source: &str, session_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        let storage_key = crate::ids::session_namespace(source, session_id);
        self.dispatch_locks
            .lock()
            .unwrap()
            .entry(storage_key)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    fn total_queued(&self) -> u64 {
        self.sessions
            .lock()
            .unwrap()
            .values()
            .map(|s| s.counters.queued.load(Ordering::Relaxed))
            .sum()
    }

    async fn capture_event(&self, mut env: Envelope) -> Result<(), String> {
        env.source = self
            .translators
            .canonical_source(&env.source)
            .ok_or_else(|| format!("unsupported coding-agent source {:?}", env.source))?
            .to_string();
        self.touch();
        let lock = self.session_lock(&env.source, &env.session_id);
        let _guard = lock.lock().await;
        let (replay_through, journal_through) = self
            .append_to_journal(&mut env)
            .await
            .map_err(|error| format!("journal failed: {error}"))?;
        match self
            .ingress_tx
            .try_send(IngressMsg::Event(Box::new(PendingEvent {
                env,
                replay_through,
                journal_through,
            }))) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.ingress_overflow.store(true, Ordering::Release);
                tracing::debug!("ingress queue full; journal will be drained by daemon worker");
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                tracing::warn!("journaled event will be recovered after daemon restart");
            }
        }
        Ok(())
    }

    async fn ingress_barrier(&self) {
        let (tx, rx) = oneshot::channel();
        if self.ingress_tx.send(IngressMsg::Barrier(tx)).await.is_ok() {
            let _ = rx.await;
        }
    }

    async fn settle_ingress(self: &Arc<Self>) {
        self.ingress_barrier().await;
        self.settle_session_actors().await;
        let _ = retry_pending_sessions(self).await;
    }

    async fn settle_session_actors(&self) {
        let sessions: Vec<_> = self.sessions.lock().unwrap().values().cloned().collect();
        for session in sessions {
            session.barrier().await;
        }
    }

    fn record_managed_run_session(&self, managed_run_id: &str, key: &DeliveryKey) -> bool {
        self.managed_run_sessions
            .lock()
            .unwrap()
            .entry(managed_run_id.to_string())
            .or_default()
            .insert(key.clone())
    }

    async fn persist_managed_run_session(
        &self,
        managed_run_id: &str,
        key: &DeliveryKey,
        route: &SessionRoute,
    ) {
        if !self.record_managed_run_session(managed_run_id, key) {
            return;
        }
        let record = journal::ManagedRunKey {
            source: Some(key.source.clone()),
            session_id: key.session_id.clone(),
            route: route.clone(),
        };
        if let Err(error) =
            journal::append_managed_run_key(&self.data_dir, managed_run_id, &record).await
        {
            tracing::warn!(
                managed_run_id,
                session_id = %key.session_id,
                %error,
                "failed to record managed run delivery pipeline"
            );
        }
    }

    async fn flush_managed_run(&self, params: ManagedRunFlushParams) -> FlushResult {
        let mut delivery_keys: HashSet<DeliveryKey> = self
            .managed_run_sessions
            .lock()
            .unwrap()
            .get(&params.managed_run_id)
            .cloned()
            .unwrap_or_default();
        // A daemon restart or idle exit loses the in-memory mapping; the
        // persisted record keeps flush accounting accurate for runs whose
        // events were accepted by an earlier daemon generation.
        for record in journal::read_managed_run_keys(&self.data_dir, &params.managed_run_id).await {
            if let Some(source) = record.source {
                if let Ok(key) = DeliveryKey::new(&source, &record.session_id, &record.route) {
                    delivery_keys.insert(key);
                }
            } else {
                // Legacy records predate source-qualified delivery keys. A
                // matching live pipeline is authoritative; otherwise recover
                // the source from the legacy journal.
                let live: Vec<_> = self
                    .sessions
                    .lock()
                    .unwrap()
                    .keys()
                    .filter(|key| {
                        key.session_id == record.session_id
                            && serde_json::from_str::<SessionRoute>(&key.route)
                                .is_ok_and(|route| route.same_route(&record.route))
                    })
                    .cloned()
                    .collect();
                if live.is_empty() {
                    if let Some(source) = journal::legacy_journal_source(
                        &self.data_dir,
                        &record.session_id,
                        &record.route,
                    )
                    .await
                    .and_then(|source| {
                        self.translators
                            .canonical_source(&source)
                            .map(str::to_owned)
                    }) {
                        if let Ok(key) =
                            DeliveryKey::new(&source, &record.session_id, &record.route)
                        {
                            delivery_keys.insert(key);
                        }
                    }
                } else {
                    delivery_keys.extend(live);
                }
            }
        }
        let mut result = FlushResult {
            flushed: true,
            pending: 0,
            accepted_sessions: delivery_keys.len() as u64,
        };
        for key in delivery_keys {
            if let Err(error) = self.refresh_session_before_flush(&key).await {
                tracing::warn!(
                    managed_run_id = %params.managed_run_id,
                    session_id = %key.session_id,
                    %error,
                    "managed run session auth refresh failed"
                );
                result.flushed = false;
                continue;
            }
            let session = { self.sessions.lock().unwrap().get(&key).cloned() };
            if let Some(session) = session {
                let (flushed, pending) = session
                    .finalize(Duration::from_millis(params.timeout_ms))
                    .await;
                result.flushed &= flushed;
                result.pending = result.pending.saturating_add(pending);
            }
        }
        result
    }

    /// Flush every live delivery route for one source session. This is used
    /// only by the daemon worker after a turn-ending event has already been
    /// durably captured and acknowledged to the hook client.
    async fn flush_source_session(&self, source: &str, session_id: &str, timeout: Duration) {
        let sessions: Vec<_> = self
            .sessions
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.source == source && key.session_id == session_id)
            .map(|(_, session)| session.clone())
            .collect();
        for session in sessions {
            let (flushed, pending) = session.flush(timeout).await;
            if !flushed {
                tracing::warn!(
                    source,
                    session_id,
                    pending,
                    "out-of-band turn-end flush did not complete"
                );
            }
        }
    }

    fn trigger_shutdown(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);
        self.shutdown.notify_waiters();
    }
}

fn lease_is_expiring(lease: &AuthLease) -> bool {
    const REFRESH_WINDOW_MS: i64 = 60_000;
    let Some(expires_at_ms) = lease.expires_at_ms else {
        return false;
    };
    expires_at_ms <= now_ms().saturating_add(REFRESH_WINDOW_MS)
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

fn spawn_ingress_worker(daemon: Arc<Daemon>, mut rx: mpsc::Receiver<IngressMsg>) {
    tokio::spawn(async move {
        loop {
            if daemon.ingress_overflow.swap(false, Ordering::AcqRel) {
                recover_ingress_overflow(&daemon).await;
                continue;
            }
            let Some(msg) = rx.recv().await else {
                break;
            };
            match msg {
                IngressMsg::Event(event) => {
                    dispatch_ingress_event(&daemon, *event).await;
                }
                IngressMsg::Barrier(reply) => {
                    loop {
                        daemon.settle_session_actors().await;
                        let _ = retry_pending_sessions(&daemon).await;
                        if !daemon.ingress_overflow.swap(false, Ordering::AcqRel) {
                            break;
                        }
                        recover_ingress_overflow(&daemon).await;
                    }
                    let _ = reply.send(());
                }
            }
        }
    });
}

async fn dispatch_ingress_event(daemon: &Arc<Daemon>, event: PendingEvent) {
    let ingress_key =
        event.env.route.as_ref().and_then(|route| {
            DeliveryKey::new(&event.env.source, &event.env.session_id, route).ok()
        });
    if ingress_key.as_ref().is_some_and(|key| {
        daemon
            .ingress_dispatched
            .lock()
            .unwrap()
            .get(key)
            .is_some_and(|through| *through >= event.journal_through)
    }) {
        return;
    }

    let schedule_flush = crate::should_flush_hook_event(
        &event.env.event,
        matches!(
            event.env.route.as_ref().map(|route| route.flush_mode),
            Some(crate::wire::FlushMode::FlushOnTurnEnd)
        ),
    );
    let flush_source = event.env.source.clone();
    let flush_session_id = event.env.session_id.clone();
    let journal_through = event.journal_through;
    // A child may arrive immediately after a parent's tool hook. Catch prior
    // session actors up before resolving a new session, entirely in the daemon.
    if is_session_start(&event.env.event) {
        daemon.settle_session_actors().await;
    }
    match accept_event(daemon, event).await {
        Ok(()) => {
            if let Some(key) = ingress_key {
                let mut dispatched = daemon.ingress_dispatched.lock().unwrap();
                let through = dispatched.entry(key).or_default();
                *through = (*through).max(journal_through);
            }
        }
        Err(error) => {
            tracing::warn!(%error, "journaled ingress event could not be dispatched");
            return;
        }
    }
    if schedule_flush {
        let daemon = daemon.clone();
        tokio::spawn(async move {
            daemon
                .flush_source_session(&flush_source, &flush_session_id, Duration::from_secs(10))
                .await;
        });
    }
}

/// Drain events omitted from the bounded in-memory queue. The queue contains
/// only a latency fast path; journals remain the complete source of ingress.
async fn recover_ingress_overflow(daemon: &Arc<Daemon>) {
    let Ok(mut entries) = tokio::fs::read_dir(journal::journal_dir(&daemon.data_dir)).await else {
        return;
    };
    let mut recovered = 0usize;
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("ndjson") {
            continue;
        }
        let recorded_len = journal::JournalReader::recorded_len(&path).await;
        let Ok(Some(mut checkpoints)) = journal::JournalReader::open(&path, recorded_len).await
        else {
            continue;
        };
        let mut acknowledged_by_route: HashMap<String, u64> = HashMap::new();
        while let Ok(Some(entry)) = checkpoints.next_record().await {
            if let journal::JournalRecord::DeliveryCheckpoint { route, through } = entry.record {
                let key = serde_json::to_string(&route).unwrap_or_default();
                let acknowledged = acknowledged_by_route.entry(key).or_default();
                *acknowledged = (*acknowledged).max(through);
            }
        }

        let Ok(Some(mut reader)) = journal::JournalReader::open(&path, recorded_len).await else {
            continue;
        };
        let mut before = 0u64;
        while let Ok(Some(entry)) = reader.next_record().await {
            let through = entry.through;
            let journal::JournalRecord::Event(redacted) = entry.record else {
                before = through;
                continue;
            };
            let mut env = journal::envelope_from_redacted(redacted);
            let Some(source) = daemon.translators.canonical_source(&env.source) else {
                before = through;
                continue;
            };
            env.source = source.to_string();
            let Some(route) = env.route.as_ref() else {
                before = through;
                continue;
            };
            let route_json = recovered_delivery_route(daemon, &env)
                .await
                .as_ref()
                .and_then(|route| serde_json::to_string(route).ok())
                .unwrap_or_else(|| serde_json::to_string(route).unwrap_or_default());
            let key = match DeliveryKey::new(&env.source, &env.session_id, route) {
                Ok(key) => key,
                Err(_) => {
                    before = through;
                    continue;
                }
            };
            let dispatched = daemon
                .ingress_dispatched
                .lock()
                .unwrap()
                .get(&key)
                .copied()
                .unwrap_or(0);
            let acknowledged = acknowledged_by_route.get(&route_json).copied().unwrap_or(0);
            if through > dispatched.max(acknowledged) {
                dispatch_ingress_event(
                    daemon,
                    PendingEvent {
                        env,
                        replay_through: before,
                        journal_through: through,
                    },
                )
                .await;
                recovered += 1;
            }
            before = through;
        }
    }
    if recovered > 0 {
        tracing::info!(recovered, "drained journal-backed ingress overflow");
    }
}

fn spawn_pending_reconciler(daemon: Arc<Daemon>) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = daemon.shutdown.notified() => return,
                _ = daemon.correlation_changed.notified() => {
                    if let Err(error) = retry_pending_sessions(&daemon).await {
                        tracing::warn!(%error, "pending child-session reconciliation failed");
                    }
                }
            }
        }
    });
}

/// Bind the socket (handling a stale/rival socket), serve until shutdown, then
/// drain sessions and remove the socket.
pub async fn run(args: ServeArgs, opts: ServeOptions) -> anyhow::Result<()> {
    let socket = paths::socket_path(args.socket.as_deref());
    let data_dir = paths::data_dir(args.data_dir.as_deref());
    paths::ensure_private_dir(&data_dir)?;
    #[cfg(unix)]
    if let Some(parent) = socket.parent() {
        paths::ensure_private_dir(parent)?;
    }

    let listener = match transport::claim(&socket, || probe_alive(&socket)).await? {
        Some(l) => l,
        None => {
            tracing::info!(
                "another daemon is already serving {}; exiting",
                socket.display()
            );
            return Ok(());
        }
    };
    tracing::info!(socket = %socket.display(), "bt-daemon listening");

    let daemon = Daemon::new(opts, data_dir);
    collect_garbage(&daemon.data_dir).await;
    restore_active_parent_snapshots(&daemon.data_dir, &daemon.correlation).await;
    restore_pending_sessions(&daemon).await;
    recover_unprocessed_journals(&daemon).await;
    let idle_timeout = Duration::from_secs(args.idle_timeout_secs);
    spawn_idle_watchdog(daemon.clone(), idle_timeout);
    spawn_session_reaper(
        daemon.clone(),
        Duration::from_secs(args.session_idle_timeout_secs),
    );
    spawn_gc(daemon.clone());

    let accept_result = accept_loop(daemon.clone(), listener).await;

    // Graceful drain regardless of why we stopped.
    drain_all(&daemon).await;
    transport::cleanup(&socket);
    accept_result
}

async fn accept_loop(daemon: Arc<Daemon>, mut listener: Listener) -> anyhow::Result<()> {
    loop {
        // `Notify::notify_waiters` does not retain a permit. If accepting a
        // connection wins the select at the same time shutdown is requested,
        // check the sticky flag before waiting again so the notification
        // cannot be lost.
        if daemon.shutting_down.load(Ordering::SeqCst) {
            tracing::info!("shutdown requested");
            return Ok(());
        }
        tokio::select! {
            _ = daemon.shutdown.notified() => {
                tracing::info!("shutdown requested");
                return Ok(());
            }
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("interrupt received");
                return Ok(());
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok(stream) => {
                        let d = daemon.clone();
                        tokio::spawn(async move {
                            if let Err(e) = serve_connection(d, stream).await {
                                tracing::debug!("connection ended: {e}");
                            }
                        });
                    }
                    Err(e) => {
                        tracing::warn!("accept error: {e}");
                    }
                }
            }
        }
    }
}

async fn serve_connection(daemon: Arc<Daemon>, stream: ServerStream) -> anyhow::Result<()> {
    let (read_half, mut write_half) = tokio::io::split(stream);
    let mut lines = BufReader::new(read_half).lines();
    let mut client = None;

    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let response = match Message::from_line(&line) {
            Ok(Message::Request(req)) => {
                let request_id = req.id.clone();
                let method = req.method.clone();
                tracing::info!(
                    request_id = ?request_id,
                    method,
                    "request received"
                );
                let response = handle_request(&daemon, req, &mut client).await;
                if let Some(error) = &response.error {
                    tracing::warn!(
                        request_id = ?request_id,
                        method,
                        error_code = error.code,
                        error = %error.message,
                        "request failed"
                    );
                } else {
                    tracing::info!(
                        request_id = ?request_id,
                        method,
                        "request completed"
                    );
                }
                Some(response)
            }
            Ok(Message::Notification(note)) => {
                tracing::info!(method = %note.method, "notification received");
                // Hot-path notifications (in-process clients): process, no reply.
                if note.method == method::EVENT_LOG {
                    if let Some(params) = note.params {
                        match serde_json::from_value::<Envelope>(params) {
                            Ok(mut env) => {
                                attach_process_capture(&mut env, client.as_ref());
                                let _ = daemon.capture_event(env).await;
                            }
                            Err(error) => tracing::warn!(
                                method = %note.method,
                                error = %error,
                                "notification parameters rejected"
                            ),
                        }
                    }
                }
                None
            }
            Ok(Message::Response(_)) => None, // clients don't send us responses
            Err(e) => {
                tracing::warn!(error = %e, "request parse failed");
                Some(Response::err(
                    crate::wire::RequestId::Int(0),
                    RpcError::new(error_code::PARSE, format!("parse error: {e}")),
                ))
            }
        };

        if let Some(resp) = response {
            let mut buf = Message::Response(resp).to_line()?;
            buf.push('\n');
            write_half.write_all(buf.as_bytes()).await?;
            write_half.flush().await?;
        }
    }
    Ok(())
}

fn attach_process_capture(env: &mut Envelope, client: Option<&crate::wire::ClientInfo>) {
    if env.capture.is_some() {
        return;
    }
    let Some(pid) = client.and_then(|client| client.pid) else {
        return;
    };
    let capture = crate::process::capture_process_context(pid);
    if !capture.process_chain.is_empty() {
        env.capture = Some(capture);
    }
}

async fn accept_event(daemon: &Arc<Daemon>, event: PendingEvent) -> Result<(), String> {
    let PendingEvent {
        mut env,
        replay_through,
        journal_through,
    } = event;
    let requested_link_key = automatic_link_key(&env);
    let correlation_lock = daemon.correlation_lock(&requested_link_key);
    let _correlation_guard = correlation_lock.lock().await;
    let mut state = daemon
        .pending_sessions
        .lock()
        .unwrap()
        .remove(&requested_link_key)
        .or_else(|| {
            daemon
                .automatic_links
                .lock()
                .unwrap()
                .get(&requested_link_key)
                .cloned()
                .map(|route| PendingSession {
                    linked_route: Some(route),
                    events: Vec::new(),
                    candidate_span_ids: Vec::new(),
                    evidence: Vec::new(),
                })
        });
    if state.is_none() {
        state = read_correlation_state(&daemon.data_dir, &requested_link_key).await;
    }

    if let Some(mut state) = state {
        if let Some(route) = state.linked_route.clone() {
            for mut pending in std::mem::take(&mut state.events) {
                pending.env.route = Some(route.clone());
                accept_resolved_event(daemon, pending).await?;
            }
            write_correlation_state(&daemon.data_dir, &requested_link_key, &state).await?;
            daemon
                .automatic_links
                .lock()
                .unwrap()
                .insert(requested_link_key, route.clone());
            env.route = Some(route);
            drop(_correlation_guard);
            return accept_resolved_and_retry_pending(
                daemon,
                PendingEvent {
                    env,
                    replay_through,
                    journal_through,
                },
            )
            .await;
        }

        let capture = env.capture.as_ref().or_else(|| {
            state
                .events
                .first()
                .and_then(|event| event.env.capture.as_ref())
        });
        state.evidence.push(correlation_evidence(&env).await);
        let evidence = Value::Array(state.evidence.clone());
        match daemon.correlation.resolve_pending(
            &env.source,
            capture,
            &evidence,
            &state.candidate_span_ids,
        ) {
            crate::correlation::Resolution::Parent(parent) => {
                state.linked_route = Some(parent.route.clone());
                state.events.push(PendingEvent {
                    env,
                    replay_through,
                    journal_through,
                });
                write_correlation_state(&daemon.data_dir, &requested_link_key, &state).await?;
                for mut event in std::mem::take(&mut state.events) {
                    event.env.route = Some(parent.route.clone());
                    accept_resolved_event(daemon, event).await?;
                }
                write_correlation_state(&daemon.data_dir, &requested_link_key, &state).await?;
                daemon
                    .automatic_links
                    .lock()
                    .unwrap()
                    .insert(requested_link_key, parent.route);
                return Ok(());
            }
            crate::correlation::Resolution::Ambiguous(_)
            | crate::correlation::Resolution::Standalone => {
                state.events.push(PendingEvent {
                    env,
                    replay_through,
                    journal_through,
                });
                write_correlation_state(&daemon.data_dir, &requested_link_key, &state).await?;
                daemon
                    .pending_sessions
                    .lock()
                    .unwrap()
                    .insert(requested_link_key, state);
                daemon.correlation_changed.notify_one();
                return Ok(());
            }
        }
    }

    if is_session_start(&env.event) {
        let evidence = correlation_evidence(&env).await;
        match daemon
            .correlation
            .resolve(&env.source, None, env.capture.as_ref(), &evidence)
        {
            crate::correlation::Resolution::Parent(parent) => {
                let state = PendingSession {
                    linked_route: Some(parent.route.clone()),
                    events: Vec::new(),
                    candidate_span_ids: Vec::new(),
                    evidence: Vec::new(),
                };
                write_correlation_state(&daemon.data_dir, &requested_link_key, &state).await?;
                daemon
                    .automatic_links
                    .lock()
                    .unwrap()
                    .insert(requested_link_key, parent.route.clone());
                env.route = Some(parent.route);
            }
            crate::correlation::Resolution::Ambiguous(candidate_span_ids) => {
                let evidence = correlation_evidence(&env).await;
                let state = PendingSession {
                    linked_route: None,
                    events: vec![PendingEvent {
                        env,
                        replay_through,
                        journal_through,
                    }],
                    candidate_span_ids,
                    evidence: vec![evidence],
                };
                write_correlation_state(&daemon.data_dir, &requested_link_key, &state).await?;
                daemon
                    .pending_sessions
                    .lock()
                    .unwrap()
                    .insert(requested_link_key, state);
                daemon.correlation_changed.notify_one();
                return Ok(());
            }
            crate::correlation::Resolution::Standalone => {}
        }
    }

    drop(_correlation_guard);
    accept_resolved_and_retry_pending(
        daemon,
        PendingEvent {
            env,
            replay_through,
            journal_through,
        },
    )
    .await
}

async fn accept_resolved_and_retry_pending(
    daemon: &Arc<Daemon>,
    event: PendingEvent,
) -> Result<(), String> {
    accept_resolved_event(daemon, event).await?;
    retry_pending_sessions(daemon).await
}

async fn retry_pending_sessions(daemon: &Arc<Daemon>) -> Result<(), String> {
    let _reconcile_guard = daemon.pending_reconcile_lock.lock().await;
    let keys: Vec<String> = daemon
        .pending_sessions
        .lock()
        .unwrap()
        .keys()
        .cloned()
        .collect();
    for key in keys {
        let lock = daemon.correlation_lock(&key);
        let _guard = lock.lock().await;
        let Some(mut state) = daemon.pending_sessions.lock().unwrap().remove(&key) else {
            continue;
        };
        if state.linked_route.is_some() || state.events.is_empty() {
            daemon.pending_sessions.lock().unwrap().insert(key, state);
            continue;
        }
        let capture = state
            .events
            .iter()
            .find_map(|event| event.env.capture.as_ref());
        let evidence = Value::Array(state.evidence.clone());
        match daemon.correlation.resolve_pending(
            state
                .events
                .first()
                .map(|event| event.env.source.as_str())
                .unwrap_or(""),
            capture,
            &evidence,
            &state.candidate_span_ids,
        ) {
            crate::correlation::Resolution::Parent(parent) => {
                state.linked_route = Some(parent.route.clone());
                write_correlation_state(&daemon.data_dir, &key, &state).await?;
                for mut event in std::mem::take(&mut state.events) {
                    event.env.route = Some(parent.route.clone());
                    accept_resolved_event(daemon, event).await?;
                }
                write_correlation_state(&daemon.data_dir, &key, &state).await?;
                daemon
                    .automatic_links
                    .lock()
                    .unwrap()
                    .insert(key, parent.route);
            }
            crate::correlation::Resolution::Ambiguous(_) => {
                daemon.pending_sessions.lock().unwrap().insert(key, state);
            }
            crate::correlation::Resolution::Standalone => {
                for event in std::mem::take(&mut state.events) {
                    accept_resolved_event(daemon, event).await?;
                }
                remove_correlation_state(&daemon.data_dir, &key).await;
            }
        }
    }
    Ok(())
}

/// Build opaque matching evidence without teaching the daemon agent or shell
/// syntax. Codex hook payloads reference a JSONL rollout rather than carrying
/// the prompt/output directly, so include a bounded tail of those native JSON
/// values when available. Other agents are fully represented by their payload.
async fn correlation_evidence(env: &Envelope) -> Value {
    const MAX_TRANSCRIPT_EVIDENCE_BYTES: u64 = 256 * 1024;

    let mut evidence = vec![env.payload.clone()];
    if env.source != "codex" {
        return Value::Array(evidence);
    }
    let mirror = env.payload.get("_bt_transcript_mirror");
    let path = mirror
        .and_then(|value| value.get("mirror"))
        .and_then(Value::as_str)
        .or_else(|| env.payload.get("transcript_path").and_then(Value::as_str));
    let Some(path) = path else {
        return Value::Array(evidence);
    };
    let Ok(mut file) = tokio::fs::File::open(path).await else {
        return Value::Array(evidence);
    };
    let Ok(metadata) = file.metadata().await else {
        return Value::Array(evidence);
    };
    let through = mirror
        .and_then(|value| value.get("through"))
        .and_then(Value::as_u64)
        .unwrap_or(metadata.len())
        .min(metadata.len());
    let start = through.saturating_sub(MAX_TRANSCRIPT_EVIDENCE_BYTES);
    if file.seek(std::io::SeekFrom::Start(start)).await.is_err() {
        return Value::Array(evidence);
    }
    let mut bytes = Vec::with_capacity((through - start) as usize);
    if file
        .take(through - start)
        .read_to_end(&mut bytes)
        .await
        .is_err()
    {
        return Value::Array(evidence);
    }
    let text = String::from_utf8_lossy(&bytes);
    let text = if start == 0 {
        text.as_ref()
    } else {
        text.split_once('\n').map(|(_, tail)| tail).unwrap_or("")
    };
    evidence.extend(
        text.lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok()),
    );
    Value::Array(evidence)
}

fn correlation_state_path(data_dir: &std::path::Path, key: &str) -> PathBuf {
    let digest = Sha256::digest(key.as_bytes());
    data_dir
        .join("correlation")
        .join(format!("{digest:x}.json"))
}

async fn read_correlation_state(data_dir: &std::path::Path, key: &str) -> Option<PendingSession> {
    let bytes = tokio::fs::read(correlation_state_path(data_dir, key))
        .await
        .ok()?;
    serde_json::from_slice(&bytes).ok()
}

async fn write_correlation_state(
    data_dir: &std::path::Path,
    key: &str,
    state: &PendingSession,
) -> Result<(), String> {
    let dir = data_dir.join("correlation");
    tokio::fs::create_dir_all(&dir)
        .await
        .map_err(|error| format!("correlation journal directory failed: {error}"))?;
    let path = correlation_state_path(data_dir, key);
    let temp = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
    let bytes = serde_json::to_vec(state)
        .map_err(|error| format!("correlation journal encoding failed: {error}"))?;
    tokio::fs::write(&temp, bytes)
        .await
        .map_err(|error| format!("correlation journal write failed: {error}"))?;
    if let Err(first_error) = tokio::fs::rename(&temp, &path).await {
        // Windows cannot replace an existing destination with rename.
        let _ = tokio::fs::remove_file(&path).await;
        tokio::fs::rename(&temp, &path).await.map_err(|error| {
            format!("correlation journal replace failed: {first_error}; retry failed: {error}")
        })?;
    }
    Ok(())
}

async fn remove_correlation_state(data_dir: &std::path::Path, key: &str) {
    let _ = tokio::fs::remove_file(correlation_state_path(data_dir, key)).await;
}

fn active_parent_snapshot_path(data_dir: &std::path::Path, key: &str) -> PathBuf {
    let digest = Sha256::digest(key.as_bytes());
    data_dir
        .join("correlation")
        .join("parents")
        .join(format!("{digest:x}.json"))
}

pub(crate) async fn persist_active_parent_snapshot(
    data_dir: &std::path::Path,
    key: &str,
    correlation: &crate::correlation::CorrelationRegistry,
) -> Result<(), String> {
    let path = active_parent_snapshot_path(data_dir, key);
    let Some(snapshot) = correlation.active_parent_snapshot(key) else {
        let _ = tokio::fs::remove_file(path).await;
        return Ok(());
    };
    let bytes = serde_json::to_vec(&snapshot)
        .map_err(|error| format!("active parent encoding failed: {error}"))?;
    write_active_parent_snapshot(data_dir, path, bytes).await
}

async fn mark_active_parent_snapshot_dirty(
    data_dir: &std::path::Path,
    key: &str,
    correlation: &crate::correlation::CorrelationRegistry,
) -> Result<(), String> {
    let Some(snapshot) = correlation.dirty_active_parent_snapshot(key) else {
        return Ok(());
    };
    let bytes = serde_json::to_vec(&snapshot)
        .map_err(|error| format!("active parent encoding failed: {error}"))?;
    write_active_parent_snapshot(data_dir, active_parent_snapshot_path(data_dir, key), bytes).await
}

async fn write_active_parent_snapshot(
    data_dir: &std::path::Path,
    path: PathBuf,
    bytes: Vec<u8>,
) -> Result<(), String> {
    let dir = data_dir.join("correlation").join("parents");
    tokio::fs::create_dir_all(&dir)
        .await
        .map_err(|error| format!("active parent directory failed: {error}"))?;
    let temp = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
    tokio::fs::write(&temp, bytes)
        .await
        .map_err(|error| format!("active parent write failed: {error}"))?;
    if let Err(first_error) = tokio::fs::rename(&temp, &path).await {
        let _ = tokio::fs::remove_file(&path).await;
        tokio::fs::rename(&temp, &path).await.map_err(|error| {
            format!("active parent replace failed: {first_error}; retry failed: {error}")
        })?;
    }
    Ok(())
}

async fn restore_active_parent_snapshots(
    data_dir: &std::path::Path,
    correlation: &crate::correlation::CorrelationRegistry,
) {
    let Ok(mut entries) = tokio::fs::read_dir(data_dir.join("correlation").join("parents")).await
    else {
        return;
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let Ok(bytes) = tokio::fs::read(entry.path()).await else {
            continue;
        };
        let Ok(snapshot) =
            serde_json::from_slice::<crate::correlation::ActiveParentSnapshot>(&bytes)
        else {
            tracing::debug!(path = %entry.path().display(), "invalid active parent snapshot ignored");
            continue;
        };
        correlation.restore_active_parent(snapshot);
    }
}

async fn restore_pending_sessions(daemon: &Arc<Daemon>) {
    let Ok(mut entries) = tokio::fs::read_dir(daemon.data_dir.join("correlation")).await else {
        return;
    };
    let mut restored = 0usize;
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let Ok(bytes) = tokio::fs::read(&path).await else {
            continue;
        };
        let Ok(state) = serde_json::from_slice::<PendingSession>(&bytes) else {
            continue;
        };
        let Some(key) = state
            .events
            .first()
            .map(|event| automatic_link_key(&event.env))
        else {
            continue;
        };
        daemon.pending_sessions.lock().unwrap().insert(key, state);
        restored += 1;
    }
    if restored > 0 {
        tracing::info!(restored, "restored pending child sessions");
    }
}

async fn recover_unprocessed_journals(daemon: &Arc<Daemon>) {
    let pending: HashSet<(String, String, u64)> = daemon
        .pending_sessions
        .lock()
        .unwrap()
        .values()
        .flat_map(|state| {
            state.events.iter().map(|event| {
                (
                    event.env.source.clone(),
                    event.env.session_id.clone(),
                    event.journal_through,
                )
            })
        })
        .collect();
    let Ok(mut entries) = tokio::fs::read_dir(journal::journal_dir(&daemon.data_dir)).await else {
        return;
    };
    let mut candidates = Vec::new();
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("ndjson") {
            continue;
        }
        let recorded_len = journal::JournalReader::recorded_len(&path).await;
        let Ok(Some(mut reader)) = journal::JournalReader::open(&path, recorded_len).await else {
            continue;
        };
        let mut before = 0u64;
        let mut acknowledged_by_route: HashMap<String, u64> = HashMap::new();
        let mut latest_by_route: HashMap<String, PendingEvent> = HashMap::new();
        while let Ok(Some(entry)) = reader.next_record().await {
            let through = entry.through;
            match entry.record {
                journal::JournalRecord::Event(redacted) => {
                    let mut env = journal::envelope_from_redacted(redacted);
                    let Some(source) = daemon.translators.canonical_source(&env.source) else {
                        before = through;
                        continue;
                    };
                    env.source = source.to_string();
                    if let Some(route) = env.route.as_ref() {
                        let key = serde_json::to_string(route).unwrap_or_default();
                        latest_by_route.insert(
                            key,
                            PendingEvent {
                                env,
                                replay_through: before,
                                journal_through: through,
                            },
                        );
                    }
                }
                journal::JournalRecord::DeliveryCheckpoint { route, through } => {
                    let key = serde_json::to_string(&route).unwrap_or_default();
                    let acknowledged = acknowledged_by_route.entry(key).or_default();
                    *acknowledged = (*acknowledged).max(through);
                }
            }
            before = through;
        }
        for event in latest_by_route.into_values() {
            if pending.contains(&(
                event.env.source.clone(),
                event.env.session_id.clone(),
                event.journal_through,
            )) {
                continue;
            }
            let route = recovered_delivery_route(daemon, &event.env)
                .await
                .as_ref()
                .and_then(|route| serde_json::to_string(route).ok())
                .unwrap_or_default();
            if acknowledged_by_route.get(&route).copied().unwrap_or(0) < event.journal_through {
                candidates.push(event);
            }
        }
    }
    candidates.sort_by_key(|event| event.env.ts_ms);
    let recovered = candidates.len();
    for event in candidates {
        let _ = daemon
            .ingress_tx
            .send(IngressMsg::Event(Box::new(event)))
            .await;
    }
    // Reconciliation stays in the daemon worker and follows every recovered
    // event in queue order. Dropping the receiver is intentional: startup and
    // hook capture do not wait for translation or reporting.
    let (reply, _ignored) = oneshot::channel();
    let _ = daemon.ingress_tx.send(IngressMsg::Barrier(reply)).await;
    if recovered > 0 {
        tracing::info!(
            recovered,
            "queued unprocessed journal sessions for recovery"
        );
    }
}

async fn accept_resolved_event(daemon: &Arc<Daemon>, event: PendingEvent) -> Result<(), String> {
    let PendingEvent {
        mut env,
        replay_through,
        journal_through,
    } = event;
    let source = env.source.clone();
    let event = env.event.clone();
    let session_id = env.session_id.clone();
    let managed_run_id = env.managed_run_id.clone();
    let route = env.route.clone();
    tracing::info!(source, event, session_id, "event received");
    let session_lock = daemon.dispatch_lock(&source, &session_id);
    let _session_guard = session_lock.lock().await;

    let result = async {
        let delivery_key = daemon
            .configure_event(&mut env)
            .await
            .map_err(|error| format!("session auth failed: {error}"))?;
        if crate::dispatch::is_tool_lifecycle_event(&env.event) {
            if let Err(error) = mark_active_parent_snapshot_dirty(
                &daemon.data_dir,
                &delivery_key.correlation_key(),
                &daemon.correlation,
            )
            .await
            {
                tracing::warn!(session_id = %env.session_id, %error, "active parent snapshot could not be marked dirty");
            }
        }
        let session = daemon
            .session_for(&env, &delivery_key, replay_through)
            .await
            .map_err(|error| format!("session init failed: {error}"))?;
        daemon
            .correlation
            .observe_session(&delivery_key.correlation_key(), env.capture.as_ref());
        session
            .enqueue(env, journal_through)
            .await
            .map_err(|error| format!("enqueue failed: {error}"))?;
        Ok(delivery_key)
    }
    .await;

    match &result {
        Ok(delivery_key) => {
            if let (Some(managed_run_id), Some(route)) = (managed_run_id, route) {
                daemon
                    .persist_managed_run_session(&managed_run_id, delivery_key, &route)
                    .await;
            }
            tracing::info!(source, event, session_id, "event accepted")
        }
        Err(error) => tracing::warn!(source, event, session_id, error, "event rejected"),
    }
    result.map(|_| ())
}

fn is_session_start(event: &str) -> bool {
    matches!(event, "SessionStart" | "session_start" | "session.created")
}

fn automatic_link_key(env: &Envelope) -> String {
    let route = env
        .route
        .as_ref()
        .and_then(|route| serde_json::to_string(route).ok())
        .unwrap_or_default();
    format!("{}\u{1f}{}\u{1f}{route}", env.source, env.session_id)
}

/// Correlated child events retain their setup route in the immutable journal,
/// while their delivery checkpoint belongs to the resolved parent route.
/// Recover against that effective route without rewriting the captured event.
async fn recovered_delivery_route(daemon: &Arc<Daemon>, env: &Envelope) -> Option<SessionRoute> {
    let key = automatic_link_key(env);
    if let Some(route) = daemon.automatic_links.lock().unwrap().get(&key).cloned() {
        return Some(route);
    }
    if let Some(route) = daemon
        .pending_sessions
        .lock()
        .unwrap()
        .get(&key)
        .and_then(|state| state.linked_route.clone())
    {
        return Some(route);
    }
    read_correlation_state(&daemon.data_dir, &key)
        .await
        .and_then(|state| state.linked_route)
        .or_else(|| env.route.clone())
}

async fn handle_request(
    daemon: &Arc<Daemon>,
    req: Request,
    client: &mut Option<crate::wire::ClientInfo>,
) -> Response {
    let id = req.id.clone();
    let params = req.params.unwrap_or(serde_json::Value::Null);

    macro_rules! parse {
        ($t:ty) => {
            match serde_json::from_value::<$t>(params) {
                Ok(v) => v,
                Err(e) => {
                    return Response::err(
                        id,
                        RpcError::new(error_code::INVALID_PARAMS, format!("invalid params: {e}")),
                    )
                }
            }
        };
    }

    match req.method.as_str() {
        method::INITIALIZE => {
            let p = parse!(InitializeParams);
            if p.protocol_version != PROTOCOL_VERSION {
                return Response::err(
                    id,
                    RpcError::new(
                        error_code::APP,
                        format!(
                            "protocol version mismatch: client {} daemon {}",
                            p.protocol_version, PROTOCOL_VERSION
                        ),
                    ),
                );
            }
            *client = Some(p.client);
            let result = InitializeResult {
                protocol_version: PROTOCOL_VERSION,
                daemon_version: daemon.version.clone(),
                capabilities: Capabilities {
                    sources: daemon.translators.sources(),
                },
            };
            Response::ok(id, serde_json::to_value(result).unwrap())
        }
        method::EVENT_LOG => {
            let mut env = parse!(Envelope);
            attach_process_capture(&mut env, client.as_ref());
            match daemon.capture_event(env).await {
                Ok(()) => Response::ok(
                    id,
                    serde_json::to_value(EventLogResult { accepted: true }).unwrap(),
                ),
                Err(error) => Response::err(id, RpcError::new(error_code::INTERNAL, error)),
            }
        }
        method::SESSION_FLUSH => {
            let p = parse!(FlushParams);
            // Explicit flushes wait for the daemon-owned ingress queue. Hook
            // capture never waits on this barrier.
            daemon.settle_ingress().await;
            let delivery_keys: Vec<_> = daemon
                .sessions
                .lock()
                .unwrap()
                .keys()
                .filter(|key| key.session_id == p.session_id)
                .cloned()
                .collect();
            let accepted_sessions = delivery_keys.len() as u64;
            let mut flushed = true;
            let mut pending = 0u64;
            for key in delivery_keys {
                if let Err(error) = daemon.refresh_session_before_flush(&key).await {
                    return Response::err(
                        id,
                        RpcError::new(
                            error_code::INTERNAL,
                            format!("session auth refresh failed: {error}"),
                        ),
                    );
                }
                let session = { daemon.sessions.lock().unwrap().get(&key).cloned() };
                if let Some(session) = session {
                    let (route_flushed, route_pending) =
                        session.flush(Duration::from_millis(p.timeout_ms)).await;
                    flushed &= route_flushed;
                    pending = pending.saturating_add(route_pending);
                }
            }
            Response::ok(
                id,
                serde_json::to_value(FlushResult {
                    flushed,
                    pending,
                    accepted_sessions,
                })
                .unwrap(),
            )
        }
        method::MANAGED_RUN_FLUSH => {
            let params = parse!(ManagedRunFlushParams);
            daemon.settle_ingress().await;
            let result = daemon.flush_managed_run(params).await;
            Response::ok(id, serde_json::to_value(result).unwrap())
        }
        method::STATUS_GET => {
            let p = parse!(StatusParams);
            daemon.settle_ingress().await;
            Response::ok(id, serde_json::to_value(daemon.status(p)).unwrap())
        }
        method::DAEMON_SHUTDOWN => {
            let resp = Response::ok(
                id,
                serde_json::to_value(ShutdownResult { ok: true }).unwrap(),
            );
            daemon.trigger_shutdown();
            resp
        }
        other => Response::err(
            id,
            RpcError::new(
                error_code::METHOD_NOT_FOUND,
                format!("unknown method: {other}"),
            ),
        ),
    }
}

impl Daemon {
    fn status(&self, p: StatusParams) -> StatusResult {
        let map = self.sessions.lock().unwrap();
        let mut sessions: Vec<_> = map
            .iter()
            .filter(|(key, _)| {
                p.session_id
                    .as_ref()
                    .is_none_or(|want| want == &key.session_id)
            })
            .map(|(key, s)| SessionStatus {
                session_id: key.session_id.clone(),
                source: s.source.clone(),
                route: serde_json::from_str(&key.route).ok(),
                queued: s.counters.queued.load(Ordering::Relaxed),
                spans_emitted: s.counters.spans_emitted.load(Ordering::Relaxed),
                permalink: s.permalink.lock().unwrap().clone(),
                last_error: s.last_error.lock().unwrap().clone(),
            })
            .collect();
        for (key, (source, error)) in self.auth_errors.lock().unwrap().iter() {
            if p.session_id
                .as_ref()
                .is_some_and(|want| want != &key.session_id)
            {
                continue;
            }
            if map.contains_key(key) {
                continue;
            }
            sessions.push(SessionStatus {
                session_id: key.session_id.clone(),
                source: source.clone(),
                route: serde_json::from_str(&key.route).ok(),
                queued: 0,
                spans_emitted: 0,
                permalink: None,
                last_error: Some(error.clone()),
            });
        }
        StatusResult {
            daemon_version: self.version.clone(),
            uptime_ms: self.started.elapsed().as_millis() as u64,
            sessions,
        }
    }
}

/// How long recovery state is kept on disk.
const RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);
/// How often that retention is enforced while the daemon keeps running.
const GC_INTERVAL: Duration = Duration::from_secs(60 * 60);

async fn collect_garbage(data_dir: &std::path::Path) {
    journal::gc_old_journals(data_dir, RETENTION).await;
    journal::gc_old_managed_runs(data_dir, RETENTION).await;
    crate::transcript_mirror::gc_old_mirrors(data_dir, RETENTION).await;
    gc_old_correlation_states(data_dir, RETENTION).await;
}

async fn gc_old_correlation_states(data_dir: &std::path::Path, max_age: Duration) {
    gc_old_correlation_dir(&data_dir.join("correlation"), max_age).await;
    gc_old_correlation_dir(&data_dir.join("correlation").join("parents"), max_age).await;
}

async fn gc_old_correlation_dir(dir: &std::path::Path, max_age: Duration) {
    let Ok(mut entries) = tokio::fs::read_dir(dir).await else {
        return;
    };
    let now = SystemTime::now();
    while let Ok(Some(entry)) = entries.next_entry().await {
        let old = entry
            .metadata()
            .await
            .ok()
            .and_then(|metadata| metadata.modified().ok())
            .and_then(|modified| now.duration_since(modified).ok())
            .is_some_and(|age| age > max_age);
        if old && entry.file_type().await.is_ok_and(|kind| kind.is_file()) {
            let _ = tokio::fs::remove_file(entry.path()).await;
        }
    }
}

/// Retire delivery pipelines that have gone quiet. Without this, every session
/// the daemon ever saw keeps its translator state, sink handles, and journal
/// file handle alive until the process exits — which for a continuously busy
/// user is never, since the idle watchdog needs *all* sessions quiet.
fn spawn_session_reaper(daemon: Arc<Daemon>, idle_timeout: Duration) {
    if idle_timeout.is_zero() {
        return; // 0 disables retirement (useful in tests)
    }
    tokio::spawn(async move {
        let tick = (idle_timeout / 4).max(Duration::from_secs(1));
        loop {
            tokio::select! {
                _ = daemon.shutdown.notified() => return,
                _ = tokio::time::sleep(tick) => {}
            }
            for key in daemon.idle_sessions(idle_timeout) {
                daemon.retire_session(&key).await;
            }
        }
    });
}

fn spawn_gc(daemon: Arc<Daemon>) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = daemon.shutdown.notified() => return,
                _ = tokio::time::sleep(GC_INTERVAL) => {}
            }
            collect_garbage(&daemon.data_dir).await;
        }
    });
}

fn spawn_idle_watchdog(daemon: Arc<Daemon>, idle_timeout: Duration) {
    if idle_timeout.is_zero() {
        return; // 0 disables the watchdog (useful in tests)
    }
    tokio::spawn(async move {
        let tick = (idle_timeout / 4).max(Duration::from_secs(1));
        loop {
            tokio::select! {
                _ = daemon.shutdown.notified() => return,
                _ = tokio::time::sleep(tick) => {}
            }
            let idle_for = daemon.last_activity.lock().unwrap().elapsed();
            if idle_for >= idle_timeout
                && daemon.total_queued() == 0
                && !daemon.correlation.has_any_active_tools()
            {
                tracing::info!("idle for {:?}; shutting down", idle_for);
                daemon.trigger_shutdown();
                return;
            }
        }
    });
}

async fn drain_all(daemon: &Arc<Daemon>) {
    daemon.settle_ingress().await;
    let sessions: Vec<Arc<Session>> = daemon.sessions.lock().unwrap().values().cloned().collect();
    for s in sessions {
        s.shutdown().await;
    }
}

/// Is a live daemon answering at the endpoint? Connect and expect any line
/// back from a well-formed `initialize`.
async fn probe_alive(endpoint: &std::path::Path) -> bool {
    let Ok(stream) = transport::connect(endpoint).await else {
        return false;
    };
    let (read_half, mut write_half) = tokio::io::split(stream);
    let init = Request::new(
        crate::wire::RequestId::Int(0),
        method::INITIALIZE,
        serde_json::json!({
            "protocol_version": PROTOCOL_VERSION,
            "client": { "source": "probe" }
        }),
    );
    let Ok(mut line) = Message::Request(init).to_line() else {
        return false;
    };
    line.push('\n');
    if write_half.write_all(line.as_bytes()).await.is_err() {
        return false;
    }
    let mut lines = BufReader::new(read_half).lines();
    matches!(
        tokio::time::timeout(Duration::from_secs(1), lines.next_line()).await,
        Ok(Ok(Some(_)))
    )
}
