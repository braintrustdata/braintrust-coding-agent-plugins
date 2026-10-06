//! The daemon: owns the session map + shared deps, binds the UDS listener,
//! serves JSON-RPC connections, and shuts down gracefully (idle timeout,
//! `daemon.shutdown`, or SIGINT/SIGTERM).

mod auth;
mod claude_transcripts;
mod correlation_store;
mod ingress;
mod maintenance;
mod managed_run;
mod recovery;
mod rpc;
mod sessions;

pub(crate) use auth::resolve_route_auth;
use claude_transcripts::*;
pub(crate) use correlation_store::persist_active_parent_snapshot;
use correlation_store::*;
use ingress::*;
use maintenance::*;
use recovery::*;
use rpc::*;
use sessions::*;

use crate::dispatch::{hydrate_transcript_reference, ReplayPlan, Session, SessionOptions};
use crate::journal::{self, JournalWriter};
use crate::sink::{BraintrustSinkConfig, BraintrustSinkFactory, DebugSinkFactory, SinkFactory};
use crate::translate::Registry;
use crate::transport::{self, Listener, ServerStream};
use crate::wire::{
    error_code, method, AuthDiagnoseParams, AuthDiagnoseResult, Capabilities, ClientInfo, Envelope,
    EventLogResult, FlushParams, FlushResult, InitializeParams, InitializeResult,
    ManagedRunFlushParams, Message, Request, Response, RpcError, SessionStatus, ShutdownResult,
    StatusParams, StatusResult, PROTOCOL_VERSION,
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
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
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

// `async_trait` marks its boxed futures as `must_use`; Clippy 1.99 flags that
// generated annotation as redundant on async trait methods.
#[allow(clippy::double_must_use)]
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
    claude_transcripts: Mutex<HashMap<DeliveryKey, ClaudeTranscriptObservation>>,
    managed_run_sessions: Mutex<HashMap<String, HashSet<DeliveryKey>>>,
    auth_errors: Mutex<HashMap<DeliveryKey, (String, String)>>,
    sessions: Mutex<HashMap<DeliveryKey, Arc<Session>>>,
    correlation: Arc<crate::correlation::CorrelationRegistry>,
    automatic_links: Mutex<HashMap<String, SessionRoute>>,
    standalone_links: Mutex<HashSet<String>>,
    pending_sessions: Mutex<HashMap<String, PendingSession>>,
    pending_reconcile_lock: tokio::sync::Mutex<()>,
    correlation_locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    correlation_changed: Arc<Notify>,
    ingress_tx: mpsc::Sender<IngressMsg>,
    ingress_overflow: AtomicBool,
    ingress_dispatched: Mutex<HashMap<DeliveryKey, u64>>,
    started: Instant,
    last_activity: Mutex<Instant>,
    /// Prevents a shutdown drain from racing capture that has passed the
    /// quiescing check but has not yet appended and queued its event.
    capture_gate: tokio::sync::RwLock<()>,
    quiescing: AtomicBool,
    drained: tokio::sync::Mutex<bool>,
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
            claude_transcripts: Mutex::new(HashMap::new()),
            managed_run_sessions: Mutex::new(HashMap::new()),
            auth_errors: Mutex::new(HashMap::new()),
            sessions: Mutex::new(HashMap::new()),
            correlation: Arc::new(crate::correlation::CorrelationRegistry::default()),
            automatic_links: Mutex::new(HashMap::new()),
            standalone_links: Mutex::new(HashSet::new()),
            pending_sessions: Mutex::new(HashMap::new()),
            pending_reconcile_lock: tokio::sync::Mutex::new(()),
            correlation_locks: Mutex::new(HashMap::new()),
            correlation_changed: Arc::new(Notify::new()),
            ingress_tx,
            ingress_overflow: AtomicBool::new(false),
            ingress_dispatched: Mutex::new(HashMap::new()),
            started: Instant::now(),
            last_activity: Mutex::new(Instant::now()),
            capture_gate: tokio::sync::RwLock::new(()),
            quiescing: AtomicBool::new(false),
            drained: tokio::sync::Mutex::new(false),
            shutting_down: AtomicBool::new(false),
            shutdown: Notify::new(),
        });
        spawn_ingress_worker(daemon.clone(), ingress_rx);
        daemon
    }

    fn touch(&self) {
        *self.last_activity.lock().unwrap() = Instant::now();
    }

    /// Queued work, excluding pending sessions whose handoff failed. Those are
    /// durable on disk and resume after a restart, so they must not keep an
    /// otherwise idle daemon alive forever.
    fn total_queued(&self) -> u64 {
        let dispatched: u64 = self
            .sessions
            .lock()
            .unwrap()
            .values()
            .map(|s| s.counters.queued.load(Ordering::Relaxed))
            .sum();
        dispatched.saturating_add(
            self.pending_sessions
                .lock()
                .unwrap()
                .values()
                .filter(|state| !state.delivery_failed)
                .map(|state| state.events.len() as u64)
                .sum(),
        )
    }

    fn trigger_shutdown(&self) {
        self.quiescing.store(true, Ordering::SeqCst);
        self.shutting_down.store(true, Ordering::SeqCst);
        self.shutdown.notify_waiters();
    }

    fn begin_quiesce(&self) {
        self.quiescing.store(true, Ordering::SeqCst);
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
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
    spawn_pending_reconciler(daemon.clone());
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

/// Run the daemon until shutdown.
pub async fn run_serve(args: ServeArgs, opts: ServeOptions) -> anyhow::Result<()> {
    run(args, opts).await
}

/// Build debug [`ServeOptions`]: the production translator registry plus a
/// debug sink writing NDJSON under `<data_dir>/spans/`.
pub fn debug_serve_options(version: impl Into<String>, data_dir: &std::path::Path) -> ServeOptions {
    ServeOptions {
        version: version.into(),
        translators: Arc::new(Registry::default_agents()),
        sink_factory: Arc::new(DebugSinkFactory {
            dir: data_dir.join("spans"),
        }),
        auth_provider: None,
    }
}

/// Build [`ServeOptions`] with the Braintrust sink. `translators` lets the
/// caller choose the translator registry (debug-only until Phase 3 adds the
/// Codex/Claude translators). Clients are built lazily per session URL, so this
/// is cheap and infallible.
pub fn braintrust_serve_options(
    version: impl Into<String>,
    sink_config: BraintrustSinkConfig,
    translators: Arc<Registry>,
) -> ServeOptions {
    ServeOptions {
        version: version.into(),
        translators,
        sink_factory: Arc::new(BraintrustSinkFactory::new(sink_config)),
        auth_provider: None,
    }
}

#[cfg(test)]
mod tests {
    use super::{client_may_shutdown, is_session_start, ClientInfo};

    #[test]
    fn antigravity_invocation_refreshes_a_resumed_conversations_process() {
        assert!(is_session_start("antigravity", "PreInvocation"));
        assert!(!is_session_start("claude-code", "PreInvocation"));
        assert!(is_session_start("claude-code", "SessionStart"));
        assert!(is_session_start("pi", "session_start"));
        assert!(is_session_start("opencode", "session.created"));
    }

    fn initialized(version: Option<&str>) -> ClientInfo {
        ClientInfo {
            source: "codex".into(),
            daemon_version: version.map(str::to_string),
            plugin_version: None,
            pid: None,
        }
    }

    #[test]
    fn legacy_initialized_clients_cannot_downgrade_the_daemon() {
        assert!(client_may_shutdown(None, "0.20.0"));
        assert!(!client_may_shutdown(Some(&initialized(None)), "0.20.0"));
        assert!(!client_may_shutdown(
            Some(&initialized(Some("0.19.3"))),
            "0.20.0"
        ));
        assert!(client_may_shutdown(
            Some(&initialized(Some("0.20.0"))),
            "0.20.0"
        ));
        assert!(client_may_shutdown(
            Some(&initialized(Some("0.21.0"))),
            "0.20.0"
        ));
    }
}
