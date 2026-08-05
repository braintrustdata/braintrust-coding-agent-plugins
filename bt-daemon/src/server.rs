//! The daemon: owns the session map + shared deps, binds the UDS listener,
//! serves JSON-RPC connections, and shuts down gracefully (idle timeout,
//! `daemon.shutdown`, or SIGINT/SIGTERM).

use crate::dispatch::Session;
use crate::journal::{self, JournalWriter};
use crate::sink::SinkFactory;
use crate::translate::Registry;
use crate::transport::{self, Listener, ServerStream};
use crate::wire::{
    error_code, method, Capabilities, Envelope, EventLogResult, FlushParams, FlushResult,
    InitializeParams, InitializeResult, Message, Request, Response, RpcError, SessionStatus,
    ShutdownResult, StatusParams, StatusResult, PROTOCOL_VERSION,
};
use crate::wire::{AuthSelection, BackendAuth, SessionRoute};
use crate::{paths, ServeArgs};
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::Notify;

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

/// A live credential lease. Only the canonical profile name is observable;
/// the backend credential remains in daemon memory and is never journaled.
#[derive(Debug, Clone)]
pub struct AuthLease {
    pub profile: String,
    pub auth: BackendAuth,
    /// Epoch milliseconds. `None` is appropriate for non-expiring API keys.
    pub expires_at_ms: Option<i64>,
}

#[async_trait]
pub trait AuthProvider: Send + Sync {
    /// Resolve without prompting. On refresh, `selection.profile` is the
    /// canonical profile returned by the initial lease, keeping an active
    /// session pinned even if the user's default profile changes.
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
    session_auth: tokio::sync::Mutex<HashMap<String, SessionAuthState>>,
    auth_errors: Mutex<HashMap<String, (String, String)>>,
    sessions: Mutex<HashMap<String, Arc<Session>>>,
    started: Instant,
    last_activity: Mutex<Instant>,
    shutting_down: AtomicBool,
    shutdown: Notify,
}

impl Daemon {
    fn new(opts: ServeOptions, data_dir: PathBuf) -> Arc<Self> {
        Arc::new(Daemon {
            version: opts.version,
            data_dir,
            translators: opts.translators,
            sink_factory: opts.sink_factory,
            auth_provider: opts.auth_provider,
            session_auth: tokio::sync::Mutex::new(HashMap::new()),
            auth_errors: Mutex::new(HashMap::new()),
            sessions: Mutex::new(HashMap::new()),
            started: Instant::now(),
            last_activity: Mutex::new(Instant::now()),
            shutting_down: AtomicBool::new(false),
            shutdown: Notify::new(),
        })
    }

    async fn configure_event(&self, env: &mut Envelope) -> anyhow::Result<()> {
        let Some(provider) = &self.auth_provider else {
            anyhow::bail!("daemon host has no Braintrust auth provider");
        };
        let route = env
            .route
            .clone()
            .ok_or_else(|| anyhow::anyhow!("event is missing its session route"))?;
        if route.destination.is_none() {
            anyhow::bail!(
                "session route is missing its trace destination; select a project or destination during `bt trace setup` or `bt trace run`"
            );
        }
        let (selection, reason, expected_profile) = {
            let states = self.session_auth.lock().await;
            match states.get(&env.session_id) {
                Some(state) => {
                    if !state.route.same_route(&route) {
                        anyhow::bail!(
                            "session route changed after initialization; start a new agent session to change profile, organization, or destination"
                        );
                    }
                    if !lease_is_expiring(&state.lease) {
                        env.config = Some(state.route.with_auth(state.lease.auth.clone()));
                        return Ok(());
                    }
                    (
                        AuthSelection {
                            profile: Some(state.lease.profile.clone()),
                            org_name: state.lease.auth.org_name.clone(),
                        },
                        AuthResolveReason::Expiring,
                        Some(state.lease.profile.clone()),
                    )
                }
                None => (route.auth.clone(), AuthResolveReason::Initial, None),
            }
        };

        let lease = provider.resolve(&selection, reason).await.map_err(|error| {
            let message = format!(
                "could not resolve Braintrust profile for {}: {error}; run `bt auth login` or select a profile explicitly",
                env.source
            );
            self.auth_errors.lock().unwrap().insert(
                env.session_id.clone(),
                (env.source.clone(), message.clone()),
            );
            anyhow::anyhow!(message)
        })?;
        if let Some(expected) = expected_profile {
            if lease.profile != expected {
                anyhow::bail!(
                    "credential refresh changed profile from {expected:?} to {:?}",
                    lease.profile
                );
            }
        }
        if let Some(expected_org) = route.auth.org_name.as_deref() {
            if lease.auth.org_name.as_deref() != Some(expected_org) {
                anyhow::bail!(
                    "profile {:?} resolved organization {:?}, expected {:?}",
                    lease.profile,
                    lease.auth.org_name,
                    expected_org
                );
            }
        }

        env.config = Some(route.with_auth(lease.auth.clone()));
        self.session_auth
            .lock()
            .await
            .insert(env.session_id.clone(), SessionAuthState { route, lease });
        self.auth_errors.lock().unwrap().remove(&env.session_id);
        Ok(())
    }

    async fn refresh_session_before_flush(&self, session_id: &str) -> anyhow::Result<()> {
        let Some(provider) = &self.auth_provider else {
            return Ok(());
        };
        let Some(state) = self.session_auth.lock().await.get(session_id).cloned() else {
            return Ok(());
        };
        if !lease_is_expiring(&state.lease) {
            return Ok(());
        }
        let selection = AuthSelection {
            profile: Some(state.lease.profile.clone()),
            org_name: state.lease.auth.org_name.clone(),
        };
        let lease = provider
            .resolve(&selection, AuthResolveReason::Expiring)
            .await?;
        if lease.profile != state.lease.profile {
            anyhow::bail!("credential refresh changed the session profile");
        }
        let config = state.route.with_auth(lease.auth.clone());
        self.session_auth.lock().await.insert(
            session_id.to_string(),
            SessionAuthState {
                route: state.route,
                lease,
            },
        );
        let session = { self.sessions.lock().unwrap().get(session_id).cloned() };
        if let Some(session) = session {
            session.configure(config).await?;
        }
        Ok(())
    }

    fn touch(&self) {
        *self.last_activity.lock().unwrap() = Instant::now();
    }

    async fn session_for(&self, env: &Envelope) -> anyhow::Result<Arc<Session>> {
        {
            let map = self.sessions.lock().unwrap();
            if let Some(s) = map.get(&env.session_id) {
                return Ok(s.clone());
            }
        }
        // Open the journal outside the lock (async I/O), then insert under it,
        // resolving a race where two connections create the same session.
        let replay =
            match journal::read_journal(&journal::journal_path(&self.data_dir, &env.session_id))
                .await
            {
                Ok(entries) => entries
                    .into_iter()
                    .map(journal::envelope_from_redacted)
                    .collect(),
                Err(e)
                    if e.downcast_ref::<std::io::Error>()
                        .is_some_and(|io| io.kind() == std::io::ErrorKind::NotFound) =>
                {
                    Vec::new()
                }
                Err(e) => {
                    tracing::warn!(session_id = %env.session_id, "journal replay skipped: {e}");
                    Vec::new()
                }
            };
        let journal = JournalWriter::open(&self.data_dir, &env.session_id).await?;
        let mut map = self.sessions.lock().unwrap();
        if let Some(s) = map.get(&env.session_id) {
            return Ok(s.clone());
        }
        let session = Session::spawn(
            env.session_id.clone(),
            env.source.clone(),
            journal,
            replay,
            self.translators.clone(),
            self.sink_factory.clone(),
        );
        map.insert(env.session_id.clone(), session.clone());
        Ok(session)
    }

    fn total_queued(&self) -> u64 {
        self.sessions
            .lock()
            .unwrap()
            .values()
            .map(|s| s.counters.queued.load(Ordering::Relaxed))
            .sum()
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
    journal::gc_old_journals(&daemon.data_dir, Duration::from_secs(7 * 24 * 60 * 60)).await;
    let idle_timeout = Duration::from_secs(args.idle_timeout_secs);
    spawn_idle_watchdog(daemon.clone(), idle_timeout);

    let accept_result = accept_loop(daemon.clone(), listener).await;

    // Graceful drain regardless of why we stopped.
    drain_all(&daemon).await;
    transport::cleanup(&socket);
    accept_result
}

async fn accept_loop(daemon: Arc<Daemon>, mut listener: Listener) -> anyhow::Result<()> {
    loop {
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
                let response = handle_request(&daemon, req).await;
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
                            Ok(env) => {
                                let _ = accept_event(&daemon, env).await;
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

async fn accept_event(daemon: &Arc<Daemon>, mut env: Envelope) -> Result<(), String> {
    let source = env.source.clone();
    let event = env.event.clone();
    let session_id = env.session_id.clone();
    tracing::info!(source, event, session_id, "event received");
    daemon.touch();

    let result = async {
        daemon
            .configure_event(&mut env)
            .await
            .map_err(|error| format!("session auth failed: {error}"))?;
        let session = daemon
            .session_for(&env)
            .await
            .map_err(|error| format!("session init failed: {error}"))?;
        session
            .append_and_enqueue(env)
            .await
            .map_err(|error| format!("enqueue failed: {error}"))
    }
    .await;

    match &result {
        Ok(()) => tracing::info!(source, event, session_id, "event accepted"),
        Err(error) => tracing::warn!(source, event, session_id, error, "event rejected"),
    }
    result
}

async fn handle_request(daemon: &Arc<Daemon>, req: Request) -> Response {
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
            let env = parse!(Envelope);
            match accept_event(daemon, env).await {
                Ok(()) => Response::ok(
                    id,
                    serde_json::to_value(EventLogResult { accepted: true }).unwrap(),
                ),
                Err(error) => Response::err(id, RpcError::new(error_code::INTERNAL, error)),
            }
        }
        method::SESSION_FLUSH => {
            let p = parse!(FlushParams);
            if let Err(error) = daemon.refresh_session_before_flush(&p.session_id).await {
                return Response::err(
                    id,
                    RpcError::new(
                        error_code::INTERNAL,
                        format!("session auth refresh failed: {error}"),
                    ),
                );
            }
            let session = { daemon.sessions.lock().unwrap().get(&p.session_id).cloned() };
            let (flushed, pending) = match session {
                Some(s) => s.flush(Duration::from_millis(p.timeout_ms)).await,
                None => (true, 0),
            };
            Response::ok(
                id,
                serde_json::to_value(FlushResult { flushed, pending }).unwrap(),
            )
        }
        method::STATUS_GET => {
            let p = parse!(StatusParams);
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
            .filter(|(sid, _)| p.session_id.as_ref().is_none_or(|want| *want == **sid))
            .map(|(sid, s)| SessionStatus {
                session_id: sid.clone(),
                source: s.source.clone(),
                queued: s.counters.queued.load(Ordering::Relaxed),
                spans_emitted: s.counters.spans_emitted.load(Ordering::Relaxed),
                permalink: s.permalink.lock().unwrap().clone(),
                last_error: s.last_error.lock().unwrap().clone(),
            })
            .collect();
        for (session_id, (source, error)) in self.auth_errors.lock().unwrap().iter() {
            if p.session_id.as_ref().is_some_and(|want| want != session_id) {
                continue;
            }
            if map.contains_key(session_id) {
                continue;
            }
            sessions.push(SessionStatus {
                session_id: session_id.clone(),
                source: source.clone(),
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
            if idle_for >= idle_timeout && daemon.total_queued() == 0 {
                tracing::info!("idle for {:?}; shutting down", idle_for);
                daemon.trigger_shutdown();
                return;
            }
        }
    });
}

async fn drain_all(daemon: &Arc<Daemon>) {
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
