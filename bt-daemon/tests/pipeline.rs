//! Phase 1 end-to-end: hook client → UDS → dispatch → journal → translate →
//! debug sink. Runs the daemon in-process on a temp socket (no process
//! spawning, so it's deterministic).

use async_trait::async_trait;
use bt_daemon::wire::{
    AuthSelection, AuthSource, BackendAuth, Envelope, SessionConfig, SessionRoute,
};
use bt_daemon::{
    debug_serve_options, flush_managed_run, flush_session, forward_envelope, run_serve, run_status,
    shutdown_daemon, source_journal_path, AuthLease, AuthProvider, AuthResolveReason, HostInfo,
    Registry, ServeArgs, ServeOptions, Sink, SinkFactory, SpanOp, StatusArgs,
};
#[cfg(all(feature = "cli", unix))]
use bt_daemon::{run_traced, RunArgs, RunHookCommand, RunSource};
use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

struct TrackingSinkFactory {
    flushes: Arc<Mutex<HashMap<String, usize>>>,
}

impl SinkFactory for TrackingSinkFactory {
    fn create(
        &self,
        session_id: &str,
        _source: &str,
        _plugin_version: Option<&str>,
    ) -> anyhow::Result<Box<dyn Sink>> {
        Ok(Box::new(TrackingSink {
            session_id: session_id.to_string(),
            flushes: self.flushes.clone(),
        }))
    }
}

struct TrackingSink {
    session_id: String,
    flushes: Arc<Mutex<HashMap<String, usize>>>,
}

struct SlowSink;

#[async_trait]
impl Sink for SlowSink {
    fn configure(&mut self, _config: &SessionConfig) {}
    async fn emit(&mut self, ops: &[SpanOp]) -> anyhow::Result<u64> {
        tokio::time::sleep(Duration::from_millis(500)).await;
        Ok(ops.len() as u64)
    }
    async fn flush(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

struct SlowSinkFactory;

impl SinkFactory for SlowSinkFactory {
    fn create(&self, _: &str, _: &str, _: Option<&str>) -> anyhow::Result<Box<dyn Sink>> {
        Ok(Box::new(SlowSink))
    }
}

struct GateSinkFactory {
    blocked: Arc<std::sync::atomic::AtomicBool>,
    gate: Arc<tokio::sync::Notify>,
    emitted: Arc<std::sync::atomic::AtomicU64>,
}

struct GateSink {
    blocked: Arc<std::sync::atomic::AtomicBool>,
    gate: Arc<tokio::sync::Notify>,
    emitted: Arc<std::sync::atomic::AtomicU64>,
}

#[async_trait]
impl Sink for GateSink {
    async fn emit(&mut self, ops: &[SpanOp]) -> anyhow::Result<u64> {
        if self
            .blocked
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            self.gate.notified().await;
        }
        self.emitted
            .fetch_add(ops.len() as u64, std::sync::atomic::Ordering::Relaxed);
        Ok(ops.len() as u64)
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

impl SinkFactory for GateSinkFactory {
    fn create(&self, _: &str, _: &str, _: Option<&str>) -> anyhow::Result<Box<dyn Sink>> {
        Ok(Box::new(GateSink {
            blocked: self.blocked.clone(),
            gate: self.gate.clone(),
            emitted: self.emitted.clone(),
        }))
    }
}

#[async_trait]
impl Sink for TrackingSink {
    fn configure(&mut self, _config: &SessionConfig) {}

    async fn emit(&mut self, ops: &[SpanOp]) -> anyhow::Result<u64> {
        Ok(ops.len() as u64)
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        *self
            .flushes
            .lock()
            .unwrap()
            .entry(self.session_id.clone())
            .or_default() += 1;
        Ok(())
    }
}

/// One observed delivery pipeline: the resolved route configuration plus the
/// span activity delivered through it.
#[derive(Default)]
struct RouteSinkRecord {
    org: Mutex<Option<String>>,
    destination: Mutex<Option<bt_daemon::wire::TraceDestination>>,
    emitted: std::sync::atomic::AtomicU64,
    flushes: std::sync::atomic::AtomicU64,
    ops: Mutex<Vec<SpanOp>>,
}

#[derive(Default)]
struct RouteRecordingSinkFactory {
    sinks: Mutex<Vec<Arc<RouteSinkRecord>>>,
}

impl SinkFactory for RouteRecordingSinkFactory {
    fn create(
        &self,
        _session_id: &str,
        _source: &str,
        _plugin_version: Option<&str>,
    ) -> anyhow::Result<Box<dyn Sink>> {
        let record = Arc::new(RouteSinkRecord::default());
        self.sinks.lock().unwrap().push(record.clone());
        Ok(Box::new(RouteRecordingSink { record }))
    }
}

struct RouteRecordingSink {
    record: Arc<RouteSinkRecord>,
}

#[async_trait]
impl Sink for RouteRecordingSink {
    fn configure(&mut self, config: &SessionConfig) {
        *self.record.org.lock().unwrap() = config.auth.org_name.clone();
        *self.record.destination.lock().unwrap() = config.destination.clone();
    }

    async fn emit(&mut self, ops: &[SpanOp]) -> anyhow::Result<u64> {
        self.record.ops.lock().unwrap().extend_from_slice(ops);
        self.record
            .emitted
            .fetch_add(ops.len() as u64, std::sync::atomic::Ordering::Relaxed);
        Ok(ops.len() as u64)
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        self.record
            .flushes
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
}

fn dummy_host() -> HostInfo {
    // The daemon is started in-process, so the client never spawns; serve_argv
    // is unused but must be non-empty.
    HostInfo {
        serve_argv: vec![OsString::from("unused")],
        version: "test".into(),
    }
}

fn envelope(session_id: &str, event: &str, ts_ms: i64) -> Envelope {
    Envelope {
        source: "debug".into(),
        source_version: Some("0.0.0".into()),
        plugin_version: None,
        session_id: session_id.into(),
        event: event.into(),
        ts_ms,
        managed_run_id: None,
        payload: serde_json::json!({ "session_id": session_id, "hook_event_name": event, "n": ts_ms }),
        route: Some(SessionRoute {
            destination: Some(bt_daemon::wire::TraceDestination::ProjectLogs {
                project_id: None,
                project_name: Some("codex".into()),
            }),
            ..SessionRoute::default()
        }),
        config: None,
        capture: None,
    }
}

fn routed_envelope(session_id: &str, profile: &str, org: &str, event: &str) -> Envelope {
    let mut env = envelope(session_id, event, 1);
    env.route = Some(SessionRoute {
        auth: AuthSelection {
            source: AuthSource::SavedProfile,
            profile_id: None,
            profile: Some(profile.into()),
            org_name: Some(org.into()),
        },
        destination: Some(bt_daemon::wire::TraceDestination::ProjectLogs {
            project_id: None,
            project_name: Some(format!("{profile}-traces")),
        }),
        ..SessionRoute::default()
    });
    env
}

fn environment_routed_envelope(session_id: &str, org: &str, event: &str) -> Envelope {
    let mut env = envelope(session_id, event, 1);
    env.route = Some(SessionRoute {
        auth: AuthSelection {
            source: AuthSource::Environment,
            profile_id: None,
            profile: None,
            org_name: Some(org.into()),
        },
        destination: Some(bt_daemon::wire::TraceDestination::ProjectLogs {
            project_id: None,
            project_name: Some("environment-traces".into()),
        }),
        ..SessionRoute::default()
    });
    env
}

struct TestAuthProvider {
    calls: Mutex<Vec<(AuthSelection, AuthResolveReason)>>,
    fail: bool,
    first_lease_expired: bool,
}

#[async_trait]
impl AuthProvider for TestAuthProvider {
    async fn resolve(
        &self,
        selection: &AuthSelection,
        reason: AuthResolveReason,
    ) -> anyhow::Result<AuthLease> {
        let mut calls = self.calls.lock().unwrap();
        calls.push((selection.clone(), reason));
        let call_index = calls.len();
        drop(calls);
        if self.fail {
            anyhow::bail!("profile credential unavailable")
        }
        let canonical = selection.clone().canonicalized()?;
        let credential_name = canonical
            .profile
            .clone()
            .unwrap_or_else(|| "environment".into());
        Ok(AuthLease {
            selection: canonical.clone(),
            auth: BackendAuth {
                token: format!("secret-{credential_name}-{call_index}"),
                api_url: Some(format!("https://{credential_name}.example.test")),
                app_url: None,
                // Model a profile with a default organization for selections
                // that do not constrain one.
                org_name: selection
                    .org_name
                    .clone()
                    .or_else(|| Some(format!("{credential_name}-org"))),
                org_id: Some(format!("org-{credential_name}")),
            },
            expires_at_ms: (self.first_lease_expired && call_index == 1).then_some(0),
        })
    }
}

async fn start_routed_daemon(
    provider: Arc<TestAuthProvider>,
) -> (
    PathBuf,
    PathBuf,
    tokio::task::JoinHandle<()>,
    tempfile::TempDir,
) {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("data");
    let socket = test_endpoint(tmp.path());
    let args = ServeArgs {
        socket: Some(socket.clone()),
        data_dir: Some(data_dir.clone()),
        idle_timeout_secs: 0,
        session_idle_timeout_secs: 0,
    };
    let mut opts = debug_serve_options("test", &data_dir);
    opts.auth_provider = Some(provider);
    let handle = tokio::spawn(async move {
        let _ = run_serve(args, opts).await;
    });
    wait_for(&socket).await;
    (data_dir, socket, handle, tmp)
}

fn test_endpoint(tmp: &Path) -> PathBuf {
    #[cfg(unix)]
    {
        tmp.join("d.sock")
    }
    #[cfg(windows)]
    {
        let _ = tmp;
        PathBuf::from(format!(r"\\.\pipe\bt-daemon-test-{}", uuid::Uuid::new_v4()))
    }
}

async fn start_daemon_with(
    provider: Arc<TestAuthProvider>,
    sink_factory: Arc<RouteRecordingSinkFactory>,
) -> (
    PathBuf,
    PathBuf,
    tokio::task::JoinHandle<()>,
    tempfile::TempDir,
) {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("data");
    let socket = test_endpoint(tmp.path());
    let handle =
        start_daemon_at_with(data_dir.clone(), socket.clone(), provider, sink_factory).await;
    (data_dir, socket, handle, tmp)
}

async fn start_daemon_at_with(
    data_dir: PathBuf,
    socket: PathBuf,
    provider: Arc<TestAuthProvider>,
    sink_factory: Arc<RouteRecordingSinkFactory>,
) -> tokio::task::JoinHandle<()> {
    let args = ServeArgs {
        socket: Some(socket.clone()),
        data_dir: Some(data_dir),
        idle_timeout_secs: 0,
        session_idle_timeout_secs: 0,
    };
    let opts = ServeOptions {
        version: "test".to_string(),
        translators: Arc::new(Registry::default_agents()),
        sink_factory,
        auth_provider: Some(provider),
    };
    let handle = tokio::spawn(async move {
        let _ = run_serve(args, opts).await;
    });
    wait_for(&socket).await;
    handle
}

async fn wait_for(endpoint: &Path) {
    for _ in 0..200 {
        if let Ok(Some(_)) = run_status(StatusArgs {
            socket: Some(endpoint.to_path_buf()),
            session_id: None,
        })
        .await
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("daemon never answered at: {}", endpoint.display());
}

async fn wait_until_gone(endpoint: &Path) {
    for _ in 0..1000 {
        match run_status(StatusArgs {
            socket: Some(endpoint.to_path_buf()),
            session_id: None,
        })
        .await
        {
            Ok(Some(_)) => {}
            // A Windows named pipe can still accept a client while the
            // daemon is unwinding, then close before answering initialize.
            // Either that or an unavailable endpoint means it is no longer
            // serving status requests.
            Ok(None) | Err(_) => return,
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("daemon still answered at: {}", endpoint.display());
}

/// Start an in-process daemon on a fresh temp socket/data dir. Returns
/// (data_dir, socket, serve task handle, tempdir guard).
async fn start_daemon() -> (
    PathBuf,
    PathBuf,
    tokio::task::JoinHandle<()>,
    tempfile::TempDir,
) {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("data");
    let socket = test_endpoint(tmp.path());
    std::fs::create_dir_all(&data_dir).unwrap();

    let args = ServeArgs {
        socket: Some(socket.clone()),
        data_dir: Some(data_dir.clone()),
        idle_timeout_secs: 0, // disable the watchdog for the test
        session_idle_timeout_secs: 0,
    };
    let mut opts = debug_serve_options("test", &data_dir);
    opts.auth_provider = Some(Arc::new(TestAuthProvider {
        calls: Mutex::new(Vec::new()),
        fail: false,
        first_lease_expired: false,
    }));
    let handle = tokio::spawn(async move {
        let _ = run_serve(args, opts).await;
    });
    wait_for(&socket).await;
    (data_dir, socket, handle, tmp)
}

async fn start_tracking_daemon(
    version: &str,
) -> (
    PathBuf,
    tokio::task::JoinHandle<()>,
    Arc<Mutex<HashMap<String, usize>>>,
    tempfile::TempDir,
) {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("data");
    let socket = test_endpoint(tmp.path());
    let flushes = Arc::new(Mutex::new(HashMap::new()));
    let opts = ServeOptions {
        version: version.to_string(),
        translators: Arc::new(Registry::default_agents()),
        sink_factory: Arc::new(TrackingSinkFactory {
            flushes: flushes.clone(),
        }),
        auth_provider: Some(Arc::new(TestAuthProvider {
            calls: Mutex::new(Vec::new()),
            fail: false,
            first_lease_expired: false,
        })),
    };
    let args = ServeArgs {
        socket: Some(socket.clone()),
        data_dir: Some(data_dir),
        idle_timeout_secs: 0,
        session_idle_timeout_secs: 0,
    };
    let handle = tokio::spawn(async move {
        let _ = run_serve(args, opts).await;
    });
    wait_for(&socket).await;
    (socket, handle, flushes, tmp)
}

async fn start_slow_daemon() -> (PathBuf, tokio::task::JoinHandle<()>, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("data");
    let socket = test_endpoint(tmp.path());
    let opts = ServeOptions {
        version: "test".into(),
        translators: Arc::new(Registry::default_agents()),
        sink_factory: Arc::new(SlowSinkFactory),
        auth_provider: Some(Arc::new(TestAuthProvider {
            calls: Mutex::new(Vec::new()),
            fail: false,
            first_lease_expired: false,
        })),
    };
    let args = ServeArgs {
        socket: Some(socket.clone()),
        data_dir: Some(data_dir),
        idle_timeout_secs: 0,
        session_idle_timeout_secs: 0,
    };
    let handle = tokio::spawn(async move {
        let _ = run_serve(args, opts).await;
    });
    wait_for(&socket).await;
    (socket, handle, tmp)
}

async fn start_gated_daemon(
    gate: Arc<tokio::sync::Notify>,
    emitted: Arc<std::sync::atomic::AtomicU64>,
) -> (
    PathBuf,
    PathBuf,
    tokio::task::JoinHandle<()>,
    tempfile::TempDir,
) {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("data");
    let socket = test_endpoint(tmp.path());
    let opts = ServeOptions {
        version: "test".into(),
        translators: Arc::new(Registry::default_agents()),
        sink_factory: Arc::new(GateSinkFactory {
            blocked: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            gate,
            emitted,
        }),
        auth_provider: Some(Arc::new(TestAuthProvider {
            calls: Mutex::new(Vec::new()),
            fail: false,
            first_lease_expired: false,
        })),
    };
    let args = ServeArgs {
        socket: Some(socket.clone()),
        data_dir: Some(data_dir.clone()),
        idle_timeout_secs: 0,
        session_idle_timeout_secs: 0,
    };
    let handle = tokio::spawn(async move {
        let _ = run_serve(args, opts).await;
    });
    wait_for(&socket).await;
    (data_dir, socket, handle, tmp)
}

async fn start_daemon_at(data_dir: PathBuf, socket: PathBuf) -> tokio::task::JoinHandle<()> {
    let args = ServeArgs {
        socket: Some(socket.clone()),
        data_dir: Some(data_dir.clone()),
        idle_timeout_secs: 0,
        session_idle_timeout_secs: 0,
    };
    let mut opts = debug_serve_options("test", &data_dir);
    opts.auth_provider = Some(Arc::new(TestAuthProvider {
        calls: Mutex::new(Vec::new()),
        fail: false,
        first_lease_expired: false,
    }));
    let handle = tokio::spawn(async move {
        let _ = run_serve(args, opts).await;
    });
    wait_for(&socket).await;
    handle
}

async fn shutdown(socket: &Path) {
    shutdown_daemon(socket).await.unwrap();
}

#[tokio::test]
async fn routed_sessions_resolve_multiple_profiles_without_journaling_credentials() {
    let provider = Arc::new(TestAuthProvider {
        calls: Mutex::new(Vec::new()),
        fail: false,
        first_lease_expired: false,
    });
    let (data_dir, socket, handle, _tmp) = start_routed_daemon(provider.clone()).await;
    let host = dummy_host();

    for (session, profile, org) in [
        ("work-session", "work", "work-org"),
        ("personal-session", "personal", "personal-org"),
    ] {
        forward_envelope(
            &routed_envelope(session, profile, org, "SessionStart"),
            &socket,
            &host,
            false,
        )
        .await
        .unwrap();
        flush_session(session, &socket, 5000).await.unwrap();
    }

    let calls = provider.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].0.profile.as_deref(), Some("work"));
    assert_eq!(calls[0].0.org_name.as_deref(), Some("work-org"));
    assert_eq!(calls[0].1, AuthResolveReason::Initial);
    assert_eq!(calls[1].0.profile.as_deref(), Some("personal"));
    assert_eq!(calls[1].0.org_name.as_deref(), Some("personal-org"));
    assert_eq!(calls[1].1, AuthResolveReason::Initial);

    for session in ["work-session", "personal-session"] {
        let journal =
            std::fs::read_to_string(source_journal_path(&data_dir, "debug", session)).unwrap();
        assert!(journal.contains("\"route\""));
        assert!(!journal.contains("secret-"));
        assert!(!journal.contains("token_sha256_prefix"));
        assert!(!journal.contains("\"config\""));
    }

    shutdown(&socket).await;
    handle.await.unwrap();
}

#[tokio::test]
async fn environment_routes_remain_environment_auth_without_journaling_credentials() {
    let provider = Arc::new(TestAuthProvider {
        calls: Mutex::new(Vec::new()),
        fail: false,
        first_lease_expired: false,
    });
    let (data_dir, socket, handle, _tmp) = start_routed_daemon(provider.clone()).await;
    let host = dummy_host();

    forward_envelope(
        &environment_routed_envelope("environment-session", "env-org", "SessionStart"),
        &socket,
        &host,
        false,
    )
    .await
    .unwrap();
    flush_session("environment-session", &socket, 5000)
        .await
        .unwrap();

    let calls = provider.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0.source, AuthSource::Environment);
    assert_eq!(calls[0].0.profile, None);
    assert_eq!(calls[0].0.org_name.as_deref(), Some("env-org"));

    let journal = std::fs::read_to_string(source_journal_path(
        &data_dir,
        "debug",
        "environment-session",
    ))
    .unwrap();
    assert!(journal.contains(r#""source":"environment""#));
    assert!(!journal.contains("secret-environment"));

    shutdown(&socket).await;
    handle.await.unwrap();
}

#[tokio::test]
async fn expiring_profile_lease_is_refreshed_for_the_pinned_profile() {
    let provider = Arc::new(TestAuthProvider {
        calls: Mutex::new(Vec::new()),
        fail: false,
        first_lease_expired: true,
    });
    let (_data_dir, socket, handle, _tmp) = start_routed_daemon(provider.clone()).await;
    let host = dummy_host();

    forward_envelope(
        &routed_envelope("refresh", "work", "work-org", "SessionStart"),
        &socket,
        &host,
        false,
    )
    .await
    .unwrap();
    forward_envelope(
        &routed_envelope("refresh", "work", "work-org", "Stop"),
        &socket,
        &host,
        false,
    )
    .await
    .unwrap();
    flush_session("refresh", &socket, 5000).await.unwrap();

    let calls = provider.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].1, AuthResolveReason::Initial);
    assert_eq!(calls[1].1, AuthResolveReason::Expiring);
    assert_eq!(calls[1].0.profile.as_deref(), Some("work"));
    assert_eq!(calls[1].0.org_name.as_deref(), Some("work-org"));

    shutdown(&socket).await;
    handle.await.unwrap();
}

#[tokio::test]
async fn one_session_reports_to_multiple_routes_and_orgs_concurrently() {
    let provider = Arc::new(TestAuthProvider {
        calls: Mutex::new(Vec::new()),
        fail: false,
        first_lease_expired: false,
    });
    let recording = Arc::new(RouteRecordingSinkFactory::default());
    let (_data_dir, socket, handle, _tmp) =
        start_daemon_with(provider.clone(), recording.clone()).await;
    let host = dummy_host();

    forward_envelope(
        &routed_envelope("shared", "work", "work-org", "SessionStart"),
        &socket,
        &host,
        false,
    )
    .await
    .unwrap();
    forward_envelope(
        &routed_envelope("shared", "personal", "personal-org", "Stop"),
        &socket,
        &host,
        false,
    )
    .await
    .unwrap();

    let flushed = flush_session("shared", &socket, 5000).await.unwrap();
    assert!(flushed.flushed, "flush did not complete: {flushed:?}");
    assert_eq!(flushed.pending, 0);
    assert_eq!(
        flushed.accepted_sessions, 2,
        "each route is an independent delivery pipeline"
    );

    let calls = provider.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 2, "each route resolves its own credentials");
    assert_eq!(calls[0].0.org_name.as_deref(), Some("work-org"));
    assert_eq!(calls[1].0.org_name.as_deref(), Some("personal-org"));

    let sinks = recording.sinks.lock().unwrap().clone();
    assert_eq!(sinks.len(), 2, "one sink per route, not per session");
    let orgs: HashSet<_> = sinks
        .iter()
        .map(|sink| sink.org.lock().unwrap().clone().unwrap())
        .collect();
    assert_eq!(
        orgs,
        HashSet::from(["work-org".to_string(), "personal-org".to_string()])
    );
    for sink in &sinks {
        assert!(
            sink.emitted.load(std::sync::atomic::Ordering::Relaxed) > 0,
            "every destination must receive spans"
        );
        assert_eq!(sink.flushes.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    let status = run_status(StatusArgs {
        socket: Some(socket.clone()),
        session_id: Some("shared".into()),
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(status.sessions.len(), 2);
    let status_orgs: HashSet<_> = status
        .sessions
        .iter()
        .map(|session| {
            session
                .route
                .as_ref()
                .and_then(|route| route.auth.org_name.clone())
                .unwrap()
        })
        .collect();
    assert_eq!(
        status_orgs,
        HashSet::from(["work-org".to_string(), "personal-org".to_string()])
    );

    shutdown(&socket).await;
    handle.await.unwrap();
}

#[tokio::test]
async fn recovery_checkpoints_are_scoped_to_their_delivery_route() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("data");
    let socket = test_endpoint(tmp.path());
    let journal = source_journal_path(&data_dir, "debug", "route-recovery");
    std::fs::create_dir_all(journal.parent().unwrap()).unwrap();

    let personal = routed_envelope("route-recovery", "personal", "personal-org", "SessionStart");
    let work = routed_envelope("route-recovery", "work", "work-org", "Stop");
    let personal_line = serde_json::to_vec(&personal.redacted()).unwrap();
    let work_line = serde_json::to_vec(&work.redacted()).unwrap();
    let work_through = (personal_line.len() + 1 + work_line.len() + 1) as u64;
    let checkpoint = serde_json::to_vec(&serde_json::json!({
        "_bt_record_type": "delivery_checkpoint",
        "route": work.route.as_ref().unwrap(),
        "through": work_through,
    }))
    .unwrap();
    let mut contents = Vec::new();
    for line in [&personal_line, &work_line, &checkpoint] {
        contents.extend_from_slice(line);
        contents.push(b'\n');
    }
    std::fs::write(&journal, contents).unwrap();

    let provider = Arc::new(TestAuthProvider {
        calls: Mutex::new(Vec::new()),
        fail: false,
        first_lease_expired: false,
    });
    let recording = Arc::new(RouteRecordingSinkFactory::default());
    let handle = start_daemon_at_with(data_dir, socket.clone(), provider, recording.clone()).await;
    let flushed = flush_session("route-recovery", &socket, 5000)
        .await
        .unwrap();
    assert!(flushed.flushed, "flush did not complete: {flushed:?}");

    let sinks = recording.sinks.lock().unwrap().clone();
    assert_eq!(sinks.len(), 1, "only the unacknowledged route is recovered");
    assert_eq!(
        sinks[0].org.lock().unwrap().as_deref(),
        Some("personal-org"),
        "a later checkpoint for work must not suppress personal recovery"
    );
    assert!(
        sinks[0].emitted.load(std::sync::atomic::Ordering::Relaxed) > 0,
        "the unacknowledged route must reach its sink"
    );

    shutdown(&socket).await;
    handle.await.unwrap();
}

#[tokio::test]
async fn a_route_backfills_observations_captured_by_another_route_once() {
    let provider = Arc::new(TestAuthProvider {
        calls: Mutex::new(Vec::new()),
        fail: false,
        first_lease_expired: false,
    });
    let recording = Arc::new(RouteRecordingSinkFactory::default());
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("data");
    let socket = test_endpoint(tmp.path());
    let handle = start_daemon_at_with(data_dir.clone(), socket.clone(), provider, recording).await;
    let host = dummy_host();

    forward_envelope(
        &routed_envelope("rerouted", "work", "work-org", "SessionStart"),
        &socket,
        &host,
        false,
    )
    .await
    .unwrap();
    forward_envelope(
        &routed_envelope("rerouted", "personal", "personal-org", "PostToolUse"),
        &socket,
        &host,
        false,
    )
    .await
    .unwrap();
    flush_session("rerouted", &socket, 5000).await.unwrap();
    shutdown(&socket).await;
    handle.await.unwrap();

    // A fresh daemon generation can activate an existing route after another
    // route captured an event. It must backfill the missing observation once,
    // then record a checkpoint for the receiving destination.
    let provider = Arc::new(TestAuthProvider {
        calls: Mutex::new(Vec::new()),
        fail: false,
        first_lease_expired: false,
    });
    let recording = Arc::new(RouteRecordingSinkFactory::default());
    let second = start_daemon_at_with(
        data_dir.clone(),
        socket.clone(),
        provider,
        recording.clone(),
    )
    .await;
    forward_envelope(
        &routed_envelope("rerouted", "work", "work-org", "Stop"),
        &socket,
        &host,
        false,
    )
    .await
    .unwrap();
    let flushed = flush_session("rerouted", &socket, 5000).await.unwrap();
    assert!(flushed.flushed, "flush did not complete: {flushed:?}");
    assert_eq!(flushed.accepted_sessions, 1);

    let sinks = recording.sinks.lock().unwrap().clone();
    assert_eq!(
        sinks.len(),
        1,
        "only the route with new events is rebuilt eagerly"
    );
    assert_eq!(
        sinks[0].org.lock().unwrap().clone().as_deref(),
        Some("work-org")
    );
    assert_eq!(
        sinks[0].emitted.load(std::sync::atomic::Ordering::Relaxed),
        2,
        "work receives the personal-route observation once, then its new Stop"
    );

    shutdown(&socket).await;
    second.await.unwrap();
}

#[tokio::test]
async fn a_new_destination_receives_history_once_then_uses_its_own_checkpoint() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("data");
    let socket = test_endpoint(tmp.path());
    let host = dummy_host();

    let provider = Arc::new(TestAuthProvider {
        calls: Mutex::new(Vec::new()),
        fail: false,
        first_lease_expired: false,
    });
    let first_sinks = Arc::new(RouteRecordingSinkFactory::default());
    let first = start_daemon_at_with(data_dir.clone(), socket.clone(), provider, first_sinks).await;
    forward_envelope(
        &routed_envelope("shared-history", "work", "work-org", "SessionStart"),
        &socket,
        &host,
        false,
    )
    .await
    .unwrap();
    flush_session("shared-history", &socket, 5000)
        .await
        .unwrap();
    shutdown(&socket).await;
    first.await.unwrap();

    // A managed run or newly configured global route can select a second
    // destination for the same native session. It gets the complete source
    // history even though that history was originally captured via work.
    let provider = Arc::new(TestAuthProvider {
        calls: Mutex::new(Vec::new()),
        fail: false,
        first_lease_expired: false,
    });
    let second_sinks = Arc::new(RouteRecordingSinkFactory::default());
    let second = start_daemon_at_with(
        data_dir.clone(),
        socket.clone(),
        provider,
        second_sinks.clone(),
    )
    .await;
    forward_envelope(
        &routed_envelope("shared-history", "personal", "personal-org", "Stop"),
        &socket,
        &host,
        false,
    )
    .await
    .unwrap();
    flush_session("shared-history", &socket, 5000)
        .await
        .unwrap();
    let delivered = second_sinks.sinks.lock().unwrap().clone();
    assert_eq!(delivered.len(), 1);
    assert_eq!(
        delivered[0]
            .emitted
            .load(std::sync::atomic::Ordering::Relaxed),
        3,
        "personal receives work's root and SessionStart plus its own Stop"
    );
    assert_eq!(
        delivered[0].org.lock().unwrap().clone().as_deref(),
        Some("personal-org")
    );
    shutdown(&socket).await;
    second.await.unwrap();

    // The same personal destination now has an independent checkpoint. Its
    // next recovery rebuilds both historical events but emits only SessionEnd.
    let provider = Arc::new(TestAuthProvider {
        calls: Mutex::new(Vec::new()),
        fail: false,
        first_lease_expired: false,
    });
    let third_sinks = Arc::new(RouteRecordingSinkFactory::default());
    let third = start_daemon_at_with(data_dir, socket.clone(), provider, third_sinks.clone()).await;
    forward_envelope(
        &routed_envelope("shared-history", "personal", "personal-org", "SessionEnd"),
        &socket,
        &host,
        false,
    )
    .await
    .unwrap();
    flush_session("shared-history", &socket, 5000)
        .await
        .unwrap();
    let delivered = third_sinks.sinks.lock().unwrap().clone();
    assert_eq!(delivered.len(), 1);
    assert_eq!(
        delivered[0]
            .emitted
            .load(std::sync::atomic::Ordering::Relaxed),
        1,
        "personal's acknowledged history is not re-delivered"
    );
    shutdown(&socket).await;
    third.await.unwrap();
}

#[tokio::test]
async fn auth_resolution_failure_is_reported_without_exposing_credentials() {
    let provider = Arc::new(TestAuthProvider {
        calls: Mutex::new(Vec::new()),
        fail: true,
        first_lease_expired: false,
    });
    let (data_dir, socket, handle, _tmp) = start_routed_daemon(provider.clone()).await;
    let host = dummy_host();

    forward_envelope(
        &routed_envelope("login-needed", "missing", "missing-org", "SessionStart"),
        &socket,
        &host,
        false,
    )
    .await
    .unwrap();
    let status = run_status(StatusArgs {
        socket: Some(socket.clone()),
        session_id: Some("login-needed".into()),
    })
    .await
    .unwrap()
    .unwrap();
    let status_error = status.sessions[0].last_error.as_deref().unwrap();
    assert!(status_error.contains("select a profile explicitly"));
    assert!(!status_error.contains("secret-"));
    assert!(
        source_journal_path(&data_dir, "debug", "login-needed").exists(),
        "capture must remain durable even when background auth fails"
    );
    assert_eq!(provider.calls.lock().unwrap().len(), 1);

    shutdown(&socket).await;
    handle.await.unwrap();
}

#[tokio::test]
async fn events_are_ordered_journaled_and_emitted() {
    let (data_dir, socket, handle, _tmp) = start_daemon().await;
    let host = dummy_host();
    let session = "sess-1";

    for (i, event) in ["SessionStart", "PostToolUse", "Stop"].iter().enumerate() {
        let env = envelope(session, event, 1000 + i as i64);
        forward_envelope(&env, &socket, &host, false).await.unwrap();
    }

    let flushed = flush_session(session, &socket, 5000).await.unwrap();
    assert!(flushed.flushed, "flush did not complete: {flushed:?}");
    assert_eq!(flushed.pending, 0);

    // Journal: three events plus a durable delivery checkpoint, with only the
    // non-secret route. Checkpoints are control records, not agent events.
    let journal = source_journal_path(&data_dir, "debug", "sess-1");
    let jtext = std::fs::read_to_string(&journal).unwrap();
    let jlines: Vec<&str> = jtext.lines().filter(|l| !l.trim().is_empty()).collect();
    assert_eq!(
        jlines.len(),
        4,
        "expected 3 event lines plus one checkpoint, got {}",
        jlines.len()
    );
    assert!(
        !jtext.contains("sk-TOP-SECRET-abc123"),
        "token leaked into journal!"
    );
    assert!(!jtext.contains("token_sha256_prefix"));

    let events: Vec<String> = jlines
        .iter()
        .filter(|line| !line.contains("\"_bt_record_type\":\"delivery_checkpoint\""))
        .map(|l| {
            serde_json::from_str::<serde_json::Value>(l).unwrap()["event"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect();
    assert_eq!(events, vec!["SessionStart", "PostToolUse", "Stop"]);

    // Spans: debug translator emits a root once + one span per event = 4.
    let spans = data_dir.join("spans").join("sess-1.ndjson");
    let stext = std::fs::read_to_string(&spans).unwrap();
    let slines: Vec<&str> = stext.lines().filter(|l| !l.trim().is_empty()).collect();
    assert_eq!(
        slines.len(),
        4,
        "expected 4 span rows, got {}: {stext}",
        slines.len()
    );
    // The span rows carry the raw payload but never the auth token.
    assert!(
        !stext.contains("sk-TOP-SECRET-abc123"),
        "token leaked into spans!"
    );

    handle.abort();
}

#[tokio::test]
async fn unknown_sources_are_rejected_before_journaling() {
    let (data_dir, socket, handle, _tmp) = start_daemon().await;
    let mut env = envelope("unknown-session", "SessionStart", 1);
    env.source = "mystery-agent".into();
    let error = forward_envelope(&env, &socket, &dummy_host(), false)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("unsupported coding-agent source"), "{error}");
    assert!(!source_journal_path(&data_dir, "mystery-agent", "unknown-session").exists());
    handle.abort();
}

#[tokio::test]
async fn identical_native_ids_from_different_sources_are_isolated() {
    let (data_dir, socket, handle, tmp) = start_daemon().await;
    let host = dummy_host();
    let codex_transcript = tmp.path().join("codex-rollout.jsonl");
    std::fs::write(
        &codex_transcript,
        serde_json::to_string(&serde_json::json!({
            "timestamp": "2026-01-01T00:00:01Z",
            "type": "session_meta",
            "payload": {
                "id": "shared-native-id",
                "cwd": "/workspace/codex",
                "cli_version": "test"
            }
        }))
        .unwrap()
            + "\n",
    )
    .unwrap();
    let mut codex = envelope("shared-native-id", "SessionStart", 1);
    codex.source = "codex".into();
    codex.payload = serde_json::json!({
        "session_id": "shared-native-id",
        "hook_event_name": "SessionStart",
        "transcript_path": codex_transcript,
        "source": "startup",
        "permission_mode": "auto"
    });
    let mut claude = envelope("shared-native-id", "SessionStart", 2);
    claude.source = "claude".into();
    claude.payload = serde_json::json!({
        "session_id": "shared-native-id",
        "hook_event_name": "SessionStart",
        "cwd": "/workspace/claude"
    });

    forward_envelope(&codex, &socket, &host, false)
        .await
        .unwrap();
    forward_envelope(&claude, &socket, &host, false)
        .await
        .unwrap();
    flush_session("shared-native-id", &socket, 5000)
        .await
        .unwrap();

    let status = run_status(StatusArgs {
        socket: Some(socket.clone()),
        session_id: Some("shared-native-id".into()),
    })
    .await
    .unwrap()
    .unwrap();
    let sources = status
        .sessions
        .iter()
        .map(|session| session.source.as_str())
        .collect::<HashSet<_>>();
    assert_eq!(sources, HashSet::from(["codex", "claude-code"]));

    let codex_journal = source_journal_path(&data_dir, "codex", "shared-native-id");
    let claude_journal = source_journal_path(&data_dir, "claude-code", "shared-native-id");
    assert_ne!(codex_journal, claude_journal);
    assert_eq!(
        std::fs::read_to_string(codex_journal)
            .unwrap()
            .lines()
            .count(),
        2
    );
    assert_eq!(
        std::fs::read_to_string(claude_journal)
            .unwrap()
            .lines()
            .count(),
        2
    );

    let rows = std::fs::read_to_string(data_dir.join("spans/shared-native-id.ndjson")).unwrap();
    let root_ids = rows
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter_map(|row| row.get("Insert").cloned())
        .filter(|row| row["root_span_id"] == row["span_id"])
        .filter_map(|row| row["span_id"].as_str().map(str::to_owned))
        .collect::<HashSet<_>>();
    assert_eq!(root_ids.len(), 2);

    handle.abort();
}

#[tokio::test]
async fn distinct_sessions_are_isolated() {
    let (data_dir, socket, handle, _tmp) = start_daemon().await;
    let host = dummy_host();

    forward_envelope(&envelope("a", "SessionStart", 1), &socket, &host, false)
        .await
        .unwrap();
    forward_envelope(&envelope("b", "SessionStart", 1), &socket, &host, false)
        .await
        .unwrap();
    forward_envelope(&envelope("a", "Stop", 2), &socket, &host, false)
        .await
        .unwrap();

    flush_session("a", &socket, 5000).await.unwrap();
    flush_session("b", &socket, 5000).await.unwrap();

    let a = std::fs::read_to_string(source_journal_path(&data_dir, "debug", "a")).unwrap();
    let b = std::fs::read_to_string(source_journal_path(&data_dir, "debug", "b")).unwrap();
    assert_eq!(a.lines().filter(|l| !l.trim().is_empty()).count(), 3);
    assert_eq!(b.lines().filter(|l| !l.trim().is_empty()).count(), 2);

    handle.abort();
}

#[tokio::test]
async fn hook_capture_stops_at_the_durable_journal_boundary() {
    let (socket, handle, tmp) = start_slow_daemon().await;
    let host = dummy_host();
    forward_envelope(
        &envelope("fast-capture", "SessionStart", 1),
        &socket,
        &host,
        false,
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(25)).await;

    let mut turn_end = envelope("fast-capture", "Stop", 2);
    turn_end.route.as_mut().unwrap().flush_mode = bt_daemon::wire::FlushMode::FlushOnTurnEnd;
    let accepted = tokio::time::timeout(
        Duration::from_millis(100),
        forward_envelope(&turn_end, &socket, &host, false),
    )
    .await;
    assert!(
        matches!(accepted, Ok(Ok(()))),
        "turn-end capture waited for translation or flushing: {accepted:?}"
    );
    let journal = std::fs::read_to_string(source_journal_path(
        &tmp.path().join("data"),
        "debug",
        "fast-capture",
    ))
    .unwrap();
    assert_eq!(
        journal.lines().count(),
        2,
        "hook returned before journaling"
    );
    shutdown(&socket).await;
    handle.await.unwrap();
}

#[tokio::test]
async fn saturated_ingress_uses_the_journal_as_its_bounded_overflow_queue() {
    const EVENT_COUNT: i64 = 1_100;
    let gate = Arc::new(tokio::sync::Notify::new());
    let emitted = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let (data_dir, socket, handle, _tmp) = start_gated_daemon(gate.clone(), emitted.clone()).await;
    let host = dummy_host();

    forward_envelope(
        &envelope("bounded-overflow", "SessionStart", 0),
        &socket,
        &host,
        false,
    )
    .await
    .unwrap();
    for ts in 1..EVENT_COUNT {
        forward_envelope(
            &envelope("bounded-overflow", "PostToolUse", ts),
            &socket,
            &host,
            false,
        )
        .await
        .unwrap();
    }

    let journal =
        std::fs::read_to_string(source_journal_path(&data_dir, "debug", "bounded-overflow"))
            .unwrap();
    assert_eq!(
        journal.lines().count(),
        EVENT_COUNT as usize,
        "every hook returns only after its event reaches the journal"
    );

    gate.notify_one();
    let flushed = flush_session("bounded-overflow", &socket, 15_000)
        .await
        .unwrap();
    assert!(flushed.flushed, "flush did not complete: {flushed:?}");
    assert_eq!(
        emitted.load(std::sync::atomic::Ordering::Relaxed),
        EVENT_COUNT as u64 + 1,
        "the root plus every journaled event must reach the sink exactly once"
    );

    shutdown(&socket).await;
    handle.await.unwrap();
}

#[tokio::test]
async fn status_reports_sessions_and_filters_by_session_id() {
    let (_data_dir, socket, handle, _tmp) = start_daemon().await;
    let host = dummy_host();
    forward_envelope(
        &envelope("visible", "SessionStart", 1),
        &socket,
        &host,
        false,
    )
    .await
    .unwrap();
    forward_envelope(&envelope("other", "SessionStart", 1), &socket, &host, false)
        .await
        .unwrap();

    let all = run_status(StatusArgs {
        socket: Some(socket.clone()),
        session_id: None,
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(all.daemon_version, "test");
    assert_eq!(all.sessions.len(), 2);

    let filtered = run_status(StatusArgs {
        socket: Some(socket.clone()),
        session_id: Some("visible".into()),
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(filtered.sessions.len(), 1);
    assert_eq!(filtered.sessions[0].session_id, "visible");

    shutdown(&socket).await;
    handle.await.unwrap();
    wait_until_gone(&socket).await;
}

#[tokio::test]
async fn a_second_server_detects_the_existing_daemon() {
    let (data_dir, socket, first, _tmp) = start_daemon().await;
    let args = ServeArgs {
        socket: Some(socket.clone()),
        data_dir: Some(data_dir.clone()),
        idle_timeout_secs: 0,
        session_idle_timeout_secs: 0,
    };
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        run_serve(args, debug_serve_options("rival", &data_dir)),
    )
    .await
    .expect("rival server should resolve ownership promptly");
    result.unwrap();

    let status = run_status(StatusArgs {
        socket: Some(socket.clone()),
        session_id: None,
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(status.daemon_version, "test");

    shutdown(&socket).await;
    first.await.unwrap();
}

#[tokio::test]
async fn no_spawn_errors_when_daemon_absent() {
    let tmp = tempfile::tempdir().unwrap();
    let socket = test_endpoint(tmp.path());
    let host = dummy_host();
    let err = forward_envelope(&envelope("x", "y", 1), &socket, &host, true).await;
    assert!(err.is_err(), "expected error with --no-spawn and no daemon");
}

#[tokio::test]
async fn no_spawn_rejects_a_mismatched_daemon_version() {
    let (_data_dir, socket, handle, _tmp) = start_daemon().await;
    let host = HostInfo {
        serve_argv: vec![OsString::from("unused")],
        version: "newer-client".into(),
    };
    let err = forward_envelope(&envelope("x", "y", 1), &socket, &host, true)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("does not match client"));
    shutdown(&socket).await;
    handle.await.unwrap();
}

#[cfg(feature = "cli")]
#[tokio::test]
async fn spawn_on_demand_runs_the_real_standalone_daemon() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("spawned-data");
    let socket = test_endpoint(tmp.path());
    let _org = EnvVarGuard::set("BRAINTRUST_ORG_NAME", "spawn-org");
    let host = HostInfo {
        serve_argv: vec![
            OsString::from(env!("CARGO_BIN_EXE_bt-daemon")),
            OsString::from("serve"),
            OsString::from("--debug-sink"),
            OsString::from("--data-dir"),
            data_dir.as_os_str().to_owned(),
            OsString::from("--idle-timeout-secs"),
            OsString::from("5"),
        ],
        version: env!("CARGO_PKG_VERSION").into(),
    };

    forward_envelope(
        &envelope("spawned", "SessionStart", 1),
        &socket,
        &host,
        false,
    )
    .await
    .unwrap();
    flush_session("spawned", &socket, 5000).await.unwrap();
    let spans = std::fs::read_to_string(data_dir.join("spans/spawned.ndjson")).unwrap();
    assert_eq!(spans.lines().count(), 2);

    shutdown(&socket).await;
    wait_until_gone(&socket).await;
}

#[tokio::test]
async fn cold_worker_rebuilds_acknowledged_journal_without_redelivery() {
    let (data_dir, socket, first, tmp) = start_daemon().await;
    let host = dummy_host();
    forward_envelope(
        &envelope("resume", "SessionStart", 1),
        &socket,
        &host,
        false,
    )
    .await
    .unwrap();
    flush_session("resume", &socket, 5000).await.unwrap();
    shutdown(&socket).await;
    first.await.unwrap();

    // A cold worker receives only the saved journal, not the first worker's
    // daemon state. Its delivery checkpoint must travel with that journal.
    let recovered_data_dir = tmp.path().join("recovered-data");
    let source = source_journal_path(&data_dir, "debug", "resume");
    let recovered_journal = source_journal_path(&recovered_data_dir, "debug", "resume");
    std::fs::create_dir_all(recovered_journal.parent().unwrap()).unwrap();
    std::fs::copy(source, &recovered_journal).unwrap();

    let second = start_daemon_at(recovered_data_dir.clone(), socket.clone()).await;
    forward_envelope(&envelope("resume", "Stop", 2), &socket, &host, false)
        .await
        .unwrap();
    flush_session("resume", &socket, 5000).await.unwrap();

    let journal = std::fs::read_to_string(recovered_journal).unwrap();
    assert_eq!(
        journal.lines().count(),
        4,
        "two events plus two delivery checkpoints"
    );
    let spans = std::fs::read_to_string(recovered_data_dir.join("spans/resume.ndjson")).unwrap();
    let rows: Vec<serde_json::Value> = spans
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(
        rows.len(),
        1,
        "only the new event is delivered; acknowledged history only rebuilds state"
    );
    let span_id = |row: &serde_json::Value| {
        row.get("Insert")
            .or_else(|| row.get("Merge"))
            .and_then(|body| body.get("span_id"))
            .and_then(serde_json::Value::as_str)
            .unwrap()
            .to_owned()
    };
    assert!(!span_id(&rows[0]).is_empty());

    shutdown(&socket).await;
    second.await.unwrap();
}

#[tokio::test]
async fn daemon_startup_recovers_an_event_journaled_before_worker_dispatch() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("data");
    let socket = test_endpoint(tmp.path());
    let journal = source_journal_path(&data_dir, "debug", "crash-window");
    std::fs::create_dir_all(journal.parent().unwrap()).unwrap();
    let mut raw =
        serde_json::to_vec(&envelope("crash-window", "SessionStart", 1).redacted()).unwrap();
    raw.push(b'\n');
    std::fs::write(&journal, raw).unwrap();

    let daemon = start_daemon_at(data_dir.clone(), socket.clone()).await;
    let flushed = flush_session("crash-window", &socket, 5000).await.unwrap();
    assert!(
        flushed.flushed,
        "recovered event did not drain: {flushed:?}"
    );
    let spans = std::fs::read_to_string(data_dir.join("spans/crash-window.ndjson")).unwrap();
    assert_eq!(
        spans.lines().count(),
        2,
        "startup must translate the uncheckpointed journal event"
    );

    shutdown(&socket).await;
    daemon.await.unwrap();
}

#[tokio::test]
async fn claude_boundary_journal_references_a_self_contained_transcript_mirror() {
    let (data_dir, socket, handle, tmp) = start_daemon().await;
    let transcript = tmp.path().join("claude.jsonl");
    std::fs::write(
        &transcript,
        r#"{"type":"assistant","timestamp":"2026-07-29T00:00:00Z","message":{"id":"m1","model":"claude","content":[{"type":"text","text":"durable"}]}}"#,
    )
    .unwrap();
    let mut env = envelope("claude-journal", "Stop", 1_775_000_000_000);
    env.source = "claude-code".into();
    env.payload = serde_json::json!({
        "session_id":"claude-journal",
        "hook_event_name":"Stop",
        "transcript_path":transcript
    });
    forward_envelope(&env, &socket, &dummy_host(), false)
        .await
        .unwrap();
    flush_session("claude-journal", &socket, 5000)
        .await
        .unwrap();

    let journal = std::fs::read_to_string(source_journal_path(
        &data_dir,
        "claude-code",
        "claude-journal",
    ))
    .unwrap();
    assert!(!journal.contains("sk-TOP-SECRET-abc123"));

    // The journal references the mirror rather than inlining the transcript,
    // so re-journaling a growing transcript stays linear in its size.
    let entry: serde_json::Value = serde_json::from_str(journal.lines().next().unwrap()).unwrap();
    let reference = &entry["payload"]["_bt_transcript_mirror"];
    assert_eq!(reference["path"], transcript.to_str().unwrap());
    assert!(
        !journal.contains("durable"),
        "the journal must not inline transcript contents: {journal}"
    );

    // The mirror is daemon-owned and survives the original being rewritten.
    let mirror = std::path::PathBuf::from(reference["mirror"].as_str().unwrap());
    assert!(mirror.starts_with(data_dir.join("transcripts")));
    let mirrored = std::fs::read_to_string(&mirror).unwrap();
    assert!(mirrored.contains("durable"));
    assert_eq!(
        reference["through"].as_u64().unwrap(),
        mirrored.len() as u64,
        "the journaled offset must bound replay to the bytes captured here"
    );
    handle.abort();
}

/// Start a daemon that retires sessions after `ttl_secs` of quiet.
async fn start_daemon_with_session_ttl(
    ttl_secs: u64,
) -> (
    PathBuf,
    PathBuf,
    tokio::task::JoinHandle<()>,
    tempfile::TempDir,
) {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("data");
    let socket = test_endpoint(tmp.path());
    std::fs::create_dir_all(&data_dir).unwrap();
    let args = ServeArgs {
        socket: Some(socket.clone()),
        data_dir: Some(data_dir.clone()),
        idle_timeout_secs: 0,
        session_idle_timeout_secs: ttl_secs,
    };
    let mut opts = debug_serve_options("test", &data_dir);
    opts.auth_provider = Some(Arc::new(TestAuthProvider {
        calls: Mutex::new(Vec::new()),
        fail: false,
        first_lease_expired: false,
    }));
    let handle = tokio::spawn(async move {
        let _ = run_serve(args, opts).await;
    });
    wait_for(&socket).await;
    (data_dir, socket, handle, tmp)
}

fn claude_stop(session_id: &str, transcript: &Path, ts_ms: i64) -> Envelope {
    let mut env = envelope(session_id, "Stop", ts_ms);
    env.source = "claude-code".into();
    env.payload = serde_json::json!({
        "session_id": session_id,
        "hook_event_name": "Stop",
        "transcript_path": transcript,
    });
    env
}

/// The 20 GB crash: every lifecycle event used to journal the whole transcript,
/// so a session's journal grew with the square of its transcript and replay
/// had to hold all of it in memory at once. Journal growth must stay tied to
/// the *number* of events, not to the transcript size times that number.
#[tokio::test]
async fn claude_journal_does_not_grow_with_the_transcript_on_every_turn() {
    let (data_dir, socket, handle, tmp) = start_daemon().await;
    let host = dummy_host();
    let transcript = tmp.path().join("claude.jsonl");

    const TURNS: i64 = 40;
    let mut contents = String::new();
    for turn in 0..TURNS {
        // Each turn appends a chunky assistant record, as a real session does.
        contents.push_str(&format!(
            r#"{{"type":"assistant","timestamp":"2026-07-29T00:00:00Z","message":{{"id":"m{turn}","model":"claude","content":[{{"type":"text","text":"{}"}}]}}}}"#,
            "x".repeat(20_000)
        ));
        contents.push('\n');
        std::fs::write(&transcript, &contents).unwrap();
        forward_envelope(
            &claude_stop("grow", &transcript, 1_775_000_000_000 + turn),
            &socket,
            &host,
            false,
        )
        .await
        .unwrap();
    }
    flush_session("grow", &socket, 5000).await.unwrap();

    let transcript_len = std::fs::metadata(&transcript).unwrap().len();
    let journal_len = std::fs::metadata(source_journal_path(&data_dir, "claude-code", "grow"))
        .unwrap()
        .len();

    // Re-journaling the transcript every turn would put this near
    // TURNS * transcript_len / 2 (tens of megabytes). References are ~a few
    // hundred bytes each.
    assert!(
        journal_len < 64 * 1024,
        "journal grew with the transcript: {journal_len} bytes for {TURNS} events \
         over a {transcript_len}-byte transcript"
    );

    // The transcript is still captured durably -- once, in the mirror.
    let mirrors: Vec<_> = std::fs::read_dir(data_dir.join("transcripts"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert_eq!(mirrors.len(), 1, "one mirror per (session, transcript)");
    assert_eq!(
        std::fs::metadata(&mirrors[0]).unwrap().len(),
        transcript_len,
        "the mirror must hold the whole transcript exactly once"
    );
    handle.abort();
}

/// A session that goes quiet must release its translator state, sink, journal
/// handle, and credential lease instead of pinning them until the process
/// exits -- which, for a continuously busy user, never happened.
#[tokio::test]
async fn idle_sessions_are_retired_and_can_resume_from_their_journal() {
    let (data_dir, socket, handle, _tmp) = start_daemon_with_session_ttl(1).await;
    let host = dummy_host();
    forward_envelope(&envelope("nap", "SessionStart", 1), &socket, &host, false)
        .await
        .unwrap();
    flush_session("nap", &socket, 5000).await.unwrap();

    let live = |socket: PathBuf| async move {
        run_status(StatusArgs {
            socket: Some(socket),
            session_id: Some("nap".into()),
        })
        .await
        .unwrap()
        .unwrap()
        .sessions
        .len()
    };
    assert_eq!(live(socket.clone()).await, 1, "session should be live");

    let mut retired = false;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if live(socket.clone()).await == 0 {
            retired = true;
            break;
        }
    }
    assert!(retired, "an idle session was never retired");

    // Retirement is not data loss: a later event rebuilds the session from its
    // journal, but acknowledged history is not delivered a second time.
    forward_envelope(&envelope("nap", "Stop", 2), &socket, &host, false)
        .await
        .unwrap();
    flush_session("nap", &socket, 5000).await.unwrap();
    let spans = std::fs::read_to_string(data_dir.join("spans/nap.ndjson")).unwrap();
    let ids: Vec<String> = spans
        .lines()
        .map(|line| {
            let row: serde_json::Value = serde_json::from_str(line).unwrap();
            row.get("Insert")
                .or_else(|| row.get("Merge"))
                .and_then(|body| body.get("span_id"))
                .and_then(serde_json::Value::as_str)
                .unwrap()
                .to_owned()
        })
        .collect();
    assert_eq!(
        ids.len(),
        3,
        "first delivery (2) + resumed event (1), without replay redelivery"
    );
    assert_eq!(
        ids.iter().collect::<HashSet<_>>().len(),
        3,
        "the resumed event adds one new span without duplicating acknowledged history"
    );
    handle.abort();
}

#[tokio::test]
async fn managed_run_flush_is_scoped_to_its_accepted_sessions() {
    let (socket, handle, flushes, _tmp) = start_tracking_daemon("test").await;
    let host = dummy_host();
    for (session_id, managed_run_id) in [
        ("run-a-main", "run-a"),
        ("run-a-child", "run-a"),
        ("run-b-main", "run-b"),
    ] {
        let mut env = envelope(session_id, "SessionStart", 1);
        env.managed_run_id = Some(managed_run_id.to_string());
        forward_envelope(&env, &socket, &host, false).await.unwrap();
    }

    let result = flush_managed_run("run-a", &socket, 5_000).await.unwrap();
    assert!(result.flushed, "managed run did not flush: {result:?}");
    assert_eq!(result.pending, 0);
    assert_eq!(
        *flushes.lock().unwrap(),
        HashMap::from([
            ("run-a-main".to_string(), 1),
            ("run-a-child".to_string(), 1)
        ])
    );

    let result = flush_managed_run("run-b", &socket, 5_000).await.unwrap();
    assert!(result.flushed, "managed run did not flush: {result:?}");
    assert_eq!(flushes.lock().unwrap().get("run-b-main"), Some(&1));

    shutdown(&socket).await;
    handle.await.unwrap();
}

#[tokio::test]
async fn managed_run_completion_finalizes_an_open_agent_root() {
    let provider = Arc::new(TestAuthProvider {
        calls: Mutex::new(Vec::new()),
        fail: false,
        first_lease_expired: false,
    });
    let recording = Arc::new(RouteRecordingSinkFactory::default());
    let (_data_dir, socket, handle, _tmp) = start_daemon_with(provider, recording.clone()).await;
    let mut env = envelope("managed-opencode", "session.created", 7);
    env.source = "opencode".into();
    env.managed_run_id = Some("managed-run".into());
    env.payload = serde_json::json!({"properties":{"info":{"id":"native"}}});
    forward_envelope(&env, &socket, &dummy_host(), false)
        .await
        .unwrap();

    let checkpoint = flush_session("managed-opencode", &socket, 5_000)
        .await
        .unwrap();
    assert!(checkpoint.flushed);
    let record = recording.sinks.lock().unwrap()[0].clone();
    assert!(record
        .ops
        .lock()
        .unwrap()
        .iter()
        .all(|op| !matches!(op, SpanOp::Merge(row) if row.end_ms.is_some())));

    let finalized = flush_managed_run("managed-run", &socket, 5_000)
        .await
        .unwrap();
    assert!(finalized.flushed);
    assert!(record
        .ops
        .lock()
        .unwrap()
        .iter()
        .any(|op| matches!(op, SpanOp::Merge(row) if row.end_ms == Some(7))));

    shutdown(&socket).await;
    handle.await.unwrap();
}

#[cfg(feature = "cli")]
struct EnvVarGuard {
    key: &'static str,
    previous: Option<std::ffi::OsString>,
}

#[cfg(feature = "cli")]
impl EnvVarGuard {
    fn set(key: &'static str, value: impl AsRef<std::ffi::OsStr>) -> Self {
        let previous = std::env::var_os(key);
        std::env::set_var(key, value);
        Self { key, previous }
    }
}

#[cfg(feature = "cli")]
impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        match &self.previous {
            Some(value) => std::env::set_var(self.key, value),
            None => std::env::remove_var(self.key),
        }
    }
}

#[cfg(all(feature = "cli", unix))]
#[tokio::test]
async fn managed_run_flushes_and_preserves_agent_exit_status() {
    use std::os::unix::fs::PermissionsExt;

    let version = env!("CARGO_PKG_VERSION");
    let (socket, handle, flushes, tmp) = start_tracking_daemon(version).await;
    let agent = tmp.path().join("fake-codex.sh");
    std::fs::write(
        &agent,
        r#"#!/bin/sh
previous=
last=
for argument in "$@"; do
  previous=$last
  last=$argument
done
session_id=$previous
mode=$last
case "$mode" in
  untraced-success|untraced-failure) ;;
  *)
    printf '{"session_id":"%s","hook_event_name":"SessionStart"}\n' "$session_id" |
      "$BT_DAEMON_TEST_BIN" hook --source debug --managed-run-hook --no-spawn
    ;;
esac
case "$mode" in
  success) exit 0 ;;
  failure) exit 7 ;;
  signal) kill -TERM "$$" ;;
  untraced-success) exit 0 ;;
  untraced-failure) exit 7 ;;
  *) exit 99 ;;
esac
"#,
    )
    .unwrap();
    std::fs::set_permissions(&agent, std::fs::Permissions::from_mode(0o755)).unwrap();

    let _socket = EnvVarGuard::set("BT_DAEMON_SOCKET", &socket);
    let _agent = EnvVarGuard::set("CODEX_BIN", &agent);
    let _daemon = EnvVarGuard::set("BT_DAEMON_TEST_BIN", env!("CARGO_BIN_EXE_bt-daemon"));
    let route = || SessionRoute {
        auth: AuthSelection {
            source: AuthSource::Environment,
            ..AuthSelection::default()
        },
        destination: Some(bt_daemon::wire::TraceDestination::ProjectLogs {
            project_id: None,
            project_name: Some("managed-run-test".into()),
        }),
        ..SessionRoute::default()
    };
    let run = |session_id: &str, mode: &str| {
        run_traced(
            RunArgs {
                source: RunSource::Codex,
                additional_metadata: None,
                agent_args: vec![session_id.into(), mode.into()],
            },
            RunHookCommand {
                program: "unused-hook-command".into(),
                args: Vec::new(),
            },
            route(),
        )
    };

    assert!(run("managed-success", "success").await.unwrap().success());
    assert_eq!(
        run("managed-failure", "failure").await.unwrap().code(),
        Some(7)
    );
    let signal_status = run("managed-signal", "signal").await.unwrap();
    assert!(!signal_status.success());
    assert_eq!(signal_status.code(), None);

    assert!(run("managed-untraced", "untraced-success")
        .await
        .unwrap()
        .success());
    assert_eq!(
        run("managed-untraced", "untraced-failure")
            .await
            .unwrap()
            .code(),
        Some(7)
    );

    assert_eq!(
        *flushes.lock().unwrap(),
        HashMap::from([
            ("managed-success".to_string(), 1),
            ("managed-failure".to_string(), 1),
            ("managed-signal".to_string(), 1),
        ])
    );

    shutdown(&socket).await;
    handle.await.unwrap();
}
