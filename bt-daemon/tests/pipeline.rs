//! Phase 1 end-to-end: hook client → UDS → dispatch → journal → translate →
//! debug sink. Runs the daemon in-process on a temp socket (no process
//! spawning, so it's deterministic).

use async_trait::async_trait;
use bt_daemon::wire::{AuthSelection, BackendAuth, Envelope, SessionRoute};
use bt_daemon::{
    debug_serve_options, flush_session, forward_envelope, run_serve, run_status, shutdown_daemon,
    AuthLease, AuthProvider, AuthResolveReason, HostInfo, ServeArgs, StatusArgs,
};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

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
        session_id: session_id.into(),
        event: event.into(),
        ts_ms,
        payload: serde_json::json!({ "session_id": session_id, "hook_event_name": event, "n": ts_ms }),
        route: Some(SessionRoute {
            destination: Some(bt_daemon::wire::TraceDestination::ProjectLogs {
                project_id: None,
                project_name: Some("codex".into()),
            }),
            ..SessionRoute::default()
        }),
        config: None,
    }
}

fn routed_envelope(session_id: &str, profile: &str, org: &str, event: &str) -> Envelope {
    let mut env = envelope(session_id, event, 1);
    env.route = Some(SessionRoute {
        auth: AuthSelection {
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
        let profile = selection
            .profile
            .clone()
            .unwrap_or_else(|| "default".into());
        Ok(AuthLease {
            profile: profile.clone(),
            auth: BackendAuth {
                token: format!("secret-{profile}-{call_index}"),
                api_url: Some(format!("https://{profile}.example.test")),
                app_url: None,
                org_name: selection.org_name.clone(),
                org_id: Some(format!("org-{profile}")),
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

async fn start_daemon_at(data_dir: PathBuf, socket: PathBuf) -> tokio::task::JoinHandle<()> {
    let args = ServeArgs {
        socket: Some(socket.clone()),
        data_dir: Some(data_dir.clone()),
        idle_timeout_secs: 0,
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
            std::fs::read_to_string(data_dir.join("journal").join(format!("{session}.ndjson")))
                .unwrap();
        assert!(journal.contains("\"route\""));
        assert!(!journal.contains("secret-"));
        assert!(!journal.contains("token_sha256_prefix"));
        assert!(!journal.contains("\"config\""));
    }

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
async fn active_session_rejects_route_changes() {
    let provider = Arc::new(TestAuthProvider {
        calls: Mutex::new(Vec::new()),
        fail: false,
        first_lease_expired: false,
    });
    let (_data_dir, socket, handle, _tmp) = start_routed_daemon(provider).await;
    let host = dummy_host();

    forward_envelope(
        &routed_envelope("pinned", "work", "work-org", "SessionStart"),
        &socket,
        &host,
        false,
    )
    .await
    .unwrap();
    let error = forward_envelope(
        &routed_envelope("pinned", "personal", "personal-org", "Stop"),
        &socket,
        &host,
        false,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("session route changed"));

    shutdown(&socket).await;
    handle.await.unwrap();
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

    let error = forward_envelope(
        &routed_envelope("login-needed", "missing", "missing-org", "SessionStart"),
        &socket,
        &host,
        false,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("bt auth login"));
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
    assert!(!data_dir.join("journal/login-needed.ndjson").exists());
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

    // Journal: three events, in order, with only the non-secret route.
    let journal = data_dir.join("journal").join("sess-1.ndjson");
    let jtext = std::fs::read_to_string(&journal).unwrap();
    let jlines: Vec<&str> = jtext.lines().filter(|l| !l.trim().is_empty()).collect();
    assert_eq!(
        jlines.len(),
        3,
        "expected 3 journal lines, got {}",
        jlines.len()
    );
    assert!(
        !jtext.contains("sk-TOP-SECRET-abc123"),
        "token leaked into journal!"
    );
    assert!(!jtext.contains("token_sha256_prefix"));

    let events: Vec<String> = jlines
        .iter()
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

    let a = std::fs::read_to_string(data_dir.join("journal").join("a.ndjson")).unwrap();
    let b = std::fs::read_to_string(data_dir.join("journal").join("b.ndjson")).unwrap();
    assert_eq!(a.lines().filter(|l| !l.trim().is_empty()).count(), 2);
    assert_eq!(b.lines().filter(|l| !l.trim().is_empty()).count(), 1);

    handle.abort();
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
async fn restart_replays_journal_with_stable_span_ids_before_new_events() {
    let (data_dir, socket, first, _tmp) = start_daemon().await;
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

    let second = start_daemon_at(data_dir.clone(), socket.clone()).await;
    forward_envelope(&envelope("resume", "Stop", 2), &socket, &host, false)
        .await
        .unwrap();
    flush_session("resume", &socket, 5000).await.unwrap();

    let journal = std::fs::read_to_string(data_dir.join("journal/resume.ndjson")).unwrap();
    assert_eq!(journal.lines().count(), 2);
    let spans = std::fs::read_to_string(data_dir.join("spans/resume.ndjson")).unwrap();
    let rows: Vec<serde_json::Value> = spans
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(
        rows.len(),
        5,
        "first delivery (2) + recovery replay (2) + resumed event (1)"
    );
    let span_id = |row: &serde_json::Value| {
        row.get("Insert")
            .or_else(|| row.get("Merge"))
            .and_then(|body| body.get("span_id"))
            .and_then(serde_json::Value::as_str)
            .unwrap()
            .to_owned()
    };
    assert_eq!(span_id(&rows[2]), span_id(&rows[0]));
    assert_eq!(span_id(&rows[3]), span_id(&rows[1]));
    assert_ne!(span_id(&rows[4]), span_id(&rows[1]));
    let unique_ids = rows
        .iter()
        .map(span_id)
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(
        unique_ids.len(),
        3,
        "recovery must reuse both historical ids; only the new event gets a new id"
    );

    shutdown(&socket).await;
    second.await.unwrap();
}

#[tokio::test]
async fn claude_boundary_journal_contains_a_self_contained_transcript_snapshot() {
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

    let journal = std::fs::read_to_string(data_dir.join("journal/claude-journal.ndjson")).unwrap();
    assert!(journal.contains("_bt_transcript_snapshot"));
    assert!(journal.contains("durable"));
    assert!(!journal.contains("sk-TOP-SECRET-abc123"));
    handle.abort();
}
