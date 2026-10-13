//! Failure tracking and recovery: a failed stage holds its work, never loses
//! it, and resumes from its durable position when its own condition is met.

use async_trait::async_trait;
use bt_daemon::wire::{
    AuthSelection, BackendAuth, Envelope, SessionConfig, SessionRoute, TraceDestination,
};
use bt_daemon::{
    debug_serve_options, flush_session, forward_envelope, run_serve, run_status, shutdown_daemon,
    AuthLease, AuthProvider, AuthResolveReason, HostInfo, Registry, ServeArgs, ServeOptions, Sink,
    SinkFactory, SpanOp, StatusArgs,
};
use std::collections::{BTreeSet, HashSet};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

struct Auth;

#[async_trait]
impl AuthProvider for Auth {
    async fn resolve(
        &self,
        selection: &AuthSelection,
        _reason: AuthResolveReason,
    ) -> anyhow::Result<AuthLease> {
        Ok(AuthLease {
            selection: selection.clone(),
            auth: BackendAuth {
                token: "secret".into(),
                api_url: Some("https://api.example.test".into()),
                app_url: None,
                org_name: Some("org".into()),
                org_id: Some("org-id".into()),
            },
            expires_at_ms: None,
        })
    }
}

/// A sink whose flush fails on demand and that records what it accepted.
#[derive(Default)]
struct Scripted {
    /// Flushes to fail before succeeding, per project.
    failures: Mutex<std::collections::HashMap<String, Arc<Mutex<u32>>>>,
    /// Projects whose flush always fails.
    blocked: Mutex<HashSet<String>>,
    accepted: Mutex<std::collections::HashMap<String, Vec<SpanOp>>>,
}

impl Scripted {
    fn fail_flushes(&self, project: &str, count: u32) {
        self.failures
            .lock()
            .unwrap()
            .insert(project.into(), Arc::new(Mutex::new(count)));
    }

    fn span_ids(&self, project: &str) -> BTreeSet<String> {
        self.accepted
            .lock()
            .unwrap()
            .get(project)
            .into_iter()
            .flatten()
            .map(|op| match op {
                SpanOp::Insert(row) | SpanOp::Merge(row) => row.span_id.clone(),
            })
            .collect()
    }
}

struct ScriptedFactory(Arc<Scripted>);

impl SinkFactory for ScriptedFactory {
    fn create(&self, _session_id: &str, _source: &str) -> anyhow::Result<Box<dyn Sink>> {
        Ok(Box::new(ScriptedSink {
            script: self.0.clone(),
            project: String::new(),
            pending: Vec::new(),
        }))
    }
}

struct ScriptedSink {
    script: Arc<Scripted>,
    project: String,
    pending: Vec<SpanOp>,
}

#[async_trait]
impl Sink for ScriptedSink {
    fn configure(&mut self, config: &SessionConfig) {
        if let Some(TraceDestination::ProjectLogs { project_name, .. }) = &config.destination {
            self.project = project_name.clone().unwrap_or_default();
        }
    }

    async fn emit(&mut self, ops: &[SpanOp]) -> anyhow::Result<u64> {
        self.pending.extend_from_slice(ops);
        Ok(ops.len() as u64)
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        if self.script.blocked.lock().unwrap().contains(&self.project) {
            self.pending.clear();
            anyhow::bail!("backend unreachable");
        }
        if let Some(remaining) = self.script.failures.lock().unwrap().get(&self.project) {
            let mut remaining = remaining.lock().unwrap();
            if *remaining > 0 {
                *remaining -= 1;
                self.pending.clear();
                anyhow::bail!("connection reset");
            }
        }
        self.script
            .accepted
            .lock()
            .unwrap()
            .entry(self.project.clone())
            .or_default()
            .append(&mut self.pending);
        Ok(())
    }
}

fn endpoint(tmp: &Path) -> PathBuf {
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

fn host() -> HostInfo {
    HostInfo {
        serve_argv: vec![OsString::from("unused")],
        version: "test".into(),
    }
}

fn event(session: &str, project: &str, name: &str, ts_ms: i64) -> Envelope {
    Envelope {
        source: "debug".into(),
        source_version: None,
        plugin_version: None,
        session_id: session.into(),
        event: name.into(),
        ts_ms,
        managed_run_id: None,
        payload: serde_json::json!({ "session_id": session, "hook_event_name": name, "n": ts_ms }),
        route: Some(SessionRoute {
            destination: Some(TraceDestination::ProjectLogs {
                project_id: None,
                project_name: Some(project.into()),
            }),
            ..SessionRoute::default()
        }),
        config: None,
        capture: None,
    }
}

async fn start(
    data_dir: &Path,
    socket: &Path,
    sinks: Option<Arc<Scripted>>,
) -> tokio::task::JoinHandle<()> {
    let args = ServeArgs {
        socket: Some(socket.to_path_buf()),
        data_dir: Some(data_dir.to_path_buf()),
        idle_timeout_secs: 0,
        session_idle_timeout_secs: 0,
    };
    let opts = match sinks {
        Some(script) => ServeOptions {
            version: "test".into(),
            translators: Arc::new(Registry::default_agents()),
            sink_factory: Arc::new(ScriptedFactory(script)),
            auth_provider: Some(Arc::new(Auth)),
        },
        None => {
            let mut opts = debug_serve_options("test", data_dir);
            opts.auth_provider = Some(Arc::new(Auth));
            opts
        }
    };
    let handle = tokio::spawn(async move {
        let _ = run_serve(args, opts).await;
    });
    for _ in 0..500 {
        if let Ok(Some(_)) = run_status(StatusArgs {
            socket: Some(socket.to_path_buf()),
            session_id: None,
        })
        .await
        {
            return handle;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("daemon never answered");
}

async fn send(socket: &Path, env: &Envelope) {
    forward_envelope(env, socket, &host(), false).await.unwrap();
}

async fn status(socket: &Path, session: &str) -> Vec<bt_daemon::wire::SessionStatus> {
    run_status(StatusArgs {
        socket: Some(socket.to_path_buf()),
        session_id: Some(session.into()),
    })
    .await
    .unwrap()
    .unwrap()
    .sessions
}

async fn until<F: FnMut() -> bool>(what: &str, seconds: u64, mut condition: F) {
    tokio::time::timeout(Duration::from_secs(seconds), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

#[tokio::test]
async fn a_transient_delivery_failure_resumes_without_loss() {
    let tmp = tempfile::tempdir().unwrap();
    let (data, socket) = (tmp.path().join("data"), endpoint(tmp.path()));
    let script = Arc::new(Scripted::default());
    script.fail_flushes("flaky", 2);
    let daemon = start(&data, &socket, Some(script.clone())).await;

    for (index, name) in ["SessionStart", "PostToolUse", "Stop"].iter().enumerate() {
        send(
            &socket,
            &event("transient", "flaky", name, 1_000 + index as i64),
        )
        .await;
    }
    // The route holds its work in Recovering, then retries by itself.
    let mut saw_recovery = false;
    let done = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let sessions = status(&socket, "transient").await;
            saw_recovery |= sessions.iter().any(|s| s.recovery.is_some());
            if saw_recovery
                && sessions
                    .iter()
                    .all(|s| s.recovery.is_none() && s.queued == 0)
                && !script.span_ids("flaky").is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(done.is_ok(), "delivery never recovered");
    assert!(saw_recovery, "the failure was never visible as Recovering");

    // Nothing was lost: a second route-less session of the same events would
    // produce the same span ids, so compare against a healthy replay.
    let healthy = Arc::new(Scripted::default());
    let tmp2 = tempfile::tempdir().unwrap();
    let (data2, socket2) = (tmp2.path().join("data"), endpoint(tmp2.path()));
    let reference = start(&data2, &socket2, Some(healthy.clone())).await;
    for (index, name) in ["SessionStart", "PostToolUse", "Stop"].iter().enumerate() {
        send(
            &socket2,
            &event("transient", "flaky", name, 1_000 + index as i64),
        )
        .await;
    }
    assert!(
        flush_session("transient", &socket2, 5_000)
            .await
            .unwrap()
            .flushed
    );
    assert_eq!(script.span_ids("flaky"), healthy.span_ids("flaky"));
    shutdown_daemon(&socket2).await.unwrap();
    reference.await.unwrap();

    shutdown_daemon(&socket).await.unwrap();
    daemon.await.unwrap();
}

#[tokio::test]
async fn a_blocked_route_neither_loses_capture_nor_blocks_its_sibling() {
    let tmp = tempfile::tempdir().unwrap();
    let (data, socket) = (tmp.path().join("data"), endpoint(tmp.path()));
    let script = Arc::new(Scripted::default());
    script.blocked.lock().unwrap().insert("down".into());
    let daemon = start(&data, &socket, Some(script.clone())).await;

    for (index, name) in ["SessionStart", "PostToolUse", "Stop"].iter().enumerate() {
        for project in ["down", "up"] {
            send(
                &socket,
                &event("siblings", project, name, 1_000 + index as i64),
            )
            .await;
        }
    }
    // The healthy route delivers everything while the other is held.
    let sessions = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let sessions = status(&socket, "siblings").await;
            let held = sessions.iter().filter(|s| s.recovery.is_some()).count();
            let drained = sessions
                .iter()
                .any(|s| s.recovery.is_none() && s.queued == 0);
            if held == 1 && drained && !script.span_ids("up").is_empty() {
                break sessions;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the healthy route did not drain beside the held one");
    let held: Vec<_> = sessions.iter().filter(|s| s.recovery.is_some()).collect();
    assert_eq!(held[0].recovery.as_ref().unwrap().stage, "delivery");
    assert!(held[0].recovery.as_ref().unwrap().automatic);

    // Capture kept working throughout: every event is in the durable journal.
    assert_eq!(
        bt_daemon::read_journaled_events(&data, "debug", "siblings")
            .unwrap()
            .len(),
        6
    );

    // The held route's own condition clears; it delivers what it held.
    script.blocked.lock().unwrap().clear();
    until("the held route to catch up", 30, || {
        !script.span_ids("down").is_empty()
    })
    .await;
    assert_eq!(script.span_ids("down"), script.span_ids("up"));

    shutdown_daemon(&socket).await.unwrap();
    daemon.await.unwrap();
}

fn debug_spans(data: &Path, session: &str) -> String {
    std::fs::read_to_string(data.join(format!("spans/{session}.ndjson"))).unwrap_or_default()
}

#[tokio::test]
async fn held_work_survives_restart_and_age_and_resumes_when_its_condition_is_met() {
    let tmp = tempfile::tempdir().unwrap();
    let (data, socket) = (tmp.path().join("data"), endpoint(tmp.path()));
    let plugin = tmp.path().join("plugin.mjs");
    std::fs::write(&plugin, "export default () => { throw new Error('boom') }").unwrap();
    let with_plugin = |mut env: Envelope| {
        env.route
            .as_mut()
            .unwrap()
            .span_plugins
            .push(plugin.clone());
        env
    };

    let first = start(&data, &socket, None).await;
    // A finished session with nothing pending, and one blocked by its plugin.
    send(&socket, &event("finished", "p", "SessionStart", 1)).await;
    assert!(
        flush_session("finished", &socket, 5_000)
            .await
            .unwrap()
            .flushed
    );
    for (index, name) in ["SessionStart", "PostToolUse", "Stop"].iter().enumerate() {
        send(
            &socket,
            &with_plugin(event("blocked", "p", name, 10 + index as i64)),
        )
        .await;
    }
    until("the plugin failure to be held", 10, || {
        std::fs::read_to_string(data.join("diagnostics/span-plugin-errors.json"))
            .is_ok_and(|text| text.contains("boom"))
    })
    .await;
    shutdown_daemon(&socket).await.unwrap();
    first.await.unwrap();

    // Both partitions look a month old. Only the one with nothing pending may
    // be collected; stuck work is never deleted.
    let old = 1_000;
    rusqlite::Connection::open(data.join("pipeline/pipeline.sqlite"))
        .unwrap()
        .execute("UPDATE partitions SET last_activity_ms = ?1", [old])
        .unwrap();
    let second = start(&data, &socket, None).await;
    assert!(
        bt_daemon::read_journaled_events(&data, "debug", "finished")
            .unwrap()
            .is_empty(),
        "a finished session past retention is collected"
    );
    assert_eq!(
        bt_daemon::read_journaled_events(&data, "debug", "blocked")
            .unwrap()
            .len(),
        3,
        "held work is never collected"
    );

    // The persisted failure restarts in Recovering and says how to unblock.
    let held = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let sessions = status(&socket, "blocked").await;
            if let Some(recovery) = sessions.iter().find_map(|s| s.recovery.clone()) {
                break recovery;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the persisted failure did not restart in Recovering");
    assert_eq!(held.cause, "span plugin");
    assert!(!held.automatic);
    assert!(held.guidance.contains("plugin.mjs"), "{}", held.guidance);

    // Saving the plugin is the recovery condition. Delivery resumes from the
    // last durable span and the failure clears.
    std::fs::write(&plugin, "export default span => span").unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let sessions = status(&socket, "blocked").await;
            if sessions
                .iter()
                .all(|s| s.recovery.is_none() && s.queued == 0)
                && debug_spans(&data, "blocked").contains("\"Insert\"")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("fixing the plugin did not resume delivery");
    assert!(!debug_spans(&data, "blocked").contains("boom"));

    shutdown_daemon(&socket).await.unwrap();
    second.await.unwrap();
}
