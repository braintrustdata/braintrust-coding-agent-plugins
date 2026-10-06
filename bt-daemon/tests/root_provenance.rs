//! Consumer-visible provenance through capture, journal replay, dispatch, and HTTP ingest.

use async_trait::async_trait;
use braintrust_sdk_rust::{SpanComponents, SpanObjectType};
use bt_daemon::wire::{AuthSelection, BackendAuth, Envelope, SessionRoute, TraceDestination};
use bt_daemon::{
    flush_session, forward_envelope, run_serve, run_status, shutdown_daemon, source_journal_path,
    AuthLease, AuthProvider, AuthResolveReason, BraintrustSinkConfig, BraintrustSinkFactory,
    HostInfo, Registry, ServeArgs, ServeOptions, StatusArgs,
};
use serde_json::{json, Value};
use std::ffi::OsString;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

struct BackendAuthProvider(String);

#[async_trait]
impl AuthProvider for BackendAuthProvider {
    async fn resolve(
        &self,
        selection: &AuthSelection,
        _reason: AuthResolveReason,
    ) -> anyhow::Result<AuthLease> {
        Ok(AuthLease {
            selection: selection.clone().canonicalized()?,
            auth: BackendAuth {
                token: "sk-test".into(),
                api_url: Some(self.0.clone()),
                app_url: Some(self.0.clone()),
                org_name: Some("acme".into()),
                org_id: Some("org-1".into()),
            },
            expires_at_ms: None,
        })
    }
}

async fn mock_backend() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/version"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/project/register"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({ "project": { "id": "proj-1" } })),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/logs3"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&server)
        .await;
    server
}

fn test_endpoint(tmp: &Path) -> PathBuf {
    #[cfg(unix)]
    {
        tmp.join("d.sock")
    }
    #[cfg(windows)]
    {
        let _ = tmp;
        PathBuf::from(format!(r"\\.\pipe\bt-root-origin-{}", uuid::Uuid::new_v4()))
    }
}

struct Daemon {
    socket: PathBuf,
    host: HostInfo,
    task: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl Daemon {
    async fn start(tmp: &Path, server: &MockServer, version: &str) -> Self {
        let socket = test_endpoint(tmp);
        let args = ServeArgs {
            socket: Some(socket.clone()),
            data_dir: Some(tmp.join("data")),
            idle_timeout_secs: 0,
            session_idle_timeout_secs: 0,
        };
        let opts = ServeOptions {
            version: version.into(),
            translators: Arc::new(Registry::default_agents()),
            sink_factory: Arc::new(BraintrustSinkFactory::new(BraintrustSinkConfig {
                api_url: Some(server.uri()),
                app_url: Some(server.uri()),
            })),
            auth_provider: Some(Arc::new(BackendAuthProvider(server.uri()))),
        };
        let task = tokio::spawn(run_serve(args, opts));
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(Some(_)) = run_status(StatusArgs {
                    socket: Some(socket.clone()),
                    session_id: None,
                })
                .await
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("daemon did not become ready");
        Self {
            socket,
            host: HostInfo {
                serve_argv: vec![OsString::from("unused")],
                version: version.into(),
            },
            task,
        }
    }

    async fn send(&self, event: &Envelope) {
        forward_envelope(event, &self.socket, &self.host, true)
            .await
            .unwrap();
    }

    async fn flush(&self, session: &str) {
        let result = flush_session(session, &self.socket, 5_000).await.unwrap();
        assert!(result.flushed, "delivery did not drain: {result:?}");
        assert_eq!(result.pending, 0, "pending delivery: {result:?}");
    }

    async fn stop(self) {
        shutdown_daemon(&self.socket).await.unwrap();
        self.task.await.unwrap().unwrap();
    }
}

fn event(
    session: &str,
    source: &str,
    name: &str,
    version: Option<&str>,
    source_version: Option<&str>,
    ts_ms: i64,
) -> Envelope {
    Envelope {
        source: source.into(),
        source_version: source_version.map(str::to_owned),
        plugin_version: version.map(str::to_owned),
        session_id: session.into(),
        event: name.into(),
        ts_ms,
        managed_run_id: None,
        payload: json!({
            "session_id": session,
            "hook_event_name": name,
            "prompt": format!("prompt at {ts_ms}"),
            "prompt_id": format!("prompt-{ts_ms}"),
        }),
        route: Some(SessionRoute {
            destination: Some(TraceDestination::ProjectLogs {
                project_id: Some("proj-1".into()),
                project_name: None,
            }),
            ..SessionRoute::default()
        }),
        config: None,
        capture: None,
    }
}

async fn logs3_rows(server: &MockServer) -> Vec<Value> {
    server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|request| request.url.path() == "/logs3")
        .flat_map(|request| {
            serde_json::from_slice::<Value>(&request.body).unwrap()["rows"]
                .as_array()
                .expect("logs3 request must contain rows")
                .clone()
        })
        .collect()
}

fn root_id(rows: &[Value], session: &str) -> String {
    rows.iter()
        .find(|row| row["metadata"]["session_id"] == session)
        .expect("session root was not ingested")["span_id"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn assert_provenance(
    rows: &[Value],
    span: &str,
    source: &str,
    plugin_version: Option<&str>,
    source_version: Option<&str>,
    bt_version: &str,
) {
    let spans: Vec<_> = rows.iter().filter(|row| row["span_id"] == span).collect();
    assert!(
        !spans.is_empty(),
        "expected ingested insert/update for {span}"
    );
    for row in spans {
        let origin = &row["context"]["span_origin"];
        assert_eq!(origin["name"], format!("braintrust.plugin.{source}"));
        // get() distinguishes explicit null from a missing version field.
        assert_eq!(origin.get("version"), Some(&json!(plugin_version)));
        assert_eq!(origin[source].get("version"), Some(&json!(source_version)));
        assert!(origin.get("agent").is_none(), "generic agent key: {row}");
        assert_eq!(origin["bt"]["version"], bt_version);
        assert_eq!(origin["instrumentation"]["name"], "braintrust-plugin");
        assert!(row["metadata"].get("bt_daemon_version").is_none());
    }
}

fn turn_id(rows: &[Value], prompt_ts: i64) -> String {
    rows.iter()
        .find(|row| row["input"] == format!("prompt at {prompt_ts}"))
        .expect("turn was not ingested")["span_id"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn assert_non_turn_provenance(rows: &[Value], root: &str, turns: &[&str]) {
    let children: Vec<_> = rows
        .iter()
        .filter(|row| {
            row["root_span_id"] == root
                && row["span_id"] != root
                && !turns.iter().any(|turn| row["span_id"] == *turn)
        })
        .collect();
    assert!(!children.is_empty(), "expected ingested non-turn work");
    for row in children {
        if let Some(origin) = row["context"].get("span_origin") {
            assert_eq!(origin["name"], "braintrust.sdk.rust");
            assert!(origin.get("bt").is_none());
            assert!(origin.get("claude-code").is_none());
            assert!(origin.get("agent").is_none());
            assert_ne!(origin["instrumentation"]["name"], "braintrust-plugin");
        } else {
            assert_eq!(row["_is_merge"], true, "insert omitted SDK origin: {row}");
        }
        assert!(row["metadata"].get("bt_daemon_version").is_none());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn root_and_turn_birth_snapshots_survive_version_changes_and_daemon_restart() {
    let server = mock_backend().await;
    let tmp = tempfile::tempdir().unwrap();
    let plugin = tmp.path().join("origin-plugin.mjs");
    std::fs::write(
        &plugin,
        "export default span => ({...span, metadata: {...span.metadata, origin_plugin: true}})",
    )
    .unwrap();
    let capture = |session, name, plugin_version, source_version, ts_ms| {
        let mut envelope = event(
            session,
            "claude-code",
            name,
            plugin_version,
            source_version,
            ts_ms,
        );
        envelope.route.as_mut().unwrap().span_plugins = vec![plugin.clone()];
        envelope
    };
    let first = Daemon::start(tmp.path(), &server, "bt1").await;
    let mut sessions = Vec::new();
    for (session, birth_plugin, birth_source) in [
        ("known-birth", Some("v1"), Some("native1")),
        ("unknown-birth", None, None),
    ] {
        first
            .send(&capture(
                session,
                "UserPromptSubmit",
                birth_plugin,
                birth_source,
                1_000,
            ))
            .await;
        first.flush(session).await;
        let rows = logs3_rows(&server).await;
        let root = root_id(&rows, session);
        let session_rows: Vec<_> = rows
            .into_iter()
            .filter(|row| row["root_span_id"] == root)
            .collect();
        let turn1 = turn_id(&session_rows, 1_000);
        assert_provenance(
            &session_rows,
            &root,
            "claude-code",
            birth_plugin,
            birth_source,
            "bt1",
        );
        assert_provenance(
            &session_rows,
            &turn1,
            "claude-code",
            birth_plugin,
            birth_source,
            "bt1",
        );

        // Opening a new turn also updates the previous turn. Neither unknown
        // nor known birth versions may be overwritten by this newer capture.
        first
            .send(&capture(
                session,
                "UserPromptSubmit",
                Some("v2"),
                Some("native2"),
                2_000,
            ))
            .await;
        let mut tool = capture(session, "PreToolUse", Some("v2"), Some("native2"), 2_100);
        tool.payload["tool_use_id"] = json!("tool-1");
        tool.payload["tool_name"] = json!("Read");
        tool.payload["tool_input"] = json!({"file_path": "/tmp/example"});
        first.send(&tool).await;
        let mut completed = tool;
        completed.event = "PostToolUse".into();
        completed.payload["hook_event_name"] = json!("PostToolUse");
        completed.payload["tool_response"] = json!("file contents");
        completed.plugin_version = Some("v3".into());
        completed.source_version = Some("native3".into());
        completed.ts_ms = 2_200;
        first.send(&completed).await;
        first.flush(session).await;
        let rows = logs3_rows(&server).await;
        let session_rows: Vec<_> = rows
            .into_iter()
            .filter(|row| row["root_span_id"] == root)
            .collect();
        let turn2 = turn_id(&session_rows, 2_000);
        assert_ne!(turn1, turn2);
        assert_provenance(
            &session_rows,
            &root,
            "claude-code",
            birth_plugin,
            birth_source,
            "bt1",
        );
        assert_provenance(
            &session_rows,
            &turn1,
            "claude-code",
            birth_plugin,
            birth_source,
            "bt1",
        );
        assert_provenance(
            &session_rows,
            &turn2,
            "claude-code",
            Some("v2"),
            Some("native2"),
            "bt1",
        );
        assert_non_turn_provenance(&session_rows, &root, &[&turn1, &turn2]);
        assert!(session_rows
            .iter()
            .any(|row| { row["span_id"] == turn2 && row["metadata"]["origin_plugin"] == true }));
        sessions.push((session, birth_plugin, birth_source, root, turn1, turn2));
    }
    first.stop().await;

    let second = Daemon::start(tmp.path(), &server, "bt2").await;
    for (session, birth_plugin, birth_source, root, turn1, turn2) in sessions {
        let before = logs3_rows(&server).await.len();
        second
            .send(&capture(
                session,
                "UserPromptSubmit",
                Some("v3"),
                Some("native3"),
                3_000,
            ))
            .await;
        second
            .send(&capture(
                session,
                "SessionEnd",
                Some("v4"),
                Some("native4"),
                4_000,
            ))
            .await;
        second.flush(session).await;
        let rows = logs3_rows(&server).await;
        let turn3 = turn_id(&rows[before..], 3_000);
        assert_ne!(turn3, turn1);
        assert_ne!(turn3, turn2);
        // Graceful shutdown completed these spans; recovery must not redeliver them.
        assert!(rows[before..].iter().all(|row| {
            row["span_id"] != root && row["span_id"] != turn1 && row["span_id"] != turn2
        }));
        assert_provenance(
            &rows[before..],
            &turn3,
            "claude-code",
            Some("v3"),
            Some("native3"),
            "bt2",
        );
        // Check every exported update, including any replayed history, rather
        // than just the final merged row visible to a consumer.
        assert_provenance(
            &rows,
            &root,
            "claude-code",
            birth_plugin,
            birth_source,
            "bt1",
        );
        assert_provenance(
            &rows,
            &turn1,
            "claude-code",
            birth_plugin,
            birth_source,
            "bt1",
        );
        assert_provenance(
            &rows,
            &turn2,
            "claude-code",
            Some("v2"),
            Some("native2"),
            "bt1",
        );
        assert_non_turn_provenance(&rows, &root, &[&turn1, &turn2, &turn3]);
        assert!(rows[before..]
            .iter()
            .any(|row| { row["span_id"] == turn3 && row["metadata"]["origin_plugin"] == true }));
    }
    second.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_acknowledged_spans_receive_late_updates_without_inventing_origins() {
    let server = mock_backend().await;
    let tmp = tempfile::tempdir().unwrap();
    let session = "legacy-origin";
    let capture = |name, plugin_version, source_version, ts_ms| {
        event(
            session,
            "claude-code",
            name,
            Some(plugin_version),
            Some(source_version),
            ts_ms,
        )
    };
    let first = Daemon::start(tmp.path(), &server, "bt1").await;
    first
        .send(&capture("UserPromptSubmit", "v1", "native1", 1_000))
        .await;
    first.flush(session).await;
    // Shutdown completes the spans, but the acknowledged journal still has an
    // active turn. A later Stop must deliver new output, not replay old inserts.
    first.stop().await;
    let initial_rows = logs3_rows(&server).await;
    let root = root_id(&initial_rows, session);
    let old_turn = turn_id(&initial_rows, 1_000);
    for span in [&root, &old_turn] {
        assert_provenance(
            &initial_rows,
            span,
            "claude-code",
            Some("v1"),
            Some("native1"),
            "bt1",
        );
        assert!(initial_rows
            .iter()
            .any(|row| row["span_id"] == *span && row["metrics"]["end"] == 1.0));
    }

    let data_dir = tmp.path().join("data");
    let journal = source_journal_path(&data_dir, "claude-code", session);
    let acknowledged_history = std::fs::read(&journal).unwrap();
    assert!(std::str::from_utf8(&acknowledged_history)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .any(|record| {
            record["_bt_record_type"] == "delivery_checkpoint"
                && record["through"].as_u64().is_some_and(|offset| offset > 0)
        }));
    let ledger_paths: Vec<_> = std::fs::read_dir(data_dir.join("delivery-ledger"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert_eq!(ledger_paths.len(), 1);
    let ledger_path = &ledger_paths[0];
    let mut ledger: Value = serde_json::from_slice(&std::fs::read(ledger_path).unwrap()).unwrap();
    let completed = ledger["completed_span_ids"].clone();
    for span in [&root, &old_turn] {
        assert!(completed.as_array().unwrap().contains(&json!(span)));
        assert!(ledger["span_origins"].get(span.as_str()).is_some());
    }
    // Simulate the old schema, preserving all completion/late-merge IDs and
    // journal checkpoints. Missing provenance is not a snapshot of null versions.
    let ledger_object = ledger.as_object_mut().unwrap();
    ledger_object.remove("span_origins");
    ledger_object.remove("root_origins");
    assert_eq!(ledger["completed_span_ids"], completed);
    assert!(ledger.get("span_origins").is_none());
    assert!(ledger.get("root_origins").is_none());
    std::fs::write(ledger_path, serde_json::to_vec(&ledger).unwrap()).unwrap();
    assert_eq!(std::fs::read(&journal).unwrap(), acknowledged_history);

    let second = Daemon::start(tmp.path(), &server, "bt2").await;
    let mut late_stop = capture("Stop", "v2", "native2", 2_000);
    late_stop.payload["last_assistant_message"] = json!("late legacy answer");
    second.send(&late_stop).await;
    second.flush(session).await;
    let rows = logs3_rows(&server).await;
    let late_rows = &rows[initial_rows.len()..];
    for span in [&root, &old_turn] {
        let updates: Vec<_> = late_rows
            .iter()
            .filter(|row| row["span_id"] == *span)
            .collect();
        assert!(!updates.is_empty(), "missing late legacy update for {span}");
        assert!(updates.iter().any(|row| row["metrics"]["end"] == 2.0));
        for row in updates {
            assert_eq!(row["_is_merge"], true, "historical insert replayed: {row}");
            assert!(
                row["context"].get("span_origin").is_none(),
                "legacy update must omit provenance entirely, not set null: {row}"
            );
        }
    }
    assert!(late_rows
        .iter()
        .any(|row| { row["span_id"] == old_turn && row["output"] == "late legacy answer" }));

    second
        .send(&capture("UserPromptSubmit", "v3", "native3", 3_000))
        .await;
    let mut stop = capture("Stop", "v4", "native4", 4_000);
    stop.payload["last_assistant_message"] = json!("fresh answer");
    second.send(&stop).await;
    second.flush(session).await;
    second.stop().await;
    let rows = logs3_rows(&server).await;
    let fresh_turn = turn_id(&rows[initial_rows.len()..], 3_000);
    assert_ne!(fresh_turn, old_turn);
    assert_provenance(
        &rows,
        &fresh_turn,
        "claude-code",
        Some("v3"),
        Some("native3"),
        "bt2",
    );
    assert!(rows.iter().any(|row| {
        row["span_id"] == fresh_turn
            && row["output"] == "fresh answer"
            && row["metrics"]["end"] == 4.0
    }));

    // Repeated recovery must not turn historical capture versions into a
    // fabricated birth snapshot, or restore SDK provenance on the next merge.
    let before_restart = rows.len();
    let third = Daemon::start(tmp.path(), &server, "bt3").await;
    third
        .send(&capture("SessionEnd", "v5", "native5", 5_000))
        .await;
    third.flush(session).await;
    third.stop().await;
    let rows = logs3_rows(&server).await;
    assert!(rows[before_restart..]
        .iter()
        .any(|row| row["span_id"] == root && row["metrics"]["end"] == 5.0));
    for row in rows[initial_rows.len()..]
        .iter()
        .filter(|row| row["span_id"] == root || row["span_id"] == old_turn)
    {
        assert_eq!(row["_is_merge"], true);
        assert!(
            row["context"].get("span_origin").is_none(),
            "legacy origin was invented after recovery: {row}"
        );
    }
    let ledger: Value = serde_json::from_slice(&std::fs::read(ledger_path).unwrap()).unwrap();
    for key in ["span_origins", "root_origins"] {
        for span in [&root, &old_turn] {
            assert!(
                ledger[key].get(span.as_str()).is_none(),
                "legacy birth snapshot was invented in {key}: {ledger}"
            );
        }
    }
    assert!(ledger["span_origins"].get(fresh_turn.as_str()).is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn newer_capture_replays_unacknowledged_root_with_its_journaled_plugin_version() {
    let server = mock_backend().await;
    let tmp = tempfile::tempdir().unwrap();
    let daemon = Daemon::start(tmp.path(), &server, "bt1").await;
    let session = "unacknowledged-history";
    let birth = event(
        session,
        "claude-code",
        "UserPromptSubmit",
        Some("v1"),
        Some("native1"),
        1_000,
    );
    // Seed the capture-before-dispatch crash window after startup recovery has
    // finished. The newer live event, not startup, must trigger history replay.
    let journal = source_journal_path(&tmp.path().join("data"), "claude-code", session);
    std::fs::create_dir_all(journal.parent().unwrap()).unwrap();
    let mut raw = serde_json::to_vec(&birth.redacted()).unwrap();
    raw.push(b'\n');
    std::fs::write(journal, raw).unwrap();
    daemon
        .send(&event(
            session,
            "claude-code",
            "SessionEnd",
            Some("v2"),
            Some("native2"),
            2_000,
        ))
        .await;
    daemon.flush(session).await;
    let rows = logs3_rows(&server).await;
    let root = root_id(&rows, session);
    assert_provenance(
        &rows,
        &root,
        "claude-code",
        Some("v1"),
        Some("native1"),
        "bt1",
    );
    let turn = turn_id(&rows, 1_000);
    assert_provenance(
        &rows,
        &turn,
        "claude-code",
        Some("v1"),
        Some("native1"),
        "bt1",
    );
    assert!(rows.iter().any(|row| row["input"] == "prompt at 1000"));
    daemon.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn attached_provisional_root_keeps_birth_snapshot_until_work_after_restart() {
    let server = mock_backend().await;
    let tmp = tempfile::tempdir().unwrap();
    let transcript = tmp.path().join("codex.jsonl");
    let session = "attached-provisional";
    let metadata = json!({
        "timestamp": "2026-01-01T00:00:01Z",
        "type": "session_meta",
        "payload": { "id": session, "cli_version": "native-codex-version" }
    });
    std::fs::write(&transcript, format!("{metadata}\n")).unwrap();
    let mut components = SpanComponents::new(SpanObjectType::ProjectLogs);
    components.object_id = Some("proj-1".into());
    components.span_id = Some("external-parent".into());
    components.root_span_id = Some("external-root".into());
    let mut birth = event(
        session,
        "codex",
        "SessionStart",
        Some("v1"),
        Some("native1"),
        1_767_225_601_000,
    );
    birth.payload = json!({
        "session_id": session,
        "hook_event_name": "SessionStart",
        "transcript_path": transcript,
        "source": "startup"
    });
    birth.route.as_mut().unwrap().destination = Some(TraceDestination::ParentSpan { components });

    let first = Daemon::start(tmp.path(), &server, "bt1").await;
    first.send(&birth).await;
    first.flush(session).await;
    assert!(
        logs3_rows(&server).await.is_empty(),
        "provisional root exported before work"
    );
    first.stop().await;
    assert!(
        logs3_rows(&server).await.is_empty(),
        "shutdown exported provisional root"
    );

    let second = Daemon::start(tmp.path(), &server, "bt2").await;
    let work = json!({
        "timestamp": "2026-01-01T00:00:02Z",
        "type": "event_msg",
        "payload": { "type": "task_started", "turn_id": "turn-after-upgrade" }
    });
    writeln!(
        std::fs::OpenOptions::new()
            .append(true)
            .open(&transcript)
            .unwrap(),
        "{work}"
    )
    .unwrap();
    let mut capture = birth;
    capture.plugin_version = Some("v2".into());
    capture.source_version = Some("native2".into());
    capture.event = "Stop".into();
    capture.ts_ms += 1_000;
    capture.payload["hook_event_name"] = json!("Stop");
    second.send(&capture).await;
    second.flush(session).await;
    let rows = logs3_rows(&server).await;
    let root = root_id(&rows, session);
    assert_ne!(root, "external-parent");
    assert_ne!(root, "external-root");
    assert_provenance(&rows, &root, "codex", Some("v1"), Some("native1"), "bt1");
    let turn = rows
        .iter()
        .find(|row| row["span_id"] != root)
        .expect("turn after restart was not ingested")["span_id"]
        .as_str()
        .unwrap();
    assert_provenance(&rows, turn, "codex", Some("v2"), Some("native2"), "bt2");
    for row in &rows {
        assert_eq!(row["root_span_id"], "external-root");
        if row["span_id"] == root {
            assert_eq!(row["span_parents"], json!(["external-parent"]));
        } else {
            assert_eq!(row["span_parents"], json!([root]));
        }
    }
    second.stop().await;
}
