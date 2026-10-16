#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "Integration test assertions and fixtures fail immediately on unexpected values."
)]

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

// Combine a legacy checkpoint with append-only updates for storage assertions.
fn ledger_state(path: &Path) -> Value {
    let mut state = json!({
        "completed_span_ids": [], "late_merge_ids": [], "span_origins": {}
    });
    let mut records = Vec::new();
    if let Ok(bytes) = std::fs::read(path.with_extension("json")) {
        records.push(serde_json::from_slice::<Value>(&bytes).unwrap());
    }
    if let Ok(text) = std::fs::read_to_string(path.with_extension("ndjson")) {
        records.extend(
            text.lines()
                .map(|line| serde_json::from_str::<Value>(line).unwrap()),
        );
    }
    for record in records {
        for key in ["completed_span_ids", "late_merge_ids"] {
            if let Some(ids) = record[key].as_array() {
                state[key]
                    .as_array_mut()
                    .unwrap()
                    .extend(ids.iter().cloned());
            }
        }
        if let Some(origins) = record["span_origins"].as_object() {
            state["span_origins"]
                .as_object_mut()
                .unwrap()
                .extend(origins.clone());
        }
    }
    state
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn span_birth_snapshots_survive_version_changes_and_daemon_restart() {
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
        let tool = session_rows
            .iter()
            .find(|row| row["span_attributes"]["type"] == "tool")
            .expect("tool span was not ingested")["span_id"]
            .as_str()
            .unwrap()
            .to_owned();
        sessions.push((
            session,
            birth_plugin,
            birth_source,
            root,
            turn1,
            turn2,
            tool,
        ));
    }
    first.stop().await;

    let second = Daemon::start(tmp.path(), &server, "bt2").await;
    for (session, birth_plugin, birth_source, root, turn1, turn2, tool) in sessions {
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
        // Journal resume does not repeat old inserts. Newly captured native
        // closure may still refine defensive retirement timestamps.
        for row in &rows[before..] {
            if row["span_id"] == root {
                assert_eq!(row["_is_merge"], true);
                assert_eq!(row["metrics"]["end"], 4.0);
            }
            if row["span_id"] == turn2 {
                assert_eq!(row["_is_merge"], true);
                assert_eq!(row["metrics"]["end"], 3.0);
            }
            assert_ne!(
                row["span_id"], turn1,
                "fully resolved history must not be repeated"
            );
        }
        assert_provenance(
            &rows[before..],
            &turn3,
            "claude-code",
            Some("v3"),
            Some("native3"),
            "bt2",
        );
        // Check all historical rows once: updates must retain birth versions,
        // including the tool closed by an event with newer capture versions.
        for (span, plugin_version, source_version) in [
            (&root, birth_plugin, birth_source),
            (&turn1, birth_plugin, birth_source),
            (&turn2, Some("v2"), Some("native2")),
            (&tool, Some("v2"), Some("native2")),
        ] {
            assert_provenance(
                &rows,
                span,
                "claude-code",
                plugin_version,
                source_version,
                "bt1",
            );
        }
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
    // Model a released pre-SpanOp-journal daemon: accepted native history and
    // a legacy delivery checkpoint, with no newer durable partition.
    let raw = std::fs::read_to_string(&journal).unwrap();
    let mut history = raw
        .lines()
        .filter(|line| !line.contains("_bt_record_type"))
        .map(|line| format!("{line}\n"))
        .collect::<String>();
    let through = history.len() as u64;
    history.push_str(&format!("{}\n",json!({"_bt_record_type":"delivery_checkpoint", "route":capture("UserPromptSubmit","v1","native1",1_000).route, "through":through})));
    std::fs::write(&journal, &history).unwrap();
    std::fs::remove_dir_all(data_dir.join("pipeline")).unwrap();
    let acknowledged_history = history.into_bytes();
    let ledger_paths: Vec<_> = std::fs::read_dir(data_dir.join("delivery-ledger"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "ndjson"))
        .collect();
    assert_eq!(ledger_paths.len(), 1);
    let ledger_path = ledger_paths[0].with_extension("json");
    let mut ledger = ledger_state(&ledger_path);
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
    std::fs::write(&ledger_path, serde_json::to_vec(&ledger).unwrap()).unwrap();
    std::fs::remove_file(ledger_path.with_extension("ndjson")).unwrap();
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
    let ledger = ledger_state(&ledger_path);
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
    let pending = flush_session(session, &first.socket, 250).await.unwrap();
    assert!(
        pending.flushed && pending.pending == 0,
        "a provisional root must not hold the delivery boundary open"
    );
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
