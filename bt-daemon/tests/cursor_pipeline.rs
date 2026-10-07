//! Cursor hooks freeze native transcript observations before journal ingress.
//! Recovery must use those mirrors even after native files change or disappear.

use async_trait::async_trait;
use bt_daemon::wire::{
    AuthSelection, BackendAuth, Envelope, FlushMode, SessionRoute, TraceDestination,
};
use bt_daemon::{
    debug_serve_options, flush_session, forward_envelope, run_serve, run_status, shutdown_daemon,
    source_journal_path, AuthLease, AuthProvider, AuthResolveReason, HostInfo, ServeArgs, Sink,
    SinkFactory, SpanOp, StatusArgs,
};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Smoke-test Cursor's portable direct command through the native Windows
/// shell. This checks executable lookup, stdin forwarding, and its JSON
/// response without relying on a POSIX-only launcher.
#[cfg(windows)]
#[allow(
    clippy::disallowed_methods,
    reason = "Exercises a foreground command exactly as Cursor invokes Windows hooks."
)]
#[test]
fn cursor_direct_hook_command_receives_native_stdin_on_windows() {
    use std::io::Write;
    use std::process::{Command, Stdio};

    let tmp = tempfile::tempdir().unwrap();
    let args_path = tmp.path().join("args.txt");
    let input_path = tmp.path().join("input.json");
    let shim = tmp.path().join("bt.cmd");
    std::fs::write(
        &shim,
        "@echo off\r\nif /I not \"%~1 %~2\"==\"trace hook\" exit /b 13\r\n> \"%CURSOR_HOOK_ARGS_FILE%\" echo %*\r\nset /p BT_EVENT=\r\n> \"%CURSOR_HOOK_INPUT_FILE%\" echo %BT_EVENT%\r\necho {}\r\nexit /b 0\r\n",
    )
    .unwrap();
    let path = std::env::join_paths(std::iter::once(tmp.path().to_owned()).chain(
        std::env::split_paths(&std::env::var_os("PATH").expect("Windows PATH is present")),
    ))
    .unwrap();
    let command = "bt trace hook --source cursor --event afterAgentResponse --session-id-field conversation_id --event-field hook_event_name --transcript-path-field transcript_path --flush-on-turn-end --capture-timeout-ms 8000";
    let payload = r#"{"conversation_id":"windows-smoke","hook_event_name":"afterAgentResponse","transcript_path":null}"#;
    let mut child = Command::new("cmd.exe")
        .args(["/d", "/s", "/c", command])
        .env("PATH", path)
        .env("CURSOR_HOOK_ARGS_FILE", &args_path)
        .env("CURSOR_HOOK_INPUT_FILE", &input_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(format!("{payload}\n").as_bytes())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).trim() == "{}",
        "Cursor CLI hook response must be valid JSON"
    );
    let args = std::fs::read_to_string(args_path).unwrap();
    assert!(args.starts_with("trace hook --source cursor "), "{args}");
    let received: Value =
        serde_json::from_str(&std::fs::read_to_string(input_path).unwrap()).unwrap();
    assert_eq!(received, serde_json::from_str::<Value>(payload).unwrap());
}

struct TestAuth;

#[cfg(all(feature = "cli", unix))]
#[allow(
    clippy::disallowed_methods,
    reason = "Runs a foreground Cursor hook to exercise its policy response deadline."
)]
async fn assert_stalled_daemon_preserves_policy_response(
    event: &str,
    stall_after_initialize: bool,
) {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let tmp = tempfile::tempdir().unwrap();
    let socket = endpoint(tmp.path());
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (read, mut write) = stream.into_split();
        let mut lines = BufReader::new(read).lines();
        let initialize: Value =
            serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
        assert_eq!(initialize["method"], "initialize");
        if stall_after_initialize {
            let response = json!({
                "jsonrpc":"2.0", "id":initialize["id"],
                "result":{
                    "protocol_version":1, "daemon_version":env!("CARGO_PKG_VERSION")
                }
            });
            write
                .write_all(format!("{response}\n").as_bytes())
                .await
                .unwrap();
            let event: Value =
                serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
            assert_eq!(event["method"], "event.log");
            assert_eq!(
                event["params"]["payload"]["conversation_id"],
                "stalled-cursor"
            );
        }
        reached_tx.send(()).unwrap();
        // Keep the socket connected while withholding the matching RPC response.
        std::future::pending::<()>().await;
    });
    let plugin = Path::new(env!("CARGO_MANIFEST_DIR")).join("../src/plugins/cursor");
    let started = tokio::time::Instant::now();
    // This is a foreground hook invocation, with the same script Cursor executes.
    let mut child = tokio::process::Command::new("/bin/sh")
        .arg(plugin.join("content/hooks/trace.sh"))
        .arg(event)
        .env("BT_BIN", plugin.join("test/bt-standalone-wrapper.sh"))
        .env("BT_DAEMON_BIN", env!("CARGO_BIN_EXE_bt-daemon"))
        .env("BT_DAEMON_SOCKET", &socket)
        .env("BT_DAEMON_CONFIG", tmp.path().join("settings.json"))
        .env("BT_DAEMON_DATA_DIR", tmp.path().join("data"))
        .env("CURSOR_PLUGIN_ROOT", plugin.join("content"))
        .env_remove("_BT_TRACE_MANAGED_RUN")
        .env(
            "BT_TRACE_INVOCATION_SETTINGS",
            json!({
                "trace_to_braintrust":true,
                "route":{"destination":{"type":"project_logs","project_name":"cursor-tests"}}
            })
            .to_string(),
        )
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(
            json!({
                "conversation_id":"stalled-cursor", "hook_event_name":event, "transcript_path":null
            })
            .to_string()
            .as_bytes(),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), reached_rx)
        .await
        .expect("hook never reached the stalled RPC")
        .unwrap();
    let output = tokio::time::timeout(Duration::from_secs(9), child.wait_with_output()).await;
    server.abort();
    let _ = server.await;
    let output = output
        .expect("capture exceeded Cursor's policy response deadline")
        .unwrap();
    assert!(output.status.success());
    assert!(started.elapsed() < Duration::from_secs(10));
    let expected = if event == "beforeSubmitPrompt" {
        json!({"continue":true})
    } else {
        json!({})
    };
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout).unwrap(),
        expected
    );
    assert_eq!(
        String::from_utf8(output.stderr).unwrap(),
        "trace-cursor: event capture unavailable; continuing.\n"
    );
}

#[cfg(all(feature = "cli", unix))]
#[tokio::test]
async fn stalled_initialize_keeps_cursor_tool_observation_fail_open() {
    assert_stalled_daemon_preserves_policy_response("postToolUse", false).await;
}

#[cfg(all(feature = "cli", unix))]
#[tokio::test]
async fn stalled_capture_acknowledgement_still_allows_cursor_prompt_submission() {
    assert_stalled_daemon_preserves_policy_response("beforeSubmitPrompt", true).await;
}

#[async_trait]
impl AuthProvider for TestAuth {
    async fn resolve(
        &self,
        selection: &AuthSelection,
        _: AuthResolveReason,
    ) -> anyhow::Result<AuthLease> {
        Ok(AuthLease {
            selection: selection.clone(),
            auth: BackendAuth {
                token: "cursor-test-secret".into(),
                api_url: None,
                app_url: None,
                org_name: None,
                org_id: None,
            },
            expires_at_ms: None,
        })
    }
}

fn endpoint(dir: &Path) -> PathBuf {
    #[cfg(unix)]
    {
        dir.join("cursor.sock")
    }
    #[cfg(windows)]
    {
        let _ = dir;
        PathBuf::from(format!(r"\\.\pipe\bt-cursor-{}", uuid::Uuid::new_v4()))
    }
}

async fn start(data: &Path, socket: &Path) -> tokio::task::JoinHandle<()> {
    start_with_sink(data, socket, None).await
}

async fn start_with_sink(
    data: &Path,
    socket: &Path,
    sink_factory: Option<Arc<dyn SinkFactory>>,
) -> tokio::task::JoinHandle<()> {
    let args = ServeArgs {
        data_dir: Some(data.into()),
        socket: Some(socket.into()),
        idle_timeout_secs: 0,
        session_idle_timeout_secs: 0,
    };
    let mut options = debug_serve_options("test", data);
    options.auth_provider = Some(Arc::new(TestAuth));
    if let Some(sink_factory) = sink_factory {
        options.sink_factory = sink_factory;
    }
    let task = tokio::spawn(async move {
        run_serve(args, options).await.unwrap();
    });
    for _ in 0..200 {
        if run_status(StatusArgs {
            socket: Some(socket.into()),
            session_id: None,
        })
        .await
        .is_ok_and(|status| status.is_some())
        {
            return task;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("Cursor test daemon never answered");
}

async fn send(socket: &Path, event: &str, ts_ms: i64, payload: Value) {
    send_with_flush_mode(socket, event, ts_ms, payload, FlushMode::default()).await;
}

async fn send_with_flush_mode(
    socket: &Path,
    event: &str,
    ts_ms: i64,
    mut payload: Value,
    flush_mode: FlushMode,
) {
    payload["conversation_id"] = json!("cursor-recovery");
    payload["hook_event_name"] = json!(event);
    // Fields the Cursor CLI sends with every agent hook.
    let object = payload.as_object_mut().unwrap();
    object
        .entry("generation_id")
        .or_insert_with(|| json!(format!("generation-{event}-{ts_ms}")));
    object.entry("model").or_insert_with(|| json!("default"));
    object
        .entry("workspace_roots")
        .or_insert_with(|| json!(["/cursor-project"]));
    let env = Envelope {
        source: "cursor".into(),
        source_version: Some("2.5.0".into()),
        plugin_version: Some("0.1.0".into()),
        session_id: "cursor-recovery".into(),
        event: event.into(),
        ts_ms,
        managed_run_id: None,
        capture: None,
        payload,
        route: Some(SessionRoute {
            flush_mode,
            destination: Some(TraceDestination::ProjectLogs {
                project_id: None,
                project_name: Some("cursor-tests".into()),
            }),
            additional_metadata: Some(json!({"test_route":"cursor"})),
            ..SessionRoute::default()
        }),
        config: None,
    };
    forward_envelope(
        &env,
        socket,
        &HostInfo {
            serve_argv: vec!["unused".into()],
            version: "test".into(),
        },
        true,
    )
    .await
    .unwrap();
}

fn transcript(prompt: &str, response: &str) -> String {
    format!(
        "{}\n{}\n",
        json!({"role":"user","message":{"content":[{"type":"text","text":prompt}]}}),
        json!({"role":"assistant","message":{"content":[{"type":"text","text":response}]}})
    )
}

fn rows(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn insert_ids(rows: &[Value]) -> BTreeSet<String> {
    rows.iter()
        .filter_map(|row| row.pointer("/Insert/span_id").and_then(Value::as_str))
        .map(str::to_owned)
        .collect()
}

#[derive(Default)]
struct BufferedRows {
    pending: Vec<SpanOp>,
    delivered: Vec<SpanOp>,
}

struct BufferedSink(Arc<Mutex<BufferedRows>>);

#[async_trait]
impl Sink for BufferedSink {
    async fn emit(&mut self, ops: &[SpanOp]) -> anyhow::Result<u64> {
        self.0.lock().unwrap().pending.extend_from_slice(ops);
        Ok(ops.len() as u64)
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        let mut rows = self.0.lock().unwrap();
        let pending = std::mem::take(&mut rows.pending);
        rows.delivered.extend(pending);
        Ok(())
    }

    fn has_pending_delivery(&self) -> bool {
        !self.0.lock().unwrap().pending.is_empty()
    }
}

impl SinkFactory for BufferedSink {
    fn create(&self, _: &str, _: &str, _: Option<&str>) -> anyhow::Result<Box<dyn Sink>> {
        Ok(Box::new(Self(self.0.clone())))
    }
}

#[tokio::test]
async fn late_response_is_delivered_and_checkpointed_without_another_hook() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    let socket = endpoint(tmp.path());
    let buffered = Arc::new(Mutex::new(BufferedRows::default()));
    let daemon = start_with_sink(
        &data,
        &socket,
        Some(Arc::new(BufferedSink(buffered.clone()))),
    )
    .await;
    for (event, ts_ms, payload) in [
        (
            "beforeSubmitPrompt",
            1000,
            json!({"generation_id":"g1","prompt":"hello"}),
        ),
        (
            "stop",
            2000,
            json!({"generation_id":"g1","status":"completed"}),
        ),
        (
            "afterAgentResponse",
            2100,
            json!({"generation_id":"g1","text":"late final answer"}),
        ),
    ] {
        send_with_flush_mode(&socket, event, ts_ms, payload, FlushMode::FlushOnTurnEnd).await;
    }
    let journal = source_journal_path(&data, "cursor", "cursor-recovery");
    // Ingress acknowledges capture before the out-of-band sink flush finishes.
    // Wait for delivery and its checkpoint without requesting an explicit flush.
    let completed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let delivered = buffered.lock().unwrap().delivered.iter().any(|op| {
                matches!(op, SpanOp::Merge(row) if row.output == Some(json!("late final answer")))
            });
            let text = std::fs::read_to_string(&journal).unwrap_or_default();
            let mut position = 0;
            let mut response_through = None;
            let mut checkpoint_through = 0;
            for line in text.split_inclusive('\n') {
                position += line.len() as u64;
                let Ok(row) = serde_json::from_str::<Value>(line) else {
                    continue;
                };
                if row["event"] == "afterAgentResponse" {
                    response_through = Some(position);
                }
                if row.get("_bt_record_type").is_some() {
                    checkpoint_through =
                        checkpoint_through.max(row["through"].as_u64().unwrap_or(0));
                }
            }
            if delivered && response_through.is_some_and(|offset| checkpoint_through >= offset) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    shutdown_daemon(&socket).await.unwrap();
    daemon.await.unwrap();
    completed
        .expect("late response was not delivered and checkpointed while the session stayed open");
}

#[tokio::test]
async fn repeated_transcript_prompt_is_delivered_as_a_second_turn_after_checkpoint() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    let socket = endpoint(tmp.path());
    let native = tmp.path().join("native.jsonl");
    let daemon = start(&data, &socket).await;
    let first = transcript("continue", "first answer");
    std::fs::write(&native, &first).unwrap();
    send(
        &socket,
        "stop",
        1000,
        json!({"status":"completed","transcript_path":native}),
    )
    .await;
    assert!(
        flush_session("cursor-recovery", &socket, 5000)
            .await
            .unwrap()
            .flushed
    );
    let second = transcript("continue", "second answer");
    std::fs::write(&native, format!("{first}{second}")).unwrap();
    send(
        &socket,
        "stop",
        2000,
        json!({"status":"completed","transcript_path":native}),
    )
    .await;
    assert!(
        flush_session("cursor-recovery", &socket, 5000)
            .await
            .unwrap()
            .flushed
    );
    shutdown_daemon(&socket).await.unwrap();
    daemon.await.unwrap();

    let delivered = rows(&data.join("spans/cursor-recovery.ndjson"));
    let turns: Vec<_> = delivered
        .iter()
        .filter_map(|op| op.get("Insert"))
        .filter(|row| {
            row["name"]
                .as_str()
                .is_some_and(|name| name.starts_with("Turn "))
        })
        .collect();
    assert_eq!(
        turns.len(),
        2,
        "the delivery ledger must accept a distinct second turn"
    );
    assert_ne!(turns[0]["span_id"], turns[1]["span_id"]);
    for (turn, answer) in turns.iter().zip(["first answer", "second answer"]) {
        assert_eq!(turn["input"], "continue");
        let output = delivered
            .iter()
            .filter_map(|op| op.get("Merge"))
            .filter(|row| row["span_id"] == turn["span_id"])
            .filter_map(|row| row.get("output"))
            .next_back()
            .unwrap();
        assert_eq!(output, answer);
    }
}

#[tokio::test]
async fn cursor_mirror_generations_and_capture_bounds_survive_deleted_native_recovery() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    let socket = endpoint(tmp.path());
    let native = tmp.path().join("native.jsonl");
    let daemon = start(&data, &socket).await;
    send(
        &socket,
        "sessionStart",
        1000,
        json!({"transcript_path":null,"cwd":"/cursor-project"}),
    )
    .await;
    send(
        &socket,
        "beforeSubmitPrompt",
        2000,
        json!({"prompt":"first prompt","generation_id":"g1"}),
    )
    .await;
    send(
        &socket,
        "preToolUse",
        2200,
        json!({
            "generation_id":"g1", "tool_use_id":"shell-1", "tool_name":"Shell",
            "tool_input":{"command":"printf cursor"}
        }),
    )
    .await;
    send(
        &socket,
        "postToolUse",
        2400,
        json!({
            "generation_id":"g1", "tool_use_id":"shell-1", "tool_name":"Shell",
            "tool_input":{"command":"printf cursor"}, "tool_output":"cursor",
            "duration":200
        }),
    )
    .await;
    let first = transcript("first prompt", "first response");
    let unobserved = transcript("never captured", "never delivered");
    std::fs::write(&native, format!("{first}{unobserved}")).unwrap();
    send(
        &socket,
        "stop",
        3000,
        json!({
            "status":"completed", "generation_id":"g1", "transcript_path":native,
            "_bt_transcript_observation":{"path":native,"observed_bytes":first.len()}
        }),
    )
    .await;
    assert!(
        flush_session("cursor-recovery", &socket, 5000)
            .await
            .unwrap()
            .flushed
    );
    send(
        &socket,
        "beforeSubmitPrompt",
        4000,
        json!({"prompt":"other prompt","generation_id":"g2"}),
    )
    .await;
    let second = transcript("other prompt", "other response");
    // Same-length rewrite exercises content detection rather than length alone.
    assert_eq!(first.len(), second.len());
    std::fs::write(&native, format!("{second}{unobserved}")).unwrap();
    send(
        &socket,
        "stop",
        5000,
        json!({
            "status":"completed", "generation_id":"g2", "transcript_path":native,
            "_bt_transcript_observation":{"path":native,"observed_bytes":second.len()}
        }),
    )
    .await;
    assert!(
        flush_session("cursor-recovery", &socket, 5000)
            .await
            .unwrap()
            .flushed
    );
    shutdown_daemon(&socket).await.unwrap();
    daemon.await.unwrap();

    let output = data.join("spans/cursor-recovery.ndjson");
    let live = rows(&output);
    let live_text = serde_json::to_string(&live).unwrap();
    assert!(live_text.contains("first response"));
    assert!(live_text.contains("other response"));
    assert!(!live_text.contains("never delivered"));
    let inserts = live
        .iter()
        .filter_map(|row| row.get("Insert"))
        .collect::<Vec<_>>();
    let root = inserts
        .iter()
        .find(|row| row["name"] == "Cursor session")
        .unwrap();
    assert_eq!(root["metadata"]["trace_cursor_version"], "2.5.0");
    assert_eq!(root["metadata"]["trace_plugin_version"], "0.1.0");
    assert_eq!(root["metadata"]["test_route"], "cursor");
    let tool = inserts
        .iter()
        .find(|row| row["span_type"] == "tool")
        .unwrap();
    let parent = inserts
        .iter()
        .find(|row| row["span_id"] == tool["parent_span_ids"][0])
        .unwrap();
    assert_eq!(parent["span_type"], "task");
    let llms = inserts
        .iter()
        .filter(|row| row["span_type"] == "llm")
        .collect::<Vec<_>>();
    assert!(llms.len() >= 2);
    assert!(llms
        .iter()
        .all(|row| row["parent_span_ids"].as_array().unwrap().len() == 1));
    let journal = source_journal_path(&data, "cursor", "cursor-recovery");
    let persisted = rows(&journal);
    let observations = persisted
        .iter()
        .filter(|row| row["event"] == "stop")
        .map(|row| &row["payload"]["_bt_transcript_mirror"])
        .collect::<Vec<_>>();
    assert_eq!(observations.len(), 2);
    assert_ne!(observations[0]["mirror"], observations[1]["mirror"]);
    for observation in &observations {
        assert_eq!(observation["through"], first.len());
        assert!(Path::new(observation["mirror"].as_str().unwrap())
            .starts_with(data.join("transcripts")));
    }
    assert!(!std::fs::read_to_string(&journal)
        .unwrap()
        .contains("cursor-test-secret"));

    // Simulate a delivery loss: retain accepted ingress, discard sink receipts.
    let pending = persisted
        .iter()
        .filter(|row| row.get("_bt_record_type").is_none())
        .map(|row| format!("{row}\n"))
        .collect::<String>();
    std::fs::write(&journal, pending).unwrap();
    std::fs::remove_dir_all(data.join("delivery-ledger")).unwrap();
    std::fs::remove_file(&native).unwrap();
    std::fs::remove_file(&output).unwrap();
    let recovered_daemon = start(&data, &socket).await;
    assert!(
        flush_session("cursor-recovery", &socket, 5000)
            .await
            .unwrap()
            .flushed
    );
    shutdown_daemon(&socket).await.unwrap();
    recovered_daemon.await.unwrap();
    let recovered = rows(&output);
    assert_eq!(insert_ids(&live), insert_ids(&recovered));
    let recovered_text = serde_json::to_string(&recovered).unwrap();
    assert!(recovered_text.contains("first response"));
    assert!(recovered_text.contains("other response"));
    assert!(!recovered_text.contains("never delivered"));

    // A normally acknowledged restart rebuilds historical state but preserves
    // delivered Insert identities, including when the native path is missing.
    let inserted_before = recovered
        .iter()
        .filter(|row| row.get("Insert").is_some())
        .count();
    let acknowledged_daemon = start(&data, &socket).await;
    send(
        &socket,
        "sessionEnd",
        6000,
        json!({"transcript_path":native,"reason":"completed","final_status":"completed"}),
    )
    .await;
    assert!(
        flush_session("cursor-recovery", &socket, 5000)
            .await
            .unwrap()
            .flushed
    );
    shutdown_daemon(&socket).await.unwrap();
    acknowledged_daemon.await.unwrap();
    let acknowledged = rows(&output);
    assert_eq!(
        acknowledged
            .iter()
            .filter(|row| row.get("Insert").is_some())
            .count(),
        inserted_before
    );
}
