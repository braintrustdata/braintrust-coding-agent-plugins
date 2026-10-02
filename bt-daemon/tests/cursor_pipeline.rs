//! Cursor hooks freeze native transcript observations before journal ingress.
//! Recovery must use those mirrors even after native files change or disappear.

use async_trait::async_trait;
use bt_daemon::wire::{AuthSelection, BackendAuth, Envelope, SessionRoute, TraceDestination};
use bt_daemon::{
    debug_serve_options, flush_session, forward_envelope, run_serve, run_status, shutdown_daemon,
    source_journal_path, AuthLease, AuthProvider, AuthResolveReason, HostInfo, ServeArgs,
    StatusArgs,
};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

struct TestAuth;

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
    let args = ServeArgs {
        data_dir: Some(data.into()),
        socket: Some(socket.into()),
        idle_timeout_secs: 0,
        session_idle_timeout_secs: 0,
    };
    let mut options = debug_serve_options("test", data);
    options.auth_provider = Some(Arc::new(TestAuth));
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

async fn send(socket: &Path, event: &str, ts_ms: i64, mut payload: Value) {
    payload["conversation_id"] = json!("cursor-recovery");
    payload["hook_event_name"] = json!(event);
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
            "tool_input":{"command":"printf cursor"}, "tool_output":{"stdout":"cursor"},
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
        json!({"transcript_path":native,"reason":"completed"}),
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
