#![cfg(unix)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "Integration assertions fail immediately."
)]
#![allow(
    clippy::disallowed_methods,
    reason = "Integration fixtures control native child processes."
)]
mod support;

use axum::middleware::{self, Next};
use axum::{extract::Request, response::Response};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use support::inference::{MockReply, OpenAiMock, OpenAiTurn};
use support::server::TestServer;
use tokio::process::{Child, Command};
use tokio::sync::Notify;

struct NativeSession {
    root: tempfile::TempDir,
    binary: std::ffi::OsString,
}
impl NativeSession {
    fn new(base_url: &str) -> Self {
        let root = tempfile::tempdir().unwrap();
        let config = root.path().join("config/muse");
        std::fs::create_dir_all(&config).unwrap();
        std::fs::create_dir_all(root.path().join("workspace")).unwrap();
        std::fs::create_dir_all(root.path().join("home")).unwrap();
        std::fs::write(
            config.join("settings.json"),
            json!({"schema_version":1,"endpoint_transport":{"base_url":base_url,"auth":"bearer"}})
                .to_string(),
        )
        .unwrap();
        std::fs::write(
            config.join("auth.json"),
            json!({"schema_version":1,"providers":{"meta":{"api_key":"test-key"}}}).to_string(),
        )
        .unwrap();
        Self {
            root,
            binary: std::env::var_os("MUSE_BIN").unwrap_or_else(|| "muse".into()),
        }
    }
    fn command(&self, binary: impl AsRef<std::ffi::OsStr>) -> Command {
        let mut command = Command::new(binary);
        command
            .kill_on_drop(true)
            .current_dir(self.root.path().join("workspace"))
            .env("HOME", self.root.path().join("home"))
            .env("TBH_CREDENTIAL_BACKEND", "file")
            .env("TBH_DISABLE_TELEMETRY", "1")
            .env("META_API_KEY", "test-key")
            .env("XDG_CONFIG_HOME", self.root.path().join("config"))
            .env("XDG_DATA_HOME", self.root.path().join("data"))
            .env(
                "BRAINTRUST_DAEMON_DATA_DIR",
                self.root.path().join("import"),
            )
            .env("MUSE_BIN", &self.binary)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for (key, _) in std::env::vars_os() {
            if [
                "CODEX_",
                "NODE_REPL_",
                "CUA_REPL_",
                "BROWSER_USE_",
                "SKY_CUA_",
            ]
            .iter()
            .any(|prefix| key.to_string_lossy().starts_with(prefix))
            {
                command.env_remove(key);
            }
        }
        command.env_remove("ELECTRON_RUN_AS_NODE");
        command
    }
    fn import(&self, session: &str, attach: bool) -> Command {
        let mut command = self.command(env!("CARGO_BIN_EXE_bt-daemon"));
        command.args(["--json", "import", "muse", session]);
        if attach {
            command.arg("--attach");
        }
        command
    }
    fn rows_path(&self, session: &str) -> PathBuf {
        self.root
            .path()
            .join("import/spans")
            .join(format!("{session}.ndjson"))
    }
}
fn rows(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}
async fn detach(mut child: Child) {
    // This is the public Ctrl-C path, delivered only to the observer process.
    let pid = child.id().unwrap();
    assert_eq!(unsafe { libc::kill(pid as libc::pid_t, libc::SIGINT) }, 0);
    let status = tokio::time::timeout(Duration::from_secs(10), child.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(status.success(), "attach observer failed: {status}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires native Muse Code 1.3+; uses deterministic loopback inference"]
async fn native_active_session_detach_reattach_preserves_authoritative_completion() {
    let requested = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let inference =
        OpenAiMock::new(|_, _| MockReply::response(OpenAiTurn::text("MUSE_ATTACH_COMPLETE")));
    let router = inference.router().layer(middleware::from_fn({
        let requested = requested.clone();
        let release = release.clone();
        move |request: Request, next: Next| {
            let requested = requested.clone();
            let release = release.clone();
            async move {
                if request.method() == axum::http::Method::POST
                    && request.uri().path().ends_with("/responses")
                {
                    requested.notify_one();
                    release.notified().await;
                }
                let response: Response = next.run(request).await;
                response
            }
        }
    }));
    let server = TestServer::start(router).await;
    let native = NativeSession::new(server.uri());
    let session = uuid::Uuid::new_v4().to_string();
    let mut command = native.command(&native.binary);
    command.args([
        "exec",
        "--session-id",
        &session,
        "--provider",
        "meta",
        "--model",
        "mock-model",
        "--no-foreign-personal-context",
        "--disable-reminders",
        "--disable-web-tools",
        "--disable-request-user-input",
        "--max-model-steps",
        "1",
        "Reply MUSE_ATTACH_COMPLETE.",
    ]);
    let mut agent = command.spawn().unwrap();
    tokio::time::timeout(Duration::from_secs(30), requested.notified())
        .await
        .unwrap();
    let export_path = native.root.path().join("active.json");
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let exported = native
                .command(&native.binary)
                .args(["export", "--session", &session, "--out"])
                .arg(&export_path)
                .output()
                .await
                .unwrap();
            if exported.status.success() {
                break;
            }
            eprintln!(
                "waiting for native retention: {}",
                String::from_utf8_lossy(&exported.stderr)
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .expect("Muse did not publish an active session export while inference was open");
    let active: Value = serde_json::from_slice(&std::fs::read(export_path).unwrap()).unwrap();
    assert!(active["events"]
        .as_array()
        .is_some_and(|events| !events.is_empty()));
    let mut first = native.import(&session, true).spawn().unwrap();
    let path = native.rows_path(&session);
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if rows(&path)
                .iter()
                .any(|row| row["Insert"]["name"] == "Muse Code")
            {
                break;
            }
            if let Some(status) = first.try_wait().unwrap() {
                let mut stderr = String::new();
                tokio::io::AsyncReadExt::read_to_string(
                    &mut first.stderr.take().unwrap(),
                    &mut stderr,
                )
                .await
                .unwrap();
                panic!("observer exited before active rows: {status}: {stderr}");
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    detach(first).await;
    assert!(
        agent.try_wait().unwrap().is_none(),
        "detaching killed native Muse"
    );
    let before = rows(&path);
    assert!(
        before.iter().all(|row| row["Merge"]["error"].is_null()),
        "detaching invented an interruption: {before:?}"
    );
    assert!(
        before.iter().all(|row| row["Merge"]["end_ms"].is_null()),
        "detaching ended native work: {before:?}"
    );
    let mut second = native.import(&session, true).spawn().unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        second.try_wait().unwrap().is_none(),
        "reattach ended before native completion"
    );
    release.notify_one();
    let output = tokio::time::timeout(Duration::from_secs(30), agent.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert!(
        output.status.success(),
        "native Muse failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let observed = tokio::time::timeout(Duration::from_secs(30), second.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert!(
        observed.status.success(),
        "reattach failed: {}",
        String::from_utf8_lossy(&observed.stderr)
    );
    let completed = rows(&path);
    let root_ids: std::collections::BTreeSet<_> = completed
        .iter()
        .filter(|row| row["Insert"]["name"] == "Muse Code")
        .map(|row| row["Insert"]["span_id"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(
        root_ids.len(),
        1,
        "reattach created a different root identity: {completed:?}"
    );
    assert!(
        completed.iter().any(|row| row["Merge"]["span_id"]
            == root_ids.iter().next().unwrap().as_str()
            && row["Merge"]["end_ms"].is_number()
            && row["Merge"]["error"].is_null()),
        "root did not receive authoritative completion: {completed:?}"
    );
    assert!(
        completed.iter().any(|row| row["Merge"]["output"]
            .to_string()
            .contains("MUSE_ATTACH_COMPLETE")),
        "native final output was lost: {completed:?}"
    );
    let imported = native.import(&session, false).output().await.unwrap();
    assert!(
        imported.status.success(),
        "final import: {}",
        String::from_utf8_lossy(&imported.stderr)
    );
    // The standalone debug importer has no destination-scoped delivery ledger.
    // Replays may emit identical upserts; they must retain their span identities.
    let identities = |rows: &[Value]| -> std::collections::BTreeSet<String> {
        rows.iter()
            .filter_map(|row| row.get("Insert").or_else(|| row.get("Merge")))
            .filter_map(|row| row["span_id"].as_str().map(str::to_owned))
            .collect()
    };
    assert_eq!(
        identities(&rows(&path)),
        identities(&completed),
        "one-shot reimport changed native identities"
    );
    assert_eq!(
        identities(&before),
        identities(&completed),
        "reattach changed the open span identities"
    );
    // Exercise the SDK handshake and full SessionListResult against the native
    // durable index, then reimport through the same production reader.
    let all = native
        .command(env!("CARGO_BIN_EXE_bt-daemon"))
        .args(["--json", "import", "muse", "--all"])
        .output()
        .await
        .unwrap();
    assert!(
        all.status.success(),
        "MSP import all: {}",
        String::from_utf8_lossy(&all.stderr)
    );
    assert!(String::from_utf8_lossy(&all.stdout).contains(&session));
    assert_eq!(inference.requests().len(), 1);
}
