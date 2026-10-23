#![cfg(all(feature = "cli", unix))]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::disallowed_methods,
    reason = "Isolated subprocess regression fixtures."
)]
use bt_daemon::wire::{SessionRoute, TraceDestination};
use bt_daemon::{run_traced_with_host, HostInfo, RunArgs, RunHookCommand, RunSource};
use std::ffi::OsString;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

struct Env(&'static str, Option<OsString>);
impl Env {
    fn set(key: &'static str, value: impl AsRef<std::ffi::OsStr>) -> Self {
        let previous = std::env::var_os(key);
        std::env::set_var(key, value);
        Self(key, previous)
    }
}
impl Drop for Env {
    fn drop(&mut self) {
        if let Some(value) = &self.1 {
            std::env::set_var(self.0, value);
        } else {
            std::env::remove_var(self.0);
        }
    }
}
fn assert_running(marker: &Path) {
    let status: serde_json::Value =
        serde_json::from_slice(&std::fs::read(marker).unwrap()).unwrap();
    assert_eq!(status["running"], true);
}
#[tokio::test]
async fn muse_starts_fresh_explicit_endpoint_and_standalone_supplies_its_host() {
    let tmp = tempfile::tempdir().unwrap();
    let fake = tmp.path().join("muse-override.sh");
    std::fs::write(
        &fake,
        r#"#!/bin/sh
if [ "$1" = "--version" ]; then echo 'Muse Code 1.4.11'; exit 0; fi
"$MUSE_TEST_DAEMON" --json status --socket "$BRAINTRUST_DAEMON_SOCKET" > "$MUSE_TEST_MARKER"
"#,
    )
    .unwrap();
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    let binary = env!("CARGO_BIN_EXE_bt-daemon");
    let marker = tmp.path().join("explicit-status.json");
    let socket = tmp.path().join("fresh.sock");
    let _bin = Env::set("BRAINTRUST_MUSE_BIN", &fake);
    let _config = Env::set("XDG_CONFIG_HOME", tmp.path().join("config"));
    let _data = Env::set("BRAINTRUST_DAEMON_DATA_DIR", tmp.path().join("data"));
    let _socket = Env::set("BRAINTRUST_DAEMON_SOCKET", &socket);
    let _daemon = Env::set("MUSE_TEST_DAEMON", binary);
    let _marker = Env::set("MUSE_TEST_MARKER", &marker);
    let result = run_traced_with_host(
        RunArgs {
            source: RunSource::Muse,
            additional_metadata: None,
            tags: Vec::new(),
            plugin: Vec::new(),
            agent_args: vec!["exec".into()],
        },
        RunHookCommand {
            program: binary.into(),
            args: vec!["hook".into()],
        },
        SessionRoute {
            destination: Some(TraceDestination::ProjectLogs {
                project_id: None,
                project_name: Some("test".into()),
            }),
            ..Default::default()
        },
        HostInfo {
            serve_argv: vec![binary.into(), "serve".into(), "--debug-sink".into()],
            version: env!("CARGO_PKG_VERSION").into(),
        },
    )
    .await;
    let _ = bt_daemon::shutdown_daemon(&socket).await;
    assert!(result.unwrap().success());
    assert_running(&marker);

    // Exercise the actual standalone entry point with no explicit endpoint.
    // This child emits no model events, so no credentials/network are required.
    let marker = tmp.path().join("standalone-status.json");
    let output = tokio::process::Command::new(binary)
        .args(["run", "--project", "test", "muse", "exec"])
        .env_remove("BRAINTRUST_DAEMON_SOCKET")
        .env_remove("BT_DAEMON_SOCKET")
        .env("MUSE_TEST_MARKER", &marker)
        .current_dir(tmp.path())
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_running(&marker);
}
