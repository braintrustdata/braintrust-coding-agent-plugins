#![cfg(feature = "cli")]
#![allow(
    clippy::disallowed_methods,
    reason = "Test fixtures intentionally launch raw children."
)]

use std::io::Write;
use std::process::Command;
use std::process::Stdio;

#[test]
fn standalone_status_json_is_valid_when_daemon_is_absent() {
    #[cfg(unix)]
    let temp = tempfile::tempdir().unwrap();
    #[cfg(unix)]
    let socket = temp.path().join("missing.sock");
    #[cfg(windows)]
    let socket = std::path::PathBuf::from(format!(
        r"\\.\pipe\missing-bt-daemon-{}",
        uuid::Uuid::new_v4()
    ));

    let output = Command::new(env!("CARGO_BIN_EXE_bt-daemon"))
        .args(["status", "--json", "--socket"])
        .arg(socket)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["command"], "status");
    assert_eq!(value["running"], false);
    assert_eq!(value["sessions"], serde_json::json!([]));
}

#[test]
fn standalone_hook_exit_status_distinguishes_capture_failures_from_disabled_tracing() {
    let temp = tempfile::tempdir().unwrap();
    let config = temp.path().join("braintrust.json");
    #[cfg(unix)]
    let socket = temp.path().join("missing.sock");
    #[cfg(windows)]
    let socket = std::path::PathBuf::from(format!(
        r"\\.\pipe\missing-bt-daemon-{}",
        uuid::Uuid::new_v4()
    ));

    let hook = |input: &str| {
        let mut child = Command::new(env!("CARGO_BIN_EXE_bt-daemon"))
            .args(["hook", "--source", "codex", "--no-spawn", "--socket"])
            .arg(&socket)
            .env("BRAINTRUST_DAEMON_CONFIG", &config)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let _ = child.stdin.take().unwrap().write_all(input.as_bytes());
        child.wait_with_output().unwrap()
    };
    let assert_failure = |output: std::process::Output, message: &str| {
        assert_eq!(output.status.code(), Some(1));
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(message),
            "stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    };

    assert_failure(hook("{}"), "read tracing settings");
    std::fs::write(&config, "{").unwrap();
    assert_failure(hook("{}"), "parse tracing settings");
    std::fs::write(&config, "{}").unwrap();
    assert_failure(hook("{}"), "no `trace_to_braintrust` value");
    std::fs::write(&config, r#"{"trace_to_braintrust":false}"#).unwrap();
    assert_eq!(hook("").status.code(), Some(0));

    std::fs::write(
        &config,
        r#"{"trace_to_braintrust":true,"route":{"destination":{"type":"project_logs","project_name":"test"}}}"#,
    )
    .unwrap();
    assert_failure(hook("{"), "EOF while parsing");
    assert_failure(
        hook(r#"{"hook_event_name":"SessionStart"}"#),
        "no `session_id` field",
    );
    assert_failure(
        hook(r#"{"session_id":"test","hook_event_name":"SessionStart"}"#),
        "--no-spawn is set",
    );

    std::fs::write(&config, r#"{"trace_to_braintrust":true}"#).unwrap();
    assert_failure(
        hook(r#"{"session_id":"test","hook_event_name":"SessionStart"}"#),
        "trace destination is not configured",
    );
}

#[test]
fn standalone_hook_honors_deprecated_bt_env_names() {
    let temp = tempfile::tempdir().unwrap();
    let old_config = temp.path().join("old.json");
    let new_config = temp.path().join("new.json");
    std::fs::write(&old_config, r#"{"trace_to_braintrust":false}"#).unwrap();
    #[cfg(unix)]
    let socket = temp.path().join("missing.sock");
    #[cfg(windows)]
    let socket = std::path::PathBuf::from(format!(
        r"\\.\pipe\missing-bt-daemon-{}",
        uuid::Uuid::new_v4()
    ));

    let hook = |env: &[(&str, &std::path::Path)]| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_bt-daemon"));
        command
            .args(["hook", "--source", "codex", "--no-spawn", "--socket"])
            .arg(&socket)
            .env("HOME", temp.path())
            .env("USERPROFILE", temp.path())
            .env_remove("BT_DAEMON_CONFIG")
            .env_remove("BRAINTRUST_DAEMON_CONFIG")
            .stdin(Stdio::null());
        for (key, value) in env {
            command.env(key, value);
        }
        command.output().unwrap()
    };

    // The deprecated name alone still selects the settings file, silently.
    let output = hook(&[("BT_DAEMON_CONFIG", &old_config)]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(0), "stderr: {stderr}");
    assert!(stderr.is_empty(), "stderr: {stderr}");

    // The canonical name wins when both are set.
    let output = hook(&[
        ("BT_DAEMON_CONFIG", &old_config),
        ("BRAINTRUST_DAEMON_CONFIG", &new_config),
    ]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "stderr: {stderr}");
    assert!(stderr.contains("new.json"), "stderr: {stderr}");
}
