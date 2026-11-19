#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "Integration test assertions and fixtures fail immediately on unexpected values."
)]
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
fn all_hook_integrations_honor_pause_and_managed_run() {
    let temp = tempfile::tempdir().unwrap();
    let settings = temp.path().join("braintrust.json");
    std::fs::write(&settings, r#"{"trace_to_braintrust":false}"#).unwrap();
    #[cfg(unix)]
    let socket = temp.path().join("absent.sock");
    #[cfg(windows)]
    let socket = std::path::PathBuf::from(format!(
        r"\\.\pipe\missing-bt-daemon-{}",
        uuid::Uuid::new_v4()
    ));
    for source in [
        "antigravity",
        "claude-code",
        "codex",
        "cursor",
        "grok",
        "muse",
    ] {
        for managed in [false, true] {
            let mut command = Command::new(env!("CARGO_BIN_EXE_bt-daemon"));
            command
                .args(["hook", "--source", source, "--no-spawn", "--socket"])
                .arg(&socket)
                .env("BRAINTRUST_DAEMON_CONFIG", &settings)
                .env_remove("BT_TRACE_INVOCATION_SETTINGS")
                .env_remove("_BT_TRACE_MANAGED_RUN")
                .env_remove("CURSOR_PLUGIN_ROOT")
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            if managed {
                command.env("BT_TRACE_INVOCATION_SETTINGS", r#"{"trace_to_braintrust":true,"route":{"destination":{"type":"project_logs","project_name":"test"}}}"#);
            }
            let mut child = command.spawn().unwrap();
            let _ = child
                .stdin
                .take()
                .unwrap()
                .write_all(br#"{"session_id":"test","hook_event_name":"SessionStart"}"#);
            let output = child.wait_with_output().unwrap();
            if managed {
                assert!(
                    !output.status.success(),
                    "{source}: managed run did not attempt capture"
                );
                assert!(
                    String::from_utf8_lossy(&output.stderr).contains("--no-spawn is set"),
                    "{source}: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            } else {
                assert!(
                    output.status.success(),
                    "{source}: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
        }
    }
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

/// Plugins released before the `BRAINTRUST_` rename read only `BT_DAEMON_SOCKET`,
/// so a managed run must hand its child the resolved socket under both names.
#[cfg(unix)]
#[test]
fn standalone_run_mirrors_an_explicit_socket_to_the_deprecated_name() {
    use std::os::unix::fs::PermissionsExt;

    let temp = tempfile::tempdir().unwrap();
    let agent = temp.path().join("fake-pi.sh");
    let seen = temp.path().join("seen-env");
    std::fs::write(
        &agent,
        format!(
            "#!/bin/sh\nprintf '%s\\n%s\\n' \"$BRAINTRUST_DAEMON_SOCKET\" \"$BT_DAEMON_SOCKET\" > '{}'\n",
            seen.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&agent, std::fs::Permissions::from_mode(0o755)).unwrap();
    let explicit = temp.path().join("explicit.sock");

    for stale in [None, Some(temp.path().join("stale.sock"))] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_bt-daemon"));
        command
            .args(["run", "--project", "mirror-test", "pi"])
            .env("HOME", temp.path())
            .env("PI_BIN", &agent)
            .env("BRAINTRUST_API_KEY", "test-key")
            .env("BRAINTRUST_DAEMON_SOCKET", &explicit)
            .env_remove("BT_DAEMON_SOCKET")
            .stdin(Stdio::null());
        if let Some(stale) = &stale {
            command.env("BT_DAEMON_SOCKET", stale);
        }
        let output = command.output().unwrap();
        let seen = std::fs::read_to_string(&seen).unwrap_or_else(|error| {
            panic!(
                "agent did not run ({error}); stderr: {}",
                String::from_utf8_lossy(&output.stderr)
            )
        });
        let explicit = explicit.display().to_string();
        assert_eq!(
            seen.lines().collect::<Vec<_>>(),
            vec![explicit.as_str(), explicit.as_str()],
            "stale deprecated socket: {stale:?}"
        );
    }
}

#[test]
fn project_settings_control_hook_capture_from_native_payload() {
    let temp = tempfile::tempdir().unwrap();
    let repo = temp.path().join("repo");
    std::fs::create_dir_all(repo.join(".git")).unwrap();
    let user = temp.path().join("user.json");
    #[cfg(unix)]
    let socket = temp.path().join("missing.sock");
    #[cfg(windows)]
    let socket = std::path::PathBuf::from(format!(
        r"\\.\pipe\missing-bt-daemon-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::write(&user, r#"{"trace_to_braintrust":false}"#).unwrap();
    for (source, dir, payload) in [
        (
            "claude",
            ".claude",
            serde_json::json!({"cwd":repo,"session_id":"s","hook_event_name":"Stop"}),
        ),
        (
            "cursor",
            ".cursor",
            serde_json::json!({"workspace_roots":[repo],"conversation_id":"s","hook_event_name":"stop"}),
        ),
        (
            "codex",
            ".codex",
            serde_json::json!({"cwd":repo,"session_id":"s","hook_event_name":"Stop"}),
        ),
        (
            "muse",
            ".muse",
            serde_json::json!({"cwd":repo,"session_id":"s","hook_event_name":"Stop"}),
        ),
        (
            "grok",
            ".grok",
            serde_json::json!({"cwd":repo,"session_id":"s","hook_event_name":"Stop"}),
        ),
        (
            "antigravity",
            ".gemini",
            serde_json::json!({"workspacePaths":[repo],"session_id":"s","hook_event_name":"Stop"}),
        ),
    ] {
        let _ = dir;
        let config = repo.join(".braintrust.json");
        let run = |enabled: bool| {
            std::fs::write(
                &config,
                format!(r#"{{"trace_to_braintrust":{enabled},"project":"test"}}"#),
            )
            .unwrap();
            let mut child = Command::new(env!("CARGO_BIN_EXE_bt-daemon"))
                .args([
                    "hook",
                    "--source",
                    source,
                    "--no-spawn",
                    "--session-id-field",
                    if source == "cursor" {
                        "conversation_id"
                    } else {
                        "session_id"
                    },
                ])
                .env("BRAINTRUST_DAEMON_CONFIG", &user)
                .env("BRAINTRUST_DAEMON_SOCKET", &socket)
                .env("HOME", temp.path())
                .env_remove("BT_TRACE_INVOCATION_SETTINGS")
                .env_remove("_BT_TRACE_MANAGED_RUN")
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap();
            child
                .stdin
                .take()
                .unwrap()
                .write_all(payload.to_string().as_bytes())
                .unwrap();
            child.wait_with_output().unwrap()
        };
        assert!(run(false).status.success());
        let enabled = run(true);
        assert!(
            !enabled.status.success(),
            "enabled hook should try to connect to the absent daemon"
        );
        assert!(!enabled.stderr.is_empty());
    }
}
