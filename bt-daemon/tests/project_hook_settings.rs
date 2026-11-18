#![cfg(feature = "cli")]

use std::io::Write;
use std::process::{Command, Stdio};

#[test]
fn project_settings_control_hook_capture_from_native_payload() {
    let temp = tempfile::tempdir().unwrap();
    let repo = temp.path().join("repo");
    std::fs::create_dir_all(repo.join(".git")).unwrap();
    let user = temp.path().join("user.json");
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
    ] {
        let config_dir = repo.join(dir);
        std::fs::create_dir_all(&config_dir).unwrap();
        let config = config_dir.join("braintrust.json");
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
