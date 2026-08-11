#![cfg(feature = "cli")]

use std::process::Command;

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
