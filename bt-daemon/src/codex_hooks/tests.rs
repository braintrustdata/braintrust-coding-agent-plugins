use super::*;

#[test]
fn required_events_match_the_packaged_plugin() {
    let hooks: Value = serde_json::from_str(PACKAGED_HOOKS).unwrap();
    let events: std::collections::BTreeSet<_> = hooks["hooks"]
        .as_object()
        .unwrap()
        .keys()
        .map(|key| format!("{}{}", key[..1].to_ascii_lowercase(), &key[1..]))
        .collect();
    assert_eq!(
        events,
        PLUGIN_EVENTS
            .iter()
            .map(|event| (*event).to_owned())
            .collect()
    );
}

#[tokio::test]
async fn rpc_distinguishes_null_results_from_missing_results() {
    let mut input = Vec::new();
    let mut output = std::io::Cursor::new(b"{\"id\":1,\"result\":null}\n");
    assert_eq!(
        request::<Value>(&mut input, &mut output, 1, "initialize", json!({}))
            .await
            .unwrap(),
        Value::Null
    );
    let mut output = std::io::Cursor::new(b"{\"id\":1}\n");
    assert!(
        request::<Value>(&mut input, &mut output, 1, "initialize", json!({}))
            .await
            .unwrap_err()
            .to_string()
            .contains("no result")
    );
}

#[tokio::test]
async fn rpc_ignores_notifications_and_reports_unsupported_methods() {
    let mut input = Vec::new();
    let mut output = std::io::Cursor::new(
        b"{\"method\":\"notice\"}\n{\"id\":3,\"error\":{\"message\":\"Method not found\"}}\n",
    );
    let error = request::<Value>(&mut input, &mut output, 3, "hooks/list", json!({}))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("Method not found"));
    assert_eq!(
        serde_json::from_slice::<Value>(&input).unwrap()["method"],
        "hooks/list"
    );
}

#[test]
fn config_read_ignores_unrelated_features_and_honors_hook_toggles() {
    // Codex 0.172.0 reports `network_proxy: null` even for a default config.
    for (config, enabled) in [
        (
            json!({"features": {"network_proxy": null, "other": {"nested": 1}}}),
            true,
        ),
        (json!({"features": null}), true),
        (json!({}), true),
        (
            json!({"features": {"network_proxy": null, "hooks": false}}),
            false,
        ),
        (
            json!({"features": {"network_proxy": null, "codex_hooks": false}}),
            false,
        ),
    ] {
        let read: ConfigRead = serde_json::from_value(json!({"config": config})).unwrap();
        assert_eq!(read.config.hooks_enabled(), enabled, "{config}");
    }
}

#[cfg(unix)]
#[tokio::test]
async fn probe_failures_include_codex_stderr() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let codex = dir.path().join("codex");
    std::fs::write(
        &codex,
        "#!/bin/sh\necho \"error: unexpected argument '--no-daemon' found\" >&2\nexit 2\n",
    )
    .unwrap();
    std::fs::set_permissions(&codex, std::fs::Permissions::from_mode(0o755)).unwrap();
    let error = run_trust_override(codex.as_os_str(), dir.path(), &[], "unused")
        .await
        .unwrap_err()
        .to_string();
    // The transport error races Codex's exit (closed stream vs broken pipe);
    // either way the stderr explanation must be present.
    assert!(
        error.contains("unexpected argument '--no-daemon' found"),
        "{error}"
    );
}
