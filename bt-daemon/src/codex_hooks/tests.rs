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
