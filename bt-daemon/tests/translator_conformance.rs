use bt_daemon::wire::Envelope;
use bt_daemon::{Registry, SessionCtx, SpanOp};
use serde_json::json;

fn envelope(source: &str, event: &str, payload: serde_json::Value) -> Envelope {
    Envelope {
        source: source.into(),
        source_version: None,
        plugin_version: None,
        session_id: "shared-native-id".into(),
        event: event.into(),
        ts_ms: 1,
        managed_run_id: None,
        payload,
        route: None,
        config: None,
    }
}

#[test]
fn registry_rejects_unknown_sources_and_canonicalizes_aliases() {
    let registry = Registry::default_agents();
    assert!(registry.create_checked("unknown-agent", "session").is_err());
    assert_eq!(registry.canonical_source("claude"), Some("claude-code"));
}

#[test]
fn equal_native_session_ids_are_source_qualified_but_metadata_stays_native() {
    let registry = Registry::default_agents();
    let ctx = SessionCtx {
        session_id: "shared-native-id".into(),
        config: None,
    };
    let mut pi = registry.create("pi", "shared-native-id");
    let mut opencode = registry.create("opencode", "shared-native-id");
    let pi_root = pi
        .handle(&envelope("pi", "session_start", json!({"event":{}})), &ctx)
        .unwrap()
        .into_iter()
        .find_map(|op| match op {
            SpanOp::Insert(row) => Some(row),
            SpanOp::Merge(_) => None,
        })
        .unwrap();
    let opencode_root = opencode
        .handle(
            &envelope(
                "opencode",
                "session.created",
                json!({"properties":{"info":{"id":"shared-native-id"}}}),
            ),
            &ctx,
        )
        .unwrap()
        .into_iter()
        .find_map(|op| match op {
            SpanOp::Insert(row) => Some(row),
            SpanOp::Merge(_) => None,
        })
        .unwrap();

    assert_ne!(pi_root.span_id, opencode_root.span_id);
    assert_eq!(
        pi_root.metadata.as_ref().unwrap()["session_id"],
        "shared-native-id"
    );
    assert_eq!(
        opencode_root.metadata.as_ref().unwrap()["session_id"],
        "shared-native-id"
    );
}
