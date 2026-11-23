use super::*;
use crate::wire::Envelope;
use serde_json::json;
use serde_json::Value;
fn envelope(name: &str, payload: Value) -> Envelope {
    Envelope {
        source: "muse".into(),
        session_id: "session".into(),
        event: name.into(),
        ts_ms: 100,
        source_version: None,
        plugin_version: None,
        managed_run_id: None,
        capture: None,
        route: None,
        config: None,
        payload,
    }
}
#[test]
fn all_hook_names_decode_and_optional_ids_remain_tolerant() {
    for name in HookName::ALL {
        let e = envelope(
            name.as_str(),
            json!({"turn_id":{},"request_id":4,"future_field":true}),
        );
        let decoded = MuseEvent::decode(&e).unwrap().unwrap();
        assert_eq!(decoded.name(), *name);
        assert_eq!(decoded.context().turn(), None);
        assert_eq!(decoded.context().request(), None);
    }
    assert!(matches!(
        MuseEvent::decode(&envelope("ToolUseStart", json!({}))).unwrap(),
        Some(MuseEvent::PreToolUse(_))
    ));
    assert!(MuseEvent::decode(&envelope("FutureEvent", json!({})))
        .unwrap()
        .is_none());
    assert!(MuseEvent::decode(&envelope("PreLLMCall", json!({"step":"wrong"}))).is_err());
}
#[test]
fn known_content_is_validated_and_unknown_content_is_lossless() {
    let block = json!({"type":"image","source":{"url":"example"},"future":true});
    let decoded: ContentBlock = serde_json::from_value(block.clone()).unwrap();
    assert_eq!(serde_json::to_value(decoded).unwrap(), block);
    assert!(serde_json::from_value::<ContentBlock>(json!({"type":"text","text":42})).is_err());
    let args = ToolArguments::Encoded("{\"x\":1}".into());
    assert_eq!(args.input().0, json!({"x":1}));
    assert_eq!(
        ToolArguments::Encoded("invalid json".into()).input().0,
        json!("invalid json")
    );
}
