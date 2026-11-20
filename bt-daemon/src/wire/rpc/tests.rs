use super::*;
use serde::Serialize;

#[test]
fn result_serialization_failure_returns_internal_error() {
    struct FailingResult;
    impl Serialize for FailingResult {
        fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("fixture serialization failure"))
        }
    }
    let response = Response::serialized(RequestId::Int(17), FailingResult);
    assert_eq!(response.id, RequestId::Int(17));
    assert!(response.result.is_none());
    let error = response.error.unwrap();
    assert_eq!(error.code, error_code::INTERNAL);
    assert!(error.message.contains("fixture serialization failure"));
    let line = Message::Response(Response::serialized(RequestId::Int(17), FailingResult))
        .to_line()
        .unwrap();
    assert!(matches!(
        Message::from_line(&line).unwrap(),
        Message::Response(_)
    ));
}

#[test]
fn result_serialization_preserves_successful_payload() {
    let result = serde_json::json!({"accepted": true});
    let response = Response::serialized(RequestId::Int(18), &result);
    assert_eq!(response.id, RequestId::Int(18));
    assert_eq!(response.result, Some(result));
    assert!(response.error.is_none());
}

#[test]
fn request_round_trips() {
    let req = Request::new(
        RequestId::Int(7),
        "event.log",
        serde_json::json!({ "a": 1 }),
    );
    let line = Message::Request(req).to_line().unwrap();
    match Message::from_line(&line).unwrap() {
        Message::Request(r) => {
            assert_eq!(r.method, "event.log");
            assert_eq!(r.id, RequestId::Int(7));
        }
        other => panic!("expected request, got {other:?}"),
    }
}

#[test]
fn response_and_notification_disambiguate() {
    let resp = Message::Response(Response::ok(RequestId::Int(1), serde_json::json!({})))
        .to_line()
        .unwrap();
    assert!(matches!(
        Message::from_line(&resp).unwrap(),
        Message::Response(_)
    ));

    let note = Message::Notification(Notification {
        jsonrpc: JsonRpcV2,
        method: "event.log".into(),
        params: Some(serde_json::json!({})),
    })
    .to_line()
    .unwrap();
    assert!(matches!(
        Message::from_line(&note).unwrap(),
        Message::Notification(_)
    ));
}

#[test]
fn bad_jsonrpc_version_rejected() {
    let line = r#"{"jsonrpc":"1.0","id":1,"method":"x"}"#;
    assert!(Message::from_line(line).is_err());
}
