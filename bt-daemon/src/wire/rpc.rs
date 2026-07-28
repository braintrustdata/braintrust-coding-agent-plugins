//! JSON-RPC 2.0 message types, newline-delimited on the wire.

use serde::{Deserialize, Serialize};

/// A JSON-RPC request id: an integer or a string.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RequestId {
    Int(i64),
    Str(String),
}

/// A single JSON-RPC frame. Untagged so one type round-trips a request, a
/// response, or a notification; disambiguated by which fields are present.
///
/// Note: `Response` must come before `Notification` in the enum. A response
/// carries `id` but no `method`; a notification carries `method` but no `id`;
/// a request carries both. Serde's untagged matching tries variants in order,
/// so ordering here plus `deny_unknown_fields`-free structs keeps them
/// unambiguous.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Message {
    Request(Request),
    Response(Response),
    Notification(Notification),
}

impl Message {
    /// Parse one newline-delimited frame.
    pub fn from_line(line: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(line)
    }

    /// Serialize to a single line (no trailing newline).
    pub fn to_line(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    pub jsonrpc: JsonRpcV2,
    pub id: RequestId,
    pub method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<serde_json::Value>,
}

impl Request {
    pub fn new(id: RequestId, method: impl Into<String>, params: serde_json::Value) -> Self {
        Self {
            jsonrpc: JsonRpcV2,
            id,
            method: method.into(),
            params: Some(params),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Notification {
    pub jsonrpc: JsonRpcV2,
    pub method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub jsonrpc: JsonRpcV2,
    pub id: RequestId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

impl Response {
    pub fn ok(id: RequestId, result: serde_json::Value) -> Self {
        Self {
            jsonrpc: JsonRpcV2,
            id,
            result: Some(result),
            error: None,
        }
    }

    pub fn err(id: RequestId, error: RpcError) -> Self {
        Self {
            jsonrpc: JsonRpcV2,
            id,
            result: None,
            error: Some(error),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcError {
    pub code: i32,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

impl RpcError {
    pub fn new(code: i32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }
}

/// Reserved JSON-RPC error codes plus the application range.
pub mod error_code {
    pub const PARSE: i32 = -32700;
    pub const INVALID_REQUEST: i32 = -32600;
    pub const METHOD_NOT_FOUND: i32 = -32601;
    pub const INVALID_PARAMS: i32 = -32602;
    pub const INTERNAL: i32 = -32603;
    /// Application errors: -32000 ..= -32099.
    pub const APP: i32 = -32000;
}

/// A zero-sized marker that serializes to the string `"2.0"` and refuses any
/// other value, so the `jsonrpc` field is validated for free.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JsonRpcV2;

impl Serialize for JsonRpcV2 {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str("2.0")
    }
}

impl<'de> Deserialize<'de> for JsonRpcV2 {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let v = String::deserialize(d)?;
        if v == "2.0" {
            Ok(JsonRpcV2)
        } else {
            Err(serde::de::Error::custom(format!(
                "unsupported jsonrpc version {v:?}"
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
