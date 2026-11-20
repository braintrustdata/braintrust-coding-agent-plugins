use super::{error_code, JsonRpcV2, RequestId, RpcError};
use serde::{Deserialize, Serialize};

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
    /// Serialize a successful result, returning an internal RPC error if serialization fails.
    pub(crate) fn serialized(id: RequestId, result: impl Serialize) -> Self {
        match serde_json::to_value(result) {
            Ok(value) => Self::ok(id, value),
            Err(error) => Self::err(
                id,
                RpcError::new(
                    error_code::INTERNAL,
                    format!("could not serialize RPC response: {error}"),
                ),
            ),
        }
    }

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
