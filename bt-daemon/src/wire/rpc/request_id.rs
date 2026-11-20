use serde::{Deserialize, Serialize};

/// A JSON-RPC request id: an integer or a string.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RequestId {
    Int(i64),
    Str(String),
}
