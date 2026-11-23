//! Reserved JSON-RPC error codes and application errors.

pub const PARSE: i32 = -32700;
pub const INVALID_REQUEST: i32 = -32600;
pub const METHOD_NOT_FOUND: i32 = -32601;
pub const INVALID_PARAMS: i32 = -32602;
pub const INTERNAL: i32 = -32603;
/// Application errors: -32000 ..= -32099.
pub const APP: i32 = -32000;
