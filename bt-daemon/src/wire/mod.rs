//! bt-daemon wire protocol: the envelope types and JSON-RPC framing shared by
//! the daemon (`serve`) and the plugin shims (`hook`).
//!
//! This module is pure data + (de)serialization — no I/O, no async. The
//! canonical description of the protocol lives in `docs/protocol.md`; keep
//! the two in sync.

mod envelope;
mod methods;
mod rpc;

pub use envelope::{
    AuthSelection, AuthSource, BackendAuth, CaptureContext, Envelope, FlushMode, ProcessIdentity,
    RedactedEnvelope, SessionConfig, SessionRoute, TraceDestination,
};
pub use methods::{
    method, Capabilities, ClientInfo, EventLogResult, FlushParams, FlushResult, InitializeParams,
    InitializeResult, ManagedRunFlushParams, SessionStatus, ShutdownResult, StatusParams,
    StatusResult,
};
pub use rpc::{error_code, Message, Request, RequestId, Response, RpcError};

/// The protocol version this build speaks. Bumped on any breaking change to
/// the envelope or method contracts. See `docs/protocol.md`.
pub const PROTOCOL_VERSION: u32 = 1;
