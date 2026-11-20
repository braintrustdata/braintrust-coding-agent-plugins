//! bt-daemon wire protocol: the envelope types and JSON-RPC framing shared by
//! the daemon (`serve`) and the plugin shims (`hook`).
//!
//! Wire types own serialization and shared in-memory interpretation, without
//! I/O or async operations. The
//! canonical description of the protocol lives in `docs/protocol.md`; keep
//! the two in sync.

mod auth_selection;
mod auth_source;
mod backend_auth;
mod capture_context;
mod envelope;
#[cfg(test)]
mod envelope_tests;
mod flush_mode;
mod methods;
mod process_identity;
mod redacted_envelope;
mod rpc;
mod session_config;
mod session_route;
mod trace_destination;

pub use auth_selection::AuthSelection;
pub use auth_source::AuthSource;
pub use backend_auth::BackendAuth;
pub use capture_context::CaptureContext;
pub use envelope::Envelope;
pub use flush_mode::FlushMode;
pub use methods::{
    method, AuthDiagnoseParams, AuthDiagnoseResult, Capabilities, ClientInfo, EventLogResult,
    FlushParams, FlushResult, ImportFinishParams, ImportFinishResult, ImportResetParams,
    InitializeParams, InitializeResult, ManagedRunFlushParams, RecoveryInfo, SessionStatus,
    ShutdownResult, StatusParams, StatusResult,
};
pub use process_identity::ProcessIdentity;
pub use redacted_envelope::RedactedEnvelope;
pub use rpc::{error_code, Message, Request, RequestId, Response, RpcError};
pub use session_config::SessionConfig;
pub use session_route::SessionRoute;
pub use trace_destination::TraceDestination;

/// The protocol version this build speaks. Bumped on any breaking change to
/// the envelope or method contracts. See `docs/protocol.md`.
pub const PROTOCOL_VERSION: u32 = 1;
