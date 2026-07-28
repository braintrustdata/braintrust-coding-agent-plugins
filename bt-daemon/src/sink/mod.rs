//! Sinks consume [`SpanOp`]s. Phase 1 shipped the debug sink (dumps ops to
//! NDJSON); Phase 2 adds the Braintrust sink over `braintrust-sdk-rust`.
//!
//! The trait is async so the Braintrust sink can drive the SDK's async
//! `flush`. `emit` is called on the per-session hot path; the SDK's `log`/`end`
//! are synchronous fire-and-forget (queue-backed), so `emit` rarely awaits.

mod braintrust;
mod debug;

pub use braintrust::{BraintrustSinkConfig, BraintrustSinkFactory};
pub use debug::DebugSinkFactory;

use crate::translate::SpanOp;
use crate::wire::SessionConfig;

/// A per-session sink. Created once per session; `configure` supplies the
/// resolved credentials/project/trace-attach settings (and may be re-called if
/// they change).
#[async_trait::async_trait]
pub trait Sink: Send {
    /// Called when the session's config is (re)resolved.
    fn configure(&mut self, config: &SessionConfig) {
        let _ = config;
    }

    /// Emit span ops. Returns the number of rows written, for status counters.
    async fn emit(&mut self, ops: &[SpanOp]) -> anyhow::Result<u64>;

    /// Deliver everything buffered (bounded by the caller's flush timeout).
    async fn flush(&mut self) -> anyhow::Result<()>;

    /// A user-facing trace permalink, once known.
    fn permalink(&self) -> Option<String> {
        None
    }
}

/// Builds a sink per session. `source` is the agent id (e.g. `codex`), used by
/// the Braintrust sink to stamp `context.span_origin`.
pub trait SinkFactory: Send + Sync {
    fn create(&self, session_id: &str, source: &str) -> anyhow::Result<Box<dyn Sink>>;
}
