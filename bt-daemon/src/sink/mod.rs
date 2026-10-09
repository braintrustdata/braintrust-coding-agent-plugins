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
// `async_trait` marks its boxed futures as `must_use`; Clippy 1.99 flags that
// generated annotation as redundant on async trait methods.
#[allow(clippy::double_must_use)]
#[async_trait::async_trait]
pub trait Sink: Send {
    /// Called when the session's config is (re)resolved.
    fn configure(&mut self, config: &SessionConfig) {
        let _ = config;
    }

    /// Versions of the event currently being translated, including replay.
    fn set_capture_versions(
        &mut self,
        _plugin_version: Option<&str>,
        _source_version: Option<&str>,
    ) {
    }

    /// Emit span ops. Returns the number of rows written, for status counters.
    async fn emit(&mut self, ops: &[SpanOp]) -> anyhow::Result<u64>;

    /// Emit daemon-owned, payload-free failure markers without running them
    /// through user plugins or the completed-span suppression ledger.
    async fn emit_plugin_marker(&mut self, op: &SpanOp) -> anyhow::Result<u64> {
        self.emit(std::slice::from_ref(op)).await
    }

    /// Replace a temporary failure marker with the successfully transformed
    /// operation, including an explicit clear of the marker's error field.
    async fn replace_plugin_marker(&mut self, op: &SpanOp) -> anyhow::Result<u64> {
        self.emit(std::slice::from_ref(op)).await
    }

    /// Deliver everything buffered (bounded by the caller's flush timeout).
    async fn flush(&mut self) -> anyhow::Result<()>;

    /// Whether the sink is holding translated rows that have intentionally
    /// not been delivered yet. Callers must not checkpoint past them because
    /// a cold worker needs to rebuild and eventually deliver that state.
    fn has_pending_delivery(&self) -> bool {
        false
    }

    /// A user-facing trace permalink, once known.
    fn permalink(&self) -> Option<String> {
        None
    }
}

/// Builds a sink per session. Capture provenance belongs to span creation,
/// not to the event that happens to create the session actor.
pub trait SinkFactory: Send + Sync {
    fn create(&self, session_id: &str, source: &str) -> anyhow::Result<Box<dyn Sink>>;
}
