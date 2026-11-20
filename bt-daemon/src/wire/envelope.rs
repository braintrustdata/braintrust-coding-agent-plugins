//! Native capture envelope and its journal-safe projection.

use super::{CaptureContext, RedactedEnvelope, SessionConfig, SessionRoute};
use serde::{Deserialize, Serialize};

/// One captured hook event, forwarded from a shim to the daemon.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    /// Which daemon-side translator interprets `payload` (e.g. `codex`,
    /// `claude-code`, `debug`).
    pub source: String,
    /// The agent version, for payload-drift handling. Optional.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_version: Option<String>,
    /// Version of the Braintrust instrumentation package that captured the
    /// event. Distinct from the coding agent's `source_version`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plugin_version: Option<String>,
    /// Per-session queue + state key.
    pub session_id: String,
    /// Agent-native hook event name (not normalized).
    pub event: String,
    /// Epoch milliseconds, stamped by the shim at capture time.
    pub ts_ms: i64,
    /// Invocation-local identifier supplied by `bt trace run`. The daemon uses
    /// it only to flush the sessions created by one managed child process tree.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub managed_run_id: Option<String>,
    /// Daemon-captured process ancestry for local cross-agent correlation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capture: Option<CaptureContext>,
    /// The raw agent-native hook payload; opaque except to the translator.
    pub payload: serde_json::Value,
    /// Non-secret, immutable routing intent for this session. New clients use
    /// this instead of resolving credentials themselves. The daemon host maps
    /// the selected profile and organization to live credentials.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route: Option<SessionRoute>,
    /// Daemon-internal resolved configuration. This field is never serialized;
    /// clients can only submit `route`.
    #[serde(skip)]
    pub config: Option<SessionConfig>,
}

impl Envelope {
    /// A copy safe to journal. Only the non-secret route is serializable; the
    /// daemon-internal resolved config is excluded by construction.
    pub fn redacted(&self) -> RedactedEnvelope {
        RedactedEnvelope {
            source: self.source.clone(),
            source_version: self.source_version.clone(),
            plugin_version: self.plugin_version.clone(),
            session_id: self.session_id.clone(),
            event: self.event.clone(),
            ts_ms: self.ts_ms,
            managed_run_id: self.managed_run_id.clone(),
            capture: self.capture.clone(),
            payload: self.payload.clone(),
            route: self.route.clone(),
        }
    }
}
