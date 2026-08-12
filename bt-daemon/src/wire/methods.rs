//! Method names and their param/result types.

use serde::{Deserialize, Serialize};

/// Method name constants — one source of truth for both sides.
pub mod method {
    pub const INITIALIZE: &str = "initialize";
    pub const EVENT_LOG: &str = "event.log";
    pub const SESSION_FLUSH: &str = "session.flush";
    pub const MANAGED_RUN_FLUSH: &str = "managed_run.flush";
    pub const STATUS_GET: &str = "status.get";
    pub const DAEMON_SHUTDOWN: &str = "daemon.shutdown";
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitializeParams {
    pub protocol_version: u32,
    pub client: ClientInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientInfo {
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plugin_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitializeResult {
    pub protocol_version: u32,
    pub daemon_version: String,
    #[serde(default)]
    pub capabilities: Capabilities,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Capabilities {
    #[serde(default)]
    pub sources: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventLogResult {
    pub accepted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlushParams {
    pub session_id: String,
    #[serde(default = "default_flush_timeout_ms")]
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManagedRunFlushParams {
    pub managed_run_id: String,
    #[serde(default = "default_flush_timeout_ms")]
    pub timeout_ms: u64,
}

fn default_flush_timeout_ms() -> u64 {
    10_000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlushResult {
    pub flushed: bool,
    pub pending: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StatusParams {
    /// Omit for daemon-wide status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusResult {
    pub daemon_version: String,
    pub uptime_ms: u64,
    pub sessions: Vec<SessionStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionStatus {
    pub session_id: String,
    pub source: String,
    pub queued: u64,
    pub spans_emitted: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permalink: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShutdownResult {
    pub ok: bool,
}
