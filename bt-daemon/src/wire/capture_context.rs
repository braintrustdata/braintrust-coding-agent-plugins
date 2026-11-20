use super::ProcessIdentity;
use serde::{Deserialize, Serialize};

/// Process evidence captured at the hook or in-process adapter boundary.
///
/// `process_chain` is ordered from the process that connected to the daemon
/// toward the operating-system root. It intentionally excludes full command
/// lines, environment variables, and working directories.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CaptureContext {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub process_chain: Vec<ProcessIdentity>,
    /// The process walk stopped before reaching the operating-system root.
    #[serde(default)]
    pub truncated: bool,
}
