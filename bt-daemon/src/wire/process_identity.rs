use serde::{Deserialize, Serialize};

/// One operating-system process observed while capturing an event.
///
/// A PID alone is not a stable identity because operating systems reuse it.
/// `start_time_secs`, when available, distinguishes different occupants of the
/// same PID.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProcessIdentity {
    pub pid: u32,
    /// Epoch seconds reported by the operating system, or zero if unavailable.
    #[serde(default)]
    pub start_time_secs: u64,
}
