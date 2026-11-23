use serde::{Deserialize, Serialize};
use serde_json::Value;

/// These values have schemas owned by the tool or provider, not by Muse tracing.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct ToolInput(pub Value);
