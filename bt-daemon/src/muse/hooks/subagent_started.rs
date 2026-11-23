use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct SubagentStarted {
    pub child_session_id: Option<String>,
    pub subagent_id: Option<String>,
}
