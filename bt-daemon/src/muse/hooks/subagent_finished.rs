use super::SubagentStarted;
use super::{ModelOutput, NativeOutcome};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct SubagentFinished {
    #[serde(flatten)]
    pub child: SubagentStarted,
    pub last_assistant_message: Option<ModelOutput>,
    #[serde(flatten)]
    pub outcome: NativeOutcome,
}
