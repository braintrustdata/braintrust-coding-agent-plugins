use super::{ModelOutput, NativeOutcome};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct TurnFinished {
    pub last_assistant_message: Option<ModelOutput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminal: Option<String>,
    #[serde(flatten)]
    pub outcome: NativeOutcome,
}
