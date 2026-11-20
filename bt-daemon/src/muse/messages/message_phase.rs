use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MessagePhase {
    Analysis,
    Commentary,
    FinalAnswer,
    #[serde(untagged)]
    Other(String),
}
