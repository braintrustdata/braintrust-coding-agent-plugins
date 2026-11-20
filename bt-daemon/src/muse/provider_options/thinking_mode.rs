use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum ThinkingMode {
    Enabled,
    Disabled,
    Adaptive,
    #[serde(untagged)]
    Other(String),
}
