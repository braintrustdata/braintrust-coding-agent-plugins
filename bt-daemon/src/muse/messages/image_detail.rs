use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum ImageDetail {
    Auto,
    Low,
    High,
    #[serde(untagged)]
    Other(String),
}
