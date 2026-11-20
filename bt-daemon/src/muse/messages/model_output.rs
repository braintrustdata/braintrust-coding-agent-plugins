use super::NativeMessage;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum ModelOutput {
    Text(String),
    Messages(Vec<NativeMessage>),
}
