use super::FunctionType;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
pub(super) enum ToolChoice {
    Mode(ChoiceMode),
    Named(NamedChoice),
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(super) enum ChoiceMode {
    Auto,
    None,
    Required,
}

#[derive(Serialize, Deserialize)]
pub(super) struct NamedChoice {
    #[serde(rename = "type")]
    kind: FunctionType,
    function: FunctionName,
}

#[derive(Serialize, Deserialize)]
struct FunctionName {
    name: String,
}
