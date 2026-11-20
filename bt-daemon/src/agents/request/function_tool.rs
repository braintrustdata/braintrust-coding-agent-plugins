use super::FunctionDefinition;
use crate::json::Extensions;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub(super) struct FunctionTool {
    #[serde(rename = "type")]
    kind: FunctionType,
    function: FunctionDefinition,
    #[serde(flatten, deserialize_with = "crate::json::deserialize_extensions")]
    extensions: Extensions,
}

#[derive(Serialize, Deserialize)]
pub(super) enum FunctionType {
    #[serde(rename = "function")]
    Function,
}
