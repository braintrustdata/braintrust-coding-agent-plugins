use super::ToolCall;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum ToolCalls {
    Many(Vec<ToolCall>),
    One(ToolCall),
}
impl ToolCalls {
    pub fn iter(&self) -> std::slice::Iter<'_, ToolCall> {
        match self {
            Self::Many(calls) => calls.iter(),
            Self::One(call) => std::slice::from_ref(call).iter(),
        }
    }
}
