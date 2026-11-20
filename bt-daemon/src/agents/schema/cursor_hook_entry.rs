use super::CursorCommandHook;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum CursorHookEntry {
    Command(CursorCommandHook),
    Other(Value),
}
impl CursorHookEntry {
    pub fn command(&self) -> Option<&str> {
        match self {
            Self::Command(hook) => Some(&hook.command),
            Self::Other(_) => None,
        }
    }
}
