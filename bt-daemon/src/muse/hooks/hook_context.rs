use super::optional_id;
use super::SessionMetadata;
use crate::muse::ids::{RequestId, TurnId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct HookContext {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(
        default,
        deserialize_with = "optional_id",
        skip_serializing_if = "Option::is_none"
    )]
    pub turn_id: Option<TurnId>,
    #[serde(
        default,
        deserialize_with = "optional_id",
        skip_serializing_if = "Option::is_none"
    )]
    pub request_id: Option<RequestId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transcript_path: Option<String>,
    #[serde(flatten)]
    pub session: SessionMetadata,
}

impl HookContext {
    pub fn turn(&self) -> Option<&str> {
        self.turn_id.as_deref().filter(|s| !s.is_empty())
    }
    pub fn request(&self) -> Option<&str> {
        self.request_id.as_deref().filter(|s| !s.is_empty())
    }
}
