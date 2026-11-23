use super::native_field::{deserialize_nonempty_string, deserialize_optional_string};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub(super) struct SubagentHook {
    #[serde(deserialize_with = "deserialize_nonempty_string")]
    pub(super) agent_id: String,
    #[serde(default, deserialize_with = "deserialize_optional_string")]
    pub(super) agent_type: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_string")]
    pub(super) agent_transcript_path: Option<String>,
}
