use super::native_field::{deserialize_nonempty_string, deserialize_optional_string};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub(crate) struct SubagentHook {
    #[serde(deserialize_with = "deserialize_nonempty_string")]
    pub(crate) agent_id: String,
    #[serde(default, deserialize_with = "deserialize_optional_string")]
    pub(super) agent_type: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_string")]
    pub(super) agent_transcript_path: Option<String>,
    #[serde(rename = "_bt_import_background", default)]
    pub(crate) import_background: bool,
}
