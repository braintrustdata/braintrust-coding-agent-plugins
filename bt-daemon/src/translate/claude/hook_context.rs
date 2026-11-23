use super::native_field::{deserialize_optional_effort, deserialize_optional_string};
use serde::{Deserialize, Serialize};

#[derive(Default, Deserialize, Serialize)]
pub(super) struct HookContext {
    #[serde(default, deserialize_with = "deserialize_optional_string")]
    pub(super) cwd: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_string")]
    pub(super) transcript_path: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_string")]
    pub(super) source: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_string")]
    pub(super) model: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_string")]
    pub(super) permission_mode: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_effort")]
    pub(super) effort: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_string")]
    pub(super) system_prompt: Option<String>,
}
