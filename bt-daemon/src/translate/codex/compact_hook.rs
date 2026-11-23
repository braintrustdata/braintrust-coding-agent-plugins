use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub(super) struct CompactHook {
    pub(super) turn_id: String,
    #[serde(default)]
    pub(super) trigger: Option<String>,
}
