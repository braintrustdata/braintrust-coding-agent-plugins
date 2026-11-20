use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub(super) struct TurnContextPayload {
    #[serde(default)]
    pub(super) model: Option<String>,
    #[serde(default)]
    pub(super) turn_id: Option<String>,
}
