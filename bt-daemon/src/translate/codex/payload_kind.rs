use serde::{Deserialize, Serialize};

/// Event and response rows use a second discriminator inside their payload.
#[derive(Deserialize, Serialize)]
pub(super) struct PayloadKind {
    #[serde(rename = "type", default)]
    pub(super) kind: Option<String>,
}
