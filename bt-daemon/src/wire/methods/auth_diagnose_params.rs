use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AuthDiagnoseParams {
    #[serde(default)]
    pub auth: crate::wire::AuthSelection,
}
