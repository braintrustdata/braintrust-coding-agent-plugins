use super::HookContext;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct Hook<B> {
    #[serde(flatten)]
    pub context: HookContext,
    #[serde(flatten)]
    pub body: B,
}
