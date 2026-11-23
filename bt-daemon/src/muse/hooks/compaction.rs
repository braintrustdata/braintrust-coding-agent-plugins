use super::NativeOutcome;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct Compaction {
    pub trigger: Option<String>,
    #[serde(flatten)]
    pub outcome: NativeOutcome,
}
