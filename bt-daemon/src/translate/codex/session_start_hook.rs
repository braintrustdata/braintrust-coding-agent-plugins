use serde::{Deserialize, Serialize};

// Codex hooks are triggers for the rollout reader. These partial types cover
// the hook-owned correlation fields while preserving raw rollout JSON for the
// separately bounded transcript reducer.
#[derive(Deserialize, Serialize)]
pub(super) struct SessionStartHook {
    #[serde(default)]
    pub(super) source: Option<String>,
    #[serde(default)]
    pub(super) permission_mode: Option<String>,
}
