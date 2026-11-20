use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Deserialize)]
pub(super) struct Config {
    #[serde(default)]
    pub(super) features: BTreeMap<String, bool>,
}

impl Config {
    pub(super) fn hooks_enabled(&self) -> bool {
        self.features.get("hooks") != Some(&false)
            && self.features.get("codex_hooks") != Some(&false)
    }
}
