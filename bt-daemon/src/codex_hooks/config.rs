use serde::Deserialize;

#[derive(Deserialize)]
pub(super) struct Config {
    #[serde(default)]
    pub(super) features: Features,
}

/// Only the flags setup reads. Codex reports every feature flag here, and some
/// are `null` when unset (for example `network_proxy`), so other flags must not
/// be decoded. `null` and an absent flag both mean Codex's default.
#[derive(Default, Deserialize)]
pub(super) struct Features {
    #[serde(default)]
    hooks: Option<bool>,
    #[serde(default)]
    codex_hooks: Option<bool>,
}

impl Config {
    pub(super) fn hooks_enabled(&self) -> bool {
        self.features.hooks != Some(false) && self.features.codex_hooks != Some(false)
    }
}
