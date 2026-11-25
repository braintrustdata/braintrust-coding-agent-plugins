use serde::Deserialize;

#[derive(Deserialize)]
pub(super) struct Config {
    features: Option<Features>,
}

/// Only the hook toggles are modeled. Serde skips every other feature entry,
/// whose values are not all booleans (Codex 0.172.0 reports
/// `network_proxy: null`).
#[derive(Deserialize)]
struct Features {
    hooks: Option<bool>,
    codex_hooks: Option<bool>,
}

impl Config {
    pub(super) fn hooks_enabled(&self) -> bool {
        self.features.as_ref().is_none_or(|features| {
            features.hooks != Some(false) && features.codex_hooks != Some(false)
        })
    }
}
