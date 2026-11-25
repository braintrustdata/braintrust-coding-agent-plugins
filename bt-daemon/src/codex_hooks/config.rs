use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize)]
pub(super) struct Config {
    // Codex feature values are not all booleans (e.g. `network_proxy: null`),
    // so only the hook toggles are interpreted and everything else is ignored.
    #[serde(default)]
    features: Value,
}

impl Config {
    pub(super) fn hooks_enabled(&self) -> bool {
        let disabled = |key| self.features.get(key) == Some(&Value::Bool(false));
        !disabled("hooks") && !disabled("codex_hooks")
    }
}
