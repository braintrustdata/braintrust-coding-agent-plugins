use super::plugin::Plugin;
use serde::Deserialize;

#[derive(Deserialize)]
pub(super) struct PluginMarketplace {
    pub(super) plugins: Vec<Plugin>,
}
