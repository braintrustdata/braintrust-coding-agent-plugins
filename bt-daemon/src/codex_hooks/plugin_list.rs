use super::plugin_marketplace::PluginMarketplace;
use serde::Deserialize;

#[derive(Deserialize)]
pub(super) struct PluginList {
    pub(super) marketplaces: Vec<PluginMarketplace>,
}
impl PluginList {
    pub(super) fn has_active_plugin(&self, id: &str) -> bool {
        self.marketplaces
            .iter()
            .flat_map(|marketplace| &marketplace.plugins)
            .any(|plugin| plugin.is_active(id))
    }
}
