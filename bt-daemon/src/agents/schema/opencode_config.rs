use super::{deserialize_present, OpenCodePlugin};
use crate::json::Field;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct OpenCodeConfig {
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub model: Field<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present"
    )]
    pub(super) plugin: Option<Vec<OpenCodePlugin>>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl OpenCodeConfig {
    pub fn find_plugin(
        &self,
        mut matches: impl FnMut(&OpenCodePlugin) -> bool,
    ) -> Option<&OpenCodePlugin> {
        self.plugin.as_ref()?.iter().find(|plugin| matches(plugin))
    }

    /// Preserve absence when removing plugins from an unconfigured document.
    pub fn retain_plugins(&mut self, keep: impl FnMut(&OpenCodePlugin) -> bool) {
        if let Some(plugins) = &mut self.plugin {
            plugins.retain(keep);
        }
    }

    pub fn replace_plugin(
        &mut self,
        mut matches: impl FnMut(&OpenCodePlugin) -> bool,
        plugin: OpenCodePlugin,
    ) {
        self.retain_plugins(|entry| !matches(entry));
        self.plugin.get_or_insert_with(Vec::new).push(plugin);
    }

    /// Managed runs add an exact spec only once; differently configured specs
    /// are left alone because their options belong to the user's invocation.
    pub fn ensure_plugin(&mut self, package: String) {
        if self
            .find_plugin(|plugin| plugin.package() == Some(&package))
            .is_none()
        {
            self.plugin
                .get_or_insert_with(Vec::new)
                .push(OpenCodePlugin::Package(package));
        }
    }
}
