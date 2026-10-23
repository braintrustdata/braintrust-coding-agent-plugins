//! Settings projection with lossless unrelated fields and explicit missing/null
//! semantics. The caller holds Muse's native lock while loading and writing.
use super::hooks::HookOverlay;
use crate::muse::field::Field;
use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

#[derive(Default, Serialize, Deserialize)]
pub(crate) struct MuseSettings {
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub schema_version: Field<u32>,
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub managed_hooks_path: Field<PathBuf>,
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    pub hooks: Field<HookOverlay>,
    #[serde(flatten)]
    extra: BTreeMap<String, serde_json::Value>,
}
impl MuseSettings {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        match std::fs::read(path) {
            Ok(raw) => serde_json::from_slice(&raw)
                .with_context(|| format!("read Muse settings {}", path.display())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(error) => {
                Err(error).with_context(|| format!("read Muse settings {}", path.display()))
            }
        }
    }
    pub fn managed_path(&self, config_dir: &Path) -> Option<PathBuf> {
        self.managed_hooks_path.as_ref().map(|path| {
            if path.is_absolute() {
                path.clone()
            } else {
                config_dir.join(path)
            }
        })
    }
    pub fn hook_overlay(&self) -> anyhow::Result<HookOverlay> {
        match &self.hooks {
            Field::Missing => Ok(HookOverlay::default()),
            Field::Value(hooks) => Ok(hooks.clone()),
            Field::Null => anyhow::bail!("Muse user hooks must be an object"),
        }
    }
    pub fn ensure_schema_version(&mut self) {
        if self.schema_version.is_missing() {
            self.schema_version = Field::Value(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn settings_preserve_unrelated_data_and_missing_vs_null() {
        for source in [
            json!({"theme":{"future":[true,null,2]},"managed_hooks_path":null,"schema_version":null}),
            json!({"theme":"dark"}),
        ] {
            let settings: MuseSettings = serde_json::from_value(source.clone()).unwrap();
            assert_eq!(serde_json::to_value(settings).unwrap(), source);
        }
        let mut absent: MuseSettings = serde_json::from_value(json!({})).unwrap();
        absent.ensure_schema_version();
        assert_eq!(
            serde_json::to_value(absent).unwrap(),
            json!({"schema_version":1})
        );
        let mut null: MuseSettings =
            serde_json::from_value(json!({"schema_version":null})).unwrap();
        null.ensure_schema_version();
        assert_eq!(
            serde_json::to_value(null).unwrap(),
            json!({"schema_version":null})
        );
    }
    #[test]
    fn managed_paths_resolve_relative_and_absolute_and_null_hooks_are_invalid() {
        let base = tempfile::tempdir().unwrap();
        let relative: MuseSettings =
            serde_json::from_value(json!({"managed_hooks_path":"enterprise.json"})).unwrap();
        assert_eq!(
            relative.managed_path(base.path()).unwrap(),
            base.path().join("enterprise.json")
        );
        let absolute = base.path().join("absolute.json");
        let settings: MuseSettings =
            serde_json::from_value(json!({"managed_hooks_path":absolute,"hooks":null})).unwrap();
        assert_eq!(settings.managed_path(base.path()).unwrap(), absolute);
        assert!(settings.hook_overlay().is_err());
    }
}
