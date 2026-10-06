//! OpenCode: the npm plugin listed in `opencode.json`. OpenCode installs it
//! from that entry, so no OpenCode CLI is involved.

use super::common::{edit_object, npm_major_spec, plugin_source, FileAccess};
use crate::paths;
use anyhow::bail;
use serde_json::{Map, Value};
use std::path::{Path, PathBuf};

const PACKAGE: &str = "@braintrust/trace-opencode";
const PACKAGE_MANIFEST: &str = plugin_source!("opencode/content/package.json");
const NOT_INSTALLED: &str =
    "OpenCode tracing plugin is not installed; run `bt trace enable opencode`";

/// The plugin entry for the bundled major version, e.g. `…@^1`.
fn plugin_spec() -> anyhow::Result<String> {
    npm_major_spec(PACKAGE, PACKAGE_MANIFEST)
}

/// `opencode.json` sits beside the agent's Braintrust settings file.
fn config_path() -> PathBuf {
    paths::agent_settings_path("opencode", None)
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("opencode.json")
}

fn is_ours(plugin: &Value) -> bool {
    plugin.as_str().is_some_and(|plugin| {
        plugin == PACKAGE
            || plugin
                .strip_prefix(PACKAGE)
                .is_some_and(|version| version.starts_with('@'))
    })
}

fn plugin_list<'a>(
    config: &'a mut Map<String, Value>,
    path: &Path,
) -> anyhow::Result<&'a mut Vec<Value>> {
    config
        .entry("plugin")
        .or_insert_with(|| Value::Array(Vec::new()))
        .as_array_mut()
        .ok_or_else(|| {
            anyhow::anyhow!(
                "OpenCode `plugin` config must be an array: {}",
                path.display()
            )
        })
}

/// Replace any entry for the plugin with the bundled major version.
fn pin_plugin(plugins: &mut Vec<Value>) -> anyhow::Result<()> {
    plugins.retain(|plugin| !is_ours(plugin));
    plugins.push(Value::String(plugin_spec()?));
    Ok(())
}

fn enable_at(path: &Path) -> anyhow::Result<()> {
    edit_object(path, FileAccess::Inherited, |config, _| {
        pin_plugin(plugin_list(config, path)?)
    })
}

fn update_at(path: &Path) -> anyhow::Result<()> {
    edit_object(path, FileAccess::Inherited, |config, existed| {
        if !existed {
            bail!(NOT_INSTALLED);
        }
        let plugins = config
            .get_mut("plugin")
            .and_then(Value::as_array_mut)
            .filter(|plugins| plugins.iter().any(is_ours))
            .ok_or_else(|| anyhow::anyhow!(NOT_INSTALLED))?;
        pin_plugin(plugins)
    })
}

fn disable_at(path: &Path) -> anyhow::Result<()> {
    edit_object(path, FileAccess::Inherited, |config, _| {
        if config.contains_key("plugin") {
            plugin_list(config, path)?.retain(|plugin| !is_ours(plugin));
        }
        Ok(())
    })
}

pub(super) fn enable() -> anyhow::Result<()> {
    enable_at(&config_path())
}

pub(super) fn disable() -> anyhow::Result<()> {
    disable_at(&config_path())
}

pub(super) fn update() -> anyhow::Result<()> {
    update_at(&config_path())
}

/// Whether `opencode.json` pins a version other than the bundled one.
pub(super) fn stale() -> bool {
    let Ok(raw) = std::fs::read(config_path()) else {
        return false;
    };
    let Ok(config) = serde_json::from_slice::<Value>(&raw) else {
        return false;
    };
    let expected = plugin_spec().ok();
    config
        .get("plugin")
        .and_then(Value::as_array)
        .and_then(|plugins| plugins.iter().find(|plugin| is_ours(plugin)))
        .and_then(Value::as_str)
        .is_some_and(|plugin| Some(plugin) != expected.as_deref())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconciles_the_published_plugin_and_preserves_config() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.json");
        std::fs::write(
            &path,
            r#"{"plugin":["other","@braintrust/trace-opencode@0.9.0"],"model":"test/model"}"#,
        )
        .unwrap();

        enable_at(&path).unwrap();

        let config: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(config["model"], "test/model");
        assert_eq!(
            config["plugin"],
            serde_json::json!(["other", plugin_spec().unwrap()])
        );
    }

    #[test]
    fn enable_creates_a_missing_config() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode/opencode.json");

        enable_at(&path).unwrap();

        let config: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(
            config["plugin"],
            serde_json::json!([plugin_spec().unwrap()])
        );
    }

    #[test]
    fn update_requires_an_existing_plugin_and_preserves_other_config() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.json");
        std::fs::write(
            &path,
            r#"{"plugin":["other","@braintrust/trace-opencode@^1"],"model":"test/model"}"#,
        )
        .unwrap();

        update_at(&path).unwrap();

        let config: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(config["model"], "test/model");
        assert_eq!(
            config["plugin"],
            serde_json::json!(["other", plugin_spec().unwrap()])
        );

        let missing = temp.path().join("missing.json");
        assert!(update_at(&missing).is_err());
        assert!(!missing.exists());

        let unrelated = temp.path().join("unrelated.json");
        std::fs::write(&unrelated, r#"{"plugin":["other"]}"#).unwrap();
        assert_eq!(
            update_at(&unrelated).unwrap_err().to_string(),
            NOT_INSTALLED
        );
    }

    #[test]
    fn disable_removes_only_the_managed_plugin() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.json");
        std::fs::write(
            &path,
            serde_json::to_vec(&serde_json::json!({
                "plugin": ["other", "@braintrust/trace-opencode-extra", plugin_spec().unwrap()],
                "model": "test/model",
            }))
            .unwrap(),
        )
        .unwrap();

        disable_at(&path).unwrap();

        let config: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(
            config["plugin"],
            serde_json::json!(["other", "@braintrust/trace-opencode-extra"])
        );
        assert_eq!(config["model"], "test/model");
    }

    #[test]
    fn disable_does_not_create_a_missing_config_directory() {
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path().join("opencode");

        disable_at(&dir.join("opencode.json")).unwrap();

        assert!(!dir.exists());
    }
}
