//! OpenCode managed runs add the tracing plugin to inline config.

use super::{Injection, ManagedRun};
use crate::agents::OpenCode;
use crate::args::RunHookCommand;

fn opencode_managed_config(existing: Option<&str>) -> anyhow::Result<String> {
    let mut config = match existing {
        Some(raw) => serde_json::from_str::<serde_json::Value>(raw)
            .map_err(|error| anyhow::anyhow!("invalid OPENCODE_CONFIG_CONTENT: {error}"))?,
        None => serde_json::json!({}),
    };
    let object = config
        .as_object_mut()
        .ok_or_else(|| anyhow::anyhow!("OPENCODE_CONFIG_CONTENT must be a JSON object"))?;
    let plugins = object
        .entry("plugin")
        .or_insert_with(|| serde_json::Value::Array(Vec::new()))
        .as_array_mut()
        .ok_or_else(|| anyhow::anyhow!("OPENCODE_CONFIG_CONTENT.plugin must be an array"))?;
    let plugin = std::env::var("BT_TRACE_OPENCODE_PLUGIN_SPEC")
        .unwrap_or_else(|_| "@braintrust/trace-opencode/tracing".to_string());
    if !plugins.iter().any(|value| value.as_str() == Some(&plugin)) {
        plugins.push(serde_json::Value::String(plugin));
    }
    Ok(serde_json::to_string(&config)?)
}

impl ManagedRun for OpenCode {
    fn executable(&self) -> (&'static str, &'static str) {
        ("OPENCODE_BIN", "opencode")
    }

    fn inject(
        &self,
        _hook_command: &RunHookCommand,
        _managed_run_id: &str,
    ) -> anyhow::Result<Injection> {
        let config =
            opencode_managed_config(std::env::var("OPENCODE_CONFIG_CONTENT").ok().as_deref())?;
        Ok(Injection {
            env: vec![("OPENCODE_CONFIG_CONTENT", config)],
            ..Injection::default()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opencode_managed_run_preserves_inline_config_and_adds_plugin() {
        let config =
            opencode_managed_config(Some(r#"{"model":"test/model","plugin":["other"]}"#)).unwrap();
        let config: serde_json::Value = serde_json::from_str(&config).unwrap();
        assert_eq!(config["model"], "test/model");
        assert_eq!(
            config["plugin"],
            serde_json::json!(["other", "@braintrust/trace-opencode/tracing"])
        );
        assert!(OpenCode
            .inject(
                &RunHookCommand {
                    program: "bt".into(),
                    args: Vec::new(),
                },
                "run",
            )
            .unwrap()
            .args
            .is_empty());
    }
}
