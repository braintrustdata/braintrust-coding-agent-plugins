//! OpenCode managed runs add the tracing plugin to inline config.

use super::{Injection, ManagedRun};
use crate::agents::OpenCode;
use crate::args::RunHookCommand;
use std::ffi::OsString;

fn is_v2() -> bool {
    let executable = crate::env::var_os("OPENCODE_BIN").unwrap_or_else(|| "opencode".into());
    crate::subprocess::background_command(executable)
        .arg("--version")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .is_some_and(|output| {
            String::from_utf8_lossy(&output.stdout)
                .split_whitespace()
                .last()
                .is_some_and(|v| v.trim_start_matches('v').starts_with("2."))
        })
}

fn opencode_managed_config(existing: Option<&str>, v2: bool) -> anyhow::Result<String> {
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
    let plugin = crate::env::var("BRAINTRUST_TRACE_OPENCODE_PLUGIN_SPEC").unwrap_or_else(|| {
        if v2 {
            "@braintrust/trace-opencode"
        } else {
            "@braintrust/trace-opencode/tracing"
        }
        .to_string()
    });
    if !plugins.iter().any(|value| value.as_str() == Some(&plugin)) {
        plugins.push(serde_json::Value::String(plugin));
    }
    Ok(serde_json::to_string(&config)?)
}

impl ManagedRun for OpenCode {
    fn executable(&self) -> (&'static str, &'static str) {
        ("OPENCODE_BIN", "opencode")
    }

    fn check(&self, args: &[OsString], _terminal: super::Terminal) -> anyhow::Result<()> {
        if is_v2()
            && args.iter().any(|arg| {
                arg == "--server"
                    || arg.to_string_lossy().starts_with("--server=")
                    || arg == "--service"
            })
        {
            anyhow::bail!("OpenCode 2 managed tracing requires a private server; remove --server");
        }
        Ok(())
    }

    fn invocation_args(&self, injection: Vec<OsString>, mut args: Vec<OsString>) -> Vec<OsString> {
        if injection.first().is_some_and(|arg| arg == "--standalone")
            && args.first().is_some_and(|arg| arg == "serve")
        {
            // `serve` already starts its own server and does not accept --standalone.
            return args;
        }
        if injection.first().is_some_and(|arg| arg == "--standalone")
            && args
                .first()
                .is_some_and(|arg| arg == "run" || arg == "mini")
        {
            let command = args.remove(0);
            return std::iter::once(command)
                .chain(injection)
                .chain(args)
                .collect();
        }
        injection.into_iter().chain(args).collect()
    }

    fn inject(
        &self,
        _hook_command: &RunHookCommand,
        _managed_run_id: &str,
    ) -> anyhow::Result<Injection> {
        let v2 = is_v2();
        let config =
            opencode_managed_config(std::env::var("OPENCODE_CONFIG_CONTENT").ok().as_deref(), v2)?;
        Ok(Injection {
            env: vec![("OPENCODE_CONFIG_CONTENT", config)],
            args: if v2 {
                vec!["--standalone".into()]
            } else {
                Vec::new()
            },
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
            opencode_managed_config(Some(r#"{"model":"test/model","plugin":["other"]}"#), false)
                .unwrap();
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

    #[test]
    fn opencode_v2_managed_run_uses_server_package() {
        let config =
            opencode_managed_config(Some(r#"{"model":"test/model","plugin":["other"]}"#), true)
                .unwrap();
        let config: serde_json::Value = serde_json::from_str(&config).unwrap();
        assert_eq!(config["model"], "test/model");
        assert_eq!(
            config["plugin"],
            serde_json::json!(["other", "@braintrust/trace-opencode"])
        );
    }

    #[test]
    fn opencode_v2_private_flag_follows_subcommand() {
        let private = vec![OsString::from("--standalone")];
        assert_eq!(
            OpenCode.invocation_args(private.clone(), vec!["run".into(), "hello".into()]),
            vec!["run", "--standalone", "hello"]
        );
        assert_eq!(
            OpenCode.invocation_args(
                private,
                vec!["serve".into(), "--port".into(), "1234".into()]
            ),
            vec!["serve", "--port", "1234"]
        );
    }
}
