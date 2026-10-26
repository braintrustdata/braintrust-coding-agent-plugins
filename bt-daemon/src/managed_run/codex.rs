//! Codex managed runs enable hooks and define them with `-c` overrides.

use std::ffi::OsString;

use super::{managed_hook_shell_command, Injection, ManagedRun, Terminal};
use crate::agents::Codex;
use crate::args::RunHookCommand;

pub(super) const PACKAGED_HOOKS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../src/plugins/codex/content/plugins/trace-codex/hooks/hooks.json"
));

pub(super) fn managed_commands(hook_command: &RunHookCommand) -> anyhow::Result<(String, String)> {
    Ok((
        managed_hook_shell_command(hook_command, "codex", false)?,
        managed_hook_shell_command(hook_command, "codex", true)?,
    ))
}

fn codex_managed_run_args(
    unix_command: &str,
    windows_command: &str,
) -> anyhow::Result<Vec<OsString>> {
    let manifest: serde_json::Value = serde_json::from_str(PACKAGED_HOOKS)?;
    let mut args = vec![OsString::from("--enable"), OsString::from("hooks")];
    for (event, groups) in manifest["hooks"]
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("Codex hook manifest has no hooks map"))?
    {
        let mut groups = groups.clone();
        for group in groups
            .as_array_mut()
            .ok_or_else(|| anyhow::anyhow!("Codex hook groups must be an array"))?
        {
            for hook in group["hooks"]
                .as_array_mut()
                .ok_or_else(|| anyhow::anyhow!("Codex hook group has no hooks array"))?
            {
                anyhow::ensure!(hook["type"] == "command", "Unsupported Codex hook type");
                hook["command"] = serde_json::Value::String(unix_command.to_owned());
                hook["commandWindows"] = serde_json::Value::String(windows_command.to_owned());
            }
        }
        args.push(OsString::from("-c"));
        args.push(OsString::from(format!(
            "hooks.{event}={}",
            toml_literal(&groups)?
        )));
    }
    Ok(args)
}

fn toml_literal(value: &serde_json::Value) -> anyhow::Result<String> {
    Ok(match value {
        serde_json::Value::Null => anyhow::bail!("null is unsupported in Codex hook config"),
        serde_json::Value::Bool(value) => value.to_string(),
        serde_json::Value::Number(value) => value.to_string(),
        serde_json::Value::String(value) => serde_json::to_string(value)?,
        serde_json::Value::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(toml_literal)
                .collect::<anyhow::Result<Vec<_>>>()?
                .join(",")
        ),
        serde_json::Value::Object(values) => format!(
            "{{{}}}",
            values
                .iter()
                .map(|(key, value)| Ok(format!(
                    "{}={}",
                    serde_json::to_string(key)?,
                    toml_literal(value)?
                )))
                .collect::<anyhow::Result<Vec<_>>>()?
                .join(",")
        ),
    })
}

impl ManagedRun for Codex {
    fn executable(&self) -> (&'static str, &'static str) {
        ("CODEX_BIN", "codex")
    }

    fn check(&self, agent_args: &[OsString], _terminal: Terminal) -> anyhow::Result<()> {
        if agent_args.iter().any(|arg| {
            let arg = arg.to_string_lossy();
            arg == "--dangerously-bypass-hook-trust"
                || arg.starts_with("--dangerously-bypass-hook-trust=")
        }) {
            anyhow::bail!(
                "bt trace run codex cannot be combined with --dangerously-bypass-hook-trust; managed tracing preserves Codex hook trust. Remove the flag, or run Codex directly"
            );
        }
        Ok(())
    }

    fn inject(
        &self,
        hook_command: &RunHookCommand,
        _managed_run_id: &str,
    ) -> anyhow::Result<Injection> {
        let (unix_command, windows_command) = managed_commands(hook_command)?;
        Ok(Injection {
            args: codex_managed_run_args(&unix_command, &windows_command)?,
            ..Injection::default()
        })
    }
}
