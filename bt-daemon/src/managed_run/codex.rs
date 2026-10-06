//! Codex managed runs enable hooks and define them with `-c` overrides.

use std::ffi::OsString;

use super::{managed_hook_shell_command, Injection, ManagedRun, Terminal};
use crate::agents::Codex;
use crate::args::RunHookCommand;

pub(super) const CODEX_RUN_HOOK_EVENTS: &[&str] = &[
    "SessionStart",
    "UserPromptSubmit",
    "PreToolUse",
    "PermissionRequest",
    "PostToolUse",
    "PreCompact",
    "PostCompact",
    "SubagentStart",
    "SubagentStop",
    "Stop",
    "SessionEnd",
];

fn codex_managed_run_args(unix_command: &str, windows_command: &str) -> Vec<OsString> {
    let unix_command = serde_json::to_string(unix_command).expect("serialize hook command");
    let windows_command =
        serde_json::to_string(windows_command).expect("serialize Windows hook command");
    let mut args = vec![OsString::from("--enable"), OsString::from("hooks")];
    for event in CODEX_RUN_HOOK_EVENTS {
        args.push(OsString::from("-c"));
        args.push(OsString::from(format!(
            "hooks.{event}=[{{hooks=[{{type=\"command\",command={unix_command},commandWindows={windows_command}}}]}}]"
        )));
    }
    args
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
        let unix_command = managed_hook_shell_command(hook_command, "codex", false)?;
        let windows_command = managed_hook_shell_command(hook_command, "codex", true)?;
        Ok(Injection {
            args: codex_managed_run_args(&unix_command, &windows_command),
            ..Injection::default()
        })
    }
}
