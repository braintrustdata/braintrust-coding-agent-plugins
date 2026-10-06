//! Claude Code managed runs pass their hooks through `--settings`.

use std::ffi::OsString;

use super::{Injection, ManagedRun};
use crate::agents::Claude;
use crate::args::RunHookCommand;

const CLAUDE_RUN_HOOK_EVENTS: &[&str] = &[
    "SessionStart",
    "Setup",
    "UserPromptSubmit",
    "UserPromptExpansion",
    "PreToolUse",
    "PermissionRequest",
    "PermissionDenied",
    "PostToolUse",
    "PostToolUseFailure",
    "PostToolBatch",
    "PreCompact",
    "PostCompact",
    "Notification",
    "MessageDisplay",
    "SubagentStart",
    "SubagentStop",
    "TaskCreated",
    "TaskCompleted",
    "Stop",
    "StopFailure",
    "SessionEnd",
];

fn claude_managed_run_args(hook_command: &RunHookCommand) -> anyhow::Result<Vec<OsString>> {
    let command = hook_command
        .program
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("managed hook command contains non-Unicode argv"))?;
    let mut args = hook_command
        .args
        .iter()
        .map(|arg| {
            arg.to_str()
                .ok_or_else(|| anyhow::anyhow!("managed hook command contains non-Unicode argv"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    args.extend(["--source", "claude", "--managed-run-hook"]);
    let matcher_group = serde_json::json!([{
        "hooks": [{
            "type": "command",
            "command": command,
            "args": args,
            "async": false
        }]
    }]);
    let hooks = CLAUDE_RUN_HOOK_EVENTS
        .iter()
        .map(|event| ((*event).to_string(), matcher_group.clone()))
        .collect::<serde_json::Map<_, _>>();
    Ok(vec![
        OsString::from("--settings"),
        OsString::from(serde_json::to_string(
            &serde_json::json!({ "hooks": hooks }),
        )?),
    ])
}

impl ManagedRun for Claude {
    fn executable(&self) -> (&'static str, &'static str) {
        ("CLAUDE_BIN", "claude")
    }

    fn inject(
        &self,
        hook_command: &RunHookCommand,
        _managed_run_id: &str,
    ) -> anyhow::Result<Injection> {
        Ok(Injection {
            args: claude_managed_run_args(hook_command)?,
            ..Injection::default()
        })
    }
}
