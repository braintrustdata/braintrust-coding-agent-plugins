//! Codex managed runs enable hooks and define them with `-c` overrides.

use std::ffi::OsString;

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

pub(super) fn codex_managed_run_args(unix_command: &str, windows_command: &str) -> Vec<OsString> {
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
