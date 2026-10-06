//! Cursor managed runs load a private plugin through `--plugin-dir`.

use std::ffi::OsString;

use super::{
    managed_hook_shell_command, quote_unix_shell_arg, quote_windows_command_arg, Injection,
    ManagedRun, Terminal,
};
use crate::agents::Cursor;
use crate::args::RunHookCommand;
use crate::paths;
use crate::setup::cursor::CursorManagedHooks;

impl ManagedRun for Cursor {
    fn executable(&self) -> (&'static str, &'static str) {
        ("CURSOR_BIN", "agent")
    }

    fn check(&self, agent_args: &[OsString], terminal: Terminal) -> anyhow::Result<()> {
        if cursor_run_requires_interactive(agent_args, terminal.stdin, terminal.stdout) {
            anyhow::bail!(
                "bt trace run cursor requires an interactive terminal for complete lifecycle tracing; Cursor print mode is inferred for non-terminal stdio and does not emit the required prompt, response, and stop hooks"
            );
        }
        Ok(())
    }

    fn inject(
        &self,
        hook_command: &RunHookCommand,
        managed_run_id: &str,
    ) -> anyhow::Result<Injection> {
        let directory = tempfile::Builder::new()
            .prefix("bt-trace-cursor-plugin-")
            .tempdir()?;
        write_cursor_managed_plugin(directory.path(), hook_command)?;
        // Cursor's CLI only discovers the plugin lifecycle callbacks when these
        // events exist in user/project hook configuration. Keep temporary no-op
        // discovery entries for this process and remove only our own entries when
        // the managed run exits.
        let hooks = CursorManagedHooks::install(&paths::cursor_config_dir(), managed_run_id)?;
        Ok(Injection {
            args: plugin_dir_args(directory.path()),
            env: Vec::new(),
            guards: vec![Box::new(hooks), Box::new(directory)],
        })
    }
}

fn plugin_dir_args(directory: &std::path::Path) -> Vec<OsString> {
    vec![
        OsString::from("--plugin-dir"),
        directory.as_os_str().to_owned(),
    ]
}

fn cursor_run_requires_interactive(
    args: &[OsString],
    stdin_is_terminal: bool,
    stdout_is_terminal: bool,
) -> bool {
    !stdin_is_terminal
        || !stdout_is_terminal
        || args.iter().any(|arg| {
            let arg = arg.to_string_lossy();
            arg == "-p" || arg == "--print" || arg.starts_with("--print=")
        })
}

const CURSOR_HOOKS_MANIFEST: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../src/plugins/cursor/content/hooks/hooks.json"
));

pub(super) fn cursor_hook_events() -> anyhow::Result<Vec<String>> {
    let manifest: serde_json::Value = serde_json::from_str(CURSOR_HOOKS_MANIFEST)?;
    manifest["hooks"]
        .as_object()
        .map(|hooks| hooks.keys().cloned().collect())
        .ok_or_else(|| anyhow::anyhow!("Cursor hook manifest has no hooks object"))
}

/// Build a private plugin that Cursor loads only for the managed invocation.
/// The plugin's hooks call this bt front-end directly, so inherited tracing
/// plugins are suppressed while these injected hooks remain active.
fn write_cursor_managed_plugin(
    directory: &std::path::Path,
    hook_command: &RunHookCommand,
) -> anyhow::Result<()> {
    write_cursor_managed_plugin_for_platform(directory, hook_command, cfg!(windows))
}

fn write_cursor_managed_plugin_for_platform(
    directory: &std::path::Path,
    hook_command: &RunHookCommand,
    windows: bool,
) -> anyhow::Result<()> {
    let hook_dir = directory.join("hooks");
    std::fs::create_dir_all(directory.join(".cursor-plugin"))?;
    std::fs::create_dir_all(&hook_dir)?;
    std::fs::write(
        directory.join(".cursor-plugin/plugin.json"),
        serde_json::to_vec(&serde_json::json!({
            "name": "braintrust-trace-cursor-managed",
            "version": "0.1.0",
            "description": "Invocation-local Braintrust tracing hooks",
            "hooks": "hooks/hooks.json"
        }))?,
    )?;
    let script_path = hook_dir.join(if windows { "trace.ps1" } else { "trace.sh" });
    let hooks = cursor_hook_events()?
        .iter()
        .map(|event| {
            let command = if windows {
                format!(
                    "powershell.exe -NoProfile -ExecutionPolicy Bypass -File {} {event}",
                    quote_windows_command_arg(&script_path.to_string_lossy())
                )
            } else {
                format!("\"${{CURSOR_PLUGIN_ROOT}}/hooks/trace.sh\" {event}")
            };
            (
                event.clone(),
                serde_json::json!([{
                    "command": command,
                    "timeout": 10,
                    "failClosed": false
                }]),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    std::fs::write(
        hook_dir.join("hooks.json"),
        serde_json::to_vec(&serde_json::json!({ "version": 1, "hooks": hooks }))?,
    )?;

    if windows {
        std::fs::write(
            &script_path,
            cursor_managed_powershell_script(hook_command)?,
        )?;
    } else {
        let mut command = managed_hook_shell_command(hook_command, "cursor", false)?;
        for arg in [
            "--session-id-field",
            "conversation_id",
            "--event-field",
            "hook_event_name",
            "--transcript-path-field",
            "transcript_path",
            "--flush-on-turn-end",
            "--capture-timeout-ms",
            "8000",
        ] {
            command.push(' ');
            command.push_str(&quote_unix_shell_arg(arg));
        }
        let script = format!(
            "#!/bin/sh\n# Keep Cursor's prompt response valid even when tracing fails.\nif ! {command} >/dev/null 2>/dev/null; then\n  :\nfi\ncase \"${{1-}}\" in\n  beforeSubmitPrompt) printf '%s\\n' '{{\"continue\":true}}' ;;\n  *) printf '%s\\n' '{{}}' ;;\nesac\nexit 0\n"
        );
        std::fs::write(&script_path, script)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&script_path, std::fs::Permissions::from_mode(0o700))?;
        }
    }
    Ok(())
}

fn cursor_managed_powershell_script(hook_command: &RunHookCommand) -> anyhow::Result<String> {
    let executable = hook_command
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
    args.extend([
        "--source",
        "cursor",
        "--managed-run-hook",
        "--session-id-field",
        "conversation_id",
        "--event-field",
        "hook_event_name",
        "--transcript-path-field",
        "transcript_path",
        "--flush-on-turn-end",
        "--capture-timeout-ms",
        "8000",
    ]);
    let powershell_literal = |value: &str| format!("'{}'", value.replace('\'', "''"));
    let rendered_args = args
        .iter()
        .map(|arg| powershell_literal(arg))
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!(
        "$bt = {}\n$hookArgs = @({})\ntry {{ & $bt @hookArgs *> $null }} catch {{}}\nif ($args.Count -gt 0 -and $args[0] -eq 'beforeSubmitPrompt') {{\n  [Console]::Out.WriteLine('{{\"continue\":true}}')\n}} else {{\n  [Console]::Out.WriteLine('{{}}')\n}}\nexit 0\n",
        powershell_literal(executable),
        rendered_args
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_managed_run_injects_an_invocation_local_plugin() {
        let temp = tempfile::tempdir().unwrap();
        let plugin = temp.path().join("plugin with spaces – ü");
        std::fs::create_dir(&plugin).unwrap();
        let hook = RunHookCommand {
            program: OsString::from("/opt/Braintrust CLI/日本語/bt"),
            args: vec![OsString::from("trace"), OsString::from("hook")],
        };
        write_cursor_managed_plugin(&plugin, &hook).unwrap();
        let args = plugin_dir_args(&plugin);
        assert_eq!(
            args,
            [OsString::from("--plugin-dir"), plugin.as_os_str().into()]
        );

        let manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(plugin.join("hooks/hooks.json")).unwrap())
                .unwrap();
        let plugin_manifest: serde_json::Value = serde_json::from_slice(
            &std::fs::read(plugin.join(".cursor-plugin/plugin.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(plugin_manifest["hooks"], "hooks/hooks.json");
        for event in cursor_hook_events().unwrap() {
            let entry = &manifest["hooks"][event][0];
            assert_eq!(entry["failClosed"], false);
            let launcher = if cfg!(windows) {
                "trace.ps1"
            } else {
                "trace.sh"
            };
            assert!(entry["command"].as_str().unwrap().contains(launcher));
        }
        if cfg!(windows) {
            let script = std::fs::read_to_string(plugin.join("hooks/trace.ps1")).unwrap();
            assert!(script.contains("'--source'"));
            assert!(script.contains("'cursor'"));
            assert!(script.contains("'--managed-run-hook'"));
            assert!(script.contains("'--session-id-field'"));
            assert!(script.contains("'conversation_id'"));
            assert!(!script.contains("permission\":\"allow"));
            assert!(script.contains("{\"continue\":true}"));
            assert!(!script.contains("--dangerously-bypass"));
        } else {
            let script = std::fs::read_to_string(plugin.join("hooks/trace.sh")).unwrap();
            assert!(script.contains("'--source' 'cursor' '--managed-run-hook'"));
            assert!(script.contains("'--session-id-field' 'conversation_id'"));
            assert!(!script.contains("\"permission\":\"allow\""));
            assert!(script.contains("\"continue\":true"));
            assert!(!script.contains("--dangerously-bypass"));
        }
    }

    #[test]
    fn cursor_managed_run_rejects_explicit_and_inferred_print_modes() {
        let args = [OsString::from("answer this")];
        assert!(!cursor_run_requires_interactive(&args, true, true));
        assert!(cursor_run_requires_interactive(&args, false, true));
        assert!(cursor_run_requires_interactive(&args, true, false));
        assert!(cursor_run_requires_interactive(
            &[OsString::from("-p")],
            true,
            true
        ));
        assert!(cursor_run_requires_interactive(
            &[OsString::from("--print")],
            true,
            true
        ));
    }

    #[test]
    fn cursor_managed_run_rejects_redirected_stdin_or_stdout() {
        let args = [OsString::from("answer this")];
        assert!(!cursor_run_requires_interactive(&args, true, true));
        assert!(cursor_run_requires_interactive(&args, false, true));
        assert!(cursor_run_requires_interactive(&args, true, false));
        assert!(cursor_run_requires_interactive(&args, false, false));
    }

    #[test]
    fn cursor_managed_run_generates_a_windows_native_hook_launcher() {
        let temp = tempfile::tempdir().unwrap();
        let plugin = temp.path().join("plugin with spaces");
        std::fs::create_dir(&plugin).unwrap();
        let hook = RunHookCommand {
            program: OsString::from("C:\\Program Files\\Braintrust\\bt.exe"),
            args: vec![OsString::from("trace"), OsString::from("hook")],
        };
        write_cursor_managed_plugin_for_platform(&plugin, &hook, true).unwrap();

        let manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(plugin.join("hooks/hooks.json")).unwrap())
                .unwrap();
        let command = manifest["hooks"]["postToolUse"][0]["command"]
            .as_str()
            .unwrap();
        assert!(command.starts_with("powershell.exe -NoProfile -ExecutionPolicy Bypass -File "));
        assert!(command.contains("trace.ps1"));
        assert!(command.ends_with(" postToolUse"));

        let script = std::fs::read_to_string(plugin.join("hooks/trace.ps1")).unwrap();
        assert!(script.contains("C:\\Program Files\\Braintrust\\bt.exe"));
        assert!(script.contains("--session-id-field"));
        assert!(script.contains("[Console]::Out.WriteLine('{}')"));
        assert!(script.contains("{\"continue\":true}"));
        assert!(!script.contains("permission\":\"allow"));
        assert!(!script.contains("/bin/sh"));
    }
}
