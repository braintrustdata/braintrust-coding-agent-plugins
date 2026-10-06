//! Managed runs: launch a coding agent with hooks injected for this
//! invocation only, then flush the traces it produced.

mod claude;
mod codex;
mod cursor;
mod opencode;

use std::ffi::OsString;
use std::io::IsTerminal;
use std::path::PathBuf;

use crate::args::{RunArgs, RunHookCommand, RunSource};
use crate::client::{flush_managed_run_in, shutdown_daemon};
use crate::hook::MANAGED_RUN_ID_ENV;
use crate::route::resolve_span_plugin_paths;
use crate::wire::{self, SessionRoute};
use crate::{paths, settings, subprocess};
use claude::claude_managed_run_args;
use codex::codex_managed_run_args;
use cursor::{cursor_run_requires_interactive, write_cursor_managed_plugin};
use opencode::opencode_managed_config;

const MANAGED_RUN_FLUSH_TIMEOUT_MS: u64 = 10_000;

/// Launch a coding agent with inherited stdio and inject Braintrust hooks for
/// this invocation, without requiring the tracing plugin to be installed or
/// enabled globally.
pub async fn run_traced(
    args: RunArgs,
    hook_command: RunHookCommand,
    mut route: SessionRoute,
) -> anyhow::Result<std::process::ExitStatus> {
    apply_run_span_plugins(&mut route, &args.plugin)?;
    if args.source == RunSource::Cursor
        && cursor_run_requires_interactive(
            &args.agent_args,
            std::io::stdin().is_terminal(),
            std::io::stdout().is_terminal(),
        )
    {
        anyhow::bail!(
            "bt trace run cursor requires an interactive terminal for complete lifecycle tracing; Cursor print mode is inferred for non-terminal stdio and does not emit the required prompt, response, and stop hooks"
        );
    }
    if route.destination.is_none() {
        anyhow::bail!(
            "managed run requires a trace destination; select a project, object destination, or parent span"
        );
    }
    if args.source == RunSource::Codex
        && args.agent_args.iter().any(|arg| {
            let arg = arg.to_string_lossy();
            arg == "--dangerously-bypass-hook-trust"
                || arg.starts_with("--dangerously-bypass-hook-trust=")
        })
    {
        anyhow::bail!(
            "bt trace run codex cannot be combined with --dangerously-bypass-hook-trust; managed tracing preserves Codex hook trust. Remove the flag, or run Codex directly"
        );
    }
    let (executable_env, default_executable) = match args.source {
        RunSource::Codex => ("CODEX_BIN", "codex"),
        RunSource::Cursor => ("CURSOR_BIN", "agent"),
        RunSource::Claude => ("CLAUDE_BIN", "claude"),
        RunSource::OpenCode => ("OPENCODE_BIN", "opencode"),
        RunSource::Pi => ("PI_BIN", "pi"),
    };
    let executable =
        std::env::var_os(executable_env).unwrap_or_else(|| OsString::from(default_executable));
    let cursor_plugin = if args.source == RunSource::Cursor {
        let directory = tempfile::Builder::new()
            .prefix("bt-trace-cursor-plugin-")
            .tempdir()?;
        write_cursor_managed_plugin(directory.path(), &hook_command)?;
        Some(directory)
    } else {
        None
    };
    let injected_args = managed_run_args(
        args.source,
        &hook_command,
        cursor_plugin.as_ref().map(tempfile::TempDir::path),
    )?;
    let managed_run_id = uuid::Uuid::new_v4().to_string();
    // A caller-supplied socket is already an explicit daemon boundary (for
    // example an integration harness or a deliberately isolated runtime).
    // Only create our own boundary when environment auth would otherwise use
    // the ambient shared daemon.
    let isolate_daemon = route.auth.effective_source() == wire::AuthSource::Environment
        && std::env::var_os(paths::SOCKET_ENV).is_none();
    let isolated_runtime = isolate_daemon
        .then(|| ManagedRunRuntime::new(&managed_run_id))
        .transpose()?;
    let invocation_settings = serde_json::to_string(&settings::InvocationSettings::enabled(route))?;
    let mut command: tokio::process::Command = subprocess::interactive_command(&executable).into();
    command
        .args(injected_args)
        .args(args.agent_args)
        .env("_BT_TRACE_MANAGED_RUN", "1")
        .env(MANAGED_RUN_ID_ENV, &managed_run_id)
        .env(settings::INVOCATION_SETTINGS_ENV, invocation_settings);
    if let Some(runtime) = &isolated_runtime {
        command
            .env(paths::SOCKET_ENV, &runtime.socket)
            .env(paths::DATA_DIR_ENV, runtime.temp_dir.path());
    }
    if args.source == RunSource::OpenCode {
        command.env(
            "OPENCODE_CONFIG_CONTENT",
            opencode_managed_config(std::env::var("OPENCODE_CONFIG_CONTENT").ok().as_deref())?,
        );
    }
    // Cursor's CLI only discovers the plugin lifecycle callbacks when these
    // events exist in user/project hook configuration. Keep temporary no-op
    // discovery entries for this process and remove only our own entries when
    // the managed run exits.
    let _cursor_hooks = if args.source == RunSource::Cursor {
        Some(crate::setup::cursor::CursorManagedHooks::install(
            &paths::cursor_config_dir(),
            &managed_run_id,
        )?)
    } else {
        None
    };
    let mut child = command.spawn().map_err(|error| {
        anyhow::anyhow!("failed to launch {}: {error}", executable.to_string_lossy())
    })?;
    let interrupt = tokio::signal::ctrl_c();
    tokio::pin!(interrupt);

    let status = tokio::select! {
        status = child.wait() => status.map_err(anyhow::Error::from),
        result = &mut interrupt => {
            match result {
                Ok(()) => {
                    let kill_result = child.start_kill();
                    let wait_result = child.wait().await;
                    kill_result
                        .map_err(anyhow::Error::from)
                        .and_then(|()| wait_result.map_err(anyhow::Error::from))
                }
                Err(error) => Err(error.into()),
            }
        }
    };
    let socket = isolated_runtime
        .as_ref()
        .map(|runtime| runtime.socket.clone())
        .unwrap_or_else(|| paths::socket_path(None));
    let data_dir = isolated_runtime
        .as_ref()
        .map(|runtime| runtime.temp_dir.path().to_path_buf())
        .unwrap_or_else(|| paths::data_dir(None));
    match flush_managed_run_in(
        &managed_run_id,
        &socket,
        MANAGED_RUN_FLUSH_TIMEOUT_MS,
        &data_dir,
    )
    .await
    {
        Ok(result) if result.accepted_sessions == 0 => tracing::warn!(
            managed_run_id,
            "managed run produced no accepted trace events"
        ),
        Ok(result) if result.flushed => {}
        Ok(result) => tracing::warn!(
            managed_run_id,
            pending = result.pending,
            "managed run trace flush timed out"
        ),
        Err(error) => tracing::warn!(managed_run_id, %error, "managed run trace flush failed"),
    }
    if let Some(runtime) = &isolated_runtime {
        let _ = shutdown_daemon(&socket).await;
        if let Err(error) =
            crate::plugin_diagnostics::merge(runtime.temp_dir.path(), &paths::data_dir(None))
        {
            tracing::warn!(managed_run_id, %error, "failed to preserve managed-run plugin diagnostics");
        }
    }
    status
}

struct ManagedRunRuntime {
    temp_dir: tempfile::TempDir,
    socket: std::path::PathBuf,
}

impl ManagedRunRuntime {
    fn new(_managed_run_id: &str) -> anyhow::Result<Self> {
        let temp_dir = tempfile::Builder::new().prefix("bt-trace-run-").tempdir()?;
        #[cfg(unix)]
        let socket = temp_dir.path().join("daemon.sock");
        #[cfg(windows)]
        let socket = std::path::PathBuf::from(format!(
            r"\\.\pipe\braintrust-bt-daemon-managed-{_managed_run_id}"
        ));
        Ok(Self { temp_dir, socket })
    }
}

fn apply_run_span_plugins(route: &mut SessionRoute, plugins: &[PathBuf]) -> anyhow::Result<()> {
    route.span_plugins = resolve_span_plugin_paths(plugins)?;
    Ok(())
}

fn managed_run_args(
    source: RunSource,
    hook_command: &RunHookCommand,
    cursor_plugin_dir: Option<&std::path::Path>,
) -> anyhow::Result<Vec<OsString>> {
    match source {
        RunSource::Codex => {
            let unix_command = managed_hook_shell_command(hook_command, "codex", false)?;
            let windows_command = managed_hook_shell_command(hook_command, "codex", true)?;
            Ok(codex_managed_run_args(&unix_command, &windows_command))
        }
        RunSource::Claude => claude_managed_run_args(hook_command),
        RunSource::Cursor => {
            let directory = cursor_plugin_dir.ok_or_else(|| {
                anyhow::anyhow!("managed Cursor run requires an invocation-local plugin directory")
            })?;
            Ok(vec![
                OsString::from("--plugin-dir"),
                directory.as_os_str().to_owned(),
            ])
        }
        RunSource::OpenCode => Ok(Vec::new()),
        RunSource::Pi => {
            let extension = match std::env::var_os("BT_TRACE_PI_PLUGIN_SPEC") {
                Some(extension) => extension,
                None => OsString::from(crate::setup::pi::plugin_spec()),
            };
            Ok(vec![OsString::from("-e"), extension])
        }
    }
}

pub(super) fn managed_hook_shell_command(
    hook_command: &RunHookCommand,
    source: &str,
    windows: bool,
) -> anyhow::Result<String> {
    let mut argv = Vec::with_capacity(hook_command.args.len() + 4);
    argv.push(hook_command.program.clone());
    argv.extend(hook_command.args.iter().cloned());
    argv.push(OsString::from("--source"));
    argv.push(OsString::from(source));
    argv.push(OsString::from("--managed-run-hook"));
    let mut rendered = Vec::with_capacity(argv.len());
    for arg in argv {
        let arg = arg
            .into_string()
            .map_err(|_| anyhow::anyhow!("managed hook command contains non-Unicode argv"))?;
        rendered.push(if windows {
            quote_windows_command_arg(&arg)
        } else {
            quote_unix_shell_arg(&arg)
        });
    }
    Ok(rendered.join(" "))
}

pub(super) fn quote_unix_shell_arg(arg: &str) -> String {
    format!("'{}'", arg.replace('\'', "'\"'\"'"))
}

pub(super) fn quote_windows_command_arg(arg: &str) -> String {
    format!("\"{}\"", arg.replace('\\', "/").replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::codex::CODEX_RUN_HOOK_EVENTS;
    use super::cursor::cursor_hook_events;
    use super::*;

    #[test]
    fn managed_run_plugins_replace_inherited_plugins() {
        let temp = tempfile::tempdir().unwrap();
        let plugin = temp.path().join("run.mjs");
        std::fs::write(&plugin, "export default span => span").unwrap();
        let mut route = SessionRoute {
            span_plugins: vec![PathBuf::from("persisted.mjs")],
            ..SessionRoute::default()
        };

        apply_run_span_plugins(&mut route, &[]).unwrap();
        assert!(route.span_plugins.is_empty());

        apply_run_span_plugins(&mut route, std::slice::from_ref(&plugin)).unwrap();
        assert_eq!(route.span_plugins, [plugin.canonicalize().unwrap()]);
    }

    fn test_run_hook_command() -> RunHookCommand {
        RunHookCommand {
            program: OsString::from("/opt/Braintrust CLI/bt"),
            args: vec![OsString::from("agents"), OsString::from("hook")],
        }
    }

    #[tokio::test]
    async fn managed_run_rejects_a_missing_destination_before_launch() {
        let error = run_traced(
            RunArgs {
                source: RunSource::Codex,
                additional_metadata: None,
                tags: Vec::new(),
                plugin: Vec::new(),
                agent_args: Vec::new(),
            },
            test_run_hook_command(),
            SessionRoute::default(),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("requires a trace destination"));
    }

    #[tokio::test]
    async fn codex_managed_run_rejects_global_hook_trust_bypass_before_launch() {
        let error = run_traced(
            RunArgs {
                source: RunSource::Codex,
                additional_metadata: None,
                tags: Vec::new(),
                plugin: Vec::new(),
                agent_args: vec![OsString::from("--dangerously-bypass-hook-trust")],
            },
            test_run_hook_command(),
            SessionRoute {
                destination: Some(wire::TraceDestination::ProjectLogs {
                    project_id: Some("project-id".into()),
                    project_name: None,
                }),
                ..SessionRoute::default()
            },
        )
        .await
        .unwrap_err();

        assert!(error
            .to_string()
            .contains("cannot be combined with --dangerously-bypass-hook-trust"));
    }

    #[test]
    fn codex_managed_run_injects_live_hooks() {
        let args = managed_run_args(RunSource::Codex, &test_run_hook_command(), None).unwrap();
        assert_eq!(args[0], "--enable");
        assert_eq!(args[1], "hooks");
        assert!(!args
            .iter()
            .any(|arg| arg == "--dangerously-bypass-hook-trust"));
        assert_eq!(
            args.iter().filter(|arg| *arg == "-c").count(),
            CODEX_RUN_HOOK_EVENTS.len()
        );
        let config = args
            .iter()
            .find_map(|arg| {
                let arg = arg.to_str()?;
                arg.starts_with("hooks.SessionStart=").then_some(arg)
            })
            .unwrap();
        assert!(config.contains("--managed-run-hook"));
        assert!(config.contains("agents"));
        assert!(config.contains("hook"));
        assert!(config.contains("--source"));
        assert!(config.contains("codex"));
        assert!(!config.contains("transcript"));
    }

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
        let args = managed_run_args(RunSource::Cursor, &hook, Some(&plugin)).unwrap();
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
    fn opencode_managed_run_preserves_inline_config_and_adds_plugin() {
        let config =
            opencode_managed_config(Some(r#"{"model":"test/model","plugin":["other"]}"#)).unwrap();
        let config: serde_json::Value = serde_json::from_str(&config).unwrap();
        assert_eq!(config["model"], "test/model");
        assert_eq!(
            config["plugin"],
            serde_json::json!(["other", "@braintrust/trace-opencode/tracing"])
        );
        assert!(
            managed_run_args(RunSource::OpenCode, &test_run_hook_command(), None)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn pi_managed_run_loads_the_npm_extension() {
        assert_eq!(
            managed_run_args(RunSource::Pi, &test_run_hook_command(), None).unwrap(),
            vec![
                OsString::from("-e"),
                OsString::from(crate::setup::pi::plugin_spec()),
            ]
        );
    }
}
