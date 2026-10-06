//! Managed runs: launch a coding agent with hooks injected for this
//! invocation only, then flush the traces it produced.

mod claude;
mod codex;
mod cursor;
mod opencode;
mod pi;

use std::ffi::OsString;
use std::io::IsTerminal;
use std::path::PathBuf;

use crate::agents::{registrar, Agent};
use crate::args::{RunArgs, RunHookCommand};
use crate::client::{flush_managed_run_in, shutdown_daemon};
use crate::hook::MANAGED_RUN_ID_ENV;
use crate::route::resolve_span_plugin_paths;
use crate::wire::{self, SessionRoute};
use crate::{paths, settings, subprocess};

const MANAGED_RUN_FLUSH_TIMEOUT_MS: u64 = 10_000;

/// The agent-specific parts of a managed run. Implemented in each agent's
/// module here and registered in [`crate::agents`].
pub(crate) trait ManagedRun: Agent {
    /// The environment variable that overrides the agent executable, and the
    /// executable used otherwise.
    fn executable(&self) -> (&'static str, &'static str);

    /// Reject agent arguments or stdio that cannot produce a complete trace.
    fn check(&self, _agent_args: &[OsString], _terminal: Terminal) -> anyhow::Result<()> {
        Ok(())
    }

    /// Inject hooks for this invocation that call `hook_command`.
    fn inject(
        &self,
        hook_command: &RunHookCommand,
        managed_run_id: &str,
    ) -> anyhow::Result<Injection>;
}

/// Whether the managed run's stdin and stdout are terminals.
#[derive(Clone, Copy)]
pub(crate) struct Terminal {
    pub stdin: bool,
    pub stdout: bool,
}

/// What an agent adds to its managed invocation.
#[derive(Default)]
pub(crate) struct Injection {
    /// Arguments placed before the user's agent arguments.
    pub args: Vec<OsString>,
    pub env: Vec<(&'static str, String)>,
    /// Kept alive until the agent exits, then dropped in order to clean up.
    pub guards: Vec<Box<dyn Send>>,
}

/// Launch a coding agent with inherited stdio and inject Braintrust hooks for
/// this invocation, without requiring the tracing plugin to be installed or
/// enabled globally.
pub async fn run_traced(
    args: RunArgs,
    hook_command: RunHookCommand,
    mut route: SessionRoute,
) -> anyhow::Result<std::process::ExitStatus> {
    apply_run_span_plugins(&mut route, &args.plugin)?;
    let name = args.source.identity().id;
    let agent = registrar()
        .run
        .get(name)
        .unwrap_or_else(|| panic!("no managed run is registered for {name}"));
    agent.check(
        &args.agent_args,
        Terminal {
            stdin: std::io::stdin().is_terminal(),
            stdout: std::io::stdout().is_terminal(),
        },
    )?;
    if route.destination.is_none() {
        anyhow::bail!(
            "managed run requires a trace destination; select a project, object destination, or parent span"
        );
    }
    let (executable_env, default_executable) = agent.executable();
    let executable =
        std::env::var_os(executable_env).unwrap_or_else(|| OsString::from(default_executable));
    let managed_run_id = uuid::Uuid::new_v4().to_string();
    let injection = agent.inject(&hook_command, &managed_run_id)?;
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
        .args(injection.args)
        .args(args.agent_args)
        .env("_BT_TRACE_MANAGED_RUN", "1")
        .env(MANAGED_RUN_ID_ENV, &managed_run_id)
        .env(settings::INVOCATION_SETTINGS_ENV, invocation_settings);
    if let Some(runtime) = &isolated_runtime {
        command
            .env(paths::SOCKET_ENV, &runtime.socket)
            .env(paths::DATA_DIR_ENV, runtime.temp_dir.path());
    }
    command.envs(injection.env);
    let _guards = injection.guards;
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
    use super::*;
    use crate::agents::{Codex, Pi};
    use crate::args::RunSource;

    #[test]
    fn every_run_source_has_a_registered_managed_run() {
        for source in [
            RunSource::Codex,
            RunSource::Cursor,
            RunSource::Claude,
            RunSource::OpenCode,
            RunSource::Pi,
        ] {
            let agent = registrar().run.get(source.identity().id).unwrap();
            assert_eq!(agent.identity().id, source.identity().id);
        }
    }

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
        let args = Codex.inject(&test_run_hook_command(), "run").unwrap().args;
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
    fn pi_managed_run_loads_the_npm_extension() {
        assert_eq!(
            Pi.inject(&test_run_hook_command(), "run").unwrap().args,
            vec![
                OsString::from("-e"),
                OsString::from(crate::setup::pi::plugin_spec()),
            ]
        );
    }
}
