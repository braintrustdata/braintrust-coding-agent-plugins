//! Complete execution of the mounted `bt trace` namespace.
//!
//! The embedding CLI supplies only host services for Braintrust credentials
//! and destination selection. Command dispatch, daemon lifecycle, hook
//! behavior, setup, managed runs, imports, and output contracts stay here with
//! the coding-agent integrations.

use crate::trace_command::TraceCommand;
use crate::wire::{AuthSelection, SessionConfig, SessionRoute};
use crate::{
    braintrust_serve_options, paths, run_hook, run_import, run_serve, run_setup, run_status,
    run_traced, shutdown_daemon, AuthLease, AuthProvider, AuthResolveReason, BraintrustSinkConfig,
    HostInfo, OutputFormat, Registry, RunHookCommand, ServeOptions, StatusArgs, TraceArgs,
    TraceCommandOutput,
};
use async_trait::async_trait;
use std::ffi::OsString;
use std::sync::Arc;

/// Host-owned services used by the integration runtime.
///
/// Implementations resolve Braintrust profiles and destination choices but do
/// not dispatch or interpret coding-agent commands.
#[async_trait]
pub trait TraceHostServices: Send + Sync {
    /// Resolve the non-secret route selected by the host. Commands such as
    /// setup and managed run require a destination; hooks may use the current
    /// selection without prompting.
    async fn resolve_route(&self, destination_required: bool) -> anyhow::Result<SessionRoute>;

    /// Resolve a Braintrust credential lease without exposing credentials to
    /// plugins, settings files, journals, or command arguments.
    async fn resolve_auth(
        &self,
        selection: &AuthSelection,
        reason: AuthResolveReason,
    ) -> anyhow::Result<AuthLease>;
}

/// Everything the plugin runtime needs from its embedding CLI.
pub struct TraceHostContext {
    pub version: String,
    pub output_format: OutputFormat,
    pub verbose: bool,
    /// Program and arguments that enter the mounted trace namespace. For `bt`
    /// this is the current executable followed by `trace`.
    pub command: RunHookCommand,
    pub services: Arc<dyn TraceHostServices>,
}

struct HostAuthProvider {
    services: Arc<dyn TraceHostServices>,
}

#[async_trait]
impl AuthProvider for HostAuthProvider {
    async fn resolve(
        &self,
        selection: &AuthSelection,
        reason: AuthResolveReason,
    ) -> anyhow::Result<AuthLease> {
        self.services.resolve_auth(selection, reason).await
    }
}

fn serve_options(host: &TraceHostContext) -> ServeOptions {
    let cfg = BraintrustSinkConfig {
        api_url: None,
        app_url: None,
        version: host.version.clone(),
    };
    let mut options =
        braintrust_serve_options(&host.version, cfg, Arc::new(Registry::default_agents()));
    options.auth_provider = Some(Arc::new(HostAuthProvider {
        services: host.services.clone(),
    }));
    options
}

fn child_command(command: &RunHookCommand, child: &str) -> RunHookCommand {
    let mut args = command.args.clone();
    args.push(OsString::from(child));
    RunHookCommand {
        program: command.program.clone(),
        args,
    }
}

fn host_info(host: &TraceHostContext) -> HostInfo {
    let command = child_command(&host.command, "daemon");
    let mut serve_argv = Vec::with_capacity(command.args.len() + 1);
    serve_argv.push(command.program);
    serve_argv.extend(command.args);
    HostInfo {
        serve_argv,
        version: host.version.clone(),
    }
}

fn init_daemon_logging(verbose: bool) {
    let fallback = if verbose { "debug" } else { "info" };
    let filter = tracing_subscriber::EnvFilter::new(fallback);
    if let Err(error) = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init()
    {
        eprintln!("bt trace daemon logging unavailable: {error}");
    }
}

async fn session_config(
    host: &TraceHostContext,
    route: &SessionRoute,
) -> anyhow::Result<SessionConfig> {
    let lease = host
        .services
        .resolve_auth(&route.auth, AuthResolveReason::Initial)
        .await?;
    require_resolved_org(route, &lease)?;
    Ok(SessionConfig {
        auth: lease.auth,
        destination: route.destination.clone(),
        flush_mode: route.flush_mode,
        additional_metadata: route.additional_metadata.clone(),
    })
}

fn require_resolved_org(route: &SessionRoute, lease: &AuthLease) -> anyhow::Result<String> {
    let org_name = lease
        .auth
        .org_name
        .as_deref()
        .filter(|org| !org.trim().is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "organization choice required for tracing; pass --org <NAME> or select an organization during setup"
            )
        })?;
    if let Some(expected) = route
        .auth
        .org_name
        .as_deref()
        .filter(|org| !org.trim().is_empty())
    {
        if expected != org_name {
            anyhow::bail!(
                "selected profile resolved organization {org_name:?}, expected {expected:?}"
            );
        }
    }
    Ok(org_name.to_string())
}

async fn resolve_command_route(
    host: &TraceHostContext,
    destination_required: bool,
) -> anyhow::Result<SessionRoute> {
    let mut route = host.services.resolve_route(destination_required).await?;
    let lease = host
        .services
        .resolve_auth(&route.auth, AuthResolveReason::Initial)
        .await?;
    let org_name = require_resolved_org(&route, &lease)?;
    route.auth.profile = Some(lease.profile);
    route.auth.org_name = Some(org_name);
    Ok(route)
}

fn print_output(output: TraceCommandOutput, format: OutputFormat) -> anyhow::Result<()> {
    println!("{}", output.render(format)?);
    Ok(())
}

/// Execute the complete mounted trace command.
pub async fn run_trace(args: TraceArgs, host: TraceHostContext) -> anyhow::Result<()> {
    match args.command {
        TraceCommand::Setup(setup_args) => {
            let route = resolve_command_route(&host, true).await?;
            print_output(run_setup(setup_args, route)?, host.output_format)
        }
        TraceCommand::Daemon(serve_args) => {
            init_daemon_logging(host.verbose);
            run_serve(serve_args, serve_options(&host)).await
        }
        TraceCommand::Hook(hook_args) => {
            // A persistent hook must never fail the coding agent's turn.
            let result = async {
                let route = host.services.resolve_route(false).await?;
                run_hook(hook_args, route, host_info(&host)).await
            }
            .await;
            if let Err(error) = result {
                eprintln!("bt trace hook (non-fatal): {error}");
            }
            Ok(())
        }
        TraceCommand::Status(status_args) => print_output(
            TraceCommandOutput::status(run_status(status_args).await?),
            host.output_format,
        ),
        TraceCommand::Stop(stop_args) => {
            let socket = paths::socket_path(stop_args.socket.as_deref());
            let status_args = StatusArgs {
                socket: Some(socket.clone()),
                session_id: None,
            };
            if run_status(status_args).await?.is_none() {
                return print_output(TraceCommandOutput::stop(false, false), host.output_format);
            }
            shutdown_daemon(&socket).await?;
            print_output(TraceCommandOutput::stop(true, true), host.output_format)
        }
        TraceCommand::Import(import_args) => {
            let route = host
                .services
                .resolve_route(import_args.destination.is_none() && import_args.parent.is_none())
                .await?;
            let config = session_config(&host, &route).await?;
            run_import(import_args, serve_options(&host), Some(config)).await
        }
        TraceCommand::Run(run_args) => {
            let route = resolve_command_route(&host, true).await?;
            let hook_command = child_command(&host.command, "hook");
            let status = run_traced(run_args, hook_command, route).await?;
            if status.success() {
                Ok(())
            } else {
                anyhow::bail!("coding agent exited with {status}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trace_command::{SetupAgent, SetupArgs, StopArgs};
    use crate::wire::{BackendAuth, TraceDestination};
    use crate::{ImportArgs, ImportSource, RunArgs, RunSource, StatusArgs, TraceCommand};
    use std::path::PathBuf;
    use std::sync::Mutex;

    #[test]
    fn mounted_child_commands_preserve_the_host_prefix() {
        let mounted = RunHookCommand {
            program: OsString::from("/path with spaces/bt"),
            args: vec![OsString::from("trace")],
        };
        assert_eq!(
            child_command(&mounted, "hook"),
            RunHookCommand {
                program: OsString::from("/path with spaces/bt"),
                args: vec![OsString::from("trace"), OsString::from("hook")],
            }
        );
        let context = TraceHostContext {
            version: "test".into(),
            output_format: OutputFormat::Human,
            verbose: false,
            command: mounted,
            services: Arc::new(PanicHost),
        };
        assert_eq!(
            host_info(&context).serve_argv,
            ["/path with spaces/bt", "trace", "daemon"]
        );
    }

    struct PanicHost;

    #[async_trait]
    impl TraceHostServices for PanicHost {
        async fn resolve_route(&self, _: bool) -> anyhow::Result<SessionRoute> {
            panic!("host service should not be called")
        }

        async fn resolve_auth(
            &self,
            _: &AuthSelection,
            _: AuthResolveReason,
        ) -> anyhow::Result<AuthLease> {
            panic!("host service should not be called")
        }
    }

    struct RecordingHost {
        route_requests: Mutex<Vec<bool>>,
        route_error: Option<&'static str>,
        auth_error: Option<&'static str>,
        resolved_org: Option<&'static str>,
    }

    impl RecordingHost {
        fn new(route_error: Option<&'static str>, auth_error: Option<&'static str>) -> Self {
            Self {
                route_requests: Mutex::new(Vec::new()),
                route_error,
                auth_error,
                resolved_org: Some("test-org"),
            }
        }

        fn without_org() -> Self {
            Self {
                route_requests: Mutex::new(Vec::new()),
                route_error: None,
                auth_error: None,
                resolved_org: None,
            }
        }
    }

    #[async_trait]
    impl TraceHostServices for RecordingHost {
        async fn resolve_route(&self, destination_required: bool) -> anyhow::Result<SessionRoute> {
            self.route_requests
                .lock()
                .unwrap()
                .push(destination_required);
            if let Some(error) = self.route_error {
                anyhow::bail!(error);
            }
            Ok(SessionRoute {
                destination: Some(TraceDestination::ProjectLogs {
                    project_id: None,
                    project_name: Some("test-project".into()),
                }),
                ..SessionRoute::default()
            })
        }

        async fn resolve_auth(
            &self,
            _: &AuthSelection,
            _: AuthResolveReason,
        ) -> anyhow::Result<AuthLease> {
            if let Some(error) = self.auth_error {
                anyhow::bail!(error);
            }
            Ok(AuthLease {
                profile: "test".into(),
                auth: BackendAuth {
                    token: "secret".into(),
                    api_url: None,
                    app_url: None,
                    org_name: self.resolved_org.map(str::to_string),
                    org_id: None,
                },
                expires_at_ms: None,
            })
        }
    }

    fn test_host(services: Arc<dyn TraceHostServices>) -> TraceHostContext {
        TraceHostContext {
            version: "test".into(),
            output_format: OutputFormat::Json,
            verbose: false,
            command: RunHookCommand {
                program: OsString::from("bt"),
                args: vec![OsString::from("trace")],
            },
            services,
        }
    }

    #[tokio::test]
    async fn setup_and_run_require_a_host_resolved_destination() {
        for command in [
            TraceCommand::Setup(SetupArgs {
                agent: SetupAgent::OpenCode,
            }),
            TraceCommand::Run(RunArgs {
                source: RunSource::Codex,
                agent_args: Vec::new(),
            }),
        ] {
            let services = Arc::new(RecordingHost::new(Some("no destination"), None));
            let error = run_trace(TraceArgs { command }, test_host(services.clone()))
                .await
                .unwrap_err();
            assert_eq!(error.to_string(), "no destination");
            assert_eq!(*services.route_requests.lock().unwrap(), [true]);
        }
    }

    #[tokio::test]
    async fn command_routes_persist_the_resolved_profile_and_organization() {
        let services = Arc::new(RecordingHost::new(None, None));
        let route = resolve_command_route(&test_host(services), true)
            .await
            .unwrap();
        assert_eq!(route.auth.profile.as_deref(), Some("test"));
        assert_eq!(route.auth.org_name.as_deref(), Some("test-org"));
    }

    #[tokio::test]
    async fn command_routes_reject_an_unresolved_organization() {
        let services = Arc::new(RecordingHost::without_org());
        let error = resolve_command_route(&test_host(services), true)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("organization choice required"));
    }

    #[tokio::test]
    async fn import_only_requires_a_default_destination_without_an_override() {
        for (destination, required) in [
            (None, true),
            (
                Some(TraceDestination::ProjectLogs {
                    project_id: None,
                    project_name: Some("override".into()),
                }),
                false,
            ),
        ] {
            let services = Arc::new(RecordingHost::new(None, Some("stop before lookup")));
            let args = ImportArgs {
                source: ImportSource::Codex,
                session_id: "00000000-0000-0000-0000-000000000000".into(),
                destination,
                parent: None,
                attach: false,
            };
            let error = run_trace(
                TraceArgs {
                    command: TraceCommand::Import(args),
                },
                test_host(services.clone()),
            )
            .await
            .unwrap_err();
            assert_eq!(error.to_string(), "stop before lookup");
            assert_eq!(*services.route_requests.lock().unwrap(), [required]);
        }
    }

    #[tokio::test]
    async fn hook_host_failures_are_non_fatal() {
        let services = Arc::new(RecordingHost::new(Some("route unavailable"), None));
        let args = crate::HookArgs {
            source: "codex".into(),
            source_version: None,
            socket: None,
            session_id_field: "session_id".into(),
            event_field: "hook_event_name".into(),
            event: None,
            no_spawn: false,
            flush_on_turn_end: false,
            flush_timeout_ms: 10_000,
            additional_metadata: None,
            managed_run_hook: false,
        };
        run_trace(
            TraceArgs {
                command: TraceCommand::Hook(args),
            },
            test_host(services.clone()),
        )
        .await
        .unwrap();
        assert_eq!(*services.route_requests.lock().unwrap(), [false]);
    }

    #[tokio::test]
    async fn status_and_absent_stop_do_not_resolve_host_state() {
        let temp = tempfile::tempdir().unwrap();
        let missing_socket = temp.path().join("missing.sock");
        for command in [
            TraceCommand::Status(StatusArgs {
                socket: Some(missing_socket.clone()),
                session_id: None,
            }),
            TraceCommand::Stop(StopArgs {
                socket: Some(PathBuf::from(&missing_socket)),
            }),
        ] {
            run_trace(TraceArgs { command }, test_host(Arc::new(PanicHost)))
                .await
                .unwrap();
        }
    }
}
