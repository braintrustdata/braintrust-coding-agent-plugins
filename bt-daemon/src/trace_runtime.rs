//! Complete execution of the mounted `bt trace` namespace.
//!
//! The embedding CLI supplies only host services for Braintrust credentials
//! and destination selection. Command dispatch, daemon lifecycle, hook
//! behavior, setup, managed runs, imports, and output contracts stay here with
//! the coding-agent integrations.

use crate::trace_command::{DoctorAgent, DoctorArgs, TraceCommand};
use crate::wire::{AuthSelection, AuthSource, SessionConfig, SessionRoute, TraceDestination};
use crate::{
    apply_additional_metadata, apply_tags, braintrust_serve_options, paths, run_disable,
    run_enable, run_hook, run_import, run_serve, run_status, run_traced, shutdown_daemon,
    AuthDiagnostic, AuthLease, AuthProvider, AuthResolveReason, BraintrustSinkConfig,
    DaemonDiagnostic, DaemonStatus, DoctorCommandOutput, HostInfo, OutputFormat, Registry,
    RunHookCommand, ServeOptions, StatusArgs, TraceArgs, TraceCommandOutput,
};
use async_trait::async_trait;
use std::ffi::OsString;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

/// What a command still needs the host to resolve before tracing can start.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RouteRequirements {
    /// The command cannot proceed without a trace destination. Hosts should
    /// resolve one from flags, stored defaults, or an interactive selection.
    pub destination_required: bool,
    /// The command may prompt to complete missing auth selections: asking the
    /// user to log in when no profile exists, or listing available
    /// organizations when the profile has no default. Hooks leave this false
    /// so an agent's turn never blocks on interactive input.
    pub interactive_auth: bool,
    /// The route will be persisted and replayed by future agent processes, so
    /// it must use durable saved-profile credentials rather than depending on
    /// the current process environment.
    pub persistent_auth: bool,
}

/// The selections the embedding CLI owns. The CLI selects a Braintrust profile,
/// organization, and default project; trace-specific destinations and settings
/// are assembled by this crate.
#[derive(Debug, Clone, Default)]
pub struct HostRouteSelection {
    pub auth: AuthSelection,
    pub project_name: Option<String>,
}

impl HostRouteSelection {
    fn into_session_route(self) -> SessionRoute {
        SessionRoute {
            auth: self.auth,
            destination: self
                .project_name
                .map(|project_name| TraceDestination::ProjectLogs {
                    project_id: None,
                    project_name: Some(project_name),
                }),
            ..SessionRoute::default()
        }
    }
}

fn auth_source_label(selection: &AuthSelection) -> &'static str {
    match selection.effective_source() {
        AuthSource::SavedProfile => "saved_profile",
        AuthSource::Environment => "environment",
        AuthSource::Auto => "command_context",
    }
}

fn ready_auth_diagnostic(
    selection: AuthSelection,
    org_name: Option<String>,
    expires_at_ms: Option<i64>,
) -> AuthDiagnostic {
    AuthDiagnostic {
        status: "ready".into(),
        source: auth_source_label(&selection).into(),
        kind: None,
        profile: selection.profile,
        org_name,
        expires_at_ms,
        error: None,
    }
}

fn unconfirmed_auth_diagnostic(
    status: &str,
    source: &str,
    error: Option<String>,
) -> AuthDiagnostic {
    AuthDiagnostic {
        status: status.into(),
        source: source.into(),
        kind: None,
        profile: None,
        org_name: None,
        expires_at_ms: None,
        error,
    }
}

/// Host-owned services used by the integration runtime.
///
/// Implementations resolve Braintrust profiles and destination choices but do
/// not dispatch or interpret coding-agent commands.
#[async_trait]
pub trait TraceHostServices: Send + Sync {
    /// Resolve only the host-owned auth and default project selections.
    /// Setup and managed run require a project and may prompt for auth;
    /// hooks use the current selection without prompting. The trace runtime
    /// owns every other route setting.
    async fn resolve_route(
        &self,
        requirements: RouteRequirements,
    ) -> anyhow::Result<HostRouteSelection>;

    /// Resolve a Braintrust credential lease without exposing credentials to
    /// plugins, settings files, journals, or command arguments.
    async fn resolve_auth(
        &self,
        selection: &AuthSelection,
        reason: AuthResolveReason,
    ) -> anyhow::Result<AuthLease>;

    /// Describe the selected auth without exposing credentials. Hosts may
    /// override this to report exact profile kind and provenance without
    /// refreshing a credential.
    async fn diagnose_auth(&self, selection: &AuthSelection) -> AuthDiagnostic {
        match self
            .resolve_auth(selection, AuthResolveReason::Initial)
            .await
        {
            Ok(lease) => {
                ready_auth_diagnostic(lease.selection, lease.auth.org_name, lease.expires_at_ms)
            }
            Err(error) => AuthDiagnostic {
                profile: selection.profile.clone(),
                org_name: selection.org_name.clone(),
                ..unconfirmed_auth_diagnostic(
                    "error",
                    auth_source_label(selection),
                    Some(error.to_string()),
                )
            },
        }
    }
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
        tags: route.tags.clone(),
        span_plugins: route.span_plugins.clone(),
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

async fn resolve_host_route(
    host: &TraceHostContext,
    requirements: RouteRequirements,
) -> anyhow::Result<SessionRoute> {
    Ok(host
        .services
        .resolve_route(requirements)
        .await?
        .into_session_route())
}

async fn resolve_command_route(
    host: &TraceHostContext,
    requirements: RouteRequirements,
) -> anyhow::Result<SessionRoute> {
    let mut route = resolve_host_route(host, requirements).await?;
    let lease = host
        .services
        .resolve_auth(&route.auth, AuthResolveReason::Initial)
        .await?;
    let org_name = require_resolved_org(&route, &lease)?;
    route.auth = lease.selection;
    route.auth.org_name = Some(org_name);
    Ok(route)
}

fn print_output(output: TraceCommandOutput, format: OutputFormat) -> anyhow::Result<()> {
    println!("{}", output.render(format)?);
    Ok(())
}

fn plugin_activation_warning(agent: DoctorAgent, enabled: bool) -> Option<&'static str> {
    (agent == DoctorAgent::Grok && enabled).then_some(
        "Grok 1.0.13 requires `/reload-plugins` in each active session after plugin installation or update before its hooks become active",
    )
}

/// Bounds `doctor`'s status query to the running daemon.
const DAEMON_PROBE_TIMEOUT: Duration = Duration::from_secs(5);
/// Resolving auth may refresh OAuth and look up the organization's data plane.
const DAEMON_AUTH_TIMEOUT: Duration = Duration::from_secs(30);

fn is_same_agent(doctor_source: &str, source: &str) -> bool {
    crate::translate::canonical_source_name(doctor_source)
        == crate::translate::canonical_source_name(source)
}

/// Ask the running daemon, not this process, whether it can authenticate the
/// route and what it last failed on. Hooks may start the daemon under a
/// different credential store or environment than the doctor's shell.
async fn diagnose_daemon(
    socket: &Path,
    source: &str,
    selection: Option<&AuthSelection>,
) -> DaemonDiagnostic {
    let status = tokio::time::timeout(
        DAEMON_PROBE_TIMEOUT,
        run_status(StatusArgs {
            socket: Some(socket.to_path_buf()),
            session_id: None,
        }),
    );
    let auth = async {
        match selection {
            Some(selection) => Some(diagnose_daemon_auth(socket, selection).await),
            None => None,
        }
    };
    let (status, auth) = tokio::join!(status, auth);
    let mut diagnostic = match status {
        Ok(Ok(None)) => return DaemonDiagnostic::default(),
        Ok(Err(error)) => {
            return DaemonDiagnostic {
                status: DaemonStatus::Unreachable,
                error: Some(error.to_string()),
                ..DaemonDiagnostic::default()
            }
        }
        Ok(Ok(Some(status))) => {
            let mut session_errors = Vec::new();
            let errors = status
                .sessions
                .into_iter()
                .filter(|session| is_same_agent(source, &session.source))
                .filter_map(|session| session.last_error);
            for error in errors {
                if !session_errors.contains(&error) {
                    session_errors.push(error);
                }
            }
            DaemonDiagnostic {
                version: Some(status.daemon_version),
                session_errors,
                ..DaemonDiagnostic::default()
            }
        }
        Err(_) => DaemonDiagnostic {
            error: Some("status did not answer in time".into()),
            ..DaemonDiagnostic::default()
        },
    };
    diagnostic.status = DaemonStatus::Running;
    diagnostic.auth = auth;
    diagnostic
}

async fn diagnose_daemon_auth(socket: &Path, selection: &AuthSelection) -> AuthDiagnostic {
    let request = async {
        let mut conn = crate::client::Conn::new(crate::client::connect(socket).await?);
        conn.initialize("doctor").await?;
        conn.call::<crate::wire::AuthDiagnoseResult>(
            crate::wire::method::AUTH_DIAGNOSE,
            crate::wire::AuthDiagnoseParams {
                auth: selection.clone(),
            },
        )
        .await
    };
    let result = tokio::time::timeout(DAEMON_AUTH_TIMEOUT, request)
        .await
        .unwrap_or_else(|_| {
            Err(anyhow::anyhow!(
                "daemon did not finish resolving auth in time"
            ))
        });
    // The route already shows what was requested; report only what the
    // daemon actually resolved.
    let source = auth_source_label(selection);
    match result {
        Err(error)
            if error
                .downcast_ref::<crate::client::RpcCallError>()
                .is_some_and(|error| error.code == crate::wire::error_code::METHOD_NOT_FOUND) =>
        {
            unconfirmed_auth_diagnostic("unsupported", source, None)
        }
        Err(error) => unconfirmed_auth_diagnostic("unknown", source, Some(error.to_string())),
        Ok(result) if result.error.is_some() => {
            unconfirmed_auth_diagnostic("error", source, result.error)
        }
        Ok(result) => ready_auth_diagnostic(
            result.selection.unwrap_or_else(|| selection.clone()),
            result.org_name,
            result.expires_at_ms,
        ),
    }
}

/// Explain where the daemon's view of delivery differs from this process's.
/// `resolves_locally` records whether this process resolves the same route
/// through the same host path the daemon uses, which separates divergent
/// credential views from failures both processes share.
fn daemon_warnings(
    agent: DoctorAgent,
    auth: &AuthDiagnostic,
    daemon: &DaemonDiagnostic,
    resolves_locally: bool,
) -> Vec<String> {
    let (source, display_name) = (agent.source(), agent.display_name());
    if daemon.status == DaemonStatus::Unreachable {
        return vec![format!(
            "the tracing daemon did not answer: {}",
            daemon.error.as_deref().unwrap_or("unknown error")
        )];
    }
    let mut warnings = Vec::new();
    match &daemon.auth {
        Some(daemon_auth) if daemon_auth.status == "error" => {
            warnings.push(format!(
                "the running tracing daemon cannot authenticate, so it rejects {display_name} events: {}",
                daemon_auth.error.as_deref().unwrap_or("unknown error")
            ));
            if resolves_locally {
                warnings.push(
                    "this shell can authenticate but the running daemon cannot, so they see different Braintrust credentials (for example, a packaged Windows desktop app gives the daemon a virtualized credential store); run `bt login` from the coding agent's own terminal, or `bt trace stop` to restart a daemon started with a stale environment".into(),
                );
            }
        }
        Some(daemon_auth) if daemon_auth.status == "ready" && auth.status == "ready" => {
            for (label, shell, daemon) in [
                ("profile", &auth.profile, &daemon_auth.profile),
                ("organization", &auth.org_name, &daemon_auth.org_name),
            ] {
                if let (Some(shell), Some(daemon)) = (shell, daemon) {
                    if shell != daemon {
                        warnings.push(format!(
                            "this shell resolved {label} {shell:?} but the running daemon resolved {daemon:?}; events are delivered with the daemon's credentials"
                        ));
                    }
                }
            }
        }
        Some(daemon_auth) if daemon_auth.status == "unsupported" => {
            warnings.push(format!(
                "the tracing daemon is still running an older bt{} that can't report whether its login works; it restarts on this bt version the next time {display_name} sends a trace event, so use {display_name} once, then rerun `bt trace doctor {source}`",
                daemon.version_suffix()
            ));
        }
        Some(daemon_auth) if daemon_auth.status == "unknown" => {
            warnings.push(format!(
                "could not confirm the running daemon's authentication: {}",
                daemon_auth.error.as_deref().unwrap_or("unknown error")
            ));
        }
        _ => {}
    }
    if !daemon.auth_failed() {
        for error in &daemon.session_errors {
            warnings.push(format!(
                "the running tracing daemon reported a {display_name} session error: {error}"
            ));
        }
    }
    warnings
}

async fn doctor_output(host: &TraceHostContext, args: DoctorArgs) -> DoctorCommandOutput {
    doctor_output_at(host, args, &paths::socket_path(None)).await
}

async fn doctor_output_at(
    host: &TraceHostContext,
    args: DoctorArgs,
    socket: &Path,
) -> DoctorCommandOutput {
    let source = args.agent.source();
    let settings_path = paths::agent_settings_path(source, None);
    let settings_present = settings_path.exists();
    let settings = crate::settings::AgentSettings::load(source);
    let enabled = settings.tracing_enabled();
    let mut warnings = Vec::new();

    if !settings_present {
        warnings.push(format!(
            "settings file is missing; run `bt trace enable {source}`"
        ));
    } else if !enabled {
        warnings.push(format!(
            "tracing is disabled; run `bt trace enable {source}`"
        ));
    }
    if let Some(warning) = plugin_activation_warning(args.agent, enabled) {
        warnings.push(warning.into());
    }
    if let Some(warning) = crate::setup::update_warning(source) {
        warnings.push(warning);
    }

    let (route, route_source) = match settings.route {
        Some(route) => (Some(route), "settings_file".to_string()),
        None => match resolve_host_route(host, RouteRequirements::default()).await {
            Ok(route) => (Some(route), "command_context".to_string()),
            Err(error) => {
                warnings.push(format!("route could not be resolved: {error}"));
                (None, "unresolved".to_string())
            }
        },
    };

    if route
        .as_ref()
        .is_some_and(|route| route.destination.is_none())
    {
        warnings.push("trace destination is not configured".into());
    }
    if route
        .as_ref()
        .and_then(|route| route.auth.profile.as_deref())
        == Some("environment")
    {
        warnings.push(
            "profile `environment` is not a saved login; rerun setup with --profile <NAME>".into(),
        );
    }

    // The local and daemon checks are independent and may each reach the
    // network, so run them together.
    let local_auth = async {
        match &route {
            Some(route) => host.services.diagnose_auth(&route.auth).await,
            None => unconfirmed_auth_diagnostic(
                "unresolved",
                "unresolved",
                Some("route is unresolved".into()),
            ),
        }
    };
    let (auth, daemon) = tokio::join!(
        local_auth,
        diagnose_daemon(socket, source, route.as_ref().map(|route| &route.auth))
    );
    if let Some(error) = &auth.error {
        warnings.push(format!("authentication is unusable: {error}"));
    }
    // Resolving the route here exactly as the daemon does, org included, is
    // what separates divergent credential views from failures both processes
    // share. Skip it when the local check already failed.
    let resolves_locally = match &route {
        Some(route) if daemon.auth_failed() && auth.status == "ready" => {
            let local = HostAuthProvider {
                services: host.services.clone(),
            };
            crate::server::resolve_route_auth(
                &local,
                &route.auth,
                AuthResolveReason::Initial,
                route.auth.org_name.as_deref(),
            )
            .await
            .is_ok()
        }
        _ => false,
    };
    warnings.extend(daemon_warnings(
        args.agent,
        &auth,
        &daemon,
        resolves_locally,
    ));

    let plugin_diagnostics = match crate::plugin_diagnostics::read(&paths::data_dir(None)) {
        Ok(diagnostics) => {
            let mut diagnostics: Vec<_> = diagnostics
                .into_iter()
                .filter(|diagnostic| is_same_agent(source, &diagnostic.source))
                .collect();
            diagnostics.sort_by_key(|diagnostic| std::cmp::Reverse(diagnostic.last_seen_ms));
            diagnostics
        }
        Err(error) => {
            warnings.push(format!("plugin diagnostics could not be read: {error}"));
            Vec::new()
        }
    };

    DoctorCommandOutput {
        source: source.into(),
        display_name: args.agent.display_name().into(),
        settings_path,
        settings_present,
        enabled,
        route_source,
        route,
        auth,
        daemon,
        warnings,
        plugin_diagnostics,
    }
}

/// Execute the complete mounted trace command.
pub async fn run_trace(args: TraceArgs, host: TraceHostContext) -> anyhow::Result<()> {
    match args.command {
        TraceCommand::Setup(enable_args) => {
            let mut route = resolve_command_route(
                &host,
                RouteRequirements {
                    destination_required: true,
                    interactive_auth: true,
                    persistent_auth: true,
                },
            )
            .await?;
            apply_additional_metadata(&mut route, enable_args.additional_metadata.as_deref())?;
            apply_tags(&mut route, &enable_args.tags)?;
            if !enable_args.plugin.is_empty() {
                route.span_plugins = crate::resolve_span_plugin_paths(&enable_args.plugin)?;
            }
            print_output(run_enable(enable_args, route)?, host.output_format)
        }
        TraceCommand::Disable(disable_args) => {
            print_output(run_disable(disable_args.agent)?, host.output_format)
        }
        TraceCommand::Update(update_args) => {
            print_output(crate::run_update(update_args.agent)?, host.output_format)
        }
        TraceCommand::Daemon(serve_args) => {
            init_daemon_logging(host.verbose);
            run_serve(serve_args, serve_options(&host)).await
        }
        TraceCommand::Hook(hook_args) => {
            // A persistent hook must never fail the coding agent's turn.
            let result = async {
                let route = resolve_host_route(&host, RouteRequirements::default()).await?;
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
        TraceCommand::Doctor(doctor_args) => print_output(
            TraceCommandOutput::doctor(doctor_output(&host, doctor_args).await),
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
            let mut route = resolve_host_route(
                &host,
                RouteRequirements {
                    destination_required: !import_args.has_destination_override(),
                    interactive_auth: true,
                    persistent_auth: false,
                },
            )
            .await?;
            apply_additional_metadata(&mut route, import_args.additional_metadata.as_deref())?;
            apply_tags(&mut route, &import_args.tags)?;
            let config = session_config(&host, &route).await?;
            let summaries = run_import(import_args, serve_options(&host), Some(config)).await?;
            print_output(TraceCommandOutput::import(summaries), host.output_format)
        }
        TraceCommand::Run(run_args) => {
            let mut route = resolve_command_route(
                &host,
                RouteRequirements {
                    destination_required: true,
                    interactive_auth: true,
                    persistent_auth: false,
                },
            )
            .await?;
            apply_additional_metadata(&mut route, run_args.additional_metadata.as_deref())?;
            apply_tags(&mut route, &run_args.tags)?;
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

    #[test]
    fn doctor_warns_about_grok_1_0_13_plugin_activation_after_enable() {
        let warning = plugin_activation_warning(DoctorAgent::Grok, true).unwrap();
        assert!(warning.contains("Grok 1.0.13"));
        assert!(warning.contains("/reload-plugins"));
        assert_eq!(plugin_activation_warning(DoctorAgent::Grok, false), None);
        assert_eq!(plugin_activation_warning(DoctorAgent::Codex, true), None);
    }

    struct PanicHost;

    #[async_trait]
    impl TraceHostServices for PanicHost {
        async fn resolve_route(&self, _: RouteRequirements) -> anyhow::Result<HostRouteSelection> {
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
        route_requests: Mutex<Vec<RouteRequirements>>,
        route_error: Option<&'static str>,
        auth_error: Option<&'static str>,
        resolved_org: Option<&'static str>,
        profile_id: Option<&'static str>,
        /// The organization the resolved route requires.
        route_org: Option<&'static str>,
    }

    impl RecordingHost {
        fn new(route_error: Option<&'static str>, auth_error: Option<&'static str>) -> Self {
            Self {
                route_requests: Mutex::new(Vec::new()),
                route_error,
                auth_error,
                resolved_org: Some("test-org"),
                profile_id: None,
                route_org: None,
            }
        }

        fn without_org() -> Self {
            Self {
                route_requests: Mutex::new(Vec::new()),
                route_error: None,
                auth_error: None,
                resolved_org: None,
                profile_id: None,
                route_org: None,
            }
        }
    }

    #[async_trait]
    impl TraceHostServices for RecordingHost {
        async fn resolve_route(
            &self,
            requirements: RouteRequirements,
        ) -> anyhow::Result<HostRouteSelection> {
            self.route_requests.lock().unwrap().push(requirements);
            if let Some(error) = self.route_error {
                anyhow::bail!(error);
            }
            Ok(HostRouteSelection {
                auth: AuthSelection {
                    org_name: self.route_org.map(str::to_string),
                    ..AuthSelection::default()
                },
                project_name: Some("test-project".into()),
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
                selection: AuthSelection {
                    source: AuthSource::SavedProfile,
                    profile_id: self.profile_id.map(str::to_string),
                    profile: Some("test".into()),
                    org_name: self.resolved_org.map(str::to_string),
                },
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

    const COMMAND_REQUIREMENTS: RouteRequirements = RouteRequirements {
        destination_required: true,
        interactive_auth: true,
        persistent_auth: false,
    };

    #[test]
    fn host_selection_only_sets_auth_and_default_project() {
        let route = HostRouteSelection {
            auth: AuthSelection {
                source: AuthSource::SavedProfile,
                profile_id: Some("00000000-0000-4000-8000-000000000001".into()),
                profile: Some("test-profile".into()),
                org_name: Some("test-org".into()),
            },
            project_name: Some("test-project".into()),
        }
        .into_session_route();

        assert_eq!(route.auth.profile.as_deref(), Some("test-profile"));
        assert_eq!(
            route
                .destination
                .as_ref()
                .and_then(TraceDestination::project_name),
            Some("test-project")
        );
        assert_eq!(route.flush_mode, crate::wire::FlushMode::FireAndForget);
        assert!(route.additional_metadata.is_none());
        assert!(route.tags.is_empty());
        assert!(route.span_plugins.is_empty());
    }

    #[tokio::test]
    async fn setup_and_run_require_a_host_resolved_destination() {
        for (command, persistent_auth) in [
            (
                TraceCommand::Setup(SetupArgs {
                    agent: SetupAgent::OpenCode,
                    additional_metadata: None,
                    tags: Vec::new(),
                    plugin: Vec::new(),
                }),
                true,
            ),
            (
                TraceCommand::Run(RunArgs {
                    source: RunSource::Codex,
                    additional_metadata: None,
                    tags: Vec::new(),
                    plugin: Vec::new(),
                    agent_args: Vec::new(),
                }),
                false,
            ),
        ] {
            let services = Arc::new(RecordingHost::new(Some("no destination"), None));
            let error = run_trace(TraceArgs { command }, test_host(services.clone()))
                .await
                .unwrap_err();
            assert_eq!(error.to_string(), "no destination");
            assert_eq!(
                *services.route_requests.lock().unwrap(),
                [RouteRequirements {
                    persistent_auth,
                    ..COMMAND_REQUIREMENTS
                }]
            );
        }
    }

    #[tokio::test]
    async fn command_routes_persist_the_resolved_identity_and_require_an_organization() {
        let services = Arc::new(RecordingHost {
            profile_id: Some("00000000-0000-4000-8000-000000000001"),
            ..RecordingHost::new(None, None)
        });
        let route = resolve_command_route(&test_host(services), COMMAND_REQUIREMENTS)
            .await
            .unwrap();
        assert_eq!(
            route.auth.profile_id.as_deref(),
            Some("00000000-0000-4000-8000-000000000001")
        );
        assert_eq!(route.auth.profile.as_deref(), Some("test"));
        assert_eq!(route.auth.org_name.as_deref(), Some("test-org"));

        let services = Arc::new(RecordingHost::without_org());
        let error = resolve_command_route(&test_host(services), COMMAND_REQUIREMENTS)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("organization choice required"));
    }

    #[tokio::test]
    async fn session_config_carries_route_additional_metadata() {
        let services = Arc::new(RecordingHost::new(None, None));
        let route = SessionRoute {
            additional_metadata: Some(serde_json::json!({"import": true})),
            ..SessionRoute::default()
        };
        let config = session_config(&test_host(services), &route).await.unwrap();
        assert_eq!(
            config.additional_metadata,
            Some(serde_json::json!({"import": true}))
        );
    }

    #[tokio::test]
    async fn import_only_requires_a_default_destination_without_an_override() {
        for (destination, destination_required) in [
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
                session_ids: vec!["00000000-0000-0000-0000-000000000000".into()],
                all: false,
                destination,
                parent: None,
                parent_span_id: None,
                parent_root_span_id: None,
                parent_object_type: None,
                parent_object_id: None,
                parent_project: None,
                attach: false,
                additional_metadata: None,
                tags: Vec::new(),
                plugin: Vec::new(),
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
            assert_eq!(
                *services.route_requests.lock().unwrap(),
                [RouteRequirements {
                    destination_required,
                    interactive_auth: true,
                    persistent_auth: false,
                }]
            );
        }
    }

    #[tokio::test]
    async fn hook_host_failures_are_non_fatal() {
        let services = Arc::new(RecordingHost::new(Some("route unavailable"), None));
        let args = crate::HookArgs {
            source: "codex".into(),
            source_version: None,
            plugin_version: None,
            socket: None,
            session_id_field: "session_id".into(),
            event_field: "hook_event_name".into(),
            event: None,
            transcript_path_field: None,
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
        assert_eq!(
            *services.route_requests.lock().unwrap(),
            [RouteRequirements::default()]
        );
    }

    /// The daemon's credential view: a packaged app's virtualized store holds
    /// a different profile than the one the user's shell logged into.
    struct DivergedDaemonAuth;

    #[async_trait]
    impl AuthProvider for DivergedDaemonAuth {
        async fn resolve(
            &self,
            _: &AuthSelection,
            _: AuthResolveReason,
        ) -> anyhow::Result<AuthLease> {
            anyhow::bail!(
                "saved profile ID 'daemon-only' no longer exists; run `bt trace enable` to select a profile"
            )
        }
    }

    fn test_endpoint(dir: &Path) -> PathBuf {
        #[cfg(unix)]
        {
            dir.join("d.sock")
        }
        #[cfg(windows)]
        {
            let _ = dir;
            PathBuf::from(format!(
                r"\\.\pipe\bt-daemon-doctor-test-{}",
                uuid::Uuid::new_v4()
            ))
        }
    }

    /// An in-process daemon whose credential view is `auth_provider`.
    struct TestDaemon {
        socket: PathBuf,
        handle: tokio::task::JoinHandle<()>,
        _dir: tempfile::TempDir,
    }

    impl TestDaemon {
        async fn start(auth_provider: Arc<dyn AuthProvider>) -> Self {
            let dir = tempfile::tempdir().unwrap();
            let socket = test_endpoint(dir.path());
            let data_dir = dir.path().join("data");
            let args = crate::ServeArgs {
                socket: Some(socket.clone()),
                data_dir: Some(data_dir.clone()),
                idle_timeout_secs: 0,
                session_idle_timeout_secs: 0,
            };
            let options = ServeOptions {
                version: "test".into(),
                translators: Arc::new(Registry::default_agents()),
                sink_factory: Arc::new(crate::DebugSinkFactory {
                    dir: data_dir.join("spans"),
                }),
                auth_provider: Some(auth_provider),
            };
            let handle = tokio::spawn(async move {
                let _ = run_serve(args, options).await;
            });
            for _ in 0..200 {
                let status = run_status(StatusArgs {
                    socket: Some(socket.clone()),
                    session_id: None,
                })
                .await;
                if matches!(status, Ok(Some(_))) {
                    return Self {
                        socket,
                        handle,
                        _dir: dir,
                    };
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            panic!("daemon never answered at {}", socket.display());
        }

        /// Run doctor from a shell whose own credentials resolve.
        async fn doctor(&self, agent: DoctorAgent) -> DoctorCommandOutput {
            self.doctor_from(RecordingHost::new(None, None), agent)
                .await
        }

        /// Run doctor from a shell with the given credential view.
        async fn doctor_from(
            &self,
            shell: RecordingHost,
            agent: DoctorAgent,
        ) -> DoctorCommandOutput {
            let host = test_host(Arc::new(shell));
            doctor_output_at(&host, DoctorArgs { agent }, &self.socket).await
        }

        async fn forward_event(&self, source: &str, session_id: &str, route: SessionRoute) {
            let env = crate::wire::Envelope {
                source: source.into(),
                source_version: None,
                plugin_version: None,
                session_id: session_id.into(),
                event: "SessionStart".into(),
                ts_ms: 1,
                managed_run_id: None,
                payload: serde_json::json!({ "session_id": session_id, "hook_event_name": "SessionStart" }),
                route: Some(SessionRoute {
                    destination: Some(TraceDestination::ProjectLogs {
                        project_id: None,
                        project_name: Some("test-project".into()),
                    }),
                    ..route
                }),
                config: None,
                capture: None,
            };
            let host = HostInfo {
                serve_argv: vec![OsString::from("unused")],
                version: "test".into(),
            };
            crate::forward_envelope(&env, &self.socket, &host, true)
                .await
                .unwrap();
        }

        async fn stop(self) {
            crate::shutdown_daemon(&self.socket).await.unwrap();
            self.handle.await.unwrap();
        }
    }

    #[tokio::test]
    async fn doctor_reports_a_daemon_that_cannot_authenticate_the_route() {
        let daemon = TestDaemon::start(Arc::new(DivergedDaemonAuth)).await;
        daemon
            .forward_event("codex", "rejected-session", SessionRoute::default())
            .await;

        let output = daemon.doctor(DoctorAgent::Codex).await;

        // The shell's credential store is healthy, so its own check passes...
        assert_eq!(output.auth.status, "ready");
        // ...but the daemon that actually delivers events cannot authenticate.
        assert_eq!(output.daemon.auth.as_ref().unwrap().status, "error");
        assert!(
            output.daemon.session_errors[0].contains("could not resolve Braintrust auth for codex"),
            "{:#?}",
            output.daemon.session_errors
        );
        assert!(
            output.warnings.iter().any(|warning| {
                warning.contains("daemon cannot authenticate")
                    && warning.contains("'daemon-only' no longer exists")
            }),
            "doctor must surface the daemon's auth failure: {:#?}",
            output.warnings
        );
        assert!(output
            .warnings
            .iter()
            .any(|warning| warning.contains("see different Braintrust credentials")));

        daemon.stop().await;
    }

    /// Resolves a working credential, but for another profile and org than
    /// the shell's.
    struct OtherProfileDaemonAuth;

    #[async_trait]
    impl AuthProvider for OtherProfileDaemonAuth {
        async fn resolve(
            &self,
            _: &AuthSelection,
            _: AuthResolveReason,
        ) -> anyhow::Result<AuthLease> {
            Ok(AuthLease {
                selection: AuthSelection {
                    source: AuthSource::SavedProfile,
                    profile_id: None,
                    profile: Some("stale".into()),
                    org_name: Some("other-org".into()),
                },
                auth: BackendAuth {
                    token: "daemon-secret".into(),
                    api_url: None,
                    app_url: None,
                    org_name: Some("other-org".into()),
                    org_id: None,
                },
                expires_at_ms: None,
            })
        }
    }

    #[tokio::test]
    async fn doctor_reports_a_daemon_that_resolves_other_credentials() {
        let daemon = TestDaemon::start(Arc::new(OtherProfileDaemonAuth)).await;
        let route = SessionRoute {
            auth: AuthSelection {
                org_name: Some("test-org".into()),
                ..AuthSelection::default()
            },
            ..SessionRoute::default()
        };
        daemon
            .forward_event("claude-code", "wrong-org", route)
            .await;

        let output = daemon.doctor(DoctorAgent::Claude).await;

        let daemon_auth = output.daemon.auth.as_ref().unwrap();
        assert_eq!(daemon_auth.status, "ready");
        assert_eq!(daemon_auth.profile.as_deref(), Some("stale"));
        assert!(output.warnings.iter().any(|warning| warning.contains(
            r#"this shell resolved profile "test" but the running daemon resolved "stale""#
        )));
        assert!(output.warnings.iter().any(|warning| warning.contains(
            r#"this shell resolved organization "test-org" but the running daemon resolved "other-org""#
        )));
        // The daemon records the org mismatch it rejected the event for.
        assert!(
            output.daemon.session_errors[0].contains(r#"expected "test-org""#),
            "{:#?}",
            output.daemon.session_errors
        );
        let rendered = TraceCommandOutput::doctor(output)
            .render(OutputFormat::Json)
            .unwrap();
        assert!(!rendered.contains("daemon-secret"));

        daemon.stop().await;
    }

    #[tokio::test]
    async fn doctor_does_not_blame_a_credential_split_when_both_resolve_the_wrong_org() {
        // Both processes resolve the profile, but neither to the org the
        // route requires, so their credential views agree.
        let daemon = TestDaemon::start(Arc::new(OtherProfileDaemonAuth)).await;
        let shell = RecordingHost {
            route_org: Some("required-org"),
            ..RecordingHost::new(None, None)
        };

        let output = daemon.doctor_from(shell, DoctorAgent::Codex).await;

        assert_eq!(output.daemon.auth.as_ref().unwrap().status, "error");
        assert!(
            !output
                .warnings
                .iter()
                .any(|warning| warning.contains("see different Braintrust credentials")),
            "{:#?}",
            output.warnings
        );

        daemon.stop().await;
    }

    #[tokio::test]
    async fn doctor_without_a_running_daemon_reports_only_the_local_view() {
        let temp = tempfile::tempdir().unwrap();
        let output = doctor_output_at(
            &test_host(Arc::new(RecordingHost::new(None, None))),
            DoctorArgs {
                agent: DoctorAgent::Codex,
            },
            &test_endpoint(temp.path()),
        )
        .await;
        assert_eq!(output.daemon.status, DaemonStatus::NotRunning);
        assert!(output.daemon.auth.is_none());
        assert!(!output
            .warnings
            .iter()
            .any(|warning| warning.contains("daemon")));
    }

    fn diagnostic(status: &str, error: Option<&str>) -> AuthDiagnostic {
        AuthDiagnostic {
            status: status.into(),
            source: "saved_profile".into(),
            kind: None,
            profile: Some("test".into()),
            org_name: Some("test-org".into()),
            expires_at_ms: None,
            error: error.map(str::to_string),
        }
    }

    #[test]
    fn doctor_warnings_depend_on_what_the_daemon_could_confirm() {
        let session_error = "could not resolve Braintrust auth for codex";
        let running_daemon = |version: &str, auth| DaemonDiagnostic {
            status: DaemonStatus::Running,
            version: Some(version.into()),
            auth: Some(auth),
            session_errors: vec![session_error.into()],
            error: None,
        };
        let shell = diagnostic("ready", None);

        // An older daemon cannot answer auth.diagnose, so its session errors
        // are the only evidence.
        let older = running_daemon("0.21.0", diagnostic("unsupported", None));
        assert_eq!(
            daemon_warnings(DoctorAgent::Codex, &shell, &older, true),
            [
                "the tracing daemon is still running an older bt (0.21.0) that can't report whether its login works; it restarts on this bt version the next time Codex sends a trace event, so use Codex once, then rerun `bt trace doctor codex`".to_string(),
                format!("the running tracing daemon reported a Codex session error: {session_error}"),
            ]
        );

        // Offline checks can pass while both processes fail to reach
        // Braintrust; that is not a credential split.
        let offline = running_daemon(
            "test",
            diagnostic("error", Some("failed to call login endpoint")),
        );
        assert_eq!(
            daemon_warnings(DoctorAgent::Codex, &shell, &offline, false),
            ["the running tracing daemon cannot authenticate, so it rejects Codex events: failed to call login endpoint"]
        );
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
