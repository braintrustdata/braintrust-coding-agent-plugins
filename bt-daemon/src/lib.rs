//! bt-daemon: the embeddable library behind the Braintrust coding-agent
//! tracing daemon. Two front-ends consume it (see `../DESIGN.md`):
//!   * `bt` wires the [`clap::Args`] structs into its command tree and exposes
//!     its profile store through [`AuthProvider`].
//!   * the feature-gated standalone `bt-daemon` binary does the same with
//!     env/flag token auth only, for isolated testing.
//!
//! Hook clients submit a non-secret [`wire::SessionRoute`]. The long-lived
//! daemon resolves and refreshes the selected profile through its host.
//! The `cli` feature only gates the standalone binary and its logging
//! subscriber.

pub mod paths;

mod client;
mod command_output;
mod correlation;
mod dispatch;
mod ids;
mod journal;
pub(crate) mod process;
mod server;
mod settings;
mod setup;
mod sink;
mod trace_command;
mod trace_runtime;
mod transcript_import;
mod transcript_mirror;
mod translate;
mod transport;

pub mod wire;
pub use client::HostInfo;
pub use command_output::{
    AuthDiagnostic, DoctorCommandOutput, OutputFormat, SetupCommandOutput, StatusCommandOutput,
    StopCommandOutput, TraceCommandOutput,
};
pub use server::{AuthLease, AuthProvider, AuthResolveReason, ServeOptions};
pub use setup::{run_disable, run_enable, run_setup};
pub use sink::{BraintrustSinkConfig, BraintrustSinkFactory, DebugSinkFactory, Sink, SinkFactory};
pub use trace_command::{
    DisableArgs, DoctorAgent, DoctorArgs, EnableArgs, SetupAgent, SetupArgs, StopArgs, TraceArgs,
    TraceCommand,
};
pub use trace_runtime::{run_trace, RouteRequirements, TraceHostContext, TraceHostServices};
pub use translate::{
    AgentTranslator, Registry, SessionCtx, SpanOp, SpanRow, SpanType, TranslatorFactory,
};

use anyhow::Context;
use braintrust_sdk_rust::SpanComponents;
use clap::{Args, ValueEnum};
use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use wire::{
    method, Envelope, ManagedRunFlushParams, SessionConfig, SessionRoute, StatusResult,
    PROTOCOL_VERSION,
};

const MANAGED_RUN_ID_ENV: &str = "BT_TRACE_MANAGED_RUN_ID";
const MANAGED_RUN_FLUSH_TIMEOUT_MS: u64 = 10_000;

/// Arguments for `serve`.
#[derive(Debug, Clone, Args)]
pub struct ServeArgs {
    /// Socket path override (default: see docs/protocol.md).
    #[arg(long)]
    pub socket: Option<PathBuf>,
    /// Data/journal directory override.
    #[arg(long)]
    pub data_dir: Option<PathBuf>,
    /// Exit after this many seconds idle (no activity, empty queues). 0
    /// disables the watchdog.
    #[arg(long, default_value_t = 300)]
    pub idle_timeout_secs: u64,
    /// Retire a session's in-memory state after this many seconds without
    /// traffic. A later event rebuilds it from the journal. 0 disables
    /// retirement.
    #[arg(long, default_value_t = 30)]
    pub session_idle_timeout_secs: u64,
}

/// Arguments for `hook`.
#[derive(Debug, Clone, Args)]
pub struct HookArgs {
    /// Which translator should interpret this event's payload.
    #[arg(long)]
    pub source: String,
    /// Optional agent version, forwarded for payload-drift handling.
    #[arg(long)]
    pub source_version: Option<String>,
    /// Socket path override.
    #[arg(long)]
    pub socket: Option<PathBuf>,
    /// JSON field in the payload holding the session id.
    #[arg(long, default_value = "session_id")]
    pub session_id_field: String,
    /// JSON field in the payload holding the event name.
    #[arg(long, default_value = "hook_event_name")]
    pub event_field: String,
    /// Explicit event name (overrides `--event-field` lookup).
    #[arg(long)]
    pub event: Option<String>,
    /// JSON field holding a transcript path. When present, capture the file
    /// length observed by this hook so deterministic journal replay cannot
    /// read transcript records written by later lifecycle events.
    #[arg(long)]
    pub transcript_path_field: Option<String>,
    /// Fail instead of spawning a daemon if none is running.
    #[arg(long)]
    pub no_spawn: bool,
    /// Flush the session after a turn-ending event. Intended for short-lived
    /// CI hosts; SessionEnd is always flushed.
    #[arg(long)]
    pub flush_on_turn_end: bool,
    /// Bound an explicit turn/session-end flush.
    #[arg(long, default_value_t = 10_000)]
    pub flush_timeout_ms: u64,
    /// JSON object merged into root-span metadata. Deliberately not read from
    /// the environment: a hook fires automatically on every event, so its
    /// configuration must come only from the persisted agent route (or, for a
    /// managed child, the invocation settings `run` injected).
    #[arg(long)]
    pub additional_metadata: Option<String>,
    /// Marks the hook definition injected by `run`; inherited plugin hooks do
    /// not carry this flag and are suppressed for the managed child.
    #[arg(long, hide = true)]
    pub managed_run_hook: bool,
}

/// Arguments for `status`.
#[derive(Debug, Clone, Args)]
pub struct StatusArgs {
    #[arg(long)]
    pub socket: Option<PathBuf>,
    /// Limit to one session.
    #[arg(long)]
    pub session_id: Option<String>,
}

/// Arguments for importing a past coding-agent session.
#[derive(Debug, Clone, Args)]
pub struct ImportArgs {
    /// Agent that produced the session.
    #[arg(value_enum)]
    pub source: ImportSource,
    /// One or more session ids shown by the agent's resume command.
    #[arg(
        value_name = "SESSION_ID",
        num_args = 1..,
        required_unless_present = "all",
        conflicts_with = "all"
    )]
    pub session_ids: Vec<String>,
    /// Import every locally discoverable session for this agent.
    #[arg(long, conflicts_with = "session_ids")]
    pub all: bool,
    /// Destination object reference, such as `project_logs:<project-id>` or
    /// `experiment:<experiment-id>`.
    #[arg(long, value_name = "DESTINATION", conflicts_with = "parent")]
    pub destination: Option<wire::TraceDestination>,
    /// Attach the imported session below an exported Braintrust span.
    #[arg(long, value_name = "SPAN_COMPONENTS", conflicts_with = "destination")]
    pub parent: Option<SpanComponents>,
    /// Keep following the transcript until Ctrl-C, importing new turns as the
    /// coding-agent session grows.
    #[arg(long, conflicts_with = "all")]
    pub attach: bool,
    /// JSON object merged into every imported root span's metadata.
    #[arg(long, env = "BRAINTRUST_ADDITIONAL_METADATA")]
    pub additional_metadata: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ImportSource {
    Codex,
    #[value(name = "claude", alias = "claude-code")]
    Claude,
}

/// Arguments for launching a coding agent with invocation-local live hooks.
#[derive(Debug, Clone, Args)]
#[command(trailing_var_arg = true)]
pub struct RunArgs {
    /// Coding agent to launch.
    #[arg(value_enum)]
    pub source: RunSource,
    /// JSON object merged into root-span metadata for this invocation.
    #[arg(long, env = "BRAINTRUST_ADDITIONAL_METADATA")]
    pub additional_metadata: Option<String>,
    /// Arguments forwarded verbatim to the coding agent.
    #[arg(allow_hyphen_values = true)]
    pub agent_args: Vec<OsString>,
}

/// Front-end command used by a managed agent run to forward one hook payload.
///
/// The standalone binary uses `[bt-daemon, hook]`; the embedded `bt` front-end
/// uses its own equivalent prefix. `run_traced` appends `--source <agent>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunHookCommand {
    pub program: OsString,
    pub args: Vec<OsString>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum RunSource {
    Codex,
    #[value(name = "claude", alias = "claude-code")]
    Claude,
    #[value(name = "opencode", alias = "open-code")]
    OpenCode,
    Pi,
}

/// Run the daemon until shutdown.
pub async fn run_serve(args: ServeArgs, opts: ServeOptions) -> anyhow::Result<()> {
    server::run(args, opts).await
}

/// Capture one hook event from `stdin` and forward it to the daemon.
///
/// `route` contains only non-secret profile and destination selection.
/// Returns `Ok` once the daemon has acked (journaled + enqueued). Callers that
/// must never fail the agent's turn should treat any `Err` as non-fatal and
/// exit 0.
pub async fn run_hook(
    args: HookArgs,
    mut route: SessionRoute,
    host: HostInfo,
) -> anyhow::Result<()> {
    // A managed run injects its own hook definitions. Suppress an inherited
    // Braintrust plugin hook for the same child, but allow the injected hook
    // process, which carries the second marker.
    if std::env::var_os("_BT_TRACE_MANAGED_RUN").is_some() && !args.managed_run_hook {
        return Ok(());
    }
    let settings = settings::AgentSettings::load(&args.source);
    if !settings.tracing_enabled() {
        return Ok(());
    }
    let mut payload = read_stdin_json()?;

    if let Some(field) = &args.transcript_path_field {
        add_transcript_observation(&mut payload, field);
    }

    let session_id = json_str_field(&payload, &args.session_id_field)
        .ok_or_else(|| anyhow::anyhow!("no `{}` field in hook payload", args.session_id_field))?;
    let event = args
        .event
        .clone()
        .or_else(|| json_str_field(&payload, &args.event_field))
        .unwrap_or_default();

    if let Some(configured_route) = settings.route {
        route = configured_route;
    }
    if args.flush_on_turn_end {
        route.flush_mode = wire::FlushMode::FlushOnTurnEnd;
    }
    apply_additional_metadata(&mut route, args.additional_metadata.as_deref())?;
    let env = Envelope {
        source: args.source.clone(),
        source_version: args.source_version.clone(),
        plugin_version: None,
        session_id,
        event,
        ts_ms: now_ms(),
        managed_run_id: std::env::var(MANAGED_RUN_ID_ENV)
            .ok()
            .filter(|value| !value.is_empty()),
        capture: None,
        payload,
        route: Some(route),
        config: None,
    };

    let socket = paths::socket_path(args.socket.as_deref());
    forward_envelope(&env, &socket, &host, args.no_spawn).await?;
    let should_flush = env.event == "SessionEnd"
        || (matches!(
            env.route.as_ref().map(|r| r.flush_mode),
            Some(wire::FlushMode::FlushOnTurnEnd)
        ) && matches!(env.event.as_str(), "Stop" | "SubagentStop"));
    if should_flush {
        flush_session(&env.session_id, &socket, args.flush_timeout_ms).await?;
    }
    Ok(())
}

/// Apply one invocation-local JSON metadata override to a non-secret route.
///
/// The route is then carried unchanged through live hooks, managed runs, and
/// transcript import. Keeping validation here gives every public command the
/// same contract and prevents individual agent shims from parsing JSON.
pub(crate) fn apply_additional_metadata(
    route: &mut SessionRoute,
    additional_metadata: Option<&str>,
) -> anyhow::Result<()> {
    let Some(metadata) = additional_metadata else {
        return Ok(());
    };
    let value: serde_json::Value = serde_json::from_str(metadata)
        .map_err(|e| anyhow::anyhow!("invalid --additional-metadata JSON: {e}"))?;
    if !value.is_object() {
        anyhow::bail!("--additional-metadata must be a JSON object");
    }
    route.additional_metadata = Some(value);
    Ok(())
}

/// Ensure a daemon is up and forward one already-built [`Envelope`] to it
/// (`initialize` handshake + `event.log`). Also the seam in-process clients and
/// tests use to send events without going through stdin.
pub async fn forward_envelope(
    env: &Envelope,
    socket: &std::path::Path,
    host: &HostInfo,
    no_spawn: bool,
) -> anyhow::Result<()> {
    let stream = client::ensure_daemon(socket, host, no_spawn).await?;
    let mut conn = client::Conn::new(stream);
    let initialized = conn
        .request(
            method::INITIALIZE,
            serde_json::json!({
                "protocol_version": PROTOCOL_VERSION,
                "client": {
                    "source": env.source,
                    "plugin_version": env.source_version,
                    "pid": std::process::id()
                }
            }),
        )
        .await?;
    let initialized: wire::InitializeResult = serde_json::from_value(initialized)?;
    if initialized.daemon_version != host.version {
        if no_spawn {
            anyhow::bail!(
                "daemon version {} does not match client {} and --no-spawn is set",
                initialized.daemon_version,
                host.version
            );
        }
        conn.request(method::DAEMON_SHUTDOWN, serde_json::json!({}))
            .await?;
        drop(conn);
        for _ in 0..100 {
            if client::connect(socket).await.is_err() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let stream = client::ensure_daemon(socket, host, false).await?;
        conn = client::Conn::new(stream);
        conn.request(
            method::INITIALIZE,
            serde_json::json!({
                "protocol_version": PROTOCOL_VERSION,
                "client": {
                    "source": env.source,
                    "plugin_version": env.source_version,
                    "pid": std::process::id()
                }
            }),
        )
        .await?;
    }
    conn.request(method::EVENT_LOG, env).await?;
    Ok(())
}

/// Ask the daemon to flush a session, bounded by `timeout_ms`. A reliable
/// barrier: on return, every event enqueued before the call has been processed
/// and its spans emitted to the sink.
pub async fn flush_session(
    session_id: &str,
    socket: &std::path::Path,
    timeout_ms: u64,
) -> anyhow::Result<wire::FlushResult> {
    let stream = client::connect(socket).await?;
    let mut conn = client::Conn::new(stream);
    conn.request(
        method::INITIALIZE,
        serde_json::json!({ "protocol_version": PROTOCOL_VERSION, "client": { "source": "flush" } }),
    )
    .await?;
    let params = wire::FlushParams {
        session_id: session_id.to_string(),
        timeout_ms,
    };
    let value = conn.request(method::SESSION_FLUSH, params).await?;
    Ok(serde_json::from_value(value)?)
}

/// Flush every daemon session accepted from one managed child process tree.
/// A missing daemon is not itself a failure: the daemon may have idle-exited
/// after draining every session, so acceptance is checked against the
/// persisted managed-run record instead.
pub async fn flush_managed_run(
    managed_run_id: &str,
    socket: &std::path::Path,
    timeout_ms: u64,
) -> anyhow::Result<wire::FlushResult> {
    flush_managed_run_in(managed_run_id, socket, timeout_ms, &paths::data_dir(None)).await
}

async fn flush_managed_run_in(
    managed_run_id: &str,
    socket: &std::path::Path,
    timeout_ms: u64,
    data_dir: &std::path::Path,
) -> anyhow::Result<wire::FlushResult> {
    let stream = match client::connect(socket).await {
        Ok(stream) => stream,
        Err(_) => {
            let accepted_sessions = journal::read_managed_run_keys(data_dir, managed_run_id)
                .await
                .len() as u64;
            return Ok(wire::FlushResult {
                flushed: true,
                pending: 0,
                accepted_sessions,
            });
        }
    };
    let mut conn = client::Conn::new(stream);
    conn.request(
        method::INITIALIZE,
        serde_json::json!({
            "protocol_version": PROTOCOL_VERSION,
            "client": { "source": "managed-run-flush" }
        }),
    )
    .await?;
    let params = ManagedRunFlushParams {
        managed_run_id: managed_run_id.to_string(),
        timeout_ms,
    };
    let value = conn.request(method::MANAGED_RUN_FLUSH, params).await?;
    Ok(serde_json::from_value(value)?)
}

/// Query daemon status. `Ok(None)` means no daemon is running.
pub async fn run_status(args: StatusArgs) -> anyhow::Result<Option<StatusResult>> {
    let socket = paths::socket_path(args.socket.as_deref());
    let stream = match client::connect(&socket).await {
        Ok(s) => s,
        Err(_) => return Ok(None),
    };
    let mut conn = client::Conn::new(stream);
    conn.request(
        method::INITIALIZE,
        serde_json::json!({
            "protocol_version": PROTOCOL_VERSION,
            "client": { "source": "status" }
        }),
    )
    .await?;
    let params = wire::StatusParams {
        session_id: args.session_id.clone(),
    };
    let value = conn.request(method::STATUS_GET, params).await?;
    Ok(Some(serde_json::from_value(value)?))
}

/// Request a graceful daemon shutdown. Primarily useful for lifecycle
/// management and transport integration tests.
pub async fn shutdown_daemon(socket: &std::path::Path) -> anyhow::Result<()> {
    let stream = client::connect(socket).await?;
    let mut conn = client::Conn::new(stream);
    conn.request(method::DAEMON_SHUTDOWN, serde_json::json!({}))
        .await?;
    Ok(())
}

/// Import a native coding-agent transcript through the normal translators and
/// sink. This is separate from daemon journal recovery: import creates traces
/// for a past session, while recovery rebuilds live correlation state.
pub async fn run_import(
    args: ImportArgs,
    opts: ServeOptions,
    mut config: Option<SessionConfig>,
) -> anyhow::Result<()> {
    validate_import_selection(&args)?;
    let destination = args
        .parent
        .map(|components| wire::TraceDestination::ParentSpan { components })
        .or(args.destination);
    apply_import_destination(&mut config, destination)?;
    let files = transcript_import::resolve_transcripts(&args.session_ids, args.all, args.source)?;
    if args.attach {
        return import_transcript(&files[0], args.source, opts, config, true).await;
    }
    import_transcripts(&files, args.source, opts, config).await
}

fn validate_import_selection(args: &ImportArgs) -> anyhow::Result<()> {
    if args.all != args.session_ids.is_empty() {
        anyhow::bail!("provide explicit session ids or use --all, but not both");
    }
    if args.attach && args.session_ids.len() != 1 {
        anyhow::bail!("--attach requires exactly one session id");
    }
    Ok(())
}

/// Launch a coding agent with inherited stdio and inject Braintrust hooks for
/// this invocation, without requiring the tracing plugin to be installed or
/// enabled globally.
pub async fn run_traced(
    args: RunArgs,
    hook_command: RunHookCommand,
    route: SessionRoute,
) -> anyhow::Result<std::process::ExitStatus> {
    if route.destination.is_none() {
        anyhow::bail!(
            "managed run requires a trace destination; select a project, object destination, or parent span"
        );
    }
    let (executable_env, default_executable) = match args.source {
        RunSource::Codex => ("CODEX_BIN", "codex"),
        RunSource::Claude => ("CLAUDE_BIN", "claude"),
        RunSource::OpenCode => ("OPENCODE_BIN", "opencode"),
        RunSource::Pi => ("PI_BIN", "pi"),
    };
    let executable =
        std::env::var_os(executable_env).unwrap_or_else(|| OsString::from(default_executable));
    let injected_args = managed_run_args(args.source, &hook_command)?;
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
    let mut command = tokio::process::Command::new(&executable);
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
    if isolated_runtime.is_some() {
        let _ = shutdown_daemon(&socket).await;
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

fn managed_run_args(
    source: RunSource,
    hook_command: &RunHookCommand,
) -> anyhow::Result<Vec<OsString>> {
    let source_name = match source {
        RunSource::Codex => "codex",
        RunSource::Claude => "claude",
        RunSource::OpenCode => "opencode",
        RunSource::Pi => "pi",
    };
    match source {
        RunSource::Codex | RunSource::Claude => {
            let unix_command = managed_hook_shell_command(hook_command, source_name, false)?;
            let windows_command = managed_hook_shell_command(hook_command, source_name, true)?;
            match source {
                RunSource::Codex => Ok(codex_managed_run_args(&unix_command, &windows_command)),
                RunSource::Claude => Ok(claude_managed_run_args(if cfg!(windows) {
                    &windows_command
                } else {
                    &unix_command
                })?),
                _ => unreachable!(),
            }
        }
        RunSource::OpenCode => Ok(Vec::new()),
        RunSource::Pi => Ok(vec![
            OsString::from("-e"),
            std::env::var_os("BT_TRACE_PI_PLUGIN_SPEC")
                .unwrap_or_else(|| OsString::from("npm:@braintrust/pi-extension@^1")),
        ]),
    }
}

fn opencode_managed_config(existing: Option<&str>) -> anyhow::Result<String> {
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
    let plugin = std::env::var("BT_TRACE_OPENCODE_PLUGIN_SPEC")
        .unwrap_or_else(|_| "@braintrust/trace-opencode@^1".to_string());
    if !plugins.iter().any(|value| value.as_str() == Some(&plugin)) {
        plugins.push(serde_json::Value::String(plugin));
    }
    Ok(serde_json::to_string(&config)?)
}

fn managed_hook_shell_command(
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

fn quote_unix_shell_arg(arg: &str) -> String {
    format!("'{}'", arg.replace('\'', "'\"'\"'"))
}

fn quote_windows_command_arg(arg: &str) -> String {
    format!("\"{}\"", arg.replace('\\', "/").replace('"', "\"\""))
}

const CODEX_RUN_HOOK_EVENTS: &[&str] = &[
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

fn claude_managed_run_args(command: &str) -> anyhow::Result<Vec<OsString>> {
    let matcher_group = serde_json::json!([{
        "hooks": [{
            "type": "command",
            "command": command,
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

fn apply_import_destination(
    config: &mut Option<SessionConfig>,
    destination: Option<wire::TraceDestination>,
) -> anyhow::Result<()> {
    if let Some(destination) = destination {
        let config = config.as_mut().ok_or_else(|| {
            anyhow::anyhow!(
                "import destination requires a resolved Braintrust session configuration; \
                 use `bt trace import` instead of the standalone `bt-daemon import` command"
            )
        })?;
        config.destination = Some(destination);
    }
    Ok(())
}

/// Import a native transcript from a known path. Front-ends should normally
/// expose [`run_import`] so users only need the agent's session id; this lower-
/// level entry point is useful for embedding and isolated tests.
pub async fn import_transcript(
    file: &std::path::Path,
    source: ImportSource,
    opts: ServeOptions,
    config: Option<SessionConfig>,
    attach: bool,
) -> anyhow::Result<()> {
    let mut tail = transcript_import::TranscriptTail::new(file.to_path_buf(), source);
    let mut processor = ImportProcessor::new(opts, config);
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);
    let mut finalizing = !attach;
    loop {
        let entries = tail.poll(finalizing)?;
        if tail.take_translator_reset() {
            processor.reset_translators();
        }
        processor.process(entries).await?;
        if finalizing {
            break;
        }
        tokio::select! {
            result = &mut shutdown => {
                result?;
                finalizing = true;
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(500)) => {}
        }
    }
    processor.finish().await
}

/// Import multiple completed native transcripts through one processor.
///
/// This shares translator and sink setup while keeping each native session's
/// correlation state isolated by session id.
pub async fn import_transcripts(
    files: &[PathBuf],
    source: ImportSource,
    opts: ServeOptions,
    config: Option<SessionConfig>,
) -> anyhow::Result<()> {
    let mut processor = ImportProcessor::new(opts, config);
    for file in files {
        let mut tail = transcript_import::TranscriptTail::new(file.clone(), source);
        let entries = tail
            .poll(true)
            .with_context(|| format!("import transcript {}", file.display()))?;
        let session_ids = entries
            .iter()
            .map(|entry| entry.session_id.clone())
            .collect::<std::collections::HashSet<_>>();
        processor.process(entries).await?;
        for session_id in session_ids {
            processor.finish_session(&session_id).await?;
        }
    }
    processor.finish().await
}

struct ImportLive {
    source: String,
    translator: Box<dyn AgentTranslator>,
    sink: Box<dyn Sink>,
    ctx: SessionCtx,
    pending_ops: usize,
}

struct ImportProcessor {
    sessions: std::collections::HashMap<String, ImportLive>,
    opts: ServeOptions,
    config: Option<SessionConfig>,
}

impl ImportProcessor {
    fn new(opts: ServeOptions, config: Option<SessionConfig>) -> Self {
        Self {
            sessions: std::collections::HashMap::new(),
            opts,
            config,
        }
    }

    async fn process(&mut self, entries: Vec<Envelope>) -> anyhow::Result<()> {
        for mut env in entries {
            env.config = self.config.clone();
            let sid = env.session_id.clone();
            let live = match self.sessions.get_mut(&sid) {
                Some(live) => live,
                None => {
                    let translator = self.opts.translators.create(&env.source, &sid);
                    let sink = self.opts.sink_factory.create(
                        &sid,
                        &env.source,
                        env.plugin_version.as_deref(),
                    )?;
                    self.sessions.insert(
                        sid.clone(),
                        ImportLive {
                            source: env.source.clone(),
                            translator,
                            sink,
                            ctx: SessionCtx {
                                session_id: sid.clone(),
                                config: None,
                            },
                            pending_ops: 0,
                        },
                    );
                    self.sessions.get_mut(&sid).unwrap()
                }
            };
            if let Some(cfg) = &env.config {
                live.sink.configure(cfg);
                live.ctx.config = Some(cfg.clone());
            }
            let ops = live.translator.handle(&env, &live.ctx)?;
            Self::emit_translator_batches(live, ops).await?;
        }
        Ok(())
    }

    fn reset_translators(&mut self) {
        for (session_id, live) in &mut self.sessions {
            live.translator = self.opts.translators.create(&live.source, session_id);
        }
    }

    async fn emit_translator_batches(
        live: &mut ImportLive,
        first: Vec<SpanOp>,
    ) -> anyhow::Result<()> {
        let mut next = Some(first);
        while let Some(ops) = next {
            // Imports can contain tens of thousands of SDK log commands. Bound the
            // number queued between drains without serializing one network flush
            // for every native turn boundary.
            const FLUSH_OPS: usize = 500;
            for chunk in ops.chunks(FLUSH_OPS) {
                live.sink.emit(chunk).await?;
                live.pending_ops += chunk.len();
                if live.pending_ops >= FLUSH_OPS {
                    live.sink.flush().await?;
                    live.pending_ops = 0;
                }
            }
            next = live.translator.drain_pending(&live.ctx)?;
        }
        Ok(())
    }

    async fn finish(self) -> anyhow::Result<()> {
        for (_sid, mut live) in self.sessions {
            let ops = live.translator.flush(&live.ctx)?;
            Self::emit_translator_batches(&mut live, ops).await?;
            live.sink.flush().await?;
        }
        Ok(())
    }

    async fn finish_session(&mut self, session_id: &str) -> anyhow::Result<()> {
        let Some(mut live) = self.sessions.remove(session_id) else {
            return Ok(());
        };
        let ops = live.translator.flush(&live.ctx)?;
        Self::emit_translator_batches(&mut live, ops).await?;
        live.sink.flush().await
    }
}

/// Build a Phase-1 debug [`ServeOptions`]: debug translator registry + a debug
/// sink writing NDJSON under `<data_dir>/spans/`.
pub fn debug_serve_options(version: impl Into<String>, data_dir: &std::path::Path) -> ServeOptions {
    ServeOptions {
        version: version.into(),
        translators: Arc::new(Registry::debug_only()),
        sink_factory: Arc::new(DebugSinkFactory {
            dir: data_dir.join("spans"),
        }),
        auth_provider: None,
    }
}

/// Build [`ServeOptions`] with the Braintrust sink. `translators` lets the
/// caller choose the translator registry (debug-only until Phase 3 adds the
/// Codex/Claude translators). Clients are built lazily per session URL, so this
/// is cheap and infallible.
pub fn braintrust_serve_options(
    version: impl Into<String>,
    sink_config: BraintrustSinkConfig,
    translators: Arc<Registry>,
) -> ServeOptions {
    ServeOptions {
        version: version.into(),
        translators,
        sink_factory: Arc::new(BraintrustSinkFactory::new(sink_config)),
        auth_provider: None,
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn read_stdin_json() -> anyhow::Result<serde_json::Value> {
    use std::io::Read;
    let mut buf = String::new();
    std::io::stdin().read_to_string(&mut buf)?;
    if buf.trim().is_empty() {
        anyhow::bail!("empty stdin (expected a JSON hook payload)");
    }
    Ok(serde_json::from_str(&buf)?)
}

/// Read a string-ish field (`session_id` / event name) from the payload,
/// coercing a JSON number to its string form.
fn json_str_field(payload: &serde_json::Value, field: &str) -> Option<String> {
    match payload.get(field) {
        Some(serde_json::Value::String(s)) => Some(s.clone()),
        Some(serde_json::Value::Number(n)) => Some(n.to_string()),
        _ => None,
    }
}

/// Stamp the transcript boundary visible when a blocking hook runs. Agent
/// transcripts are append-only, while daemon journal replay may happen after
/// the session has advanced. Recording byte lengths keeps translation causally
/// aligned with each native hook without copying transcript contents into the
/// journal.
fn add_transcript_observation(payload: &mut serde_json::Value, field: &str) {
    let Some(path) = json_str_field(payload, field) else {
        return;
    };
    let transcript = std::path::Path::new(&path);
    let mut observation = serde_json::Map::new();
    observation.insert("path".into(), serde_json::Value::String(path.clone()));
    if let Ok(metadata) = std::fs::metadata(transcript) {
        observation.insert(
            "observed_bytes".into(),
            serde_json::Value::Number(metadata.len().into()),
        );
    }

    if transcript.file_name().and_then(|name| name.to_str()) == Some("transcript.jsonl") {
        let full = transcript.with_file_name("transcript_full.jsonl");
        if let Ok(metadata) = std::fs::metadata(&full) {
            observation.insert(
                "full_path".into(),
                serde_json::Value::String(full.to_string_lossy().into_owned()),
            );
            observation.insert(
                "full_observed_bytes".into(),
                serde_json::Value::Number(metadata.len().into()),
            );
        }
    }

    if let Some(object) = payload.as_object_mut() {
        object.insert(
            "_bt_transcript_observation".into(),
            serde_json::Value::Object(observation),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Debug, Parser)]
    struct ImportCli {
        #[command(flatten)]
        args: ImportArgs,
    }

    #[derive(Debug, Parser)]
    struct ServeCli {
        #[command(flatten)]
        args: ServeArgs,
    }

    #[test]
    fn serve_defaults_to_short_journal_backed_session_retirement() {
        let args = ServeCli::try_parse_from(["test"]).unwrap().args;
        assert_eq!(args.session_idle_timeout_secs, 30);
    }

    #[test]
    fn additional_metadata_overrides_a_route_only_with_a_json_object() {
        let mut route = SessionRoute {
            additional_metadata: Some(serde_json::json!({"saved": true})),
            ..SessionRoute::default()
        };
        apply_additional_metadata(&mut route, Some(r#"{"run_id":"123"}"#)).unwrap();
        assert_eq!(
            route.additional_metadata,
            Some(serde_json::json!({"run_id": "123"}))
        );

        let error = apply_additional_metadata(&mut route, Some("[]")).unwrap_err();
        assert!(error.to_string().contains("must be a JSON object"));
        let error = apply_additional_metadata(&mut route, Some("not-json")).unwrap_err();
        assert!(error
            .to_string()
            .contains("invalid --additional-metadata JSON"));
    }

    #[test]
    fn import_args_accept_multiple_sessions_or_all() {
        let explicit = ImportCli::try_parse_from([
            "test",
            "codex",
            "session-a",
            "session-b",
            "--destination",
            "project_logs:project-id",
        ])
        .unwrap()
        .args;
        assert_eq!(explicit.session_ids, ["session-a", "session-b"]);
        assert!(!explicit.all);
        assert!(explicit.destination.is_some());

        let all = ImportCli::try_parse_from(["test", "claude", "--all"])
            .unwrap()
            .args;
        assert!(all.session_ids.is_empty());
        assert!(all.all);

        assert!(ImportCli::try_parse_from(["test", "codex"]).is_err());
        assert!(ImportCli::try_parse_from(["test", "codex", "session-a", "--all"]).is_err());
    }

    #[test]
    fn attach_requires_one_explicit_session() {
        let args = ImportArgs {
            source: ImportSource::Codex,
            session_ids: vec!["one".into(), "two".into()],
            all: false,
            destination: None,
            parent: None,
            attach: true,
            additional_metadata: None,
        };
        assert!(validate_import_selection(&args)
            .unwrap_err()
            .to_string()
            .contains("exactly one"));
    }

    #[test]
    fn json_string_fields_accept_strings_and_numbers_only() {
        let payload = serde_json::json!({
            "string": "session",
            "number": 42,
            "boolean": true,
            "null": null
        });
        assert_eq!(
            json_str_field(&payload, "string").as_deref(),
            Some("session")
        );
        assert_eq!(json_str_field(&payload, "number").as_deref(), Some("42"));
        assert_eq!(json_str_field(&payload, "boolean"), None);
        assert_eq!(json_str_field(&payload, "null"), None);
        assert_eq!(json_str_field(&payload, "missing"), None);
    }

    #[test]
    fn clock_returns_a_positive_epoch_timestamp() {
        assert!(now_ms() > 0);
    }

    #[test]
    fn transcript_observation_captures_compact_and_full_boundaries() {
        let dir = tempfile::tempdir().unwrap();
        let compact = dir.path().join("transcript.jsonl");
        let full = dir.path().join("transcript_full.jsonl");
        std::fs::write(&compact, b"compact\n").unwrap();
        std::fs::write(&full, b"complete record\n").unwrap();
        let mut payload = serde_json::json!({
            "transcriptPath": compact.to_string_lossy()
        });

        add_transcript_observation(&mut payload, "transcriptPath");

        let observed = &payload["_bt_transcript_observation"];
        assert_eq!(observed["path"], compact.to_string_lossy().as_ref());
        assert_eq!(observed["observed_bytes"], 8);
        assert_eq!(observed["full_path"], full.to_string_lossy().as_ref());
        assert_eq!(observed["full_observed_bytes"], 16);
    }

    #[test]
    fn import_destination_without_session_config_fails_fast() {
        let mut config = None;
        let destination = wire::TraceDestination::ProjectLogs {
            project_id: Some("project-id".to_string()),
            project_name: None,
        };

        let error = apply_import_destination(&mut config, Some(destination)).unwrap_err();

        assert!(error
            .to_string()
            .contains("import destination requires a resolved Braintrust session configuration"));
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
                agent_args: Vec::new(),
            },
            test_run_hook_command(),
            SessionRoute::default(),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("requires a trace destination"));
    }

    #[test]
    fn codex_managed_run_injects_live_hooks() {
        let args = managed_run_args(RunSource::Codex, &test_run_hook_command()).unwrap();
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
    fn claude_managed_run_injects_live_hooks() {
        let args = managed_run_args(RunSource::Claude, &test_run_hook_command()).unwrap();
        assert_eq!(args[0], "--settings");
        let settings: serde_json::Value = serde_json::from_str(args[1].to_str().unwrap()).unwrap();
        let hooks = settings["hooks"].as_object().unwrap();
        assert_eq!(hooks.len(), CLAUDE_RUN_HOOK_EVENTS.len());
        for event in ["SessionStart", "PreToolUse"] {
            let matcher_groups = hooks[event].as_array().unwrap();
            assert_eq!(matcher_groups.len(), 1);
            assert!(matcher_groups[0]["hooks"].is_array());
            assert!(matcher_groups[0]["hooks"][0]["hooks"].is_null());
        }
        let command = hooks["SessionStart"][0]["hooks"][0]["command"]
            .as_str()
            .unwrap();
        assert!(command.contains("--managed-run-hook"));
        assert!(command.contains("agents"));
        assert!(command.contains("hook"));
        assert!(command.contains("--source"));
        assert!(command.contains("claude"));
        assert!(!command.contains("transcript"));
    }

    #[test]
    fn opencode_managed_run_preserves_inline_config_and_adds_plugin() {
        let config =
            opencode_managed_config(Some(r#"{"model":"test/model","plugin":["other"]}"#)).unwrap();
        let config: serde_json::Value = serde_json::from_str(&config).unwrap();
        assert_eq!(config["model"], "test/model");
        assert_eq!(
            config["plugin"],
            serde_json::json!(["other", "@braintrust/trace-opencode@^1"])
        );
        assert!(
            managed_run_args(RunSource::OpenCode, &test_run_hook_command())
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn pi_managed_run_loads_the_npm_extension() {
        assert_eq!(
            managed_run_args(RunSource::Pi, &test_run_hook_command()).unwrap(),
            ["-e", "npm:@braintrust/pi-extension@^1"]
        );
    }

    #[test]
    fn managed_hook_commands_quote_frontend_paths() {
        let hook = test_run_hook_command();
        let unix = managed_hook_shell_command(&hook, "codex", false).unwrap();
        assert!(unix.contains("'/opt/Braintrust CLI/bt' 'agents' 'hook' '--source' 'codex'"));
        let windows = managed_hook_shell_command(&hook, "claude", true).unwrap();
        assert!(windows
            .contains("\"/opt/Braintrust CLI/bt\" \"agents\" \"hook\" \"--source\" \"claude\""));
    }
}
