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
mod dispatch;
mod ids;
mod journal;
mod server;
mod settings;
mod sink;
mod transcript_import;
mod translate;
mod transport;

pub mod wire;
pub use client::HostInfo;
pub use server::{AuthLease, AuthProvider, AuthResolveReason, ServeOptions};
pub use sink::{BraintrustSinkConfig, BraintrustSinkFactory, DebugSinkFactory, Sink, SinkFactory};
pub use translate::{
    AgentTranslator, Registry, SessionCtx, SpanOp, SpanRow, SpanType, TranslatorFactory,
};

use braintrust_sdk_rust::SpanComponents;
use clap::{Args, ValueEnum};
use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use wire::{method, Envelope, SessionConfig, SessionRoute, StatusResult, PROTOCOL_VERSION};

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
    /// JSON object merged into root-span metadata.
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
    /// Codex or Claude Code session id shown by the agent's resume command.
    pub session_id: String,
    /// Destination object reference, such as `project_logs:<project-id>` or
    /// `experiment:<experiment-id>`.
    #[arg(value_name = "DESTINATION", conflicts_with = "parent")]
    pub destination: Option<wire::TraceDestination>,
    /// Attach the imported session below an exported Braintrust span.
    #[arg(long, value_name = "SPAN_COMPONENTS", conflicts_with = "destination")]
    pub parent: Option<SpanComponents>,
    /// Keep following the transcript until Ctrl-C, importing new turns as the
    /// coding-agent session grows.
    #[arg(long)]
    pub attach: bool,
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
    let settings = settings::SharedSettings::load();
    if !settings.tracing_enabled() {
        return Ok(());
    }
    let payload = read_stdin_json()?;

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
    if let Some(metadata) = &args.additional_metadata {
        let value: serde_json::Value = serde_json::from_str(metadata)
            .map_err(|e| anyhow::anyhow!("invalid --additional-metadata JSON: {e}"))?;
        if !value.is_object() {
            anyhow::bail!("--additional-metadata must be a JSON object");
        }
        route.additional_metadata = Some(value);
    }
    let env = Envelope {
        source: args.source.clone(),
        source_version: args.source_version.clone(),
        session_id,
        event,
        ts_ms: now_ms(),
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
    let destination = args
        .parent
        .map(|components| wire::TraceDestination::ParentSpan { components })
        .or(args.destination);
    apply_import_destination(&mut config, destination)?;
    let file = transcript_import::resolve_transcript(&args.session_id, args.source)?;
    if args.attach {
        attach_transcript(&file, args.source, opts, config).await
    } else {
        import_transcript(&file, args.source, opts, config).await
    }
}

/// Launch a coding agent with inherited stdio and inject Braintrust hooks for
/// this invocation, without requiring the tracing plugin to be installed or
/// enabled globally.
pub async fn run_traced(
    args: RunArgs,
    hook_command: RunHookCommand,
) -> anyhow::Result<std::process::ExitStatus> {
    let executable = match args.source {
        RunSource::Codex => "codex",
        RunSource::Claude => "claude",
    };
    let injected_args = managed_run_args(args.source, &hook_command)?;
    let mut child = tokio::process::Command::new(executable)
        .args(injected_args)
        .args(args.agent_args)
        .env("_BT_TRACE_MANAGED_RUN", "1")
        .spawn()
        .map_err(|error| anyhow::anyhow!("failed to launch {executable}: {error}"))?;
    let interrupt = tokio::signal::ctrl_c();
    tokio::pin!(interrupt);

    tokio::select! {
        status = child.wait() => Ok(status?),
        result = &mut interrupt => {
            result?;
            child.start_kill()?;
            Ok(child.wait().await?)
        }
    }
}

fn managed_run_args(
    source: RunSource,
    hook_command: &RunHookCommand,
) -> anyhow::Result<Vec<OsString>> {
    let source_name = match source {
        RunSource::Codex => "codex",
        RunSource::Claude => "claude",
    };
    let unix_command = managed_hook_shell_command(hook_command, source_name, false)?;
    let windows_command = managed_hook_shell_command(hook_command, source_name, true)?;
    match source {
        RunSource::Codex => Ok(codex_managed_run_args(&unix_command, &windows_command)),
        RunSource::Claude => Ok(claude_managed_run_args(if cfg!(windows) {
            &windows_command
        } else {
            &unix_command
        })?),
    }
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
    let mut args = vec![
        OsString::from("--enable"),
        OsString::from("hooks"),
        OsString::from("--dangerously-bypass-hook-trust"),
    ];
    for event in CODEX_RUN_HOOK_EVENTS {
        args.push(OsString::from("-c"));
        args.push(OsString::from(format!(
            "hooks.{event}=[{{hooks=[{{type=\"command\",command={unix_command},commandWindows={windows_command}}}]}}]"
        )));
    }
    args
}

fn claude_managed_run_args(command: &str) -> anyhow::Result<Vec<OsString>> {
    let hook = serde_json::json!({
        "hooks": [{
            "hooks": [{
                "type": "command",
                "command": command,
                "async": false
            }]
        }]
    });
    let hooks = CLAUDE_RUN_HOOK_EVENTS
        .iter()
        .map(|event| ((*event).to_string(), hook.clone()))
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
) -> anyhow::Result<()> {
    let entries = transcript_import::transcript_envelopes(file, source)?;
    let mut processor = ImportProcessor::new(opts, config);
    processor.process(entries).await?;
    processor.finish().await
}

async fn attach_transcript(
    file: &std::path::Path,
    source: ImportSource,
    opts: ServeOptions,
    config: Option<SessionConfig>,
) -> anyhow::Result<()> {
    let mut tail = transcript_import::TranscriptTail::new(file.to_path_buf(), source);
    let mut processor = ImportProcessor::new(opts, config);
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);
    loop {
        processor.process(tail.poll(false)?).await?;
        tokio::select! {
            result = &mut shutdown => {
                result?;
                break;
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(500)) => {}
        }
    }
    processor.process(tail.poll(true)?).await?;
    processor.finish().await
}

struct ImportLive {
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
                    let sink = self.opts.sink_factory.create(&sid, &env.source)?;
                    self.sessions.insert(
                        sid.clone(),
                        ImportLive {
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
        }
        Ok(())
    }

    async fn finish(self) -> anyhow::Result<()> {
        for (_sid, mut live) in self.sessions {
            let ops = live.translator.flush(&live.ctx)?;
            live.sink.emit(&ops).await?;
            live.sink.flush().await?;
        }
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn codex_managed_run_injects_live_hooks() {
        let args = managed_run_args(RunSource::Codex, &test_run_hook_command()).unwrap();
        assert_eq!(args[0], "--enable");
        assert_eq!(args[1], "hooks");
        assert_eq!(args[2], "--dangerously-bypass-hook-trust");
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
        let command = hooks["SessionStart"]["hooks"][0]["hooks"][0]["command"]
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
    fn managed_hook_commands_quote_frontend_paths() {
        let hook = test_run_hook_command();
        let unix = managed_hook_shell_command(&hook, "codex", false).unwrap();
        assert!(unix.contains("'/opt/Braintrust CLI/bt' 'agents' 'hook' '--source' 'codex'"));
        let windows = managed_hook_shell_command(&hook, "claude", true).unwrap();
        assert!(windows
            .contains("\"/opt/Braintrust CLI/bt\" \"agents\" \"hook\" \"--source\" \"claude\""));
    }
}
