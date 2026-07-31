//! bt-daemon: the embeddable library behind the Braintrust coding-agent
//! tracing daemon. Two front-ends consume it (see `../DESIGN.md`):
//!   * `bt` wires the [`clap::Args`] structs into its command tree and fills
//!     [`wire::SessionConfig`] from its own auth resolution.
//!   * the feature-gated standalone `bt-daemon` binary does the same with
//!     env/flag token auth only, for isolated testing.
//!
//! The core is credential-passive: it only ever *receives* a resolved
//! [`wire::BackendAuth`] with the session config, so both front-ends share all
//! core behavior. The `cli` feature only gates the standalone binary and its
//! logging subscriber.

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
pub use server::ServeOptions;
pub use sink::{BraintrustSinkConfig, BraintrustSinkFactory, DebugSinkFactory, Sink, SinkFactory};
pub use translate::{
    AgentTranslator, Registry, SessionCtx, SpanOp, SpanRow, SpanType, TranslatorFactory,
};

use clap::{Args, ValueEnum};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use wire::{method, Envelope, SessionConfig, StatusResult, PROTOCOL_VERSION};

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
    /// Attach the agent session below an existing Braintrust span.
    #[arg(long)]
    pub parent_span_id: Option<String>,
    /// Existing trace root when attaching below a non-root parent.
    #[arg(long)]
    pub root_span_id: Option<String>,
    /// JSON object merged into root-span metadata.
    #[arg(long)]
    pub additional_metadata: Option<String>,
    /// Route spans to an existing Braintrust experiment instead of project
    /// logs. The Claude shim supplies this from CC_EXPERIMENT_ID.
    #[arg(long)]
    pub experiment_id: Option<String>,
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ImportSource {
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
/// `config` is the caller-resolved session config (auth + trace settings).
/// Returns `Ok` once the daemon has acked (journaled + enqueued). Callers that
/// must never fail the agent's turn should treat any `Err` as non-fatal and
/// exit 0.
pub async fn run_hook(
    args: HookArgs,
    mut config: SessionConfig,
    host: HostInfo,
) -> anyhow::Result<()> {
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

    if let Some(project) = settings.project.filter(|project| !project.is_empty()) {
        config.project = Some(project);
    }
    match settings.flush_on_turn_end {
        Some(true) => config.flush_mode = wire::FlushMode::FlushOnTurnEnd,
        Some(false) => config.flush_mode = wire::FlushMode::FireAndForget,
        None if args.flush_on_turn_end => config.flush_mode = wire::FlushMode::FlushOnTurnEnd,
        None => {}
    }
    if args.parent_span_id.is_some() {
        config.parent_span_id = args.parent_span_id.clone();
    }
    if args.root_span_id.is_some() {
        config.root_span_id = args.root_span_id.clone();
    }
    match (config.parent_span_id.clone(), config.root_span_id.clone()) {
        (Some(parent), None) => config.root_span_id = Some(parent),
        (None, Some(root)) => config.parent_span_id = Some(root),
        _ => {}
    }
    if let Some(metadata) = settings.additional_metadata {
        config.additional_metadata = Some(serde_json::Value::Object(metadata));
    } else if let Some(metadata) = &args.additional_metadata {
        let value: serde_json::Value = serde_json::from_str(metadata)
            .map_err(|e| anyhow::anyhow!("invalid --additional-metadata JSON: {e}"))?;
        if !value.is_object() {
            anyhow::bail!("--additional-metadata must be a JSON object");
        }
        config.additional_metadata = Some(value);
    }
    if let Some(experiment_id) = &args.experiment_id {
        let mut metadata = config
            .additional_metadata
            .take()
            .and_then(|value| value.as_object().cloned())
            .unwrap_or_default();
        metadata.insert(
            "_bt_experiment_id".to_string(),
            serde_json::Value::String(experiment_id.clone()),
        );
        config.additional_metadata = Some(serde_json::Value::Object(metadata));
    }
    let env = Envelope {
        source: args.source.clone(),
        source_version: args.source_version.clone(),
        session_id,
        event,
        ts_ms: now_ms(),
        payload,
        config: Some(config),
    };

    let socket = paths::socket_path(args.socket.as_deref());
    forward_envelope(&env, &socket, &host, args.no_spawn).await?;
    let should_flush = env.event == "SessionEnd"
        || (matches!(
            env.config.as_ref().map(|c| c.flush_mode),
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
    config: Option<SessionConfig>,
) -> anyhow::Result<()> {
    let file = transcript_import::resolve_transcript(&args.session_id, args.source)?;
    import_transcript(&file, args.source, opts, config).await
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
    use std::collections::HashMap;
    let entries = transcript_import::transcript_envelopes(file, source)?;

    struct Live {
        translator: Box<dyn AgentTranslator>,
        sink: Box<dyn Sink>,
        ctx: SessionCtx,
        pending_ops: usize,
    }
    let mut sessions: HashMap<String, Live> = HashMap::new();

    for mut env in entries {
        env.config = config.clone();
        let sid = env.session_id.clone();
        let live = match sessions.get_mut(&sid) {
            Some(l) => l,
            None => {
                let translator = opts.translators.create(&env.source, &sid);
                let sink = opts.sink_factory.create(&sid, &env.source)?;
                sessions.insert(
                    sid.clone(),
                    Live {
                        translator,
                        sink,
                        ctx: SessionCtx {
                            session_id: sid.clone(),
                            config: None,
                        },
                        pending_ops: 0,
                    },
                );
                sessions.get_mut(&sid).unwrap()
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

    for (_sid, mut live) in sessions {
        let ops = live.translator.flush(&live.ctx)?;
        live.sink.emit(&ops).await?;
        live.sink.flush().await?;
    }
    Ok(())
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
}
