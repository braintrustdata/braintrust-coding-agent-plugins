//! Client side: ensure a daemon is running (spawn detached if not) and do
//! JSON-RPC round-trips over the socket. Used by the `hook` and `status`
//! entry points, and by tests.

use crate::args::StatusArgs;
use crate::wire::{
    self, method, Envelope, InitializeResult, ManagedRunFlushParams, Message, Request, RequestId,
    Response, StatusResult, PROTOCOL_VERSION,
};
use crate::{journal, paths};
use std::ffi::OsString;
use std::path::Path;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines, ReadHalf, WriteHalf};

use crate::transport::ClientStream;

/// Host-specific bits the client needs to (re)launch the daemon: the argv that
/// runs `serve` (e.g. `[bt, daemon, serve]` embedded, `[bt-daemon, serve]`
/// standalone) and the host binary's version string.
#[derive(Debug, Clone)]
pub struct HostInfo {
    pub serve_argv: Vec<OsString>,
    pub version: String,
}

/// A JSON-RPC error response from the daemon, kept typed so callers can tell
/// an older daemon's unknown method apart from a failed call.
#[derive(Debug)]
pub struct RpcCallError {
    pub method: String,
    pub code: i32,
    pub message: String,
}

impl std::fmt::Display for RpcCallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "rpc error {} on {}: {}",
            self.code, self.method, self.message
        )
    }
}

impl std::error::Error for RpcCallError {}

/// A framed JSON-RPC connection with request/response correlation.
pub struct Conn {
    reader: Lines<BufReader<ReadHalf<ClientStream>>>,
    writer: WriteHalf<ClientStream>,
    next_id: i64,
}

impl Conn {
    pub fn new(stream: ClientStream) -> Self {
        let (r, w) = tokio::io::split(stream);
        Conn {
            reader: BufReader::new(r).lines(),
            writer: w,
            next_id: 1,
        }
    }

    /// Send a request and await its matching response (ignoring any interleaved
    /// notifications). Returns the `result` value or an error on `error`.
    pub async fn request<T: serde::Serialize>(
        &mut self,
        method: &str,
        params: T,
    ) -> anyhow::Result<serde_json::Value> {
        let id = self.next_id;
        self.next_id += 1;
        let req = Request::new(RequestId::Int(id), method, serde_json::to_value(params)?);
        self.write(&Message::Request(req)).await?;

        loop {
            let line =
                self.reader.next_line().await?.ok_or_else(|| {
                    anyhow::anyhow!("connection closed before response to {method}")
                })?;
            if let Message::Response(Response {
                id: rid,
                result,
                error,
                ..
            }) = Message::from_line(&line)?
            {
                if rid != RequestId::Int(id) {
                    continue;
                }
                if let Some(err) = error {
                    return Err(RpcCallError {
                        method: method.to_string(),
                        code: err.code,
                        message: err.message,
                    }
                    .into());
                }
                return Ok(result.unwrap_or(serde_json::Value::Null));
            }
        }
    }

    /// Send a request and decode its result.
    pub async fn call<R: serde::de::DeserializeOwned>(
        &mut self,
        method: &str,
        params: impl serde::Serialize,
    ) -> anyhow::Result<R> {
        Ok(serde_json::from_value(self.request(method, params).await?)?)
    }

    /// Perform the `initialize` handshake as a named, non-hook client.
    pub async fn initialize(&mut self, source: &str) -> anyhow::Result<InitializeResult> {
        self.call(
            method::INITIALIZE,
            serde_json::json!({
                "protocol_version": PROTOCOL_VERSION,
                "client": { "source": source }
            }),
        )
        .await
    }

    async fn write(&mut self, msg: &Message) -> anyhow::Result<()> {
        let mut line = msg.to_line()?;
        line.push('\n');
        self.writer.write_all(line.as_bytes()).await?;
        self.writer.flush().await?;
        Ok(())
    }
}

/// Whether a connect error means no daemon is listening, as opposed to a
/// daemon endpoint that exists but cannot be opened.
pub(crate) fn daemon_absent(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
    )
}

pub(crate) async fn connect(socket: &Path) -> std::io::Result<ClientStream> {
    crate::transport::connect(socket).await
}

/// Connect to the daemon, spawning it (detached) if it isn't up yet. With
/// `no_spawn`, a missing daemon is a hard error (tests / diagnostics).
pub async fn ensure_daemon(
    socket: &Path,
    host: &HostInfo,
    no_spawn: bool,
) -> anyhow::Result<ClientStream> {
    if let Ok(s) = connect(socket).await {
        return Ok(s);
    }
    if no_spawn {
        anyhow::bail!("no daemon at {} and --no-spawn is set", socket.display());
    }
    spawn_daemon(host, socket)?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Ok(s) = connect(socket).await {
            return Ok(s);
        }
        if tokio::time::Instant::now() >= deadline {
            break;
        }
    }
    anyhow::bail!("daemon did not come up at {}", socket.display())
}

fn spawn_daemon(host: &HostInfo, socket: &Path) -> anyhow::Result<()> {
    use std::process::Stdio;

    let (exe, rest) = host
        .serve_argv
        .split_first()
        .ok_or_else(|| anyhow::anyhow!("empty serve_argv"))?;

    let data_dir = crate::paths::data_dir(None);
    let _ = crate::paths::ensure_private_dir(&data_dir);
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(data_dir.join("serve.log"))
        .ok();

    let mut cmd = crate::subprocess::detached_daemon_command(exe);
    cmd.args(rest);
    cmd.arg("--socket").arg(socket);
    cmd.stdin(Stdio::null());
    match log {
        Some(f) => {
            let f2 = f.try_clone()?;
            cmd.stdout(Stdio::from(f));
            cmd.stderr(Stdio::from(f2));
        }
        None => {
            cmd.stdout(Stdio::null());
            cmd.stderr(Stdio::null());
        }
    }
    cmd.spawn()?;
    Ok(())
}

pub(crate) fn initialize_params(env: &Envelope, daemon_version: &str) -> serde_json::Value {
    serde_json::json!({
        "protocol_version": PROTOCOL_VERSION,
        "client": {
            "source": env.source,
            "daemon_version": daemon_version,
            "plugin_version": env.plugin_version,
            "pid": std::process::id()
        }
    })
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
    const MAX_HANDOVER_ATTEMPTS: usize = 3;
    let mut handover_attempts = 0;
    let mut capture_retry_deadline = None;

    loop {
        if capture_retry_deadline.is_some_and(|deadline| tokio::time::Instant::now() >= deadline) {
            anyhow::bail!(
                "replacement daemon did not accept the event at {}",
                socket.display()
            );
        }

        let stream = match connect(socket).await {
            Ok(stream) => stream,
            Err(_) if capture_retry_deadline.is_some() && no_spawn => {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                continue;
            }
            Err(_) => ensure_daemon(socket, host, no_spawn).await?,
        };
        let mut conn = Conn::new(stream);
        let initialized = match conn
            .request(method::INITIALIZE, initialize_params(env, &host.version))
            .await
        {
            Ok(initialized) => initialized,
            Err(_) if capture_retry_deadline.is_some() => {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                continue;
            }
            Err(error) => return Err(error),
        };
        let initialized: wire::InitializeResult = serde_json::from_value(initialized)?;
        if daemon_needs_upgrade(&initialized.daemon_version, &host.version) {
            if no_spawn {
                anyhow::bail!(
                    "daemon version {} is older than client {} and --no-spawn is set",
                    initialized.daemon_version,
                    host.version
                );
            }
            if handover_attempts == MAX_HANDOVER_ATTEMPTS {
                anyhow::bail!(
                    "daemon version {} is still older than client {} after {MAX_HANDOVER_ATTEMPTS} handover attempts",
                    initialized.daemon_version,
                    host.version
                );
            }
            conn.request(method::DAEMON_SHUTDOWN, serde_json::json!({}))
                .await?;
            handover_attempts += 1;
            drop(conn);
            wait_for_daemon_exit(socket).await;
            continue;
        }
        let result = match conn.request(method::EVENT_LOG, env).await {
            Ok(result) => result,
            Err(_) if capture_retry_deadline.is_some() => {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                continue;
            }
            Err(error) => return Err(error),
        };
        let result: wire::EventLogResult = serde_json::from_value(result)?;
        if result.accepted {
            return Ok(());
        }
        capture_retry_deadline
            .get_or_insert_with(|| tokio::time::Instant::now() + std::time::Duration::from_secs(5));
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

async fn wait_for_daemon_exit(socket: &std::path::Path) {
    for _ in 0..100 {
        if connect(socket).await.is_err() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// A shared daemon only moves forward. Released versions use semver; opaque
/// development versions retain the old exact-match handover behavior.
fn daemon_needs_upgrade(daemon_version: &str, client_version: &str) -> bool {
    if daemon_version == client_version {
        return false;
    }
    compare_daemon_versions(daemon_version, client_version).is_none_or(|ordering| ordering.is_lt())
}

pub(crate) fn compare_daemon_versions(left: &str, right: &str) -> Option<std::cmp::Ordering> {
    Some(
        semver::Version::parse(left)
            .ok()?
            .cmp(&semver::Version::parse(right).ok()?),
    )
}

/// Ask the daemon to flush a session, bounded by `timeout_ms`. A reliable
/// barrier: on return, every event enqueued before the call has been processed
/// and its spans emitted to the sink.
pub async fn flush_session(
    session_id: &str,
    socket: &std::path::Path,
    timeout_ms: u64,
) -> anyhow::Result<wire::FlushResult> {
    let stream = connect(socket).await?;
    let mut conn = Conn::new(stream);
    conn.initialize("flush").await?;
    let params = wire::FlushParams {
        session_id: session_id.to_string(),
        timeout_ms,
    };
    conn.call(method::SESSION_FLUSH, params).await
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

pub(crate) async fn flush_managed_run_in(
    managed_run_id: &str,
    socket: &std::path::Path,
    timeout_ms: u64,
    data_dir: &std::path::Path,
) -> anyhow::Result<wire::FlushResult> {
    let stream = match connect(socket).await {
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
    let mut conn = Conn::new(stream);
    conn.initialize("managed-run-flush").await?;
    let params = ManagedRunFlushParams {
        managed_run_id: managed_run_id.to_string(),
        timeout_ms,
    };
    conn.call(method::MANAGED_RUN_FLUSH, params).await
}

/// Query daemon status. `Ok(None)` means no daemon is running.
pub async fn run_status(args: StatusArgs) -> anyhow::Result<Option<StatusResult>> {
    let socket = paths::socket_path(args.socket.as_deref());
    let stream = match connect(&socket).await {
        Ok(s) => s,
        Err(_) => return Ok(None),
    };
    Ok(Some(status_over(stream, args.session_id).await?))
}

/// Query status over an already-open daemon connection.
pub(crate) async fn status_over(
    stream: ClientStream,
    session_id: Option<String>,
) -> anyhow::Result<StatusResult> {
    let mut conn = Conn::new(stream);
    conn.initialize("status").await?;
    conn.call(method::STATUS_GET, wire::StatusParams { session_id })
        .await
}

/// Request a graceful daemon shutdown. Primarily useful for lifecycle
/// management and transport integration tests.
pub async fn shutdown_daemon(socket: &std::path::Path) -> anyhow::Result<()> {
    let stream = connect(socket).await?;
    let mut conn = Conn::new(stream);
    conn.request(method::DAEMON_SHUTDOWN, serde_json::json!({}))
        .await?;
    Ok(())
}
