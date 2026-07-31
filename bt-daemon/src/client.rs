//! Client side: ensure a daemon is running (spawn detached if not) and do
//! JSON-RPC round-trips over the socket. Used by the `hook` and `status`
//! entry points, and by tests.

use crate::wire::{Message, Request, RequestId, Response};
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
                    anyhow::bail!("rpc error {} on {method}: {}", err.code, err.message);
                }
                return Ok(result.unwrap_or(serde_json::Value::Null));
            }
        }
    }

    async fn write(&mut self, msg: &Message) -> anyhow::Result<()> {
        let mut line = msg.to_line()?;
        line.push('\n');
        self.writer.write_all(line.as_bytes()).await?;
        self.writer.flush().await?;
        Ok(())
    }
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

#[cfg(unix)]
fn spawn_daemon(host: &HostInfo, socket: &Path) -> anyhow::Result<()> {
    use std::os::unix::process::CommandExt;
    use std::process::{Command, Stdio};

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

    let mut cmd = Command::new(exe);
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
    // Detach into our own process group so the daemon outlives the hook (and
    // the agent's) process and its controlling terminal.
    cmd.process_group(0);
    cmd.spawn()?;
    Ok(())
}

#[cfg(windows)]
fn spawn_daemon(host: &HostInfo, socket: &Path) -> anyhow::Result<()> {
    use std::os::windows::process::CommandExt;
    use std::process::{Command, Stdio};

    const DETACHED_PROCESS: u32 = 0x0000_0008;
    const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;

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

    let mut cmd = Command::new(exe);
    cmd.args(rest);
    cmd.arg("--socket").arg(socket);
    cmd.stdin(Stdio::null());
    match log {
        Some(file) => {
            let stderr = file.try_clone()?;
            cmd.stdout(Stdio::from(file));
            cmd.stderr(Stdio::from(stderr));
        }
        None => {
            cmd.stdout(Stdio::null());
            cmd.stderr(Stdio::null());
        }
    }
    cmd.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP);
    cmd.spawn()?;
    Ok(())
}
