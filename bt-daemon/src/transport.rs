//! Local daemon transport.
//!
//! Unix hosts use a Unix-domain socket. Windows uses a byte-mode named pipe.
//! Both transports expose the same async byte stream to the JSON-lines RPC
//! layer, keeping framing and daemon behavior platform-independent.

use std::future::Future;
use std::path::Path;
#[cfg(windows)]
use std::time::Duration;

#[cfg(unix)]
pub(crate) type ClientStream = tokio::net::UnixStream;
#[cfg(windows)]
pub(crate) type ClientStream = tokio::net::windows::named_pipe::NamedPipeClient;

#[cfg(unix)]
pub(crate) type ServerStream = tokio::net::UnixStream;
#[cfg(windows)]
pub(crate) type ServerStream = tokio::net::windows::named_pipe::NamedPipeServer;

/// Connect to the local daemon. Windows retries briefly when all named-pipe
/// instances are occupied (`ERROR_PIPE_BUSY`) so normal concurrent hooks do
/// not spuriously conclude that the daemon is absent.
#[cfg(unix)]
pub(crate) async fn connect(endpoint: &Path) -> std::io::Result<ClientStream> {
    tokio::net::UnixStream::connect(endpoint).await
}

#[cfg(windows)]
pub(crate) async fn connect(endpoint: &Path) -> std::io::Result<ClientStream> {
    use tokio::net::windows::named_pipe::ClientOptions;

    const ERROR_PIPE_BUSY: i32 = 231;
    let mut last_busy = None;
    for _ in 0..20 {
        match ClientOptions::new().open(endpoint) {
            Ok(stream) => return Ok(stream),
            Err(error) if error.raw_os_error() == Some(ERROR_PIPE_BUSY) => {
                last_busy = Some(error);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => return Err(error),
        }
    }
    Err(last_busy.unwrap_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::WouldBlock, "named pipe remained busy")
    }))
}

#[cfg(unix)]
pub(crate) struct Listener {
    inner: tokio::net::UnixListener,
}

#[cfg(unix)]
impl Listener {
    fn bind_raw(endpoint: &Path) -> std::io::Result<Self> {
        Ok(Self {
            inner: tokio::net::UnixListener::bind(endpoint)?,
        })
    }

    pub(crate) async fn accept(&mut self) -> std::io::Result<ServerStream> {
        self.inner.accept().await.map(|(stream, _)| stream)
    }
}

#[cfg(windows)]
pub(crate) struct Listener {
    endpoint: std::ffi::OsString,
    next: tokio::net::windows::named_pipe::NamedPipeServer,
}

#[cfg(windows)]
impl Listener {
    /// Create the first server instance exclusively. This is the named-pipe
    /// equivalent of binding a Unix socket and is what resolves daemon races.
    fn bind_raw(endpoint: &Path) -> std::io::Result<Self> {
        use tokio::net::windows::named_pipe::ServerOptions;

        let next = ServerOptions::new()
            .first_pipe_instance(true)
            .create(endpoint)?;
        Ok(Self {
            endpoint: endpoint.as_os_str().to_owned(),
            next,
        })
    }

    pub(crate) async fn accept(&mut self) -> std::io::Result<ServerStream> {
        use tokio::net::windows::named_pipe::ServerOptions;

        self.next.connect().await?;
        // Install another listening instance before handing the connected
        // stream to a task, avoiding a gap where concurrent hook clients see
        // ERROR_PIPE_BUSY.
        let next = ServerOptions::new().create(&self.endpoint)?;
        Ok(std::mem::replace(&mut self.next, next))
    }
}

/// Claim the daemon endpoint. Returns `None` when another healthy daemon
/// already owns it.
pub(crate) async fn claim<F, Fut>(
    endpoint: &Path,
    mut probe_alive: F,
) -> anyhow::Result<Option<Listener>>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    #[cfg(unix)]
    {
        claim_unix(endpoint, &mut probe_alive).await
    }
    #[cfg(windows)]
    {
        claim_windows(endpoint, &mut probe_alive).await
    }
}

#[cfg(unix)]
async fn claim_unix<F, Fut>(
    endpoint: &Path,
    probe_alive: &mut F,
) -> anyhow::Result<Option<Listener>>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    if endpoint.exists() {
        if probe_alive().await {
            return Ok(None);
        }
        cleanup(endpoint);
    }
    match Listener::bind_raw(endpoint) {
        Ok(listener) => Ok(Some(listener)),
        Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {
            if probe_alive().await {
                Ok(None)
            } else {
                cleanup(endpoint);
                Ok(Some(Listener::bind_raw(endpoint)?))
            }
        }
        Err(error) => Err(error.into()),
    }
}

#[cfg(windows)]
async fn claim_windows<F, Fut>(
    endpoint: &Path,
    probe_alive: &mut F,
) -> anyhow::Result<Option<Listener>>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    if probe_alive().await {
        return Ok(None);
    }

    // Named pipes have no stale filesystem node: the name is released when
    // the last server handle closes. Retry briefly to cover a rival daemon
    // winning the probe/create race or a just-terminated daemon unwinding.
    let mut last_error = None;
    for _ in 0..50 {
        match Listener::bind_raw(endpoint) {
            Ok(listener) => return Ok(Some(listener)),
            Err(error) => {
                last_error = Some(error);
                if probe_alive().await {
                    return Ok(None);
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
    Err(last_error
        .unwrap_or_else(|| std::io::Error::other("could not create named pipe"))
        .into())
}

/// Unix socket nodes survive process death and need explicit cleanup. Windows
/// named-pipe names disappear automatically when their final handle closes.
#[cfg(unix)]
pub(crate) fn cleanup(endpoint: &Path) {
    let _ = std::fs::remove_file(endpoint);
}

#[cfg(windows)]
pub(crate) fn cleanup(_endpoint: &Path) {}
