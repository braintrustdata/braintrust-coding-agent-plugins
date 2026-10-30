//! Local daemon transport.
//!
//! Unix hosts use a Unix-domain socket. Windows uses a byte-mode named pipe.
//! Both transports expose the same async byte stream to the JSON-lines RPC
//! layer, keeping framing and daemon behavior platform-independent.

use std::future::Future;
use std::path::Path;
#[cfg(windows)]
use std::time::Duration;

/// Hold a stable lock file for the lifetime of a Unix listener. A timed-out
/// RPC cannot establish that the process which bound the socket has died.
#[cfg(unix)]
pub(crate) fn lock_endpoint(endpoint: &Path) -> anyhow::Result<Option<std::fs::File>> {
    let mut name = endpoint.as_os_str().to_os_string();
    name.push(".lock");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(std::path::PathBuf::from(name))?;
    match fs2::FileExt::try_lock_exclusive(&file) {
        Ok(()) => Ok(Some(file)),
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// A departing process (or a short-lived inherited file descriptor) can hold
/// the lock briefly after removing the socket. Allow that handover to finish.
#[cfg(unix)]
pub(crate) async fn wait_for_endpoint_lock(
    endpoint: &Path,
) -> anyhow::Result<Option<std::fs::File>> {
    for _ in 0..50 {
        if let Some(lock) = lock_endpoint(endpoint)? {
            return Ok(Some(lock));
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    Ok(None)
}

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
    security: crate::win_acl::SecurityDescriptor,
    next: tokio::net::windows::named_pipe::NamedPipeServer,
}

#[cfg(windows)]
impl Listener {
    /// Create the first server instance exclusively. This is the named-pipe
    /// equivalent of binding a Unix socket and is what resolves daemon races.
    fn bind_raw(endpoint: &Path) -> std::io::Result<Self> {
        // The default named-pipe DACL lets Everyone and Anonymous read. Every
        // instance instead admits only the current user, like the private
        // socket directory on unix.
        let security = crate::win_acl::owner_only(false)?;
        let next = create_instance(endpoint.as_os_str(), &security, true)?;
        Ok(Self {
            endpoint: endpoint.as_os_str().to_owned(),
            security,
            next,
        })
    }

    pub(crate) async fn accept(&mut self) -> std::io::Result<ServerStream> {
        self.next.connect().await?;
        // Install another listening instance before handing the connected
        // stream to a task, avoiding a gap where concurrent hook clients see
        // ERROR_PIPE_BUSY.
        let next = create_instance(&self.endpoint, &self.security, false)?;
        Ok(std::mem::replace(&mut self.next, next))
    }
}

#[cfg(windows)]
fn create_instance(
    endpoint: &std::ffi::OsStr,
    security: &crate::win_acl::SecurityDescriptor,
    first: bool,
) -> std::io::Result<ServerStream> {
    use tokio::net::windows::named_pipe::ServerOptions;

    let mut attributes = security.attributes();
    // SAFETY: `attributes` and the descriptor it points to outlive the call,
    // which copies them into the new pipe instance.
    unsafe {
        ServerOptions::new()
            .first_pipe_instance(first)
            .create_with_security_attributes_raw(
                endpoint,
                std::ptr::addr_of_mut!(attributes).cast(),
            )
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
    _probe_alive: &mut F,
) -> anyhow::Result<Option<Listener>>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    if endpoint.exists() {
        if unix_endpoint_in_use(endpoint).await? {
            return Ok(None);
        }
        cleanup(endpoint);
    }
    match Listener::bind_raw(endpoint) {
        Ok(listener) => Ok(Some(listener)),
        Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {
            if unix_endpoint_in_use(endpoint).await? {
                Ok(None)
            } else {
                cleanup(endpoint);
                Ok(Some(Listener::bind_raw(endpoint)?))
            }
        }
        Err(error) => Err(error.into()),
    }
}

/// A successful transport connection proves ownership even when an older
/// daemon has not started accepting RPCs. A timeout is ambiguous and must not
/// authorize unlinking a potentially live listener.
#[cfg(unix)]
async fn unix_endpoint_in_use(endpoint: &Path) -> anyhow::Result<bool> {
    match tokio::time::timeout(std::time::Duration::from_secs(1), connect(endpoint)).await {
        Ok(Ok(_)) | Err(_) => Ok(true),
        Ok(Err(error))
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
            ) =>
        {
            Ok(false)
        }
        Ok(Err(error)) => Err(error.into()),
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

#[cfg(all(test, unix))]
mod unix_tests {
    #[test]
    fn endpoint_lock_survives_an_unresponsive_owner_and_is_reusable() {
        let tmp = tempfile::tempdir().unwrap();
        let endpoint = tmp.path().join("daemon.sock");
        let owner = super::lock_endpoint(&endpoint).unwrap().unwrap();
        assert!(super::lock_endpoint(&endpoint).unwrap().is_none());
        drop(owner);
        assert!(super::lock_endpoint(&endpoint).unwrap().is_some());
    }

    #[tokio::test]
    async fn startup_owner_excludes_a_contender_before_socket_bind() {
        let tmp = tempfile::tempdir().unwrap();
        let endpoint = tmp.path().join("daemon.sock");
        let owner = super::lock_endpoint(&endpoint).unwrap().unwrap();
        assert!(!endpoint.exists());
        let contender_path = endpoint.clone();
        let mut contender = tokio::spawn(async move {
            super::wait_for_endpoint_lock(&contender_path)
                .await
                .unwrap()
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut contender)
                .await
                .is_err()
        );
        assert!(!endpoint.exists());
        drop(owner);
        assert!(contender.await.unwrap().is_some());
    }

    #[tokio::test]
    async fn unresponsive_legacy_listener_is_not_unlinked() {
        let tmp = tempfile::tempdir().unwrap();
        let endpoint = tmp.path().join("daemon.sock");
        let listener = tokio::net::UnixListener::bind(&endpoint).unwrap();
        // A legacy daemon doesn't take the new lock or answer initialize while
        // recovering. Its successful transport connection still owns the path.
        let claimed = super::claim(&endpoint, || async { false }).await.unwrap();
        assert!(claimed.is_none());
        assert!(endpoint.exists());
        drop(listener);
        let claimed = super::claim(&endpoint, || async { false }).await.unwrap();
        assert!(claimed.is_some());
    }
}

#[cfg(all(test, windows))]
mod tests {
    use super::*;
    use crate::win_acl::test_support::{assert_owner_only, handle_dacl_sddl};
    use std::os::windows::io::AsRawHandle;

    #[tokio::test]
    async fn named_pipe_instances_are_private_to_the_owner() {
        let endpoint = std::path::PathBuf::from(format!(
            r"\\.\pipe\bt-daemon-acl-test-{}",
            uuid::Uuid::new_v4()
        ));
        let mut listener = claim(&endpoint, || async { false }).await.unwrap().unwrap();
        assert_owner_only(&handle_dacl_sddl(listener.next.as_raw_handle()), false);

        // The owner can still connect, and the replacement instance `accept`
        // installs for the next client is private as well.
        let _client = connect(&endpoint).await.unwrap();
        let _server = listener.accept().await.unwrap();
        assert_owner_only(&handle_dacl_sddl(listener.next.as_raw_handle()), false);
    }
}
