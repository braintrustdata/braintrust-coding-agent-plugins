//! Read-only MSP client. The SDK owns framing, correlation, and initialization;
//! we own process lifetime, timeouts, pagination, and the repo's spawn policy.
use anyhow::{bail, Context, Result};
use muse_code_sdk::connection::{Connection, StdioTransport};
use muse_code_sdk::handshake::Handshake;
use muse_code_sdk::msp::{ClientInfo, InitializeParams, SessionListParams, SessionListResult};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashSet;
use std::ffi::OsStr;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::task::JoinHandle;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_PAGES: usize = 1_000;
const DIAGNOSTIC_BYTES: usize = 64 * 1024;

trait Method {
    const NAME: &'static str;
    type Params: Serialize;
    type Response: DeserializeOwned;
}
struct ListSessions;
impl Method for ListSessions {
    const NAME: &'static str = "session/list";
    type Params = SessionListParams;
    type Response = SessionListResult;
}

pub(crate) struct MuseMspClient {
    connection: Connection,
    host: tokio::process::Child,
    driver: JoinHandle<()>,
    diagnostics: Option<JoinHandle<Result<String>>>,
}

impl Drop for MuseMspClient {
    fn drop(&mut self) {
        // Cancellation must not leave a detached driver holding the child's pipes.
        self.driver.abort();
        if let Some(diagnostics) = &self.diagnostics {
            diagnostics.abort();
        }
        // host was spawned with kill_on_drop.
    }
}

impl MuseMspClient {
    pub(crate) async fn connect(executable: &OsStr) -> Result<Self> {
        let mut command: tokio::process::Command =
            crate::subprocess::background_command(executable).into();
        let mut host = command
            .arg("serve")
            .kill_on_drop(true)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("start Muse MSP host")?;
        let stdin = host.stdin.take().context("Muse MSP stdin was not piped")?;
        let stdout = host
            .stdout
            .take()
            .context("Muse MSP stdout was not piped")?;
        let stderr = host
            .stderr
            .take()
            .context("Muse MSP stderr was not piped")?;
        let (connection, driver) = Connection::new(StdioTransport::new(stdin, stdout));
        let client = Self {
            connection,
            host,
            driver: tokio::spawn(driver.run()),
            diagnostics: Some(tokio::spawn(drain_stderr(stderr))),
        };
        let initialized = tokio::time::timeout(
            REQUEST_TIMEOUT,
            Handshake::new(client.connection.clone()).initialize(InitializeParams {
                client_info: ClientInfo {
                    name: "braintrust_muse_import".into(),
                    version: env!("CARGO_PKG_VERSION").into(),
                    title: None,
                },
                capabilities: None,
            }),
        )
        .await
        .context("Muse MSP initialize timed out")?
        .context("initialize Muse MSP connection")?;
        if let Some(warning) = initialized.fingerprint_warning {
            tracing::warn!(?warning, "Muse MSP schema fingerprint differs from SDK");
        }
        Ok(client)
    }

    async fn call<M: Method>(&self, params: &M::Params) -> Result<M::Response> {
        call::<M>(&self.connection, params, REQUEST_TIMEOUT).await
    }

    pub(crate) async fn list_sessions(
        &self,
        params: &SessionListParams,
    ) -> Result<SessionListResult> {
        self.call::<ListSessions>(params).await
    }

    pub(crate) async fn session_ids(&self) -> Result<Vec<String>> {
        let mut sessions = Vec::new();
        let mut seen_cursors = HashSet::new();
        let mut cursor = None;
        for page_index in 1..=MAX_PAGES {
            let page = self
                .list_sessions(&SessionListParams {
                    cursor,
                    limit: Some(200),
                    workspace_root: None,
                    updated_after: None,
                    filter: None,
                })
                .await
                .with_context(|| format!("Muse session/list page {page_index}"))?;
            cursor = append_page(page, page_index, &mut sessions, &mut seen_cursors)?;
            if cursor.is_none() {
                sessions.sort();
                sessions.dedup();
                return Ok(sessions);
            }
        }
        bail!("Muse session/list exceeded {MAX_PAGES} pages")
    }

    pub(crate) async fn shutdown(mut self) -> Result<()> {
        tokio::time::timeout(SHUTDOWN_TIMEOUT, self.connection.close())
            .await
            .context("close Muse MSP connection timed out")?;
        tokio::time::timeout(SHUTDOWN_TIMEOUT, &mut self.driver)
            .await
            .context("Muse MSP driver did not stop")??;
        let status = match tokio::time::timeout(SHUTDOWN_TIMEOUT, self.host.wait()).await {
            Ok(status) => status.context("wait for Muse MSP host")?,
            Err(_) => {
                self.host.start_kill().context("stop Muse MSP host")?;
                self.host.wait().await.context("reap Muse MSP host")?;
                bail!("Muse MSP host did not exit after stdin closed")
            }
        };
        let diagnostics = self
            .diagnostics
            .as_mut()
            .context("Muse MSP diagnostics missing")?;
        let stderr = tokio::time::timeout(SHUTDOWN_TIMEOUT, diagnostics)
            .await
            .context("collect Muse MSP diagnostics timed out")???;
        if !status.success() {
            bail!("Muse MSP session listing failed: {}", stderr.trim());
        }
        Ok(())
    }
}

/// The SDK's low-level transport is object-valued. Confine that representation
/// here: neither callers nor pagination can inspect untyped protocol values.
async fn call<M: Method>(
    connection: &Connection,
    params: &M::Params,
    timeout: Duration,
) -> Result<M::Response> {
    let serde_json::Value::Object(params) = serde_json::to_value(params)? else {
        bail!("MSP {} parameters must serialize as an object", M::NAME)
    };
    let response = tokio::time::timeout(timeout, connection.request(M::NAME, Some(params)))
        .await
        .with_context(|| format!("Muse MSP {} timed out", M::NAME))?
        .with_context(|| format!("Muse MSP {} failed", M::NAME))?;
    serde_json::from_value(serde_json::Value::Object(response))
        .with_context(|| format!("decode Muse MSP {} response", M::NAME))
}

fn append_page(
    page: SessionListResult,
    index: usize,
    sessions: &mut Vec<String>,
    seen_cursors: &mut HashSet<String>,
) -> Result<Option<String>> {
    for session in page.sessions {
        crate::transcript_import::validate_session_id(&session.session_id)
            .with_context(|| format!("invalid Muse session id on session/list page {index}"))?;
        sessions.push(session.session_id);
    }
    if let Some(cursor) = &page.next_cursor {
        if !seen_cursors.insert(cursor.clone()) {
            bail!("Muse session/list repeated cursor on page {index}")
        }
    }
    Ok(page.next_cursor)
}

/// Drain all bytes to prevent pipe backpressure, retaining bounded diagnostics.
/// Also used by the independent `muse export` subprocess.
pub(crate) async fn drain_stderr(mut stderr: tokio::process::ChildStderr) -> Result<String> {
    let mut output = Vec::new();
    let mut buffer = [0u8; 4096];
    loop {
        let count = stderr.read(&mut buffer).await?;
        if count == 0 {
            break;
        }
        let remaining = DIAGNOSTIC_BYTES.saturating_sub(output.len());
        output.extend_from_slice(&buffer[..count.min(remaining)]);
    }
    Ok(String::from_utf8_lossy(&output).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use muse_code_sdk::DuplexTransport;
    use serde_json::{json, Value};
    use std::future::Future;
    use std::pin::Pin;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream, ReadHalf, WriteHalf};

    struct TestTransport {
        reader: BufReader<ReadHalf<DuplexStream>>,
        writer: WriteHalf<DuplexStream>,
    }
    impl DuplexTransport for TestTransport {
        fn read_chunk(
            &mut self,
        ) -> Pin<Box<dyn Future<Output = std::io::Result<Option<String>>> + Send + '_>> {
            Box::pin(async move {
                let bytes = self.reader.fill_buf().await?;
                if bytes.is_empty() {
                    return Ok(None);
                }
                let chunk = String::from_utf8(bytes.to_vec()).map_err(std::io::Error::other)?;
                let len = bytes.len();
                self.reader.consume(len);
                Ok(Some(chunk))
            })
        }
        fn write<'a>(
            &'a mut self,
            frame: &str,
        ) -> Pin<Box<dyn Future<Output = std::io::Result<()>> + Send + 'a>> {
            let bytes = frame.as_bytes().to_vec();
            Box::pin(async move { self.writer.write_all(&bytes).await })
        }
    }
    fn params() -> SessionListParams {
        SessionListParams {
            cursor: None,
            limit: Some(200),
            workspace_root: None,
            updated_after: None,
            filter: None,
        }
    }
    fn session(id: &str) -> Value {
        json!({"sessionId":id,"path":"/sessions/example","status":"notLoaded","turnCount":0,"activeTurnId":null,"createdAt":"2026-10-23T00:00:00Z","updatedAt":"2026-10-23T00:00:00Z"})
    }
    async fn exchange(result: Value, is_error: bool) -> Result<SessionListResult> {
        let (client, server) = tokio::io::duplex(8192);
        let (read, write) = tokio::io::split(client);
        let (connection, driver) = Connection::new(TestTransport {
            reader: BufReader::new(read),
            writer: write,
        });
        let driver = tokio::spawn(driver.run());
        let server = tokio::spawn(async move {
            let (read, mut write) = tokio::io::split(server);
            let request = BufReader::new(read)
                .lines()
                .next_line()
                .await
                .unwrap()
                .unwrap();
            let request: Value = serde_json::from_str(&request).unwrap();
            assert_eq!(request["method"], "session/list");
            assert_eq!(request["params"], json!({"limit":200}));
            let response = if is_error {
                json!({"jsonrpc":"2.0","id":request["id"],"error":result})
            } else {
                json!({"jsonrpc":"2.0","id":request["id"],"result":result})
            };
            write
                .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"progress\",\"params\":{}}\n")
                .await
                .unwrap();
            write
                .write_all(format!("{response}\n").as_bytes())
                .await
                .unwrap();
        });
        let result = call::<ListSessions>(&connection, &params(), Duration::from_secs(1)).await;
        connection.close().await;
        driver.await.unwrap();
        server.await.unwrap();
        result
    }
    #[tokio::test]
    async fn typed_response_and_errors_through_sdk_connection() {
        let page = exchange(json!({"sessions":[session("a")],"nextCursor":null}), false)
            .await
            .unwrap();
        assert_eq!(page.sessions[0].session_id, "a");
        assert!(exchange(json!({"sessions":[{"sessionId":"a"}]}), false)
            .await
            .unwrap_err()
            .to_string()
            .contains("decode Muse MSP session/list"));
        assert!(
            exchange(json!({"code":-32601,"message":"unsupported"}), true)
                .await
                .unwrap_err()
                .to_string()
                .contains("session/list failed")
        );
    }
    #[test]
    fn pagination_validates_ids_and_rejects_cycles() {
        let page = |id: &str| {
            serde_json::from_value::<SessionListResult>(
                json!({"sessions":[session(id)],"nextCursor":"next"}),
            )
            .unwrap()
        };
        let mut ids = Vec::new();
        let mut cursors = HashSet::new();
        assert_eq!(
            append_page(page("session-a"), 1, &mut ids, &mut cursors).unwrap(),
            Some("next".into())
        );
        assert_eq!(ids, ["session-a"]);
        assert!(append_page(page("session-b"), 2, &mut ids, &mut cursors)
            .unwrap_err()
            .to_string()
            .contains("repeated cursor"));
        assert!(append_page(page("../escape"), 3, &mut ids, &mut HashSet::new()).is_err());
    }
    #[tokio::test]
    async fn timeout_is_bounded_when_host_does_not_answer() {
        let (client, _server) = tokio::io::duplex(8192);
        let (read, write) = tokio::io::split(client);
        let (connection, driver) = Connection::new(TestTransport {
            reader: BufReader::new(read),
            writer: write,
        });
        let driver = tokio::spawn(driver.run());
        let result = call::<ListSessions>(&connection, &params(), Duration::from_millis(10)).await;
        assert!(result.unwrap_err().to_string().contains("timed out"));
        connection.close().await;
        driver.await.unwrap();
    }
}
