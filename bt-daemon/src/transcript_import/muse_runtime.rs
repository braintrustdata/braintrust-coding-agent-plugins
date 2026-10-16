//! Muse exports are streamed through the shared import processor.
use super::*;
use anyhow::Context;
use serde::Deserialize;
use std::ffi::OsString;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, Lines,
};

const MUSE_MSP_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const MUSE_MSP_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const MUSE_MSP_MAX_PAGES: usize = 1_000;
const MUSE_MSP_DIAGNOSTIC_BYTES: usize = 64 * 1024;

pub(super) async fn run(
    args: ImportArgs,
    opts: ServeOptions,
    config: Option<SessionConfig>,
    ledger: Option<PathBuf>,
) -> anyhow::Result<Vec<ImportSummary>> {
    if args.attach {
        return attach_muse_session(&args.session_ids[0], opts, config, ledger).await;
    }
    let ids = if args.all {
        muse_msp_session_ids().await?
    } else {
        args.session_ids
    };
    if ids.is_empty() {
        anyhow::bail!("no durable Muse sessions found to import");
    }
    import_muse_sessions_with_ledger(&ids, opts, config, ledger).await
}

async fn export_muse_session(session_id: &str, path: &std::path::Path) -> anyhow::Result<()> {
    transcript_import::validate_session_id(session_id)?;
    let executable = std::env::var_os("MUSE_BIN").unwrap_or_else(|| OsString::from("muse"));
    let mut command: tokio::process::Command =
        crate::subprocess::background_command(&executable).into();
    let mut child = command
        .kill_on_drop(true)
        .args(["export", "--session", session_id, "--out"])
        .arg(path)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| {
            format!(
                "run {} export for Muse session {session_id}",
                executable.to_string_lossy()
            )
        })?;
    let stderr = child
        .stderr
        .take()
        .context("Muse export stderr was not piped")?;
    let diagnostics = tokio::spawn(muse_drain_stderr(stderr));
    let status = tokio::time::timeout(Duration::from_secs(300), child.wait())
        .await
        .with_context(|| format!("Muse export for session {session_id} timed out"))?
        .with_context(|| format!("wait for Muse export of session {session_id}"))?;
    let stderr = diagnostics
        .await
        .context("collect Muse export diagnostics")??;
    if !status.success() {
        anyhow::bail!(
            "Muse export for session {session_id} failed: {}",
            stderr.trim()
        );
    }
    Ok(())
}

/// List durable Muse sessions through the read-only MSP surface. This never
/// resumes a session, obtains a writer lease, or subscribes to its view; the
/// export command remains the authoritative content reader.
async fn muse_msp_session_ids() -> anyhow::Result<Vec<String>> {
    let executable = std::env::var_os("MUSE_BIN").unwrap_or_else(|| OsString::from("muse"));
    let mut command: tokio::process::Command =
        crate::subprocess::background_command(&executable).into();
    let mut child = command
        .kill_on_drop(true)
        .arg("serve")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| {
            format!(
                "run {} serve to list Muse sessions",
                executable.to_string_lossy()
            )
        })?;
    let mut stdin = child.stdin.take().context("Muse MSP stdin was not piped")?;
    let stdout = child
        .stdout
        .take()
        .context("Muse MSP stdout was not piped")?;
    let stderr = child
        .stderr
        .take()
        .context("Muse MSP stderr was not piped")?;
    let diagnostics = tokio::spawn(muse_drain_stderr(stderr));
    let mut reader = BufReader::new(stdout).lines();
    let listing = async {
        let mut request_id = 1u64;
        muse_msp_request(
            &mut stdin,
            &mut reader,
            request_id,
            "initialize",
            serde_json::json!({"clientInfo":{"name":"braintrust_muse_import","version":"1"}}),
        )
        .await?;
        // Muse requires the initialized notification before other requests.
        muse_msp_notify(&mut stdin, "initialized").await?;
        let mut cursor = None;
        let mut seen_cursors = std::collections::HashSet::new();
        let mut sessions = Vec::new();
        for page_index in 0..MUSE_MSP_MAX_PAGES {
            request_id += 1;
            let result = muse_msp_request(
                &mut stdin,
                &mut reader,
                request_id,
                "session/list",
                serde_json::json!({"cursor": cursor, "limit": 200}),
            )
            .await?;
            match muse_msp_append_page(result, page_index + 1, &mut sessions, &mut seen_cursors)? {
                None => {
                    sessions.sort();
                    sessions.dedup();
                    return Ok(sessions);
                }
                Some(next) => cursor = Some(next),
            }
        }
        anyhow::bail!("Muse session/list exceeded {MUSE_MSP_MAX_PAGES} pages")
    }
    .await;
    drop(stdin);
    drop(reader);
    let status = match tokio::time::timeout(MUSE_MSP_SHUTDOWN_TIMEOUT, child.wait()).await {
        Ok(status) => status.context("wait for Muse MSP host")?,
        Err(_) => {
            child
                .start_kill()
                .context("stop unresponsive Muse MSP host")?;
            child
                .wait()
                .await
                .context("reap unresponsive Muse MSP host")?;
            anyhow::bail!("Muse MSP host did not exit after stdin closed")
        }
    };
    let stderr = diagnostics
        .await
        .context("collect Muse MSP diagnostics")??;
    let sessions = listing?;
    if !status.success() {
        anyhow::bail!("Muse MSP session listing failed: {}", stderr.trim());
    }
    Ok(sessions)
}

#[derive(Deserialize)]
struct MuseMspSessionPage {
    sessions: Vec<MuseMspSessionEntry>,
    #[serde(rename = "nextCursor")]
    next_cursor: Option<String>,
}

#[derive(Deserialize)]
struct MuseMspSessionEntry {
    #[serde(rename = "sessionId", alias = "session_id")]
    session_id: String,
}

fn muse_msp_append_page(
    result: serde_json::Value,
    page_index: usize,
    sessions: &mut Vec<String>,
    seen_cursors: &mut std::collections::HashSet<String>,
) -> anyhow::Result<Option<String>> {
    let page: MuseMspSessionPage = serde_json::from_value(result)
        .with_context(|| format!("decode Muse session/list page {page_index}"))?;
    for session in page.sessions {
        transcript_import::validate_session_id(&session.session_id).with_context(|| {
            format!("invalid Muse session id on session/list page {page_index}")
        })?;
        sessions.push(session.session_id);
    }
    if let Some(cursor) = &page.next_cursor {
        if !seen_cursors.insert(cursor.clone()) {
            anyhow::bail!("Muse session/list repeated cursor on page {page_index}")
        }
    }
    Ok(page.next_cursor)
}

async fn muse_drain_stderr(mut stderr: tokio::process::ChildStderr) -> anyhow::Result<String> {
    let mut output = Vec::new();
    let mut buffer = [0u8; 4096];
    loop {
        let count = stderr.read(&mut buffer).await?;
        if count == 0 {
            break;
        }
        let remaining = MUSE_MSP_DIAGNOSTIC_BYTES.saturating_sub(output.len());
        output.extend_from_slice(&buffer[..count.min(remaining)]);
    }
    Ok(String::from_utf8_lossy(&output).into_owned())
}

async fn muse_msp_notify<W>(stdin: &mut W, method: &str) -> anyhow::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut frame = serde_json::to_string(&serde_json::json!({"jsonrpc":"2.0", "method":method}))?;
    frame.push('\n');
    tokio::time::timeout(MUSE_MSP_REQUEST_TIMEOUT, async {
        stdin.write_all(frame.as_bytes()).await?;
        stdin.flush().await?;
        Ok(())
    })
    .await
    .map_err(|_| anyhow::anyhow!("Muse MSP timed out sending {method}"))?
}

async fn muse_msp_request<R, W>(
    stdin: &mut W,
    reader: &mut Lines<R>,
    id: u64,
    method: &str,
    params: serde_json::Value,
) -> anyhow::Result<serde_json::Value>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut request = serde_json::to_string(
        &serde_json::json!({"jsonrpc":"2.0", "id":id, "method":method, "params":params}),
    )?;
    request.push('\n');
    tokio::time::timeout(MUSE_MSP_REQUEST_TIMEOUT, async {
        stdin.write_all(request.as_bytes()).await?;
        stdin.flush().await?;
        loop {
            let line = reader
                .next_line()
                .await?
                .ok_or_else(|| anyhow::anyhow!("Muse MSP closed before responding to {method}"))?;
            let frame: serde_json::Value = serde_json::from_str(&line)
                .with_context(|| format!("parse Muse MSP frame while waiting for {method}"))?;
            if frame.get("jsonrpc").and_then(serde_json::Value::as_str) != Some("2.0") {
                anyhow::bail!(
                    "Muse MSP sent a frame without JSON-RPC 2.0 while waiting for {method}"
                )
            }
            let Some(response_id) = frame.get("id") else {
                // Notifications can be interleaved with responses.
                continue;
            };
            if response_id.as_u64() != Some(id) {
                anyhow::bail!("Muse MSP returned an unexpected response id for {method}")
            }
            if let Some(error) = frame.get("error") {
                anyhow::bail!("Muse MSP {method} failed: {error}")
            }
            return frame
                .get("result")
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("Muse MSP {method} response has no result"));
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("Muse MSP timed out waiting for {method}"))?
}

pub(super) async fn import_muse_sessions_with_ledger(
    session_ids: &[String],
    opts: ServeOptions,
    config: Option<SessionConfig>,
    ledger_dir: Option<PathBuf>,
) -> anyhow::Result<Vec<ImportSummary>> {
    let mut processor = ImportProcessor::new(opts, config, ledger_dir)?;
    let mut summaries = Vec::new();
    for session_id in session_ids {
        let directory = tempfile::Builder::new()
            .prefix("bt-muse-import-")
            .tempdir()?;
        let export = directory.path().join("session.json");
        export_muse_session(session_id, &export).await?;
        let ended = process_muse_export(&mut processor, &export, session_id).await?;
        let summary = if ended {
            processor.finish_session(session_id).await?
        } else {
            processor.detach_session(session_id).await?
        };
        if let Some(summary) = summary {
            summaries.push(summary);
        }
    }
    summaries.extend(processor.finish().await?);
    Ok(summaries)
}

pub(super) async fn process_muse_export(
    processor: &mut ImportProcessor,
    export: &std::path::Path,
    session_id: &str,
) -> anyhow::Result<bool> {
    process_muse_snapshot(processor, export, session_id, true, 0)
        .await
        .map(|(_, ended)| ended)
}

/// The file is fully parsed on every poll. Only envelopes beyond the previous
/// snapshot are sent to the long-lived translator and sink, so open model and
/// turn state is retained without replaying old starts into it.
pub(super) async fn process_muse_snapshot(
    processor: &mut ImportProcessor,
    export: &std::path::Path,
    session_id: &str,
    attach_snapshot: bool,
    skip_envelopes: usize,
) -> anyhow::Result<(usize, bool)> {
    let (sender, mut receiver) = tokio::sync::mpsc::channel::<Vec<Envelope>>(4);
    let export_for_parser = export.to_path_buf();
    let expected_id = session_id.to_owned();
    let parser = tokio::task::spawn_blocking(move || {
        let mut batch = Vec::with_capacity(128);
        let mut envelope_count = 0usize;
        let ended = transcript_import::muse::for_each_snapshot_envelope_for_session(
            &export_for_parser,
            &expected_id,
            attach_snapshot,
            |entry| {
                envelope_count += 1;
                if envelope_count <= skip_envelopes {
                    return Ok(());
                }
                batch.push(entry);
                if batch.len() == 128 {
                    sender
                        .blocking_send(std::mem::take(&mut batch))
                        .map_err(|_| {
                            anyhow::anyhow!("Muse import stopped while parsing {expected_id}")
                        })?;
                }
                Ok(())
            },
        )?;
        if envelope_count < skip_envelopes {
            anyhow::bail!(
                "Muse export for {expected_id} shrank from {skip_envelopes} to {envelope_count} trace events"
            );
        }
        if !batch.is_empty() {
            sender
                .blocking_send(batch)
                .map_err(|_| anyhow::anyhow!("Muse import stopped while parsing {expected_id}"))?;
        }
        Ok::<(usize, bool), anyhow::Error>((envelope_count, ended))
    });
    let mut processing_error = None;
    while let Some(batch) = receiver.recv().await {
        if let Err(error) = processor.process(batch).await {
            processing_error = Some(error);
            break;
        }
    }
    drop(receiver);
    let parse_result = parser.await.context("Muse export parser task failed")?;
    if let Some(error) = processing_error {
        return Err(error);
    }
    parse_result
}

pub(super) async fn attach_muse_session(
    session_id: &str,
    opts: ServeOptions,
    config: Option<SessionConfig>,
    ledger_dir: Option<PathBuf>,
) -> anyhow::Result<Vec<ImportSummary>> {
    transcript_import::validate_session_id(session_id)?;
    let directory = tempfile::Builder::new()
        .prefix("bt-muse-attach-")
        .tempdir()?;
    let export = directory.path().join("session.json");
    attach_muse_export(&export, session_id, true, opts, config, ledger_dir).await
}

pub(super) async fn attach_muse_export(
    export: &std::path::Path,
    session_id: &str,
    reexport: bool,
    opts: ServeOptions,
    config: Option<SessionConfig>,
    ledger_dir: Option<PathBuf>,
) -> anyhow::Result<Vec<ImportSummary>> {
    let mut processor = ImportProcessor::new(opts, config, ledger_dir)?;
    let mut emitted = 0usize;
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);
    loop {
        if reexport {
            export_muse_session(session_id, export).await?;
        }
        let (count, ended) =
            process_muse_snapshot(&mut processor, export, session_id, true, emitted).await?;
        if count != emitted {
            processor.flush_open().await?;
        }
        emitted = count;
        if ended {
            return processor.finish().await;
        }
        tokio::select! {
            result = &mut shutdown => {
                result?;
                break;
            }
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
    }
    // Capture one final complete snapshot before interrupting any still-open
    // spans, matching the finalization behavior of other attach sources.
    if reexport {
        export_muse_session(session_id, export).await?;
    }
    process_muse_snapshot(&mut processor, export, session_id, true, emitted).await?;
    processor.finish().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[tokio::test]
    async fn muse_msp_handshake_writes_notification_and_skips_notifications() {
        let (client, server) = tokio::io::duplex(4_096);
        let (client_reader, mut client_writer) = tokio::io::split(client);
        let (server_reader, mut server_writer) = tokio::io::split(server);
        let server = tokio::spawn(async move {
            let mut incoming = BufReader::new(server_reader).lines();
            let initialized = incoming.next_line().await.unwrap().unwrap();
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&initialized).unwrap(),
                json!({"jsonrpc": "2.0", "method": "initialized"})
            );
            let request = incoming.next_line().await.unwrap().unwrap();
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&request).unwrap()["id"],
                7
            );
            server_writer
                .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"session/changed\"}\n{\"jsonrpc\":\"2.0\",\"id\":7,\"result\":{\"sessions\":[]}}\n")
                .await
                .unwrap();
        });

        let mut reader = BufReader::new(client_reader).lines();
        muse_msp_notify(&mut client_writer, "initialized")
            .await
            .unwrap();
        let result = muse_msp_request(
            &mut client_writer,
            &mut reader,
            7,
            "session/list",
            json!({"cursor": null, "limit": 200}),
        )
        .await
        .unwrap();
        assert_eq!(result, json!({"sessions": []}));
        server.await.unwrap();
    }

    #[test]
    fn muse_msp_pages_reject_partial_or_cyclic_session_lists() {
        let mut sessions = Vec::new();
        let mut cursors = std::collections::HashSet::new();
        assert_eq!(
            muse_msp_append_page(
                json!({"sessions":[{"sessionId":"session-a"}],"nextCursor":"next"}),
                1,
                &mut sessions,
                &mut cursors,
            )
            .unwrap(),
            Some("next".to_owned())
        );
        assert_eq!(sessions, ["session-a"]);
        assert!(muse_msp_append_page(
            json!({"sessions":[],"nextCursor":"next"}),
            2,
            &mut sessions,
            &mut cursors,
        )
        .unwrap_err()
        .to_string()
        .contains("repeated cursor"));
        assert!(muse_msp_append_page(
            json!({"sessions":[{"status":"notLoaded"}]}),
            2,
            &mut sessions,
            &mut cursors,
        )
        .unwrap_err()
        .to_string()
        .contains("decode Muse session/list page 2"));
    }

    #[tokio::test]
    async fn muse_msp_request_accepts_notifications_but_rejects_wrong_response_ids() {
        async fn exchange(response: &str) -> anyhow::Result<serde_json::Value> {
            let (client, server) = tokio::io::duplex(4096);
            let (client_read, mut client_write) = tokio::io::split(client);
            let (server_read, mut server_write) = tokio::io::split(server);
            let response = response.to_owned();
            let server = tokio::spawn(async move {
                let request = BufReader::new(server_read)
                    .lines()
                    .next_line()
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    serde_json::from_str::<serde_json::Value>(&request).unwrap()["id"],
                    7
                );
                server_write.write_all(response.as_bytes()).await.unwrap();
            });
            let result = muse_msp_request(
                &mut client_write,
                &mut BufReader::new(client_read).lines(),
                7,
                "session/list",
                json!({}),
            )
            .await;
            server.await.unwrap();
            result
        }

        let result = exchange(
            "{\"jsonrpc\":\"2.0\",\"method\":\"progress\"}\n{\"jsonrpc\":\"2.0\",\"id\":7,\"result\":{\"sessions\":[]}}\n",
        )
        .await
        .unwrap();
        assert_eq!(result, json!({"sessions": []}));
        assert!(exchange("{\"jsonrpc\":\"2.0\",\"id\":8,\"result\":{}}\n")
            .await
            .unwrap_err()
            .to_string()
            .contains("unexpected response id"));
    }
}
