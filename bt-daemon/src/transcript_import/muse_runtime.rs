//! Muse exports are streamed through the shared import processor.
use super::*;
use crate::muse::msp::{drain_stderr, MuseMspClient};
use crate::transcript_import::muse::ReadMode;
use anyhow::Context;
use std::ffi::OsString;
use std::process::Stdio;
use std::time::Duration;
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
    let executable =
        crate::env::var_os("BRAINTRUST_MUSE_BIN").unwrap_or_else(|| OsString::from("muse"));
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
    let diagnostics = tokio::spawn(drain_stderr(stderr));
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
    let executable =
        crate::env::var_os("BRAINTRUST_MUSE_BIN").unwrap_or_else(|| OsString::from("muse"));
    let client = MuseMspClient::connect(&executable).await?;
    let listing = client.session_ids().await;
    let shutdown = client.shutdown().await;
    let sessions = listing?;
    shutdown?;
    Ok(sessions)
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
    process_muse_snapshot(processor, export, session_id, ReadMode::Import, 0)
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
    mode: ReadMode,
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
            mode,
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
            tokio::select! {
                result = &mut shutdown => {
                    result?;
                    break;
                }
                result = export_muse_session(session_id, export) => result?,
            }
        }
        let (count, ended) = process_muse_snapshot(
            &mut processor,
            export,
            session_id,
            ReadMode::Attach,
            emitted,
        )
        .await?;
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
    // Detaching stops observation, not the native session. Leave open spans
    // unfinished so a later export can deliver their authoritative completion.
    // Do not launch another potentially slow export after cancellation.
    Ok(processor
        .detach_session(session_id)
        .await?
        .into_iter()
        .collect())
}
