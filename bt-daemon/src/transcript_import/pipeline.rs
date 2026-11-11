//! Transcript import: read native transcript records and capture them with the
//! daemon, exactly as hooks do. The daemon translates, delivers and retains
//! them through the same pipeline; import only waits for delivery.

#[path = "muse_runtime.rs"]
mod muse_runtime;

use anyhow::Context;
use braintrust_sdk_rust::SpanComponents;
use std::path::PathBuf;

use crate::args::{ImportArgs, ImportSource, ParentObjectType};
use crate::client::{DaemonClient, HostInfo};
use crate::route::resolve_span_plugin_paths;
use crate::wire::{self, method, Envelope, SessionRoute};
use crate::{transcript_import, ImportSummary};

/// The daemon an import captures into, and the route its sessions deliver to.
#[derive(Debug, Clone)]
pub struct ImportTarget {
    pub socket: PathBuf,
    pub host: HostInfo,
    pub route: SessionRoute,
    /// Fail instead of starting a daemon when none is running.
    pub no_spawn: bool,
}

/// Import a native coding-agent transcript through the daemon's translators
/// and delivery routes. A daemon is started, or replaced by a newer one, as
/// hook capture does.
pub async fn run_import(
    args: ImportArgs,
    mut target: ImportTarget,
) -> anyhow::Result<Vec<ImportSummary>> {
    validate_import_selection(&args)?;
    target.route.span_plugins = resolve_span_plugin_paths(&args.plugin)?;
    if let Some(destination) = import_parent_components(&args)?
        .map(|components| wire::TraceDestination::ParentSpan { components })
        .or_else(|| args.destination.clone())
    {
        target.route.destination = Some(destination);
    }
    if args.source == ImportSource::Muse {
        return muse_runtime::run(args, target).await;
    }
    let files = transcript_import::resolve_transcripts(&args.session_ids, args.all, args.source)?;
    if args.attach {
        return import_transcript(&files[0], args.source, target, true).await;
    }
    import_transcripts(&files, args.source, target).await
}

fn validate_import_selection(args: &ImportArgs) -> anyhow::Result<()> {
    if args.all != args.session_ids.is_empty() {
        anyhow::bail!("provide explicit session ids or use --all, but not both");
    }
    if args.attach && args.session_ids.len() != 1 {
        anyhow::bail!("--attach requires exactly one session id");
    }
    import_parent_components(args)?;
    Ok(())
}

fn import_parent_components(args: &ImportArgs) -> anyhow::Result<Option<SpanComponents>> {
    if let Some(parent) = &args.parent {
        parent
            .to_parent_span_info()
            .map_err(|error| anyhow::anyhow!("invalid --parent value: {error}"))?;
        return Ok(Some(parent.clone()));
    }

    let Some(span_id) = args.parent_span_id.as_deref() else {
        return Ok(None);
    };
    let span_id = span_id.trim();
    if span_id.is_empty() {
        anyhow::bail!("--parent-span-id must not be empty");
    }
    let root_span_id = args
        .parent_root_span_id
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("--parent-span-id requires --parent-root-span-id"))?;
    let root_span_id = root_span_id.trim();
    if root_span_id.is_empty() {
        anyhow::bail!("--parent-root-span-id must not be empty");
    }
    let object_type = args
        .parent_object_type
        .ok_or_else(|| anyhow::anyhow!("--parent-span-id requires --parent-object-type"))?;

    let object_id = args
        .parent_object_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let parent_project = args
        .parent_project
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let compute_object_metadata_args = match object_type {
        ParentObjectType::ProjectLogs if object_id.is_none() => {
            let project = parent_project.ok_or_else(|| {
                anyhow::anyhow!(
                    "project_logs parent requires --parent-object-id or --parent-project"
                )
            })?;
            Some(serde_json::Map::from_iter([(
                "project_name".to_string(),
                serde_json::Value::String(project.to_string()),
            )]))
        }
        ParentObjectType::ProjectLogs => None,
        _ if parent_project.is_some() => {
            anyhow::bail!("--parent-project is only valid with --parent-object-type project_logs")
        }
        _ if object_id.is_none() => {
            anyhow::bail!("this parent object type requires --parent-object-id")
        }
        _ => None,
    };

    let components = SpanComponents {
        object_type: object_type.into(),
        object_id,
        compute_object_metadata_args,
        row_id: None,
        span_id: Some(span_id.to_string()),
        root_span_id: Some(root_span_id.to_string()),
        span_parents: None,
        propagated_event: None,
    };
    components
        .to_parent_span_info()
        .map_err(|error| anyhow::anyhow!("invalid parent identifiers: {error}"))?;
    Ok(Some(components))
}

/// Import a native transcript from a known path. Front-ends should normally
/// expose [`run_import`] so users only need the agent's session id.
pub async fn import_transcript(
    file: &std::path::Path,
    source: ImportSource,
    target: ImportTarget,
    attach: bool,
) -> anyhow::Result<Vec<ImportSummary>> {
    if source == ImportSource::Muse {
        let session_id = transcript_import::muse::session_id(file)?;
        if attach {
            return muse_runtime::attach_muse_export(file, &session_id, false, target).await;
        }
        let mut processor = ImportProcessor::new(target);
        let ended = muse_runtime::process_muse_export(&mut processor, file, &session_id).await?;
        let summary = if ended {
            processor.finish_session(&session_id).await?
        } else {
            processor.detach_session(&session_id).await?
        };
        return Ok(summary.into_iter().collect());
    }
    let mut tail = transcript_import::TranscriptTail::new(file.to_path_buf(), source);
    if attach {
        tail.allow_incomplete_final_record_on_shutdown();
    }
    let mut processor = ImportProcessor::new(target);
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);
    let mut finalizing = !attach;
    loop {
        let entries = tail.poll(finalizing)?;
        if tail.take_translator_reset() {
            processor.reset_translators().await?;
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

/// Import multiple completed native transcripts over one daemon connection.
/// Each native session keeps its own source pipeline in the daemon.
pub async fn import_transcripts(
    files: &[PathBuf],
    source: ImportSource,
    target: ImportTarget,
) -> anyhow::Result<Vec<ImportSummary>> {
    let mut processor = ImportProcessor::new(target);
    let mut summaries = Vec::new();
    for file in files {
        if source == ImportSource::Muse {
            let session_id = transcript_import::muse::session_id(file)?;
            let ended =
                muse_runtime::process_muse_export(&mut processor, file, &session_id).await?;
            let summary = if ended {
                processor.finish_session(&session_id).await?
            } else {
                processor.detach_session(&session_id).await?
            };
            summaries.extend(summary);
            continue;
        }
        let mut tail = transcript_import::TranscriptTail::new(file.clone(), source);
        let entries = tail
            .poll(true)
            .with_context(|| format!("import transcript {}", file.display()))?;
        let session_ids = entries
            .iter()
            .map(|entry| entry.session_id.clone())
            .collect::<std::collections::BTreeSet<_>>();
        processor.process(entries).await?;
        for session_id in session_ids {
            if let Some(summary) = processor.finish_session(&session_id).await? {
                summaries.push(summary);
            }
        }
    }
    summaries.extend(processor.finish().await?);
    Ok(summaries)
}

/// How long the daemon may take to deliver one imported session.
const FINISH_TIMEOUT_MS: u64 = 120_000;

/// Captures native records for the sessions an import observes.
struct ImportProcessor {
    client: DaemonClient,
    route: SessionRoute,
    /// Session id to source, for each session with captured records.
    sessions: std::collections::BTreeMap<String, String>,
}

impl ImportProcessor {
    fn new(target: ImportTarget) -> Self {
        Self {
            client: DaemonClient::new(&target.socket, &target.host, target.no_spawn),
            route: target.route,
            sessions: std::collections::BTreeMap::new(),
        }
    }

    async fn process(&mut self, entries: Vec<Envelope>) -> anyhow::Result<()> {
        for mut env in entries {
            env.route = Some(self.route.clone());
            env.config = None;
            self.client.log(&env).await?;
            self.sessions
                .entry(env.session_id.clone())
                .or_insert(env.source);
        }
        Ok(())
    }

    /// The transcript was rewritten: replay it through fresh translator state.
    async fn reset_translators(&mut self) -> anyhow::Result<()> {
        for (session_id, source) in &self.sessions {
            self.client
                .call::<serde_json::Value>(
                    method::IMPORT_RESET,
                    wire::ImportResetParams {
                        source: source.clone(),
                        session_id: session_id.clone(),
                    },
                )
                .await?;
        }
        Ok(())
    }

    async fn complete(
        &mut self,
        session_id: &str,
        source: &str,
        finalize: bool,
    ) -> anyhow::Result<ImportSummary> {
        let result: wire::ImportFinishResult = self
            .client
            .call(
                method::IMPORT_FINISH,
                wire::ImportFinishParams {
                    source: source.to_owned(),
                    session_id: session_id.to_owned(),
                    route: self.route.clone(),
                    finalize,
                    timeout_ms: FINISH_TIMEOUT_MS,
                },
            )
            .await?;
        anyhow::ensure!(
            result.flushed,
            "imported session {session_id} still has {} undelivered span operations; \
             the daemon retains them and will retry delivery",
            result.pending
        );
        Ok(ImportSummary {
            session_id: session_id.to_owned(),
            destination: self.route.destination.clone(),
            root_span_id: result.root_span_id,
            span_count: result.span_count as usize,
            finalized: finalize,
        })
    }

    async fn finish(mut self) -> anyhow::Result<Vec<ImportSummary>> {
        let mut summaries = Vec::new();
        for (session_id, source) in std::mem::take(&mut self.sessions) {
            summaries.push(self.complete(&session_id, &source, true).await?);
        }
        Ok(summaries)
    }

    async fn finish_session(&mut self, session_id: &str) -> anyhow::Result<Option<ImportSummary>> {
        let Some(source) = self.sessions.remove(session_id) else {
            return Ok(None);
        };
        self.complete(session_id, &source, true).await.map(Some)
    }

    /// Deliver what an attached import has observed so far.
    async fn flush_open(&mut self) -> anyhow::Result<()> {
        for (session_id, source) in self.sessions.clone() {
            self.complete(&session_id, &source, false).await?;
        }
        Ok(())
    }

    /// Stop observing without closing the native session's open spans.
    async fn detach_session(&mut self, session_id: &str) -> anyhow::Result<Option<ImportSummary>> {
        let Some(source) = self.sessions.remove(session_id) else {
            return Ok(None);
        };
        self.complete(session_id, &source, false).await.map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::muse_runtime::*;
    use super::*;
    use crate::ids;
    use crate::wire::SessionConfig;
    use braintrust_sdk_rust::SpanObjectType;
    use clap::Parser;
    use serde_json::json;
    #[allow(dead_code)]
    mod import_daemon {
        use crate as bt_daemon;
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/support/import_daemon.rs"
        ));
    }

    #[tokio::test]
    async fn muse_attach_snapshots_reuse_one_trace_and_keep_open_spans() {
        let temp = tempfile::tempdir().unwrap();
        let export = temp.path().join("export.json");
        let output = temp.path().join("spans");
        let mut processor = ImportProcessor::new(import_daemon::target(&output, None).await);
        let mut events = vec![
            json!({"recorded_at":1_000_000,"envelope":{"payload":{"kind":"run","run_id":"run-1","event":{"kind":"started","prompt":"hello"}}}}),
            json!({"recorded_at":1_001_000,"envelope":{"payload":{"kind":"run","run_id":"run-1","event":{"kind":"model_input_trace_recorded","request_record_id":"request-1"}}}}),
        ];
        let write = |events: &[serde_json::Value]| {
            std::fs::write(
                &export,
                serde_json::to_vec(&json!({
                    "export_schema_version":1,
                    "sessions":[{"session_id":"muse-attach"}],
                    "events":events,
                }))
                .unwrap(),
            )
            .unwrap();
        };
        write(&events);
        let (count, ended) = process_muse_snapshot(
            &mut processor,
            &export,
            "muse-attach",
            crate::transcript_import::muse::ReadMode::Attach,
            0,
        )
        .await
        .unwrap();
        assert_eq!(count, 3);
        assert!(!ended);
        processor.flush_open().await.unwrap();
        let (same_count, ended) = process_muse_snapshot(
            &mut processor,
            &export,
            "muse-attach",
            crate::transcript_import::muse::ReadMode::Attach,
            count,
        )
        .await
        .unwrap();
        assert_eq!(same_count, count);
        assert!(!ended);
        events.extend([
            json!({"recorded_at":1_002_000,"envelope":{"payload":{"kind":"run","run_id":"run-1","event":{"kind":"model_completed","usage":{"input_tokens":2}}}}}),
            json!({"recorded_at":1_003_000,"envelope":{"payload":{"kind":"run","run_id":"run-1","event":{"kind":"assistant_message_committed","text":"done"}}}}),
            json!({"recorded_at":1_004_000,"envelope":{"payload":{"kind":"run","run_id":"run-1","event":{"kind":"terminal","terminal":"completed"}}}}),
            json!({"recorded_at":1_005_000,"envelope":{"payload":{"kind":"session_end"}}}),
        ]);
        write(&events);
        let (_, ended) = process_muse_snapshot(
            &mut processor,
            &export,
            "muse-attach",
            crate::transcript_import::muse::ReadMode::Attach,
            count,
        )
        .await
        .unwrap();
        assert!(ended);
        processor.finish().await.unwrap();
        let rows = std::fs::read_to_string(output.join("muse-attach.ndjson"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        for identity in ["root", "turn:run-1", "llm:request-1"] {
            let id = ids::span_id(&ids::session_namespace("muse", "muse-attach"), identity);
            assert_eq!(
                rows.iter()
                    .filter(|row| row
                        .pointer("/Insert/span_id")
                        .and_then(|value| value.as_str())
                        == Some(id.as_str()))
                    .count(),
                1,
                "duplicate insert for {identity}"
            );
            assert_eq!(
                rows.iter()
                    .filter(|row| row
                        .pointer("/Merge/span_id")
                        .and_then(|value| value.as_str())
                        == Some(id.as_str())
                        && row.pointer("/Merge/end_ms").is_some())
                    .count(),
                1,
                "missing or duplicate completion for {identity}"
            );
        }
    }

    #[tokio::test]
    async fn repeated_muse_import_does_not_emit_a_second_trace() {
        let temp = tempfile::tempdir().unwrap();
        let export = temp.path().join("muse-export.json");
        std::fs::write(
            &export,
            serde_json::to_vec(&json!({
                "export_schema_version":1,
                "sessions":[{"session_id":"muse-ledger"}],
                "events":[
                    {"recorded_at":1_000_000,"envelope":{"payload":{"kind":"run","run_id":"run-1","event":{"kind":"started","prompt":"hello"}}}},
                    {"recorded_at":1_001_000,"envelope":{"payload":{"kind":"run","run_id":"run-1","event":{"kind":"terminal","terminal":"completed"}}}},
                    {"recorded_at":1_002_000,"envelope":{"payload":{"kind":"session_end"}}}
                ]
            }))
            .unwrap(),
        )
        .unwrap();
        let output = temp.path().join("output");
        let target =
            import_daemon::target(&output.join("spans"), Some(import_test_config("project-a")))
                .await;
        let import = || import_transcript(&export, ImportSource::Muse, target.clone(), false);
        import().await.unwrap();
        let rows = output.join("spans/muse-ledger.ndjson");
        let first = std::fs::read_to_string(&rows).unwrap();
        assert!(first.contains("Muse Code"));
        import().await.unwrap();
        assert_eq!(std::fs::read_to_string(&rows).unwrap(), first);
    }

    #[tokio::test]
    async fn later_muse_import_completes_an_active_snapshot_on_the_same_root() {
        let temp = tempfile::tempdir().unwrap();
        let export = temp.path().join("muse-export.json");
        let output = temp.path().join("output");
        let started = json!({"recorded_at":1_000_000,"envelope":{"payload":{"kind":"run","run_id":"run-1","event":{"kind":"started","prompt":"hello"}}}});
        let write = |events: Vec<serde_json::Value>| {
            std::fs::write(
                &export,
                serde_json::to_vec(&json!({
                    "export_schema_version":1,
                    "sessions":[{"session_id":"muse-active-import"}],
                    "events":events,
                }))
                .unwrap(),
            )
            .unwrap();
        };
        let target =
            import_daemon::target(&output.join("spans"), Some(import_test_config("project-a")))
                .await;
        let import = || import_transcript(&export, ImportSource::Muse, target.clone(), false);
        write(vec![started.clone()]);
        let summaries = import().await.unwrap();
        assert!(!summaries[0].finalized);
        let rows_path = output.join("spans/muse-active-import.ndjson");
        let first = std::fs::read_to_string(&rows_path).unwrap();
        assert!(!first.contains("\"Merge\""));

        write(vec![
            started,
            json!({"recorded_at":1_001_000,"envelope":{"payload":{"kind":"run","run_id":"run-1","event":{"kind":"terminal","terminal":"completed"}}}}),
            json!({"recorded_at":1_002_000,"envelope":{"payload":{"kind":"session_end"}}}),
        ]);
        let summaries = import().await.unwrap();
        assert!(summaries[0].finalized);
        let rows = std::fs::read_to_string(&rows_path).unwrap();
        let parsed = rows
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        let root_id = ids::span_id(
            &ids::session_namespace("muse", "muse-active-import"),
            "root",
        );
        assert_eq!(
            parsed
                .iter()
                .filter(|row| row
                    .pointer("/Insert/span_id")
                    .and_then(|value| value.as_str())
                    == Some(root_id.as_str()))
                .count(),
            1,
            "the live session already holds the root; capture is not repeated"
        );
        assert_eq!(
            parsed
                .iter()
                .filter(|row| row
                    .pointer("/Merge/span_id")
                    .and_then(|value| value.as_str())
                    == Some(root_id.as_str())
                    && row.pointer("/Merge/end_ms").is_some())
                .count(),
            1
        );
        assert!(rows.contains("\"end_ms\":1002"));
        let completed = rows.clone();
        import().await.unwrap();
        assert_eq!(std::fs::read_to_string(&rows_path).unwrap(), completed);
    }

    #[tokio::test]
    async fn muse_live_capture_then_export_reuses_model_and_late_updates_resumed_root() {
        let temp = tempfile::tempdir().unwrap();
        let output = temp.path().join("output");
        let export = temp.path().join("export.json");
        let target =
            import_daemon::target(&output.join("spans"), Some(import_test_config("project-a")))
                .await;
        let make_processor = || ImportProcessor::new(target.clone());
        let mut live = make_processor();
        for (name, payload, ts) in [
            (
                "UserPromptSubmit",
                json!({"turn_id":"run-1","prompt":"first"}),
                1000,
            ),
            (
                "PreLLMCall",
                json!({"turn_id":"run-1","request_id":"run-1:0:1"}),
                1001,
            ),
            (
                "PostLLMCall",
                json!({"turn_id":"run-1","request_id":"run-1:0:1","output_text_preview":"preview"}),
                1003,
            ),
            (
                "Stop",
                json!({"turn_id":"run-1","last_assistant_message":"first answer"}),
                1003,
            ),
            ("SessionEnd", json!({}), 1004),
            (
                "UserPromptSubmit",
                json!({"turn_id":"run-2","prompt":"second"}),
                2400,
            ),
            (
                "Stop",
                json!({"turn_id":"run-2","last_assistant_message":"latest preview"}),
                2500,
            ),
            ("SessionEnd", json!({}), 2501),
        ] {
            live.process(vec![transcript_import::envelope(
                "muse",
                None,
                "cross-ingress",
                name,
                ts,
                payload,
            )])
            .await
            .unwrap();
        }
        live.finish().await.unwrap();
        std::fs::write(&export, serde_json::to_vec(&json!({
            "export_schema_version":1,"sessions":[{"session_id":"cross-ingress"}],
            "events":[
                {"recorded_at":1000000,"envelope":{"payload":{"kind":"run","run_id":"run-1","event":{"kind":"started","prompt":"first"}}}},
                {"recorded_at":1001000,"envelope":{"payload":{"kind":"run","run_id":"run-1","event":{"kind":"model_input_trace_recorded","request_record_id":"uuid-1","model_step":0}}}},
                {"recorded_at":1002000,"envelope":{"payload":{"kind":"run","run_id":"run-1","event":{"kind":"assistant_message_committed","text":"full exported answer"}}}},
                {"recorded_at":1003000,"envelope":{"payload":{"kind":"run","run_id":"run-1","event":{"kind":"terminal","terminal":"completed"}}}},
                {"recorded_at":1004000,"envelope":{"payload":{"kind":"session_end"}}},
                {"recorded_at":2000000,"envelope":{"payload":{"kind":"run","run_id":"run-2","event":{"kind":"started","prompt":"second"}}}},
                {"recorded_at":2001000,"envelope":{"payload":{"kind":"run","run_id":"run-2","event":{"kind":"assistant_message_committed","text":"latest answer"}}}},
                {"recorded_at":2002000,"envelope":{"payload":{"kind":"run","run_id":"run-2","event":{"kind":"terminal","terminal":"completed"}}}},
                {"recorded_at":2003000,"envelope":{"payload":{"kind":"session_end"}}}
            ]
        })).unwrap()).unwrap();
        let mut imported = make_processor();
        assert!(process_muse_export(&mut imported, &export, "cross-ingress")
            .await
            .unwrap());
        imported.finish().await.unwrap();
        let rows: Vec<serde_json::Value> =
            std::fs::read_to_string(output.join("spans/cross-ingress.ndjson"))
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
        let namespace = ids::session_namespace("muse", "cross-ingress");
        let model = ids::span_id(&namespace, "llm:run-1:0:1");
        assert_eq!(
            rows.iter()
                .filter(|row| row
                    .pointer("/Insert/span_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(model.as_str()))
                .count(),
            1
        );
        assert!(rows
            .iter()
            .filter_map(|row| row.get("Merge"))
            .any(|row| row["span_id"] == model && row["output"] == "full exported answer"));
        let root = ids::span_id(&namespace, "root");
        let root_updates: Vec<_> = rows
            .iter()
            .filter_map(|row| row.get("Merge"))
            .filter(|row| row["span_id"] == root)
            .collect();
        assert!(root_updates
            .iter()
            .any(|row| row["output"] == "latest answer"));
        assert_eq!(root_updates.last().unwrap()["end_ms"], 2003);
        assert_eq!(
            root_updates
                .iter()
                .rev()
                .find(|row| row.get("output").is_some())
                .unwrap()["output"],
            "latest answer"
        );
        // An older complete export must not roll the root back to run-1.
        let rows_path = output.join("spans/cross-ingress.ndjson");
        let before_stale = std::fs::read_to_string(&rows_path).unwrap();
        let mut stale: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&export).unwrap()).unwrap();
        stale["events"]
            .as_array_mut()
            .unwrap()
            .retain(|event| event["recorded_at"].as_i64().unwrap() < 2_000_000);
        std::fs::write(&export, serde_json::to_vec(&stale).unwrap()).unwrap();
        let mut old = make_processor();
        process_muse_export(&mut old, &export, "cross-ingress")
            .await
            .unwrap();
        old.finish().await.unwrap();
        assert_eq!(std::fs::read_to_string(rows_path).unwrap(), before_stale);
    }

    #[tokio::test]
    async fn muse_native_tool_export_reuses_delivered_live_tool_identity() {
        let temp = tempfile::tempdir().unwrap();
        let output = temp.path().join("output");
        let target =
            import_daemon::target(&output.join("spans"), Some(import_test_config("project-a")))
                .await;
        let make_processor = || ImportProcessor::new(target.clone());
        let mut live = make_processor();
        for (name, payload, ts) in [
            (
                "UserPromptSubmit",
                json!({"turn_id":"fixture-turn","prompt":"Print an integration marker."}),
                1,
            ),
            (
                "PreToolUse",
                json!({"turn_id":"fixture-turn","tool_use_id":"call_muse_1","tool_name":"shell","tool_input":{"command":"printf MUSE_TOOL_OK"}}),
                2,
            ),
            (
                "PostToolUse",
                json!({"turn_id":"fixture-turn","tool_use_id":"call_muse_1","tool_name":"shell","tool_response":"MUSE_TOOL_OK"}),
                3,
            ),
            (
                "Stop",
                json!({"turn_id":"fixture-turn","last_assistant_message":"MUSE_TOOL_OK"}),
                4,
            ),
            ("SessionEnd", json!({}), 5),
        ] {
            live.process(vec![transcript_import::envelope(
                "muse",
                Some("1.4.11".into()),
                "muse-export-fixture",
                name,
                ts,
                payload,
            )])
            .await
            .unwrap();
        }
        live.finish().await.unwrap();
        let export = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/muse-export-1.4.11.json");
        let mut imported = make_processor();
        assert!(
            process_muse_export(&mut imported, &export, "muse-export-fixture")
                .await
                .unwrap()
        );
        imported.finish().await.unwrap();
        let rows: Vec<serde_json::Value> =
            std::fs::read_to_string(output.join("spans/muse-export-fixture.ndjson"))
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
        let tool = ids::span_id(
            &ids::session_namespace("muse", "muse-export-fixture"),
            "tool:call_muse_1",
        );
        assert_eq!(
            rows.iter()
                .filter(|row| row
                    .pointer("/Insert/span_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(tool.as_str()))
                .count(),
            1
        );
        assert!(rows
            .iter()
            .filter_map(|row| row.get("Merge"))
            .any(|row| row["span_id"] == tool && row["output"] == "MUSE_TOOL_OK"));
    }

    #[derive(Debug, Parser)]
    struct ImportCli {
        #[command(flatten)]
        args: ImportArgs,
    }

    #[test]
    fn import_parent_accepts_complete_cli_identifiers_without_guessing() {
        let args = ImportCli::try_parse_from([
            "test",
            "codex",
            "session-a",
            "--parent-span-id",
            "span-1",
            "--parent-root-span-id",
            "root-1",
            "--parent-object-type",
            "project_logs",
            "--parent-project",
            "Agents",
        ])
        .unwrap()
        .args;

        let components = import_parent_components(&args).unwrap().unwrap();
        assert_eq!(components.object_type, SpanObjectType::ProjectLogs);
        assert_eq!(components.span_id.as_deref(), Some("span-1"));
        assert_eq!(components.root_span_id.as_deref(), Some("root-1"));
        assert_eq!(
            components
                .compute_object_metadata_args
                .as_ref()
                .and_then(|value| value.get("project_name"))
                .and_then(serde_json::Value::as_str),
            Some("Agents")
        );
    }

    #[test]
    fn attach_requires_one_explicit_session() {
        let args = ImportArgs {
            source: ImportSource::Codex,
            session_ids: vec!["one".into(), "two".into()],
            all: false,
            destination: None,
            parent: None,
            parent_span_id: None,
            parent_root_span_id: None,
            parent_object_type: None,
            parent_object_id: None,
            parent_project: None,
            attach: true,
            additional_metadata: None,
            tags: Default::default(),
            plugin: Vec::new(),
            socket: None,
            no_spawn: false,
        };
        assert!(validate_import_selection(&args)
            .unwrap_err()
            .to_string()
            .contains("exactly one"));
    }

    fn import_test_config(project_id: &str) -> SessionConfig {
        SessionConfig {
            auth: wire::BackendAuth {
                token: "test".into(),
                api_url: Some("https://api.example.test".into()),
                app_url: None,
                org_name: Some("test-org".into()),
                org_id: Some("test-org-id".into()),
            },
            destination: Some(wire::TraceDestination::ProjectLogs {
                project_id: Some(project_id.into()),
                project_name: None,
            }),
            flush_mode: wire::FlushMode::FireAndForget,
            additional_metadata: None,
            tags: Default::default(),
            span_plugins: Vec::new(),
        }
    }

    fn imported_terminal_span_ids(path: &std::path::Path) -> Vec<String> {
        std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .filter_map(|op| {
                let row = op.get("Insert").or_else(|| op.get("Merge"))?;
                row.get("end_ms")?.as_i64()?;
                row.get("span_id")?.as_str().map(str::to_string)
            })
            .collect()
    }

    #[tokio::test]
    async fn reimporting_a_live_session_captures_nothing_twice() {
        let temp = tempfile::tempdir().unwrap();
        let transcript = temp.path().join("session.jsonl");
        std::fs::write(
            &transcript,
            format!(
                "{}\n{}\n{}\n{}\n",
                json!({"timestamp":"2026-01-01T00:00:01Z","type":"session_meta","payload":{"id":"reimport","cwd":"/tmp/demo"}}),
                json!({"timestamp":"2026-01-01T00:00:02Z","type":"turn_context","payload":{"model":"gpt-test"}}),
                json!({"timestamp":"2026-01-01T00:00:03Z","type":"event_msg","payload":{"type":"task_started","turn_id":"turn-1"}}),
                json!({"timestamp":"2026-01-01T00:00:04Z","type":"event_msg","payload":{"type":"task_complete","last_agent_message":"done"}}),
            ),
        )
        .unwrap();
        let output = temp.path().join("spans");
        let target = import_daemon::target(&output, Some(import_test_config("project-a"))).await;
        let first = import_transcript(&transcript, ImportSource::Codex, target.clone(), false)
            .await
            .unwrap();
        assert!(first[0].span_count > 0);
        let spans = output.join("reimport.ndjson");
        let first_terminals = imported_terminal_span_ids(&spans);
        assert!(!first_terminals.is_empty());
        let first_operations = std::fs::read_to_string(&spans).unwrap();
        import_transcript(&transcript, ImportSource::Codex, target, false)
            .await
            .unwrap();
        assert_eq!(std::fs::read_to_string(&spans).unwrap(), first_operations);
    }
}
