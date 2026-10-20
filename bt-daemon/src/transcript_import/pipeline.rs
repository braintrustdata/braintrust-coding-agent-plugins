//! Transcript import: run native transcript records through the translators
//! and a sink without a daemon, recording deliveries in the ledger.

#[path = "muse_runtime.rs"]
mod muse_runtime;

use anyhow::Context;
use braintrust_sdk_rust::SpanComponents;
use std::path::PathBuf;

use crate::args::{ImportArgs, ImportSource, ParentObjectType};
use crate::route::resolve_span_plugin_paths;
use crate::wire::{self, Envelope, SessionConfig};
use crate::{
    delivery_ledger, paths, transcript_import, AgentTranslator, ImportSummary, ServeOptions,
    SessionCtx, Sink, SpanOp,
};

/// Import a native coding-agent transcript through the normal translators and
/// sink. This is separate from daemon journal recovery: import creates traces
/// for a past session, while recovery rebuilds live correlation state.
pub async fn run_import(
    args: ImportArgs,
    opts: ServeOptions,
    mut config: Option<SessionConfig>,
) -> anyhow::Result<Vec<ImportSummary>> {
    validate_import_selection(&args)?;
    apply_import_span_plugins(&mut config, &args.plugin)?;
    let destination = import_parent_components(&args)?
        .map(|components| wire::TraceDestination::ParentSpan { components })
        .or_else(|| args.destination.clone());
    apply_import_destination(&mut config, destination)?;
    if args.source == ImportSource::Muse {
        return muse_runtime::run(args, opts, config, Some(paths::data_dir(None))).await;
    }
    let files = transcript_import::resolve_transcripts(&args.session_ids, args.all, args.source)?;
    let ledger_dir = paths::data_dir(None);
    if args.attach {
        return import_transcript_with_ledger(
            &files[0],
            args.source,
            opts,
            config,
            true,
            Some(ledger_dir),
        )
        .await;
    }
    import_transcripts_with_ledger(&files, args.source, opts, config, Some(ledger_dir)).await
}

fn apply_import_span_plugins(
    config: &mut Option<SessionConfig>,
    plugins: &[PathBuf],
) -> anyhow::Result<()> {
    let plugins = resolve_span_plugin_paths(plugins)?;
    if let Some(config) = config.as_mut() {
        config.span_plugins = plugins;
    } else if !plugins.is_empty() {
        *config = Some(SessionConfig {
            auth: wire::BackendAuth {
                token: String::new(),
                api_url: None,
                app_url: None,
                org_name: None,
                org_id: None,
            },
            destination: None,
            flush_mode: wire::FlushMode::FireAndForget,
            additional_metadata: None,
            tags: Vec::new(),
            span_plugins: plugins,
        });
    }
    Ok(())
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
    attach: bool,
) -> anyhow::Result<Vec<ImportSummary>> {
    import_transcript_with_ledger(file, source, opts, config, attach, None).await
}

async fn import_transcript_with_ledger(
    file: &std::path::Path,
    source: ImportSource,
    opts: ServeOptions,
    config: Option<SessionConfig>,
    attach: bool,
    ledger_dir: Option<PathBuf>,
) -> anyhow::Result<Vec<ImportSummary>> {
    if source == ImportSource::Muse {
        let session_id = transcript_import::muse::session_id(file)?;
        if attach {
            return muse_runtime::attach_muse_export(
                file,
                &session_id,
                false,
                opts,
                config,
                ledger_dir,
            )
            .await;
        }
        let mut processor = ImportProcessor::new(opts, config, ledger_dir)?;
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
    let mut processor = ImportProcessor::new(opts, config, ledger_dir)?;
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);
    let mut finalizing = !attach;
    loop {
        let entries = tail.poll(finalizing)?;
        if tail.take_translator_reset() {
            processor.reset_translators();
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

/// Import multiple completed native transcripts through one processor.
///
/// This shares translator and sink setup while keeping each native session's
/// correlation state isolated by session id.
pub async fn import_transcripts(
    files: &[PathBuf],
    source: ImportSource,
    opts: ServeOptions,
    config: Option<SessionConfig>,
) -> anyhow::Result<Vec<ImportSummary>> {
    import_transcripts_with_ledger(files, source, opts, config, None).await
}

async fn import_transcripts_with_ledger(
    files: &[PathBuf],
    source: ImportSource,
    opts: ServeOptions,
    config: Option<SessionConfig>,
    ledger_dir: Option<PathBuf>,
) -> anyhow::Result<Vec<ImportSummary>> {
    if source == ImportSource::Muse {
        let mut processor = ImportProcessor::new(opts, config, ledger_dir)?;
        let mut summaries = Vec::new();
        for file in files {
            let session_id = transcript_import::muse::session_id(file)?;
            let ended =
                muse_runtime::process_muse_export(&mut processor, file, &session_id).await?;
            let summary = if ended {
                processor.finish_session(&session_id).await?
            } else {
                processor.detach_session(&session_id).await?
            };
            summaries.extend(summary);
        }
        return Ok(summaries);
    }
    let mut processor = ImportProcessor::new(opts, config, ledger_dir)?;
    let mut summaries = Vec::new();
    for file in files {
        let mut tail = transcript_import::TranscriptTail::new(file.clone(), source);
        let entries = tail
            .poll(true)
            .with_context(|| format!("import transcript {}", file.display()))?;
        let session_ids = entries
            .iter()
            .map(|entry| entry.session_id.clone())
            .collect::<std::collections::HashSet<_>>();
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

struct ImportLive {
    source: String,
    translator: Box<dyn AgentTranslator>,
    sink: Box<dyn Sink>,
    ctx: SessionCtx,
    pending_ops: usize,
    span_ids: std::collections::HashSet<String>,
    root_span_id: Option<String>,
    destination: Option<wire::TraceDestination>,
    diagnostics_dir: Option<PathBuf>,
}

struct ImportProcessor {
    sessions: std::collections::HashMap<String, ImportLive>,
    opts: ServeOptions,
    config: Option<SessionConfig>,
    ledger_dir: Option<PathBuf>,
}

impl ImportProcessor {
    /// Without a daemon, import is the entry point that creates the ledger's
    /// data directory, so it must make that directory private itself.
    fn new(
        opts: ServeOptions,
        config: Option<SessionConfig>,
        ledger_dir: Option<PathBuf>,
    ) -> anyhow::Result<Self> {
        if let Some(dir) = &ledger_dir {
            paths::ensure_private_dir(dir)?;
        }
        Ok(Self {
            sessions: std::collections::HashMap::new(),
            opts,
            config,
            ledger_dir,
        })
    }

    async fn process(&mut self, entries: Vec<Envelope>) -> anyhow::Result<()> {
        for mut env in entries {
            env.config = self.config.clone();
            let sid = env.session_id.clone();
            let live = match self.sessions.get_mut(&sid) {
                Some(live) => live,
                None => {
                    let translator = self.opts.translators.create_checked(&env.source, &sid)?;
                    let sink = self.opts.sink_factory.create(&sid, &env.source)?;
                    let sink: Box<dyn Sink> = Box::new(
                        delivery_ledger::LedgerSink::new(
                            sink,
                            self.ledger_dir.as_deref(),
                            &env.source,
                            &sid,
                            env.config.as_ref(),
                            &self.opts.version,
                        )
                        .await?,
                    );
                    self.sessions.insert(
                        sid.clone(),
                        ImportLive {
                            source: env.source.clone(),
                            translator,
                            sink,
                            ctx: SessionCtx {
                                session_id: sid.clone(),
                                config: None,
                            },
                            pending_ops: 0,
                            span_ids: std::collections::HashSet::new(),
                            root_span_id: self
                                .config
                                .as_ref()
                                .and_then(|config| config.attached_span_ids().1),
                            destination: self
                                .config
                                .as_ref()
                                .and_then(|config| config.destination.clone()),
                            diagnostics_dir: self.ledger_dir.clone(),
                        },
                    );
                    self.sessions.get_mut(&sid).ok_or_else(|| {
                        anyhow::anyhow!("import session {sid:?} was not initialized")
                    })?
                }
            };
            if let Some(cfg) = &env.config {
                live.sink.configure(cfg);
                live.ctx.config = Some(cfg.clone());
            }
            live.sink
                .set_capture_versions(env.plugin_version.as_deref(), env.source_version.as_deref());
            let ops = live.translator.handle(&env, &live.ctx)?;
            Self::emit_translator_batches(live, ops).await?;
        }
        Ok(())
    }

    fn reset_translators(&mut self) {
        for (session_id, live) in &mut self.sessions {
            live.translator = self.opts.translators.create(&live.source, session_id);
        }
    }

    async fn emit_translator_batches(
        live: &mut ImportLive,
        first: Vec<SpanOp>,
    ) -> anyhow::Result<()> {
        let mut next = Some(first);
        while let Some(ops) = next {
            // Imports can contain tens of thousands of SDK log commands. Bound the
            // number queued between drains without serializing one network flush
            // for every native turn boundary.
            const FLUSH_OPS: usize = 500;
            for chunk in ops.chunks(FLUSH_OPS) {
                for op in chunk {
                    let row = match op {
                        SpanOp::Insert(row) | SpanOp::Merge(row) => row,
                    };
                    live.span_ids.insert(row.span_id.clone());
                    if live.root_span_id.is_none() && !row.root_span_id.is_empty() {
                        live.root_span_id = Some(row.root_span_id.clone());
                    }
                }
                let plugins = live
                    .ctx
                    .config
                    .as_ref()
                    .map(|config| config.span_plugins.as_slice())
                    .unwrap_or_default();
                let mut transformed = Vec::with_capacity(chunk.len());
                for op in chunk {
                    match crate::span_processor::process(
                        plugins,
                        op,
                        &live.source,
                        &live.ctx.session_id,
                    ) {
                        Ok(result) => {
                            if let Some(failure) = result.failure {
                                if failure.newly_seen {
                                    if let Some(data_dir) = &live.diagnostics_dir {
                                        if let Err(error) = crate::plugin_diagnostics::record(
                                            data_dir,
                                            &live.source,
                                            &failure.path,
                                            &failure.message,
                                        ) {
                                            tracing::warn!(
                                                session_id = %live.ctx.session_id,
                                                "failed to persist span plugin diagnostic: {error}"
                                            );
                                        }
                                    }
                                    tracing::warn!(
                                        session_id = %live.ctx.session_id,
                                        plugin = %failure.path.display(),
                                        error = %failure.message,
                                        "span plugin failed during import; span operations are being discarded"
                                    );
                                }
                            }
                            if let Some(op) = result.op {
                                transformed.push(op);
                            }
                        }
                        Err(error) => {
                            if let (Some(data_dir), Some(plugin)) =
                                (&live.diagnostics_dir, plugins.first())
                            {
                                if let Err(diagnostic_error) = crate::plugin_diagnostics::record(
                                    data_dir,
                                    &live.source,
                                    plugin,
                                    &error.to_string(),
                                ) {
                                    tracing::warn!(
                                        session_id = %live.ctx.session_id,
                                        "failed to persist span plugin diagnostic: {diagnostic_error}"
                                    );
                                }
                            }
                            tracing::warn!(
                                session_id = %live.ctx.session_id,
                                %error,
                                "span plugin processor failed during import; span operation discarded"
                            );
                        }
                    }
                }
                if !transformed.is_empty() {
                    live.sink.emit(&transformed).await?;
                }
                live.pending_ops += transformed.len();
                if live.pending_ops >= FLUSH_OPS {
                    live.sink.flush().await?;
                    live.pending_ops = 0;
                }
            }
            next = live.translator.drain_pending(&live.ctx)?;
        }
        Ok(())
    }

    async fn finish(self) -> anyhow::Result<Vec<ImportSummary>> {
        let mut summaries = Vec::new();
        for (sid, mut live) in self.sessions {
            let ops = live.translator.flush(&live.ctx)?;
            Self::emit_translator_batches(&mut live, ops).await?;
            live.sink.flush().await?;
            summaries.push(Self::summary(sid, live, true));
        }
        summaries.sort_by(|left, right| left.session_id.cmp(&right.session_id));
        Ok(summaries)
    }

    async fn finish_session(&mut self, session_id: &str) -> anyhow::Result<Option<ImportSummary>> {
        let Some(mut live) = self.sessions.remove(session_id) else {
            return Ok(None);
        };
        let ops = live.translator.flush(&live.ctx)?;
        Self::emit_translator_batches(&mut live, ops).await?;
        live.sink.flush().await?;
        Ok(Some(Self::summary(session_id.to_string(), live, true)))
    }

    async fn flush_open(&mut self) -> anyhow::Result<()> {
        for live in self.sessions.values_mut() {
            live.sink.flush().await?;
            live.pending_ops = 0;
        }
        Ok(())
    }

    async fn detach_session(&mut self, session_id: &str) -> anyhow::Result<Option<ImportSummary>> {
        let Some(mut live) = self.sessions.remove(session_id) else {
            return Ok(None);
        };
        live.sink.flush().await?;
        Ok(Some(Self::summary(session_id.to_string(), live, false)))
    }

    fn summary(session_id: String, live: ImportLive, finalized: bool) -> ImportSummary {
        ImportSummary {
            session_id,
            destination: live.destination,
            root_span_id: live.root_span_id,
            span_count: live.span_ids.len(),
            finalized,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::muse_runtime::*;
    use super::*;
    use crate::debug_serve_options;
    use crate::{ids, DebugSinkFactory, Registry};
    use braintrust_sdk_rust::SpanObjectType;
    use clap::Parser;
    use serde_json::json;
    use std::sync::Arc;

    #[tokio::test]
    async fn muse_attach_snapshots_reuse_one_trace_and_keep_open_spans() {
        let temp = tempfile::tempdir().unwrap();
        let export = temp.path().join("export.json");
        let output = temp.path().join("spans");
        let mut processor = ImportProcessor::new(
            ServeOptions {
                version: "test".into(),
                translators: Arc::new(Registry::default_agents()),
                sink_factory: Arc::new(DebugSinkFactory {
                    dir: output.clone(),
                }),
                auth_provider: None,
            },
            None,
            None,
        )
        .unwrap();
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
        let (count, ended) = process_muse_snapshot(&mut processor, &export, "muse-attach", true, 0)
            .await
            .unwrap();
        assert_eq!(count, 3);
        assert!(!ended);
        processor.flush_open().await.unwrap();
        let (same_count, ended) =
            process_muse_snapshot(&mut processor, &export, "muse-attach", true, count)
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
        let (_, ended) = process_muse_snapshot(&mut processor, &export, "muse-attach", true, count)
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
        let ledger = temp.path().join("ledger");
        let import = || {
            import_transcript_with_ledger(
                &export,
                ImportSource::Muse,
                debug_serve_options("test", &output),
                Some(import_test_config("project-a")),
                false,
                Some(ledger.clone()),
            )
        };
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
        let ledger = temp.path().join("ledger");
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
        let import = || {
            import_transcript_with_ledger(
                &export,
                ImportSource::Muse,
                debug_serve_options("test", &output),
                Some(import_test_config("project-a")),
                false,
                Some(ledger.clone()),
            )
        };
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
            2
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
        let ledger = temp.path().join("ledger");
        let export = temp.path().join("export.json");
        let make_processor = || {
            ImportProcessor::new(
                debug_serve_options("test", &output),
                Some(import_test_config("project-a")),
                Some(ledger.clone()),
            )
            .unwrap()
        };
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
        let ledger = temp.path().join("ledger");
        let make_processor = || {
            ImportProcessor::new(
                debug_serve_options("test", &output),
                Some(import_test_config("project-a")),
                Some(ledger.clone()),
            )
            .unwrap()
        };
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
    fn import_plugins_replace_inherited_plugins() {
        let temp = tempfile::tempdir().unwrap();
        let plugin = temp.path().join("import.mjs");
        std::fs::write(&plugin, "export default span => span").unwrap();
        let mut config = Some(SessionConfig {
            auth: wire::BackendAuth {
                token: String::new(),
                api_url: None,
                app_url: None,
                org_name: None,
                org_id: None,
            },
            destination: None,
            flush_mode: wire::FlushMode::FireAndForget,
            additional_metadata: None,
            tags: Vec::new(),
            span_plugins: vec![PathBuf::from("persisted.mjs")],
        });

        apply_import_span_plugins(&mut config, &[]).unwrap();
        assert!(config.as_ref().unwrap().span_plugins.is_empty());

        apply_import_span_plugins(&mut config, std::slice::from_ref(&plugin)).unwrap();
        assert_eq!(
            config.unwrap().span_plugins,
            [plugin.canonicalize().unwrap()]
        );
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
            tags: Vec::new(),
            plugin: Vec::new(),
        };
        assert!(validate_import_selection(&args)
            .unwrap_err()
            .to_string()
            .contains("exactly one"));
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
            tags: Vec::new(),
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
    async fn imports_share_destination_delivery_ledger_and_replay_to_new_destinations() {
        let temp = tempfile::tempdir().unwrap();
        let transcript = temp.path().join("session.jsonl");
        std::fs::write(
            &transcript,
            format!(
                "{}\n{}\n{}\n{}\n",
                json!({"timestamp":"2026-01-01T00:00:01Z","type":"session_meta","payload":{"id":"ledger-import","cwd":"/tmp/demo"}}),
                json!({"timestamp":"2026-01-01T00:00:02Z","type":"turn_context","payload":{"model":"gpt-test"}}),
                json!({"timestamp":"2026-01-01T00:00:03Z","type":"event_msg","payload":{"type":"task_started","turn_id":"turn-1"}}),
                json!({"timestamp":"2026-01-01T00:00:04Z","type":"event_msg","payload":{"type":"task_complete","last_agent_message":"done"}}),
            ),
        )
        .unwrap();
        let ledger_dir = temp.path().join("ledger");
        let first_output = temp.path().join("first");
        import_transcript_with_ledger(
            &transcript,
            ImportSource::Codex,
            debug_serve_options("test", &first_output),
            Some(import_test_config("project-a")),
            false,
            Some(ledger_dir.clone()),
        )
        .await
        .unwrap();
        let spans = first_output.join("spans/ledger-import.ndjson");
        let first_terminals = imported_terminal_span_ids(&spans);
        assert!(!first_terminals.is_empty());
        let first_operations = std::fs::read_to_string(&spans).unwrap();

        import_transcript_with_ledger(
            &transcript,
            ImportSource::Codex,
            debug_serve_options("test", &first_output),
            Some(import_test_config("project-a")),
            false,
            Some(ledger_dir.clone()),
        )
        .await
        .unwrap();
        assert_eq!(imported_terminal_span_ids(&spans), first_terminals);
        assert_eq!(std::fs::read_to_string(&spans).unwrap(), first_operations);

        let second_output = temp.path().join("second");
        import_transcript_with_ledger(
            &transcript,
            ImportSource::Codex,
            debug_serve_options("test", &second_output),
            Some(import_test_config("project-b")),
            false,
            Some(ledger_dir),
        )
        .await
        .unwrap();
        assert_eq!(
            imported_terminal_span_ids(&second_output.join("spans/ledger-import.ndjson")),
            first_terminals
        );
    }

    /// `bt trace import` writes the delivery ledger without a daemon, so it
    /// must make the data directory private itself.
    #[tokio::test]
    async fn import_keeps_the_delivery_ledger_private_to_the_owner() {
        let temp = tempfile::tempdir().unwrap();
        let shared = temp.path().join("shared");
        std::fs::create_dir(&shared).unwrap();
        #[cfg(windows)]
        crate::win_acl::test_support::set_sddl(&shared, "D:P(A;OICI;FA;;;{user})(A;OICI;FA;;;WD)");
        let transcript = temp.path().join("session.jsonl");
        std::fs::write(
            &transcript,
            format!(
                "{}\n{}\n{}\n",
                json!({"timestamp":"2026-01-01T00:00:01Z","type":"session_meta","payload":{"id":"private-ledger","cwd":"/tmp/demo"}}),
                json!({"timestamp":"2026-01-01T00:00:02Z","type":"event_msg","payload":{"type":"task_started","turn_id":"turn-1"}}),
                json!({"timestamp":"2026-01-01T00:00:03Z","type":"event_msg","payload":{"type":"task_complete","last_agent_message":"done"}}),
            ),
        )
        .unwrap();
        let data_dir = shared.join("bt-daemon");

        import_transcript_with_ledger(
            &transcript,
            ImportSource::Codex,
            debug_serve_options("test", &temp.path().join("output")),
            Some(import_test_config("project-a")),
            false,
            Some(data_dir.clone()),
        )
        .await
        .unwrap();

        let ledger = std::fs::read_dir(data_dir.join("delivery-ledger"))
            .unwrap()
            .next()
            .expect("a delivery ledger was written")
            .unwrap()
            .path();
        assert!(ledger.is_file());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&data_dir).unwrap().permissions().mode() & 0o777,
                0o700
            );
        }
        #[cfg(windows)]
        {
            use crate::win_acl::test_support::{
                assert_only_owner_access, assert_owner_only, path_dacl_sddl,
            };
            assert_owner_only(&path_dacl_sddl(&data_dir), true);
            assert_only_owner_access(&path_dacl_sddl(&ledger));
        }
    }
}
