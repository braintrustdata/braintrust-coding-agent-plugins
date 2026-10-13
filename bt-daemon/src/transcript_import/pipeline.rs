//! Transcript import: each imported session is a partition with its own
//! import route, fed by a transcript backfill and delivered by the same route
//! machine as live traffic, in a pipeline private to the importing process.

use anyhow::Context;
use braintrust_sdk_rust::SpanComponents;
use std::path::PathBuf;

use crate::args::{ImportArgs, ImportSource, ParentObjectType};
use crate::pipeline::runtime::{BackfillSource, ResolvedAuth, RouteAuth};
use crate::pipeline::types::{PartitionId, RouteOrigin};
use crate::route::resolve_span_plugin_paths;
use crate::wire::{self, SessionConfig};
use crate::{paths, transcript_import, ImportSummary, ServeOptions};
use std::sync::Arc;

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
    import_files(
        std::slice::from_ref(&file.to_path_buf()),
        source,
        opts,
        config,
        attach,
        ledger_dir,
    )
    .await
}

/// Import multiple completed native transcripts through one pipeline.
///
/// Each native session is its own partition, so correlation state stays
/// isolated by session id.
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
    import_files(files, source, opts, config, false, ledger_dir).await
}

/// Credentials of an import: the resolved configuration the caller supplied.
struct ImportAuth {
    auth: wire::BackendAuth,
}

#[async_trait::async_trait]
impl RouteAuth for ImportAuth {
    async fn resolve(
        &self,
        _source: &str,
        route: &wire::SessionRoute,
        _reason: crate::AuthResolveReason,
    ) -> anyhow::Result<ResolvedAuth> {
        Ok(ResolvedAuth {
            config: route.with_auth(self.auth.clone()),
            expires_at_ms: None,
        })
    }
}

/// One import: every session in the files is a partition with its own
/// import route, fed by a transcript backfill and delivered by the same
/// route machine as live traffic. The pipeline's state is private to this
/// process (`<data dir>/import`, or a temporary directory without a data
/// directory), so it never contends with a running daemon.
async fn import_files(
    files: &[PathBuf],
    source: ImportSource,
    opts: ServeOptions,
    config: Option<SessionConfig>,
    attach: bool,
    data_dir: Option<PathBuf>,
) -> anyhow::Result<Vec<ImportSummary>> {
    let scratch;
    let (data_dir, state_dir) = match data_dir {
        Some(dir) => {
            paths::ensure_private_dir(&dir)?;
            let state = dir.join("import");
            (dir, state)
        }
        None => {
            scratch = tempfile::tempdir()?;
            (scratch.path().to_path_buf(), scratch.path().join("import"))
        }
    };
    let auth = config
        .as_ref()
        .map(|config| config.auth.clone())
        .unwrap_or(wire::BackendAuth {
            token: String::new(),
            api_url: None,
            app_url: None,
            org_name: None,
            org_id: None,
        });
    let route = wire::SessionRoute {
        auth: wire::AuthSelection::default(),
        destination: config.as_ref().and_then(|c| c.destination.clone()),
        flush_mode: config.as_ref().map(|c| c.flush_mode).unwrap_or_default(),
        additional_metadata: config.as_ref().and_then(|c| c.additional_metadata.clone()),
        tags: config.as_ref().map(|c| c.tags.clone()).unwrap_or_default(),
        span_plugins: config
            .as_ref()
            .map(|c| c.span_plugins.clone())
            .unwrap_or_default(),
    };
    let pipeline =
        crate::pipeline::runtime::Pipeline::open(crate::pipeline::runtime::PipelineOptions {
            data_dir,
            state_dir: Some(state_dir),
            version: opts.version.clone(),
            translators: opts.translators.clone(),
            sink_factory: opts.sink_factory.clone(),
            auth: Arc::new(ImportAuth { auth }),
            correlation: None,
            assembly_gate: false,
        })?;
    let import_id = uuid::Uuid::new_v4().to_string();
    let origin = RouteOrigin::Import(import_id);
    let mut branches = Vec::new();
    for file in files {
        let sessions = transcript_sessions(file, source, attach)
            .with_context(|| format!("import transcript {}", file.display()))?;
        for (partition_source, session_id) in sessions {
            let partition = PartitionId::new(partition_source, session_id);
            let branch = pipeline
                .register_route(
                    &partition,
                    &route,
                    origin.clone(),
                    Some(BackfillSource::Transcript {
                        path: file.clone(),
                        source,
                        attach,
                    }),
                )
                .await?;
            branches.push((partition, branch));
        }
    }
    // An attached import follows its transcript until interrupted.
    let finish = async {
        if attach {
            let _ = tokio::signal::ctrl_c().await;
            for (_, branch) in &branches {
                branch.finish_backfill.send_replace(true);
            }
        }
        std::future::pending::<()>().await
    };
    tokio::pin!(finish);
    let mut summaries = Vec::new();
    for (partition, branch) in &branches {
        let outcome = tokio::select! {
            outcome = pipeline.wait_import(branch) => outcome,
            _ = &mut finish => unreachable!("the finish future never completes"),
        };
        let view = match outcome {
            Ok(view) => view,
            Err(record) => {
                for (partition, _) in &branches {
                    pipeline.retire(partition).await;
                }
                anyhow::bail!(
                    "import of {} stopped: {}",
                    partition.session_id,
                    record.summary()
                );
            }
        };
        summaries.push(ImportSummary {
            session_id: partition.session_id.clone(),
            destination: route.destination.clone(),
            root_span_id: config
                .as_ref()
                .and_then(|config| config.attached_span_ids().1)
                .or(view.root_span_id),
            span_count: view.span_ids,
            finalized: true,
        });
    }
    for (partition, _) in &branches {
        pipeline.retire(partition).await;
    }
    summaries.sort_by(|left, right| left.session_id.cmp(&right.session_id));
    Ok(summaries)
}

/// The `(source, session id)` of every native session in a transcript.
fn transcript_sessions(
    file: &std::path::Path,
    source: ImportSource,
    attach: bool,
) -> anyhow::Result<Vec<(String, String)>> {
    let mut tail = transcript_import::TranscriptTail::new(file.to_path_buf(), source);
    if attach {
        tail.allow_incomplete_final_record_on_shutdown();
    }
    let entries = tail.poll(!attach)?;
    let mut sessions = Vec::new();
    for entry in entries {
        let session = (entry.source, entry.session_id);
        if !sessions.contains(&session) {
            sessions.push(session);
        }
    }
    anyhow::ensure!(
        !sessions.is_empty(),
        "{} has no session records to import",
        file.display()
    );
    Ok(sessions)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::debug_serve_options;
    use braintrust_sdk_rust::SpanObjectType;
    use clap::Parser;
    use serde_json::json;

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

    /// `bt trace import` writes its delivery state without a daemon, so it
    /// must make the data directory private itself.
    #[tokio::test]
    async fn import_keeps_its_delivery_state_private_to_the_owner() {
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

        let state_dir = data_dir.join("import");
        let ledger = state_dir.join("pipeline.sqlite");
        assert!(ledger.is_file(), "the import's delivery state was written");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            for dir in [&data_dir, &state_dir] {
                assert_eq!(
                    std::fs::metadata(dir).unwrap().permissions().mode() & 0o777,
                    0o700
                );
            }
        }
        #[cfg(windows)]
        {
            use crate::win_acl::test_support::{
                assert_only_owner_access, assert_owner_only, path_dacl_sddl,
            };
            assert_owner_only(&path_dacl_sddl(&data_dir), true);
            assert_owner_only(&path_dacl_sddl(&state_dir), true);
            assert_only_owner_access(&path_dacl_sddl(&ledger));
        }
    }
}
