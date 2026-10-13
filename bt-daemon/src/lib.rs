//! bt-daemon: the embeddable library behind the Braintrust coding-agent
//! tracing daemon. Two front-ends consume it (see `../DESIGN.md`):
//!   * `bt` wires the [`clap::Args`] structs into its command tree and exposes
//!     its profile store through [`AuthProvider`].
//!   * the feature-gated standalone `bt-daemon` binary does the same with
//!     env/flag token auth only, for isolated testing.
//!
//! Hook clients submit a non-secret [`wire::SessionRoute`]. The long-lived
//! daemon resolves and refreshes the selected profile through its host.
//! The `cli` feature only gates the standalone binary and its logging
//! subscriber.

pub mod paths;

mod agents;
mod args;
mod capture;
mod client;
mod command_output;
mod correlation;
mod delivery_ledger;
mod env;
mod hook;
mod ids;
mod journal;
mod managed_run;
mod pipeline;
mod plugin_diagnostics;
pub(crate) mod process;
mod route;
mod server;
mod settings;
mod setup;
mod sink;
mod span_processor;
mod subprocess;
mod trace_command;
mod trace_runtime;
mod transcript_import;
mod transcript_mirror;
mod translate;
mod transport;
#[cfg(windows)]
mod win_acl;

pub mod wire;
pub use args::{
    HookArgs, ImportArgs, ImportSource, ParentObjectType, RunArgs, RunHookCommand, RunSource,
    ServeArgs, StatusArgs,
};
pub use client::{
    flush_managed_run, flush_session, forward_envelope, run_status, shutdown_daemon, HostInfo,
};
pub use command_output::{
    AuthDiagnostic, DaemonDiagnostic, DaemonStatus, DoctorCommandOutput, ImportSummary,
    OutputFormat, SetupCommandOutput, StatusCommandOutput, StopCommandOutput, TraceCommandOutput,
};
pub use hook::run_hook;
/// The native observations journaled for one source session, in order, as
/// JSON envelopes. Daemon controls (checkpoints, finalization) are omitted.
#[doc(hidden)]
pub fn read_journaled_events(
    data_dir: &std::path::Path,
    source: &str,
    session_id: &str,
) -> anyhow::Result<Vec<serde_json::Value>> {
    let partition = pipeline::types::PartitionId::new(source, session_id);
    let store = pipeline::events::EventStore::new(&pipeline::runtime::pipeline_dir(data_dir));
    let mut cursor = store.cursor(&partition);
    let mut events = Vec::new();
    while let Some((_, input)) = cursor.read_one()? {
        if let pipeline::types::TranslationInput::Native(env) = input {
            events.push(serde_json::to_value(*env)?);
        }
    }
    Ok(events)
}

/// Append one captured event to a session's event journal without a daemon,
/// as a capture that was journaled and then lost its process.
#[doc(hidden)]
pub async fn seed_journaled_event(
    data_dir: &std::path::Path,
    event: &wire::RedactedEnvelope,
) -> anyhow::Result<()> {
    let partition = pipeline::types::PartitionId::new(&event.source, &event.session_id);
    let store = pipeline::events::EventStore::new(&pipeline::runtime::pipeline_dir(data_dir));
    std::fs::create_dir_all(pipeline::events::events_dir(
        &pipeline::runtime::pipeline_dir(data_dir),
    ))?;
    store
        .append(
            &partition,
            pipeline::types::TranslationInput::Native(Box::new(event.clone())),
        )
        .await?;
    Ok(())
}

/// The per-session journal file written before the pipeline existed.
#[doc(hidden)]
pub fn legacy_source_journal_path(
    data_dir: &std::path::Path,
    source: &str,
    session_id: &str,
) -> std::path::PathBuf {
    journal::source_journal_path(data_dir, source, session_id)
}

/// The event journal file of one source session.
#[doc(hidden)]
pub fn source_journal_path(
    data_dir: &std::path::Path,
    source: &str,
    session_id: &str,
) -> std::path::PathBuf {
    pipeline::events::event_path(
        &pipeline::runtime::pipeline_dir(data_dir),
        &pipeline::types::PartitionId::new(source, session_id),
    )
}
pub use managed_run::run_traced;
pub use plugin_diagnostics::PluginDiagnostic;
pub use server::{
    braintrust_serve_options, debug_serve_options, run_serve, AuthLease, AuthProvider,
    AuthResolveReason, ServeOptions,
};
pub use setup::{run_disable, run_enable, run_setup, run_update};
pub use sink::{BraintrustSinkConfig, BraintrustSinkFactory, DebugSinkFactory, Sink, SinkFactory};
pub use trace_command::{
    DisableArgs, DoctorAgent, DoctorArgs, EnableArgs, SetupAgent, SetupArgs, StopArgs, TraceArgs,
    TraceCommand, UpdateArgs,
};
pub use trace_runtime::{
    run_trace, HostRouteSelection, RouteRequirements, TraceHostContext, TraceHostServices,
};
pub use transcript_import::{import_transcript, import_transcripts, run_import};
pub use translate::{
    AgentTranslator, OriginSnapshot, Registry, RouteOverlay, SessionCtx, SpanOp, SpanRow, SpanType,
    TURN_SPAN_ID_KEY,
};
