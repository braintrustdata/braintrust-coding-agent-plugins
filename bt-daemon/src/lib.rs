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

mod args;
mod client;
mod command_output;
mod correlation;
mod delivery_ledger;
mod dispatch;
mod hook;
mod ids;
mod journal;
mod managed_run;
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
#[doc(hidden)]
pub use journal::source_journal_path;
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
    AgentTranslator, Registry, SessionCtx, SpanOp, SpanRow, SpanType, TranslatorFactory,
};
