//! Command definitions mounted by the host CLI.
//!
//! Coding-agent command names, aliases, and argument shapes live with the
//! integrations they control. Hosts such as `bt` provide global auth flags and
//! dispatch these commands without duplicating agent-specific CLI knowledge.

use crate::{HookArgs, ImportArgs, RunArgs, ServeArgs, StatusArgs};
use clap::{Args, Subcommand};
use std::path::PathBuf;

#[derive(Debug, Clone, Args)]
pub struct TraceArgs {
    #[command(subcommand)]
    pub command: TraceCommand,
}

#[derive(Debug, Clone, Subcommand)]
// Clap argument structs are parsed once; keeping their natural shapes is
// clearer than boxing individual command variants for stack-size savings.
#[allow(clippy::large_enum_variant)]
pub enum TraceCommand {
    /// Install the published Braintrust tracing plugin for a coding agent.
    Setup(SetupArgs),
    /// Run the tracing daemon (foreground).
    #[command(hide = true)]
    Daemon(ServeArgs),
    /// Forward one coding-agent hook event (read from stdin) to the daemon.
    #[command(hide = true)]
    Hook(HookArgs),
    /// Print daemon/session status.
    #[command(hide = true)]
    Status(StatusArgs),
    /// Gracefully stop the tracing daemon.
    #[command(hide = true)]
    Stop(StopArgs),
    /// Import a past Codex or Claude Code session by its resume id.
    Import(ImportArgs),
    /// Launch a coding agent with tracing enabled for this invocation.
    Run(RunArgs),
}

#[derive(Debug, Clone, Args)]
pub struct StopArgs {
    /// Socket path override (default: see the daemon protocol documentation).
    #[arg(long)]
    pub socket: Option<PathBuf>,
}

#[derive(Debug, Clone, Args)]
pub struct SetupArgs {
    #[command(subcommand)]
    pub agent: SetupAgent,
}

#[derive(Debug, Clone, Copy, Subcommand)]
pub enum SetupAgent {
    /// Install the published Codex tracing plugin.
    Codex,
    /// Install the published Claude Code tracing plugin.
    Claude,
    /// Configure the published OpenCode tracing plugin.
    #[command(name = "opencode", alias = "open-code")]
    OpenCode,
    /// Install the published Pi tracing extension.
    Pi,
}
