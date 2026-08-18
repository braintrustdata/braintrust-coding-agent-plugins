//! Command definitions mounted by the host CLI.
//!
//! Coding-agent command names, aliases, and argument shapes live with the
//! integrations they control. Hosts such as `bt` provide global auth flags and
//! dispatch these commands without duplicating agent-specific CLI knowledge.

use crate::{AgentId, HookArgs, ImportArgs, RunArgs, ServeArgs, StatusArgs};
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
    /// Install or enable the published Braintrust tracing plugin for a coding agent.
    #[command(name = "enable", alias = "setup")]
    Setup(EnableArgs),
    /// Uninstall the Braintrust tracing plugin and remove its saved configuration.
    Disable(DisableArgs),
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
pub struct EnableArgs {
    /// Coding agent to configure.
    #[arg(value_enum)]
    pub agent: SetupAgent,
    /// JSON object persisted in this agent's tracing route and merged into root-span metadata.
    #[arg(long, global = true, env = "BRAINTRUST_ADDITIONAL_METADATA")]
    pub additional_metadata: Option<String>,
}

/// Backwards-compatible API name for hosts that mounted the former setup command.
pub type SetupArgs = EnableArgs;

#[derive(Debug, Clone, Args)]
pub struct DisableArgs {
    /// Coding agent whose Braintrust tracing integration should be removed.
    #[arg(value_enum)]
    pub agent: AgentId,
}

/// Backwards-compatible name for the shared coding-agent identity.
pub type SetupAgent = AgentId;

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Debug, Parser)]
    struct Cli {
        #[command(flatten)]
        trace: TraceArgs,
    }

    #[test]
    fn every_public_trace_ingress_accepts_additional_metadata() {
        let setup = Cli::try_parse_from([
            "bt",
            "setup",
            "claude",
            "--additional-metadata",
            r#"{"setup":true}"#,
        ])
        .unwrap();
        assert!(matches!(
            setup.trace.command,
            TraceCommand::Setup(SetupArgs {
                agent: SetupAgent::Claude,
                additional_metadata: Some(ref value),
            }) if value == r#"{"setup":true}"#
        ));

        let hook = Cli::try_parse_from([
            "bt",
            "hook",
            "--source",
            "claude-code",
            "--additional-metadata",
            r#"{"hook":true}"#,
        ])
        .unwrap();
        assert!(matches!(
            hook.trace.command,
            TraceCommand::Hook(HookArgs {
                additional_metadata: Some(ref value),
                ..
            }) if value == r#"{"hook":true}"#
        ));

        let run = Cli::try_parse_from([
            "bt",
            "run",
            "--additional-metadata",
            r#"{"run":true}"#,
            "codex",
        ])
        .unwrap();
        assert!(matches!(
            run.trace.command,
            TraceCommand::Run(RunArgs {
                additional_metadata: Some(ref value),
                ..
            }) if value == r#"{"run":true}"#
        ));

        let import = Cli::try_parse_from([
            "bt",
            "import",
            "codex",
            "session-id",
            "--additional-metadata",
            r#"{"import":true}"#,
        ])
        .unwrap();
        assert!(matches!(
            import.trace.command,
            TraceCommand::Import(ImportArgs {
                additional_metadata: Some(ref value),
                ..
            }) if value == r#"{"import":true}"#
        ));
    }

    #[test]
    fn lifecycle_commands_share_agent_aliases() {
        let disable = Cli::try_parse_from(["bt", "disable", "claude-code"]).unwrap();
        assert!(matches!(
            disable.trace.command,
            TraceCommand::Disable(DisableArgs {
                agent: AgentId::Claude
            })
        ));

        let enable = Cli::try_parse_from(["bt", "enable", "open-code"]).unwrap();
        assert!(matches!(
            enable.trace.command,
            TraceCommand::Setup(EnableArgs {
                agent: AgentId::OpenCode,
                ..
            })
        ));
    }

    #[test]
    fn hook_accepts_adapter_provenance() {
        let hook = Cli::try_parse_from([
            "bt",
            "hook",
            "--source",
            "codex",
            "--source-version",
            "1.2.3",
            "--plugin-version",
            "4.5.6",
        ])
        .unwrap();
        assert!(matches!(
            hook.trace.command,
            TraceCommand::Hook(HookArgs {
                source_version: Some(ref source_version),
                plugin_version: Some(ref plugin_version),
                ..
            }) if source_version == "1.2.3" && plugin_version == "4.5.6"
        ));
    }
}
