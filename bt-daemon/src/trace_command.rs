//! Command definitions mounted by the host CLI.
//!
//! Coding-agent command names, aliases, and argument shapes live with the
//! integrations they control. Hosts such as `bt` provide global auth flags and
//! dispatch these commands without duplicating agent-specific CLI knowledge.

use crate::{HookArgs, ImportArgs, RunArgs, ServeArgs, StatusArgs};
use clap::{Args, Subcommand, ValueEnum};
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
    /// Explain the effective tracing configuration for a coding agent.
    Doctor(DoctorArgs),
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
pub struct DoctorArgs {
    /// Coding agent whose tracing configuration should be inspected.
    #[arg(value_enum)]
    pub agent: DoctorAgent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum DoctorAgent {
    Codex,
    #[value(name = "claude", alias = "claude-code")]
    Claude,
    #[value(name = "opencode", alias = "open-code")]
    OpenCode,
    Pi,
}

impl DoctorAgent {
    pub(crate) fn source(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Claude => "claude",
            Self::OpenCode => "opencode",
            Self::Pi => "pi",
        }
    }

    pub(crate) fn display_name(self) -> &'static str {
        match self {
            Self::Codex => "Codex",
            Self::Claude => "Claude Code",
            Self::OpenCode => "OpenCode",
            Self::Pi => "Pi",
        }
    }
}

#[derive(Debug, Clone, Args)]
pub struct EnableArgs {
    #[command(subcommand)]
    pub agent: SetupAgent,
    /// JSON object persisted in this agent's tracing route and merged into root-span metadata.
    #[arg(long, global = true, env = "BRAINTRUST_ADDITIONAL_METADATA")]
    pub additional_metadata: Option<String>,
}

/// Backwards-compatible API name for hosts that mounted the former setup command.
pub type SetupArgs = EnableArgs;

#[derive(Debug, Clone, Args)]
pub struct DisableArgs {
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
    /// Install the Google Antigravity tracing hooks.
    Antigravity,
}

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

        let legacy_setup = Cli::try_parse_from(["bt", "setup", "codex"]).unwrap();
        assert!(matches!(
            legacy_setup.trace.command,
            TraceCommand::Setup(SetupArgs {
                agent: SetupAgent::Codex,
                ..
            })
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
    fn doctor_accepts_every_supported_agent_alias() {
        for (agent, expected) in [
            ("codex", DoctorAgent::Codex),
            ("claude-code", DoctorAgent::Claude),
            ("open-code", DoctorAgent::OpenCode),
            ("pi", DoctorAgent::Pi),
        ] {
            let cli = Cli::try_parse_from(["bt", "doctor", agent]).unwrap();
            assert!(matches!(
                cli.trace.command,
                TraceCommand::Doctor(DoctorArgs { agent }) if agent == expected
            ));
        }
    }

    #[test]
    fn antigravity_uses_shared_enable_and_disable_commands() {
        for command in ["enable", "setup"] {
            let parsed = Cli::try_parse_from(["bt", command, "antigravity"]).unwrap();
            assert!(matches!(
                parsed.trace.command,
                TraceCommand::Setup(SetupArgs {
                    agent: SetupAgent::Antigravity,
                    ..
                })
            ));
        }

        let parsed = Cli::try_parse_from(["bt", "disable", "antigravity"]).unwrap();
        assert!(matches!(
            parsed.trace.command,
            TraceCommand::Disable(DisableArgs {
                agent: SetupAgent::Antigravity
            })
        ));

        assert!(Cli::try_parse_from(["bt", "setup", "antigravity", "--disable"]).is_err());
    }
}
