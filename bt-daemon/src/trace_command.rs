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
    /// JSON object persisted in this agent's tracing route and merged into root-span metadata.
    #[arg(long, global = true, env = "BRAINTRUST_ADDITIONAL_METADATA")]
    pub additional_metadata: Option<String>,
    /// JavaScript span transform to persist for this agent. Repeat to compose
    /// transforms in order.
    #[arg(long, global = true, value_name = "PATH")]
    pub plugin: Vec<PathBuf>,
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
                ..
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
    fn public_commands_preserve_repeated_plugin_order() {
        for args in [
            vec![
                "bt",
                "setup",
                "codex",
                "--plugin",
                "first.mjs",
                "--plugin",
                "second.mjs",
            ],
            vec![
                "bt",
                "run",
                "--plugin",
                "first.mjs",
                "--plugin",
                "second.mjs",
                "codex",
            ],
            vec![
                "bt",
                "import",
                "codex",
                "session",
                "--plugin",
                "first.mjs",
                "--plugin",
                "second.mjs",
            ],
        ] {
            let parsed = Cli::try_parse_from(args).unwrap();
            let plugins = match parsed.trace.command {
                TraceCommand::Setup(args) => args.plugin,
                TraceCommand::Run(args) => args.plugin,
                TraceCommand::Import(args) => args.plugin,
                _ => unreachable!(),
            };
            assert_eq!(
                plugins,
                [PathBuf::from("first.mjs"), PathBuf::from("second.mjs")]
            );
        }
    }
}
