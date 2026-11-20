//! Command definitions mounted by the host CLI.
//!
//! Coding-agent command names, aliases, and argument shapes live with the
//! integrations they control. Hosts such as `bt` provide global auth flags and
//! dispatch these commands without duplicating agent-specific CLI knowledge.

use crate::agents::{self, Agent, Identity};
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
    /// Install and configure the Braintrust tracing plugin for a coding agent.
    #[command(visible_alias = "setup")]
    Install(SetupArgs),
    /// Enable tracing, setting it up when needed or when settings are supplied.
    Enable(EnableArgs),
    /// Disable tracing while preserving the plugin and its configuration.
    Disable(DisableArgs),
    /// Uninstall the tracing plugin or remove the shared project configuration.
    Uninstall(DisableArgs),
    /// Update an already installed Braintrust tracing plugin without changing its configuration.
    Update(UpdateArgs),
    /// Run the tracing daemon (foreground).
    #[command(hide = true)]
    Daemon(ServeArgs),
    /// Capture one coding-agent hook event from stdin; exits nonzero if capture fails.
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
    /// Import a past coding-agent session by its resume id.
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
    #[value(alias = "muse-code")]
    Muse,
    #[value(name = "claude", alias = "claude-code")]
    Claude,
    #[value(name = "opencode", alias = "open-code")]
    OpenCode,
    Pi,
    Antigravity,
    Grok,
    Cursor,
}

impl DoctorAgent {
    pub(crate) fn identity(self) -> &'static Identity {
        match self {
            Self::Codex => agents::Codex.identity(),
            Self::Muse => agents::Muse.identity(),
            Self::Claude => agents::Claude.identity(),
            Self::OpenCode => agents::OpenCode.identity(),
            Self::Pi => agents::Pi.identity(),
            Self::Antigravity => agents::Antigravity.identity(),
            Self::Grok => agents::Grok.identity(),
            Self::Cursor => agents::Cursor.identity(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, ValueEnum)]
pub enum SettingsScope {
    #[default]
    User,
    Project,
}

#[derive(Debug, Clone, Args)]
pub struct SetupArgs {
    /// Install with tracing disabled until explicitly enabled.
    #[arg(long, global = true)]
    pub disable: bool,
    /// Where the tracing settings are stored.
    #[arg(long, global = true, value_enum, default_value_t = SettingsScope::User)]
    pub scope: SettingsScope,
    #[command(subcommand)]
    pub agent: SetupAgent,
    /// JSON object persisted in this agent's tracing route and merged into root-span metadata.
    #[arg(long, global = true)]
    pub additional_metadata: Option<String>,
    /// Tag applied to each root span. May be repeated or comma-separated.
    #[arg(long = "tag", global = true, value_delimiter = ',')]
    pub tags: Vec<String>,
    /// Replace inherited span plugins instead of appending to them.
    #[arg(long, global = true, conflicts_with = "inherit_plugins")]
    pub no_inherit_plugins: bool,
    /// Restore inheritance of user-level span plugins.
    #[arg(long, global = true)]
    pub inherit_plugins: bool,
    /// JavaScript span transform to persist for this agent. Repeat to compose
    /// transforms in order.
    #[arg(long, global = true, value_name = "PATH")]
    pub plugin: Vec<PathBuf>,
}

#[derive(Debug, Clone, Args)]
pub struct EnableArgs {
    /// Where the tracing settings are stored.
    #[arg(long, global = true, value_enum, default_value_t = SettingsScope::User)]
    pub scope: SettingsScope,
    #[command(subcommand)]
    pub agent: SetupAgent,
    /// JSON object merged into root-span metadata.
    #[arg(long, global = true)]
    pub additional_metadata: Option<String>,
    /// Tag applied to each root span. May be repeated or comma-separated.
    #[arg(long = "tag", global = true, value_delimiter = ',')]
    pub tags: Vec<String>,
    /// Replace inherited span plugins instead of appending to them.
    #[arg(long, global = true, conflicts_with = "inherit_plugins")]
    pub no_inherit_plugins: bool,
    /// Restore inheritance of user-level span plugins.
    #[arg(long, global = true)]
    pub inherit_plugins: bool,
    /// JavaScript span transform to persist. May be repeated.
    #[arg(long, global = true, value_name = "PATH")]
    pub plugin: Vec<PathBuf>,
}

#[derive(Debug, Clone, Args)]
pub struct DisableArgs {
    /// Where the tracing settings are stored.
    #[arg(long, global = true, value_enum, default_value_t = SettingsScope::User)]
    pub scope: SettingsScope,
    #[command(subcommand)]
    pub agent: SetupAgent,
}

#[derive(Debug, Clone, Args)]
pub struct UpdateArgs {
    #[command(subcommand)]
    pub agent: SetupAgent,
}

#[derive(Debug, Clone, Copy, Subcommand)]
pub enum SetupAgent {
    /// Install the published Codex tracing plugin.
    Codex,
    /// Install the Muse Code tracing hooks.
    #[command(alias = "muse-code")]
    Muse,
    /// Install the published Claude Code tracing plugin.
    Claude,
    /// Configure the published OpenCode tracing plugin.
    #[command(name = "opencode", alias = "open-code")]
    OpenCode,
    /// Install the published Pi tracing extension.
    Pi,
    /// Install the published Grok tracing plugin.
    Grok,
    /// Install the published Cursor tracing plugin.
    Cursor,
    /// Install the Google Antigravity tracing hooks.
    Antigravity,
}

impl SetupAgent {
    pub(crate) fn identity(self) -> &'static Identity {
        match self {
            Self::Codex => agents::Codex.identity(),
            Self::Muse => agents::Muse.identity(),
            Self::Claude => agents::Claude.identity(),
            Self::OpenCode => agents::OpenCode.identity(),
            Self::Pi => agents::Pi.identity(),
            Self::Grok => agents::Grok.identity(),
            Self::Cursor => agents::Cursor.identity(),
            Self::Antigravity => agents::Antigravity.identity(),
        }
    }
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
            "install",
            "claude",
            "--additional-metadata",
            r#"{"setup":true}"#,
        ])
        .unwrap();
        assert!(matches!(
            setup.trace.command,
            TraceCommand::Install(SetupArgs {
                agent: SetupAgent::Claude,
                additional_metadata: Some(ref value),
                ..
            }) if value == r#"{"setup":true}"#
        ));

        let legacy_setup = Cli::try_parse_from(["bt", "setup", "codex"]).unwrap();
        assert!(matches!(
            legacy_setup.trace.command,
            TraceCommand::Install(SetupArgs {
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

        let cursor_import = Cli::try_parse_from(["bt", "import", "cursor", "session-id"]).unwrap();
        assert!(matches!(
            cursor_import.trace.command,
            TraceCommand::Import(ImportArgs {
                source: crate::ImportSource::Cursor,
                ref session_ids,
                ..
            }) if session_ids == &["session-id"]
        ));
    }

    #[test]
    fn setup_run_and_import_accept_tags() {
        let setup =
            Cli::try_parse_from(["bt", "setup", "claude", "--tag", "ci", "--tag", "docs"]).unwrap();
        assert!(matches!(
            setup.trace.command,
            TraceCommand::Install(SetupArgs { ref tags, .. }) if tags.as_slice() == ["ci", "docs"]
        ));

        let run = Cli::try_parse_from(["bt", "run", "codex", "--tag", "ci,docs", "--", "status"])
            .unwrap();
        assert!(matches!(
            run.trace.command,
            TraceCommand::Run(RunArgs { ref tags, .. }) if tags.as_slice() == ["ci", "docs"]
        ));

        let import = Cli::try_parse_from([
            "bt",
            "import",
            "claude",
            "session-id",
            "--tag",
            "historical",
        ])
        .unwrap();
        assert!(matches!(
            import.trace.command,
            TraceCommand::Import(ImportArgs { ref tags, .. }) if tags.as_slice() == ["historical"]
        ));
    }

    #[test]
    fn run_accepts_cursor_with_parent_and_forwards_agent_arguments() {
        let parent: braintrust_sdk_rust::SpanComponents =
            serde_json::from_value(serde_json::json!({
                "object_type": 2,
                "object_id": "project-id",
                "span_id": "parent-span",
                "root_span_id": "trace-root"
            }))
            .unwrap();
        let exported = parent.to_str();
        let parsed = Cli::try_parse_from([
            "bt", "run", "cursor", "--parent", &exported, "--tag", "ci", "--", "--print", "hello",
        ])
        .unwrap();
        assert!(matches!(
            parsed.trace.command,
            TraceCommand::Run(RunArgs {
                inherit: false,
                source: crate::RunSource::Cursor,
                parent: Some(ref parent),
                ref agent_args,
                ref tags,
                ..
            }) if parent.to_str() == exported && agent_args == &["--print", "hello"] && tags == &["ci"]
        ));
        assert!(Cli::try_parse_from(["bt", "run", "--parent", "invalid", "cursor"]).is_err());
    }

    #[test]
    fn doctor_accepts_every_supported_agent_alias() {
        for (agent, expected, source, display_name) in [
            ("codex", DoctorAgent::Codex, "codex", "Codex"),
            ("claude-code", DoctorAgent::Claude, "claude", "Claude Code"),
            ("open-code", DoctorAgent::OpenCode, "opencode", "OpenCode"),
            ("pi", DoctorAgent::Pi, "pi", "Pi"),
            (
                "antigravity",
                DoctorAgent::Antigravity,
                "antigravity",
                "Google Antigravity",
            ),
            ("grok", DoctorAgent::Grok, "grok", "Grok"),
            ("cursor", DoctorAgent::Cursor, "cursor", "Cursor"),
        ] {
            let cli = Cli::try_parse_from(["bt", "doctor", agent]).unwrap();
            assert!(matches!(
                cli.trace.command,
                TraceCommand::Doctor(DoctorArgs { agent }) if agent == expected
            ));
            assert_eq!(expected.identity().id, source);
            assert_eq!(expected.identity().display_name, display_name);
        }
    }

    #[test]
    fn antigravity_uses_shared_enable_and_disable_commands() {
        {
            let parsed = Cli::try_parse_from(["bt", "setup", "antigravity"]).unwrap();
            assert!(matches!(
                parsed.trace.command,
                TraceCommand::Install(SetupArgs {
                    agent: SetupAgent::Antigravity,
                    ..
                })
            ));
        }

        let parsed = Cli::try_parse_from(["bt", "disable", "antigravity"]).unwrap();
        assert!(matches!(
            parsed.trace.command,
            TraceCommand::Disable(DisableArgs {
                agent: SetupAgent::Antigravity,
                ..
            })
        ));

        assert!(Cli::try_parse_from(["bt", "install", "antigravity", "--disable"]).is_ok());
    }

    #[test]
    fn grok_uses_shared_enable_and_disable_commands() {
        {
            let parsed = Cli::try_parse_from(["bt", "setup", "grok"]).unwrap();
            assert!(matches!(
                parsed.trace.command,
                TraceCommand::Install(SetupArgs {
                    agent: SetupAgent::Grok,
                    ..
                })
            ));
        }

        let parsed = Cli::try_parse_from(["bt", "disable", "grok"]).unwrap();
        assert!(matches!(
            parsed.trace.command,
            TraceCommand::Disable(DisableArgs {
                agent: SetupAgent::Grok,
                ..
            })
        ));
    }

    #[test]
    fn scoped_commands_accept_project_before_or_after_agent() {
        for command in ["install", "setup", "enable", "disable", "uninstall"] {
            for args in [
                vec!["bt", command, "--scope", "project", "claude"],
                vec!["bt", command, "claude", "--scope", "project"],
            ] {
                let parsed = Cli::try_parse_from(args).unwrap();
                let scope = match parsed.trace.command {
                    TraceCommand::Install(args) => args.scope,
                    TraceCommand::Enable(args) => args.scope,
                    TraceCommand::Disable(args) | TraceCommand::Uninstall(args) => args.scope,
                    _ => panic!("wrong command"),
                };
                assert_eq!(scope, SettingsScope::Project);
            }
        }
    }

    #[test]
    fn project_plugin_inheritance_flags_are_exclusive() {
        assert!(Cli::try_parse_from([
            "bt",
            "install",
            "claude",
            "--scope",
            "project",
            "--no-inherit-plugins"
        ])
        .is_ok());
        assert!(Cli::try_parse_from([
            "bt",
            "enable",
            "claude",
            "--scope",
            "project",
            "--inherit-plugins"
        ])
        .is_ok());
        assert!(Cli::try_parse_from([
            "bt",
            "install",
            "claude",
            "--no-inherit-plugins",
            "--inherit-plugins"
        ])
        .is_err());
        assert!(
            Cli::try_parse_from(["bt", "run", "codex", "--inherit", "--no-inherit-plugins"])
                .is_ok()
        );
    }

    #[test]
    fn enable_accepts_explicit_tracing_settings() {
        let parsed = Cli::try_parse_from(["bt", "enable", "pi"]).unwrap();
        assert!(matches!(
            parsed.trace.command,
            TraceCommand::Enable(EnableArgs {
                agent: SetupAgent::Pi,
                ..
            })
        ));
        assert!(Cli::try_parse_from(["bt", "enable", "pi", "--tag", "changed"]).is_ok());
    }

    #[test]
    fn uninstall_accepts_an_agent() {
        let parsed = Cli::try_parse_from(["bt", "uninstall", "pi"]).unwrap();
        assert!(matches!(
            parsed.trace.command,
            TraceCommand::Uninstall(DisableArgs {
                agent: SetupAgent::Pi,
                ..
            })
        ));
    }

    #[test]
    fn update_accepts_every_setup_agent() {
        for agent in [
            "codex",
            "claude",
            "opencode",
            "pi",
            "grok",
            "cursor",
            "antigravity",
        ] {
            assert!(matches!(
                Cli::try_parse_from(["bt", "update", agent])
                    .unwrap()
                    .trace
                    .command,
                TraceCommand::Update(_)
            ));
        }
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
                TraceCommand::Install(args) => args.plugin,
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
