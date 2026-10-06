//! Clap argument types for the daemon's commands. `bt` and the standalone
//! binary embed these in their own command trees.

use braintrust_sdk_rust::{SpanComponents, SpanObjectType};
use clap::{Args, ValueEnum};
use std::ffi::OsString;
use std::path::PathBuf;

use crate::agents::{self, Agent, Identity};
use crate::wire;

/// Arguments for `serve`.
#[derive(Debug, Clone, Args)]
pub struct ServeArgs {
    /// Socket path override (default: see docs/protocol.md).
    #[arg(long)]
    pub socket: Option<PathBuf>,
    /// Data/journal directory override.
    #[arg(long)]
    pub data_dir: Option<PathBuf>,
    /// Exit after this many seconds idle (no activity, empty queues). 0
    /// disables the watchdog.
    #[arg(long, default_value_t = 300)]
    pub idle_timeout_secs: u64,
    /// Retire a session's in-memory state after this many seconds without
    /// traffic. A later event rebuilds it from the journal. 0 disables
    /// retirement.
    #[arg(long, default_value_t = 30)]
    pub session_idle_timeout_secs: u64,
}

/// Arguments for `hook`.
#[derive(Debug, Clone, Args)]
pub struct HookArgs {
    /// Which translator should interpret this event's payload.
    #[arg(long)]
    pub source: String,
    /// Optional agent version, forwarded for payload-drift handling.
    #[arg(long)]
    pub source_version: Option<String>,
    /// Optional instrumentation package version, forwarded independently from
    /// the coding agent version.
    #[arg(long)]
    pub plugin_version: Option<String>,
    /// Socket path override.
    #[arg(long)]
    pub socket: Option<PathBuf>,
    /// JSON field in the payload holding the session id.
    #[arg(long, default_value = "session_id")]
    pub session_id_field: String,
    /// JSON field in the payload holding the event name.
    #[arg(long, default_value = "hook_event_name")]
    pub event_field: String,
    /// Explicit event name (overrides `--event-field` lookup).
    #[arg(long)]
    pub event: Option<String>,
    /// JSON field holding a transcript path. When present, capture the file
    /// length observed by this hook so deterministic journal replay cannot
    /// read transcript records written by later lifecycle events.
    #[arg(long)]
    pub transcript_path_field: Option<String>,
    /// Fail instead of spawning a daemon if none is running.
    #[arg(long)]
    pub no_spawn: bool,
    /// Maximum time to resolve routing, connect or start the daemon, and
    /// receive durable capture acknowledgement.
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    pub capture_timeout_ms: Option<u64>,
    /// Ask the daemon to flush the session after a turn-ending event. The
    /// flush is scheduled out-of-band; hook capture still returns immediately
    /// after the durable journal write.
    #[arg(long)]
    pub flush_on_turn_end: bool,
    /// Deprecated compatibility option. Hook capture never waits for daemon
    /// translation, reporting, or flushing.
    #[arg(long, default_value_t = 10_000, hide = true)]
    pub flush_timeout_ms: u64,
    /// JSON object merged into root-span metadata. Deliberately not read from
    /// the environment: a hook fires automatically on every event, so its
    /// configuration must come only from the persisted agent route (or, for a
    /// managed child, the invocation settings `run` injected).
    #[arg(long)]
    pub additional_metadata: Option<String>,
    /// Marks the hook definition injected by `run`; inherited plugin hooks do
    /// not carry this flag and are suppressed for the managed child.
    #[arg(long, hide = true)]
    pub managed_run_hook: bool,
}

/// Arguments for `status`.
#[derive(Debug, Clone, Args)]
pub struct StatusArgs {
    #[arg(long)]
    pub socket: Option<PathBuf>,
    /// Limit to one session.
    #[arg(long)]
    pub session_id: Option<String>,
}

/// Arguments for importing a past coding-agent session.
#[derive(Debug, Clone, Args)]
pub struct ImportArgs {
    /// Agent that produced the session.
    #[arg(value_enum)]
    pub source: ImportSource,
    /// One or more session ids shown by the agent's resume command.
    #[arg(
        value_name = "SESSION_ID",
        num_args = 1..,
        required_unless_present = "all",
        conflicts_with = "all"
    )]
    pub session_ids: Vec<String>,
    /// Import every locally discoverable session for this agent.
    #[arg(long, conflicts_with = "session_ids")]
    pub all: bool,
    /// Destination object reference, such as `project_logs:<project-id>` or
    /// `experiment:<experiment-id>`.
    #[arg(
        long,
        value_name = "DESTINATION",
        conflicts_with_all = ["parent", "parent_span_id", "parent_root_span_id", "parent_object_type", "parent_object_id", "parent_project"]
    )]
    pub destination: Option<wire::TraceDestination>,
    /// Attach below the opaque value returned by a Braintrust SDK span.export().
    #[arg(
        long,
        value_name = "SPAN_EXPORT",
        conflicts_with_all = ["destination", "parent_span_id", "parent_root_span_id", "parent_object_type", "parent_object_id", "parent_project"]
    )]
    pub parent: Option<SpanComponents>,
    /// Attach below this span ID. Also requires --parent-root-span-id and
    /// --parent-object-type, plus --parent-object-id or --parent-project.
    #[arg(
        long,
        value_name = "SPAN_ID",
        requires_all = ["parent_root_span_id", "parent_object_type"],
        conflicts_with_all = ["destination", "parent"]
    )]
    pub parent_span_id: Option<String>,
    /// Root span ID for --parent-span-id.
    #[arg(long, value_name = "ROOT_SPAN_ID", requires = "parent_span_id")]
    pub parent_root_span_id: Option<String>,
    /// Braintrust object type for --parent-span-id.
    #[arg(long, value_enum, requires = "parent_span_id")]
    pub parent_object_type: Option<ParentObjectType>,
    /// Braintrust object ID for --parent-span-id.
    #[arg(
        long,
        value_name = "OBJECT_ID",
        requires = "parent_span_id",
        conflicts_with = "parent_project"
    )]
    pub parent_object_id: Option<String>,
    /// Braintrust project name for a project-logs parent.
    #[arg(
        long,
        value_name = "PROJECT",
        requires = "parent_span_id",
        conflicts_with = "parent_object_id"
    )]
    pub parent_project: Option<String>,
    /// Keep following the transcript until Ctrl-C, importing new turns as the
    /// coding-agent session grows.
    #[arg(long, conflicts_with = "all")]
    pub attach: bool,
    /// JSON object merged into every imported root span's metadata.
    #[arg(long, env = "BRAINTRUST_ADDITIONAL_METADATA")]
    pub additional_metadata: Option<String>,
    /// Tag applied to each imported root span. May be repeated or comma-separated.
    #[arg(long = "tag", env = "BRAINTRUST_TAGS", value_delimiter = ',')]
    pub tags: Vec<String>,
    /// JavaScript span transform for this import. Repeat to compose an isolated
    /// transform chain; persistent setup plugins are not included.
    #[arg(long, value_name = "PATH")]
    pub plugin: Vec<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ImportSource {
    Codex,
    Cursor,
    #[value(name = "claude", alias = "claude-code")]
    Claude,
    #[value(name = "antigravity", alias = "agy")]
    Antigravity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ParentObjectType {
    Experiment,
    #[value(name = "project_logs", alias = "project-logs")]
    ProjectLogs,
    #[value(name = "playground_logs", alias = "playground-logs")]
    PlaygroundLogs,
}

impl ImportSource {
    pub(crate) fn identity(self) -> &'static Identity {
        match self {
            Self::Codex => agents::Codex.identity(),
            Self::Cursor => agents::Cursor.identity(),
            Self::Claude => agents::Claude.identity(),
            Self::Antigravity => agents::Antigravity.identity(),
        }
    }
}

impl RunSource {
    pub(crate) fn identity(self) -> &'static Identity {
        match self {
            Self::Codex => agents::Codex.identity(),
            Self::Cursor => agents::Cursor.identity(),
            Self::Claude => agents::Claude.identity(),
            Self::OpenCode => agents::OpenCode.identity(),
            Self::Pi => agents::Pi.identity(),
        }
    }
}

impl From<ParentObjectType> for SpanObjectType {
    fn from(value: ParentObjectType) -> Self {
        match value {
            ParentObjectType::Experiment => Self::Experiment,
            ParentObjectType::ProjectLogs => Self::ProjectLogs,
            ParentObjectType::PlaygroundLogs => Self::PlaygroundLogs,
        }
    }
}

impl ImportArgs {
    pub fn has_destination_override(&self) -> bool {
        self.destination.is_some() || self.parent.is_some() || self.parent_span_id.is_some()
    }
}

/// Arguments for launching a coding agent with invocation-local live hooks.
#[derive(Debug, Clone, Args)]
#[command(trailing_var_arg = true)]
pub struct RunArgs {
    /// Coding agent to launch.
    #[arg(value_enum)]
    pub source: RunSource,
    /// JSON object merged into root-span metadata for this invocation.
    #[arg(long, env = "BRAINTRUST_ADDITIONAL_METADATA")]
    pub additional_metadata: Option<String>,
    /// Tag applied to each root span for this invocation. May be repeated or comma-separated.
    #[arg(long = "tag", env = "BRAINTRUST_TAGS", value_delimiter = ',')]
    pub tags: Vec<String>,
    /// JavaScript span transform for this invocation. Repeat to compose an
    /// isolated transform chain; persistent setup plugins are not included.
    #[arg(long, value_name = "PATH")]
    pub plugin: Vec<PathBuf>,
    /// Arguments forwarded verbatim to the coding agent.
    #[arg(allow_hyphen_values = true)]
    pub agent_args: Vec<OsString>,
}

/// Front-end command used by a managed agent run to forward one hook payload.
///
/// The standalone binary uses `[bt-daemon, hook]`; the embedded `bt` front-end
/// uses its own equivalent prefix. `run_traced` appends `--source <agent>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunHookCommand {
    pub program: OsString,
    pub args: Vec<OsString>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum RunSource {
    Codex,
    Cursor,
    #[value(name = "claude", alias = "claude-code")]
    Claude,
    #[value(name = "opencode", alias = "open-code")]
    OpenCode,
    Pi,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Debug, Parser)]
    struct ImportCli {
        #[command(flatten)]
        args: ImportArgs,
    }

    #[derive(Debug, Parser)]
    struct ServeCli {
        #[command(flatten)]
        args: ServeArgs,
    }

    #[test]
    fn serve_defaults_to_short_journal_backed_session_retirement() {
        let args = ServeCli::try_parse_from(["test"]).unwrap().args;
        assert_eq!(args.session_idle_timeout_secs, 30);
    }

    #[test]
    fn import_args_accept_multiple_sessions_or_all() {
        let explicit = ImportCli::try_parse_from([
            "test",
            "codex",
            "session-a",
            "session-b",
            "--destination",
            "project_logs:project-id",
        ])
        .unwrap()
        .args;
        assert_eq!(explicit.session_ids, ["session-a", "session-b"]);
        assert!(!explicit.all);
        assert!(explicit.destination.is_some());

        let all = ImportCli::try_parse_from(["test", "claude", "--all"])
            .unwrap()
            .args;
        assert!(all.session_ids.is_empty());
        assert!(all.all);

        assert!(ImportCli::try_parse_from(["test", "codex"]).is_err());
        assert!(ImportCli::try_parse_from(["test", "codex", "session-a", "--all"]).is_err());
    }

    #[test]
    fn import_parent_rejects_incomplete_or_conflicting_identifiers() {
        assert!(ImportCli::try_parse_from([
            "test",
            "codex",
            "session-a",
            "--parent-span-id",
            "span-1",
        ])
        .is_err());
        assert!(ImportCli::try_parse_from([
            "test",
            "codex",
            "session-a",
            "--destination",
            "project_logs:project-id",
            "--parent-span-id",
            "span-1",
            "--parent-root-span-id",
            "root-1",
            "--parent-object-type",
            "project_logs",
            "--parent-project",
            "Agents",
        ])
        .is_err());
    }
}
