use bt_daemon::wire::{CaptureContext, Envelope, ProcessIdentity, SessionRoute, TraceDestination};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AgentKind {
    Claude,
    Codex,
    Pi,
    OpenCode,
}

impl AgentKind {
    pub const ALL: [Self; 4] = [Self::Claude, Self::Codex, Self::Pi, Self::OpenCode];

    pub fn label(self) -> &'static str {
        match self {
            Self::Claude => "claude",
            Self::Codex => "codex",
            Self::Pi => "pi",
            Self::OpenCode => "opencode",
        }
    }

    fn source(self) -> &'static str {
        match self {
            Self::Claude => "claude-code",
            Self::Codex => "codex",
            Self::Pi => "pi",
            Self::OpenCode => "opencode",
        }
    }

    fn uses_command_hook(self) -> bool {
        matches!(self, Self::Claude | Self::Codex)
    }
}

#[derive(Clone, Debug)]
pub struct ProcessTree {
    pub agent: ProcessIdentity,
    pub ancestors: Vec<ProcessIdentity>,
}

impl ProcessTree {
    pub fn root(pid: u32) -> Self {
        Self {
            agent: process(pid),
            ancestors: Vec::new(),
        }
    }

    pub fn child(pid: u32, shell_pid: u32, parent: &Self) -> Self {
        let mut ancestors = vec![process(shell_pid), parent.agent.clone()];
        ancestors.extend(parent.ancestors.iter().cloned());
        Self {
            agent: process(pid),
            ancestors,
        }
    }

    fn capture(&self, kind: AgentKind, hook_pid: u32) -> CaptureContext {
        let mut process_chain = Vec::new();
        if kind.uses_command_hook() {
            process_chain.push(process(hook_pid));
        }
        process_chain.push(self.agent.clone());
        process_chain.extend(self.ancestors.iter().cloned());
        CaptureContext { process_chain }
    }
}

fn process(pid: u32) -> ProcessIdentity {
    ProcessIdentity {
        pid,
        // Test PIDs are synthetic, so use a stable nonzero boot-relative value
        // to exercise the same PID-reuse-safe identity used by live capture.
        start_time_secs: 1_700_000_000 + u64::from(pid),
    }
}

/// Produces compact but agent-native lifecycle streams. Codex is deliberately
/// backed by a growing rollout mirror because its live translator learns tool
/// lifecycle from transcript records rather than hook payloads alone.
pub struct DistributedFixtures {
    root: tempfile::TempDir,
    codex_transcripts: HashMap<String, CodexTranscript>,
    next_hook_pid: u32,
}

struct CodexTranscript {
    path: PathBuf,
    bytes: u64,
}

impl DistributedFixtures {
    pub fn new() -> Self {
        Self {
            root: tempfile::tempdir().expect("create distributed fixture directory"),
            codex_transcripts: HashMap::new(),
            next_hook_pid: 50_000,
        }
    }

    pub fn start_turn(
        &mut self,
        kind: AgentKind,
        session: &str,
        tree: &ProcessTree,
        prompt: &str,
        ts_ms: i64,
    ) -> Vec<Envelope> {
        match kind {
            AgentKind::Claude => vec![
                self.envelope(
                    kind,
                    session,
                    tree,
                    "SessionStart",
                    ts_ms,
                    json!({
                        "session_id": session,
                        "hook_event_name": "SessionStart",
                        "cwd": "/tmp/distributed",
                        "source": "startup"
                    }),
                ),
                self.envelope(
                    kind,
                    session,
                    tree,
                    "UserPromptSubmit",
                    ts_ms + 1,
                    json!({
                        "session_id": session,
                        "hook_event_name": "UserPromptSubmit",
                        "cwd": "/tmp/distributed",
                        "prompt": prompt
                    }),
                ),
            ],
            AgentKind::Codex => {
                let records = [
                    json!({"timestamp":iso(ts_ms),"type":"session_meta","payload":{"id":session,"cwd":"/tmp/distributed","cli_version":"test"}}),
                    json!({"timestamp":iso(ts_ms + 1),"type":"turn_context","payload":{"model":"mock-model"}}),
                    json!({"timestamp":iso(ts_ms + 2),"type":"event_msg","payload":{"type":"task_started","turn_id":"turn-1"}}),
                    json!({"timestamp":iso(ts_ms + 3),"type":"event_msg","payload":{"type":"user_message","message":prompt}}),
                ];
                self.append_codex(session, &records);
                let payload = self.codex_payload(session, "SessionStart", Value::Null);
                vec![self.envelope(kind, session, tree, "SessionStart", ts_ms + 3, payload)]
            }
            AgentKind::Pi => vec![
                self.envelope(
                    kind,
                    session,
                    tree,
                    "session_start",
                    ts_ms,
                    pi_payload(session, json!({"reason":"new"})),
                ),
                self.envelope(
                    kind,
                    session,
                    tree,
                    "before_agent_start",
                    ts_ms + 1,
                    pi_payload(session, json!({"prompt":prompt})),
                ),
            ],
            AgentKind::OpenCode => vec![
                self.envelope(
                    kind,
                    session,
                    tree,
                    "session.created",
                    ts_ms,
                    json!({"properties":{"info":{"id":session,"title":"Distributed test"}}}),
                ),
                self.envelope(
                    kind,
                    session,
                    tree,
                    "chat.message",
                    ts_ms + 1,
                    json!({
                        "input":{"sessionID":session,"model":{"modelID":"mock-model"}},
                        "output":{"parts":[{"type":"text","text":prompt}]}
                    }),
                ),
            ],
        }
    }

    pub fn open_tool(
        &mut self,
        kind: AgentKind,
        session: &str,
        tree: &ProcessTree,
        call_id: &str,
        delegated_prompt: &str,
        ts_ms: i64,
    ) -> Envelope {
        let command = format!("agent --prompt {delegated_prompt}");
        match kind {
            AgentKind::Claude => self.envelope(
                kind,
                session,
                tree,
                "PreToolUse",
                ts_ms,
                json!({
                    "session_id":session,
                    "hook_event_name":"PreToolUse",
                    "cwd":"/tmp/distributed",
                    "tool_name":"Bash",
                    "tool_use_id":call_id,
                    "tool_input":{"command":command}
                }),
            ),
            AgentKind::Codex => {
                self.append_codex(
                    session,
                    &[json!({
                        "timestamp":iso(ts_ms),
                        "type":"response_item",
                        "payload":{"type":"function_call","call_id":call_id,"name":"exec_command","arguments":json!({"cmd":command}).to_string()}
                    })],
                );
                let payload = self.codex_payload(
                    session,
                    "PreToolUse",
                    json!({
                        "tool_name":"exec_command",
                        "tool_use_id":call_id,
                        "tool_input":{"cmd":command}
                    }),
                );
                self.envelope(kind, session, tree, "PreToolUse", ts_ms, payload)
            }
            AgentKind::Pi => self.envelope(
                kind,
                session,
                tree,
                "tool_execution_start",
                ts_ms,
                pi_payload(
                    session,
                    json!({"toolCallId":call_id,"toolName":"bash","args":{"command":command}}),
                ),
            ),
            AgentKind::OpenCode => self.envelope(
                kind,
                session,
                tree,
                "tool.execute.before",
                ts_ms,
                json!({
                    "input":{"sessionID":session,"callID":call_id,"tool":"bash"},
                    "output":{"args":{"command":command}}
                }),
            ),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn close_tool(
        &mut self,
        kind: AgentKind,
        session: &str,
        tree: &ProcessTree,
        call_id: &str,
        delegated_prompt: &str,
        output: &str,
        ts_ms: i64,
    ) -> Envelope {
        let command = format!("agent --prompt {delegated_prompt}");
        match kind {
            AgentKind::Claude => self.envelope(
                kind,
                session,
                tree,
                "PostToolUse",
                ts_ms,
                json!({
                    "session_id":session,
                    "hook_event_name":"PostToolUse",
                    "cwd":"/tmp/distributed",
                    "tool_name":"Bash",
                    "tool_use_id":call_id,
                    "tool_input":{"command":command},
                    "tool_response":{"stdout":output,"stderr":"","interrupted":false}
                }),
            ),
            AgentKind::Codex => {
                self.append_codex(
                    session,
                    &[json!({
                        "timestamp":iso(ts_ms),
                        "type":"response_item",
                        "payload":{"type":"function_call_output","call_id":call_id,"output":output}
                    })],
                );
                let payload = self.codex_payload(
                    session,
                    "PostToolUse",
                    json!({
                        "tool_name":"exec_command",
                        "tool_use_id":call_id,
                        "tool_input":{"cmd":command},
                        "tool_response":{"output":output}
                    }),
                );
                self.envelope(kind, session, tree, "PostToolUse", ts_ms, payload)
            }
            AgentKind::Pi => self.envelope(
                kind,
                session,
                tree,
                "tool_execution_end",
                ts_ms,
                pi_payload(
                    session,
                    json!({"toolCallId":call_id,"toolName":"bash","result":output,"isError":false}),
                ),
            ),
            AgentKind::OpenCode => self.envelope(
                kind,
                session,
                tree,
                "tool.execute.after",
                ts_ms,
                json!({
                    "input":{"sessionID":session,"callID":call_id,"tool":"bash"},
                    "result":{"title":"Bash","output":output}
                }),
            ),
        }
    }

    pub fn close_session(
        &mut self,
        kind: AgentKind,
        session: &str,
        tree: &ProcessTree,
        output: &str,
        ts_ms: i64,
    ) -> Envelope {
        match kind {
            AgentKind::Claude => self.envelope(
                kind,
                session,
                tree,
                "Stop",
                ts_ms,
                json!({
                    "session_id":session,
                    "hook_event_name":"Stop",
                    "cwd":"/tmp/distributed",
                    "last_assistant_message":output
                }),
            ),
            AgentKind::Codex => {
                self.append_codex(
                    session,
                    &[json!({
                        "timestamp":iso(ts_ms),
                        "type":"event_msg",
                        "payload":{"type":"task_complete","last_agent_message":output}
                    })],
                );
                let payload = self.codex_payload(session, "Stop", Value::Null);
                self.envelope(kind, session, tree, "Stop", ts_ms, payload)
            }
            AgentKind::Pi => self.envelope(
                kind,
                session,
                tree,
                "agent_end",
                ts_ms,
                pi_payload(session, json!({"messages":[]})),
            ),
            AgentKind::OpenCode => self.envelope(
                kind,
                session,
                tree,
                "session.deleted",
                ts_ms,
                json!({"properties":{"sessionID":session}}),
            ),
        }
    }

    fn envelope(
        &mut self,
        kind: AgentKind,
        session: &str,
        tree: &ProcessTree,
        event: &str,
        ts_ms: i64,
        payload: Value,
    ) -> Envelope {
        let hook_pid = self.next_hook_pid;
        self.next_hook_pid += 1;
        Envelope {
            source: kind.source().into(),
            source_version: Some("integration-test".into()),
            plugin_version: Some("integration-test".into()),
            session_id: session.into(),
            event: event.into(),
            ts_ms,
            managed_run_id: None,
            capture: Some(tree.capture(kind, hook_pid)),
            payload,
            route: Some(test_route()),
            config: None,
        }
    }

    fn append_codex(&mut self, session: &str, records: &[Value]) {
        let transcript = self
            .codex_transcripts
            .entry(session.into())
            .or_insert_with(|| CodexTranscript {
                path: self.root.path().join(format!("{session}.jsonl")),
                bytes: 0,
            });
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&transcript.path)
            .expect("open Codex fixture transcript");
        for record in records {
            let line = serde_json::to_string(record).expect("serialize Codex fixture record");
            writeln!(file, "{line}").expect("append Codex fixture record");
            transcript.bytes += line.len() as u64 + 1;
        }
    }

    fn codex_payload(&self, session: &str, event: &str, extra: Value) -> Value {
        let transcript = self
            .codex_transcripts
            .get(session)
            .expect("Codex fixture transcript exists");
        let path = transcript.path.to_string_lossy();
        let mut payload = json!({
            "session_id":session,
            "hook_event_name":event,
            "transcript_path":path,
            "_bt_transcript_mirror":{
                "path":path,
                "mirror":path,
                "through":transcript.bytes
            }
        });
        if let (Value::Object(payload), Value::Object(extra)) = (&mut payload, extra) {
            payload.extend(extra);
        }
        payload
    }
}

fn pi_payload(session: &str, event: Value) -> Value {
    json!({
        "event":event,
        "extension_version":"integration-test",
        "native_session_id":session,
        "cwd":"/tmp/distributed"
    })
}

fn test_route() -> SessionRoute {
    SessionRoute {
        destination: Some(TraceDestination::ProjectLogs {
            project_id: None,
            project_name: Some("distributed-tracing-test".into()),
        }),
        ..SessionRoute::default()
    }
}

fn iso(ts_ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ts_ms)
        .expect("fixture timestamp")
        .to_rfc3339()
}
