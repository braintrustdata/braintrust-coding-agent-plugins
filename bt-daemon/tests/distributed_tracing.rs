//! Deterministic cross-agent linkage through the complete daemon pipeline.
//!
//! These tests intentionally use native agent envelopes rather than invoking
//! installed coding-agent binaries. That keeps the complete 4x4 compatibility
//! matrix in the ordinary test suite while still exercising IPC, journaling,
//! translation, automatic routing, and sink configuration.

mod support;

use async_trait::async_trait;
use bt_daemon::wire::{AuthSelection, BackendAuth, Envelope, SessionConfig, TraceDestination};
use bt_daemon::{
    flush_session, forward_envelope, run_serve, run_status, shutdown_daemon, AuthLease,
    AuthProvider, AuthResolveReason, HostInfo, Registry, ServeArgs, ServeOptions, Sink,
    SinkFactory, SpanOp, SpanRow, SpanType, StatusArgs,
};
use std::collections::HashMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use support::distributed::{AgentKind, DistributedFixtures, ProcessTree};

#[derive(Default)]
struct RecordedSession {
    source: String,
    configs: Mutex<Vec<SessionConfig>>,
    ops: Mutex<Vec<SpanOp>>,
}

#[derive(Default)]
struct RecordingSinkFactory {
    sessions: Mutex<HashMap<String, Arc<RecordedSession>>>,
}

impl RecordingSinkFactory {
    fn session(&self, session_id: &str) -> Arc<RecordedSession> {
        self.sessions
            .lock()
            .unwrap()
            .get(session_id)
            .unwrap_or_else(|| panic!("no sink created for session {session_id}"))
            .clone()
    }
}

impl SinkFactory for RecordingSinkFactory {
    fn create(
        &self,
        session_id: &str,
        source: &str,
        _plugin_version: Option<&str>,
    ) -> anyhow::Result<Box<dyn Sink>> {
        let record = Arc::new(RecordedSession {
            source: source.into(),
            ..RecordedSession::default()
        });
        self.sessions
            .lock()
            .unwrap()
            .insert(session_id.into(), record.clone());
        Ok(Box::new(RecordingSink { record }))
    }
}

struct RecordingSink {
    record: Arc<RecordedSession>,
}

#[async_trait]
impl Sink for RecordingSink {
    fn configure(&mut self, config: &SessionConfig) {
        self.record.configs.lock().unwrap().push(config.clone());
    }

    async fn emit(&mut self, ops: &[SpanOp]) -> anyhow::Result<u64> {
        self.record.ops.lock().unwrap().extend_from_slice(ops);
        Ok(ops.len() as u64)
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

struct TestAuthProvider;

#[async_trait]
impl AuthProvider for TestAuthProvider {
    async fn resolve(
        &self,
        selection: &AuthSelection,
        _reason: AuthResolveReason,
    ) -> anyhow::Result<AuthLease> {
        Ok(AuthLease {
            selection: selection.clone().canonicalized()?,
            auth: BackendAuth {
                token: "test-token".into(),
                api_url: Some("http://127.0.0.1.invalid".into()),
                app_url: Some("http://127.0.0.1.invalid".into()),
                org_name: Some("test".into()),
                org_id: Some("test-org".into()),
            },
            expires_at_ms: None,
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_instrumented_parent_child_agent_pair_shares_one_trace() {
    let (socket, daemon, recording, _tmp) = start_daemon().await;
    let host = HostInfo {
        serve_argv: vec![OsString::from("unused")],
        version: "test".into(),
    };
    let mut fixtures = DistributedFixtures::new();
    let mut pair_index = 0_u32;

    for parent_kind in AgentKind::ALL {
        for child_kind in AgentKind::ALL {
            pair_index += 1;
            let label = format!("{}-to-{}", parent_kind.label(), child_kind.label());
            let parent_session = format!("parent-{label}");
            let child_session = format!("child-{label}");
            let call_id = format!("call-{label}");
            let delegated_prompt = format!("LINK_PROMPT_{label}");
            let child_output = format!("LINK_OUTPUT_{label}");
            let base_pid = 1_000 + pair_index * 10;
            let parent_tree = ProcessTree::root(base_pid);
            let child_tree = ProcessTree::child(base_pid + 2, base_pid + 1, &parent_tree);
            let base_ts = 1_700_000_000_000 + i64::from(pair_index) * 1_000;

            forward_all(
                &mut fixtures.start_turn(
                    parent_kind,
                    &parent_session,
                    &parent_tree,
                    "Delegate the marked task.",
                    base_ts,
                ),
                &socket,
                &host,
            )
            .await;
            forward(
                fixtures.open_tool(
                    parent_kind,
                    &parent_session,
                    &parent_tree,
                    &call_id,
                    &delegated_prompt,
                    base_ts + 10,
                ),
                &socket,
                &host,
            )
            .await;

            // The child starts immediately after the blocking start-tool hook.
            // No parent flush is inserted here: forwarding the start event must
            // not return until its correlation marker is visible.
            forward_all(
                &mut fixtures.start_turn(
                    child_kind,
                    &child_session,
                    &child_tree,
                    &delegated_prompt,
                    base_ts + 20,
                ),
                &socket,
                &host,
            )
            .await;
            forward(
                fixtures.close_session(
                    child_kind,
                    &child_session,
                    &child_tree,
                    &child_output,
                    base_ts + 30,
                ),
                &socket,
                &host,
            )
            .await;
            forward(
                fixtures.close_tool(
                    parent_kind,
                    &parent_session,
                    &parent_tree,
                    &call_id,
                    &delegated_prompt,
                    &child_output,
                    base_ts + 40,
                ),
                &socket,
                &host,
            )
            .await;
            forward(
                fixtures.close_session(
                    parent_kind,
                    &parent_session,
                    &parent_tree,
                    "Parent complete",
                    base_ts + 50,
                ),
                &socket,
                &host,
            )
            .await;

            flush(&child_session, &socket).await;
            flush(&parent_session, &socket).await;
            assert_pair_linked(recording.as_ref(), &parent_session, &child_session, &label);
        }
    }

    shutdown_daemon(&socket).await.unwrap();
    daemon.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_agent_pair_links_when_the_daemon_restarts_between_spawn_and_child_start() {
    let host = HostInfo {
        serve_argv: vec![OsString::from("unused")],
        version: "test".into(),
    };
    let mut fixtures = DistributedFixtures::new();
    let mut pair_index = 0_u32;

    for parent_kind in AgentKind::ALL {
        for child_kind in AgentKind::ALL {
            pair_index += 1;
            let tmp = tempfile::tempdir().unwrap();
            let data_dir = tmp.path().join("data");
            let socket = test_endpoint(tmp.path());
            let recording = Arc::new(RecordingSinkFactory::default());
            let label = format!("restart-{}-to-{}", parent_kind.label(), child_kind.label());
            let parent_session = format!("parent-{label}");
            let child_session = format!("child-{label}");
            let call_id = format!("call-{label}");
            let delegated_prompt = format!("DURABLE_LINK_PROMPT_{label}");
            let base_pid = 30_000 + pair_index * 10;
            let parent_tree = ProcessTree::root(base_pid);
            let child_tree = ProcessTree::child(base_pid + 2, base_pid + 1, &parent_tree);
            let base_ts = 1_702_000_000_000 + i64::from(pair_index) * 1_000;

            let first = spawn_daemon(socket.clone(), data_dir.clone(), recording.clone()).await;
            forward_all(
                &mut fixtures.start_turn(
                    parent_kind,
                    &parent_session,
                    &parent_tree,
                    "Delegate across a daemon restart.",
                    base_ts,
                ),
                &socket,
                &host,
            )
            .await;
            forward(
                fixtures.open_tool(
                    parent_kind,
                    &parent_session,
                    &parent_tree,
                    &call_id,
                    &delegated_prompt,
                    base_ts + 10,
                ),
                &socket,
                &host,
            )
            .await;

            let parent_snapshot_dir = data_dir.join("correlation").join("parents");
            let mut snapshots = tokio::fs::read_dir(&parent_snapshot_dir).await.unwrap();
            let snapshot = snapshots
                .next_entry()
                .await
                .unwrap()
                .expect("active snapshot");
            let bytes = tokio::fs::read(snapshot.path()).await.unwrap();
            let serialized = String::from_utf8(bytes).unwrap();
            assert!(!serialized.contains(&delegated_prompt));
            assert!(!serialized.contains("test-token"));

            shutdown_daemon(&socket).await.unwrap();
            first.await.unwrap();

            let second = spawn_daemon(socket.clone(), data_dir, recording.clone()).await;
            forward_all(
                &mut fixtures.start_turn(
                    child_kind,
                    &child_session,
                    &child_tree,
                    &delegated_prompt,
                    base_ts + 20,
                ),
                &socket,
                &host,
            )
            .await;
            forward(
                fixtures.close_session(
                    child_kind,
                    &child_session,
                    &child_tree,
                    "Child completed after daemon restart.",
                    base_ts + 30,
                ),
                &socket,
                &host,
            )
            .await;
            flush(&child_session, &socket).await;
            assert_pair_linked(recording.as_ref(), &parent_session, &child_session, &label);

            shutdown_daemon(&socket).await.unwrap();
            second.await.unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_recursive_hierarchy_survives_a_restart_at_every_spawn_boundary() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("data");
    let socket = test_endpoint(tmp.path());
    let recording = Arc::new(RecordingSinkFactory::default());
    let host = HostInfo {
        serve_argv: vec![OsString::from("unused")],
        version: "test".into(),
    };
    let kinds = AgentKind::ALL;
    let sessions = [
        "durable-recursive-claude",
        "durable-recursive-codex",
        "durable-recursive-pi",
        "durable-recursive-opencode",
    ];
    let prompts = [
        "durable recursive level one",
        "durable recursive level two",
        "durable recursive level three",
    ];
    let trees = [
        ProcessTree::root(35_000),
        ProcessTree::child(35_002, 35_001, &ProcessTree::root(35_000)),
        ProcessTree::child(
            35_004,
            35_003,
            &ProcessTree::child(35_002, 35_001, &ProcessTree::root(35_000)),
        ),
        ProcessTree::child(
            35_006,
            35_005,
            &ProcessTree::child(
                35_004,
                35_003,
                &ProcessTree::child(35_002, 35_001, &ProcessTree::root(35_000)),
            ),
        ),
    ];
    let mut fixtures = DistributedFixtures::new();

    for level in 0..kinds.len() {
        let daemon = spawn_daemon(socket.clone(), data_dir.clone(), recording.clone()).await;
        let prompt = if level == 0 {
            "begin durable recursive hierarchy"
        } else {
            prompts[level - 1]
        };
        forward_all(
            &mut fixtures.start_turn(
                kinds[level],
                sessions[level],
                &trees[level],
                prompt,
                1_704_000_000_000 + level as i64 * 100,
            ),
            &socket,
            &host,
        )
        .await;
        if level < prompts.len() {
            forward(
                fixtures.open_tool(
                    kinds[level],
                    sessions[level],
                    &trees[level],
                    &format!("durable-recursive-call-{level}"),
                    prompts[level],
                    1_704_000_000_010 + level as i64 * 100,
                ),
                &socket,
                &host,
            )
            .await;
        } else {
            forward(
                fixtures.close_session(
                    kinds[level],
                    sessions[level],
                    &trees[level],
                    "deepest child complete",
                    1_704_000_000_010 + level as i64 * 100,
                ),
                &socket,
                &host,
            )
            .await;
            flush(sessions[level], &socket).await;
        }
        shutdown_daemon(&socket).await.unwrap();
        daemon.await.unwrap();
    }

    let root = recording.session(sessions[0]);
    let root_id = session_root(&inserted_rows(&root), sessions[0], "durable recursive root")
        .root_span_id
        .clone();
    for level in 0..prompts.len() {
        let parent = recording.session(sessions[level]);
        let tool = inserted_rows(&parent)
            .into_iter()
            .find(|row| {
                row.span_type == SpanType::Tool
                    && row
                        .input
                        .as_ref()
                        .is_some_and(|input| input.to_string().contains(prompts[level]))
            })
            .unwrap_or_else(|| panic!("durable recursive level {level}: missing tool"));
        let child = recording.session(sessions[level + 1]);
        let child_root = session_root(
            &inserted_rows(&child),
            sessions[level + 1],
            "durable recursive child",
        )
        .clone();
        assert_eq!(child_root.parent_span_ids, vec![tool.span_id.clone()]);
        let configs = child.configs.lock().unwrap();
        let components = configs
            .iter()
            .find_map(|config| match &config.destination {
                Some(TraceDestination::ParentSpan { components }) => Some(components),
                _ => None,
            })
            .unwrap_or_else(|| panic!("durable recursive level {level}: child not attached"));
        assert_eq!(components.span_id.as_deref(), Some(tool.span_id.as_str()));
        assert_eq!(components.root_span_id.as_deref(), Some(root_id.as_str()));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_tools_are_disambiguated_by_prompt_fingerprints() {
    let (socket, daemon, recording, _tmp) = start_daemon().await;
    let host = HostInfo {
        serve_argv: vec![OsString::from("unused")],
        version: "test".into(),
    };
    let mut fixtures = DistributedFixtures::new();
    let parent_tree = ProcessTree::root(9_000);
    let child_tree = ProcessTree::child(9_002, 9_001, &parent_tree);
    let parent_session = "concurrent-parent";
    let child_session = "concurrent-child";
    let first_prompt = "inspect the unrelated alpha candidate carefully";
    let selected_prompt = "inspect the selected beta candidate carefully";

    forward_all(
        &mut fixtures.start_turn(
            AgentKind::Claude,
            parent_session,
            &parent_tree,
            "Run two delegated tasks.",
            1_700_100_000_000,
        ),
        &socket,
        &host,
    )
    .await;
    forward(
        fixtures.open_tool(
            AgentKind::Claude,
            parent_session,
            &parent_tree,
            "call-alpha",
            first_prompt,
            1_700_100_000_010,
        ),
        &socket,
        &host,
    )
    .await;
    forward(
        fixtures.open_tool(
            AgentKind::Claude,
            parent_session,
            &parent_tree,
            "call-beta",
            selected_prompt,
            1_700_100_000_011,
        ),
        &socket,
        &host,
    )
    .await;

    // SessionStart alone is ambiguous and is held. UserPromptSubmit supplies
    // the opaque-content fingerprints that select call-beta.
    forward_all(
        &mut fixtures.start_turn(
            AgentKind::Pi,
            child_session,
            &child_tree,
            selected_prompt,
            1_700_100_000_020,
        ),
        &socket,
        &host,
    )
    .await;
    forward(
        fixtures.close_session(
            AgentKind::Pi,
            child_session,
            &child_tree,
            "selected beta complete",
            1_700_100_000_030,
        ),
        &socket,
        &host,
    )
    .await;
    forward(
        fixtures.close_tool(
            AgentKind::Claude,
            parent_session,
            &parent_tree,
            "call-alpha",
            first_prompt,
            "alpha complete",
            1_700_100_000_040,
        ),
        &socket,
        &host,
    )
    .await;
    forward(
        fixtures.close_tool(
            AgentKind::Claude,
            parent_session,
            &parent_tree,
            "call-beta",
            selected_prompt,
            "beta complete",
            1_700_100_000_041,
        ),
        &socket,
        &host,
    )
    .await;
    flush(child_session, &socket).await;
    flush(parent_session, &socket).await;

    let parent = recording.session(parent_session);
    let child = recording.session(child_session);
    let selected_tool = inserted_rows(&parent)
        .into_iter()
        .find(|row| {
            row.span_type == SpanType::Tool
                && row
                    .input
                    .as_ref()
                    .is_some_and(|input| input.to_string().contains(selected_prompt))
        })
        .expect("selected parent tool span");
    let child_root = session_root(
        &inserted_rows(&child),
        child_session,
        "concurrent fingerprint match",
    )
    .clone();
    assert_eq!(child_root.parent_span_ids, vec![selected_tool.span_id]);

    shutdown_daemon(&socket).await.unwrap();
    daemon.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_agents_link_recursively_through_every_generation() {
    let (socket, daemon, recording, _tmp) = start_daemon().await;
    let host = HostInfo {
        serve_argv: vec![OsString::from("unused")],
        version: "test".into(),
    };
    let mut fixtures = DistributedFixtures::new();
    let kinds = [
        AgentKind::Claude,
        AgentKind::Codex,
        AgentKind::Pi,
        AgentKind::OpenCode,
    ];
    let sessions = [
        "recursive-claude",
        "recursive-codex",
        "recursive-pi",
        "recursive-opencode",
    ];
    let prompts = [
        "delegate recursive level one",
        "delegate recursive level two",
        "delegate recursive level three",
    ];
    let trees = [
        ProcessTree::root(11_000),
        ProcessTree::child(11_002, 11_001, &ProcessTree::root(11_000)),
        ProcessTree::child(
            11_004,
            11_003,
            &ProcessTree::child(11_002, 11_001, &ProcessTree::root(11_000)),
        ),
        ProcessTree::child(
            11_006,
            11_005,
            &ProcessTree::child(
                11_004,
                11_003,
                &ProcessTree::child(11_002, 11_001, &ProcessTree::root(11_000)),
            ),
        ),
    ];

    forward_all(
        &mut fixtures.start_turn(
            kinds[0],
            sessions[0],
            &trees[0],
            "start recursive work",
            1_700_200_000_000,
        ),
        &socket,
        &host,
    )
    .await;
    for level in 0..3 {
        forward(
            fixtures.open_tool(
                kinds[level],
                sessions[level],
                &trees[level],
                &format!("recursive-call-{level}"),
                prompts[level],
                1_700_200_000_010 + level as i64 * 20,
            ),
            &socket,
            &host,
        )
        .await;
        forward_all(
            &mut fixtures.start_turn(
                kinds[level + 1],
                sessions[level + 1],
                &trees[level + 1],
                prompts[level],
                1_700_200_000_020 + level as i64 * 20,
            ),
            &socket,
            &host,
        )
        .await;
    }

    forward(
        fixtures.close_session(
            kinds[3],
            sessions[3],
            &trees[3],
            "leaf complete",
            1_700_200_000_100,
        ),
        &socket,
        &host,
    )
    .await;
    for level in (0..3).rev() {
        forward(
            fixtures.close_tool(
                kinds[level],
                sessions[level],
                &trees[level],
                &format!("recursive-call-{level}"),
                prompts[level],
                "child complete",
                1_700_200_000_110 + (2 - level) as i64 * 20,
            ),
            &socket,
            &host,
        )
        .await;
        forward(
            fixtures.close_session(
                kinds[level],
                sessions[level],
                &trees[level],
                "level complete",
                1_700_200_000_120 + (2 - level) as i64 * 20,
            ),
            &socket,
            &host,
        )
        .await;
    }
    for session in sessions {
        flush(session, &socket).await;
    }

    let root_record = recording.session(sessions[0]);
    let root_id = session_root(&inserted_rows(&root_record), sessions[0], "recursive root")
        .root_span_id
        .clone();
    for level in 0..3 {
        let parent = recording.session(sessions[level]);
        let child = recording.session(sessions[level + 1]);
        let tool = inserted_rows(&parent)
            .into_iter()
            .find(|row| row.span_type == SpanType::Tool)
            .unwrap_or_else(|| panic!("recursive level {level}: missing tool"));
        let child_root = session_root(
            &inserted_rows(&child),
            sessions[level + 1],
            "recursive child",
        )
        .clone();
        assert_eq!(child_root.parent_span_ids, vec![tool.span_id.clone()]);
        let configs = child.configs.lock().unwrap();
        let components = configs
            .iter()
            .find_map(|config| match &config.destination {
                Some(TraceDestination::ParentSpan { components }) => Some(components),
                _ => None,
            })
            .unwrap_or_else(|| panic!("recursive level {level}: child not attached"));
        assert_eq!(components.span_id.as_deref(), Some(tool.span_id.as_str()));
        assert_eq!(components.root_span_id.as_deref(), Some(root_id.as_str()));
    }

    shutdown_daemon(&socket).await.unwrap();
    daemon.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn indistinguishable_concurrent_tools_fail_safe_without_a_parent() {
    let (socket, daemon, recording, _tmp) = start_daemon().await;
    let host = HostInfo {
        serve_argv: vec![OsString::from("unused")],
        version: "test".into(),
    };
    let mut fixtures = DistributedFixtures::new();
    let parent_tree = ProcessTree::root(12_000);
    let child_tree = ProcessTree::child(12_002, 12_001, &parent_tree);
    let prompt = "the exact same delegated task for both candidates";

    forward_all(
        &mut fixtures.start_turn(
            AgentKind::Claude,
            "ambiguous-parent",
            &parent_tree,
            "delegate twice",
            1_700_300_000_000,
        ),
        &socket,
        &host,
    )
    .await;
    for call in ["ambiguous-a", "ambiguous-b"] {
        forward(
            fixtures.open_tool(
                AgentKind::Claude,
                "ambiguous-parent",
                &parent_tree,
                call,
                prompt,
                1_700_300_000_010,
            ),
            &socket,
            &host,
        )
        .await;
    }
    forward_all(
        &mut fixtures.start_turn(
            AgentKind::Pi,
            "ambiguous-child",
            &child_tree,
            prompt,
            1_700_300_000_020,
        ),
        &socket,
        &host,
    )
    .await;
    forward(
        fixtures.close_session(
            AgentKind::Pi,
            "ambiguous-child",
            &child_tree,
            "done",
            1_700_300_000_030,
        ),
        &socket,
        &host,
    )
    .await;
    for call in ["ambiguous-a", "ambiguous-b"] {
        forward(
            fixtures.close_tool(
                AgentKind::Claude,
                "ambiguous-parent",
                &parent_tree,
                call,
                prompt,
                "same indistinguishable output",
                1_700_300_000_040,
            ),
            &socket,
            &host,
        )
        .await;
    }
    flush("ambiguous-child", &socket).await;

    let child = recording.session("ambiguous-child");
    assert!(
        child.configs.lock().unwrap().iter().all(|config| !matches!(
            config.destination,
            Some(TraceDestination::ParentSpan { .. })
        )),
        "an ambiguous child must never be attached by guessing"
    );
    let child_root = session_root(
        &inserted_rows(&child),
        "ambiguous-child",
        "ambiguous standalone",
    )
    .clone();
    assert!(child_root.parent_span_ids.is_empty());

    shutdown_daemon(&socket).await.unwrap();
    daemon.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resolved_child_link_survives_daemon_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("data");
    let socket = test_endpoint(tmp.path());
    let first_recording = Arc::new(RecordingSinkFactory::default());
    let first_daemon =
        spawn_daemon(socket.clone(), data_dir.clone(), first_recording.clone()).await;
    let host = HostInfo {
        serve_argv: vec![OsString::from("unused")],
        version: "test".into(),
    };
    let mut fixtures = DistributedFixtures::new();
    let parent_tree = ProcessTree::root(13_000);
    let child_tree = ProcessTree::child(13_002, 13_001, &parent_tree);
    let prompt = "persist this exact distributed child link";

    forward_all(
        &mut fixtures.start_turn(
            AgentKind::Claude,
            "restart-parent",
            &parent_tree,
            "delegate",
            1_700_400_000_000,
        ),
        &socket,
        &host,
    )
    .await;
    forward(
        fixtures.open_tool(
            AgentKind::Claude,
            "restart-parent",
            &parent_tree,
            "restart-call",
            prompt,
            1_700_400_000_010,
        ),
        &socket,
        &host,
    )
    .await;
    forward_all(
        &mut fixtures.start_turn(
            AgentKind::Pi,
            "restart-child",
            &child_tree,
            prompt,
            1_700_400_000_020,
        ),
        &socket,
        &host,
    )
    .await;
    flush("restart-child", &socket).await;
    shutdown_daemon(&socket).await.unwrap();
    first_daemon.await.unwrap();

    let second_recording = Arc::new(RecordingSinkFactory::default());
    let second_daemon = spawn_daemon(socket.clone(), data_dir, second_recording.clone()).await;
    forward(
        fixtures.close_session(
            AgentKind::Pi,
            "restart-child",
            &child_tree,
            "complete after restart",
            1_700_400_000_030,
        ),
        &socket,
        &host,
    )
    .await;
    flush("restart-child", &socket).await;

    let child = second_recording.session("restart-child");
    assert!(child.configs.lock().unwrap().iter().any(|config| matches!(
        config.destination,
        Some(TraceDestination::ParentSpan { .. })
    )));
    shutdown_daemon(&socket).await.unwrap();
    second_daemon.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ambiguous_child_evidence_survives_daemon_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("data");
    let socket = test_endpoint(tmp.path());
    let host = HostInfo {
        serve_argv: vec![OsString::from("unused")],
        version: "test".into(),
    };
    let parent_tree = ProcessTree::root(14_000);
    let child_tree = ProcessTree::child(14_002, 14_001, &parent_tree);
    let selected = "select durable beta evidence after restart";
    let mut fixtures = DistributedFixtures::new();

    let first_recording = Arc::new(RecordingSinkFactory::default());
    let first_daemon =
        spawn_daemon(socket.clone(), data_dir.clone(), first_recording.clone()).await;
    forward_all(
        &mut fixtures.start_turn(
            AgentKind::Claude,
            "pending-parent",
            &parent_tree,
            "delegate twice",
            1_700_500_000_000,
        ),
        &socket,
        &host,
    )
    .await;
    for (call, prompt) in [
        ("pending-alpha", "unrelated durable alpha evidence"),
        ("pending-beta", selected),
    ] {
        forward(
            fixtures.open_tool(
                AgentKind::Claude,
                "pending-parent",
                &parent_tree,
                call,
                prompt,
                1_700_500_000_010,
            ),
            &socket,
            &host,
        )
        .await;
    }
    let mut child_start = fixtures.start_turn(
        AgentKind::Pi,
        "pending-child",
        &child_tree,
        selected,
        1_700_500_000_020,
    );
    forward(child_start.remove(0), &socket, &host).await;
    assert!(tokio::fs::read_dir(data_dir.join("correlation"))
        .await
        .unwrap()
        .next_entry()
        .await
        .unwrap()
        .is_some());
    shutdown_daemon(&socket).await.unwrap();
    first_daemon.await.unwrap();

    let second_recording = Arc::new(RecordingSinkFactory::default());
    let second_daemon = spawn_daemon(socket.clone(), data_dir, second_recording.clone()).await;
    // No parent event is sent after restart. Both candidates and their hashed
    // evidence must come from the compact active-parent snapshot.
    forward(child_start.remove(0), &socket, &host).await;
    forward(
        fixtures.close_session(
            AgentKind::Pi,
            "pending-child",
            &child_tree,
            "resolved",
            1_700_500_000_040,
        ),
        &socket,
        &host,
    )
    .await;
    flush("pending-child", &socket).await;

    let parent = first_recording.session("pending-parent");
    let beta = inserted_rows(&parent)
        .into_iter()
        .find(|row| {
            row.span_type == SpanType::Tool
                && row
                    .input
                    .as_ref()
                    .is_some_and(|input| input.to_string().contains(selected))
        })
        .expect("replayed beta tool");
    let child = second_recording.session("pending-child");
    let child_root = session_root(
        &inserted_rows(&child),
        "pending-child",
        "durable pending child",
    )
    .clone();
    assert_eq!(child_root.parent_span_ids, vec![beta.span_id]);

    shutdown_daemon(&socket).await.unwrap();
    second_daemon.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn completed_tools_are_not_resurrected_after_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("data");
    let socket = test_endpoint(tmp.path());
    let recording = Arc::new(RecordingSinkFactory::default());
    let host = HostInfo {
        serve_argv: vec![OsString::from("unused")],
        version: "test".into(),
    };
    let mut fixtures = DistributedFixtures::new();
    let parent_tree = ProcessTree::root(34_000);
    let prompt = "completed snapshot must be removed";

    let first = spawn_daemon(socket.clone(), data_dir.clone(), recording.clone()).await;
    forward_all(
        &mut fixtures.start_turn(
            AgentKind::Claude,
            "closed-restart-parent",
            &parent_tree,
            "delegate",
            1_703_000_000_000,
        ),
        &socket,
        &host,
    )
    .await;
    forward(
        fixtures.open_tool(
            AgentKind::Claude,
            "closed-restart-parent",
            &parent_tree,
            "closed-restart-call",
            prompt,
            1_703_000_000_010,
        ),
        &socket,
        &host,
    )
    .await;
    forward(
        fixtures.close_tool(
            AgentKind::Claude,
            "closed-restart-parent",
            &parent_tree,
            "closed-restart-call",
            prompt,
            "done",
            1_703_000_000_020,
        ),
        &socket,
        &host,
    )
    .await;
    shutdown_daemon(&socket).await.unwrap();
    first.await.unwrap();

    let second = spawn_daemon(socket.clone(), data_dir, recording.clone()).await;
    for (index, reused_pid) in [false, true].into_iter().enumerate() {
        let child_session = format!("closed-restart-child-{index}");
        let child_tree = ProcessTree::child(34_010 + index as u32, 34_001, &parent_tree);
        let mut events = fixtures.start_turn(
            AgentKind::Pi,
            &child_session,
            &child_tree,
            prompt,
            1_703_000_000_030 + index as i64,
        );
        if reused_pid {
            for event in &mut events {
                for process in &mut event.capture.as_mut().unwrap().process_chain {
                    if process.pid == parent_tree.agent.pid {
                        process.start_time_secs += 1;
                    }
                }
            }
        }
        forward_all(&mut events, &socket, &host).await;
        forward(
            fixtures.close_session(
                AgentKind::Pi,
                &child_session,
                &child_tree,
                "standalone",
                1_703_000_000_040 + index as i64,
            ),
            &socket,
            &host,
        )
        .await;
        flush(&child_session, &socket).await;
        let child = recording.session(&child_session);
        assert!(child.configs.lock().unwrap().iter().all(|config| !matches!(
            config.destination,
            Some(TraceDestination::ParentSpan { .. })
        )));
    }

    shutdown_daemon(&socket).await.unwrap();
    second.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interrupted_tool_transition_snapshot_fails_safe_after_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("data");
    let socket = test_endpoint(tmp.path());
    let recording = Arc::new(RecordingSinkFactory::default());
    let host = HostInfo {
        serve_argv: vec![OsString::from("unused")],
        version: "test".into(),
    };
    let mut fixtures = DistributedFixtures::new();
    let parent_tree = ProcessTree::root(34_100);
    let child_tree = ProcessTree::child(34_102, 34_101, &parent_tree);
    let prompt = "dirty transition must never resurrect a parent";

    let first = spawn_daemon(socket.clone(), data_dir.clone(), recording.clone()).await;
    forward_all(
        &mut fixtures.start_turn(
            AgentKind::Claude,
            "dirty-restart-parent",
            &parent_tree,
            "delegate",
            1_703_100_000_000,
        ),
        &socket,
        &host,
    )
    .await;
    forward(
        fixtures.open_tool(
            AgentKind::Claude,
            "dirty-restart-parent",
            &parent_tree,
            "dirty-restart-call",
            prompt,
            1_703_100_000_010,
        ),
        &socket,
        &host,
    )
    .await;
    shutdown_daemon(&socket).await.unwrap();
    first.await.unwrap();

    let parent_dir = data_dir.join("correlation").join("parents");
    let mut entries = tokio::fs::read_dir(parent_dir).await.unwrap();
    let path = entries.next_entry().await.unwrap().unwrap().path();
    let mut snapshot: serde_json::Value =
        serde_json::from_slice(&tokio::fs::read(&path).await.unwrap()).unwrap();
    snapshot["dirty"] = serde_json::Value::Bool(true);
    tokio::fs::write(&path, serde_json::to_vec(&snapshot).unwrap())
        .await
        .unwrap();

    let second = spawn_daemon(socket.clone(), data_dir, recording.clone()).await;
    forward_all(
        &mut fixtures.start_turn(
            AgentKind::Pi,
            "dirty-restart-child",
            &child_tree,
            prompt,
            1_703_100_000_020,
        ),
        &socket,
        &host,
    )
    .await;
    forward(
        fixtures.close_session(
            AgentKind::Pi,
            "dirty-restart-child",
            &child_tree,
            "standalone",
            1_703_100_000_030,
        ),
        &socket,
        &host,
    )
    .await;
    flush("dirty-restart-child", &socket).await;
    let child = recording.session("dirty-restart-child");
    assert!(child.configs.lock().unwrap().iter().all(|config| !matches!(
        config.destination,
        Some(TraceDestination::ParentSpan { .. })
    )));

    shutdown_daemon(&socket).await.unwrap();
    second.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupt_active_parent_snapshot_is_ignored() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("data");
    let parent_dir = data_dir.join("correlation").join("parents");
    tokio::fs::create_dir_all(&parent_dir).await.unwrap();
    tokio::fs::write(parent_dir.join("corrupt.json"), b"{not-json")
        .await
        .unwrap();
    let socket = test_endpoint(tmp.path());
    let recording = Arc::new(RecordingSinkFactory::default());
    let daemon = spawn_daemon(socket.clone(), data_dir, recording).await;

    assert!(run_status(StatusArgs {
        socket: Some(socket.clone()),
        session_id: None,
    })
    .await
    .unwrap()
    .is_some());

    shutdown_daemon(&socket).await.unwrap();
    daemon.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restored_parent_snapshot_does_not_pin_an_otherwise_idle_daemon() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("data");
    let socket = test_endpoint(tmp.path());
    let recording = Arc::new(RecordingSinkFactory::default());
    let host = HostInfo {
        serve_argv: vec![OsString::from("unused")],
        version: "test".into(),
    };
    let mut fixtures = DistributedFixtures::new();
    let parent_tree = ProcessTree::root(36_000);

    let first = spawn_daemon(socket.clone(), data_dir.clone(), recording).await;
    forward_all(
        &mut fixtures.start_turn(
            AgentKind::Claude,
            "idle-restored-parent",
            &parent_tree,
            "delegate before restart",
            1_705_000_000_000,
        ),
        &socket,
        &host,
    )
    .await;
    forward(
        fixtures.open_tool(
            AgentKind::Claude,
            "idle-restored-parent",
            &parent_tree,
            "idle-restored-call",
            "child may arrive after restart",
            1_705_000_000_010,
        ),
        &socket,
        &host,
    )
    .await;
    shutdown_daemon(&socket).await.unwrap();
    first.await.unwrap();

    let second = spawn_daemon_with_timeouts(
        socket,
        data_dir,
        Arc::new(RecordingSinkFactory::default()),
        1,
        1,
    )
    .await;
    tokio::time::timeout(Duration::from_secs(4), second)
        .await
        .expect("restored snapshot pinned idle daemon")
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ipc_initialize_pid_is_captured_without_hook_metadata() {
    let (socket, daemon, _recording, tmp) = start_daemon().await;
    let host = HostInfo {
        serve_argv: vec![OsString::from("unused")],
        version: "test".into(),
    };
    let mut fixtures = DistributedFixtures::new();
    let mut event = fixtures
        .start_turn(
            AgentKind::Claude,
            "automatic-process-capture",
            &ProcessTree::root(15_000),
            "capture automatically",
            1_700_600_000_000,
        )
        .remove(0);
    event.capture = None;
    forward(event, &socket, &host).await;
    flush("automatic-process-capture", &socket).await;
    shutdown_daemon(&socket).await.unwrap();
    daemon.await.unwrap();

    let journal = tokio::fs::read_to_string(
        tmp.path()
            .join("data/journal/automatic-process-capture.ndjson"),
    )
    .await
    .unwrap();
    let row: serde_json::Value = serde_json::from_str(journal.lines().next().unwrap()).unwrap();
    let chain = row
        .pointer("/capture/process_chain")
        .and_then(serde_json::Value::as_array)
        .expect("daemon-added process ancestry in journal");
    assert_eq!(
        chain.first().and_then(|process| process.get("pid")),
        Some(&serde_json::json!(std::process::id()))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn completed_tools_are_not_candidates_for_later_children() {
    let (socket, daemon, recording, _tmp) = start_daemon().await;
    let host = HostInfo {
        serve_argv: vec![OsString::from("unused")],
        version: "test".into(),
    };
    let mut fixtures = DistributedFixtures::new();

    for (index, kind) in AgentKind::ALL.into_iter().enumerate() {
        let pid = 16_000 + index as u32 * 10;
        let parent_tree = ProcessTree::root(pid);
        let child_tree = ProcessTree::child(pid + 2, pid + 1, &parent_tree);
        let parent_session = format!("closed-parent-{}", kind.label());
        let child_session = format!("after-close-child-{}", kind.label());
        let prompt = format!("already completed prompt for {}", kind.label());
        let ts = 1_700_700_000_000 + index as i64 * 100;
        forward_all(
            &mut fixtures.start_turn(kind, &parent_session, &parent_tree, "run once", ts),
            &socket,
            &host,
        )
        .await;
        forward(
            fixtures.open_tool(
                kind,
                &parent_session,
                &parent_tree,
                "closed-call",
                &prompt,
                ts + 10,
            ),
            &socket,
            &host,
        )
        .await;
        forward(
            fixtures.close_tool(
                kind,
                &parent_session,
                &parent_tree,
                "closed-call",
                &prompt,
                "done",
                ts + 20,
            ),
            &socket,
            &host,
        )
        .await;
        forward_all(
            &mut fixtures.start_turn(AgentKind::Pi, &child_session, &child_tree, &prompt, ts + 30),
            &socket,
            &host,
        )
        .await;
        forward(
            fixtures.close_session(
                AgentKind::Pi,
                &child_session,
                &child_tree,
                "standalone",
                ts + 40,
            ),
            &socket,
            &host,
        )
        .await;
        flush(&child_session, &socket).await;
        let child = recording.session(&child_session);
        assert!(child.configs.lock().unwrap().iter().all(|config| !matches!(
            config.destination,
            Some(TraceDestination::ParentSpan { .. })
        )));
    }

    shutdown_daemon(&socket).await.unwrap();
    daemon.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn child_output_can_disambiguate_when_inputs_do_not_overlap() {
    let (socket, daemon, recording, _tmp) = start_daemon().await;
    let host = HostInfo {
        serve_argv: vec![OsString::from("unused")],
        version: "test".into(),
    };
    let mut fixtures = DistributedFixtures::new();
    let parent_tree = ProcessTree::root(17_000);
    let child_tree = ProcessTree::child(17_002, 17_001, &parent_tree);
    let selected_output = "OUTPUT_FINGERPRINT_BETA_928374";

    forward_all(
        &mut fixtures.start_turn(
            AgentKind::Claude,
            "output-parent",
            &parent_tree,
            "run opaque file tasks",
            1_700_800_000_000,
        ),
        &socket,
        &host,
    )
    .await;
    for (call, opaque_input) in [
        ("output-alpha", "agent --prompt-file /tmp/opaque-alpha"),
        ("output-beta", "agent --prompt-file /tmp/opaque-beta"),
    ] {
        forward(
            fixtures.open_tool(
                AgentKind::Claude,
                "output-parent",
                &parent_tree,
                call,
                opaque_input,
                1_700_800_000_010,
            ),
            &socket,
            &host,
        )
        .await;
    }
    forward_all(
        &mut fixtures.start_turn(
            AgentKind::Claude,
            "output-child",
            &child_tree,
            "perform the task loaded from the file",
            1_700_800_000_020,
        ),
        &socket,
        &host,
    )
    .await;
    forward(
        fixtures.close_session(
            AgentKind::Claude,
            "output-child",
            &child_tree,
            selected_output,
            1_700_800_000_030,
        ),
        &socket,
        &host,
    )
    .await;
    forward(
        fixtures.close_tool(
            AgentKind::Claude,
            "output-parent",
            &parent_tree,
            "output-alpha",
            "agent --prompt-file /tmp/opaque-alpha",
            "OUTPUT_FINGERPRINT_ALPHA_193746",
            1_700_800_000_040,
        ),
        &socket,
        &host,
    )
    .await;
    forward(
        fixtures.close_tool(
            AgentKind::Claude,
            "output-parent",
            &parent_tree,
            "output-beta",
            "agent --prompt-file /tmp/opaque-beta",
            selected_output,
            1_700_800_000_050,
        ),
        &socket,
        &host,
    )
    .await;
    flush("output-child", &socket).await;

    let parent = recording.session("output-parent");
    let beta = inserted_rows(&parent)
        .into_iter()
        .find(|row| {
            row.span_type == SpanType::Tool
                && row
                    .input
                    .as_ref()
                    .is_some_and(|input| input.to_string().contains("opaque-beta"))
        })
        .expect("beta tool");
    let child = recording.session("output-child");
    let child_root = session_root(
        &inserted_rows(&child),
        "output-child",
        "output fingerprint child",
    )
    .clone();
    assert_eq!(child_root.parent_span_ids, vec![beta.span_id]);

    shutdown_daemon(&socket).await.unwrap();
    daemon.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_agent_sessions_in_one_process_choose_their_own_tools() {
    let (socket, daemon, recording, _tmp) = start_daemon().await;
    let host = HostInfo {
        serve_argv: vec![OsString::from("unused")],
        version: "test".into(),
    };
    let mut fixtures = DistributedFixtures::new();
    let shared_parent_tree = ProcessTree::root(18_000);
    let mut cases = Vec::new();

    for (index, parent_kind) in AgentKind::ALL.into_iter().enumerate() {
        let child_kind = AgentKind::ALL[(index + 1) % AgentKind::ALL.len()];
        let parent_session = format!("multiplex-parent-{}", parent_kind.label());
        let child_session = format!("multiplex-child-{}", child_kind.label());
        let call_id = format!("multiplex-call-{index}");
        let prompt = format!(
            "unique multiplexed prompt number {index} for {}",
            parent_kind.label()
        );
        let ts = 1_700_900_000_000 + index as i64 * 100;
        forward_all(
            &mut fixtures.start_turn(
                parent_kind,
                &parent_session,
                &shared_parent_tree,
                "serve one multiplexed child",
                ts,
            ),
            &socket,
            &host,
        )
        .await;
        forward(
            fixtures.open_tool(
                parent_kind,
                &parent_session,
                &shared_parent_tree,
                &call_id,
                &prompt,
                ts + 10,
            ),
            &socket,
            &host,
        )
        .await;
        let child_tree = ProcessTree::child(
            18_100 + index as u32 * 2,
            18_101 + index as u32 * 2,
            &shared_parent_tree,
        );
        let events = fixtures.start_turn(child_kind, &child_session, &child_tree, &prompt, ts + 20);
        cases.push((
            parent_kind,
            child_kind,
            parent_session,
            child_session,
            call_id,
            prompt,
            child_tree,
            ts,
            events,
        ));
    }

    for (_, _, parent_session, _, _, _, _, _, _) in &cases {
        let parent = recording.session(parent_session);
        assert!(
            parent
                .configs
                .lock()
                .unwrap()
                .iter()
                .all(|config| !matches!(
                    config.destination,
                    Some(TraceDestination::ParentSpan { .. })
                )),
            "top-level session {parent_session} was mistaken for a child"
        );
    }

    let mut starts = tokio::task::JoinSet::new();
    for (_, _, _, _, _, _, _, _, events) in &cases {
        let events = events.clone();
        let socket = socket.clone();
        let host = host.clone();
        starts.spawn(async move {
            for event in events {
                forward(event, &socket, &host).await;
            }
        });
    }
    while let Some(result) = starts.join_next().await {
        result.unwrap();
    }

    for (
        parent_kind,
        child_kind,
        parent_session,
        child_session,
        call_id,
        prompt,
        child_tree,
        ts,
        _,
    ) in &cases
    {
        forward(
            fixtures.close_session(
                *child_kind,
                child_session,
                child_tree,
                "multiplexed child complete",
                *ts + 30,
            ),
            &socket,
            &host,
        )
        .await;
        forward(
            fixtures.close_tool(
                *parent_kind,
                parent_session,
                &shared_parent_tree,
                call_id,
                prompt,
                "multiplexed tool complete",
                *ts + 40,
            ),
            &socket,
            &host,
        )
        .await;
        flush(child_session, &socket).await;
        assert_pair_linked(
            recording.as_ref(),
            parent_session,
            child_session,
            &format!("concurrent shared-process parent {}", parent_kind.label()),
        );
    }

    shutdown_daemon(&socket).await.unwrap();
    daemon.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pid_reuse_and_unavailable_start_times_fail_safe() {
    let (socket, daemon, recording, _tmp) = start_daemon().await;
    let host = HostInfo {
        serve_argv: vec![OsString::from("unused")],
        version: "test".into(),
    };
    let mut fixtures = DistributedFixtures::new();
    let parent_tree = ProcessTree::root(19_000);
    let prompt = "PID_REUSE_MUST_NOT_LINK_THIS_CHILD";
    forward_all(
        &mut fixtures.start_turn(
            AgentKind::Claude,
            "pid-parent",
            &parent_tree,
            "delegate safely",
            1_701_000_000_000,
        ),
        &socket,
        &host,
    )
    .await;
    forward(
        fixtures.open_tool(
            AgentKind::Claude,
            "pid-parent",
            &parent_tree,
            "pid-call",
            prompt,
            1_701_000_000_010,
        ),
        &socket,
        &host,
    )
    .await;

    for (child_session, mode) in [("pid-reused-child", 0), ("unknown-start-child", 1)] {
        let child_tree = ProcessTree::child(19_002 + mode, 19_001, &parent_tree);
        let mut events = fixtures.start_turn(
            AgentKind::Pi,
            child_session,
            &child_tree,
            prompt,
            1_701_000_000_020 + i64::from(mode),
        );
        for event in &mut events {
            let capture = event.capture.as_mut().unwrap();
            if mode == 0 {
                for process in &mut capture.process_chain {
                    if process.pid == parent_tree.agent.pid {
                        process.start_time_secs += 1;
                    }
                }
            } else {
                for process in &mut capture.process_chain {
                    process.start_time_secs = 0;
                }
            }
        }
        forward_all(&mut events, &socket, &host).await;
        forward(
            fixtures.close_session(
                AgentKind::Pi,
                child_session,
                &child_tree,
                "standalone",
                1_701_000_000_030 + i64::from(mode),
            ),
            &socket,
            &host,
        )
        .await;
        flush(child_session, &socket).await;
        let child = recording.session(child_session);
        assert!(child.configs.lock().unwrap().iter().all(|config| !matches!(
            config.destination,
            Some(TraceDestination::ParentSpan { .. })
        )));
    }

    shutdown_daemon(&socket).await.unwrap();
    daemon.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn open_tool_prevents_idle_retirement_during_a_long_child_spawn() {
    let tmp = tempfile::tempdir().unwrap();
    let socket = test_endpoint(tmp.path());
    let data_dir = tmp.path().join("data");
    let recording = Arc::new(RecordingSinkFactory::default());
    let daemon =
        spawn_daemon_with_timeouts(socket.clone(), data_dir, recording.clone(), 1, 1).await;
    let host = HostInfo {
        serve_argv: vec![OsString::from("unused")],
        version: "test".into(),
    };
    let mut fixtures = DistributedFixtures::new();
    let parent_tree = ProcessTree::root(20_000);
    let child_tree = ProcessTree::child(20_002, 20_001, &parent_tree);
    let prompt = "LONG_RUNNING_CHILD_AFTER_IDLE_WINDOW";
    forward_all(
        &mut fixtures.start_turn(
            AgentKind::Claude,
            "idle-parent",
            &parent_tree,
            "start long child",
            1_701_100_000_000,
        ),
        &socket,
        &host,
    )
    .await;
    forward(
        fixtures.open_tool(
            AgentKind::Claude,
            "idle-parent",
            &parent_tree,
            "idle-call",
            prompt,
            1_701_100_000_010,
        ),
        &socket,
        &host,
    )
    .await;

    tokio::time::sleep(Duration::from_millis(1_300)).await;
    assert!(run_status(StatusArgs {
        socket: Some(socket.clone()),
        session_id: None,
    })
    .await
    .unwrap()
    .is_some());
    forward_all(
        &mut fixtures.start_turn(
            AgentKind::Pi,
            "idle-child",
            &child_tree,
            prompt,
            1_701_100_001_500,
        ),
        &socket,
        &host,
    )
    .await;
    flush("idle-child", &socket).await;
    assert_pair_linked(
        recording.as_ref(),
        "idle-parent",
        "idle-child",
        "active tool idle protection",
    );

    shutdown_daemon(&socket).await.unwrap();
    daemon.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_ambiguous_children_do_not_deadlock_or_cross_link() {
    let (socket, daemon, recording, _tmp) = start_daemon().await;
    let host = HostInfo {
        serve_argv: vec![OsString::from("unused")],
        version: "test".into(),
    };
    let mut fixtures = DistributedFixtures::new();
    let parent_tree = ProcessTree::root(21_000);
    let prompt = "identical concurrent ambiguous child request";
    forward_all(
        &mut fixtures.start_turn(
            AgentKind::Claude,
            "deadlock-parent",
            &parent_tree,
            "spawn ambiguous children",
            1_701_200_000_000,
        ),
        &socket,
        &host,
    )
    .await;
    for call in ["deadlock-a", "deadlock-b"] {
        forward(
            fixtures.open_tool(
                AgentKind::Claude,
                "deadlock-parent",
                &parent_tree,
                call,
                prompt,
                1_701_200_000_010,
            ),
            &socket,
            &host,
        )
        .await;
    }

    let child_cases = [
        (
            AgentKind::Pi,
            "deadlock-child-pi",
            ProcessTree::child(21_002, 21_001, &parent_tree),
        ),
        (
            AgentKind::OpenCode,
            "deadlock-child-opencode",
            ProcessTree::child(21_004, 21_003, &parent_tree),
        ),
    ];
    let mut starts = tokio::task::JoinSet::new();
    for (kind, session, tree) in &child_cases {
        let events = fixtures.start_turn(*kind, session, tree, prompt, 1_701_200_000_020);
        let socket = socket.clone();
        let host = host.clone();
        starts.spawn(async move {
            for event in events {
                forward(event, &socket, &host).await;
            }
        });
    }
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(result) = starts.join_next().await {
            result.unwrap();
        }
    })
    .await
    .expect("concurrent ambiguous child starts deadlocked");

    for (kind, session, tree) in &child_cases {
        forward(
            fixtures.close_session(*kind, session, tree, "same child output", 1_701_200_000_030),
            &socket,
            &host,
        )
        .await;
    }
    for call in ["deadlock-a", "deadlock-b"] {
        forward(
            fixtures.close_tool(
                AgentKind::Claude,
                "deadlock-parent",
                &parent_tree,
                call,
                prompt,
                "same parent output",
                1_701_200_000_040,
            ),
            &socket,
            &host,
        )
        .await;
    }
    for (_, session, _) in &child_cases {
        flush(session, &socket).await;
        let child = recording.session(session);
        assert!(child.configs.lock().unwrap().iter().all(|config| !matches!(
            config.destination,
            Some(TraceDestination::ParentSpan { .. })
        )));
    }

    shutdown_daemon(&socket).await.unwrap();
    daemon.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_tool_can_fan_out_to_concurrent_children_of_every_agent() {
    let (socket, daemon, recording, _tmp) = start_daemon().await;
    let host = HostInfo {
        serve_argv: vec![OsString::from("unused")],
        version: "test".into(),
    };
    let mut fixtures = DistributedFixtures::new();
    let parent_tree = ProcessTree::root(22_000);
    let parent_session = "fanout-parent";
    let prompt = "fan out one parent operation to every child agent";
    forward_all(
        &mut fixtures.start_turn(
            AgentKind::Claude,
            parent_session,
            &parent_tree,
            "fan out",
            1_701_300_000_000,
        ),
        &socket,
        &host,
    )
    .await;
    forward(
        fixtures.open_tool(
            AgentKind::Claude,
            parent_session,
            &parent_tree,
            "fanout-call",
            prompt,
            1_701_300_000_010,
        ),
        &socket,
        &host,
    )
    .await;

    let mut children = Vec::new();
    let mut starts = tokio::task::JoinSet::new();
    for (index, kind) in AgentKind::ALL.into_iter().enumerate() {
        let session = format!("fanout-child-{}", kind.label());
        let tree = ProcessTree::child(
            22_100 + index as u32 * 2,
            22_101 + index as u32 * 2,
            &parent_tree,
        );
        let events = fixtures.start_turn(kind, &session, &tree, prompt, 1_701_300_000_020);
        let socket = socket.clone();
        let host = host.clone();
        starts.spawn(async move {
            for event in events {
                forward(event, &socket, &host).await;
            }
        });
        children.push((kind, session, tree));
    }
    while let Some(result) = starts.join_next().await {
        result.unwrap();
    }

    for (kind, session, tree) in &children {
        forward(
            fixtures.close_session(*kind, session, tree, "fanout complete", 1_701_300_000_030),
            &socket,
            &host,
        )
        .await;
        flush(session, &socket).await;
        assert_pair_linked(
            recording.as_ref(),
            parent_session,
            session,
            &format!("fanout child {}", kind.label()),
        );
    }

    shutdown_daemon(&socket).await.unwrap();
    daemon.await.unwrap();
}

fn assert_pair_linked(
    recording: &RecordingSinkFactory,
    parent_session: &str,
    child_session: &str,
    label: &str,
) {
    let parent = recording.session(parent_session);
    let child = recording.session(child_session);
    assert!(!parent.source.is_empty(), "{label}: parent source absent");
    assert!(!child.source.is_empty(), "{label}: child source absent");

    let parent_rows = inserted_rows(&parent);
    let child_rows = inserted_rows(&child);
    let parent_root = session_root(&parent_rows, parent_session, label);
    let parent_tool = parent_rows
        .iter()
        .find(|row| row.span_type == SpanType::Tool)
        .unwrap_or_else(|| panic!("{label}: parent emitted no tool span"));
    let child_root = session_root(&child_rows, child_session, label);

    assert_eq!(
        child_root.parent_span_ids,
        vec![parent_tool.span_id.clone()],
        "{label}: child root was not parented to the spawning tool"
    );

    let configs = child.configs.lock().unwrap();
    let attached = configs.iter().find_map(|config| match &config.destination {
        Some(TraceDestination::ParentSpan { components }) => Some(components),
        _ => None,
    });
    let attached = attached.unwrap_or_else(|| panic!("{label}: child sink was never attached"));
    assert_eq!(
        attached.span_id.as_deref(),
        Some(parent_tool.span_id.as_str()),
        "{label}: sink attachment chose a different parent"
    );
    assert_eq!(
        attached.root_span_id.as_deref(),
        Some(parent_root.root_span_id.as_str()),
        "{label}: sink attachment chose a different trace root"
    );
}

fn inserted_rows(record: &RecordedSession) -> Vec<SpanRow> {
    record
        .ops
        .lock()
        .unwrap()
        .iter()
        .filter_map(|op| match op {
            SpanOp::Insert(row) => Some(row.clone()),
            SpanOp::Merge(_) => None,
        })
        .collect()
}

fn session_root<'a>(rows: &'a [SpanRow], session: &str, label: &str) -> &'a SpanRow {
    rows.iter()
        .find(|row| {
            row.span_type == SpanType::Task
                && row
                    .metadata
                    .as_ref()
                    .and_then(|metadata| metadata.get("session_id"))
                    .and_then(serde_json::Value::as_str)
                    == Some(session)
        })
        .unwrap_or_else(|| panic!("{label}: no root span found for {session}"))
}

async fn forward_all(events: &mut [Envelope], socket: &Path, host: &HostInfo) {
    for event in events.iter().cloned() {
        forward(event, socket, host).await;
    }
}

async fn forward(event: Envelope, socket: &Path, host: &HostInfo) {
    forward_envelope(&event, socket, host, false)
        .await
        .unwrap_or_else(|error| {
            panic!(
                "forward {} {} event {}: {error}",
                event.source, event.session_id, event.event
            )
        });
}

async fn flush(session: &str, socket: &Path) {
    let result = flush_session(session, socket, 5_000)
        .await
        .unwrap_or_else(|error| panic!("flush {session}: {error}"));
    assert!(
        result.flushed && result.pending == 0,
        "session {session} did not fully flush: {result:?}"
    );
}

async fn start_daemon() -> (
    PathBuf,
    tokio::task::JoinHandle<()>,
    Arc<RecordingSinkFactory>,
    tempfile::TempDir,
) {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("data");
    let socket = test_endpoint(tmp.path());
    let recording = Arc::new(RecordingSinkFactory::default());
    let daemon = spawn_daemon(socket.clone(), data_dir, recording.clone()).await;
    (socket, daemon, recording, tmp)
}

async fn spawn_daemon(
    socket: PathBuf,
    data_dir: PathBuf,
    recording: Arc<RecordingSinkFactory>,
) -> tokio::task::JoinHandle<()> {
    spawn_daemon_with_timeouts(socket, data_dir, recording, 0, 0).await
}

async fn spawn_daemon_with_timeouts(
    socket: PathBuf,
    data_dir: PathBuf,
    recording: Arc<RecordingSinkFactory>,
    idle_timeout_secs: u64,
    session_idle_timeout_secs: u64,
) -> tokio::task::JoinHandle<()> {
    let opts = ServeOptions {
        version: "test".into(),
        translators: Arc::new(Registry::default_agents()),
        sink_factory: recording.clone(),
        auth_provider: Some(Arc::new(TestAuthProvider)),
    };
    let args = ServeArgs {
        socket: Some(socket.clone()),
        data_dir: Some(data_dir),
        idle_timeout_secs,
        session_idle_timeout_secs,
    };
    let daemon = tokio::spawn(async move {
        let _ = run_serve(args, opts).await;
    });
    for _ in 0..200 {
        if matches!(
            run_status(StatusArgs {
                socket: Some(socket.clone()),
                session_id: None,
            })
            .await,
            Ok(Some(_))
        ) {
            return daemon;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("distributed tracing test daemon did not start");
}

fn test_endpoint(root: &Path) -> PathBuf {
    #[cfg(unix)]
    {
        root.join("distributed.sock")
    }
    #[cfg(windows)]
    {
        let _ = root;
        PathBuf::from(format!(
            r"\\.\pipe\bt-daemon-distributed-test-{}",
            uuid::Uuid::new_v4()
        ))
    }
}
