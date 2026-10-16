//! Per-session dispatch. Each session owns an ordered queue and a single actor
//! task that runs its translator + sink serially, so events for one session
//! are processed strictly in arrival order. Different sessions run
//! concurrently.
//!
//! Hook acknowledgement happens before this layer, immediately after the raw
//! event is durably journaled. Translation, correlation, and delivery here are
//! entirely out-of-band from hook execution.

use crate::pipeline::{
    consumer::{Consumer, ConsumerOptions},
    translation::Translation,
};
use crate::sink::SinkFactory;
use crate::translate::Registry;
use crate::wire::Envelope;
use parking_lot::Mutex;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot};

/// Bound on a session's in-flight queue. Enqueue awaits a slot rather than
/// letting a stalled sink accumulate events without limit; the daemon has
/// already journaled anything waiting here, so backpressure costs latency,
/// never data.
const QUEUE_CAPACITY: usize = 1024;

#[derive(Default)]
pub struct Counters {
    pub queued: AtomicU64,
    pub spans_emitted: AtomicU64,
}

enum SessionMsg {
    Event(Box<Envelope>, u64),
    Configure(Box<crate::wire::SessionConfig>, oneshot::Sender<()>),
    Barrier(oneshot::Sender<()>),
    Flush(oneshot::Sender<u64>),
    Finalize(oneshot::Sender<u64>),
    Shutdown(Option<u64>, oneshot::Sender<()>),
}

/// Where to rebuild a session's translator state from, streamed at startup.
pub struct ReplayPlan {
    pub journal_path: PathBuf,
    /// Events through this offset reached the backend before the prior daemon
    /// stopped. Replay still rebuilds translator state from them, but must not
    /// deliver them again.
    pub acknowledged_through: u64,
}

pub(crate) struct SessionOptions {
    pub session_id: String,
    pub source: String,
    pub bt_version: String,
    pub config: crate::wire::SessionConfig,
    pub data_dir: PathBuf,
    pub translation: Arc<Translation>,
    pub cursor: crate::pipeline::journal::Cursor,
}

/// Handle to one live session: its queue plus observable counters/state.
pub struct Session {
    pub source: String,
    tx: mpsc::Sender<SessionMsg>,
    pub counters: Arc<Counters>,
    pub last_error: Arc<Mutex<Option<String>>>,
    source_progress: Arc<crate::pipeline::translation::Progress>,
    pub source_error: Arc<Mutex<Option<String>>>,
    pub permalink: Arc<Mutex<Option<String>>>,
    last_activity: Mutex<Instant>,
}

impl Session {
    /// Spawn a session's actor task and return its handle.
    pub fn spawn(
        options: SessionOptions,
        _translators: Arc<Registry>,
        sink_factory: Arc<dyn SinkFactory>,
    ) -> Arc<Session> {
        let (tx, mut rx) = mpsc::channel(QUEUE_CAPACITY);
        let counters = Arc::new(Counters::default());
        let last_error = Arc::new(Mutex::new(None));
        let permalink = Arc::new(Mutex::new(None));
        let source = options.source.clone();
        let route = Consumer::spawn(
            options.translation.journal.clone(),
            options.cursor,
            ConsumerOptions {
                source: options.source.clone(),
                session_id: options.session_id.clone(),
                version: options.bt_version,
                data_dir: options.data_dir,
                config: options.config,
                counters: counters.clone(),
                error: last_error.clone(),
                permalink: permalink.clone(),
                factory: sink_factory,
            },
        );
        let producer = options.translation;
        let source_error = producer.error.clone();
        let source_progress = producer.progress.clone();
        let errors = last_error.clone();
        let queued = counters.clone();
        tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                let finalize = matches!(&message, SessionMsg::Finalize(_));
                match message {
                    SessionMsg::Event(env, through) => {
                        if let Some(config) = env.config {
                            route.configure(config);
                        }
                        if let Err(error) = producer.advance(through).await {
                            *errors.lock() = Some(error.to_string());
                        }
                    }
                    SessionMsg::Configure(config, reply) => {
                        route.configure(*config);
                        let _ = reply.send(());
                    }
                    SessionMsg::Barrier(reply) => {
                        let _ = producer.barrier().await;
                        let _ = reply.send(());
                    }
                    SessionMsg::Flush(reply) | SessionMsg::Finalize(reply) => {
                        let kind = if finalize {
                            crate::journal::ControlKind::Finalize
                        } else {
                            crate::journal::ControlKind::Checkpoint
                        };
                        let target = producer.control(kind).await;
                        let route = route.clone();
                        let queued = queued.clone();
                        let errors = errors.clone();
                        tokio::spawn(async move {
                            let mut reply = reply;
                            let waiting = async {
                                match target {
                                    Ok(tail) => match route.through(tail).await {
                                        Ok(()) => 0,
                                        Err(error) => {
                                            *errors.lock() = Some(error);
                                            queued.queued.load(Ordering::Relaxed).max(1)
                                        }
                                    },
                                    Err(error) => {
                                        *errors.lock() = Some(error);
                                        queued.queued.load(Ordering::Relaxed).max(1)
                                    }
                                }
                            };
                            tokio::select! { _=reply.closed()=>{}, pending=waiting=> { let _=reply.send(pending); } }
                        });
                    }
                    SessionMsg::Shutdown(target, reply) => {
                        let last = producer.routes.fetch_sub(1, Ordering::Relaxed) == 1;
                        let target = match target {
                            Some(tail) => Some(tail),
                            None if last => producer
                                .control(crate::journal::ControlKind::Retire)
                                .await
                                .ok(),
                            None => None,
                        };
                        if let Some(tail) = target {
                            let _ = tokio::time::timeout(
                                std::time::Duration::from_secs(2),
                                route.through(tail),
                            )
                            .await;
                        }
                        route.shutdown().await;
                        if producer.routes.load(Ordering::Relaxed) == 0 {
                            producer.shutdown().await;
                        }
                        let _ = reply.send(());
                        break;
                    }
                }
            }
        });

        Arc::new(Session {
            source,
            tx,
            counters,
            last_error,
            source_error,
            source_progress,
            permalink,
            last_activity: Mutex::new(Instant::now()),
        })
    }

    pub(crate) fn pending(&self) -> u64 {
        self.counters
            .queued
            .load(Ordering::Relaxed)
            .saturating_add(self.source_progress.pending())
    }

    /// Queue a journaled event without waiting for translation or delivery.
    pub async fn enqueue(&self, env: Envelope, journal_through: u64) -> anyhow::Result<()> {
        self.touch();

        self.tx
            .send(SessionMsg::Event(Box::new(env), journal_through))
            .await
            .map_err(|_| anyhow::anyhow!("session actor is gone"))
    }

    fn touch(&self) {
        *self.last_activity.lock() = Instant::now();
    }

    /// Wait until events already accepted by this daemon worker have updated
    /// translator and correlation state. Hook capture never calls this.
    pub async fn barrier(&self) {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self.tx.send(SessionMsg::Barrier(reply_tx)).await.is_ok() {
            let _ = reply_rx.await;
        }
    }

    /// How long since this session last saw traffic. Drives idle retirement.
    pub fn idle_for(&self) -> std::time::Duration {
        self.last_activity.lock().elapsed()
    }

    /// Insert a flush in actor order without waiting for backend delivery.
    pub(crate) async fn enqueue_flush(&self) -> anyhow::Result<oneshot::Receiver<u64>> {
        self.touch();
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(SessionMsg::Flush(reply_tx))
            .await
            .map_err(|_| anyhow::anyhow!("session actor is gone"))?;
        Ok(reply_rx)
    }

    /// Ask the actor to drain and flush its sink, bounded by `timeout`.
    /// Returns `(flushed, pending)`.
    pub async fn flush(&self, timeout: std::time::Duration) -> (bool, u64) {
        let Ok(reply_rx) = self.enqueue_flush().await else {
            return (false, self.counters.queued.load(Ordering::Relaxed));
        };
        match tokio::time::timeout(timeout, reply_rx).await {
            Ok(Ok(pending)) => (pending == 0, pending),
            _ => (false, self.counters.queued.load(Ordering::Relaxed)),
        }
    }

    /// Finalize an invocation-local session and flush its sink. Unlike a
    /// delivery checkpoint, a managed-run completion is a terminal boundary.
    pub async fn finalize(&self, timeout: std::time::Duration) -> (bool, u64) {
        self.touch();
        let (reply_tx, reply_rx) = oneshot::channel();
        if self.tx.send(SessionMsg::Finalize(reply_tx)).await.is_err() {
            return (false, self.counters.queued.load(Ordering::Relaxed));
        }
        match tokio::time::timeout(timeout, reply_rx).await {
            Ok(Ok(pending)) => (pending == 0, pending),
            _ => (false, self.counters.queued.load(Ordering::Relaxed)),
        }
    }

    /// Reconfigure the sink before a refresh-triggered flush. Queue ordering
    /// guarantees that all earlier events are processed first.
    pub async fn configure(&self, config: crate::wire::SessionConfig) -> anyhow::Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(SessionMsg::Configure(Box::new(config), reply_tx))
            .await
            .map_err(|_| anyhow::anyhow!("session actor is gone"))?;
        reply_rx
            .await
            .map_err(|_| anyhow::anyhow!("session actor dropped configuration reply"))
    }

    /// Drain, flush, and stop the actor (used on daemon shutdown and when an
    /// idle session is retired).
    pub async fn shutdown(&self) {
        self.shutdown_through(None).await;
    }

    pub(crate) async fn shutdown_through(&self, tail: Option<u64>) {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self
            .tx
            .send(SessionMsg::Shutdown(tail, reply_tx))
            .await
            .is_ok()
        {
            let _ = reply_rx.await;
        }
    }
}

/// Agent transcript files are external mutable state. Mirror them into
/// daemon-owned storage at lifecycle boundaries and journal only bounded
/// references, so recovery sees exactly the bytes that live translation
/// observed instead of depending on mutable external paths or copying a full
/// transcript into every event. Capture failures remain fail-open.
pub(crate) async fn hydrate_transcript_reference(data_dir: &std::path::Path, env: &mut Envelope) {
    if env.source == "cursor" {
        hydrate_cursor_transcript_reference(data_dir, env).await;
        return;
    }
    if env.source == "grok" {
        hydrate_grok_transcript_references(data_dir, env).await;
        return;
    }
    if env.source == "claude-code" {
        hydrate_claude_transcript_references(data_dir, env).await;
        return;
    }
    if env.source != "codex" {
        return;
    }
    let field = if env.event == "SubagentStop" {
        "agent_transcript_path"
    } else {
        "transcript_path"
    };
    let Some(path) = env
        .payload
        .get(field)
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
    else {
        return;
    };
    let mirror_session = crate::ids::session_namespace(&env.source, &env.session_id);
    let (mirror, through) =
        match crate::transcript_mirror::capture(data_dir, &mirror_session, &path).await {
            Ok(captured) => captured,
            Err(error) => {
                tracing::debug!(session_id = %env.session_id, %error, "transcript mirror skipped");
                return;
            }
        };
    if let Some(payload) = env.payload.as_object_mut() {
        payload.insert(
            "_bt_transcript_mirror".to_string(),
            serde_json::json!({
                "path": path,
                "mirror": mirror.to_string_lossy(),
                "through": through,
            }),
        );
    }
}

async fn hydrate_cursor_transcript_reference(data_dir: &std::path::Path, env: &mut Envelope) {
    if env.payload.get("_bt_transcript_mirror").is_some() {
        return;
    }
    let path = env
        .payload
        .get("transcript_path")
        .and_then(serde_json::Value::as_str);
    let mut observation = serde_json::json!({});
    if let Some(path) = path.filter(|path| !path.is_empty()) {
        let observed_bytes = env
            .payload
            .get("_bt_transcript_observation")
            .filter(|observation| {
                observation.get("path").and_then(serde_json::Value::as_str) == Some(path)
            })
            .and_then(|observation| observation.get("observed_bytes"))
            .and_then(serde_json::Value::as_u64);
        let mirror_session = crate::ids::session_namespace(&env.source, &env.session_id);
        match crate::transcript_mirror::capture_generation(
            data_dir,
            &mirror_session,
            path,
            observed_bytes,
        )
        .await
        {
            Ok((mirror, through)) => {
                observation =
                    serde_json::json!({"path": path, "mirror": mirror, "through": through});
            }
            Err(error) => {
                tracing::debug!(session_id = %env.session_id, %error, "Cursor transcript mirror skipped");
            }
        }
    }
    if let Some(payload) = env.payload.as_object_mut() {
        // An empty observation is also durable: no transcript was available
        // at this boundary, even if the external path appears during replay.
        payload.insert("_bt_transcript_mirror".to_string(), observation);
    }
}

async fn hydrate_claude_transcript_references(data_dir: &std::path::Path, env: &mut Envelope) {
    // Historical snapshots and newly captured references are observations,
    // not invitations to reread an external file during journal recovery.
    if env.payload.get("_bt_claude_transcript_mirrors").is_some()
        || env.payload.get("_bt_transcript_mirror").is_some()
        || env.payload.get("_bt_transcript_snapshot").is_some()
        || env.payload.get("_bt_transcript_replay").is_some()
    {
        return;
    }
    let mirror_session = crate::ids::session_namespace(&env.source, &env.session_id);
    let mut mirrors = serde_json::Map::new();
    for field in ["transcript_path", "agent_transcript_path"] {
        let Some(path) = env.payload.get(field).and_then(serde_json::Value::as_str) else {
            continue;
        };
        if mirrors.contains_key(path) {
            continue;
        }
        match crate::transcript_mirror::capture(data_dir, &mirror_session, path).await {
            Ok((mirror, through)) => {
                mirrors.insert(
                    path.to_string(),
                    serde_json::json!({"path": path, "mirror": mirror, "through": through}),
                );
            }
            Err(error) => {
                tracing::debug!(session_id = %env.session_id, %error, "Claude transcript mirror skipped");
            }
        }
    }
    if !env.payload.is_object() {
        env.payload = serde_json::json!({});
    }
    if let Some(payload) = env.payload.as_object_mut() {
        payload.insert(
            "_bt_claude_transcript_mirrors".to_string(),
            serde_json::Value::Object(mirrors),
        );
    }
}

const GROK_TERMINAL_SNAPSHOT_ATTEMPTS: usize = 8;
const GROK_TERMINAL_SNAPSHOT_QUIET_PERIOD: std::time::Duration =
    std::time::Duration::from_millis(10);

async fn hydrate_grok_transcript_references(data_dir: &std::path::Path, env: &mut Envelope) {
    let Some(transcript) = env
        .payload
        .get("transcriptPath")
        .or_else(|| env.payload.get("transcript_path"))
        .and_then(serde_json::Value::as_str)
        .map(std::path::PathBuf::from)
    else {
        return;
    };
    let Some(session_dir) = transcript.parent() else {
        return;
    };
    let mirror_session = crate::ids::session_namespace(&env.source, &env.session_id);
    let mut mirrors = serde_json::Map::new();
    capture_grok_transcript_pass(
        data_dir,
        session_dir,
        &mirror_session,
        &env.session_id,
        &mut mirrors,
    )
    .await;

    if matches!(env.event.as_str(), "SessionEnd" | "session_end") {
        for _ in 0..GROK_TERMINAL_SNAPSHOT_ATTEMPTS {
            tokio::time::sleep(GROK_TERMINAL_SNAPSHOT_QUIET_PERIOD).await;
            capture_grok_transcript_pass(
                data_dir,
                session_dir,
                &mirror_session,
                &env.session_id,
                &mut mirrors,
            )
            .await;
        }
    }

    if !mirrors.is_empty() {
        if let Some(payload) = env.payload.as_object_mut() {
            payload.insert(
                "_bt_grok_transcript_mirrors".to_string(),
                serde_json::Value::Object(mirrors),
            );
        }
    }
}

async fn capture_grok_transcript_pass(
    data_dir: &std::path::Path,
    session_dir: &std::path::Path,
    mirror_session: &str,
    session_id: &str,
    mirrors: &mut serde_json::Map<String, serde_json::Value>,
) {
    for (name, key) in [
        ("updates.jsonl", "updates"),
        ("events.jsonl", "events"),
        ("system_prompt.txt", "system_prompt"),
    ] {
        let source = session_dir.join(name);
        let Some(source_str) = source.to_str() else {
            continue;
        };
        match crate::transcript_mirror::capture(data_dir, mirror_session, source_str).await {
            Ok((mirror, through)) => {
                mirrors.insert(
                    key.to_string(),
                    serde_json::json!({
                        "path": source,
                        "mirror": mirror,
                        "through": through,
                    }),
                );
            }
            Err(error) => {
                tracing::debug!(%session_id, %error, file = name, "Grok transcript mirror skipped");
            }
        }
    }
}

pub(crate) fn is_tool_lifecycle_event(event: &str) -> bool {
    matches!(
        event,
        "PreToolUse"
            | "PostToolUse"
            | "PostToolUseFailure"
            | "preToolUse"
            | "postToolUse"
            | "postToolUseFailure"
            | "tool_execution_start"
            | "tool_execution_end"
            | "tool.execute.before"
            | "tool.execute.after"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cursor_hydration_freezes_hook_bytes_and_missing_paths() {
        let tmp = tempfile::tempdir().unwrap();
        let transcript = tmp.path().join("cursor.jsonl");
        std::fs::write(&transcript, b"first\nfuture\n").unwrap();
        let mut env: Envelope = serde_json::from_value(serde_json::json!({
            "source":"cursor", "session_id":"cursor-boundary", "event":"stop", "ts_ms":1,
            "payload": {
                "transcript_path":transcript,
                "_bt_transcript_observation":{"path":transcript,"observed_bytes":6}
            }
        }))
        .unwrap();
        hydrate_transcript_reference(tmp.path(), &mut env).await;
        let observation = env.payload["_bt_transcript_mirror"].clone();
        assert_eq!(observation["through"], 6);
        assert_eq!(
            std::fs::read(observation["mirror"].as_str().unwrap()).unwrap(),
            b"first\n"
        );
        std::fs::write(&transcript, b"replacement\n").unwrap();
        hydrate_transcript_reference(tmp.path(), &mut env).await;
        assert_eq!(env.payload["_bt_transcript_mirror"], observation);

        env.payload = serde_json::json!({"transcript_path":null});
        hydrate_transcript_reference(tmp.path(), &mut env).await;
        assert_eq!(env.payload["_bt_transcript_mirror"], serde_json::json!({}));
        env.payload["transcript_path"] = serde_json::json!(transcript);
        hydrate_transcript_reference(tmp.path(), &mut env).await;
        assert_eq!(env.payload["_bt_transcript_mirror"], serde_json::json!({}));
    }

    #[tokio::test]
    async fn enqueue_flush_preserves_boundary_without_waiting_for_delivery() {
        let (tx, mut rx) = mpsc::channel(4);
        let session = Session {
            source: "pi".into(),
            tx,
            counters: Arc::new(Counters::default()),
            last_error: Arc::new(Mutex::new(None)),
            source_error: Arc::new(Mutex::new(None)),
            source_progress: Arc::new(crate::pipeline::translation::Progress::default()),
            permalink: Arc::new(Mutex::new(None)),
            last_activity: Mutex::new(Instant::now()),
        };
        let envelope = |event| {
            serde_json::from_value::<Envelope>(serde_json::json!({
                "source": "pi", "session_id": "ordered-turns", "event": event,
                "ts_ms": 1, "payload": {"event": {}}
            }))
            .unwrap()
        };
        session.enqueue(envelope("agent_end"), 1).await.unwrap();
        // No actor is reading yet: enqueue must complete independently of delivery.
        let mut completion =
            tokio::time::timeout(std::time::Duration::from_secs(1), session.enqueue_flush())
                .await
                .expect("enqueue waited for delivery")
                .unwrap();
        session
            .enqueue(envelope("before_agent_start"), 2)
            .await
            .unwrap();
        assert!(matches!(
            completion.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(
            matches!(rx.recv().await, Some(SessionMsg::Event(env, 1)) if env.event == "agent_end")
        );
        let Some(SessionMsg::Flush(reply)) = rx.recv().await else {
            panic!("next turn overtook the boundary flush");
        };
        assert!(
            matches!(rx.recv().await, Some(SessionMsg::Event(env, 2)) if env.event == "before_agent_start")
        );
        reply.send(1).unwrap();
        assert_eq!(completion.await.unwrap(), 1);
        drop(rx);
        assert!(session.enqueue_flush().await.is_err());
    }

    #[tokio::test]
    async fn grok_hydration_mirrors_transcripts_and_system_prompt_at_one_boundary() {
        let tmp = tempfile::tempdir().unwrap();
        let session = tmp.path().join("native-session");
        tokio::fs::create_dir(&session).await.unwrap();
        tokio::fs::write(session.join("chat_history.jsonl"), b"")
            .await
            .unwrap();
        tokio::fs::write(session.join("updates.jsonl"), b"{\"update\":1}\n")
            .await
            .unwrap();
        tokio::fs::write(session.join("events.jsonl"), b"{\"event\":1}\n")
            .await
            .unwrap();
        tokio::fs::write(session.join("system_prompt.txt"), b"You are Grok.")
            .await
            .unwrap();
        let mut env = Envelope {
            source: "grok".into(),
            source_version: None,
            plugin_version: None,
            session_id: "session-1".into(),
            event: "stop".into(),
            ts_ms: 1,
            managed_run_id: None,
            capture: None,
            payload: serde_json::json!({
                "transcriptPath": session.join("chat_history.jsonl")
            }),
            route: None,
            config: None,
        };

        hydrate_transcript_reference(tmp.path(), &mut env).await;

        let mirrors = &env.payload["_bt_grok_transcript_mirrors"];
        for name in ["updates", "events", "system_prompt"] {
            let path = mirrors[name]["mirror"].as_str().unwrap();
            assert!(std::path::Path::new(path).is_file());
            assert!(mirrors[name]["through"].as_u64().unwrap() > 0);
        }
    }

    #[tokio::test]
    async fn terminal_grok_hydration_waits_for_a_stable_transcript_boundary() {
        let tmp = tempfile::tempdir().unwrap();
        let session = tmp.path().join("native-session");
        tokio::fs::create_dir(&session).await.unwrap();
        tokio::fs::write(session.join("chat_history.jsonl"), b"")
            .await
            .unwrap();
        let updates = session.join("updates.jsonl");
        let initial = b"{\"update\":1}\n";
        tokio::fs::write(&updates, initial).await.unwrap();
        let mut env = Envelope {
            source: "grok".into(),
            source_version: None,
            plugin_version: None,
            session_id: "session-1".into(),
            event: "session_end".into(),
            ts_ms: 1,
            managed_run_id: None,
            capture: None,
            payload: serde_json::json!({
                "transcriptPath": session.join("chat_history.jsonl")
            }),
            route: None,
            config: None,
        };

        let mirror = crate::transcript_mirror::mirror_path(
            tmp.path(),
            &crate::ids::session_namespace("grok", "session-1"),
            updates.to_str().unwrap(),
        );
        {
            let hydration = hydrate_transcript_reference(tmp.path(), &mut env);
            tokio::pin!(hydration);
            tokio::select! {
                biased;
                () = async {
                    // Append after the first copy, before polling hydration again.
                    // A delayed task can miss the entire capture window on busy CI.
                    while !std::fs::metadata(&mirror).is_ok_and(|meta| meta.len() == initial.len() as u64) {
                        tokio::task::yield_now().await;
                    }
                    std::fs::write(&updates, b"{\"update\":1}\n{\"update\":2}\n").unwrap();
                } => {},
                () = &mut hydration => panic!("terminal hydration did not wait for another capture"),
                _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
                    panic!("initial transcript capture did not complete");
                },
            }
            hydration.await;
        }

        let update_mirror = &env.payload["_bt_grok_transcript_mirrors"]["updates"];
        assert_eq!(
            update_mirror["through"],
            std::fs::metadata(&updates).unwrap().len()
        );
        assert_eq!(
            std::fs::read(update_mirror["mirror"].as_str().unwrap()).unwrap(),
            std::fs::read(updates).unwrap()
        );
    }
}
