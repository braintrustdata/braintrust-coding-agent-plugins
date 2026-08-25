//! Per-session dispatch. Each session owns an ordered queue and a single actor
//! task that runs its translator + sink serially, so events for one session
//! are processed strictly in arrival order. Different sessions run
//! concurrently.
//!
//! Ack semantics: `event.log` is acked once the event is journaled and its
//! first translation batch has updated local correlation state. Tool-start
//! events drain bounded translator continuations before ack so the spawning
//! marker is guaranteed visible. A downstream error never fails the caller's
//! turn.

use crate::sink::SinkFactory;
use crate::translate::{Registry, SessionCtx};
use crate::wire::{Envelope, SessionRoute};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
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
    Event(Box<Envelope>, oneshot::Sender<()>),
    Configure(Box<crate::wire::SessionConfig>, oneshot::Sender<()>),
    Flush(oneshot::Sender<u64>),
    Shutdown(oneshot::Sender<()>),
}

/// Where to rebuild a session's translator state from, streamed at startup.
pub struct ReplayPlan {
    pub journal_path: PathBuf,
    pub route: SessionRoute,
    /// Replay stops here — the journal's length when this session was
    /// created, so the event creating it is not replayed and then delivered
    /// a second time from the queue.
    pub through: u64,
}

pub(crate) struct SessionOptions {
    pub session_id: String,
    pub source: String,
    pub plugin_version: Option<String>,
    pub replay: Option<ReplayPlan>,
    pub config: crate::wire::SessionConfig,
    pub correlation_key: String,
    pub route: SessionRoute,
    pub correlation: Arc<crate::correlation::CorrelationRegistry>,
    pub data_dir: PathBuf,
}

/// Handle to one live session: its queue plus observable counters/state.
pub struct Session {
    pub source: String,
    tx: mpsc::Sender<SessionMsg>,
    pub counters: Arc<Counters>,
    pub last_error: Arc<Mutex<Option<String>>>,
    pub permalink: Arc<Mutex<Option<String>>>,
    last_activity: Mutex<Instant>,
}

impl Session {
    /// Spawn a session's actor task and return its handle.
    pub fn spawn(
        options: SessionOptions,
        translators: Arc<Registry>,
        sink_factory: Arc<dyn SinkFactory>,
    ) -> Arc<Session> {
        let SessionOptions {
            session_id,
            source,
            plugin_version,
            replay,
            config,
            correlation_key,
            route,
            correlation,
            data_dir,
        } = options;
        let (tx, rx) = mpsc::channel(QUEUE_CAPACITY);
        let counters = Arc::new(Counters::default());
        let last_error = Arc::new(Mutex::new(None));
        let permalink = Arc::new(Mutex::new(None));

        let actor = SessionActor {
            session_id: session_id.clone(),
            source: source.clone(),
            plugin_version,
            translators,
            sink_factory,
            counters: counters.clone(),
            last_error: last_error.clone(),
            permalink: permalink.clone(),
            replay,
            config,
            correlation_key,
            route,
            correlation,
            data_dir,
        };
        tokio::spawn(actor.run(rx));

        Arc::new(Session {
            source,
            tx,
            counters,
            last_error,
            permalink,
            last_activity: Mutex::new(Instant::now()),
        })
    }

    /// Enqueue an event after the daemon has journaled it.
    pub async fn enqueue(&self, env: Envelope) -> anyhow::Result<()> {
        self.touch();
        self.counters.queued.fetch_add(1, Ordering::Relaxed);
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(SessionMsg::Event(Box::new(env), reply_tx))
            .await
            .map_err(|_| anyhow::anyhow!("session actor is gone"))?;
        reply_rx
            .await
            .map_err(|_| anyhow::anyhow!("session actor dropped event acknowledgement"))
    }

    fn touch(&self) {
        *self.last_activity.lock().unwrap() = Instant::now();
    }

    /// How long since this session last saw traffic. Drives idle retirement.
    pub fn idle_for(&self) -> std::time::Duration {
        self.last_activity.lock().unwrap().elapsed()
    }

    /// Ask the actor to drain and flush its sink, bounded by `timeout`.
    /// Returns `(flushed, pending)`.
    pub async fn flush(&self, timeout: std::time::Duration) -> (bool, u64) {
        self.touch();
        let (reply_tx, reply_rx) = oneshot::channel();
        if self.tx.send(SessionMsg::Flush(reply_tx)).await.is_err() {
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
        let (reply_tx, reply_rx) = oneshot::channel();
        if self.tx.send(SessionMsg::Shutdown(reply_tx)).await.is_ok() {
            let _ = reply_rx.await;
        }
    }
}

/// Claude transcript files are external mutable state. Mirror them into
/// daemon-owned storage at lifecycle boundaries and journal only a reference,
/// so recovery/replay does not depend on a path that Claude may later rewrite
/// or delete — and so the transcript is stored once rather than re-copied into
/// every event. Fail open: without a reference the translator reads the live
/// path exactly as before.
pub(crate) async fn hydrate_transcript_reference(data_dir: &std::path::Path, env: &mut Envelope) {
    if env.source != "claude-code"
        || !matches!(
            env.event.as_str(),
            "UserPromptSubmit" | "Stop" | "StopFailure" | "SubagentStop" | "SessionEnd"
        )
    {
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
    let (mirror, through) =
        match crate::transcript_mirror::capture(data_dir, &env.session_id, &path).await {
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

struct SessionActor {
    session_id: String,
    source: String,
    plugin_version: Option<String>,
    translators: Arc<Registry>,
    sink_factory: Arc<dyn SinkFactory>,
    counters: Arc<Counters>,
    last_error: Arc<Mutex<Option<String>>>,
    permalink: Arc<Mutex<Option<String>>>,
    replay: Option<ReplayPlan>,
    config: crate::wire::SessionConfig,
    correlation_key: String,
    route: SessionRoute,
    correlation: Arc<crate::correlation::CorrelationRegistry>,
    data_dir: PathBuf,
}

#[derive(Clone, Copy)]
enum BatchMode {
    Live,
    Replay,
    Flush,
}

impl BatchMode {
    fn errors(self) -> (&'static str, &'static str) {
        match self {
            Self::Live => ("translate failed", "sink emit failed"),
            Self::Replay => ("journal replay failed", "sink replay emit failed"),
            Self::Flush => ("translate flush failed", "sink emit (flush) failed"),
        }
    }

    fn observes_correlation(self) -> bool {
        !matches!(self, Self::Flush)
    }
}

impl SessionActor {
    async fn run(self, mut rx: mpsc::Receiver<SessionMsg>) {
        let mut translator = self.translators.create(&self.source, &self.session_id);
        let mut sink = match self.sink_factory.create(
            &self.session_id,
            &self.source,
            self.plugin_version.as_deref(),
        ) {
            Ok(s) => s,
            Err(e) => {
                self.set_error(format!("sink init failed: {e}"));
                // Still drain the queue so the daemon's counters settle and
                // callers waiting on flush don't hang.
                while let Some(msg) = rx.recv().await {
                    match msg {
                        SessionMsg::Event(_, reply) => {
                            self.counters.queued.fetch_sub(1, Ordering::Relaxed);
                            let _ = reply.send(());
                        }
                        SessionMsg::Configure(_, r) => {
                            let _ = r.send(());
                        }
                        SessionMsg::Flush(r) => {
                            let _ = r.send(0);
                        }
                        SessionMsg::Shutdown(r) => {
                            let _ = r.send(());
                            break;
                        }
                    }
                }
                return;
            }
        };
        let mut ctx = SessionCtx {
            session_id: self.session_id.clone(),
            config: Some(self.config.clone()),
        };
        sink.configure(&self.config);
        self.refresh_permalink(sink.as_ref());
        // Rebuild translator state before accepting the first new event.
        // Stable span ids make this both crash recovery and a complete copy
        // when an existing source session is sent to another destination.
        self.replay_into(&mut translator, &mut sink, &ctx).await;

        while let Some(msg) = rx.recv().await {
            match msg {
                SessionMsg::Event(env, reply) => {
                    let correlation_barrier = is_tool_lifecycle_event(&env.event);
                    let mut reply = Some(reply);
                    if let Some(cfg) = &env.config {
                        sink.configure(cfg);
                        ctx.config = Some(cfg.clone());
                        self.refresh_permalink(sink.as_ref());
                    }
                    let translated = translator.handle(&env, &ctx);
                    if !correlation_barrier {
                        let _ = reply.take().expect("event reply").send(());
                    }
                    let correlation_changed = self
                        .emit_translator_batches(
                            &mut translator,
                            &mut sink,
                            &ctx,
                            translated,
                            BatchMode::Live,
                        )
                        .await;
                    if correlation_changed || correlation_barrier {
                        if let Err(error) = crate::server::persist_active_parent_snapshot(
                            &self.data_dir,
                            &self.correlation_key,
                            &self.correlation,
                        )
                        .await
                        {
                            self.set_error(error);
                        }
                    }
                    if let Some(reply) = reply {
                        let _ = reply.send(());
                    }
                    self.counters.queued.fetch_sub(1, Ordering::Relaxed);
                }
                SessionMsg::Configure(config, reply) => {
                    sink.configure(&config);
                    ctx.config = Some(*config);
                    self.refresh_permalink(sink.as_ref());
                    let _ = reply.send(());
                }
                SessionMsg::Flush(reply) => {
                    self.drain_flush(&mut translator, &mut sink, &ctx).await;
                    let _ = reply.send(self.counters.queued.load(Ordering::Relaxed));
                }
                SessionMsg::Shutdown(reply) => {
                    self.drain_flush(&mut translator, &mut sink, &ctx).await;
                    let _ = reply.send(());
                    break;
                }
            }
        }
    }

    /// Emit a translator result and every bounded continuation it schedules.
    /// A continuation may be empty (for example, irrelevant rollout rows), so
    /// only `None` signals that the translator is fully caught up.
    async fn emit_translator_batches(
        &self,
        translator: &mut Box<dyn crate::translate::AgentTranslator>,
        sink: &mut Box<dyn crate::sink::Sink>,
        ctx: &SessionCtx,
        first: anyhow::Result<Vec<crate::translate::SpanOp>>,
        mode: BatchMode,
    ) -> bool {
        let (translate_error, emit_error) = mode.errors();
        let mut next = match first {
            Ok(ops) => Some(ops),
            Err(e) => {
                self.set_error(format!("{translate_error}: {e}"));
                return false;
            }
        };
        let mut correlation_changed = false;
        while let Some(ops) = next {
            if !ops.is_empty() {
                if mode.observes_correlation() {
                    correlation_changed |= self.correlation.observe_ops(
                        &self.correlation_key,
                        &self.route,
                        ctx.config.as_ref().expect("session config"),
                        &ops,
                    );
                }
                match sink.emit(&ops).await {
                    Ok(n) => {
                        self.counters.spans_emitted.fetch_add(n, Ordering::Relaxed);
                    }
                    Err(e) => self.set_error(format!("{emit_error}: {e}")),
                }
            }
            next = match translator.drain_pending(ctx) {
                Ok(next) => next,
                Err(e) => {
                    self.set_error(format!("{translate_error}: {e}"));
                    None
                }
            };
        }
        correlation_changed
    }

    /// Stream the journal through the translator, emitting each entry's spans
    /// as they are produced. Nothing is accumulated across entries: peak
    /// memory is one journal entry and the ops it yields, so recovering a
    /// long session costs the same as running it.
    async fn replay_into(
        &self,
        translator: &mut Box<dyn crate::translate::AgentTranslator>,
        sink: &mut Box<dyn crate::sink::Sink>,
        ctx: &SessionCtx,
    ) {
        let Some(plan) = &self.replay else {
            return;
        };
        let mut reader = match crate::journal::JournalReader::open(&plan.journal_path, plan.through)
            .await
        {
            Ok(Some(reader)) => reader,
            Ok(None) => return,
            Err(error) => {
                tracing::warn!(session_id = %self.session_id, "journal replay skipped: {error}");
                return;
            }
        };
        loop {
            let entry = match reader.next_entry().await {
                Ok(Some(entry)) => entry,
                Ok(None) => break,
                Err(error) => {
                    tracing::warn!(session_id = %self.session_id, "journal replay stopped: {error}");
                    break;
                }
            };
            if !entry
                .route
                .as_ref()
                .is_some_and(|candidate| candidate.same_route(&plan.route))
            {
                continue;
            }
            let env = crate::journal::envelope_from_redacted(entry);
            let translated = translator.handle(&env, ctx);
            let _ = self
                .emit_translator_batches(translator, sink, ctx, translated, BatchMode::Replay)
                .await;
        }
    }

    async fn drain_flush(
        &self,
        translator: &mut Box<dyn crate::translate::AgentTranslator>,
        sink: &mut Box<dyn crate::sink::Sink>,
        ctx: &SessionCtx,
    ) {
        let translated = translator.flush(ctx);
        let _ = self
            .emit_translator_batches(translator, sink, ctx, translated, BatchMode::Flush)
            .await;
        if let Err(e) = sink.flush().await {
            self.set_error(format!("sink flush failed: {e}"));
        }
        self.refresh_permalink(sink.as_ref());
    }

    fn refresh_permalink(&self, sink: &dyn crate::sink::Sink) {
        if let Some(link) = sink.permalink() {
            *self.permalink.lock().unwrap() = Some(link);
        }
    }

    fn set_error(&self, msg: String) {
        tracing::warn!(session_id = %self.session_id, "{msg}");
        *self.last_error.lock().unwrap() = Some(msg);
    }
}

pub(crate) fn is_tool_lifecycle_event(event: &str) -> bool {
    matches!(
        event,
        "PreToolUse"
            | "PostToolUse"
            | "PostToolUseFailure"
            | "tool_execution_start"
            | "tool_execution_end"
            | "tool.execute.before"
            | "tool.execute.after"
    )
}
