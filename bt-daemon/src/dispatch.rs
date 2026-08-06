//! Per-session dispatch. Each session owns an ordered queue and a single actor
//! task that runs its translator + sink serially, so events for one session
//! are processed strictly in arrival order. Different sessions run
//! concurrently.
//!
//! Ack semantics: `event.log` is acked once the event is journaled and handed
//! to the session's queue (see [`Session::append_and_enqueue`]). Delivery to
//! Braintrust happens later in the actor; a downstream error never fails the
//! caller's turn.

use crate::journal::JournalWriter;
use crate::sink::SinkFactory;
use crate::translate::{Registry, SessionCtx};
use crate::wire::Envelope;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};

#[derive(Default)]
pub struct Counters {
    pub queued: AtomicU64,
    pub spans_emitted: AtomicU64,
}

enum SessionMsg {
    Event(Box<Envelope>),
    Configure(Box<crate::wire::SessionConfig>, oneshot::Sender<()>),
    Flush(oneshot::Sender<u64>),
    Shutdown(oneshot::Sender<()>),
}

/// Handle to one live session: its queue plus observable counters/state.
pub struct Session {
    pub source: String,
    tx: mpsc::UnboundedSender<SessionMsg>,
    journal: tokio::sync::Mutex<JournalWriter>,
    pub counters: Arc<Counters>,
    pub last_error: Arc<Mutex<Option<String>>>,
    pub permalink: Arc<Mutex<Option<String>>>,
}

impl Session {
    /// Spawn a session's actor task and return its handle.
    pub fn spawn(
        session_id: String,
        source: String,
        journal: JournalWriter,
        replay: Vec<Envelope>,
        translators: Arc<Registry>,
        sink_factory: Arc<dyn SinkFactory>,
    ) -> Arc<Session> {
        let (tx, rx) = mpsc::unbounded_channel();
        let counters = Arc::new(Counters::default());
        let last_error = Arc::new(Mutex::new(None));
        let permalink = Arc::new(Mutex::new(None));

        let actor = SessionActor {
            session_id: session_id.clone(),
            source: source.clone(),
            translators,
            sink_factory,
            counters: counters.clone(),
            last_error: last_error.clone(),
            permalink: permalink.clone(),
            replay,
        };
        tokio::spawn(actor.run(rx));

        Arc::new(Session {
            source,
            tx,
            journal: tokio::sync::Mutex::new(journal),
            counters,
            last_error,
            permalink,
        })
    }

    /// Journal (redacted) then enqueue. Both complete before the caller acks.
    pub async fn append_and_enqueue(&self, mut env: Envelope) -> anyhow::Result<()> {
        hydrate_transcript_snapshot(&mut env).await;
        {
            let mut j = self.journal.lock().await;
            j.append(&env).await?;
        }
        self.counters.queued.fetch_add(1, Ordering::Relaxed);
        self.tx
            .send(SessionMsg::Event(Box::new(env)))
            .map_err(|_| anyhow::anyhow!("session actor is gone"))?;
        Ok(())
    }

    /// Ask the actor to drain and flush its sink, bounded by `timeout`.
    /// Returns `(flushed, pending)`.
    pub async fn flush(&self, timeout: std::time::Duration) -> (bool, u64) {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self.tx.send(SessionMsg::Flush(reply_tx)).is_err() {
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
            .map_err(|_| anyhow::anyhow!("session actor is gone"))?;
        reply_rx
            .await
            .map_err(|_| anyhow::anyhow!("session actor dropped configuration reply"))
    }

    /// Drain, flush, and stop the actor (used on daemon shutdown).
    pub async fn shutdown(&self) {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self.tx.send(SessionMsg::Shutdown(reply_tx)).is_ok() {
            let _ = reply_rx.await;
        }
    }
}

/// Claude transcript files are external mutable state. Capture them in the
/// journal at lifecycle boundaries so recovery/replay does not depend on a
/// path that Claude may later rewrite or delete. Fail open: a missing file is
/// handled by the translator exactly as before.
async fn hydrate_transcript_snapshot(env: &mut Envelope) {
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
    let Ok(contents) = tokio::fs::read_to_string(&path).await else {
        return;
    };
    if let Some(payload) = env.payload.as_object_mut() {
        payload.insert(
            "_bt_transcript_snapshot".to_string(),
            serde_json::json!({ "path": path, "contents": contents }),
        );
    }
}

struct SessionActor {
    session_id: String,
    source: String,
    translators: Arc<Registry>,
    sink_factory: Arc<dyn SinkFactory>,
    counters: Arc<Counters>,
    last_error: Arc<Mutex<Option<String>>>,
    permalink: Arc<Mutex<Option<String>>>,
    replay: Vec<Envelope>,
}

impl SessionActor {
    async fn run(self, mut rx: mpsc::UnboundedReceiver<SessionMsg>) {
        let mut translator = self.translators.create(&self.source, &self.session_id);
        let mut sink = match self.sink_factory.create(&self.session_id, &self.source) {
            Ok(s) => s,
            Err(e) => {
                self.set_error(format!("sink init failed: {e}"));
                // Still drain the queue so the daemon's counters settle and
                // callers waiting on flush don't hang.
                while let Some(msg) = rx.recv().await {
                    if let SessionMsg::Event(_) = msg {
                        self.counters.queued.fetch_sub(1, Ordering::Relaxed);
                    } else if let SessionMsg::Configure(_, r) = msg {
                        let _ = r.send(());
                    } else if let SessionMsg::Flush(r) = msg {
                        let _ = r.send(0);
                    } else if let SessionMsg::Shutdown(r) = msg {
                        let _ = r.send(());
                        break;
                    }
                }
                return;
            }
        };
        let mut ctx = SessionCtx {
            session_id: self.session_id.clone(),
            config: None,
        };
        // Rebuild translator state before accepting the first new event. Keep
        // the deterministic replay ops buffered until live credentials arrive;
        // then re-emitting them repairs any rows lost by a prior crash. The
        // stable span ids ensure these target existing rows rather than create
        // duplicate spans.
        let mut replay_ops = Vec::new();
        for env in &self.replay {
            if let Some(cfg) = &env.config {
                ctx.config = Some(cfg.clone());
            }
            match translator.handle(env, &ctx) {
                Ok(mut ops) => replay_ops.append(&mut ops),
                Err(e) => self.set_error(format!("journal replay failed: {e}")),
            }
        }

        while let Some(msg) = rx.recv().await {
            match msg {
                SessionMsg::Event(env) => {
                    if let Some(cfg) = &env.config {
                        sink.configure(cfg);
                        ctx.config = Some(cfg.clone());
                        self.refresh_permalink(sink.as_ref());
                    }
                    if !replay_ops.is_empty() {
                        match sink.emit(&replay_ops).await {
                            Ok(n) => {
                                self.counters.spans_emitted.fetch_add(n, Ordering::Relaxed);
                                replay_ops.clear();
                            }
                            Err(e) => self.set_error(format!("sink replay emit failed: {e}")),
                        }
                    }
                    match translator.handle(&env, &ctx) {
                        Ok(ops) => match sink.emit(&ops).await {
                            Ok(n) => {
                                self.counters.spans_emitted.fetch_add(n, Ordering::Relaxed);
                            }
                            Err(e) => self.set_error(format!("sink emit failed: {e}")),
                        },
                        Err(e) => self.set_error(format!("translate failed: {e}")),
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

    async fn drain_flush(
        &self,
        translator: &mut Box<dyn crate::translate::AgentTranslator>,
        sink: &mut Box<dyn crate::sink::Sink>,
        ctx: &SessionCtx,
    ) {
        match translator.flush(ctx) {
            Ok(ops) => {
                if let Err(e) = sink.emit(&ops).await {
                    self.set_error(format!("sink emit (flush) failed: {e}"));
                }
            }
            Err(e) => self.set_error(format!("translate flush failed: {e}")),
        }
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
