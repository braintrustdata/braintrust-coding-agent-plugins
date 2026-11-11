//! The ingress queue between hook capture and session dispatch. Capture only
//! journals and enqueues; the worker routes events to session actors, using
//! the journals as overflow when the queue is full.

use super::*;
use std::collections::VecDeque;
/// Captures remain ordered and bounded. Evicted entries are
/// already durable; their source journal's high-water mark is replayed first.
#[derive(Default)]
pub(super) struct CaptureBuffer {
    pub(super) events: VecDeque<PendingEvent>,
    pub(super) evicted: HashMap<(String, String), EvictedRange>,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct EvictedRange {
    pub(super) from: u64,
    pub(super) through: u64,
}

impl CaptureBuffer {
    fn push(&mut self, event: PendingEvent) {
        if self.events.len() == INGRESS_QUEUE_CAPACITY {
            if let Some(oldest) = self.events.pop_front() {
                self.evicted
                    .entry((oldest.env.source.clone(), oldest.env.session_id.clone()))
                    .and_modify(|range| range.through = range.through.max(oldest.journal_through))
                    .or_insert(EvictedRange {
                        from: oldest.replay_through,
                        through: oldest.journal_through,
                    });
            }
        }
        self.events.push_back(event);
    }
}

pub(super) enum CaptureWork {
    Evicted(HashMap<(String, String), EvictedRange>),
    Event(Box<PendingEvent>),
    Finished,
}

impl Daemon {
    pub(super) fn next_capture_work(&self) -> CaptureWork {
        let mut buffer = self.capture_buffer.lock();
        if !buffer.evicted.is_empty() {
            return CaptureWork::Evicted(std::mem::take(&mut buffer.evicted));
        }
        if let Some(event) = buffer.events.pop_front() {
            return CaptureWork::Event(Box::new(event));
        }
        CaptureWork::Finished
    }
}

pub(super) async fn drain_capture_buffer(daemon: &Arc<Daemon>) -> anyhow::Result<()> {
    loop {
        match daemon.next_capture_work() {
            CaptureWork::Evicted(bounds) => replay_evicted(daemon, &bounds).await?,
            CaptureWork::Event(event) => {
                daemon
                    .source_input_for(&event.env.source, &event.env.session_id)
                    .push_event(
                        event.env.clone(),
                        crate::pipeline::input::SourcePosition {
                            before: event.replay_through,
                            through: event.journal_through,
                        },
                    );
                dispatch_ingress_event(daemon, *event).await;
            }
            CaptureWork::Finished => return Ok(()),
        }
    }
}

/// Read frozen evicted ranges in source order, merging by event timestamp as
/// startup recovery does. Rebuild decoder context before each range.
pub(super) async fn replay_evicted(
    daemon: &Arc<Daemon>,
    bounds: &HashMap<(String, String), EvictedRange>,
) -> anyhow::Result<()> {
    let mut ready = Vec::new();
    for ((source, session_id), range) in bounds {
        let mut reader = daemon
            .events_for(source, session_id)
            .await?
            .read(range.from, range.through);
        if let Some((timestamp, event)) =
            next_evicted_event(daemon, &mut reader, range.through).await?
        {
            ready.push((timestamp, event, reader, range.through));
        }
    }
    while let Some(index) = ready
        .iter()
        .enumerate()
        .min_by_key(|(_, item)| item.0)
        .map(|(index, _)| index)
    {
        let (_, event, mut reader, through) = ready.swap_remove(index);
        dispatch_ingress_event(daemon, event).await;
        if let Some((timestamp, event)) = next_evicted_event(daemon, &mut reader, through).await? {
            ready.push((timestamp, event, reader, through));
        }
    }
    Ok(())
}

async fn next_evicted_event(
    daemon: &Daemon,
    reader: &mut crate::collections::EventReader,
    through: u64,
) -> anyhow::Result<Option<(i64, PendingEvent)>> {
    while reader.position() < through {
        let before = reader.position();
        let entry = reader
            .next()
            .await?
            .ok_or_else(|| anyhow::anyhow!("replay range ends inside a record"))?;
        anyhow::ensure!(entry.id <= through, "invalid replay end boundary");
        if let EventRecord::Event(redacted) = entry.record {
            let mut env = journal::envelope_from_redacted(redacted);
            let Some(source) = daemon.translators.canonical_source(&env.source) else {
                continue;
            };
            env.source = source.to_owned();
            return Ok(Some((
                env.ts_ms,
                PendingEvent {
                    env,
                    replay_through: before,
                    journal_through: entry.id,
                    settings_through: None,
                },
            )));
        }
    }
    anyhow::ensure!(reader.position() == through, "invalid replay end boundary");
    Ok(None)
}

pub(super) enum IngressMsg {
    Wake,
    Barrier(oneshot::Sender<()>),
    Stop(oneshot::Sender<()>),
}

/// The hook path never waits for this queue. Once it fills, the append-only
/// journals become the overflow queue and the worker catches them up in place.
pub(super) const INGRESS_QUEUE_CAPACITY: usize = 64;

pub(super) fn spawn_ingress_worker(
    daemon: Arc<Daemon>,
    mut rx: mpsc::Receiver<IngressMsg>,
    startup: Option<oneshot::Receiver<()>>,
) {
    tokio::spawn(async move {
        if let Some(startup) = startup {
            if startup.await.is_err() {
                tracing::error!("startup recovery failed; stopping ingress");
                daemon.trigger_shutdown();
                return;
            }
        }
        while let Some(msg) = rx.recv().await {
            // The same queue drains before barriers and after ordinary wakes.
            // Capture never waits for its reader, actors, or destinations.
            if let Err(error) = drain_capture_buffer(&daemon).await {
                tracing::error!(%error, "capture recovery failed; stopping ingress");
                daemon.trigger_shutdown();
                return;
            }
            match msg {
                IngressMsg::Wake => {}
                IngressMsg::Barrier(reply) => {
                    daemon.settle_session_actors().await;
                    let _ = retry_pending_sessions(&daemon).await;
                    let _ = reply.send(());
                }
                IngressMsg::Stop(reply) => {
                    let _ = reply.send(());
                    break;
                }
            }
        }
    });
}

pub(super) async fn dispatch_ingress_event(daemon: &Arc<Daemon>, event: PendingEvent) {
    // Startup discovery rebuilds these observations before this worker is
    // released, so a historical event cannot overwrite a newer capture.
    daemon.remember_claude_transcripts(&event.env);

    let journal_through = event.journal_through;
    // A child may arrive immediately after a parent's tool hook. Catch prior
    // session actors up before resolving a new session, entirely in the daemon.
    // Only an undecided session resolves a parent. Antigravity marks every
    // invocation as a start, so skip the all-session barrier once decided.
    if is_session_start(&event.env.source, &event.env.event)
        && !daemon.correlation_decided(&automatic_link_key(&event.env))
    {
        daemon.settle_session_actors().await;
    }
    let source_env = event.env.clone();
    if let Err(error) = accept_event(daemon, event).await {
        tracing::warn!(%error, "journaled ingress event could not be dispatched");
    }
    // Authentication and route discovery do not prevent canonical persistence.
    match daemon.translation_for(&source_env).await {
        Ok(source) => {
            if let Err(error) = source.advance(journal_through).await {
                tracing::warn!(%error,"source processing could not be queued");
            }
        }
        Err(error) => {
            tracing::warn!(%error,"source journal could not be opened; captured events retained")
        }
    }
}

pub(super) fn spawn_pending_reconciler(daemon: Arc<Daemon>) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        loop {
            tokio::select! {
                _ = daemon.shutdown.notified() => return,
                _ = daemon.correlation_changed.notified() => {},
                _ = tick.tick() => {},
            }
            daemon.observe_claude_transcripts(None, false).await;
            if let Err(error) = retry_pending_sessions(&daemon).await {
                tracing::warn!(%error, "pending child-session reconciliation failed");
            }
        }
    });
}

pub(super) async fn accept_resolved_event(
    daemon: &Arc<Daemon>,
    event: PendingEvent,
    attempt: u32,
) -> Result<(), Box<crate::pipeline::recovery::Failure>> {
    use crate::pipeline::recovery::Failure;
    let PendingEvent {
        mut env,
        replay_through,
        journal_through,
        settings_through,
    } = event;
    // Every fresh Claude capture carries a bounded observation marker. Older
    // persisted ingress must never consume live bytes while rebuilding state.
    if env.source == "claude-code" && env.payload.get("_bt_claude_transcript_mirrors").is_none() {
        if let Some(payload) = env.payload.as_object_mut() {
            payload.insert("_bt_transcript_replay".to_string(), serde_json::json!(true));
        }
    }
    // Startup resumes an existing boundary rather than inventing a new
    // native/control observation that would require translator reconstruction.
    let schedule_flush = journal_through > replay_through
        && (crate::hook::should_flush_ingress_event(&env)
            || (env.source == "claude-code" && env.event == "TranscriptUpdate"));
    let source = env.source.clone();
    let event = env.event.clone();
    let session_id = env.session_id.clone();
    let managed_run_id = env.managed_run_id.clone();
    let route = env.route.clone();
    tracing::info!(source, event, session_id, "event received");
    let session_lock = daemon.dispatch_lock(&source, &session_id);
    let _session_guard = session_lock.lock().await;

    let result = async {
        let delivery_key = daemon
            .configure_event(&mut env)
            .await
            .map_err(|error| Failure::credentials(format!("session auth failed: {error}"), attempt).at(journal_through))?;
        daemon.reconfigure_held_routes(&env, settings_through.unwrap_or(journal_through)).await.map_err(|error| Failure::storage(&error, &daemon.version, attempt).at(journal_through))?;
        if crate::dispatch::is_tool_lifecycle_event(&env.event) {
            if let Err(error) = mark_active_parent_snapshot_dirty(
                &daemon.data_dir,
                &delivery_key.correlation_key(),
                &daemon.correlation,
            )
            .await
            {
                tracing::warn!(session_id = %env.session_id, %error, "active parent snapshot could not be marked dirty");
            }
        }
        let session = daemon
            .session_for(&mut env, &delivery_key, replay_through)
            .await
            .map_err(|error| Failure::storage(&error, &daemon.version, attempt).at(journal_through))?;
        daemon
            .correlation
            .observe_session(&delivery_key.correlation_key(), &env.source, env.capture.as_ref(),
                is_session_start(&env.source, &env.event));
        session
            .enqueue(env, journal_through)
            .await
            .map_err(|error| Failure::delay(crate::pipeline::recovery::Boundary::Translation, journal_through, format!("enqueue failed: {error}"), attempt))?;
        Ok(delivery_key)
    }
    .await;

    match &result {
        Ok(delivery_key) => {
            if let (Some(managed_run_id), Some(route)) = (managed_run_id, route) {
                daemon
                    .persist_managed_run_session(&managed_run_id, delivery_key, &route)
                    .await;
            }
            tracing::info!(source, event, session_id, "event accepted")
        }
        Err(error) => tracing::warn!(source, event, session_id, %error, "event rejected"),
    }
    if result.is_ok() && schedule_flush {
        // Deferred terminal events need the same flush as immediate events;
        // there may have been no session actor when ingress first accepted them.
        daemon
            .enqueue_source_session_flush(&source, &session_id, Duration::from_secs(10))
            .await;
    }
    result.map(|_| ()).map_err(Box::new)
}

pub(super) fn is_session_start(source: &str, event: &str) -> bool {
    matches!(event, "SessionStart" | "session_start" | "session.created")
        || (source == "cursor" && event == "sessionStart")
        // Antigravity has no dedicated session-start or resume hook. Its
        // PreInvocation is the earliest hook in each invocation and is the
        // only opportunity to refresh a resumed conversation's process.
        || (source == "antigravity" && event == "PreInvocation")
}

impl Daemon {
    pub(super) async fn capture_event(&self, mut env: Envelope) -> Result<bool, String> {
        let _capture_guard = self.capture_gate.read().await;
        if self.quiescing.load(Ordering::SeqCst) {
            return Ok(false);
        }
        env.source = self
            .translators
            .canonical_source(&env.source)
            .ok_or_else(|| format!("unsupported coding-agent source {:?}", env.source))?
            .to_string();
        self.touch();
        let lock = self.session_lock(&env.source, &env.session_id);
        let _guard = lock.lock().await;
        hydrate_transcript_reference(&self.data_dir, &mut env).await;
        self.journal_and_enqueue(env).await
    }

    /// Caller holds the source-session capture lock; frozen observations enter
    /// the same durable queue as hooks without reacquiring that lock.
    pub(super) async fn journal_and_enqueue(&self, env: Envelope) -> Result<bool, String> {
        let events = self
            .events_for(&env.source, &env.session_id)
            .await
            .map_err(|error| format!("capture storage failed: {error}"))?;
        let appended = self
            .append_event(&env, &events)
            .await
            .map_err(|error| format!("capture storage failed: {error}"))?;
        let (replay_through, journal_through) = (appended.before, appended.through);
        let event = PendingEvent {
            env,
            replay_through,
            journal_through,
            settings_through: None,
        };
        self.capture_buffer.lock().push(event);
        match self.ingress_tx.try_send(IngressMsg::Wake) {
            Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => {}
            Err(mpsc::error::TrySendError::Closed(_)) => {
                tracing::warn!("journaled event will be recovered after daemon restart");
            }
        }
        Ok(true)
    }

    pub(super) async fn ingress_barrier(&self) {
        let (tx, rx) = oneshot::channel();
        if self.ingress_tx.send(IngressMsg::Barrier(tx)).await.is_ok() {
            let _ = rx.await;
        }
    }

    pub(super) async fn settle_ingress(self: &Arc<Self>) {
        self.ingress_barrier().await;
        self.settle_session_actors().await;
        let _ = retry_pending_sessions(self).await;
    }

    pub(super) async fn settle_session_actors(&self) {
        let sessions: Vec<_> = self.sessions.lock().values().cloned().collect();
        for session in sessions {
            session.barrier().await;
        }
    }
}
