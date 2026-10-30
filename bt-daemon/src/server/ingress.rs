//! The ingress queue between hook capture and session dispatch. Capture only
//! journals and enqueues; the worker routes events to session actors, using
//! the journals as overflow when the queue is full.

use super::*;
use std::collections::VecDeque;

/// Live startup captures remain ordered and bounded. Evicted entries are
/// already durable; their source journal's high-water mark is replayed first.
pub(super) struct StartupBuffer {
    pub(super) events: VecDeque<PendingEvent>,
    pub(super) evicted: HashMap<PathBuf, EvictedRange>,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct EvictedRange {
    pub(super) from: u64,
    pub(super) through: u64,
}

impl StartupBuffer {
    pub(super) fn new() -> Self {
        Self {
            events: VecDeque::new(),
            evicted: HashMap::new(),
        }
    }

    fn push(&mut self, data_dir: &std::path::Path, event: PendingEvent) {
        if self.events.len() == INGRESS_QUEUE_CAPACITY {
            if let Some(oldest) = self.events.pop_front() {
                let path = journal::source_journal_path(
                    data_dir,
                    &oldest.env.source,
                    &oldest.env.session_id,
                );
                self.evicted
                    .entry(path)
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

pub(super) enum StartupWork {
    Evicted(HashMap<PathBuf, EvictedRange>),
    Event(Box<PendingEvent>),
    Finished,
}

impl Daemon {
    pub(super) fn next_startup_work(&self) -> StartupWork {
        let mut state = self.startup_buffer.lock();
        let Some(buffer) = state.as_mut() else {
            return StartupWork::Finished;
        };
        if !buffer.evicted.is_empty() {
            return StartupWork::Evicted(std::mem::take(&mut buffer.evicted));
        }
        if let Some(event) = buffer.events.pop_front() {
            return StartupWork::Event(Box::new(event));
        }
        *state = None;
        StartupWork::Finished
    }
}

pub(super) async fn drain_startup_buffer(daemon: &Arc<Daemon>) -> anyhow::Result<()> {
    let mut cursors = HashMap::new();
    loop {
        match daemon.next_startup_work() {
            StartupWork::Evicted(bounds) => {
                replay_evicted_startup(daemon, &mut cursors, &bounds).await?
            }
            StartupWork::Event(event) => dispatch_ingress_event(daemon, *event).await,
            StartupWork::Finished => return Ok(()),
        }
    }
}

pub(super) struct StartupCursor {
    reader: journal::JournalReader,
    position: u64,
}

/// Advance each evicted journal range from its previous position, retaining
/// the reader's Pi delta context between advances. Older records are decoded
/// only to initialize that context; they are not dispatched again.
pub(super) async fn replay_evicted_startup(
    daemon: &Arc<Daemon>,
    cursors: &mut HashMap<PathBuf, StartupCursor>,
    bounds: &HashMap<PathBuf, EvictedRange>,
) -> anyhow::Result<()> {
    for (path, range) in bounds {
        anyhow::ensure!(
            journal::JournalReader::recorded_len(path).await >= range.through,
            "evicted startup journal is truncated"
        );
        if !cursors.contains_key(path) {
            let reader = journal::JournalReader::open(path, range.from)
                .await?
                .ok_or_else(|| anyhow::anyhow!("evicted startup journal disappeared"))?;
            cursors.insert(
                path.clone(),
                StartupCursor {
                    reader,
                    position: 0,
                },
            );
        }
        let cursor = cursors
            .get_mut(path)
            .ok_or_else(|| anyhow::anyhow!("startup cursor missing"))?;
        anyhow::ensure!(
            cursor.position <= range.from,
            "evicted startup ranges overlap"
        );
        cursor.reader.extend(range.from);
        while cursor.position < range.from {
            let entry =
                cursor.reader.next_record().await?.ok_or_else(|| {
                    anyhow::anyhow!("evicted startup range begins inside a record")
                })?;
            cursor.position = entry.through;
        }
        anyhow::ensure!(
            cursor.position == range.from,
            "invalid evicted startup boundary"
        );
        cursor.reader.extend(range.through);
        while cursor.position < range.through {
            let before = cursor.position;
            let entry = cursor
                .reader
                .next_record()
                .await?
                .ok_or_else(|| anyhow::anyhow!("evicted startup range ends inside a record"))?;
            cursor.position = entry.through;
            if let journal::JournalRecord::Event(redacted) = entry.record {
                let mut env = journal::envelope_from_redacted(redacted);
                let Some(source) = daemon.translators.canonical_source(&env.source) else {
                    continue;
                };
                env.source = source.to_owned();
                dispatch_ingress_event(
                    daemon,
                    PendingEvent {
                        env,
                        replay_through: before,
                        journal_through: entry.through,
                        settings_through: None,
                    },
                )
                .await;
            }
        }
        anyhow::ensure!(
            cursor.position == range.through,
            "invalid evicted startup end"
        );
    }
    Ok(())
}

pub(super) enum IngressMsg {
    Event(Box<PendingEvent>),
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
        loop {
            if daemon.ingress_overflow.swap(false, Ordering::AcqRel) {
                recover_ingress_overflow(&daemon).await;
                continue;
            }
            let Some(msg) = rx.recv().await else {
                break;
            };
            match msg {
                IngressMsg::Stop(reply) => {
                    let _ = reply.send(());
                    break;
                }
                IngressMsg::Event(event) => {
                    dispatch_ingress_event(&daemon, *event).await;
                }
                IngressMsg::Barrier(reply) => {
                    loop {
                        daemon.settle_session_actors().await;
                        let _ = retry_pending_sessions(&daemon).await;
                        if !daemon.ingress_overflow.swap(false, Ordering::AcqRel) {
                            break;
                        }
                        recover_ingress_overflow(&daemon).await;
                    }
                    let _ = reply.send(());
                }
            }
        }
    });
}

pub(super) async fn dispatch_ingress_event(daemon: &Arc<Daemon>, event: PendingEvent) {
    let ingress_key =
        event.env.route.as_ref().and_then(|route| {
            DeliveryKey::new(&event.env.source, &event.env.session_id, route).ok()
        });
    if ingress_key.as_ref().is_some_and(|key| {
        daemon
            .ingress_dispatched
            .lock()
            .get(key)
            .is_some_and(|through| *through >= event.journal_through)
    }) {
        return;
    }

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
    match accept_event(daemon, event).await {
        Ok(()) => {
            if let Some(key) = ingress_key {
                let mut dispatched = daemon.ingress_dispatched.lock();
                let through = dispatched.entry(key).or_default();
                *through = (*through).max(journal_through);
            }
        }
        Err(error) => {
            tracing::warn!(%error, "journaled ingress event could not be dispatched");
        }
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

/// Drain events omitted from the bounded in-memory queue. The queue contains
/// only a latency fast path; journals remain the complete source of ingress.
pub(super) async fn recover_ingress_overflow(daemon: &Arc<Daemon>) {
    let Ok(mut entries) = tokio::fs::read_dir(journal::journal_dir(&daemon.data_dir)).await else {
        return;
    };
    let mut recovered = 0usize;
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("ndjson") {
            continue;
        }
        let recorded_len = journal::JournalReader::recorded_len(&path).await;
        let Ok(Some(mut checkpoints)) = journal::JournalReader::open(&path, recorded_len).await
        else {
            continue;
        };
        let mut acknowledged_by_route: HashMap<String, u64> = HashMap::new();
        while let Ok(Some(entry)) = checkpoints.next_record().await {
            if let journal::JournalRecord::DeliveryCheckpoint { route, through } = entry.record {
                let key = serde_json::to_string(&route).unwrap_or_default();
                let acknowledged = acknowledged_by_route.entry(key).or_default();
                *acknowledged = (*acknowledged).max(through);
            }
        }

        let Ok(Some(mut reader)) = journal::JournalReader::open(&path, recorded_len).await else {
            continue;
        };
        let mut before = 0u64;
        while let Ok(Some(entry)) = reader.next_record().await {
            let through = entry.through;
            let journal::JournalRecord::Event(redacted) = entry.record else {
                before = through;
                continue;
            };
            let mut env = journal::envelope_from_redacted(redacted);
            let Some(source) = daemon.translators.canonical_source(&env.source) else {
                before = through;
                continue;
            };
            env.source = source.to_string();
            let Some(route) = env.route.as_ref() else {
                before = through;
                continue;
            };
            let route_json = recovered_delivery_route(daemon, &env)
                .await
                .as_ref()
                .and_then(|route| serde_json::to_string(route).ok())
                .unwrap_or_else(|| serde_json::to_string(route).unwrap_or_default());
            let key = match DeliveryKey::new(&env.source, &env.session_id, route) {
                Ok(key) => key,
                Err(_) => {
                    before = through;
                    continue;
                }
            };
            let dispatched = daemon
                .ingress_dispatched
                .lock()
                .get(&key)
                .copied()
                .unwrap_or(0);
            let acknowledged = acknowledged_by_route.get(&route_json).copied().unwrap_or(0);
            if through > dispatched.max(acknowledged) {
                dispatch_ingress_event(
                    daemon,
                    PendingEvent {
                        env,
                        replay_through: before,
                        journal_through: through,
                        settings_through: None,
                    },
                )
                .await;
                recovered += 1;
            }
            before = through;
        }
    }
    if recovered > 0 {
        tracing::info!(recovered, "drained journal-backed ingress overflow");
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
    pub(super) async fn journal_and_enqueue(&self, mut env: Envelope) -> Result<bool, String> {
        let (replay_through, journal_through) = self
            .append_to_journal(&mut env)
            .await
            .map_err(|error| format!("journal failed: {error}"))?;
        let event = PendingEvent {
            env,
            replay_through,
            journal_through,
            settings_through: None,
        };
        let event = {
            let mut state = self.startup_buffer.lock();
            if let Some(buffer) = state.as_mut() {
                buffer.push(&self.data_dir, event);
                return Ok(true);
            }
            event
        };
        match self.ingress_tx.try_send(IngressMsg::Event(Box::new(event))) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.ingress_overflow.store(true, Ordering::Release);
                tracing::debug!("ingress queue full; journal will be drained by daemon worker");
            }
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
