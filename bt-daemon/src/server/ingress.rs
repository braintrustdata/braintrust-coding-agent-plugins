//! Capture and assembly.
//!
//! Capture durably appends a native observation to its partition's event
//! journal and acknowledges the hook; nothing downstream participates in the
//! acknowledgement. The assembly worker then processes captured events in
//! order: it settles correlation for new sessions and registers each event's
//! (effective) route as a delivery branch. Assembly keeps its own durable
//! cursor on the event journal, so a crash between capture and registration
//! resumes registration at startup.

use super::*;
use crate::pipeline::store;
use crate::pipeline::types::{ConsumerId, JournalId, RouteKey, RouteOrigin};

pub(super) enum AssemblyMsg {
    Event(Box<PendingEvent>),
    Barrier(oneshot::Sender<()>),
}

/// Assembly's durable progress on an event journal.
pub(super) fn assembly_consumer() -> String {
    "assembly".to_string()
}

pub(super) fn spawn_assembly_worker(
    daemon: Arc<Daemon>,
    mut rx: mpsc::UnboundedReceiver<AssemblyMsg>,
) {
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            match msg {
                AssemblyMsg::Event(event) => assemble_event(&daemon, *event).await,
                AssemblyMsg::Barrier(reply) => {
                    let _ = retry_pending_sessions(&daemon).await;
                    let _ = reply.send(());
                }
            }
        }
    });
}

pub(super) async fn assemble_event(daemon: &Arc<Daemon>, event: PendingEvent) {
    let partition = PartitionId::new(&event.env.source, &event.env.session_id);
    let node = event.journal_through;
    // A child may arrive immediately after a parent's tool hook. Before
    // resolving an undecided session, let every other session's translation
    // catch up to what was captured before it. Delivery is irrelevant.
    if is_session_start(&event.env.source, &event.env.event)
        && !daemon.correlation_decided(&automatic_link_key(&event.env))
    {
        daemon.settle_translation(&partition).await;
    }
    if let Err(error) = accept_event(daemon, event).await {
        tracing::warn!(%partition, %error, "captured event could not be assembled");
        daemon.pipeline.mark_assembled(&partition, node);
    }
    // Registration failures are reported through status; the event itself
    // remains durable and translated regardless.
    let jkey = JournalId::events(&partition).key();
    let consumer = assembly_consumer();
    if let Err(error) = daemon
        .pipeline
        .store
        .write(move |tx| store::advance_head(tx, &jkey, &consumer, node + 1))
        .await
    {
        tracing::warn!(%partition, %error, "assembly cursor not persisted");
    }
}

/// Register an event's effective route as a delivery branch.
pub(super) async fn accept_resolved_event(
    daemon: &Arc<Daemon>,
    event: PendingEvent,
) -> Result<(), String> {
    let partition = PartitionId::new(&event.env.source, &event.env.session_id);
    let node = event.journal_through;
    let result = register_event_route(daemon, event).await;
    // Translation of this event may proceed whether or not a route could be
    // registered; registration problems are reported through status.
    daemon.pipeline.mark_assembled(&partition, node);
    result
}

async fn register_event_route(daemon: &Arc<Daemon>, event: PendingEvent) -> Result<(), String> {
    let env = event.env;
    let route = env
        .route
        .clone()
        .ok_or_else(|| "event is missing its session route".to_string())?;
    let delivery_key = DeliveryKey::new(&env.source, &env.session_id, &route)
        .map_err(|error| error.to_string())?;
    if route.destination.is_none() {
        let message = "session route is missing its trace destination; select a project or destination during `bt trace setup` or `bt trace run`".to_string();
        daemon
            .route_errors
            .lock()
            .unwrap()
            .insert(delivery_key, (env.source.clone(), message.clone()));
        return Err(message);
    }
    let partition = PartitionId::new(&env.source, &env.session_id);
    let route_key = RouteKey::live(&route);
    let correlation_key = crate::pipeline::translation::correlation_key(&partition, &route_key);
    if crate::capture::is_tool_lifecycle_event(&env.event) {
        if let Err(error) = mark_active_parent_snapshot_dirty(
            &daemon.data_dir,
            &correlation_key,
            &daemon.correlation,
        )
        .await
        {
            tracing::warn!(session_id = %env.session_id, %error, "active parent snapshot could not be marked dirty");
        }
    }
    daemon.correlation.observe_session(
        &correlation_key,
        &env.source,
        env.capture.as_ref(),
        is_session_start(&env.source, &env.event),
    );
    daemon
        .pipeline
        .register_route(&partition, &route, RouteOrigin::Live, None)
        .await
        .map_err(|error| format!("route registration failed: {error:#}"))?;
    daemon.route_errors.lock().unwrap().remove(&delivery_key);
    if let Some(managed_run_id) = &env.managed_run_id {
        daemon
            .persist_managed_run_session(managed_run_id, &delivery_key, &route)
            .await;
    }
    tracing::debug!(source = %env.source, event = %env.event, session_id = %env.session_id, "route assembled");
    Ok(())
}

pub(super) fn is_session_start(source: &str, event: &str) -> bool {
    matches!(event, "SessionStart" | "session_start" | "session.created")
        || (source == "cursor" && event == "sessionStart")
        // Antigravity has no dedicated session-start or resume hook. Its
        // PreInvocation is the earliest hook in each invocation and is the
        // only opportunity to refresh a resumed conversation's process.
        || (source == "antigravity" && event == "PreInvocation")
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

/// Load every partition that holds pending work, and finish assembling any
/// event captured before a crash but never assembled.
pub(super) async fn resume_pending_partitions(daemon: &Arc<Daemon>) {
    let pipeline = &daemon.pipeline;
    let rows = match pipeline.store.read(store::partitions).await {
        Ok(rows) => rows,
        Err(error) => {
            tracing::warn!(%error, "partition index unreadable; nothing resumed");
            return;
        }
    };
    let known: Vec<PartitionId> = rows.iter().map(|row| row.partition.clone()).collect();
    let mut resumed = 0usize;
    // Event journals are the source of truth: one whose index entry is gone is
    // adopted and translated from its first event.
    let adopted = pipeline.events.unindexed_partitions(&known);
    let partitions = rows.into_iter().map(|row| row.partition).chain(adopted);
    for partition in partitions {
        let pending = match pipeline_pending(pipeline, &partition).await {
            Ok(pending) => pending,
            Err(error) => {
                tracing::warn!(%partition, %error, "partition state unreadable; resuming it");
                Pending {
                    work: true,
                    assembly_from: None,
                }
            }
        };
        if !pending.work && pending.assembly_from.is_none() {
            continue;
        }
        if let Err(error) = pipeline.partition(&partition).await {
            tracing::warn!(%partition, %error, "partition could not be resumed");
            continue;
        }
        resumed += 1;
        if let Some(from) = pending.assembly_from {
            reassemble(daemon, &partition, from).await;
        }
    }
    if resumed > 0 {
        tracing::info!(resumed, "resumed partitions with pending work");
    }
}

pub(super) struct Pending {
    pub(super) work: bool,
    /// First event never assembled, if any.
    pub(super) assembly_from: Option<u64>,
}

/// Whether a partition holds pending work: untranslated events, undelivered
/// spans, a failure, or a backfill.
pub(super) async fn pipeline_pending(
    pipeline: &Arc<Pipeline>,
    partition: &PartitionId,
) -> anyhow::Result<Pending> {
    let events_tail = pipeline.events.tail(partition).await?;
    let events = JournalId::events(partition).key();
    let spans = JournalId::spans(partition).key();
    let pkey = partition.key();
    let (work, assembly_head) = pipeline
        .store
        .read(move |conn| {
            let receipt = store::head(conn, &events, &ConsumerId::Translation.key())?
                .unwrap_or(1)
                .saturating_sub(1);
            let assembly = store::head(conn, &events, &assembly_consumer())?.unwrap_or(1);
            let spans_tail = store::journal_tail(conn, &spans)?;
            let mut work = receipt < events_tail;
            for (_, state) in store::branches(conn, &pkey)? {
                let record: crate::pipeline::runtime::BranchRecord = serde_json::from_str(&state)?;
                if record.phase != crate::pipeline::runtime::Phase::Live
                    || record.delivered_through < spans_tail
                {
                    let head = store::head(conn, &spans, &ConsumerId::branch(&record.key).key())?
                        .unwrap_or(1);
                    if head <= spans_tail || record.phase != crate::pipeline::runtime::Phase::Live {
                        work = true;
                    }
                }
            }
            work |= !store::backfills(conn, &pkey)?.is_empty();
            work |= store::failures(conn)?
                .iter()
                .any(|(jkey, _, _)| jkey.starts_with(&format!("{pkey}\u{1f}")));
            Ok((work, assembly))
        })
        .await?;
    let assembly_from = if assembly_head <= events_tail {
        // Controls follow events in the journal; only an unassembled native
        // event needs assembly.
        let mut cursor = pipeline.events.cursor(partition);
        let unassembled = tokio::task::spawn_blocking(move || {
            cursor.seek(assembly_head)?;
            while let Some((_, input)) = cursor.read_one()? {
                if matches!(input, crate::pipeline::types::TranslationInput::Native(_)) {
                    return anyhow::Ok(true);
                }
            }
            anyhow::Ok(false)
        })
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))??;
        unassembled.then_some(assembly_head)
    } else {
        None
    };
    Ok(Pending {
        work,
        assembly_from,
    })
}

/// Assemble events captured but never assembled before a restart.
async fn reassemble(daemon: &Arc<Daemon>, partition: &PartitionId, from: u64) {
    let mut cursor = daemon.pipeline.events.cursor(partition);
    let events = tokio::task::spawn_blocking(move || {
        cursor.seek(from)?;
        let mut events = Vec::new();
        while let Some((node, input)) = cursor.read_one()? {
            if let crate::pipeline::types::TranslationInput::Native(env) = input {
                events.push((node, *env));
            }
        }
        anyhow::Ok(events)
    })
    .await;
    let events = match events {
        Ok(Ok(events)) => events,
        Ok(Err(error)) => {
            tracing::warn!(%partition, %error, "unassembled events unreadable");
            return;
        }
        Err(error) => {
            tracing::warn!(%partition, %error, "unassembled event read failed");
            return;
        }
    };
    for (node, env) in events {
        let env = crate::journal::envelope_from_redacted(env);
        let _ = daemon
            .assembly_tx
            .send(AssemblyMsg::Event(Box::new(PendingEvent {
                env,
                replay_through: 0,
                journal_through: node,
            })));
    }
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
        if env.session_id.is_empty() {
            return Err("event has an empty session id".to_string());
        }
        self.touch();
        let lock = self.session_lock(&env.source, &env.session_id);
        let _guard = lock.lock().await;
        crate::capture::hydrate_transcript_reference(&self.data_dir, &mut env).await;
        self.journal_and_enqueue(env).await
    }

    /// Caller holds the source-session capture lock. Durably append, then
    /// hand the event to assembly without waiting for it.
    pub(super) async fn journal_and_enqueue(&self, env: Envelope) -> Result<bool, String> {
        let (_, node) = self
            .pipeline
            .capture(&env)
            .await
            .map_err(|error| format!("journal failed: {error:#}"))?;
        self.remember_claude_transcripts(&env);
        // A route's flush mode decides where checkpoints go. They follow the
        // event in journal order; nothing waits for them here.
        let checkpoint = crate::hook::should_flush_ingress_event(&env)
            || (env.source == "claude-code" && env.event == "TranscriptUpdate");
        if checkpoint {
            let partition = PartitionId::new(&env.source, &env.session_id);
            if let Err(error) = self.pipeline.control(&partition, None).await {
                tracing::warn!(%partition, %error, "turn-end checkpoint not captured");
            }
        }
        if self
            .assembly_tx
            .send(AssemblyMsg::Event(Box::new(PendingEvent {
                env,
                replay_through: 0,
                journal_through: node,
            })))
            .is_err()
        {
            tracing::warn!("captured event will be assembled after daemon restart");
        }
        Ok(true)
    }

    /// Wait until assembly has processed everything captured so far.
    pub(super) async fn settle_assembly(&self) {
        let (tx, rx) = oneshot::channel();
        if self.assembly_tx.send(AssemblyMsg::Barrier(tx)).is_ok() {
            let _ = rx.await;
        }
    }

    /// Wait, bounded by the correlation wait, until every other partition's
    /// translation has handed off everything captured so far. A recovering
    /// translation does not extend the wait.
    pub(super) async fn settle_translation(&self, except: &PartitionId) {
        let deadline = tokio::time::Instant::now() + CORRELATION_WAIT;
        for runtime in self.pipeline.runtimes() {
            if &runtime.partition == except {
                continue;
            }
            let Ok(tail) = self.pipeline.events.tail(&runtime.partition).await else {
                continue;
            };
            // Only events already assembled can have been translated: assembly
            // is sequential, so these are exactly those captured earlier.
            let tail = tail.min(*runtime.assembled.borrow());
            let mut view = runtime.translation.subscribe();
            loop {
                {
                    let current = view.borrow_and_update();
                    if current.receipt >= tail || current.recovering.is_some() {
                        break;
                    }
                }
                if tokio::time::timeout_at(deadline, view.changed())
                    .await
                    .is_err()
                {
                    return;
                }
            }
        }
    }
}
