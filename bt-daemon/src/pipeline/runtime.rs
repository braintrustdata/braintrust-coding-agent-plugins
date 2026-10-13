//! Session assembly: partitions, branches, registration, barriers, retention,
//! and the read-only status projection.
//!
//! Assembly wires components together; it never processes items, decides
//! retries, or discards pending work. Each partition runs one translation
//! machine, and each registered route runs one branch (route machine).

use super::events::EventStore;
use super::recovery::FailureRecord;
use super::store::{self, Store};
use super::types::{
    now_ms, ConsumerId, FinalizeKind, JournalId, NodeId, PartitionId, RouteConfig, RouteKey,
    RouteOrigin, TranslationControl, TranslationInput,
};
use crate::correlation::CorrelationRegistry;
use crate::server::AuthResolveReason;
use crate::sink::SinkFactory;
use crate::translate::Registry;
use crate::wire::{Envelope, SessionConfig, SessionRoute};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{watch, Notify};

/// Resolves live credentials for a branch. Credentials never leave memory.
#[async_trait::async_trait]
pub(crate) trait RouteAuth: Send + Sync {
    async fn resolve(
        &self,
        source: &str,
        route: &SessionRoute,
        reason: AuthResolveReason,
    ) -> anyhow::Result<ResolvedAuth>;
}

#[derive(Clone)]
pub(crate) struct ResolvedAuth {
    pub(crate) config: SessionConfig,
    pub(crate) expires_at_ms: Option<i64>,
}

impl ResolvedAuth {
    pub(crate) fn expiring(&self) -> bool {
        const REFRESH_WINDOW_MS: i64 = 60_000;
        self.expires_at_ms
            .is_some_and(|expires| expires <= now_ms().saturating_add(REFRESH_WINDOW_MS))
    }
}

pub(crate) struct PipelineOptions {
    pub(crate) data_dir: PathBuf,
    /// Where journals and the database live. Defaults to `<data dir>/pipeline`.
    pub(crate) state_dir: Option<PathBuf>,
    /// The embedding `bt` version: provenance and translator revision.
    pub(crate) version: String,
    pub(crate) translators: Arc<Registry>,
    pub(crate) sink_factory: Arc<dyn SinkFactory>,
    pub(crate) auth: Arc<dyn RouteAuth>,
    /// Daemon-wide correlation. `None` for isolated pipelines (library
    /// imports), which have no other sessions to correlate with.
    pub(crate) correlation: Option<Arc<CorrelationRegistry>>,
    /// Hold translation of each native event until assembly has registered
    /// its routes, so correlation observes it under the right route.
    pub(crate) assembly_gate: bool,
}

/// Persisted branch state. Not journal data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct BranchRecord {
    pub(crate) key: RouteKey,
    pub(crate) config: RouteConfig,
    pub(crate) phase: Phase,
    #[serde(default)]
    pub(crate) legacy_ledger_imported: bool,
    /// The last span node this branch has submitted or completed. A held
    /// deferred root keeps the head behind it, but nodes it has delivered are
    /// not pending work for retention.
    #[serde(default)]
    pub(crate) delivered_through: NodeId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Phase {
    Backfilling,
    Live,
}

/// What a backfill producer reads.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum BackfillSource {
    /// Re-translate events `1..=receipt` to regenerate span nodes `1..=tail`
    /// deleted before a late live route registered.
    Events { receipt: NodeId, tail: NodeId },
    /// Import or attach from an agent transcript.
    Transcript {
        path: PathBuf,
        source: crate::ImportSource,
        attach: bool,
    },
}

/// Persisted backfill producer state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct BackfillRecord {
    pub(crate) source: BackfillSource,
    /// Inputs already handed off into the backfill journal.
    pub(crate) consumed: u64,
    #[serde(default)]
    pub(crate) complete: bool,
}

/// The translation machine's observable state.
#[derive(Debug, Clone, Default)]
pub(crate) struct TranslationView {
    /// Events handed off through this node.
    pub(crate) receipt: NodeId,
    pub(crate) recovering: Option<FailureRecord>,
}

/// A branch's observable state.
#[derive(Debug, Clone, Default)]
pub(crate) struct BranchView {
    pub(crate) head: NodeId,
    pub(crate) recovering: Option<FailureRecord>,
    pub(crate) reached: HashSet<String>,
    pub(crate) spans_emitted: u64,
    pub(crate) permalink: Option<String>,
    pub(crate) held_root: bool,
    /// The last span node delivered or completed; a held root does not count
    /// as backlog.
    pub(crate) delivered_through: NodeId,
    pub(crate) backfilling: bool,
    /// Distinct span ids delivered, for import summaries.
    pub(crate) span_ids: usize,
    pub(crate) root_span_id: Option<String>,
}

pub(crate) struct BranchRuntime {
    pub(crate) key: RouteKey,
    pub(crate) config: watch::Sender<RouteConfig>,
    pub(crate) view: watch::Sender<BranchView>,
    /// Woken when a recovery condition may have changed.
    pub(crate) wake: Notify,
    /// Signals an attach backfill to finish following its transcript.
    pub(crate) finish_backfill: watch::Sender<bool>,
    /// Backfill journal append notifications.
    pub(crate) backfill_changed: watch::Sender<NodeId>,
    pub(crate) task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    pub(crate) producer: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

pub(crate) struct PartitionRuntime {
    pub(crate) partition: PartitionId,
    pub(crate) namespace: String,
    pub(crate) translation: watch::Sender<TranslationView>,
    pub(crate) translation_wake: Notify,
    pub(crate) translation_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    pub(crate) branches: Mutex<HashMap<String, Arc<BranchRuntime>>>,
    /// Span journal position of every barrier translation has handed off.
    pub(crate) barriers: watch::Sender<HashMap<String, NodeId>>,
    /// Span journal append notifications.
    pub(crate) spans_changed: watch::Sender<NodeId>,
    pub(crate) stop: watch::Sender<bool>,
    pub(crate) last_activity: Mutex<std::time::Instant>,
    /// When `partitions.last_activity_ms` was last written, so capture
    /// persists activity at most once per interval, not per event.
    pub(crate) persisted_activity_ms: std::sync::atomic::AtomicI64,
    /// The last native event whose routes assembly has registered.
    pub(crate) assembled: watch::Sender<NodeId>,
    /// Set when a live route registered after events were already handed off:
    /// translation rebuilds once so correlation observes them under it.
    pub(crate) reobserve: std::sync::atomic::AtomicBool,
    pub(crate) reobserve_wake: Notify,
    /// The last translated input was a retirement finalize: retiring again
    /// would only repeat the same closing output.
    pub(crate) retired: std::sync::atomic::AtomicBool,
}

impl PartitionRuntime {
    pub(crate) fn touch(&self) {
        *self
            .last_activity
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = std::time::Instant::now();
    }

    pub(crate) fn idle_for(&self) -> Duration {
        self.last_activity
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .elapsed()
    }

    pub(crate) fn branch_list(&self) -> Vec<Arc<BranchRuntime>> {
        self.branches
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .cloned()
            .collect()
    }

    /// Live routes and their current configuration, for correlation.
    pub(crate) fn live_routes(&self) -> Vec<(RouteKey, RouteConfig)> {
        self.branch_list()
            .into_iter()
            .filter(|branch| branch.key.origin == RouteOrigin::Live)
            .map(|branch| (branch.key.clone(), branch.config.borrow().clone()))
            .collect()
    }
}

pub(crate) struct Pipeline {
    pub(crate) data_dir: PathBuf,
    pub(crate) version: String,
    pub(crate) translators: Arc<Registry>,
    pub(crate) sink_factory: Arc<dyn SinkFactory>,
    pub(crate) auth: Arc<dyn RouteAuth>,
    pub(crate) correlation: Option<Arc<CorrelationRegistry>>,
    pub(crate) assembly_gate: bool,
    pub(crate) store: Arc<Store>,
    pub(crate) events: Arc<EventStore>,
    partitions: Mutex<HashMap<PartitionId, Arc<PartitionRuntime>>>,
    /// Serializes registration and partition creation per partition.
    registration: tokio::sync::Mutex<()>,
}

pub(crate) fn pipeline_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("pipeline")
}

impl Pipeline {
    pub(crate) fn open(options: PipelineOptions) -> anyhow::Result<Arc<Self>> {
        let dir = options
            .state_dir
            .clone()
            .unwrap_or_else(|| pipeline_dir(&options.data_dir));
        crate::paths::ensure_private_dir(&dir)?;
        let store = Store::open(&dir.join("pipeline.sqlite"))?;
        let events = EventStore::new(&dir);
        Ok(Arc::new(Self {
            data_dir: options.data_dir,
            version: options.version,
            translators: options.translators,
            sink_factory: options.sink_factory,
            auth: options.auth,
            correlation: options.correlation,
            assembly_gate: options.assembly_gate,
            store,
            events,
            partitions: Mutex::new(HashMap::new()),
            registration: tokio::sync::Mutex::new(()),
        }))
    }

    pub(crate) fn runtimes(&self) -> Vec<Arc<PartitionRuntime>> {
        self.partitions
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .cloned()
            .collect()
    }

    pub(crate) fn runtime(&self, partition: &PartitionId) -> Option<Arc<PartitionRuntime>> {
        self.partitions
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(partition)
            .cloned()
    }

    /// The translator namespace of a new partition. Sessions first recorded
    /// by a pre-source-qualification daemon keep their legacy namespace.
    async fn namespace_for(&self, partition: &PartitionId) -> String {
        if crate::journal::legacy_journal_has_session(
            &self.data_dir,
            &partition.source,
            &partition.session_id,
        )
        .await
        {
            partition.session_id.clone()
        } else {
            crate::ids::session_namespace(&partition.source, &partition.session_id)
        }
    }

    /// The running partition, loading it from storage and starting its
    /// machines if necessary.
    pub(crate) async fn partition(
        self: &Arc<Self>,
        partition: &PartitionId,
    ) -> anyhow::Result<Arc<PartitionRuntime>> {
        if let Some(runtime) = self.runtime(partition) {
            return Ok(runtime);
        }
        let _guard = self.registration.lock().await;
        if let Some(runtime) = self.runtime(partition) {
            return Ok(runtime);
        }
        let existing = {
            let partition = partition.clone();
            self.store
                .read(move |conn| store::partition(conn, &partition))
                .await?
        };
        let namespace = match existing {
            Some(row) => row.namespace,
            None => {
                let namespace = self.namespace_for(partition).await;
                let partition = partition.clone();
                let stored = namespace.clone();
                self.store
                    .write(move |tx| store::ensure_partition(tx, &partition, &stored, now_ms()))
                    .await?;
                namespace
            }
        };
        let receipt = {
            let jkey = JournalId::events(partition).key();
            self.store
                .read(move |conn| {
                    Ok(store::head(conn, &jkey, &ConsumerId::Translation.key())?
                        .unwrap_or(1)
                        .saturating_sub(1))
                })
                .await?
        };
        let spans_tail = {
            let jkey = JournalId::spans(partition).key();
            self.store
                .read(move |conn| store::journal_tail(conn, &jkey))
                .await?
        };
        let assembled = if self.assembly_gate {
            let jkey = JournalId::events(partition).key();
            self.store
                .read(move |conn| {
                    Ok(store::head(conn, &jkey, "assembly")?
                        .unwrap_or(1)
                        .saturating_sub(1))
                })
                .await?
        } else {
            NodeId::MAX
        };
        let runtime = Arc::new(PartitionRuntime {
            partition: partition.clone(),
            namespace,
            translation: watch::Sender::new(TranslationView {
                receipt,
                recovering: None,
            }),
            translation_wake: Notify::new(),
            translation_task: Mutex::new(None),
            branches: Mutex::new(HashMap::new()),
            barriers: watch::Sender::new(HashMap::new()),
            spans_changed: watch::Sender::new(spans_tail),
            stop: watch::Sender::new(false),
            last_activity: Mutex::new(std::time::Instant::now()),
            persisted_activity_ms: std::sync::atomic::AtomicI64::new(0),
            assembled: watch::Sender::new(assembled),
            reobserve: std::sync::atomic::AtomicBool::new(false),
            reobserve_wake: Notify::new(),
            retired: std::sync::atomic::AtomicBool::new(false),
        });
        self.partitions
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(partition.clone(), runtime.clone());
        // Resume every persisted branch, then translation.
        let records = {
            let pkey = partition.key();
            self.store
                .read(move |conn| store::branches(conn, &pkey))
                .await?
        };
        for (_, state) in records {
            match serde_json::from_str::<BranchRecord>(&state) {
                Ok(record) => {
                    self.start_branch(&runtime, record);
                }
                Err(error) => tracing::warn!(%partition, %error, "unreadable branch state"),
            }
        }
        self.start_translation(&runtime);
        Ok(runtime)
    }

    fn start_translation(self: &Arc<Self>, runtime: &Arc<PartitionRuntime>) {
        let mut task = runtime
            .translation_task
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if task.as_ref().is_some_and(|task| !task.is_finished()) {
            return;
        }
        *task = Some(tokio::spawn(super::translation::run(
            self.clone(),
            runtime.clone(),
        )));
    }

    fn start_branch(
        self: &Arc<Self>,
        runtime: &Arc<PartitionRuntime>,
        record: BranchRecord,
    ) -> Arc<BranchRuntime> {
        let mut branches = runtime
            .branches
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(branch) = branches.get(&record.key.id()) {
            return branch.clone();
        }
        let branch = Arc::new(BranchRuntime {
            key: record.key.clone(),
            config: watch::Sender::new(record.config.clone()),
            view: watch::Sender::new(BranchView {
                backfilling: record.phase == Phase::Backfilling,
                ..BranchView::default()
            }),
            wake: Notify::new(),
            finish_backfill: watch::Sender::new(false),
            backfill_changed: watch::Sender::new(0),
            task: Mutex::new(None),
            producer: Mutex::new(None),
        });
        branches.insert(record.key.id(), branch.clone());
        drop(branches);
        *branch
            .task
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(tokio::spawn(super::routing::run(
            self.clone(),
            runtime.clone(),
            branch.clone(),
        )));
        branch
    }

    /// Durably append a native observation. Returns its event node.
    pub(crate) async fn capture(
        self: &Arc<Self>,
        env: &Envelope,
    ) -> anyhow::Result<(Arc<PartitionRuntime>, NodeId)> {
        let partition = PartitionId::new(&env.source, &env.session_id);
        let runtime = self.partition(&partition).await?;
        runtime.touch();
        // Retention counts from the last modification, so activity on an old
        // session is recorded durably (throttled; the window is days).
        const ACTIVITY_PERSIST_INTERVAL_MS: i64 = 60_000;
        let now = now_ms();
        let persisted = runtime
            .persisted_activity_ms
            .load(std::sync::atomic::Ordering::Relaxed);
        if now - persisted >= ACTIVITY_PERSIST_INTERVAL_MS {
            let touched = partition.clone();
            self.store
                .write(move |tx| store::touch_partition(tx, &touched, now))
                .await?;
            runtime
                .persisted_activity_ms
                .store(now, std::sync::atomic::Ordering::Relaxed);
        }
        let node = self
            .events
            .append(
                &partition,
                TranslationInput::Native(Box::new(env.redacted())),
            )
            .await?;
        runtime.translation_wake.notify_one();
        Ok((runtime, node))
    }

    /// Append an ordered control and return its barrier id.
    pub(crate) async fn control(
        self: &Arc<Self>,
        partition: &PartitionId,
        finalize: Option<FinalizeKind>,
    ) -> anyhow::Result<String> {
        let runtime = self.partition(partition).await?;
        let barrier = super::types::new_barrier_id();
        let control = match finalize {
            Some(kind) => TranslationControl::Finalize {
                barrier: barrier.clone(),
                kind,
            },
            None => TranslationControl::Checkpoint {
                barrier: barrier.clone(),
            },
        };
        self.events
            .append(partition, TranslationInput::Control(control))
            .await?;
        runtime.translation_wake.notify_one();
        Ok(barrier)
    }

    /// Register a route on a partition (idempotent), updating its
    /// configuration version when its settings changed.
    pub(crate) async fn register_route(
        self: &Arc<Self>,
        partition: &PartitionId,
        route: &SessionRoute,
        origin: RouteOrigin,
        transcript: Option<BackfillSource>,
    ) -> anyhow::Result<Arc<BranchRuntime>> {
        let runtime = self.partition(partition).await?;
        let key = RouteKey::with_origin(route, origin.clone());
        let _guard = self.registration.lock().await;
        let existing = runtime
            .branches
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&key.id())
            .cloned();
        if let Some(branch) = existing {
            let current = branch.config.borrow().clone();
            let proposed = RouteConfig::from_route(route, current.version + 1);
            if !current.same_settings(&proposed) {
                let pkey = partition.key();
                let route_id = key.id();
                let record = BranchRecord {
                    key: key.clone(),
                    config: proposed.clone(),
                    phase: Phase::Live,
                    legacy_ledger_imported: true,
                    delivered_through: 0,
                };
                self.store
                    .write(move |tx| {
                        let mut stored = store::branch_state(tx, &pkey, &route_id)?
                            .and_then(|state| serde_json::from_str::<BranchRecord>(&state).ok())
                            .unwrap_or(record);
                        stored.config = proposed;
                        store::set_branch_state(
                            tx,
                            &pkey,
                            &route_id,
                            &serde_json::to_string(&stored)?,
                        )
                    })
                    .await?;
                branch
                    .config
                    .send_replace(RouteConfig::from_route(route, current.version + 1));
                branch.wake.notify_one();
            }
            return Ok(branch);
        }

        let spans = JournalId::spans(partition).key();
        let events = JournalId::events(partition).key();
        let consumer = ConsumerId::branch(&key).key();
        let pkey = partition.key();
        let route_id = key.id();
        let config = RouteConfig::from_route(route, 1);
        let record_key = key.clone();
        let backfill_jkey = JournalId::backfill(partition, &key).key();
        let record = self
            .store
            .write(move |tx| {
                if let Some(state) = store::branch_state(tx, &pkey, &route_id)? {
                    return Ok(serde_json::from_str::<BranchRecord>(&state)?);
                }
                let retained = store::retained_start(tx, &spans)?;
                let tail = store::journal_tail(tx, &spans)?;
                let receipt = store::head(tx, &events, &ConsumerId::Translation.key())?
                    .unwrap_or(1)
                    .saturating_sub(1);
                let backfill = match transcript {
                    Some(source) => Some(source),
                    // Earlier span nodes were deleted: regenerate them from
                    // the event journal rather than handing out partial history.
                    None if retained > 1 => Some(BackfillSource::Events { receipt, tail }),
                    None => None,
                };
                let head = match &backfill {
                    Some(BackfillSource::Events { tail, .. }) => tail + 1,
                    _ => retained,
                };
                store::register_head(tx, &spans, &consumer, head)?;
                let phase = if let Some(source) = backfill {
                    store::register_head(tx, &backfill_jkey, &consumer, 1)?;
                    store::set_backfill_state(
                        tx,
                        &pkey,
                        &route_id,
                        &serde_json::to_string(&BackfillRecord {
                            source,
                            consumed: 0,
                            complete: false,
                        })?,
                    )?;
                    Phase::Backfilling
                } else {
                    Phase::Live
                };
                let record = BranchRecord {
                    key: record_key,
                    config,
                    phase,
                    legacy_ledger_imported: false,
                    delivered_through: 0,
                };
                store::set_branch_state(tx, &pkey, &route_id, &serde_json::to_string(&record)?)?;
                Ok(record)
            })
            .await?;
        let branch = self.start_branch(&runtime, record);
        if origin == RouteOrigin::Live && runtime.translation.borrow().receipt > 0 {
            runtime
                .reobserve
                .store(true, std::sync::atomic::Ordering::SeqCst);
            runtime.reobserve_wake.notify_one();
        }
        Ok(branch)
    }

    /// Record that assembly registered the routes of native event `node`.
    pub(crate) fn mark_assembled(&self, partition: &PartitionId, node: NodeId) {
        if let Some(runtime) = self.runtime(partition) {
            runtime.assembled.send_if_modified(|current| {
                if node > *current {
                    *current = node;
                    true
                } else {
                    false
                }
            });
        }
    }

    /// Delete span nodes every branch has completed. A span journal with no
    /// branches deletes nothing.
    pub(crate) fn collect_spans(
        tx: &rusqlite::Transaction<'_>,
        partition: &PartitionId,
    ) -> anyhow::Result<()> {
        let jkey = JournalId::spans(partition).key();
        let heads = store::heads_for(tx, &jkey)?;
        if let Some(boundary) = heads.iter().map(|(_, head)| *head).min() {
            store::delete_span_entries_before(tx, &jkey, boundary)?;
        }
        Ok(())
    }

    /// Wait until every branch of `partition` has reached `barrier`, bounded
    /// by `timeout`. Returns `(reached, pending)` where `pending` counts span
    /// nodes and events not yet through the barrier.
    pub(crate) async fn wait_barrier(
        self: &Arc<Self>,
        runtime: &Arc<PartitionRuntime>,
        barrier: &str,
        timeout: Duration,
    ) -> (bool, u64) {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut barriers = runtime.barriers.subscribe();
        let position = loop {
            if let Some(position) = barriers.borrow_and_update().get(barrier).copied() {
                break Some(position);
            }
            if tokio::time::timeout_at(deadline, barriers.changed())
                .await
                .is_err()
            {
                break None;
            }
        };
        let Some(position) = position else {
            return (false, self.pending_work(runtime).await);
        };
        for branch in runtime.branch_list() {
            let mut view = branch.view.subscribe();
            loop {
                {
                    let current = view.borrow_and_update();
                    if current.reached.contains(barrier) || current.head > position {
                        break;
                    }
                }
                if tokio::time::timeout_at(deadline, view.changed())
                    .await
                    .is_err()
                {
                    return (false, self.pending_work(runtime).await);
                }
            }
        }
        (true, 0)
    }

    /// Wait until an import branch has delivered its whole backfill. A branch
    /// that is recovering from a failure only user action can fix is returned
    /// as an error; one that retries on its own is waited out.
    pub(crate) async fn wait_import(
        &self,
        branch: &Arc<BranchRuntime>,
    ) -> Result<BranchView, Box<FailureRecord>> {
        let mut view = branch.view.subscribe();
        loop {
            {
                let current = view.borrow_and_update();
                match &current.recovering {
                    Some(record) if !record.failure.automatic => {
                        return Err(Box::new(record.clone()));
                    }
                    None if !current.backfilling => return Ok(current.clone()),
                    _ => {}
                }
            }
            if view.changed().await.is_err() {
                return Err(Box::new(FailureRecord::record(
                    None,
                    super::translation::persistence_failure(
                        "import",
                        &anyhow::anyhow!("the import pipeline stopped"),
                    ),
                )));
            }
        }
    }

    /// Untranslated events plus undelivered span nodes across branches.
    pub(crate) async fn pending_work(&self, runtime: &PartitionRuntime) -> u64 {
        let events_tail = self.events.tail(&runtime.partition).await.unwrap_or(0);
        let receipt = runtime.translation.borrow().receipt;
        let spans_tail = *runtime.spans_changed.borrow();
        let mut pending = events_tail.saturating_sub(receipt);
        for branch in runtime.branch_list() {
            let view = branch.view.borrow();
            if view.head > 0 {
                pending += spans_tail.saturating_sub(view.delivered_through.max(view.head - 1));
            }
        }
        pending
    }

    /// Stop a partition's machines once its work is complete or blocked,
    /// keeping all durable state.
    pub(crate) async fn retire(self: &Arc<Self>, partition: &PartitionId) {
        let Some(runtime) = self.runtime(partition) else {
            return;
        };
        runtime.stop.send_replace(true);
        runtime.translation_wake.notify_waiters();
        let translation = runtime
            .translation_task
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        if let Some(task) = translation {
            let _ = task.await;
        }
        for branch in runtime.branch_list() {
            branch.wake.notify_waiters();
            let task = branch
                .task
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take();
            if let Some(task) = task {
                let _ = task.await;
            }
        }
        self.partitions
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(partition);
        self.events.release(partition);
    }
}
