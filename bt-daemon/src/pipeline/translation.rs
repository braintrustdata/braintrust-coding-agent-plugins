//! One ordered source consumer. Output publication is independent of route delivery.
use super::input::{SourceEvent, SourceInput, SourcePosition, SourceRecord};
use super::journal::SpanJournal;
use super::prepare::Prepare;
use super::roots::Roots;
use super::{
    recovery::{Boundary, Failure, Recovery, RecoveryOutcome},
    Stage,
};
use crate::{
    journal::{ControlKind, JournalReader, JournalRecord, JournalWriter, PipelineControl},
    translate::{AgentTranslator, OriginSnapshot, Registry, SessionCtx, SpanOp, WeakOrigins},
    wire::{SessionConfig, SessionRoute},
};
use parking_lot::Mutex;
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tokio::sync::{mpsc, oneshot, Notify};

#[derive(Clone)]
pub(crate) struct Observation {
    pub key: String,
    pub route: SessionRoute,
    pub config: SessionConfig,
}

enum Command {
    Observe(Box<Observation>),
    Advance(u64),
    Unobserve(String),
    Stop,
    Barrier(oneshot::Sender<Result<u64, String>>),
    Control(ControlKind, oneshot::Sender<Result<u64, String>>),
}

#[derive(Default)]
pub(crate) struct Progress {
    requested: AtomicU64,
    committed: AtomicU64,
}
impl Progress {
    pub fn pending(&self) -> u64 {
        u64::from(self.requested.load(Ordering::Relaxed) > self.committed.load(Ordering::Relaxed))
    }
}

#[derive(Default)]
struct RouteLifetimes {
    active: usize,
    draining: usize,
}

pub(crate) struct Translation {
    tx: mpsc::Sender<Command>,
    task: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    journal: SpanJournal,
    pub error: Arc<Mutex<Option<String>>>,
    pub recovery: Arc<Mutex<Option<crate::wire::RecoveryInfo>>>,
    routes: Mutex<RouteLifetimes>,
    pub progress: Arc<Progress>,
}

#[derive(Clone)]
pub(crate) struct TranslationOptions {
    pub source: String,
    pub session_id: String,
    pub namespace: String,
    pub version: String,
    pub data_dir: PathBuf,
    pub events: Arc<tokio::sync::Mutex<JournalWriter>>,
    pub event_path: PathBuf,
    pub input: SourceInput,
    pub registry: Arc<Registry>,
    pub correlation: Arc<crate::correlation::CorrelationRegistry>,
    pub correlation_changed: Arc<Notify>,
}

impl Translation {
    pub async fn spawn(options: TranslationOptions) -> anyhow::Result<Arc<Self>> {
        let actor = Producer::open(options).await?;
        let journal = actor.journal.clone();
        let progress = actor.progress.clone();
        let error = actor.error.clone();
        let recovery = actor.recovery.clone();
        let (tx, rx) = mpsc::channel(64);
        let task = tokio::spawn(actor.run(rx));
        Ok(Arc::new(Self {
            tx,
            task: tokio::sync::Mutex::new(Some(task)),
            journal,
            error,
            recovery,
            routes: Mutex::new(RouteLifetimes::default()),
            progress,
        }))
    }
    pub async fn shutdown(&self) {
        let _ = self.tx.send(Command::Stop).await;
        let mut task = self.task.lock().await;
        if let Some(task) = task.take() {
            let _ = task.await;
        }
        if let Err(error) = self.journal.close().await {
            tracing::warn!(%error, "SpanOp journal could not be closed during source shutdown");
        }
    }
    pub async fn observe(&self, observer: Observation) -> anyhow::Result<()> {
        self.routes.lock().active += 1;
        self.tx
            .send(Command::Observe(Box::new(observer)))
            .await
            .map_err(|_| anyhow::anyhow!("translation actor is gone"))
    }
    pub fn begin_route_shutdown(&self) -> bool {
        let mut routes = self.routes.lock();
        routes.active -= 1;
        routes.draining += 1;
        routes.active == 0
    }
    pub async fn finish_route_shutdown(&self) {
        let finished = {
            let mut routes = self.routes.lock();
            routes.draining -= 1;
            routes.active == 0 && routes.draining == 0
        };
        // The last route to finish owns closing storage. Merely starting
        // shutdown on every route does not mean their acknowledgements are saved.
        if finished {
            self.shutdown().await;
        }
    }
    pub async fn unobserve(&self, key: String) {
        let _ = self.tx.send(Command::Unobserve(key)).await;
    }
    pub async fn advance(&self, through: u64) -> anyhow::Result<()> {
        self.progress
            .requested
            .fetch_max(through, Ordering::Relaxed);
        self.tx
            .send(Command::Advance(through))
            .await
            .map_err(|_| anyhow::anyhow!("translation actor is gone"))
    }
    pub async fn barrier(&self) -> Result<u64, String> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Command::Barrier(tx))
            .await
            .map_err(|_| "translation actor is gone".to_owned())?;
        rx.await
            .map_err(|_| "translation barrier lost".to_owned())?
    }
    pub async fn control(&self, kind: ControlKind) -> Result<u64, String> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Command::Control(kind, tx))
            .await
            .map_err(|_| "translation actor is gone".to_owned())?;
        rx.await
            .map_err(|_| "translation control lost".to_owned())?
    }

    pub async fn reconfigure_held(
        &self,
        route: &SessionRoute,
        through: u64,
    ) -> anyhow::Result<Vec<(SessionRoute, SessionRoute)>> {
        self.journal.reconfigure_held(route, through).await
    }

    pub async fn route_settings(&self, route: &SessionRoute) -> anyhow::Result<SessionRoute> {
        self.journal.route_settings(route).await
    }

    /// Connect a route to the committed SpanOp stream without exposing its
    /// storage adapter to the route actor.
    pub async fn subscribe(
        &self,
        route: &SessionRoute,
        legacy_through: u64,
    ) -> anyhow::Result<super::journal::Cursor> {
        self.journal.subscribe(route, legacy_through).await
    }
}

enum State {
    Processing,
    Recovering(Failure),
}
struct Publication {
    input: u64,
    ordinal: u64,
    observation: bool,
    capture: Option<OriginSnapshot>,
    ops: Option<Vec<SpanOp>>,
    begun: bool,
    prepared: bool,
    complete: bool,
}

struct Producer {
    options: TranslationOptions,
    progress: Arc<Progress>,
    journal: SpanJournal,
    error: Arc<Mutex<Option<String>>>,
    recovery: Arc<Mutex<Option<crate::wire::RecoveryInfo>>>,
    translator: Option<Box<dyn AgentTranslator>>,
    reader: Option<JournalReader>,
    observers: Vec<Observation>,
    origins: WeakOrigins,
    roots: Roots,
    capture: Option<OriginSnapshot>,
    pending_snapshots: HashSet<String>,
    bounds: HashMap<String, u64>,
    position: u64,
    committed: u64,
    state: State,
    publication: Option<Publication>,
    phase: Boundary,
}

impl Producer {
    async fn open(options: TranslationOptions) -> anyhow::Result<Self> {
        let path = options.data_dir.join("pipeline").join(format!(
            "{}.sqlite",
            crate::ids::session_storage_id(&options.source, &options.session_id)
        ));
        let journal = SpanJournal::open(&path).await?;
        let committed = journal.translated_through().await?;
        let progress = Arc::new(Progress {
            requested: AtomicU64::new(committed),
            committed: AtomicU64::new(committed),
        });
        let recovery = Arc::new(Mutex::new(None));
        let error = Arc::new(Mutex::new(None));
        let state = match journal.source_failure().await? {
            Some(raw) => State::Recovering(serde_json::from_str(&raw).unwrap_or_else(|_| {
                Failure::revision(Boundary::Journal, committed + 1, raw, &options.version)
            })),
            None => State::Processing,
        };
        Ok(Self {
            options,
            progress,
            journal,
            error,
            recovery,
            translator: None,
            reader: None,
            observers: Vec::new(),
            origins: WeakOrigins::default(),
            roots: Roots::default(),
            capture: None,
            pending_snapshots: HashSet::new(),
            bounds: HashMap::new(),
            position: 0,
            committed,
            state,
            publication: None,
            phase: Boundary::Journal,
        })
    }

    /// Translators perform synchronous native transcript I/O and reduction.
    /// Keep both replay and live reduction off the runtime serving hook IPC.
    async fn translate<T: Send + 'static>(
        &mut self,
        operation: impl FnOnce(&mut dyn AgentTranslator, &SessionCtx) -> anyhow::Result<T>
            + Send
            + 'static,
    ) -> anyhow::Result<T> {
        let mut translator = self
            .translator
            .take()
            .ok_or_else(|| anyhow::anyhow!("translator is missing"))?;
        let ctx = SessionCtx {
            session_id: self.options.session_id.clone(),
            config: None,
        };
        let (translator, result) = tokio::task::spawn_blocking(move || {
            let result = operation(translator.as_mut(), &ctx);
            (translator, result)
        })
        .await?;
        self.translator = Some(translator);
        result
    }

    async fn run(mut self, mut rx: mpsc::Receiver<Command>) {
        let mut recorded = None;
        let mut record_retry = 0;
        let mut attempts = 0;
        loop {
            if let State::Recovering(mut failure) = self.state {
                *self.error.lock() = Some(failure.message());
                *self.recovery.lock() = Some(failure.info(false));
                let encoded = match serde_json::to_string(&failure) {
                    Ok(encoded) => encoded,
                    Err(error) => {
                        self.state = State::Recovering(Failure::revision(
                            Boundary::Journal,
                            failure.position,
                            format!(
                                "{}; recovery evidence cannot be encoded: {error}",
                                failure.message()
                            ),
                            &self.options.version,
                        ));
                        continue;
                    }
                };
                if recorded.as_ref() != Some(&encoded) && super::recovery::now_ms() >= record_retry
                {
                    match self.journal.set_source_failure(Some(encoded.clone())).await {
                        Ok(()) => recorded = Some(encoded.clone()),
                        Err(error) => {
                            record_retry = super::recovery::now_ms().saturating_add(1_000);
                            tracing::warn!(%error,"source recovery evidence could not be persisted; original failure retained");
                        }
                    }
                }
                self.state = State::Recovering(failure.clone());
                tokio::select! {
                    command=rx.recv()=>match command {
                        Some(Command::Stop)|None=>break,
                        Some(Command::Barrier(reply))=> { let _=reply.send(self.boundary().await); },
                        Some(Command::Observe(observer))=> { let _=self.observe_history(&observer).await; self.observers.push(*observer); },
                        Some(Command::Advance(_))=>{},
                        Some(Command::Unobserve(key))=>self.observers.retain(|observer| observer.key != key),
                        Some(Command::Control(kind,reply))=> {
                            let result=self.append_control(kind).await;
                            if let Err(error)=result { tracing::warn!(%error,"ordered control could not be persisted while source is recovering"); }
                            let _=reply.send(self.boundary().await);
                        },
                    },
                    _=tokio::time::sleep(std::time::Duration::from_millis(250))=>{},
                }
                if recorded.as_ref() != Some(&encoded)
                    || matches!(
                        (Recovery {
                            config: None,
                            version: &self.options.version
                        })
                        .process(&failure)
                        .await,
                        RecoveryOutcome::Waiting
                    )
                {
                    continue;
                }
                *self.recovery.lock() = Some(failure.info(true));
                attempts = failure.attempt.saturating_add(1);
                self.state = State::Processing;
                let result = async {
                    // Retry a failed correlation write before releasing the
                    // canonical candidate, even if registry observation is idempotent.
                    for key in self.pending_snapshots.clone() {
                        self.phase = Boundary::Journal;
                        crate::server::persist_active_parent_snapshot(
                            &self.options.data_dir,
                            &key,
                            &self.options.correlation,
                        )
                        .await
                        .map_err(anyhow::Error::msg)?;
                        self.pending_snapshots.remove(&key);
                    }
                    if self.publication.is_some() {
                        self.publish().await?;
                    } else if self.progress.requested.load(Ordering::Relaxed) > self.committed {
                        self.reader = None;
                        self.translator = None;
                        self.position = 0;
                    } else {
                        for observer in self.observers.clone() {
                            self.observe_history(&observer).await?;
                        }
                    }
                    self.advance(self.progress.requested.load(Ordering::Relaxed))
                        .await
                }
                .await;
                if let Err(error) = result {
                    failure = self.failure(error, attempts);
                    self.state = State::Recovering(failure);
                    continue;
                }
                if let Err(error) = self.journal.set_source_failure(None).await {
                    self.phase = Boundary::Journal;
                    self.state = State::Recovering(self.failure(error, attempts));
                    continue;
                }
                *self.error.lock() = None;
                *self.recovery.lock() = None;
                recorded = None;
                attempts = 0;
                continue;
            }
            let Some(command) = rx.recv().await else {
                break;
            };
            let result = match command {
                Command::Stop => break,
                Command::Observe(observer) => {
                    self.phase = Boundary::Journal;
                    let result = self.observe_history(&observer).await;
                    self.observers.push(*observer);
                    result
                }
                Command::Advance(through) => self.advance(through).await,
                Command::Unobserve(key) => {
                    self.observers.retain(|observer| observer.key != key);
                    Ok(())
                }
                Command::Barrier(reply) => {
                    let _ = reply.send(self.boundary().await);
                    Ok(())
                }
                Command::Control(kind, reply) => {
                    let result = self.control(kind).await;
                    let _ = reply.send(result.as_ref().copied().map_err(ToString::to_string));
                    result.map(|_| ())
                }
            };
            if let Err(error) = result {
                self.state = State::Recovering(self.failure(error, attempts));
            }
        }
    }

    fn failure(&self, error: anyhow::Error, attempt: u32) -> Failure {
        let position = self.committed.saturating_add(1);
        let mut failure = if matches!(self.phase, Boundary::Translation) {
            Failure::revision(
                self.phase,
                position,
                error.to_string(),
                &self.options.version,
            )
        } else {
            Failure::storage(&error, &self.options.version, attempt).at(position)
        };
        failure.through = self
            .publication
            .as_ref()
            .map(|publication| publication.input);
        failure
    }

    async fn boundary(&mut self) -> Result<u64, String> {
        if let State::Recovering(failure) = &self.state {
            return Err(failure.message());
        }
        self.journal.tail().await.map_err(|error| error.to_string())
    }

    async fn control(&mut self, kind: ControlKind) -> anyhow::Result<u64> {
        anyhow::ensure!(
            matches!(self.state, State::Processing),
            "translation is recovering"
        );
        self.initialize().await?;
        self.advance(self.progress.requested.load(Ordering::Relaxed))
            .await?;
        if let Some(through) = self.append_control(kind).await? {
            self.advance(through).await?;
        }
        self.journal.tail().await
    }
    async fn append_control(&mut self, kind: ControlKind) -> anyhow::Result<Option<u64>> {
        self.phase = Boundary::Journal;
        let mut bounds = HashMap::new();
        for path in self.bounds.keys() {
            let through = tokio::fs::metadata(path)
                .await
                .map(|m| m.len())
                .unwrap_or(0);
            bounds.insert(path.clone(), through);
        }
        let control = PipelineControl { kind, bounds };
        let through = {
            let mut writer = self.options.events.lock().await;
            // A checkpoint observes dispatched work. If capture is ahead,
            // flush the published tail and let queued events arrive normally;
            // chasing the file here can reorder parent/child processing.
            if writer.position() > self.position {
                return Ok(None);
            }
            let before = writer.position();
            let through = writer.append_control(&control).await?;
            let position = SourcePosition { before, through };
            self.options.input.push(SourceRecord {
                event: SourceEvent::Control(control),
                position,
            });
            through
        };
        self.progress
            .requested
            .fetch_max(through, Ordering::Relaxed);
        Ok(Some(through))
    }

    async fn initialize(&mut self) -> anyhow::Result<()> {
        if self.translator.is_some() {
            return Ok(());
        }
        self.committed = self.journal.translated_through().await?;
        self.roots = Roots::restore(self.journal.root_candidates().await?);
        for ops in self.roots.pending().values() {
            for op in ops {
                let (SpanOp::Insert(row) | SpanOp::Merge(row)) = op;
                if let Some(origin) = &row.origin {
                    self.origins.insert(row.span_id.clone(), origin);
                }
            }
        }
        let registry = self.options.registry.clone();
        let source = self.options.source.clone();
        let namespace = self.options.namespace.clone();
        self.translator = Some(
            tokio::task::spawn_blocking(move || {
                registry.create_checked_with_session_namespace(&source, &namespace)
            })
            .await??,
        );
        self.position = 0;
        self.reader = if self.committed > 0 {
            JournalReader::open(&self.options.event_path, self.committed).await?
        } else {
            None
        };
        Ok(())
    }

    async fn next_input(&mut self, through: u64) -> anyhow::Result<Option<SourceRecord>> {
        if let Some(record) = self.options.input.take(self.position, through) {
            // A future gap resumes from this logical position. The live path
            // does not open, seek, read, or pass payloads through a journal reader.
            self.reader = None;
            return Ok(Some(record));
        }
        if self.position >= through {
            return Ok(None);
        }
        if self.reader.is_none() {
            self.reader = Some(
                JournalReader::resume(&self.options.event_path, self.position, through).await?,
            );
        }
        let reader = self
            .reader
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("recovery reader is missing"))?;
        reader.extend(through);
        let Some(entry) = reader.next_record().await? else {
            return Ok(None);
        };
        let event = match entry.record {
            JournalRecord::Event(event) => {
                SourceEvent::Event(Box::new(crate::journal::envelope_from_redacted(event)))
            }
            JournalRecord::Control(control) => SourceEvent::Control(control),
            JournalRecord::DeliveryCheckpoint { .. } => SourceEvent::LegacyDeliveryCheckpoint,
        };
        Ok(Some(SourceRecord {
            event,
            position: SourcePosition {
                before: self.position,
                through: entry.through,
            },
        }))
    }

    async fn advance(&mut self, through: u64) -> anyhow::Result<()> {
        anyhow::ensure!(
            matches!(self.state, State::Processing),
            "translation is recovering"
        );
        if self.translator.is_none() && through <= self.committed {
            return Ok(());
        }
        self.phase = Boundary::Journal;
        self.initialize().await?;
        let through = through.max(self.committed);
        if self.position >= through {
            return Ok(());
        }
        self.phase = Boundary::Journal;
        while let Some(entry) = self.next_input(through).await? {
            let input = entry.position.through;
            let observation = !matches!(&entry.event,SourceEvent::Control(control) if matches!(control.kind,ControlKind::Retire));
            let (first, capture) = match entry.event {
                SourceEvent::Event(mut env) => {
                    if let Some(route) = &env.route {
                        self.journal.register(route).await?;
                    }
                    if crate::translate::canonical_source_name(&env.source) != self.options.source {
                        self.position = input;
                        continue;
                    }
                    if env.source == "claude-code" {
                        if let Some(payload) = env.payload.as_object_mut() {
                            payload.insert("_bt_transcript_replay".into(), true.into());
                        }
                    }
                    if let Some(path) = env
                        .payload
                        .pointer("/_bt_transcript_mirror/mirror")
                        .and_then(|v| v.as_str())
                        .or_else(|| env.payload.get("transcript_path").and_then(|v| v.as_str()))
                        .or_else(|| {
                            env.payload
                                .get("agent_transcript_path")
                                .and_then(|v| v.as_str())
                        })
                    {
                        self.bounds.entry(path.into()).or_insert(0);
                    }
                    let capture = OriginSnapshot {
                        plugin_version: env.plugin_version.clone(),
                        source_version: env.source_version.clone(),
                        bt_version: self.options.version.clone(),
                    };
                    self.capture = Some(capture.clone());
                    self.phase = Boundary::Translation;
                    (
                        self.translate(move |translator, ctx| translator.handle(&env, ctx))
                            .await?,
                        Some(capture),
                    )
                }
                SourceEvent::Control(control) => {
                    // Retirement exports defensive terminal updates but does not
                    // close native state when rebuilding for later captured work.
                    if input <= self.committed && matches!(control.kind, ControlKind::Retire) {
                        self.position = input;
                        continue;
                    }
                    self.phase = Boundary::Translation;
                    let ops = self
                        .translate(move |translator, ctx| {
                            if !matches!(control.kind, ControlKind::Checkpoint) {
                                translator.finalize_at(ctx, &control.bounds)
                            } else {
                                translator.checkpoint_at(ctx, &control.bounds)
                            }
                        })
                        .await?;
                    (ops, self.capture.clone())
                }
                SourceEvent::LegacyDeliveryCheckpoint => {
                    if input > self.committed {
                        self.journal.begin(input, observation).await?;
                        self.journal.commit(input).await?;
                        self.committed = input;
                        self.progress.committed.store(input, Ordering::Relaxed);
                    }
                    self.position = input;
                    continue;
                }
            };
            self.publication = Some(Publication {
                input,
                ordinal: 0,
                observation,
                capture,
                ops: Some(first),
                begun: false,
                prepared: false,
                complete: false,
            });
            self.publish().await?;
            self.phase = Boundary::Journal;
            self.position = input;
        }
        Ok(())
    }

    /// Each chunk stays owned until its local write succeeds. A commit retry
    /// cannot call translation or advance to the following event.
    async fn publish(&mut self) -> anyhow::Result<()> {
        let mut pending = self
            .publication
            .take()
            .ok_or_else(|| anyhow::anyhow!("pending publication is missing"))?;
        let result = async {
            let publish = pending.input > self.committed;
            if publish && !pending.begun {
                self.phase = Boundary::Journal;
                self.journal
                    .begin(pending.input, pending.observation)
                    .await?;
                pending.begun = true;
            }
            while !pending.complete {
                if let Some(ops) = pending.ops.as_mut() {
                    if !pending.prepared {
                        let missing = ops
                            .iter()
                            .filter_map(|op| {
                                let (SpanOp::Insert(row) | SpanOp::Merge(row)) = op;
                                self.origins
                                    .get(&row.span_id)
                                    .is_none()
                                    .then(|| row.span_id.clone())
                            })
                            .collect();
                        let mut saved = self.journal.origins_for(missing).await?;
                        for op in ops.iter_mut() {
                            let insert = matches!(op, SpanOp::Insert(_));
                            let (SpanOp::Insert(row) | SpanOp::Merge(row)) = op;
                            let origin = self
                                .origins
                                .get(&row.span_id)
                                .or_else(|| saved.remove(&row.span_id))
                                .or_else(|| {
                                    insert.then(|| {
                                        Arc::new(pending.capture.clone().unwrap_or_else(|| {
                                            OriginSnapshot {
                                                plugin_version: None,
                                                source_version: None,
                                                bt_version: self.options.version.clone(),
                                            }
                                        }))
                                    })
                                });
                            if let Some(origin) = &origin {
                                self.origins.insert(row.span_id.clone(), origin);
                            }
                            row.origin = origin;
                        }
                        self.phase = Boundary::Journal;
                        if pending.observation {
                            for observer in &self.observers {
                                Self::observe(
                                    &self.options,
                                    &mut self.pending_snapshots,
                                    ops,
                                    observer,
                                )
                                .await?;
                            }
                        }
                        if publish {
                            *ops = self.roots.process_owned(std::mem::take(ops));
                        }
                        pending.prepared = true;
                    }
                    if publish {
                        self.journal
                            .append_live(pending.input, &mut pending.ordinal, ops)
                            .await?;
                    }
                    pending.ops = None;
                }
                self.phase = Boundary::Translation;
                pending.ops = self
                    .translate(|translator, ctx| translator.drain_pending(ctx))
                    .await?;
                pending.complete = pending.ops.is_none();
                pending.prepared = false;
            }
            if publish {
                self.phase = Boundary::Journal;
                self.journal
                    .commit_with_roots(pending.input, self.roots.pending())
                    .await?;
                self.committed = pending.input;
                self.progress
                    .committed
                    .store(pending.input, Ordering::Relaxed);
            }
            self.position = pending.input;
            Ok(())
        }
        .await;
        if result.is_err() {
            self.publication = Some(pending);
        }
        result
    }

    async fn observe_history(&mut self, observer: &Observation) -> anyhow::Result<()> {
        // Candidates have no delivery position yet, but their birth records
        // identify the session for correlation. Defensive retirement updates
        // are not native observations and must not close a recreated observer.
        for ops in self.journal.root_candidates().await?.into_values() {
            let births = ops
                .into_iter()
                .filter(|op| matches!(op, SpanOp::Insert(_)))
                .collect::<Vec<_>>();
            Self::observe(
                &self.options,
                &mut self.pending_snapshots,
                &births,
                observer,
            )
            .await?;
        }
        let ops = self.journal.observations().await?;
        Self::observe(&self.options, &mut self.pending_snapshots, &ops, observer).await?;
        Ok(())
    }
    async fn observe(
        options: &TranslationOptions,
        pending_snapshots: &mut HashSet<String>,
        ops: &[SpanOp],
        observer: &Observation,
    ) -> anyhow::Result<()> {
        let mut ops = ops
            .iter()
            .map(super::journal::metadata_only)
            .collect::<Vec<_>>();
        Prepare::new(&options.source).apply(&mut ops, &observer.config);
        if options
            .correlation
            .observe_ops(&observer.key, &observer.route, &observer.config, &ops)
        {
            pending_snapshots.insert(observer.key.clone());
        }
        if pending_snapshots.contains(&observer.key) {
            crate::server::persist_active_parent_snapshot(
                &options.data_dir,
                &observer.key,
                &options.correlation,
            )
            .await
            .map_err(anyhow::Error::msg)?;
            pending_snapshots.remove(&observer.key);
            options.correlation_changed.notify_one();
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::{pipeline::delivery::InputPort, translate::SpanRow};
    mod streams {
        use crate as bt_daemon;
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/support/translation_streams.rs"
        ));
    }

    pub(crate) struct SourceFixture {
        _dir: tempfile::TempDir,
        pub(crate) options: TranslationOptions,
        pub(crate) source: Arc<Translation>,
    }
    impl SourceFixture {
        pub(crate) async fn new(agent: &str) -> Self {
            let dir = tempfile::tempdir().unwrap();
            let path = crate::journal::source_journal_path(dir.path(), agent, "checkpoint");
            let options = TranslationOptions {
                source: agent.into(),
                session_id: "checkpoint".into(),
                namespace: crate::ids::session_namespace(agent, "checkpoint"),
                version: "test".into(),
                data_dir: dir.path().into(),
                events: Arc::new(tokio::sync::Mutex::new(
                    JournalWriter::open_path(&path).await.unwrap(),
                )),
                event_path: path,
                input: SourceInput::default(),
                registry: Arc::new(Registry::default_agents()),
                correlation: Arc::default(),
                correlation_changed: Arc::default(),
            };
            let source = Translation::spawn(options.clone()).await.unwrap();
            Self {
                _dir: dir,
                options,
                source,
            }
        }
        pub(crate) async fn write(&self, event: &crate::wire::Envelope, live: bool) -> u64 {
            let mut writer = self.options.events.lock().await;
            let before = writer.position();
            let through = writer.append(event).await.unwrap();
            let position = SourcePosition { before, through };
            if live {
                self.options.input.push_event(event.clone(), position);
            }
            through
        }
        pub(crate) async fn process(&self, through: u64) {
            self.source.advance(through).await.unwrap();
            self.source.barrier().await.unwrap();
            assert_eq!(
                self.source.journal.translated_through().await.unwrap(),
                through
            );
        }
        async fn reopen(&mut self) {
            self.source.shutdown().await;
            *self.options.events.lock().await = JournalWriter::open_path(&self.options.event_path)
                .await
                .unwrap();
            self.options.input = SourceInput::default();
            self.source = Translation::spawn(self.options.clone()).await.unwrap();
        }
        fn db(&self) -> rusqlite::Connection {
            rusqlite::Connection::open(self.options.data_dir.join("pipeline").join(format!(
                "{}.sqlite",
                crate::ids::session_storage_id(&self.options.source, &self.options.session_id)
            )))
            .unwrap()
        }
        pub(crate) async fn poison(&self, range: std::ops::Range<u64>) {
            let mut bytes = tokio::fs::read(&self.options.event_path).await.unwrap();
            for byte in &mut bytes[range.start as usize..range.end as usize] {
                if *byte != b'\n' {
                    *byte = b'x';
                }
            }
            tokio::fs::write(&self.options.event_path, bytes)
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn every_agent_recovers_equivalent_spans_at_every_native_boundary() {
        let mut covered = Vec::new();
        for (agent, mut events, _files) in streams::native_streams() {
            covered.push(agent.clone());
            events.insert(0, events[0].clone());
            for event in &mut events {
                event.payload["padding"] = serde_json::json!("é📚history".repeat(1024));
            }
            let mut cases = vec![("journal", 0), ("live", 0), ("overflow", 2)];
            cases.extend((1..events.len()).map(|split| ("gap", split)));
            cases.extend((1..=events.len()).map(|split| ("cold", split)));
            let mut expected = None;
            for (mode, split) in cases {
                let mut f = SourceFixture::new(&agent).await;
                let mut positions = vec![0];
                for (index, event) in events.iter().enumerate() {
                    let live =
                        mode == "live" || mode == "overflow" || (mode == "gap" && index != split);
                    positions.push(f.write(event, live).await);
                }
                let tail = *positions.last().unwrap();
                match mode {
                    "live" => {
                        f.process(positions[1]).await;
                        f.source.control(ControlKind::Checkpoint).await.unwrap();
                        assert_eq!(
                            f.source.journal.translated_through().await.unwrap(),
                            positions[1]
                        );
                        assert_eq!(f.options.events.lock().await.position(), tail);
                        f.poison(0..tail).await;
                    }
                    "overflow" => {
                        assert!(f.options.input.take(0, positions[1]).is_some());
                        assert!(f.options.input.take(positions[1], positions[2]).is_some());
                        f.poison(positions[2]..tail).await;
                    }
                    "gap" => {
                        // Rebuild decoder context after live input, then return
                        // to live input without rereading its retained tail.
                        f.poison(positions[split + 1]..tail).await;
                    }
                    _ => {
                        f.process(positions[split]).await;
                        f.source.shutdown().await;
                        f.reopen().await;
                    }
                }
                f.process(tail).await;
                f.source.control(ControlKind::Finalize).await.unwrap();
                assert!(f.source.journal.translated_through().await.unwrap() > tail);
                // StoredOp already includes the private correlation fields;
                // compare its complete encoding rather than duplicate its codec.
                let db = f.db();
                let mut query = db
                    .prepare("SELECT value FROM ops ORDER BY position")
                    .unwrap();
                let rows: Vec<Vec<u8>> = query
                    .query_map([], |row| row.get(0))
                    .unwrap()
                    .map(Result::unwrap)
                    .collect();
                assert!(!rows.is_empty(), "{agent} {mode} produced no observations");
                if let Some(expected) = &expected {
                    assert_eq!(&rows, expected, "{agent} {mode} split {split}");
                } else {
                    expected = Some(rows);
                }
                f.source.shutdown().await;
            }
        }
        covered.sort();
        assert_eq!(covered, Registry::default_agents().sources());
    }

    struct BlockedTranslator {
        entered: Arc<Notify>,
        release: std::sync::mpsc::Receiver<()>,
    }
    impl BlockedTranslator {
        fn block(&self) {
            self.entered.notify_one();
            self.release
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
        }
    }
    impl AgentTranslator for BlockedTranslator {
        fn handle(
            &mut self,
            _: &crate::wire::Envelope,
            _: &SessionCtx,
        ) -> anyhow::Result<Vec<SpanOp>> {
            self.block();
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn synchronous_replay_does_not_block_capture_or_chase_new_events() {
        let f = SourceFixture::new("debug").await;
        let event = streams::native_streams().pop().unwrap().1.remove(0);
        let through = f.write(&event, false).await;
        let mut producer = Producer::open(f.options.clone()).await.unwrap();
        let entered = Arc::new(Notify::new());
        let (release, receiver) = std::sync::mpsc::channel();
        producer.translator = Some(Box::new(BlockedTranslator {
            entered: entered.clone(),
            release: receiver,
        }));
        let started = std::time::Instant::now();
        let work = tokio::spawn(async move {
            let result = producer.advance(through).await;
            (producer, result)
        });
        entered.notified().await;
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), f.write(&event, true))
                .await
                .unwrap()
                > through
        );
        assert!(
            !work.is_finished(),
            "capture must complete before replay is released"
        );
        release.send(()).unwrap();
        let (producer, result) = work.await.unwrap();
        result.unwrap();
        assert_eq!(
            producer.journal.translated_through().await.unwrap(),
            through
        );
        f.source.shutdown().await;
    }

    #[tokio::test]
    async fn shared_storage_stays_open_until_the_last_route_finishes_draining() {
        let f = SourceFixture::new("debug").await;
        let translation = &f.source;
        translation.routes.lock().active = 2;
        assert!(!translation.begin_route_shutdown());
        assert!(translation.begin_route_shutdown());
        translation.finish_route_shutdown().await;
        assert_eq!(translation.journal.tail().await.unwrap(), 0);
        translation.finish_route_shutdown().await;
        assert!(translation.journal.tail().await.is_err());
    }

    #[tokio::test]
    async fn recreated_observer_restores_a_staged_session_root() {
        let f = SourceFixture::new("debug").await;
        let translation = &f.source;
        let correlation = &f.options.correlation;
        let mut roots = Roots::default();
        roots
            .process(&vec![
                SpanOp::Insert(SpanRow {
                    span_id: "root".into(),
                    root_span_id: "root".into(),
                    metadata: Some(serde_json::json!({"_bt_defer_root":true})),
                    ..Default::default()
                }),
                SpanOp::Merge(SpanRow {
                    span_id: "root".into(),
                    end_ms: Some(1),
                    ..Default::default()
                }),
            ])
            .await;
        translation.journal.begin(10, false).await.unwrap();
        translation
            .journal
            .commit_with_roots(10, roots.pending())
            .await
            .unwrap();
        let route = SessionRoute::default();
        let observer = Observation {
            key: "observer".into(),
            config: route.with_auth(crate::wire::BackendAuth {
                token: "test".into(),
                api_url: None,
                app_url: None,
                org_name: Some("org".into()),
                org_id: None,
            }),
            route,
        };
        for _ in 0..2 {
            translation.observe(observer.clone()).await.unwrap();
            translation.barrier().await.unwrap();
            // A retired producer's defensive terminal update must not close
            // the newly restored session fallback.
            assert!(
                correlation.active_parent_snapshot(&observer.key).is_some(),
                "staged root must restore the session fallback"
            );
            translation.unobserve(observer.key.clone()).await;
            correlation.remove_session(&observer.key);
        }
        translation.shutdown().await;
    }

    #[tokio::test]
    async fn routing_resume_does_not_read_or_translate_the_committed_event_prefix() {
        let tmp = tempfile::tempdir().unwrap();
        let source = "debug";
        let session = "stored";
        let event_path = crate::journal::source_journal_path(tmp.path(), source, session);
        std::fs::create_dir_all(event_path.parent().unwrap()).unwrap();
        // Any translation replay of this file would fail immediately.
        std::fs::write(&event_path, b"not JSON\n").unwrap();
        let input = std::fs::metadata(&event_path).unwrap().len();
        let journal = SpanJournal::open(&tmp.path().join("pipeline").join(format!(
            "{}.sqlite",
            crate::ids::session_storage_id(source, session)
        )))
        .await
        .unwrap();
        journal.begin(input, true).await.unwrap();
        journal
            .append(
                input,
                &[SpanOp::Insert(SpanRow {
                    span_id: "durable".into(),
                    root_span_id: "durable".into(),
                    name: "already translated".into(),
                    ..SpanRow::default()
                })],
            )
            .await
            .unwrap();
        let tail = journal.commit(input).await.unwrap();
        let translation = Translation::spawn(TranslationOptions {
            source: source.into(),
            session_id: session.into(),
            namespace: crate::ids::session_namespace(source, session),
            version: "test".into(),
            data_dir: tmp.path().into(),
            events: Arc::new(tokio::sync::Mutex::new(
                JournalWriter::open_path(&event_path).await.unwrap(),
            )),
            event_path,
            input: SourceInput::default(),
            registry: Arc::new(Registry::default_agents()),
            correlation: Arc::new(crate::correlation::CorrelationRegistry::default()),
            correlation_changed: Arc::new(Notify::new()),
        })
        .await
        .unwrap();
        translation.advance(input).await.unwrap();
        assert_eq!(translation.barrier().await.unwrap(), tail);
        assert_eq!(translation.progress.pending(), 0);
        assert!(translation.error.lock().is_none());
        let mut route = translation
            .journal
            .subscribe(&SessionRoute::default(), 0)
            .await
            .unwrap();
        let delivery = InputPort::receive(&mut route).await.unwrap().unwrap();
        let (SpanOp::Insert(row) | SpanOp::Merge(row)) = &delivery.value()[0];
        assert_eq!(row.name, "already translated");
        delivery.into_completed().await.ok().unwrap();
        route.rewind().await.unwrap();
        assert_eq!(route.completed_position(), tail);
    }
    struct CountedTranslation(Arc<AtomicU64>);
    impl AgentTranslator for CountedTranslation {
        fn handle(
            &mut self,
            _: &crate::wire::Envelope,
            _: &SessionCtx,
        ) -> anyhow::Result<Vec<SpanOp>> {
            panic!("publication retry must not translate the event again")
        }
        fn drain_pending(&mut self, _: &SessionCtx) -> anyhow::Result<Option<Vec<SpanOp>>> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(None)
        }
    }
    #[tokio::test]
    async fn failed_append_and_commit_retain_the_built_chunk() {
        let f = SourceFixture::new("debug").await;
        f.source.shutdown().await;
        let mut producer = Producer::open(f.options.clone()).await.unwrap();
        let journal = producer.journal.clone();
        let path = f.options.data_dir.as_path().join("pipeline").join(format!(
            "{}.sqlite",
            crate::ids::session_storage_id("debug", "checkpoint")
        ));
        let db = rusqlite::Connection::open(path).unwrap();
        let drains = Arc::new(AtomicU64::new(0));
        producer.translator = Some(Box::new(CountedTranslation(drains.clone())));
        producer.publication = Some(Publication {
            input: 10,
            ordinal: 0,
            observation: true,
            capture: None,
            ops: Some(vec![
                SpanOp::Insert(SpanRow {
                    span_id: "unused-root".into(),
                    metadata: Some(serde_json::json!({"_bt_defer_root": true})),
                    ..Default::default()
                }),
                SpanOp::Insert(SpanRow {
                    span_id: "built".into(),
                    ..Default::default()
                }),
            ]),
            begun: false,
            prepared: false,
            complete: false,
        });
        db.execute_batch("CREATE TRIGGER fail_append BEFORE INSERT ON ops BEGIN SELECT RAISE(FAIL,'append'); END;").unwrap();
        assert!(producer.publish().await.is_err());
        assert_eq!(drains.load(Ordering::Relaxed), 0);
        assert_eq!(journal.translated_through().await.unwrap(), 0);
        db.execute_batch("DROP TRIGGER fail_append; CREATE TRIGGER fail_commit BEFORE UPDATE OF committed ON batches BEGIN SELECT RAISE(FAIL,'commit'); END;").unwrap();
        assert!(producer.publish().await.is_err());
        assert_eq!(drains.load(Ordering::Relaxed), 1);
        assert_eq!(journal.tail().await.unwrap(), 1);
        db.execute_batch("DROP TRIGGER fail_commit").unwrap();
        producer.publish().await.unwrap();
        assert_eq!(drains.load(Ordering::Relaxed), 1);
        assert_eq!(journal.translated_through().await.unwrap(), 10);
        assert_eq!(journal.tail().await.unwrap(), 1);
        let roots = journal.root_candidates().await.unwrap();
        assert_eq!(
            roots["unused-root"].len(),
            1,
            "a local retry must not stage the root twice"
        );
    }
}
