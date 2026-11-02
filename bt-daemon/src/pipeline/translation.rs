//! One ordered source consumer. Output publication is independent of route delivery.
use super::journal::{SpanJournal, TranslationCheckpoint};
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

const CHECKPOINT_EVENTS: u64 = 64;
const CHECKPOINT_BYTES: u64 = 1024 * 1024;

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct RecoveryContext {
    format: u32,
    translator: Box<serde_json::value::RawValue>,
    pub decoder: Vec<serde_json::Value>,
    capture: Option<OriginSnapshot>,
    bounds: HashMap<String, u64>,
    pub templates: HashMap<String, crate::wire::RedactedEnvelope>,
}

impl RecoveryContext {
    pub fn decode(
        checkpoint: &TranslationCheckpoint,
        version: &str,
        through: u64,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            checkpoint.version == version && checkpoint.input <= through,
            "translation checkpoint is incompatible with the current head"
        );
        let decoder = zstd::stream::read::Decoder::new(checkpoint.state.as_slice())?;
        let context: Self = serde_json::from_reader(std::io::BufReader::new(decoder))?;
        anyhow::ensure!(
            context.format == 1,
            "unsupported translation context format"
        );
        Ok(context)
    }
}

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

pub(crate) struct TranslationOptions {
    pub source: String,
    pub session_id: String,
    pub namespace: String,
    pub version: String,
    pub data_dir: PathBuf,
    pub events: Arc<tokio::sync::Mutex<JournalWriter>>,
    pub event_path: PathBuf,
    pub registry: Arc<Registry>,
    pub correlation: Arc<crate::correlation::CorrelationRegistry>,
    pub correlation_changed: Arc<Notify>,
}

impl Translation {
    pub async fn spawn(options: TranslationOptions) -> anyhow::Result<Arc<Self>> {
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
        let (tx, rx) = mpsc::channel(64);
        let state = match journal.source_failure().await? {
            Some(raw) => State::Recovering(serde_json::from_str(&raw).unwrap_or_else(|_| {
                Failure::revision(Boundary::Journal, committed + 1, raw, &options.version)
            })),
            None => State::Processing,
        };
        let actor = Producer {
            options,
            progress: progress.clone(),
            journal: journal.clone(),
            error: error.clone(),
            recovery: recovery.clone(),
            translator: None,
            reader: None,
            observers: Vec::new(),
            origins: WeakOrigins::default(),
            roots: Roots::default(),
            capture: None,
            pending_snapshots: HashSet::new(),
            bounds: HashMap::new(),
            templates: HashMap::new(),
            position: 0,
            committed,
            checkpoint_position: 0,
            checkpoint_events: 0,
            checkpoint_safe: true,
            state,
            publication: None,
            phase: Boundary::Journal,
        };
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
    templates: HashMap<String, crate::wire::RedactedEnvelope>,
    position: u64,
    committed: u64,
    checkpoint_position: u64,
    checkpoint_events: u64,
    checkpoint_safe: bool,
    state: State,
    publication: Option<Publication>,
    phase: Boundary,
}

impl Producer {
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
        if self.publication.is_none() && self.position == self.committed {
            if let Err(error) = self.persist_current_context().await {
                tracing::warn!(%error, "translation checkpoint could not be saved at shutdown");
            }
        }
    }

    async fn checkpoint_context(
        &mut self,
        input: u64,
    ) -> anyhow::Result<Option<TranslationCheckpoint>> {
        if !self.checkpoint_safe || self.translator.is_none() || self.reader.is_none() {
            return Ok(None);
        }
        let translator = self
            .translator
            .take()
            .ok_or_else(|| anyhow::anyhow!("translator is missing"))?;
        let reader = self
            .reader
            .take()
            .ok_or_else(|| anyhow::anyhow!("source reader is missing"))?;
        let capture = self.capture.clone();
        let bounds = self.bounds.clone();
        let templates = self.templates.clone();
        let version = self.options.version.clone();
        // Context may include a long active conversation. Its encoding and
        // compression must not monopolize a runtime thread serving hook IPC.
        let (translator, reader, checkpoint) = tokio::task::spawn_blocking(move || {
            let checkpoint = (|| -> anyhow::Result<Option<TranslationCheckpoint>> {
                let Some(state) = translator.save_state()? else {
                    return Ok(None);
                };
                let state = RecoveryContext {
                    format: 1,
                    translator: serde_json::value::RawValue::from_string(String::from_utf8(
                        state,
                    )?)?,
                    decoder: reader.decoder_context().to_vec(),
                    capture,
                    bounds,
                    templates,
                };
                let mut encoder = zstd::stream::Encoder::new(Vec::new(), 1)?;
                serde_json::to_writer(&mut encoder, &state)?;
                let state = encoder.finish()?;
                Ok(Some(TranslationCheckpoint {
                    input,
                    version,
                    state,
                }))
            })();
            (translator, reader, checkpoint)
        })
        .await?;
        self.translator = Some(translator);
        self.reader = Some(reader);
        checkpoint
    }

    async fn persist_current_context(&mut self) -> anyhow::Result<()> {
        if self.checkpoint_position == self.committed {
            return Ok(());
        }
        if let Some(checkpoint) = self.checkpoint_context(self.committed).await? {
            self.journal.save_translation_checkpoint(checkpoint).await?;
            self.checkpoint_position = self.committed;
            self.checkpoint_events = 0;
        }
        Ok(())
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
        self.append_control(kind).await?;
        self.advance(self.progress.requested.load(Ordering::Relaxed))
            .await?;
        self.journal.tail().await
    }
    async fn append_control(&mut self, kind: ControlKind) -> anyhow::Result<()> {
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
        let through = self
            .options
            .events
            .lock()
            .await
            .append_control(&control)
            .await?;
        self.progress
            .requested
            .fetch_max(through, Ordering::Relaxed);
        Ok(())
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
        self.translator = Some(
            self.options
                .registry
                .create_checked_with_session_namespace(
                    &self.options.source,
                    &self.options.namespace,
                )?,
        );
        self.position = 0;
        self.checkpoint_position = 0;
        self.checkpoint_events = 0;
        self.checkpoint_safe = true;
        if let Some(checkpoint) = self.journal.translation_checkpoint().await? {
            if checkpoint.version == self.options.version && checkpoint.input <= self.committed {
                let restored = async {
                    let registry = self.options.registry.clone();
                    let source = self.options.source.clone();
                    let namespace = self.options.namespace.clone();
                    let version = self.options.version.clone();
                    let committed = self.committed;
                    let input = checkpoint.input;
                    let (translator, context) =
                        tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
                            let context =
                                RecoveryContext::decode(&checkpoint, &version, committed)?;
                            let mut translator = registry
                                .create_checked_with_session_namespace(&source, &namespace)?;
                            translator.restore_state(context.translator.get().as_bytes())?;
                            Ok((translator, context))
                        })
                        .await??;
                    let reader = JournalReader::resume(
                        &self.options.event_path,
                        input,
                        self.committed,
                        context.decoder,
                    )
                    .await?;
                    self.translator = Some(translator);
                    self.reader = Some(reader);
                    self.capture = context.capture;
                    self.bounds = context.bounds;
                    self.templates = context.templates;
                    self.position = input;
                    self.checkpoint_position = input;
                    Ok::<_, anyhow::Error>(())
                }
                .await;
                match restored {
                    Ok(()) => return Ok(()),
                    Err(error) => {
                        tracing::warn!(%error, "invalid translation checkpoint; rebuilding from journal")
                    }
                }
            }
        }
        self.reader = JournalReader::open(&self.options.event_path, self.committed).await?;
        Ok(())
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
        let reader = self
            .reader
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("event journal is missing"))?;
        reader.extend(through);
        self.phase = Boundary::Journal;
        while let Some(entry) = self
            .reader
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("event journal is missing"))?
            .next_record()
            .await?
        {
            let input = entry.through;
            let observation = !matches!(&entry.record,JournalRecord::Control(control) if matches!(control.kind,ControlKind::Retire));
            let ctx = SessionCtx {
                session_id: self.options.session_id.clone(),
                config: None,
            };
            let (first, capture) = match entry.record {
                JournalRecord::Event(event) => {
                    let mut env = crate::journal::envelope_from_redacted(event);
                    if let Some(route) = &env.route {
                        self.journal.register(route).await?;
                        // Discovery needs subscriptions and transcript references,
                        // not copies of native prompts, responses, or credentials.
                        let mut template = env.redacted();
                        template.payload = env
                            .payload
                            .as_object()
                            .map(|payload| {
                                serde_json::Value::Object(
                                    payload
                                        .iter()
                                        .filter(|(key, _)| {
                                            matches!(
                                                key.as_str(),
                                                "transcript_path"
                                                    | "agent_transcript_path"
                                                    | "cwd"
                                                    | "_bt_transcript_mirror"
                                                    | "_bt_claude_transcript_mirrors"
                                            )
                                        })
                                        .map(|(key, value)| (key.clone(), value.clone()))
                                        .collect(),
                                )
                            })
                            .unwrap_or_else(|| serde_json::json!({}));
                        self.templates
                            .insert(serde_json::to_string(route)?, template);
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
                        self.translator
                            .as_mut()
                            .ok_or_else(|| anyhow::anyhow!("translator is missing"))?
                            .handle(&env, &ctx)?,
                        Some(capture),
                    )
                }
                JournalRecord::Control(control) => {
                    // Retirement exports defensive terminal updates but does not
                    // close native state when rebuilding for later captured work.
                    if input <= self.committed && matches!(control.kind, ControlKind::Retire) {
                        self.position = input;
                        continue;
                    }
                    if matches!(control.kind, ControlKind::Retire) {
                        // Retirement emits defensive ends without making them
                        // part of the logical state restored on a later resume.
                        self.persist_current_context().await?;
                        self.checkpoint_safe = false;
                    }
                    self.phase = Boundary::Translation;
                    let translator = self
                        .translator
                        .as_mut()
                        .ok_or_else(|| anyhow::anyhow!("translator is missing"))?;
                    let ops = if !matches!(control.kind, ControlKind::Checkpoint) {
                        translator.finalize_at(&ctx, &control.bounds)?
                    } else {
                        translator.checkpoint_at(&ctx, &control.bounds)?
                    };
                    (ops, self.capture.clone())
                }
                JournalRecord::DeliveryCheckpoint { .. } => {
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
            let ctx = SessionCtx {
                session_id: self.options.session_id.clone(),
                config: None,
            };
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
                    .translator
                    .as_mut()
                    .ok_or_else(|| anyhow::anyhow!("translator is missing"))?
                    .drain_pending(&ctx)?;
                pending.complete = pending.ops.is_none();
                pending.prepared = false;
            }
            if publish {
                self.phase = Boundary::Journal;
                let checkpoint = if self.checkpoint_events + 1 >= CHECKPOINT_EVENTS
                    || pending.input.saturating_sub(self.checkpoint_position) >= CHECKPOINT_BYTES
                {
                    self.checkpoint_context(pending.input).await?
                } else {
                    None
                };
                let checkpointed = checkpoint.is_some();
                self.journal
                    .commit_with_context(pending.input, self.roots.pending(), checkpoint)
                    .await?;
                if checkpointed {
                    self.checkpoint_position = pending.input;
                    self.checkpoint_events = 0;
                } else {
                    self.checkpoint_events += 1;
                }
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
mod tests {
    use super::*;
    use crate::{pipeline::delivery::InputPort, translate::SpanRow};

    fn resume_options(
        data_dir: &std::path::Path,
        source: &str,
        events: Arc<tokio::sync::Mutex<JournalWriter>>,
    ) -> TranslationOptions {
        TranslationOptions {
            source: source.into(),
            session_id: "checkpoint".into(),
            namespace: crate::ids::session_namespace(source, "checkpoint"),
            version: "test".into(),
            data_dir: data_dir.into(),
            events,
            event_path: crate::journal::source_journal_path(data_dir, source, "checkpoint"),
            registry: Arc::new(Registry::default_agents()),
            correlation: Arc::default(),
            correlation_changed: Arc::default(),
        }
    }

    struct BlockedCheckpoint {
        entered: Arc<Notify>,
        release: std::sync::mpsc::Receiver<()>,
    }
    impl AgentTranslator for BlockedCheckpoint {
        fn handle(
            &mut self,
            _: &crate::wire::Envelope,
            _: &SessionCtx,
        ) -> anyhow::Result<Vec<SpanOp>> {
            Ok(Vec::new())
        }
        fn save_state(&self) -> anyhow::Result<Option<Vec<u8>>> {
            self.entered.notify_one();
            self.release
                .recv_timeout(std::time::Duration::from_secs(10))?;
            Ok(Some(b"{}".to_vec()))
        }
    }

    #[tokio::test]
    async fn slow_checkpoint_encoding_does_not_block_journal_capture_on_the_runtime() {
        let tmp = tempfile::tempdir().unwrap();
        let path = crate::journal::source_journal_path(tmp.path(), "debug", "checkpoint");
        let writer = Arc::new(tokio::sync::Mutex::new(
            JournalWriter::open_path(&path).await.unwrap(),
        ));
        let entered = Arc::new(Notify::new());
        let (release, resume) = std::sync::mpsc::channel();
        let mut producer = Producer {
            options: resume_options(tmp.path(), "debug", writer.clone()),
            journal: SpanJournal::open(&tmp.path().join("checkpoint.sqlite"))
                .await
                .unwrap(),
            progress: Arc::default(),
            error: Arc::default(),
            recovery: Arc::default(),
            translator: Some(Box::new(BlockedCheckpoint {
                entered: entered.clone(),
                release: resume,
            })),
            reader: JournalReader::open(&path, 0).await.unwrap(),
            observers: Vec::new(),
            origins: WeakOrigins::default(),
            roots: Roots::default(),
            capture: None,
            pending_snapshots: HashSet::new(),
            bounds: HashMap::new(),
            templates: HashMap::new(),
            position: 0,
            committed: 0,
            checkpoint_position: 0,
            checkpoint_events: 0,
            checkpoint_safe: true,
            state: State::Processing,
            publication: None,
            phase: Boundary::Journal,
        };
        let checkpoint = tokio::spawn(async move { producer.checkpoint_context(0).await });
        entered.notified().await;
        let env = serde_json::from_value(serde_json::json!({"source":"debug","session_id":"checkpoint","event":"during-checkpoint","ts_ms":1,"payload":{}})).unwrap();
        let through = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            writer.lock().await.append(&env).await
        })
        .await
        .unwrap()
        .unwrap();
        assert!(through > 0);
        release.send(()).unwrap();
        assert!(checkpoint.await.unwrap().unwrap().is_some());
    }

    #[tokio::test]
    async fn every_agent_journal_resumes_at_its_persisted_byte_offset() {
        for source in Registry::default_agents().sources() {
            let tmp = tempfile::tempdir().unwrap();
            let path = crate::journal::source_journal_path(tmp.path(), &source, "checkpoint");
            let writer = Arc::new(tokio::sync::Mutex::new(
                JournalWriter::open_path(&path).await.unwrap(),
            ));
            let translation =
                Translation::spawn(resume_options(tmp.path(), &source, writer.clone()))
                    .await
                    .unwrap();
            let control = PipelineControl {
                kind: ControlKind::Checkpoint,
                bounds: HashMap::new(),
            };
            let mut tail = 0;
            // Ordered controls exercise the real producer and each concrete
            // translator without depending on a particular native hook schema.
            // Native open-span/state equivalence is covered separately.
            for _ in 0..CHECKPOINT_EVENTS + 3 {
                tail = writer.lock().await.append_control(&control).await.unwrap();
            }
            translation.advance(tail).await.unwrap();
            translation.barrier().await.unwrap();
            let checkpoint = translation
                .journal
                .translation_checkpoint()
                .await
                .unwrap()
                .unwrap();
            assert!(
                checkpoint.input > 0 && checkpoint.input < tail,
                "{source}: periodic checkpoint must precede the uncheckpointed suffix"
            );
            let task = translation.task.lock().await.take().unwrap();
            task.abort();
            let _ = task.await;
            let mut contents = tokio::fs::read(&path).await.unwrap();
            for byte in &mut contents[..checkpoint.input as usize] {
                if *byte != b'\n' {
                    *byte = b'x';
                }
            }
            tokio::fs::write(&path, contents).await.unwrap();
            tail = writer.lock().await.append_control(&control).await.unwrap();
            let resumed = Translation::spawn(resume_options(tmp.path(), &source, writer.clone()))
                .await
                .unwrap();
            resumed.advance(tail).await.unwrap();
            resumed.barrier().await.unwrap_or_else(|error| {
                panic!("{source} could not resume past its saved byte offset: {error}")
            });
            assert_eq!(
                resumed.journal.translated_through().await.unwrap(),
                tail,
                "{source}"
            );
            resumed.shutdown().await;
        }
    }

    #[tokio::test]
    async fn checkpoint_seeks_past_completed_history_and_restores_pi_delta_context() {
        for source in ["debug", "pi"] {
            let tmp = tempfile::tempdir().unwrap();
            let path = crate::journal::source_journal_path(tmp.path(), source, "checkpoint");
            let writer = Arc::new(tokio::sync::Mutex::new(
                JournalWriter::open_path(&path).await.unwrap(),
            ));
            let translation =
                Translation::spawn(resume_options(tmp.path(), source, writer.clone()))
                    .await
                    .unwrap();
            let mut env = crate::wire::Envelope {
                source: source.into(),
                session_id: "checkpoint".into(),
                event: "context".into(),
                ts_ms: 0,
                payload: serde_json::json!({}),
                route: Some(SessionRoute::default()),
                config: None,
                managed_run_id: None,
                capture: None,
                source_version: None,
                plugin_version: None,
            };
            let mut boundary = 0;
            for i in 0..CHECKPOINT_EVENTS + 3 {
                env.ts_ms = i as i64;
                env.payload = serde_json::json!({"event":{"messages":[{"role":"user","content":"shared"},{"role":"assistant","content":i}]}});
                boundary = writer.lock().await.append(&env).await.unwrap();
            }
            translation.advance(boundary).await.unwrap();
            translation.barrier().await.unwrap();
            let checkpoint = translation
                .journal
                .translation_checkpoint()
                .await
                .unwrap()
                .unwrap();
            assert!(
                checkpoint.input < boundary,
                "test must replay a suffix after a periodic checkpoint"
            );
            let task = translation.task.lock().await.take().unwrap();
            task.abort();
            let _ = task.await; // crash, without the idle checkpoint
                                // Keep boundaries intact but make all pre-checkpoint JSON invalid.
                                // Recovery must seek over this prefix rather than decode/retranslate it.
            let mut contents = tokio::fs::read(&path).await.unwrap();
            for byte in &mut contents[..checkpoint.input as usize] {
                if *byte != b'\n' {
                    *byte = b'x';
                }
            }
            tokio::fs::write(&path, contents).await.unwrap();
            env.ts_ms += 1;
            env.payload["event"]["messages"][1]["content"] = serde_json::json!("after restart");
            let tail = writer.lock().await.append(&env).await.unwrap();
            let resumed = Translation::spawn(resume_options(tmp.path(), source, writer.clone()))
                .await
                .unwrap();
            resumed.advance(tail).await.unwrap();
            resumed.barrier().await.unwrap();
            assert!(
                resumed.error.lock().is_none(),
                "{source}: {:?}",
                resumed.error.lock()
            );
            assert_eq!(resumed.journal.translated_through().await.unwrap(), tail);
            resumed.shutdown().await;
        }
    }

    #[tokio::test]
    async fn invalid_or_old_context_rebuilds_safely_from_the_source_journal() {
        for alteration in [
            "UPDATE translation_checkpoint SET state=x'00'",
            "UPDATE translation_checkpoint SET version='old'",
        ] {
            let tmp = tempfile::tempdir().unwrap();
            let path = crate::journal::source_journal_path(tmp.path(), "debug", "checkpoint");
            let writer = Arc::new(tokio::sync::Mutex::new(
                JournalWriter::open_path(&path).await.unwrap(),
            ));
            let env: crate::wire::Envelope = serde_json::from_value(serde_json::json!({
                "source":"debug", "session_id":"checkpoint", "event":"event", "ts_ms":1,"payload":{},
            })).unwrap();
            let translation =
                Translation::spawn(resume_options(tmp.path(), "debug", writer.clone()))
                    .await
                    .unwrap();
            let mut tail = 0;
            for _ in 0..CHECKPOINT_EVENTS {
                tail = writer.lock().await.append(&env).await.unwrap();
            }
            translation.advance(tail).await.unwrap();
            translation.barrier().await.unwrap();
            translation.shutdown().await;
            let storage = tmp.path().join("pipeline").join(format!(
                "{}.sqlite",
                crate::ids::session_storage_id("debug", "checkpoint")
            ));
            let db = rusqlite::Connection::open(storage).unwrap();
            db.execute_batch(alteration).unwrap();
            tail = writer.lock().await.append(&env).await.unwrap();
            let resumed = Translation::spawn(resume_options(tmp.path(), "debug", writer.clone()))
                .await
                .unwrap();
            resumed.advance(tail).await.unwrap();
            resumed.barrier().await.unwrap();
            assert!(resumed.error.lock().is_none());
            assert_eq!(resumed.journal.translated_through().await.unwrap(), tail);
            let batches: u64 = db
                .query_row(
                    "SELECT count(*) FROM batches WHERE committed=1",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(batches, CHECKPOINT_EVENTS + 1);
            resumed.shutdown().await;
        }
    }

    #[tokio::test]
    async fn translation_checkpoint_commit_failure_does_not_move_the_source_head() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("atomic.sqlite");
        let journal = SpanJournal::open(&path).await.unwrap();
        journal.begin(10, true).await.unwrap();
        let db = rusqlite::Connection::open(&path).unwrap();
        db.execute_batch("CREATE TRIGGER fail_context BEFORE INSERT ON translation_checkpoint BEGIN SELECT RAISE(FAIL,'checkpoint write failed'); END;").unwrap();
        let context = TranslationCheckpoint {
            input: 10,
            version: "test".into(),
            state: vec![1, 2],
        };
        assert!(journal
            .commit_with_context(10, &Default::default(), Some(context.clone()))
            .await
            .is_err());
        assert_eq!(journal.translated_through().await.unwrap(), 0);
        assert!(journal.translation_checkpoint().await.unwrap().is_none());
        db.execute_batch("DROP TRIGGER fail_context;").unwrap();
        journal
            .commit_with_context(10, &Default::default(), Some(context))
            .await
            .unwrap();
        assert_eq!(journal.translated_through().await.unwrap(), 10);
        assert_eq!(
            journal
                .translation_checkpoint()
                .await
                .unwrap()
                .unwrap()
                .input,
            10
        );
    }

    #[tokio::test]
    async fn shared_storage_stays_open_until_the_last_route_finishes_draining() {
        let tmp = tempfile::tempdir().unwrap();
        let event_path = tmp.path().join("events.ndjson");
        let translation = Translation::spawn(TranslationOptions {
            source: "debug".into(),
            session_id: "draining".into(),
            namespace: "draining".into(),
            version: "test".into(),
            data_dir: tmp.path().into(),
            events: Arc::new(tokio::sync::Mutex::new(
                JournalWriter::open_path(&event_path).await.unwrap(),
            )),
            event_path,
            registry: Arc::new(Registry::default_agents()),
            correlation: Arc::new(crate::correlation::CorrelationRegistry::default()),
            correlation_changed: Arc::new(Notify::new()),
        })
        .await
        .unwrap();
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
        let tmp = tempfile::tempdir().unwrap();
        let event_path = tmp.path().join("events.ndjson");
        let correlation = Arc::new(crate::correlation::CorrelationRegistry::default());
        let translation = Translation::spawn(TranslationOptions {
            source: "debug".into(),
            session_id: "staged".into(),
            namespace: "staged".into(),
            version: "test".into(),
            data_dir: tmp.path().into(),
            events: Arc::new(tokio::sync::Mutex::new(
                JournalWriter::open_path(&event_path).await.unwrap(),
            )),
            event_path,
            registry: Arc::new(Registry::default_agents()),
            correlation: correlation.clone(),
            correlation_changed: Arc::new(Notify::new()),
        })
        .await
        .unwrap();
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
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("spans.sqlite");
        let journal = SpanJournal::open(&path).await.unwrap();
        let db = rusqlite::Connection::open(&path).unwrap();
        let event_path = tmp.path().join("events.ndjson");
        let drains = Arc::new(AtomicU64::new(0));
        let mut producer = Producer {
            options: TranslationOptions {
                source: "debug".into(),
                session_id: "test".into(),
                namespace: "test".into(),
                version: "test".into(),
                data_dir: tmp.path().into(),
                events: Arc::new(tokio::sync::Mutex::new(
                    JournalWriter::open_path(&event_path).await.unwrap(),
                )),
                event_path,
                registry: Arc::new(Registry::default_agents()),
                correlation: Arc::new(crate::correlation::CorrelationRegistry::default()),
                correlation_changed: Arc::new(Notify::new()),
            },
            progress: Arc::new(Progress::default()),
            journal: journal.clone(),
            error: Arc::new(Mutex::new(None)),
            recovery: Arc::new(Mutex::new(None)),
            translator: Some(Box::new(CountedTranslation(drains.clone()))),
            reader: None,
            observers: Vec::new(),
            origins: WeakOrigins::default(),
            roots: Roots::default(),
            capture: None,
            pending_snapshots: HashSet::new(),
            bounds: HashMap::new(),
            templates: HashMap::new(),
            position: 0,
            committed: 0,
            checkpoint_position: 0,
            checkpoint_events: 0,
            checkpoint_safe: true,
            state: State::Processing,
            phase: Boundary::Journal,
            publication: Some(Publication {
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
            }),
        };
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
