//! One ordered source consumer. Output publication is independent of route delivery.
use super::input::{SourceEvent, SourceInput, SourcePosition, SourceRecord};
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
    translator: Box<serde_json::value::RawValue>,
    pub decoder: crate::journal::DecoderContext,
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

    pub async fn has_head(&self, route: &SessionRoute) -> anyhow::Result<bool> {
        self.journal.has_head(route).await
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
    decoded_bytes: u64,
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
    decoder: crate::journal::DecoderContext,
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
    checkpoint_work_bytes: u64,
    checkpoint_safe: bool,
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
            decoder: Default::default(),
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
            checkpoint_work_bytes: 0,
            checkpoint_safe: true,
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
        if !self.checkpoint_safe || self.translator.is_none() {
            return Ok(None);
        }
        let translator = self
            .translator
            .take()
            .ok_or_else(|| anyhow::anyhow!("translator is missing"))?;
        let decoder = std::mem::take(&mut self.decoder);
        let capture = self.capture.clone();
        let bounds = self.bounds.clone();
        let templates = self.templates.clone();
        let version = self.options.version.clone();
        // Context may include a long active conversation. Its encoding and
        // compression must not monopolize a runtime thread serving hook IPC.
        let (translator, decoder, checkpoint) = tokio::task::spawn_blocking(move || {
            let checkpoint = (|| -> anyhow::Result<Option<TranslationCheckpoint>> {
                let Some(state) = translator.save_state()? else {
                    return Ok(None);
                };
                let state = RecoveryContext {
                    translator: serde_json::value::RawValue::from_string(String::from_utf8(
                        state,
                    )?)?,
                    decoder: decoder.clone(),
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
            (translator, decoder, checkpoint)
        })
        .await?;
        self.translator = Some(translator);
        self.decoder = decoder;
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
            self.checkpoint_work_bytes = 0;
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
            let position = writer.append_control_record(&control).await?;
            let through = position.through;
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
        self.decoder = Default::default();
        self.checkpoint_position = 0;
        self.checkpoint_events = 0;
        self.checkpoint_work_bytes = 0;
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
                        Default::default(),
                    )
                    .await?;
                    self.translator = Some(translator);
                    self.reader = Some(reader);
                    self.decoder = context.decoder;
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
        self.reader = if self.committed > 0 {
            JournalReader::open(&self.options.event_path, self.committed).await?
        } else {
            None
        };
        Ok(())
    }

    async fn next_input(&mut self, through: u64) -> anyhow::Result<Option<SourceRecord>> {
        if let Some(record) = self.options.input.take(self.position, through) {
            if let SourceEvent::Event(event) = &record.event {
                self.decoder.record_live(&event.event, &record.position);
            }
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
                JournalReader::resume(
                    &self.options.event_path,
                    self.position,
                    through,
                    Default::default(),
                )
                .await?,
            );
        }
        let reader = self
            .reader
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("recovery reader is missing"))?;
        reader.extend(through);
        reader.swap_decoder_context(&mut self.decoder);
        let entry = reader.next_record().await;
        reader.swap_decoder_context(&mut self.decoder);
        let Some(entry) = entry? else {
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
                decoded_bytes: entry.decoded_bytes,
                decoder_base: None,
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
                    if matches!(control.kind, ControlKind::Retire) {
                        // Retirement emits defensive ends without making them
                        // part of the logical state restored on a later resume.
                        self.persist_current_context().await?;
                        self.checkpoint_safe = false;
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
                decoded_bytes: entry.position.decoded_bytes,
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
                let checkpoint = if self.checkpoint_events + 1 >= CHECKPOINT_EVENTS
                    || pending.input.saturating_sub(self.checkpoint_position) >= CHECKPOINT_BYTES
                    || self
                        .checkpoint_work_bytes
                        .saturating_add(pending.decoded_bytes)
                        >= CHECKPOINT_BYTES
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
                    self.checkpoint_work_bytes = 0;
                } else {
                    self.checkpoint_events += 1;
                    self.checkpoint_work_bytes = self
                        .checkpoint_work_bytes
                        .saturating_add(pending.decoded_bytes);
                }
                self.committed = pending.input;
                self.progress
                    .committed
                    .store(pending.input, Ordering::Relaxed);
            } else {
                // Restoring a checkpoint replays the committed suffix too.
                // Count that work so repeated short-lived processes cannot
                // keep extending the distance from the same old checkpoint.
                self.checkpoint_events += 1;
                self.checkpoint_work_bytes = self
                    .checkpoint_work_bytes
                    .saturating_add(pending.decoded_bytes);
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
    mod streams {
        use crate as bt_daemon;
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/support/translation_streams.rs"
        ));
    }

    #[tokio::test]
    async fn native_source_output_matches_live_overflow_and_cold_recovery() {
        let mut covered = Vec::new();
        for (source, mut events, _fixtures) in streams::native_streams() {
            covered.push(source.clone());
            events.insert(0, events[0].clone());
            for event in &mut events {
                event.payload["codec_padding"] = serde_json::json!("é📚history".repeat(1024));
            }
            let mut expected = None;
            for mode in ["live", "overflow", "cold"] {
                let (tmp, path, writer) = source_journal(&source).await;
                let options = resume_options(tmp.path(), &source, writer.clone());
                let input = options.input.clone();
                let mut translation = Translation::spawn(options).await.unwrap();
                let mut positions = Vec::new();
                for (index, event) in events.iter().enumerate() {
                    let through = if mode == "cold" && index > 0 {
                        writer.lock().await.append(event).await.unwrap()
                    } else {
                        capture(&writer, &input, event).await
                    };
                    positions.push(through);
                }
                if mode == "overflow" {
                    // Emulate losing the first two queued records while their
                    // complete persisted forms remain recoverable.
                    assert!(input.take(0, positions[0]).is_some());
                    assert!(input.take(positions[0], positions[1]).is_some());
                }
                if mode == "cold" {
                    translation.advance(positions[0]).await.unwrap();
                    translation.barrier().await.unwrap();
                    translation.shutdown().await;
                    // A restart must seek over this prefix, not replay it.
                    let mut bytes = tokio::fs::read(&path).await.unwrap();
                    for byte in &mut bytes[..positions[0] as usize] {
                        if *byte != b'\n' {
                            *byte = b'x';
                        }
                    }
                    tokio::fs::write(&path, bytes).await.unwrap();
                    // New writer and input: the following stored delta must
                    // rely on the SQLite decoder/reducer checkpoint alone.
                    *writer.lock().await = JournalWriter::open_path(&path).await.unwrap();
                    translation =
                        Translation::spawn(resume_options(tmp.path(), &source, writer.clone()))
                            .await
                            .unwrap();
                }
                translation
                    .advance(*positions.last().unwrap())
                    .await
                    .unwrap();
                translation
                    .barrier()
                    .await
                    .unwrap_or_else(|error| panic!("{source} {mode}: {error}"));
                translation.control(ControlKind::Finalize).await.unwrap();
                let rows = streams::comparable(
                    translation
                        .journal
                        .read_after(0, 10000)
                        .await
                        .unwrap()
                        .into_iter()
                        .map(|(_, op)| op)
                        .collect(),
                );
                assert!(!rows.is_empty(), "{source} produced no observations");
                if let Some(expected) = &expected {
                    assert_eq!(&rows, expected, "{source} {mode}");
                } else {
                    expected = Some(rows);
                }
                translation.shutdown().await;
            }
        }
        covered.sort();
        assert_eq!(covered, Registry::default_agents().sources());
    }

    async fn source_journal(
        source: &str,
    ) -> (
        tempfile::TempDir,
        PathBuf,
        Arc<tokio::sync::Mutex<JournalWriter>>,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let path = crate::journal::source_journal_path(dir.path(), source, "checkpoint");
        let writer = Arc::new(tokio::sync::Mutex::new(
            JournalWriter::open_path(&path).await.unwrap(),
        ));
        (dir, path, writer)
    }

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
            input: SourceInput::default(),
            registry: Arc::new(Registry::default_agents()),
            correlation: Arc::default(),
            correlation_changed: Arc::default(),
        }
    }

    #[tokio::test]
    async fn checkpoints_cannot_absorb_captured_events_before_dispatch() {
        let (tmp, _, writer) = source_journal("debug").await;
        let options = resume_options(tmp.path(), "debug", writer.clone());
        let input = options.input.clone();
        let source = Translation::spawn(options).await.unwrap();
        let mut event: crate::wire::Envelope = serde_json::from_value(serde_json::json!({"source":"debug","session_id":"checkpoint","event":"event","ts_ms":1,"payload":{"index":0}})).unwrap();
        let first = capture(&writer, &input, &event).await;
        source.advance(first).await.unwrap();
        source.barrier().await.unwrap();
        event.payload["index"] = 1.into();
        let second = capture(&writer, &input, &event).await;
        // Capture has acknowledged a later event whose routing/ancestry has
        // not been dispatched. A flush must not translate it ahead of that.
        source.control(ControlKind::Checkpoint).await.unwrap();
        assert_eq!(source.journal.translated_through().await.unwrap(), first);
        assert_eq!(
            writer.lock().await.position(),
            second,
            "no control marker should leap over undispatched work"
        );
        source.advance(second).await.unwrap();
        source.barrier().await.unwrap();
        source.control(ControlKind::Checkpoint).await.unwrap();
        assert!(source.journal.translated_through().await.unwrap() > second);
        source.shutdown().await;
    }

    #[tokio::test]
    async fn live_translation_and_controls_do_not_read_events_back_from_disk() {
        let (tmp, path, writer) = source_journal("debug").await;
        let options = resume_options(tmp.path(), "debug", writer.clone());
        let input = options.input.clone();
        let translation = Translation::spawn(options).await.unwrap();
        let mut event: crate::wire::Envelope = serde_json::from_value(serde_json::json!({
            "source":"debug", "session_id":"checkpoint", "event":"event", "ts_ms":1,
            "payload":{"history":"shared history".repeat(1024),"index":0}
        }))
        .unwrap();
        capture(&writer, &input, &event).await;
        event.payload["index"] = 1.into();
        let through = capture(&writer, &input, &event).await;
        let original = tokio::fs::read(&path).await.unwrap();
        assert!(String::from_utf8_lossy(&original).contains("payload_delta_v1"));
        // A journal read would fail. The complete captured records are still
        // available, so normal translation and flush controls must succeed.
        let mut poisoned = original.clone();
        for byte in &mut poisoned {
            if *byte != b'\n' {
                *byte = b'x';
            }
        }
        tokio::fs::write(&path, poisoned).await.unwrap();
        translation.advance(through).await.unwrap();
        translation.barrier().await.unwrap();
        translation.control(ControlKind::Checkpoint).await.unwrap();
        let rows = translation.journal.read_after(0, 32).await.unwrap();
        let inputs = rows
            .iter()
            .filter_map(|(_, op)| {
                let (SpanOp::Insert(row) | SpanOp::Merge(row)) = op;
                row.input.as_ref()
            })
            .collect::<Vec<_>>();
        assert_eq!(inputs.len(), 2);
        assert_eq!(inputs[1], &event.payload);
        translation.shutdown().await;

        // A cold recovery must restore the decoder bases learned from live
        // records, then expand a delta written after the saved checkpoint.
        event.payload["index"] = 2.into();
        let tail = writer.lock().await.append(&event).await.unwrap();
        *writer.lock().await = JournalWriter::open_path(&path).await.unwrap();
        let resumed = Translation::spawn(resume_options(tmp.path(), "debug", writer.clone()))
            .await
            .unwrap();
        resumed.advance(tail).await.unwrap();
        resumed.barrier().await.unwrap();
        assert_eq!(resumed.journal.translated_through().await.unwrap(), tail);
        let rows = resumed.journal.read_after(0, 32).await.unwrap();
        assert!(rows.iter().any(|(_, op)| {
            let (SpanOp::Insert(row) | SpanOp::Merge(row)) = op;
            row.input.as_ref() == Some(&event.payload)
        }));
        resumed.shutdown().await;
    }

    async fn capture(
        writer: &Arc<tokio::sync::Mutex<JournalWriter>>,
        input: &SourceInput,
        event: &crate::wire::Envelope,
    ) -> u64 {
        let mut writer = writer.lock().await;
        let position = writer.append_ordered(event, None).await.unwrap();
        let through = position.through;
        input.push_event(event.clone(), position);
        through
    }

    #[tokio::test]
    async fn evicted_input_recovers_only_the_gap_then_rejoins_direct_capture() {
        let (tmp, path, writer) = source_journal("debug").await;
        let options = resume_options(tmp.path(), "debug", writer.clone());
        let input = options.input.clone();
        let translation = Translation::spawn(options).await.unwrap();
        let mut event: crate::wire::Envelope = serde_json::from_value(serde_json::json!({
            "source":"debug", "session_id":"checkpoint", "event":"event", "ts_ms":1,
            "payload":{"history":"shared".repeat(1024),"index":0}
        }))
        .unwrap();
        for batch in 0..2 {
            let mut recovery_end = 0;
            let mut through = 0;
            for index in 0..80 {
                event.payload["index"] = (batch * 80 + index).into();
                through = capture(&writer, &input, &event).await;
                if index == 15 {
                    recovery_end = through;
                }
            }
            // Only the first 16 records were evicted. Poison the retained
            // live suffix so any attempt to read it through storage fails.
            let mut stored = tokio::fs::read(&path).await.unwrap();
            for byte in &mut stored[recovery_end as usize..through as usize] {
                if *byte != b'\n' {
                    *byte = b'x';
                }
            }
            tokio::fs::write(&path, stored).await.unwrap();
            translation.advance(through).await.unwrap();
            translation.barrier().await.unwrap();
            assert!(input.is_empty());
        }
        let rows = translation.journal.read_after(0, 256).await.unwrap();
        let inputs = rows
            .iter()
            .filter_map(|(_, op)| {
                let (SpanOp::Insert(row) | SpanOp::Merge(row)) = op;
                row.input.as_ref()
            })
            .collect::<Vec<_>>();
        assert_eq!(inputs.len(), 160);
        for (index, payload) in inputs.into_iter().enumerate() {
            assert_eq!(payload["index"], index);
            assert_eq!(payload["history"], event.payload["history"]);
        }
        translation.shutdown().await;
    }

    struct BlockedCheckpoint {
        entered: Arc<Notify>,
        release: std::sync::mpsc::Receiver<()>,
    }

    struct BlockedReplay {
        entered: Arc<Notify>,
        finished: Arc<std::sync::atomic::AtomicBool>,
        release: std::sync::mpsc::Receiver<()>,
    }
    impl AgentTranslator for BlockedReplay {
        fn handle(
            &mut self,
            _: &crate::wire::Envelope,
            _: &SessionCtx,
        ) -> anyhow::Result<Vec<SpanOp>> {
            self.entered.notify_one();
            let _ = self.release.recv_timeout(std::time::Duration::from_secs(5));
            self.finished.store(true, Ordering::Release);
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn synchronous_replay_work_cannot_block_the_hook_runtime() {
        let (tmp, _, writer) = source_journal("debug").await;
        let mut event: crate::wire::Envelope = serde_json::from_value(serde_json::json!({"source":"debug","session_id":"checkpoint","event":"event","ts_ms":1,"payload":{}})).unwrap();
        let through = writer.lock().await.append(&event).await.unwrap();
        let options = resume_options(tmp.path(), "debug", writer.clone());
        let input = options.input.clone();
        let entered = Arc::new(Notify::new());
        let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (release, receiver) = std::sync::mpsc::channel();
        let mut producer = Producer::open(options).await.unwrap();
        producer.translator = Some(Box::new(BlockedReplay {
            entered: entered.clone(),
            finished: finished.clone(),
            release: receiver,
        }));
        let started = std::time::Instant::now();
        let processing = tokio::spawn(async move {
            let result = producer.advance(through).await;
            (producer, result)
        });
        entered.notified().await;
        let elapsed = started.elapsed();
        event.ts_ms = 2;
        let captured = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            capture(&writer, &input, &event),
        )
        .await;
        let replay_still_blocked = !finished.load(Ordering::Acquire);
        let _ = release.send(());
        let (producer, result) = processing.await.unwrap();
        assert!(
            elapsed < std::time::Duration::from_secs(1),
            "replay monopolized the hook runtime for {elapsed:?}"
        );
        assert!(captured.unwrap() > through);
        assert!(
            replay_still_blocked,
            "capture only completed after synchronous replay returned"
        );
        result.unwrap();
        assert_eq!(
            producer.journal.translated_through().await.unwrap(),
            through,
            "replay must retain its original stopping point"
        );
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
        let (tmp, _, writer) = source_journal("debug").await;
        let entered = Arc::new(Notify::new());
        let (release, resume) = std::sync::mpsc::channel();
        let mut producer = Producer::open(resume_options(tmp.path(), "debug", writer.clone()))
            .await
            .unwrap();
        producer.translator = Some(Box::new(BlockedCheckpoint {
            entered: entered.clone(),
            release: resume,
        }));
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
    async fn checkpoint_budget_counts_decoded_payloads_after_compression() {
        let (tmp, _, writer) = source_journal("debug").await;
        let source = Translation::spawn(resume_options(tmp.path(), "debug", writer.clone()))
            .await
            .unwrap();
        let env = serde_json::from_value(serde_json::json!({"source":"debug","session_id":"checkpoint","event":"event","ts_ms":1,"payload":{"history":"x".repeat(256 * 1024)}})).unwrap();
        let mut first = 0;
        let mut tail = 0;
        for index in 0..10 {
            tail = writer.lock().await.append(&env).await.unwrap();
            if index == 0 {
                first = tail;
            }
        }
        assert!(
            tail < CHECKPOINT_BYTES,
            "fixture must compress below the physical byte threshold"
        );
        source.advance(tail).await.unwrap();
        source.barrier().await.unwrap();
        let checkpoint = source
            .journal
            .translation_checkpoint()
            .await
            .unwrap()
            .expect("decoded work must trigger a checkpoint before 64 inputs");
        assert!(checkpoint.input > first);
        source.shutdown().await;
    }

    #[tokio::test]
    async fn checkpoint_window_accounts_for_committed_suffix_on_restart() {
        let (tmp, _, writer) = source_journal("debug").await;
        let source = Translation::spawn(resume_options(tmp.path(), "debug", writer.clone()))
            .await
            .unwrap();
        let env = serde_json::from_value(serde_json::json!({"source":"debug","session_id":"checkpoint","event":"event","ts_ms":1,"payload":{}})).unwrap();
        let mut tail = 0;
        for _ in 0..2 * CHECKPOINT_EVENTS - 1 {
            tail = writer.lock().await.append(&env).await.unwrap();
        }
        source.advance(tail).await.unwrap();
        source.barrier().await.unwrap();
        let before = source
            .journal
            .translation_checkpoint()
            .await
            .unwrap()
            .unwrap()
            .input;
        assert!(before < tail);
        let task = source.task.lock().await.take().unwrap();
        task.abort();
        let _ = task.await;
        tail = writer.lock().await.append(&env).await.unwrap();
        let resumed = Translation::spawn(resume_options(tmp.path(), "debug", writer.clone()))
            .await
            .unwrap();
        resumed.advance(tail).await.unwrap();
        resumed.barrier().await.unwrap();
        assert_eq!(
            resumed
                .journal
                .translation_checkpoint()
                .await
                .unwrap()
                .unwrap()
                .input,
            tail,
            "the 63 reconstructed inputs must count toward the next periodic checkpoint"
        );
        resumed.shutdown().await;
    }

    #[tokio::test]
    async fn checkpoint_seeks_past_history_for_native_and_legacy_journals() {
        for format in ["debug", "pi", "legacy-pi"] {
            let source = if format == "legacy-pi" { "pi" } else { format };
            let (tmp, path, writer) = source_journal(source).await;
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
            let mut legacy = String::new();
            for i in 0..CHECKPOINT_EVENTS + 3 {
                env.ts_ms = i as i64;
                env.payload = serde_json::json!({"event":{"messages":[{"role":"user","content":"shared".repeat(1024)},{"role":"assistant","content":i}]}});
                if format == "legacy-pi" {
                    let mut event = env.redacted();
                    event.payload["event"]
                        .as_object_mut()
                        .unwrap()
                        .remove("messages");
                    let suffix = if i == 0 {
                        env.payload["event"]["messages"].clone()
                    } else {
                        serde_json::json!([{"role":"assistant","content":i}])
                    };
                    event.payload["event"]["_bt_messages_delta"] = serde_json::json!({"common_prefix": if i == 0 { 0 } else { 1 }, "suffix":suffix});
                    legacy.push_str(&serde_json::to_string(&event).unwrap());
                    legacy.push('\n');
                    boundary = legacy.len() as u64;
                } else {
                    boundary = writer.lock().await.append(&env).await.unwrap();
                }
            }
            if format == "legacy-pi" {
                tokio::fs::write(&path, legacy).await.unwrap();
                *writer.lock().await = JournalWriter::open_path(&path).await.unwrap();
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
            let saved = RecoveryContext::decode(&checkpoint, "test", boundary).unwrap();
            assert_eq!(
                saved.decoder.legacy_pi.is_empty(),
                format != "legacy-pi",
                "only legacy delta records should need a saved journal decoder base"
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
            let (tmp, _, writer) = source_journal("debug").await;
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
            input: SourceInput::default(),
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
            input: SourceInput::default(),
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
        let (tmp, _, writer) = source_journal("debug").await;
        let mut producer = Producer::open(resume_options(tmp.path(), "debug", writer))
            .await
            .unwrap();
        let journal = producer.journal.clone();
        let path = tmp.path().join("pipeline").join(format!(
            "{}.sqlite",
            crate::ids::session_storage_id("debug", "checkpoint")
        ));
        let db = rusqlite::Connection::open(path).unwrap();
        let drains = Arc::new(AtomicU64::new(0));
        producer.translator = Some(Box::new(CountedTranslation(drains.clone())));
        producer.publication = Some(Publication {
            input: 10,
            decoded_bytes: 0,
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
