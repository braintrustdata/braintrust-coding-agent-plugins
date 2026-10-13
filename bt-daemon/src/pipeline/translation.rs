//! One ordered source consumer. Output publication is independent of route delivery.
use super::journal::SpanJournal;
use super::prepare::Prepare;
use super::{
    recovery::{Boundary, Failure, Recovery, RecoveryOutcome},
    Stage,
};
use crate::{
    journal::{ControlKind, JournalReader, JournalRecord, JournalWriter, PipelineControl},
    translate::{AgentTranslator, OriginSnapshot, Registry, SessionCtx, SpanOp},
    wire::{SessionConfig, SessionRoute},
};
use parking_lot::Mutex;
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
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

pub(crate) struct Translation {
    tx: mpsc::Sender<Command>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    pub journal: SpanJournal,
    pub error: Arc<Mutex<Option<String>>>,
    pub recovery: Arc<Mutex<Option<crate::wire::RecoveryInfo>>>,
    pub routes: AtomicUsize,
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
            origins: HashMap::new(),
            capture: None,
            pending_snapshots: HashSet::new(),
            bounds: HashMap::new(),
            position: 0,
            committed,
            state,
            publication: None,
            phase: Boundary::Journal,
        };
        let task = tokio::spawn(actor.run(rx));
        Ok(Arc::new(Self {
            tx,
            task: Mutex::new(Some(task)),
            journal,
            error,
            recovery,
            routes: AtomicUsize::new(0),
            progress,
        }))
    }
    pub async fn shutdown(&self) {
        let _ = self.tx.send(Command::Stop).await;
        let task = self.task.lock().take();
        if let Some(task) = task {
            let _ = task.await;
        }
    }
    pub async fn observe(&self, observer: Observation) -> anyhow::Result<()> {
        self.routes.fetch_add(1, Ordering::Relaxed);
        self.tx
            .send(Command::Observe(Box::new(observer)))
            .await
            .map_err(|_| anyhow::anyhow!("translation actor is gone"))
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
}

enum State {
    Processing,
    Recovering(Failure),
}
struct Publication {
    input: u64,
    observation: bool,
    capture: Option<OriginSnapshot>,
    ops: Option<Vec<SpanOp>>,
    begun: bool,
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
    origins: HashMap<String, OriginSnapshot>,
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
            Failure::storage(&error, position, &self.options.version, attempt)
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
        // Load creation provenance so an upgrade cannot restamp old spans.
        let mut after = 0;
        loop {
            let rows = self.journal.read_after(after, 128).await?;
            if rows.is_empty() {
                break;
            }
            for (position, op) in rows {
                after = position;
                let (SpanOp::Insert(row) | SpanOp::Merge(row)) = op;
                if let Some(origin) = row.origin {
                    self.origins.entry(row.span_id).or_insert(origin);
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
                observation,
                capture,
                ops: Some(first),
                begun: false,
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
                    for op in ops.iter_mut() {
                        let insert = matches!(op, SpanOp::Insert(_));
                        let (SpanOp::Insert(row) | SpanOp::Merge(row)) = op;
                        if insert {
                            self.origins.entry(row.span_id.clone()).or_insert_with(|| {
                                pending.capture.clone().unwrap_or_else(|| OriginSnapshot {
                                    plugin_version: None,
                                    source_version: None,
                                    bt_version: self.options.version.clone(),
                                })
                            });
                        }
                        row.origin = self.origins.get(&row.span_id).cloned();
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
                        self.journal.append(pending.input, ops).await?;
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
            }
            if publish {
                self.phase = Boundary::Journal;
                self.journal.commit(pending.input).await?;
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
        let mut after = 0;
        loop {
            let rows = self.journal.read_after(after, 128).await?;
            let Some(&(through, _)) = rows.last() else {
                break;
            };
            after = through;
            let ops = rows.into_iter().map(|(_, op)| op).collect::<Vec<_>>();
            Self::observe(&self.options, &mut self.pending_snapshots, &ops, observer).await?;
        }
        Ok(())
    }
    async fn observe(
        options: &TranslationOptions,
        pending_snapshots: &mut HashSet<String>,
        ops: &[SpanOp],
        observer: &Observation,
    ) -> anyhow::Result<()> {
        let mut ops = ops.to_vec();
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
            origins: HashMap::new(),
            capture: None,
            pending_snapshots: HashSet::new(),
            bounds: HashMap::new(),
            position: 0,
            committed: 0,
            state: State::Processing,
            phase: Boundary::Journal,
            publication: Some(Publication {
                input: 10,
                observation: true,
                capture: None,
                ops: Some(vec![SpanOp::Insert(SpanRow {
                    span_id: "built".into(),
                    ..Default::default()
                })]),
                begun: false,
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
        assert_eq!(journal.tail().await.unwrap(), 0);
        db.execute_batch("DROP TRIGGER fail_commit").unwrap();
        producer.publish().await.unwrap();
        assert_eq!(drains.load(Ordering::Relaxed), 1);
        assert_eq!(journal.translated_through().await.unwrap(), 10);
        assert_eq!(journal.tail().await.unwrap(), 1);
    }
}
