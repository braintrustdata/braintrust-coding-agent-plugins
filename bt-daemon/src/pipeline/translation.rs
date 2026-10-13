//! One ordered source consumer. Output publication is independent of route delivery.
use super::journal::SpanJournal;
use super::prepare::Prepare;
use crate::{
    journal::{ControlKind, JournalReader, JournalRecord, JournalWriter, PipelineControl},
    translate::{AgentTranslator, OriginSnapshot, Registry, SessionCtx, SpanOp, SpanType},
    wire::{SessionConfig, SessionRoute},
};
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
use tokio::sync::{mpsc, oneshot, Notify};

pub(crate) struct Observation {
    pub key: String,
    pub route: SessionRoute,
    pub config: SessionConfig,
}

enum Command {
    Observe(Box<Observation>),
    Advance(u64),
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
        let error = Arc::new(Mutex::new(None));
        let (tx, rx) = mpsc::channel(64);
        let actor = Producer {
            options,
            progress: progress.clone(),
            journal: journal.clone(),
            error: error.clone(),
            translator: None,
            reader: None,
            observers: Vec::new(),
            origins: HashMap::new(),
            bounds: HashMap::new(),
            position: 0,
            committed,
            held: false,
        };
        let task = tokio::spawn(actor.run(rx));
        Ok(Arc::new(Self {
            tx,
            task: Mutex::new(Some(task)),
            journal,
            error,
            routes: AtomicUsize::new(0),
            progress,
        }))
    }
    pub async fn shutdown(&self) {
        let _ = self.tx.send(Command::Stop).await;
        let task = self.task.lock().unwrap().take();
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

struct Producer {
    options: TranslationOptions,
    progress: Arc<Progress>,
    journal: SpanJournal,
    error: Arc<Mutex<Option<String>>>,
    translator: Option<Box<dyn AgentTranslator>>,
    reader: Option<JournalReader>,
    observers: Vec<Observation>,
    origins: HashMap<String, OriginSnapshot>,
    bounds: HashMap<String, u64>,
    position: u64,
    committed: u64,
    held: bool,
}

impl Producer {
    async fn run(mut self, mut rx: mpsc::Receiver<Command>) {
        while let Some(command) = rx.recv().await {
            let result = match command {
                Command::Stop => break,
                Command::Observe(observer) => {
                    let result = self.observe_history(&observer).await;
                    self.observers.push(*observer);
                    result
                }
                Command::Advance(through) => self.advance(through).await,
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
                self.held = true;
                *self.error.lock().unwrap() =
                    Some(format!("translation held at {}: {error:#}", self.position));
            }
        }
    }

    async fn boundary(&mut self) -> Result<u64, String> {
        if self.held {
            return Err(self
                .error
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_else(|| "translation held".into()));
        }
        self.journal.tail().await.map_err(|error| error.to_string())
    }

    async fn control(&mut self, kind: ControlKind) -> anyhow::Result<u64> {
        anyhow::ensure!(!self.held, "translation is held");
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
        self.advance(through).await?;
        self.journal.tail().await
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
        anyhow::ensure!(!self.held, "translation is held");
        if self.translator.is_none() && through <= self.committed {
            return Ok(());
        }
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
        while let Some(entry) = self
            .reader
            .as_mut()
            .expect("reader initialized")
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
                    (
                        self.translator
                            .as_mut()
                            .expect("translator initialized")
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
                    let translator = self.translator.as_mut().expect("translator initialized");
                    let ops = if !matches!(control.kind, ControlKind::Checkpoint) {
                        translator.finalize_at(&ctx, &control.bounds)?
                    } else {
                        translator.checkpoint_at(&ctx, &control.bounds)?
                    };
                    (ops, None)
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
            let publish = input > self.committed;
            if publish {
                self.journal.begin(input, observation).await?;
            }
            let mut next = Some(first);
            while let Some(mut ops) = next {
                for op in &mut ops {
                    let insert = matches!(op, SpanOp::Insert(_));
                    let (SpanOp::Insert(row) | SpanOp::Merge(row)) = op;
                    if insert
                        && (row.is_turn
                            || (row.span_type == SpanType::Task && row.parent_span_ids.is_empty()))
                    {
                        if let Some(capture) = &capture {
                            self.origins
                                .entry(row.span_id.clone())
                                .or_insert_with(|| capture.clone());
                        }
                    }
                    row.origin = self.origins.get(&row.span_id).cloned();
                }
                if observation {
                    for observer in &self.observers {
                        Self::observe(&self.options, &ops, observer).await?;
                    }
                }
                if publish {
                    self.journal.append(input, &ops).await?;
                }
                next = self
                    .translator
                    .as_mut()
                    .expect("translator initialized")
                    .drain_pending(&ctx)?;
            }
            if publish {
                self.journal.commit(input).await?;
                self.committed = input;
                self.progress.committed.store(input, Ordering::Relaxed);
            }
            self.position = input;
        }
        Ok(())
    }

    async fn observe_history(&mut self, observer: &Observation) -> anyhow::Result<()> {
        let mut after = 0;
        loop {
            let rows = self.journal.read_after(after, 128).await?;
            if rows.is_empty() {
                break;
            }
            after = rows.last().expect("nonempty").0;
            let ops = rows.into_iter().map(|(_, op)| op).collect::<Vec<_>>();
            Self::observe(&self.options, &ops, observer).await?;
        }
        Ok(())
    }
    async fn observe(
        options: &TranslationOptions,
        ops: &[SpanOp],
        observer: &Observation,
    ) -> anyhow::Result<()> {
        let mut ops = ops.to_vec();
        Prepare::new(&options.source).apply(&mut ops, &observer.config);
        if options
            .correlation
            .observe_ops(&observer.key, &observer.route, &observer.config, &ops)
        {
            crate::server::persist_active_parent_snapshot(
                &options.data_dir,
                &observer.key,
                &options.correlation,
            )
            .await
            .map_err(anyhow::Error::msg)?;
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
        assert!(translation.error.lock().unwrap().is_none());
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
}
