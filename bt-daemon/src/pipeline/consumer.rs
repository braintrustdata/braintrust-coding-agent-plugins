//! A routing subscription owns pending work independently of the source producer.
use super::{
    journal::{Cursor, SpanJournal},
    routing::RoutingSink,
    Delivery,
};
use crate::{
    delivery_ledger::LedgerSink,
    dispatch::Counters,
    sink::{Sink, SinkFactory},
    wire::SessionConfig,
};
use std::{
    path::PathBuf,
    sync::{atomic::Ordering, Arc, Mutex},
};
use tokio::sync::{watch, Notify};

pub(crate) struct Consumer {
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    config: watch::Sender<SessionConfig>,
    progress: watch::Receiver<u64>,
    stop: Arc<Notify>,
    error: Arc<Mutex<Option<String>>>,
}

pub(crate) struct ConsumerOptions {
    pub source: String,
    pub session_id: String,
    pub version: String,
    pub data_dir: PathBuf,
    pub config: SessionConfig,
    pub counters: Arc<Counters>,
    pub error: Arc<Mutex<Option<String>>>,
    pub permalink: Arc<Mutex<Option<String>>>,
    pub factory: Arc<dyn SinkFactory>,
}

impl Consumer {
    pub fn spawn(journal: SpanJournal, mut cursor: Cursor, options: ConsumerOptions) -> Arc<Self> {
        let (config, mut settings) = watch::channel(options.config.clone());
        let (progress, receive) = watch::channel(cursor.position());
        let stop = Arc::new(Notify::new());
        let handle = Arc::new(Self {
            task: Mutex::new(None),
            config,
            progress: receive,
            stop: stop.clone(),
            error: options.error.clone(),
        });
        let task = tokio::spawn(async move {
            let task = async {
                let sink = options
                    .factory
                    .create(&options.session_id, &options.source)?;
                let sink = Box::new(
                    LedgerSink::new(
                        sink,
                        Some(&options.data_dir),
                        &options.source,
                        &options.session_id,
                        Some(&options.config),
                        &options.version,
                    )
                    .await,
                );
                let mut sink = RoutingSink::spawn(
                    sink,
                    options.source.clone(),
                    options.session_id.clone(),
                    options.data_dir.clone(),
                    options.error.clone(),
                );
                let mut held = false;
                if let Some(failure) = journal.failure(&cursor.route).await? {
                    *options.error.lock().unwrap() = Some(failure);
                }
                let mut completion: Option<Delivery<()>> = None;
                loop {
                    if held {
                        tokio::select! {
                            _=stop.notified()=>return Ok::<(),anyhow::Error>(()),
                            result=settings.changed()=> {
                                if result.is_err() { return Ok(()); }
                                if let Some(pending)=completion.take() {
                                    match pending.into_completed().await {
                                        Ok(())=> { cursor.rewind().await?; progress.send_replace(cursor.completed_position()); },
                                        Err(failure)=> { completion=Some(failure.delivery); continue; },
                                    }
                                }
                                held=false;
                                cursor.rewind().await?;
                            }
                        }
                        continue;
                    }
                    let tail = journal.tail().await?;
                    options.counters.queued.store(
                        tail.saturating_sub(cursor.completed_position()),
                        Ordering::Relaxed,
                    );
                    let received = tokio::select! {
                        _=stop.notified()=>return Ok(()),
                        result=super::delivery::InputPort::receive(&mut cursor)=>result?,
                        result=settings.changed()=> { if result.is_err() { return Ok(()); } continue; },
                    };
                    match received {
                        Some(delivery) => {
                            sink.configure(&settings.borrow_and_update());
                            let ops = delivery.value();
                            let attempt = async {
                                let count = sink.emit(ops).await?;
                                sink.flush().await?;
                                Ok::<_, anyhow::Error>(count)
                            };
                            let result = tokio::select! { _=stop.notified()=>return Ok(()),result=attempt=>result };
                            match result {
                                Ok(count) => {
                                    options
                                        .counters
                                        .spans_emitted
                                        .fetch_add(count, Ordering::Relaxed);
                                    *options.permalink.lock().unwrap() = sink.permalink();
                                    // The journal retains a deferred prefix. No payloads or token list
                                    // need accumulate in memory while waiting for its releasing child.
                                    drop(delivery);
                                    if !sink.has_pending_delivery() {
                                        match cursor.pending_prefix().into_completed().await {
                                            Ok(()) => {
                                                cursor.rewind().await?;
                                                progress.send_replace(cursor.completed_position());
                                                *options.error.lock().unwrap() = None;
                                            }
                                            Err(failure) => {
                                                *options.error.lock().unwrap() = Some(format!(
                                                    "route completion held: {}",
                                                    failure.error
                                                ));
                                                completion = Some(failure.delivery);
                                                held = true;
                                                progress.send_replace(cursor.completed_position());
                                            }
                                        }
                                    }
                                }
                                Err(error) => {
                                    if let Some(failure) =
                                        error.downcast_ref::<super::routing::EmitFailure>()
                                    {
                                        if failure.report {
                                            tracing::warn!(session_id=%options.session_id, %error, "route processing failed");
                                        }
                                        options
                                            .counters
                                            .spans_emitted
                                            .fetch_add(failure.emitted, Ordering::Relaxed);
                                    }
                                    *options.error.lock().unwrap() =
                                        Some(format!("routing held: {error:#}"));
                                    let failure = options.error.lock().unwrap().clone();
                                    journal.set_failure(&cursor.route, failure).await?;
                                    held = true;
                                    progress.send_replace(cursor.completed_position());
                                }
                            }
                        }
                        None => tokio::select! {
                            _=stop.notified()=>return Ok(()),
                            _=cursor.changed()=>{},
                            result=settings.changed()=>if result.is_err() { return Ok(()); },
                        },
                    }
                }
            };
            if let Err(error) = task.await {
                *options.error.lock().unwrap() = Some(format!("routing journal held: {error:#}"));
                progress.send_replace(cursor.completed_position());
            }
        });
        *handle.task.lock().unwrap() = Some(task);
        handle
    }
    pub fn configure(&self, config: SessionConfig) {
        self.config.send_if_modified(|current| {
            if serde_json::to_value(&*current).ok() == serde_json::to_value(&config).ok() {
                return false;
            }
            *current = config;
            true
        });
    }
    pub async fn shutdown(&self) {
        self.stop.notify_one();
        let task = self.task.lock().unwrap().take();
        if let Some(task) = task {
            let _ = task.await;
        }
    }
    pub async fn through(&self, tail: u64) -> Result<(), String> {
        let mut progress = self.progress.clone();
        loop {
            if *progress.borrow_and_update() >= tail {
                return Ok(());
            }
            if let Some(error) = self.error.lock().unwrap().clone() {
                return Err(error);
            }
            progress
                .changed()
                .await
                .map_err(|_| "routing actor is gone".to_owned())?;
        }
    }
}
