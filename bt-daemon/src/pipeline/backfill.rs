//! Backfill producers: span history a branch needs before its live input.
//!
//! - A live route registered after span nodes were deleted is caught up by
//!   re-translating the event journal with live semantics, which (with an
//!   unchanged translator revision) reproduces exactly the deleted nodes.
//! - An import or attach translates the agent transcript.
//!
//! Both use the translation engine and hand off into the branch's backfill
//! journal with a receipt (inputs consumed) in the same transaction, then end
//! with `BackfillComplete`. A producer resumed after a crash rebuilds its
//! translator from scratch and suppresses output already handed off.

use super::recovery::{FailureCause, FailureRecord, RecoveryCondition};
use super::runtime::{BackfillRecord, BackfillSource, BranchRuntime, PartitionRuntime, Pipeline};
use super::store;
use super::translation::{persistence_failure, Engine};
use super::types::{
    new_barrier_id, FinalizeKind, JournalId, SpanEntry, TranslationControl, TranslationInput,
};
use std::sync::Arc;

pub(crate) fn ensure_producer(
    pipeline: &Arc<Pipeline>,
    runtime: &Arc<PartitionRuntime>,
    branch: &Arc<BranchRuntime>,
) {
    let mut task = branch
        .producer
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    if task.as_ref().is_some_and(|task| !task.is_finished()) {
        return;
    }
    *task = Some(tokio::spawn(run(
        pipeline.clone(),
        runtime.clone(),
        branch.clone(),
    )));
}

struct Producer {
    pipeline: Arc<Pipeline>,
    runtime: Arc<PartitionRuntime>,
    branch: Arc<BranchRuntime>,
    jkey: String,
    consumed: u64,
    /// Inputs processed in this run, including suppressed ones.
    seen: u64,
}

impl Producer {
    /// Hand off output for the inputs processed since the last handoff.
    async fn handoff(&mut self, entries: Vec<SpanEntry>, complete: bool) -> anyhow::Result<()> {
        if self.seen <= self.consumed && !complete {
            return Ok(());
        }
        let pkey = self.runtime.partition.key();
        let route_id = self.branch.key.id();
        let jkey = self.jkey.clone();
        let consumer = producer_consumer(&self.branch.key);
        let consumed = self.seen;
        let range = self
            .pipeline
            .store
            .write(move |tx| {
                let mut record: BackfillRecord = serde_json::from_str(
                    &store::backfill_state(tx, &pkey, &route_id)?
                        .ok_or_else(|| anyhow::anyhow!("backfill state missing"))?,
                )?;
                let mut entries = entries;
                if complete {
                    entries.push(SpanEntry::BackfillComplete);
                }
                let range = store::append_span_entries(tx, &jkey, &entries)?;
                record.consumed = consumed;
                record.complete = complete;
                store::clear_failure(tx, &jkey, &consumer)?;
                store::set_backfill_state(tx, &pkey, &route_id, &serde_json::to_string(&record)?)?;
                Ok(range)
            })
            .await?;
        self.consumed = consumed;
        self.branch.view.send_modify(|view| {
            if view.recovering.as_ref().is_some_and(|record| {
                record.failure.cause.label() == "translation"
                    || matches!(record.failure.cause, FailureCause::TranscriptRead { .. })
            }) {
                view.recovering = None;
            }
        });
        if let Some((_, last)) = range {
            self.branch.backfill_changed.send_replace(last);
        }
        Ok(())
    }

    /// Process one input, keeping output only beyond the receipt.
    fn keep(&mut self) -> bool {
        self.seen += 1;
        self.seen > self.consumed
    }
}

pub(crate) fn producer_consumer(route: &super::types::RouteKey) -> String {
    format!("producer\u{1f}{}", route.id())
}

async fn run(pipeline: Arc<Pipeline>, runtime: Arc<PartitionRuntime>, branch: Arc<BranchRuntime>) {
    let partition = runtime.partition.clone();
    let mut stop = runtime.stop.subscribe();
    let failure_jkey = JournalId::backfill(&partition, &branch.key).key();
    let failure_consumer = producer_consumer(&branch.key);
    // Resume a persisted producer failure before retrying.
    let persisted = {
        let jkey = failure_jkey.clone();
        let consumer = failure_consumer.clone();
        pipeline
            .store
            .read(move |conn| store::failure(conn, &jkey, &consumer))
            .await
            .ok()
            .flatten()
            .and_then(|text| FailureRecord::decode(&text).ok())
    };
    if let Some(record) = persisted {
        branch
            .view
            .send_modify(|view| view.recovering = Some(record.clone()));
        let context = BackfillContext {
            revision: &pipeline.version,
        };
        tokio::select! {
            _ = super::recovery::wait_until_eligible(&record, &context, &branch.wake) => {}
            _ = stop.wait_for(|stop| *stop) => return,
        }
    }
    loop {
        let record = {
            let pkey = partition.key();
            let route_id = branch.key.id();
            pipeline
                .store
                .read(move |conn| store::backfill_state(conn, &pkey, &route_id))
                .await
        };
        let record = match record {
            Ok(Some(text)) => match serde_json::from_str::<BackfillRecord>(&text) {
                Ok(record) => record,
                Err(error) => {
                    tracing::warn!(%partition, %error, "unreadable backfill state");
                    return;
                }
            },
            Ok(None) => return,
            Err(error) => {
                tracing::warn!(%partition, %error, "backfill state unavailable");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                continue;
            }
        };
        if record.complete {
            return;
        }
        let mut producer = Producer {
            pipeline: pipeline.clone(),
            runtime: runtime.clone(),
            branch: branch.clone(),
            jkey: JournalId::backfill(&partition, &branch.key).key(),
            consumed: record.consumed,
            seen: 0,
        };
        let result = match &record.source {
            BackfillSource::Events { receipt, .. } => from_events(&mut producer, *receipt).await,
            BackfillSource::Transcript {
                path,
                source,
                attach,
            } => from_transcript(&mut producer, path.clone(), *source, *attach, &mut stop).await,
        };
        match result {
            Ok(()) => return,
            Err(failure) => {
                let failure = *failure;
                let record =
                    FailureRecord::record(branch.view.borrow().recovering.as_ref(), failure);
                tracing::warn!(%partition, failure = %record.summary(), "backfill recovering");
                // The producer has its own failure slot so it never clears or
                // overwrites the branch's delivery failure.
                let jkey = JournalId::backfill(&partition, &branch.key).key();
                let consumer = producer_consumer(&branch.key);
                let encoded = record.encode();
                let _ = pipeline
                    .store
                    .write(move |tx| store::set_failure(tx, &jkey, &consumer, &encoded))
                    .await;
                branch
                    .view
                    .send_modify(|view| view.recovering = Some(record.clone()));
                let context = BackfillContext {
                    revision: &pipeline.version,
                };
                tokio::select! {
                    _ = super::recovery::wait_until_eligible(&record, &context, &branch.wake) => {}
                    _ = stop.wait_for(|stop| *stop) => return,
                }
            }
        }
    }
}

struct BackfillContext<'a> {
    revision: &'a str,
}

impl super::recovery::RecoveryContext for BackfillContext<'_> {
    fn translator_revision(&self) -> &str {
        self.revision
    }
}

fn translation_failure(
    producer: &Producer,
    revision: &str,
    error: &anyhow::Error,
) -> Box<super::recovery::Failure> {
    let source = &producer.runtime.partition.source;
    Box::new(super::recovery::Failure {
        cause: FailureCause::TranslationShape {
            source: source.clone(),
            node: producer.seen,
            revision: revision.to_string(),
        },
        diagnostic: format!("{error:#}"),
        guidance: format!(
            "the {source} translator could not interpret backfilled record {}; upgrading `bt` \
             to a version with a translator fix retries it automatically",
            producer.seen
        ),
        automatic: false,
        conditions: vec![RecoveryCondition::TranslatorRevision {
            failed: revision.to_string(),
        }],
    })
}

type Failed = Box<super::recovery::Failure>;

/// Re-translate events with live semantics, including the translator
/// rebuild that follows a retirement finalize.
async fn from_events(producer: &mut Producer, receipt: u64) -> Result<(), Failed> {
    let pipeline = producer.pipeline.clone();
    let partition = producer.runtime.partition.clone();
    let revision = pipeline.version.clone();
    let mut engine = Engine::new(
        pipeline.translators.clone(),
        &partition,
        &producer.runtime.namespace,
    )
    .map_err(|error| translation_failure(producer, &revision, &error))?;
    let mut cursor = pipeline.events.cursor(&partition);
    let inputs = tokio::task::spawn_blocking(move || {
        let mut inputs = Vec::new();
        while (inputs.len() as u64) < receipt {
            match cursor.read_one()? {
                Some((_, input)) => inputs.push(input),
                None => break,
            }
        }
        anyhow::Ok(inputs)
    })
    .await
    .map_err(|error| {
        Box::new(persistence_failure(
            "read event journal",
            &anyhow::anyhow!("{error}"),
        ))
    })?
    .map_err(|error| Box::new(persistence_failure("read event journal", &error)))?;
    let mut history: Vec<TranslationInput> = Vec::new();
    for input in inputs {
        let keep = producer.keep();
        let applied = engine
            .apply(&input, false)
            .map_err(|error| translation_failure(producer, &revision, &error))?;
        history.push(input);
        if keep {
            producer
                .handoff(applied.entries, false)
                .await
                .map_err(|error| Box::new(persistence_failure("commit backfill", &error)))?;
        }
        if applied.rebuild {
            engine
                .reset()
                .map_err(|error| translation_failure(producer, &revision, &error))?;
            for input in &history {
                engine
                    .apply(input, true)
                    .map_err(|error| translation_failure(producer, &revision, &error))?;
            }
        }
    }
    producer
        .handoff(Vec::new(), true)
        .await
        .map_err(|error| Box::new(persistence_failure("complete backfill", &error)))
}

async fn from_transcript(
    producer: &mut Producer,
    path: std::path::PathBuf,
    source: crate::ImportSource,
    attach: bool,
    stop: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<(), Failed> {
    let pipeline = producer.pipeline.clone();
    let partition = producer.runtime.partition.clone();
    let revision = pipeline.version.clone();
    let mut engine = Engine::new(
        pipeline.translators.clone(),
        &partition,
        &producer.runtime.namespace,
    )
    .map_err(|error| translation_failure(producer, &revision, &error))?;
    let mut tail = crate::transcript_import::TranscriptTail::new(path.clone(), source);
    if attach {
        tail.allow_incomplete_final_record_on_shutdown();
    }
    let mut finish = producer.branch.finish_backfill.subscribe();
    let mut finalizing = !attach;
    loop {
        let entries = tail.poll(finalizing).map_err(|error| {
            Box::new(super::recovery::Failure {
                cause: FailureCause::TranscriptRead { path: path.clone() },
                diagnostic: format!("{error:#}"),
                guidance: format!(
                    "restore or fix permissions on the transcript {}; the import resumes \
                     automatically",
                    path.display()
                ),
                automatic: true,
                conditions: vec![
                    RecoveryCondition::FileChanged {
                        path: path.clone(),
                        digest: super::recovery::file_digest(&path),
                    },
                    RecoveryCondition::Backoff,
                ],
            })
        })?;
        if tail.take_translator_reset() {
            engine
                .reset()
                .map_err(|error| translation_failure(producer, &revision, &error))?;
        }
        let mut output = Vec::new();
        for env in entries {
            // A transcript may interleave sessions; each partition reads its own.
            if env.session_id != partition.session_id {
                continue;
            }
            let keep = producer.keep();
            let input = TranslationInput::Native(Box::new(env.redacted()));
            let applied = engine
                .apply(&input, false)
                .map_err(|error| translation_failure(producer, &revision, &error))?;
            if keep {
                output.extend(applied.entries);
            }
        }
        if finalizing {
            let keep = producer.keep();
            let applied = engine
                .apply(
                    &TranslationInput::Control(TranslationControl::Finalize {
                        barrier: new_barrier_id(),
                        kind: FinalizeKind::Terminal,
                    }),
                    false,
                )
                .map_err(|error| translation_failure(producer, &revision, &error))?;
            if keep {
                output.extend(applied.entries);
            }
            producer
                .handoff(output, true)
                .await
                .map_err(|error| Box::new(persistence_failure("complete backfill", &error)))?;
            return Ok(());
        }
        producer
            .handoff(output, false)
            .await
            .map_err(|error| Box::new(persistence_failure("commit backfill", &error)))?;
        tokio::select! {
            _ = finish.wait_for(|finish| *finish) => finalizing = true,
            _ = stop.wait_for(|stop| *stop) => finalizing = true,
            _ = tokio::time::sleep(std::time::Duration::from_millis(500)) => {}
        }
    }
}
