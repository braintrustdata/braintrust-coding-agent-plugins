//! The translation machine: one per partition (and one per backfill).
//!
//! Translators are disposable. Whenever one must be rebuilt (startup,
//! retirement, recovery), a fresh translator replays the partition's events up
//! to the receipt with output discarded, because that output is already in the
//! span journal. Each later input is handed off on its own: span nodes and the
//! receipt commit in one SQLite transaction.

use super::journal::{EventReader, Journal};
use super::recovery::{
    wait_until_eligible, Failure, FailureCause, FailureRecord, RecoveryCondition, RecoveryContext,
};
use super::runtime::{PartitionRuntime, Pipeline};
use super::store;
use super::types::{
    CaptureVersions, ConsumerId, FinalizeKind, JournalId, JournalSpanOp, NodeId, PartitionId,
    RouteConfig, RouteKey, SpanEntry, TranslationControl, TranslationInput,
};
use crate::translate::{AgentTranslator, Registry, SessionCtx, SpanOp};
use crate::wire::{BackendAuth, Envelope, RedactedEnvelope};
use std::sync::Arc;
use tokio_stream::StreamExt;

/// The result of applying one input.
pub(crate) struct Applied {
    pub(crate) entries: Vec<SpanEntry>,
    /// Ops as emitted, for correlation observation.
    pub(crate) ops: Vec<SpanOp>,
    /// A retirement finalize: the translator must be discarded afterwards.
    pub(crate) rebuild: bool,
}

/// Live translation semantics shared by the partition machine and backfills.
pub(crate) struct Engine {
    translators: Arc<Registry>,
    source: String,
    namespace: String,
    translator: Box<dyn AgentTranslator>,
    ctx: SessionCtx,
    capture: CaptureVersions,
}

impl Engine {
    pub(crate) fn new(
        translators: Arc<Registry>,
        partition: &PartitionId,
        namespace: &str,
    ) -> anyhow::Result<Self> {
        let translator =
            translators.create_checked_with_session_namespace(&partition.source, namespace)?;
        Ok(Self {
            translators,
            source: partition.source.clone(),
            namespace: namespace.to_string(),
            translator,
            ctx: SessionCtx {
                session_id: partition.session_id.clone(),
                config: None,
            },
            capture: CaptureVersions::default(),
        })
    }

    /// Discard the translator and start over from an empty state.
    pub(crate) fn reset(&mut self) -> anyhow::Result<()> {
        self.translator = self
            .translators
            .create_checked_with_session_namespace(&self.source, &self.namespace)?;
        self.capture = CaptureVersions::default();
        Ok(())
    }

    fn drain(&mut self, first: Vec<SpanOp>) -> anyhow::Result<Vec<SpanOp>> {
        let mut ops = first;
        while let Some(more) = self.translator.drain_pending(&self.ctx)? {
            ops.extend(more);
        }
        Ok(ops)
    }

    /// Translate one native envelope as live translation does.
    pub(crate) fn native(
        &mut self,
        mut env: Envelope,
        replay: bool,
    ) -> anyhow::Result<Vec<SpanOp>> {
        if env.source == "claude-code" {
            // Recovery must read the bounded mirrors recorded at capture, never
            // the mutable external transcript. Fresh captures carry mirrors.
            let mark = replay || env.payload.get("_bt_claude_transcript_mirrors").is_none();
            if mark {
                if let Some(payload) = env.payload.as_object_mut() {
                    payload.insert("_bt_transcript_replay".to_string(), serde_json::json!(true));
                }
            }
        }
        self.capture = CaptureVersions {
            plugin_version: env.plugin_version.clone(),
            source_version: env.source_version.clone(),
        };
        let first = self.translator.handle(&env, &self.ctx)?;
        self.drain(first)
    }

    fn entries(&self, ops: &[SpanOp]) -> Vec<SpanEntry> {
        ops.iter()
            .map(|op| {
                let (merge, row) = match op {
                    SpanOp::Insert(row) => (false, row.clone()),
                    SpanOp::Merge(row) => (true, row.clone()),
                };
                SpanEntry::Op(Box::new(JournalSpanOp {
                    merge,
                    row,
                    capture: self.capture.clone(),
                }))
            })
            .collect()
    }

    /// Apply one input. During replay, retirement finalizes are skipped:
    /// the live translator that saw them was discarded right afterwards.
    pub(crate) fn apply(
        &mut self,
        input: &TranslationInput,
        replay: bool,
    ) -> anyhow::Result<Applied> {
        match input {
            TranslationInput::Native(env) => {
                let ops = self.native(envelope(env.as_ref().clone()), replay)?;
                Ok(Applied {
                    entries: self.entries(&ops),
                    ops,
                    rebuild: false,
                })
            }
            TranslationInput::Control(TranslationControl::Checkpoint { barrier }) => {
                let first = self.translator.checkpoint(&self.ctx)?;
                let ops = self.drain(first)?;
                let mut entries = self.entries(&ops);
                entries.push(SpanEntry::Barrier {
                    id: barrier.clone(),
                    finalize: false,
                });
                Ok(Applied {
                    entries,
                    ops,
                    rebuild: false,
                })
            }
            TranslationInput::Control(TranslationControl::Finalize { barrier, kind }) => {
                if replay && *kind == FinalizeKind::Retire {
                    return Ok(Applied {
                        entries: Vec::new(),
                        ops: Vec::new(),
                        rebuild: false,
                    });
                }
                let first = self.translator.finalize(&self.ctx)?;
                let ops = self.drain(first)?;
                let mut entries = self.entries(&ops);
                entries.push(SpanEntry::Barrier {
                    id: barrier.clone(),
                    finalize: *kind == FinalizeKind::Terminal,
                });
                Ok(Applied {
                    entries,
                    ops,
                    rebuild: *kind == FinalizeKind::Retire,
                })
            }
        }
    }
}

/// Rebuild a translator-usable envelope from its journal form. Credentials
/// were never journaled, and translators never read route configuration.
pub(crate) fn envelope(redacted: RedactedEnvelope) -> Envelope {
    let mut env = crate::journal::envelope_from_redacted(redacted);
    env.config = None;
    env
}

struct Context<'a> {
    revision: &'a str,
}

impl RecoveryContext for Context<'_> {
    fn translator_revision(&self) -> &str {
        self.revision
    }
}

fn translation_failure(
    source: &str,
    node: NodeId,
    revision: &str,
    error: &anyhow::Error,
) -> Failure {
    Failure {
        cause: FailureCause::TranslationShape {
            source: source.to_string(),
            node,
            revision: revision.to_string(),
        },
        diagnostic: format!("{error:#}"),
        guidance: format!(
            "the {source} translator could not interpret event {node}; later events are still \
             captured. Upgrading `bt` to a version with a translator fix retries it automatically. \
             Please report this with `bt trace doctor` output."
        ),
        automatic: false,
        conditions: vec![RecoveryCondition::TranslatorRevision {
            failed: revision.to_string(),
        }],
    }
}

pub(crate) fn persistence_failure(operation: &str, error: &anyhow::Error) -> Failure {
    Failure {
        cause: FailureCause::LocalPersistence {
            operation: operation.to_string(),
        },
        diagnostic: format!("{error:#}"),
        guidance: "free disk space or fix permissions on the daemon data directory; this \
                   retries automatically"
            .into(),
        automatic: true,
        conditions: vec![RecoveryCondition::Backoff],
    }
}

/// Observe committed output for correlation, once per live route.
pub(crate) async fn observe_correlation(
    pipeline: &Pipeline,
    runtime: &PartitionRuntime,
    ops: &[SpanOp],
    routes: &[(RouteKey, RouteConfig)],
) {
    let Some(correlation) = &pipeline.correlation else {
        return;
    };
    if ops.is_empty() {
        return;
    }
    for (key, config) in routes {
        let route = config.session_route(key);
        let session_config = route.with_auth(BackendAuth {
            token: String::new(),
            api_url: None,
            app_url: None,
            org_name: route.auth.org_name.clone(),
            org_id: None,
        });
        let overlay = crate::translate::RouteOverlay::from_config(&session_config);
        let routed = overlay.apply_all(ops.to_vec());
        let correlation_key = correlation_key(&runtime.partition, key);
        if correlation.observe_ops(&correlation_key, &route, &session_config, &routed) {
            if let Err(error) = crate::server::persist_active_parent_snapshot(
                &pipeline.data_dir,
                &correlation_key,
                correlation,
            )
            .await
            {
                tracing::warn!(partition = %runtime.partition, %error, "active parent snapshot failed");
            }
        }
    }
}

/// The correlation registry key of one delivery route of a session. Stable
/// for the branch's lifetime, so process and span observations always meet.
pub(crate) fn correlation_key(partition: &PartitionId, route: &RouteKey) -> String {
    format!(
        "{}\u{1f}{}\u{1f}{}",
        partition.source,
        partition.session_id,
        route.id()
    )
}

async fn record_failure(
    pipeline: &Pipeline,
    runtime: &PartitionRuntime,
    failure: Failure,
) -> FailureRecord {
    let jkey = JournalId::events(&runtime.partition).key();
    let consumer = ConsumerId::Translation.key();
    let previous = runtime.translation.borrow().recovering.clone();
    let record = FailureRecord::record(previous.as_ref(), failure);
    let encoded = record.encode();
    let result = pipeline
        .store
        .write(move |tx| store::set_failure(tx, &jkey, &consumer, &encoded))
        .await;
    if let Err(error) = result {
        tracing::warn!(partition = %runtime.partition, %error, "translation failure record not persisted");
    }
    tracing::warn!(partition = %runtime.partition, failure = %record.summary(), "translation recovering");
    runtime
        .translation
        .send_modify(|view| view.recovering = Some(record.clone()));
    record
}

/// Run a partition's translation machine until the partition is retired.
pub(crate) async fn run(pipeline: Arc<Pipeline>, runtime: Arc<PartitionRuntime>) {
    let partition = runtime.partition.clone();
    let events_jkey = JournalId::events(&partition).key();
    let consumer = ConsumerId::Translation.key();
    let mut stop = runtime.stop.subscribe();
    loop {
        // Recovering: a persisted failure waits for its own condition.
        let persisted = {
            let jkey = events_jkey.clone();
            let consumer = consumer.clone();
            pipeline
                .store
                .read(move |conn| store::failure(conn, &jkey, &consumer))
                .await
        };
        let record = match persisted {
            Ok(Some(text)) => FailureRecord::decode(&text).ok(),
            Ok(None) => None,
            Err(error) => {
                tracing::warn!(%partition, %error, "translation failure state unreadable");
                None
            }
        };
        if let Some(record) = record {
            runtime
                .translation
                .send_modify(|view| view.recovering = Some(record.clone()));
            let context = Context {
                revision: &pipeline.version,
            };
            tokio::select! {
                _ = wait_until_eligible(&record, &context, &runtime.translation_wake) => {}
                _ = stop.wait_for(|stop| *stop) => return,
            }
        }

        match run_attempt(&pipeline, &runtime, &mut stop).await {
            Ok(()) => return,
            Err(Attempt::Retry) => continue,
            Err(Attempt::Failed(failure)) => {
                record_failure(&pipeline, &runtime, failure).await;
            }
        }
    }
}

enum Attempt {
    /// Rebuild the translator (retirement finalize) and continue.
    Retry,
    Failed(Failure),
}

async fn run_attempt(
    pipeline: &Arc<Pipeline>,
    runtime: &Arc<PartitionRuntime>,
    stop: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<(), Attempt> {
    let partition = &runtime.partition;
    let revision = pipeline.version.clone();
    let mut engine = Engine::new(pipeline.translators.clone(), partition, &runtime.namespace)
        .map_err(|error| {
            Attempt::Failed(translation_failure(&partition.source, 0, &revision, &error))
        })?;
    let events_jkey = JournalId::events(partition).key();
    let spans_jkey = JournalId::spans(partition).key();
    let consumer = ConsumerId::Translation.key();
    let receipt = {
        let jkey = events_jkey.clone();
        let consumer = consumer.clone();
        pipeline
            .store
            .read(move |conn| Ok(store::head(conn, &jkey, &consumer)?.unwrap_or(1) - 1))
            .await
            .map_err(|error| {
                Attempt::Failed(persistence_failure("read translation receipt", &error))
            })?
    };
    let changed = pipeline
        .events
        .subscribe(partition)
        .map_err(|error| Attempt::Failed(persistence_failure("open event journal", &error)))?;
    let mut journal = Journal::new(
        JournalId::events(partition),
        ConsumerId::Translation,
        Box::new(EventReader {
            cursor: pipeline.events.cursor(partition),
        }),
        changed,
        1,
    );

    // Rebuild translator state from already handed-off events.
    let routes = runtime.live_routes();
    while journal.position() <= receipt {
        let pending = match journal.next_now().await {
            Ok(Some(pending)) => pending,
            Ok(None) => break,
            Err(error) => {
                return Err(Attempt::Failed(persistence_failure(
                    "read event journal",
                    &error,
                )));
            }
        };
        match engine.apply(&pending.value, true) {
            Ok(applied) => {
                observe_correlation(pipeline, runtime, &applied.ops, &routes).await;
            }
            Err(error) => {
                return Err(Attempt::Failed(translation_failure(
                    &partition.source,
                    pending.node,
                    &revision,
                    &error,
                )));
            }
        }
    }
    runtime.translation.send_modify(|view| {
        view.receipt = receipt;
        view.recovering = None;
    });

    let mut assembled = runtime.assembled.subscribe();
    loop {
        if runtime
            .reobserve
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(Attempt::Retry);
        }
        let next = tokio::select! {
            next = journal.next() => next,
            _ = runtime.reobserve_wake.notified() => continue,
            _ = stop.wait_for(|stop| *stop) => {
                // Retire only when caught up; pending input stays durable.
                return Ok(());
            }
        };
        let Some(next) = next else {
            return Ok(());
        };
        let pending = next
            .map_err(|error| Attempt::Failed(persistence_failure("read event journal", &error)))?;
        if matches!(pending.value, TranslationInput::Native(_)) {
            // Correlation observes this event's output under the routes
            // assembly registers for it; wait for that registration.
            tokio::select! {
                result = assembled.wait_for(|assembled| *assembled >= pending.node) => {
                    if result.is_err() {
                        return Ok(());
                    }
                }
                _ = stop.wait_for(|stop| *stop) => return Ok(()),
            }
        }
        runtime.touch();
        let applied = engine.apply(&pending.value, false).map_err(|error| {
            Attempt::Failed(translation_failure(
                &partition.source,
                pending.node,
                &revision,
                &error,
            ))
        })?;
        let barriers: Vec<String> = applied
            .entries
            .iter()
            .filter_map(|entry| match entry {
                SpanEntry::Barrier { id, .. } => Some(id.clone()),
                _ => None,
            })
            .collect();
        let entries = applied.entries;
        let node = pending.node;
        let spans = spans_jkey.clone();
        let events = events_jkey.clone();
        let translation = consumer.clone();
        let range = pipeline
            .store
            .write(move |tx| {
                let range = store::append_span_entries(tx, &spans, &entries)?;
                store::advance_head(tx, &events, &translation, node + 1)?;
                store::clear_failure(tx, &events, &translation)?;
                Ok(range)
            })
            .await
            .map_err(|error| {
                Attempt::Failed(persistence_failure("commit translated spans", &error))
            })?;
        if let Some((_, last)) = range {
            runtime.spans_changed.send_replace(last);
            if !barriers.is_empty() {
                // A barrier is the last entry of its handoff.
                runtime.barriers.send_modify(|positions| {
                    for barrier in barriers {
                        positions.insert(barrier, last);
                    }
                });
            }
        }
        runtime.translation.send_modify(|view| {
            view.receipt = node;
            view.recovering = None;
        });
        runtime.retired.store(
            matches!(
                pending.value,
                TranslationInput::Control(TranslationControl::Finalize {
                    kind: FinalizeKind::Retire,
                    ..
                })
            ),
            std::sync::atomic::Ordering::SeqCst,
        );
        let routes = runtime.live_routes();
        observe_correlation(pipeline, runtime, &applied.ops, &routes).await;
        if applied.rebuild {
            return Err(Attempt::Retry);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn native(event: &str, ts_ms: i64) -> TranslationInput {
        TranslationInput::Native(Box::new(RedactedEnvelope {
            source: "debug".into(),
            source_version: Some("1".into()),
            plugin_version: Some("2".into()),
            session_id: "s".into(),
            event: event.into(),
            ts_ms,
            managed_run_id: None,
            capture: None,
            payload: serde_json::json!({}),
            route: None,
        }))
    }

    #[test]
    fn replay_skips_retirement_and_stamps_capture_versions() {
        let registry = Arc::new(Registry::default_agents());
        let partition = PartitionId::new("debug", "s");
        let mut engine = Engine::new(registry, &partition, "ns").unwrap();
        let applied = engine.apply(&native("a", 1), false).unwrap();
        let SpanEntry::Op(op) = &applied.entries[0] else {
            panic!("expected an op");
        };
        assert_eq!(op.capture.source_version.as_deref(), Some("1"));
        let retire = TranslationInput::Control(TranslationControl::Finalize {
            barrier: "b".into(),
            kind: FinalizeKind::Retire,
        });
        let replayed = engine.apply(&retire, true).unwrap();
        assert!(replayed.entries.is_empty() && !replayed.rebuild);
        let live = engine.apply(&retire, false).unwrap();
        assert!(live.rebuild);
        assert!(matches!(
            live.entries.last(),
            Some(SpanEntry::Barrier {
                finalize: false,
                ..
            })
        ));
    }
}
