//! One-shot import of the per-session journals released on `main`.
//!
//! Legacy journals are already complete session event history. For each
//! session the importer copies its events into the event journal, translates
//! them once to fill the span journal, and registers one branch per route. A
//! branch head is the first span node produced after the lowest delivery
//! checkpoint among the legacy routes it replaces, so work the old daemon
//! already delivered is not delivered again. It is idempotent: a partition
//! interrupted mid-import is discarded and imported again.

use super::runtime::{BranchRecord, Phase, Pipeline};
use super::store;
use super::translation::Engine;
use super::types::{
    now_ms, ConsumerId, JournalId, NodeId, PartitionId, RouteConfig, RouteKey, SpanEntry,
    TranslationInput,
};
use crate::journal::{JournalReader, JournalRecord};
use crate::wire::{RedactedEnvelope, SessionRoute};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const DONE: &str = "legacy_journals_imported";

fn partition_marker(partition: &PartitionId, state: &str) -> String {
    format!("legacy_import\u{1f}{state}\u{1f}{}", partition.key())
}

struct LegacySession {
    events: Vec<(u64, RedactedEnvelope)>,
    /// Highest delivered byte offset per legacy route.
    checkpoints: Vec<(SessionRoute, u64)>,
}

async fn read_legacy(
    pipeline: &Pipeline,
    path: &Path,
) -> anyhow::Result<HashMap<PartitionId, LegacySession>> {
    let recorded = JournalReader::recorded_len(path).await;
    let mut sessions: HashMap<PartitionId, LegacySession> = HashMap::new();
    let Some(mut reader) = JournalReader::open(path, recorded).await? else {
        return Ok(sessions);
    };
    let mut current: Option<PartitionId> = None;
    while let Some(entry) = reader.next_record().await? {
        match entry.record {
            JournalRecord::Event(mut env) => {
                let Some(source) = pipeline
                    .translators
                    .canonical_source(&env.source)
                    .map(str::to_owned)
                else {
                    continue;
                };
                if env.session_id.is_empty() {
                    continue;
                }
                env.source = source;
                let partition = PartitionId::new(&env.source, &env.session_id);
                current = Some(partition.clone());
                sessions
                    .entry(partition)
                    .or_insert_with(|| LegacySession {
                        events: Vec::new(),
                        checkpoints: Vec::new(),
                    })
                    .events
                    .push((entry.through, env));
            }
            JournalRecord::DeliveryCheckpoint { route, through } => {
                if let Some(partition) = &current {
                    if let Some(session) = sessions.get_mut(partition) {
                        session.checkpoints.push((route, through));
                    }
                }
            }
        }
    }
    Ok(sessions)
}

/// Import every legacy journal once, then delete the legacy files.
pub(crate) async fn import_legacy_journals(pipeline: &Arc<Pipeline>) -> anyhow::Result<()> {
    if pipeline
        .store
        .read(|conn| store::get_meta(conn, DONE))
        .await?
        .is_some()
    {
        return Ok(());
    }
    let dir = crate::journal::journal_dir(&pipeline.data_dir);
    let mut qualified = Vec::new();
    let mut unqualified = Vec::new();
    if let Ok(mut entries) = tokio::fs::read_dir(&dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("ndjson") {
                continue;
            }
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_default();
            if name.matches("--").count() >= 2 {
                qualified.push(path);
            } else {
                unqualified.push(path);
            }
        }
    }
    qualified.sort();
    unqualified.sort();
    let mut processed: Vec<PathBuf> = Vec::new();
    let mut failed = false;
    // A source-qualified journal is a superset of the session-only file it
    // was copied from, so it is preferred.
    for path in qualified.into_iter().chain(unqualified) {
        match import_file(pipeline, &path).await {
            Ok(()) => processed.push(path),
            Err(error) => {
                failed = true;
                tracing::warn!(path = %path.display(), %error, "legacy journal not imported");
            }
        }
    }
    if failed {
        anyhow::bail!("some legacy journals could not be imported");
    }
    pipeline
        .store
        .write(|tx| store::set_meta(tx, DONE, &now_ms().to_string()))
        .await?;
    for path in processed {
        let _ = tokio::fs::remove_file(&path).await;
    }
    Ok(())
}

async fn import_file(pipeline: &Arc<Pipeline>, path: &Path) -> anyhow::Result<()> {
    let sessions = read_legacy(pipeline, path).await?;
    for (partition, session) in sessions {
        let done = partition_marker(&partition, "done");
        let started = partition_marker(&partition, "started");
        let (is_done, is_started, exists) = {
            let (done, started, partition) = (done.clone(), started.clone(), partition.clone());
            pipeline
                .store
                .read(move |conn| {
                    Ok((
                        store::get_meta(conn, &done)?.is_some(),
                        store::get_meta(conn, &started)?.is_some(),
                        store::partition(conn, &partition)?.is_some(),
                    ))
                })
                .await?
        };
        if is_done || (exists && !is_started) {
            continue;
        }
        // Discard anything a previous interrupted import left behind.
        pipeline.events.delete(&partition)?;
        {
            let (partition, started) = (partition.clone(), started.clone());
            pipeline
                .store
                .write(move |tx| {
                    store::delete_partition(tx, &partition)?;
                    store::set_meta(tx, &started, "1")
                })
                .await?;
        }
        import_session(pipeline, &partition, session).await?;
        pipeline
            .store
            .write(move |tx| {
                store::set_meta(tx, &done, "1")?;
                tx.execute("DELETE FROM meta WHERE key = ?1", [&started])?;
                Ok(())
            })
            .await?;
    }
    Ok(())
}

async fn import_session(
    pipeline: &Arc<Pipeline>,
    partition: &PartitionId,
    session: LegacySession,
) -> anyhow::Result<()> {
    let namespace = if crate::journal::legacy_journal_has_session(
        &pipeline.data_dir,
        &partition.source,
        &partition.session_id,
    )
    .await
    {
        partition.session_id.clone()
    } else {
        crate::ids::session_namespace(&partition.source, &partition.session_id)
    };
    let mut engine = Engine::new(pipeline.translators.clone(), partition, &namespace)?;
    let mut entries: Vec<SpanEntry> = Vec::new();
    // For each legacy event: its byte offset and the next span node after it.
    let mut boundaries: Vec<(u64, NodeId)> = Vec::new();
    let mut routes: Vec<SessionRoute> = Vec::new();
    let mut last_node: NodeId = 0;
    for (through, env) in session.events {
        if let Some(route) = &env.route {
            if !routes.iter().any(|known| known.same_route(route)) {
                routes.push(route.clone());
            }
        }
        let input = TranslationInput::Native(Box::new(env));
        last_node = pipeline.events.append(partition, input.clone()).await?;
        match engine.apply(&input, true) {
            Ok(applied) => entries.extend(applied.entries),
            Err(error) => {
                // The old daemon skipped this event too; keep the history.
                tracing::warn!(%partition, %error, "legacy event not translated during import");
            }
        }
        boundaries.push((through, entries.len() as NodeId + 1));
    }
    for (route, _) in &session.checkpoints {
        if !routes.iter().any(|known| known.same_route(route)) {
            routes.push(route.clone());
        }
    }
    let mut branches: HashMap<String, (RouteKey, SessionRoute, NodeId)> = HashMap::new();
    for route in routes {
        let key = RouteKey::live(&route);
        let delivered = session
            .checkpoints
            .iter()
            .filter(|(candidate, _)| candidate.same_replay_route(&route))
            .map(|(_, through)| *through)
            .max()
            .unwrap_or(0);
        let head = boundaries
            .iter()
            .rev()
            .find(|(through, _)| *through <= delivered)
            .map(|(_, next)| *next)
            .unwrap_or(1);
        branches
            .entry(key.id())
            .and_modify(|(_, _, existing)| *existing = (*existing).min(head))
            .or_insert((key, route, head));
    }
    let partition = partition.clone();
    pipeline
        .store
        .write(move |tx| {
            store::ensure_partition(tx, &partition, &namespace, now_ms())?;
            let spans = JournalId::spans(&partition).key();
            let events = JournalId::events(&partition).key();
            store::append_span_entries(tx, &spans, &entries)?;
            store::advance_head(tx, &events, &ConsumerId::Translation.key(), last_node + 1)?;
            store::advance_head(tx, &events, "assembly", last_node + 1)?;
            for (_, (key, route, head)) in branches {
                let consumer = ConsumerId::branch(&key).key();
                store::register_head(tx, &spans, &consumer, head)?;
                let record = BranchRecord {
                    key: key.clone(),
                    config: RouteConfig::from_route(&route, 1),
                    phase: Phase::Live,
                    legacy_ledger_imported: false,
                    delivered_through: head.saturating_sub(1),
                };
                store::set_branch_state(
                    tx,
                    &partition.key(),
                    &key.id(),
                    &serde_json::to_string(&record)?,
                )?;
            }
            Pipeline::collect_spans(tx, &partition)
        })
        .await?;
    Ok(())
}
