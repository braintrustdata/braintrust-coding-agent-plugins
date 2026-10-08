//! Startup recovery of journaled events the previous daemon did not finish.

use super::*;

pub(super) async fn recover_unprocessed_journals(daemon: &Arc<Daemon>) {
    let pending: HashSet<(String, String, u64)> = daemon
        .pending_sessions
        .lock()
        .unwrap()
        .values()
        .flat_map(|state| {
            state.events.iter().map(|event| {
                (
                    event.env.source.clone(),
                    event.env.session_id.clone(),
                    event.journal_through,
                )
            })
        })
        .collect();
    let Ok(mut entries) = tokio::fs::read_dir(journal::journal_dir(&daemon.data_dir)).await else {
        return;
    };
    let mut candidates = Vec::new();
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("ndjson") {
            continue;
        }
        let recorded_len = journal::JournalReader::recorded_len(&path).await;
        let Ok(Some(mut reader)) = journal::JournalReader::open(&path, recorded_len).await else {
            continue;
        };
        let mut before = 0u64;
        let mut acknowledged_by_route: HashMap<String, u64> = HashMap::new();
        let mut latest_by_route: HashMap<String, PendingEvent> = HashMap::new();
        while let Ok(Some(entry)) = reader.next_record().await {
            let through = entry.through;
            match entry.record {
                journal::JournalRecord::Event(redacted) => {
                    let mut env = journal::envelope_from_redacted(redacted);
                    let Some(source) = daemon.translators.canonical_source(&env.source) else {
                        before = through;
                        continue;
                    };
                    env.source = source.to_string();
                    daemon.remember_claude_transcripts(&env);
                    if env.source == "claude-code" {
                        if let Some(payload) = env.payload.as_object_mut() {
                            payload.insert(
                                "_bt_transcript_replay".to_string(),
                                serde_json::json!(true),
                            );
                        }
                    }
                    if let Some(route) = env.route.as_ref() {
                        let key = serde_json::to_string(route).unwrap_or_default();
                        latest_by_route.insert(
                            key,
                            PendingEvent {
                                env,
                                replay_through: before,
                                journal_through: through,
                            },
                        );
                    }
                }
                journal::JournalRecord::DeliveryCheckpoint { route, through } => {
                    let key = serde_json::to_string(&route).unwrap_or_default();
                    let acknowledged = acknowledged_by_route.entry(key).or_default();
                    *acknowledged = (*acknowledged).max(through);
                }
            }
            before = through;
        }
        // The capture catalog survives input collection and retains only routing/recovery metadata.
        if let Ok(routes) = journal::captured_routes(&path) {
            for captured in routes {
                let env = journal::envelope_from_redacted(captured.envelope);
                if let Some(route) = env.route.as_ref() {
                    let key = serde_json::to_string(route).unwrap_or_default();
                    latest_by_route.entry(key).or_insert(PendingEvent {
                        env,
                        replay_through: recorded_len,
                        journal_through: captured.through,
                    });
                }
            }
        }
        for (route, through) in journal::read_checkpoint_state(&path).await {
            let key = serde_json::to_string(&route).unwrap_or_default();
            let acknowledged = acknowledged_by_route.entry(key).or_default();
            *acknowledged = (*acknowledged).max(through);
        }
        for event in latest_by_route.into_values() {
            if pending.contains(&(
                event.env.source.clone(),
                event.env.session_id.clone(),
                event.journal_through,
            )) {
                continue;
            }
            let route = recovered_delivery_route(daemon, &event.env)
                .await
                .as_ref()
                .and_then(|route| serde_json::to_string(route).ok())
                .unwrap_or_default();
            let output_pending = event.env.route.as_ref().is_some_and(|route| {
                crate::derived::pending_output(
                    &daemon.data_dir,
                    &event.env.source,
                    &event.env.session_id,
                    route,
                )
                .unwrap_or(true)
            });
            if output_pending
                || acknowledged_by_route.get(&route).copied().unwrap_or(0) < event.journal_through
            {
                candidates.push(event);
            } else if event.env.source == "claude-code" {
                // A fully delivered historical session has no actor to retire
                // its observer. Probe once for bytes appended after the final
                // checkpoint, retaining only paths that have new work.
                if let Some(key) = event.env.route.as_ref().and_then(|route| {
                    DeliveryKey::new(&event.env.source, &event.env.session_id, route).ok()
                }) {
                    let observation = daemon.claude_transcripts.lock().unwrap().get(&key).cloned();
                    if let Some(observation) = observation {
                        let path = observation.template.payload["transcript_path"]
                            .as_str()
                            .unwrap();
                        let changed = tokio::fs::metadata(path)
                            .await
                            .is_ok_and(|metadata| observation.through != Some(metadata.len()));
                        if !changed {
                            daemon.claude_transcripts.lock().unwrap().remove(&key);
                        }
                    }
                }
            }
        }
    }
    candidates.sort_by_key(|event| event.env.ts_ms);
    let recovered = candidates.len();
    for event in candidates {
        let _ = daemon
            .ingress_tx
            .send(IngressMsg::Event(Box::new(event)))
            .await;
    }
    // Reconciliation stays in the daemon worker and follows every recovered
    // event in queue order. Dropping the receiver is intentional: startup and
    // hook capture do not wait for translation or reporting.
    let (reply, _ignored) = oneshot::channel();
    let _ = daemon.ingress_tx.send(IngressMsg::Barrier(reply)).await;
    if recovered > 0 {
        tracing::info!(
            recovered,
            "queued unprocessed journal sessions for recovery"
        );
    }
}
