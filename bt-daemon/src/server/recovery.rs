//! Startup recovery of journaled events the previous daemon did not finish.

use super::*;

pub(super) async fn recover_unprocessed_journals(daemon: &Arc<Daemon>) {
    let pending: Vec<PendingEvent> = daemon
        .pending_sessions
        .lock()
        .values()
        .flat_map(|state| state.events.iter().cloned())
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
                journal::JournalRecord::Control(_) => {}
                journal::JournalRecord::DeliveryCheckpoint { route, through } => {
                    let key = serde_json::to_string(&route).unwrap_or_default();
                    let acknowledged = acknowledged_by_route.entry(key).or_default();
                    *acknowledged = (*acknowledged).max(through);
                }
            }
            before = through;
        }
        // Heads are the authoritative subscriptions, including dormant routes
        // and canonical profile IDs absent from older captured envelopes.
        let templates: Vec<_> = latest_by_route.values().cloned().collect();
        let mut sources = HashSet::new();
        for template in templates {
            if !sources.insert((template.env.source.clone(), template.env.session_id.clone())) {
                continue;
            }
            let span_path = daemon.data_dir.join("pipeline").join(format!(
                "{}.sqlite",
                crate::ids::session_storage_id(&template.env.source, &template.env.session_id)
            ));
            if !tokio::fs::try_exists(&span_path).await.unwrap_or(false) {
                continue;
            }
            let Ok(journal) = crate::pipeline::journal::SpanJournal::open(&span_path).await else {
                continue;
            };
            let Ok(routes) = journal.routes().await else {
                continue;
            };
            for route in routes {
                let route_key = match serde_json::to_string(&route) {
                    Ok(key) => key,
                    Err(error) => {
                        tracing::warn!(%error, "durable route cannot be encoded for recovery");
                        continue;
                    }
                };
                let matching = latest_by_route
                    .iter()
                    .find(|(_, event)| {
                        event.env.source == template.env.source
                            && event.env.session_id == template.env.session_id
                            && event
                                .env
                                .route
                                .as_ref()
                                .is_some_and(|recorded| recorded.same_route(&route))
                    })
                    .map(|(key, _)| key.clone());
                let mut event = matching
                    .and_then(|key| latest_by_route.remove(&key))
                    .unwrap_or_else(|| template.clone());
                event.env.route = Some(route.clone());
                latest_by_route.insert(route_key, event);
            }
        }
        for mut event in latest_by_route.into_values() {
            if pending.iter().any(|pending| {
                pending.env.source == event.env.source
                    && pending.env.session_id == event.env.session_id
                    && pending.journal_through == event.journal_through
                    && match (&pending.env.route, &event.env.route) {
                        (Some(a), Some(b)) => a.same_route(b),
                        (None, None) => true,
                        _ => false,
                    }
            }) {
                continue;
            }
            let recovered_route = recovered_delivery_route(daemon, &event.env).await;
            let route = recovered_route
                .as_ref()
                .and_then(|route| serde_json::to_string(route).ok())
                .unwrap_or_default();
            let span_path = daemon.data_dir.join("pipeline").join(format!(
                "{}.sqlite",
                crate::ids::session_storage_id(&event.env.source, &event.env.session_id)
            ));
            let completed = if tokio::fs::try_exists(&span_path).await.unwrap_or(false) {
                match (
                    crate::pipeline::journal::SpanJournal::open(&span_path).await,
                    recovered_route.as_ref(),
                ) {
                    (Ok(journal), Some(route)) => journal
                        .route_complete(route, recorded_len)
                        .await
                        .unwrap_or(false),
                    _ => false,
                }
            } else {
                acknowledged_by_route.get(&route).copied().unwrap_or(0) >= event.journal_through
            };
            if !completed {
                // Ordered controls after the final native event also belong to recovery.
                event.replay_through = recorded_len;
                event.journal_through = recorded_len;
                candidates.push(event);
            } else if event.env.source == "claude-code" {
                // A fully delivered historical session has no actor to retire
                // its observer. Probe once for bytes appended after the final
                // checkpoint, retaining only paths that have new work.
                if let Some(key) = event.env.route.as_ref().and_then(|route| {
                    DeliveryKey::new(&event.env.source, &event.env.session_id, route).ok()
                }) {
                    let observation = daemon.claude_transcripts.lock().get(&key).cloned();
                    if let Some(observation) = observation {
                        let Some(path) = observation
                            .template
                            .payload
                            .get("transcript_path")
                            .and_then(Value::as_str)
                        else {
                            tracing::warn!(session_id = %key.session_id, "Claude transcript observation has no transcript path");
                            continue;
                        };
                        let changed = tokio::fs::metadata(path)
                            .await
                            .is_ok_and(|metadata| observation.through != Some(metadata.len()));
                        if !changed {
                            daemon.claude_transcripts.lock().remove(&key);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::translate::{SpanOp, SpanRow};

    struct TestAuth;
    #[async_trait::async_trait]
    impl AuthProvider for TestAuth {
        async fn resolve(
            &self,
            selection: &AuthSelection,
            _: AuthResolveReason,
        ) -> anyhow::Result<AuthLease> {
            Ok(AuthLease {
                selection: selection.clone(),
                auth: BackendAuth {
                    token: "test".into(),
                    api_url: None,
                    app_url: None,
                    org_id: None,
                    org_name: None,
                },
                expires_at_ms: None,
            })
        }
    }

    #[tokio::test]
    async fn pending_capture_does_not_suppress_an_unrelated_durable_head() {
        let tmp = tempfile::tempdir().unwrap();
        let route = SessionRoute {
            destination: Some(crate::wire::TraceDestination::ProjectLogs {
                project_id: Some("project".into()),
                project_name: None,
            }),
            ..Default::default()
        };
        let env: Envelope = serde_json::from_value(serde_json::json!({
            "source":"debug", "session_id":"test", "event":"event", "ts_ms":1,
            "payload":{}, "route":route,
        }))
        .unwrap();
        let raw = journal::source_journal_path(tmp.path(), "debug", "test");
        let mut writer = JournalWriter::open_path(&raw).await.unwrap();
        let through = writer.append(&env).await.unwrap();
        drop(writer);
        let path = tmp.path().join("pipeline").join(format!(
            "{}.sqlite",
            crate::ids::session_storage_id("debug", "test"),
        ));
        let spans = crate::pipeline::journal::SpanJournal::open(&path)
            .await
            .unwrap();
        spans.begin(through, true).await.unwrap();
        spans
            .append(
                through,
                &[SpanOp::Insert(SpanRow {
                    span_id: "stored".into(),
                    name: "stored".into(),
                    root_span_id: "stored".into(),
                    end_ms: Some(1),
                    ..Default::default()
                })],
            )
            .await
            .unwrap();
        spans.commit(through).await.unwrap();
        spans.subscribe(&route, 0).await.unwrap();
        let mut dormant = route.clone();
        dormant.tags.push("dormant".into());
        spans.subscribe(&dormant, 0).await.unwrap();

        let mut options = debug_serve_options("test", tmp.path());
        options.auth_provider = Some(Arc::new(TestAuth));
        let daemon = Daemon::new(options, tmp.path().into());
        let pending_key = automatic_link_key(&env);
        daemon.pending_sessions.lock().insert(
            pending_key.clone(),
            PendingSession {
                deadline: Some(Instant::now() + Duration::from_secs(60)),
                events: vec![PendingEvent {
                    env,
                    replay_through: 0,
                    journal_through: through,
                }],
                ..Default::default()
            },
        );
        recover_unprocessed_journals(&daemon).await;
        daemon.ingress_barrier().await;
        tokio::time::timeout(Duration::from_secs(5), async {
            while !spans.route_complete(&dormant, through).await.unwrap() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(!spans.route_complete(&route, through).await.unwrap());
        assert_eq!(daemon.pending_sessions.lock()[&pending_key].events.len(), 1);
        assert!(daemon
            .sessions
            .lock()
            .contains_key(&DeliveryKey::new("debug", "test", &dormant).unwrap()));
        super::super::maintenance::drain_all(&daemon).await;
    }
}
