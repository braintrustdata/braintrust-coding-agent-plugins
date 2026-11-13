//! Startup recovery of journaled events the previous daemon did not finish.

use super::*;

pub(super) async fn discover_unprocessed_journals(daemon: &Arc<Daemon>) -> Vec<PendingEvent> {
    let pending: Vec<PendingEvent> = daemon
        .pending_sessions
        .lock()
        .values()
        .flat_map(|state| state.events.iter().cloned())
        .collect();
    let Ok(sessions) = crate::collections::sessions(&daemon.data_dir).await else {
        return Vec::new();
    };
    let mut candidates = Vec::new();
    for storage in sessions {
        let Ok(Some((source, session_id))) = (match storage.events().await {
            Ok(events) => events.owner().await,
            Err(error) => {
                tracing::warn!(%error, path = %storage.path.display(), "session storage cannot be recovered");
                continue;
            }
        }) else {
            continue;
        };
        let Ok(events) = daemon.events_for(&source, &session_id).await else {
            continue;
        };
        let Ok(tail) = events.tail().await else {
            continue;
        };
        let namespace = crate::ids::session_namespace(&source, &session_id);
        let recorded_len = daemon
            .startup_cutoffs
            .lock()
            .as_ref()
            .and_then(|cutoffs| cutoffs.get(&namespace).copied())
            .map_or(tail, |cutoff| tail.min(cutoff));
        if let Some(cutoffs) = daemon.startup_cutoffs.lock().as_mut() {
            cutoffs.entry(namespace).or_insert(recorded_len);
        }
        let mut reader = events.read(0, recorded_len);
        let mut before = 0u64;
        let mut latest_by_route: HashMap<String, PendingEvent> = HashMap::new();
        if let Ok(Some(template)) = events.identity().await {
            let env = journal::envelope_from_redacted(template);
            daemon.remember_claude_transcripts(&env);
            if let Some(route) = &env.route {
                latest_by_route.insert(
                    serde_json::to_string(route).unwrap_or_default(),
                    PendingEvent {
                        env,
                        replay_through: recorded_len,
                        journal_through: recorded_len,
                        settings_through: Some(0),
                    },
                );
            }
        }
        while let Ok(Some(entry)) = reader.next().await {
            let through = entry.id;
            match entry.record {
                EventRecord::Event(redacted) => {
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
                                settings_through: Some(through),
                            },
                        );
                    }
                }
                EventRecord::Control(_) => {}
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
            let Ok(journal) = crate::collections::SpanOps::for_events(&events).await else {
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
                    .unwrap_or_else(|| {
                        let mut event = template.clone();
                        // A dormant head has no captured settings correction.
                        event.settings_through = Some(0);
                        event
                    });
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
            let completed = {
                match (
                    crate::collections::SpanOps::for_events(&events).await,
                    recovered_route.as_ref(),
                ) {
                    (Ok(journal), Some(route)) => {
                        let routes = journal.routes().await.unwrap_or_default();
                        if routes.iter().any(|head| head.same_route(route)) {
                            journal
                                .route_complete(route, recorded_len)
                                .await
                                .unwrap_or(false)
                        } else {
                            // Only migrated input an earlier daemon delivered
                            // is translated before any route subscribes.
                            routes.is_empty()
                                && journal.translated_through().await.unwrap_or(0)
                                    >= event.journal_through
                        }
                    }
                    _ => false,
                }
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
    if recovered > 0 {
        tracing::info!(
            recovered,
            "queued unprocessed journal sessions for recovery"
        );
    }
    candidates
}

#[cfg(test)]
pub(super) async fn recover_unprocessed_journals(daemon: &Arc<Daemon>) {
    for event in discover_unprocessed_journals(daemon).await {
        dispatch_ingress_event(daemon, event).await;
    }
    let (reply, _ignored) = oneshot::channel();
    let _ = daemon.ingress_tx.send(IngressMsg::Barrier(reply)).await;
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::pipeline::journal::Cursor;
    use crate::translate::{SpanOp, SpanRow};

    pub(crate) struct TestAuth;
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
        let writer = crate::collections::EventCollection::open(tmp.path(), "debug", "test")
            .await
            .unwrap();
        let through = writer
            .append((&env).into(), None)
            .await
            .unwrap()
            .unwrap()
            .through;
        let spans = crate::collections::SpanOps::for_events(&writer)
            .await
            .unwrap();
        drop(writer);
        spans
            .translated(
                through,
                vec![SpanOp::Insert(SpanRow {
                    span_id: "stored".into(),
                    name: "stored".into(),
                    root_span_id: "stored".into(),
                    end_ms: Some(1),
                    ..Default::default()
                })],
            )
            .await
            .unwrap();
        Cursor::open(&spans, &route).await.unwrap();
        let mut dormant = route.clone();
        dormant.tags.insert("dormant".into());
        Cursor::open(&spans, &dormant).await.unwrap();

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
                    settings_through: None,
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
    #[tokio::test]
    async fn startup_preserves_corrected_settings_before_a_held_head_completes() {
        use crate::collections::SpanOps;
        use crate::pipeline::{journal::Cursor, recovery::Failure};
        let tmp = tempfile::tempdir().unwrap();
        let original = SessionRoute {
            destination: Some(crate::wire::TraceDestination::ProjectLogs {
                project_id: Some("test-project".into()),
                project_name: None,
            }),
            ..Default::default()
        };
        let mut corrected = original.clone();
        corrected.tags.insert("corrected".into());
        let mut env: Envelope = serde_json::from_value(serde_json::json!({
            "source":"debug", "session_id":"restart-correction", "event":"SessionStart", "ts_ms":1,
            "payload":{}, "route":original
        }))
        .unwrap();
        let writer =
            crate::collections::EventCollection::open(tmp.path(), "debug", &env.session_id)
                .await
                .unwrap();
        let first = writer
            .append((&env).into(), None)
            .await
            .unwrap()
            .unwrap()
            .through;
        env.route = Some(corrected.clone());
        env.event = "Stop".into();
        env.ts_ms = 2;
        let settings_through = writer
            .append((&env).into(), None)
            .await
            .unwrap()
            .unwrap()
            .through;
        let tail = writer
            .append(
                crate::collections::EventRecord::Control(crate::pipeline::input::PipelineControl {
                    kind: crate::pipeline::input::ControlKind::Checkpoint,
                    bounds: HashMap::new(),
                }),
                Some(settings_through),
            )
            .await
            .unwrap()
            .unwrap()
            .through;
        let path = crate::collections::inspect::session_path(tmp.path(), "debug", &env.session_id);
        let spans = SpanOps::for_events(&writer).await.unwrap();
        drop(writer);
        spans
            .translated(
                first,
                vec![SpanOp::Insert(SpanRow {
                    span_id: "stored".into(),
                    name: "stored".into(),
                    root_span_id: "stored".into(),
                    end_ms: Some(1),
                    ..Default::default()
                })],
            )
            .await
            .unwrap();
        Cursor::open(&spans, &original).await.unwrap();
        let lease = TestAuth
            .resolve(&original.auth, AuthResolveReason::Initial)
            .await
            .unwrap();
        let failure = Failure::legacy("rejected row".into(), 1, &original.with_auth(lease.auth));
        spans
            .set_failure(
                &serde_json::to_string(&original).unwrap(),
                Some(serde_json::to_string(&failure).unwrap()),
            )
            .await
            .unwrap();
        crate::pipeline::journal::reconfigure_held(&spans, &corrected, settings_through)
            .await
            .unwrap();
        // Crash window: the correction is durable, but no consumer has retried it.
        drop(spans);
        let mut options = debug_serve_options("test", tmp.path());
        options.auth_provider = Some(Arc::new(TestAuth));
        let daemon = Daemon::new(options, tmp.path().into());
        recover_unprocessed_journals(&daemon).await;
        daemon.ingress_barrier().await;
        let restored = SpanOps::open(&path).await.unwrap();
        assert_eq!(
            restored.route_settings(&original).await.unwrap().tags,
            corrected.tags
        );
        tokio::time::timeout(Duration::from_secs(5), async {
            while !restored.route_complete(&original, tail).await.unwrap() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        super::super::maintenance::drain_all(&daemon).await;
    }
    #[tokio::test]
    async fn established_linked_handoff_retains_its_route_while_recovering() {
        let tmp = tempfile::tempdir().unwrap();
        let original = SessionRoute {
            destination: Some(crate::wire::TraceDestination::ProjectLogs {
                project_id: Some("original-project".into()),
                project_name: None,
            }),
            ..Default::default()
        };
        let linked = SessionRoute {
            destination: Some(crate::wire::TraceDestination::ProjectLogs {
                project_id: Some("linked-project".into()),
                project_name: None,
            }),
            ..Default::default()
        };
        let env: Envelope = serde_json::from_value(serde_json::json!({
            "source":"debug", "session_id":"linked-retry", "event":"Stop", "ts_ms":1,
            "payload":{}, "route":original
        }))
        .unwrap();
        let writer =
            crate::collections::EventCollection::open(tmp.path(), "debug", &env.session_id)
                .await
                .unwrap();
        let through = writer
            .append((&env).into(), None)
            .await
            .unwrap()
            .unwrap()
            .through;
        drop(writer);
        let mut options = debug_serve_options("test", tmp.path());
        options.auth_provider = Some(Arc::new(TestAuth));
        let daemon = Daemon::new(options, tmp.path().into());
        let key = automatic_link_key(&env);
        daemon
            .automatic_links
            .lock()
            .insert(key.clone(), linked.clone());
        let storage =
            crate::collections::inspect::Inspector::session(tmp.path(), "debug", &env.session_id)
                .unwrap();
        storage.set_schema_version(999).unwrap();
        assert!(accept_event(
            &daemon,
            PendingEvent {
                env,
                replay_through: 0,
                journal_through: through,
                settings_through: None,
            }
        )
        .await
        .is_err());
        let saved = read_correlation_state(tmp.path(), &key).await.unwrap();
        assert!(saved.linked_route.as_ref().unwrap().same_route(&linked));
        assert_eq!(saved.recovery.unwrap().info(false).stage, "journal");
        assert_eq!(daemon.pending_sessions.lock()[&key].events.len(), 1);
        storage.restore_schema_version().unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let _ = retry_pending_sessions(&daemon).await;
                if !daemon.pending_sessions.lock().contains_key(&key) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        daemon.ingress_barrier().await;
        let sessions = daemon.status(StatusParams::default()).sessions;
        assert_eq!(sessions.len(), 1);
        assert!(sessions[0].route.as_ref().unwrap().same_route(&linked));
        assert!(sessions[0].recovery.is_none());
        assert!(sessions[0].last_error.is_none());
        super::super::maintenance::drain_all(&daemon).await;
    }
}
