//! Managed-run bookkeeping: which sessions a managed child produced, and
//! flushing all of them when the run exits.

use super::*;

impl Daemon {
    pub(super) fn record_managed_run_session(
        &self,
        managed_run_id: &str,
        key: &DeliveryKey,
    ) -> bool {
        self.managed_run_sessions
            .lock()
            .unwrap()
            .entry(managed_run_id.to_string())
            .or_default()
            .insert(key.clone())
    }

    pub(super) async fn persist_managed_run_session(
        &self,
        managed_run_id: &str,
        key: &DeliveryKey,
        route: &SessionRoute,
    ) {
        if !self.record_managed_run_session(managed_run_id, key) {
            return;
        }
        let record = journal::ManagedRunKey {
            source: Some(key.source.clone()),
            session_id: key.session_id.clone(),
            route: route.clone(),
        };
        if let Err(error) =
            journal::append_managed_run_key(&self.data_dir, managed_run_id, &record).await
        {
            tracing::warn!(
                managed_run_id,
                session_id = %key.session_id,
                %error,
                "failed to record managed run delivery pipeline"
            );
        }
    }

    pub(super) async fn flush_managed_run(&self, params: ManagedRunFlushParams) -> FlushResult {
        let mut delivery_keys: HashSet<DeliveryKey> = self
            .managed_run_sessions
            .lock()
            .unwrap()
            .get(&params.managed_run_id)
            .cloned()
            .unwrap_or_default();
        // A daemon restart or idle exit loses the in-memory mapping; the
        // persisted record keeps flush accounting accurate for runs whose
        // events were accepted by an earlier daemon generation.
        for record in journal::read_managed_run_keys(&self.data_dir, &params.managed_run_id).await {
            if let Some(source) = record.source {
                if let Ok(key) = DeliveryKey::new(&source, &record.session_id, &record.route) {
                    delivery_keys.insert(key);
                }
            } else {
                // Legacy records predate source-qualified delivery keys. A
                // matching live pipeline is authoritative; otherwise recover
                // the source from the legacy journal.
                let live: Vec<_> = self
                    .sessions
                    .lock()
                    .unwrap()
                    .keys()
                    .filter(|key| {
                        key.session_id == record.session_id
                            && serde_json::from_str::<SessionRoute>(&key.route)
                                .is_ok_and(|route| route.same_route(&record.route))
                    })
                    .cloned()
                    .collect();
                if live.is_empty() {
                    if let Some(source) = journal::legacy_journal_source(
                        &self.data_dir,
                        &record.session_id,
                        &record.route,
                    )
                    .await
                    .and_then(|source| {
                        self.translators
                            .canonical_source(&source)
                            .map(str::to_owned)
                    }) {
                        if let Ok(key) =
                            DeliveryKey::new(&source, &record.session_id, &record.route)
                        {
                            delivery_keys.insert(key);
                        }
                    }
                } else {
                    delivery_keys.extend(live);
                }
            }
        }
        let mut result = FlushResult {
            flushed: true,
            pending: 0,
            accepted_sessions: delivery_keys.len() as u64,
        };
        for key in &delivery_keys {
            self.observe_claude_transcripts(Some(key), false).await;
        }
        // Managed completion is terminal. Stop polling before the barrier so
        // an in-flight observer cannot enqueue after translator finalization.
        for key in &delivery_keys {
            let lock = self.session_lock(&key.source, &key.session_id);
            let _guard = lock.lock().await;
            self.claude_transcripts
                .lock()
                .unwrap()
                .retain(|candidate, observation| {
                    candidate.source != key.source
                        || candidate.session_id != key.session_id
                        || observation.template.managed_run_id.as_deref()
                            != Some(params.managed_run_id.as_str())
                });
        }
        self.ingress_barrier().await;
        for key in delivery_keys {
            if let Err(error) = self.refresh_session_before_flush(&key).await {
                tracing::warn!(
                    managed_run_id = %params.managed_run_id,
                    session_id = %key.session_id,
                    %error,
                    "managed run session auth refresh failed"
                );
                result.flushed = false;
                continue;
            }
            let session = { self.sessions.lock().unwrap().get(&key).cloned() };
            if let Some(session) = session {
                let (flushed, pending) = session
                    .finalize(Duration::from_millis(params.timeout_ms))
                    .await;
                result.flushed &= flushed;
                result.pending = result.pending.saturating_add(pending);
            }
        }
        result
    }

    /// Queue a flush on every live delivery route before dispatching more ingress.
    /// Only the completion wait runs in a separate task. This is used
    /// only by the daemon worker after a turn-ending event has already been
    /// durably captured and acknowledged to the hook client.
    pub(super) async fn enqueue_source_session_flush(
        &self,
        source: &str,
        session_id: &str,
        timeout: Duration,
    ) {
        let sessions: Vec<_> = self
            .sessions
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.source == source && key.session_id == session_id)
            .map(|(_, session)| session.clone())
            .collect();
        for session in sessions {
            let reply = match session.enqueue_flush().await {
                Ok(reply) => reply,
                Err(error) => {
                    tracing::warn!(source, session_id, %error, "turn-end flush enqueue failed");
                    continue;
                }
            };
            let source = source.to_owned();
            let session_id = session_id.to_owned();
            tokio::spawn(async move {
                // Later events may remain queued after this boundary has flushed.
                // Their presence does not make this boundary's flush incomplete.
                if !matches!(tokio::time::timeout(timeout, reply).await, Ok(Ok(_))) {
                    tracing::warn!(
                        source,
                        session_id,
                        "out-of-band turn-end flush did not complete"
                    );
                }
            });
        }
    }
}
