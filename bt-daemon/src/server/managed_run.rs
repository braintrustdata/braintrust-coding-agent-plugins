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
            let source = match record.source {
                Some(source) => Some(source),
                // Legacy records predate source-qualified delivery keys.
                None => journal::legacy_journal_source(
                    &self.data_dir,
                    &record.session_id,
                    &record.route,
                )
                .await
                .and_then(|source| {
                    self.translators
                        .canonical_source(&source)
                        .map(str::to_owned)
                }),
            };
            if let Some(source) = source {
                if let Ok(key) = DeliveryKey::new(&source, &record.session_id, &record.route) {
                    delivery_keys.insert(key);
                }
            }
        }
        for key in &delivery_keys {
            self.observe_claude_transcripts(Some(key), false).await;
        }
        // Managed completion is terminal. Stop polling before the barrier so
        // an in-flight observer cannot append after translator finalization.
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
        self.settle_assembly().await;
        let mut partitions: Vec<PartitionId> = Vec::new();
        for key in &delivery_keys {
            let partition = key.partition();
            if !partitions.contains(&partition) {
                partitions.push(partition);
            }
        }
        let mut result = self
            .flush_partitions(
                partitions,
                Some(crate::pipeline::types::FinalizeKind::Terminal),
                Duration::from_millis(params.timeout_ms),
            )
            .await;
        result.accepted_sessions = delivery_keys.len() as u64;
        result
    }
}
