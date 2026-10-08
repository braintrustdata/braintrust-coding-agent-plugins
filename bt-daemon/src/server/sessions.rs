//! Delivery keys and the per-session actors, journal writers and locks.

use super::*;

/// One independent delivery pipeline for a source session and the exact route
/// carried by its hook or import envelope.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct DeliveryKey {
    pub(super) source: String,
    pub(super) session_id: String,
    pub(super) route: String,
}

impl DeliveryKey {
    pub(super) fn new(
        source: &str,
        session_id: &str,
        route: &SessionRoute,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            source: source.to_string(),
            session_id: session_id.to_string(),
            route: serde_json::to_string(route)?,
        })
    }

    pub(super) fn correlation_key(&self) -> String {
        format!(
            "{}\u{1f}{}\u{1f}{}",
            self.source, self.session_id, self.route
        )
    }
}

impl Daemon {
    pub(super) async fn session_for(
        &self,
        env: &Envelope,
        key: &DeliveryKey,
        replay_through: u64,
    ) -> anyhow::Result<Arc<Session>> {
        {
            let map = self.sessions.lock().unwrap();
            if let Some(session) = map.get(key) {
                return Ok(session.clone());
            }
        }
        let route = env
            .route
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("event is missing its session route"))?;
        let config = env.config.clone().ok_or_else(|| {
            anyhow::anyhow!("resolved route is missing its session configuration")
        })?;
        // The actor streams the journal itself, so creating a session stays
        // cheap and allocation-free here no matter how long the recorded
        // session is. Bound it to what is recorded now, before this event is
        // appended, so replay covers prior source observations only. Every
        // destination consumes that shared observation stream, with its own
        // delivery checkpoint controlling which rows reach its sink.
        let journal_path =
            journal::ensure_source_journal(&self.data_dir, &env.source, &env.session_id).await?;
        let translator_session_id =
            if journal::legacy_journal_has_session(&self.data_dir, &env.source, &env.session_id)
                .await
            {
                env.session_id.clone()
            } else {
                crate::ids::session_namespace(&env.source, &env.session_id)
            };
        let mut replay = ReplayPlan {
            acknowledged_through: journal::JournalReader::acknowledged_through(
                &journal_path,
                replay_through,
                route,
            )
            .await,
            through: replay_through,
        };
        let source_key = crate::ids::session_namespace(&env.source, &env.session_id);
        let derived = {
            let mut map = self.derived.lock().unwrap();
            if let Some(existing) = map.get(&source_key) {
                existing.clone()
            } else {
                let created = crate::derived::SourceTranslation::new(
                    &env.source,
                    &env.session_id,
                    &translator_session_id,
                    &self.data_dir,
                    &self.translators,
                )?;
                map.insert(source_key, created.clone());
                created
            }
        };
        for captured in journal::captured_routes(&journal_path)? {
            let template = journal::envelope_from_redacted(captured.envelope);
            if let (Some(original), Some(effective)) = (
                template.route.as_ref(),
                recovered_delivery_route(self, &template).await,
            ) {
                if effective.same_route(route) {
                    derived.bind_route(original, route)?;
                }
            }
        }
        replay.acknowledged_through = derived.register(route, replay.acknowledged_through)?;
        let journal = self
            .journal_writer_for(&env.source, &env.session_id)
            .await?;
        let mut map = self.sessions.lock().unwrap();
        if let Some(s) = map.get(key) {
            return Ok(s.clone());
        }
        let session = Session::spawn(
            SessionOptions {
                session_id: env.session_id.clone(),
                source: env.source.clone(),
                plugin_version: env.plugin_version.clone(),
                replay: Some(replay),
                config,
                correlation_key: key.correlation_key(),
                route: route.clone(),
                correlation: self.correlation.clone(),
                data_dir: self.data_dir.clone(),
                journal,
                correlation_changed: self.correlation_changed.clone(),
                auth_provider: self.auth_provider.clone(),
                derived,
            },
            self.translators.clone(),
            self.sink_factory.clone(),
        );
        map.insert(key.clone(), session.clone());
        Ok(session)
    }

    /// Drop every trace of one delivery pipeline. A session that goes quiet
    /// must not pin its translator state, sink handles, credential lease, or
    /// journal file for the rest of the daemon's life; deterministic span ids
    /// mean a late event simply rebuilds it from the journal.
    pub(super) async fn retire_session(&self, key: &DeliveryKey) {
        self.observe_claude_transcripts(Some(key), false).await;
        self.ingress_barrier().await;
        let lock = self.dispatch_lock(&key.source, &key.session_id);
        let _guard = lock.lock().await;

        let session = { self.sessions.lock().unwrap().remove(key) };
        let Some(session) = session else {
            return;
        };
        session.shutdown().await;
        self.correlation.retire_session(&key.correlation_key());

        self.session_auth.lock().await.remove(key);
        self.auth_errors.lock().unwrap().remove(key);
        self.route_aliases
            .lock()
            .unwrap()
            .retain(|alias, target| alias != key && target != key);
        self.managed_run_sessions.lock().unwrap().retain(|_, keys| {
            keys.remove(key);
            !keys.is_empty()
        });

        // Several delivery routes can share one source session, so release
        // its journal writer and lock only after its final route retires.
        let last = !self
            .sessions
            .lock()
            .unwrap()
            .keys()
            .any(|other| other.source == key.source && other.session_id == key.session_id);
        if last {
            let storage_key = crate::ids::session_namespace(&key.source, &key.session_id);
            // Capture can proceed while the actor flushes above. Only take its
            // lock for the brief writer-map cleanup after daemon work ends.
            let capture_lock = self.session_lock(&key.source, &key.session_id);
            let _capture_guard = capture_lock.lock().await;
            self.claude_transcripts
                .lock()
                .unwrap()
                .retain(|candidate, _| {
                    candidate.source != key.source || candidate.session_id != key.session_id
                });
            self.journals.lock().unwrap().remove(&storage_key);
            {
                let mut locks = self.session_locks.lock().unwrap();
                // A waiting capture or observer must keep using this same
                // lock, rather than race a newly allocated mirror writer.
                if Arc::strong_count(&capture_lock) == 2 {
                    locks.remove(&storage_key);
                }
            }
            self.dispatch_locks.lock().unwrap().remove(&storage_key);
            self.ingress_dispatched
                .lock()
                .unwrap()
                .retain(|candidate, _| {
                    candidate.source != key.source || candidate.session_id != key.session_id
                });
        }
        tracing::info!(session_id = %key.session_id, "session retired");
    }

    /// Delivery pipelines with no traffic for `idle_timeout` and nothing left
    /// queued.
    pub(super) fn idle_sessions(&self, idle_timeout: Duration) -> Vec<DeliveryKey> {
        self.sessions
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, session)| {
                session.idle_for() >= idle_timeout
                    && session.counters.queued.load(Ordering::Relaxed) == 0
                    && !session.has_paused_work()
            })
            .filter(|(key, _)| !self.correlation.has_active_tools(&key.correlation_key()))
            .map(|(key, _)| key.clone())
            .collect()
    }

    pub(super) async fn journal_writer_for(
        &self,
        source: &str,
        session_id: &str,
    ) -> anyhow::Result<Arc<tokio::sync::Mutex<JournalWriter>>> {
        let storage_key = crate::ids::session_namespace(source, session_id);
        if let Some(writer) = self.journals.lock().unwrap().get(&storage_key).cloned() {
            return Ok(writer);
        }
        let path = journal::ensure_source_journal(&self.data_dir, source, session_id).await?;
        let writer = Arc::new(tokio::sync::Mutex::new(
            JournalWriter::open_path(&path).await?,
        ));
        Ok(self
            .journals
            .lock()
            .unwrap()
            .entry(storage_key)
            .or_insert_with(|| writer.clone())
            .clone())
    }

    pub(super) async fn append_to_journal(&self, env: &mut Envelope) -> anyhow::Result<(u64, u64)> {
        let writer = self
            .journal_writer_for(&env.source, &env.session_id)
            .await?;
        let mut writer = writer.lock().await;
        let before = writer.position();
        let through = writer.append(env).await?;
        Ok((before, through))
    }

    pub(super) fn session_lock(
        &self,
        source: &str,
        session_id: &str,
    ) -> Arc<tokio::sync::Mutex<()>> {
        let storage_key = crate::ids::session_namespace(source, session_id);
        self.session_locks
            .lock()
            .unwrap()
            .entry(storage_key)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    pub(super) fn dispatch_lock(
        &self,
        source: &str,
        session_id: &str,
    ) -> Arc<tokio::sync::Mutex<()>> {
        let storage_key = crate::ids::session_namespace(source, session_id);
        self.dispatch_locks
            .lock()
            .unwrap()
            .entry(storage_key)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }
}
