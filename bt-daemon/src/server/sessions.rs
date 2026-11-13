//! Delivery keys and the per-session actors, event collections and locks.

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
    pub(super) async fn translation_for(
        &self,
        env: &Envelope,
    ) -> anyhow::Result<Arc<crate::pipeline::translation::Translation>> {
        let storage_key = crate::ids::session_namespace(&env.source, &env.session_id);
        if let Some(source) = self.translations.lock().get(&storage_key).cloned() {
            return Ok(source);
        }
        // Initialization owns one reducer per source. This lock covers only
        // opening its SQLite head and spawning the actor, never capture/replay.
        let _init = self.translation_init.lock().await;
        if let Some(source) = self.translations.lock().get(&storage_key).cloned() {
            return Ok(source);
        }
        let events = self.events_for(&env.source, &env.session_id).await?;
        let namespace = if events.legacy_namespace().await? {
            env.session_id.clone()
        } else {
            storage_key.clone()
        };
        let source = crate::pipeline::translation::Translation::spawn(
            crate::pipeline::translation::TranslationOptions {
                source: env.source.clone(),
                session_id: env.session_id.clone(),
                namespace,
                version: self.version.clone(),
                data_dir: self.data_dir.clone(),
                events,
                input: self.source_input_for(&env.source, &env.session_id),
                registry: self.translators.clone(),
                correlation: self.correlation.clone(),
                correlation_changed: self.correlation_changed.clone(),
            },
        )
        .await?;
        self.translations.lock().insert(storage_key, source.clone());
        Ok(source)
    }

    pub(super) async fn session_for(
        &self,
        env: &mut Envelope,
        key: &DeliveryKey,
        replay_through: u64,
    ) -> anyhow::Result<Arc<Session>> {
        {
            let map = self.sessions.lock();
            if let Some(session) = map.get(key) {
                return Ok(session.clone());
            }
        }
        let route = env
            .route
            .clone()
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
        let translation = self.translation_for(env).await?;
        let settings = translation.route_settings(&route).await?;
        let config = settings.with_auth(config.auth);
        env.config = Some(config.clone());
        if let Some(state) = self.session_auth.lock().await.get_mut(key) {
            state.route = settings;
        }
        translation
            .observe(crate::pipeline::translation::Observation {
                key: key.correlation_key(),
                route: route.clone(),
                config: config.clone(),
            })
            .await?;
        // Materialize historical output before seeding a released completion boundary.
        translation.advance(replay_through).await?;
        let _ = translation.barrier().await;
        let input = translation.subscribe(&route).await?;
        let mut map = self.sessions.lock();
        if let Some(s) = map.get(key) {
            return Ok(s.clone());
        }
        let session = Session::spawn(
            SessionOptions {
                observer_key: key.correlation_key(),
                translation,
                input,
                route: route.clone(),
                auth_provider: self.route_auth_provider(key),
                session_id: env.session_id.clone(),
                source: env.source.clone(),
                bt_version: self.version.clone(),
                config,
                data_dir: self.data_dir.clone(),
            },
            self.sink_factory.clone(),
        );
        map.insert(key.clone(), session.clone());
        Ok(session)
    }

    /// New settings can repair older subscriptions held on configuration, without
    /// merging independent routes or acknowledging any unresolved work.
    pub(super) async fn reconfigure_held_routes(
        &self,
        env: &Envelope,
        through: u64,
    ) -> anyhow::Result<()> {
        let Some(route) = &env.route else {
            return Ok(());
        };
        let translation = self.translation_for(env).await?;
        for (stored, settings) in translation.reconfigure_held(route, through).await? {
            let key = DeliveryKey::new(&env.source, &env.session_id, &stored)?;
            let config = {
                let mut states = self.session_auth.lock().await;
                states.get_mut(&key).map(|state| {
                    state.route = settings;
                    state.route.with_auth(state.lease.auth.clone())
                })
            };
            let session = self.sessions.lock().get(&key).cloned();
            if let (Some(session), Some(config)) = (session, config) {
                session.configure(config).await?;
            }
        }
        Ok(())
    }

    /// Drop every trace of one delivery pipeline. A session that goes quiet
    /// must not pin its translator state, sink handles, credential lease, or
    /// journal file for the rest of the daemon's life; deterministic span ids
    /// mean a late event simply rebuilds it from the journal.
    pub(super) async fn retire_session(&self, key: &DeliveryKey) {
        self.ingress_barrier().await;
        self.observe_claude_transcripts(Some(key), false).await;
        self.ingress_barrier().await;
        let lock = self.dispatch_lock(&key.source, &key.session_id);
        let _guard = lock.lock().await;

        let session = { self.sessions.lock().remove(key) };
        let Some(session) = session else {
            return;
        };
        session.shutdown().await;
        self.correlation.retire_session(&key.correlation_key());

        self.session_auth.lock().await.remove(key);
        self.auth_errors.lock().remove(key);
        self.route_aliases
            .lock()
            .retain(|alias, target| alias != key && target != key);
        self.managed_run_sessions.lock().retain(|_, keys| {
            keys.remove(key);
            !keys.is_empty()
        });

        // Several delivery routes can share one source session, so release
        // its journal writer and lock only after its final route retires.
        let last = !self
            .sessions
            .lock()
            .keys()
            .any(|other| other.source == key.source && other.session_id == key.session_id);
        if last {
            let storage_key = crate::ids::session_namespace(&key.source, &key.session_id);
            // Capture can proceed while the actor flushes above. Only take its
            // lock for the brief writer-map cleanup after daemon work ends.
            let capture_lock = self.session_lock(&key.source, &key.session_id);
            let _capture_guard = capture_lock.lock().await;
            self.claude_transcripts.lock().retain(|candidate, _| {
                candidate.source != key.source || candidate.session_id != key.session_id
            });
            self.journals.lock().remove(&storage_key);
            let mut inputs = self.source_inputs.lock();
            if inputs
                .get(&storage_key)
                .is_some_and(|input| input.is_empty())
            {
                inputs.remove(&storage_key);
            }
            drop(inputs);
            self.translations.lock().remove(&storage_key);
            {
                let mut locks = self.session_locks.lock();
                // A waiting capture or observer must keep using this same
                // lock, rather than race a newly allocated mirror writer.
                if Arc::strong_count(&capture_lock) == 2 {
                    locks.remove(&storage_key);
                }
            }
            self.dispatch_locks.lock().remove(&storage_key);
        }
        tracing::info!(session_id = %key.session_id, "session retired");
    }

    /// Delivery pipelines with no traffic for `idle_timeout` and nothing left
    /// queued.
    pub(super) fn idle_sessions(&self, idle_timeout: Duration) -> Vec<DeliveryKey> {
        self.sessions
            .lock()
            .iter()
            .filter(|(_, session)| session.idle_for() >= idle_timeout && session.pending() == 0)
            .filter(|(key, _)| !self.correlation.has_active_tools(&key.correlation_key()))
            .map(|(key, _)| key.clone())
            .collect()
    }

    /// The shared input collection for a source session. Opening it imports
    /// any legacy journal once, before capture or recovery can use it.
    pub(super) async fn events_for(
        &self,
        source: &str,
        session_id: &str,
    ) -> anyhow::Result<EventCollection> {
        let slot = self
            .journals
            .lock()
            .entry(crate::ids::session_namespace(source, session_id))
            .or_default()
            .clone();
        let events = slot
            .get_or_try_init(|| EventCollection::open(&self.data_dir, source, session_id))
            .await?;
        Ok(events.clone())
    }

    pub(super) fn source_input_for(
        &self,
        source: &str,
        session_id: &str,
    ) -> crate::pipeline::input::SourceInput {
        self.source_inputs
            .lock()
            .entry(crate::ids::session_namespace(source, session_id))
            .or_default()
            .clone()
    }

    pub(super) async fn append_event(
        &self,
        env: &Envelope,
        events: &EventCollection,
    ) -> anyhow::Result<Option<crate::pipeline::input::SourcePosition>> {
        let Some(appended) = events.append(env, None).await? else {
            return Ok(None);
        };
        if let Some(cutoffs) = self.startup_cutoffs.lock().as_mut() {
            cutoffs
                .entry(crate::ids::session_namespace(&env.source, &env.session_id))
                .or_insert(appended.before);
        }
        Ok(Some(appended))
    }

    pub(super) fn session_lock(
        &self,
        source: &str,
        session_id: &str,
    ) -> Arc<tokio::sync::Mutex<()>> {
        let storage_key = crate::ids::session_namespace(source, session_id);
        self.session_locks
            .lock()
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
            .entry(storage_key)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }
}
