//! Per-session authentication: resolving a route to a backend lease through
//! the host, refreshing it before flushes, and diagnosing failures.

use super::*;

pub(super) fn lease_is_expiring(lease: &AuthLease) -> bool {
    const REFRESH_WINDOW_MS: i64 = 60_000;
    let Some(expires_at_ms) = lease.expires_at_ms else {
        return false;
    };
    expires_at_ms <= now_ms().saturating_add(REFRESH_WINDOW_MS)
}

/// Resolve a route's lease the way event delivery does, including the
/// organization the route requires.
pub(crate) async fn resolve_route_auth(
    provider: &dyn AuthProvider,
    selection: &AuthSelection,
    reason: AuthResolveReason,
    required_org: Option<&str>,
) -> anyhow::Result<AuthLease> {
    let lease = provider.resolve(selection, reason).await?;
    if let Some(expected_org) = required_org {
        if lease.auth.org_name.as_deref() != Some(expected_org) {
            anyhow::bail!(
                "profile {:?} resolved organization {:?}, expected {:?}",
                lease.selection,
                lease.auth.org_name,
                expected_org
            );
        }
    }
    Ok(lease)
}

/// A route refresh updates the same in-memory lease cache used by later hooks.
/// Persisted head identity remains independent of the credential itself.
struct RouteAuthProvider {
    provider: Arc<dyn AuthProvider>,
    key: DeliveryKey,
    states: Arc<tokio::sync::Mutex<HashMap<DeliveryKey, SessionAuthState>>>,
}
#[async_trait::async_trait]
impl AuthProvider for RouteAuthProvider {
    async fn resolve(
        &self,
        selection: &AuthSelection,
        reason: AuthResolveReason,
    ) -> anyhow::Result<AuthLease> {
        let lease = self.provider.resolve(selection, reason).await?;
        let mut states = self.states.lock().await;
        if let Some(state) = states.get_mut(&self.key) {
            anyhow::ensure!(
                lease.selection == state.lease.selection,
                "credential refresh changed the pinned auth selection"
            );
            anyhow::ensure!(
                lease.auth.api_url == state.lease.auth.api_url
                    && lease.auth.org_id == state.lease.auth.org_id
                    && lease.auth.org_name == state.lease.auth.org_name,
                "credential refresh changed the pinned backend or organization"
            );
            state.lease = lease.clone();
        }
        Ok(lease)
    }
}

impl Daemon {
    pub(super) fn route_auth_provider(&self, key: &DeliveryKey) -> Option<Arc<dyn AuthProvider>> {
        self.auth_provider.as_ref().map(|provider| {
            Arc::new(RouteAuthProvider {
                provider: provider.clone(),
                key: key.clone(),
                states: self.session_auth.clone(),
            }) as Arc<dyn AuthProvider>
        })
    }
    pub(super) async fn configure_event(&self, env: &mut Envelope) -> anyhow::Result<DeliveryKey> {
        if self.auth_provider.is_none() {
            anyhow::bail!("daemon host has no Braintrust auth provider");
        }
        let requested_route = env
            .route
            .clone()
            .ok_or_else(|| anyhow::anyhow!("event is missing its session route"))?;
        if requested_route.destination.is_none() {
            anyhow::bail!(
                "session route is missing its trace destination; select a project or destination during `bt trace setup` or `bt trace run`"
            );
        }
        let requested_key = DeliveryKey::new(&env.source, &env.session_id, &requested_route)?;
        let key = self
            .route_aliases
            .lock()
            .get(&requested_key)
            .cloned()
            .unwrap_or_else(|| requested_key.clone());
        let (selection, reason, expected_selection) = {
            let states = self.session_auth.lock().await;
            match states.get(&key) {
                Some(state) => {
                    if !lease_is_expiring(&state.lease) {
                        env.config = Some(state.route.with_auth(state.lease.auth.clone()));
                        return Ok(key);
                    }
                    (
                        state.lease.selection.clone(),
                        AuthResolveReason::Expiring,
                        Some(state.lease.selection.clone()),
                    )
                }
                None => (
                    requested_route.auth.clone(),
                    AuthResolveReason::Initial,
                    None,
                ),
            }
        };

        let provider = self
            .route_auth_provider(&key)
            .ok_or_else(|| anyhow::anyhow!("daemon host has no Braintrust auth provider"))?;
        let lease = resolve_route_auth(
            provider.as_ref(),
            &selection,
            reason,
            requested_route.auth.org_name.as_deref(),
        )
        .await
        .map_err(|error| {
            let message = format!(
                "could not resolve Braintrust auth for {}: {error}; run `bt login` or select a profile explicitly",
                env.source
            );
            self.record_auth_error(&key, &env.source, message.clone());
            anyhow::anyhow!(message)
        })?;
        if let Some(expected) = expected_selection {
            if lease.selection != expected {
                anyhow::bail!(
                    "credential refresh changed auth selection from {expected:?} to {:?}",
                    lease.selection
                );
            }
        }

        let mut route = requested_route;
        if route.auth.effective_source() == crate::wire::AuthSource::SavedProfile
            && route.auth.profile_id.is_none()
            && lease.selection.profile_id.is_some()
        {
            route.auth = lease.selection.clone();
            if let Err(error) = crate::settings::migrate_persisted_route(
                &env.source,
                env.route.as_ref().ok_or_else(|| {
                    anyhow::anyhow!("session route is missing during profile migration")
                })?,
                &route,
            ) {
                tracing::warn!(source = %env.source, "could not migrate legacy profile route: {error}");
            }
        }
        let canonical_key = DeliveryKey::new(&env.source, &env.session_id, &route)?;
        if canonical_key != requested_key {
            self.route_aliases
                .lock()
                .insert(requested_key, canonical_key.clone());
        }
        env.route = Some(route.clone());
        env.config = Some(route.with_auth(lease.auth.clone()));
        self.session_auth
            .lock()
            .await
            .insert(canonical_key.clone(), SessionAuthState { route, lease });
        self.auth_errors.lock().remove(&canonical_key);
        Ok(canonical_key)
    }

    /// Keep a route's auth failure visible to `status.get` until it succeeds.
    pub(super) fn record_auth_error(&self, key: &DeliveryKey, source: &str, message: String) {
        self.auth_errors
            .lock()
            .insert(key.clone(), (source.to_string(), message));
    }

    /// Resolve a route's credentials exactly as event delivery would, so
    /// diagnostics see this process's credential store and environment rather
    /// than the caller's.
    pub(super) async fn diagnose_auth(&self, selection: AuthSelection) -> AuthDiagnoseResult {
        let result = match &self.auth_provider {
            Some(provider) => {
                resolve_route_auth(
                    provider.as_ref(),
                    &selection,
                    AuthResolveReason::Initial,
                    selection.org_name.as_deref(),
                )
                .await
            }
            None => Err(anyhow::anyhow!(
                "daemon host has no Braintrust auth provider"
            )),
        };
        match result {
            Ok(lease) => AuthDiagnoseResult {
                selection: Some(lease.selection),
                org_name: lease.auth.org_name,
                expires_at_ms: lease.expires_at_ms,
                error: None,
            },
            Err(error) => AuthDiagnoseResult {
                error: Some(error.to_string()),
                ..AuthDiagnoseResult::default()
            },
        }
    }

    pub(super) async fn refresh_session_before_flush(
        &self,
        key: &DeliveryKey,
    ) -> anyhow::Result<()> {
        let Some(provider) = self.route_auth_provider(key) else {
            return Ok(());
        };
        let Some(state) = self.session_auth.lock().await.get(key).cloned() else {
            return Ok(());
        };
        if !lease_is_expiring(&state.lease) {
            return Ok(());
        }
        let selection = state.lease.selection.clone();
        let lease = provider
            .resolve(&selection, AuthResolveReason::Expiring)
            .await?;
        let config = state.route.with_auth(lease.auth);
        let session = { self.sessions.lock().get(key).cloned() };
        if let Some(session) = session {
            session.configure(config).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Refresh(AuthLease);
    #[async_trait::async_trait]
    impl AuthProvider for Refresh {
        async fn resolve(
            &self,
            _: &AuthSelection,
            reason: AuthResolveReason,
        ) -> anyhow::Result<AuthLease> {
            assert_eq!(reason, AuthResolveReason::Expiring);
            Ok(self.0.clone())
        }
    }

    #[tokio::test]
    async fn expiring_refreshes_preserve_backend_binding_and_accept_new_tokens() {
        for trigger in ["flush", "event"] {
            for change in ["backend", "org_id", "org_name", "token"] {
                let tmp = tempfile::tempdir().unwrap();
                let route = SessionRoute {
                    destination: Some(crate::wire::TraceDestination::ProjectLogs {
                        project_id: Some("project".into()),
                        project_name: None,
                    }),
                    ..Default::default()
                };
                let lease = AuthLease {
                    selection: route.auth.clone(),
                    auth: BackendAuth {
                        token: "old".into(),
                        api_url: Some("https://original.test".into()),
                        app_url: None,
                        org_id: Some("original-id".into()),
                        org_name: Some("original-name".into()),
                    },
                    expires_at_ms: Some(0),
                };
                let mut refreshed = lease.clone();
                refreshed.auth.token = "new".into();
                match change {
                    "backend" => refreshed.auth.api_url = Some("https://other.test".into()),
                    "org_id" => refreshed.auth.org_id = Some("other-id".into()),
                    "org_name" => refreshed.auth.org_name = Some("other-name".into()),
                    _ => {}
                }
                let mut options = debug_serve_options("test", tmp.path());
                options.auth_provider = Some(Arc::new(Refresh(refreshed)));
                let daemon = Daemon::new(options, tmp.path().into());
                let key = DeliveryKey::new("debug", "test", &route).unwrap();
                daemon.session_auth.lock().await.insert(
                    key.clone(),
                    SessionAuthState {
                        route: route.clone(),
                        lease: lease.clone(),
                    },
                );
                let mut env: Envelope = serde_json::from_value(serde_json::json!({
                    "source":"debug", "session_id":"test", "event":"event", "ts_ms":1,
                    "payload":{}, "route":route,
                }))
                .unwrap();
                let result = if trigger == "flush" {
                    daemon.refresh_session_before_flush(&key).await
                } else {
                    daemon.configure_event(&mut env).await.map(|_| ())
                };
                assert_eq!(result.is_ok(), change == "token", "{trigger}: {change}");
                let cached = daemon.session_auth.lock().await[&key].lease.clone();
                assert_eq!(cached.auth.api_url, lease.auth.api_url);
                assert_eq!(cached.auth.org_id, lease.auth.org_id);
                assert_eq!(cached.auth.org_name, lease.auth.org_name);
                assert_eq!(
                    cached.auth.token,
                    if change == "token" { "new" } else { "old" }
                );
                super::super::maintenance::drain_all(&daemon).await;
            }
        }
    }
}
