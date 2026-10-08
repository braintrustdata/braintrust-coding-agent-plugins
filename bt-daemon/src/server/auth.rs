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

impl Daemon {
    pub(super) async fn configure_event(&self, env: &mut Envelope) -> anyhow::Result<DeliveryKey> {
        let Some(provider) = &self.auth_provider else {
            anyhow::bail!("daemon host has no Braintrust auth provider");
        };
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
            .unwrap()
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
                env.route.as_ref().expect("route checked above"),
                &route,
            ) {
                tracing::warn!(source = %env.source, "could not migrate legacy profile route: {error}");
            }
        }
        let canonical_key = DeliveryKey::new(&env.source, &env.session_id, &route)?;
        if canonical_key != requested_key {
            self.route_aliases
                .lock()
                .unwrap()
                .insert(requested_key, canonical_key.clone());
        }
        env.route = Some(route.clone());
        env.config = Some(route.with_auth(lease.auth.clone()));
        self.session_auth
            .lock()
            .await
            .insert(canonical_key.clone(), SessionAuthState { route, lease });
        self.auth_errors.lock().unwrap().remove(&canonical_key);
        Ok(canonical_key)
    }

    /// Keep a route's auth failure visible to `status.get` until it succeeds.
    pub(super) fn record_auth_error(&self, key: &DeliveryKey, source: &str, message: String) {
        self.auth_errors
            .lock()
            .unwrap()
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
                    // Report whether the daemon can resolve credentials. A
                    // route's required org is a separate delivery failure.
                    None,
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
        let Some(provider) = &self.auth_provider else {
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
        if lease.selection != state.lease.selection {
            anyhow::bail!("credential refresh changed the session auth selection");
        }
        let config = state.route.with_auth(lease.auth.clone());
        self.session_auth.lock().await.insert(
            key.clone(),
            SessionAuthState {
                route: state.route,
                lease,
            },
        );
        let session = { self.sessions.lock().unwrap().get(key).cloned() };
        if let Some(session) = session {
            session.configure(config).await?;
        }
        Ok(())
    }
}
