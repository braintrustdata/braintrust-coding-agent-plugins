//! Per-route authentication: resolving a route to a backend lease through the
//! host, refreshing it before it expires, and diagnosing failures. Leases live
//! only in memory.

use super::*;
use crate::pipeline::runtime::{ResolvedAuth, RouteAuth};

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

/// The daemon's [`RouteAuth`]: host-resolved leases cached per route. A lease
/// keeps its canonical selection pinned across refreshes even if the user's
/// default profile changes.
pub(super) struct DaemonAuth {
    provider: Option<Arc<dyn AuthProvider>>,
    leases: tokio::sync::Mutex<HashMap<String, AuthLease>>,
}

impl DaemonAuth {
    pub(super) fn new(provider: Option<Arc<dyn AuthProvider>>) -> Self {
        Self {
            provider,
            leases: tokio::sync::Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl RouteAuth for DaemonAuth {
    async fn resolve(
        &self,
        source: &str,
        route: &SessionRoute,
        reason: AuthResolveReason,
    ) -> anyhow::Result<ResolvedAuth> {
        let Some(provider) = &self.provider else {
            anyhow::bail!("daemon host has no Braintrust auth provider");
        };
        if route.destination.is_none() {
            anyhow::bail!(
                "session route is missing its trace destination; select a project or destination during `bt trace setup` or `bt trace run`"
            );
        }
        let key = serde_json::to_string(route)?;
        let cached = self.leases.lock().await.get(&key).cloned();
        if let Some(lease) = &cached {
            if reason == AuthResolveReason::Initial && !lease_is_expiring(lease) {
                return Ok(ResolvedAuth {
                    config: route.with_auth(lease.auth.clone()),
                    expires_at_ms: lease.expires_at_ms,
                });
            }
        }
        let (selection, reason) = match &cached {
            Some(lease) if reason == AuthResolveReason::Initial => {
                (lease.selection.clone(), AuthResolveReason::Expiring)
            }
            Some(lease) => (lease.selection.clone(), reason),
            None => (route.auth.clone(), reason),
        };
        let lease = resolve_route_auth(
            provider.as_ref(),
            &selection,
            reason,
            route.auth.org_name.as_deref(),
        )
        .await?;
        if let Some(previous) = &cached {
            if lease.selection != previous.selection {
                anyhow::bail!(
                    "credential refresh changed auth selection from {:?} to {:?}",
                    previous.selection,
                    lease.selection
                );
            }
        }
        if route.auth.effective_source() == crate::wire::AuthSource::SavedProfile
            && route.auth.profile_id.is_none()
            && lease.selection.profile_id.is_some()
        {
            let mut canonical = route.clone();
            canonical.auth = lease.selection.clone();
            if let Err(error) = crate::settings::migrate_persisted_route(source, route, &canonical)
            {
                tracing::warn!(%source, "could not migrate legacy profile route: {error}");
            }
        }
        let resolved = ResolvedAuth {
            config: route.with_auth(lease.auth.clone()),
            expires_at_ms: lease.expires_at_ms,
        };
        self.leases.lock().await.insert(key, lease);
        Ok(resolved)
    }
}

impl Daemon {
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
}
