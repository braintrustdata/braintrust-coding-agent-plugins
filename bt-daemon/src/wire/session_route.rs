use super::{AuthSelection, BackendAuth, FlushMode, SessionConfig, TraceDestination};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, path::PathBuf};

/// Immutable, journal-safe routing and trace settings for one agent session.
/// Credentials are deliberately absent and are resolved inside the daemon.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SessionRoute {
    #[serde(default)]
    pub auth: AuthSelection,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination: Option<TraceDestination>,
    #[serde(default)]
    pub flush_mode: FlushMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub additional_metadata: Option<serde_json::Value>,
    /// Tags applied to each root span for this session.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub tags: BTreeSet<String>,
    /// Ordered JavaScript span transforms. Paths are resolved by explicit
    /// setup, run, and import commands before entering a session route.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub span_plugins: Vec<PathBuf>,
}

impl SessionRoute {
    pub fn with_auth(&self, auth: BackendAuth) -> SessionConfig {
        SessionConfig {
            auth,
            destination: self.destination.clone(),
            flush_mode: self.flush_mode,
            additional_metadata: self.additional_metadata.clone(),
            tags: self.tags.clone(),
            span_plugins: self.span_plugins.clone(),
        }
    }

    pub fn same_route(&self, other: &Self) -> bool {
        let mut left = self.clone();
        let mut right = other.clone();
        if left
            .auth
            .profile_id
            .as_ref()
            .zip(right.auth.profile_id.as_ref())
            .is_some_and(|(left, right)| left != right)
        {
            return false;
        }
        // A stable-ID route is the canonical successor to an old named route.
        // Journals can legitimately contain both forms for the same session,
        // so replay compares their unchanged routing intent, not the newly
        // learned identifier.
        left.auth.profile_id = None;
        right.auth.profile_id = None;
        left.auth.source = left.auth.effective_source();
        right.auth.source = right.auth.effective_source();
        serde_json::to_value(left).ok() == serde_json::to_value(right).ok()
    }

    /// Delivery settings may change without moving held work to another destination.
    pub(crate) fn same_destination(&self, other: &Self) -> bool {
        let identity = |route: &Self| Self {
            auth: route.auth.clone(),
            destination: route.destination.clone(),
            ..Self::default()
        };
        identity(self).same_route(&identity(other))
    }

    /// Raw journal entries can be replayed through a newer plugin chain as
    /// long as their Braintrust delivery route is otherwise unchanged.
    pub fn same_replay_route(&self, other: &Self) -> bool {
        let mut left = self.clone();
        let mut right = other.clone();
        left.span_plugins.clear();
        right.span_plugins.clear();
        left.same_route(&right)
    }

    /// Apply one invocation-local JSON metadata override to a non-secret route.
    ///
    /// The route is then carried unchanged through live hooks, managed runs, and
    /// transcript import. Keeping validation here gives every public command the
    /// same contract and prevents individual agent shims from parsing JSON.
    pub(crate) fn apply_additional_metadata(
        &mut self,
        additional_metadata: Option<&str>,
    ) -> anyhow::Result<()> {
        let Some(metadata) = additional_metadata else {
            return Ok(());
        };
        let value: serde_json::Value = serde_json::from_str(metadata)
            .map_err(|e| anyhow::anyhow!("invalid --additional-metadata JSON: {e}"))?;
        if !value.is_object() {
            anyhow::bail!("--additional-metadata must be a JSON object");
        }
        self.additional_metadata = Some(value);
        Ok(())
    }

    /// Apply invocation-local root-span tags to a route. Trim whitespace and reject
    /// empty tags here; the set keeps route tags unique.
    pub(crate) fn apply_tags(&mut self, tags: &[String]) -> anyhow::Result<()> {
        if tags.is_empty() {
            return Ok(());
        }
        let mut normalized = std::collections::BTreeSet::new();
        for tag in tags {
            let tag = tag.trim();
            if tag.is_empty() {
                anyhow::bail!("--tag must not be empty");
            }
            normalized.insert(tag.to_string());
        }
        self.tags = normalized;
        Ok(())
    }
}
