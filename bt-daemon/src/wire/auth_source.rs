use serde::{Deserialize, Serialize};

/// The non-secret credential source selected for a session route.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthSource {
    /// Resolve using the host's ordinary precedence rules. Retained for old
    /// profile-less routes; newly resolved command routes are canonicalized.
    #[default]
    Auto,
    /// Resolve a named credential from the host's saved profile store.
    SavedProfile,
    /// Resolve BRAINTRUST_API_KEY from the process environment at delivery
    /// time. The key itself is never serialized into the route.
    Environment,
}

impl AuthSource {
    pub(super) fn is_auto(&self) -> bool {
        *self == Self::Auto
    }
}
