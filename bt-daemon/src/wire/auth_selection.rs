use super::AuthSource;
use serde::{Deserialize, Serialize};

/// Non-secret auth selection. An optional organization constrains credentials
/// that can address more than one organization.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AuthSelection {
    #[serde(default, skip_serializing_if = "AuthSource::is_auto")]
    pub source: AuthSource,
    /// Immutable profile identifier. Preferred over the legacy display name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub org_name: Option<String>,
}

impl AuthSelection {
    /// Interpret old `profile: "environment"` routes as environment auth
    /// without preserving the synthetic value as a saved profile name.
    pub fn effective_source(&self) -> AuthSource {
        match self.source {
            AuthSource::Auto if self.profile.as_deref() == Some("environment") => {
                AuthSource::Environment
            }
            AuthSource::Auto if self.profile_id.is_some() || self.profile.is_some() => {
                AuthSource::SavedProfile
            }
            source => source,
        }
    }

    pub fn canonicalized(mut self) -> anyhow::Result<Self> {
        self.source = self.effective_source();
        match self.source {
            AuthSource::SavedProfile if self.profile_id.is_none() && self.profile.is_none() => {
                anyhow::bail!("saved-profile auth requires a profile ID or name")
            }
            AuthSource::Environment => {
                self.profile_id = None;
                self.profile = None;
            }
            AuthSource::Auto | AuthSource::SavedProfile => {}
        }
        Ok(self)
    }
}
