use crate::wire::{AuthSelection, AuthSource};
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct AuthDiagnostic {
    pub status: String,
    pub source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub org_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl AuthDiagnostic {
    pub(crate) fn source_label(selection: &AuthSelection) -> &'static str {
        match selection.effective_source() {
            AuthSource::SavedProfile => "saved_profile",
            AuthSource::Environment => "environment",
            AuthSource::Auto => "command_context",
        }
    }

    pub(crate) fn ready(
        selection: AuthSelection,
        org_name: Option<String>,
        expires_at_ms: Option<i64>,
    ) -> Self {
        Self {
            status: "ready".into(),
            source: Self::source_label(&selection).into(),
            kind: None,
            profile: selection.profile,
            org_name,
            expires_at_ms,
            error: None,
        }
    }

    pub(crate) fn unconfirmed(status: &str, source: &str, error: Option<String>) -> Self {
        Self {
            status: status.into(),
            source: source.into(),
            kind: None,
            profile: None,
            org_name: None,
            expires_at_ms: None,
            error,
        }
    }

    /// Authentication resolved successfully in the reporting process.
    pub fn is_ready(&self) -> bool {
        self.status == "ready"
    }

    /// Authentication was checked and failed, rather than remaining unknown.
    pub fn is_error(&self) -> bool {
        self.status == "error"
    }

    /// Render one auth view; `prefix` distinguishes the daemon's from the local.
    pub(super) fn append_human(&self, rendered: &mut String, prefix: &str) {
        let label = |name: &str| {
            if prefix.is_empty() {
                name.to_string()
            } else {
                format!("{prefix}{}", name.to_lowercase())
            }
        };
        rendered.push_str(&format!(
            "\n{}: {} ({})",
            label("Auth"),
            self.status,
            self.source
        ));
        for (name, value) in [
            ("Profile", &self.profile),
            ("Organization", &self.org_name),
            ("Auth error", &self.error),
        ] {
            if let Some(value) = value {
                rendered.push_str(&format!("\n{}: {value}", label(name)));
            }
        }
    }
}
