use super::{AuthDiagnostic, DaemonDiagnostic};
use crate::wire::SessionRoute;
use serde::Serialize;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize)]
pub struct DoctorCommandOutput {
    pub source: String,
    pub display_name: String,
    pub settings_path: PathBuf,
    pub settings_present: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_settings_path: Option<PathBuf>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub project_settings_fields: Vec<String>,
    pub enabled: bool,
    pub route_source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub route: Option<SessionRoute>,
    pub auth: AuthDiagnostic,
    pub daemon: DaemonDiagnostic,
    pub warnings: Vec<String>,
    pub plugin_diagnostics: Vec<crate::PluginDiagnostic>,
}

impl DoctorCommandOutput {
    pub(super) fn render_human(&self) -> anyhow::Result<String> {
        let route = self
            .route
            .as_ref()
            .map(serde_json::to_string_pretty)
            .transpose()?
            .unwrap_or_else(|| "(unresolved)".into());
        let mut rendered = format!(
            "Braintrust tracing doctor: {}\nEnabled: {}\nSettings: {}{}\nRoute source: {}\nRoute: {}",
            self.display_name,
            self.enabled,
            self.settings_path.display(),
            if self.settings_present { "" } else { " (missing)" },
            self.route_source,
            route,
                );
        if let Some(path) = &self.project_settings_path {
            rendered.push_str(&format!("\nProject settings: {}", path.display()));
            if !self.project_settings_fields.is_empty() {
                rendered.push_str(&format!(
                    "\nProject overrides: {}",
                    self.project_settings_fields.join(", ")
                ));
            }
        }
        self.auth.append_human(&mut rendered, "");
        self.daemon.append_human(&mut rendered);
        for warning in &self.warnings {
            rendered.push_str(&format!("\nWarning: {warning}"));
        }
        for diagnostic in &self.plugin_diagnostics {
            rendered.push_str(&format!(
                "\nHistorical plugin error: {} ({} recorded occurrence{}; not an affected-span count)\nFirst seen: {}\nLast seen: {}\n{}",
                diagnostic.plugin_path.display(),
                diagnostic.occurrences,
                if diagnostic.occurrences == 1 { "" } else { "s" },
                render_timestamp(diagnostic.first_seen_ms),
                render_timestamp(diagnostic.last_seen_ms),
                diagnostic.exception
            ));
        }
        Ok(rendered)
    }
}

fn render_timestamp(timestamp_ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(timestamp_ms)
        .map(|timestamp| timestamp.to_rfc3339())
        .unwrap_or_else(|| timestamp_ms.to_string())
}
