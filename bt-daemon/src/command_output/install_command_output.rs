use serde::Serialize;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize)]
pub struct InstallCommandOutput {
    pub source: String,
    pub display_name: String,
    pub settings_path: PathBuf,
    pub restart_required: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub configuration: Option<&'static str>,
    pub scope: &'static str,
    #[serde(skip_serializing_if = "is_false")]
    pub applies_to_all_agents: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

/// Backwards-compatible name for the shared install and enable output.
pub type SetupCommandOutput = InstallCommandOutput;

fn is_false(value: &bool) -> bool {
    !value
}

impl InstallCommandOutput {
    pub(super) fn new(
        source: impl Into<String>,
        display_name: impl Into<String>,
        settings_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            source: source.into(),
            display_name: display_name.into(),
            settings_path: settings_path.into(),
            restart_required: true,
            configuration: None,
            scope: "user",
            applies_to_all_agents: false,
            enabled: None,
            warnings: Vec::new(),
        }
    }

    pub(super) fn enabling(
        source: impl Into<String>,
        display_name: impl Into<String>,
        settings_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            configuration: Some("reused"),
            ..Self::new(source, display_name, settings_path)
        }
    }

    pub(super) fn set_scope(&mut self, scope: &'static str) {
        self.scope = scope;
        self.applies_to_all_agents = scope == "project";
    }

    pub(super) fn for_project(mut self) -> Self {
        self.set_scope("project");
        self.warnings.push("Settings not specified here must be supplied by each user's configuration. Each user needs a user-level installation and usable Braintrust credentials for their sessions to be captured.".into());
        self.warnings.push(
            "This shared project configuration applies to all coding agents in the repository."
                .into(),
        );
        self
    }

    pub(super) fn render_install(&self) -> String {
        if self.scope == "project" {
            return format!(
                "Project tracing settings for {} were written to {}.\nTracing is {}.\n{}",
                self.display_name,
                self.settings_path.display(),
                if self.enabled == Some(false) {
                    "disabled"
                } else {
                    "enabled"
                },
                self.warnings.join("\n")
            );
        }
        format!(
            "The Braintrust tracing plugin is installed for {} and configured in {}.{}\nRestart the coding agent to load the tracing plugin.",
            self.display_name, self.settings_path.display(),
            if self.enabled == Some(false) { " Tracing is disabled by default." } else { "" },
        )
    }

    pub(super) fn render_enable(&self) -> String {
        if self.scope == "project" {
            return format!(
                "Braintrust tracing is enabled for all coding agents in this repository. Project configuration {} at {}.",
                self.configuration.unwrap_or("reused"), self.settings_path.display()
            );
        }
        if self.configuration.is_some_and(|value| value != "reused") {
            return format!(
                "Braintrust tracing configuration {} for {} at {} scope in {}.\nRestart the coding agent to apply the change.",
                self.configuration.unwrap_or_default(), self.display_name, self.scope, self.settings_path.display()
            );
        }
        format!(
            "Braintrust tracing is enabled for {} at {} scope in {}.\nRestart the coding agent to apply the change.",
            self.display_name, self.scope, self.settings_path.display()
        )
    }

    pub(super) fn render_disable(&self) -> String {
        if self.scope == "project" {
            return format!(
                "Braintrust tracing is disabled for all coding agents in this repository at {}.",
                self.settings_path.display()
            );
        }
        format!(
            "Braintrust tracing is disabled for {} at {} scope in {}.\nRestart the coding agent to apply the change.",
            self.display_name, self.scope, self.settings_path.display()
        )
    }

    pub(super) fn render_uninstall(&self) -> String {
        if self.scope == "project" {
            return format!("The shared repository tracing configuration was removed from {}. User-level settings now apply to all agents in this repository.", self.settings_path.display());
        }
        format!(
            "The Braintrust tracing plugin and configuration were removed for {} from {}.\nRestart the coding agent to apply the change.",
            self.display_name, self.settings_path.display()
        )
    }
}
