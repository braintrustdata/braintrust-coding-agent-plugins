use super::{
    DoctorCommandOutput, ImportSummary, InstallCommandOutput, OutputFormat, StatusCommandOutput,
    StopCommandOutput, UpdateCommandOutput,
};
use crate::wire::StatusResult;
use serde::Serialize;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum TraceCommandOutput {
    Status(StatusCommandOutput),
    Doctor(Box<DoctorCommandOutput>),
    Install(InstallCommandOutput),
    Enable(InstallCommandOutput),
    Disable(InstallCommandOutput),
    Uninstall(InstallCommandOutput),
    Update(UpdateCommandOutput),
    Stop(StopCommandOutput),
    Import {
        /// Summaries for the imported sessions.
        summaries: Vec<ImportSummary>,
    },
}

impl TraceCommandOutput {
    pub fn status(status: Option<StatusResult>) -> Self {
        Self::Status(status.into())
    }

    pub fn doctor(output: DoctorCommandOutput) -> Self {
        Self::Doctor(Box::new(output))
    }

    pub fn install(
        source: impl Into<String>,
        display_name: impl Into<String>,
        settings_path: impl Into<PathBuf>,
    ) -> Self {
        Self::Install(InstallCommandOutput::new(
            source,
            display_name,
            settings_path,
        ))
    }

    /// Compatibility alias for creating install output.
    pub fn setup(
        source: impl Into<String>,
        display_name: impl Into<String>,
        settings_path: impl Into<PathBuf>,
    ) -> Self {
        Self::install(source, display_name, settings_path)
    }

    pub fn enable(
        source: impl Into<String>,
        display_name: impl Into<String>,
        settings_path: impl Into<PathBuf>,
    ) -> Self {
        Self::Enable(InstallCommandOutput::enabling(
            source,
            display_name,
            settings_path,
        ))
    }

    pub fn project_install(
        source: impl Into<String>,
        display_name: impl Into<String>,
        settings_path: impl Into<PathBuf>,
    ) -> Self {
        Self::Install(InstallCommandOutput::new(source, display_name, settings_path).for_project())
    }

    pub(crate) fn with_scope(mut self, scope: &'static str) -> Self {
        match &mut self {
            Self::Install(output)
            | Self::Enable(output)
            | Self::Disable(output)
            | Self::Uninstall(output) => {
                output.set_scope(scope);
            }
            _ => {}
        }
        self
    }

    pub(crate) fn with_enabled(mut self, enabled: bool) -> Self {
        if let Self::Install(output) = &mut self {
            output.enabled = Some(enabled);
        }
        self
    }

    pub(crate) fn into_enable(mut self, configuration: &'static str) -> Self {
        match &mut self {
            Self::Install(output) | Self::Enable(output) => {
                output.configuration = Some(configuration)
            }
            _ => return self,
        }
        match self {
            Self::Install(output) => Self::Enable(output),
            _ => self,
        }
    }

    pub fn stop(running: bool, stopped: bool) -> Self {
        Self::Stop(StopCommandOutput { running, stopped })
    }

    pub fn import(summaries: Vec<ImportSummary>) -> Self {
        Self::Import { summaries }
    }

    pub fn disable(
        source: impl Into<String>,
        display_name: impl Into<String>,
        settings_path: impl Into<PathBuf>,
    ) -> Self {
        Self::Disable(InstallCommandOutput::new(
            source,
            display_name,
            settings_path,
        ))
    }

    pub fn uninstall(
        source: impl Into<String>,
        display_name: impl Into<String>,
        settings_path: impl Into<PathBuf>,
    ) -> Self {
        Self::Uninstall(InstallCommandOutput::new(
            source,
            display_name,
            settings_path,
        ))
    }

    pub fn update(source: impl Into<String>, display_name: impl Into<String>) -> Self {
        let source = source.into();
        Self::Update(UpdateCommandOutput {
            source,
            display_name: display_name.into(),
            restart_required: true,
        })
    }

    pub fn render(&self, format: OutputFormat) -> anyhow::Result<String> {
        match format {
            OutputFormat::Json => Ok(serde_json::to_string(self)?),
            OutputFormat::Human => self.render_human(),
        }
    }

    fn render_human(&self) -> anyhow::Result<String> {
        match self {
            Self::Status(output) => output.render_human(),
            Self::Doctor(output) => output.render_human(),
            Self::Install(output) => Ok(output.render_install()),
            Self::Enable(output) => Ok(output.render_enable()),
            Self::Disable(output) => Ok(output.render_disable()),
            Self::Uninstall(output) => Ok(output.render_uninstall()),
            Self::Update(output) => Ok(output.render_human()),
            Self::Stop(output) => Ok(output.render_human()),
            Self::Import { summaries } => Ok(summaries
                .iter()
                .map(ImportSummary::render_human)
                .collect::<Vec<_>>()
                .join("\n")),
        }
    }
}
