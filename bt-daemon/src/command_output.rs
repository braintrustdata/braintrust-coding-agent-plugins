//! Stable, host-independent output contracts for user-facing trace commands.
//!
//! Embedders such as `bt` own their global `--json` flag, but should delegate
//! the output shape to this crate so every front-end reports daemon commands
//! consistently and JSON mode never falls back to human prose.

mod auth_diagnostic;
mod daemon_diagnostic;
mod daemon_status;
mod doctor_command_output;
mod import_summary;
mod install_command_output;
mod output_format;
mod status_command_output;
mod stop_command_output;
#[cfg(test)]
mod tests;
mod trace_command_output;
mod update_command_output;

pub use auth_diagnostic::AuthDiagnostic;
pub use daemon_diagnostic::DaemonDiagnostic;
pub use daemon_status::DaemonStatus;
pub use doctor_command_output::DoctorCommandOutput;
pub use import_summary::ImportSummary;
pub use install_command_output::InstallCommandOutput;
pub use install_command_output::SetupCommandOutput;
pub use output_format::OutputFormat;
pub use status_command_output::StatusCommandOutput;
pub use stop_command_output::StopCommandOutput;
pub use trace_command_output::TraceCommandOutput;
pub use update_command_output::UpdateCommandOutput;
