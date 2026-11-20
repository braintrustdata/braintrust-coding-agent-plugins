//! Request and result schemas, each owned by its matching type module.

mod auth_diagnose_params;
mod auth_diagnose_result;
mod capabilities;
mod client_info;
mod event_log_result;
mod flush_params;
mod flush_result;
mod import_finish_params;
mod import_finish_result;
mod import_reset_params;
mod initialize_params;
mod initialize_result;
mod managed_run_flush_params;
pub mod method;
mod recovery_info;
mod session_status;
mod shutdown_result;
mod status_params;
mod status_result;

pub use auth_diagnose_params::AuthDiagnoseParams;
pub use auth_diagnose_result::AuthDiagnoseResult;
pub use capabilities::Capabilities;
pub use client_info::ClientInfo;
pub use event_log_result::EventLogResult;
pub use flush_params::FlushParams;
pub use flush_result::FlushResult;
pub use import_finish_params::ImportFinishParams;
pub use import_finish_result::ImportFinishResult;
pub use import_reset_params::ImportResetParams;
pub use initialize_params::InitializeParams;
pub use initialize_result::InitializeResult;
pub use managed_run_flush_params::ManagedRunFlushParams;
pub use recovery_info::RecoveryInfo;
pub use session_status::SessionStatus;
pub use shutdown_result::ShutdownResult;
pub use status_params::StatusParams;
pub use status_result::StatusResult;

fn default_flush_timeout_ms() -> u64 {
    10_000
}
