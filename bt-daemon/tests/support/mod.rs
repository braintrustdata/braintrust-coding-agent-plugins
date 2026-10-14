#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "Integration test assertions and fixtures fail immediately on unexpected values."
)]
#![allow(dead_code)]
#![allow(
    clippy::disallowed_methods,
    reason = "Test fixtures intentionally launch raw children."
)]

pub mod agent_process;
pub mod agents;
pub mod distributed;
pub mod inference;
pub mod ingest;
pub mod server;
