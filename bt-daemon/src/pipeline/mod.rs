//! The recoverable stream pipeline.
//!
//! Capture appends native observations to a partition's event journal. A
//! translation machine turns them into route-neutral span operations in the
//! span journal. Each delivery route consumes that journal through its own
//! branch head, applies its overlay and plugins, and submits to the backend.
//! Every failure holds only the machine it belongs to, in a persisted
//! `Recovering` state, until its scenario-specific recovery condition holds.

pub(crate) mod backfill;
pub(crate) mod codec;
pub(crate) mod events;
pub(crate) mod journal;
pub(crate) mod migrate;
pub(crate) mod recovery;
pub(crate) mod routing;
pub(crate) mod runtime;
pub(crate) mod store;
pub(crate) mod translation;
pub(crate) mod types;
