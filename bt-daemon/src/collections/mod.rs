//! Durable pipeline storage. This module is the only owner of SQLite: callers
//! see one [`SessionStorage`] per source session, holding concrete collections of
//! events and SpanOps. They read records in order and remove them once the
//! next stage owns their data or they are irrelevant.
//!
//! Translation transfers events to SpanOps in one transaction; each route
//! removes SpanOps from its view once the backend accepts them. Storage of a
//! session that has met every obligation is deleted.
mod events;
#[cfg(any(test, feature = "test-support"))]
pub mod inspect;
mod legacy;
mod session;
mod span_ops;
mod store;
#[cfg(test)]
mod tests;
mod upgrade;

pub(crate) use events::{EventReader, EventRecord};
pub(crate) use session::{remove_session, sessions, SessionStorage};
pub(crate) use span_ops::{metadata_only, BindingChanged, HeldRoute, Translated, DETACHED};
pub(crate) use store::is_corruption;
