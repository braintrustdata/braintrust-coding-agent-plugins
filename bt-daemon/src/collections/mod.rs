//! Durable pipeline storage. This module is the only owner of SQLite: callers
//! see concrete collections of events and SpanOps, read records in order, and
//! remove them once the next stage owns their data or they are irrelevant.
//!
//! Each source session has one database. Translation transfers events to
//! SpanOps in one transaction; each route removes SpanOps from its view once
//! the backend accepts them. Storage of a session that has met every
//! obligation is deleted.
mod catalog;
mod events;
#[cfg(any(test, feature = "test-support"))]
pub mod inspect;
mod legacy;
mod span_ops;
mod store;
#[cfg(test)]
mod tests;

pub(crate) use catalog::{remove_session, sessions, SessionStorage};
pub(crate) use events::{EventCollection, EventReader, EventRecord};
pub(crate) use span_ops::{metadata_only, BindingChanged, HeldRoute, SpanOps, Translated};
pub(crate) use store::is_corruption;
