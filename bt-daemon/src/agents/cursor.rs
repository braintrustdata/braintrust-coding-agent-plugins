use super::{Agent, Identity, Register, Registrar};
use std::sync::Arc;

pub(crate) struct Cursor;

pub(crate) const IDENTITY: Identity = Identity {
    id: "cursor",
    source: "cursor",
    aliases: &[],
    display_name: "Cursor",
};

impl Agent for Cursor {
    fn identity(&self) -> &'static Identity {
        &IDENTITY
    }
}

impl Register for Cursor {
    fn register(self: Arc<Self>, registrar: &mut Registrar) {
        registrar.setup.insert(self);
    }
}
