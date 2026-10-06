use super::{Agent, Identity, Register, Registrar};
use std::sync::Arc;

pub(crate) struct Antigravity;

pub(crate) const IDENTITY: Identity = Identity {
    id: "antigravity",
    source: "antigravity",
    aliases: &["agy"],
    display_name: "Google Antigravity",
};

impl Agent for Antigravity {
    fn identity(&self) -> &'static Identity {
        &IDENTITY
    }
}

impl Register for Antigravity {
    fn register(self: Arc<Self>, registrar: &mut Registrar) {
        registrar.setup.insert(self);
    }
}
