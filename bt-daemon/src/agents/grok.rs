use super::{Agent, Identity, Register, Registrar};
use std::sync::Arc;

pub(crate) struct Grok;

pub(crate) const IDENTITY: Identity = Identity {
    id: "grok",
    source: "grok",
    aliases: &[],
    display_name: "Grok",
};

impl Agent for Grok {
    fn identity(&self) -> &'static Identity {
        &IDENTITY
    }
}

impl Register for Grok {
    fn register(self: Arc<Self>, registrar: &mut Registrar) {
        registrar.setup.insert(self);
    }
}
