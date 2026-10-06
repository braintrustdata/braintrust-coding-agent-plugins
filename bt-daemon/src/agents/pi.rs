use super::{Agent, Identity, Register, Registrar};
use std::sync::Arc;

pub(crate) struct Pi;

pub(crate) const IDENTITY: Identity = Identity {
    id: "pi",
    source: "pi",
    aliases: &[],
    display_name: "Pi",
};

impl Agent for Pi {
    fn identity(&self) -> &'static Identity {
        &IDENTITY
    }
}

impl Register for Pi {
    fn register(self: Arc<Self>, registrar: &mut Registrar) {
        registrar.setup.insert(self);
    }
}
