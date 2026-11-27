use super::{Agent, Identity, Register, Registrar};
use std::sync::Arc;

pub(crate) struct Devin;

const IDENTITY: Identity = Identity {
    id: "devin",
    source: "devin",
    aliases: &[],
    display_name: "Devin",
};

impl Agent for Devin {
    fn identity(&self) -> &'static Identity {
        &IDENTITY
    }
}

impl Register for Devin {
    fn register(self: Arc<Self>, registrar: &mut Registrar) {
        registrar.translate.insert(self);
    }
}
