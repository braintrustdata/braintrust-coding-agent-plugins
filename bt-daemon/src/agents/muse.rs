use super::{Agent, Identity, Register, Registrar};
use std::sync::Arc;

pub(crate) struct Muse;

pub(crate) const IDENTITY: Identity = Identity {
    id: "muse",
    source: "muse",
    aliases: &["muse-code"],
    display_name: "Muse Code",
};

impl Agent for Muse {
    fn identity(&self) -> &'static Identity {
        &IDENTITY
    }
}

impl Register for Muse {
    fn register(self: Arc<Self>, registrar: &mut Registrar) {
        registrar.setup.insert(self.clone());
        registrar.run.insert(self.clone());
        registrar.translate.insert(self);
    }
}
