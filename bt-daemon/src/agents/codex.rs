use super::{Agent, Identity, Register, Registrar};
use std::sync::Arc;

pub(crate) struct Codex;

pub(crate) const IDENTITY: Identity = Identity {
    id: "codex",
    source: "codex",
    aliases: &[],
    display_name: "Codex",
};

impl Agent for Codex {
    fn identity(&self) -> &'static Identity {
        &IDENTITY
    }
}

impl Register for Codex {
    fn register(self: Arc<Self>, registrar: &mut Registrar) {
        registrar.setup.insert(self.clone());
        registrar.run.insert(self.clone());
        registrar.translate.insert(self);
    }
}
