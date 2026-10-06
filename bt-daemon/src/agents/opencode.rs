use super::{Agent, Identity, Register, Registrar};
use std::sync::Arc;

pub(crate) struct OpenCode;

pub(crate) const IDENTITY: Identity = Identity {
    id: "opencode",
    source: "opencode",
    aliases: &["open-code"],
    display_name: "OpenCode",
};

impl Agent for OpenCode {
    fn identity(&self) -> &'static Identity {
        &IDENTITY
    }
}

impl Register for OpenCode {
    fn register(self: Arc<Self>, registrar: &mut Registrar) {
        registrar.setup.insert(self.clone());
        registrar.run.insert(self);
    }
}
