use super::{Agent, Identity, Register, Registrar};
use std::sync::Arc;

pub(crate) struct Claude;

pub(crate) const IDENTITY: Identity = Identity {
    id: "claude",
    source: "claude-code",
    aliases: &[],
    display_name: "Claude Code",
};

impl Agent for Claude {
    fn identity(&self) -> &'static Identity {
        &IDENTITY
    }
}

impl Register for Claude {
    fn register(self: Arc<Self>, registrar: &mut Registrar) {
        registrar.setup.insert(self);
    }
}
