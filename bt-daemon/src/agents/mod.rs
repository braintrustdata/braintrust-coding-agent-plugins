//! One type per supported coding agent.
//!
//! Each agent implements the capability traits it supports, such as
//! [`Setup`], and registers itself with the matching registries in
//! [`Register::register`]. A command looks up agents in its own registry, so
//! an agent without a capability simply does not appear there.

mod antigravity;
mod claude;
mod codex;
mod cursor;
mod grok;
mod opencode;
mod pi;

pub(crate) use antigravity::Antigravity;
pub(crate) use claude::Claude;
pub(crate) use codex::Codex;
pub(crate) use cursor::Cursor;
pub(crate) use grok::Grok;
pub(crate) use opencode::OpenCode;
pub(crate) use pi::Pi;

use crate::managed_run::ManagedRun;
use crate::setup::Setup;
use crate::translate::Translate;
use std::sync::{Arc, OnceLock};

/// The names an agent is known by.
#[derive(Debug)]
pub(crate) struct Identity {
    /// The CLI name, which also names the agent's settings file.
    pub id: &'static str,
    /// The `source` its hooks report and its translator is registered under.
    pub source: &'static str,
    /// Other accepted spellings.
    pub aliases: &'static [&'static str],
    pub display_name: &'static str,
}

impl Identity {
    pub fn matches(&self, name: &str) -> bool {
        self.id == name || self.source == name || self.aliases.contains(&name)
    }
}

/// Implemented by every agent type and required by every capability trait,
/// so any registry can find an agent by name.
pub(crate) trait Agent: Send + Sync + 'static {
    fn identity(&self) -> &'static Identity;
}

/// The agents that support one capability.
pub(crate) struct Agents<T: ?Sized> {
    agents: Vec<Arc<T>>,
}

impl<T: ?Sized> Default for Agents<T> {
    fn default() -> Self {
        Self { agents: Vec::new() }
    }
}

impl<T: ?Sized + Agent> Agents<T> {
    pub fn insert(&mut self, agent: Arc<T>) {
        self.agents.push(agent);
    }

    /// Find an agent by its id, source, or an alias.
    pub fn get(&self, name: &str) -> Option<&T> {
        self.agents
            .iter()
            .find(|agent| agent.identity().matches(name))
            .map(|agent| &**agent)
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.agents.iter().map(|agent| &**agent)
    }
}

/// Every capability registry. Agents add themselves in [`Register::register`].
#[derive(Default)]
pub(crate) struct Registrar {
    pub setup: Agents<dyn Setup>,
    pub run: Agents<dyn ManagedRun>,
    pub translate: Agents<dyn Translate>,
}

pub(crate) trait Register {
    fn register(self: Arc<Self>, registrar: &mut Registrar);
}

/// The registries for all supported agents.
pub(crate) fn registrar() -> &'static Registrar {
    static REGISTRAR: OnceLock<Registrar> = OnceLock::new();
    REGISTRAR.get_or_init(|| {
        let mut registrar = Registrar::default();
        Arc::new(Antigravity).register(&mut registrar);
        Arc::new(Claude).register(&mut registrar);
        Arc::new(Codex).register(&mut registrar);
        Arc::new(Cursor).register(&mut registrar);
        Arc::new(Grok).register(&mut registrar);
        Arc::new(OpenCode).register(&mut registrar);
        Arc::new(Pi).register(&mut registrar);
        registrar
    })
}
