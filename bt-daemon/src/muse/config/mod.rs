//! Typed native settings, preservable hook trees, and strict owned-file detection.
mod hooks;
mod ownership;
mod settings;

pub(crate) use hooks::HookOverlay;
#[cfg(test)]
pub(crate) use ownership::OwnedHookFile;
pub(crate) use ownership::{inspect_hook_file, HookFileInspection};
pub(crate) use settings::MuseSettings;

pub(crate) const HOOK_COMMAND: &str = "bt trace hook --source muse --flush-on-turn-end";
const LEGACY_HOOK_COMMAND: &str = "bt trace hook --source muse --flush-on-turn-end || true";
