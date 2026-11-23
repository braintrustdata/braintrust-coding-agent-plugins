//! Schemas shared by capture, setup, managed runs, and transcript import.
//!
//! Unknown extension fields are preserved when editing an agent-owned file.
//! Known fields are decoded here and used as concrete Rust values internally.
use serde::Deserialize;

mod command_hook;
mod cursor_command_hook;
mod cursor_hook_entries;
mod cursor_hook_entry;
mod cursor_hooks;
mod hook_file;
mod hook_group;
mod opencode_config;
mod opencode_plugin;
mod plugin_manifest;
mod repository;

pub(crate) use command_hook::CommandHook;
pub(crate) use cursor_command_hook::CursorCommandHook;
pub(crate) use cursor_hook_entries::CursorHookEntries;
pub(crate) use cursor_hook_entry::CursorHookEntry;
pub(crate) use cursor_hooks::CursorHooks;
pub(crate) use hook_file::HookFile;
pub(crate) use hook_group::HookGroup;
pub(crate) use opencode_config::OpenCodeConfig;
pub(crate) use opencode_plugin::OpenCodePlugin;
pub(crate) use plugin_manifest::PluginManifest;
pub(crate) use repository::Repository;

/// Preserve explicit null for opaque JSON fields while a missing field remains
/// None. Used only when native null has different semantics from absence.
pub(crate) fn deserialize_present<'de, T: Deserialize<'de>, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<T>, D::Error> {
    T::deserialize(deserializer).map(Some)
}

#[cfg(test)]
mod tests;
