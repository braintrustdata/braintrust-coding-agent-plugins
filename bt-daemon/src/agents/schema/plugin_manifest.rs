use super::Repository;
use serde::Deserialize;

/// The manifest identity fields used by setup and capture version detection.
#[derive(Debug, Deserialize)]
pub(crate) struct PluginManifest {
    pub name: Option<String>,
    pub version: Option<String>,
    pub repository: Option<Repository>,
}
