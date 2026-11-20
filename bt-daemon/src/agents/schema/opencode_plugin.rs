use serde::{Deserialize, Serialize};
use serde_json::Value;

/// OpenCode also accepts plugin tuples with provider-specific options.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum OpenCodePlugin {
    Package(String),
    Configured((String, Value)),
}
impl OpenCodePlugin {
    pub fn package(&self) -> Option<&str> {
        match self {
            Self::Package(package) => Some(package),
            Self::Configured(_) => None,
        }
    }
}

impl OpenCodePlugin {
    /// Match a plain package entry, including a version suffix. Configured
    /// tuples carry caller-owned options and are deliberately not replaced.
    pub fn matches_package(&self, package: &str) -> bool {
        self.package().is_some_and(|entry| {
            entry == package
                || entry
                    .strip_prefix(package)
                    .is_some_and(|suffix| suffix.starts_with('@'))
        })
    }
}
