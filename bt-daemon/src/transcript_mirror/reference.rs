use std::path::Path;

/// Journaled transcript reference. An empty object records captured absence.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub(crate) struct Reference {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mirror: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub through: Option<u64>,
}

impl Reference {
    pub fn captured(path: &str, mirror: &Path, through: u64) -> Self {
        Self {
            path: Some(path.to_owned()),
            mirror: Some(mirror.to_string_lossy().into_owned()),
            through: Some(through),
        }
    }
    pub fn read_path(&self) -> Option<&str> {
        self.mirror.as_deref().or(self.path.as_deref())
    }

    /// Only a daemon mirror with a recorded byte boundary is safe for a
    /// captured read. An empty reference records absence, not a live fallback.
    pub fn captured_prefix(&self) -> Option<(&str, u64)> {
        Some((self.mirror.as_deref()?, self.through?))
    }

    pub fn matches_path(&self, path: &str) -> bool {
        self.path.as_deref() == Some(path)
    }
}
