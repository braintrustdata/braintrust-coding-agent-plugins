use serde::Deserialize;

#[derive(Deserialize)]
#[serde(untagged)]
pub(crate) enum Version {
    String(String),
    Object { semver: String },
}

impl Version {
    pub(crate) fn into_string(self) -> String {
        match self {
            Self::String(value) | Self::Object { semver: value } => value,
        }
    }
}
