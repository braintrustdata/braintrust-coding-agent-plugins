use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum Repository {
    Url(String),
    Package {
        #[serde(rename = "type")]
        _kind: Option<String>,
        #[serde(rename = "url")]
        _url: String,
    },
}
impl Repository {
    pub fn url(&self) -> Option<&str> {
        match self {
            Self::Url(url) => Some(url),
            Self::Package { .. } => None,
        }
    }
}
