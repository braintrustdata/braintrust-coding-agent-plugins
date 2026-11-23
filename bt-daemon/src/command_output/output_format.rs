#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    Human,
    Json,
}

impl From<bool> for OutputFormat {
    fn from(json: bool) -> Self {
        if json {
            Self::Json
        } else {
            Self::Human
        }
    }
}
