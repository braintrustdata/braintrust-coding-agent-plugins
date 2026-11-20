use serde::Serialize;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DaemonStatus {
    Running,
    #[default]
    NotRunning,
    Unreachable,
}

impl std::fmt::Display for DaemonStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Running => "running",
            Self::NotRunning => "not running",
            Self::Unreachable => "unreachable",
        })
    }
}
