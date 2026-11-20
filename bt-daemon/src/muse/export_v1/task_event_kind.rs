use serde::Deserialize;

#[derive(Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TaskEventKind {
    Scheduled,
    Started,
    Completed,
    Failed,
    #[serde(alias = "canceled")]
    Cancelled,
    Rejected,
    #[default]
    #[serde(other)]
    Ignored,
}
