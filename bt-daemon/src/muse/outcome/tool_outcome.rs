#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ToolOutcome {
    Success,
    Failure(String),
    Unknown,
}
