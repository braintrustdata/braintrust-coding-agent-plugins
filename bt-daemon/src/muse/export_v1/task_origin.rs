use serde::Deserialize;

#[derive(Deserialize)]
pub(crate) struct TaskOrigin {
    pub tool_call_id: Option<String>,
}
