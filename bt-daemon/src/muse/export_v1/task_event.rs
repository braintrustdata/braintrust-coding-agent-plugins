use super::{TaskEventKind, TaskOrigin};
use serde::Deserialize;

#[derive(Deserialize)]
pub(crate) struct TaskEvent {
    #[serde(default)]
    pub kind: TaskEventKind,
    pub task_id: Option<String>,
    pub idempotency_key: Option<String>,
    pub origin: Option<TaskOrigin>,
    pub reason: Option<String>,
}
