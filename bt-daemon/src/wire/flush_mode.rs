use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FlushMode {
    /// Deliver in the background; flush on session end / idle. The default.
    #[default]
    FireAndForget,
    /// Additionally block on `session.flush` at each turn boundary.
    FlushOnTurnEnd,
}
