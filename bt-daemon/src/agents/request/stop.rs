use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
pub(super) enum Stop {
    One(String),
    Many(Vec<String>),
}
