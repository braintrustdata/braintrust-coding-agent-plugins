use serde::Deserialize;

#[derive(Clone, Deserialize)]
pub(crate) struct RunStream {
    pub id: String,
}
