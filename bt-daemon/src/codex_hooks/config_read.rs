use super::config::Config;
use serde::Deserialize;

#[derive(Deserialize)]
pub(super) struct ConfigRead {
    pub(super) config: Config,
}
