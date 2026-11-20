use serde::{Deserialize, Serialize};

/// Pi normalizes provider usage before forwarding events to the daemon.
#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Serialize)]
pub(crate) struct PiUsage {
    #[serde(default)]
    pub input: i64,
    #[serde(default)]
    pub output: i64,
    // Pi reports `reasoning` as a subset of `output`, and only for providers
    // that expose the breakdown.
    #[serde(default)]
    pub reasoning: Option<i64>,
    #[serde(default)]
    pub cache_read: i64,
    // `cacheWrite1h` is the subset of `cacheWrite` written with 1h retention.
    #[serde(default)]
    pub cache_write: i64,
    #[serde(default)]
    pub cache_write1h: Option<i64>,
    #[serde(default)]
    pub total_tokens: Option<i64>,
    #[serde(default)]
    pub cost: Option<PiCost>,
}

#[derive(Default, Deserialize, Serialize)]
pub(crate) struct PiCost {
    #[serde(default)]
    pub total: Option<f64>,
}
