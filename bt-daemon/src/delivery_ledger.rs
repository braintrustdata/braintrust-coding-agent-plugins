//! Reader for the file-based delivery ledger released on `main`.
//!
//! The pipeline records deliveries in its own database. A destination that was
//! delivered to before the upgrade has its ledger file imported once, by the
//! branch that now owns that destination.

use crate::translate::OriginSnapshot;
use crate::wire::SessionConfig;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::path::Path;

#[derive(serde::Deserialize, Default)]
struct LedgerFile {
    #[serde(default, alias = "terminal_span_ids")]
    completed_span_ids: HashSet<String>,
    #[serde(default, alias = "late_merge_span_ids")]
    late_merge_ids: HashSet<String>,
    #[serde(default)]
    span_origins: HashMap<String, OriginSnapshot>,
}

/// Load the `main`-era ledger file for one destination, if there is one.
async fn load(
    data_dir: &Path,
    source: &str,
    session_id: &str,
    config: &SessionConfig,
) -> anyhow::Result<LedgerFile> {
    let fingerprint = serde_json::json!({
        "api_url": config.auth.api_url,
        "org_id": config.auth.org_id,
        "org_name": config.auth.org_name,
        "destination": config.destination,
    });
    let digest = Sha256::digest(serde_json::to_vec(&fingerprint)?);
    let fingerprint_id = format!("{digest:x}");
    let path = data_dir.join("delivery-ledger").join(format!(
        "{}--{}.json",
        crate::ids::session_storage_id(source, session_id),
        &fingerprint_id[..32]
    ));
    match tokio::fs::read(&path).await {
        Ok(bytes) => Ok(serde_json::from_slice::<LedgerFile>(&bytes)?),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(LedgerFile::default()),
        Err(error) => Err(error.into()),
    }
}

/// Rows of a `main`-era destination ledger file, imported once by the
/// delivery branch that now owns that destination.
pub(crate) struct LegacyLedgerRows {
    pub(crate) completed: HashSet<String>,
    pub(crate) late_merges: HashSet<String>,
    pub(crate) origins: HashMap<String, OriginSnapshot>,
}

pub(crate) async fn legacy_rows(
    data_dir: &Path,
    source: &str,
    session_id: &str,
    config: &SessionConfig,
) -> Option<LegacyLedgerRows> {
    let ledger = load(data_dir, source, session_id, config).await.ok()?;
    if ledger.completed_span_ids.is_empty()
        && ledger.late_merge_ids.is_empty()
        && ledger.span_origins.is_empty()
    {
        return None;
    }
    Some(LegacyLedgerRows {
        completed: ledger.completed_span_ids,
        late_merges: ledger.late_merge_ids,
        origins: ledger.span_origins,
    })
}
