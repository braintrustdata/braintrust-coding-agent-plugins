//! Durable, failure-mode-neutral pause records for journal-backed work.
//!
//! An incident describes the first unprocessed position and the condition
//! that must change before retry. Diagnostics may retain resolved incidents,
//! but active work has no permanent "recovered" processing mode.

use crate::wire::SessionRoute;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

const FILE_NAME: &str = "recovery.json";
const MAX_RESOLVED: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkScope {
    SourceSession {
        source: String,
        session_id: String,
    },
    DeliveryRoute {
        source: String,
        session_id: String,
        route_id: String,
    },
}

impl WorkScope {
    pub fn delivery(source: &str, session_id: &str, route: &SessionRoute) -> anyhow::Result<Self> {
        let mut stable_route = route.clone();
        stable_route.auth.source = stable_route.auth.effective_source();
        stable_route.auth.profile_id = None;
        let route_id = format!("{:x}", Sha256::digest(serde_json::to_vec(&stable_route)?));
        Ok(Self::DeliveryRoute {
            source: source.into(),
            session_id: session_id.into(),
            route_id,
        })
    }

    pub fn source_session(&self) -> (&str, &str) {
        match self {
            Self::SourceSession { source, session_id }
            | Self::DeliveryRoute {
                source, session_id, ..
            } => (source, session_id),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FailureCause {
    InputShape {
        event: String,
        translator_revision: String,
    },
    TranslatorFault {
        translator_revision: String,
    },
    LocalStorage {
        retry_after_ms: i64,
    },
    PluginFile {
        path: PathBuf,
        digest: Option<String>,
        plugin_index: usize,
    },
    Credentials {
        selection: crate::wire::AuthSelection,
        #[serde(default)]
        retry_after_ms: i64,
    },
    Destination {
        destination: String,
        #[serde(default)]
        retry_after_ms: i64,
    },
    Permission {
        destination: String,
        #[serde(default)]
        retry_after_ms: i64,
    },
    RateLimited {
        retry_after_ms: i64,
    },
    Transport {
        endpoint: String,
        retry_after_ms: i64,
    },
    PermanentDelivery {
        sink_revision: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkState {
    Paused,
    Reprocessing,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Incident {
    pub scope: WorkScope,
    pub state: WorkState,
    /// Cursor immediately before blocked work: an event WAL byte offset for a
    /// source scope, or a span ledger sequence for a delivery scope.
    pub first_unprocessed: u64,
    /// Operation index within the blocked span revision.
    pub operation_index: u32,
    pub cause: FailureCause,
    pub local_error: String,
    pub marker_span_id: Option<String>,
    pub first_seen_ms: i64,
    pub last_seen_ms: i64,
    pub attempts: u64,
    pub resolved_at_ms: Option<i64>,
}

#[derive(Default, Serialize, Deserialize)]
struct Store {
    #[serde(default)]
    active: Vec<Incident>,
    #[serde(default)]
    resolved: Vec<Incident>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckResult {
    Unchanged,
    RetryCandidate,
}

impl FailureCause {
    /// Local checks are pure eligibility signals. Work succeeds only after
    /// replay and acknowledged delivery move the cursor forward.
    pub fn check_local(&self, translator_revision: &str, now_ms: i64) -> CheckResult {
        let ready = match self {
            Self::InputShape {
                translator_revision: failed,
                ..
            }
            | Self::TranslatorFault {
                translator_revision: failed,
            } => failed != translator_revision,
            Self::PluginFile { path, digest, .. } => &plugin_digest(path) != digest,
            Self::LocalStorage { retry_after_ms }
            | Self::RateLimited { retry_after_ms }
            | Self::Transport { retry_after_ms, .. }
            | Self::Destination { retry_after_ms, .. }
            | Self::Permission { retry_after_ms, .. } => now_ms >= *retry_after_ms,
            Self::Credentials { .. } => false,
            Self::PermanentDelivery { sink_revision } => sink_revision != translator_revision,
        };
        if ready {
            CheckResult::RetryCandidate
        } else {
            CheckResult::Unchanged
        }
    }
}

pub fn plugin_digest(path: &Path) -> Option<String> {
    std::fs::read(path)
        .ok()
        .map(|bytes| format!("{:x}", Sha256::digest(bytes)))
}

pub fn translator_revision() -> &'static str {
    static REVISION: OnceLock<String> = OnceLock::new();
    REVISION.get_or_init(|| {
        let digest = std::env::current_exe()
            .ok()
            .and_then(|path| plugin_digest(&path));
        digest.unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_owned())
    })
}

pub fn classify_delivery_error(
    error: &anyhow::Error,
    route: &SessionRoute,
    api_url: Option<&str>,
    attempts: u64,
) -> FailureCause {
    if error
        .downcast_ref::<braintrust_sdk_rust::BraintrustError>()
        .is_none()
        && crate::derived::retryable_storage(error)
    {
        return FailureCause::LocalStorage {
            retry_after_ms: now_ms() + 1_000,
        };
    }

    let now = now_ms();
    let delay_ms = (1_000i64.saturating_mul(1i64 << attempts.min(6))).min(60_000);
    let destination = route
        .destination
        .as_ref()
        .and_then(|value| serde_json::to_string(value).ok())
        .unwrap_or_else(|| "missing destination".into());
    match error.downcast_ref::<braintrust_sdk_rust::BraintrustError>() {
        Some(braintrust_sdk_rust::BraintrustError::Api { status: 401, .. }) => {
            FailureCause::Credentials {
                selection: route.auth.clone(),
                retry_after_ms: now + delay_ms,
            }
        }
        Some(braintrust_sdk_rust::BraintrustError::Api { status: 403, .. }) => {
            FailureCause::Permission {
                destination,
                retry_after_ms: now + delay_ms.max(30_000),
            }
        }
        Some(braintrust_sdk_rust::BraintrustError::Api { status: 404, .. }) => {
            FailureCause::Destination {
                destination,
                retry_after_ms: now + delay_ms.max(30_000),
            }
        }
        Some(braintrust_sdk_rust::BraintrustError::Api { status: 429, .. }) => {
            FailureCause::RateLimited {
                retry_after_ms: now + delay_ms,
            }
        }
        Some(braintrust_sdk_rust::BraintrustError::Api { status, .. }) if *status >= 500 => {
            FailureCause::Transport {
                endpoint: api_url.unwrap_or_default().into(),
                retry_after_ms: now + delay_ms,
            }
        }
        Some(braintrust_sdk_rust::BraintrustError::Api { .. }) => FailureCause::PermanentDelivery {
            sink_revision: translator_revision().into(),
        },
        Some(braintrust_sdk_rust::BraintrustError::Http(_))
        | Some(braintrust_sdk_rust::BraintrustError::Network(_))
        | Some(braintrust_sdk_rust::BraintrustError::Background(_))
        | None => FailureCause::Transport {
            endpoint: api_url.unwrap_or_default().into(),
            retry_after_ms: now + delay_ms,
        },
        Some(_) => FailureCause::PermanentDelivery {
            sink_revision: translator_revision().into(),
        },
    }
}

pub fn active(data_dir: &Path, scope: &WorkScope) -> anyhow::Result<Option<Incident>> {
    read(data_dir, |store| {
        Ok(store.active.iter().find(|v| &v.scope == scope).cloned())
    })
}

pub fn all_active(data_dir: &Path) -> anyhow::Result<Vec<Incident>> {
    read(data_dir, |store| Ok(store.active.clone()))
}

pub fn pause(
    data_dir: &Path,
    scope: WorkScope,
    first_unprocessed: u64,
    operation_index: u32,
    cause: FailureCause,
    local_error: String,
    marker_span_id: Option<String>,
) -> anyhow::Result<Incident> {
    locked(data_dir, |store| {
        let now = now_ms();
        let incident = if let Some(existing) = store.active.iter_mut().find(|v| v.scope == scope) {
            existing.first_unprocessed = existing.first_unprocessed.min(first_unprocessed);
            existing.operation_index = operation_index;
            existing.cause = cause;
            existing.local_error = local_error;
            existing.marker_span_id = marker_span_id;
            existing.last_seen_ms = now;
            existing.state = WorkState::Paused;
            existing.clone()
        } else {
            let incident = Incident {
                scope,
                state: WorkState::Paused,
                first_unprocessed,
                operation_index,
                cause,
                local_error,
                marker_span_id,
                first_seen_ms: now,
                last_seen_ms: now,
                attempts: 0,
                resolved_at_ms: None,
            };
            store.active.push(incident.clone());
            incident
        };
        Ok(incident)
    })
}

pub fn begin_retry(data_dir: &Path, scope: &WorkScope) -> anyhow::Result<Option<Incident>> {
    locked(data_dir, |store| {
        let Some(incident) = store.active.iter_mut().find(|v| &v.scope == scope) else {
            return Ok(None);
        };
        incident.state = WorkState::Reprocessing;
        incident.attempts = incident.attempts.saturating_add(1);
        incident.last_seen_ms = now_ms();
        Ok(Some(incident.clone()))
    })
}

pub fn resolve(data_dir: &Path, scope: &WorkScope) -> anyhow::Result<()> {
    locked(data_dir, |store| {
        if let Some(index) = store.active.iter().position(|v| &v.scope == scope) {
            let mut incident = store.active.remove(index);
            incident.resolved_at_ms = Some(now_ms());
            store.resolved.push(incident);
            let excess = store.resolved.len().saturating_sub(MAX_RESOLVED);
            store.resolved.drain(..excess);
        }
        Ok(())
    })
}

fn path(data_dir: &Path) -> PathBuf {
    data_dir.join("diagnostics").join(FILE_NAME)
}

fn read_unlocked(path: &Path) -> anyhow::Result<Store> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Store::default()),
        Err(error) => Err(error.into()),
    }
}

fn read<T>(data_dir: &Path, f: impl FnOnce(&Store) -> anyhow::Result<T>) -> anyhow::Result<T> {
    let path = path(data_dir);
    crate::paths::ensure_private_dir(path.parent().expect("recovery path has parent"))?;
    crate::settings::with_settings_lock(&path, || f(&read_unlocked(&path)?))
}

fn locked<T>(
    data_dir: &Path,
    f: impl FnOnce(&mut Store) -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    let path = path(data_dir);
    crate::paths::ensure_private_dir(path.parent().expect("recovery path has parent"))?;
    crate::settings::with_settings_lock(&path, || {
        let mut store = read_unlocked(&path)?;
        let result = f(&mut store)?;
        let mut temporary = tempfile::NamedTempFile::new_in(path.parent().unwrap())?;
        serde_json::to_writer_pretty(&mut temporary, &store)?;
        temporary.write_all(b"\n")?;
        temporary.persist(&path).map_err(|error| error.error)?;
        Ok(result)
    })
}

pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retries_only_the_failed_plugin_when_its_bytes_change() {
        let temp = tempfile::tempdir().unwrap();
        let failed = temp.path().join("failed.mjs");
        let other = temp.path().join("other.mjs");
        std::fs::write(&failed, b"throw Error('bad')").unwrap();
        std::fs::write(&other, b"export default x => x").unwrap();
        let cause = FailureCause::PluginFile {
            path: failed.clone(),
            digest: plugin_digest(&failed),
            plugin_index: 1,
        };
        std::fs::write(&other, b"export default x => ({...x})").unwrap();
        assert_eq!(cause.check_local("v1", now_ms()), CheckResult::Unchanged);
        std::fs::write(&failed, b"export default x => x").unwrap();
        assert_eq!(
            cause.check_local("v1", now_ms()),
            CheckResult::RetryCandidate
        );
    }

    #[test]
    fn incident_persists_and_resolves_without_active_recovered_state() {
        let temp = tempfile::tempdir().unwrap();
        let scope = WorkScope::SourceSession {
            source: "pi".into(),
            session_id: "one".into(),
        };
        pause(
            temp.path(),
            scope.clone(),
            17,
            0,
            FailureCause::InputShape {
                event: "message_end".into(),
                translator_revision: "v1".into(),
            },
            "missing required message".into(),
            None,
        )
        .unwrap();
        assert_eq!(
            active(temp.path(), &scope)
                .unwrap()
                .unwrap()
                .first_unprocessed,
            17
        );
        begin_retry(temp.path(), &scope).unwrap();
        assert_eq!(active(temp.path(), &scope).unwrap().unwrap().attempts, 1);
        resolve(temp.path(), &scope).unwrap();
        assert!(active(temp.path(), &scope).unwrap().is_none());
    }

    #[test]
    fn delivery_scope_survives_profile_id_canonicalization() {
        let mut route = SessionRoute::default();
        route.auth.profile = Some("work".into());
        let original = WorkScope::delivery("pi", "one", &route).unwrap();
        route.auth.profile_id = Some("profile-id".into());
        let canonical = WorkScope::delivery("pi", "one", &route).unwrap();
        assert_eq!(original, canonical);
    }
}
