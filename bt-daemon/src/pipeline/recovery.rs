//! Failure records, recovery conditions, and waiting for eligibility.
//!
//! A failure belongs to the blocked machine. Its recovery condition is
//! scenario-specific: a restart is not a recovery condition, so a failure
//! persisted before a crash resumes waiting for the same condition. A met
//! condition only makes a retry eligible; only successful processing clears
//! the failure.

use super::types::now_ms;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::Duration;

const BACKOFF_BASE_MS: i64 = 1_000;
const BACKOFF_CAP_MS: i64 = 5 * 60 * 1_000;
/// Bounded recheck interval covering file-watcher gaps and external changes
/// the daemon cannot observe directly.
const FILE_CHECK_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum FailureCause {
    TranslationShape {
        source: String,
        node: u64,
        revision: String,
    },
    TranscriptRead {
        path: PathBuf,
    },
    Plugin {
        path: PathBuf,
        index: usize,
        config_version: u64,
    },
    Credentials {
        selection: String,
    },
    Transport {
        endpoint: String,
    },
    RateLimit {
        destination: String,
    },
    Permission {
        destination: String,
        status: Option<u16>,
    },
    LocalPersistence {
        operation: String,
    },
    PermanentRejection {
        status: Option<u16>,
        config_version: u64,
    },
}

impl FailureCause {
    pub(crate) fn label(&self) -> &'static str {
        match self {
            Self::TranslationShape { .. } => "translation",
            Self::TranscriptRead { .. } => "transcript read",
            Self::Plugin { .. } => "span plugin",
            Self::Credentials { .. } => "credentials",
            Self::Transport { .. } => "transport",
            Self::RateLimit { .. } => "rate limit",
            Self::Permission { .. } => "permission",
            Self::LocalPersistence { .. } => "local persistence",
            Self::PermanentRejection { .. } => "rejected",
        }
    }
}

/// Evidence that makes a retry eligible. Any one condition suffices.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum RecoveryCondition {
    /// A different translator revision is running.
    TranslatorRevision { failed: String },
    /// The file's content differs from the recorded digest, or its existence
    /// changed. `None` records that the file was missing.
    FileChanged {
        path: PathBuf,
        digest: Option<String>,
    },
    /// The branch's route configuration version changed.
    ConfigChanged { version: u64 },
    /// A capped exponential delay expires. Each failed attempt doubles it.
    Backoff,
    /// A server-provided time passes.
    RetryAfter { at_ms: i64 },
}

/// A failure as reported by a processor.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct Failure {
    pub(crate) cause: FailureCause,
    /// Full local diagnostic. Never sent to the backend.
    pub(crate) diagnostic: String,
    /// What will unblock it, for doctor.
    pub(crate) guidance: String,
    /// Whether the condition can be met without user action.
    pub(crate) automatic: bool,
    pub(crate) conditions: Vec<RecoveryCondition>,
}

/// The plugin failure marker published for this failure, if any.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct MarkerState {
    pub(crate) span_id: String,
    pub(crate) published: bool,
}

/// The persisted record of a machine's active failure.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct FailureRecord {
    pub(crate) failure: Failure,
    pub(crate) occurrences: u32,
    pub(crate) first_seen_ms: i64,
    pub(crate) last_seen_ms: i64,
    pub(crate) next_attempt_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) marker: Option<MarkerState>,
}

impl FailureRecord {
    /// Record `failure`, continuing the previous record when it is the same
    /// failure so occurrences and backoff accumulate.
    pub(crate) fn record(previous: Option<&FailureRecord>, failure: Failure) -> Self {
        let now = now_ms();
        let same = previous.filter(|previous| previous.failure.cause == failure.cause);
        let occurrences = same.map_or(1, |previous| previous.occurrences.saturating_add(1));
        let next_attempt_ms = failure
            .conditions
            .iter()
            .filter_map(|condition| match condition {
                RecoveryCondition::Backoff => Some(now + backoff_ms(occurrences)),
                RecoveryCondition::RetryAfter { at_ms } => Some(*at_ms),
                _ => None,
            })
            .min();
        Self {
            failure,
            occurrences,
            first_seen_ms: same.map_or(now, |previous| previous.first_seen_ms),
            last_seen_ms: now,
            next_attempt_ms,
            marker: same.and_then(|previous| previous.marker.clone()),
        }
    }

    pub(crate) fn encode(&self) -> String {
        serde_json::to_string(self).expect("failure records serialize")
    }

    pub(crate) fn decode(text: &str) -> anyhow::Result<Self> {
        Ok(serde_json::from_str(text)?)
    }

    pub(crate) fn summary(&self) -> String {
        format!(
            "{} failure ({} attempt{}): {}",
            self.failure.cause.label(),
            self.occurrences,
            if self.occurrences == 1 { "" } else { "s" },
            self.failure.diagnostic
        )
    }
}

fn backoff_ms(occurrences: u32) -> i64 {
    let exponent = occurrences.saturating_sub(1).min(20);
    BACKOFF_BASE_MS
        .saturating_mul(1i64 << exponent)
        .min(BACKOFF_CAP_MS)
}

/// The content digest of a file, or `None` when it does not exist.
pub(crate) fn file_digest(path: &Path) -> Option<String> {
    use sha2::{Digest, Sha256};
    let bytes = std::fs::read(path).ok()?;
    Some(format!("{:x}", Sha256::digest(bytes)))
}

/// Live inputs the evaluator observes besides time and files.
pub(crate) trait RecoveryContext: Send + Sync {
    /// The running translator revision: the embedding `bt` version. A
    /// different revision (an upgrade) makes a translation failure eligible.
    fn translator_revision(&self) -> &str;

    /// The current route configuration version, for branch failures.
    fn config_version(&self) -> Option<u64> {
        None
    }
}

/// Whether any recorded condition holds now. Conditions are checked against
/// the recorded evidence, never assumed.
pub(crate) fn eligible(record: &FailureRecord, context: &dyn RecoveryContext) -> bool {
    let now = now_ms();
    record
        .failure
        .conditions
        .iter()
        .any(|condition| match condition {
            RecoveryCondition::TranslatorRevision { failed } => {
                failed != context.translator_revision()
            }
            RecoveryCondition::FileChanged { path, digest } => &file_digest(path) != digest,
            RecoveryCondition::ConfigChanged { version } => context
                .config_version()
                .is_some_and(|current| current != *version),
            RecoveryCondition::Backoff | RecoveryCondition::RetryAfter { .. } => record
                .next_attempt_ms
                .is_some_and(|next_attempt| now >= next_attempt),
        })
}

/// Wait until the record's condition holds, waking early on `notify`, which
/// owners signal when relevant state (such as route configuration) changes.
pub(crate) async fn wait_until_eligible(
    record: &FailureRecord,
    context: &dyn RecoveryContext,
    notify: &tokio::sync::Notify,
) {
    loop {
        if eligible(record, context) {
            return;
        }
        let watches_files = record
            .failure
            .conditions
            .iter()
            .any(|condition| matches!(condition, RecoveryCondition::FileChanged { .. }));
        let mut delay = Duration::from_secs(60);
        if watches_files {
            delay = delay.min(FILE_CHECK_INTERVAL);
        }
        if let Some(next_attempt) = record.next_attempt_ms {
            let remaining = next_attempt.saturating_sub(now_ms()).max(0) as u64;
            delay = delay.min(Duration::from_millis(remaining));
        }
        tokio::select! {
            _ = notify.notified() => {}
            _ = tokio::time::sleep(delay) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct NoContext;
    impl RecoveryContext for NoContext {
        fn translator_revision(&self) -> &str {
            "1.0.0"
        }
    }

    fn plugin_failure(path: &Path) -> Failure {
        Failure {
            cause: FailureCause::Plugin {
                path: path.to_path_buf(),
                index: 0,
                config_version: 1,
            },
            diagnostic: "boom".into(),
            guidance: "fix it".into(),
            automatic: false,
            conditions: vec![RecoveryCondition::FileChanged {
                path: path.to_path_buf(),
                digest: file_digest(path),
            }],
        }
    }

    #[test]
    fn file_conditions_hold_only_after_that_file_changes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("plugin.js");
        let other = dir.path().join("other.js");
        std::fs::write(&path, "a").unwrap();
        let record = FailureRecord::record(None, plugin_failure(&path));
        assert!(!eligible(&record, &NoContext));
        std::fs::write(&other, "unrelated").unwrap();
        assert!(!eligible(&record, &NoContext));
        std::fs::write(&path, "b").unwrap();
        assert!(eligible(&record, &NoContext));
    }

    #[test]
    fn repeated_failures_back_off_and_keep_history() {
        let failure = Failure {
            cause: FailureCause::Transport {
                endpoint: "https://api".into(),
            },
            diagnostic: "down".into(),
            guidance: "wait".into(),
            automatic: true,
            conditions: vec![RecoveryCondition::Backoff],
        };
        let first = FailureRecord::record(None, failure.clone());
        let second = FailureRecord::record(Some(&first), failure);
        assert_eq!(second.occurrences, 2);
        assert_eq!(second.first_seen_ms, first.first_seen_ms);
        assert!(
            second.next_attempt_ms.unwrap() - second.last_seen_ms
                > first.next_attempt_ms.unwrap() - first.last_seen_ms
        );
        assert_eq!(backoff_ms(100), BACKOFF_CAP_MS);
    }

    #[test]
    fn revision_conditions_need_a_different_binary() {
        let record = FailureRecord::record(
            None,
            Failure {
                cause: FailureCause::TranslationShape {
                    source: "codex".into(),
                    node: 3,
                    revision: "1.0.0".into(),
                },
                diagnostic: "bad".into(),
                guidance: "upgrade".into(),
                automatic: false,
                conditions: vec![RecoveryCondition::TranslatorRevision {
                    failed: "1.0.0".into(),
                }],
            },
        );
        assert!(!eligible(&record, &NoContext));
        let mut upgraded = record.clone();
        upgraded.failure.conditions = vec![RecoveryCondition::TranslatorRevision {
            failed: "0.0.0-old".into(),
        }];
        assert!(eligible(&upgraded, &NoContext));
    }
}
