//! Failure outcomes wait for explicit evidence before re-entering processing.
use super::{plugins::PluginRejected, Stage};
use crate::wire::{RecoveryInfo, SessionConfig};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Boundary {
    Plugin,
    Sink,
    Credentials,
    Translation,
    Journal,
    Ledger,
    Completion,
}
impl std::fmt::Display for Boundary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Plugin => "plugin",
            Self::Sink => "sink",
            Self::Credentials => "credentials",
            Self::Translation => "translation",
            Self::Journal => "journal",
            Self::Ledger => "delivery_ledger",
            Self::Completion => "completion",
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "when", rename_all = "snake_case")]
pub(crate) enum Condition {
    Plugin {
        path: PathBuf,
        digest: Option<[u8; 32]>,
        chain: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        processors: Option<Vec<Option<[u8; 32]>>>,
    },
    Configuration {
        signature: String,
        recheck_ms: Option<i64>,
    },
    Credentials {
        recheck_ms: i64,
    },
    Delay {
        recheck_ms: i64,
    },
    Revision {
        version: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Failure {
    pub boundary: Boundary,
    pub position: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub through: Option<u64>,
    pub reason: String,
    pub attempt: u32,
    pub condition: Condition,
}

pub(crate) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}
fn digest(value: &impl std::fmt::Debug) -> String {
    format!("{:x}", Sha256::digest(format!("{value:?}")))
}
fn chain(config: &SessionConfig) -> String {
    digest(&config.span_plugins)
}
fn configuration(config: &SessionConfig) -> String {
    let processors: Vec<_> = config
        .span_plugins
        .iter()
        .map(|path| {
            (
                format!("{path:?}"),
                crate::span_processor::file_digest(path),
            )
        })
        .collect();
    digest(
        &serde_json::json!({"destination":config.destination,"metadata":config.additional_metadata,
        "tags":config.tags,"processors":processors,"api_url":config.auth.api_url,"app_url":config.auth.app_url,
        "org_name":config.auth.org_name,"org_id":config.auth.org_id}),
    )
}

impl Failure {
    /// Source adapters attach their own diagnostic location to stage failures.
    pub fn at(mut self, position: u64) -> Self {
        self.position = position;
        self
    }

    pub fn delay(boundary: Boundary, position: u64, reason: String, attempt: u32) -> Self {
        let delay = 250i64.saturating_mul(1i64 << attempt.min(8)).min(30_000);
        Self {
            boundary,
            position,
            through: None,
            reason,
            attempt,
            condition: Condition::Delay {
                recheck_ms: now_ms().saturating_add(delay),
            },
        }
    }
    pub fn revision(boundary: Boundary, position: u64, reason: String, version: &str) -> Self {
        Self {
            boundary,
            position,
            through: None,
            reason,
            attempt: 0,
            condition: Condition::Revision {
                version: version.into(),
            },
        }
    }
    pub fn routing(error: &anyhow::Error, config: &SessionConfig, attempt: u32) -> Self {
        let error = error
            .downcast_ref::<super::routing::EmitFailure>()
            .map_or(error, |failure| &failure.cause);
        let mut failure = Self::delay(Boundary::Sink, 0, format!("{error:#}"), attempt);
        if let Some(plugin) = error.downcast_ref::<PluginRejected>() {
            failure.boundary = Boundary::Plugin;
            failure.condition = Condition::Plugin {
                path: plugin.failure.path.clone(),
                digest: plugin.digest,
                chain: chain(config),
                processors: Some(plugin.processors.clone()),
            };
        } else if let Some(error) = error.downcast_ref::<braintrust_sdk_rust::BraintrustError>() {
            use braintrust_sdk_rust::BraintrustError;
            match error {
                BraintrustError::Api { status: 401, .. } => {
                    failure.boundary = Boundary::Credentials;
                    failure.condition = Condition::Credentials {
                        recheck_ms: now_ms().saturating_add(1_000),
                    };
                }
                BraintrustError::Api {
                    status: 403 | 404, ..
                } => {
                    failure.condition = Condition::Configuration {
                        signature: configuration(config),
                        recheck_ms: Some(now_ms().saturating_add(60_000)),
                    }
                }
                BraintrustError::Api {
                    status: 408 | 429 | 500..=599,
                    ..
                }
                | BraintrustError::Http(_)
                | BraintrustError::Network(_)
                | BraintrustError::ChannelClosed => {}
                _ => {
                    failure.condition = Condition::Configuration {
                        signature: configuration(config),
                        recheck_ms: None,
                    }
                }
            }
        }
        if !config.auth.token.is_empty() {
            failure.reason = failure.reason.replace(&config.auth.token, "<redacted>");
        }
        failure
    }
    pub fn credentials(reason: String, attempt: u32) -> Self {
        let mut failure = Self::delay(Boundary::Credentials, 0, reason, attempt);
        if let Condition::Delay { recheck_ms } = failure.condition {
            failure.condition = Condition::Credentials { recheck_ms };
        }
        failure
    }
    pub fn legacy(reason: String, position: u64, config: &SessionConfig) -> Self {
        Self {
            boundary: Boundary::Sink,
            position,
            through: None,
            reason,
            attempt: 0,
            condition: Condition::Configuration {
                signature: configuration(config),
                recheck_ms: None,
            },
        }
    }
    pub fn storage(error: &anyhow::Error, version: &str, attempt: u32) -> Self {
        if error.downcast_ref::<serde_json::Error>().is_some() || error.downcast_ref::<rusqlite::Error>().is_some_and(|error| {
            matches!(error,rusqlite::Error::SqliteFailure(code,_) if matches!(code.code,rusqlite::ErrorCode::DatabaseCorrupt|rusqlite::ErrorCode::NotADatabase))
        }) {
            Self::revision(Boundary::Journal,0,error.to_string(),version)
        } else { Self::delay(Boundary::Journal,0,error.to_string(),attempt) }
    }
    pub fn info(&self, eligible: bool) -> RecoveryInfo {
        let retry = match &self.condition {
            Condition::Plugin { path, .. } => format!(
                "bytes/existence of {} or configured plugin chain changes",
                path.display()
            ),
            Condition::Configuration { recheck_ms, .. } => match recheck_ms {
                Some(when) => format!("relevant settings/processors change or recheck at {when}"),
                None => "relevant settings or processors change".into(),
            },
            Condition::Credentials { recheck_ms } => {
                format!("fresh credential resolution at {recheck_ms}")
            }
            Condition::Delay { recheck_ms } => format!("bounded retry at {recheck_ms}"),
            Condition::Revision { version } => format!("processor revision changes from {version}"),
        };
        RecoveryInfo {
            stage: self.boundary.to_string(),
            position: self.position,
            reason: self.reason.clone(),
            retry,
            eligible,
        }
    }
    pub fn message(&self) -> String {
        let info = self.info(false);
        format!(
            "recovering {} at {}: {}; waiting for {}",
            info.stage, info.position, info.reason, info.retry
        )
    }
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message())
    }
}
impl std::error::Error for Failure {}

pub(crate) enum RecoveryOutcome {
    Waiting,
    Retry,
}
pub(crate) struct Recovery<'a> {
    pub config: Option<&'a SessionConfig>,
    pub version: &'a str,
}
#[async_trait::async_trait]
impl Stage for Recovery<'_> {
    type Input = Failure;
    type Outcome = RecoveryOutcome;
    async fn process(&mut self, failure: &Failure) -> RecoveryOutcome {
        let ready = match &failure.condition {
            Condition::Plugin {
                path,
                digest,
                chain: previous,
                processors,
            } => {
                self.config.is_some_and(|config| chain(config) != *previous)
                    || crate::span_processor::file_digest(path) != *digest
                    || processors.as_ref().is_some_and(|previous| {
                        self.config.is_some_and(|config| {
                            config
                                .span_plugins
                                .iter()
                                .map(|path| crate::span_processor::file_digest(path))
                                .collect::<Vec<_>>()
                                != *previous
                        })
                    })
            }
            Condition::Configuration {
                signature,
                recheck_ms,
            } => {
                self.config
                    .is_some_and(|config| configuration(config) != *signature)
                    || recheck_ms.is_some_and(|when| now_ms() >= when)
            }
            Condition::Credentials { recheck_ms } | Condition::Delay { recheck_ms } => {
                now_ms() >= *recheck_ms
            }
            Condition::Revision { version } => self.version != version,
        };
        if ready {
            RecoveryOutcome::Retry
        } else {
            RecoveryOutcome::Waiting
        }
    }
}
