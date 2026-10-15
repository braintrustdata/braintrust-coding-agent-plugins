//! The route-local sink adapter is one stage in the routing container.
use super::{recovery::RetryCondition, Stage};
use crate::{sink::Sink, translate::SpanOp, wire::SessionConfig};

pub(crate) enum Operation {
    Emit(Vec<SpanOp>),
    Flush,
}

pub(crate) struct Input {
    pub config: SessionConfig,
    pub operation: Operation,
}

pub(crate) struct State {
    pub pending: bool,
    pub permalink: Option<String>,
}

pub(crate) enum Outcome {
    Emitted {
        result: Result<u64, Failure>,
        state: State,
    },
    Flushed {
        result: Result<(), Failure>,
        state: State,
    },
}

pub(crate) struct Failure {
    pub cause: anyhow::Error,
    pub retry: RetryCondition,
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.cause.fmt(f)
    }
}
impl std::fmt::Debug for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubmissionFailure")
            .field("cause", &self.cause.to_string())
            .field("retry", &self.retry)
            .finish()
    }
}
impl std::error::Error for Failure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.cause.as_ref())
    }
}

impl Failure {
    fn classify(cause: anyhow::Error) -> Self {
        let retry = match cause.downcast_ref::<braintrust_sdk_rust::BraintrustError>() {
            Some(braintrust_sdk_rust::BraintrustError::Api { status: 401, .. }) => {
                RetryCondition::Credentials
            }
            Some(braintrust_sdk_rust::BraintrustError::Api {
                status: 403 | 404, ..
            }) => RetryCondition::Configuration { recheck: true },
            Some(braintrust_sdk_rust::BraintrustError::Api {
                status: 408 | 429 | 500..=599,
                ..
            })
            | Some(braintrust_sdk_rust::BraintrustError::Http(_))
            | Some(braintrust_sdk_rust::BraintrustError::Network(_))
            | Some(braintrust_sdk_rust::BraintrustError::ChannelClosed)
            | None => RetryCondition::Transient,
            Some(_) => RetryCondition::Configuration { recheck: false },
        };
        Self { cause, retry }
    }
}

pub(crate) struct Submission {
    sink: Box<dyn Sink>,
}

impl Submission {
    pub fn new(sink: Box<dyn Sink>) -> Self {
        Self { sink }
    }

    fn state(&self) -> State {
        State {
            pending: self.sink.has_pending_delivery(),
            permalink: self.sink.permalink(),
        }
    }
}

#[async_trait::async_trait]
impl Stage for Submission {
    type Input = Input;
    type Outcome = Outcome;

    async fn process(&mut self, input: &Self::Input) -> Self::Outcome {
        self.sink.configure(&input.config);
        match &input.operation {
            Operation::Emit(ops) => {
                let result = if ops.is_empty() {
                    Ok(0)
                } else {
                    self.sink.emit(ops).await.map_err(Failure::classify)
                };
                Outcome::Emitted {
                    result,
                    state: self.state(),
                }
            }
            Operation::Flush => {
                let result = self.sink.flush().await.map_err(Failure::classify);
                Outcome::Flushed {
                    result,
                    state: self.state(),
                }
            }
        }
    }
}
