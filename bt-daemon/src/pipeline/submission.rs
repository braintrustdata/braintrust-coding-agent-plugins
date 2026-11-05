//! The route-local sink adapter is one stage in the routing container.
use super::{recovery::RetryCondition, Stage};
use crate::{sink::Sink, wire::SessionConfig};

pub(crate) enum Operation {
    Emit(Vec<crate::translate::CowSpanOp>),
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
        let sdk_error = cause
            .downcast_ref::<braintrust_sdk_rust::BraintrustError>()
            .or_else(
                || match cause.downcast_ref::<braintrust_sdk_rust::FlushError>() {
                    Some(braintrust_sdk_rust::FlushError::Delivery(failure)) => {
                        failure.cause().cause().downcast_ref()
                    }
                    _ => None,
                },
            )
            .or_else(
                || match cause.downcast_ref::<braintrust_sdk_rust::WriteError>() {
                    Some(braintrust_sdk_rust::WriteError::Delivery(failure)) => {
                        failure.cause().downcast_ref()
                    }
                    _ => None,
                },
            );
        let retry = match sdk_error {
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
                    self.sink.emit_shared(ops).await.map_err(Failure::classify)
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

#[cfg(test)]
mod tests {
    use super::*;
    use braintrust_sdk_rust::{BraintrustClient, ParentSpanInfo, SpanLog};
    use wiremock::{
        matchers::{method, path},
        Mock, MockServer, ResponseTemplate,
    };

    #[tokio::test]
    async fn classifies_delivery_errors_from_both_flush_and_write() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/logs3"))
            .respond_with(
                ResponseTemplate::new(400).set_delay(std::time::Duration::from_millis(50)),
            )
            .mount(&server)
            .await;
        let client = BraintrustClient::builder()
            .skip_login(true)
            .api_url(server.uri())
            .app_url(server.uri())
            .build()
            .await
            .unwrap();
        let span = client
            .span_builder_with_credentials("token", "org")
            .parent_info(ParentSpanInfo::ProjectLogs {
                object_id: "project".into(),
            })
            .build();
        let write = span.log(SpanLog::builder().build().unwrap());
        let flush = client.flush().await.unwrap_err();
        let write = write.await.unwrap_err();
        for error in [anyhow::Error::new(flush), anyhow::Error::new(write)] {
            assert!(matches!(
                Failure::classify(error).retry,
                RetryCondition::Configuration { recheck: false }
            ));
        }
    }
}
