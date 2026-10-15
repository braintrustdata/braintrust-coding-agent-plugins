//! The route-local sink adapter is one stage in the routing container.
use super::Stage;
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
        result: anyhow::Result<u64>,
        state: State,
    },
    Flushed {
        result: anyhow::Result<()>,
        state: State,
    },
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
                    self.sink.emit(ops).await
                };
                Outcome::Emitted {
                    result,
                    state: self.state(),
                }
            }
            Operation::Flush => {
                let result = self.sink.flush().await;
                Outcome::Flushed {
                    result,
                    state: self.state(),
                }
            }
        }
    }
}
