//! Typed stage contracts and completion-preserving channel messages.

pub(crate) mod delivery;
pub(crate) mod plugins;

pub(crate) use delivery::{Completion, Delivery};

#[async_trait::async_trait]
pub(crate) trait Stage: Send {
    type Input: Sync;
    type Outcome: Send;

    async fn process(&mut self, input: &Self::Input) -> Self::Outcome;
}
