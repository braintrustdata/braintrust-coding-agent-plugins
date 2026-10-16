//! Typed stage contracts and completion-preserving channel messages.

pub(crate) mod delivery;
pub(crate) mod plugins;
pub(crate) mod prepare;
pub(crate) mod routing;

pub(crate) use delivery::{Completion, Delivery};

// async_trait adds a redundant must_use annotation to its boxed futures.
#[allow(clippy::double_must_use)]
#[async_trait::async_trait]
pub(crate) trait Stage: Send {
    type Input: Sync;
    type Outcome: Send;

    async fn process(&mut self, input: &Self::Input) -> Self::Outcome;
}

// Storage is activated by the next stack layer after its crash contract is tested.
#[cfg(test)]
mod journal;
