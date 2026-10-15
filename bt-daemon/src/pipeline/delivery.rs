use tokio::sync::mpsc;

/// A message owns its completion obligation until terminal handling succeeds.
pub(crate) struct Delivery<T> {
    value: T,
    completion: Box<dyn Completion>,
}

// async_trait adds a redundant must_use annotation to its boxed futures.
#[allow(clippy::double_must_use)]
#[async_trait::async_trait]
pub(crate) trait Completion: Send {
    async fn complete(&mut self) -> anyhow::Result<()>;
}

pub(crate) struct Unfinished<T> {
    pub delivery: Delivery<T>,
    pub error: anyhow::Error,
}

impl<T> Delivery<T> {
    pub fn new(value: T, completion: impl Completion + 'static) -> Self {
        Self {
            value,
            completion: Box::new(completion),
        }
    }

    pub fn value(&self) -> &T {
        &self.value
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn map<U>(self, map: impl FnOnce(T) -> U) -> Delivery<U> {
        Delivery {
            value: map(self.value),
            completion: self.completion,
        }
    }

    pub async fn into_completed(mut self) -> Result<T, Unfinished<T>> {
        match self.completion.complete().await {
            Ok(()) => Ok(self.value),
            Err(error) => Err(Unfinished {
                delivery: self,
                error,
            }),
        }
    }
}

// async_trait adds a redundant must_use annotation to its boxed futures.
#[allow(clippy::double_must_use)]
#[async_trait::async_trait]
pub(crate) trait InputPort: Send {
    type Item: Send;
    async fn receive(&mut self) -> anyhow::Result<Option<Delivery<Self::Item>>>;
}

// async_trait adds a redundant must_use annotation to its boxed futures.
#[allow(clippy::double_must_use)]
#[async_trait::async_trait]
pub(crate) trait OutputPort: Send {
    type Item: Send;
    async fn send(&mut self, delivery: Delivery<Self::Item>) -> Result<(), Delivery<Self::Item>>;
}

#[async_trait::async_trait]
impl<T: Send + 'static> InputPort for mpsc::Receiver<Delivery<T>> {
    type Item = T;
    async fn receive(&mut self) -> anyhow::Result<Option<Delivery<T>>> {
        Ok(self.recv().await)
    }
}

#[async_trait::async_trait]
impl<T: InputPort + ?Sized> InputPort for Box<T> {
    type Item = T::Item;
    async fn receive(&mut self) -> anyhow::Result<Option<Delivery<Self::Item>>> {
        (**self).receive().await
    }
}

#[async_trait::async_trait]
impl<T: Send + 'static> OutputPort for mpsc::Sender<Delivery<T>> {
    type Item = T;
    async fn send(&mut self, delivery: Delivery<T>) -> Result<(), Delivery<T>> {
        mpsc::Sender::send(self, delivery)
            .await
            .map_err(|error| error.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use tokio::sync::mpsc;

    struct Count(Arc<AtomicUsize>);
    #[async_trait::async_trait]
    impl Completion for Count {
        async fn complete(&mut self) -> anyhow::Result<()> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn mapping_and_channel_forwarding_preserve_completion() {
        let count = Arc::new(AtomicUsize::new(0));
        let (tx, mut rx) = mpsc::channel(1);
        tx.send(Delivery::new(7, Count(count.clone())).map(|n| n.to_string()))
            .await
            .unwrap_or_else(|_| panic!("channel closed"));
        let message = rx.recv().await.unwrap();
        assert_eq!(message.value(), "7");
        assert_eq!(count.load(Ordering::SeqCst), 0);
        assert_eq!(message.into_completed().await.ok().unwrap(), "7");
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn failed_send_returns_the_obligation_and_drop_does_not_complete() {
        let count = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        let pending = tx
            .send(Delivery::new(7, Count(count.clone())))
            .await
            .err()
            .unwrap();
        drop(pending);
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }

    struct Retry {
        first: bool,
    }
    #[async_trait::async_trait]
    impl Completion for Retry {
        async fn complete(&mut self) -> anyhow::Result<()> {
            if std::mem::take(&mut self.first) {
                anyhow::bail!("head write failed");
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn failed_completion_returns_the_value_and_can_retry() {
        let failed = Delivery::new(7, Retry { first: true })
            .into_completed()
            .await
            .err()
            .unwrap();
        assert_eq!(failed.error.to_string(), "head write failed");
        assert_eq!(*failed.delivery.value(), 7);
        assert_eq!(failed.delivery.into_completed().await.ok().unwrap(), 7);
    }
}
