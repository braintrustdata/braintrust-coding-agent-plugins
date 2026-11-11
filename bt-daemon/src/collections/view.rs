//! Collection facade over an existing durable route subscription. Completion
//! capabilities stay inside this adapter while callers use read/remove.
use super::{
    async_iter::{AsyncIterator, IntoAsyncIterator},
    Delivery, InputPort,
};
use crate::pipeline::recovery::Failure;
use parking_lot::Mutex;
use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

/// A record read through a view, removable by its ID.
#[derive(Debug)]
pub(crate) struct Entry<T> {
    pub id: u64,
    pub value: T,
}

type Input<T> = Box<dyn InputPort<Item = T>>;
type Read<T> =
    Pin<Box<dyn Future<Output = (Input<T>, anyhow::Result<Option<Delivery<T>>>)> + Send>>;

pub(crate) struct RouteCollection<T> {
    input: Input<T>,
    membership: RemovalView,
}
#[derive(Clone, Default)]
pub(crate) struct RemovalView(Arc<Mutex<BTreeMap<u64, Delivery<()>>>>);
impl RemovalView {
    pub async fn remove(&self, id: u64) -> anyhow::Result<()> {
        let delivery = self
            .0
            .lock()
            .remove(&id)
            .ok_or_else(|| anyhow::anyhow!("record does not belong to this route view"))?;
        if let Err(unfinished) = delivery.into_completed().await {
            self.0.lock().insert(id, unfinished.delivery);
            return Err(unfinished.error);
        }
        Ok(())
    }
    pub async fn report_failure(&self, id: u64, failure: &mut Failure) -> anyhow::Result<()> {
        let delivery = self.0.lock().remove(&id);
        if let Some(mut delivery) = delivery {
            let result = delivery.failed(failure).await;
            self.0.lock().insert(id, delivery);
            result
        } else {
            Ok(())
        }
    }
    pub fn retry(&self) {
        self.0.lock().clear();
    }
}
impl<T> RouteCollection<T> {
    pub fn new(input: Input<T>) -> Self {
        Self {
            input,
            membership: RemovalView::default(),
        }
    }
    pub fn view(&self) -> RemovalView {
        self.membership.clone()
    }
}
pub(crate) struct RouteIter<T> {
    input: Option<Input<T>>,
    read: Option<Read<T>>,
    membership: RemovalView,
    sequence: u64,
    ended: bool,
}
impl<T> Unpin for RouteIter<T> {}
impl<T: Clone + Send + 'static> IntoAsyncIterator for RouteCollection<T> {
    type Item = anyhow::Result<Entry<T>>;
    type IntoAsyncIter = RouteIter<T>;
    fn into_async_iter(self) -> Self::IntoAsyncIter {
        RouteIter {
            input: Some(self.input),
            read: None,
            membership: self.membership,
            sequence: 0,
            ended: false,
        }
    }
}
impl<T: Clone + Send + 'static> AsyncIterator for RouteIter<T> {
    type Item = anyhow::Result<Entry<T>>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.ended {
            return Poll::Ready(None);
        }
        if this.read.is_none() {
            let Some(mut input) = this.input.take() else {
                return Poll::Ready(None);
            };
            this.read = Some(Box::pin(async move {
                let result = input.receive().await;
                (input, result)
            }));
        }
        let Some(read) = this.read.as_mut() else {
            return Poll::Pending;
        };
        match read.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready((input, result)) => {
                this.read = None;
                this.input = Some(input);
                match result {
                    Err(error) => Poll::Ready(Some(Err(error))),
                    Ok(None) => {
                        this.ended = true;
                        Poll::Ready(None)
                    }
                    Ok(Some(delivery)) => {
                        this.sequence += 1;
                        let id = delivery.record_id().unwrap_or(this.sequence);
                        let value = delivery.value().clone();
                        this.membership.0.lock().insert(id, delivery.map(|_| ()));
                        Poll::Ready(Some(Ok(Entry { id, value })))
                    }
                }
            }
        }
    }
}
