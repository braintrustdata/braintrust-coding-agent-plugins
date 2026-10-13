//! `Journal<T>`: one consumer's ordered stream of pending nodes.
//!
//! A journal yields available nodes, then waits for appends. Temporary
//! exhaustion is `Poll::Pending`, never end-of-stream. Its read position may
//! run ahead of the consumer's durable head (accumulation and batching), and
//! `rewind` resets it to the head. Completing a node is a separate, durable
//! operation performed by the consumer's machine (`Completion`); reading,
//! buffering or dropping a node never completes it.

use super::events::EventCursor;
use super::store::{read_span_entries, Store};
use super::types::{ConsumerId, JournalId, NodeId, SpanEntry, TranslationInput};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::watch;
use tokio_stream::wrappers::WatchStream;
use tokio_stream::Stream;

const READ_BATCH: usize = 256;

/// Blocking node reads for one journal kind.
pub(crate) trait NodeReader<T>: Send + 'static {
    /// Up to `limit` nodes starting at `from`, in order.
    fn read(&mut self, from: NodeId, limit: usize) -> anyhow::Result<Vec<(NodeId, T)>>;
}

pub(crate) struct EventReader {
    pub(crate) cursor: EventCursor,
}

impl NodeReader<TranslationInput> for EventReader {
    fn read(
        &mut self,
        from: NodeId,
        limit: usize,
    ) -> anyhow::Result<Vec<(NodeId, TranslationInput)>> {
        self.cursor.seek(from)?;
        let mut nodes = Vec::new();
        while nodes.len() < limit {
            match self.cursor.read_one()? {
                Some(node) => nodes.push(node),
                None => break,
            }
        }
        Ok(nodes)
    }
}

pub(crate) struct SpanReader {
    pub(crate) store: Arc<Store>,
    pub(crate) jkey: String,
}

impl NodeReader<SpanEntry> for SpanReader {
    fn read(&mut self, from: NodeId, limit: usize) -> anyhow::Result<Vec<(NodeId, SpanEntry)>> {
        self.store
            .read_blocking(|conn| read_span_entries(conn, &self.jkey, from, limit))
    }
}

/// One pending node and the range it occupies.
pub(crate) struct Pending<T> {
    pub(crate) node: NodeId,
    pub(crate) value: T,
}

type ReadOutput<T> = (
    Option<Box<dyn NodeReader<T>>>,
    anyhow::Result<Vec<(NodeId, T)>>,
);
type ReadFuture<T> = Pin<Box<dyn Future<Output = ReadOutput<T>> + Send>>;

pub(crate) struct Journal<T: Send + 'static> {
    /// Names the journal and its consumer in read errors.
    label: String,
    reader: Option<Box<dyn NodeReader<T>>>,
    read_future: Option<ReadFuture<T>>,
    changes: WatchStream<NodeId>,
    position: NodeId,
    buffer: VecDeque<(NodeId, T)>,
    /// Whether the last storage read found nothing beyond `position`.
    caught_up: bool,
}

impl<T: Send + 'static> Journal<T> {
    pub(crate) fn new(
        id: JournalId,
        consumer: ConsumerId,
        reader: Box<dyn NodeReader<T>>,
        changed: watch::Receiver<NodeId>,
        head: NodeId,
    ) -> Self {
        Self {
            label: format!("{} for {}", id.key(), consumer.key()),
            reader: Some(reader),
            read_future: None,
            changes: WatchStream::from_changes(changed),
            position: head.max(1),
            buffer: VecDeque::new(),
            caught_up: false,
        }
    }

    /// The next node this journal will yield.
    pub(crate) fn position(&self) -> NodeId {
        self.buffer
            .front()
            .map(|(node, _)| *node)
            .unwrap_or(self.position)
    }

    fn start_read(&mut self) {
        if self.read_future.is_some() {
            return;
        }
        let Some(mut reader) = self.reader.take() else {
            return;
        };
        let from = self.position;
        self.read_future = Some(Box::pin(async move {
            match tokio::task::spawn_blocking(move || {
                let result = reader.read(from, READ_BATCH);
                (Some(reader), result)
            })
            .await
            {
                Ok(output) => output,
                // Cancelled at runtime shutdown; the journal is unusable now.
                Err(error) => (
                    None,
                    Err(anyhow::anyhow!("journal read task failed: {error}")),
                ),
            }
        }));
    }

    fn poll_read(&mut self, cx: &mut Context<'_>) -> Poll<anyhow::Result<()>> {
        self.start_read();
        let Some(future) = self.read_future.as_mut() else {
            return Poll::Ready(Err(anyhow::anyhow!("journal reader unavailable")));
        };
        let (reader, result) = match future.as_mut().poll(cx) {
            Poll::Ready(output) => output,
            Poll::Pending => return Poll::Pending,
        };
        self.read_future = None;
        self.reader = reader;
        let nodes = match result {
            Ok(nodes) => nodes,
            Err(error) => {
                return Poll::Ready(Err(error.context(format!("reading {}", self.label))));
            }
        };
        self.caught_up = nodes.is_empty();
        for (node, value) in nodes {
            if node < self.position {
                continue;
            }
            self.position = node + 1;
            self.buffer.push_back((node, value));
        }
        Poll::Ready(Ok(()))
    }

    /// Return the next node if one is available now, without waiting for an
    /// append. `Ok(None)` means the journal is caught up.
    pub(crate) async fn next_now(&mut self) -> anyhow::Result<Option<Pending<T>>> {
        if self.buffer.is_empty() {
            std::future::poll_fn(|cx| self.poll_read(cx)).await?;
        }
        Ok(self
            .buffer
            .pop_front()
            .map(|(node, value)| Pending { node, value }))
    }
}

impl<T: Send + Unpin + 'static> Stream for Journal<T> {
    type Item = anyhow::Result<Pending<T>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Some((node, value)) = this.buffer.pop_front() {
                return Poll::Ready(Some(Ok(Pending { node, value })));
            }
            if this.caught_up {
                // The watch receiver predates every read, so an append that
                // landed after the last empty read is still reported here.
                match Pin::new(&mut this.changes).poll_next(cx) {
                    Poll::Ready(Some(_)) => this.caught_up = false,
                    Poll::Ready(None) => {
                        // The writer was released; storage stays authoritative.
                        this.caught_up = false;
                        let waker = cx.waker().clone();
                        tokio::spawn(async move {
                            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                            waker.wake();
                        });
                        return Poll::Pending;
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }
            match this.poll_read(cx) {
                Poll::Ready(Ok(())) => continue,
                Poll::Ready(Err(error)) => return Poll::Ready(Some(Err(error))),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::events::EventStore;
    use crate::pipeline::types::PartitionId;
    use tokio_stream::StreamExt;

    #[tokio::test]
    async fn waits_for_appends_and_rewinds() {
        let dir = tempfile::tempdir().unwrap();
        let events = EventStore::new(dir.path());
        let partition = PartitionId::new("debug", "s");
        let input = || {
            TranslationInput::Control(crate::pipeline::types::TranslationControl::Checkpoint {
                barrier: "b".into(),
            })
        };
        events.append(&partition, input()).await.unwrap();
        let mut journal = Journal::new(
            JournalId::events(&partition),
            ConsumerId::Translation,
            Box::new(EventReader {
                cursor: events.cursor(&partition),
            }),
            events.subscribe(&partition).unwrap(),
            1,
        );
        assert_eq!(journal.next().await.unwrap().unwrap().node, 1);
        assert!(journal.next_now().await.unwrap().is_none());

        let appender = {
            let events = events.clone();
            let partition = partition.clone();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                events.append(&partition, input()).await.unwrap();
            })
        };
        let next = tokio::time::timeout(std::time::Duration::from_secs(5), journal.next())
            .await
            .expect("append wakes the journal");
        assert_eq!(next.unwrap().unwrap().node, 2);
        appender.await.unwrap();
    }
}
