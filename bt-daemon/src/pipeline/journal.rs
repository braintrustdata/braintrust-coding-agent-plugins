//! Route delivery from the durable SpanOp collection: each route receives
//! committed ops live or, after a gap, from storage, and removes them from its
//! view once its destination accepts them.

use super::delivery::InputPort;
use super::live::{Receiver, Record, SharedOp, BYTE_BUDGET, WINDOW};
use super::recovery::{Boundary, Failure, Recovery, RecoveryOutcome};
use super::{Completion, Delivery, Stage};
use crate::collections::{BindingChanged, HeldRoute, SpanOps};
#[cfg(test)]
use crate::translate::SpanOp;
use crate::wire::{RecoveryInfo, SessionConfig, SessionRoute};
use parking_lot::Mutex;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::watch;

/// Keep the original subscription and backend binding while correcting
/// settings that can unblock an explicitly configuration-held delivery.
pub(crate) async fn reconfigure_held(
    ops: &SpanOps,
    route: &SessionRoute,
    through: u64,
) -> anyhow::Result<Vec<(SessionRoute, SessionRoute)>> {
    let route = route.clone();
    ops.reconfigure_held(through, move |held: &HeldRoute| {
        let failure = serde_json::from_str::<Failure>(&held.failure).ok()?;
        if !matches!(
            failure.condition,
            super::recovery::Condition::Configuration { .. }
                | super::recovery::Condition::Plugin { .. }
        ) || !held.route.same_destination(&route)
        {
            return None;
        }
        let previous = held.settings.as_ref().unwrap_or(&held.route);
        if previous.same_route(&route) {
            return None;
        }
        let mut corrected = route.clone();
        // Preserve the subscription's pinned credential selection.
        corrected.auth = held.route.auth.clone();
        Some(corrected)
    })
    .await
}

pub(crate) struct Cursor {
    ops: SpanOps,
    pub route: String,
    read: u64,
    completed: u64,
    changes: watch::Receiver<u64>,
    progress: watch::Sender<u64>,
    drain: watch::Sender<u64>,
    abandoned: Arc<AtomicBool>,
    settings: Option<watch::Receiver<SessionConfig>>,
    version: String,
    restored: bool,
    live: Receiver,
    queued: Option<Record>,
    error: Arc<Mutex<Option<String>>>,
    recovery: Arc<Mutex<Option<RecoveryInfo>>>,
}

/// The composition layer keeps this status handle; downstream stages only get
/// the InputPort and managed deliveries.
#[derive(Clone)]
pub(crate) struct Progress {
    completed: watch::Receiver<u64>,
    published: watch::Receiver<u64>,
    drain: watch::Sender<u64>,
}
#[cfg(test)]
impl Default for Progress {
    fn default() -> Self {
        Self {
            completed: watch::channel(0).1,
            published: watch::channel(0).1,
            drain: watch::channel(0).0,
        }
    }
}
impl Progress {
    pub fn completed(&self) -> u64 {
        *self.completed.borrow()
    }
    pub fn pending(&self) -> u64 {
        self.published.borrow().saturating_sub(self.completed())
    }

    pub async fn through(&self, target: u64, error: &Mutex<Option<String>>) -> Result<(), String> {
        // Flush callers already know the publication boundary they need. Wake
        // collection and keep draining promptly until that boundary is reported.
        self.drain.send_if_modified(|requested| {
            if target > *requested {
                *requested = target;
                true
            } else {
                false
            }
        });
        let mut completed = self.completed.clone();
        loop {
            if *completed.borrow_and_update() >= target {
                return Ok(());
            }
            if let Some(error) = error.lock().clone() {
                return Err(error);
            }
            tokio::select! {
                changed = completed.changed() => changed.map_err(|_| "journal input is gone".to_owned())?,
                _ = tokio::time::sleep(std::time::Duration::from_millis(250)) => {},
            }
        }
    }
}

pub(crate) struct Subscription {
    pub input: Box<dyn InputPort<Item = Vec<SharedOp>>>,
    pub progress: Progress,
}

impl Cursor {
    pub async fn open(ops: &SpanOps, route: &SessionRoute) -> anyhow::Result<Self> {
        let subscriber = ops.subscribe(route).await?;
        let position = subscriber.position;
        Ok(Self {
            ops: ops.clone(),
            route: serde_json::to_string(route)?,
            read: position,
            completed: position,
            changes: subscriber.published,
            progress: watch::channel(position).0,
            drain: watch::channel(position).0,
            abandoned: Arc::new(AtomicBool::new(false)),
            settings: None,
            version: String::new(),
            restored: false,
            live: subscriber.live,
            queued: None,
            error: Arc::new(Mutex::new(None)),
            recovery: Arc::new(Mutex::new(None)),
        })
    }

    pub fn into_input(
        mut self,
        settings: watch::Receiver<SessionConfig>,
        version: String,
        error: Arc<Mutex<Option<String>>>,
        recovery: Arc<Mutex<Option<RecoveryInfo>>>,
    ) -> Subscription {
        let progress = Progress {
            completed: self.progress.subscribe(),
            published: self.changes.clone(),
            drain: self.drain.clone(),
        };
        self.settings = Some(settings);
        self.version = version;
        self.error = error;
        self.recovery = recovery;
        Subscription {
            input: Box::new(SharedCursor(self)),
            progress,
        }
    }

    #[cfg(test)]
    pub async fn next_batch(
        &mut self,
        limit: usize,
    ) -> anyhow::Result<Option<Delivery<Vec<SpanOp>>>> {
        anyhow::ensure!(limit > 0, "cursor batch limit must be nonzero");
        let after = self.read;
        let mut rows = self
            .ops
            .read_after(after, usize::MAX, std::time::Duration::MAX)
            .await?;
        rows.truncate(limit);
        let Some(through) = rows.last().map(|row| row.position) else {
            return Ok(None);
        };
        let ops = rows.into_iter().map(|row| row.op).collect::<Vec<_>>();
        self.read = through;
        Ok(Some(Delivery::new(
            ops,
            HeadCompletion {
                ops: self.ops.clone(),
                live: self.live.control(),
                route: self.route.clone(),
                expected: after,
                through,
                progress: self.progress.clone(),
                abandoned: self.abandoned.clone(),
                acknowledged: false,
                error: self.error.clone(),
                recovery: self.recovery.clone(),
            },
        )))
    }

    pub(crate) async fn next_shared(&mut self) -> anyhow::Result<Option<Delivery<Vec<SharedOp>>>> {
        let published = *self.changes.borrow();
        if self.read >= published {
            self.live.resume();
            return Ok(None);
        }
        let after = self.read;
        let first = self.queued.take().or_else(|| self.live.try_recv());
        let mut records = Vec::new();
        if let Some(first) = first {
            if first.position == after + 1 {
                records.push(first);
            }
        }
        if records.is_empty() {
            // Restart, abandoned delivery, a late subscription, or a bounded
            // live-queue overrun. Healthy live delivery never enters this path.
            records = self.ops.recover_after(after).await?;
            self.live.clear();
        } else {
            let deadline = tokio::time::Instant::now() + WINDOW;
            let mut drain = self.drain.subscribe();
            let mut bytes = records[0].bytes;
            while bytes < BYTE_BUDGET {
                let next = if *drain.borrow_and_update() > after {
                    match self.live.try_recv() {
                        Some(record) => record,
                        None => break,
                    }
                } else {
                    tokio::select! {
                        next = tokio::time::timeout_at(deadline, self.live.recv()) => match next {
                            Ok(Some(record)) => record,
                            _ => break,
                        },
                        _ = drain.changed() => continue,
                    }
                };
                if next.position != records.last().map_or(after, |record| record.position) + 1
                    || bytes + next.bytes > BYTE_BUDGET
                {
                    self.queued = Some(next);
                    break;
                }
                bytes += next.bytes;
                records.push(next);
            }
        }
        let Some(last) = records.last() else {
            return Ok(None);
        };
        let through = last.position;
        self.read = through;
        Ok(Some(Delivery::new(
            records.into_iter().map(|record| record.op).collect(),
            HeadCompletion {
                ops: self.ops.clone(),
                live: self.live.control(),
                route: self.route.clone(),
                expected: after,
                through,
                progress: self.progress.clone(),
                abandoned: self.abandoned.clone(),
                acknowledged: false,
                error: self.error.clone(),
                recovery: self.recovery.clone(),
            },
        )))
    }

    #[cfg(test)]
    pub fn completed_position(&self) -> u64 {
        self.completed
    }

    pub async fn rewind(&mut self) -> anyhow::Result<()> {
        self.completed = self.ops.head(&self.route).await?;
        self.read = self.completed;
        self.queued = None;
        self.live.clear();
        self.progress.send_replace(self.completed);
        Ok(())
    }

    #[cfg(test)]
    pub async fn changed(&mut self) {
        let _ = self.changes.changed().await;
    }
}

struct SharedCursor(Cursor);

#[async_trait::async_trait]
impl InputPort for SharedCursor {
    type Item = Vec<SharedOp>;
    async fn receive(&mut self) -> anyhow::Result<Option<Delivery<Self::Item>>> {
        let result = self.0.receive_managed().await;
        match result {
            Ok(delivery) => Ok(Some(delivery)),
            Err(error) => {
                self.0.live.control().pause();
                let error = match error.downcast::<Failure>() {
                    Ok(failure) => return Err(anyhow::Error::new(failure)),
                    Err(error) => error,
                };
                let failure = Failure::storage(&error, &self.0.version, 0).at(self.0.completed + 1);
                // An input failure has no delivery receipt to report through.
                // Its source still owns persistence of the recovery evidence.
                if let Err(error) = self
                    .0
                    .ops
                    .set_failure(&self.0.route, Some(serde_json::to_string(&failure)?))
                    .await
                {
                    tracing::warn!(%error, "input recovery evidence could not be persisted");
                }
                Err(anyhow::Error::new(failure))
            }
        }
    }
}

impl Cursor {
    async fn receive_managed(&mut self) -> anyhow::Result<Delivery<Vec<SharedOp>>> {
        let mut acknowledgements = self.progress.subscribe();
        loop {
            if self.abandoned.load(Ordering::Acquire) {
                self.rewind().await?;
                self.abandoned.store(false, Ordering::Release);
            }
            self.completed = *self.progress.borrow();
            self.read = self.read.max(self.completed);
            let config = self
                .settings
                .as_ref()
                .map(|settings| settings.borrow().clone());
            if !self.restored {
                if let Some(raw) = self.ops.failure(&self.route).await? {
                    let failure = match serde_json::from_str::<Failure>(&raw) {
                        Ok(failure) => failure,
                        Err(error) => match &config {
                            Some(config) => Failure::legacy(raw, self.completed + 1, config),
                            None => return Err(error.into()),
                        },
                    };
                    if matches!(failure.boundary, Boundary::Completion)
                        && self.completed >= failure.through.unwrap_or(failure.position)
                    {
                        self.ops.set_failure(&self.route, None).await?;
                    } else {
                        *self.error.lock() = Some(failure.message());
                        *self.recovery.lock() = Some(failure.info(false));
                        if matches!(
                            (Recovery {
                                config: config.as_ref(),
                                version: &self.version
                            })
                            .process(&failure)
                            .await,
                            RecoveryOutcome::Waiting
                        ) {
                            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                            continue;
                        }
                        *self.recovery.lock() = Some(failure.info(true));
                    }
                }
            }
            if let Some(config) = &config {
                if let Err(error) = self.ops.bind(&self.route, config).await {
                    if error.downcast_ref::<BindingChanged>().is_some() {
                        let failure =
                            Failure::legacy(error.to_string(), self.completed + 1, config);
                        self.ops
                            .set_failure(&self.route, Some(serde_json::to_string(&failure)?))
                            .await?;
                        return Err(anyhow::Error::new(failure));
                    }
                    return Err(error);
                }
            }
            if let Some(delivery) = self.next_shared().await? {
                if self.abandoned.load(Ordering::Acquire) {
                    drop(delivery);
                    continue;
                }
                self.restored = true;
                return Ok(delivery);
            }
            tokio::select! {
                _ = self.changes.changed() => {},
                _ = acknowledgements.changed() => {},
            }
        }
    }
}

#[cfg(test)]
#[async_trait::async_trait]
impl InputPort for Cursor {
    type Item = Vec<SpanOp>;
    async fn receive(&mut self) -> anyhow::Result<Option<Delivery<Self::Item>>> {
        Ok(Some(self.receive_managed().await?.map(|ops| {
            ops.into_iter().map(|op| op.as_ref().clone()).collect()
        })))
    }
}

struct HeadCompletion {
    ops: SpanOps,
    live: super::live::Control,
    route: String,
    expected: u64,
    through: u64,
    progress: watch::Sender<u64>,
    abandoned: Arc<AtomicBool>,
    acknowledged: bool,
    error: Arc<Mutex<Option<String>>>,
    recovery: Arc<Mutex<Option<RecoveryInfo>>>,
}
impl Drop for HeadCompletion {
    fn drop(&mut self) {
        if !self.acknowledged {
            self.live.pause();
            self.abandoned.store(true, Ordering::Release);
            self.progress.send_modify(|_| {});
        }
    }
}
#[async_trait::async_trait]
impl Completion for HeadCompletion {
    async fn complete(&mut self) -> anyhow::Result<()> {
        self.ops
            .remove_through(&self.route, self.expected, self.through)
            .await
            .map(|head| {
                self.acknowledged = true;
                self.progress.send_replace(head);
                *self.error.lock() = None;
                *self.recovery.lock() = None;
            })
    }

    async fn failed(&mut self, failure: &mut Failure) -> anyhow::Result<()> {
        self.live.pause();
        failure.position = self.expected + 1;
        failure.through = Some(self.through);
        *self.error.lock() = Some(failure.message());
        *self.recovery.lock() = Some(failure.info(false));
        self.ops
            .set_failure(&self.route, Some(serde_json::to_string(failure)?))
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collections::inspect::{Fault, Inspector};
    use crate::translate::{OriginSnapshot, SpanRow};

    fn op(name: &str) -> SpanOp {
        SpanOp::Insert(SpanRow {
            span_id: name.into(),
            name: name.into(),
            late_merge_key: Some("late".into()),
            turn_span_id: Some("turn".into()),
            is_turn: true,
            origin: Some(
                OriginSnapshot {
                    plugin_version: Some("capture".into()),
                    source_version: None,
                    bt_version: "test".into(),
                }
                .into(),
            ),
            ..SpanRow::default()
        })
    }
    #[test]
    fn codec_preserves_aggregate_watermarks_and_reads_older_records() {
        let mut op = op("root");
        let (SpanOp::Insert(row) | SpanOp::Merge(row)) = &mut op;
        let version = crate::translate::LateMergeVersion {
            ts_ms: 123,
            native_id: Some("turn".into()),
        };
        row.late_merge_version = Some(version.clone());
        let encoded = SpanOps::encode_op(&op).unwrap();
        let decoded = SpanOps::decode_op(&encoded).unwrap();
        let (SpanOp::Insert(row) | SpanOp::Merge(row)) = decoded;
        assert_eq!(row.late_merge_version, Some(version));
        let mut legacy: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        legacy.as_object_mut().unwrap().remove("late_merge_version");
        let decoded = SpanOps::decode_op(&serde_json::to_vec(&legacy).unwrap()).unwrap();
        let (SpanOp::Insert(row) | SpanOp::Merge(row)) = decoded;
        assert!(row.late_merge_version.is_none());
        assert!(serde_json::to_value(&op).unwrap()["Insert"]
            .get("late_merge_version")
            .is_none());
    }

    fn route(name: &str) -> SessionRoute {
        SessionRoute {
            tags: [name.into()].into_iter().collect(),
            ..SessionRoute::default()
        }
    }

    #[tokio::test]
    async fn resolved_profile_completes_the_pre_authentication_placeholder() {
        let tmp = tempfile::tempdir().unwrap();
        let journal = SpanOps::open(&tmp.path().join("auth.sqlite"))
            .await
            .unwrap();
        let mut named = route("profile");
        named.auth.profile = Some("work".into());
        journal.register(&named).await.unwrap();
        journal.translated(10, vec![op("one")]).await.unwrap();
        journal.translated(10, Vec::new()).await.unwrap();
        let mut resolved = named.clone();
        resolved.auth.profile_id = Some("stable-id".into());
        let mut cursor = Cursor::open(&journal, &resolved).await.unwrap();
        assert_eq!(journal.routes().await.unwrap().len(), 1);
        cursor
            .next_shared()
            .await
            .unwrap()
            .unwrap()
            .into_completed()
            .await
            .ok()
            .unwrap();
        assert_eq!(journal.stored_ops().await, 0);
    }

    #[tokio::test]
    async fn recovery_routes_at_different_heads_share_the_same_decode_window() {
        let tmp = tempfile::tempdir().unwrap();
        let journal = SpanOps::open(&tmp.path().join("shared-recovery.sqlite"))
            .await
            .unwrap();
        journal
            .translated(10, vec![op("one"), op("two"), op("three")])
            .await
            .unwrap();
        let mut a = Cursor::open(&journal, &route("a")).await.unwrap();
        let mut b = Cursor::open(&journal, &route("b")).await.unwrap();
        b.read = 1;
        let first = a.next_shared().await.unwrap().unwrap();
        let second = b.next_shared().await.unwrap().unwrap();
        assert_eq!(first.value().len(), 3);
        assert_eq!(second.value().len(), 2);
        assert!(std::ptr::eq(
            first.value()[1].as_ref(),
            second.value()[0].as_ref()
        ));
        assert_eq!(journal.payload_reads(), 3);
    }

    #[tokio::test]
    async fn all_route_acknowledgements_reclaim_payloads_and_preserve_watermarks() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("cleanup.sqlite");
        let journal = SpanOps::open(&path).await.unwrap();
        let mut a = Cursor::open(&journal, &route("a")).await.unwrap();
        let mut b = Cursor::open(&journal, &route("b")).await.unwrap();
        journal.translated(10, vec![op("one")]).await.unwrap();
        journal.translated(10, Vec::new()).await.unwrap();
        let first = a.next_shared().await.unwrap().unwrap();
        let second = b.next_shared().await.unwrap().unwrap();
        first.into_completed().await.ok().unwrap();
        assert_eq!(journal.stored_ops().await, 1);
        second.into_completed().await.ok().unwrap();
        assert_eq!(journal.stored_ops().await, 0);
        assert_eq!(journal.tail().await.unwrap(), 1);
        assert_eq!(journal.origins().await.unwrap()["one"].bt_version, "test");
        drop(journal);
        let restored = SpanOps::open(&path).await.unwrap();
        assert_eq!(restored.tail().await.unwrap(), 1);
        assert_eq!(restored.translated_through().await.unwrap(), 10);
        let mut late = Cursor::open(&restored, &route("new")).await.unwrap();
        assert!(late.next_shared().await.unwrap().is_none());
        restored.translated(20, vec![op("two")]).await.unwrap();
        assert_eq!(restored.tail().await.unwrap(), 2);
        assert!(late.next_shared().await.unwrap().is_some());
    }

    #[tokio::test]
    async fn explicit_flush_interrupts_live_batch_collection() {
        let tmp = tempfile::tempdir().unwrap();
        let journal = SpanOps::open(&tmp.path().join("flush.sqlite"))
            .await
            .unwrap();
        let mut cursor = Cursor::open(&journal, &route("a")).await.unwrap();
        let progress = Progress {
            completed: cursor.progress.subscribe(),
            published: cursor.changes.clone(),
            drain: cursor.drain.clone(),
        };
        journal.translated(10, vec![op("one")]).await.unwrap();
        let mut receiving = Box::pin(cursor.next_shared());
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut receiving)
                .await
                .is_err(),
            "a sparse batch should wait for more operations"
        );
        let error = Mutex::new(None);
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            let ((), result) = tokio::join!(
                async {
                    receiving
                        .await
                        .unwrap()
                        .unwrap()
                        .into_completed()
                        .await
                        .ok()
                        .unwrap();
                },
                progress.through(1, &error)
            );
            result.unwrap();
        })
        .await
        .expect("explicit flush must bypass the 30-second window");
        assert_eq!(journal.payload_reads(), 0);
    }

    #[tokio::test]
    async fn newly_registered_route_starts_after_an_existing_routes_pending_output() {
        let tmp = tempfile::tempdir().unwrap();
        let journal = SpanOps::open(&tmp.path().join("routes.sqlite"))
            .await
            .unwrap();
        journal.register(&route("first")).await.unwrap();
        journal.translated(10, vec![op("old")]).await.unwrap();
        journal.register(&route("new")).await.unwrap();
        journal.translated(20, vec![op("new")]).await.unwrap();
        let mut new = Cursor::open(&journal, &route("new")).await.unwrap();
        let batch = new.next_batch(8).await.unwrap().unwrap();
        assert_eq!(batch.value().len(), 1);
        let (SpanOp::Insert(row) | SpanOp::Merge(row)) = &batch.value()[0];
        assert_eq!(row.span_id, "new");
        batch.into_completed().await.ok().unwrap();
        let mut first = Cursor::open(&journal, &route("first")).await.unwrap();
        let batch = first.next_batch(8).await.unwrap().unwrap();
        assert_eq!(
            batch.value().len(),
            2,
            "the original destination still owns its backlog"
        );
    }

    #[tokio::test]
    async fn live_routes_share_original_payload_without_sqlite_reads() {
        let tmp = tempfile::tempdir().unwrap();
        let journal = SpanOps::open(&tmp.path().join("live.sqlite"))
            .await
            .unwrap();
        let mut first = Cursor::open(&journal, &route("first")).await.unwrap();
        let mut second = Cursor::open(&journal, &route("second")).await.unwrap();
        let payload = "x".repeat(BYTE_BUDGET * 2);
        let pointer = payload.as_ptr();
        let mut ops = vec![SpanOp::Insert(SpanRow {
            input: Some(serde_json::Value::String(payload)),
            ..Default::default()
        })];
        journal
            .translated(10, std::mem::take(&mut ops))
            .await
            .unwrap();
        assert!(ops.is_empty());
        // Live delivery is possible before the native event has finished.
        let a = first.next_shared().await.unwrap().unwrap();
        let b = second.next_shared().await.unwrap().unwrap();
        assert!(std::ptr::eq(a.value()[0].as_ref(), b.value()[0].as_ref()));
        let (SpanOp::Insert(row) | SpanOp::Merge(row)) = a.value()[0].as_ref();
        assert_eq!(
            row.input.as_ref().unwrap().as_str().unwrap().as_ptr(),
            pointer
        );
        assert_eq!(journal.payload_reads(), 0);
        let weak = a.value()[0].downgrade();
        drop(a.into_completed().await.ok().unwrap());
        drop(b.into_completed().await.ok().unwrap());
        assert!(weak.upgrade().is_none());
    }

    #[tokio::test]
    async fn recovery_decodes_once_for_concurrent_routes_and_keeps_only_weak_references() {
        let tmp = tempfile::tempdir().unwrap();
        let journal = SpanOps::open(&tmp.path().join("recovery.sqlite"))
            .await
            .unwrap();
        journal.translated(10, vec![op("recover")]).await.unwrap();
        let mut first = Cursor::open(&journal, &route("first")).await.unwrap();
        let mut second = Cursor::open(&journal, &route("second")).await.unwrap();
        let a = first.next_shared().await.unwrap().unwrap();
        let b = second.next_shared().await.unwrap().unwrap();
        assert!(std::ptr::eq(a.value()[0].as_ref(), b.value()[0].as_ref()));
        assert_eq!(journal.payload_reads(), 1);
        let weak = a.value()[0].downgrade();
        drop(a);
        drop(b);
        assert!(weak.upgrade().is_none());
    }

    #[tokio::test]
    async fn paused_route_recovers_without_blocking_a_healthy_live_route() {
        let tmp = tempfile::tempdir().unwrap();
        let journal = SpanOps::open(&tmp.path().join("lag.sqlite")).await.unwrap();
        let mut fast = Cursor::open(&journal, &route("fast")).await.unwrap();
        let mut slow = Cursor::open(&journal, &route("slow")).await.unwrap();
        slow.live.control().pause();
        for index in 0..3 {
            journal
                .translated(
                    10 + index,
                    vec![SpanOp::Insert(SpanRow {
                        name: index.to_string(),
                        input: Some(serde_json::json!("x".repeat(BYTE_BUDGET))),
                        ..Default::default()
                    })],
                )
                .await
                .unwrap();
            fast.next_shared()
                .await
                .unwrap()
                .unwrap()
                .into_completed()
                .await
                .ok()
                .unwrap();
        }
        assert_eq!(journal.payload_reads(), 0);
        for _ in 0..3 {
            let delivery = slow.next_shared().await.unwrap().unwrap();
            assert_eq!(
                delivery.value().len(),
                1,
                "an oversized operation travels alone"
            );
            delivery.into_completed().await.ok().unwrap();
        }
        slow.rewind().await.unwrap();
        assert_eq!(slow.completed_position(), 3);
        assert_eq!(journal.payload_reads(), 3);
    }

    #[tokio::test]
    async fn root_candidates_commit_and_release_atomically() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("roots.sqlite");
        let journal = SpanOps::open(&path).await.unwrap();
        let mut roots = crate::pipeline::roots::Roots::default();
        let mut root = op("root");
        let (SpanOp::Insert(row) | SpanOp::Merge(row)) = &mut root;
        row.metadata = Some(serde_json::json!({"_bt_defer_root": true}));
        assert!(roots.process(&vec![root]).await.is_empty());
        let db = Inspector::open(&path).unwrap();
        db.inject(Fault::Handoff).unwrap();
        assert!(journal
            .translated_with_roots(10, Vec::new(), roots.pending())
            .await
            .is_err());
        assert_eq!(journal.translated_through().await.unwrap(), 0);
        assert!(journal.root_candidates().await.unwrap().is_empty());
        db.clear(Fault::Handoff).unwrap();
        journal
            .translated_with_roots(10, Vec::new(), roots.pending())
            .await
            .unwrap();
        drop(journal);

        let journal = SpanOps::open(&path).await.unwrap();
        let candidates = journal.root_candidates().await.unwrap();
        let (SpanOp::Insert(row) | SpanOp::Merge(row)) = &candidates["root"][0];
        assert_eq!(row.late_merge_key.as_deref(), Some("late"));
        assert_eq!(
            row.origin.as_ref().unwrap().plugin_version.as_deref(),
            Some("capture")
        );
        let mut roots = crate::pipeline::roots::Roots::restore(candidates);
        let child = SpanOp::Insert(SpanRow {
            span_id: "turn".into(),
            parent_span_ids: vec!["root".into()],
            ..Default::default()
        });
        let ready = roots.process(&vec![child]).await;
        assert_eq!(ready.len(), 2);
        db.inject(Fault::Handoff).unwrap();
        assert!(journal
            .translated_with_roots(20, ready.clone(), roots.pending())
            .await
            .is_err());
        assert_eq!(journal.translated_through().await.unwrap(), 10);
        assert_eq!(journal.tail().await.unwrap(), 0);
        assert_eq!(journal.root_candidates().await.unwrap().len(), 1);
        db.clear(Fault::Handoff).unwrap();
        journal
            .translated_with_roots(20, ready.clone(), roots.pending())
            .await
            .unwrap();
        assert_eq!(journal.tail().await.unwrap(), 2);
        assert!(journal.root_candidates().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn input_port_replays_abandoned_delivery_without_cursor_control() {
        use crate::pipeline::delivery::InputPort;
        let tmp = tempfile::tempdir().unwrap();
        let journal = SpanOps::open(&tmp.path().join("spans.sqlite"))
            .await
            .unwrap();
        journal.translated(1, vec![op("one")]).await.unwrap();
        let mut input: Box<dyn InputPort<Item = Vec<SpanOp>>> =
            Box::new(Cursor::open(&journal, &route("a")).await.unwrap());
        let delivery = input.receive().await.unwrap().unwrap();
        drop(delivery);
        // Cancel a receive while its storage read is pending, as a concurrent
        // configuration update does. The replay obligation must survive it.
        let storage = journal.lock_storage();
        let mut cancelled = Box::pin(input.receive());
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(std::future::Future::poll(cancelled.as_mut(), &mut context).is_pending());
        drop(cancelled);
        drop(storage);
        let replay = tokio::time::timeout(std::time::Duration::from_millis(250), input.receive())
            .await
            .expect("unacknowledged input must be replayed")
            .unwrap()
            .unwrap();
        let (SpanOp::Insert(row) | SpanOp::Merge(row)) = &replay.value()[0];
        assert_eq!(row.name, "one");
        replay.into_completed().await.ok().unwrap();
        assert!(journal.route_complete(&route("a"), 1).await.unwrap());
        assert!(!journal.route_complete(&route("b"), 1).await.unwrap());
    }

    #[tokio::test]
    async fn a_committed_input_is_final_and_empty_commits_keep_the_boundary() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("spans.sqlite");
        let journal = SpanOps::open(&path).await.unwrap();
        journal.translated(10, vec![op("retained")]).await.unwrap();
        let tail = journal.tail().await.unwrap();
        journal.translated(10, vec![op("again")]).await.unwrap();
        assert_eq!(
            journal.tail().await.unwrap(),
            tail,
            "a committed input is final"
        );
        journal.translated(20, Vec::new()).await.unwrap();
        assert_eq!(journal.translated_through().await.unwrap(), 20);
        let mut reader = Cursor::open(&journal, &route("a")).await.unwrap();
        let message = reader.next_batch(8).await.unwrap().unwrap();
        let (SpanOp::Insert(row) | SpanOp::Merge(row)) = &message.value()[0];
        assert_eq!(row.name, "retained");
        assert_eq!(row.late_merge_key.as_deref(), Some("late"));
        assert!(row.is_turn);
        assert_eq!(
            row.origin.as_ref().unwrap().plugin_version.as_deref(),
            Some("capture")
        );
    }

    #[tokio::test]
    async fn independent_heads_and_dropped_deliveries_resume_without_skipping() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("spans.sqlite");
        let journal = SpanOps::open(&path).await.unwrap();
        journal
            .translated(10, vec![op("one"), op("two")])
            .await
            .unwrap();
        let mut a = Cursor::open(&journal, &route("a")).await.unwrap();
        let mut b = Cursor::open(&journal, &route("b")).await.unwrap();
        a.next_batch(2)
            .await
            .unwrap()
            .unwrap()
            .into_completed()
            .await
            .ok()
            .unwrap();
        drop(b.next_batch(2).await.unwrap().unwrap());
        a.rewind().await.unwrap();
        b.rewind().await.unwrap();
        assert!(a.next_batch(2).await.unwrap().is_none());
        assert_eq!(b.next_batch(2).await.unwrap().unwrap().value().len(), 2);
        let mut c = Cursor::open(&journal, &route("c")).await.unwrap();
        let first = c.next_batch(1).await.unwrap().unwrap();
        let second = c.next_batch(1).await.unwrap().unwrap();
        assert!(
            second.into_completed().await.is_err(),
            "later work cannot release an earlier obligation"
        );
        first.into_completed().await.ok().unwrap();
        c.rewind().await.unwrap();
        assert_eq!(c.next_batch(2).await.unwrap().unwrap().value().len(), 1);
    }
    #[tokio::test]
    async fn head_write_failure_retains_work_and_commit_wakes_readers() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("spans.sqlite");
        let journal = SpanOps::open(&path).await.unwrap();
        let mut reader = Cursor::open(&journal, &route("a")).await.unwrap();
        journal.translated(10, vec![op("one")]).await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), reader.changed())
            .await
            .unwrap();
        assert_eq!(journal.tail().await.unwrap(), 1);
        journal
            .set_failure(&reader.route, Some("held".into()))
            .await
            .unwrap();
        let inspector = Inspector::open(&path).unwrap();
        inspector.inject(Fault::AdvanceHead).unwrap();
        let pending = reader.next_batch(8).await.unwrap().unwrap();
        let failed = pending.into_completed().await.err().unwrap();
        assert!(failed.error.to_string().contains("injected fault_head"));
        assert_eq!(
            journal.failure(&reader.route).await.unwrap().as_deref(),
            Some("held")
        );
        reader.rewind().await.unwrap();
        assert!(reader.next_batch(8).await.unwrap().is_some());
        inspector.clear(Fault::AdvanceHead).unwrap();
        failed.delivery.into_completed().await.ok().unwrap();
        assert!(journal.failure(&reader.route).await.unwrap().is_none());
        reader.rewind().await.unwrap();
        assert!(reader.next_batch(8).await.unwrap().is_none());
    }
    #[tokio::test]
    async fn a_completed_canonical_head_also_covers_its_legacy_named_route() {
        let tmp = tempfile::tempdir().unwrap();
        let journal = SpanOps::open(&tmp.path().join("spans.sqlite"))
            .await
            .unwrap();
        journal.translated(10, vec![op("one")]).await.unwrap();
        let mut legacy = route("work");
        legacy.auth.profile = Some("work".into());
        let mut canonical = legacy.clone();
        canonical.auth.profile_id = Some("profile-id".into());
        let mut head = Cursor::open(&journal, &canonical).await.unwrap();
        head.next_batch(8)
            .await
            .unwrap()
            .unwrap()
            .into_completed()
            .await
            .ok()
            .unwrap();
        assert!(journal.route_complete(&legacy, 10).await.unwrap());
        let mut other_profile = canonical.clone();
        other_profile.auth.profile_id = Some("different-id".into());
        assert!(!journal.route_complete(&other_profile, 10).await.unwrap());
        legacy.tags.insert("different route".into());
        assert!(!journal.route_complete(&legacy, 10).await.unwrap());
    }

    #[tokio::test]
    async fn corrected_settings_survive_restart_without_advancing_or_retargeting_heads() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("spans.sqlite");
        let journal = SpanOps::open(&path).await.unwrap();
        let original = SessionRoute {
            destination: Some(crate::wire::TraceDestination::ProjectLogs {
                project_id: Some("test-project".into()),
                project_name: None,
            }),
            ..route("a")
        };
        let cursor = Cursor::open(&journal, &original).await.unwrap();
        let failure = Failure {
            boundary: Boundary::Sink,
            position: 1,
            through: None,
            reason: "rejected row".into(),
            attempt: 0,
            condition: super::super::recovery::Condition::Configuration {
                signature: "original".into(),
                recheck_ms: None,
            },
        };
        let raw = serde_json::to_string(&failure).unwrap();
        journal
            .set_failure(&cursor.route, Some(raw.clone()))
            .await
            .unwrap();
        let mut corrected = original.clone();
        corrected.tags.insert("corrected".into());
        for unrelated in [
            SessionRoute {
                destination: Some(crate::wire::TraceDestination::ProjectLogs {
                    project_id: Some("other-project".into()),
                    project_name: None,
                }),
                ..corrected.clone()
            },
            SessionRoute {
                auth: crate::wire::AuthSelection {
                    org_name: Some("other-org".into()),
                    ..Default::default()
                },
                ..corrected.clone()
            },
        ] {
            assert!(reconfigure_held(&journal, &unrelated, 20)
                .await
                .unwrap()
                .is_empty());
        }
        assert_eq!(
            reconfigure_held(&journal, &corrected, 20)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(cursor.completed, 0);
        assert_eq!(journal.failure(&cursor.route).await.unwrap(), Some(raw));
        // A captured prefix must not revert a newer correction during startup replay.
        assert!(reconfigure_held(&journal, &original, 10)
            .await
            .unwrap()
            .is_empty());
        drop(journal);
        let restored = SpanOps::open(&path).await.unwrap();
        assert_eq!(
            restored.route_settings(&original).await.unwrap().tags,
            corrected.tags
        );
        assert_eq!(restored.routes().await.unwrap().len(), 1);
        assert_eq!(
            Cursor::open(&restored, &original).await.unwrap().completed,
            0
        );
        restored.set_failure(&cursor.route, None).await.unwrap();
        let mut newer = corrected.clone();
        newer.tags.insert("independent".into());
        assert!(
            reconfigure_held(&restored, &newer, 30)
                .await
                .unwrap()
                .is_empty(),
            "healthy independent subscriptions retain their settings"
        );
    }
}
