//! The route machine: one per partition and route.
//!
//! Each branch consumes the span journal through its own head (an
//! import/attach branch first consumes its backfill journal). Per op it applies
//! the route overlay and plugin chain, holds deferred roots, batches, resolves
//! credentials, and submits through the SDK with checked delivery. Only an
//! accepted, filtered, or withheld op advances the head. Any failure enters
//! `Recovering` for this branch alone; a met condition rewinds to the head and
//! rebuilds everything from durable state, so plugins re-run for retried ops.

use super::journal::{Journal, SpanReader};
use super::recovery::{
    file_digest, wait_until_eligible, Failure, FailureCause, FailureRecord, MarkerState,
    RecoveryCondition, RecoveryContext,
};
use super::runtime::{
    BranchRecord, BranchRuntime, PartitionRuntime, Phase, Pipeline, ResolvedAuth,
};
use super::store::{self, LEDGER_COMPLETED, LEDGER_LATE_MERGE};
use super::translation::persistence_failure;
use super::types::{
    CaptureVersions, ConsumerId, JournalId, JournalSpanOp, NodeId, RouteConfig, RouteKey, SpanEntry,
};
use crate::server::AuthResolveReason;
use crate::sink::Sink;
use crate::translate::{OriginSnapshot, RouteOverlay, SpanOp, SpanRow, SpanType};
use crate::wire::SessionRoute;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio_stream::StreamExt;

const BATCH_OPS: usize = 500;
const BATCH_BYTES: usize = 4 * 1024 * 1024;

/// An op ready for submission.
#[derive(Clone)]
struct Prepared {
    op: SpanOp,
    capture: CaptureVersions,
}

impl Prepared {
    fn row(&self) -> &SpanRow {
        match &self.op {
            SpanOp::Insert(row) | SpanOp::Merge(row) => row,
        }
    }
}

/// A deferred root held until direct child work exists.
struct Held {
    node: NodeId,
    span_id: String,
    ops: Vec<Prepared>,
}

/// The branch-owned delivery ledger: completed spans and late merges already
/// accepted by this destination, and immutable birth provenance.
#[derive(Default)]
struct Ledger {
    completed: HashSet<String>,
    late_merges: HashSet<String>,
    origins: HashMap<String, OriginSnapshot>,
    new_completed: Vec<String>,
    new_late_merges: Vec<String>,
    new_origins: Vec<(String, String)>,
}

fn late_merge_id(row: &SpanRow, key: &str) -> String {
    format!("{}\u{1f}{key}", row.span_id)
}

impl Ledger {
    fn load(rows: store::LedgerRows) -> Self {
        Self {
            completed: rows.completed,
            late_merges: rows.late_merges,
            origins: rows
                .origins
                .into_iter()
                .filter_map(|(id, snapshot)| Some((id, serde_json::from_str(&snapshot).ok()?)))
                .collect(),
            ..Self::default()
        }
    }

    /// Suppress later partial merges for spans this destination already
    /// accepted as complete, and replays of identical late merges.
    fn admits(&self, row: &SpanRow) -> bool {
        match &row.late_merge_key {
            Some(key) => !self.late_merges.contains(&late_merge_id(row, key)),
            None => !self.completed.contains(&row.span_id),
        }
    }

    fn stamp(&mut self, prepared: &mut Prepared, attached_parent: Option<&str>, bt_version: &str) {
        let is_insert = matches!(prepared.op, SpanOp::Insert(_));
        let (SpanOp::Insert(row) | SpanOp::Merge(row)) = &mut prepared.op;
        let is_root = row.span_type == SpanType::Task
            && match attached_parent {
                Some(parent) => {
                    row.parent_span_ids.len() == 1
                        && row.parent_span_ids[0] == parent
                        && row.span_id != parent
                }
                None => row.parent_span_ids.is_empty(),
            };
        if is_insert && (is_root || row.is_turn) && !self.origins.contains_key(&row.span_id) {
            let snapshot = OriginSnapshot {
                plugin_version: prepared.capture.plugin_version.clone(),
                bt_version: bt_version.to_string(),
                source_version: prepared.capture.source_version.clone(),
            };
            self.new_origins.push((
                row.span_id.clone(),
                serde_json::to_string(&snapshot).expect("origins serialize"),
            ));
            self.origins.insert(row.span_id.clone(), snapshot);
        }
        // Stamp only known roots and turns; this also discards provenance a
        // span plugin may have supplied.
        row.origin = self.origins.get(&row.span_id).cloned();
    }

    fn record_accepted(&mut self, ops: &[Prepared]) {
        for prepared in ops {
            let row = prepared.row();
            if row.end_ms.is_some() && self.completed.insert(row.span_id.clone()) {
                self.new_completed.push(row.span_id.clone());
            }
            if let Some(key) = &row.late_merge_key {
                let id = late_merge_id(row, key);
                if self.late_merges.insert(id.clone()) {
                    self.new_late_merges.push(id);
                }
            }
        }
    }
}

struct Context<'a> {
    revision: &'a str,
    branch: &'a BranchRuntime,
}

impl RecoveryContext for Context<'_> {
    fn translator_revision(&self) -> &str {
        self.revision
    }

    fn config_version(&self) -> Option<u64> {
        Some(self.branch.config.borrow().version)
    }
}

fn destination_label(key: &RouteKey) -> String {
    key.destination
        .as_ref()
        .and_then(|destination| serde_json::to_string(destination).ok())
        .unwrap_or_else(|| "default project".into())
}

fn selection_label(key: &RouteKey) -> String {
    let auth = &key.auth;
    match (&auth.profile, &auth.profile_id, &auth.org_name) {
        (Some(profile), _, Some(org)) => format!("profile {profile:?} (org {org:?})"),
        (Some(profile), _, None) => format!("profile {profile:?}"),
        (None, Some(id), _) => format!("profile id {id:?}"),
        (None, None, Some(org)) => format!("default credentials (org {org:?})"),
        (None, None, None) => format!("{:?} credentials", auth.effective_source()),
    }
}

fn credentials_failure(source: &str, key: &RouteKey, error: &anyhow::Error) -> Failure {
    let login = match &key.auth.profile {
        Some(profile) => format!("bt login --profile {profile}"),
        None => "bt login".to_string(),
    };
    Failure {
        cause: FailureCause::Credentials {
            selection: selection_label(key),
        },
        diagnostic: format!(
            "could not resolve Braintrust auth for {source}: {error:#}; run `bt login` or select a profile explicitly"
        ),
        guidance: format!(
            "credentials for {} were rejected or unavailable: run `{login}`; delivery retries \
             automatically",
            selection_label(key)
        ),
        automatic: true,
        conditions: vec![RecoveryCondition::Backoff],
    }
}

/// Classify a submission failure by its typed SDK error.
fn submission_failure(
    source: &str,
    key: &RouteKey,
    config_version: u64,
    error: &anyhow::Error,
) -> Failure {
    use braintrust_sdk_rust::BraintrustError;
    let destination = destination_label(key);
    let status = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<BraintrustError>())
        .and_then(|error| match error {
            BraintrustError::Api { status, .. } => Some(*status),
            _ => None,
        });
    let diagnostic = format!("{error:#}");
    match status {
        Some(401) => credentials_failure(source, key, error),
        Some(403 | 404) => Failure {
            cause: FailureCause::Permission {
                destination: destination.clone(),
                status,
            },
            diagnostic,
            guidance: format!(
                "the backend refused {destination} for {}: grant access to or create the \
                 destination, or change this session's trace destination",
                selection_label(key)
            ),
            automatic: false,
            conditions: vec![
                RecoveryCondition::Backoff,
                RecoveryCondition::ConfigChanged {
                    version: config_version,
                },
            ],
        },
        Some(429) => Failure {
            cause: FailureCause::RateLimit {
                destination: destination.clone(),
            },
            diagnostic,
            guidance: "the backend is rate limiting this destination; delivery resumes \
                       automatically"
                .into(),
            automatic: true,
            conditions: vec![RecoveryCondition::Backoff],
        },
        Some(status) if (400..500).contains(&status) && status != 408 => Failure {
            cause: FailureCause::PermanentRejection {
                status: Some(status),
                config_version,
            },
            diagnostic,
            guidance: format!(
                "the backend rejected these spans (HTTP {status}); a route configuration change \
                 or a `bt` upgrade retries them"
            ),
            automatic: false,
            conditions: vec![RecoveryCondition::ConfigChanged {
                version: config_version,
            }],
        },
        _ => Failure {
            cause: FailureCause::Transport {
                endpoint: destination,
            },
            diagnostic,
            guidance: "the backend is unreachable or failing; delivery resumes automatically"
                .into(),
            automatic: true,
            conditions: vec![RecoveryCondition::Backoff],
        },
    }
}

fn plugin_failure(
    path: &std::path::Path,
    index: usize,
    message: &str,
    config_version: u64,
) -> Failure {
    Failure {
        cause: FailureCause::Plugin {
            path: path.to_path_buf(),
            index,
            config_version,
        },
        diagnostic: format!("span plugin {} failed: {message}", path.display()),
        guidance: format!(
            "fix or remove the span plugin {}, or change this route's plugin list; delivery \
             resumes when the file is saved",
            path.display()
        ),
        automatic: false,
        conditions: vec![
            RecoveryCondition::FileChanged {
                path: path.to_path_buf(),
                digest: file_digest(path),
            },
            RecoveryCondition::ConfigChanged {
                version: config_version,
            },
        ],
    }
}

/// Restore translator-owned fields a plugin round trip cannot carry.
fn restore_private_fields(transformed: &mut SpanRow, original: &SpanRow) {
    transformed.is_turn = original.is_turn;
    transformed.late_merge_key = original.late_merge_key.clone();
    transformed.turn_span_id = original.turn_span_id.clone();
    transformed.defer_until_child = original.defer_until_child;
    transformed.route_slots = original.route_slots;
    transformed.origin = original.origin.clone();
}

enum Prepare {
    Ready(Prepared),
    Filtered,
    Failed(Failure, Prepared),
}

fn prepare(
    op: &JournalSpanOp,
    overlay: &RouteOverlay,
    config: &RouteConfig,
    source: &str,
    session_id: &str,
) -> Prepare {
    let mut row = op.row.clone();
    overlay.apply_row(&mut row);
    let overlaid = Prepared {
        op: if op.merge {
            SpanOp::Merge(row.clone())
        } else {
            SpanOp::Insert(row.clone())
        },
        capture: op.capture.clone(),
    };
    match crate::span_processor::process(&config.span_plugins, &overlaid.op, source, session_id) {
        Ok(result) => {
            if let Some(failure) = result.failure {
                return Prepare::Failed(
                    plugin_failure(
                        &failure.path,
                        failure.index,
                        &failure.message,
                        config.version,
                    ),
                    overlaid,
                );
            }
            if result.filtered {
                return Prepare::Filtered;
            }
            let Some(mut op) = result.op else {
                return Prepare::Filtered;
            };
            let (SpanOp::Insert(transformed) | SpanOp::Merge(transformed)) = &mut op;
            restore_private_fields(transformed, &row);
            Prepare::Ready(Prepared {
                op,
                capture: overlaid.capture,
            })
        }
        Err(error) => {
            let path = config.span_plugins.first().cloned().unwrap_or_default();
            Prepare::Failed(
                plugin_failure(&path, 0, &format!("{error:#}"), config.version),
                overlaid,
            )
        }
    }
}

/// The payload-free marker shown in place of a span whose plugin failed.
fn marker(prepared: &Prepared, path: &std::path::Path) -> SpanOp {
    let row = prepared.row();
    let marker = SpanRow {
        span_id: row.span_id.clone(),
        root_span_id: row.root_span_id.clone(),
        parent_span_ids: row.parent_span_ids.clone(),
        name: row.name.clone(),
        span_type: row.span_type,
        start_ms: row.start_ms,
        end_ms: row.end_ms,
        error: Some(format!("plugin failure: {}", path.display())),
        is_turn: row.is_turn,
        ..SpanRow::default()
    };
    match prepared.op {
        SpanOp::Insert(_) => SpanOp::Insert(marker),
        SpanOp::Merge(_) => SpanOp::Merge(marker),
    }
}

/// One batch item: either an op to submit or a node completed without
/// submission (filtered op, barrier, held merge).
#[allow(clippy::large_enum_variant)]
enum Item {
    Submit(Prepared),
    Done,
}

struct Batch {
    first: Option<NodeId>,
    last: NodeId,
    items: Vec<Item>,
    bytes: usize,
    barriers: Vec<String>,
}

impl Batch {
    fn new() -> Self {
        Self {
            first: None,
            last: 0,
            items: Vec::new(),
            bytes: 0,
            barriers: Vec::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.first.is_none()
    }

    fn push(&mut self, node: NodeId, item: Item) {
        if let Item::Submit(prepared) = &item {
            self.bytes += serde_json::to_vec(prepared.row()).map_or(0, |bytes| bytes.len());
        }
        self.first.get_or_insert(node);
        self.last = node;
        self.items.push(item);
    }

    fn full(&self) -> bool {
        self.items.len() >= BATCH_OPS || self.bytes >= BATCH_BYTES
    }
}

/// Everything one attempt needs; rebuilt from durable state on every resume.
struct Attempt {
    pipeline: Arc<Pipeline>,
    runtime: Arc<PartitionRuntime>,
    branch: Arc<BranchRuntime>,
    key: RouteKey,
    route_id: String,
    consumer: String,
    record: BranchRecord,
    /// Created with the branch's first credential resolution.
    sink: Option<Box<dyn Sink>>,
    auth: Option<ResolvedAuth>,
    ledger: Ledger,
    held: Option<Held>,
    /// The journal currently consumed: backfill or spans.
    jkey: String,
    head: NodeId,
    previous_failure: Option<FailureRecord>,
}

enum Outcome {
    Stopped,
}

impl Attempt {
    fn route(&self) -> SessionRoute {
        self.branch.config.borrow().session_route(&self.key)
    }

    async fn ensure_auth(&mut self, reason: AuthResolveReason) -> Result<(), Failure> {
        let needs = match &self.auth {
            None => true,
            Some(auth) => auth.expiring() || reason == AuthResolveReason::Unauthorized,
        };
        if !needs {
            return Ok(());
        }
        let reason = if self.auth.is_some() && reason == AuthResolveReason::Initial {
            AuthResolveReason::Expiring
        } else {
            reason
        };
        let route = self.route();
        let source = self.runtime.partition.source.clone();
        let resolved = self
            .pipeline
            .auth
            .resolve(&source, &route, reason)
            .await
            .map_err(|error| credentials_failure(&source, &self.key, &error))?;
        if self.sink.is_none() {
            let partition = &self.runtime.partition;
            let sink = self
                .pipeline
                .sink_factory
                .create(&partition.session_id, &partition.source)
                .map_err(|error| persistence_failure("create delivery sink", &error))?;
            self.sink = Some(sink);
        }
        self.sink
            .as_mut()
            .expect("sink created")
            .configure(&resolved.config);
        if !self.record.legacy_ledger_imported {
            self.import_legacy_ledger(&resolved.config).await;
        }
        self.auth = Some(resolved);
        Ok(())
    }

    /// Bring a `main`-era destination ledger into this branch once.
    async fn import_legacy_ledger(&mut self, config: &crate::wire::SessionConfig) {
        let partition = &self.runtime.partition;
        if let Some(rows) = crate::delivery_ledger::legacy_rows(
            &self.pipeline.data_dir,
            &partition.source,
            &partition.session_id,
            config,
        )
        .await
        {
            for id in rows.completed {
                if self.ledger.completed.insert(id.clone()) {
                    self.ledger.new_completed.push(id);
                }
            }
            for id in rows.late_merges {
                if self.ledger.late_merges.insert(id.clone()) {
                    self.ledger.new_late_merges.push(id);
                }
            }
            for (span_id, origin) in rows.origins {
                if !self.ledger.origins.contains_key(&span_id) {
                    self.ledger.new_origins.push((
                        span_id.clone(),
                        serde_json::to_string(&origin).expect("origins serialize"),
                    ));
                    self.ledger.origins.insert(span_id, origin);
                }
            }
        }
        self.record.legacy_ledger_imported = true;
    }

    /// Submit the batch and, on acceptance, commit the new head.
    async fn flush(&mut self, batch: &mut Batch) -> Result<(), Failure> {
        if batch.is_empty() {
            return Ok(());
        }
        let attached_parent = self.key.attachment().0;
        let mut submit = Vec::new();
        for item in batch.items.drain(..) {
            if let Item::Submit(mut prepared) = item {
                if !self.ledger.admits(prepared.row()) {
                    continue;
                }
                if let Some(marker) = self.published_marker() {
                    let (SpanOp::Insert(row) | SpanOp::Merge(row)) = &mut prepared.op;
                    if row.span_id == marker && row.error.is_none() {
                        row.clear_error = true;
                    }
                }
                self.ledger.stamp(
                    &mut prepared,
                    attached_parent.as_deref(),
                    &self.pipeline.version,
                );
                submit.push(prepared);
            }
        }
        if !submit.is_empty() {
            self.submit(&submit).await?;
            self.ledger.record_accepted(&submit);
        }
        // A held root keeps the head on its node; everything after it that
        // was submitted is redelivered after a crash, which merges idempotently.
        let next = match &self.held {
            Some(held) => held.node.min(batch.last + 1),
            None => batch.last + 1,
        };
        self.commit(next, batch.last).await?;
        tracing::debug!(
            partition = %self.runtime.partition,
            submitted = submit.len(),
            through = batch.last,
            head = next,
            held = self.held.as_ref().map(|held| held.node),
            "branch batch committed"
        );
        let mut span_ids = HashSet::new();
        let mut root = None;
        for prepared in &submit {
            span_ids.insert(prepared.row().span_id.clone());
            if root.is_none() && !prepared.row().root_span_id.is_empty() {
                root = Some(prepared.row().root_span_id.clone());
            }
        }
        let permalink = self.sink.as_ref().and_then(|sink| sink.permalink());
        let batch_last = batch.last;
        let barriers = std::mem::take(&mut batch.barriers);
        self.branch.view.send_modify(|view| {
            view.head = next;
            view.recovering = None;
            view.spans_emitted += submit.len() as u64;
            view.span_ids += span_ids.len();
            if view.root_span_id.is_none() {
                view.root_span_id = root;
            }
            if permalink.is_some() {
                view.permalink = permalink;
            }
            view.held_root = self.held.is_some();
            view.delivered_through = view.delivered_through.max(batch_last);
            view.reached.extend(barriers);
        });
        *batch = Batch::new();
        Ok(())
    }

    async fn submit(&mut self, ops: &[Prepared]) -> Result<(), Failure> {
        self.ensure_auth(AuthResolveReason::Initial).await?;
        let source = self.runtime.partition.source.clone();
        let version = self.branch.config.borrow().version;
        let ops: Vec<SpanOp> = ops.iter().map(|prepared| prepared.op.clone()).collect();
        let mut retried = false;
        loop {
            let sink = self.sink.as_mut().expect("sink created with auth");
            let result = async {
                sink.emit(&ops).await?;
                sink.flush().await
            }
            .await;
            match result {
                Ok(()) => return Ok(()),
                Err(error) => {
                    let failure = submission_failure(&source, &self.key, version, &error);
                    if matches!(failure.cause, FailureCause::Credentials { .. }) && !retried {
                        // One immediate re-resolution for a rejected lease.
                        retried = true;
                        self.ensure_auth(AuthResolveReason::Unauthorized).await?;
                        continue;
                    }
                    return Err(failure);
                }
            }
        }
    }

    async fn commit(&mut self, next: NodeId, delivered_through: NodeId) -> Result<(), Failure> {
        let partition = self.runtime.partition.clone();
        let pkey = partition.key();
        let jkey = self.jkey.clone();
        let consumer = self.consumer.clone();
        let route_id = self.route_id.clone();
        let ledger_scope = ledger_scope(&self.key);
        let completed = std::mem::take(&mut self.ledger.new_completed);
        let late = std::mem::take(&mut self.ledger.new_late_merges);
        let origins = std::mem::take(&mut self.ledger.new_origins);
        let is_spans = jkey == JournalId::spans(&partition).key();
        let mut record = self.record.clone();
        if is_spans {
            record.delivered_through = record.delivered_through.max(delivered_through);
        }
        let encoded = serde_json::to_string(&record).expect("branch records serialize");
        let result = self
            .pipeline
            .store
            .write(move |tx| {
                store::advance_head(tx, &jkey, &consumer, next)?;
                store::clear_failure(tx, &jkey, &consumer)?;
                store::add_ledger_ids(tx, &pkey, &ledger_scope, LEDGER_COMPLETED, &completed)?;
                store::add_ledger_ids(tx, &pkey, &ledger_scope, LEDGER_LATE_MERGE, &late)?;
                store::add_origins(tx, &pkey, &ledger_scope, &origins)?;
                store::set_branch_state(tx, &pkey, &route_id, &encoded)?;
                if is_spans {
                    Pipeline::collect_spans(tx, &partition)?;
                } else {
                    store::delete_span_entries_before(tx, &jkey, next)?;
                }
                Ok(())
            })
            .await;
        match result {
            Ok(()) => {
                self.record = record;
                self.head = next;
                Ok(())
            }
            Err(error) => Err(persistence_failure("commit branch progress", &error)),
        }
    }

    fn open_journal(&self) -> Journal<SpanEntry> {
        let partition = &self.runtime.partition;
        let (id, changed) = if self.record.phase == Phase::Backfilling {
            (
                JournalId::backfill(partition, &self.key),
                self.branch.backfill_changed.subscribe(),
            )
        } else {
            (
                JournalId::spans(partition),
                self.runtime.spans_changed.subscribe(),
            )
        };
        Journal::new(
            id,
            ConsumerId::branch(&self.key),
            Box::new(SpanReader {
                store: self.pipeline.store.clone(),
                jkey: self.jkey.clone(),
            }),
            changed,
            self.head,
        )
    }

    /// Switch from the backfill journal to the span journal.
    async fn finish_backfill(&mut self) -> Result<(), Failure> {
        let partition = self.runtime.partition.clone();
        let pkey = partition.key();
        let route_id = self.route_id.clone();
        let backfill = JournalId::backfill(&partition, &self.key).key();
        let spans = JournalId::spans(&partition).key();
        let consumer = self.consumer.clone();
        let mut record = self.record.clone();
        record.phase = Phase::Live;
        let encoded = serde_json::to_string(&record).expect("branch records serialize");
        let head = self
            .pipeline
            .store
            .write(move |tx| {
                store::set_branch_state(tx, &pkey, &route_id, &encoded)?;
                store::delete_journal(tx, &backfill)?;
                store::remove_backfill(tx, &pkey, &route_id)?;
                Ok(store::head(tx, &spans, &consumer)?.unwrap_or(1))
            })
            .await
            .map_err(|error| persistence_failure("finish backfill", &error))?;
        self.record = record;
        self.jkey = JournalId::spans(&partition).key();
        self.head = head;
        self.branch.view.send_modify(|view| {
            view.backfilling = false;
            view.head = head;
        });
        Ok(())
    }

    /// The span id of a failure marker this branch published and has not yet
    /// replaced with the recovered span.
    fn published_marker(&self) -> Option<String> {
        self.previous_failure
            .as_ref()
            .and_then(|record| record.marker.as_ref())
            .filter(|marker| marker.published)
            .map(|marker| marker.span_id.clone())
    }

    async fn publish_marker(
        &mut self,
        prepared: &Prepared,
        failure: &Failure,
    ) -> Option<MarkerState> {
        let FailureCause::Plugin { path, .. } = &failure.cause else {
            return None;
        };
        if let Some(previous) = &self.previous_failure {
            if let Some(marker) = &previous.marker {
                if marker.published && marker.span_id == prepared.row().span_id {
                    return Some(marker.clone());
                }
            }
        }
        let mut marker_op = Prepared {
            op: marker(prepared, path),
            capture: prepared.capture.clone(),
        };
        let attached_parent = self.key.attachment().0;
        self.ledger.stamp(
            &mut marker_op,
            attached_parent.as_deref(),
            &self.pipeline.version,
        );
        let published = match self.submit(std::slice::from_ref(&marker_op)).await {
            Ok(()) => true,
            Err(error) => {
                // A reporting failure is secondary; the plugin stays the cause.
                tracing::warn!(
                    partition = %self.runtime.partition,
                    error = %error.diagnostic,
                    "plugin failure marker not published"
                );
                false
            }
        };
        Some(MarkerState {
            span_id: prepared.row().span_id.clone(),
            published,
        })
    }

    async fn run(
        &mut self,
        stop: &mut tokio::sync::watch::Receiver<bool>,
    ) -> Result<Outcome, (Failure, Option<MarkerState>)> {
        let partition = self.runtime.partition.clone();
        // A route that has delivered nothing resolves its credentials up
        // front, so one that cannot is reported before it has any work. Routes
        // that already delivered resolve lazily with their next batch.
        if self.record.delivered_through == 0 {
            self.ensure_auth(AuthResolveReason::Initial)
                .await
                .map_err(|failure| (failure, None))?;
        }
        let mut journal = self.open_journal();
        let mut batch = Batch::new();
        loop {
            let next = if batch.is_empty() {
                if *stop.borrow() {
                    return Ok(Outcome::Stopped);
                }
                tokio::select! {
                    next = journal.next() => next,
                    _ = stop.wait_for(|stop| *stop) => return Ok(Outcome::Stopped),
                }
            } else {
                match journal.next_now().await {
                    Ok(Some(pending)) => Some(Ok(pending)),
                    Ok(None) => {
                        // Idle: nothing more is immediately available.
                        self.flush(&mut batch)
                            .await
                            .map_err(|failure| (failure, None))?;
                        continue;
                    }
                    Err(error) => Some(Err(error)),
                }
            };
            let Some(next) = next else {
                return Ok(Outcome::Stopped);
            };
            let pending =
                next.map_err(|error| (persistence_failure("read span journal", &error), None))?;
            let node = pending.node;
            match pending.value {
                SpanEntry::Op(op) => {
                    let config = self.branch.config.borrow().clone();
                    let overlay = RouteOverlay::new(
                        self.key.destination.as_ref(),
                        config.additional_metadata.as_ref(),
                        &config.tags,
                    );
                    match prepare(
                        &op,
                        &overlay,
                        &config,
                        &partition.source,
                        &partition.session_id,
                    ) {
                        Prepare::Ready(prepared) => self.route_prepared(&mut batch, node, prepared),
                        Prepare::Filtered => batch.push(node, Item::Done),
                        Prepare::Failed(failure, overlaid) => {
                            self.flush(&mut batch)
                                .await
                                .map_err(|failure| (failure, None))?;
                            let marker = self.publish_marker(&overlaid, &failure).await;
                            return Err((failure, marker));
                        }
                    }
                }
                SpanEntry::Barrier { id, finalize } => {
                    if finalize {
                        // The session ended without exportable work under the
                        // held root: withhold it and its merges.
                        self.held = None;
                    }
                    batch.push(node, Item::Done);
                    batch.barriers.push(id);
                    self.flush(&mut batch)
                        .await
                        .map_err(|failure| (failure, None))?;
                }
                SpanEntry::BackfillComplete => {
                    batch.push(node, Item::Done);
                    self.flush(&mut batch)
                        .await
                        .map_err(|failure| (failure, None))?;
                    if self.record.phase == Phase::Backfilling {
                        self.finish_backfill()
                            .await
                            .map_err(|failure| (failure, None))?;
                        journal = self.open_journal();
                    }
                }
            }
            if batch.full() {
                self.flush(&mut batch)
                    .await
                    .map_err(|failure| (failure, None))?;
            }
        }
    }

    fn route_prepared(&mut self, batch: &mut Batch, node: NodeId, mut prepared: Prepared) {
        if prepared.row().defer_until_child
            && matches!(prepared.op, SpanOp::Insert(_))
            && self.held.is_none()
        {
            // The root's birth provenance is fixed when it is born, not when
            // child work later releases it, and commits with the head.
            let attached_parent = self.key.attachment().0;
            self.ledger.stamp(
                &mut prepared,
                attached_parent.as_deref(),
                &self.pipeline.version,
            );
        }
        let row = prepared.row();
        if let Some(held) = &mut self.held {
            if row.span_id == held.span_id {
                held.ops.push(prepared);
                batch.push(node, Item::Done);
                return;
            }
            if row
                .parent_span_ids
                .iter()
                .any(|parent| parent == &held.span_id)
            {
                let held = self.held.take().expect("held root checked");
                for mut op in held.ops {
                    // Deferral is resolved here; the sink delivers it as is.
                    let (SpanOp::Insert(row) | SpanOp::Merge(row)) = &mut op.op;
                    row.defer_until_child = false;
                    batch.push(node, Item::Submit(op));
                }
                batch.push(node, Item::Submit(prepared));
                return;
            }
        }
        if row.defer_until_child && matches!(prepared.op, SpanOp::Insert(_)) && self.held.is_none()
        {
            self.held = Some(Held {
                node,
                span_id: row.span_id.clone(),
                ops: vec![prepared],
            });
            batch.push(node, Item::Done);
            return;
        }
        batch.push(node, Item::Submit(prepared));
    }
}

/// The ledger scope: shared by every route delivering to the same destination
/// with the same credentials, so live and imported deliveries of one session
/// apply the same completed-span suppression.
fn ledger_scope(key: &RouteKey) -> String {
    let mut scope = key.clone();
    scope.origin = super::types::RouteOrigin::Live;
    scope.id()
}

async fn load_attempt(
    pipeline: &Arc<Pipeline>,
    runtime: &Arc<PartitionRuntime>,
    branch: &Arc<BranchRuntime>,
    previous_failure: Option<FailureRecord>,
) -> anyhow::Result<Attempt> {
    let partition = runtime.partition.clone();
    let key = branch.key.clone();
    let pkey = partition.key();
    let route_id = key.id();
    let consumer = ConsumerId::branch(&key).key();
    let scope = ledger_scope(&key);
    let spans = JournalId::spans(&partition).key();
    let backfill = JournalId::backfill(&partition, &key).key();
    let (record, ledger_rows, span_head, backfill_head) = {
        let consumer = consumer.clone();
        let pkey = pkey.clone();
        let route_id = route_id.clone();
        pipeline
            .store
            .read(move |conn| {
                let record = store::branch_state(conn, &pkey, &route_id)?
                    .ok_or_else(|| anyhow::anyhow!("branch state missing"))?;
                let record: BranchRecord = serde_json::from_str(&record)?;
                Ok((
                    record,
                    store::ledger(conn, &pkey, &scope)?,
                    store::head(conn, &spans, &consumer)?.unwrap_or(1),
                    store::head(conn, &backfill, &consumer)?.unwrap_or(1),
                ))
            })
            .await?
    };
    let (jkey, head) = if record.phase == Phase::Backfilling {
        (JournalId::backfill(&partition, &key).key(), backfill_head)
    } else {
        (JournalId::spans(&partition).key(), span_head)
    };
    Ok(Attempt {
        pipeline: pipeline.clone(),
        runtime: runtime.clone(),
        branch: branch.clone(),
        key,
        route_id,
        consumer,
        record,
        sink: None,
        auth: None,
        ledger: Ledger::load(ledger_rows),
        held: None,
        jkey,
        head,
        previous_failure,
    })
}

async fn persisted_failure(
    pipeline: &Pipeline,
    runtime: &PartitionRuntime,
    branch: &BranchRuntime,
) -> Option<FailureRecord> {
    let partition = &runtime.partition;
    let consumer = ConsumerId::branch(&branch.key).key();
    let jkeys = [
        JournalId::spans(partition).key(),
        JournalId::backfill(partition, &branch.key).key(),
    ];
    for jkey in jkeys {
        let consumer = consumer.clone();
        if let Ok(Some(text)) = pipeline
            .store
            .read(move |conn| store::failure(conn, &jkey, &consumer))
            .await
        {
            if let Ok(record) = FailureRecord::decode(&text) {
                return Some(record);
            }
        }
    }
    None
}

/// Run one branch until the partition is retired.
pub(crate) async fn run(
    pipeline: Arc<Pipeline>,
    runtime: Arc<PartitionRuntime>,
    branch: Arc<BranchRuntime>,
) {
    let mut stop = runtime.stop.subscribe();
    if branch.view.borrow().backfilling {
        super::backfill::ensure_producer(&pipeline, &runtime, &branch);
    }
    loop {
        let previous = persisted_failure(&pipeline, &runtime, &branch).await;
        if let Some(record) = &previous {
            branch
                .view
                .send_modify(|view| view.recovering = Some(record.clone()));
            let context = Context {
                revision: &pipeline.version,
                branch: &branch,
            };
            tokio::select! {
                _ = wait_until_eligible(record, &context, &branch.wake) => {}
                _ = stop.wait_for(|stop| *stop) => return,
            }
        }
        let mut attempt = match load_attempt(&pipeline, &runtime, &branch, previous.clone()).await {
            Ok(attempt) => attempt,
            Err(error) => {
                let failure = persistence_failure("load branch state", &error);
                record_failure(
                    &pipeline,
                    &runtime,
                    &branch,
                    previous.as_ref(),
                    failure,
                    None,
                    None,
                )
                .await;
                continue;
            }
        };
        branch.view.send_modify(|view| {
            view.head = attempt.head;
            view.delivered_through = view.delivered_through.max(attempt.record.delivered_through);
            view.backfilling = attempt.record.phase == Phase::Backfilling;
        });
        match attempt.run(&mut stop).await {
            Ok(Outcome::Stopped) => return,
            Err((failure, marker)) => {
                let jkey = attempt.jkey.clone();
                record_failure(
                    &pipeline,
                    &runtime,
                    &branch,
                    previous.as_ref(),
                    failure,
                    marker,
                    Some(jkey),
                )
                .await;
            }
        }
    }
}

async fn record_failure(
    pipeline: &Pipeline,
    runtime: &PartitionRuntime,
    branch: &BranchRuntime,
    previous: Option<&FailureRecord>,
    failure: Failure,
    marker: Option<MarkerState>,
    jkey: Option<String>,
) {
    let first_occurrence = previous.is_none_or(|previous| previous.failure.cause != failure.cause);
    if let (true, FailureCause::Plugin { path, .. }) = (first_occurrence, &failure.cause) {
        // Doctor's plugin diagnostics keep the raw exception locally.
        if let Err(error) = crate::plugin_diagnostics::record(
            &pipeline.data_dir,
            &runtime.partition.source,
            path,
            failure
                .diagnostic
                .trim_start_matches(&format!("span plugin {} failed: ", path.display())),
        ) {
            tracing::warn!(partition = %runtime.partition, "failed to persist span plugin diagnostic: {error}");
        }
    }
    let mut record = FailureRecord::record(previous, failure);
    if marker.is_some() {
        record.marker = marker;
    }
    let jkey = jkey.unwrap_or_else(|| JournalId::spans(&runtime.partition).key());
    let consumer = ConsumerId::branch(&branch.key).key();
    let encoded = record.encode();
    if let Err(error) = pipeline
        .store
        .write(move |tx| store::set_failure(tx, &jkey, &consumer, &encoded))
        .await
    {
        tracing::warn!(partition = %runtime.partition, %error, "branch failure record not persisted");
    }
    tracing::warn!(
        partition = %runtime.partition,
        failure = %record.summary(),
        "delivery recovering"
    );
    branch
        .view
        .send_modify(|view| view.recovering = Some(record.clone()));
}
