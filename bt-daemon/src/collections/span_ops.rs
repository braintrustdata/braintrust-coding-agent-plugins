//! Translated SpanOps for one source session, with one durable head per route.
//!
//! Routes receive ops live, or read them back after a gap. A route removes
//! ops from its view by advancing its head once a destination accepts them,
//! and an op is deleted when every route has passed it.
use super::store::{json, json_or_null, read_batch, select, Json, One, SessionStorage, NOW_MS};
use crate::{
    attachments::Attachment,
    pipeline::live::{Multiplexer, Receiver, Record, SharedOp, BYTE_BUDGET, WINDOW},
    translate::{OriginSnapshot, SinkFields, SpanOp, SpanRow, SpanType},
    wire::{SessionConfig, SessionRoute},
};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::watch;

#[derive(Debug, thiserror::Error)]
#[error("credential resolution changed the durable head's pinned backend or organization")]
pub(crate) struct BindingChanged;

/// The head position of a detached route. SQLite integers are signed, so this
/// is the largest position a head can hold; it is past every SpanOp.
pub(crate) const DETACHED: u64 = i64::MAX as u64;

/// Explicit codec preserves fields intentionally omitted from wire/plugin JSON.
#[derive(Serialize, Deserialize)]
pub(super) struct StoredOp<Op> {
    version: u32,
    op: Op,
    #[serde(flatten)]
    fields: SinkFields,
}

impl<'a> From<&'a SpanOp> for StoredOp<&'a SpanOp> {
    fn from(op: &'a SpanOp) -> Self {
        Self {
            version: 1,
            op,
            fields: SinkFields::of(op.row()),
        }
    }
}

impl TryFrom<StoredOp<SpanOp>> for SpanOp {
    type Error = anyhow::Error;
    fn try_from(mut stored: StoredOp<SpanOp>) -> anyhow::Result<Self> {
        anyhow::ensure!(
            stored.version == 1,
            "unsupported SpanOp codec {}",
            stored.version
        );
        stored.fields.restore(stored.op.row_mut());
        Ok(stored.op)
    }
}

impl StoredOp<SpanOp> {
    pub(super) fn encode(op: &SpanOp) -> anyhow::Result<Vec<u8>> {
        Ok(serde_json::to_vec(&StoredOp::from(op))?)
    }
    pub(super) fn decode(bytes: &[u8]) -> anyhow::Result<SpanOp> {
        serde_json::from_slice::<Self>(bytes)?.try_into()
    }
}

pub(super) fn encode_roots(
    roots: &BTreeMap<String, Vec<SpanOp>>,
) -> anyhow::Result<Option<Vec<u8>>> {
    if roots.is_empty() {
        return Ok(None);
    }
    let roots: BTreeMap<_, Vec<StoredOp<&SpanOp>>> = roots
        .iter()
        .map(|(id, ops)| (id, ops.iter().map(StoredOp::from).collect()))
        .collect();
    Ok(Some(serde_json::to_vec(&roots)?))
}

pub(super) fn decode_roots(bytes: &[u8]) -> anyhow::Result<BTreeMap<String, Vec<SpanOp>>> {
    serde_json::from_slice::<BTreeMap<String, Vec<StoredOp<SpanOp>>>>(bytes)?
        .into_iter()
        .map(|(id, ops)| {
            let ops = ops.into_iter().map(SpanOp::try_from);
            Ok((id, ops.collect::<anyhow::Result<_>>()?))
        })
        .collect()
}

/// The op as stored, with its attachments' bytes moved out: storage keeps one
/// copy of each attachment however many ops reference it. Inline media in the
/// op's input and output are captured as attachments first.
pub(super) fn detach_attachments(op: &mut SpanOp) -> anyhow::Result<(Vec<u8>, Vec<Attachment>)> {
    let row = op.row_mut();
    for value in [&mut row.input, &mut row.output].into_iter().flatten() {
        crate::attachments::capture(value, &mut row.attachments);
    }
    for attachment in &row.attachments {
        anyhow::ensure!(
            format!("{:x}", Sha256::digest(&attachment.data)) == attachment.digest,
            "attachment digest does not match captured bytes"
        );
    }
    let mut stored = op.clone();
    let blobs = stored
        .row_mut()
        .attachments
        .iter_mut()
        .map(|attachment| Attachment {
            data: std::mem::take(&mut attachment.data),
            ..attachment.clone()
        })
        .collect();
    Ok((StoredOp::encode(&stored)?, blobs))
}

/// Record that the op at `position` references each of `blobs`. A blob is
/// deleted with the last op that references it.
pub(super) fn store_attachments(
    db: &Connection,
    position: u64,
    blobs: &[Attachment],
) -> anyhow::Result<()> {
    for blob in blobs {
        db.execute(
            "INSERT OR IGNORE INTO attachment_blobs(digest,data,refs) VALUES (?,?,0)",
            params![blob.digest, blob.data],
        )?;
        db.execute(
            "INSERT OR IGNORE INTO operation_attachments(position,digest) VALUES (?,?)",
            params![position, blob.digest],
        )?;
    }
    Ok(())
}

fn load_attachments(db: &Connection, mut op: SpanOp) -> anyhow::Result<SpanOp> {
    for attachment in &mut op.row_mut().attachments {
        attachment.data = db.one(
            "SELECT data FROM attachment_blobs WHERE digest=?",
            [&attachment.digest],
        )?;
    }
    Ok(op)
}

/// Metadata needed to restore correlation and provenance without retaining
/// conversation input/output after all delivery obligations are complete.
pub(crate) fn metadata_only(op: &SpanOp) -> SpanOp {
    let row = op.row();
    let row = SpanRow {
        span_id: row.span_id.clone(),
        root_span_id: row.root_span_id.clone(),
        parent_span_ids: row.parent_span_ids.clone(),
        name: row.name.clone(),
        span_type: row.span_type,
        start_ms: row.start_ms,
        end_ms: row.end_ms,
        metadata: row.metadata.clone(),
        origin: row.origin.clone(),
        is_turn: row.is_turn,
        ..Default::default()
    };
    match op {
        SpanOp::Insert(_) => SpanOp::Insert(row),
        SpanOp::Merge(_) => SpanOp::Merge(row),
    }
}

pub(super) fn save_metadata(db: &Connection, op: &SpanOp, observation: bool) -> anyhow::Result<()> {
    let row = op.row();
    if let Some(origin) = &row.origin {
        db.execute(
            "INSERT OR IGNORE INTO origins(span_id,value) VALUES (?,?)",
            params![row.span_id, Json(origin)],
        )?;
    }
    if row.end_ms.is_some() {
        db.execute(
            &format!("UPDATE origins SET ended_ms={NOW_MS} WHERE span_id=?"),
            [&row.span_id],
        )?;
    }
    if !observation {
        return Ok(());
    }
    if matches!(op, SpanOp::Insert(_))
        && (row.span_type == SpanType::Tool
            || (row.span_type == SpanType::Task && row.span_id == row.root_span_id))
    {
        let mut birth = metadata_only(op);
        birth.row_mut().end_ms = None;
        db.execute(
            &format!(
                "INSERT OR REPLACE INTO span_state(span_id,kind,value,end_ms,touched_ms)
                 VALUES (?,?,?,?,{NOW_MS})"
            ),
            params![
                row.span_id,
                if row.span_type == SpanType::Tool {
                    "tool"
                } else {
                    "root"
                },
                StoredOp::encode(&birth)?,
                row.end_ms
            ],
        )?;
    } else {
        db.execute(
            &format!("UPDATE span_state SET end_ms=?, touched_ms={NOW_MS} WHERE span_id=?"),
            params![row.end_ms, row.span_id],
        )?;
    }
    db.execute(
        "DELETE FROM span_state WHERE span_id=? AND kind='tool' AND end_ms IS NOT NULL",
        [&row.span_id],
    )?;
    Ok(())
}

/// Delete ops every head has passed.
pub(super) fn prune(db: &Connection) -> anyhow::Result<()> {
    db.execute(
        "DELETE FROM ops WHERE position<=(SELECT min(position) FROM heads)",
        [],
    )?;
    Ok(())
}

/// Heads that have owed output since before `cutoff` without delivering any.
pub(super) fn stalled(db: &Connection, cutoff: i64) -> anyhow::Result<Vec<String>> {
    let mut query = db.prepare(
        "SELECT route FROM heads WHERE progress_ms<?
         AND position<(SELECT op_tail FROM session WHERE id=1)",
    )?;
    let rows = query.query_map([cutoff], |r| r.get::<_, String>(0))?;
    Ok(rows.collect::<Result<Vec<_>, _>>()?)
}

/// Heads selected by `filter` that are the same route as `route`, by key.
fn same_heads(db: &Connection, filter: &str, route: &SessionRoute) -> anyhow::Result<Vec<String>> {
    let mut query = db.prepare(&format!("SELECT route FROM heads WHERE {filter}"))?;
    let matching = query.query_and_then([], |r| {
        let key: String = r.get(0)?;
        let head: SessionRoute = serde_json::from_str(&key)?;
        anyhow::Ok(head.same_route(route).then_some(key))
    })?;
    matching.filter_map(Result::transpose).collect()
}

/// One input event's complete translation.
#[derive(Clone, Default)]
pub(crate) struct Translated {
    pub input: u64,
    /// Whether the ops are native observations; retirement output is not.
    pub observation: bool,
    pub ops: Vec<SpanOp>,
    /// Root candidates still waiting for work after this input.
    pub roots: BTreeMap<String, Vec<SpanOp>>,
    /// The translator continuation after this input.
    pub state: Option<Vec<u8>>,
}

/// A head that is held by a failure and may accept corrected settings.
#[derive(serde::Deserialize)]
pub(crate) struct HeldRoute {
    /// The stored route, exactly as it keys its head.
    key: String,
    #[serde(deserialize_with = "json")]
    pub route: SessionRoute,
    pub failure: String,
    #[serde(deserialize_with = "json_or_null")]
    pub settings: Option<SessionRoute>,
}

/// One route's durable delivery head.
#[derive(serde::Deserialize)]
pub(crate) struct Head {
    pub position: u64,
    pub failure: Option<String>,
    /// Corrected settings recorded for a held head.
    #[serde(deserialize_with = "json_or_null")]
    pub settings: Option<SessionRoute>,
}

/// A route's head and its feed of ops committed after subscribing.
pub(crate) struct Subscriber {
    pub position: u64,
    pub live: Receiver,
    /// The position of the last committed op.
    pub published: watch::Receiver<u64>,
}

type ReplayWindow = Vec<(u64, std::sync::Weak<SpanOp>, usize)>;

/// In-memory delivery state shared by every handle on one session database.
pub(super) struct Channel {
    published: watch::Sender<u64>,
    live: Multiplexer,
    replay: tokio::sync::Mutex<ReplayWindow>,
    #[cfg(test)]
    payload_reads: std::sync::atomic::AtomicU64,
}

impl Channel {
    pub fn new(tail: u64) -> Self {
        Self {
            published: watch::channel(tail).0,
            live: Multiplexer::default(),
            replay: Default::default(),
            #[cfg(test)]
            payload_reads: Default::default(),
        }
    }

    /// Advertise durable ops to routes: the tail first, so routes can drain
    /// the bounded live feed (or read storage) while it is being filled.
    pub async fn publish(&self, tail: u64, records: impl IntoIterator<Item = Record>) {
        self.published.send_replace(tail);
        for record in records {
            self.live.publish(record).await;
        }
    }
}

impl SessionStorage {
    /// Stored ops after `after`, bounded by bytes and elapsed decode time.
    pub async fn read_after(
        &self,
        after: u64,
        budget: usize,
        window: std::time::Duration,
    ) -> anyhow::Result<Vec<Record>> {
        self.run(move |db| {
            read_batch(
                db,
                "SELECT position,value FROM ops WHERE position>? ORDER BY position",
                [after],
                budget,
                window,
                |row| {
                    let value = row.get_ref(1)?.as_blob()?;
                    let op = load_attachments(db, StoredOp::decode(value)?)?;
                    let size = op
                        .row()
                        .attachments
                        .iter()
                        .fold(value.len(), |size, attachment| {
                            size.saturating_add(attachment.data.len())
                        });
                    let position = row.get(0)?;
                    Ok((
                        size,
                        Record {
                            position,
                            op: SharedOp::new(op),
                            bytes: size,
                        },
                    ))
                },
            )
        })
        .await
    }

    /// Ops after `after` for a route that fell behind the live feed. One
    /// weakly held window is shared by all routes, so no decoded payload
    /// stays alive solely because of this cache.
    pub async fn recover_after(&self, after: u64) -> anyhow::Result<Vec<Record>> {
        let mut cache = self.channel().replay.lock().await;
        if cache
            .first()
            .is_some_and(|(position, _, _)| *position <= after + 1)
            && cache
                .last()
                .is_some_and(|(position, _, _)| *position > after)
        {
            let shared = cache
                .iter()
                .filter(|(position, _, _)| *position > after)
                .map(|(position, op, bytes)| {
                    Some(Record {
                        position: *position,
                        op: SharedOp::from_arc(op.upgrade()?),
                        bytes: *bytes,
                    })
                })
                .collect::<Option<Vec<_>>>();
            if let Some(shared) = shared {
                return Ok(shared);
            }
        }
        let records = self.read_after(after, BYTE_BUDGET, WINDOW).await?;
        #[cfg(test)]
        self.channel()
            .payload_reads
            .fetch_add(records.len() as u64, std::sync::atomic::Ordering::Relaxed);
        *cache = records
            .iter()
            .map(|record| (record.position, record.op.downgrade(), record.bytes))
            .collect();
        Ok(records)
    }

    /// Create the route's head if needed. A new head starts after ops that
    /// every existing route has already removed.
    pub async fn subscribe(&self, route: &SessionRoute) -> anyhow::Result<Subscriber> {
        let live = self.channel().live.subscribe();
        let key = serde_json::to_string(route)?;
        let candidate = route.clone();
        let position = self
            .write(move |tx| {
                // Auth can register a named-profile obligation before learning
                // its stable ID. Move that unresolved placeholder to the resolved
                // identity instead of leaving a second head that can never run.
                if candidate.auth.profile_id.is_some() {
                    for alias in same_heads(tx, "binding IS NULL", &candidate)? {
                        let previous: SessionRoute = serde_json::from_str(&alias)?;
                        if alias != key && previous.auth.profile_id.is_none() {
                            tx.execute(
                                "UPDATE OR IGNORE heads SET route=? WHERE route=?",
                                params![key, alias],
                            )?;
                            tx.execute("DELETE FROM heads WHERE route=?", [alias])?;
                        }
                    }
                }
                tx.execute(
                    &format!(
                        "INSERT OR IGNORE INTO heads(route,position,progress_ms) VALUES (?,coalesce(
                        (SELECT min(position)-1 FROM ops),(SELECT op_tail FROM session WHERE id=1)),
                        {NOW_MS})"
                    ),
                    [&key],
                )?;
                Ok(tx.one("SELECT position FROM heads WHERE route=?", [key])?)
            })
            .await?;
        Ok(Subscriber {
            position,
            live,
            published: self.channel().published.subscribe(),
        })
    }

    /// Register a delivery obligation for new output without backfilling.
    pub async fn register(&self, route: &SessionRoute) -> anyhow::Result<()> {
        let key = serde_json::to_string(route)?;
        let candidate = route.clone();
        self.write(move |tx| {
            if same_heads(tx, "1", &candidate)?.is_empty() {
                tx.execute(
                    &format!(
                        "INSERT OR IGNORE INTO heads(route,position,progress_ms)
                             VALUES (?,(SELECT op_tail FROM session WHERE id=1),{NOW_MS})"
                    ),
                    [key],
                )?;
            }
            Ok(())
        })
        .await
    }

    /// Whether `route` has delivered every op translated from input through `input`.
    pub async fn route_complete(&self, route: &SessionRoute, input: u64) -> anyhow::Result<bool> {
        let route = route.clone();
        self.run(move |db| {
            let through: u64 = db.one("SELECT through FROM session WHERE id=1", [])?;
            Ok(through >= input
                && !same_heads(
                    db,
                    "failure IS NULL AND position>=(SELECT op_tail FROM session WHERE id=1)",
                    &route,
                )?
                .is_empty())
        })
        .await
    }

    pub async fn origins_for(
        &self,
        spans: Vec<String>,
    ) -> anyhow::Result<BTreeMap<String, Arc<OriginSnapshot>>> {
        if spans.is_empty() {
            return Ok(BTreeMap::new());
        }
        self.run(move |db| {
            let mut query = db.prepare_cached("SELECT value FROM origins WHERE span_id=?")?;
            let mut origins = BTreeMap::new();
            for span in spans {
                if let Some(Json(origin)) = query.query_row([&span], |r| r.get(0)).optional()? {
                    origins.insert(span, origin);
                }
            }
            Ok(origins)
        })
        .await
    }

    /// Births of open tools and roots, each followed by its terminal update.
    pub async fn observations(&self) -> anyhow::Result<Vec<SpanOp>> {
        self.run(|db| {
            let mut query = db.prepare("SELECT value,end_ms FROM span_state ORDER BY rowid")?;
            let rows = query.query_map([], |r| {
                Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Option<i64>>(1)?))
            })?;
            let mut result = Vec::new();
            for row in rows {
                let (bytes, end) = row?;
                let op = StoredOp::decode(&bytes)?;
                let terminal = end.map(|end_ms| {
                    let mut terminal = op.row().clone();
                    terminal.end_ms = Some(end_ms);
                    SpanOp::Merge(terminal)
                });
                result.push(op);
                result.extend(terminal);
            }
            Ok(result)
        })
        .await
    }

    /// Routes that have owed output for `retention` without delivering any of
    /// it: a route whose credentials never resolved, or one held by a failure
    /// that was never corrected. Their output can no longer be expected to
    /// arrive.
    pub async fn stalled(
        &self,
        retention: std::time::Duration,
    ) -> anyhow::Result<Vec<SessionRoute>> {
        let retention = i64::try_from(retention.as_millis()).unwrap_or(i64::MAX);
        let routes = self
            .run(move |db| {
                let cutoff: i64 = db.one(&format!("SELECT {NOW_MS}-?"), [retention])?;
                stalled(db, cutoff)
            })
            .await?;
        Ok(routes
            .iter()
            .map(|route| serde_json::from_str(route))
            .collect::<Result<_, _>>()?)
    }

    pub async fn routes(&self) -> anyhow::Result<Vec<SessionRoute>> {
        self.run(|db| {
            let mut query = db.prepare("SELECT route FROM heads ORDER BY route")?;
            let routes = query
                .query_map([], |r| r.get::<_, Json<SessionRoute>>(0))?
                .map(|route| Ok(route?.0))
                .collect();
            routes
        })
        .await
    }

    pub async fn head(&self, route: &str) -> anyhow::Result<Option<Head>> {
        let route = route.to_owned();
        self.run(move |db| {
            Ok(select(
                db,
                "SELECT position, failure, settings FROM heads WHERE route=?",
                [route],
            )?
            .pop())
        })
        .await
    }

    /// The route's corrected settings, or the route itself if it has none.
    pub async fn route_settings(&self, route: &SessionRoute) -> anyhow::Result<SessionRoute> {
        let head = self.head(&serde_json::to_string(route)?).await?;
        Ok(head
            .and_then(|head| head.settings)
            .unwrap_or_else(|| route.clone()))
    }

    pub async fn set_failure(&self, route: &str, failure: Option<String>) -> anyhow::Result<()> {
        let route = route.to_owned();
        self.write(move |tx| {
            tx.execute(
                "UPDATE heads SET failure=? WHERE route=?",
                params![failure, route],
            )?;
            Ok(())
        })
        .await
    }

    /// Tokens can change; a durable head's backend and organization cannot.
    pub async fn bind(&self, route: &str, config: &SessionConfig) -> anyhow::Result<()> {
        let route = route.to_owned();
        let binding = serde_json::to_string(&serde_json::json!({
            "api_url": config.auth.api_url.as_deref().unwrap_or("https://api.braintrust.dev"),
            "org_id":config.auth.org_id,"org_name":config.auth.org_name,
        }))?;
        self.write(move |tx| {
            let previous: Option<String> =
                tx.one("SELECT binding FROM heads WHERE route=?", [&route])?;
            match previous {
                Some(previous) => anyhow::ensure!(previous == binding, BindingChanged),
                None => {
                    tx.execute(
                        "UPDATE heads SET binding=? WHERE route=?",
                        params![binding, route],
                    )?;
                }
            }
            Ok(())
        })
        .await
    }

    /// Atomically replace settings of held heads selected by `correct`, which
    /// returns the corrected route for a held head it can unblock.
    pub async fn reconfigure_held(
        &self,
        through: u64,
        correct: impl Fn(&HeldRoute) -> Option<SessionRoute> + Send + 'static,
    ) -> anyhow::Result<Vec<(SessionRoute, SessionRoute)>> {
        self.write(move |tx| {
            let held: Vec<HeldRoute> = select(
                tx,
                "SELECT route AS key, route, failure, settings FROM heads
                     WHERE failure IS NOT NULL AND settings_through<?",
                [through],
            )?;
            let mut updates = Vec::new();
            for held in held {
                if let Some(corrected) = correct(&held) {
                    tx.execute(
                        "UPDATE heads SET settings=?, settings_through=? WHERE route=?",
                        params![Json(&corrected), through, held.key],
                    )?;
                    updates.push((held.route, corrected));
                }
            }
            Ok(updates)
        })
        .await
    }

    /// Stop a route for good, as when a span plugin stops tracing it: it owes
    /// nothing from now on and never resumes, even after a restart.
    pub async fn detach(&self, route: &str) -> anyhow::Result<()> {
        let route = route.to_string();
        self.write(move |tx| {
            tx.execute(
                "UPDATE heads SET position=?, failure=NULL WHERE route=?",
                params![DETACHED, route],
            )?;
            prune(tx)
        })
        .await
    }

    /// Remove ops through `through` from the route's view. The head must not
    /// skip unresolved ops before `expected`. Ops every route has passed are
    /// deleted. Returns the head after the update.
    pub async fn remove_through(
        &self,
        route: &str,
        expected: u64,
        through: u64,
    ) -> anyhow::Result<u64> {
        let route = route.to_owned();
        self.write(move |tx| {
            let head: u64 = tx.one("SELECT position FROM heads WHERE route=?", [&route])?;
            if head >= through {
                return Ok(head);
            }
            anyhow::ensure!(head >= expected, "completion cannot skip unresolved input");
            tx.execute(
                &format!(
                    "UPDATE heads SET position=?, failure=NULL, progress_ms={NOW_MS} WHERE route=?"
                ),
                params![through, route],
            )?;
            prune(tx)?;
            super::store::touch(tx)?;
            Ok(through)
        })
        .await
    }
}

#[cfg(test)]
impl SessionStorage {
    /// Stored ops decoded for assertions.
    pub async fn stored(&self) -> Vec<SpanOp> {
        self.read_after(0, usize::MAX, std::time::Duration::MAX)
            .await
            .unwrap()
            .into_iter()
            .map(|record| record.op.as_ref().clone())
            .collect()
    }

    pub fn payload_reads(&self) -> u64 {
        self.channel()
            .payload_reads
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Commit native output for `input` with no continuation.
    pub async fn translated(&self, input: u64, ops: Vec<SpanOp>) -> anyhow::Result<u64> {
        self.commit_translation(Translated {
            input,
            observation: true,
            ops,
            ..Default::default()
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codec_preserves_aggregate_watermarks_and_reads_older_records() {
        let mut op = SpanOp::Insert(SpanRow {
            span_id: "root".into(),
            late_merge_key: Some("late".into()),
            ..SpanRow::default()
        });
        let version = crate::translate::LateMergeVersion {
            ts_ms: 123,
            native_id: Some("turn".into()),
        };
        op.row_mut().late_merge_version = Some(version.clone());
        let encoded = StoredOp::encode(&op).unwrap();
        let decoded = StoredOp::decode(&encoded).unwrap();
        assert_eq!(decoded.row().late_merge_version, Some(version));
        let mut legacy: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        legacy.as_object_mut().unwrap().remove("late_merge_version");
        let decoded = StoredOp::decode(&serde_json::to_vec(&legacy).unwrap()).unwrap();
        assert!(decoded.row().late_merge_version.is_none());
        assert!(serde_json::to_value(&op).unwrap()["Insert"]
            .get("late_merge_version")
            .is_none());
    }
}
