//! Translated SpanOps for one source session, with one durable head per route.
//!
//! Translation hands each input event's ops, correlation metadata and
//! translator continuation over in the same transaction that removes the
//! event. Routes then receive the ops live, or read them back after a gap. A
//! route removes ops from its view by advancing its head once a destination
//! accepts them, and an op is deleted when every route has passed it.
//! Writers acquire the write lock before reading, so read-to-write upgrades
//! cannot fail mid-way.
use super::{events::EventCollection, store::Store};
use crate::{
    pipeline::live::{Multiplexer, Receiver, Record, SharedOp, BYTE_BUDGET, WINDOW},
    translate::{OriginSnapshot, SpanOp, SpanRow, SpanType},
    wire::{SessionConfig, SessionRoute},
};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::{collections::BTreeMap, path::Path, sync::Arc};
use tokio::sync::watch;

#[derive(Debug)]
pub(crate) struct BindingChanged;
impl std::fmt::Display for BindingChanged {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            "credential resolution changed the durable head's pinned backend or organization",
        )
    }
}
impl std::error::Error for BindingChanged {}

/// Explicit codec preserves fields intentionally omitted from wire/plugin JSON.
#[derive(Serialize, Deserialize)]
struct StoredOp {
    version: u32,
    op: SpanOp,
    late_merge_key: Option<String>,
    #[serde(default)]
    late_merge_version: Option<crate::translate::LateMergeVersion>,
    turn_span_id: Option<String>,
    is_turn: bool,
}

impl StoredOp {
    fn encode(op: &SpanOp) -> anyhow::Result<Vec<u8>> {
        let (SpanOp::Insert(row) | SpanOp::Merge(row)) = op;
        #[derive(Serialize)]
        struct Borrowed<'a> {
            version: u32,
            op: &'a SpanOp,
            late_merge_key: &'a Option<String>,
            late_merge_version: &'a Option<crate::translate::LateMergeVersion>,
            turn_span_id: &'a Option<String>,
            is_turn: bool,
        }
        Ok(serde_json::to_vec(&Borrowed {
            version: 1,
            op,
            late_merge_key: &row.late_merge_key,
            late_merge_version: &row.late_merge_version,
            turn_span_id: &row.turn_span_id,
            is_turn: row.is_turn,
        })?)
    }
    fn decode(bytes: &[u8]) -> anyhow::Result<SpanOp> {
        let mut stored: Self = serde_json::from_slice(bytes)?;
        anyhow::ensure!(
            stored.version == 1,
            "unsupported SpanOp codec {}",
            stored.version
        );
        let (SpanOp::Insert(row) | SpanOp::Merge(row)) = &mut stored.op;
        row.late_merge_key = stored.late_merge_key;
        row.late_merge_version = stored.late_merge_version;
        row.turn_span_id = stored.turn_span_id;
        row.is_turn = stored.is_turn;
        Ok(stored.op)
    }
}

/// Metadata needed to restore correlation and provenance without retaining
/// conversation input/output after all delivery obligations are complete.
pub(crate) fn metadata_only(op: &SpanOp) -> SpanOp {
    let (SpanOp::Insert(row) | SpanOp::Merge(row)) = op;
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

fn save_metadata(db: &Connection, op: &SpanOp, observation: bool) -> anyhow::Result<()> {
    let (SpanOp::Insert(row) | SpanOp::Merge(row)) = op;
    if let Some(origin) = &row.origin {
        db.execute(
            "INSERT OR IGNORE INTO origins(span_id,value) VALUES (?,?)",
            params![row.span_id, serde_json::to_string(origin)?],
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
        let (SpanOp::Insert(birth_row) | SpanOp::Merge(birth_row)) = &mut birth;
        birth_row.end_ms = None;
        db.execute(
            "INSERT OR REPLACE INTO span_state(span_id,kind,value,end_ms) VALUES (?,?,?,?)",
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
            "UPDATE span_state SET end_ms=? WHERE span_id=?",
            params![row.end_ms, row.span_id],
        )?;
    }
    db.execute(
        "DELETE FROM span_state WHERE span_id=? AND kind='tool' AND end_ms IS NOT NULL",
        [&row.span_id],
    )?;
    Ok(())
}

/// Schema 6 upgrade: one bounded pass extracts small recovery metadata.
pub(super) fn extract_metadata(db: &Connection) -> anyhow::Result<()> {
    let mut after = 0;
    loop {
        let record: Option<(u64, Vec<u8>, bool)> = db
            .query_row(
                "SELECT position,value,observation FROM ops JOIN batches USING(input)
                 WHERE position>? ORDER BY position LIMIT 1",
                [after],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        let Some((position, value, observation)) = record else {
            break;
        };
        save_metadata(db, &StoredOp::decode(&value)?, observation)?;
        after = position;
    }
    prune(db)
}

fn prune(db: &Connection) -> anyhow::Result<()> {
    db.execute(
        "DELETE FROM ops WHERE position<=(SELECT min(position) FROM heads)",
        [],
    )?;
    Ok(())
}

fn tail(db: &Connection) -> rusqlite::Result<u64> {
    db.query_row("SELECT tail FROM translation WHERE id=1", [], |r| r.get(0))
}

fn translated_through(db: &Connection) -> rusqlite::Result<u64> {
    db.query_row("SELECT through FROM translation WHERE id=1", [], |r| {
        r.get(0)
    })
}

type Roots = BTreeMap<String, Vec<Box<RawValue>>>;

fn encode_roots(roots: &BTreeMap<String, Vec<SpanOp>>) -> anyhow::Result<Option<Vec<u8>>> {
    if roots.is_empty() {
        return Ok(None);
    }
    let roots = roots
        .iter()
        .map(|(id, ops)| {
            let ops = ops
                .iter()
                .map(|op| {
                    Ok(RawValue::from_string(String::from_utf8(
                        StoredOp::encode(op)?,
                    )?)?)
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            Ok((id.clone(), ops))
        })
        .collect::<anyhow::Result<Roots>>()?;
    Ok(Some(serde_json::to_vec(&roots)?))
}

/// Schema 7 upgrade: a pre-collection channel stored each root candidate in
/// its own row. Translation now replaces them all in one value.
pub(super) fn migrate_roots(db: &Connection) -> anyhow::Result<()> {
    let mut roots = Roots::new();
    let mut query = db.prepare("SELECT span_id,value FROM root_candidates")?;
    for row in query.query_map([], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, Vec<u8>>(1)?))
    })? {
        let (id, value) = row?;
        let ops: Vec<Vec<u8>> = serde_json::from_slice(&value)?;
        let ops = ops
            .into_iter()
            .map(|op| Ok(RawValue::from_string(String::from_utf8(op)?)?))
            .collect::<anyhow::Result<Vec<_>>>()?;
        roots.insert(id, ops);
    }
    if !roots.is_empty() {
        db.execute(
            "UPDATE translation SET roots=? WHERE id=1",
            [serde_json::to_vec(&roots)?],
        )?;
    }
    Ok(())
}

/// Mark legacy input through `through` as translated and delivered by an
/// earlier daemon, unless this session already has SpanOp history. The
/// translator rebuilds its state from that input without publishing it.
pub(super) fn seed_delivered(tx: &Connection, through: u64) -> anyhow::Result<()> {
    tx.execute(
        "UPDATE translation SET through=? WHERE id=1 AND through=0 AND tail=0",
        [through],
    )?;
    Ok(())
}

/// One input event's complete translation.
#[derive(Clone)]
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

/// One stored op read back for delivery.
pub(crate) struct StoredRecord {
    pub position: u64,
    pub op: SpanOp,
    pub bytes: usize,
}

/// A head that is held by a failure and may accept corrected settings.
pub(crate) struct HeldRoute {
    pub route: SessionRoute,
    pub failure: String,
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

#[derive(Clone)]
pub(crate) struct SpanOps {
    store: Store,
    channel: Arc<Channel>,
}

impl SpanOps {
    pub async fn open(path: &Path) -> anyhow::Result<Self> {
        Self::new(Store::open(path).await?).await
    }

    /// The SpanOps stored in the same session database as `events`.
    pub async fn for_events(events: &EventCollection) -> anyhow::Result<Self> {
        Self::new(events.store().clone()).await
    }

    async fn new(store: Store) -> anyhow::Result<Self> {
        let channel = match store.channel().get() {
            Some(channel) => channel.clone(),
            None => {
                let tail = store.run(|db| Ok(tail(db)?)).await?;
                store
                    .channel()
                    .get_or_init(|| {
                        Arc::new(Channel {
                            published: watch::channel(tail).0,
                            live: Multiplexer::default(),
                            replay: Default::default(),
                            #[cfg(test)]
                            payload_reads: Default::default(),
                        })
                    })
                    .clone()
            }
        };
        Ok(Self { store, channel })
    }

    /// The last input whose translation has been handed over.
    pub async fn translated_through(&self) -> anyhow::Result<u64> {
        self.store.run(|db| Ok(translated_through(db)?)).await
    }

    /// The position of the last committed op.
    pub async fn tail(&self) -> anyhow::Result<u64> {
        self.store.run(|db| Ok(tail(db)?)).await
    }

    /// Stored ops after `after`, bounded by bytes and elapsed decode time.
    pub async fn read_after(
        &self,
        after: u64,
        budget: usize,
        window: std::time::Duration,
    ) -> anyhow::Result<Vec<StoredRecord>> {
        self.store
            .run(move |db| {
                let mut statement = db.prepare_cached(
                    "SELECT position,value FROM ops WHERE position>? ORDER BY position",
                )?;
                let mut rows = statement.query([after])?;
                let started = std::time::Instant::now();
                let mut records = Vec::new();
                let mut bytes = 0;
                while let Some(row) = rows.next()? {
                    let value = row.get_ref(1)?.as_blob()?;
                    if !records.is_empty() && bytes + value.len() > budget {
                        break;
                    }
                    bytes += value.len();
                    records.push(StoredRecord {
                        position: row.get(0)?,
                        op: StoredOp::decode(value)?,
                        bytes: value.len(),
                    });
                    if bytes >= budget || started.elapsed() >= window {
                        break;
                    }
                }
                Ok(records)
            })
            .await
    }

    /// Ops after `after` for a route that fell behind the live feed. One
    /// weakly held window is shared by all routes, so no decoded payload
    /// stays alive solely because of this cache.
    pub async fn recover_after(&self, after: u64) -> anyhow::Result<Vec<Record>> {
        let mut cache = self.channel.replay.lock().await;
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
        let records: Vec<Record> = self
            .read_after(after, BYTE_BUDGET, WINDOW)
            .await?
            .into_iter()
            .map(|record| Record {
                position: record.position,
                op: SharedOp::new(record.op),
                bytes: record.bytes,
            })
            .collect();
        #[cfg(test)]
        self.channel
            .payload_reads
            .fetch_add(records.len() as u64, std::sync::atomic::Ordering::Relaxed);
        *cache = records
            .iter()
            .map(|record| (record.position, record.op.downgrade(), record.bytes))
            .collect();
        Ok(records)
    }

    /// The saved translator continuation and the input it covers.
    pub async fn checkpoint(&self) -> anyhow::Result<Option<(u64, Vec<u8>)>> {
        self.store
            .run(|db| {
                Ok(db
                    .query_row(
                        "SELECT through,continuation FROM translation WHERE id=1 AND continuation IS NOT NULL",
                        [],
                        |r| Ok((r.get(0)?, r.get(1)?)),
                    )
                    .optional()?)
            })
            .await
    }

    /// Root candidates still waiting for work.
    pub async fn root_candidates(&self) -> anyhow::Result<BTreeMap<String, Vec<SpanOp>>> {
        self.store
            .run(|db| {
                let bytes: Option<Vec<u8>> =
                    db.query_row("SELECT roots FROM translation WHERE id=1", [], |r| r.get(0))?;
                let Some(bytes) = bytes else {
                    return Ok(BTreeMap::new());
                };
                serde_json::from_slice::<Roots>(&bytes)?
                    .into_iter()
                    .map(|(id, ops)| {
                        let ops = ops
                            .iter()
                            .map(|op| StoredOp::decode(op.get().as_bytes()))
                            .collect::<anyhow::Result<Vec<_>>>()?;
                        Ok((id, ops))
                    })
                    .collect()
            })
            .await
    }

    /// Hand one input event's complete translation to delivery in a single
    /// transaction: store its ops, metadata, root candidates and translator
    /// continuation, and remove the consumed events through `input`. Routes
    /// then receive the original op instances. An input that is already
    /// handed over is left unchanged. Returns the published tail.
    pub async fn commit_translation(&self, translated: Translated) -> anyhow::Result<u64> {
        let encoded = translated
            .ops
            .iter()
            .map(|op| Ok((StoredOp::encode(op)?, metadata_only(op))))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let roots = encode_roots(&translated.roots)?;
        let (input, observation, state) =
            (translated.input, translated.observation, translated.state);
        let (tail, positions) = self
            .store
            .write(move |tx| {
                if input <= translated_through(tx)? {
                    return Ok((tail(tx)?, Vec::new()));
                }
                let mut positions = Vec::with_capacity(encoded.len());
                for (value, metadata) in encoded {
                    save_metadata(tx, &metadata, observation)?;
                    let bytes = value.len();
                    tx.execute("INSERT INTO ops(value) VALUES (?)", [value])?;
                    positions.push((tx.last_insert_rowid() as u64, bytes));
                }
                let tail = positions.last().map(|(position, _)| *position);
                tx.execute(
                    "UPDATE translation SET through=?, tail=coalesce(?,tail), roots=?,
                     continuation=coalesce(?,continuation) WHERE id=1",
                    params![input, tail, roots, state],
                )?;
                EventCollection::remove_through(tx, input)?;
                prune(tx)?;
                super::store::touch(tx)?;
                Ok((self::tail(tx)?, positions))
            })
            .await?;
        // Every op is durable: advertise the tail first, so routes can drain
        // the bounded live feed (or read storage) while it is being filled.
        self.channel.published.send_replace(tail);
        for ((position, bytes), op) in positions.into_iter().zip(translated.ops) {
            self.channel
                .live
                .publish(Record {
                    position,
                    bytes,
                    op: SharedOp::new(op),
                })
                .await;
        }
        Ok(tail)
    }

    /// Create the route's head if needed. A new head starts after ops that
    /// every existing route has already removed.
    pub async fn subscribe(&self, route: &SessionRoute) -> anyhow::Result<Subscriber> {
        let live = self.channel.live.subscribe();
        let key = serde_json::to_string(route)?;
        let candidate = route.clone();
        let position = self
            .store
            .write(move |tx| {
                // Auth can register a named-profile obligation before learning
                // its stable ID. Move that unresolved placeholder to the resolved
                // identity instead of leaving a second head that can never run.
                if candidate.auth.profile_id.is_some() {
                    let aliases = {
                        let mut query =
                            tx.prepare("SELECT route FROM heads WHERE binding IS NULL AND route<>?")?;
                        let rows = query.query_map([&key], |r| r.get::<_, String>(0))?;
                        rows.collect::<Result<Vec<_>, _>>()?
                    };
                    for alias in aliases {
                        let previous: SessionRoute = serde_json::from_str(&alias)?;
                        if previous.auth.profile_id.is_none() && previous.same_route(&candidate) {
                            tx.execute(
                                "UPDATE OR IGNORE heads SET route=? WHERE route=?",
                                params![key, alias],
                            )?;
                            tx.execute("DELETE FROM heads WHERE route=?", [alias])?;
                        }
                    }
                }
                tx.execute(
                    "INSERT OR IGNORE INTO heads(route,position) VALUES (?,coalesce(
                        (SELECT min(position)-1 FROM ops),(SELECT tail FROM translation WHERE id=1)))",
                    [&key],
                )?;
                Ok(tx.query_row("SELECT position FROM heads WHERE route=?", [key], |r| {
                    r.get(0)
                })?)
            })
            .await?;
        Ok(Subscriber {
            position,
            live,
            published: self.channel.published.subscribe(),
        })
    }

    pub async fn origins_for(
        &self,
        spans: Vec<String>,
    ) -> anyhow::Result<BTreeMap<String, Arc<OriginSnapshot>>> {
        if spans.is_empty() {
            return Ok(BTreeMap::new());
        }
        self.store
            .run(move |db| {
                let mut query = db.prepare_cached("SELECT value FROM origins WHERE span_id=?")?;
                let mut origins = BTreeMap::new();
                for span in spans {
                    if let Some(value) = query
                        .query_row([&span], |r| r.get::<_, String>(0))
                        .optional()?
                    {
                        origins.insert(span, serde_json::from_str(&value)?);
                    }
                }
                Ok(origins)
            })
            .await
    }

    /// Births of open tools and roots, each followed by its terminal update.
    pub async fn observations(&self) -> anyhow::Result<Vec<SpanOp>> {
        self.store
            .run(|db| {
                let mut query = db.prepare("SELECT value,end_ms FROM span_state ORDER BY rowid")?;
                let rows = query.query_map([], |r| {
                    Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Option<i64>>(1)?))
                })?;
                let mut result = Vec::new();
                for row in rows {
                    let (bytes, end) = row?;
                    let op = StoredOp::decode(&bytes)?;
                    let (SpanOp::Insert(birth) | SpanOp::Merge(birth)) = &op;
                    if let Some(end_ms) = end {
                        let mut terminal = birth.clone();
                        terminal.end_ms = Some(end_ms);
                        result.push(op);
                        result.push(SpanOp::Merge(terminal));
                    } else {
                        result.push(op);
                    }
                }
                Ok(result)
            })
            .await
    }

    /// Register a delivery obligation for new output without backfilling.
    pub async fn register(&self, route: &SessionRoute) -> anyhow::Result<()> {
        let key = serde_json::to_string(route)?;
        let candidate = route.clone();
        self.store
            .write(move |tx| {
                let existing = {
                    let mut query = tx.prepare("SELECT route FROM heads")?;
                    let rows = query.query_map([], |r| r.get::<_, String>(0))?;
                    let mut existing = false;
                    for row in rows {
                        if serde_json::from_str::<SessionRoute>(&row?)?.same_route(&candidate) {
                            existing = true;
                            break;
                        }
                    }
                    existing
                };
                if !existing {
                    tx.execute(
                        "INSERT OR IGNORE INTO heads(route,position) VALUES (?,(SELECT tail FROM translation WHERE id=1))",
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
        self.store
            .run(move |db| {
                if translated_through(db)? < input {
                    return Ok(false);
                }
                let mut query = db.prepare(
                    "SELECT route FROM heads WHERE failure IS NULL
                     AND position>=(SELECT tail FROM translation WHERE id=1)",
                )?;
                for stored in query.query_map([], |r| r.get::<_, String>(0))? {
                    let stored: SessionRoute = serde_json::from_str(&stored?)?;
                    if stored.same_route(&route) {
                        return Ok(true);
                    }
                }
                Ok(false)
            })
            .await
    }

    /// Every obligation is met: all input through `input` is translated and
    /// every route has delivered every op. An empty route set is not completion.
    pub async fn complete(&self, input: u64) -> anyhow::Result<bool> {
        self.store
            .run(move |db| {
                Ok(db.query_row(
                    "SELECT EXISTS(SELECT 1 FROM heads)
                     AND NOT EXISTS(SELECT 1 FROM heads WHERE failure IS NOT NULL OR position<t.tail)
                     AND t.failure IS NULL AND t.roots IS NULL AND t.through>=?
                     AND NOT EXISTS(SELECT 1 FROM events)
                     FROM translation t WHERE id=1",
                    [input],
                    |r| r.get(0),
                )?)
            })
            .await
    }

    /// A translation failure recorded for recovery.
    pub async fn source_failure(&self) -> anyhow::Result<Option<String>> {
        self.store
            .run(|db| {
                Ok(
                    db.query_row("SELECT failure FROM translation WHERE id=1", [], |r| {
                        r.get(0)
                    })?,
                )
            })
            .await
    }

    pub async fn set_source_failure(&self, failure: Option<String>) -> anyhow::Result<()> {
        self.store
            .run(move |db| {
                db.execute("UPDATE translation SET failure=? WHERE id=1", [failure])?;
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
        self.store
            .run(move |db| {
                let previous: Option<String> =
                    db.query_row("SELECT binding FROM heads WHERE route=?", [&route], |r| {
                        r.get(0)
                    })?;
                anyhow::ensure!(
                    previous
                        .as_ref()
                        .is_none_or(|previous| previous == &binding),
                    BindingChanged
                );
                if previous.is_none() {
                    db.execute(
                        "UPDATE heads SET binding=? WHERE route=?",
                        params![binding, route],
                    )?;
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
        self.store
            .write(move |tx| {
                let held = {
                    let mut query = tx.prepare(
                        "SELECT route,failure,settings FROM heads WHERE failure IS NOT NULL AND settings_through<?",
                    )?;
                    let rows = query.query_map([through], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, Option<String>>(2)?,
                        ))
                    })?;
                    rows.collect::<Result<Vec<_>, _>>()?
                };
                let mut updates = Vec::new();
                for (key, failure, settings) in held {
                    let held = HeldRoute {
                        route: serde_json::from_str(&key)?,
                        failure,
                        settings: settings.as_deref().map(serde_json::from_str).transpose()?,
                    };
                    if let Some(corrected) = correct(&held) {
                        tx.execute(
                            "UPDATE heads SET settings=?, settings_through=? WHERE route=?",
                            params![serde_json::to_string(&corrected)?, through, key],
                        )?;
                        updates.push((held.route, corrected));
                    }
                }
                Ok(updates)
            })
            .await
    }

    pub async fn route_settings(&self, route: &SessionRoute) -> anyhow::Result<SessionRoute> {
        let fallback = route.clone();
        let key = serde_json::to_string(route)?;
        self.store
            .run(move |db| {
                let settings: Option<String> = db
                    .query_row("SELECT settings FROM heads WHERE route=?", [key], |row| {
                        row.get(0)
                    })
                    .optional()?
                    .flatten();
                Ok(settings
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()?
                    .unwrap_or(fallback))
            })
            .await
    }

    pub async fn routes(&self) -> anyhow::Result<Vec<SessionRoute>> {
        self.store
            .run(|db| {
                let mut query = db.prepare("SELECT route FROM heads ORDER BY route")?;
                let rows = query
                    .query_map([], |r| r.get::<_, String>(0))?
                    .map(|route| Ok(serde_json::from_str(&route?)?))
                    .collect();
                rows
            })
            .await
    }

    pub async fn failure(&self, route: &str) -> anyhow::Result<Option<String>> {
        let route = route.to_owned();
        self.store
            .run(move |db| {
                Ok(
                    db.query_row("SELECT failure FROM heads WHERE route=?", [route], |r| {
                        r.get(0)
                    })?,
                )
            })
            .await
    }

    pub async fn set_failure(&self, route: &str, failure: Option<String>) -> anyhow::Result<()> {
        let route = route.to_owned();
        self.store
            .run(move |db| {
                db.execute(
                    "UPDATE heads SET failure=? WHERE route=?",
                    params![failure, route],
                )?;
                Ok(())
            })
            .await
    }

    pub async fn head(&self, route: &str) -> anyhow::Result<u64> {
        let route = route.to_owned();
        self.store
            .run(move |db| {
                Ok(
                    db.query_row("SELECT position FROM heads WHERE route=?", [route], |r| {
                        r.get(0)
                    })?,
                )
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
        self.store
            .write(move |tx| {
                let head: u64 =
                    tx.query_row("SELECT position FROM heads WHERE route=?", [&route], |r| {
                        r.get(0)
                    })?;
                if head >= through {
                    return Ok(head);
                }
                anyhow::ensure!(head >= expected, "completion cannot skip unresolved input");
                tx.execute(
                    "UPDATE heads SET position=?, failure=NULL WHERE route=?",
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
impl SpanOps {
    pub async fn stored_ops(&self) -> u64 {
        self.store
            .run(|db| Ok(db.query_row("SELECT count(*) FROM ops", [], |r| r.get(0))?))
            .await
            .unwrap_or(u64::MAX)
    }

    /// Stored ops decoded for assertions.
    pub async fn stored(&self) -> Vec<SpanOp> {
        self.read_after(0, usize::MAX, std::time::Duration::MAX)
            .await
            .unwrap()
            .into_iter()
            .map(|record| record.op)
            .collect()
    }

    pub fn payload_reads(&self) -> u64 {
        self.channel
            .payload_reads
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Hold the storage lock, blocking every operation until released.
    pub fn lock_storage(&self) -> impl Drop + '_ {
        self.store.lock()
    }

    pub fn encode_op(op: &SpanOp) -> anyhow::Result<Vec<u8>> {
        StoredOp::encode(op)
    }
    pub fn decode_op(bytes: &[u8]) -> anyhow::Result<SpanOp> {
        StoredOp::decode(bytes)
    }

    pub async fn origins(&self) -> anyhow::Result<BTreeMap<String, OriginSnapshot>> {
        self.store
            .run(|db| {
                let mut query = db.prepare("SELECT span_id,value FROM origins")?;
                let rows = query
                    .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
                rows.map(|row| {
                    let (id, value) = row?;
                    Ok((id, serde_json::from_str(&value)?))
                })
                .collect()
            })
            .await
    }

    /// Commit native output for `input` with no continuation.
    pub async fn translated(&self, input: u64, ops: Vec<SpanOp>) -> anyhow::Result<u64> {
        self.translated_with_roots(input, ops, &BTreeMap::new())
            .await
    }

    pub async fn translated_with_roots(
        &self,
        input: u64,
        ops: Vec<SpanOp>,
        roots: &BTreeMap<String, Vec<SpanOp>>,
    ) -> anyhow::Result<u64> {
        self.commit_translation(Translated {
            input,
            observation: true,
            ops,
            roots: roots.clone(),
            state: None,
        })
        .await
    }
}

/// Upgrades from SpanOp channel schemas that predate collections.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::journal::Cursor;

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

    fn route(name: &str) -> SessionRoute {
        SessionRoute {
            tags: [name.into()].into_iter().collect(),
            ..SessionRoute::default()
        }
    }

    /// The SpanOp channel schema written by the last pre-collection release.
    const SCHEMA_FIVE: &str = "CREATE TABLE batches(input INTEGER PRIMARY KEY,
            committed INTEGER NOT NULL DEFAULT 0, observation INTEGER NOT NULL DEFAULT 1);
        CREATE TABLE ops(position INTEGER PRIMARY KEY AUTOINCREMENT,
            input INTEGER NOT NULL, value BLOB NOT NULL);
        CREATE INDEX ops_input ON ops(input);
        CREATE TABLE heads(route TEXT PRIMARY KEY, position INTEGER NOT NULL DEFAULT 0,
            failure TEXT, binding TEXT, settings TEXT, settings_through INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE recovery(id INTEGER PRIMARY KEY CHECK(id=1), failure TEXT);
        CREATE TABLE root_candidates(span_id TEXT PRIMARY KEY, value BLOB NOT NULL);
        PRAGMA user_version=5;";

    const SCHEMA_SIX: &str = "CREATE TABLE receipts(input INTEGER NOT NULL,
            ordinal INTEGER NOT NULL, PRIMARY KEY(input,ordinal));
        CREATE TABLE publication(id INTEGER PRIMARY KEY CHECK(id=1),
            tail INTEGER NOT NULL, floor INTEGER NOT NULL);
        CREATE TABLE origins(span_id TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE span_state(span_id TEXT PRIMARY KEY, kind TEXT NOT NULL,
            value BLOB NOT NULL, end_ms INTEGER);
        PRAGMA user_version=6;";

    #[tokio::test]
    async fn schema_five_upgrade_reclaims_acknowledged_payloads_without_losing_provenance() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("upgrade.sqlite");
        let db = Connection::open(&path).unwrap();
        db.execute_batch(SCHEMA_FIVE).unwrap();
        db.execute(
            "INSERT INTO heads(route,position) VALUES(?,1)",
            [serde_json::to_string(&route("old")).unwrap()],
        )
        .unwrap();
        db.execute_batch("INSERT INTO batches(input,committed) VALUES(10,1)")
            .unwrap();
        db.execute(
            "INSERT INTO ops(input,value) VALUES(10,?)",
            [StoredOp::encode(&op("old")).unwrap()],
        )
        .unwrap();
        drop(db);
        let upgraded = SpanOps::open(&path).await.unwrap();
        assert_eq!(upgraded.tail().await.unwrap(), 1);
        assert_eq!(upgraded.origins().await.unwrap()["old"].bt_version, "test");
        assert_eq!(upgraded.stored_ops().await, 0);
        assert!(upgraded.complete(10).await.unwrap());
    }

    #[tokio::test]
    async fn schema_six_upgrade_keeps_only_published_ops_and_translation_state() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("upgrade.sqlite");
        let db = Connection::open(&path).unwrap();
        db.execute_batch(SCHEMA_FIVE).unwrap();
        db.execute_batch(SCHEMA_SIX).unwrap();
        db.execute(
            "INSERT INTO heads(route,position) VALUES(?,0)",
            [serde_json::to_string(&route("a")).unwrap()],
        )
        .unwrap();
        // Input 10 committed; input 20 published one op, then the daemon
        // stopped before storing or publishing the rest.
        db.execute_batch(
            "INSERT INTO batches(input,committed) VALUES(10,1),(20,0);
             INSERT INTO receipts VALUES(20,0);
             INSERT INTO publication VALUES(1,2,0);
             INSERT INTO recovery VALUES(1,'held');",
        )
        .unwrap();
        for (input, name) in [(10, "committed"), (20, "published"), (20, "unpublished")] {
            db.execute(
                "INSERT INTO ops(input,value) VALUES(?,?)",
                params![input, StoredOp::encode(&op(name)).unwrap()],
            )
            .unwrap();
        }
        let root = serde_json::to_vec(&[StoredOp::encode(&op("root")).unwrap()]).unwrap();
        db.execute("INSERT INTO root_candidates VALUES('root',?)", [root])
            .unwrap();
        drop(db);
        let upgraded = SpanOps::open(&path).await.unwrap();
        assert_eq!(upgraded.translated_through().await.unwrap(), 10);
        assert_eq!(upgraded.tail().await.unwrap(), 2);
        let names: Vec<_> = upgraded
            .stored()
            .await
            .into_iter()
            .map(|op| {
                let (SpanOp::Insert(row) | SpanOp::Merge(row)) = op;
                row.span_id
            })
            .collect();
        assert_eq!(names, ["committed", "published"]);
        assert_eq!(
            upgraded.source_failure().await.unwrap().as_deref(),
            Some("held")
        );
        let roots = upgraded.root_candidates().await.unwrap();
        let (SpanOp::Insert(row) | SpanOp::Merge(row)) = &roots["root"][0];
        assert_eq!(row.late_merge_key.as_deref(), Some("late"));
        assert!(upgraded.checkpoint().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn concurrent_opens_upgrade_one_schema_atomically() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("concurrent-upgrade.sqlite");
        let db = Connection::open(&path).unwrap();
        db.execute_batch("CREATE TABLE batches(input INTEGER PRIMARY KEY,committed INTEGER NOT NULL DEFAULT 0); PRAGMA user_version=1;").unwrap();
        drop(db);
        let ready = Arc::new(tokio::sync::Barrier::new(8));
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let path = path.clone();
            let ready = ready.clone();
            tasks.push(tokio::spawn(async move {
                ready.wait().await;
                SpanOps::open(&path).await
            }));
        }
        let mut opened = Vec::new();
        for task in tasks {
            opened.push(task.await.unwrap().unwrap());
        }
        for journal in opened {
            assert_eq!(journal.tail().await.unwrap(), 0);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn translation_begin_waits_for_a_concurrent_writer_before_reading() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("concurrent.sqlite");
        let journal = SpanOps::open(&path).await.unwrap();
        let mut peer = Connection::open(&path).unwrap();
        let transaction = peer
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .unwrap();
        transaction
            .execute("UPDATE translation SET through=10", [])
            .unwrap();
        let begin = tokio::spawn(async move {
            journal.translated(10, vec![op("late")]).await.unwrap();
            journal
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let failed_early = begin.is_finished();
        transaction.commit().unwrap();
        assert_eq!(
            begin.await.unwrap().stored_ops().await,
            0,
            "a committed input must remain committed after waiting for its writer"
        );
        assert!(
            !failed_early,
            "a read-before-write transaction must wait rather than fail to upgrade its snapshot"
        );
    }

    #[tokio::test]
    async fn preparatory_schema_upgrades_without_changing_committed_positions() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("old.sqlite");
        let db = Connection::open(&path).unwrap();
        db.execute_batch("CREATE TABLE batches(input INTEGER PRIMARY KEY,committed INTEGER NOT NULL DEFAULT 0); INSERT INTO batches VALUES(10,1); PRAGMA user_version=1;").unwrap();
        drop(db);
        let journal = SpanOps::open(&path).await.unwrap();
        assert_eq!(journal.translated_through().await.unwrap(), 10);
        journal
            .commit_translation(Translated {
                input: 20,
                observation: false,
                ops: vec![op("retirement")],
                roots: BTreeMap::new(),
                state: None,
            })
            .await
            .unwrap();
        assert!(
            journal.observations().await.unwrap().is_empty(),
            "retirement output is not a native correlation observation"
        );
        let mut route = Cursor::open(&journal, &route("a")).await.unwrap();
        assert_eq!(
            route.next_batch(8).await.unwrap().unwrap().value().len(),
            1,
            "all published output still reaches durable delivery subscribers"
        );
    }

    #[tokio::test]
    async fn schema_two_heads_upgrade_and_keep_a_nonsecret_backend_binding() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("old.sqlite");
        let db = Connection::open(&path).unwrap();
        db.execute_batch("CREATE TABLE heads(route TEXT PRIMARY KEY,position INTEGER NOT NULL DEFAULT 0,failure TEXT); PRAGMA user_version=2;").unwrap();
        let route = route("canonical");
        let key = serde_json::to_string(&route).unwrap();
        db.execute("INSERT INTO heads(route,position) VALUES(?,7)", [&key])
            .unwrap();
        drop(db);
        let journal = SpanOps::open(&path).await.unwrap();
        let mut config = route.with_auth(crate::wire::BackendAuth {
            token: "secret-a".into(),
            api_url: Some("https://one.test".into()),
            app_url: None,
            org_id: Some("org".into()),
            org_name: None,
        });
        journal.bind(&key, &config).await.unwrap();
        config.auth.token = "secret-b".into();
        journal.bind(&key, &config).await.unwrap();
        config.auth.api_url = Some("https://other.test".into());
        assert!(journal
            .bind(&key, &config)
            .await
            .unwrap_err()
            .downcast_ref::<BindingChanged>()
            .is_some());
        let restored = Cursor::open(&journal, &route).await.unwrap();
        assert_eq!(restored.completed_position(), 7);
        let binding: String = Connection::open(&path)
            .unwrap()
            .query_row("SELECT binding FROM heads", [], |r| r.get(0))
            .unwrap();
        assert!(!binding.contains("secret-"));
    }
}
