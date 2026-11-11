//! Translated SpanOps for one source session, with one durable head per route.
//!
//! Translation inserts ops for an input event; each route removes ops from its
//! view by advancing its head once a destination accepts them. An op is
//! physically deleted when every route has passed it. Writers acquire the
//! write lock before reading, so read-to-write upgrades cannot fail mid-way.
use super::{events::EventCollection, store::Store};
use crate::{
    translate::{OriginSnapshot, SpanOp, SpanRow, SpanType},
    wire::{SessionConfig, SessionRoute},
};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::Path, sync::Arc};

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
    let through: Option<u64> = db.query_row("SELECT min(position) FROM heads", [], |r| r.get(0))?;
    if let Some(through) = through {
        db.execute("DELETE FROM ops WHERE position<=?", [through])?;
        db.execute(
            "UPDATE publication SET floor=max(floor,?) WHERE id=1",
            [through],
        )?;
        db.execute("DELETE FROM receipts WHERE EXISTS(SELECT 1 FROM batches WHERE batches.input=receipts.input AND committed=1) AND NOT EXISTS(SELECT 1 FROM ops WHERE ops.input=receipts.input)", [])?;
    }
    Ok(())
}

fn committed_through(db: &Connection) -> rusqlite::Result<u64> {
    db.query_row(
        "SELECT coalesce(max(input),0) FROM batches WHERE committed=1",
        [],
        |r| r.get(0),
    )
}

fn publication_tail(db: &Connection) -> rusqlite::Result<u64> {
    db.query_row("SELECT tail FROM publication WHERE id=1", [], |r| r.get(0))
}

fn encode_roots(roots: &BTreeMap<String, Vec<SpanOp>>) -> anyhow::Result<Vec<(String, Vec<u8>)>> {
    roots
        .iter()
        .map(|(id, ops)| {
            let ops = ops
                .iter()
                .map(StoredOp::encode)
                .collect::<anyhow::Result<Vec<_>>>()?;
            Ok((id.clone(), serde_json::to_vec(&ops)?))
        })
        .collect()
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

#[derive(Clone)]
pub(crate) struct SpanOps {
    store: Store,
}

impl SpanOps {
    pub async fn open(path: &Path) -> anyhow::Result<Self> {
        Ok(Self {
            store: Store::open(path).await?,
        })
    }

    /// The SpanOps stored in the same session database as `events`.
    pub async fn for_events(events: &EventCollection) -> anyhow::Result<Self> {
        Ok(Self {
            store: events.store().clone(),
        })
    }

    pub async fn translated_through(&self) -> anyhow::Result<u64> {
        self.store.run(|db| Ok(committed_through(db)?)).await
    }

    /// Begin translating an input. Returns false if it was already committed.
    pub async fn begin(&self, input: u64, observation: bool) -> anyhow::Result<bool> {
        self.store
            .write(move |tx| {
                let done: Option<bool> = tx
                    .query_row(
                        "SELECT committed FROM batches WHERE input=?",
                        [input],
                        |row| row.get(0),
                    )
                    .optional()?;
                if done == Some(true) {
                    return Ok(false);
                }
                let published: bool = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM receipts WHERE input=?)",
                    [input],
                    |r| r.get(0),
                )?;
                if !published {
                    tx.execute("DELETE FROM ops WHERE input=?", [input])?;
                }
                tx.execute(
                    "INSERT OR REPLACE INTO batches(input,committed,observation) VALUES (?,0,?)",
                    params![input, observation],
                )?;
                Ok(true)
            })
            .await
    }

    /// Insert a leading run of `ops` produced for `input`, numbered from
    /// `first` within it and bounded by encoded bytes and encoding time. An op
    /// whose ordinal was already stored (before a crash) is skipped, returning
    /// `None` in its place, so rebuilding translator state never duplicates it.
    /// The result has one entry per consumed op.
    pub async fn insert(
        &self,
        input: u64,
        first: u64,
        ops: &[SpanOp],
        budget: usize,
        window: std::time::Duration,
    ) -> anyhow::Result<Vec<Option<(u64, usize)>>> {
        let started = std::time::Instant::now();
        let mut encoded = Vec::new();
        let mut bytes = 0;
        for op in ops {
            let value = StoredOp::encode(op)?;
            if !encoded.is_empty() && bytes + value.len() > budget {
                break;
            }
            bytes += value.len();
            encoded.push((value, metadata_only(op)));
            if bytes >= budget || started.elapsed() >= window {
                break;
            }
        }
        self.store
            .write(move |tx| {
                let mut positions = Vec::new();
                let (committed, observation): (bool, bool) = tx.query_row(
                    "SELECT committed,observation FROM batches WHERE input=?",
                    [input],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )?;
                for (index, (value, metadata)) in encoded.into_iter().enumerate() {
                    let ordinal = first + index as u64;
                    let exists = tx.query_row(
                        "SELECT EXISTS(SELECT 1 FROM receipts WHERE input=? AND ordinal=?)",
                        params![input, ordinal],
                        |r| r.get::<_, bool>(0),
                    )?;
                    if exists {
                        positions.push(None);
                        continue;
                    }
                    anyhow::ensure!(!committed, "cannot change a completed translation input");
                    save_metadata(tx, &metadata, observation)?;
                    let bytes = value.len();
                    tx.execute(
                        "INSERT INTO ops(input,value) VALUES (?,?)",
                        params![input, value],
                    )?;
                    let position = tx.last_insert_rowid() as u64;
                    positions.push(Some((position, bytes)));
                    tx.execute("UPDATE publication SET tail=? WHERE id=1", [position])?;
                    tx.execute(
                        "INSERT INTO receipts(input,ordinal) VALUES (?,?)",
                        params![input, ordinal],
                    )?;
                }
                Ok(positions)
            })
            .await
    }

    /// Published ops after `after`, bounded by bytes and elapsed decode time.
    pub async fn read_after(
        &self,
        after: u64,
        budget: usize,
        window: std::time::Duration,
    ) -> anyhow::Result<Vec<StoredRecord>> {
        self.store.run(move |db| {
            let mut statement = db.prepare_cached("SELECT position,value FROM ops JOIN batches USING(input) WHERE (committed=1 OR EXISTS(SELECT 1 FROM receipts WHERE receipts.input=ops.input)) AND position>? ORDER BY position")?;
            let mut rows = statement.query([after])?;
            let started = std::time::Instant::now();
            let mut records = Vec::new();
            let mut bytes = 0;
            while let Some(row) = rows.next()? {
                let value = row.get_ref(1)?.as_blob()?;
                if !records.is_empty() && bytes + value.len() > budget { break; }
                bytes += value.len();
                records.push(StoredRecord { position: row.get(0)?, op: StoredOp::decode(value)?, bytes: value.len() });
                if bytes >= budget || started.elapsed() >= window { break; }
            }
            Ok(records)
        }).await
    }

    /// The saved translator continuation and the input it covers.
    pub async fn checkpoint(&self) -> anyhow::Result<Option<(u64, Vec<u8>)>> {
        self.store
            .run(|db| {
                let saved: Option<(u32, u64, Vec<u8>)> = db
                    .query_row(
                        "SELECT version,through,value FROM source_checkpoint WHERE id=1",
                        [],
                        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                    )
                    .optional()?;
                saved
                    .map(|(version, through, value)| {
                        anyhow::ensure!(
                            version == 1,
                            "unsupported translator checkpoint version {version}"
                        );
                        Ok((through, value))
                    })
                    .transpose()
            })
            .await
    }

    /// Transfer ownership of consumed input to its output in one transaction:
    /// remove the events through `input`, commit the batch, replace root
    /// candidates and save the translator continuation. Returns the new tail.
    pub async fn commit_translation(
        &self,
        events: &EventCollection,
        input: u64,
        roots: &BTreeMap<String, Vec<SpanOp>>,
        state: Vec<u8>,
    ) -> anyhow::Result<u64> {
        anyhow::ensure!(
            self.store.same(events.store()),
            "translation input and output must share a session database"
        );
        let roots = encode_roots(roots)?;
        self.store
            .write(move |tx| {
                EventCollection::remove_through(tx, input)?;
                commit_batch(tx, input, Some(roots), Some(state))
            })
            .await
    }

    #[cfg(test)]
    pub async fn commit(
        &self,
        input: u64,
        roots: Option<&BTreeMap<String, Vec<SpanOp>>>,
    ) -> anyhow::Result<u64> {
        let roots = roots.map(encode_roots).transpose()?;
        self.store
            .write(move |tx| commit_batch(tx, input, roots, None))
            .await
    }

    pub async fn root_candidates(&self) -> anyhow::Result<BTreeMap<String, Vec<SpanOp>>> {
        self.store
            .run(|db| {
                let mut query = db.prepare("SELECT span_id,value FROM root_candidates")?;
                let roots = query
                    .query_map([], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
                    })?
                    .map(|row| {
                        let (id, bytes) = row?;
                        let ops: Vec<Vec<u8>> = serde_json::from_slice(&bytes)?;
                        Ok((
                            id,
                            ops.iter()
                                .map(|bytes| StoredOp::decode(bytes))
                                .collect::<anyhow::Result<Vec<_>>>()?,
                        ))
                    })
                    .collect();
                roots
            })
            .await
    }

    /// Create the route's head if needed and return its position. A new head
    /// starts after ops a legacy delivery checkpoint proved were delivered.
    pub async fn subscribe(
        &self,
        route: &SessionRoute,
        legacy_through: u64,
    ) -> anyhow::Result<u64> {
        let key = serde_json::to_string(route)?;
        let candidate = route.clone();
        self.store
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
                let legacy: u64 = tx.query_row(
                    "SELECT coalesce(max(position),0) FROM ops JOIN batches USING(input)
                     WHERE input<=? AND committed=1",
                    [legacy_through],
                    |r| r.get(0),
                )?;
                tx.execute(
                    "INSERT OR IGNORE INTO heads(route,position) VALUES (?,max(?,(SELECT floor FROM publication WHERE id=1)))",
                    params![key, legacy],
                )?;
                Ok(tx.query_row("SELECT position FROM heads WHERE route=?", [key], |r| {
                    r.get(0)
                })?)
            })
            .await
    }

    pub async fn tail(&self) -> anyhow::Result<u64> {
        self.store.run(|db| Ok(publication_tail(db)?)).await
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

    /// Apply a pre-collection delivery checkpoint to the route's head.
    pub async fn acknowledge_legacy(&self, route: &SessionRoute, input: u64) -> anyhow::Result<()> {
        let route = serde_json::to_string(route)?;
        self.store
            .write(move |tx| {
                let position: u64 = tx.query_row(
                    "SELECT coalesce(max(position),0) FROM ops WHERE input<=?",
                    [input],
                    |r| r.get(0),
                )?;
                tx.execute(
                    "INSERT INTO heads(route,position) VALUES(?,?) ON CONFLICT(route) DO UPDATE SET position=max(position,excluded.position)",
                    params![route, position],
                )?;
                prune(tx)
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
                        "INSERT OR IGNORE INTO heads(route,position) VALUES (?,(SELECT tail FROM publication WHERE id=1))",
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
                if committed_through(db)? < input {
                    return Ok(false);
                }
                let tail = publication_tail(db)?;
                let mut query =
                    db.prepare("SELECT route FROM heads WHERE failure IS NULL AND position>=?")?;
                for stored in query.query_map([tail], |r| r.get::<_, String>(0))? {
                    let stored: SessionRoute = serde_json::from_str(&stored?)?;
                    if stored.same_route(&route) {
                        return Ok(true);
                    }
                }
                Ok(false)
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

    /// Every obligation is met: all input through `input` is translated and
    /// every route has delivered every op. An empty route set is not completion.
    pub async fn complete(&self, input: u64) -> anyhow::Result<bool> {
        self.store.run(move |db| Ok(db.query_row("SELECT EXISTS(SELECT 1 FROM heads)
            AND NOT EXISTS(SELECT 1 FROM heads WHERE failure IS NOT NULL OR position < (SELECT tail FROM publication WHERE id=1))
            AND NOT EXISTS(SELECT 1 FROM recovery WHERE failure IS NOT NULL)
            AND NOT EXISTS(SELECT 1 FROM batches WHERE committed=0)
            AND NOT EXISTS(SELECT 1 FROM root_candidates)
            AND NOT EXISTS(SELECT 1 FROM events)
            AND (SELECT coalesce(max(input),0) FROM batches WHERE committed=1)>=?",[input],|r|r.get(0))?)).await
    }

    pub async fn source_failure(&self) -> anyhow::Result<Option<String>> {
        self.store
            .run(|db| {
                Ok(db
                    .query_row("SELECT failure FROM recovery WHERE id=1", [], |row| {
                        row.get(0)
                    })
                    .optional()?
                    .flatten())
            })
            .await
    }

    pub async fn set_source_failure(&self, failure: Option<String>) -> anyhow::Result<()> {
        self.store
            .run(move |db| {
                db.execute(
                    "INSERT OR REPLACE INTO recovery(id,failure) VALUES(1,?)",
                    [failure],
                )?;
                Ok(())
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

fn commit_batch(
    tx: &rusqlite::Transaction<'_>,
    input: u64,
    roots: Option<Vec<(String, Vec<u8>)>>,
    state: Option<Vec<u8>>,
) -> anyhow::Result<u64> {
    anyhow::ensure!(
        input >= committed_through(tx)?,
        "translation commits must be ordered"
    );
    let earlier: u64 = tx.query_row(
        "SELECT count(*) FROM batches WHERE committed=0 AND input<?",
        [input],
        |row| row.get(0),
    )?;
    anyhow::ensure!(
        earlier == 0,
        "translation cannot publish past an unfinished input"
    );
    anyhow::ensure!(
        tx.execute("UPDATE batches SET committed=1 WHERE input=?", [input])? == 1,
        "missing translation batch"
    );
    if let Some(roots) = roots {
        tx.execute("DELETE FROM root_candidates", [])?;
        for (id, value) in roots {
            tx.execute(
                "INSERT INTO root_candidates(span_id,value) VALUES (?,?)",
                params![id, value],
            )?;
        }
    }
    let tail: u64 = tx.query_row(
        "SELECT max((SELECT floor FROM publication WHERE id=1),coalesce(max(position),0)) FROM ops WHERE input<=?",
        [input],
        |r| r.get(0),
    )?;
    tx.execute("UPDATE publication SET tail=max(tail,?) WHERE id=1", [tail])?;
    let tail = publication_tail(tx)?;
    prune(tx)?;
    super::store::touch(tx)?;
    if let Some(state) = state {
        tx.execute(
            "INSERT OR REPLACE INTO source_checkpoint VALUES(1,1,?,?)",
            params![input, state],
        )?;
        // The continuation replaces every earlier batch's bookkeeping.
        tx.execute("DELETE FROM batches WHERE input<? AND committed=1 AND NOT EXISTS(SELECT 1 FROM ops WHERE ops.input=batches.input) AND NOT EXISTS(SELECT 1 FROM receipts WHERE receipts.input=batches.input)", [input])?;
    }
    Ok(tail)
}

#[cfg(test)]
impl SpanOps {
    pub async fn stored_ops(&self) -> u64 {
        self.store
            .run(|db| Ok(db.query_row("SELECT count(*) FROM ops", [], |r| r.get(0))?))
            .await
            .unwrap_or(u64::MAX)
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

    /// Insert ops without receipts, as schema-5 translation did.
    pub async fn insert_unreceipted(&self, input: u64, ops: &[SpanOp]) -> anyhow::Result<()> {
        let encoded = ops
            .iter()
            .map(StoredOp::encode)
            .collect::<anyhow::Result<Vec<_>>>()?;
        self.store
            .write(move |tx| {
                let done: bool = tx.query_row(
                    "SELECT committed FROM batches WHERE input=?",
                    [input],
                    |r| r.get(0),
                )?;
                anyhow::ensure!(!done, "cannot change a committed translation input");
                for value in encoded {
                    save_metadata(tx, &StoredOp::decode(&value)?, true)?;
                    tx.execute(
                        "INSERT INTO ops(input,value) VALUES (?,?)",
                        params![input, value],
                    )?;
                }
                Ok(())
            })
            .await
    }

    /// Observation ops visible to delivery, for assertions.
    pub async fn published(&self, after: u64, limit: usize) -> anyhow::Result<Vec<(u64, SpanOp)>> {
        self.store.run(move |db| {
            let mut statement = db.prepare("SELECT position,value FROM ops JOIN batches USING(input) WHERE (committed=1 OR EXISTS(SELECT 1 FROM receipts WHERE receipts.input=ops.input)) AND observation=1 AND position>? ORDER BY position LIMIT ?")?;
            let rows = statement.query_map(params![after,limit as u64], |r| Ok((r.get::<_,u64>(0)?,r.get::<_,Vec<u8>>(1)?)))?;
            rows.map(|row| { let (position,bytes)=row?; Ok((position,StoredOp::decode(&bytes)?)) }).collect()
        }).await
    }
}

/// Upgrades from SpanOp channel schemas that predate collections.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::journal::SpanJournal;

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

    #[tokio::test]
    async fn schema_five_upgrade_reclaims_acknowledged_payloads_without_losing_provenance() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("upgrade.sqlite");
        let journal = SpanJournal::open(&path).await.unwrap();
        journal.register(&route("old")).await.unwrap();
        journal.begin(10, true).await.unwrap();
        journal.append(10, &[op("old")]).await.unwrap();
        journal.commit(10).await.unwrap();
        drop(journal);
        let db = Connection::open(&path).unwrap();
        db.execute_batch("UPDATE heads SET position=1; DROP TABLE receipts; DROP TABLE publication; DROP TABLE origins; DROP TABLE span_state; DROP TABLE events; DROP TABLE event_state; DROP TABLE source_checkpoint; PRAGMA user_version=5").unwrap();
        drop(db);
        let upgraded = SpanJournal::open(&path).await.unwrap();
        assert_eq!(upgraded.tail().await.unwrap(), 1);
        assert_eq!(upgraded.origins().await.unwrap()["old"].bt_version, "test");
        assert_eq!(upgraded.stored_ops().await, 0);
        assert!(upgraded.complete(10).await.unwrap());
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
                SpanJournal::open(&path).await
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
        let journal = SpanJournal::open(&path).await.unwrap();
        let mut peer = Connection::open(&path).unwrap();
        let transaction = peer
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .unwrap();
        transaction
            .execute("INSERT INTO batches(input,committed) VALUES(10,1)", [])
            .unwrap();
        let begin = tokio::spawn(async move { journal.begin(10, true).await });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let failed_early = begin.is_finished();
        transaction.commit().unwrap();
        assert!(
            !begin.await.unwrap().unwrap(),
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
        let journal = SpanJournal::open(&path).await.unwrap();
        assert_eq!(journal.translated_through().await.unwrap(), 10);
        journal.begin(20, false).await.unwrap();
        journal.append(20, &[op("retirement")]).await.unwrap();
        journal.commit(20).await.unwrap();
        assert!(
            journal.published(0, 8).await.unwrap().is_empty(),
            "retirement output is not a native correlation observation"
        );
        let mut route = journal.subscribe(&route("a"), 0).await.unwrap();
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
        let journal = SpanJournal::open(&path).await.unwrap();
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
        let restored = journal.subscribe(&route, 0).await.unwrap();
        assert_eq!(restored.completed_position(), 7);
        let binding: String = Connection::open(&path)
            .unwrap()
            .query_row("SELECT binding FROM heads", [], |r| r.get(0))
            .unwrap();
        assert!(!binding.contains("secret-"));
    }
}
