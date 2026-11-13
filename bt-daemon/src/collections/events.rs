//! Captured input for one source session. Capture inserts a redacted event and
//! acknowledges it only after the insert commits. Translation reads records in
//! order and removes them in the same transaction that stores their output.
//!
//! Record IDs grow by each record's encoded size, so IDs imported from a legacy
//! NDJSON journal keep their original byte boundaries and input budgets still
//! measure bytes. Removal never renumbers later records.
use super::store::One;
use super::store::{Json, Store, NOW_MS};
use crate::pipeline::input::{PipelineControl, SourcePosition};
use crate::wire::RedactedEnvelope;
use rusqlite::{params, OptionalExtension};
use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
};

use sha2::{Digest, Sha256};

const READ_RECORDS: usize = 32;
/// Payload fields the daemon adds to record what it captured alongside an event.
const CAPTURE_REFERENCES: [&str; 3] = [
    "_bt_claude_transcript_mirrors",
    "_bt_transcript_mirror",
    "_bt_transcript_replay",
];
const READ_BYTES: usize = 256 * 1024;
/// Payload fields that identify on-disk state the session still references.
const IDENTITY_PAYLOAD: [&str; 3] = [
    "transcript_path",
    "agent_transcript_path",
    "_bt_transcript_mirror",
];

#[derive(Debug)]
// Keep event records inline on the read path; the capture completeness bit
// makes this enum just exceed Clippy's size threshold.
#[allow(clippy::large_enum_variant)]
pub(crate) enum EventRecord {
    Event(RedactedEnvelope),
    Control(PipelineControl),
}

impl From<&crate::wire::Envelope> for EventRecord {
    fn from(env: &crate::wire::Envelope) -> Self {
        Self::Event(env.redacted())
    }
}

impl From<PipelineControl> for EventRecord {
    fn from(control: PipelineControl) -> Self {
        Self::Control(control)
    }
}

#[derive(Debug)]
pub(crate) struct EventEntry {
    pub id: u64,
    pub record: EventRecord,
}

/// What a fully imported legacy file implies about its session.
pub(super) struct Imported {
    /// The latest event, with only state-referencing payload fields.
    pub identity: Option<RedactedEnvelope>,
    /// Whether this source keeps the pre-qualification span-id namespace.
    pub namespace: Option<bool>,
    /// The file's length, the boundary for new records.
    pub through: u64,
    /// Input every route had delivered, already translated.
    pub delivered: u64,
}

/// A record as stored: its kind, and the event or control as JSON.
#[derive(PartialEq)]
pub(super) struct Stored {
    pub control: bool,
    pub value: Vec<u8>,
}

impl TryFrom<&EventRecord> for Stored {
    type Error = serde_json::Error;
    fn try_from(record: &EventRecord) -> serde_json::Result<Self> {
        Ok(match record {
            EventRecord::Event(event) => Self {
                control: false,
                value: serde_json::to_vec(event)?,
            },
            EventRecord::Control(control) => Self {
                control: true,
                value: serde_json::to_vec(control)?,
            },
        })
    }
}

impl TryFrom<&Stored> for EventRecord {
    type Error = serde_json::Error;
    fn try_from(stored: &Stored) -> serde_json::Result<Self> {
        Ok(if stored.control {
            Self::Control(serde_json::from_slice(&stored.value)?)
        } else {
            Self::Event(serde_json::from_slice(&stored.value)?)
        })
    }
}

/// The database file for one source session.
pub(crate) fn session_path(data_dir: &Path, source: &str, session_id: &str) -> PathBuf {
    data_dir.join("pipeline").join(format!(
        "{}.sqlite",
        crate::ids::session_storage_id(source, session_id)
    ))
}

#[derive(Clone)]
pub(crate) struct EventCollection {
    store: Store,
}

impl EventCollection {
    /// Open the session's input, first importing any legacy NDJSON journal for
    /// it. A legacy file is deleted only after its records are committed here.
    pub async fn open(data_dir: &Path, source: &str, session_id: &str) -> anyhow::Result<Self> {
        let source = crate::translate::canonical_source_name(source).to_owned();
        let events = Self::claim(data_dir, &source, session_id).await?;
        super::legacy::import_session(data_dir, &source, session_id, &events).await?;
        Ok(events)
    }

    /// Open the session database without legacy import, recording its owner.
    pub(super) async fn claim(
        data_dir: &Path,
        source: &str,
        session_id: &str,
    ) -> anyhow::Result<Self> {
        let events = Self::open_path(&session_path(data_dir, source, session_id)).await?;
        let (source, session_id) = (source.to_owned(), session_id.to_owned());
        events
            .store
            .write(move |tx| {
                tx.execute(
                    "UPDATE event_state SET source=?, session_id=? WHERE id=1 AND source IS NULL",
                    params![source, session_id],
                )?;
                Ok(())
            })
            .await?;
        Ok(events)
    }

    pub(super) async fn open_path(path: &Path) -> anyhow::Result<Self> {
        Ok(Self {
            store: Store::open(path).await?,
        })
    }

    pub(super) fn store(&self) -> &Store {
        &self.store
    }

    /// The ID of the last record ever inserted, including removed records.
    pub async fn tail(&self) -> anyhow::Result<u64> {
        self.store
            .run(|db| Ok(db.one("SELECT tail FROM event_state WHERE id=1", [])?))
            .await
    }

    /// The source and native session this database belongs to.
    pub async fn owner(&self) -> anyhow::Result<Option<(String, String)>> {
        self.store
            .run(|db| {
                Ok(db
                    .query_row(
                        "SELECT source, session_id FROM event_state WHERE id=1 AND source IS NOT NULL",
                        [],
                        |r| Ok((r.get(0)?, r.get(1)?)),
                    )
                    .optional()?)
            })
            .await
    }

    /// Append a record and return the ID range it occupies. The caller may
    /// acknowledge it once this returns: it is durable and visible to
    /// translation. Returns `None` when nothing was appended:
    /// - an event already captured, such as a re-import or a capture retried
    ///   across a daemon handover, is acknowledged without a second record;
    /// - with `after`, a record lands only if nothing else has since `after`,
    ///   so a control is never ordered ahead of work translation has not seen.
    pub async fn append(
        &self,
        record: impl Into<EventRecord>,
        after: Option<u64>,
    ) -> anyhow::Result<Option<SourcePosition>> {
        let mut record = record.into();
        let capture = match &mut record {
            EventRecord::Event(event) => {
                let capture = (digest(event)?, identity(event));
                compact_pi_payload(event);
                Some(capture)
            }
            EventRecord::Control(_) => None,
        };
        let stored = Stored::try_from(&record)?;
        self.store
            .write(move |tx| {
                let tail: u64 = tx.one("SELECT tail FROM event_state WHERE id=1", [])?;
                if after.is_some_and(|after| tail > after) {
                    return Ok(None);
                }
                let Some((digest, mut identity)) = capture else {
                    return insert(tx, stored).map(Some);
                };
                if tx.execute(
                    &format!("INSERT OR IGNORE INTO captured(digest,at_ms) VALUES(?,{NOW_MS})"),
                    [digest],
                )? == 0
                {
                    return Ok(None);
                }
                let previous: Option<Json<RedactedEnvelope>> =
                    tx.one("SELECT identity FROM event_state WHERE id=1", [])?;
                if let Some(Json(previous)) = previous {
                    retain_references(&mut identity, &previous);
                }
                let appended = insert(tx, stored)?;
                tx.execute(
                    "UPDATE event_state SET identity=? WHERE id=1",
                    [Json(&identity)],
                )?;
                Ok(Some(appended))
            })
            .await
    }

    /// The latest captured event with only state-referencing payload fields.
    pub async fn identity(&self) -> anyhow::Result<Option<RedactedEnvelope>> {
        self.store
            .run(|db| {
                let identity: Option<Json<_>> =
                    db.one("SELECT identity FROM event_state WHERE id=1", [])?;
                Ok(identity.map(|Json(identity)| identity))
            })
            .await
    }

    /// Time since capture, translation or delivery last made progress.
    pub async fn idle_for(&self) -> anyhow::Result<std::time::Duration> {
        let active: i64 = self
            .store
            .run(|db| Ok(db.one("SELECT active_ms FROM event_state WHERE id=1", [])?))
            .await?;
        let now = chrono::Utc::now().timestamp_millis();
        Ok(std::time::Duration::from_millis(
            u64::try_from(now.saturating_sub(active)).unwrap_or(0),
        ))
    }

    /// Whether this source kept the pre-qualification span-id namespace.
    pub async fn legacy_namespace(&self) -> anyhow::Result<bool> {
        self.store
            .run(|db| Ok(db.one("SELECT legacy_namespace FROM event_state WHERE id=1", [])?))
            .await
    }

    /// Lazily read records with IDs in `(after, through]`. Reading never
    /// changes membership; only a committed translation removes records.
    pub fn read(&self, after: u64, through: u64) -> EventReader {
        EventReader {
            events: self.clone(),
            position: after,
            through,
            buffer: VecDeque::new(),
        }
    }

    /// Import legacy records at their original byte boundaries. Retrying is
    /// idempotent. The last batch of a file carries what the file implies
    /// about the session as a whole.
    pub(super) async fn import(
        &self,
        records: Vec<(u64, Stored)>,
        finished: Option<Imported>,
    ) -> anyhow::Result<()> {
        self.store
            .write(move |tx| {
                for (id, stored) in records {
                    anyhow::ensure!(
                        id <= i64::MAX as u64,
                        "legacy position exceeds storage range"
                    );
                    let prior = tx
                        .query_row("SELECT control,value FROM events WHERE id=?", [id], |r| {
                            Ok(Stored {
                                control: r.get(0)?,
                                value: r.get(1)?,
                            })
                        })
                        .optional()?;
                    match prior {
                        Some(prior) => anyhow::ensure!(
                            prior == stored,
                            "legacy record changed during migration"
                        ),
                        None => {
                            tx.execute(
                                "INSERT INTO events(id,control,value) VALUES(?,?,?)",
                                params![id, stored.control, stored.value],
                            )?;
                        }
                    }
                    tx.execute("UPDATE event_state SET tail=max(tail,?) WHERE id=1", [id])?;
                }
                let Some(finished) = finished else {
                    return Ok(());
                };
                if let Some(identity) = &finished.identity {
                    tx.execute(
                        "UPDATE event_state SET identity=? WHERE id=1",
                        [Json(identity)],
                    )?;
                }
                if let Some(namespace) = finished.namespace {
                    tx.execute(
                        "UPDATE event_state SET legacy_namespace=? WHERE id=1",
                        [namespace],
                    )?;
                }
                tx.execute(
                    "UPDATE event_state SET tail=max(tail,?) WHERE id=1",
                    [finished.through],
                )?;
                super::span_ops::seed_delivered(tx, finished.delivered)
            })
            .await
    }

    /// Forget capture identities older than `retention`: a capture repeated
    /// after that is recorded again.
    pub async fn expire(&self, retention: std::time::Duration) -> anyhow::Result<()> {
        let cutoff = cutoff_ms(retention);
        self.store
            .write(move |tx| {
                tx.execute("DELETE FROM captured WHERE at_ms<?", [cutoff])?;
                Ok(())
            })
            .await
    }

    pub(super) fn remove_through(
        tx: &rusqlite::Transaction<'_>,
        through: u64,
    ) -> anyhow::Result<()> {
        tx.execute("DELETE FROM events WHERE id<=?", [through])?;
        Ok(())
    }
}

/// The wall-clock time `retention` ago, in milliseconds.
pub(super) fn cutoff_ms(retention: std::time::Duration) -> i64 {
    chrono::Utc::now().timestamp_millis() - i64::try_from(retention.as_millis()).unwrap_or(i64::MAX)
}

/// What identifies one capture of a native event. Route and capture context
/// are part of it: hooks may deliver one native event to several
/// destinations, each as its own capture. Daemon-added transcript references
/// describe the capture, not the native event.
fn digest(event: &RedactedEnvelope) -> anyhow::Result<Vec<u8>> {
    let mut payload = event.payload.clone();
    if let Some(payload) = payload.as_object_mut() {
        for key in CAPTURE_REFERENCES {
            payload.remove(key);
        }
    }
    Ok(Sha256::digest(serde_json::to_vec(&(
        &event.event,
        event.ts_ms,
        &payload,
        &event.route,
        &event.capture,
    ))?)
    .to_vec())
}

/// Keep on-disk references an earlier identity recorded that `identity` omits.
pub(super) fn retain_references(identity: &mut RedactedEnvelope, previous: &RedactedEnvelope) {
    if let (Some(payload), Some(old)) = (
        identity.payload.as_object_mut(),
        previous.payload.as_object(),
    ) {
        for key in IDENTITY_PAYLOAD {
            if let (false, Some(value)) = (payload.contains_key(key), old.get(key)) {
                payload.insert(key.into(), value.clone());
            }
        }
    }
}

/// The event with only the payload fields that reference on-disk state.
pub(super) fn identity(event: &RedactedEnvelope) -> RedactedEnvelope {
    let mut identity = event.clone();
    identity.payload = serde_json::Value::Object(
        IDENTITY_PAYLOAD
            .iter()
            .filter_map(|key| Some(((*key).into(), event.payload.get(*key)?.clone())))
            .collect(),
    );
    identity
}

fn insert(tx: &rusqlite::Transaction<'_>, stored: Stored) -> anyhow::Result<SourcePosition> {
    let before: u64 = tx.one("SELECT tail FROM event_state WHERE id=1", [])?;
    let through = before
        .checked_add(stored.value.len() as u64 + 1)
        .filter(|id| *id <= i64::MAX as u64)
        .ok_or_else(|| anyhow::anyhow!("event ID overflow"))?;
    tx.execute(
        "INSERT INTO events(id,control,value) VALUES(?,?,?)",
        params![through, stored.control, stored.value],
    )?;
    tx.execute("UPDATE event_state SET tail=? WHERE id=1", [through])?;
    super::store::touch(tx)?;
    Ok(SourcePosition { before, through })
}

/// Ordered, batched reader over a range of event records.
pub(crate) struct EventReader {
    events: EventCollection,
    position: u64,
    through: u64,
    buffer: VecDeque<(u64, Stored)>,
}

impl EventReader {
    pub fn position(&self) -> u64 {
        self.position
    }

    pub fn extend(&mut self, through: u64) {
        self.through = self.through.max(through);
    }

    pub async fn next(&mut self) -> anyhow::Result<Option<EventEntry>> {
        if self.buffer.is_empty() && self.position < self.through {
            let (after, through) = (self.position, self.through);
            self.buffer = self
                .events
                .store
                .run(move |db| {
                    let mut query = db.prepare_cached(
                        "SELECT id,control,value FROM events WHERE id>? AND id<=? ORDER BY id LIMIT ?",
                    )?;
                    let mut rows = query.query(params![after, through, READ_RECORDS])?;
                    let mut records = VecDeque::new();
                    let mut bytes = 0;
                    while let Some(row) = rows.next()? {
                        let stored = Stored {
                            control: row.get(1)?,
                            value: row.get(2)?,
                        };
                        if !records.is_empty() && bytes + stored.value.len() > READ_BYTES {
                            break;
                        }
                        bytes += stored.value.len();
                        records.push_back((row.get(0)?, stored));
                    }
                    Ok(records)
                })
                .await?;
        }
        let Some((id, stored)) = self.buffer.pop_front() else {
            return Ok(None);
        };
        let record =
            match tokio::task::spawn_blocking(move || EventRecord::try_from(&stored)).await? {
                Ok(record) => record,
                Err(error) => {
                    self.buffer.clear();
                    return Err(anyhow::Error::new(error).context(format!("event record {id}")));
                }
            };
        self.position = id;
        Ok(Some(EventEntry { id, record }))
    }

    /// The next native event, skipping controls.
    pub async fn next_event(&mut self) -> anyhow::Result<Option<RedactedEnvelope>> {
        while let Some(entry) = self.next().await? {
            if let EventRecord::Event(event) = entry.record {
                return Ok(Some(event));
            }
        }
        Ok(None)
    }
}

/// Drop Pi payload fields the translator never reads before they reach disk.
fn compact_pi_payload(event: &mut RedactedEnvelope) {
    if event.source != "pi" {
        return;
    }
    let Some(native) = event
        .payload
        .get_mut("event")
        .and_then(serde_json::Value::as_object_mut)
    else {
        return;
    };
    match event.event.as_str() {
        "before_provider_request" => {
            // The translator records only the allowlisted request
            // configuration and tool definitions, never the prompt or messages.
            if let Some(payload) = native.get_mut("payload") {
                *payload = serde_json::Value::Object(crate::translate::pi_request_config(payload));
            }
        }
        "agent_end" => {
            // The translator only consumes willRetry from this lifecycle event.
            native.remove("messages");
        }
        "message_update" => {
            // Pi repeats the progressively growing assistant message on every
            // streaming update. Translation only needs the update type to mark
            // time-to-first-token; the completed message arrives separately.
            native.remove("message");
            if let Some(update) = native
                .get_mut("assistantMessageEvent")
                .and_then(serde_json::Value::as_object_mut)
            {
                update.retain(|key, _| key == "type");
            }
            native.retain(|key, _| key == "type" || key == "assistantMessageEvent");
        }
        _ => {}
    }
}
