//! Captured input for one source session. Capture inserts a redacted event and
//! acknowledges it only after the insert commits. Translation reads records in
//! order and removes them in the same transaction that stores their output.
//!
//! Record IDs grow by each record's encoded size, so IDs imported from a legacy
//! NDJSON journal keep their original byte boundaries and input budgets still
//! measure bytes. Removal never renumbers later records.
use super::store::{read_batch, Json, One, SessionStorage, NOW_MS};
use crate::pipeline::input::{PipelineControl, SourcePosition};
use crate::wire::RedactedEnvelope;
use futures::{future::BoxFuture, ready, Stream, TryStreamExt};
use rusqlite::params;
use sha2::{Digest, Sha256};
use std::{
    collections::VecDeque,
    pin::Pin,
    task::{Context, Poll},
};

const READ_RECORDS: usize = 32;
const READ_BYTES: usize = 256 * 1024;
/// Payload fields the daemon adds to record what it captured alongside an event.
const CAPTURE_REFERENCES: [&str; 3] = [
    "_bt_claude_transcript_mirrors",
    "_bt_transcript_mirror",
    "_bt_transcript_replay",
];
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

impl SessionStorage {
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
        self.write(move |tx| {
            let tail: u64 = tx.one("SELECT event_tail FROM session WHERE id=1", [])?;
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
                tx.one("SELECT identity FROM session WHERE id=1", [])?;
            if let Some(Json(previous)) = previous {
                retain_references(&mut identity, &previous);
            }
            let appended = insert(tx, stored)?;
            tx.execute(
                "UPDATE session SET identity=? WHERE id=1",
                [Json(&identity)],
            )?;
            Ok(Some(appended))
        })
        .await
    }

    /// The latest captured event with only state-referencing payload fields.
    pub async fn identity(&self) -> anyhow::Result<Option<RedactedEnvelope>> {
        self.run(|db| {
            let identity: Option<Json<_>> =
                db.one("SELECT identity FROM session WHERE id=1", [])?;
            Ok(identity.map(|Json(identity)| identity))
        })
        .await
    }

    /// Lazily read records with IDs in `(after, through]`. Reading never
    /// changes membership; only a committed translation removes records.
    pub fn read(&self, after: u64, through: u64) -> EventReader {
        EventReader {
            storage: self.clone(),
            position: after,
            through,
            buffer: VecDeque::new(),
            fetch: None,
        }
    }
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
        if let Some(old_children) = old
            .get("_bt_claude_children")
            .and_then(serde_json::Value::as_object)
        {
            let children = payload
                .entry("_bt_claude_children")
                .or_insert_with(|| serde_json::json!({}));
            if let Some(children) = children.as_object_mut() {
                for (agent, observation) in old_children {
                    children
                        .entry(agent.clone())
                        .or_insert_with(|| observation.clone());
                }
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
    if event.source == "claude-code"
        && matches!(event.event.as_str(), "SubagentStop" | "TranscriptUpdate")
    {
        if let (Some(agent), Some(path)) = (
            event
                .payload
                .get("agent_id")
                .and_then(serde_json::Value::as_str),
            event
                .payload
                .get("agent_transcript_path")
                .and_then(serde_json::Value::as_str),
        ) {
            let through = event
                .payload
                .get("_bt_claude_transcript_mirrors")
                .and_then(|m| m.get(path))
                .and_then(|m| m.get("through"))
                .cloned();
            identity.payload["_bt_claude_children"] =
                serde_json::json!({agent: {"path": path, "through": through}});
        }
    }
    identity
}

fn insert(tx: &rusqlite::Transaction<'_>, stored: Stored) -> anyhow::Result<SourcePosition> {
    let before: u64 = tx.one("SELECT event_tail FROM session WHERE id=1", [])?;
    let through = before
        .checked_add(stored.value.len() as u64 + 1)
        .filter(|id| *id <= i64::MAX as u64)
        .ok_or_else(|| anyhow::anyhow!("event ID overflow"))?;
    tx.execute(
        "INSERT INTO events(id,control,value) VALUES(?,?,?)",
        params![through, stored.control, stored.value],
    )?;
    tx.execute("UPDATE session SET event_tail=? WHERE id=1", [through])?;
    super::store::touch(tx)?;
    Ok(SourcePosition { before, through })
}

type Batch = VecDeque<(u64, serde_json::Result<EventRecord>)>;

/// An ordered stream of the event records in a range, fetched in batches.
/// It ends at the end of the range; [`extend`](Self::extend) lets it
/// continue as later records arrive.
pub(crate) struct EventReader {
    storage: SessionStorage,
    position: u64,
    through: u64,
    /// Each fetched record decoded, or why it could not be.
    buffer: Batch,
    fetch: Option<BoxFuture<'static, anyhow::Result<Batch>>>,
}

impl EventReader {
    /// The ID of the last record read.
    pub fn position(&self) -> u64 {
        self.position
    }

    pub fn extend(&mut self, through: u64) {
        self.through = self.through.max(through);
    }

    /// The native events, skipping controls.
    pub fn events(self) -> impl Stream<Item = anyhow::Result<RedactedEnvelope>> {
        self.try_filter_map(|entry| async move {
            Ok(match entry.record {
                EventRecord::Event(event) => Some(event),
                EventRecord::Control(_) => None,
            })
        })
    }

    fn fetch(&self) -> BoxFuture<'static, anyhow::Result<Batch>> {
        let (storage, after, through) = (self.storage.clone(), self.position, self.through);
        Box::pin(async move {
            let batch = storage
                .run(move |db| {
                    read_batch(
                        db,
                        "SELECT id,control,value FROM events WHERE id>? AND id<=? ORDER BY id LIMIT ?",
                        params![after, through, READ_RECORDS],
                        READ_BYTES,
                        std::time::Duration::MAX,
                        |row| {
                            let stored = Stored {
                                control: row.get(1)?,
                                value: row.get(2)?,
                            };
                            let size = stored.value.len();
                            Ok((size, (row.get(0)?, EventRecord::try_from(&stored))))
                        },
                    )
                })
                .await?;
            Ok(batch.into())
        })
    }
}

impl Stream for EventReader {
    type Item = anyhow::Result<EventEntry>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        if this.buffer.is_empty() && this.position < this.through {
            let fetch = match this.fetch.take() {
                Some(fetch) => fetch,
                None => this.fetch(),
            };
            let fetch = this.fetch.insert(fetch);
            let batch = ready!(fetch.as_mut().poll(cx));
            this.fetch = None;
            this.buffer = match batch {
                Ok(batch) => batch,
                Err(error) => return Poll::Ready(Some(Err(error))),
            };
        }
        let Some((id, record)) = this.buffer.pop_front() else {
            return Poll::Ready(None);
        };
        Poll::Ready(Some(match record {
            Ok(record) => {
                this.position = id;
                Ok(EventEntry { id, record })
            }
            Err(error) => {
                // Leave the position at the last good record, so a retry
                // reads the undecodable record again.
                this.buffer.clear();
                Err(anyhow::Error::new(error).context(format!("event record {id}")))
            }
        }))
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
