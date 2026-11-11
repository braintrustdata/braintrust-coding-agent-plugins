//! Captured input for one source session. Capture inserts a redacted event and
//! acknowledges it only after the insert commits. Translation reads records in
//! order and removes them in the same transaction that stores their output.
//!
//! Record IDs grow by each record's encoded size, so IDs imported from a legacy
//! NDJSON journal keep their original byte boundaries and input budgets still
//! measure bytes. Removal never renumbers later records.
use super::store::Store;
use crate::pipeline::input::PipelineControl;
use crate::wire::{Envelope, RedactedEnvelope, SessionRoute};
use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};
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
    /// Delivery acknowledgement written by a pre-collection daemon.
    LegacyDeliveryCheckpoint {
        route: SessionRoute,
        through: u64,
    },
}

#[derive(Debug)]
pub(crate) struct EventEntry {
    pub id: u64,
    pub record: EventRecord,
}

/// The ID range a successful insert occupies.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Appended {
    pub before: u64,
    pub through: u64,
}

#[derive(Serialize, Deserialize)]
struct Checkpoint {
    #[serde(rename = "_bt_record_type")]
    record_type: String,
    route: SessionRoute,
    through: u64,
}

pub(super) fn encode(record: &EventRecord) -> anyhow::Result<Vec<u8>> {
    let value = match record {
        EventRecord::Event(event) => serde_json::to_value(event)?,
        EventRecord::Control(control) => {
            let mut value = serde_json::to_value(control)?;
            value["_bt_record_type"] = "pipeline_control".into();
            value
        }
        EventRecord::LegacyDeliveryCheckpoint { route, through } => {
            serde_json::to_value(Checkpoint {
                record_type: "delivery_checkpoint".into(),
                route: route.clone(),
                through: *through,
            })?
        }
    };
    Ok(serde_json::to_vec(&value)?)
}

pub(super) fn decode(bytes: &[u8]) -> anyhow::Result<EventRecord> {
    let value: serde_json::Value = serde_json::from_slice(bytes)?;
    Ok(
        match value
            .get("_bt_record_type")
            .and_then(serde_json::Value::as_str)
        {
            Some("delivery_checkpoint") => {
                let checkpoint: Checkpoint = serde_json::from_value(value)?;
                EventRecord::LegacyDeliveryCheckpoint {
                    route: checkpoint.route,
                    through: checkpoint.through,
                }
            }
            Some("pipeline_control") => EventRecord::Control(serde_json::from_value(value)?),
            _ => EventRecord::Event(serde_json::from_value(value)?),
        },
    )
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
            .run(
                |db| Ok(db.query_row("SELECT tail FROM event_state WHERE id=1", [], |r| r.get(0))?),
            )
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

    /// Insert one redacted event. The caller may acknowledge capture once this
    /// returns: the record is durable and visible to translation.
    #[cfg(test)]
    pub async fn append(&self, env: &Envelope) -> anyhow::Result<Appended> {
        self.insert_event(env, None).await.map(|appended| {
            appended.unwrap_or(Appended {
                before: 0,
                through: 0,
            })
        })
    }

    /// Capture a native event once. The same event captured again, such as a
    /// re-import or a capture retried across a daemon handover, returns `None`
    /// while this session's storage exists.
    pub async fn capture(&self, env: &Envelope) -> anyhow::Result<Option<Appended>> {
        let mut identity = env.redacted();
        if let Some(payload) = identity.payload.as_object_mut() {
            // Daemon-added transcript references describe this capture, not
            // the native event.
            for key in CAPTURE_REFERENCES {
                payload.remove(key);
            }
        }
        // Route and capture context are part of a capture: hooks may deliver
        // one native event to several destinations, each as its own capture.
        let digest = Sha256::digest(serde_json::to_vec(&(
            &identity.event,
            identity.ts_ms,
            &identity.payload,
            &identity.route,
            &identity.capture,
        ))?);
        self.insert_event(env, Some(digest.to_vec())).await
    }

    async fn insert_event(
        &self,
        env: &Envelope,
        digest: Option<Vec<u8>>,
    ) -> anyhow::Result<Option<Appended>> {
        let mut record = env.redacted();
        compact_pi_payload(&mut record);
        let mut identity = env.redacted();
        identity.payload = serde_json::Value::Object(
            IDENTITY_PAYLOAD
                .iter()
                .filter_map(|key| Some(((*key).into(), identity.payload.get(*key)?.clone())))
                .collect(),
        );
        let value = encode(&EventRecord::Event(record))?;
        self.store
            .write(move |tx| {
                if let Some(digest) = digest {
                    if tx.execute("INSERT OR IGNORE INTO captured(digest) VALUES(?)", [digest])?
                        == 0
                    {
                        return Ok(None);
                    }
                }
                let previous: Option<Vec<u8>> =
                    tx.query_row("SELECT identity FROM event_state WHERE id=1", [], |r| {
                        r.get(0)
                    })?;
                // Keep referenced transcript paths that later events omit.
                if let Some(previous) = previous
                    .as_deref()
                    .map(serde_json::from_slice::<RedactedEnvelope>)
                    .transpose()?
                {
                    if let (Some(payload), Some(old)) = (
                        identity.payload.as_object_mut(),
                        previous.payload.as_object(),
                    ) {
                        for key in IDENTITY_PAYLOAD {
                            if let (false, Some(value)) = (payload.contains_key(key), old.get(key))
                            {
                                payload.insert(key.into(), value.clone());
                            }
                        }
                    }
                }
                let appended = insert(tx, value)?;
                tx.execute(
                    "UPDATE event_state SET identity=? WHERE id=1",
                    [serde_json::to_vec(&identity)?],
                )?;
                Ok(Some(appended))
            })
            .await
    }

    /// Insert a control only if no capture has landed after `expected` (the
    /// translator's position). Otherwise the control would be ordered ahead of
    /// work the translator has not yet observed.
    pub async fn append_control(
        &self,
        control: &PipelineControl,
        expected: u64,
    ) -> anyhow::Result<Option<Appended>> {
        let value = encode(&EventRecord::Control(control.clone()))?;
        self.store
            .write(move |tx| {
                let tail: u64 =
                    tx.query_row("SELECT tail FROM event_state WHERE id=1", [], |r| r.get(0))?;
                if tail > expected {
                    return Ok(None);
                }
                insert(tx, value).map(Some)
            })
            .await
    }

    /// The latest captured event with only state-referencing payload fields.
    pub async fn identity(&self) -> anyhow::Result<Option<RedactedEnvelope>> {
        self.store
            .run(|db| {
                let bytes: Option<Vec<u8>> =
                    db.query_row("SELECT identity FROM event_state WHERE id=1", [], |r| {
                        r.get(0)
                    })?;
                bytes
                    .map(|bytes| Ok(serde_json::from_slice(&bytes)?))
                    .transpose()
            })
            .await
    }

    /// Time since capture, translation or delivery last made progress.
    pub async fn idle_for(&self) -> anyhow::Result<std::time::Duration> {
        let active: i64 = self
            .store
            .run(|db| {
                Ok(
                    db.query_row("SELECT active_ms FROM event_state WHERE id=1", [], |r| {
                        r.get(0)
                    })?,
                )
            })
            .await?;
        let now = chrono::Utc::now().timestamp_millis();
        Ok(std::time::Duration::from_millis(
            u64::try_from(now.saturating_sub(active)).unwrap_or(0),
        ))
    }

    /// Whether this source kept the pre-qualification span-id namespace.
    pub async fn legacy_namespace(&self) -> anyhow::Result<bool> {
        self.store
            .run(|db| {
                Ok(db.query_row(
                    "SELECT legacy_namespace FROM event_state WHERE id=1",
                    [],
                    |r| r.get(0),
                )?)
            })
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

    /// Import a legacy record at its original byte boundary. Retrying is idempotent.
    pub(super) async fn import(
        &self,
        records: Vec<(u64, Vec<u8>)>,
        identity: Option<RedactedEnvelope>,
        namespace: Option<bool>,
    ) -> anyhow::Result<()> {
        let identity = identity.map(|v| serde_json::to_vec(&v)).transpose()?;
        self.store
            .write(move |tx| {
                for (id, value) in records {
                    anyhow::ensure!(
                        id <= i64::MAX as u64,
                        "legacy position exceeds storage range"
                    );
                    let prior: Option<Vec<u8>> = tx
                        .query_row("SELECT value FROM events WHERE id=?", [id], |r| r.get(0))
                        .optional()?;
                    match prior {
                        Some(prior) => anyhow::ensure!(
                            prior == value,
                            "legacy record changed during migration"
                        ),
                        None => {
                            tx.execute(
                                "INSERT INTO events(id,value) VALUES(?,?)",
                                params![id, value],
                            )?;
                        }
                    }
                    tx.execute("UPDATE event_state SET tail=max(tail,?) WHERE id=1", [id])?;
                }
                if let Some(identity) = identity {
                    tx.execute("UPDATE event_state SET identity=? WHERE id=1", [identity])?;
                }
                if let Some(namespace) = namespace {
                    tx.execute(
                        "UPDATE event_state SET legacy_namespace=? WHERE id=1",
                        [namespace],
                    )?;
                }
                Ok(())
            })
            .await
    }

    /// Preserve the legacy file's length as the boundary for new records.
    pub(super) async fn reserve(&self, through: u64) -> anyhow::Result<()> {
        self.store
            .write(move |tx| {
                tx.execute(
                    "UPDATE event_state SET tail=max(tail,?) WHERE id=1",
                    [through],
                )?;
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

fn insert(tx: &rusqlite::Transaction<'_>, value: Vec<u8>) -> anyhow::Result<Appended> {
    let before: u64 = tx.query_row("SELECT tail FROM event_state WHERE id=1", [], |r| r.get(0))?;
    let through = before
        .checked_add(value.len() as u64 + 1)
        .filter(|id| *id <= i64::MAX as u64)
        .ok_or_else(|| anyhow::anyhow!("event ID overflow"))?;
    tx.execute(
        "INSERT INTO events(id,value) VALUES(?,?)",
        params![through, value],
    )?;
    tx.execute("UPDATE event_state SET tail=? WHERE id=1", [through])?;
    super::store::touch(tx)?;
    Ok(Appended { before, through })
}

/// Ordered, batched reader over a range of event records.
pub(crate) struct EventReader {
    events: EventCollection,
    position: u64,
    through: u64,
    buffer: VecDeque<(u64, Vec<u8>)>,
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
                        "SELECT id,value FROM events WHERE id>? AND id<=? ORDER BY id LIMIT ?",
                    )?;
                    let mut rows = query.query(params![after, through, READ_RECORDS])?;
                    let mut records = VecDeque::new();
                    let mut bytes = 0;
                    while let Some(row) = rows.next()? {
                        let value: Vec<u8> = row.get(1)?;
                        if !records.is_empty() && bytes + value.len() > READ_BYTES {
                            break;
                        }
                        bytes += value.len();
                        records.push_back((row.get(0)?, value));
                    }
                    Ok(records)
                })
                .await?;
        }
        let Some((id, bytes)) = self.buffer.pop_front() else {
            return Ok(None);
        };
        let record = match tokio::task::spawn_blocking(move || decode(&bytes)).await? {
            Ok(record) => record,
            Err(error) => {
                self.buffer.clear();
                return Err(error.context(format!("event record {id}")));
            }
        };
        self.position = id;
        Ok(Some(EventEntry { id, record }))
    }

    /// The next native event, skipping controls and legacy checkpoints.
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
