//! Read-only migration of NDJSON event journals written before collections.
//!
//! Records are imported at their original byte boundaries, so translation
//! positions recorded by an older daemon remain valid. Delivery checkpoints are
//! not imported: input every route had delivered is instead marked translated,
//! so the translator rebuilds its state from it without publishing it again. A
//! journal file is deleted only after every record it contributes has been
//! committed.
use super::events::{encode, EventCollection, EventRecord};
use crate::journal::{journal_dir, journal_path, source_journal_path};
use crate::wire::{RedactedEnvelope, SessionRoute};
use anyhow::Context;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::path::Path;
use tokio::io::AsyncBufReadExt;

const IMPORT_RECORDS: usize = 1024;
const IMPORT_BYTES: usize = 8 * 1024 * 1024;
const PI_MESSAGES_DELTA: &str = "_bt_messages_delta";

/// Import any legacy journals that hold records for this source session.
pub(super) async fn import_session(
    data_dir: &Path,
    source: &str,
    session_id: &str,
    events: &EventCollection,
) -> anyhow::Result<()> {
    let unqualified = journal_path(data_dir, session_id);
    if unqualified
        .file_name()
        .is_some_and(|name| name.len() <= crate::journal::MAX_FILE_NAME_BYTES)
        && tokio::fs::try_exists(&unqualified).await?
    {
        import_unqualified(data_dir, &unqualified).await?;
    }
    let qualified = source_journal_path(data_dir, source, session_id);
    if tokio::fs::try_exists(&qualified).await? {
        import_file(&qualified, source, events, None).await?;
        tokio::fs::remove_file(&qualified).await?;
    }
    Ok(())
}

/// Import every legacy journal in the data directory. Failures leave that
/// file in place for a later attempt without blocking other sessions.
pub(crate) async fn import_all(data_dir: &Path) {
    let Ok(mut entries) = tokio::fs::read_dir(journal_dir(data_dir)).await else {
        return;
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if path.extension().is_none_or(|ext| ext != "ndjson") {
            continue;
        }
        let result = async {
            let owners = sessions(&path).await?;
            if owners.is_empty() {
                // Nothing was ever acknowledged from a file without events.
                tokio::fs::remove_file(&path).await?;
            }
            for (source, session_id) in owners {
                EventCollection::open(data_dir, &source, &session_id).await?;
            }
            anyhow::Ok(())
        }
        .await;
        if let Err(error) = result {
            tracing::warn!(%error, path = %path.display(), "legacy journal migration is held");
        }
    }
    // Leave no empty legacy directory behind.
    let _ = tokio::fs::remove_dir(journal_dir(data_dir)).await;
}

/// A pre-qualification journal holds every source observed for one native
/// session id. Split it into each source's collection, then remove it.
async fn import_unqualified(data_dir: &Path, path: &Path) -> anyhow::Result<()> {
    // Only the first source recorded for a native session keeps the old
    // span-id namespace: that daemon had one actor per native session.
    let mut owner = None;
    let mut reader = Reader::open(path).await?;
    while let Some((_, line)) = reader.next().await? {
        if let Line::Record(EventRecord::Event(event)) = line {
            owner.get_or_insert_with(|| {
                crate::translate::canonical_source_name(&event.source).to_owned()
            });
        }
    }
    for (source, session_id) in sessions(path).await? {
        // An older daemon copied this file into a qualified journal on first
        // use and appended there. That copy is the authoritative superset.
        if tokio::fs::try_exists(source_journal_path(data_dir, &source, &session_id)).await? {
            let events = EventCollection::claim(data_dir, &source, &session_id).await?;
            events
                .import(
                    Vec::new(),
                    None,
                    Some(owner.as_deref() == Some(source.as_str())),
                )
                .await?;
            continue;
        }
        let events = EventCollection::claim(data_dir, &source, &session_id).await?;
        import_file(
            path,
            &source,
            &events,
            Some(owner.as_deref() == Some(source.as_str())),
        )
        .await?;
    }
    tokio::fs::remove_file(path).await?;
    Ok(())
}

async fn sessions(path: &Path) -> anyhow::Result<BTreeSet<(String, String)>> {
    let mut sessions = BTreeSet::new();
    let mut reader = Reader::open(path).await?;
    while let Some((_, line)) = reader.next().await? {
        if let Line::Record(EventRecord::Event(event)) = line {
            sessions.insert((
                crate::translate::canonical_source_name(&event.source).to_owned(),
                event.session_id,
            ));
        }
    }
    Ok(sessions)
}

async fn import_file(
    path: &Path,
    source: &str,
    events: &EventCollection,
    namespace: Option<bool>,
) -> anyhow::Result<()> {
    let mut reader = Reader::open(path).await?;
    // Each route's latest event, and the latest checkpoint it delivered.
    let mut routes: Vec<(SessionRoute, u64)> = Vec::new();
    let mut delivered: HashMap<String, u64> = HashMap::new();
    let mut batch = Vec::new();
    let mut bytes = 0;
    let mut identity: Option<RedactedEnvelope> = None;
    while let Some((through, line)) = reader.next().await? {
        let record = match line {
            Line::Record(record) => record,
            Line::Checkpoint(checkpoint) => {
                let key = serde_json::to_string(&checkpoint.route)?;
                let entry = delivered.entry(key).or_default();
                *entry = (*entry).max(checkpoint.through);
                continue;
            }
        };
        match &record {
            EventRecord::Event(event) => {
                if crate::translate::canonical_source_name(&event.source) != source {
                    continue;
                }
                if let Some(route) = &event.route {
                    match routes.iter_mut().find(|(known, _)| known.same_route(route)) {
                        Some((_, latest)) => *latest = through,
                        None => routes.push((route.clone(), through)),
                    }
                }
                let mut latest = event.clone();
                let mut payload = serde_json::Map::new();
                for key in [
                    "transcript_path",
                    "agent_transcript_path",
                    "_bt_transcript_mirror",
                ] {
                    let value =
                        event.payload.get(key).cloned().or_else(|| {
                            identity.as_ref().and_then(|v| v.payload.get(key)).cloned()
                        });
                    if let Some(value) = value {
                        payload.insert(key.into(), value);
                    }
                }
                latest.payload = payload.into();
                identity = Some(latest);
            }
            EventRecord::Control(_) => {}
        }
        let value = encode(&record)?;
        bytes += value.len();
        batch.push((through, value));
        if batch.len() >= IMPORT_RECORDS || bytes >= IMPORT_BYTES {
            events
                .import(std::mem::take(&mut batch), None, None)
                .await?;
            bytes = 0;
        }
    }
    events.import(batch, identity, namespace).await?;
    // Input is delivered only through the point every route had delivered.
    let delivered = routes
        .iter()
        .map(|(route, _)| {
            let key = serde_json::to_string(route)?;
            anyhow::Ok(delivered.get(&key).copied().unwrap_or(0))
        })
        .try_fold(None, |least: Option<u64>, through| {
            let through = through?;
            anyhow::Ok(Some(least.map_or(through, |least| least.min(through))))
        })?
        .unwrap_or(0);
    events.reserve(reader.position, delivered).await
}

/// A delivery acknowledgement written by a daemon before SpanOp storage.
#[derive(serde::Deserialize)]
struct Checkpoint {
    route: SessionRoute,
    through: u64,
}

#[allow(clippy::large_enum_variant, reason = "One line is read at a time.")]
enum Line {
    Record(EventRecord),
    Checkpoint(Checkpoint),
}

/// Streaming NDJSON reader. An incomplete final line was never acknowledged
/// to capture and is not imported.
struct Reader {
    lines: tokio::io::BufReader<tokio::fs::File>,
    position: u64,
    pi_context: Vec<serde_json::Value>,
}

impl Reader {
    async fn open(path: &Path) -> anyhow::Result<Self> {
        Ok(Self {
            lines: tokio::io::BufReader::new(tokio::fs::File::open(path).await?),
            position: 0,
            pi_context: Vec::new(),
        })
    }

    async fn next(&mut self) -> anyhow::Result<Option<(u64, Line)>> {
        loop {
            let mut line = Vec::new();
            let read = self.lines.read_until(b'\n', &mut line).await?;
            if read == 0 || line.last() != Some(&b'\n') {
                return Ok(None);
            }
            self.position += read as u64;
            let text = std::str::from_utf8(&line)?.trim();
            if text.is_empty() {
                continue;
            }
            let context = || format!("legacy journal offset {}", self.position);
            let value: serde_json::Value = serde_json::from_str(text)?;
            if value
                .get("_bt_record_type")
                .and_then(serde_json::Value::as_str)
                == Some("delivery_checkpoint")
            {
                let checkpoint = serde_json::from_value(value).map_err(anyhow::Error::from);
                return Ok(Some((
                    self.position,
                    Line::Checkpoint(checkpoint.with_context(context)?),
                )));
            }
            let mut record = super::events::decode_value(value).with_context(context)?;
            if let EventRecord::Event(event) = &mut record {
                expand_pi_payload(event, &mut self.pi_context)?;
            }
            return Ok(Some((self.position, Line::Record(record))));
        }
    }
}

/// Older journals stored Pi context as a delta against the previous context.
fn expand_pi_payload(
    event: &mut RedactedEnvelope,
    previous: &mut Vec<serde_json::Value>,
) -> anyhow::Result<()> {
    if event.source != "pi" || event.event != "context" {
        return Ok(());
    }
    let Some(native) = event
        .payload
        .get_mut("event")
        .and_then(serde_json::Value::as_object_mut)
    else {
        return Ok(());
    };
    let Some(delta) = native.remove(PI_MESSAGES_DELTA) else {
        if let Some(messages) = native.get("messages").and_then(serde_json::Value::as_array) {
            *previous = messages.clone();
        }
        return Ok(());
    };
    let common_prefix = delta
        .get("common_prefix")
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| anyhow::anyhow!("invalid Pi context common prefix"))?;
    anyhow::ensure!(
        common_prefix <= previous.len(),
        "Pi context common prefix exceeds prior context"
    );
    let suffix = delta
        .get("suffix")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("invalid Pi context suffix"))?;
    previous.truncate(common_prefix);
    previous.extend(suffix.iter().cloned());
    native.insert(
        "messages".into(),
        serde_json::Value::Array(previous.clone()),
    );
    Ok(())
}
