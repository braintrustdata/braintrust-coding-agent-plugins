//! Append deltas between pending events. SQLite owns both the dependency and
//! its lifetime. Once translation consumes a base, its surviving child is
//! materialized in the same transaction so consumed events can be deleted.
use crate::{history, wire::RedactedEnvelope};
use anyhow::{ensure, Context};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{value::RawValue, Value};

// Native hooks already fuse prior responses into the captured history.
// Limit this optimization to the usual history fields and leave other
// payloads as self-contained snapshots.
const HISTORIES: [&str; 4] = ["/messages", "/event/messages", "/history", "/event/history"];

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredDelta<'a> {
    path: &'a str,
    #[serde(borrow)]
    envelope: &'a RawValue,
    #[serde(borrow)]
    history: &'a RawValue,
}

pub(super) fn encode(
    db: &Connection,
    base: u64,
    event: &mut RedactedEnvelope,
    snapshot_bytes: usize,
) -> anyhow::Result<Option<Vec<u8>>> {
    let Some((path, input)) = HISTORIES
        .iter()
        .find_map(|path| Some((*path, event.payload.pointer(path)?.as_array()?)))
    else {
        return Ok(None);
    };
    if input.is_empty() || snapshot_bytes < 1024 {
        return Ok(None);
    }
    let Some(previous) = load(db, base)? else {
        return Ok(None);
    };
    if previous.source != event.source
        || previous.session_id != event.session_id
        || previous.event != event.event
    {
        return Ok(None);
    }
    let Some(prefix) = previous.payload.pointer(path).and_then(Value::as_array) else {
        return Ok(None);
    };
    let Some(current_prefix) = input.get(..prefix.len()) else {
        return Ok(None);
    };
    // Compare serialized values to preserve numeric spelling and object order
    // with the embedding CLI's serde_json features.
    if prefix.is_empty() || serde_json::to_vec(prefix)? != serde_json::to_vec(current_prefix)? {
        return Ok(None);
    }
    #[derive(Serialize)]
    struct Delta<'a> {
        path: &'a str,
        envelope: &'a RedactedEnvelope,
        history: &'a RawValue,
    }
    let input = std::mem::replace(
        event.payload.pointer_mut(path).context("missing history")?,
        Value::Array(Vec::new()),
    );
    let history = history::write(&input, None, Some(prefix.len()))?;
    let bytes = serde_json::to_vec(&Delta {
        path,
        envelope: event,
        history: &history,
    })?;
    Ok((bytes.len() < snapshot_bytes).then_some(bytes))
}

type Row = (bool, Vec<u8>, Option<u64>);

fn row(db: &Connection, id: u64) -> anyhow::Result<Option<Row>> {
    Ok(db
        .query_row(
            "SELECT control,value,base FROM events WHERE id=?",
            [id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?)
}

fn load(db: &Connection, id: u64) -> anyhow::Result<Option<RedactedEnvelope>> {
    match row(db, id)? {
        Some((false, value, Some(base))) => expand(db, id, base, &value).map(Some),
        Some((false, value, None)) => Ok(Some(serde_json::from_slice(&value)?)),
        _ => Ok(None),
    }
}

fn delta(bytes: &[u8]) -> anyhow::Result<(RedactedEnvelope, String, Vec<Value>)> {
    let stored: StoredDelta<'_> = serde_json::from_slice(bytes)?;
    ensure!(
        HISTORIES.contains(&stored.path),
        "invalid event history path"
    );
    let event: RedactedEnvelope = serde_json::from_str(stored.envelope.get())?;
    ensure!(
        event
            .payload
            .pointer(stored.path)
            .and_then(Value::as_array)
            .is_some_and(Vec::is_empty),
        "nonempty or missing event history placeholder"
    );
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Input<'a> {
        #[serde(borrow)]
        input_delta: &'a RawValue,
    }
    let input: Input<'_> = serde_json::from_str(stored.history.get())?;
    Ok((
        event,
        stored.path.to_owned(),
        serde_json::from_str(input.input_delta.get())?,
    ))
}

fn expand(
    db: &Connection,
    mut id: u64,
    mut base: u64,
    bytes: &[u8],
) -> anyhow::Result<RedactedEnvelope> {
    let (mut event, path, suffix) = delta(bytes)?;
    let mut parts = vec![suffix];
    let mut messages = loop {
        ensure!(base < id, "invalid event history dependency");
        let (control, value, parent) = row(db, base)?.context("missing event history base")?;
        ensure!(!control, "event history references a control");
        let mut previous = if let Some(parent) = parent {
            let (previous, previous_path, suffix) = delta(&value)?;
            ensure!(previous_path == path, "event history path changed");
            parts.push(suffix);
            id = base;
            base = parent;
            previous
        } else {
            serde_json::from_slice::<RedactedEnvelope>(&value)?
        };
        ensure!(
            event.source == previous.source
                && event.session_id == previous.session_id
                && event.event == previous.event,
            "event history crossed streams"
        );
        if parent.is_none() {
            break std::mem::take(
                previous
                    .payload
                    .pointer_mut(&path)
                    .and_then(Value::as_array_mut)
                    .context("missing base history")?,
            );
        }
    };
    messages.extend(parts.into_iter().rev().flatten());
    *event
        .payload
        .pointer_mut(&path)
        .and_then(Value::as_array_mut)
        .context("missing history array")? = messages;
    Ok(event)
}

pub(super) fn decode(
    db: &Connection,
    id: u64,
    base: u64,
    bytes: &[u8],
) -> anyhow::Result<(RedactedEnvelope, usize)> {
    let event = expand(db, id, base, bytes)?;
    // IDs grow by the original serialized size, even for delta rows.
    let size = usize::try_from(id - base - 1)?;
    Ok((event, size))
}

pub(super) fn remove_through(db: &Connection, through: u64) -> anyhow::Result<()> {
    // Appends only reference the immediately preceding row. At most one
    // dependency crosses this cut, even when there are legacy snapshots.
    let survivor: Option<u64> = db
        .query_row(
            "SELECT id FROM events WHERE base<=? AND id>? LIMIT 1",
            params![through, through],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(id) = survivor {
        let event = load(db, id)?.context("missing surviving history")?;
        db.execute(
            "UPDATE events SET value=?,base=NULL WHERE id=?",
            params![serde_json::to_vec(&event)?, id],
        )?;
    }
    db.execute("DELETE FROM events WHERE id<=?", [through])?;
    Ok(())
}
