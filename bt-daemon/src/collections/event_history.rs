//! Append deltas between pending events. SQLite owns both the dependency and
//! its lifetime. Once translation consumes a base, its surviving child is
//! materialized in the same transaction so consumed events can be deleted.
use crate::{history, wire::RedactedEnvelope};
use anyhow::{ensure, Context};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{value::RawValue, Value};

const MAX_DELTAS: usize = 8;

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
    let Some((previous, depth)) = load(db, base, MAX_DELTAS)? else {
        return Ok(None);
    };
    if depth == MAX_DELTAS
        || previous.source != event.source
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

fn load(
    db: &Connection,
    id: u64,
    remaining: usize,
) -> anyhow::Result<Option<(RedactedEnvelope, usize)>> {
    let row: Option<(bool, Vec<u8>, Option<u64>)> = db
        .query_row(
            "SELECT control,value,base FROM events WHERE id=?",
            [id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    match row {
        Some((false, value, Some(base))) => expand(db, id, base, &value, remaining).map(Some),
        Some((false, value, None)) => Ok(Some((serde_json::from_slice(&value)?, 0))),
        _ => Ok(None),
    }
}

fn expand(
    db: &Connection,
    id: u64,
    base: u64,
    bytes: &[u8],
    remaining: usize,
) -> anyhow::Result<(RedactedEnvelope, usize)> {
    ensure!(
        base < id && remaining > 0,
        "invalid event history dependency"
    );
    let delta: StoredDelta<'_> = serde_json::from_slice(bytes)?;
    ensure!(
        HISTORIES.contains(&delta.path),
        "invalid event history path"
    );
    let (mut previous, depth) =
        load(db, base, remaining - 1)?.context("missing event history base")?;
    let mut event: RedactedEnvelope = serde_json::from_str(delta.envelope.get())?;
    ensure!(
        event.source == previous.source
            && event.session_id == previous.session_id
            && event.event == previous.event,
        "event history crossed streams"
    );
    let target = event
        .payload
        .pointer_mut(delta.path)
        .and_then(Value::as_array_mut)
        .context("missing history array")?;
    ensure!(target.is_empty(), "nonempty event history placeholder");
    let prefix = previous
        .payload
        .pointer_mut(delta.path)
        .and_then(Value::as_array_mut)
        .context("missing base history")?;
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Delta<'a> {
        #[serde(borrow)]
        input_delta: &'a RawValue,
    }
    let delta: Delta<'_> = serde_json::from_str(delta.history.get())?;
    *target = std::mem::take(prefix);
    target.extend(serde_json::from_str::<Vec<Value>>(delta.input_delta.get())?);
    Ok((event, depth + 1))
}

pub(super) fn decode(
    db: &Connection,
    id: u64,
    base: u64,
    bytes: &[u8],
) -> anyhow::Result<(RedactedEnvelope, usize)> {
    let (event, _) = expand(db, id, base, bytes, MAX_DELTAS)?;
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
        let (event, _) = load(db, id, MAX_DELTAS)?.context("missing surviving history")?;
        db.execute(
            "UPDATE events SET value=?,base=NULL WHERE id=?",
            params![serde_json::to_vec(&event)?, id],
        )?;
    }
    db.execute("DELETE FROM events WHERE id<=?", [through])?;
    Ok(())
}
