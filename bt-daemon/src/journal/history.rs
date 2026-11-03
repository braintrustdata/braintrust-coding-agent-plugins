//! Lossless history prefixes. References point only into the same append-only
//! journal; caches affect efficiency, never the data available for recovery.
use crate::wire::RedactedEnvelope;
use anyhow::{ensure, Context};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, VecDeque},
    io::{BufRead, Seek},
    path::Path,
    sync::Arc,
};

pub(super) const FORMAT: &str = "event_history_v1";
const STREAMS: usize = 16;
const PREFIXES: usize = 128;
const CACHE_BYTES: usize = 8 * 1024 * 1024;
const CACHE_RECORDS: usize = 64;

pub(super) fn parse(bytes: &[u8]) -> anyhow::Result<Value> {
    match serde_json::from_slice(bytes) {
        Ok(value) => Ok(value),
        Err(original) => {
            // The storage wrapper must not reduce the native event's JSON
            // depth allowance. Parse its raw envelope independently on this
            // uncommon path, keeping the normal parser's recursion protection.
            #[derive(Deserialize)]
            struct Raw<'a> {
                _bt_record_type: String,
                #[serde(borrow)]
                envelope: &'a serde_json::value::RawValue,
                #[serde(default)]
                _bt_history_refs: Vec<Reference>,
            }
            let raw: Raw<'_> = serde_json::from_slice(bytes).map_err(|_| original)?;
            ensure!(
                raw._bt_record_type == FORMAT,
                "unsupported journal record format"
            );
            let envelope: Value = serde_json::from_str(raw.envelope.get())?;
            Ok(
                serde_json::json!({"_bt_record_type":FORMAT,"envelope":envelope,
                "_bt_history_refs":raw._bt_history_refs}),
            )
        }
    }
}

#[derive(Clone)]
struct Prefix {
    hash: [u8; 32],
    wire: [u8; 32],
    count: usize,
    bytes: usize,
}
#[derive(Clone)]
struct History {
    key: [u8; 32],
    offset: u64,
    anchor: u64,
    delta_bytes: usize,
    prefixes: VecDeque<Prefix>,
}
#[derive(Clone, Default)]
pub(super) struct Encoder(VecDeque<History>);
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Reference {
    path: String,
    base: u64,
    anchor: u64,
    keep: usize,
    hash: String,
}

// No native keys are reserved: reference metadata lives outside payload.
fn paths(value: &Value, path: &str, found: &mut Vec<String>) {
    if found.len() == STREAMS {
        return;
    }
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                let next = format!("{path}/{}", key.replace('~', "~0").replace('/', "~1"));
                if matches!(key.as_str(), "messages" | "history") && value.is_array() {
                    found.push(next);
                } else {
                    paths(value, &next, found);
                }
                if found.len() == STREAMS {
                    break;
                }
            }
        }
        Value::Array(array) => {
            for (i, value) in array.iter().enumerate() {
                paths(value, &format!("{path}/{i}"), found);
                if found.len() == STREAMS {
                    break;
                }
            }
        }
        _ => {}
    }
}
struct Counter(usize);
impl std::io::Write for Counter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 += bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
// Stable type/length framing and sorted object keys make persisted checksums
// independent of serde_json's preserve_order feature. The separate, memory-only
// wire hash preserves object order and number spellings a translator may stringify.
// Normalize equivalent decimal spellings without rounding through f64. Very
// large exponents fall back to their exact spelling; no precision is discarded.
fn hash_number(text: &str, hash: &mut Sha256) {
    let (mantissa, exponent) = text.split_once(['e', 'E']).unwrap_or((text, "0"));
    let digits = mantissa.trim_start_matches('-').replace('.', "");
    let digits = digits.trim_start_matches('0');
    let coefficient = digits.trim_end_matches('0');
    let fraction = mantissa
        .split_once('.')
        .map_or(0, |(_, fraction)| fraction.len());
    let exponent = exponent
        .parse::<i64>()
        .ok()
        .and_then(|e| e.checked_sub(fraction as i64))
        .and_then(|e| e.checked_add((digits.len() - coefficient.len()) as i64));
    if coefficient.is_empty() {
        hash.update(b"0");
    } else if let Some(exponent) = exponent {
        hash.update(if text.starts_with('-') { b"-" } else { b"+" });
        hash.update((coefficient.len() as u64).to_be_bytes());
        hash.update(coefficient.as_bytes());
        hash.update(exponent.to_be_bytes());
    } else {
        hash.update(b"x");
        hash.update((text.len() as u64).to_be_bytes());
        hash.update(text.as_bytes());
    }
}
fn hash_value(value: &Value, hash: &mut Sha256, wire: &mut Sha256) {
    let sized = |hash: &mut Sha256, bytes: &[u8]| {
        hash.update((bytes.len() as u64).to_be_bytes());
        hash.update(bytes);
    };
    match value {
        Value::Null => hash.update(b"n"),
        Value::Bool(value) => hash.update(if *value { b"t" } else { b"f" }),
        Value::Number(number) => {
            let text = number.to_string();
            hash.update(b"#");
            hash_number(&text, hash);
            wire.update(b"#");
            sized(wire, text.as_bytes());
        }
        Value::String(text) => {
            hash.update(b"s");
            sized(hash, text.as_bytes());
        }
        Value::Array(array) => {
            hash.update(b"[");
            hash.update((array.len() as u64).to_be_bytes());
            for value in array {
                hash_value(value, hash, wire);
            }
        }
        Value::Object(object) => {
            hash.update(b"{");
            hash.update((object.len() as u64).to_be_bytes());
            for key in object.keys() {
                sized(wire, key.as_bytes());
            }
            let mut entries: Vec<_> = object.iter().collect();
            entries.sort_unstable_by(|a, b| a.0.cmp(b.0));
            for (key, value) in entries {
                sized(hash, key.as_bytes());
                hash_value(value, hash, wire);
            }
        }
    }
}
fn hex(hash: [u8; 32]) -> String {
    hash.iter().map(|byte| format!("{byte:02x}")).collect()
}
fn prefixes(messages: &[Value]) -> anyhow::Result<VecDeque<Prefix>> {
    extend_prefixes(VecDeque::new(), messages)
}
fn extend_prefixes(
    mut result: VecDeque<Prefix>,
    messages: &[Value],
) -> anyhow::Result<VecDeque<Prefix>> {
    let mut hash = result.back().map_or([0; 32], |p| p.hash);
    let mut wire = result.back().map_or([0; 32], |p| p.wire);
    let mut bytes = result.back().map_or(2, |p| p.bytes);
    let start = result.back().map_or(0, |p| p.count);
    for (index, message) in messages.iter().enumerate() {
        let count = start + index + 1;
        let mut digest = Sha256::new();
        digest.update(hash);
        let mut wire_digest = Sha256::new();
        wire_digest.update(wire);
        hash_value(message, &mut digest, &mut wire_digest);
        let mut counter = Counter(0);
        serde_json::to_writer(&mut counter, message)?;
        bytes += counter.0 + usize::from(count > 1);
        hash = digest.finalize().into();
        wire = wire_digest.finalize().into();
        if result.len() == PREFIXES {
            result.pop_front();
        }
        result.push_back(Prefix {
            hash,
            wire,
            count,
            bytes,
        });
    }
    Ok(result)
}

impl Encoder {
    pub(super) fn encode(
        &mut self,
        mut value: RedactedEnvelope,
        offset: u64,
    ) -> anyhow::Result<Vec<u8>> {
        let source = &value.source;
        let session = &value.session_id;
        let mut candidates = Vec::new();
        paths(&value.payload, "", &mut candidates);
        if candidates.is_empty() && !(source == "pi" && value.event == "context") {
            return Ok(serde_json::to_vec(&value)?);
        }
        let mut references = Vec::new();
        for pointer in candidates {
            let path = format!("/payload{pointer}");
            let mut stream = Sha256::new();
            for part in [source.as_str(), session.as_str(), path.as_str()] {
                stream.update((part.len() as u64).to_be_bytes());
                stream.update(part.as_bytes());
            }
            let key: [u8; 32] = stream.finalize().into();
            let messages = value
                .payload
                .pointer(&pointer)
                .and_then(Value::as_array)
                .context("missing history")?;
            let current = prefixes(messages)?;
            // Keep a few older heads too: a truncated/branched context can
            // return to an earlier prefix without writing its messages again.
            let candidate = current.iter().rev().find_map(|prefix| {
                self.0
                    .iter()
                    .enumerate()
                    .filter(|(_, h)| {
                        h.key == key
                            && h.prefixes.iter().any(|old| {
                                old.count == prefix.count
                                    && old.hash == prefix.hash
                                    && old.wire == prefix.wire
                            })
                    })
                    .max_by_key(|(index, h)| {
                        (
                            h.prefixes.back().map(|p| p.count) == Some(prefix.count),
                            *index,
                        )
                    })
                    .map(|(index, _)| (index, prefix.clone()))
            });
            let previous = candidate
                .as_ref()
                .and_then(|(index, _)| self.0.remove(*index));
            let full_bytes = current.back().map_or(2, |p| p.bytes);
            let mut anchor = offset;
            let mut delta_bytes = 0;
            let mut unchanged = false;
            if let Some(previous) = &previous {
                if let Some((_, common)) = &candidate {
                    let reference = Reference {
                        path: path.clone(),
                        base: previous.offset,
                        anchor: previous.anchor,
                        keep: common.count,
                        hash: hex(common.hash),
                    };
                    let cost = serde_json::to_vec(&reference)?.len() + 32;
                    let accumulated = previous
                        .delta_bytes
                        .saturating_add(full_bytes.saturating_sub(common.bytes))
                        .saturating_add(cost);
                    // Each stream pays for its own checkpoints. Charging global
                    // journal bytes would amplify checkpoints for multiple histories.
                    if common.bytes > cost && accumulated < full_bytes {
                        value
                            .payload
                            .pointer_mut(&pointer)
                            .and_then(Value::as_array_mut)
                            .context("missing history")?
                            .drain(..common.count);
                        references.push(reference);
                        anchor = previous.anchor;
                        delta_bytes = accumulated;
                        unchanged = previous.prefixes.back().map(|p| (p.count, p.hash, p.wire))
                            == current.back().map(|p| (p.count, p.hash, p.wire));
                    }
                }
            }
            let mut entry = if unchanged {
                previous.context("missing prefix base")?
            } else {
                if let Some(previous) = previous {
                    self.0.push_back(previous);
                }
                History {
                    key,
                    offset,
                    anchor,
                    delta_bytes,
                    prefixes: current,
                }
            };
            entry.delta_bytes = delta_bytes;
            self.0.push_back(entry);
            while self.0.len() > STREAMS {
                self.0.pop_front();
            }
        }
        // A wrapper deliberately cannot deserialize as an older plain envelope:
        // rollback must fail closed, rather than translate an unexpanded suffix.
        #[derive(Serialize)]
        struct Stored<'a> {
            _bt_record_type: &'static str,
            envelope: &'a RedactedEnvelope,
            #[serde(skip_serializing_if = "Vec::is_empty")]
            _bt_history_refs: Vec<Reference>,
        }
        Ok(serde_json::to_vec(&Stored {
            _bt_record_type: FORMAT,
            envelope: &value,
            _bt_history_refs: references,
        })?)
    }
}

struct Cached {
    value: Arc<Value>,
    bytes: usize,
    anchors: BTreeMap<String, u64>,
    prefixes: BTreeMap<String, VecDeque<Prefix>>,
}
#[derive(Default)]
pub(super) struct Decoder {
    records: BTreeMap<u64, Cached>,
    order: VecDeque<u64>,
    bytes: usize,
}
impl Decoder {
    fn remember(
        &mut self,
        offset: u64,
        value: &Value,
        bytes: usize,
        anchors: BTreeMap<String, u64>,
        prefixes: BTreeMap<String, VecDeque<Prefix>>,
    ) {
        if self.records.contains_key(&offset) {
            return;
        }
        // One oversized event travels alone, as on the live SpanOp feed.
        while !self.records.is_empty()
            && (self.bytes.saturating_add(bytes) > CACHE_BYTES
                || self.records.len() >= CACHE_RECORDS)
        {
            if let Some(old) = self
                .order
                .pop_front()
                .and_then(|key| self.records.remove(&key))
            {
                self.bytes -= old.bytes;
            } else {
                break;
            }
        }
        self.records.insert(
            offset,
            Cached {
                value: Arc::new(value.clone()),
                bytes,
                anchors,
                prefixes,
            },
        );
        self.order.push_back(offset);
        self.bytes += bytes;
    }
    fn read(file: &mut std::fs::File, offset: u64, limit: u64) -> anyhow::Result<Value> {
        ensure!(offset < limit, "history reference is not backward");
        if offset > 0 {
            file.seek(std::io::SeekFrom::Start(offset - 1))?;
            let mut byte = [0];
            std::io::Read::read_exact(file, &mut byte)?;
            ensure!(
                byte[0] == b'\n',
                "history reference is not at a record boundary"
            );
        }
        file.seek(std::io::SeekFrom::Start(offset))?;
        let mut line = Vec::new();
        std::io::BufReader::new(std::io::Read::take(&mut *file, limit - offset))
            .read_until(b'\n', &mut line)?;
        ensure!(line.last() == Some(&b'\n'), "incomplete history reference");
        parse(&line)
    }
    pub(super) fn expand(
        &mut self,
        mut value: Value,
        file_path: &Path,
        offset: u64,
        bytes: usize,
    ) -> anyhow::Result<Value> {
        let references: Vec<Reference> = value
            .get("_bt_history_refs")
            .cloned()
            .map(serde_json::from_value)
            .transpose()?
            .unwrap_or_default();
        let mut stored_paths = Vec::new();
        paths(&value["envelope"]["payload"], "/payload", &mut stored_paths);
        // Empty suffixes cannot become new writer bases. Keep their actual
        // referenced heads hot instead of caching duplicate complete histories.
        let cacheable = stored_paths.iter().any(|path| {
            value["envelope"]
                .pointer(path)
                .and_then(Value::as_array)
                .is_some_and(|a| !a.is_empty())
        });
        let mut expanded_bytes = bytes;
        let mut anchors = BTreeMap::new();
        let mut validated = BTreeMap::new();
        let mut value = value
            .as_object_mut()
            .and_then(|object| object.remove("envelope"))
            .context("missing history envelope")?;
        let mut file = None;
        let mut seen = Vec::<String>::new();
        for reference in references {
            ensure!(
                reference.path.starts_with("/payload/"),
                "history reference outside payload"
            );
            ensure!(
                !seen.iter().any(|p| p == &reference.path
                    || p.starts_with(&format!("{}/", reference.path))
                    || reference.path.starts_with(&format!("{p}/"))),
                "overlapping history references"
            );
            seen.push(reference.path.clone());
            ensure!(
                reference.anchor <= reference.base && reference.base < offset && reference.keep > 0,
                "invalid history reference range"
            );
            let mut base = reference.base;
            let mut limit = offset;
            let mut count = reference.keep;
            let mut parts = Vec::new();
            let mut cached_prefix = None;
            loop {
                if let Some(record) = self.records.get_mut(&base) {
                    ensure!(
                        record.anchors.get(&reference.path).copied().unwrap_or(base)
                            == reference.anchor
                            && record.value["source"] == value["source"]
                            && record.value["session_id"] == value["session_id"],
                        "history reference crossed its cached stream"
                    );
                    let messages = record
                        .value
                        .pointer(&reference.path)
                        .and_then(Value::as_array)
                        .context("cached history is not an array")?;
                    ensure!(count <= messages.len(), "history prefix exceeds cache");
                    if !record.prefixes.contains_key(&reference.path) {
                        record
                            .prefixes
                            .insert(reference.path.clone(), prefixes(messages)?);
                    }
                    let points = &record.prefixes[&reference.path];
                    let points = if points.iter().any(|p| p.count == count) {
                        points
                            .iter()
                            .filter(|p| p.count <= count)
                            .cloned()
                            .collect()
                    } else {
                        prefixes(&messages[..count])?
                    };
                    cached_prefix = Some(points);
                    parts.push(messages[..count].to_vec());
                    self.order.retain(|key| *key != base);
                    self.order.push_back(base);
                    break;
                }
                let file = match &mut file {
                    Some(file) => file,
                    slot @ None => slot.insert(std::fs::File::open(file_path)?),
                };
                let record = Self::read(file, base, limit)?;
                ensure!(
                    record["_bt_record_type"] == FORMAT
                        && record["envelope"]["source"] == value["source"]
                        && record["envelope"]["session_id"] == value["session_id"],
                    "history reference crossed its stream"
                );
                let messages = record["envelope"]
                    .pointer(&reference.path)
                    .and_then(Value::as_array)
                    .context("history reference target is not an array")?;
                let refs: Vec<Reference> = record
                    .get("_bt_history_refs")
                    .cloned()
                    .map(serde_json::from_value)
                    .transpose()?
                    .unwrap_or_default();
                if let Some(parent) = refs.iter().find(|r| r.path == reference.path) {
                    ensure!(
                        parent.anchor == reference.anchor
                            && parent.anchor <= parent.base
                            && parent.base < base
                            && parent.keep > 0,
                        "invalid history dependency"
                    );
                    ensure!(
                        count <= parent.keep.saturating_add(messages.len()),
                        "history prefix exceeds target"
                    );
                    parts.push(messages[..count.saturating_sub(parent.keep)].to_vec());
                    count = count.min(parent.keep);
                    limit = base;
                    base = parent.base;
                } else {
                    ensure!(
                        base == reference.anchor && count <= messages.len(),
                        "invalid history anchor"
                    );
                    parts.push(messages[..count].to_vec());
                    break;
                }
            }
            let points = if let Some(mut points) = cached_prefix {
                // The final part came from a validated cached prefix. Only
                // uncached suffixes need hashing when unwinding dependencies.
                for suffix in parts.iter().rev().skip(1) {
                    points = extend_prefixes(points, suffix)?;
                }
                points
            } else {
                let mut points = VecDeque::new();
                for part in parts.iter().rev() {
                    points = extend_prefixes(points, part)?;
                }
                points
            };
            let prefix = points.back().context("empty referenced prefix")?;
            ensure!(
                hex(prefix.hash) == reference.hash,
                "history prefix checksum mismatch"
            );
            expanded_bytes = expanded_bytes.saturating_add(prefix.bytes);
            let suffix = value
                .pointer_mut(&reference.path)
                .and_then(Value::as_array_mut)
                .context("history suffix is not an array")?;
            let points = extend_prefixes(points, suffix)?;
            let mut prefix: Vec<_> = parts.into_iter().rev().flatten().collect();
            prefix.append(suffix);
            *suffix = prefix;
            anchors.insert(reference.path.clone(), reference.anchor);
            validated.insert(reference.path, points);
        }
        if cacheable {
            self.remember(offset, &value, expanded_bytes, anchors, validated);
        }
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        journal::{JournalReader, JournalWriter},
        wire::Envelope,
    };
    use serde_json::json;

    fn event(messages: Vec<Value>) -> Envelope {
        serde_json::from_value(
            json!({"source":"muse","session_id":"test","event":"PreLLMCall","ts_ms":1,
            "payload":{"messages":messages,"native":{"_bt_history_refs":"untouched"}}}),
        )
        .unwrap()
    }
    fn message(index: usize) -> Value {
        json!({"role":"user","content":format!("{index} {}", "unicode 📚\n".repeat(128))})
    }

    #[tokio::test]
    async fn storage_wrapping_preserves_the_native_json_depth_allowance() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("depth.ndjson");
        let mut writer = JournalWriter::open_path(&path).await.unwrap();
        let mut env = event(vec![message(0)]);
        let mut deepest = env.clone();
        loop {
            env.payload["nested"] = json!([env.payload.get("nested").cloned()]);
            if serde_json::from_slice::<Value>(&serde_json::to_vec(&env).unwrap()).is_err() {
                break;
            }
            deepest = env.clone();
        }
        let first = writer.append(&deepest).await.unwrap();
        writer.append(&deepest).await.unwrap();
        let bytes = std::fs::read(&path).unwrap();
        assert!(serde_json::from_slice::<Value>(&bytes[..first as usize]).is_err());
        let mut reader = JournalReader::open(&path, writer.position())
            .await
            .unwrap()
            .unwrap();
        for _ in 0..2 {
            assert_eq!(
                reader.next_entry().await.unwrap().unwrap().payload,
                deepest.payload
            );
        }
        // The backward-seek path must use the same depth allowance too.
        let record = parse(&bytes[first as usize..]).unwrap();
        let recovered = Decoder::default()
            .expand(record, &path, first, bytes.len() - first as usize)
            .unwrap();
        assert_eq!(recovered["payload"], deepest.payload);
    }

    #[test]
    fn fingerprints_are_stable_without_coalescing_wire_order_or_number_types() {
        let left: Value = serde_json::from_str(r#"{"z":1,"a":[true,"é"]}"#).unwrap();
        let right: Value = serde_json::from_str(r#"{"a":[true,"é"],"z":1}"#).unwrap();
        let a = prefixes(std::slice::from_ref(&left))
            .unwrap()
            .pop_back()
            .unwrap();
        let b = prefixes(std::slice::from_ref(&right))
            .unwrap()
            .pop_back()
            .unwrap();
        // This is part of the on-disk v1 format, including cross-build key order.
        assert_eq!(
            hex(a.hash),
            "77274ba03369e2db77acaabb7dc2d895be5b9106f7b9278ec521e259f1781138"
        );
        assert_eq!(a.hash, b.hash);
        if serde_json::to_string(&left).unwrap() != serde_json::to_string(&right).unwrap() {
            assert_ne!(a.wire, b.wire);
        }
        let integer = prefixes(&[json!(1)]).unwrap().pop_back().unwrap();
        let decimal = prefixes(&[json!(1.0)]).unwrap().pop_back().unwrap();
        assert_eq!(integer.hash, decimal.hash);
        assert_ne!(integer.wire, decimal.wire);
        let digest = |number: &str| {
            let mut h = Sha256::new();
            hash_number(number, &mut h);
            hex(h.finalize().into())
        };
        assert_eq!(digest("0.00100"), digest("1e-3"));
        assert_ne!(digest("1.0000000000000000001"), digest("1.0"));
    }

    #[tokio::test]
    async fn changes_branches_eviction_and_cold_reads_are_lossless() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.ndjson");
        let mut writer = JournalWriter::open_path(&path).await.unwrap();
        let mut histories = vec![vec![message(0)], vec![message(0), message(1)]];
        histories.push(histories[1].clone()); // identical
        histories.push(vec![message(0), message(2)]); // edited tail
        histories.push(vec![message(0)]); // truncation
        histories.push(vec![message(1), message(0)]); // reordered / branched
        histories.push(vec![json!({"role":"system","summary":"compacted"})]);
        histories.push(vec![]);
        let numbers: Vec<_> = (0..128u64)
            .filter_map(|i| {
                serde_json::Number::from_f64(f64::from_bits(i.wrapping_mul(6364136223846793005)))
            })
            .map(Value::Number)
            .collect();
        let numeric = vec![json!({"role":"tool","values":numbers,"zero":-0.0,"maximum":f64::MAX})];
        histories.push(numeric.clone());
        histories.push(numeric);
        histories.extend((0..80).map(|i| vec![message(0), message(i)]));
        let mut seed = 17u64;
        for _ in 0..160 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let mut branch = histories[(seed as usize >> 8) % histories.len()].clone();
            match seed % 3 {
                0 if branch.len() < 16 => branch.push(message(seed as usize)),
                1 if !branch.is_empty() => {
                    let index = seed as usize % branch.len();
                    branch[index] = message(seed as usize);
                }
                _ => branch.truncate(branch.len() / 2),
            }
            histories.push(branch);
        }
        let mut expected = Vec::new();
        for (i, messages) in histories.into_iter().enumerate() {
            if i == 5 {
                writer = JournalWriter::open_path(&path).await.unwrap();
            }
            let mut env = event(messages);
            if i % 7 == 0 {
                env.payload["request"] = json!({"history":[message(0)]});
            }
            writer.append(&env).await.unwrap();
            expected.push(env.payload);
        }
        let stored = std::fs::read_to_string(&path).unwrap();
        assert!(stored.contains("_bt_history_refs"));
        let mut offset = 0;
        for (line, expected) in stored.split_inclusive('\n').zip(&expected) {
            // No warmed decoder state: every reference must work from disk.
            let value = Decoder::default()
                .expand(
                    serde_json::from_str(line).unwrap(),
                    &path,
                    offset,
                    line.len(),
                )
                .unwrap();
            assert_eq!(&value["payload"], expected);
            offset += line.len() as u64;
        }
        let mut reader = JournalReader::open(&path, writer.position())
            .await
            .unwrap()
            .unwrap();
        for expected in expected {
            assert_eq!(
                reader.next_entry().await.unwrap().unwrap().payload,
                expected
            );
        }
        assert!(reader.next_entry().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn returning_to_an_older_branch_reuses_its_complete_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.ndjson");
        let mut writer = JournalWriter::open_path(&path).await.unwrap();
        let full = event((0..64).map(message).collect());
        let short = event((0..8).map(message).collect());
        let first = writer.append(&full).await.unwrap();
        for _ in 0..100 {
            for event in [&short, &full] {
                writer.append(event).await.unwrap();
            }
        }
        assert!(
            writer.position() < 3 * first,
            "branch returns must reuse exact cached heads"
        );
        let stored = std::fs::read_to_string(&path).unwrap();
        let record: Value = serde_json::from_str(stored.lines().nth(2).unwrap()).unwrap();
        assert_eq!(record["_bt_history_refs"][0]["keep"], 64);
        assert_eq!(record["envelope"]["payload"]["messages"], json!([]));
        let mut reader = JournalReader::open(&path, writer.position())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            reader.next_entry().await.unwrap().unwrap().payload,
            full.payload
        );
        for _ in 0..100 {
            for expected in [&short, &full] {
                assert_eq!(
                    reader.next_entry().await.unwrap().unwrap().payload,
                    expected.payload
                );
            }
        }
    }

    #[tokio::test]
    async fn references_are_generic_and_never_modify_the_live_envelope() {
        let dir = tempfile::tempdir().unwrap();
        for source in crate::translate::Registry::default_agents().sources() {
            let path = dir.path().join(format!("{source}.ndjson"));
            let mut writer = JournalWriter::open_path(&path).await.unwrap();
            let mut env = event(vec![message(0)]);
            env.source = source.to_owned();
            env.event = "context".into();
            env.payload["a~/b"] = json!({"history":[message(0)]});
            let original = env.payload.clone();
            writer.append(&env).await.unwrap();
            writer.append(&env).await.unwrap();
            assert_eq!(env.payload, original);
            let stored = std::fs::read_to_string(&path).unwrap();
            assert!(stored.contains("_bt_history_refs"), "{source}");
            let mut reader = JournalReader::open(&path, writer.position())
                .await
                .unwrap()
                .unwrap();
            for _ in 0..2 {
                assert_eq!(
                    reader.next_entry().await.unwrap().unwrap().payload,
                    original
                );
            }
        }
    }

    #[tokio::test]
    async fn invalid_references_fail_instead_of_silently_changing_history() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.ndjson");
        let mut writer = JournalWriter::open_path(&path).await.unwrap();
        writer.append(&event(vec![message(0)])).await.unwrap();
        let offset = writer
            .append(&event(vec![message(0), message(1)]))
            .await
            .unwrap();
        writer
            .append(&event(vec![message(0), message(1), message(2)]))
            .await
            .unwrap();
        let stored = std::fs::read_to_string(&path).unwrap();
        let value: Value = serde_json::from_str(stored.lines().nth(2).unwrap()).unwrap();
        assert!(value.get("_bt_history_refs").is_some());
        let mut bad = Vec::new();
        for (key, val) in [
            ("base", json!(offset)),
            ("base", json!(1)),
            ("anchor", json!(offset + 1)),
            ("keep", json!(100)),
            ("keep", json!(0)),
            ("hash", json!("wrong")),
            ("path", json!("/source")),
            ("path", json!("/payload/missing")),
        ] {
            let mut copy = value.clone();
            copy["_bt_history_refs"][0][key] = val;
            bad.push(copy);
        }
        let mut copy = value.clone();
        copy["_bt_history_refs"][0]["anchor"] = value["_bt_history_refs"][0]["base"].clone();
        bad.push(copy);
        let mut copy = value.clone();
        copy["envelope"]["session_id"] = "other".into();
        bad.push(copy);
        let mut copy = value.clone();
        copy["_bt_history_refs"]
            .as_array_mut()
            .unwrap()
            .push(value["_bt_history_refs"][0].clone());
        bad.push(copy);
        let mut warm = JournalReader::open(&path, offset).await.unwrap().unwrap();
        for _ in 0..2 {
            warm.next_entry().await.unwrap().unwrap();
        }
        for value in bad {
            assert!(Decoder::default()
                .expand(value.clone(), &path, offset, 1)
                .is_err());
            assert!(warm.histories.expand(value, &path, offset, 1).is_err());
        }
    }

    #[tokio::test]
    async fn stream_cache_eviction_changes_only_storage_efficiency() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.ndjson");
        let mut writer = JournalWriter::open_path(&path).await.unwrap();
        for i in 0..2 * STREAMS {
            let mut env = event(vec![message(0)]);
            env.session_id = i.to_string();
            writer.append(&env).await.unwrap();
        }
        let env = event(vec![message(0)]);
        writer.append(&env).await.unwrap();
        let mut reader = JournalReader::open(&path, writer.position())
            .await
            .unwrap()
            .unwrap();
        for _ in 0..2 * STREAMS + 1 {
            assert_eq!(
                reader.next_entry().await.unwrap().unwrap().payload,
                env.payload
            );
        }
        assert!(writer.histories.0.len() <= STREAMS);
        assert!(
            (reader.histories.bytes <= CACHE_BYTES || reader.histories.records.len() == 1)
                && reader.histories.records.len() <= CACHE_RECORDS
        );
    }

    #[tokio::test]
    async fn released_pi_deltas_and_new_records_can_share_a_journal() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.ndjson");
        let mut old = event(vec![]);
        old.source = "pi".into();
        old.event = "context".into();
        old.payload =
            json!({"event":{"_bt_messages_delta":{"common_prefix":0,"suffix":[message(0)]}}});
        std::fs::write(
            &path,
            format!("{}\n", serde_json::to_string(&old.redacted()).unwrap()),
        )
        .unwrap();
        let mut writer = JournalWriter::open_path(&path).await.unwrap();
        let mut new = old.clone();
        new.payload =
            json!({"event":{"messages":[message(0)],"_bt_messages_delta":"native value"}});
        writer.append(&new).await.unwrap();
        writer.append(&new).await.unwrap();
        let mut reader = JournalReader::open(&path, writer.position())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            reader.next_entry().await.unwrap().unwrap().payload["event"]["messages"],
            json!([message(0)])
        );
        for _ in 0..2 {
            assert_eq!(
                reader.next_entry().await.unwrap().unwrap().payload,
                new.payload
            );
            assert!(
                reader.pi_context.is_empty(),
                "new records must release the legacy history cache"
            );
        }
    }

    #[test]
    fn growing_sessions_do_not_pay_for_full_snapshots_at_fixed_intervals() {
        let size = |events: usize| {
            let mut encoder = Encoder::default();
            let mut position = 0u64;
            let mut messages = Vec::new();
            for index in 0..events {
                messages
                    .push(json!({"role":"user","content":format!("{index}:{}", "x".repeat(128))}));
                position += encoder
                    .encode(event(messages.clone()).redacted(), position)
                    .unwrap()
                    .len() as u64
                    + 1;
            }
            position
        };
        let small = size(256);
        let large = size(512);
        assert!(
            large < 3 * small,
            "doubling a growing session must not quadruple its history storage: {small} -> {large}"
        );
    }

    #[tokio::test]
    async fn cache_limits_never_truncate_history_or_native_fields() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.ndjson");
        let mut writer = JournalWriter::open_path(&path).await.unwrap();
        let mut env = event(vec![message(0)]);
        for i in 0..2 * STREAMS {
            env.payload[format!("field{i}")] = json!({"history":[message(i)]});
        }
        let large = event(vec![
            json!({"role":"tool","content":"image-data".repeat(CACHE_BYTES/8)}),
        ]);
        let long = event((0..3 * PREFIXES).map(message).collect());
        let mut edited = long.clone();
        edited.payload["messages"][0] = message(999);
        for event in [&env, &env, &large, &large, &long, &edited] {
            writer.append(event).await.unwrap();
        }
        let mut reader = JournalReader::open(&path, writer.position())
            .await
            .unwrap()
            .unwrap();
        for expected in [&env, &env, &large, &large, &long, &edited] {
            assert_eq!(
                reader.next_entry().await.unwrap().unwrap().payload,
                expected.payload
            );
            if expected.payload == large.payload {
                assert!(reader.histories.bytes > CACHE_BYTES);
                assert_eq!(
                    reader.histories.records.len(),
                    1,
                    "keep a single oversized history hot"
                );
            }
            assert!(
                (reader.histories.bytes <= CACHE_BYTES || reader.histories.records.len() == 1)
                    && reader.histories.records.len() <= CACHE_RECORDS
            );
        }
        assert!(writer
            .histories
            .0
            .iter()
            .all(|h| h.prefixes.len() <= PREFIXES));
    }

    #[tokio::test]
    async fn failed_and_partial_appends_never_publish_a_prefix_base() {
        use tokio::io::AsyncWriteExt;
        let dir = tempfile::tempdir().unwrap();
        for reopen in [false, true] {
            let path = dir.path().join(format!("{reopen}.ndjson"));
            let mut writer = JournalWriter::open_path(&path).await.unwrap();
            let first = event(vec![message(0)]);
            let through = writer.append(&first).await.unwrap();
            writer.file = tokio::fs::File::open(&path).await.unwrap();
            assert!(writer
                .append(&event(vec![message(0), message(1)]))
                .await
                .is_err());
            assert_eq!(writer.position(), through);
            writer.file = tokio::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .await
                .unwrap();
            writer.file.write_all(b"{interrupted").await.unwrap();
            writer.file.flush().await.unwrap();
            if reopen {
                writer = JournalWriter::open_path(&path).await.unwrap();
            }
            let last = event(vec![message(0), message(1), message(2)]);
            writer.append(&last).await.unwrap();
            let mut reader = JournalReader::open(&path, writer.position())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                reader.next_entry().await.unwrap().unwrap().payload,
                first.payload
            );
            assert_eq!(
                reader.next_entry().await.unwrap().unwrap().payload,
                last.payload
            );
            assert!(reader.next_entry().await.unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn cancelling_a_pending_write_preserves_the_last_durable_boundary() {
        use std::{future::Future, task::Poll};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.ndjson");
        let mut writer = JournalWriter::open_path(&path).await.unwrap();
        let first = event(vec![message(0)]);
        writer.append(&first).await.unwrap();
        let interrupted = event(vec![json!({"content":"x".repeat(16*1024*1024)})]);
        let mut line = serde_json::to_vec(&interrupted.redacted()).unwrap();
        line.push(b'\n');
        let mut pending = Box::pin(writer.append_line(&line));
        let completed = std::future::poll_fn(|cx| {
            Poll::Ready(match pending.as_mut().poll(cx) {
                Poll::Ready(result) => {
                    result.unwrap();
                    true
                }
                Poll::Pending => false,
            })
        })
        .await;
        drop(pending);
        if !completed {
            assert!(writer.failed_write);
        }
        let last = event(vec![message(0), message(1)]);
        writer.append(&last).await.unwrap();
        let mut reader = JournalReader::open(&path, writer.position())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            reader.next_entry().await.unwrap().unwrap().payload,
            first.payload
        );
        if completed {
            assert_eq!(
                reader.next_entry().await.unwrap().unwrap().payload,
                interrupted.payload
            );
        }
        assert_eq!(
            reader.next_entry().await.unwrap().unwrap().payload,
            last.payload
        );
        assert!(reader.next_entry().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn older_readers_cannot_misinterpret_suffixes_as_complete_events() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.ndjson");
        let mut writer = JournalWriter::open_path(&path).await.unwrap();
        for _ in 0..2 {
            writer.append(&event(vec![message(0)])).await.unwrap();
        }
        for line in std::fs::read_to_string(&path).unwrap().lines() {
            assert!(serde_json::from_str::<crate::wire::RedactedEnvelope>(line).is_err());
        }
        // Complete corruption remains visible; only unfinished trailing bytes
        // may be discarded on reopen.
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        std::io::Write::write_all(&mut file, b"{bad}\n").unwrap();
        let writer = JournalWriter::open_path(&path).await.unwrap();
        let mut reader = JournalReader::open(&path, writer.position())
            .await
            .unwrap()
            .unwrap();
        for _ in 0..2 {
            reader.next_entry().await.unwrap().unwrap();
        }
        assert!(reader.next_entry().await.is_err());
    }
}
