//! Lossless history prefixes. References point only into the same append-only
//! journal; caches affect efficiency, never the data available for recovery.
use crate::wire::RedactedEnvelope;
use anyhow::{ensure, Context};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::VecDeque,
    io::{BufRead, Seek},
    path::Path,
};

pub(super) const FORMAT: &str = "event_history_v2";
const STREAMS: usize = 16;
const PREFIXES: usize = 128;
const CACHE_BYTES: usize = 8 * 1024 * 1024;
const CACHE_RECORDS: usize = 64;

pub(super) struct Record {
    envelope: RedactedEnvelope,
    references: Vec<Reference>,
}

pub(super) fn parse(bytes: &[u8]) -> anyhow::Result<Record> {
    // Parse the envelope independently so the storage wrapper does not consume
    // any of the native event's JSON depth allowance. This is the only path.
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Raw<'a> {
        _bt_record_type: super::RecordKind,
        #[serde(borrow)]
        envelope: &'a serde_json::value::RawValue,
        #[serde(default)]
        _bt_history_refs: Vec<Reference>,
    }
    let raw: Raw<'_> = serde_json::from_slice(bytes)?;
    ensure!(
        matches!(raw._bt_record_type, super::RecordKind::History),
        "not a history record"
    );
    Ok(Record {
        envelope: serde_json::from_str(raw.envelope.get())?,
        references: raw._bt_history_refs,
    })
}

#[derive(Clone)]
struct Prefix {
    hash: [u8; 32],
    count: usize,
    bytes: usize,
}
#[derive(Clone)]
struct History {
    key: [u8; 32],
    offset: u64,
    record_hash: [u8; 32],
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
    keep: usize,
    // SHA-256 of the referenced record, excluding its terminating newline.
    #[serde(rename = "hash")]
    checksum: String,
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
// Hash the exact serialized representation while measuring its storage cost.
struct FingerprintWriter {
    bytes: usize,
    digest: Sha256,
}
impl std::io::Write for FingerprintWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.bytes += bytes.len();
        self.digest.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn hex(hash: [u8; 32]) -> String {
    hash.iter().map(|byte| format!("{byte:02x}")).collect()
}

// Persisted checksums cover stored bytes, so JSON parser features cannot
// change their meaning. Prefix fingerprints are only an in-memory optimization.
pub(super) fn record_hash(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes.strip_suffix(b"\n").unwrap_or(bytes)).into()
}
fn prefixes(messages: &[Value]) -> anyhow::Result<VecDeque<Prefix>> {
    let mut result = VecDeque::new();
    let mut hash = [0; 32];
    let mut bytes = 2;
    for (index, message) in messages.iter().enumerate() {
        let mut encoded = FingerprintWriter {
            bytes: 0,
            digest: Sha256::new(),
        };
        encoded.digest.update(hash);
        serde_json::to_writer(&mut encoded, message)?;
        hash = encoded.digest.finalize().into();
        bytes += encoded.bytes + usize::from(index > 0);
        if result.len() == PREFIXES {
            result.pop_front();
        }
        result.push_back(Prefix {
            hash,
            count: index + 1,
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
                            && h.prefixes
                                .iter()
                                .any(|old| old.count == prefix.count && old.hash == prefix.hash)
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
                .and_then(|(index, common)| self.0.remove(index).map(|history| (history, common)));
            let full_bytes = current.back().map_or(2, |p| p.bytes);
            let mut delta_bytes = 0;
            let mut unchanged = false;
            if let Some((previous, common)) = &previous {
                let reference = Reference {
                    path: path.clone(),
                    base: previous.offset,
                    keep: common.count,
                    checksum: hex(previous.record_hash),
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
                    delta_bytes = accumulated;
                    unchanged = previous.prefixes.back().map(|p| (p.count, p.hash))
                        == current.back().map(|p| (p.count, p.hash));
                }
            }
            let mut entry = if unchanged {
                previous.context("missing prefix base")?.0
            } else {
                if let Some((previous, _)) = previous {
                    self.0.push_back(previous);
                }
                History {
                    key,
                    offset,
                    record_hash: [0; 32],
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
        let bytes = serde_json::to_vec(&Stored {
            _bt_record_type: FORMAT,
            envelope: &value,
            _bt_history_refs: references,
        })?;
        let hash = record_hash(&bytes);
        for history in self.0.iter_mut().filter(|h| h.offset == offset) {
            history.record_hash = hash;
        }
        Ok(bytes)
    }
}

struct Cached {
    source: String,
    session_id: String,
    payload: Value,
    bytes: usize,
    hash: [u8; 32],
}
#[derive(Default)]
pub(super) struct Decoder {
    records: VecDeque<(u64, Cached)>,
    bytes: usize,
}
impl Decoder {
    fn remember(&mut self, offset: u64, value: &RedactedEnvelope, hash: [u8; 32]) {
        if self.records.iter().any(|(key, _)| *key == offset) {
            return;
        }
        let bytes = crate::wire::retained_json_bytes(&value.payload) as usize
            + value.source.len()
            + value.session_id.len()
            + std::mem::size_of::<Cached>();
        // One oversized event travels alone, as on the live SpanOp feed.
        while !self.records.is_empty()
            && (self.bytes.saturating_add(bytes) > CACHE_BYTES
                || self.records.len() >= CACHE_RECORDS)
        {
            if let Some((_, evicted)) = self.records.pop_front() {
                self.bytes -= evicted.bytes;
            }
        }
        self.records.push_back((
            offset,
            Cached {
                source: value.source.clone(),
                session_id: value.session_id.clone(),
                payload: value.payload.clone(),
                bytes,
                hash,
            },
        ));
        self.bytes += bytes;
    }
    fn read(
        file: &mut std::fs::File,
        offset: u64,
        limit: u64,
    ) -> anyhow::Result<(Record, [u8; 32])> {
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
        Ok((parse(&line)?, record_hash(&line)))
    }
    pub(super) fn expand(
        &mut self,
        record: Record,
        file_path: &Path,
        offset: u64,
        hash: [u8; 32],
    ) -> anyhow::Result<RedactedEnvelope> {
        let Record {
            envelope: mut value,
            references,
        } = record;
        let mut stored_paths = Vec::new();
        paths(&value.payload, "", &mut stored_paths);
        // Empty suffixes cannot become new writer bases. Keep their actual
        // referenced heads hot instead of caching duplicate complete histories.
        let cacheable = stored_paths.iter().any(|path| {
            value
                .payload
                .pointer(path)
                .and_then(Value::as_array)
                .is_some_and(|a| !a.is_empty())
        });
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
            let pointer = reference
                .path
                .strip_prefix("/payload")
                .context("history reference outside payload")?;
            ensure!(
                reference.base < offset && reference.keep > 0,
                "invalid history reference range"
            );
            let mut base = reference.base;
            let mut limit = offset;
            let mut count = reference.keep;
            let mut parts = Vec::new();
            let mut expected_hash = reference.checksum;
            loop {
                if let Some(index) = self.records.iter().position(|(key, _)| *key == base) {
                    let record = &mut self.records[index].1;
                    ensure!(
                        record.source == value.source && record.session_id == value.session_id,
                        "history reference crossed its cached stream"
                    );
                    let messages = record
                        .payload
                        .pointer(pointer)
                        .and_then(Value::as_array)
                        .context("cached history is not an array")?;
                    ensure!(count <= messages.len(), "history prefix exceeds cache");
                    ensure!(
                        hex(record.hash) == expected_hash,
                        "history record checksum mismatch"
                    );
                    parts.push(messages[..count].to_vec());
                    let cached = self
                        .records
                        .remove(index)
                        .context("missing cached record")?;
                    self.records.push_back(cached);
                    break;
                }
                let file = match &mut file {
                    Some(file) => file,
                    slot @ None => slot.insert(std::fs::File::open(file_path)?),
                };
                let (record, hash) = Self::read(file, base, limit)?;
                ensure!(
                    hex(hash) == expected_hash,
                    "history record checksum mismatch"
                );
                ensure!(
                    record.envelope.source == value.source
                        && record.envelope.session_id == value.session_id,
                    "history reference crossed its stream"
                );
                let messages = record
                    .envelope
                    .payload
                    .pointer(pointer)
                    .and_then(Value::as_array)
                    .context("history reference target is not an array")?;
                if let Some(parent) = record.references.iter().find(|r| r.path == reference.path) {
                    ensure!(
                        parent.base < base && parent.keep > 0,
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
                    expected_hash = parent.checksum.clone();
                } else {
                    ensure!(count <= messages.len(), "history prefix exceeds literal");
                    parts.push(messages[..count].to_vec());
                    break;
                }
            }
            let suffix = value
                .payload
                .pointer_mut(pointer)
                .and_then(Value::as_array_mut)
                .context("history suffix is not an array")?;
            let mut prefix: Vec<_> = parts.into_iter().rev().flatten().collect();
            prefix.append(suffix);
            *suffix = prefix;
        }
        if cacheable {
            self.remember(offset, &value, hash);
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

    #[test]
    fn record_structure_is_validated_before_recovery() {
        let valid = json!({"_bt_record_type":FORMAT,"envelope":event(vec![]).redacted(),
            "_bt_history_refs":[{"path":"/payload/messages","base":0,"keep":1,"hash":"abc"}]});
        assert!(parse(&serde_json::to_vec(&valid).unwrap()).is_ok());
        for (pointer, wrong) in [
            ("/_bt_record_type", json!(null)),
            ("/_bt_record_type", json!("event_history_v1")),
            ("/envelope", json!({})),
            ("/envelope", json!(null)),
            ("/envelope/source", json!(42)),
            ("/envelope/session_id", json!(null)),
            ("/envelope/ts_ms", json!("now")),
            ("/_bt_history_refs", json!({})),
            ("/_bt_history_refs/0/base", json!(-1)),
            ("/_bt_history_refs/0/keep", json!("1")),
        ] {
            let mut bad = valid.clone();
            *bad.pointer_mut(pointer).unwrap() = wrong;
            assert!(
                parse(&serde_json::to_vec(&bad).unwrap()).is_err(),
                "{pointer}"
            );
        }
        for field in ["source", "session_id", "event", "ts_ms", "payload"] {
            let mut bad = valid.clone();
            bad["envelope"].as_object_mut().unwrap().remove(field);
            assert!(
                parse(&serde_json::to_vec(&bad).unwrap()).is_err(),
                "{field}"
            );
        }
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
            if serde_json::from_slice::<Envelope>(&serde_json::to_vec(&env).unwrap()).is_err() {
                break;
            }
            deepest = env.clone();
        }
        let first = writer.append(&deepest).await.unwrap();
        writer.append(&deepest).await.unwrap();
        let bytes = std::fs::read(&path).unwrap();
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
            .expand(record, &path, first, record_hash(&bytes[first as usize..]))
            .unwrap();
        assert_eq!(recovered.payload, deepest.payload);
    }

    #[test]
    fn prefix_matching_preserves_serialized_order_and_number_types() {
        let fingerprint = |value: &Value| {
            prefixes(std::slice::from_ref(value))
                .unwrap()
                .pop_back()
                .unwrap()
                .hash
        };
        let left: Value = serde_json::from_str(r#"{"z":1,"a":[true,"é"]}"#).unwrap();
        let right: Value = serde_json::from_str(r#"{"a":[true,"é"],"z":1}"#).unwrap();
        assert_eq!(
            fingerprint(&left) == fingerprint(&right),
            serde_json::to_vec(&left).unwrap() == serde_json::to_vec(&right).unwrap()
        );
        assert_ne!(fingerprint(&json!(1)), fingerprint(&json!(1.0)));
    }

    #[tokio::test]
    async fn references_verify_stored_bytes_independently_of_json_features() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bytes.ndjson");
        // Deliberately noncanonical key order and numeric spelling. Re-encoding
        // this Value may differ, depending on the embedding CLI's JSON features.
        let base = format!(
            r#"{{"_bt_record_type":"{FORMAT}","envelope":{{"source":"muse","session_id":"test","event":"native","ts_ms":1,"payload":{{"messages":[{{"z":1e+00,"a":"text"}}]}}}}}}"#
        );
        let value = parse(base.as_bytes()).unwrap();
        let expected = value.envelope.payload.clone();
        let mut next: Value = serde_json::from_str(&base).unwrap();
        next["envelope"]["payload"]["messages"] = json!([]);
        next["_bt_history_refs"] = json!([{"path":"/payload/messages","base":0,
            "keep":1,"hash":hex(record_hash(base.as_bytes()))}]);
        let next_bytes = serde_json::to_vec(&next).unwrap();
        let offset = base.len() as u64 + 1;
        std::fs::write(
            &path,
            format!(
                "{base}\n{}\n",
                String::from_utf8(next_bytes.clone()).unwrap()
            ),
        )
        .unwrap();
        assert_eq!(
            Decoder::default()
                .expand(
                    parse(&next_bytes).unwrap(),
                    &path,
                    offset,
                    record_hash(&next_bytes)
                )
                .unwrap()
                .payload,
            expected
        );
        let mut reader = JournalReader::open(&path, std::fs::metadata(&path).unwrap().len())
            .await
            .unwrap()
            .unwrap();
        for _ in 0..2 {
            assert_eq!(
                reader.next_entry().await.unwrap().unwrap().payload,
                expected
            );
        }
        // A valid JSON edit must still invalidate the reference's checksum.
        std::fs::write(&path, format!("{}\n", base.replace("text", "edit"))).unwrap();
        assert!(Decoder::default()
            .expand(
                parse(&next_bytes).unwrap(),
                &path,
                offset,
                record_hash(&next_bytes)
            )
            .is_err());
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
                    parse(line.as_bytes()).unwrap(),
                    &path,
                    offset,
                    record_hash(line.as_bytes()),
                )
                .unwrap();
            assert_eq!(&value.payload, expected);
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
            ("unknown", json!(offset + 1)),
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
            let bytes = serde_json::to_vec(&value).unwrap();
            assert!(parse(&bytes)
                .and_then(|record| Decoder::default().expand(
                    record,
                    &path,
                    offset,
                    record_hash(&bytes)
                ))
                .is_err());
            assert!(parse(&bytes)
                .and_then(|record| warm.histories.expand(
                    record,
                    &path,
                    offset,
                    record_hash(&bytes)
                ))
                .is_err());
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
