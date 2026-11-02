//! Lossless payload encoding shared by every source journal. Repeated event
//! kinds may reference their previous payload, with a bounded per-session cache.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::VecDeque, io::Write, sync::Arc};

// These bounds are part of the v1 decoding contract.
const MIN_BYTES: usize = 4 * 1024;
const MAX_BASES: usize = 8;
const MAX_BYTES: usize = 8 * 1024 * 1024;
pub(super) const DELTA_RECORD: &str = "payload_delta_v1";

#[derive(Clone, Default, Serialize, Deserialize)]
pub(crate) struct PayloadCache {
    entries: VecDeque<Base>,
}
#[derive(Clone, Serialize, Deserialize)]
struct Base {
    event: String,
    through: u64,
    json: Arc<str>,
}
#[derive(Serialize, Deserialize)]
pub(super) struct Delta {
    base: u64,
    prefix: usize,
    suffix: usize,
    insert: String,
}

/// Stop serialization once the cache budget is exceeded. Large events remain
/// complete raw records without allocating another unbounded payload copy.
struct LimitedJson {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
}
impl Write for LimitedJson {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            self.exceeded = true;
            return Err(std::io::Error::other("journal codec cache budget exceeded"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(super) fn cache_json(event: &str, payload: &Value) -> anyhow::Result<Option<Arc<str>>> {
    if event.len() >= MAX_BYTES {
        return Ok(None);
    }
    let mut output = LimitedJson {
        bytes: Vec::new(),
        limit: MAX_BYTES - event.len(),
        exceeded: false,
    };
    let result = serde_json::to_writer(&mut output, payload);
    if output.exceeded {
        return Ok(None);
    }
    result?;
    if output.bytes.len() < MIN_BYTES {
        return Ok(None);
    }
    Ok(Some(String::from_utf8(output.bytes)?.into()))
}

impl PayloadCache {
    pub(super) fn encode(
        &self,
        event: &str,
        payload: &Value,
    ) -> anyhow::Result<(Option<Arc<str>>, Option<Delta>)> {
        let Some(json) = cache_json(event, payload)? else {
            return Ok((None, None));
        };
        let Some(base) = self.entries.iter().find(|base| base.event == event) else {
            return Ok((Some(json), None));
        };
        let mut prefix = base
            .json
            .bytes()
            .zip(json.bytes())
            .take_while(|(a, b)| a == b)
            .count();
        while !base.json.is_char_boundary(prefix) || !json.is_char_boundary(prefix) {
            prefix -= 1;
        }
        let remaining = (base.json.len() - prefix).min(json.len() - prefix);
        let mut suffix = base
            .json
            .bytes()
            .rev()
            .zip(json.bytes().rev())
            .take(remaining)
            .take_while(|(a, b)| a == b)
            .count();
        while !base.json.is_char_boundary(base.json.len() - suffix)
            || !json.is_char_boundary(json.len() - suffix)
        {
            suffix -= 1;
        }
        let delta = Delta {
            base: base.through,
            prefix,
            suffix,
            insert: json[prefix..json.len() - suffix].to_owned(),
        };
        // Account for the extra record tag and avoid expanding small payloads.
        let worthwhile = serde_json::to_vec(&delta)?.len() + 16 < json.len();
        Ok((Some(json), worthwhile.then_some(delta)))
    }

    pub(super) fn record(&mut self, event: &str, through: u64, json: Option<Arc<str>>) {
        self.entries.retain(|base| base.event != event);
        let Some(json) = json else {
            return;
        };
        if json.len() > MAX_BYTES.saturating_sub(event.len()) {
            return;
        }
        while self.entries.len() >= MAX_BASES
            || self
                .entries
                .iter()
                .map(|base| base.json.len() + base.event.len())
                .sum::<usize>()
                + json.len()
                + event.len()
                > MAX_BYTES
        {
            self.entries.pop_front();
        }
        self.entries.push_back(Base {
            event: event.to_owned(),
            through,
            json,
        });
    }

    pub(super) fn decode(&self, event: &str, encoded: Value) -> anyhow::Result<(Value, Arc<str>)> {
        let delta: Delta = serde_json::from_value(encoded)?;
        let base = self
            .entries
            .iter()
            .find(|base| base.event == event && base.through == delta.base)
            .ok_or_else(|| anyhow::anyhow!("journal payload delta base is unavailable"))?;
        let kept = delta
            .prefix
            .checked_add(delta.suffix)
            .ok_or_else(|| anyhow::anyhow!("invalid journal delta bounds"))?;
        anyhow::ensure!(
            kept <= base.json.len()
                && base.json.is_char_boundary(delta.prefix)
                && base.json.is_char_boundary(base.json.len() - delta.suffix),
            "invalid journal delta bounds"
        );
        let budget = MAX_BYTES.saturating_sub(event.len());
        anyhow::ensure!(
            delta.insert.len() <= budget && kept <= budget - delta.insert.len(),
            "journal delta exceeds codec budget"
        );
        let mut json = String::with_capacity(kept + delta.insert.len());
        json.push_str(&base.json[..delta.prefix]);
        json.push_str(&delta.insert);
        json.push_str(&base.json[base.json.len() - delta.suffix..]);
        Ok((serde_json::from_str(&json)?, json.into()))
    }

    pub(super) fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.entries.len() <= MAX_BASES
                && self
                    .entries
                    .iter()
                    .map(|base| base.json.len() + base.event.len())
                    .sum::<usize>()
                    <= MAX_BYTES,
            "journal decoder checkpoint exceeds codec budget"
        );
        Ok(())
    }
}

#[derive(Clone, Default, Serialize)]
pub(crate) struct DecoderContext {
    pub(crate) legacy_pi: Vec<Value>,
    pub(super) payloads: PayloadCache,
}
impl<'de> Deserialize<'de> for DecoderContext {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Stored {
            Legacy(Vec<Value>),
            Current {
                legacy_pi: Vec<Value>,
                payloads: PayloadCache,
            },
        }
        let context = match Stored::deserialize(deserializer)? {
            Stored::Legacy(legacy_pi) => Self {
                legacy_pi,
                payloads: PayloadCache::default(),
            },
            Stored::Current {
                legacy_pi,
                payloads,
            } => Self {
                legacy_pi,
                payloads,
            },
        };
        context
            .payloads
            .validate()
            .map_err(serde::de::Error::custom)?;
        Ok(context)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn unicode_prefix_suffix_deltas_are_lossless_and_reject_missing_bases() {
        let first = json!({"data":format!("{}é{}", "é".repeat(1000), "tail".repeat(1000))});
        let next = json!({"data":format!("{}ê{}", "é".repeat(1000), "tail".repeat(1000))});
        let mut cache = PayloadCache::default();
        let (base, delta) = cache.encode("arbitrary-event", &first).unwrap();
        assert!(delta.is_none());
        cache.record("arbitrary-event", 123, base);
        let (_, delta) = cache.encode("arbitrary-event", &next).unwrap();
        let encoded = serde_json::to_value(delta.unwrap()).unwrap();
        assert_eq!(
            cache.decode("arbitrary-event", encoded.clone()).unwrap().0,
            next
        );
        assert!(PayloadCache::default()
            .decode("arbitrary-event", encoded.clone())
            .is_err());
        let mut invalid = encoded;
        invalid["prefix"] = json!(usize::MAX);
        assert!(cache.decode("arbitrary-event", invalid).is_err());
    }

    #[test]
    fn cache_limits_evict_old_bases_and_preserve_oversized_payloads() {
        let mut cache = PayloadCache::default();
        let payload = json!({"data":"x".repeat(MAX_BYTES / 3)});
        for index in 0..MAX_BASES + 2 {
            let name = format!("event-{index}");
            let (json, _) = cache.encode(&name, &payload).unwrap();
            cache.record(&name, index as u64, json);
            cache.validate().unwrap();
        }
        assert!(cache.entries.iter().all(|base| base.event != "event-0"));
        let (small, delta) = cache.encode("small", &json!({"value":1})).unwrap();
        assert!(
            small.is_none() && delta.is_none(),
            "small hook payloads should retain the direct serialization path"
        );
        let oversized = json!({"data":"x".repeat(MAX_BYTES + 1)});
        let (json, delta) = cache.encode("large", &oversized).unwrap();
        assert!(json.is_none());
        assert!(delta.is_none());
        cache.record("large", 100, json);
        cache.record("large", 101, Some("x".repeat(MAX_BYTES + 1).into()));
        cache.validate().unwrap();
        assert!(cache.entries.iter().all(|base| base.event != "large"));
    }

    #[test]
    fn checkpoints_read_legacy_decoder_state_and_preserve_shared_bases() {
        let legacy = vec![json!({"role":"user","content":"legacy"})];
        let context: DecoderContext = serde_json::from_value(json!(legacy)).unwrap();
        assert_eq!(context.legacy_pi, legacy);
        let mut context = DecoderContext::default();
        let payload = json!({"messages":["content".repeat(1000)]});
        let (json, _) = context.payloads.encode("message", &payload).unwrap();
        context.payloads.record("message", 100, json);
        let saved = serde_json::to_vec(&context).unwrap();
        let restored: DecoderContext = serde_json::from_slice(&saved).unwrap();
        let (_, delta) = context.payloads.encode("message", &payload).unwrap();
        assert_eq!(
            restored
                .payloads
                .decode("message", serde_json::to_value(delta.unwrap()).unwrap())
                .unwrap()
                .0,
            payload
        );
    }
}
