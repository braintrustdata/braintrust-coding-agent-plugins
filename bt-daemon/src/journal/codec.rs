//! Lossless history compression using the last eligible payload as a zstd
//! dictionary. All sources share this storage codec; live events stay intact.
use base64::{engine::general_purpose::STANDARD, Engine};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

const MIN_BYTES: usize = 4 * 1024;
const MAX_BYTES: usize = 8 * 1024 * 1024;
pub(super) const DELTA_RECORD: &str = "payload_zstd_v1";

#[derive(Clone, Default, Serialize, Deserialize)]
pub(crate) struct CodecState {
    pub(crate) legacy_pi: Vec<Value>,
    pub(super) base: Option<Base>,
}
#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Base {
    through: u64,
    json: Arc<str>,
}
#[derive(Serialize, Deserialize)]
pub(super) struct Delta {
    base: u64,
    data: String,
}

// Cache limits govern retained state, never which event data is journaled.
// Preserve the writer's exact JSON bytes as the compression dictionary.
pub(super) fn cache_serialized(json: &str) -> Option<Arc<str>> {
    (MIN_BYTES..=MAX_BYTES)
        .contains(&json.len())
        .then(|| Arc::from(json))
}

impl CodecState {
    pub(super) fn encode(
        &self,
        payload: &Value,
    ) -> anyhow::Result<(Option<Arc<str>>, Option<Delta>)> {
        let Some(json) = cache_serialized(&serde_json::to_string(payload)?) else {
            return Ok((None, None));
        };
        let Some(base) = &self.base else {
            return Ok((Some(json), None));
        };
        let mut compressor = zstd::bulk::Compressor::with_dictionary(1, base.json.as_bytes())?;
        compressor.set_parameter(zstd::zstd_safe::CParameter::ChecksumFlag(true))?;
        let delta = Delta {
            base: base.through,
            data: STANDARD.encode(compressor.compress(json.as_bytes())?),
        };
        let worthwhile = serde_json::to_vec(&delta)?.len() + 16 < json.len();
        Ok((Some(json), worthwhile.then_some(delta)))
    }

    pub(super) fn record(&mut self, through: u64, json: Option<Arc<str>>) {
        // Small intervening hooks need not discard the last history base.
        if let Some(json) = json {
            self.base = Some(Base { through, json });
        }
    }

    pub(super) fn decode(&self, encoded: Value) -> anyhow::Result<(Value, Arc<str>)> {
        let delta: Delta = serde_json::from_value(encoded)?;
        let base = self
            .base
            .as_ref()
            .filter(|base| base.through == delta.base)
            .ok_or_else(|| anyhow::anyhow!("journal dictionary base is unavailable"))?;
        anyhow::ensure!(
            delta.data.len() <= MAX_BYTES,
            "compressed payload exceeds codec budget"
        );
        let bytes = zstd::bulk::Decompressor::with_dictionary(base.json.as_bytes())?
            .decompress(&STANDARD.decode(delta.data)?, MAX_BYTES)?;
        let json = String::from_utf8(bytes)?;
        Ok((serde_json::from_str(&json)?, json.into()))
    }

    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.base
                .as_ref()
                .is_none_or(|base| base.json.len() <= MAX_BYTES),
            "journal dictionary exceeds codec budget"
        );
        Ok(())
    }
    pub(crate) fn record_live(&mut self, position: &crate::pipeline::input::SourcePosition) {
        self.legacy_pi.clear();
        self.record(position.through, position.decoder_base.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn dictionary_roundtrip_is_lossless_bounded_and_rejects_invalid_bases() {
        let first = json!({"history":"é📚".repeat(1024),"index":1});
        let next = json!({"history":format!("{}more context", "é📚".repeat(1024)),"index":2});
        let mut cache = CodecState::default();
        let (base, delta) = cache.encode(&first).unwrap();
        assert!(delta.is_none());
        cache.record(100, base);
        let (_, delta) = cache.encode(&next).unwrap();
        let encoded = serde_json::to_value(delta.unwrap()).unwrap();
        assert!(encoded.to_string().len() < first.to_string().len() / 3);
        assert_eq!(cache.decode(encoded.clone()).unwrap().0, next);
        assert!(CodecState::default().decode(encoded.clone()).is_err());
        cache.record(101, cache.base.as_ref().map(|base| base.json.clone()));
        assert!(cache.decode(encoded.clone()).is_err());
        cache.record(100, Some("different dictionary".into()));
        assert!(cache.decode(encoded).is_err());
        for payload in [
            json!({"small":1}),
            json!({"large":"x".repeat(MAX_BYTES + 1)}),
        ] {
            let (base, delta) = cache.encode(&payload).unwrap();
            assert!(base.is_none() && delta.is_none());
        }
    }
}
