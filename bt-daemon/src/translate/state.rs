//! Preserve sink-only row fields in durable translator state without changing wire JSON.
use super::{LateMergeVersion, SpanRow};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize)]
struct Row {
    value: SpanRow,
    late_merge_key: Option<String>,
    late_merge_version: Option<LateMergeVersion>,
    turn_span_id: Option<String>,
    is_turn: bool,
}
impl From<&SpanRow> for Row {
    fn from(value: &SpanRow) -> Self {
        Self {
            value: value.clone(),
            late_merge_key: value.late_merge_key.clone(),
            late_merge_version: value.late_merge_version.clone(),
            turn_span_id: value.turn_span_id.clone(),
            is_turn: value.is_turn,
        }
    }
}
impl Row {
    fn restore(mut self) -> SpanRow {
        self.value.late_merge_key = self.late_merge_key;
        self.value.late_merge_version = self.late_merge_version;
        self.value.turn_span_id = self.turn_span_id;
        self.value.is_turn = self.is_turn;
        self.value
    }
}
pub(super) mod row {
    use super::*;
    pub fn serialize<S: Serializer>(value: &SpanRow, serializer: S) -> Result<S::Ok, S::Error> {
        Row::from(value).serialize(serializer)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<SpanRow, D::Error> {
        Ok(Row::deserialize(deserializer)?.restore())
    }
}
pub(super) mod rows {
    use super::*;
    pub fn serialize<S: Serializer>(
        value: &BTreeMap<String, SpanRow>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        value
            .iter()
            .map(|(k, v)| (k, Row::from(v)))
            .collect::<BTreeMap<_, _>>()
            .serialize(serializer)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<BTreeMap<String, SpanRow>, D::Error> {
        Ok(BTreeMap::<String, Row>::deserialize(deserializer)?
            .into_iter()
            .map(|(k, v)| (k, v.restore()))
            .collect())
    }
}

/// JSON objects cannot use tuple keys; continuation maps retain typed keys as pairs.
pub(super) mod pairs {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::{collections::HashMap, hash::Hash};
    pub fn serialize<K: Serialize, V: Serialize, S: Serializer>(
        value: &HashMap<K, V>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        value.iter().collect::<Vec<_>>().serialize(serializer)
    }
    pub fn deserialize<
        'de,
        K: Deserialize<'de> + Eq + Hash,
        V: Deserialize<'de>,
        D: Deserializer<'de>,
    >(
        deserializer: D,
    ) -> Result<HashMap<K, V>, D::Error> {
        Ok(Vec::<(K, V)>::deserialize(deserializer)?
            .into_iter()
            .collect())
    }
}
