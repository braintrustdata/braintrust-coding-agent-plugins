//! Preserve sink-only row fields in durable translator state without changing wire JSON.
use super::{LateMergeVersion, SpanRow};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;

/// Row fields the wire format omits but delivery needs.
#[derive(Serialize, Deserialize)]
pub(crate) struct SinkFields {
    late_merge_key: Option<String>,
    #[serde(default)]
    late_merge_version: Option<LateMergeVersion>,
    turn_span_id: Option<String>,
    is_turn: bool,
}
impl SinkFields {
    pub fn of(row: &SpanRow) -> Self {
        Self {
            late_merge_key: row.late_merge_key.clone(),
            late_merge_version: row.late_merge_version.clone(),
            turn_span_id: row.turn_span_id.clone(),
            is_turn: row.is_turn,
        }
    }
    pub fn restore(self, row: &mut SpanRow) {
        row.late_merge_key = self.late_merge_key;
        row.late_merge_version = self.late_merge_version;
        row.turn_span_id = self.turn_span_id;
        row.is_turn = self.is_turn;
    }
}

#[derive(Serialize, Deserialize)]
struct Row {
    value: SpanRow,
    #[serde(flatten)]
    fields: SinkFields,
}
impl From<&SpanRow> for Row {
    fn from(value: &SpanRow) -> Self {
        Self {
            value: value.clone(),
            fields: SinkFields::of(value),
        }
    }
}
impl Row {
    fn restore(mut self) -> SpanRow {
        self.fields.restore(&mut self.value);
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
