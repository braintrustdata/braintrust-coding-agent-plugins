//! Checkpoint-only SpanRow codec. The public row JSON intentionally omits
//! internal correlation and merge fields; reducer checkpoints must retain them.
use super::{LateMergeVersion, SpanRow};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Row {
    value: SpanRow,
    late_merge_key: Option<String>,
    late_merge_version: Option<LateMergeVersion>,
    turn_span_id: Option<String>,
    is_turn: bool,
}
impl Row {
    fn from_row(value: &SpanRow) -> Self {
        Self {
            value: value.clone(),
            late_merge_key: value.late_merge_key.clone(),
            late_merge_version: value.late_merge_version.clone(),
            turn_span_id: value.turn_span_id.clone(),
            is_turn: value.is_turn,
        }
    }
    fn into_row(mut self) -> SpanRow {
        self.value.late_merge_key = self.late_merge_key;
        self.value.late_merge_version = self.late_merge_version;
        self.value.turn_span_id = self.turn_span_id;
        self.value.is_turn = self.is_turn;
        self.value
    }
}
pub(super) mod row {
    use super::*;
    pub fn serialize<S: serde::Serializer>(
        value: &SpanRow,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        Row::from_row(value).serialize(serializer)
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<SpanRow, D::Error> {
        Ok(Row::deserialize(deserializer)?.into_row())
    }
}
pub(super) mod rows {
    use super::*;
    use std::collections::BTreeMap;
    pub fn serialize<S: serde::Serializer>(
        value: &BTreeMap<String, SpanRow>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        value
            .iter()
            .map(|(id, row)| (id, Row::from_row(row)))
            .collect::<BTreeMap<_, _>>()
            .serialize(serializer)
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<BTreeMap<String, SpanRow>, D::Error> {
        Ok(BTreeMap::<String, Row>::deserialize(deserializer)?
            .into_iter()
            .map(|(id, row)| (id, row.into_row()))
            .collect())
    }
}
