//! Versioned persisted encodings.
//!
//! Journal entries outlive the binary that wrote them, so they use their own
//! codecs rather than the sink's serde representation: `SpanRow` deliberately
//! skips translator-owned and sink-only fields when serialized, and every one
//! of those fields must survive a restart.

use super::types::{CaptureVersions, JournalSpanOp, SpanEntry};
use crate::translate::{OriginSnapshot, RouteSlots, SpanRow, SpanType};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Current span entry codec version.
pub(crate) const SPAN_ENTRY_VERSION: u32 = 1;

#[derive(Serialize, Deserialize)]
struct StoredSlots {
    #[serde(default, skip_serializing_if = "is_false")]
    attach: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    metadata: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    tags: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    project: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Serialize, Deserialize)]
struct StoredRow {
    span_id: String,
    root_span_id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    parent_span_ids: Vec<String>,
    #[serde(default)]
    name: String,
    span_type: SpanType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    start_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    end_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    input: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    output: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    metadata: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    metrics: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    late_merge_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    turn_span_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tags: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "is_false")]
    is_turn: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    origin: Option<OriginSnapshot>,
    #[serde(default, skip_serializing_if = "is_false")]
    defer_until_child: bool,
    #[serde(default)]
    slots: Option<StoredSlots>,
}

impl From<&SpanRow> for StoredRow {
    fn from(row: &SpanRow) -> Self {
        let slots = row.route_slots;
        Self {
            span_id: row.span_id.clone(),
            root_span_id: row.root_span_id.clone(),
            parent_span_ids: row.parent_span_ids.clone(),
            name: row.name.clone(),
            span_type: row.span_type,
            start_ms: row.start_ms,
            end_ms: row.end_ms,
            input: row.input.clone(),
            output: row.output.clone(),
            metadata: row.metadata.clone(),
            metrics: row.metrics.clone(),
            error: row.error.clone(),
            late_merge_key: row.late_merge_key.clone(),
            turn_span_id: row.turn_span_id.clone(),
            tags: row.tags.clone(),
            is_turn: row.is_turn,
            origin: row.origin.clone(),
            defer_until_child: row.defer_until_child,
            slots: (slots != RouteSlots::default()).then_some(StoredSlots {
                attach: slots.attach,
                metadata: slots.metadata,
                tags: slots.tags,
                project: slots.project,
            }),
        }
    }
}

impl From<StoredRow> for SpanRow {
    fn from(row: StoredRow) -> Self {
        let slots = row.slots.unwrap_or(StoredSlots {
            attach: false,
            metadata: false,
            tags: false,
            project: false,
        });
        Self {
            span_id: row.span_id,
            root_span_id: row.root_span_id,
            parent_span_ids: row.parent_span_ids,
            name: row.name,
            span_type: row.span_type,
            start_ms: row.start_ms,
            end_ms: row.end_ms,
            input: row.input,
            output: row.output,
            metadata: row.metadata,
            metrics: row.metrics,
            error: row.error,
            late_merge_key: row.late_merge_key,
            turn_span_id: row.turn_span_id,
            tags: row.tags,
            is_turn: row.is_turn,
            origin: row.origin,
            defer_until_child: row.defer_until_child,
            clear_error: false,
            route_slots: RouteSlots {
                attach: slots.attach,
                metadata: slots.metadata,
                tags: slots.tags,
                project: slots.project,
            },
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum StoredEntry {
    Insert {
        row: StoredRow,
        #[serde(default)]
        capture: CaptureVersions,
    },
    Merge {
        row: StoredRow,
        #[serde(default)]
        capture: CaptureVersions,
    },
    Barrier {
        id: String,
        #[serde(default)]
        finalize: bool,
    },
    BackfillComplete,
}

#[derive(Serialize, Deserialize)]
struct Versioned<T> {
    v: u32,
    #[serde(flatten)]
    entry: T,
}

pub(crate) fn encode_span_entry(entry: &SpanEntry) -> Vec<u8> {
    let stored = match entry {
        SpanEntry::Op(op) if op.merge => StoredEntry::Merge {
            row: (&op.row).into(),
            capture: op.capture.clone(),
        },
        SpanEntry::Op(op) => StoredEntry::Insert {
            row: (&op.row).into(),
            capture: op.capture.clone(),
        },
        SpanEntry::Barrier { id, finalize } => StoredEntry::Barrier {
            id: id.clone(),
            finalize: *finalize,
        },
        SpanEntry::BackfillComplete => StoredEntry::BackfillComplete,
    };
    serde_json::to_vec(&Versioned {
        v: SPAN_ENTRY_VERSION,
        entry: stored,
    })
    .expect("span entries serialize")
}

pub(crate) fn decode_span_entry(bytes: &[u8]) -> anyhow::Result<SpanEntry> {
    let versioned: Versioned<StoredEntry> = serde_json::from_slice(bytes)?;
    anyhow::ensure!(
        versioned.v == SPAN_ENTRY_VERSION,
        "unsupported span entry version {} (supported: {SPAN_ENTRY_VERSION})",
        versioned.v
    );
    Ok(match versioned.entry {
        StoredEntry::Insert { row, capture } => SpanEntry::Op(Box::new(JournalSpanOp {
            merge: false,
            row: row.into(),
            capture,
        })),
        StoredEntry::Merge { row, capture } => SpanEntry::Op(Box::new(JournalSpanOp {
            merge: true,
            row: row.into(),
            capture,
        })),
        StoredEntry::Barrier { id, finalize } => SpanEntry::Barrier { id, finalize },
        StoredEntry::BackfillComplete => SpanEntry::BackfillComplete,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_every_span_row_field() {
        let row = SpanRow {
            span_id: "s".into(),
            root_span_id: "r".into(),
            parent_span_ids: vec!["p".into()],
            name: "n".into(),
            span_type: SpanType::Llm,
            start_ms: Some(1),
            end_ms: Some(2),
            input: Some(serde_json::json!({"a": 1})),
            output: Some(serde_json::json!("o")),
            metadata: Some(serde_json::json!({"m": true})),
            metrics: Some(serde_json::json!({"tokens": 3})),
            error: Some("e".into()),
            late_merge_key: Some("late".into()),
            turn_span_id: Some("turn".into()),
            tags: Some(vec!["t".into()]),
            is_turn: true,
            origin: Some(OriginSnapshot {
                plugin_version: Some("p".into()),
                bt_version: "b".into(),
                source_version: None,
            }),
            defer_until_child: true,
            clear_error: false,
            route_slots: RouteSlots::ROOT,
        };
        let entry = SpanEntry::Op(Box::new(JournalSpanOp {
            merge: true,
            row: row.clone(),
            capture: CaptureVersions {
                plugin_version: Some("1".into()),
                source_version: Some("2".into()),
            },
        }));
        let SpanEntry::Op(decoded) = decode_span_entry(&encode_span_entry(&entry)).unwrap() else {
            panic!("expected an op");
        };
        assert!(decoded.merge);
        assert_eq!(decoded.capture.source_version.as_deref(), Some("2"));
        let back = decoded.row;
        assert_eq!(back.late_merge_key, row.late_merge_key);
        assert_eq!(back.turn_span_id, row.turn_span_id);
        assert!(back.is_turn && back.defer_until_child);
        assert_eq!(back.route_slots, RouteSlots::ROOT);
        assert_eq!(
            serde_json::to_value(&back).unwrap(),
            serde_json::to_value(&row).unwrap()
        );
    }

    #[test]
    fn rejects_unknown_versions() {
        assert!(decode_span_entry(br#"{"v":99,"kind":"backfill_complete"}"#).is_err());
    }
}
