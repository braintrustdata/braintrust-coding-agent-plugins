//! Stage input shared by direct capture and recovery. Storage supplies only
//! position/decoder metadata; original live payloads arrive from capture.
use crate::wire::Envelope;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::{collections::VecDeque, sync::Arc};

const RECORD_BUDGET: usize = 64;
const BYTE_BUDGET: u64 = 8 * 1024 * 1024;

/// Durable input position, returned by the storage side path. Decoder data is
/// used only to checkpoint future recovery, never to construct a live payload.
#[derive(Clone)]
pub(crate) struct SourcePosition {
    pub before: u64,
    pub through: u64,
    pub decoded_bytes: u64,
    pub decoder_base: Option<Arc<str>>,
}

pub(crate) enum SourceEvent {
    Event(Box<Envelope>),
    Control(PipelineControl),
    LegacyDeliveryCheckpoint,
}

pub(crate) struct SourceRecord {
    pub event: SourceEvent,
    pub position: SourcePosition,
}

impl SourceRecord {
    fn bytes(&self) -> u64 {
        self.position.decoded_bytes.saturating_add(
            self.position
                .decoder_base
                .as_ref()
                .map_or(0, |json| json.len() as u64),
        )
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlKind {
    Checkpoint,
    Finalize,
    Retire,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineControl {
    pub kind: ControlKind,
    pub bounds: HashMap<String, u64>,
}

#[derive(Default)]
struct Buffer {
    records: VecDeque<SourceRecord>,
    bytes: u64,
}

#[derive(Clone, Default)]
pub(crate) struct SourceInput(Arc<Mutex<Buffer>>);

impl SourceInput {
    pub(crate) fn push_event(&self, event: Envelope, position: SourcePosition) {
        self.push(SourceRecord {
            event: SourceEvent::Event(Box::new(event)),
            position,
        });
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.0.lock().records.is_empty()
    }
    pub(crate) fn push(&self, record: SourceRecord) {
        let mut buffer = self.0.lock();
        let bytes = record.bytes();
        // An oversized event travels alone, just as it does on the SpanOp feed.
        while !buffer.records.is_empty()
            && (buffer.records.len() >= RECORD_BUDGET
                || buffer.bytes.saturating_add(bytes) > BYTE_BUDGET)
        {
            if let Some(evicted) = buffer.records.pop_front() {
                buffer.bytes -= evicted.bytes();
            }
        }
        buffer.bytes = buffer.bytes.saturating_add(bytes);
        buffer.records.push_back(record);
    }

    pub(crate) fn take(&self, position: u64, through: u64) -> Option<SourceRecord> {
        let mut buffer = self.0.lock();
        // Recovery may already have consumed records whose live copies remained.
        while buffer
            .records
            .front()
            .is_some_and(|record| record.position.through <= position)
        {
            if let Some(consumed) = buffer.records.pop_front() {
                buffer.bytes -= consumed.bytes();
            }
        }
        let first = buffer.records.front()?;
        if first.position.before != position || first.position.through > through {
            return None;
        }
        let record = buffer.records.pop_front()?;
        buffer.bytes -= record.bytes();
        Some(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn event(source: &str, index: usize, bytes: usize) -> Envelope {
        serde_json::from_value(json!({
            "source":source, "session_id":"direct", "event":"native", "ts_ms":index,
            "payload":{"history":"x".repeat(bytes),"index":index}
        }))
        .unwrap()
    }

    fn position(before: u64, event: &Envelope) -> SourcePosition {
        let bytes = serde_json::to_vec(event).unwrap().len() as u64;
        SourcePosition {
            before,
            through: before + bytes,
            decoded_bytes: bytes,
            decoder_base: None,
        }
    }

    #[test]
    fn oversized_events_flow_directly_but_eviction_keeps_the_input_bounded() {
        let input = SourceInput::default();
        let oversized = event("debug", 0, 9 * 1024 * 1024);
        let first = position(0, &oversized);
        let boundary = first.through;
        input.push_event(oversized.clone(), first);
        assert!(input.take(0, boundary - 1).is_none());
        let SourceEvent::Event(actual) = input.take(0, boundary).unwrap().event else {
            panic!("expected original event");
        };
        assert_eq!(actual.payload, oversized.payload);
        input.push_event(oversized.clone(), position(boundary, &oversized));
        let gap_end = position(boundary, &oversized).through;
        let next = event("debug", 1, 3 * 1024 * 1024);
        let next_position = position(gap_end, &next);
        let through = next_position.through;
        input.push_event(next, next_position);
        assert!(
            input.take(boundary, through).is_none(),
            "evicted input requires recovery of the gap"
        );
        assert!(input.take(gap_end, through).is_some());
        assert!(input.is_empty());
    }
}
