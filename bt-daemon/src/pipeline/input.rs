//! Stage input shared by direct capture and recovery. Storage supplies only
//! byte positions; original live payloads arrive from capture.
pub use crate::journal::PipelineControl;
use crate::wire::Envelope;
use parking_lot::Mutex;
use std::{collections::VecDeque, sync::Arc};

const RECORD_BUDGET: usize = 64;
const BYTE_BUDGET: u64 = 8 * 1024 * 1024;

/// Durable byte range returned by the journal append.
#[derive(Clone)]
pub(crate) struct SourcePosition {
    pub before: u64,
    pub through: u64,
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
        // Journal references can be tiny while the live payload is huge.
        // Charge retained values, without serializing/copying their strings.
        fn payload_bytes(value: &serde_json::Value) -> u64 {
            std::mem::size_of::<serde_json::Value>() as u64
                + match value {
                    serde_json::Value::String(text) => text.len() as u64,
                    serde_json::Value::Array(array) => array.iter().map(payload_bytes).sum(),
                    serde_json::Value::Object(object) => object
                        .iter()
                        .map(|(key, value)| key.len() as u64 + 64 + payload_bytes(value))
                        .sum(),
                    _ => 0,
                }
        }
        let stored = self.position.through.saturating_sub(self.position.before);
        match &self.event {
            SourceEvent::Event(event) => stored.max(payload_bytes(&event.payload)),
            _ => stored,
        }
    }
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

    #[test]
    fn oversized_events_flow_alone_and_eviction_exposes_a_recovery_gap() {
        let input = SourceInput::default();
        let event: Envelope = serde_json::from_value(json!({"source":"debug","session_id":"direct","event":"native","ts_ms":1,"payload":{"history":"x".repeat(9 * 1024 * 1024)}})).unwrap();
        // References have a small disk footprint but retain full live data.
        let stride = 128;
        let position = |before| SourcePosition {
            before,
            through: before + stride,
        };
        input.push_event(event.clone(), position(0));
        assert!(input.take(0, 0).is_none());
        let SourceEvent::Event(actual) = input.take(0, stride).unwrap().event else {
            panic!("expected original event");
        };
        assert_eq!(actual.payload, event.payload);
        input.push_event(event.clone(), position(stride));
        input.push_event(event, position(2 * stride));
        assert!(
            input.take(stride, 3 * stride).is_none(),
            "eviction requires recovery of the gap"
        );
        assert!(input.take(2 * stride, 3 * stride).is_some());
        assert!(input.is_empty());
    }
}
