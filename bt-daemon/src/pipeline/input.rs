//! Stage input shared by direct capture and recovery. Storage supplies only
//! record IDs; original live payloads arrive from capture.
use crate::wire::Envelope;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::{collections::VecDeque, sync::Arc};

const RECORD_BUDGET: usize = 64;
const BYTE_BUDGET: u64 = 8 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlKind {
    Checkpoint,
    Finalize,
    Retire,
    /// Discard translator state: the native source was rewritten.
    Reset,
}

/// An ordered lifecycle instruction stored among a session's events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PipelineControl {
    pub kind: ControlKind,
    pub bounds: std::collections::HashMap<String, u64>,
}

/// Durable ID range returned by the event insert.
#[derive(Clone)]
pub(crate) struct SourcePosition {
    pub before: u64,
    pub through: u64,
}

pub(crate) enum SourceEvent {
    Event(Box<Envelope>),
    Control(PipelineControl),
    LegacyDeliveryCheckpoint {
        route: Box<crate::wire::SessionRoute>,
        through: u64,
    },
}

pub(crate) struct SourceRecord {
    pub event: SourceEvent,
    pub position: SourcePosition,
}

impl SourceRecord {
    fn bytes(&self) -> u64 {
        self.position.through.saturating_sub(self.position.before)
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
        let stride = 9 * 1024 * 1024;
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
