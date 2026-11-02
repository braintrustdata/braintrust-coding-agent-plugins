//! Complete durable records awaiting their single source translator. Eviction
//! transfers the recovery obligation to the journal without delaying capture.
use super::JournalRecordEntry;
use parking_lot::Mutex;
use std::{collections::VecDeque, sync::Arc};

const RECORD_BUDGET: usize = 64;
const BYTE_BUDGET: u64 = 8 * 1024 * 1024;

pub(super) struct LiveRecord {
    pub before: u64,
    pub entry: JournalRecordEntry,
    pub json: Option<Arc<str>>,
}

impl LiveRecord {
    fn bytes(&self) -> u64 {
        self.entry
            .decoded_bytes
            .saturating_add(self.json.as_ref().map_or(0, |json| json.len() as u64))
    }
}

#[derive(Default)]
struct Buffer {
    records: VecDeque<LiveRecord>,
    bytes: u64,
}

#[derive(Clone, Default)]
pub(crate) struct LiveRecords(Arc<Mutex<Buffer>>);

impl LiveRecords {
    pub(super) fn push(&self, record: LiveRecord) {
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

    pub(super) fn take(&self, position: u64, through: u64) -> Option<LiveRecord> {
        let mut buffer = self.0.lock();
        // Recovery may already have consumed records whose live copies remained.
        while buffer
            .records
            .front()
            .is_some_and(|record| record.entry.through <= position)
        {
            if let Some(consumed) = buffer.records.pop_front() {
                buffer.bytes -= consumed.bytes();
            }
        }
        let first = buffer.records.front()?;
        if first.before != position || first.entry.through > through {
            return None;
        }
        let record = buffer.records.pop_front()?;
        buffer.bytes -= record.bytes();
        Some(record)
    }
}
