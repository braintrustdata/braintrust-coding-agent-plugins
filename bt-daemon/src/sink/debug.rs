//! Debug sink: appends each emitted [`SpanOp`] as one NDJSON line to
//! `<data_dir>/spans/<session_id>.ndjson` (a digest for overlong ids). Lets
//! tests assert on exactly what the pipeline produced without touching
//! Braintrust.

use super::{Sink, SinkFactory};
use crate::ids::session_storage_id;
use crate::journal::{sanitize, MAX_FILE_NAME_BYTES};
use crate::translate::SpanOp;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;

pub struct DebugSinkFactory {
    pub dir: PathBuf,
}

impl SinkFactory for DebugSinkFactory {
    fn create(&self, session_id: &str, source: &str) -> anyhow::Result<Box<dyn Sink>> {
        std::fs::create_dir_all(&self.dir)?;
        let name = format!("{}.ndjson", sanitize(session_id));
        // Fall back to a stable digest when the id cannot be one file name.
        let name = if name.len() > MAX_FILE_NAME_BYTES {
            format!("{}.ndjson", session_storage_id(source, session_id))
        } else {
            name
        };
        let path = self.dir.join(name);
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(Box::new(DebugSink {
            writer: BufWriter::new(file),
            written: 0,
        }))
    }
}

struct DebugSink {
    writer: BufWriter<File>,
    written: u64,
}

#[async_trait::async_trait]
impl Sink for DebugSink {
    async fn emit(&mut self, ops: &[SpanOp]) -> anyhow::Result<u64> {
        for op in ops {
            serde_json::to_writer(&mut self.writer, op)?;
            self.writer.write_all(b"\n")?;
            self.written += 1;
        }
        // Flush per batch so a reader (test) sees rows promptly.
        self.writer.flush()?;
        Ok(ops.len() as u64)
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        self.writer.flush()?;
        Ok(())
    }
}
