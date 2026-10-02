//! Debug sink: appends each emitted [`SpanOp`] as one NDJSON line to
//! `<data_dir>/spans/<session_id>.ndjson` (a digest for overlong ids). Lets
//! tests assert on exactly what the pipeline produced without touching
//! Braintrust.

use super::{Sink, SinkFactory};
use crate::translate::SpanOp;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;

pub struct DebugSinkFactory {
    pub dir: PathBuf,
}

impl SinkFactory for DebugSinkFactory {
    fn create(
        &self,
        session_id: &str,
        source: &str,
        _plugin_version: Option<&str>,
    ) -> anyhow::Result<Box<dyn Sink>> {
        std::fs::create_dir_all(&self.dir)?;
        let mut name = format!("{}.ndjson", sanitize(session_id));
        // Fall back to a stable digest when the id cannot be one file name.
        if name.len() > 255 {
            name = format!(
                "{}.ndjson",
                crate::ids::session_storage_id(source, session_id)
            );
        }
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

/// Keep session ids filesystem-safe for the per-session file name.
fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}
