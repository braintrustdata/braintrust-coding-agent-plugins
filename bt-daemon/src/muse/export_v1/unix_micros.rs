use serde::Deserialize;

/// Export timestamps are microseconds; daemon envelopes use milliseconds.
#[derive(Clone, Copy, Deserialize)]
#[serde(transparent)]
pub(crate) struct UnixMicros(i64);

impl UnixMicros {
    pub fn millis(self) -> i64 {
        self.0 / 1_000
    }
}
