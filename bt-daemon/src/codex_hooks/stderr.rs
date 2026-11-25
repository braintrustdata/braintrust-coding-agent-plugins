//! Keep the tail of Codex's stderr so probe failures can explain themselves,
//! e.g. an older Codex rejecting `--no-daemon` and exiting before replying.

use anyhow::anyhow;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::task::JoinHandle;

const TAIL_BYTES: usize = 4 * 1024;
const DRAIN_TIMEOUT: Duration = Duration::from_millis(500);

pub(super) struct StderrTail(Option<JoinHandle<Vec<u8>>>);

impl StderrTail {
    /// Drain the pipe continuously so Codex never blocks on a full stderr
    /// buffer, retaining only the last `TAIL_BYTES`.
    pub(super) fn capture(child: &mut tokio::process::Child) -> Self {
        Self(child.stderr.take().map(|stderr| tokio::spawn(tail(stderr))))
    }

    /// Call after the child is killed so the pipe reaches EOF. The bounded
    /// wait covers descendants that may still hold the pipe open.
    pub(super) async fn attach<T>(self, result: anyhow::Result<T>) -> anyhow::Result<T> {
        let Err(error) = result else {
            if let Some(task) = self.0 {
                task.abort();
            }
            return result;
        };
        let Some(mut task) = self.0 else {
            return Err(error);
        };
        let bytes = match tokio::time::timeout(DRAIN_TIMEOUT, &mut task).await {
            Ok(Ok(bytes)) => bytes,
            _ => {
                task.abort();
                return Err(error);
            }
        };
        Err(with_stderr(error, &bytes))
    }
}

async fn tail(mut stderr: impl AsyncRead + Unpin) -> Vec<u8> {
    let mut kept = Vec::new();
    let mut chunk = [0; 1024];
    while let Ok(read @ 1..) = stderr.read(&mut chunk).await {
        kept.extend_from_slice(&chunk[..read]);
        if kept.len() > TAIL_BYTES {
            kept.drain(..kept.len() - TAIL_BYTES);
        }
    }
    kept
}

fn with_stderr(error: anyhow::Error, bytes: &[u8]) -> anyhow::Error {
    let text = String::from_utf8_lossy(bytes);
    let text = text.trim();
    if text.is_empty() {
        return error;
    }
    anyhow!("{error:#}\nCodex stderr:\n{text}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn tail_keeps_only_the_last_bytes() {
        let input = [b"x".repeat(TAIL_BYTES * 3), b"end".to_vec()].concat();
        let kept = tail(input.as_slice()).await;
        assert_eq!(kept.len(), TAIL_BYTES);
        assert!(kept.ends_with(b"end"));
    }

    #[test]
    fn stderr_is_appended_only_when_present() {
        let error = with_stderr(
            anyhow!("Codex closed the probe stream"),
            b"error: unexpected argument '--no-daemon' found\n",
        );
        assert_eq!(
            error.to_string(),
            "Codex closed the probe stream\nCodex stderr:\nerror: unexpected argument '--no-daemon' found"
        );
        let error = with_stderr(anyhow!("Codex closed the probe stream"), b" \n");
        assert_eq!(error.to_string(), "Codex closed the probe stream");
    }
}
