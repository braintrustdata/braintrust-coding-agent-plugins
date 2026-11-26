use std::ffi::OsString;

use serde::Deserialize;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[derive(Deserialize)]
struct ResultRecord {
    #[serde(rename = "type")]
    kind: Option<RecordKind>,
    subtype: Option<ResultSubtype>,
    session_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum RecordKind {
    Result,
    #[serde(other)]
    Other,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum ResultSubtype {
    ErrorMaxBudgetUsd,
    #[serde(other)]
    Other,
}

pub(in crate::managed_run) fn observes_budget_result(args: &[OsString]) -> bool {
    let mut budget = false;
    let mut format = None;
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        match arg.to_str() {
            Some("--") => break,
            Some("--max-budget-usd") => budget = true,
            Some("--output-format") => format = args.next().and_then(|value| value.to_str()),
            Some(arg) => {
                if arg.starts_with("--max-budget-usd=") {
                    budget = true;
                }
                if let Some(value) = arg.strip_prefix("--output-format=") {
                    format = Some(value);
                }
            }
            None => {}
        }
    }
    budget && matches!(format, Some("json" | "stream-json"))
}

/// Mirror the child's original bytes and retain only the session ID from a
/// budget-limited result. Claude emits one JSON record per line in both formats.
pub(in crate::managed_run) async fn observe_budget_result(
    mut child_stdout: impl AsyncRead + Unpin,
    mut stdout: impl AsyncWrite + Unpin,
) -> std::io::Result<Option<String>> {
    // Ignore unusually large individual records without buffering unbounded stdout.
    const MAX_RECORD_BYTES: usize = 8 * 1024 * 1024;
    let mut bytes = [0; 8192];
    let mut record = Some(Vec::new());
    let mut budget_session = None;
    loop {
        let read = child_stdout.read(&mut bytes).await?;
        if read == 0 {
            if let Some(record) = record {
                observe_record(&record, &mut budget_session);
            }
            stdout.flush().await?;
            return Ok(budget_session);
        }
        let chunk = &bytes[..read];
        stdout.write_all(chunk).await?;
        for part in chunk.split_inclusive(|byte| *byte == b'\n') {
            if let Some(line) = &mut record {
                if line.len() + part.len() <= MAX_RECORD_BYTES {
                    line.extend_from_slice(part);
                } else {
                    record = None;
                }
            }
            if part.ends_with(b"\n") {
                if let Some(line) = &mut record {
                    observe_record(line, &mut budget_session);
                    line.clear();
                } else {
                    record = Some(Vec::new());
                }
            }
        }
    }
}

fn observe_record(record: &[u8], budget_session: &mut Option<String>) {
    if let Ok(ResultRecord {
        kind: Some(RecordKind::Result),
        subtype,
        session_id,
    }) = serde_json::from_slice(record)
    {
        *budget_session = match subtype {
            Some(ResultSubtype::ErrorMaxBudgetUsd) => session_id.filter(|id| !id.is_empty()),
            _ => None,
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn mirrors_stream_json_and_only_accepts_native_budget_results() {
        let other_records = concat!(
            "{\"type\":\"system\",\"subtype\":\"error_max_budget_usd\",\"session_id\":\"wrong\"}\n",
            "{\"type\":\"result\",\"subtype\":\"error_max_turns\",\"session_id\":\"wrong\"}\n"
        );
        let budget_record =
            r#"{"type":"result","subtype":"error_max_budget_usd","session_id":"correct"}"#;
        let input = format!("{other_records}{budget_record}");
        let mut output = Vec::new();
        assert_eq!(
            observe_budget_result(input.as_bytes(), &mut output)
                .await
                .unwrap()
                .as_deref(),
            Some("correct")
        );
        assert_eq!(output, input.as_bytes());
        assert!(
            observe_budget_result(other_records.as_bytes(), tokio::io::sink())
                .await
                .unwrap()
                .is_none()
        );
    }
}
