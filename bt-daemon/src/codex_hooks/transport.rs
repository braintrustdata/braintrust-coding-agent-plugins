use super::response::Response;
use anyhow::Context;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};

pub(super) async fn request<T: DeserializeOwned>(
    stdin: &mut (impl AsyncWrite + Unpin),
    stdout: &mut (impl AsyncBufRead + Unpin),
    id: u64,
    method: &str,
    params: Value,
) -> anyhow::Result<T> {
    let mut encoded = serde_json::to_vec(&json!({"id":id,"method":method,"params":params}))?;
    encoded.push(b'\n');
    stdin.write_all(&encoded).await?;
    stdin.flush().await?;
    loop {
        let mut line = String::new();
        anyhow::ensure!(
            stdout.read_line(&mut line).await? != 0,
            "Codex closed the probe stream"
        );
        let response: Response = serde_json::from_str(&line).context("invalid Codex response")?;
        if !response.matches_request(id) {
            continue;
        }
        return response.into_result(method);
    }
}
