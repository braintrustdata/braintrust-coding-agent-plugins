use anyhow::{anyhow, Context};
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::{json, Value};

#[derive(Deserialize)]
pub(super) struct Response {
    id: Option<Value>,
    error: Option<RpcError>,
    #[serde(
        default,
        deserialize_with = "crate::agents::schema::deserialize_present"
    )]
    result: Option<Box<serde_json::value::RawValue>>,
}
#[derive(Deserialize)]
struct RpcError {
    message: Option<String>,
}

impl Response {
    pub(super) fn matches_request(&self, id: u64) -> bool {
        self.id == Some(json!(id))
    }

    pub(super) fn into_result<T: DeserializeOwned>(self, method: &str) -> anyhow::Result<T> {
        if let Some(error) = self.error {
            return Err(anyhow!(
                "{method}: {}",
                error.message.as_deref().unwrap_or("request failed")
            ));
        }
        let result = self.result.context("Codex response has no result")?;
        serde_json::from_str(result.get()).context("invalid Codex result")
    }
}
