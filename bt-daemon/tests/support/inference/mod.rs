mod anthropic;
mod openai;
mod server;

#[allow(unused_imports)]
pub use anthropic::{AnthropicMock, AnthropicRequest, AnthropicTurn};
#[allow(unused_imports)]
pub use openai::{OpenAiMock, OpenAiRequest, OpenAiTurn};

use axum::http::StatusCode;
use serde_json::Value;

#[derive(Debug, Clone, Copy)]
pub struct RequestContext {
    pub request_index: usize,
}

/// A provider-neutral transport outcome. Protocol response bodies remain
/// provider-specific and are rendered by the OpenAI/Anthropic adapters.
#[derive(Debug, Clone)]
pub enum MockReply<T> {
    Response(T),
    HttpError {
        status: StatusCode,
        body: Value,
    },
    Raw {
        status: StatusCode,
        content_type: &'static str,
        body: Vec<u8>,
    },
}

impl<T> MockReply<T> {
    pub fn response(value: T) -> Self {
        Self::Response(value)
    }

    pub fn http_error(status: StatusCode, body: Value) -> Self {
        Self::HttpError { status, body }
    }

    pub fn raw_sse(body: impl Into<Vec<u8>>) -> Self {
        Self::Raw {
            status: StatusCode::OK,
            content_type: "text/event-stream",
            body: body.into(),
        }
    }
}

fn decode_json_body(headers: &axum::http::HeaderMap, body: &[u8]) -> Result<Value, String> {
    let decoded = match headers
        .get(axum::http::header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
    {
        Some(value) if value.split(',').any(|part| part.trim() == "zstd") => {
            zstd::stream::decode_all(std::io::Cursor::new(body))
                .map_err(|error| format!("decode zstd request: {error}"))?
        }
        _ => body.to_vec(),
    };
    serde_json::from_slice(&decoded).map_err(|error| format!("decode JSON request: {error}"))
}

fn json_response(status: StatusCode, body: Value) -> axum::response::Response {
    use axum::response::IntoResponse;
    (status, axum::Json(body)).into_response()
}

fn raw_response(
    status: StatusCode,
    content_type: &'static str,
    body: Vec<u8>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    (
        status,
        [(axum::http::header::CONTENT_TYPE, content_type)],
        body,
    )
        .into_response()
}

fn sse(events: &[Value]) -> Vec<u8> {
    use std::fmt::Write;

    let mut body = String::new();
    for event in events {
        let kind = event["type"].as_str().expect("SSE event type");
        writeln!(&mut body, "event: {kind}").expect("write SSE event");
        writeln!(&mut body, "data: {event}\n").expect("write SSE data");
    }
    body.into_bytes()
}
