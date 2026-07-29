use super::server::InferenceServer;
use super::{decode_json_body, json_response, raw_response, sse, MockReply, RequestContext};
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::Router;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct AnthropicRequest {
    pub body: Value,
}

impl AnthropicRequest {
    pub fn model(&self) -> Option<&str> {
        self.body["model"].as_str()
    }

    pub fn contains_text(&self, text: &str) -> bool {
        self.body.to_string().contains(text)
    }

    pub fn has_tool_result(&self, tool_use_id: &str) -> bool {
        self.body["messages"].as_array().is_some_and(|messages| {
            messages.iter().any(|message| {
                message["content"].as_array().is_some_and(|blocks| {
                    blocks.iter().any(|block| {
                        block["type"] == "tool_result" && block["tool_use_id"] == tool_use_id
                    })
                })
            })
        })
    }
}

#[derive(Debug, Clone)]
pub enum AnthropicTurn {
    Text {
        text: String,
        input_tokens: u64,
        output_tokens: u64,
    },
    ToolUse {
        tool_use_id: String,
        name: String,
        input: Value,
        input_tokens: u64,
        output_tokens: u64,
    },
    Events(Vec<Value>),
}

impl AnthropicTurn {
    pub fn text(text: impl Into<String>) -> Self {
        Self::Text {
            text: text.into(),
            input_tokens: 10,
            output_tokens: 5,
        }
    }

    pub fn tool_use(tool_use_id: impl Into<String>, name: impl Into<String>, input: Value) -> Self {
        Self::ToolUse {
            tool_use_id: tool_use_id.into(),
            name: name.into(),
            input,
            input_tokens: 10,
            output_tokens: 5,
        }
    }

    fn events(self, response_index: usize) -> Vec<Value> {
        let message_id = format!("msg_mock_{response_index}");
        match self {
            Self::Text {
                text,
                input_tokens,
                output_tokens,
            } => {
                let mut events = message_start(&message_id, input_tokens);
                events.extend([
                    json!({
                        "type": "content_block_start",
                        "index": 0,
                        "content_block": {"type": "text", "text": ""}
                    }),
                    json!({
                        "type": "content_block_delta",
                        "index": 0,
                        "delta": {"type": "text_delta", "text": text}
                    }),
                    json!({"type": "content_block_stop", "index": 0}),
                    message_delta("end_turn", output_tokens),
                    json!({"type": "message_stop"}),
                ]);
                events
            }
            Self::ToolUse {
                tool_use_id,
                name,
                input,
                input_tokens,
                output_tokens,
            } => {
                let mut events = message_start(&message_id, input_tokens);
                events.extend([
                    json!({
                        "type": "content_block_start",
                        "index": 0,
                        "content_block": {
                            "type": "tool_use",
                            "id": tool_use_id,
                            "name": name,
                            "input": {}
                        }
                    }),
                    json!({
                        "type": "content_block_delta",
                        "index": 0,
                        "delta": {
                            "type": "input_json_delta",
                            "partial_json": input.to_string()
                        }
                    }),
                    json!({"type": "content_block_stop", "index": 0}),
                    message_delta("tool_use", output_tokens),
                    json!({"type": "message_stop"}),
                ]);
                events
            }
            Self::Events(events) => events,
        }
    }
}

fn message_start(id: &str, input_tokens: u64) -> Vec<Value> {
    vec![json!({
        "type": "message_start",
        "message": {
            "id": id,
            "type": "message",
            "role": "assistant",
            "content": [],
            "model": "mock-model",
            "stop_reason": null,
            "stop_sequence": null,
            "usage": {
                "input_tokens": input_tokens,
                "cache_creation_input_tokens": 0,
                "cache_read_input_tokens": 0,
                "output_tokens": 1
            }
        }
    })]
}

fn message_delta(stop_reason: &str, output_tokens: u64) -> Value {
    json!({
        "type": "message_delta",
        "delta": {"stop_reason": stop_reason, "stop_sequence": null},
        "usage": {"output_tokens": output_tokens}
    })
}

type Handler =
    dyn Fn(RequestContext, AnthropicRequest) -> MockReply<AnthropicTurn> + Send + Sync + 'static;

struct MockState {
    handler: Arc<Handler>,
    requests: Mutex<Vec<AnthropicRequest>>,
    next_index: AtomicUsize,
}

pub struct AnthropicMock {
    server: InferenceServer,
    state: Arc<MockState>,
}

impl AnthropicMock {
    pub async fn start<H>(handler: H) -> Self
    where
        H: Fn(RequestContext, AnthropicRequest) -> MockReply<AnthropicTurn> + Send + Sync + 'static,
    {
        let state = Arc::new(MockState {
            handler: Arc::new(handler),
            requests: Mutex::new(Vec::new()),
            next_index: AtomicUsize::new(0),
        });
        let router = Router::new()
            .route("/v1/models", get(models))
            .route("/v1/messages", post(messages))
            .route("/v1/messages/count_tokens", post(count_tokens))
            .with_state(Arc::clone(&state));
        Self {
            server: InferenceServer::start(router).await,
            state,
        }
    }

    pub fn base_url(&self) -> &str {
        self.server.uri()
    }

    pub fn requests(&self) -> Vec<AnthropicRequest> {
        self.state.requests.lock().expect("request lock").clone()
    }

    pub async fn shutdown(self) {
        self.server.shutdown().await;
    }
}

async fn models() -> axum::Json<Value> {
    axum::Json(json!({
        "data": [{
            "type": "model",
            "id": "mock-model",
            "display_name": "Mock model",
            "created_at": "2026-01-01T00:00:00Z"
        }],
        "has_more": false,
        "first_id": "mock-model",
        "last_id": "mock-model"
    }))
}

async fn count_tokens() -> axum::Json<Value> {
    axum::Json(json!({"input_tokens": 10}))
}

async fn messages(
    State(state): State<Arc<MockState>>,
    headers: HeaderMap,
    body: Bytes,
) -> axum::response::Response {
    let body = match decode_json_body(&headers, &body) {
        Ok(body) => body,
        Err(error) => return json_response(StatusCode::BAD_REQUEST, json!({"error": error})),
    };
    let request = AnthropicRequest { body };
    state
        .requests
        .lock()
        .expect("request lock")
        .push(request.clone());
    let index = state.next_index.fetch_add(1, Ordering::SeqCst);
    match (state.handler)(
        RequestContext {
            request_index: index,
        },
        request,
    ) {
        MockReply::Response(turn) => raw_response(
            StatusCode::OK,
            "text/event-stream",
            sse(&turn.events(index)),
        ),
        MockReply::HttpError { status, body } => json_response(status, body),
        MockReply::Raw {
            status,
            content_type,
            body,
        } => raw_response(status, content_type, body),
    }
}
