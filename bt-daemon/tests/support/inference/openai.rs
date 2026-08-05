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
pub struct OpenAiRequest {
    pub body: Value,
}

impl OpenAiRequest {
    pub fn model(&self) -> Option<&str> {
        self.body["model"].as_str()
    }

    pub fn contains_text(&self, text: &str) -> bool {
        self.body.to_string().contains(text)
    }

    pub fn has_function_output(&self, call_id: &str) -> bool {
        self.body["input"].as_array().is_some_and(|items| {
            items
                .iter()
                .any(|item| item["type"] == "function_call_output" && item["call_id"] == call_id)
        })
    }

    pub fn tool_names(&self) -> Vec<&str> {
        self.body["tools"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|tool| tool["name"].as_str())
            .collect()
    }
}

#[derive(Debug, Clone)]
pub enum OpenAiTurn {
    Text {
        text: String,
        input_tokens: u64,
        output_tokens: u64,
    },
    ToolCall {
        call_id: String,
        name: String,
        arguments: Value,
        input_tokens: u64,
        output_tokens: u64,
    },
    Events(Vec<Value>),
}

impl OpenAiTurn {
    pub fn text(text: impl Into<String>) -> Self {
        Self::Text {
            text: text.into(),
            input_tokens: 10,
            output_tokens: 5,
        }
    }

    pub fn tool_call(
        call_id: impl Into<String>,
        name: impl Into<String>,
        arguments: Value,
    ) -> Self {
        Self::ToolCall {
            call_id: call_id.into(),
            name: name.into(),
            arguments,
            input_tokens: 10,
            output_tokens: 5,
        }
    }

    fn events(self, response_index: usize) -> Vec<Value> {
        let response_id = format!("resp_mock_{response_index}");
        let created = json!({
            "type": "response.created",
            "response": {"id": response_id}
        });
        match self {
            Self::Text {
                text,
                input_tokens,
                output_tokens,
            } => vec![
                created,
                json!({
                    "type": "response.output_item.added",
                    "output_index": 0,
                    "item": {
                        "type": "message",
                        "role": "assistant",
                        "id": format!("msg_mock_{response_index}"),
                        "status": "in_progress",
                        "content": []
                    }
                }),
                json!({
                    "type": "response.content_part.added",
                    "item_id": format!("msg_mock_{response_index}"),
                    "output_index": 0,
                    "content_index": 0,
                    "part": {"type": "output_text", "text": "", "annotations": []}
                }),
                json!({
                    "type": "response.output_text.delta",
                        "item_id": format!("msg_mock_{response_index}"),
                        "output_index": 0,
                        "content_index": 0,
                    "delta": text.clone()
                }),
                json!({
                    "type": "response.output_text.done",
                    "item_id": format!("msg_mock_{response_index}"),
                    "output_index": 0,
                    "content_index": 0,
                    "text": text.clone()
                }),
                json!({
                    "type": "response.content_part.done",
                    "item_id": format!("msg_mock_{response_index}"),
                    "output_index": 0,
                    "content_index": 0,
                    "part": {"type": "output_text", "text": text.clone(), "annotations": []}
                }),
                json!({
                    "type": "response.output_item.done",
                    "item": {
                        "type": "message",
                        "role": "assistant",
                        "id": format!("msg_mock_{response_index}"),
                        "content": [{"type": "output_text", "text": text}]
                    }
                }),
                completed(&response_id, input_tokens, output_tokens),
            ],
            Self::ToolCall {
                call_id,
                name,
                arguments,
                input_tokens,
                output_tokens,
            } => vec![
                created,
                json!({
                    "type": "response.output_item.done",
                    "item": {
                        "type": "function_call",
                        "call_id": call_id,
                        "name": name,
                        "arguments": arguments.to_string()
                    }
                }),
                completed(&response_id, input_tokens, output_tokens),
            ],
            Self::Events(events) => events,
        }
    }
}

fn completed(id: &str, input_tokens: u64, output_tokens: u64) -> Value {
    json!({
        "type": "response.completed",
        "response": {
            "id": id,
            "usage": {
                "input_tokens": input_tokens,
                "input_tokens_details": {"cached_tokens": 0},
                "output_tokens": output_tokens,
                "output_tokens_details": {"reasoning_tokens": 0},
                "total_tokens": input_tokens + output_tokens
            }
        }
    })
}

type Handler =
    dyn Fn(RequestContext, OpenAiRequest) -> MockReply<OpenAiTurn> + Send + Sync + 'static;

struct MockState {
    handler: Arc<Handler>,
    requests: Mutex<Vec<OpenAiRequest>>,
    next_index: AtomicUsize,
}

pub struct OpenAiMock {
    state: Arc<MockState>,
}

impl OpenAiMock {
    pub fn new<H>(handler: H) -> Self
    where
        H: Fn(RequestContext, OpenAiRequest) -> MockReply<OpenAiTurn> + Send + Sync + 'static,
    {
        let state = Arc::new(MockState {
            handler: Arc::new(handler),
            requests: Mutex::new(Vec::new()),
            next_index: AtomicUsize::new(0),
        });
        Self { state }
    }

    pub fn router(&self) -> Router {
        Router::new()
            .route("/v1/models", get(models))
            .route("/v1/responses", post(responses))
            .route("/v1/chat/completions", post(chat_completions))
            .route("/backend-api/plugins/featured", get(featured_plugins))
            .with_state(Arc::clone(&self.state))
    }

    pub fn requests(&self) -> Vec<OpenAiRequest> {
        self.state.requests.lock().expect("request lock").clone()
    }
}

async fn models() -> axum::Json<Value> {
    axum::Json(json!({
        "object": "list",
        "data": [{"id": "mock-model", "object": "model", "owned_by": "mock"}]
    }))
}

async fn featured_plugins() -> axum::Json<Value> {
    axum::Json(json!([]))
}

async fn responses(
    State(state): State<Arc<MockState>>,
    headers: HeaderMap,
    body: Bytes,
) -> axum::response::Response {
    let body = match decode_json_body(&headers, &body) {
        Ok(body) => body,
        Err(error) => return json_response(StatusCode::BAD_REQUEST, json!({"error": error})),
    };
    let request = OpenAiRequest { body };
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

async fn chat_completions(
    State(state): State<Arc<MockState>>,
    headers: HeaderMap,
    body: Bytes,
) -> axum::response::Response {
    let body = match decode_json_body(&headers, &body) {
        Ok(body) => body,
        Err(error) => return json_response(StatusCode::BAD_REQUEST, json!({"error": error})),
    };
    let request = OpenAiRequest { body };
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
        MockReply::Response(OpenAiTurn::Text {
            text,
            input_tokens,
            output_tokens,
        }) => {
            let id = format!("chatcmpl_mock_{index}");
            let chunks = [
                json!({
                    "id": id,
                    "object": "chat.completion.chunk",
                    "choices": [{"index": 0, "delta": {"role": "assistant", "content": text}}]
                }),
                json!({
                    "id": id,
                    "object": "chat.completion.chunk",
                    "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
                    "usage": {
                        "prompt_tokens": input_tokens,
                        "completion_tokens": output_tokens,
                        "total_tokens": input_tokens + output_tokens
                    }
                }),
            ];
            let mut body = chunks
                .iter()
                .map(|chunk| format!("data: {chunk}\n\n"))
                .collect::<String>();
            body.push_str("data: [DONE]\n\n");
            raw_response(StatusCode::OK, "text/event-stream", body.into_bytes())
        }
        MockReply::Response(OpenAiTurn::ToolCall {
            call_id,
            name,
            arguments,
            ..
        }) => {
            let body = json!({
                "id": format!("chatcmpl_mock_{index}"),
                "object": "chat.completion",
                "choices": [{
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "tool_calls": [{
                            "id": call_id,
                            "type": "function",
                            "function": {"name": name, "arguments": arguments.to_string()}
                        }]
                    },
                    "finish_reason": "tool_calls"
                }]
            });
            json_response(StatusCode::OK, body)
        }
        MockReply::Response(OpenAiTurn::Events(_)) => json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error": "raw response events are unsupported on chat completions"}),
        ),
        MockReply::HttpError { status, body } => json_response(status, body),
        MockReply::Raw {
            status,
            content_type,
            body,
        } => raw_response(status, content_type, body),
    }
}
