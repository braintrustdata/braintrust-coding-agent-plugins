mod support;

use axum::http::StatusCode;
use serde_json::json;
use support::inference::{AnthropicMock, AnthropicTurn, MockReply, OpenAiMock, OpenAiTurn};

#[tokio::test]
async fn openai_mock_streams_text_and_captures_requests() {
    let mock = OpenAiMock::start(|context, request| {
        assert_eq!(context.request_index, 0);
        assert_eq!(request.model(), Some("mock-model"));
        MockReply::response(OpenAiTurn::text("deterministic"))
    })
    .await;

    let response = reqwest::Client::new()
        .post(format!("{}/v1/responses", mock.base_url()))
        .json(&json!({"model":"mock-model","input":[],"stream":true}))
        .send()
        .await
        .unwrap();
    let body = response.text().await.unwrap();

    assert!(body.contains("response.output_item.done"));
    assert!(body.contains("deterministic"));
    assert_eq!(mock.requests().len(), 1);
}

#[tokio::test]
async fn openai_mock_injects_retryable_and_malformed_responses() {
    let mock = OpenAiMock::start(|context, _request| match context.request_index {
        0 => MockReply::http_error(
            StatusCode::TOO_MANY_REQUESTS,
            json!({"error":{"type":"rate_limit_error","message":"deterministic limit"}}),
        ),
        _ => MockReply::raw_sse("event: response.output_item.done\ndata: not-json\n\n"),
    })
    .await;
    let client = reqwest::Client::new();

    let limited = client
        .post(format!("{}/v1/responses", mock.base_url()))
        .json(&json!({"model":"mock-model","input":[],"stream":true}))
        .send()
        .await
        .unwrap();
    assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);

    let malformed = client
        .post(format!("{}/v1/responses", mock.base_url()))
        .json(&json!({"model":"mock-model","input":[],"stream":true}))
        .send()
        .await
        .unwrap();
    assert!(malformed.text().await.unwrap().contains("not-json"));
}

#[tokio::test]
async fn anthropic_mock_supports_tool_use_and_http_errors() {
    let mock = AnthropicMock::start(|context, request| match context.request_index {
        0 => {
            assert!(request.contains_text("run a command"));
            MockReply::response(AnthropicTurn::tool_use(
                "toolu_mock",
                "Bash",
                json!({"command":"printf hello"}),
            ))
        }
        _ => MockReply::http_error(
            StatusCode::TOO_MANY_REQUESTS,
            json!({
                "type":"error",
                "error":{"type":"rate_limit_error","message":"deterministic limit"}
            }),
        ),
    })
    .await;

    let client = reqwest::Client::new();
    let first = client
        .post(format!("{}/v1/messages", mock.base_url()))
        .json(&json!({
            "model":"mock-model",
            "messages":[{"role":"user","content":"run a command"}],
            "stream":true
        }))
        .send()
        .await
        .unwrap();
    assert!(first.text().await.unwrap().contains("toolu_mock"));

    let second = client
        .post(format!("{}/v1/messages", mock.base_url()))
        .json(&json!({"model":"mock-model","messages":[],"stream":true}))
        .send()
        .await
        .unwrap();
    assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(mock.requests().len(), 2);
}
