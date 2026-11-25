#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "Test fixtures and assertions fail immediately on unexpected values."
)]
//! Exercise native agent translation through durable attachment capture and upload.
use crate::collections::SessionStorage;
use crate::{
    sink::{BraintrustSinkConfig, BraintrustSinkFactory, SinkFactory},
    translate::{Registry, SessionCtx},
    wire::{Envelope, SessionConfig},
};
use serde_json::{json, Value};
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn event(source: &str, name: &str, ts: i64, payload: Value) -> Envelope {
    serde_json::from_value(json!({"source":source,"session_id":"attachments","event":name,"ts_ms":ts,"payload":payload})).unwrap()
}
fn reference(value: &Value, mime: &str) -> bool {
    if value["type"] == "braintrust_attachment" && value["content_type"] == mime {
        return true;
    }
    match value {
        Value::Array(items) => items.iter().any(|v| reference(v, mime)),
        Value::Object(fields) => fields.values().any(|v| reference(v, mime)),
        _ => false,
    }
}
async fn upload(source: &str, events: Vec<Envelope>) -> Vec<Value> {
    let server = MockServer::start().await;
    let storage = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/version"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/project/register"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"project":{"id":"project"}})))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/logs3"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&server)
        .await;
    let storage_url = storage.uri();
    Mock::given(method("POST"))
        .and(path("/attachment"))
        .respond_with(move |request: &wiremock::Request| {
            let metadata: Value = serde_json::from_slice(&request.body).unwrap();
            ResponseTemplate::new(200).set_body_json(json!({"signedUrl":format!("{storage_url}/bytes/{}",metadata["key"].as_str().unwrap()),"headers":{}}))
        })
        .mount(&server).await;
    Mock::given(method("POST"))
        .and(path("/attachment/status"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .and(path_regex("^/bytes/"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&storage)
        .await;
    let tmp = tempfile::tempdir().unwrap();
    let session = SessionStorage::at(&tmp.path().join("attachments.sqlite"))
        .await
        .unwrap();
    session.register(&Default::default()).await.unwrap();
    let mut translator = Registry::default_agents().create(source, "attachments");
    let ctx = SessionCtx {
        session_id: "attachments".into(),
        config: None,
    };
    for (i, event) in events.iter().enumerate() {
        let input = i as u64 + 1;
        let mut ops = translator.handle(event, &ctx).unwrap();
        while let Some(more) = translator.drain_pending(&ctx).unwrap() {
            ops.extend(more);
        }
        session.translated(input, ops).await.unwrap();
    }
    let input = events.len() as u64 + 1;
    let mut ops = translator.finalize(&ctx).unwrap();
    while let Some(more) = translator.drain_pending(&ctx).unwrap() {
        ops.extend(more);
    }
    session.translated(input, ops).await.unwrap();
    let mut sink = BraintrustSinkFactory::new(BraintrustSinkConfig::default())
        .create("attachments", source)
        .unwrap();
    let config: SessionConfig = serde_json::from_value(json!({"auth":{"token":"test-key","org_id":"org","api_url":server.uri(),"app_url":server.uri()},"destination":{"type":"project_logs","project_name":"attachments"}})).unwrap();
    sink.configure(&config);
    for op in session.stored().await {
        sink.emit(&[op]).await.unwrap();
    }
    sink.flush().await.unwrap();
    let uploaded = storage.received_requests().await.unwrap();
    assert!(!uploaded.is_empty(), "{source} uploaded no attachments");
    let requests = server.received_requests().await.unwrap();
    for request in &uploaded {
        let key = request.url.path().rsplit('/').next().unwrap();
        let metadata = requests
            .iter()
            .filter(|request| request.url.path() == "/attachment")
            .map(|request| serde_json::from_slice::<Value>(&request.body).unwrap())
            .find(|metadata| metadata["key"] == key)
            .unwrap();
        let expected: &[u8] = match metadata["content_type"].as_str().unwrap() {
            "image/png" => b"image bytes",
            "application/pdf" => {
                assert_eq!(metadata["filename"], "document.pdf");
                b"pdf bytes"
            }
            mime => panic!("unexpected attachment MIME type: {mime}"),
        };
        assert_eq!(request.body.as_slice(), expected);
        assert!(!request.headers.contains_key("authorization"));
    }

    let mut rows = std::collections::BTreeMap::<String, serde_json::Map<String, Value>>::new();
    for row in requests
        .into_iter()
        .filter(|r| r.url.path() == "/logs3")
        .flat_map(|r| {
            serde_json::from_slice::<Value>(&r.body).unwrap()["rows"]
                .as_array()
                .unwrap()
                .clone()
        })
    {
        rows.entry(row["id"].as_str().unwrap().to_owned())
            .or_default()
            .extend(row.as_object().unwrap().clone());
    }
    rows.into_values().map(Value::Object).collect()
}

#[tokio::test]
async fn pi_uploads_prompt_context_and_tool_images() {
    let image = json!({"type":"image","mimeType":"image/png","data":"aW1hZ2UgYnl0ZXM="});
    let rows = upload(
        "pi",
        vec![
            event(
                "pi",
                "before_agent_start",
                1,
                json!({"prompt":"describe","images":[image.clone()]}),
            ),
            event(
                "pi",
                "context",
                2,
                json!({"messages":[{"role":"user","content":[image.clone()]}]}),
            ),
            event(
                "pi",
                "message_end",
                3,
                json!({"message":{"role":"assistant","content":[{"type":"text","text":"read"}]}}),
            ),
            event(
                "pi",
                "tool_execution_start",
                4,
                json!({"toolCallId":"read","toolName":"read","args":{}}),
            ),
            event(
                "pi",
                "tool_execution_end",
                5,
                json!({"toolCallId":"read","toolName":"read","result":{"content":[image]}}),
            ),
            event("pi", "agent_end", 6, json!({})),
        ],
    )
    .await;
    for (kind, field) in [("task", "input"), ("llm", "input"), ("tool", "output")] {
        assert!(
            rows.iter()
                .any(|row| row["span_attributes"]["type"] == kind
                    && reference(&row[field], "image/png")),
            "missing {kind} {field}: {rows:?}"
        );
    }
}

#[tokio::test]
async fn codex_uploads_prompt_files_and_json_tool_images() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("rollout.jsonl");
    let records = [
        json!({"type":"event_msg","payload":{"type":"task_started","turn_id":"turn"}}),
        json!({"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"describe"},{"type":"input_file","filename":"document.pdf","file_data":"data:application/pdf;base64,cGRmIGJ5dGVz"}]}}),
        json!({"type":"response_item","payload":{"type":"function_call","call_id":"read","name":"read","arguments":"{}"}}),
        json!({"type":"event_msg","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":1,"output_tokens":1}}}}),
        json!({"type":"response_item","payload":{"type":"function_call_output","call_id":"read","output":json!([{"type":"input_image","image_url":"data:image/png;base64,aW1hZ2UgYnl0ZXM="}]).to_string()}}),
        json!({"type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}}),
        json!({"type":"event_msg","payload":{"type":"task_complete","turn_id":"turn"}}),
    ];
    std::fs::write(
        &path,
        records
            .into_iter()
            .enumerate()
            .map(|(i, mut record)| {
                record["timestamp"] = json!(format!("2026-01-01T00:00:0{}Z", i + 1));
                format!("{record}\n")
            })
            .collect::<String>(),
    )
    .unwrap();
    let rows = upload(
        "codex",
        vec![event(
            "codex",
            "SessionStart",
            0,
            json!({"transcript_path":path}),
        )],
    )
    .await;
    for kind in ["task", "llm"] {
        assert!(
            rows.iter().any(|row| row["span_attributes"]["type"] == kind
                && reference(&row["input"], "application/pdf")),
            "missing {kind} PDF: {rows:?}"
        );
    }
    assert!(
        rows.iter()
            .any(|row| row["span_attributes"]["type"] == "tool"
                && reference(&row["output"], "image/png")),
        "missing tool image: {rows:?}"
    );
    assert!(
        rows.iter()
            .any(|row| row["span_attributes"]["type"] == "llm"
                && reference(&row["input"], "image/png")),
        "missing tool image in model history: {rows:?}"
    );
}

#[tokio::test]
async fn claude_uploads_prompt_documents_and_tool_images() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("transcript.jsonl");
    let records = [
        json!({"type":"user","uuid":"user","promptId":"prompt","message":{"role":"user","content":[{"type":"text","text":"describe"},{"type":"document","title":"document.pdf","source":{"type":"base64","media_type":"application/pdf","data":"cGRmIGJ5dGVz"}}]}}),
        json!({"type":"assistant","uuid":"assistant","requestId":"r1","message":{"id":"m1","role":"assistant","model":"claude","content":[{"type":"tool_use","id":"read","name":"Read","input":{}}],"usage":{"input_tokens":1,"output_tokens":1}}}),
        json!({"type":"user","uuid":"result","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"read","content":[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"aW1hZ2UgYnl0ZXM="}}]}]}}),
        json!({"type":"assistant","uuid":"answer","requestId":"r2","message":{"id":"m2","role":"assistant","model":"claude","content":[{"type":"text","text":"done"}],"usage":{"input_tokens":1,"output_tokens":1}}}),
    ];
    std::fs::write(
        &path,
        records
            .into_iter()
            .enumerate()
            .map(|(i, mut record)| {
                record["timestamp"] = json!(format!("1970-01-01T00:00:00.00{}Z", i + 3));
                format!("{record}\n")
            })
            .collect::<String>(),
    )
    .unwrap();
    let rows = upload(
        "claude-code",
        vec![
            event("claude-code", "SessionStart", 1, json!({})),
            event(
                "claude-code",
                "UserPromptSubmit",
                2,
                json!({"prompt":"describe","prompt_id":"prompt"}),
            ),
            event("claude-code", "PreToolUse", 4, json!({"tool_name":"Read","tool_use_id":"read","tool_input":{}})),
            event("claude-code", "PostToolUse", 5, json!({"tool_name":"Read","tool_use_id":"read","tool_response":{"type":"image","file":{"type":"image/png","base64":"aW1hZ2UgYnl0ZXM="}}})),
            // The first turn's transcript arrives after another turn has started.
            event("claude-code", "UserPromptSubmit", 7, json!({"prompt":"follow-up","prompt_id":"follow-up"})),
            event("claude-code", "Stop", 10, json!({"transcript_path":path})),
        ],
    )
    .await;
    for kind in ["task", "llm"] {
        assert!(
            rows.iter().any(|row| row["span_attributes"]["type"] == kind
                && reference(&row["input"], "application/pdf")),
            "missing {kind} PDF: {rows:?}"
        );
    }
    assert!(rows
        .iter()
        .any(|row| row["span_attributes"]["name"] == "Turn 1"
            && reference(&row["input"], "application/pdf")));
    assert!(!rows
        .iter()
        .any(|row| row["span_attributes"]["name"] == "Turn 2"
            && reference(&row["input"], "application/pdf")));
    assert!(
        rows.iter()
            .any(|row| row["span_attributes"]["type"] == "tool"
                && reference(&row["output"], "image/png")),
        "missing tool image: {rows:?}"
    );
}
