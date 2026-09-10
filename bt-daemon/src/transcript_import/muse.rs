//! Versioned Muse Code export-v1 reader.
//!
//! Muse's durable local export is the authoritative import input.  Hook
//! payloads are intentionally not replayed here because their LLM output is a
//! preview and their subagent attribution is incomplete.

use super::{envelope, validate_session_id};
use crate::wire::Envelope;
use anyhow::{bail, Context};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::Path;

pub(crate) fn envelopes(path: &Path) -> anyhow::Result<Vec<Envelope>> {
    let export: Value = serde_json::from_slice(
        &std::fs::read(path).with_context(|| format!("read Muse export {}", path.display()))?,
    )
    .with_context(|| format!("parse Muse export {}", path.display()))?;
    if export.get("export_schema_version").and_then(Value::as_i64) != Some(1) {
        bail!(
            "unsupported Muse export schema in {}; expected version 1",
            path.display()
        );
    }
    let session = export
        .get("sessions")
        .and_then(Value::as_array)
        .and_then(|sessions| sessions.first())
        .ok_or_else(|| anyhow::anyhow!("Muse export {} has no sessions", path.display()))?;
    let session_id = session
        .get("session_id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("Muse export {} has no session_id", path.display()))?;
    validate_session_id(session_id)?;
    let version = export
        .get("exporter_version")
        .and_then(|version| {
            version
                .as_str()
                .or_else(|| version.get("semver").and_then(Value::as_str))
        })
        .map(str::to_owned);
    let events = export
        .get("events")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("Muse export {} has no events", path.display()))?;
    let transcript_path = path.to_string_lossy().into_owned();
    let first_ts = events.iter().filter_map(recorded_ms).next().unwrap_or(0);
    let mut out = vec![event(
        version.clone(),
        session_id,
        "SessionStart",
        first_ts,
        json!({
            "session_id": session_id,
            "hook_event_name": "SessionStart",
            "source": "muse_export_v1",
            "trajectory_id": session.get("trajectory_id"),
            "root_session_id": session.get("root_session_id"),
            "transcript_path": transcript_path,
        }),
    )];
    let mut requests = HashMap::<String, String>::new();
    let mut assistant = HashMap::<String, Value>::new();
    let mut completed = HashMap::<String, (String, Value)>::new();
    let mut last_ts = first_ts;
    for item in events {
        let Some(envelope) = item.get("envelope") else {
            continue;
        };
        let Some(payload) = envelope.get("payload") else {
            continue;
        };
        let run_id = payload.get("run_id").and_then(Value::as_str);
        let native = payload.get("event").and_then(Value::as_object);
        let Some(native) = native else { continue };
        let Some(kind) = native.get("kind").and_then(Value::as_str) else {
            continue;
        };
        let ts = recorded_ms(item).unwrap_or(last_ts);
        last_ts = last_ts.max(ts);
        match kind {
            "started" if payload.get("kind").and_then(Value::as_str) == Some("run") => {
                let Some(run_id) = run_id else { continue };
                out.push(event(
                    version.clone(),
                    session_id,
                    "UserPromptSubmit",
                    ts,
                    json!({
                        "session_id": session_id,
                        "hook_event_name": "UserPromptSubmit",
                        "turn_id": run_id,
                        "prompt": native.get("prompt").cloned().unwrap_or(Value::Null),
                        "source": "muse_export_v1",
                    }),
                ));
            }
            "model_input_trace_recorded" => {
                let Some(run_id) = run_id else { continue };
                let Some(request_id) = native.get("request_record_id").and_then(Value::as_str)
                else {
                    continue;
                };
                requests.insert(run_id.to_owned(), request_id.to_owned());
                out.push(event(
                    version.clone(),
                    session_id,
                    "PreLLMCall",
                    ts,
                    json!({
                        "session_id": session_id,
                        "hook_event_name": "PreLLMCall",
                        "turn_id": run_id,
                        "request_id": request_id,
                        "messages": native.get("bounded").cloned().unwrap_or(Value::Null),
                        "source": "muse_export_v1",
                    }),
                ));
            }
            "model_completed" => {
                let Some(run_id) = run_id else { continue };
                let Some(request_id) = requests.get(run_id) else {
                    continue;
                };
                completed.insert(
                    run_id.to_owned(),
                    (
                        request_id.to_owned(),
                        native.get("usage").cloned().unwrap_or(Value::Null),
                    ),
                );
            }
            "assistant_message_committed" => {
                if let Some(run_id) = run_id {
                    let text = native.get("text").cloned().unwrap_or(Value::Null);
                    assistant.insert(run_id.to_owned(), text.clone());
                    if let Some((request_id, usage)) = completed.remove(run_id) {
                        out.push(event(
                            version.clone(),
                            session_id,
                            "PostLLMCall",
                            ts,
                            json!({
                                "session_id": session_id,
                                "hook_event_name": "PostLLMCall",
                                "turn_id": run_id,
                                "request_id": request_id,
                                "usage": usage,
                                "output_text_preview": text,
                                "source": "muse_export_v1",
                            }),
                        ));
                    }
                }
            }
            "terminal" => {
                let Some(run_id) = run_id else { continue };
                if let Some((request_id, usage)) = completed.remove(run_id) {
                    out.push(event(version.clone(), session_id, "PostLLMCall", ts, json!({
                        "session_id": session_id,
                        "hook_event_name": "PostLLMCall",
                        "turn_id": run_id,
                        "request_id": request_id,
                        "usage": usage,
                        "output_text_preview": assistant.get(run_id).cloned().unwrap_or(Value::Null),
                        "source": "muse_export_v1",
                    })));
                }
                out.push(event(
                    version.clone(),
                    session_id,
                    "Stop",
                    ts,
                    json!({
                        "session_id": session_id,
                        "hook_event_name": "Stop",
                        "turn_id": run_id,
                        "last_assistant_message": assistant.remove(run_id).unwrap_or(Value::Null),
                        "source": "muse_export_v1",
                    }),
                ));
            }
            _ => {}
        }
    }
    out.push(event(
        version,
        session_id,
        "SessionEnd",
        last_ts,
        json!({
            "session_id": session_id,
            "hook_event_name": "SessionEnd",
            "reason": session.pointer("/session_end/exit_reason"),
            "source": "muse_export_v1",
        }),
    ));
    Ok(out)
}

fn recorded_ms(item: &Value) -> Option<i64> {
    item.get("recorded_at")?
        .as_i64()
        .map(|micros| micros / 1_000)
}

fn event(
    version: Option<String>,
    session_id: &str,
    name: &str,
    ts: i64,
    payload: Value,
) -> Envelope {
    envelope("muse", version, session_id, name, ts, payload)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn export_v1_becomes_shared_muse_lifecycle_events() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("export.json");
        std::fs::write(&path, serde_json::to_vec(&json!({
            "export_schema_version": 1, "exporter_version": "1.1.1",
            "sessions": [{"session_id":"session-1", "trajectory_id":"t", "root_session_id":"session-1", "session_end":{"exit_reason":"clean"}}],
            "events": [
                {"recorded_at": 1_000_000, "envelope":{"payload":{"kind":"run","run_id":"run-1","event":{"kind":"started","prompt":"hello"}}}},
                {"recorded_at": 1_001_000, "envelope":{"payload":{"kind":"run","run_id":"run-1","event":{"kind":"model_input_trace_recorded","request_record_id":"request-1","bounded":{"message":"hello"}}}}},
                {"recorded_at": 1_002_000, "envelope":{"payload":{"kind":"run","run_id":"run-1","event":{"kind":"model_completed","usage":{"input_tokens":1}}}}},
                {"recorded_at": 1_003_000, "envelope":{"payload":{"kind":"run","run_id":"run-1","event":{"kind":"assistant_message_committed","text":"world"}}}},
                {"recorded_at": 1_004_000, "envelope":{"payload":{"kind":"run","run_id":"run-1","event":{"kind":"terminal"}}}}
            ]
        })).unwrap()).unwrap();
        let events = envelopes(&path).unwrap();
        assert_eq!(
            events
                .iter()
                .map(|event| event.event.as_str())
                .collect::<Vec<_>>(),
            [
                "SessionStart",
                "UserPromptSubmit",
                "PreLLMCall",
                "PostLLMCall",
                "Stop",
                "SessionEnd"
            ]
        );
        assert_eq!(events[2].ts_ms, 1001);
        assert_eq!(events[4].payload["last_assistant_message"], "world");
    }
}
