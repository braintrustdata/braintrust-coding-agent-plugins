//! Capture-side transcript hydration. Agent transcript files are external
//! mutable state, so capture mirrors them into daemon-owned storage and
//! journals only bounded references; translation, live or replayed, then
//! reads exactly the bytes capture observed.

use crate::wire::Envelope;

/// Agent transcript files are external mutable state. Mirror them into
/// daemon-owned storage at lifecycle boundaries and journal only bounded
/// references, so recovery sees exactly the bytes that live translation
/// observed instead of depending on mutable external paths or copying a full
/// transcript into every event. Capture failures remain fail-open.
pub(crate) async fn hydrate_transcript_reference(data_dir: &std::path::Path, env: &mut Envelope) {
    if env.source == "cursor" {
        hydrate_cursor_transcript_reference(data_dir, env).await;
        return;
    }
    if env.source == "grok" {
        hydrate_grok_transcript_references(data_dir, env).await;
        return;
    }
    if env.source == "claude-code" {
        hydrate_claude_transcript_references(data_dir, env).await;
        return;
    }
    if env.source != "codex" {
        return;
    }
    let field = if env.event == "SubagentStop" {
        "agent_transcript_path"
    } else {
        "transcript_path"
    };
    let Some(path) = env
        .payload
        .get(field)
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
    else {
        return;
    };
    let mirror_session = crate::ids::session_namespace(&env.source, &env.session_id);
    let (mirror, through) =
        match crate::transcript_mirror::capture(data_dir, &mirror_session, &path).await {
            Ok(captured) => captured,
            Err(error) => {
                tracing::debug!(session_id = %env.session_id, %error, "transcript mirror skipped");
                return;
            }
        };
    if let Some(payload) = env.payload.as_object_mut() {
        payload.insert(
            "_bt_transcript_mirror".to_string(),
            serde_json::json!({
                "path": path,
                "mirror": mirror.to_string_lossy(),
                "through": through,
            }),
        );
    }
}

async fn hydrate_cursor_transcript_reference(data_dir: &std::path::Path, env: &mut Envelope) {
    if env.payload.get("_bt_transcript_mirror").is_some() {
        return;
    }
    let path = env
        .payload
        .get("transcript_path")
        .and_then(serde_json::Value::as_str);
    let mut observation = serde_json::json!({});
    if let Some(path) = path.filter(|path| !path.is_empty()) {
        let observed_bytes = env
            .payload
            .get("_bt_transcript_observation")
            .filter(|observation| {
                observation.get("path").and_then(serde_json::Value::as_str) == Some(path)
            })
            .and_then(|observation| observation.get("observed_bytes"))
            .and_then(serde_json::Value::as_u64);
        let mirror_session = crate::ids::session_namespace(&env.source, &env.session_id);
        match crate::transcript_mirror::capture_generation(
            data_dir,
            &mirror_session,
            path,
            observed_bytes,
        )
        .await
        {
            Ok((mirror, through)) => {
                observation =
                    serde_json::json!({"path": path, "mirror": mirror, "through": through});
            }
            Err(error) => {
                tracing::debug!(session_id = %env.session_id, %error, "Cursor transcript mirror skipped");
            }
        }
    }
    if let Some(payload) = env.payload.as_object_mut() {
        // An empty observation is also durable: no transcript was available
        // at this boundary, even if the external path appears during replay.
        payload.insert("_bt_transcript_mirror".to_string(), observation);
    }
}

async fn hydrate_claude_transcript_references(data_dir: &std::path::Path, env: &mut Envelope) {
    // Historical snapshots and newly captured references are observations,
    // not invitations to reread an external file during journal recovery.
    if env.payload.get("_bt_claude_transcript_mirrors").is_some()
        || env.payload.get("_bt_transcript_mirror").is_some()
        || env.payload.get("_bt_transcript_snapshot").is_some()
        || env.payload.get("_bt_transcript_replay").is_some()
    {
        return;
    }
    let mirror_session = crate::ids::session_namespace(&env.source, &env.session_id);
    let mut mirrors = serde_json::Map::new();
    for field in ["transcript_path", "agent_transcript_path"] {
        let Some(path) = env.payload.get(field).and_then(serde_json::Value::as_str) else {
            continue;
        };
        if mirrors.contains_key(path) {
            continue;
        }
        match crate::transcript_mirror::capture(data_dir, &mirror_session, path).await {
            Ok((mirror, through)) => {
                mirrors.insert(
                    path.to_string(),
                    serde_json::json!({"path": path, "mirror": mirror, "through": through}),
                );
            }
            Err(error) => {
                tracing::debug!(session_id = %env.session_id, %error, "Claude transcript mirror skipped");
            }
        }
    }
    if !env.payload.is_object() {
        env.payload = serde_json::json!({});
    }
    env.payload.as_object_mut().unwrap().insert(
        "_bt_claude_transcript_mirrors".to_string(),
        serde_json::Value::Object(mirrors),
    );
}

const GROK_TERMINAL_SNAPSHOT_ATTEMPTS: usize = 4;
const GROK_TERMINAL_SNAPSHOT_QUIET_PERIOD: std::time::Duration =
    std::time::Duration::from_millis(10);

async fn hydrate_grok_transcript_references(data_dir: &std::path::Path, env: &mut Envelope) {
    let Some(transcript) = env
        .payload
        .get("transcriptPath")
        .or_else(|| env.payload.get("transcript_path"))
        .and_then(serde_json::Value::as_str)
        .map(std::path::PathBuf::from)
    else {
        return;
    };
    let Some(session_dir) = transcript.parent() else {
        return;
    };
    let mirror_session = crate::ids::session_namespace(&env.source, &env.session_id);
    let mut mirrors = serde_json::Map::new();
    capture_grok_transcript_pass(
        data_dir,
        session_dir,
        &mirror_session,
        &env.session_id,
        &mut mirrors,
    )
    .await;

    if matches!(env.event.as_str(), "SessionEnd" | "session_end") {
        for _ in 0..GROK_TERMINAL_SNAPSHOT_ATTEMPTS {
            tokio::time::sleep(GROK_TERMINAL_SNAPSHOT_QUIET_PERIOD).await;
            capture_grok_transcript_pass(
                data_dir,
                session_dir,
                &mirror_session,
                &env.session_id,
                &mut mirrors,
            )
            .await;
        }
    }

    if !mirrors.is_empty() {
        if let Some(payload) = env.payload.as_object_mut() {
            payload.insert(
                "_bt_grok_transcript_mirrors".to_string(),
                serde_json::Value::Object(mirrors),
            );
        }
    }
}

async fn capture_grok_transcript_pass(
    data_dir: &std::path::Path,
    session_dir: &std::path::Path,
    mirror_session: &str,
    session_id: &str,
    mirrors: &mut serde_json::Map<String, serde_json::Value>,
) {
    for (name, key) in [
        ("updates.jsonl", "updates"),
        ("events.jsonl", "events"),
        ("system_prompt.txt", "system_prompt"),
    ] {
        let source = session_dir.join(name);
        let Some(source_str) = source.to_str() else {
            continue;
        };
        match crate::transcript_mirror::capture(data_dir, mirror_session, source_str).await {
            Ok((mirror, through)) => {
                mirrors.insert(
                    key.to_string(),
                    serde_json::json!({
                        "path": source,
                        "mirror": mirror,
                        "through": through,
                    }),
                );
            }
            Err(error) => {
                tracing::debug!(%session_id, %error, file = name, "Grok transcript mirror skipped");
            }
        }
    }
}

pub(crate) fn is_tool_lifecycle_event(event: &str) -> bool {
    matches!(
        event,
        "PreToolUse"
            | "PostToolUse"
            | "PostToolUseFailure"
            | "preToolUse"
            | "postToolUse"
            | "postToolUseFailure"
            | "tool_execution_start"
            | "tool_execution_end"
            | "tool.execute.before"
            | "tool.execute.after"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cursor_hydration_freezes_hook_bytes_and_missing_paths() {
        let tmp = tempfile::tempdir().unwrap();
        let transcript = tmp.path().join("cursor.jsonl");
        std::fs::write(&transcript, b"first\nfuture\n").unwrap();
        let mut env: Envelope = serde_json::from_value(serde_json::json!({
            "source":"cursor", "session_id":"cursor-boundary", "event":"stop", "ts_ms":1,
            "payload": {
                "transcript_path":transcript,
                "_bt_transcript_observation":{"path":transcript,"observed_bytes":6}
            }
        }))
        .unwrap();
        hydrate_transcript_reference(tmp.path(), &mut env).await;
        let observation = env.payload["_bt_transcript_mirror"].clone();
        assert_eq!(observation["through"], 6);
        assert_eq!(
            std::fs::read(observation["mirror"].as_str().unwrap()).unwrap(),
            b"first\n"
        );
        std::fs::write(&transcript, b"replacement\n").unwrap();
        hydrate_transcript_reference(tmp.path(), &mut env).await;
        assert_eq!(env.payload["_bt_transcript_mirror"], observation);

        env.payload = serde_json::json!({"transcript_path":null});
        hydrate_transcript_reference(tmp.path(), &mut env).await;
        assert_eq!(env.payload["_bt_transcript_mirror"], serde_json::json!({}));
        env.payload["transcript_path"] = serde_json::json!(transcript);
        hydrate_transcript_reference(tmp.path(), &mut env).await;
        assert_eq!(env.payload["_bt_transcript_mirror"], serde_json::json!({}));
    }
}
