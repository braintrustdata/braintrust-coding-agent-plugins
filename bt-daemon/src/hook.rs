//! Hook capture: read one native hook payload from stdin, wrap it in an
//! [`Envelope`], and forward it to the daemon.

use anyhow::Context;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::args::HookArgs;
use crate::client::{forward_envelope, HostInfo};
use crate::route::apply_additional_metadata;
use crate::wire::{self, Envelope, SessionRoute};
use crate::{paths, settings};

/// Identifies the managed run whose child process emitted a hook.
pub(crate) const MANAGED_RUN_ID_ENV: &str = "BT_TRACE_MANAGED_RUN_ID";

fn build_hook_envelope(
    args: &HookArgs,
    route: SessionRoute,
    payload: serde_json::Value,
    session_id: String,
    event: String,
) -> Envelope {
    Envelope {
        source: args.source.clone(),
        source_version: args.source_version.clone(),
        plugin_version: args.plugin_version.clone(),
        session_id,
        event,
        ts_ms: now_ms(),
        managed_run_id: std::env::var(MANAGED_RUN_ID_ENV)
            .ok()
            .filter(|value| !value.is_empty()),
        capture: None,
        payload,
        route: Some(route),
        config: None,
    }
}

pub(crate) fn should_flush_hook_event(event: &str, flush_on_turn_end: bool) -> bool {
    matches!(event, "SessionEnd" | "session_end" | "sessionEnd")
        || (flush_on_turn_end
            && matches!(
                event,
                "Stop"
                    | "stop"
                    | "StopFailure"
                    | "stop_failure"
                    | "StopCancelled"
                    | "stop_cancelled"
                    | "SubagentStop"
                    | "subagent_stop"
                    | "subagentStop"
                    // Cursor can emit its final answer after `stop`.
                    | "afterAgentResponse"
            ))
}

pub(crate) fn should_flush_ingress_event(env: &wire::Envelope) -> bool {
    let native = env.payload.get("event").unwrap_or(&env.payload);
    let flush_on_turn_end = matches!(
        env.route.as_ref().map(|route| route.flush_mode),
        Some(wire::FlushMode::FlushOnTurnEnd)
    );
    should_flush_hook_event(&env.event, flush_on_turn_end)
        || match env.source.as_str() {
            "pi" => match env.event.as_str() {
                // Preserve Pi's explicit lifecycle flushes even in batched mode.
                "session_shutdown" | "session_compact" | "session_tree" => true,
                // OMP can end an attempt while keeping the same turn open for retry.
                "agent_end" => {
                    flush_on_turn_end
                        && native.get("willRetry").and_then(serde_json::Value::as_bool)
                            != Some(true)
                }
                _ => false,
            },
            "opencode" => matches!(
                env.event.as_str(),
                "session.idle" | "session.deleted" | "session.error" | "server.instance.disposed"
            ),
            _ => false,
        }
}

/// Capture one hook event from `stdin` and forward it to the daemon.
///
/// `route` contains only non-secret profile and destination selection.
/// Returns `Ok` once the daemon has durably journaled the event. Delivery
/// happens later; an `Err` means capture failed before acknowledgement.
pub async fn run_hook(args: HookArgs, route: SessionRoute, host: HostInfo) -> anyhow::Result<()> {
    let response =
        (args.source == "cursor").then(|| cursor_hook_response_for_event(args.event.as_deref()));
    let result = async move {
        if suppress_inherited_hook(&args) {
            return Ok(());
        }
        let settings = settings::AgentSettings::load_for_hook(&args.source)?;
        if !settings.tracing_enabled() {
            return Ok(());
        }
        run_hook_with_route(args, settings.route.unwrap_or(route), host).await
    }
    .await;
    if let Some(response) = response {
        println!("{response}");
    }
    result
}

fn cursor_hook_response_for_event(event: Option<&str>) -> &'static str {
    if event == Some("beforeSubmitPrompt") {
        r#"{"continue":true}"#
    } else {
        "{}"
    }
}

/// A managed run injects its own hook definitions. Suppress an inherited
/// Braintrust plugin hook for the same child, but allow the injected hook
/// process, which carries the second marker.
pub(crate) fn suppress_inherited_hook(args: &HookArgs) -> bool {
    std::env::var_os("_BT_TRACE_MANAGED_RUN").is_some() && !args.managed_run_hook
}

pub(crate) async fn run_hook_with_route(
    args: HookArgs,
    route: SessionRoute,
    host: HostInfo,
) -> anyhow::Result<()> {
    with_hook_capture_timeout(args.capture_timeout_ms, capture_hook(args, route, host)).await
}

pub(crate) async fn with_hook_capture_timeout<T>(
    timeout_ms: Option<u64>,
    capture: impl std::future::Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
    match timeout_ms {
        Some(timeout_ms) => {
            tokio::time::timeout(std::time::Duration::from_millis(timeout_ms), capture)
                .await
                .context("hook capture timed out")?
        }
        None => capture.await,
    }
}

async fn capture_hook(
    mut args: HookArgs,
    mut route: SessionRoute,
    host: HostInfo,
) -> anyhow::Result<()> {
    let mut payload = read_stdin_json()?;

    resolve_dynamic_hook_versions(&mut args, &payload);

    if let Some(field) = &args.transcript_path_field {
        add_transcript_observation(&mut payload, field);
    }

    let session_id = json_str_field(&payload, &args.session_id_field)
        .ok_or_else(|| anyhow::anyhow!("no `{}` field in hook payload", args.session_id_field))?;
    let event = args
        .event
        .clone()
        .or_else(|| json_str_field(&payload, &args.event_field))
        .unwrap_or_default();

    if args.flush_on_turn_end {
        route.flush_mode = wire::FlushMode::FlushOnTurnEnd;
    }
    if route.destination.is_none() {
        anyhow::bail!("trace destination is not configured for {}", args.source);
    }
    apply_additional_metadata(&mut route, args.additional_metadata.as_deref())?;
    let env = build_hook_envelope(&args, route, payload, session_id, event);

    let socket = paths::socket_path(args.socket.as_deref());
    forward_envelope(&env, &socket, &host, args.no_spawn).await?;

    Ok(())
}

fn resolve_dynamic_hook_versions(args: &mut HookArgs, payload: &serde_json::Value) {
    if args.source_version.is_none() {
        args.source_version = source_version_from_env(&args.source)
            .or_else(|| source_version_from_payload(&args.source, payload));
    }
    if args.plugin_version.is_none() {
        args.plugin_version = plugin_version_from_plugin_root(&args.source);
    }
}

fn source_version_from_env(source: &str) -> Option<String> {
    let key = match source {
        "claude-code" => "CLAUDE_CODE_VERSION",
        "codex" => "CODEX_VERSION",
        "cursor" => "CURSOR_VERSION",
        "grok" => "GROK_VERSION",
        "antigravity" => "AGY_VERSION",
        _ => return None,
    };
    std::env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn source_version_from_payload(source: &str, payload: &serde_json::Value) -> Option<String> {
    let fields: &[&str] = match source {
        "codex" => &["cli_version", "version"],
        // Hook schema `version` is numeric; the agent version is separate.
        "cursor" => &["cursor_version"],
        "claude-code" | "grok" | "antigravity" => &["version", "cli_version"],
        _ => &[],
    };
    fields
        .iter()
        .find_map(|field| json_str_field(payload, field))
}

fn plugin_version_from_plugin_root(source: &str) -> Option<String> {
    let root = match source {
        "claude-code" => std::env::var_os("CLAUDE_PLUGIN_ROOT"),
        "codex" => std::env::var_os("PLUGIN_ROOT"),
        "cursor" => std::env::var_os("CURSOR_PLUGIN_ROOT"),
        "grok" => std::env::var_os("GROK_PLUGIN_ROOT"),
        // Antigravity runs plugin hooks with the plugin root as cwd.
        "antigravity" => std::env::current_dir()
            .ok()
            .map(|path| path.into_os_string()),
        _ => None,
    }?;
    let manifest = match source {
        "claude-code" => ".claude-plugin/plugin.json",
        "codex" => ".codex-plugin/plugin.json",
        "cursor" => ".cursor-plugin/plugin.json",
        "grok" => ".grok-plugin/plugin.json",
        "antigravity" => "plugin.json",
        _ => return None,
    };
    plugin_version_from_manifest(&PathBuf::from(root).join(manifest))
}

fn plugin_version_from_manifest(path: &std::path::Path) -> Option<String> {
    serde_json::from_slice::<serde_json::Value>(&std::fs::read(path).ok()?)
        .ok()?
        .get("version")?
        .as_str()
        .map(str::to_owned)
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn read_stdin_json() -> anyhow::Result<serde_json::Value> {
    use std::io::Read;
    let mut buf = String::new();
    std::io::stdin().read_to_string(&mut buf)?;
    if buf.trim().is_empty() {
        anyhow::bail!("empty stdin (expected a JSON hook payload)");
    }
    Ok(serde_json::from_str(&buf)?)
}

/// Read a string-ish field (`session_id` / event name) from the payload,
/// coercing a JSON number to its string form.
fn json_str_field(payload: &serde_json::Value, field: &str) -> Option<String> {
    match payload.get(field) {
        Some(serde_json::Value::String(s)) => Some(s.clone()),
        Some(serde_json::Value::Number(n)) => Some(n.to_string()),
        _ => None,
    }
}

/// Stamp the transcript boundary visible when a blocking hook runs. Agent
/// transcripts are append-only, while daemon journal replay may happen after
/// the session has advanced. Recording byte lengths keeps translation causally
/// aligned with each native hook without copying transcript contents into the
/// journal.
fn add_transcript_observation(payload: &mut serde_json::Value, field: &str) {
    let Some(path) = json_str_field(payload, field) else {
        return;
    };
    let transcript = std::path::Path::new(&path);
    let mut observation = serde_json::Map::new();
    observation.insert("path".into(), serde_json::Value::String(path.clone()));
    if let Ok(metadata) = std::fs::metadata(transcript) {
        observation.insert(
            "observed_bytes".into(),
            serde_json::Value::Number(metadata.len().into()),
        );
    }

    if transcript.file_name().and_then(|name| name.to_str()) == Some("transcript.jsonl") {
        let full = transcript.with_file_name("transcript_full.jsonl");
        if let Ok(metadata) = std::fs::metadata(&full) {
            observation.insert(
                "full_path".into(),
                serde_json::Value::String(full.to_string_lossy().into_owned()),
            );
            observation.insert(
                "full_observed_bytes".into(),
                serde_json::Value::Number(metadata.len().into()),
            );
        }
    }

    if let Some(object) = payload.as_object_mut() {
        object.insert(
            "_bt_transcript_observation".into(),
            serde_json::Value::Object(observation),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::initialize_params;
    use clap::Parser;
    use serde_json::json;

    #[derive(Debug, Parser)]
    struct HookCli {
        #[command(flatten)]
        args: HookArgs,
    }

    #[test]
    fn hook_plugin_version_reaches_envelope_and_every_initialize_attempt() {
        let args = HookCli::try_parse_from([
            "test",
            "--source",
            "grok",
            "--source-version",
            "1.0.13",
            "--plugin-version",
            "0.1.0",
        ])
        .unwrap()
        .args;
        assert_eq!(args.source_version.as_deref(), Some("1.0.13"));
        assert_eq!(args.plugin_version.as_deref(), Some("0.1.0"));

        let payload = serde_json::json!({"native": "unchanged"});
        let env = build_hook_envelope(
            &args,
            SessionRoute::default(),
            payload.clone(),
            "session-1".into(),
            "SessionEnd".into(),
        );
        assert_eq!(env.source_version.as_deref(), Some("1.0.13"));
        assert_eq!(env.plugin_version.as_deref(), Some("0.1.0"));
        assert_eq!(env.payload, payload);

        // Both the initial connection and the post-restart retry use this
        // shared parameter builder.
        let initialize = initialize_params(&env, "1.0.13");
        assert_eq!(initialize["client"]["source"], "grok");
        assert_eq!(initialize["client"]["daemon_version"], "1.0.13");
        assert_eq!(initialize["client"]["plugin_version"], "0.1.0");
        assert_ne!(initialize["client"]["plugin_version"], "1.0.13");
    }

    #[test]
    fn hook_versions_are_discovered_without_hook_configuration_literals() {
        let payload = serde_json::json!({"cli_version": "2.3.4"});
        assert_eq!(
            source_version_from_payload("codex", &payload).as_deref(),
            Some("2.3.4")
        );
        assert_eq!(
            source_version_from_payload("cursor", &json!({"version":1,"cursor_version":"2.5.0"}))
                .as_deref(),
            Some("2.5.0")
        );
        assert_eq!(
            source_version_from_payload("cursor", &json!({"version":1})),
            None
        );

        let temp = tempfile::tempdir().unwrap();
        let manifest = temp.path().join("plugin.json");
        std::fs::write(&manifest, br#"{"version":"5.6.7"}"#).unwrap();
        assert_eq!(
            plugin_version_from_manifest(&manifest).as_deref(),
            Some("5.6.7")
        );
    }

    #[test]
    fn hook_flush_recognizes_native_and_documented_terminal_events() {
        for event in ["session_end", "SessionEnd", "sessionEnd"] {
            assert!(should_flush_hook_event(event, false));
        }
        for event in [
            "stop",
            "Stop",
            "stop_failure",
            "StopFailure",
            "stop_cancelled",
            "StopCancelled",
            "subagent_stop",
            "SubagentStop",
            "subagentStop",
            "afterAgentResponse",
        ] {
            assert!(!should_flush_hook_event(event, false));
            assert!(should_flush_hook_event(event, true));
        }
        assert!(!should_flush_hook_event("turn_completed", true));
    }

    #[test]
    fn pi_agent_end_flushes_only_completed_turns_when_enabled() {
        let mut env: wire::Envelope = serde_json::from_value(serde_json::json!({
            "source": "pi", "session_id": "pi-turn", "event": "agent_end",
            "ts_ms": 1, "payload": {"event": {}},
            "route": {"flush_mode": "flush_on_turn_end"}
        }))
        .unwrap();
        for native in [
            serde_json::json!({}),
            serde_json::json!({"willRetry": false}),
        ] {
            env.payload = serde_json::json!({"event": native});
            assert!(should_flush_ingress_event(&env));
        }
        for payload in [
            serde_json::json!({"event": {"willRetry": true}}),
            serde_json::json!({"willRetry": true}),
        ] {
            env.payload = payload;
            assert!(!should_flush_ingress_event(&env));
        }
        env.payload = serde_json::json!({"event": {}});
        env.source = "opencode".into();
        assert!(!should_flush_ingress_event(&env));
        env.source = "pi".into();
        env.event = "message_end".into();
        assert!(!should_flush_ingress_event(&env));
        env.event = "agent_end".into();
        env.route.as_mut().unwrap().flush_mode = wire::FlushMode::FireAndForget;
        assert!(!should_flush_ingress_event(&env));
        env.route = None;
        assert!(!should_flush_ingress_event(&env));
        for event in ["session_shutdown", "session_compact", "session_tree"] {
            env.event = event.into();
            assert!(should_flush_ingress_event(&env));
            env.source = "opencode".into();
            assert!(!should_flush_ingress_event(&env));
            env.source = "pi".into();
        }
    }

    #[test]
    fn opencode_lifecycle_events_flush_from_ingress() {
        let mut env: wire::Envelope = serde_json::from_value(serde_json::json!({
            "source": "opencode", "session_id": "opencode-session", "event": "session.idle",
            "ts_ms": 1, "payload": {},
            "route": {"flush_mode": "fire_and_forget"}
        }))
        .unwrap();
        for event in [
            "session.idle",
            "session.deleted",
            "session.error",
            "server.instance.disposed",
        ] {
            env.event = event.into();
            assert!(should_flush_ingress_event(&env));
        }
        env.event = "session.compacted".into();
        assert!(!should_flush_ingress_event(&env));
        env.event = "session.idle".into();
        env.source = "pi".into();
        assert!(!should_flush_ingress_event(&env));
    }

    #[test]
    fn json_string_fields_accept_strings_and_numbers_only() {
        let payload = serde_json::json!({
            "string": "session",
            "number": 42,
            "boolean": true,
            "null": null
        });
        assert_eq!(
            json_str_field(&payload, "string").as_deref(),
            Some("session")
        );
        assert_eq!(json_str_field(&payload, "number").as_deref(), Some("42"));
        assert_eq!(json_str_field(&payload, "boolean"), None);
        assert_eq!(json_str_field(&payload, "null"), None);
        assert_eq!(json_str_field(&payload, "missing"), None);
    }

    #[test]
    fn clock_returns_a_positive_epoch_timestamp() {
        assert!(now_ms() > 0);
    }

    #[test]
    fn transcript_observation_captures_compact_and_full_boundaries() {
        let dir = tempfile::tempdir().unwrap();
        let compact = dir.path().join("transcript.jsonl");
        let full = dir.path().join("transcript_full.jsonl");
        std::fs::write(&compact, b"compact\n").unwrap();
        std::fs::write(&full, b"complete record\n").unwrap();
        let mut payload = serde_json::json!({
            "transcriptPath": compact.to_string_lossy()
        });

        add_transcript_observation(&mut payload, "transcriptPath");

        let observed = &payload["_bt_transcript_observation"];
        assert_eq!(observed["path"], compact.to_string_lossy().as_ref());
        assert_eq!(observed["observed_bytes"], 8);
        assert_eq!(observed["full_path"], full.to_string_lossy().as_ref());
        assert_eq!(observed["full_observed_bytes"], 16);
    }
}
