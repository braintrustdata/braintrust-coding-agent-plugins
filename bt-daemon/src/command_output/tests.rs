use crate::wire::{SessionRoute, TraceDestination};
use std::path::PathBuf;

use super::*;

#[test]
fn absent_status_is_machine_readable_in_json_mode() {
    let output = TraceCommandOutput::status(None);
    let rendered = output.render(OutputFormat::Json).unwrap();
    let value: serde_json::Value = serde_json::from_str(&rendered).unwrap();
    assert_eq!(value["command"], "status");
    assert_eq!(value["running"], false);
    assert_eq!(value["sessions"], serde_json::json!([]));
    assert!(value.get("daemon_version").is_none());
    assert!(!rendered.contains("not running"));
}

#[test]
fn install_json_contains_stable_selection_fields_without_prose() {
    let output = TraceCommandOutput::install(
        "opencode",
        "OpenCode",
        PathBuf::from("/tmp/opencode/braintrust.json"),
    );
    let rendered = output.render(OutputFormat::Json).unwrap();
    let value: serde_json::Value = serde_json::from_str(&rendered).unwrap();
    assert_eq!(value["command"], "install");
    assert_eq!(value["source"], "opencode");
    assert_eq!(value["restart_required"], true);
    assert!(!rendered.contains("installed for"));
}

#[test]
fn resume_output_reports_enable() {
    let output = TraceCommandOutput::enable("pi", "Pi", PathBuf::from("braintrust.json"));
    let json: serde_json::Value =
        serde_json::from_str(&output.render(OutputFormat::Json).unwrap()).unwrap();
    assert_eq!(json["command"], "enable");
    assert_eq!(json["configuration"], "reused");
    assert!(output
        .render(OutputFormat::Human)
        .unwrap()
        .contains("tracing is enabled"));
}

#[test]
fn disable_output_is_explicit() {
    let output = TraceCommandOutput::disable(
        "antigravity",
        "Google Antigravity",
        PathBuf::from("/tmp/antigravity/braintrust.json"),
    );
    let value: serde_json::Value =
        serde_json::from_str(&output.render(OutputFormat::Json).unwrap()).unwrap();
    assert_eq!(value["command"], "disable");
    assert!(output
        .render(OutputFormat::Human)
        .unwrap()
        .contains("disabled for Google Antigravity"));
}

#[test]
fn codex_setup_and_updates_do_not_request_manual_trust() {
    for output in [
        TraceCommandOutput::install("codex", "Codex", PathBuf::from("braintrust.json")),
        TraceCommandOutput::update("codex", "Codex"),
    ] {
        let human = output.render(OutputFormat::Human).unwrap();
        assert!(!human.contains("Open /hooks"));
        let json: serde_json::Value =
            serde_json::from_str(&output.render(OutputFormat::Json).unwrap()).unwrap();
        assert!(json.get("next_steps").is_none());
    }
    let disabled = TraceCommandOutput::disable("codex", "Codex", PathBuf::from("braintrust.json"));
    assert!(!disabled
        .render(OutputFormat::Human)
        .unwrap()
        .contains("Open /hooks"));
}

#[test]
fn stop_json_reports_idempotent_and_successful_shutdowns() {
    let absent: serde_json::Value = serde_json::from_str(
        &TraceCommandOutput::stop(false, false)
            .render(OutputFormat::Json)
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        absent,
        serde_json::json!({
            "command": "stop",
            "running": false,
            "stopped": false
        })
    );

    let stopped: serde_json::Value = serde_json::from_str(
        &TraceCommandOutput::stop(true, true)
            .render(OutputFormat::Json)
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        stopped,
        serde_json::json!({
            "command": "stop",
            "running": true,
            "stopped": true
        })
    );
}

#[test]
fn import_summary_has_stable_human_and_json_output() {
    let output = TraceCommandOutput::import(vec![ImportSummary {
        session_id: "session-1".into(),
        destination: Some(TraceDestination::ProjectLogs {
            project_id: None,
            project_name: Some("Agents".into()),
        }),
        root_span_id: Some("root-1".into()),
        span_count: 3,
        finalized: true,
    }]);
    assert_eq!(
        output.render(OutputFormat::Human).unwrap(),
        "Imported session session-1 to project Agents: 3 spans, root span root-1."
    );
    let value: serde_json::Value =
        serde_json::from_str(&output.render(OutputFormat::Json).unwrap()).unwrap();
    assert_eq!(value["command"], "import");
    assert_eq!(value["summaries"][0]["session_id"], "session-1");
    assert_eq!(value["summaries"][0]["span_count"], 3);
    assert_eq!(value["summaries"][0]["finalized"], true);
}

#[test]
fn human_output_preserves_existing_messages() {
    assert_eq!(
        TraceCommandOutput::status(None)
            .render(OutputFormat::Human)
            .unwrap(),
        "bt-daemon is not running"
    );
    assert_eq!(
        TraceCommandOutput::stop(false, false)
            .render(OutputFormat::Human)
            .unwrap(),
        "No tracing daemon is running."
    );
}

#[test]
fn doctor_json_is_structured_and_contains_no_credentials() {
    let output = TraceCommandOutput::doctor(DoctorCommandOutput {
        source: "codex".into(),
        display_name: "Codex".into(),
        settings_path: PathBuf::from("/tmp/braintrust.json"),
        settings_present: true,
        project_settings_path: None,
        project_settings_fields: Vec::new(),
        enabled: true,
        route_source: "settings_file".into(),
        route: Some(SessionRoute::default()),
        auth: AuthDiagnostic {
            status: "ready".into(),
            source: "saved_profile".into(),
            kind: Some("oauth".into()),
            profile: Some("test-profile".into()),
            org_name: Some("test-org".into()),
            expires_at_ms: Some(123),
            error: None,
        },
        daemon: DaemonDiagnostic {
            status: DaemonStatus::Running,
            version: Some("1.2.3".into()),
            auth: Some(AuthDiagnostic {
                status: "error".into(),
                source: "saved_profile".into(),
                kind: None,
                profile: None,
                org_name: None,
                expires_at_ms: None,
                error: Some("saved profile ID 'p' no longer exists".into()),
            }),
            session_errors: vec!["could not resolve Braintrust auth for codex".into()],
            recovering_sessions: vec![crate::wire::SessionStatus {
                session_id: "held-session".into(),
                source: "codex".into(),
                route: None,
                recovery: Some(crate::wire::RecoveryInfo {
                    stage: "plugin".into(),
                    position: 1,
                    reason: "deadline".into(),
                    retry: "bounded retry".into(),
                    eligible: false,
                    attempt: 1,
                }),
                queued: 8,
                queued_span_ops: Some(7),
                spans_emitted: 0,
                permalink: None,
                last_error: None,
            }],
            error: None,
        },
        warnings: Vec::new(),
        plugin_diagnostics: vec![crate::PluginDiagnostic {
            source: "codex".into(),
            plugin_path: PathBuf::from("/tmp/redact.mjs"),
            plugin_digest: Some("abc".into()),
            exception: "Error: raw secret\n    at redact (redact.mjs:1)".into(),
            first_seen_ms: 1,
            last_seen_ms: 2,
            occurrences: 3,
        }],
    });
    let rendered = output.render(OutputFormat::Json).unwrap();
    let value: serde_json::Value = serde_json::from_str(&rendered).unwrap();
    assert_eq!(value["command"], "doctor");
    assert_eq!(value["auth"]["source"], "saved_profile");
    assert_eq!(value["daemon"]["status"], "running");
    assert_eq!(value["daemon"]["auth"]["status"], "error");
    assert_eq!(
        value["daemon"]["session_errors"][0],
        "could not resolve Braintrust auth for codex"
    );
    assert_eq!(
        value["daemon"]["recovering_sessions"][0]["queued_span_ops"],
        7
    );
    assert_eq!(
        value["daemon"]["recovering_sessions"][0]["recovery"]["attempt"],
        1
    );
    let human = output.render(OutputFormat::Human).unwrap();
    assert!(human.contains("Active delivery hold: codex / held-session"));
    assert!(human.contains("Queued span operations: 7"));
    assert!(human.contains("Historical plugin error:"));
    assert!(human.contains("not an affected-span count"));
    assert!(!rendered.contains("token"));
    assert!(!rendered.contains("api_key"));
    assert_eq!(
        value["plugin_diagnostics"][0]["exception"],
        "Error: raw secret\n    at redact (redact.mjs:1)"
    );
}
