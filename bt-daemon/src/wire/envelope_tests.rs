use super::*;

fn sample() -> Envelope {
    Envelope {
        source: "codex".into(),
        source_version: Some("1.2.3".into()),
        plugin_version: Some("0.4.0".into()),
        session_id: "sess-1".into(),
        event: "PostToolUse".into(),
        ts_ms: 1_753_639_552_123,
        managed_run_id: Some("run-1".into()),
        capture: Some(CaptureContext {
            process_chain: vec![ProcessIdentity {
                pid: 42,
                start_time_secs: 1_753_639_500,
            }],
            truncated: false,
        }),
        payload: serde_json::json!({ "session_id": "sess-1", "tool_name": "shell" }),
        route: Some(SessionRoute {
            auth: AuthSelection {
                source: AuthSource::SavedProfile,
                profile_id: None,
                profile: Some("work".into()),
                org_name: Some("acme".into()),
            },
            destination: Some(TraceDestination::ProjectLogs {
                project_id: None,
                project_name: Some("codex".into()),
            }),
            ..SessionRoute::default()
        }),
        config: Some(SessionConfig {
            auth: BackendAuth {
                token: "sk-super-secret".into(),
                api_url: Some("https://api.braintrust.dev".into()),
                app_url: None,
                org_name: Some("acme".into()),
                org_id: None,
            },
            destination: None,
            flush_mode: FlushMode::FireAndForget,
            tags: Default::default(),
            additional_metadata: None,
            span_plugins: Vec::new(),
        }),
    }
}

#[test]
fn envelope_round_trips() {
    let e = sample();
    let s = serde_json::to_string(&e).unwrap();
    let back: Envelope = serde_json::from_str(&s).unwrap();
    assert_eq!(back.session_id, "sess-1");
    assert_eq!(back.managed_run_id.as_deref(), Some("run-1"));
    assert_eq!(
        back.capture.as_ref().unwrap().process_chain[0],
        ProcessIdentity {
            pid: 42,
            start_time_secs: 1_753_639_500,
        }
    );
    assert_eq!(back.route.unwrap().auth.profile.as_deref(), Some("work"));
    assert!(back.config.is_none());
    assert!(!s.contains("sk-super-secret"));
}

#[test]
fn journal_form_contains_only_the_route() {
    let e = sample();
    let r = e.redacted();
    let s = serde_json::to_string(&r).unwrap();
    assert!(
        !s.contains("sk-super-secret"),
        "token leaked into journal form: {s}"
    );
    assert_eq!(r.route.unwrap().auth.profile.as_deref(), Some("work"));
    assert_eq!(r.managed_run_id.as_deref(), Some("run-1"));
    assert_eq!(r.capture.unwrap().process_chain[0].pid, 42);
}

#[test]
fn capture_context_is_backward_compatible() {
    let mut value = serde_json::to_value(sample()).unwrap();
    value.as_object_mut().unwrap().remove("capture");

    let envelope: Envelope = serde_json::from_value(value).unwrap();

    assert!(envelope.capture.is_none());
}

#[test]
fn route_is_journal_safe_and_builds_resolved_config() {
    let route = SessionRoute {
        auth: AuthSelection {
            source: AuthSource::SavedProfile,
            profile_id: None,
            profile: Some("work".into()),
            org_name: Some("acme".into()),
        },
        destination: Some(TraceDestination::ProjectLogs {
            project_id: None,
            project_name: Some("agent-traces".into()),
        }),
        ..SessionRoute::default()
    };
    let config = route.with_auth(BackendAuth {
        token: "secret".into(),
        api_url: None,
        app_url: None,
        org_name: Some("acme".into()),
        org_id: None,
    });
    assert!(matches!(
        config.destination,
        Some(TraceDestination::ProjectLogs { project_name: Some(ref name), .. })
            if name == "agent-traces"
    ));
    assert_eq!(route.auth.profile.as_deref(), Some("work"));

    let mut envelope = sample();
    envelope.route = Some(route);
    envelope.config = Some(config);
    let journal = serde_json::to_string(&envelope.redacted()).unwrap();
    assert!(journal.contains("work"));
    assert!(!journal.contains("secret"));
}

#[test]
fn environment_auth_serializes_without_a_profile_or_secret() {
    let selection = AuthSelection {
        source: AuthSource::Environment,
        profile_id: None,
        profile: None,
        org_name: Some("acme".into()),
    };
    let json = serde_json::to_string(&selection).unwrap();
    assert_eq!(json, r#"{"source":"environment","org_name":"acme"}"#);
    assert!(!json.contains("BRAINTRUST_API_KEY"));
}

#[test]
fn legacy_environment_profile_canonicalizes_to_environment_auth() {
    let selection: AuthSelection =
        serde_json::from_str(r#"{"profile":"environment","org_name":"acme"}"#).unwrap();
    assert_eq!(selection.effective_source(), AuthSource::Environment);
    let canonical = selection.canonicalized().unwrap();
    assert_eq!(canonical.source, AuthSource::Environment);
    assert_eq!(canonical.profile, None);
    assert_eq!(canonical.org_name.as_deref(), Some("acme"));
}

#[test]
fn stable_profile_id_route_matches_its_legacy_name_route() {
    let legacy = SessionRoute {
        auth: AuthSelection {
            source: AuthSource::Auto,
            profile_id: None,
            profile: Some("test-profile".into()),
            org_name: Some("test-org".into()),
        },
        destination: Some(TraceDestination::ProjectLogs {
            project_id: None,
            project_name: Some("test-project".into()),
        }),
        ..SessionRoute::default()
    };
    let mut canonical = legacy.clone();
    canonical.auth.source = AuthSource::SavedProfile;
    canonical.auth.profile_id = Some("00000000-0000-4000-8000-000000000001".into());

    assert!(legacy.same_route(&canonical));
    canonical.auth.profile = Some("other-profile".into());
    assert!(!legacy.same_route(&canonical));
}

#[test]
fn flush_mode_defaults_to_fire_and_forget() {
    let json = serde_json::json!({
        "auth": { "token": "t" },
    });
    let cfg: SessionConfig = serde_json::from_value(json).unwrap();
    assert_eq!(cfg.flush_mode, FlushMode::FireAndForget);
}

#[test]
fn import_destination_references_are_typed() {
    let project: TraceDestination = "project_logs:proj-123".parse().unwrap();
    assert!(matches!(
        project,
        TraceDestination::ProjectLogs {
            project_id: Some(ref id),
            project_name: None
        } if id == "proj-123"
    ));
    let experiment: TraceDestination = "experiment:exp-456".parse().unwrap();
    assert!(matches!(
        experiment,
        TraceDestination::Experiment { ref experiment_id } if experiment_id == "exp-456"
    ));
    assert!("project:ambiguous".parse::<TraceDestination>().is_err());
}

#[test]
fn replay_restores_non_secret_route_configuration_without_credentials() {
    let native = sample();
    let redacted = native.redacted();
    let recorded = serde_json::to_value(&redacted).unwrap();
    let replay = redacted.into_envelope();
    assert_eq!(serde_json::to_value(replay.redacted()).unwrap(), recorded);
    let config = replay.config.unwrap();
    assert!(config.auth.token.is_empty());
    assert_eq!(config.auth.org_name.as_deref(), Some("acme"));
    assert_eq!(config.project_name(), Some("codex"));

    let mut without_route = native.redacted();
    without_route.route = None;
    assert!(without_route.into_envelope().config.is_none());
}
