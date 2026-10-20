//! Read-only hook readiness through Codex's app-server protocol. Trust remains
//! owned by Codex; this module never writes its private trust store.

use anyhow::{anyhow, Context};
use serde::Serialize;
use serde_json::{json, Value};
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader,
};

const PLUGIN: &str = "trace-codex@braintrust-codex-plugins";
const TIMEOUT: Duration = Duration::from_secs(5);
const EVENTS: &[&str] = &[
    "sessionStart",
    "userPromptSubmit",
    "preToolUse",
    "permissionRequest",
    "postToolUse",
    "preCompact",
    "postCompact",
    "subagentStart",
    "subagentStop",
    "stop",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CodexHookState {
    Trusted,
    ReviewRequired,
    Disabled,
    PluginMissing,
    MissingHooks,
    ConfigurationError,
    Unknown,
}

impl std::fmt::Display for CodexHookState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Trusted => "trusted",
            Self::ReviewRequired => "review required",
            Self::Disabled => "disabled",
            Self::PluginMissing => "plugin missing",
            Self::MissingHooks => "missing hooks",
            Self::ConfigurationError => "configuration error",
            Self::Unknown => "unknown",
        })
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct CodexHookReadiness {
    pub state: CodexHookState,
    pub hook_count: usize,
    pub trusted_count: usize,
    pub disabled_count: usize,
    pub review_required_count: usize,
    pub missing_events: Vec<String>,
    pub detail: String,
}

impl CodexHookReadiness {
    fn unknown(error: impl std::fmt::Display) -> Self {
        Self {
            state: CodexHookState::Unknown,
            hook_count: 0, trusted_count: 0, disabled_count: 0, review_required_count: 0,
            missing_events: Vec::new(),
            detail: format!("Could not inspect Codex hooks: {error}. Open /hooks in Codex to inspect Braintrust hook trust; older Codex versions may not expose hooks/list."),
        }
    }
}

pub(crate) async fn probe() -> CodexHookReadiness {
    let cwd = match std::env::current_dir() {
        Ok(cwd) => cwd,
        Err(error) => return CodexHookReadiness::unknown(error),
    };
    probe_at("codex", &cwd, None).await
}

async fn probe_at(program: &str, cwd: &Path, home: Option<&Path>) -> CodexHookReadiness {
    let mut command: tokio::process::Command =
        crate::subprocess::background_command(program).into();
    command
        .args(["--no-daemon", "app-server", "--stdio"])
        .current_dir(cwd)
        .kill_on_drop(true)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    if let Some(home) = home {
        command.env("CODEX_HOME", home);
    }
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => return CodexHookReadiness::unknown(error),
    };
    let result =
        tokio::time::timeout(TIMEOUT, async {
            let mut stdin = child.stdin.take().context("Codex stdin unavailable")?;
            let stdout = child.stdout.take().context("Codex stdout unavailable")?;
            // Bound both total response bytes and wall time, including notifications.
            let mut stdout = BufReader::new(stdout.take(8 * 1024 * 1024));
            request(&mut stdin, &mut stdout, 1, "initialize", json!({
            "clientInfo":{"name":"braintrust_trace_doctor","version":env!("CARGO_PKG_VERSION")},
            "capabilities":{"experimentalApi":true}
        })).await?;
            stdin.write_all(b"{\"method\":\"initialized\"}\n").await?;
            let config = request(
                &mut stdin,
                &mut stdout,
                2,
                "config/read",
                json!({"includeLayers":false}),
            )
            .await?;
            let hooks = request(
                &mut stdin,
                &mut stdout,
                3,
                "hooks/list",
                json!({"cwds":[cwd]}),
            )
            .await?;
            let plugins = request(
                &mut stdin,
                &mut stdout,
                4,
                "plugin/list",
                json!({
                    "cwds":[cwd], "forceRefetch":false, "marketplaceKinds":["local"]
                }),
            )
            .await?;
            classify(&hooks, &config, &plugins)
        })
        .await;
    // Always reap the probe, even when the protocol is unsupported or stalls.
    let _ = child.kill().await;
    match result {
        Ok(Ok(readiness)) => readiness,
        Ok(Err(error)) => CodexHookReadiness::unknown(error),
        Err(_) => CodexHookReadiness::unknown("probe timed out after 5 seconds"),
    }
}

async fn request(
    stdin: &mut (impl AsyncWrite + Unpin),
    stdout: &mut (impl AsyncBufRead + Unpin),
    id: u64,
    method: &str,
    params: Value,
) -> anyhow::Result<Value> {
    let mut encoded = serde_json::to_vec(&json!({"id":id,"method":method,"params":params}))?;
    encoded.push(b'\n');
    stdin.write_all(&encoded).await?;
    stdin.flush().await?;
    loop {
        let mut line = String::new();
        anyhow::ensure!(
            stdout.read_line(&mut line).await? != 0,
            "Codex closed the probe stream"
        );
        let response: Value = serde_json::from_str(&line).context("invalid Codex response")?;
        if response.get("id") != Some(&json!(id)) {
            continue;
        }
        if let Some(error) = response.get("error") {
            return Err(anyhow!(
                "{method}: {}",
                error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("request failed")
            ));
        }
        return response
            .get("result")
            .cloned()
            .context("Codex response has no result");
    }
}

fn classify(hooks: &Value, config: &Value, plugins: &Value) -> anyhow::Result<CodexHookReadiness> {
    let entries = hooks
        .get("data")
        .and_then(Value::as_array)
        .context("hooks/list has no data array")?;
    let mut selected = Vec::new();
    let mut has_errors = false;
    for entry in entries {
        has_errors |= entry
            .get("errors")
            .and_then(Value::as_array)
            .is_none_or(|errors| !errors.is_empty());
        for hook in entry
            .get("hooks")
            .and_then(Value::as_array)
            .context("hooks/list entry has no hooks array")?
        {
            let is_plugin = hook.get("pluginId").and_then(Value::as_str) == Some(PLUGIN);
            let is_managed = hook.get("isManaged").and_then(Value::as_bool) == Some(true)
                && hook
                    .get("command")
                    .and_then(Value::as_str)
                    .is_some_and(|command| command.ends_with(" trace hook --source codex"));
            if is_plugin || is_managed {
                selected.push(hook);
            }
        }
    }
    let mut readiness = CodexHookReadiness {
        state: CodexHookState::Unknown,
        hook_count: selected.len(),
        trusted_count: 0,
        disabled_count: 0,
        review_required_count: 0,
        missing_events: EVENTS
            .iter()
            .filter(|event| {
                !selected
                    .iter()
                    .any(|hook| hook.get("eventName").and_then(Value::as_str) == Some(**event))
            })
            .map(|event| (*event).to_owned())
            .collect(),
        detail: String::new(),
    };
    for hook in &selected {
        anyhow::ensure!(
            hook.get("enabled").and_then(Value::as_bool).is_some(),
            "hook has no enabled state"
        );
        if hook.get("enabled").and_then(Value::as_bool) == Some(false) {
            readiness.disabled_count += 1;
        }
        match hook.get("trustStatus").and_then(Value::as_str) {
            Some("trusted" | "managed") => readiness.trusted_count += 1,
            Some("untrusted" | "modified") => readiness.review_required_count += 1,
            _ => {}
        }
    }
    let marketplaces = plugins
        .get("marketplaces")
        .and_then(Value::as_array)
        .context("plugin/list has no marketplaces array")?;
    let plugin = marketplaces
        .iter()
        .filter_map(|marketplace| marketplace.get("plugins").and_then(Value::as_array))
        .flatten()
        .find(|plugin| plugin.get("id").and_then(Value::as_str) == Some(PLUGIN));
    let config = config
        .get("config")
        .filter(|value| value.is_object())
        .context("config/read has no config object")?;
    let features = &config["features"];
    let plugin_capture = selected.is_empty()
        || selected
            .iter()
            .any(|hook| hook.get("pluginId").and_then(Value::as_str) == Some(PLUGIN));
    let disabled = features
        .get("hooks")
        .or_else(|| features.get("codex_hooks"))
        .and_then(Value::as_bool)
        == Some(false)
        || plugin_capture
            && plugin.is_some_and(|plugin| {
                plugin.get("enabled").and_then(Value::as_bool) == Some(false)
            });
    let (state, detail) = if disabled || readiness.disabled_count > 0 {
        (CodexHookState::Disabled, "Codex hooks or the Braintrust plugin are disabled. Enable them in Codex and inspect /hooks.")
    } else if has_errors {
        (CodexHookState::ConfigurationError, "Codex reported hook configuration errors. Inspect /hooks and Codex startup diagnostics.")
    } else if readiness.review_required_count > 0 {
        (CodexHookState::ReviewRequired, "Open /hooks in Codex and review the current Braintrust hook definitions. Untrusted or modified hooks are skipped before capture.")
    } else if selected.is_empty()
        && plugin
            .is_none_or(|plugin| plugin.get("installed").and_then(Value::as_bool) != Some(true))
    {
        (CodexHookState::PluginMissing, "Braintrust Codex hooks are not installed. Run bt trace enable codex, then review /hooks in Codex.")
    } else if !readiness.missing_events.is_empty() {
        (CodexHookState::MissingHooks, "Some Braintrust lifecycle hooks are missing. Update the plugin or complete the managed hook policy, then inspect /hooks.")
    } else if readiness.trusted_count == readiness.hook_count {
        (CodexHookState::Trusted, "Braintrust hook definitions are enabled and trusted for this working directory. This does not confirm hook execution or Braintrust delivery.")
    } else {
        (
            CodexHookState::Unknown,
            "Codex reported an unknown hook trust state. Inspect /hooks before relying on capture.",
        )
    };
    readiness.state = state;
    readiness.detail = detail.into();
    Ok(readiness)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(trust: &str) -> Value {
        json!({"data":[{"errors":[],"hooks":EVENTS.iter().map(|event|json!({
            "eventName":event,"pluginId":PLUGIN,"enabled":true,"trustStatus":trust
        })).collect::<Vec<_>>()}]})
    }
    fn installed() -> Value {
        json!({"marketplaces":[{"plugins":[{"id":PLUGIN,"installed":true,"enabled":true}]}]})
    }

    #[test]
    fn required_events_match_the_packaged_plugin() {
        let hooks: Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../src/plugins/codex/content/plugins/trace-codex/hooks/hooks.json"
        )))
        .unwrap();
        let events: std::collections::BTreeSet<_> = hooks["hooks"]
            .as_object()
            .unwrap()
            .keys()
            .map(|key| format!("{}{}", key[..1].to_ascii_lowercase(), &key[1..]))
            .collect();
        assert_eq!(
            events,
            EVENTS.iter().map(|event| (*event).to_owned()).collect()
        );
    }

    #[test]
    fn classifies_trust_modifications_missing_disabled_and_managed_hooks() {
        let config = json!({"config":{"features":{"hooks":true}}});
        for (trust, state) in [
            ("trusted", CodexHookState::Trusted),
            ("untrusted", CodexHookState::ReviewRequired),
            ("modified", CodexHookState::ReviewRequired),
            ("future", CodexHookState::Unknown),
        ] {
            assert_eq!(
                classify(&snapshot(trust), &config, &installed())
                    .unwrap()
                    .state,
                state
            );
        }
        let mut hooks = snapshot("trusted");
        hooks["data"][0]["hooks"][0]["enabled"] = json!(false);
        assert_eq!(
            classify(&hooks, &config, &installed()).unwrap().state,
            CodexHookState::Disabled
        );
        let missing = json!({"data":[{"errors":[],"hooks":[]}]});
        assert_eq!(
            classify(&missing, &config, &installed()).unwrap().state,
            CodexHookState::MissingHooks
        );
        assert_eq!(
            classify(&missing, &config, &json!({"marketplaces":[]}))
                .unwrap()
                .state,
            CodexHookState::PluginMissing
        );
        assert_eq!(
            classify(
                &snapshot("trusted"),
                &json!({"config":{"features":{"hooks":false}}}),
                &installed()
            )
            .unwrap()
            .state,
            CodexHookState::Disabled
        );
        let mut managed = snapshot("managed");
        for hook in managed["data"][0]["hooks"].as_array_mut().unwrap() {
            hook["pluginId"] = Value::Null;
            hook["isManaged"] = json!(true);
            hook["command"] = json!("/enterprise/bt trace hook --source codex");
        }
        assert_eq!(
            classify(&managed, &config, &json!({"marketplaces":[]}))
                .unwrap()
                .state,
            CodexHookState::Trusted
        );
    }

    #[test]
    fn unrelated_plugins_and_invalid_responses_never_establish_readiness() {
        let mut hooks = snapshot("trusted");
        for hook in hooks["data"][0]["hooks"].as_array_mut().unwrap() {
            hook["pluginId"] = json!("other@marketplace");
        }
        assert_ne!(
            classify(&hooks, &json!({"config":{}}), &installed())
                .unwrap()
                .state,
            CodexHookState::Trusted
        );
        assert!(classify(&json!({}), &json!({}), &installed()).is_err());
        hooks["data"][0]["errors"] = json!([{"message":"bad hooks"}]);
        assert_eq!(
            classify(&hooks, &json!({"config":{}}), &installed())
                .unwrap()
                .state,
            CodexHookState::ConfigurationError
        );
    }

    #[tokio::test]
    async fn rpc_ignores_notifications_and_reports_unsupported_methods() {
        let mut input = Vec::new();
        let mut output = std::io::Cursor::new(
            b"{\"method\":\"notice\"}\n{\"id\":3,\"error\":{\"message\":\"Method not found\"}}\n",
        );
        let error = request(&mut input, &mut output, 3, "hooks/list", json!({}))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("Method not found"));
        assert_eq!(
            serde_json::from_slice::<Value>(&input).unwrap()["method"],
            "hooks/list"
        );
    }

    #[tokio::test]
    async fn unavailable_codex_and_closed_protocol_report_unknown() {
        let tmp = tempfile::tempdir().unwrap();
        let result = probe_at(
            tmp.path().join("missing-codex").to_str().unwrap(),
            tmp.path(),
            None,
        )
        .await;
        assert_eq!(result.state, CodexHookState::Unknown);
        let mut input = Vec::new();
        let mut output = std::io::Cursor::new(b"");
        assert!(request(&mut input, &mut output, 1, "initialize", json!({}))
            .await
            .unwrap_err()
            .to_string()
            .contains("closed"));
    }

    #[tokio::test]
    #[ignore = "Requires an installed Codex CLI; installs only into an isolated temporary home"]
    async fn installed_codex_distinguishes_missing_and_unreviewed_hooks() {
        let tmp = tempfile::tempdir().unwrap();
        let readiness = probe_at("codex", tmp.path(), Some(tmp.path())).await;
        assert_eq!(
            readiness.state,
            CodexHookState::PluginMissing,
            "{readiness:?}"
        );
        let marketplace = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../src/plugins/codex/content")
            .canonicalize()
            .unwrap();
        let marketplace = marketplace.to_string_lossy();
        for args in [
            vec![
                "plugin",
                "marketplace",
                "add",
                marketplace.as_ref(),
                "--json",
            ],
            vec!["plugin", "add", PLUGIN, "--json"],
            vec!["plugin", "add", PLUGIN, "--json"],
        ] {
            let mut command: tokio::process::Command =
                crate::subprocess::background_command("codex").into();
            let output = command
                .args(["--no-daemon"])
                .args(args)
                .env("CODEX_HOME", tmp.path())
                .current_dir(tmp.path())
                .kill_on_drop(true)
                .output()
                .await
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        let readiness = probe_at("codex", tmp.path(), Some(tmp.path())).await;
        assert_eq!(
            readiness.state,
            CodexHookState::ReviewRequired,
            "{readiness:?}"
        );
        assert_eq!(readiness.review_required_count, EVENTS.len());
        assert!(readiness.missing_events.is_empty());
    }
}
