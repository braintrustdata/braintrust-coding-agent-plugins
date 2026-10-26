//! Inspect and activate Braintrust hooks through Codex's app-server protocol.
//! Trust writes use the same exact-definition config API as Codex's /hooks UI.

use anyhow::{anyhow, Context};
use serde_json::{json, Value};
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader,
};

const PLUGIN: &str = "trace-codex@braintrust-codex-plugins";
const PACKAGED_HOOKS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../src/plugins/codex/content/plugins/trace-codex/hooks/hooks.json"
));
const TIMEOUT: Duration = Duration::from_secs(5);
const PLUGIN_EVENTS: &[&str] = &[
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
    "interrupt",
    "sessionEnd",
];
pub(crate) async fn activate() -> anyhow::Result<()> {
    let cwd = std::env::current_dir()?;
    let mut command: tokio::process::Command =
        crate::subprocess::background_command("codex").into();
    command
        .args(["--no-daemon", "app-server", "--stdio"])
        .current_dir(&cwd)
        .kill_on_drop(true)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let mut child = command
        .spawn()
        .context("start Codex to activate tracing hooks")?;
    let result =
        tokio::time::timeout(TIMEOUT, async {
            let mut stdin = child.stdin.take().context("Codex stdin unavailable")?;
            let stdout = child.stdout.take().context("Codex stdout unavailable")?;
            let mut stdout = BufReader::new(stdout.take(8 * 1024 * 1024));
            request(&mut stdin, &mut stdout, 1, "initialize", json!({
            "clientInfo":{"name":"braintrust_trace_setup","version":env!("CARGO_PKG_VERSION")},
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
            let features = &config["config"]["features"];
            anyhow::ensure!(
                features["hooks"] != false && features["codex_hooks"] != false,
                "Codex hooks are disabled in config; enable hooks and retry setup"
            );
            let plugins = request(
                &mut stdin,
                &mut stdout,
                3,
                "plugin/list",
                json!({
                    "cwds":[cwd], "forceRefetch":false, "marketplaceKinds":["local"]
                }),
            )
            .await?;
            let installed = plugins["marketplaces"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|marketplace| marketplace["plugins"].as_array())
                .flatten()
                .any(|plugin| {
                    plugin["id"] == PLUGIN
                        && plugin["installed"] == true
                        && plugin["enabled"] == true
                });
            anyhow::ensure!(
                installed,
                "Codex tracing plugin is not installed and enabled; rerun `bt trace enable codex`"
            );
            let hooks = request(
                &mut stdin,
                &mut stdout,
                4,
                "hooks/list",
                json!({"cwds":[cwd]}),
            )
            .await?;
            let edits = trust_edits(&hooks)?;
            if !edits.is_empty() {
                request(
                    &mut stdin,
                    &mut stdout,
                    5,
                    "config/batchWrite",
                    json!({
                        "edits":[{"keyPath":"hooks.state", "value":edits,"mergeStrategy":"upsert"}],
                        "reloadUserConfig":true
                    }),
                )
                .await?;
            }
            let hooks = request(
                &mut stdin,
                &mut stdout,
                6,
                "hooks/list",
                json!({"cwds":[cwd]}),
            )
            .await?;
            anyhow::ensure!(
                trust_edits(&hooks)?.is_empty(),
                "Codex did not retain Braintrust hook trust after setup"
            );
            Ok(())
        })
        .await;
    let _ = child.kill().await;
    match result {
        Ok(result) => result.context("Codex plugin installed, but hook activation failed"),
        Err(_) => Err(anyhow!("Codex hook activation timed out after 5 seconds")),
    }
}

/// Build a command-line-scoped hooks.state value for Codex managed runs. The
/// keys and hashes come from Codex after it has loaded our exact hook overrides.
pub(crate) async fn run_trust_override(
    program: &std::ffi::OsStr,
    cwd: &Path,
    hook_args: &[std::ffi::OsString],
    expected_command: &str,
) -> anyhow::Result<std::ffi::OsString> {
    let mut command: tokio::process::Command =
        crate::subprocess::background_command(program).into();
    command
        .arg("--no-daemon")
        .args(hook_args)
        .args(["app-server", "--stdio"])
        .current_dir(cwd)
        .kill_on_drop(true)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let mut child = command
        .spawn()
        .context("start Codex to prepare managed hook trust")?;
    let result =
        tokio::time::timeout(TIMEOUT, async {
            let mut stdin = child.stdin.take().context("Codex stdin unavailable")?;
            let stdout = child.stdout.take().context("Codex stdout unavailable")?;
            let mut stdout = BufReader::new(stdout.take(8 * 1024 * 1024));
            request(&mut stdin, &mut stdout, 1, "initialize", json!({
            "clientInfo":{"name":"braintrust_trace_run","version":env!("CARGO_PKG_VERSION")},
            "capabilities":{"experimentalApi":true}
        })).await?;
            stdin.write_all(b"{\"method\":\"initialized\"}\n").await?;
            let hooks = request(
                &mut stdin,
                &mut stdout,
                2,
                "hooks/list",
                json!({"cwds":[cwd]}),
            )
            .await?;
            managed_run_trust_state(&hooks, expected_command)
        })
        .await;
    let _ = child.kill().await;
    match result {
        Ok(result) => result,
        Err(_) => Err(anyhow!(
            "Codex managed hook trust inspection timed out after 5 seconds"
        )),
    }
}

fn managed_run_trust_state(
    hooks: &Value,
    expected_command: &str,
) -> anyhow::Result<std::ffi::OsString> {
    let expected: Value = serde_json::from_str(PACKAGED_HOOKS)?;
    let expected_events: std::collections::HashSet<String> = expected["hooks"]
        .as_object()
        .context("packaged hooks missing")?
        .keys()
        .map(|event| format!("{}{}", event[..1].to_ascii_lowercase(), &event[1..]))
        .collect();
    let entries = hooks["data"]
        .as_array()
        .context("hooks/list has no data array")?;
    let mut seen = std::collections::HashSet::new();
    let mut trust = std::collections::BTreeMap::new();
    for entry in entries {
        anyhow::ensure!(
            entry["errors"].as_array().is_some_and(Vec::is_empty),
            "Codex reported hook errors during managed-run setup"
        );
        for hook in entry["hooks"]
            .as_array()
            .context("hooks/list entry has no hooks array")?
        {
            if hook["source"] != "sessionFlags" {
                continue;
            }
            let Some(event) = hook["eventName"].as_str() else {
                continue;
            };
            if !expected_events.contains(event) {
                continue;
            }
            anyhow::ensure!(
                seen.insert(event.to_owned())
                    && hook["handlerType"] == "command"
                    && hook["command"] == expected_command
                    && hook["enabled"] == true
                    && hook["isManaged"] == false,
                "Codex changed a Braintrust hook while preparing this run"
            );
            anyhow::ensure!(
                matches!(
                    hook["trustStatus"].as_str(),
                    Some("trusted" | "untrusted" | "modified")
                ),
                "Codex reported an unsupported trust state for a managed Braintrust hook"
            );
            let key = hook["key"]
                .as_str()
                .filter(|value| !value.is_empty())
                .context("Codex omitted a managed Braintrust hook key")?;
            let hash = hook["currentHash"]
                .as_str()
                .filter(|value| !value.is_empty())
                .context("Codex omitted a managed Braintrust hook hash")?;
            anyhow::ensure!(
                trust.insert(key.to_owned(), hash.to_owned()).is_none(),
                "duplicate managed Braintrust hook key"
            );
        }
    }
    anyhow::ensure!(
        seen == expected_events,
        "Codex did not load the complete Braintrust hook set for this run"
    );
    let fields = trust
        .iter()
        .map(|(key, hash)| {
            Ok(format!(
                "{}={{trusted_hash={}}}",
                serde_json::to_string(key)?,
                serde_json::to_string(hash)?
            ))
        })
        .collect::<anyhow::Result<Vec<_>>>()?
        .join(",");
    Ok(std::ffi::OsString::from(format!(
        "hooks.state={{{fields}}}"
    )))
}

/// Validate the complete installed file, including the other platform's command,
/// before approving exact hashes returned by Codex. Never infer hashes or keys.
fn trust_edits(hooks: &Value) -> anyhow::Result<serde_json::Map<String, Value>> {
    let expected: Value = serde_json::from_str(PACKAGED_HOOKS)?;
    let mut checked_paths = std::collections::HashSet::new();
    let mut events = std::collections::HashSet::new();
    let mut edits = serde_json::Map::new();
    for entry in hooks["data"]
        .as_array()
        .context("hooks/list has no data array")?
    {
        for hook in entry["hooks"]
            .as_array()
            .context("hooks/list entry has no hooks array")?
        {
            if hook["pluginId"].as_str() != Some(PLUGIN) {
                continue;
            }
            let event = hook["eventName"].as_str().context("hook event missing")?;
            anyhow::ensure!(
                PLUGIN_EVENTS.contains(&event)
                    && events.insert(event)
                    && hook["handlerType"] == "command"
                    && hook["command"] == "bt trace hook --source codex"
                    && hook["enabled"] == true
                    && hook["isManaged"] == false
                    && hook["source"] == "plugin",
                "Unexpected or disabled Braintrust hook definition; inspect /hooks"
            );
            let path = hook["sourcePath"]
                .as_str()
                .context("hook source path missing")?;
            anyhow::ensure!(
                Path::new(path).is_absolute(),
                "hook source path is not absolute"
            );
            if checked_paths.insert(path) {
                let installed: Value = serde_json::from_slice(&std::fs::read(path)?)?;
                anyhow::ensure!(installed == expected,
                    "Installed Braintrust hooks differ from this bt version; update bt and retry setup");
            }
            if hook["trustStatus"] == "trusted" {
                continue;
            }
            anyhow::ensure!(
                hook["trustStatus"] == "untrusted" || hook["trustStatus"] == "modified",
                "Unknown Braintrust hook trust state"
            );
            let key = hook["key"]
                .as_str()
                .filter(|s| !s.is_empty())
                .context("hook key missing")?;
            let hash = hook["currentHash"]
                .as_str()
                .filter(|s| !s.is_empty())
                .context("hook hash missing")?;
            anyhow::ensure!(
                edits
                    .insert(key.to_owned(), json!({"trusted_hash":hash}))
                    .is_none(),
                "Duplicate Braintrust hook key"
            );
        }
    }
    anyhow::ensure!(
        events.len() == PLUGIN_EVENTS.len(),
        "Braintrust lifecycle hooks are missing"
    );
    Ok(edits)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn required_events_match_the_packaged_plugin() {
        let hooks: Value = serde_json::from_str(PACKAGED_HOOKS).unwrap();
        let events: std::collections::BTreeSet<_> = hooks["hooks"]
            .as_object()
            .unwrap()
            .keys()
            .map(|key| format!("{}{}", key[..1].to_ascii_lowercase(), &key[1..]))
            .collect();
        assert_eq!(
            events,
            PLUGIN_EVENTS
                .iter()
                .map(|event| (*event).to_owned())
                .collect()
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
}
