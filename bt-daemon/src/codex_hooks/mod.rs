//! Inspect and activate Braintrust hooks through Codex's app-server protocol.
//! Trust writes use the same exact-definition config API as Codex's /hooks UI.

use anyhow::{anyhow, Context};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};

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
mod config;
mod config_read;
mod hook;
mod hook_entry;
mod hooks_list;
mod plugin;
mod plugin_list;
mod plugin_marketplace;
mod response;
mod stderr;
mod transport;
mod trust_state;

use config_read::ConfigRead;
use hooks_list::HooksList;
use plugin_list::PluginList;
use stderr::StderrTail;
use transport::request;
use trust_state::TrustState;

pub(crate) async fn activate() -> anyhow::Result<()> {
    activate_with(crate::subprocess::background_command("codex")).await
}

/// Takes the Codex command so the real-Codex contract test can isolate
/// `CODEX_HOME` and select `CODEX_BIN`.
async fn activate_with(command: std::process::Command) -> anyhow::Result<()> {
    let cwd = std::env::current_dir()?;
    let mut command: tokio::process::Command = command.into();
    command
        .args(["--no-daemon", "app-server", "--stdio"])
        .current_dir(&cwd)
        .kill_on_drop(true)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command
        .spawn()
        .context("start Codex to activate tracing hooks")?;
    let stderr = StderrTail::capture(&mut child);
    let result =
        tokio::time::timeout(TIMEOUT, async {
            let mut stdin = child.stdin.take().context("Codex stdin unavailable")?;
            let stdout = child.stdout.take().context("Codex stdout unavailable")?;
            let mut stdout = BufReader::new(stdout.take(8 * 1024 * 1024));
            request::<Value>(&mut stdin, &mut stdout, 1, "initialize", json!({
            "clientInfo":{"name":"braintrust_trace_setup","version":env!("CARGO_PKG_VERSION")},
            "capabilities":{"experimentalApi":true}
        })).await?;
            stdin.write_all(b"{\"method\":\"initialized\"}\n").await?;
            let config: ConfigRead = request(
                &mut stdin,
                &mut stdout,
                2,
                "config/read",
                json!({"includeLayers":false}),
            )
            .await?;
            anyhow::ensure!(
                config.config.hooks_enabled(),
                "Codex hooks are disabled in config; enable hooks and retry setup"
            );
            let plugins: PluginList = request(
                &mut stdin,
                &mut stdout,
                3,
                "plugin/list",
                json!({
                    "cwds":[cwd], "forceRefetch":false, "marketplaceKinds":["local"]
                }),
            )
            .await?;
            anyhow::ensure!(
                plugins.has_active_plugin(PLUGIN),
                "Codex tracing plugin is not installed and enabled; rerun `bt trace install codex`"
            );
            let hooks: HooksList = request(
                &mut stdin,
                &mut stdout,
                4,
                "hooks/list",
                json!({"cwds":[cwd]}),
            )
            .await?;
            let edits = trust_edits(&hooks)?;
            if !edits.is_empty() {
                request::<Value>(
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
            let hooks: HooksList = request(
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
        // Inline the cause chain: callers may print only the top-level message.
        Ok(result) => stderr.attach(result).await.map_err(|error| {
            anyhow!("Codex plugin installed, but hook activation failed: {error:#}")
        }),
        Err(_) => {
            stderr
                .attach(Err(anyhow!(
                    "Codex hook activation timed out after 5 seconds"
                )))
                .await
        }
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
        .stderr(Stdio::piped());
    let mut child = command
        .spawn()
        .context("start Codex to prepare managed hook trust")?;
    let stderr = StderrTail::capture(&mut child);
    let result =
        tokio::time::timeout(TIMEOUT, async {
            let mut stdin = child.stdin.take().context("Codex stdin unavailable")?;
            let stdout = child.stdout.take().context("Codex stdout unavailable")?;
            let mut stdout = BufReader::new(stdout.take(8 * 1024 * 1024));
            request::<Value>(&mut stdin, &mut stdout, 1, "initialize", json!({
            "clientInfo":{"name":"braintrust_trace_run","version":env!("CARGO_PKG_VERSION")},
            "capabilities":{"experimentalApi":true}
        })).await?;
            stdin.write_all(b"{\"method\":\"initialized\"}\n").await?;
            let hooks: HooksList = request(
                &mut stdin,
                &mut stdout,
                2,
                "hooks/list",
                json!({"cwds":[cwd]}),
            )
            .await?;
            hooks.managed_run_trust_state(expected_command)
        })
        .await;
    let _ = child.kill().await;
    let result = match result {
        Ok(result) => result,
        Err(_) => Err(anyhow!(
            "Codex managed hook trust inspection timed out after 5 seconds"
        )),
    };
    stderr.attach(result).await
}

/// Validate the complete installed file, including the other platform's command,
/// before approving exact hashes returned by Codex. Never infer hashes or keys.
fn trust_edits(hooks: &HooksList) -> anyhow::Result<BTreeMap<String, TrustState<'_>>> {
    // Full-file equality is an encoding-boundary check: absent and null
    // fields must remain distinct before trusting Codex's exact hashes.
    let expected: Value = serde_json::from_str(PACKAGED_HOOKS)?;
    let mut checked_paths = std::collections::HashSet::new();
    let mut events = std::collections::HashSet::new();
    let mut edits = BTreeMap::new();
    for entry in &hooks.data {
        for hook in &entry.hooks {
            if hook.plugin_id.as_deref() != Some(PLUGIN) {
                continue;
            }
            let event = hook.event_name.as_deref().context("hook event missing")?;
            anyhow::ensure!(
                PLUGIN_EVENTS.contains(&event)
                    && events.insert(event)
                    && hook.is_enabled_command("bt trace hook --source codex")
                    && hook.source.as_deref() == Some("plugin"),
                "Unexpected or disabled Braintrust hook definition; inspect /hooks"
            );
            let path = hook
                .source_path
                .as_deref()
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
            if hook.trust_status.as_deref() == Some("trusted") {
                continue;
            }
            anyhow::ensure!(
                hook.trust_status.as_deref() == Some("untrusted")
                    || hook.trust_status.as_deref() == Some("modified"),
                "Unknown Braintrust hook trust state"
            );
            let key = hook
                .key
                .as_deref()
                .filter(|s| !s.is_empty())
                .context("hook key missing")?;
            let hash = hook
                .current_hash
                .as_deref()
                .filter(|s| !s.is_empty())
                .context("hook hash missing")?;
            anyhow::ensure!(
                edits
                    .insert(key.to_owned(), TrustState { trusted_hash: hash })
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

#[cfg(test)]
mod tests;
