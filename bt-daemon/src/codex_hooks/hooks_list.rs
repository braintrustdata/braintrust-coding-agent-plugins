use super::hook_entry::HookEntry;
use super::PACKAGED_HOOKS;
use crate::agents::schema::{HookFile, HookGroup};
use anyhow::Context;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub(super) struct HooksList {
    pub(super) data: Vec<HookEntry>,
}

impl HooksList {
    pub(super) fn managed_run_trust_state(
        &self,
        expected_command: &str,
    ) -> anyhow::Result<std::ffi::OsString> {
        let expected: HookFile<HookGroup> = serde_json::from_str(PACKAGED_HOOKS)?;
        let expected_events: std::collections::HashSet<String> = expected
            .hooks
            .keys()
            .map(|event| format!("{}{}", event[..1].to_ascii_lowercase(), &event[1..]))
            .collect();
        let entries = &self.data;
        let mut seen = std::collections::HashSet::new();
        let mut trust = std::collections::BTreeMap::new();
        for entry in entries {
            anyhow::ensure!(
                entry.errors.as_ref().is_some_and(Vec::is_empty),
                "Codex reported hook errors during managed-run setup"
            );
            for hook in &entry.hooks {
                if hook.source.as_deref() != Some("sessionFlags") {
                    continue;
                }
                let Some(event) = hook.event_name.as_deref() else {
                    continue;
                };
                if !expected_events.contains(event) {
                    continue;
                }
                anyhow::ensure!(
                    seen.insert(event.to_owned()) && hook.is_enabled_command(expected_command),
                    "Codex changed a Braintrust hook while preparing this run"
                );
                anyhow::ensure!(
                    matches!(
                        hook.trust_status.as_deref(),
                        Some("trusted" | "untrusted" | "modified")
                    ),
                    "Codex reported an unsupported trust state for a managed Braintrust hook"
                );
                let key = hook
                    .key
                    .as_deref()
                    .filter(|value| !value.is_empty())
                    .context("Codex omitted a managed Braintrust hook key")?;
                let hash = hook
                    .current_hash
                    .as_deref()
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    fn captured_hooks() -> Value {
        json!({"data":[{"errors":[],"hooks":super::super::PLUGIN_EVENTS.iter().map(|event|json!({
            "eventName":event,"source":"sessionFlags","handlerType":"command",
            "command":"expected", "enabled":true,"isManaged":false,
            "trustStatus":"untrusted", "key":format!("key-{event}"),"currentHash":format!("hash-{event}")
        })).collect::<Vec<_>>()}]})
    }

    #[test]
    fn managed_trust_requires_the_complete_unchanged_hook_set() {
        let raw = captured_hooks();
        let observed: HooksList = serde_json::from_value(raw.clone()).unwrap();
        let state = observed.managed_run_trust_state("expected").unwrap();
        let state = state.to_str().unwrap();
        for event in super::super::PLUGIN_EVENTS {
            assert!(state.contains(&format!("hash-{event}")));
        }
        for (field, value) in [
            ("command", json!("changed")),
            ("enabled", json!(false)),
            ("isManaged", json!(true)),
            ("trustStatus", json!("future")),
            ("currentHash", json!("")),
        ] {
            let mut changed = raw.clone();
            changed["data"][0]["hooks"][0][field] = value;
            let observed: HooksList = serde_json::from_value(changed).unwrap();
            assert!(
                observed.managed_run_trust_state("expected").is_err(),
                "{field}"
            );
        }
        let mut missing = raw.clone();
        missing["data"][0]["hooks"].as_array_mut().unwrap().pop();
        let observed: HooksList = serde_json::from_value(missing).unwrap();
        assert!(observed.managed_run_trust_state("expected").is_err());
        let mut duplicated = raw;
        let entry = duplicated["data"][0]["hooks"][0].clone();
        duplicated["data"][0]["hooks"]
            .as_array_mut()
            .unwrap()
            .push(entry);
        let observed: HooksList = serde_json::from_value(duplicated).unwrap();
        assert!(observed.managed_run_trust_state("expected").is_err());
    }
}
