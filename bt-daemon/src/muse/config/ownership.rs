//! Strict owned-file recognition, separate from preservable foreign documents.
use super::hooks::{opaque_contains_braintrust, CommandType, HookDocument};
use super::{HOOK_COMMAND, LEGACY_HOOK_COMMAND};
use crate::muse::field::Field;
use crate::muse::hooks::HookName;
use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OwnedHookFile {
    schema_version: u32,
    hooks: BTreeMap<HookName, Vec<OwnedHookGroup>>,
}
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnedHookGroup {
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    matcher: Field<String>,
    hooks: Vec<OwnedCommandHook>,
}
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnedCommandHook {
    #[serde(rename = "type")]
    kind: CommandType,
    command: String,
}
impl OwnedHookFile {
    fn for_events(events: &[HookName], command: &str, matcher: bool) -> Self {
        Self {
            schema_version: 1,
            hooks: events
                .iter()
                .map(|event| {
                    (
                        *event,
                        vec![OwnedHookGroup {
                            matcher: if matcher {
                                Field::Value("*".into())
                            } else {
                                Field::Missing
                            },
                            hooks: vec![OwnedCommandHook {
                                kind: CommandType::Command,
                                command: command.into(),
                            }],
                        }],
                    )
                })
                .collect(),
        }
    }
    pub fn current() -> Self {
        Self::for_events(HookName::ALL, HOOK_COMMAND, false)
    }
    pub fn previous() -> Self {
        Self::for_events(
            &[
                HookName::SessionStart,
                HookName::UserPromptSubmit,
                HookName::PreLLMCall,
                HookName::PostLLMCall,
                HookName::Stop,
                HookName::SessionEnd,
            ],
            HOOK_COMMAND,
            true,
        )
    }
    fn legacy(matcher: bool) -> Self {
        Self::for_events(
            &[
                HookName::SessionStart,
                HookName::UserPromptSubmit,
                HookName::PreToolUse,
                HookName::PermissionRequest,
                HookName::PostToolUse,
                HookName::PreLLMCall,
                HookName::PostLLMCall,
                HookName::PreCompact,
                HookName::PostCompact,
                HookName::SubagentStart,
                HookName::SubagentStop,
                HookName::Stop,
                HookName::SessionEnd,
            ],
            LEGACY_HOOK_COMMAND,
            matcher,
        )
    }
    fn is_recognized(&self) -> bool {
        self == &Self::current()
            || self == &Self::previous()
            || self == &Self::legacy(true)
            || self == &Self::legacy(false)
    }
}
#[derive(Debug, PartialEq)]
pub(crate) enum HookFileInspection {
    Missing,
    Owned,
    Foreign { contains_braintrust: bool },
}
impl HookFileInspection {
    pub fn is_owned(&self) -> bool {
        matches!(self, Self::Owned)
    }
    pub fn contains_braintrust(&self) -> bool {
        matches!(
            self,
            Self::Owned
                | Self::Foreign {
                    contains_braintrust: true
                }
        )
    }
}
pub(crate) fn inspect_hook_file(path: &Path) -> anyhow::Result<HookFileInspection> {
    match std::fs::read(path) {
        Ok(raw) => inspect(&raw)
            .with_context(|| format!("read Muse hook configuration {}", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok(HookFileInspection::Missing)
        }
        Err(error) => {
            Err(error).with_context(|| format!("read Muse hook configuration {}", path.display()))
        }
    }
}
fn inspect(raw: &[u8]) -> anyhow::Result<HookFileInspection> {
    // Schema mismatch means foreign, not invalid JSON. Validate syntax without
    // allocating an untyped tree before trying the strict owned schema.
    serde_json::from_slice::<serde::de::IgnoredAny>(raw)?;
    if serde_json::from_slice::<OwnedHookFile>(raw).is_ok_and(|file| file.is_recognized()) {
        return Ok(HookFileInspection::Owned);
    }
    let contains_braintrust = match serde_json::from_slice::<HookDocument>(raw) {
        Ok(document) => document.contains_braintrust(),
        // Unknown root/group formats retain the conservative read-only scan.
        Err(_) => opaque_contains_braintrust(&serde_json::from_slice(raw)?),
    };
    Ok(HookFileInspection::Foreign {
        contains_braintrust,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn recognize_all_exact_owned_versions() {
        for file in [
            OwnedHookFile::current(),
            OwnedHookFile::previous(),
            OwnedHookFile::legacy(true),
            OwnedHookFile::legacy(false),
        ] {
            assert_eq!(
                inspect(&serde_json::to_vec(&file).unwrap()).unwrap(),
                HookFileInspection::Owned
            );
        }
    }
    #[test]
    fn extensions_null_matchers_and_unknown_events_are_never_owned() {
        let original = serde_json::to_value(OwnedHookFile::current()).unwrap();
        let mut variants = Vec::new();
        let mut file = original.clone();
        file["future"] = json!(true);
        variants.push(file);
        let mut file = original.clone();
        file["hooks"]["Stop"][0]["matcher"] = json!(null);
        variants.push(file);
        let mut file = original.clone();
        file["hooks"]["Stop"][0]["future"] = json!(true);
        variants.push(file);
        let mut file = original.clone();
        file["hooks"]["Stop"][0]["hooks"][0]["timeout"] = json!(1000);
        variants.push(file);
        let mut file = original.clone();
        file["hooks"]["FutureEvent"] = json!([]);
        variants.push(file);
        for file in variants {
            assert_eq!(
                inspect(&serde_json::to_vec(&file).unwrap()).unwrap(),
                HookFileInspection::Foreign {
                    contains_braintrust: true
                }
            );
        }
        assert!(inspect(b"{").is_err());
        assert_eq!(
            inspect(b"[]").unwrap(),
            HookFileInspection::Foreign {
                contains_braintrust: false
            }
        );
    }
    #[test]
    fn unknown_file_schema_still_checks_nested_forwarders() {
        let raw = serde_json::to_vec(&json!({"future_schema":[{"command":HOOK_COMMAND}]})).unwrap();
        assert_eq!(
            inspect(&raw).unwrap(),
            HookFileInspection::Foreign {
                contains_braintrust: true
            }
        );
    }
}
