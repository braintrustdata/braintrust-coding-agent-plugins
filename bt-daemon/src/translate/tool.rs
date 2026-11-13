//! Shared terminal-state conventions for coding-agent tool spans.
//!
//! Translators decide which native events prove execution, failure, or denial.
//! This module only encodes the common Braintrust representation once that
//! source-specific interpretation has been made.

use regex::Regex;
use serde::Serialize;
use serde_json::{json, Map, Value};
use std::path::Path;
use std::sync::OnceLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ToolApproval {
    Approved,
    Denied,
}

impl ToolApproval {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Approved => "approved",
            Self::Denied => "denied",
        }
    }
}

/// Adds approval metadata only when a translator has a reliable native signal.
pub fn add_tool_approval(metadata: &mut Map<String, Value>, approval: Option<ToolApproval>) {
    if let Some(approval) = approval {
        metadata.insert("tool_approval".into(), json!(approval.as_str()));
    }
}

pub fn tool_approval_metadata(approval: Option<ToolApproval>) -> Value {
    let mut metadata = Map::new();
    add_tool_approval(&mut metadata, approval);
    Value::Object(metadata)
}

/// Explicit `/skill`-style requests on the message or turn that made them.
pub fn explicit_skill_metadata(names: &[String]) -> Option<Value> {
    (!names.is_empty()).then(|| {
        json!({
            "loaded_skill_names": names,
            "loaded_skills": names.iter().map(|name| json!({ "name": name })).collect::<Vec<_>>(),
        })
    })
}

/// Compiles a skill-detection pattern, logging instead of panicking if it is
/// invalid so detection degrades to "no skill" rather than taking down tracing.
pub fn compile_skill_regex(pattern: &str) -> Option<Regex> {
    match Regex::new(pattern) {
        Ok(regex) => Some(regex),
        Err(error) => {
            tracing::error!(%error, pattern, "could not compile skill metadata pattern");
            None
        }
    }
}

/// A tool call that loaded a skill, as far as the source reveals it.
#[derive(Default, serde::Serialize, serde::Deserialize)]
pub struct SkillLoad {
    pub name: Option<String>,
    pub path: Option<String>,
}

/// Records a skill load on a tool span. Only a load the user asked for is
/// explicit; the model choosing a skill on its own carries no trigger.
pub fn insert_skill_metadata(
    metadata: &mut Map<String, Value>,
    load: &SkillLoad,
    requested: &[String],
) {
    metadata.insert("tool_kind".into(), json!("skill"));
    if let Some(name) = &load.name {
        metadata.insert("skill_name".into(), json!(name));
        if skill_was_requested(requested, name) {
            metadata.insert("skill_load_trigger".into(), json!("explicit"));
        }
    }
    if let Some(path) = &load.path {
        metadata.insert("skill_path".into(), json!(path));
    }
}

/// Appends `name` unless it is already present, keeping first-seen order.
pub fn push_unique(names: &mut Vec<String>, name: String) {
    if !names.contains(&name) {
        names.push(name);
    }
}

fn is_skill_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | ':' | '-')
}

/// Whether `name` looks like a skill identifier rather than a glob, shell
/// variable, hidden directory, or empty path component.
pub fn is_skill_name(name: &str) -> bool {
    name.starts_with(|c: char| c.is_ascii_alphanumeric()) && name.chars().all(is_skill_char)
}

/// The skill named by a `…/<name>/SKILL.md` path. `find . -name SKILL.md` or
/// `ls skills/*/SKILL.md` mention the file without naming one skill.
pub fn skill_dir_name(path: &str) -> Option<String> {
    let normalized = path.replace('\\', "/");
    Path::new(&normalized)
        .parent()
        .and_then(Path::file_name)
        .and_then(|value| value.to_str())
        .filter(|name| is_skill_name(name))
        .map(str::to_string)
}

/// The skill load for a file path that is itself a `…/<name>/SKILL.md`.
pub fn skill_file_load(path: &str) -> Option<SkillLoad> {
    let normalized = path.replace('\\', "/");
    let file = Path::new(&normalized).file_name()?.to_str()?;
    if !file.eq_ignore_ascii_case("SKILL.md") {
        return None;
    }
    Some(SkillLoad {
        name: Some(skill_dir_name(path)?),
        path: Some(path.to_string()),
    })
}

/// Each concrete `…/<name>/SKILL.md` path in `text`, as a skill load.
pub fn skill_md_paths(text: &str) -> impl Iterator<Item = SkillLoad> + '_ {
    static PATH: OnceLock<Option<Regex>> = OnceLock::new();
    PATH.get_or_init(|| compile_skill_regex(r#"(?i)([^\s"'`()<>]*SKILL\.md)"#))
        .as_ref()
        .into_iter()
        .flat_map(move |pattern| pattern.captures_iter(text))
        .filter_map(|capture| {
            let path = capture.get(1)?.as_str();
            Some(SkillLoad {
                name: Some(skill_dir_name(path)?),
                path: Some(path.to_string()),
            })
        })
}

/// Skills a prompt requests by naming their `SKILL.md` ("Read the skill
/// instructions at docs/.agents/skills/x/SKILL.md"), as directly as a
/// `/x` command would.
pub fn prompt_skill_names(prompt: &str) -> Vec<String> {
    let mut names = Vec::new();
    for name in skill_md_paths(prompt).filter_map(|load| load.name) {
        push_unique(&mut names, name);
    }
    names
}

/// Whether a loaded skill was one the user explicitly requested. Namespaced
/// requests (`plugin:skill`) also match a load identified only by its
/// directory name (`skill`).
pub fn skill_was_requested(requested: &[String], name: &str) -> bool {
    requested.iter().any(|requested| {
        requested == name
            || requested
                .rsplit_once(':')
                .is_some_and(|(_, short)| short == name)
            || name
                .rsplit_once(':')
                .is_some_and(|(_, short)| short == requested)
    })
}

/// Omits fields the source did not report instead of recording them as null.
pub fn without_nulls(mut value: Value) -> Value {
    if let Some(object) = value.as_object_mut() {
        object.retain(|_, value| !value.is_null());
    }
    value
}

pub fn with_tool_approval(mut metadata: Value, approval: Option<ToolApproval>) -> Value {
    if let Some(object) = metadata.as_object_mut() {
        add_tool_approval(object, approval);
    }
    metadata
}

/// Returns a concise, source-provided error message without serializing an
/// arbitrary native payload into the regular span error field.
pub fn error_text(value: Option<&Value>, fallback: &str) -> String {
    value
        .and_then(nonempty_error_text)
        .unwrap_or_else(|| fallback.to_string())
}

/// Extracts a concise message when the source has already identified a value
/// as an error. Callers must not use this to infer failure from normal output.
pub fn nonempty_error_text(value: &Value) -> Option<String> {
    if let Some(text) = value.as_str() {
        return text
            .lines()
            .map(str::trim)
            .find(|line| !line.is_empty())
            .map(str::to_string);
    }
    // MCP and ACP tool results carry their message in `content` text blocks.
    if let Some(values) = value.as_array() {
        return values.iter().find_map(nonempty_error_text);
    }
    let object = value.as_object()?;
    for key in [
        "error", "message", "stderr", "output", "result", "content", "text",
    ] {
        if let Some(text) = object.get(key).and_then(nonempty_error_text) {
            return Some(text);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn approval_is_omitted_when_the_host_cannot_prove_it() {
        let mut metadata = Map::new();
        add_tool_approval(&mut metadata, None);
        assert!(!metadata.contains_key("tool_approval"));
        add_tool_approval(&mut metadata, Some(ToolApproval::Denied));
        assert_eq!(metadata["tool_approval"], "denied");
    }

    #[test]
    fn error_text_keeps_the_useful_nested_message() {
        assert_eq!(
            error_text(
                Some(&json!({"result":{"message":"disk full\ntrace"}})),
                "fallback"
            ),
            "disk full"
        );
        assert_eq!(error_text(None, "fallback"), "fallback");
        assert_eq!(
            error_text(
                Some(&json!({
                    "content":[
                        {"type":"image","data":"aGk="},
                        {"type":"text","text":"ENOENT: missing\ntrace"}
                    ],
                    "details":{}
                })),
                "fallback"
            ),
            "ENOENT: missing"
        );
        assert_eq!(
            error_text(
                Some(&json!({"content":[{"type":"text","text":""}]})),
                "fallback"
            ),
            "fallback"
        );
        assert_eq!(
            nonempty_error_text(&json!("\n  disk full\ntrace")),
            Some("disk full".into())
        );
    }
}
