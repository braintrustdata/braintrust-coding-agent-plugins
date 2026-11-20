//! Per-agent, non-credential tracing settings.
//!
//! Every coding agent has an independent user file; a repository can overlay
//! them all with one shared `.braintrust.json`.
//! Authentication and backend URLs deliberately stay with the embedding `bt`
//! CLI. `bt trace run` overlays invocation settings without mutating any
//! persistent agent configuration.

use crate::paths;
use crate::wire::{AuthSource, SessionRoute, TraceDestination};
use anyhow::Context;
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::io::Write;
use std::path::{Path, PathBuf};

pub(crate) const INVOCATION_SETTINGS_ENV: &str = "BT_TRACE_INVOCATION_SETTINGS";

/// Recursively overlay objects and append arrays; scalar values replace.
pub(crate) fn merge_json(base: &mut Value, overlay: Value) {
    match (base, overlay) {
        (Value::Object(base), Value::Object(overlay)) => {
            for (key, value) in overlay {
                match base.get_mut(&key) {
                    Some(existing) => merge_json(existing, value),
                    None => {
                        base.insert(key, value);
                    }
                }
            }
        }
        (Value::Array(base), Value::Array(overlay)) => base.extend(overlay),
        (base, overlay) => *base = overlay,
    }
}

/// Non-secret settings scoped to one `bt trace run` process tree.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct InvocationSettings {
    pub trace_to_braintrust: bool,
    pub route: SessionRoute,
}

impl InvocationSettings {
    pub(crate) fn enabled(route: SessionRoute) -> Self {
        Self {
            trace_to_braintrust: true,
            route,
        }
    }
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct AgentSettings {
    pub trace_to_braintrust: Option<bool>,
    pub route: Option<SessionRoute>,
}

impl AgentSettings {
    pub(crate) fn load_for_hook_payload(
        source: &str,
        payload: Option<&Value>,
    ) -> anyhow::Result<Self> {
        let invocation = std::env::var(INVOCATION_SETTINGS_ENV).ok();
        let user_path = paths::agent_settings_path(source, None);
        let project_path = payload.and_then(|payload| project_settings_path(source, payload));
        Self::load_hook_with_project(&user_path, project_path.as_deref(), invocation.as_deref())
    }

    /// Resolve persistent settings for commands running in a repository.
    pub(crate) fn load_for_directory(
        source: &str,
        cwd: &Path,
    ) -> anyhow::Result<(Self, Option<PathBuf>)> {
        let user = paths::agent_settings_path(source, None);
        let project = paths::project_settings_path(source, cwd)
            .ok()
            .filter(|path| path.is_file());
        let settings = if project.is_some() {
            Self::load_hook_with_project(&user, project.as_deref(), None)?
        } else if user.exists() {
            Self::load_hook_from_sources(&user, None)?
        } else {
            Self::default()
        };
        Ok((settings, project))
    }

    fn load_hook_with_project(
        user_path: &Path,
        project_path: Option<&Path>,
        invocation: Option<&str>,
    ) -> anyhow::Result<Self> {
        let path = match (invocation, project_path) {
            (Some(_), _) | (_, None) => return Self::load_hook_from_sources(user_path, invocation),
            (None, Some(path)) => path,
        };
        let raw = std::fs::read_to_string(path).map_err(|error| {
            anyhow::anyhow!("read tracing settings at {}: {error}", path.display())
        })?;
        let project: Value = serde_json::from_str(&raw).map_err(|error| {
            anyhow::anyhow!("parse tracing settings at {}: {error}", path.display())
        })?;
        let object = project.as_object().ok_or_else(|| {
            anyhow::anyhow!("tracing settings at {} must be an object", path.display())
        })?;
        let mut settings = if user_path.exists() {
            Self::load_hook_from_sources(user_path, None)?
        } else {
            Self::default()
        };
        let inherit_plugins = object
            .get("inherit_plugins")
            .map(|value| {
                value.as_bool().ok_or_else(|| {
                    anyhow::anyhow!("inherit_plugins in {} must be a boolean", path.display())
                })
            })
            .transpose()?
            .unwrap_or(true);
        if !inherit_plugins {
            if let Some(route) = &mut settings.route {
                route.span_plugins.clear();
            }
        }
        if let Some(project_route) = object.get("route") {
            let project_route = project_route.as_object().ok_or_else(|| {
                anyhow::anyhow!("project route in {} must be an object", path.display())
            })?;
            let mut project_route = project_route.clone();
            if let Some(plugins) = project_route.get("span_plugins") {
                let plugins: Vec<PathBuf> = serde_json::from_value(plugins.clone())
                    .with_context(|| format!("invalid span plugins at {}", path.display()))?;
                let directory = path
                    .parent()
                    .ok_or_else(|| anyhow::anyhow!("project settings path has no parent"))?;
                let plugins: Vec<PathBuf> = plugins
                    .into_iter()
                    .map(|plugin| {
                        let full = if plugin.is_absolute() {
                            plugin
                        } else {
                            directory.join(plugin)
                        };
                        std::fs::canonicalize(&full).unwrap_or(full)
                    })
                    .collect();
                project_route.insert("span_plugins".into(), serde_json::to_value(plugins)?);
            }
            let mut route = serde_json::to_value(settings.route.take().unwrap_or_default())?;
            if project_route
                .get("auth")
                .and_then(Value::as_object)
                .is_some_and(|auth| {
                    auth.contains_key("profile") && !auth.contains_key("profile_id")
                })
            {
                if let Some(auth) = route.get_mut("auth").and_then(Value::as_object_mut) {
                    auth.remove("profile_id");
                    auth.insert("source".into(), Value::String("saved_profile".into()));
                }
            }
            if project_route
                .get("destination")
                .and_then(Value::as_object)
                .is_some_and(|destination| {
                    destination.contains_key("project_name")
                        && !destination.contains_key("project_id")
                })
            {
                if let Some(destination) =
                    route.get_mut("destination").and_then(Value::as_object_mut)
                {
                    destination.remove("project_id");
                }
            }
            merge_json(&mut route, Value::Object(project_route));
            settings.route = Some(
                serde_json::from_value(route)
                    .with_context(|| format!("invalid project route at {}", path.display()))?,
            );
        }
        if let Some(value) = object.get("trace_to_braintrust") {
            settings.trace_to_braintrust = Some(serde_json::from_value(value.clone())?);
        }
        if let Some(project) = object.get("project") {
            let name = project
                .as_str()
                .filter(|name| !name.trim().is_empty())
                .ok_or_else(|| {
                    anyhow::anyhow!("project in {} must be a nonempty string", path.display())
                })?;
            let route = settings.route.get_or_insert_with(SessionRoute::default);
            route.destination = Some(TraceDestination::ProjectLogs {
                project_id: None,
                project_name: Some(name.to_owned()),
            });
        }
        for (field, is_profile) in [("profile", true), ("org_name", false)] {
            if let Some(value) = object.get(field) {
                let value = value
                    .as_str()
                    .filter(|s| !s.trim().is_empty())
                    .ok_or_else(|| {
                        anyhow::anyhow!("{field} in {} must be a nonempty string", path.display())
                    })?;
                let route = settings.route.get_or_insert_with(SessionRoute::default);
                if is_profile {
                    route.auth.source = AuthSource::SavedProfile;
                    route.auth.profile_id = None;
                    route.auth.profile = Some(value.to_owned());
                } else {
                    route.auth.org_name = Some(value.to_owned());
                }
            }
        }
        if let Some(tags) = object.get("tags") {
            let tags: std::collections::BTreeSet<String> = serde_json::from_value(tags.clone())
                .with_context(|| format!("invalid project tags at {}", path.display()))?;
            settings
                .route
                .get_or_insert_with(SessionRoute::default)
                .tags
                .extend(tags);
        }
        if let Some(metadata) = object.get("additional_metadata") {
            if !metadata.is_object() {
                anyhow::bail!("project metadata at {} must be an object", path.display());
            }
            let route = settings.route.get_or_insert_with(SessionRoute::default);
            match &mut route.additional_metadata {
                Some(existing) => merge_json(existing, metadata.clone()),
                None => route.additional_metadata = Some(metadata.clone()),
            }
        }
        Ok(settings)
    }

    fn load_hook_from_sources(path: &Path, invocation: Option<&str>) -> anyhow::Result<Self> {
        if let Some(raw) = invocation {
            let settings: InvocationSettings = serde_json::from_str(raw).map_err(|error| {
                anyhow::anyhow!("invalid managed run tracing settings: {error}")
            })?;
            return Ok(Self {
                trace_to_braintrust: Some(settings.trace_to_braintrust),
                route: Some(settings.route),
            });
        }
        let raw = std::fs::read_to_string(path).map_err(|error| {
            anyhow::anyhow!("read tracing settings at {}: {error}", path.display())
        })?;
        let settings: Self = serde_json::from_str(&raw).map_err(|error| {
            anyhow::anyhow!("parse tracing settings at {}: {error}", path.display())
        })?;
        if settings.trace_to_braintrust.is_none() {
            anyhow::bail!(
                "tracing settings at {} have no `trace_to_braintrust` value",
                path.display()
            );
        }
        Ok(settings)
    }

    #[cfg(test)]
    fn load_from_sources(path: &Path, invocation: Option<&str>) -> Self {
        let mut settings = Self::load_from(path);
        if let Some(raw) = invocation {
            match serde_json::from_str::<InvocationSettings>(raw) {
                Ok(invocation) => {
                    settings.trace_to_braintrust = Some(invocation.trace_to_braintrust);
                    settings.route = Some(invocation.route);
                }
                Err(error) => {
                    tracing::warn!("managed run settings ignored: {error}");
                    // Never fall back to the persistent route for a managed
                    // child whose invocation selection cannot be decoded.
                    settings.trace_to_braintrust = Some(false);
                    settings.route = None;
                }
            }
        }
        settings
    }

    #[cfg(test)]
    fn load_from(path: &Path) -> Self {
        let raw = match std::fs::read_to_string(path) {
            Ok(raw) => raw,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Self::default(),
            Err(error) => {
                tracing::warn!(path = %path.display(), "shared daemon settings ignored: {error}");
                return Self::default();
            }
        };
        match serde_json::from_str(&raw) {
            Ok(settings) => settings,
            Err(error) => {
                tracing::warn!(path = %path.display(), "shared daemon settings ignored: {error}");
                Self::default()
            }
        }
    }

    pub(crate) fn tracing_enabled(&self) -> bool {
        self.trace_to_braintrust.unwrap_or(false)
    }
}

fn project_settings_path(source: &str, payload: &Value) -> Option<PathBuf> {
    if !paths::supports_project_settings(source) {
        return None;
    }
    let roots: Vec<&str> = match source {
        "cursor" => payload
            .get("workspace_roots")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .collect(),
        "antigravity" => payload
            .get("workspacePaths")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .collect(),
        _ => payload
            .get("cwd")
            .and_then(Value::as_str)
            .into_iter()
            .collect(),
    };
    if roots.is_empty() {
        let current = std::env::current_dir().ok()?;
        let root = paths::repository_root(&current).ok()?;
        let candidate = root.join(".braintrust.json");
        return candidate.is_file().then_some(candidate);
    }
    for root in roots {
        let path = Path::new(root);
        if !path.is_absolute() {
            continue;
        }
        let root = paths::repository_root(path).unwrap_or_else(|_| path.to_path_buf());
        let candidate = root.join(".braintrust.json");
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

/// Replace a still-matching name-only route with the host-canonical route.
/// The caller must hold the settings lock used by setup while invoking this.
pub(crate) fn migrate_persisted_route(
    source: &str,
    legacy_route: &SessionRoute,
    canonical_route: &SessionRoute,
) -> anyhow::Result<bool> {
    let path = paths::agent_settings_path(source, None);
    migrate_persisted_route_at(&path, legacy_route, canonical_route)
}

fn migrate_persisted_route_at(
    path: &Path,
    legacy_route: &SessionRoute,
    canonical_route: &SessionRoute,
) -> anyhow::Result<bool> {
    with_settings_lock(path, || {
        let raw = match std::fs::read_to_string(path) {
            Ok(raw) => raw,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error.into()),
        };
        let mut settings: Map<String, Value> = serde_json::from_str(&raw)?;
        let Some(stored) = settings.get("route") else {
            return Ok(false);
        };
        let stored: SessionRoute = serde_json::from_value(stored.clone())?;
        // Do not overwrite a route that setup changed while resolution was in
        // flight. `same_route` deliberately treats a legacy route and its
        // stable-ID successor as equivalent for journal replay, so the
        // persistence guard must compare their exact serialized forms.
        if serde_json::to_value(&stored)? != serde_json::to_value(legacy_route)? {
            return Ok(false);
        }
        settings.insert("route".into(), serde_json::to_value(canonical_route)?);
        write_json_atomic(path, &settings)?;
        Ok(true)
    })
}

/// Serialize all Braintrust-owned updates to an agent settings file. The lock
/// stays separate from the JSON file so atomic replacement never drops it.
pub(crate) fn with_settings_lock<T>(
    path: &Path,
    action: impl FnOnce() -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("configuration path has no parent: {}", path.display()))?;
    std::fs::create_dir_all(parent)?;
    let sibling_lock = || {
        path.with_extension(format!(
            "{}lock",
            path.extension()
                .and_then(|extension| extension.to_str())
                .unwrap_or_default()
        ))
    };
    // Keep repository locks out of the working tree. Worktrees use a .git file
    // pointing to their own Git directory; ordinary repositories use a directory.
    let lock_path = if path
        .file_name()
        .is_some_and(|name| name == ".braintrust.json")
    {
        let git = parent.join(".git");
        let git_dir = if git.is_dir() {
            Some(git.clone())
        } else {
            std::fs::read_to_string(&git)
                .ok()
                .and_then(|contents| contents.trim().strip_prefix("gitdir: ").map(str::to_owned))
                .map(|directory| parent.join(directory))
                .filter(|directory| directory.is_dir())
        };
        git_dir
            .map(|directory| directory.join("braintrust-config.lock"))
            .unwrap_or_else(sibling_lock)
    } else {
        sibling_lock()
    };
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(lock_path)?;
    lock.lock_exclusive()?;
    let result = action();
    FileExt::unlock(&lock)?;
    result
}

fn write_json_atomic(path: &Path, settings: &Map<String, Value>) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("configuration path has no parent: {}", path.display()))?;
    let mut encoded = serde_json::to_string_pretty(&Value::Object(settings.clone()))?;
    encoded.push('\n');
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    // Protect the replacement before it takes the settings file's place;
    // otherwise it would carry the directory's inherited access.
    paths::restrict_file_to_owner(temporary.path())?;
    temporary.write_all(encoded.as_bytes())?;
    temporary.persist(path).map_err(|error| error.error)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn project_settings_override_user_and_managed_run_overrides_both() {
        use super::*;
        let temp = tempfile::tempdir().unwrap();
        let user = temp.path().join("user.json");
        let project = temp.path().join("project.json");
        std::fs::write(
            &user,
            r#"{"trace_to_braintrust":false,"route":{"tags":["saved"]}}"#,
        )
        .unwrap();
        std::fs::write(
            &project,
            r#"{"trace_to_braintrust":true,"project":"repo-project"}"#,
        )
        .unwrap();
        let settings = AgentSettings::load_hook_with_project(&user, Some(&project), None).unwrap();
        assert!(settings.tracing_enabled());
        let route = settings.route.unwrap();
        assert!(route.tags.contains("saved"));
        assert!(
            matches!(route.destination, Some(TraceDestination::ProjectLogs { project_name: Some(ref name), .. }) if name == "repo-project")
        );
        let invocation = r#"{"trace_to_braintrust":false,"route":{}}"#;
        assert!(
            !AgentSettings::load_hook_with_project(&user, Some(&project), Some(invocation))
                .unwrap()
                .tracing_enabled()
        );
        std::fs::write(&user, r#"{"trace_to_braintrust":false}"#).unwrap();
        assert!(
            AgentSettings::load_hook_with_project(&user, Some(&project), None)
                .unwrap()
                .tracing_enabled()
        );
        assert!(AgentSettings::load_hook_with_project(
            &user,
            Some(&project),
            Some(r#"{"trace_to_braintrust":true,"route":{}}"#)
        )
        .unwrap()
        .tracing_enabled());
        std::fs::write(&project, r#"{"trace_to_braintrust":false}"#).unwrap();
        assert!(
            !AgentSettings::load_hook_with_project(&user, Some(&project), None)
                .unwrap()
                .tracing_enabled()
        );
        std::fs::remove_file(&user).unwrap();
        std::fs::write(&project, r#"{"trace_to_braintrust":true,"project":"new"}"#).unwrap();
        assert!(
            AgentSettings::load_hook_with_project(&user, Some(&project), None)
                .unwrap()
                .tracing_enabled()
        );
        std::fs::write(
            &project,
            r#"{"trace_to_braintrust":true,"route":{"span_plugins":["./malicious.js"]}}"#,
        )
        .unwrap();
        let loaded = AgentSettings::load_hook_with_project(&user, Some(&project), None).unwrap();
        assert_eq!(
            loaded.route.unwrap().span_plugins,
            [temp.path().join("./malicious.js")]
        );
        std::fs::write(&project, "invalid").unwrap();
        assert!(AgentSettings::load_hook_with_project(&user, Some(&project), None).is_err());
    }

    #[test]
    fn project_route_recursively_overlays_user_settings() {
        let temp = tempfile::tempdir().unwrap();
        let user = temp.path().join("user.json");
        let project = temp.path().join("project.json");
        std::fs::write(&user, r#"{"trace_to_braintrust":false,"route":{"auth":{"profile_id":"old","org_name":"org"},"destination":{"type":"project_logs","project_id":"old","project_name":"global"},"tags":["user"],"span_plugins":["trusted"],"additional_metadata":{"nested":{"one":1,"two":2},"list":[1,2]}}}"#).unwrap();
        std::fs::write(&project, r#"{"trace_to_braintrust":true,"route":{"auth":{"profile":"local"},"destination":{"project_name":"new"},"tags":["project"],"span_plugins":[],"additional_metadata":{"nested":{"two":3},"list":[4]}}}"#).unwrap();
        let route = AgentSettings::load_hook_with_project(&user, Some(&project), None)
            .unwrap()
            .route
            .unwrap();
        assert_eq!(route.span_plugins, [PathBuf::from("trusted")]);
        assert_eq!(route.auth.profile_id, None);
        assert_eq!(route.auth.profile.as_deref(), Some("local"));
        assert_eq!(route.auth.org_name.as_deref(), Some("org"));
        assert!(
            matches!(route.destination, Some(TraceDestination::ProjectLogs { project_id: None, project_name: Some(ref name) }) if name == "new")
        );
        assert_eq!(
            route.tags.into_iter().collect::<Vec<_>>(),
            ["project", "user"]
        );
        assert_eq!(
            route.additional_metadata,
            Some(serde_json::json!({"nested":{"one":1,"two":3},"list":[1,2,4]}))
        );
    }

    #[test]
    fn project_settings_locks_stay_in_git_metadata_for_repos_and_worktrees() {
        for worktree in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let repo = temp.path().join("repo");
            let metadata = if worktree {
                temp.path().join("worktree-git")
            } else {
                repo.join(".git")
            };
            std::fs::create_dir_all(&metadata).unwrap();
            if worktree {
                std::fs::create_dir_all(&repo).unwrap();
                std::fs::write(
                    repo.join(".git"),
                    format!("gitdir: {}\n", metadata.display()),
                )
                .unwrap();
            }
            let path = repo.join(".braintrust.json");
            with_settings_lock(&path, || {
                std::fs::write(&path, "{}")?;
                Ok(())
            })
            .unwrap();
            assert!(metadata.join("braintrust-config.lock").exists());
            assert!(!repo.join(".braintrust.jsonlock").exists());
        }
    }

    #[test]
    fn project_plugins_append_in_order_or_replace_the_user_chain() {
        let temp = tempfile::tempdir().unwrap();
        let user = temp.path().join("user.json");
        let project = temp.path().join(".braintrust.json");
        let plugin = temp.path().join("plugin.mjs");
        std::fs::write(&plugin, "export default span => span").unwrap();
        std::fs::write(
            &user,
            r#"{"trace_to_braintrust":true,"route":{"span_plugins":["user.mjs"]}}"#,
        )
        .unwrap();
        std::fs::write(&project, r#"{"route":{"span_plugins":["./plugin.mjs"]}}"#).unwrap();
        let route = AgentSettings::load_hook_with_project(&user, Some(&project), None)
            .unwrap()
            .route
            .unwrap();
        assert_eq!(
            route.span_plugins,
            [PathBuf::from("user.mjs"), plugin.canonicalize().unwrap()]
        );
        std::fs::write(
            &project,
            r#"{"inherit_plugins":false,"route":{"span_plugins":["./plugin.mjs"]}}"#,
        )
        .unwrap();
        let route = AgentSettings::load_hook_with_project(&user, Some(&project), None)
            .unwrap()
            .route
            .unwrap();
        assert_eq!(route.span_plugins, [plugin.canonicalize().unwrap()]);
        std::fs::write(&project, r#"{"inherit_plugins":false}"#).unwrap();
        assert!(
            AgentSettings::load_hook_with_project(&user, Some(&project), None)
                .unwrap()
                .route
                .unwrap()
                .span_plugins
                .is_empty()
        );
    }

    #[test]
    fn resolved_project_plugin_runs_after_user_plugin() {
        use crate::translate::{SpanOp, SpanRow};
        let temp = tempfile::tempdir().unwrap();
        let user_config = temp.path().join("user.json");
        let project_config = temp.path().join(".braintrust.json");
        let user_plugin = temp.path().join("user.mjs");
        let project_plugin = temp.path().join("project.mjs");
        std::fs::write(
            &user_plugin,
            "export default span => ({...span, name: `${span.name}:user`})",
        )
        .unwrap();
        std::fs::write(
            &project_plugin,
            "export default span => ({...span, name: `${span.name}:project`})",
        )
        .unwrap();
        std::fs::write(
            &user_config,
            serde_json::json!({"trace_to_braintrust":true,"route":{"span_plugins":[user_plugin]}})
                .to_string(),
        )
        .unwrap();
        let row = SpanOp::Insert(SpanRow {
            span_id: "span".into(),
            root_span_id: "root".into(),
            name: "start".into(),
            ..Default::default()
        });
        for (inherit, expected) in [(true, "start:user:project"), (false, "start:project")] {
            std::fs::write(&project_config, serde_json::json!({"inherit_plugins":inherit,"route":{"span_plugins":["./project.mjs"]}}).to_string()).unwrap();
            let route =
                AgentSettings::load_hook_with_project(&user_config, Some(&project_config), None)
                    .unwrap()
                    .route
                    .unwrap();
            let result =
                crate::span_processor::process(&route.span_plugins, &row, "claude", "test")
                    .unwrap();
            let Some(SpanOp::Insert(row)) = result.op else {
                panic!("plugins did not produce a span")
            };
            assert_eq!(row.name, expected);
        }
    }

    #[test]
    fn project_fields_inherit_and_override_the_user_route() {
        let temp = tempfile::tempdir().unwrap();
        let user = temp.path().join("user.json");
        let project = temp.path().join("project.json");
        std::fs::write(&user, r#"{"trace_to_braintrust":false,"route":{"auth":{"source":"environment","profile_id":"old","org_name":"org"},"destination":{"type":"project_logs","project_name":"global"},"tags":["user"],"additional_metadata":{"origin":"user"},"span_plugins":["trusted.js"]}}"#).unwrap();
        std::fs::write(
            &project,
            r#"{"trace_to_braintrust":true,"profile":"new","project":"local"}"#,
        )
        .unwrap();
        let settings = AgentSettings::load_hook_with_project(&user, Some(&project), None).unwrap();
        assert!(settings.tracing_enabled());
        let route = settings.route.unwrap();
        assert_eq!(route.auth.source, AuthSource::SavedProfile);
        assert_eq!(route.auth.profile.as_deref(), Some("new"));
        assert_eq!(route.auth.profile_id, None);
        assert_eq!(route.auth.org_name.as_deref(), Some("org"));
        assert!(
            matches!(route.destination, Some(TraceDestination::ProjectLogs { project_name: Some(ref name), .. }) if name == "local")
        );
        assert!(route.tags.contains("user"));
        assert_eq!(
            route.additional_metadata,
            Some(serde_json::json!({"origin":"user"}))
        );
        assert_eq!(route.span_plugins, vec![PathBuf::from("trusted.js")]);
    }

    #[test]
    fn finds_one_shared_repository_file_for_every_agent() {
        use super::*;
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path().join("repo");
        let nested = repo.join("src/nested");
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::create_dir(repo.join(".git")).unwrap();
        std::fs::create_dir(repo.join(".claude")).unwrap();
        std::fs::write(repo.join(".claude/braintrust.json"), "{}").unwrap();
        assert!(project_settings_path("claude", &serde_json::json!({"cwd": nested})).is_none());
        let path = repo.join(".braintrust.json");
        std::fs::write(&path, "{}").unwrap();
        for source in [
            "claude",
            "cursor",
            "codex",
            "muse",
            "grok",
            "antigravity",
            "pi",
            "opencode",
        ] {
            let payload = match source {
                "cursor" => serde_json::json!({"workspace_roots": [nested]}),
                "antigravity" => serde_json::json!({"workspacePaths": [nested]}),
                _ => serde_json::json!({"cwd": nested}),
            };
            assert_eq!(project_settings_path(source, &payload), Some(path.clone()));
        }
        let outside = temp.path().join("outside");
        std::fs::create_dir(&outside).unwrap();
        assert!(project_settings_path("claude", &serde_json::json!({"cwd": outside})).is_none());
    }
    use super::*;

    #[test]
    fn shared_file_contains_behavior_only() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("config.json");
        std::fs::write(
            &path,
            r#"{
                "trace_to_braintrust": true,
                "route": {
                    "destination": {"type": "project_logs", "project_name": "agents"},
                    "flush_mode": "fire_and_forget",
                    "additional_metadata": {"team": "platform"}
                },
                "apiKey": "ignored",
                "apiUrl": "https://ignored.example"
            }"#,
        )
        .unwrap();

        let settings = AgentSettings::load_from(&path);
        assert_eq!(settings.trace_to_braintrust, Some(true));
        let route = settings.route.unwrap();
        assert_eq!(route.destination.unwrap().project_name(), Some("agents"));
        assert_eq!(route.additional_metadata.unwrap()["team"], "platform");
    }

    #[test]
    fn malformed_or_missing_settings_are_fail_open() {
        let temp = tempfile::tempdir().unwrap();
        assert!(AgentSettings::load_from(&temp.path().join("missing.json"))
            .route
            .is_none());
        let malformed = temp.path().join("malformed.json");
        std::fs::write(&malformed, "{").unwrap();
        assert!(AgentSettings::load_from(&malformed).route.is_none());
    }

    #[test]
    fn hook_settings_use_managed_override_without_persistent_settings() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("braintrust.json");
        let invocation = r#"{"trace_to_braintrust":true,"route":{}}"#;
        let settings = AgentSettings::load_hook_from_sources(&path, Some(invocation)).unwrap();
        assert!(settings.tracing_enabled());
        assert!(settings.route.is_some());
        assert!(AgentSettings::load_hook_from_sources(&path, Some("{"))
            .unwrap_err()
            .to_string()
            .contains("invalid managed run tracing settings"));
    }

    #[test]
    fn tracing_enablement_comes_only_from_stored_settings() {
        let enabled = AgentSettings {
            trace_to_braintrust: Some(true),
            ..Default::default()
        };
        assert!(enabled.tracing_enabled());

        let disabled = AgentSettings {
            trace_to_braintrust: Some(false),
            ..Default::default()
        };
        assert!(!disabled.tracing_enabled());

        assert!(!AgentSettings::default().tracing_enabled());
    }

    #[test]
    fn invocation_settings_override_setup_without_mutating_it() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("config.json");
        std::fs::write(
            &path,
            r#"{
                "trace_to_braintrust": false,
                "route": {
                    "auth": {"profile": "global", "org_name": "global-org"},
                    "destination": {"type": "project_logs", "project_name": "global-project"}
                }
            }"#,
        )
        .unwrap();

        let invocation = |profile: &str, project: &str| {
            serde_json::to_string(&InvocationSettings::enabled(SessionRoute {
                auth: crate::wire::AuthSelection {
                    source: crate::wire::AuthSource::SavedProfile,
                    profile_id: None,
                    profile: Some(profile.to_string()),
                    org_name: Some(format!("{profile}-org")),
                },
                destination: Some(crate::wire::TraceDestination::ProjectLogs {
                    project_id: None,
                    project_name: Some(project.to_string()),
                }),
                additional_metadata: Some(serde_json::json!({"profile": profile})),
                ..SessionRoute::default()
            }))
            .unwrap()
        };

        let work =
            AgentSettings::load_from_sources(&path, Some(&invocation("work", "work-project")));
        let personal = AgentSettings::load_from_sources(
            &path,
            Some(&invocation("personal", "personal-project")),
        );
        let global = AgentSettings::load_from_sources(&path, None);

        assert!(work.tracing_enabled());
        assert!(personal.tracing_enabled());
        let work_route = work.route.unwrap();
        assert_eq!(work_route.auth.profile.as_deref(), Some("work"));
        assert_eq!(
            work_route.additional_metadata,
            Some(serde_json::json!({"profile": "work"}))
        );
        let personal_route = personal.route.unwrap();
        assert_eq!(personal_route.auth.profile.as_deref(), Some("personal"));
        assert_eq!(
            personal_route.additional_metadata,
            Some(serde_json::json!({"profile": "personal"}))
        );
        assert!(!global.tracing_enabled());
        let global_route = global.route.unwrap();
        assert_eq!(global_route.auth.profile.as_deref(), Some("global"));
        assert_eq!(
            global_route.destination.unwrap().project_name(),
            Some("global-project")
        );
    }

    #[test]
    fn malformed_invocation_settings_do_not_fall_back_to_setup_route() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("config.json");
        std::fs::write(
            &path,
            r#"{
                "trace_to_braintrust": true,
                "route": {
                    "destination": {"type": "project_logs", "project_name": "global-project"}
                }
            }"#,
        )
        .unwrap();

        let settings = AgentSettings::load_from_sources(&path, Some("{"));
        assert!(!settings.tracing_enabled());
        assert!(settings.route.is_none());
    }

    fn legacy_and_canonical_routes() -> (SessionRoute, SessionRoute) {
        let legacy = SessionRoute {
            auth: crate::wire::AuthSelection {
                source: crate::wire::AuthSource::Auto,
                profile_id: None,
                profile: Some("test-profile".into()),
                org_name: Some("test-org".into()),
            },
            destination: Some(crate::wire::TraceDestination::ProjectLogs {
                project_id: None,
                project_name: Some("test-project".into()),
            }),
            ..SessionRoute::default()
        };
        let mut canonical = legacy.clone();
        canonical.auth.source = crate::wire::AuthSource::SavedProfile;
        canonical.auth.profile_id = Some("00000000-0000-4000-8000-000000000001".into());
        (legacy, canonical)
    }

    #[test]
    fn migration_replaces_only_the_matching_legacy_route() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("config.json");
        let (legacy, canonical) = legacy_and_canonical_routes();
        let settings = serde_json::json!({
            "trace_to_braintrust": true,
            "route": legacy,
            "unrelated": { "preserved": true },
        });
        std::fs::write(&path, serde_json::to_vec(&settings).unwrap()).unwrap();

        assert!(migrate_persisted_route_at(&path, &legacy, &canonical).unwrap());
        let migrated: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(
            migrated["route"]["auth"]["profile_id"],
            "00000000-0000-4000-8000-000000000001"
        );
        assert_eq!(migrated["unrelated"]["preserved"], true);

        assert!(!migrate_persisted_route_at(&path, &legacy, &canonical).unwrap());
    }

    #[test]
    fn migration_keeps_settings_private_to_the_owner() {
        let temp = tempfile::tempdir().unwrap();
        let config = temp.path().join("config");
        std::fs::create_dir(&config).unwrap();
        // Replacements inherit from a configuration directory others can read.
        #[cfg(windows)]
        crate::win_acl::test_support::set_sddl(&config, "D:P(A;OICI;FA;;;{user})(A;OICI;FA;;;WD)");
        let path = config.join("braintrust.json");
        let (legacy, canonical) = legacy_and_canonical_routes();
        let settings = serde_json::json!({ "trace_to_braintrust": true, "route": legacy });
        std::fs::write(&path, serde_json::to_vec(&settings).unwrap()).unwrap();
        paths::restrict_file_to_owner(&path).unwrap();

        assert!(migrate_persisted_route_at(&path, &legacy, &canonical).unwrap());

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        #[cfg(windows)]
        {
            use crate::win_acl::test_support::{assert_owner_only, path_dacl_sddl};
            assert_owner_only(&path_dacl_sddl(&path), false);
        }
    }
}
