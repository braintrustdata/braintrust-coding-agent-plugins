//! Invocation-local overrides applied to a [`SessionRoute`] before it is
//! used by a live hook, managed run, or transcript import.

use std::path::PathBuf;

use crate::wire::SessionRoute;

/// Apply one invocation-local JSON metadata override to a non-secret route.
///
/// The route is then carried unchanged through live hooks, managed runs, and
/// transcript import. Keeping validation here gives every public command the
/// same contract and prevents individual agent shims from parsing JSON.
pub(crate) fn apply_additional_metadata(
    route: &mut SessionRoute,
    additional_metadata: Option<&str>,
) -> anyhow::Result<()> {
    let Some(metadata) = additional_metadata else {
        return Ok(());
    };
    let value: serde_json::Value = serde_json::from_str(metadata)
        .map_err(|e| anyhow::anyhow!("invalid --additional-metadata JSON: {e}"))?;
    if !value.is_object() {
        anyhow::bail!("--additional-metadata must be a JSON object");
    }
    route.additional_metadata = Some(value);
    Ok(())
}

/// Apply invocation-local root-span tags to a route. Tags are normalized once
/// at the CLI boundary so hook shims and translators only receive valid values.
pub(crate) fn apply_tags(route: &mut SessionRoute, tags: &[String]) -> anyhow::Result<()> {
    if tags.is_empty() {
        return Ok(());
    }
    let mut normalized = Vec::with_capacity(tags.len());
    for tag in tags {
        let tag = tag.trim();
        if tag.is_empty() {
            anyhow::bail!("--tag must not be empty");
        }
        if !normalized.iter().any(|existing| existing == tag) {
            normalized.push(tag.to_string());
        }
    }
    route.tags = normalized;
    Ok(())
}

pub(crate) fn resolve_span_plugin_paths(paths: &[PathBuf]) -> anyhow::Result<Vec<PathBuf>> {
    let paths: Vec<_> = paths
        .iter()
        .map(|path| {
            path.canonicalize().map_err(|error| {
                anyhow::anyhow!("could not resolve span plugin {}: {error}", path.display())
            })
        })
        .collect::<anyhow::Result<_>>()?;
    crate::span_processor::validate(&paths)?;
    Ok(paths)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn span_plugin_paths_are_canonicalized_before_use() {
        let dir = tempfile::Builder::new()
            .prefix("span-plugin-path-")
            .tempdir_in(".")
            .unwrap();
        let plugin = dir.path().join("plugin.mjs");
        std::fs::write(&plugin, "export default span => span").unwrap();
        let relative = PathBuf::from(dir.path().file_name().unwrap()).join("plugin.mjs");

        let resolved = resolve_span_plugin_paths(&[relative]).unwrap();

        assert_eq!(resolved, [plugin.canonicalize().unwrap()]);
        assert!(resolved[0].is_absolute());
    }

    #[test]
    fn additional_metadata_overrides_a_route_only_with_a_json_object() {
        let mut route = SessionRoute {
            additional_metadata: Some(serde_json::json!({"saved": true})),
            ..SessionRoute::default()
        };
        apply_additional_metadata(&mut route, Some(r#"{"run_id":"123"}"#)).unwrap();
        assert_eq!(
            route.additional_metadata,
            Some(serde_json::json!({"run_id": "123"}))
        );

        let error = apply_additional_metadata(&mut route, Some("[]")).unwrap_err();
        assert!(error.to_string().contains("must be a JSON object"));
        let error = apply_additional_metadata(&mut route, Some("not-json")).unwrap_err();
        assert!(error
            .to_string()
            .contains("invalid --additional-metadata JSON"));
    }

    #[test]
    fn tags_override_a_route_and_are_normalized() {
        let mut route = SessionRoute {
            tags: vec!["saved".into()],
            ..SessionRoute::default()
        };
        apply_tags(&mut route, &[" ci ".into(), "ci".into(), "docs".into()]).unwrap();
        assert_eq!(route.tags, ["ci", "docs"]);

        let error = apply_tags(&mut route, &[" ".into()]).unwrap_err();
        assert!(error.to_string().contains("must not be empty"));
    }
}
