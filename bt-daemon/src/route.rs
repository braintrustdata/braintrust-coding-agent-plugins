//! Invocation-local overrides applied to a [`SessionRoute`] before it is
//! used by a live hook, managed run, or transcript import.

use std::path::PathBuf;

#[cfg(test)]
use crate::wire::SessionRoute;

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
    fn saved_tags_deserialize_as_a_sorted_unique_json_array() {
        let route: SessionRoute = serde_json::from_value(serde_json::json!({
            "tags": ["z", "a", "z"]
        }))
        .unwrap();
        assert_eq!(
            serde_json::to_value(route).unwrap()["tags"],
            serde_json::json!(["a", "z"])
        );
    }

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
        route
            .apply_additional_metadata(Some(r#"{"run_id":"123"}"#))
            .unwrap();
        assert_eq!(
            route.additional_metadata,
            Some(serde_json::json!({"run_id": "123"}))
        );

        let error = route.apply_additional_metadata(Some("[]")).unwrap_err();
        assert!(error.to_string().contains("must be a JSON object"));
        let error = route
            .apply_additional_metadata(Some("not-json"))
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("invalid --additional-metadata JSON"));
    }

    #[test]
    fn tags_override_a_route_and_deduplicate_after_trimming() {
        let mut route = SessionRoute {
            tags: ["saved".into()].into_iter().collect(),
            ..SessionRoute::default()
        };
        route
            .apply_tags(&[" ci ".into(), "ci".into(), "docs".into()])
            .unwrap();
        assert_eq!(
            route.tags,
            ["ci".into(), "docs".into()].into_iter().collect()
        );

        let error = route.apply_tags(&[" ".into()]).unwrap_err();
        assert!(error.to_string().contains("must not be empty"));
    }
}
