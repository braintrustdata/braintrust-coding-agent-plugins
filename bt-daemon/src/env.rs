//! User-facing environment variables use the `BRAINTRUST_` prefix, matching
//! `bt` and the Braintrust SDKs. Read them through [`var_os`] so that the
//! deprecated `BT_` names keep working until support is removed.

use std::ffi::{OsStr, OsString};

/// Pre-`BRAINTRUST_` names of tracing env vars. Each `BT_<NAME>` is still
/// honored as an alias for `BRAINTRUST_<NAME>` until support is removed in a
/// major release. Do not add entries: new env vars must use the `BRAINTRUST_`
/// prefix.
///
/// `BT_TRACE_MANAGED_RUN_ID` and `BT_TRACE_INVOCATION_SETTINGS` are not listed:
/// they are the internal protocol `bt trace run` uses to configure its child
/// process tree, read by separately released plugins, and are unchanged.
pub(crate) const DEPRECATED_BT_ENV_VARS: &[&str] = &[
    "BT_ANTIGRAVITY_CONFIG_DIR",
    "BT_CURSOR_CONFIG_DIR",
    "BT_CURSOR_PLUGIN_DIR",
    "BT_DAEMON_CONFIG",
    "BT_DAEMON_DATA_DIR",
    "BT_DAEMON_SOCKET",
    "BT_TRACE_OPENCODE_PLUGIN_SPEC",
    "BT_TRACE_PI_PLUGIN_SPEC",
];

pub(crate) fn canonical_env_name(deprecated: &str) -> String {
    let name = deprecated.strip_prefix("BT_").unwrap_or(deprecated);
    format!("BRAINTRUST_{name}")
}

/// The deprecated `BT_` alias of `canonical`, if it has one.
pub(crate) fn deprecated_env_name(canonical: &str) -> Option<&'static str> {
    DEPRECATED_BT_ENV_VARS
        .iter()
        .copied()
        .find(|deprecated| canonical_env_name(deprecated) == canonical)
}

/// Read `canonical`, falling back to its deprecated `BT_` alias. The canonical
/// name wins when both are set.
pub(crate) fn var_os(canonical: &str) -> Option<OsString> {
    std::env::var_os(canonical)
        .or_else(|| deprecated_env_name(canonical).and_then(std::env::var_os))
}

pub(crate) fn var(canonical: &str) -> Option<String> {
    var_os(canonical).and_then(|value| value.into_string().ok())
}

/// Pass every resolved value to a child process under both names, so plugins
/// released before the rename read the same value instead of a stale or
/// missing deprecated one.
pub(crate) fn mirror_aliases(command: &mut tokio::process::Command) {
    for deprecated in DEPRECATED_BT_ENV_VARS {
        let canonical = canonical_env_name(deprecated);
        if let Some(value) = var_os(&canonical) {
            command.env(&canonical, &value).env(deprecated, &value);
        }
    }
}

/// Set `canonical` and its deprecated alias to the same value on a child
/// process, so plugins released before the rename still read it.
pub(crate) fn set_with_alias(
    command: &mut tokio::process::Command,
    canonical: &str,
    value: impl AsRef<OsStr>,
) {
    command.env(canonical, value.as_ref());
    if let Some(deprecated) = deprecated_env_name(canonical) {
        command.env(deprecated, value.as_ref());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn canonical_env_name_swaps_prefix() {
        assert_eq!(
            canonical_env_name("BT_DAEMON_SOCKET"),
            "BRAINTRUST_DAEMON_SOCKET"
        );
        assert_eq!(
            deprecated_env_name("BRAINTRUST_DAEMON_SOCKET"),
            Some("BT_DAEMON_SOCKET")
        );
        assert_eq!(deprecated_env_name("BRAINTRUST_API_KEY"), None);
    }

    /// New user-facing env vars must use the `BRAINTRUST_` prefix. Every
    /// `BT_*` name in the crate must be a deprecated alias or internal protocol,
    /// and every deprecated alias must map to a name the crate reads.
    #[test]
    fn env_vars_use_braintrust_prefix() {
        const INTERNAL: &[&str] = &[
            "BT_TRACE_INVOCATION_SETTINGS",
            "BT_TRACE_MANAGED_RUN_ID",
            // Fallback for `BRAINTRUST_BT_BIN` in the Cursor hook launchers.
            "BT_BIN",
        ];
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut sources = String::new();
        let mut pending = vec![root];
        while let Some(path) = pending.pop() {
            if path.is_dir() {
                pending.extend(std::fs::read_dir(&path).unwrap().map(|e| e.unwrap().path()));
            } else if path.extension().is_some_and(|ext| ext == "rs") && !path.ends_with("env.rs") {
                sources.push_str(&std::fs::read_to_string(&path).unwrap());
            }
        }
        let pattern = regex::Regex::new(r"\bBT_[A-Z0-9_]+\b").unwrap();
        let unexpected: BTreeSet<_> = pattern
            .find_iter(&sources)
            .map(|m| m.as_str())
            .filter(|name| !DEPRECATED_BT_ENV_VARS.contains(name) && !INTERNAL.contains(name))
            .collect();
        assert!(
            unexpected.is_empty(),
            "env vars must use the BRAINTRUST_ prefix: {unexpected:?}"
        );
        for deprecated in DEPRECATED_BT_ENV_VARS {
            let canonical = canonical_env_name(deprecated);
            assert!(
                sources.contains(&format!("\"{canonical}\"")),
                "{deprecated} maps to {canonical}, which nothing reads"
            );
        }
    }
}
