use std::path::{Path, PathBuf};

fn repository_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("bt-daemon crate has repository parent")
}

/// Use the production marketplace tree so real-agent tests exercise the same
/// manifest and launcher that a release installs.
pub fn codex_marketplace() -> PathBuf {
    repository_root().join("src/plugins/codex/content")
}

/// Use the production plugin tree so real-agent tests exercise the same
/// manifest and forwarder that a release installs.
pub fn claude_plugin() -> PathBuf {
    repository_root().join("src/plugins/claude/content/plugins/trace-claude-code")
}
