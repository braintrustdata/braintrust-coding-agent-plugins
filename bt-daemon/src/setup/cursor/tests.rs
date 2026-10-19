use super::*;
use serde_json::json;
use std::collections::BTreeMap;

/// Models a distribution newer than this bt build, with OAuth MCP and an
/// additional component. A fake Git command checks the source and writes the
/// checkout to the requested staging path, including paths containing spaces.
struct PublishedPlugin {
    files: BTreeMap<&'static str, String>,
    downloads: usize,
    fail: bool,
    missing_git: bool,
}

impl PublishedPlugin {
    fn new() -> Self {
        Self {
            files: BTreeMap::from([
                (
                    ".cursor-plugin/plugin.json",
                    json!({
                        "name": "braintrust",
                        "version": "999.0.0",
                        "repository": format!("https://github.com/{PLUGIN_SOURCE}"),
                        "hooks": "hooks/hooks.json",
                        "mcpServers": "mcp.json"
                    })
                    .to_string(),
                ),
                (
                    "hooks/hooks.json",
                    json!({"version": 1, "hooks": {
                        "sessionStart": [{"command": "bt trace hook --source cursor --event sessionStart", "failClosed": false}],
                        "futureEvent": [{"command": "bt trace hook --source cursor --event futureEvent", "timeout": 12}]
                    }})
                    .to_string(),
                ),
                (
                    "mcp.json",
                    json!({"mcpServers": {"braintrust": {"url": "https://api.braintrust.dev/mcp"}}})
                        .to_string(),
                ),
                ("skills/new-skill/SKILL.md", "Published skill".to_owned()),
            ]),
            downloads: 0,
            fail: false,
            missing_git: false,
        }
    }

    fn assert_installed_at(&self, plugin: &Path) {
        for (relative, contents) in &self.files {
            assert_eq!(
                std::fs::read_to_string(plugin.join(relative)).unwrap(),
                *contents,
                "published {relative} must be installed verbatim"
            );
        }
    }
}

impl CommandRunner for PublishedPlugin {
    fn run(&mut self, program: &str, args: &[&str]) -> anyhow::Result<()> {
        assert_eq!(program, "git");
        assert_eq!(
            &args[..args.len() - 1],
            [
                "-c",
                "core.autocrlf=false",
                "clone",
                "--depth",
                "1",
                "--",
                "https://github.com/braintrustdata/braintrust-cursor-extension.git"
            ]
        );
        self.downloads += 1;
        if self.missing_git {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "git not found",
            ))
            .context("failed to run `git`");
        }
        let destination = Path::new(args.last().unwrap());
        for (relative, contents) in &self.files {
            let path = destination.join(relative);
            std::fs::create_dir_all(path.parent().unwrap())?;
            std::fs::write(path, contents)?;
        }
        if self.fail {
            bail!("download interrupted");
        }
        Ok(())
    }

    fn json(&mut self, _: &str, _: &[&str]) -> anyhow::Result<Value> {
        panic!("Cursor setup must not query an agent marketplace")
    }

    fn json_in_home(&mut self, _: &str, _: &[&str], _: &Path) -> anyhow::Result<Value> {
        panic!("Cursor setup must not query an agent marketplace")
    }

    fn run_in_home(&mut self, _: &str, _: &[&str], _: &Path) -> anyhow::Result<()> {
        panic!("Cursor setup must not override the user's home")
    }
}

#[test]
fn installs_and_refreshes_the_published_plugin_preserving_neighbors_and_user_hooks() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("Cursor Plugins/local");
    let plugin = root.join(PLUGIN_NAME);
    let config = temp.path().join(".cursor");
    let neighbor = root.join("my-plugin/keep.txt");
    std::fs::create_dir_all(neighbor.parent().unwrap()).unwrap();
    std::fs::write(&neighbor, "leave me").unwrap();
    std::fs::create_dir_all(&config).unwrap();
    let user_hook = json!({"command": "my-hook", "timeout": 42});
    std::fs::write(
        config.join("hooks.json"),
        json!({"version": 1, "hooks": {"sessionStart": [user_hook]}}).to_string(),
    )
    .unwrap();
    let mut published = PublishedPlugin::new();

    enable_at(&mut published, &plugin, &config).unwrap();
    published.assert_installed_at(&plugin);
    // Simulate an installation from an older bt build before enabling again.
    std::fs::write(plugin.join(".cursor-plugin/plugin.json"), PLUGIN_MANIFEST).unwrap();
    std::fs::write(
        plugin.join("mcp.json"),
        r#"{"Authorization":"Bearer ${BRAINTRUST_API_KEY}"}"#,
    )
    .unwrap();
    std::fs::write(plugin.join("obsolete.txt"), "old release").unwrap();
    published
        .files
        .insert("skills/new-skill/SKILL.md", "Updated skill".to_owned());
    enable_at(&mut published, &plugin, &config).unwrap();
    published.assert_installed_at(&plugin);
    assert!(!plugin.join("obsolete.txt").exists());
    published
        .files
        .insert("skills/new-skill/SKILL.md", "Next release".to_owned());
    update_at(&mut published, &plugin, &config).unwrap();
    published.assert_installed_at(&plugin);
    assert_eq!(published.downloads, 3);
    assert!(capture_hooks_are_installed_at(&config));
    disable_at(&plugin, &config).unwrap();
    assert!(!plugin.exists());
    assert!(!capture_hooks_are_installed_at(&config));
    assert_eq!(std::fs::read_to_string(neighbor).unwrap(), "leave me");
    let hooks_after_disable: Value =
        serde_json::from_slice(&std::fs::read(config.join("hooks.json")).unwrap()).unwrap();
    assert_eq!(
        hooks_after_disable["hooks"]["sessionStart"],
        json!([user_hook])
    );
}

#[test]
fn failed_download_or_invalid_distribution_preserves_the_installation() {
    let temp = tempfile::tempdir().unwrap();
    let plugin = temp.path().join(PLUGIN_NAME);
    let config = temp.path().join(".cursor");
    let expected = PublishedPlugin::new();
    let mut published = PublishedPlugin::new();
    enable_at(&mut published, &plugin, &config).unwrap();
    let hooks_before = std::fs::read(config.join("hooks.json")).unwrap();

    for (relative, replacement) in [
        ("download", None),
        (".cursor-plugin/plugin.json", None),
        (".cursor-plugin/plugin.json", Some("{}")),
        ("hooks/hooks.json", None),
        ("hooks/hooks.json", Some("invalid json")),
        ("mcp.json", Some("{}")),
    ] {
        published.files.clone_from(&expected.files);
        published.fail = relative == "download";
        published
            .files
            .insert("skills/new-skill/SKILL.md", "Next release".to_owned());
        match replacement {
            Some(contents) => {
                published.files.insert(relative, contents.to_owned());
            }
            None => {
                published.files.remove(relative);
            }
        }
        assert!(
            update_at(&mut published, &plugin, &config).is_err(),
            "{relative}"
        );
        expected.assert_installed_at(&plugin);
        assert_eq!(
            std::fs::read(config.join("hooks.json")).unwrap(),
            hooks_before
        );
    }

    // A network failure must not fall back to embedded content either.
    published.fail = true;
    let fresh_plugin = temp.path().join("fresh-plugin");
    let fresh_config = temp.path().join("fresh-config");
    assert!(enable_at(&mut published, &fresh_plugin, &fresh_config).is_err());
    assert!(!fresh_plugin.exists());
    assert!(!fresh_config.exists());
}

#[test]
fn missing_git_installs_the_bundled_plugin_and_git_restores_published_updates() {
    let temp = tempfile::tempdir().unwrap();
    let plugin = temp.path().join(PLUGIN_NAME);
    let config = temp.path().join(".cursor");
    let mut published = PublishedPlugin::new();
    published.missing_git = true;

    enable_at(&mut published, &plugin, &config).unwrap();
    update_at(&mut published, &plugin, &config).unwrap();
    assert_eq!(
        std::fs::read_to_string(plugin.join(".cursor-plugin/plugin.json")).unwrap(),
        PLUGIN_MANIFEST
    );
    assert_eq!(
        std::fs::read_to_string(plugin.join("mcp.json")).unwrap(),
        plugin_source!("cursor/content/mcp.json")
    );
    assert!(plugin.join("logo.svg").is_file());
    assert!(capture_hooks_are_installed_at(&config));

    published.missing_git = false;
    update_at(&mut published, &plugin, &config).unwrap();
    published.assert_installed_at(&plugin);
}

#[test]
fn update_requires_an_installation_and_setup_preserves_unrelated_plugins() {
    let temp = tempfile::tempdir().unwrap();
    let plugin = temp.path().join(PLUGIN_NAME);
    let config = temp.path().join(".cursor");
    let mut published = PublishedPlugin::new();
    assert!(update_at(&mut published, &plugin, &config).is_err());
    assert!(!plugin.exists());
    assert!(!config.exists());

    std::fs::create_dir_all(plugin.join(".cursor-plugin")).unwrap();
    let unrelated =
        r#"{"name":"trace-cursor","version":"1.0.0","repository":"https://example.com/custom"}"#;
    std::fs::write(plugin.join(".cursor-plugin/plugin.json"), unrelated).unwrap();
    assert!(enable_at(&mut published, &plugin, &config).is_err());
    assert!(update_at(&mut published, &plugin, &config).is_err());
    assert!(disable_at(&plugin, &config).is_err());
    assert_eq!(published.downloads, 0);
    assert!(!config.exists());
    assert_eq!(
        std::fs::read_to_string(plugin.join(".cursor-plugin/plugin.json")).unwrap(),
        unrelated
    );
}

#[test]
fn malformed_user_hooks_fail_before_downloading_or_changing_the_plugin() {
    let temp = tempfile::tempdir().unwrap();
    let plugin = temp.path().join(PLUGIN_NAME);
    let config = temp.path().join(".cursor");
    std::fs::create_dir_all(&config).unwrap();
    std::fs::write(config.join("hooks.json"), "invalid json").unwrap();
    let mut published = PublishedPlugin::new();
    assert!(enable_at(&mut published, &plugin, &config).is_err());
    assert_eq!(published.downloads, 0);
    assert!(!plugin.exists());
    assert_eq!(
        std::fs::read_to_string(config.join("hooks.json")).unwrap(),
        "invalid json"
    );
}
