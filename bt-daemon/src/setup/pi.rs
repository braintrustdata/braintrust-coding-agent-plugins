//! Pi: the Braintrust extension installed from npm by Pi's package manager.

use super::common::CommandRunner;

const PACKAGE: &str = "@braintrust/pi-extension";

/// The `pi install` spec for the published extension.
pub(crate) fn plugin_spec() -> String {
    format!("npm:{PACKAGE}")
}

pub(super) fn enable(runner: &mut impl CommandRunner) -> anyhow::Result<()> {
    runner.run("pi", &["install", &plugin_spec()])
}

pub(super) fn disable(runner: &mut impl CommandRunner) -> anyhow::Result<()> {
    runner.run("pi", &["uninstall", &plugin_spec()])
}

pub(super) fn update(runner: &mut impl CommandRunner) -> anyhow::Result<()> {
    runner.run("pi", &["update", &plugin_spec()])
}

/// Whether `pi list` reports a pinned version of the extension.
pub(super) fn stale() -> bool {
    let output = crate::subprocess::background_command("pi")
        .arg("list")
        .output();
    let Ok(output) = output else { return false };
    if !output.status.success() {
        return false;
    }
    let installed = String::from_utf8_lossy(&output.stdout);
    let expected = plugin_spec();
    installed.lines().any(|line| {
        let plugin = line.trim();
        plugin.starts_with(&expected) && plugin != expected
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::setup::common::test_support::FakeRunner;

    #[test]
    fn installs_the_latest_published_extension() {
        let mut runner = FakeRunner::new([]);

        enable(&mut runner).unwrap();

        assert!(runner.called("pi install npm:@braintrust/pi-extension"));
    }

    #[test]
    fn updates_the_npm_extension_without_installing_it() {
        let mut runner = FakeRunner::new([]);

        update(&mut runner).unwrap();

        assert!(runner.called("pi update npm:@braintrust/pi-extension"));
        assert!(!runner
            .calls
            .iter()
            .any(|call| call.starts_with("pi install ")));
    }

    #[test]
    fn disable_uses_the_uninstall_command() {
        let mut runner = FakeRunner::new([]);
        disable(&mut runner).unwrap();
        assert!(runner.called("pi uninstall npm:@braintrust/pi-extension"));
    }
}
