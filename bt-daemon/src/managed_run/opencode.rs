//! OpenCode managed runs add the tracing plugin to inline config.

use super::{Injection, ManagedRun};
use crate::agents::OpenCode;
use crate::args::RunHookCommand;
use std::ffi::OsString;

/// Select only a recognized protocol; an unknown version must not silently
/// choose v1 and bypass v2's private-server safeguards.
pub(super) async fn is_v2(executable: &std::ffi::OsStr) -> anyhow::Result<bool> {
    match detect_major_version(executable).await? {
        1 => Ok(false),
        2 => Ok(true),
        major => anyhow::bail!("unsupported OpenCode major version {major}"),
    }
}

async fn detect_major_version(executable: &std::ffi::OsStr) -> anyhow::Result<u64> {
    let mut command: tokio::process::Command =
        crate::subprocess::background_command(executable).into();
    command.arg("--version").kill_on_drop(true);
    let output = tokio::time::timeout(std::time::Duration::from_secs(15), command.output())
        .await
        .map_err(|_| anyhow::anyhow!("timed out running --version"))?
        .map_err(|error| anyhow::anyhow!("failed to run --version: {error}"))?;
    if !output.status.success() {
        anyhow::bail!("--version exited with {}", output.status);
    }
    parse_major_version(&String::from_utf8_lossy(&output.stdout))
        .or_else(|| parse_major_version(&String::from_utf8_lossy(&output.stderr)))
        .ok_or_else(|| anyhow::anyhow!("--version printed no recognizable version"))
}

/// The first `MAJOR.MINOR...` token, so banners or trailing notes such as
/// `2.2.6 (update available 3.0.0)` do not change the result.
fn parse_major_version(output: &str) -> Option<u64> {
    output.split_whitespace().find_map(|token| {
        let token = token.trim_start_matches('v');
        let (major, rest) = token.split_once('.')?;
        rest.starts_with(|c: char| c.is_ascii_digit())
            .then(|| major.parse().ok())
            .flatten()
    })
}

pub(super) fn inject_for_version(v2: bool, args: &[OsString]) -> anyhow::Result<Injection> {
    if v2
        && args.iter().any(|arg| {
            arg == "--server"
                || arg == "--service"
                || arg.to_string_lossy().starts_with("--server=")
                || arg.to_string_lossy().starts_with("--service=")
        })
    {
        anyhow::bail!("OpenCode 2 managed tracing requires a private server; remove --server");
    }
    let config =
        opencode_managed_config(std::env::var("OPENCODE_CONFIG_CONTENT").ok().as_deref(), v2)?;
    Ok(Injection {
        env: vec![("OPENCODE_CONFIG_CONTENT", config)],
        args: if v2 {
            vec!["--standalone".into()]
        } else {
            Vec::new()
        },
        ..Injection::default()
    })
}

fn opencode_managed_config(existing: Option<&str>, v2: bool) -> anyhow::Result<String> {
    let mut config = match existing {
        Some(raw) => serde_json::from_str::<serde_json::Value>(raw)
            .map_err(|error| anyhow::anyhow!("invalid OPENCODE_CONFIG_CONTENT: {error}"))?,
        None => serde_json::json!({}),
    };
    let object = config
        .as_object_mut()
        .ok_or_else(|| anyhow::anyhow!("OPENCODE_CONFIG_CONTENT must be a JSON object"))?;
    let plugins = object
        .entry("plugin")
        .or_insert_with(|| serde_json::Value::Array(Vec::new()))
        .as_array_mut()
        .ok_or_else(|| anyhow::anyhow!("OPENCODE_CONFIG_CONTENT.plugin must be an array"))?;
    let plugin = crate::env::var("BRAINTRUST_TRACE_OPENCODE_PLUGIN_SPEC").unwrap_or_else(|| {
        if v2 {
            "@braintrust/trace-opencode"
        } else {
            "@braintrust/trace-opencode/tracing"
        }
        .to_string()
    });
    if !plugins.iter().any(|value| value.as_str() == Some(&plugin)) {
        plugins.push(serde_json::Value::String(plugin));
    }
    Ok(serde_json::to_string(&config)?)
}

impl ManagedRun for OpenCode {
    fn executable(&self) -> (&'static str, &'static str) {
        ("OPENCODE_BIN", "opencode")
    }

    fn invocation_args(&self, injection: Vec<OsString>, mut args: Vec<OsString>) -> Vec<OsString> {
        if injection.first().is_some_and(|arg| arg == "--standalone")
            && args.first().is_some_and(|arg| arg == "serve")
        {
            // `serve` already starts its own server and does not accept --standalone.
            return args;
        }
        if injection.first().is_some_and(|arg| arg == "--standalone")
            && args
                .first()
                .is_some_and(|arg| arg == "run" || arg == "mini")
        {
            let command = args.remove(0);
            return std::iter::once(command)
                .chain(injection)
                .chain(args)
                .collect();
        }
        injection.into_iter().chain(args).collect()
    }

    fn inject(
        &self,
        _hook_command: &RunHookCommand,
        _managed_run_id: &str,
    ) -> anyhow::Result<Injection> {
        inject_for_version(false, &[])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opencode_managed_run_preserves_inline_config_and_adds_plugin() {
        let config =
            opencode_managed_config(Some(r#"{"model":"test/model","plugin":["other"]}"#), false)
                .unwrap();
        let config: serde_json::Value = serde_json::from_str(&config).unwrap();
        assert_eq!(config["model"], "test/model");
        assert_eq!(
            config["plugin"],
            serde_json::json!(["other", "@braintrust/trace-opencode/tracing"])
        );
        assert!(OpenCode
            .inject(
                &RunHookCommand {
                    program: "bt".into(),
                    args: Vec::new(),
                },
                "run",
            )
            .unwrap()
            .args
            .is_empty());
    }

    #[test]
    fn opencode_v2_managed_run_uses_server_package() {
        let config =
            opencode_managed_config(Some(r#"{"model":"test/model","plugin":["other"]}"#), true)
                .unwrap();
        let config: serde_json::Value = serde_json::from_str(&config).unwrap();
        assert_eq!(config["model"], "test/model");
        assert_eq!(
            config["plugin"],
            serde_json::json!(["other", "@braintrust/trace-opencode"])
        );
    }

    #[test]
    fn opencode_v2_rejects_external_server_arguments() {
        assert!(inject_for_version(true, &["--server=http://localhost".into()]).is_err());
        assert!(inject_for_version(false, &["--server=http://localhost".into()]).is_ok());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn version_detection_executes_the_selected_binary() {
        use std::os::unix::fs::PermissionsExt;
        let temp = tempfile::tempdir().unwrap();
        let executable = temp.path().join("opencode");
        std::fs::write(&executable, "#!/bin/sh\nprintf '2.2.5\\n'\n").unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(is_v2(executable.as_os_str()).await.unwrap());
        std::fs::write(&executable, "#!/bin/sh\nprintf '1.18.0\\n'\n").unwrap();
        assert!(!is_v2(executable.as_os_str()).await.unwrap());
        // Detection failures must not silently select v1.
        std::fs::write(&executable, "#!/bin/sh\nexit 3\n").unwrap();
        assert!(is_v2(executable.as_os_str()).await.is_err());
        std::fs::write(&executable, "#!/bin/sh\nprintf unknown\n").unwrap();
        assert!(is_v2(executable.as_os_str()).await.is_err());
        std::fs::write(&executable, "#!/bin/sh\nprintf 3.0.0\n").unwrap();
        assert!(is_v2(executable.as_os_str()).await.is_err());
        std::fs::write(&executable, "#!/bin/sh\nprintf '2.0.1\\n' >&2\n").unwrap();
        assert!(is_v2(executable.as_os_str()).await.unwrap());
    }

    #[test]
    fn version_parsing_uses_the_first_version_token() {
        assert_eq!(parse_major_version("2.2.6\n"), Some(2));
        assert_eq!(parse_major_version("v2.0.0"), Some(2));
        assert_eq!(
            parse_major_version("2.2.6 (update available 3.0.0)"),
            Some(2)
        );
        assert_eq!(parse_major_version("opencode 1.18.4"), Some(1));
        assert_eq!(parse_major_version("opencode"), None);
        assert_eq!(parse_major_version(""), None);
    }

    #[test]
    fn opencode_v2_rejects_service_equals_argument() {
        assert!(inject_for_version(true, &["--service=remote".into()]).is_err());
    }

    #[test]
    fn opencode_v2_private_flag_follows_subcommand() {
        let private = vec![OsString::from("--standalone")];
        assert_eq!(
            OpenCode.invocation_args(private.clone(), vec!["run".into(), "hello".into()]),
            vec!["run", "--standalone", "hello"]
        );
        assert_eq!(
            OpenCode.invocation_args(
                private,
                vec!["serve".into(), "--port".into(), "1234".into()]
            ),
            vec!["serve", "--port", "1234"]
        );
    }
}
