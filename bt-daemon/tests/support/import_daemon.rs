// Run imports against an in-process daemon writing debug span files. Shared
// by integration suites and, through `include!`, by unit tests.
use async_trait::async_trait;
use bt_daemon::wire::{AuthSelection, BackendAuth, SessionConfig, SessionRoute, TraceDestination};
use bt_daemon::{
    run_serve, run_status, AuthLease, AuthProvider, AuthResolveReason, DebugSinkFactory, HostInfo,
    ImportTarget, Registry, ServeArgs, ServeOptions, StatusArgs,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct StaticAuth;

#[async_trait]
impl AuthProvider for StaticAuth {
    async fn resolve(
        &self,
        selection: &AuthSelection,
        _: AuthResolveReason,
    ) -> anyhow::Result<AuthLease> {
        Ok(AuthLease {
            selection: selection.clone(),
            auth: BackendAuth {
                token: "test".into(),
                org_name: selection
                    .org_name
                    .clone()
                    .or_else(|| Some("test-org".into())),
                org_id: Some("org-test".into()),
                ..Default::default()
            },
            expires_at_ms: None,
        })
    }
}

fn endpoint(root: &Path) -> PathBuf {
    #[cfg(unix)]
    {
        root.join("import.sock")
    }
    #[cfg(windows)]
    {
        let _ = root;
        PathBuf::from(format!(
            r"\\.\pipe\bt-daemon-import-test-{}",
            uuid::Uuid::new_v4()
        ))
    }
}

/// One daemon per span output directory, started on first use and reused by
/// later imports into the same directory.
pub async fn target(output: &Path, config: Option<SessionConfig>) -> ImportTarget {
    static DAEMONS: std::sync::LazyLock<
        tokio::sync::Mutex<std::collections::HashMap<PathBuf, PathBuf>>,
    > = std::sync::LazyLock::new(Default::default);
    let root = output.with_extension("daemon");
    let mut daemons = DAEMONS.lock().await;
    let socket = match daemons.get(output) {
        Some(socket) => socket.clone(),
        None => {
            std::fs::create_dir_all(&root).unwrap();
            let socket = endpoint(&root);
            let args = ServeArgs {
                socket: Some(socket.clone()),
                data_dir: Some(root.join("data")),
                idle_timeout_secs: 0,
                session_idle_timeout_secs: 0,
            };
            let options = ServeOptions {
                version: "test".into(),
                translators: Arc::new(Registry::default_agents()),
                sink_factory: Arc::new(DebugSinkFactory { dir: output.into() }),
                auth_provider: Some(Arc::new(StaticAuth)),
            };
            tokio::spawn(async move {
                let _ = run_serve(args, options).await;
            });
            for _ in 0..200 {
                if let Ok(Some(_)) = run_status(StatusArgs {
                    socket: Some(socket.clone()),
                    session_id: None,
                })
                .await
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            daemons.insert(output.to_owned(), socket.clone());
            socket
        }
    };
    let route = match config {
        Some(config) => SessionRoute {
            destination: config.destination,
            flush_mode: config.flush_mode,
            additional_metadata: config.additional_metadata,
            tags: config.tags,
            span_plugins: config.span_plugins,
            ..Default::default()
        },
        None => SessionRoute::default(),
    };
    let route = SessionRoute {
        destination: route.destination.or(Some(TraceDestination::ProjectLogs {
            project_id: Some("test-project".into()),
            project_name: None,
        })),
        ..route
    };
    ImportTarget {
        socket,
        host: HostInfo {
            serve_argv: vec!["unused".into()],
            version: "test".into(),
        },
        route,
        no_spawn: true,
    }
}
