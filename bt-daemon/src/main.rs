//! Standalone `bt-daemon` binary — the isolated-testing front-end over the
//! `bt-daemon` library. Built only with the `cli` feature. Auth is env/flag
//! static-token only; there are no
//! profiles, OAuth, or keychain here (that lives in `bt`). See
//! the crate README's "Dual consumption" section.

use async_trait::async_trait;
use bt_daemon::wire::{AuthSelection, BackendAuth, SessionRoute, TraceDestination};
use bt_daemon::{
    braintrust_serve_options, paths, run_hook, run_import, run_serve, run_status, AuthLease,
    AuthProvider, AuthResolveReason, BraintrustSinkConfig, DebugSinkFactory, HookArgs, HostInfo,
    ImportArgs, Registry, RunArgs, RunHookCommand, ServeArgs, ServeOptions, StatusArgs, run_traced,
};
use clap::{Args, Parser, Subcommand};
use std::ffi::OsString;
use std::sync::Arc;

/// A debug-sink [`ServeOptions`] with all real agent translators registered
/// (Braintrust delivery off — NDJSON to `<data_dir>/spans/`).
fn debug_serve_options(version: &str, data_dir: &std::path::Path) -> ServeOptions {
    ServeOptions {
        version: version.to_string(),
        translators: Arc::new(Registry::default_agents()),
        sink_factory: Arc::new(DebugSinkFactory {
            dir: data_dir.join("spans"),
        }),
        auth_provider: None,
    }
}

struct EnvironmentAuthProvider {
    require_token: bool,
}

#[async_trait]
impl AuthProvider for EnvironmentAuthProvider {
    async fn resolve(
        &self,
        selection: &AuthSelection,
        _reason: AuthResolveReason,
    ) -> anyhow::Result<AuthLease> {
        let token = std::env::var("BRAINTRUST_API_KEY").unwrap_or_default();
        if self.require_token && token.is_empty() {
            anyhow::bail!("BRAINTRUST_API_KEY is not set");
        }
        Ok(AuthLease {
            profile: selection
                .profile
                .clone()
                .unwrap_or_else(|| "environment".to_string()),
            auth: BackendAuth {
                token,
                api_url: std::env::var("BRAINTRUST_API_URL").ok(),
                app_url: std::env::var("BRAINTRUST_APP_URL").ok(),
                org_name: selection
                    .org_name
                    .clone()
                    .or_else(|| std::env::var("BRAINTRUST_ORG_NAME").ok()),
                org_id: std::env::var("BRAINTRUST_ORG_ID").ok(),
            },
            expires_at_ms: None,
        })
    }
}

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Parser)]
#[command(
    name = "bt-daemon",
    version,
    about = "Braintrust coding-agent tracing daemon (standalone test binary)"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
// Clap parse structs; sizes are irrelevant for a one-shot CLI dispatch.
#[allow(clippy::large_enum_variant)]
enum Command {
    /// Run the daemon (foreground).
    Serve {
        #[command(flatten)]
        args: ServeArgs,
        /// Use the debug sink (NDJSON to disk) instead of sending to Braintrust.
        /// For offline isolated testing.
        #[arg(long)]
        debug_sink: bool,
        /// Braintrust API URL for the sink (default: SDK default).
        #[arg(long, env = "BRAINTRUST_API_URL")]
        api_url: Option<String>,
        /// Braintrust app URL for the sink (default: SDK default).
        #[arg(long, env = "BRAINTRUST_APP_URL")]
        app_url: Option<String>,
    },
    /// Forward one hook event (read from stdin) to the daemon.
    Hook {
        #[command(flatten)]
        args: HookArgs,
        #[command(flatten)]
        route: RouteArgs,
    },
    /// Print daemon/session status.
    Status(StatusArgs),
    /// Import a past Codex or Claude Code session by its resume id.
    Import(ImportArgs),
    /// Launch a coding agent with live tracing hooks for this invocation.
    Run(RunArgs),
}

/// Non-secret session selection. Credentials are resolved by the daemon.
#[derive(Args)]
struct RouteArgs {
    #[arg(long, env = "BRAINTRUST_PROFILE")]
    profile: Option<String>,
    #[arg(long = "org", env = "BRAINTRUST_ORG_NAME")]
    org_name: Option<String>,
    #[arg(long, env = "BRAINTRUST_PROJECT", conflicts_with = "destination")]
    project: Option<String>,
    #[arg(long, env = "BRAINTRUST_DESTINATION")]
    destination: Option<TraceDestination>,
}

impl RouteArgs {
    fn into_route(self) -> SessionRoute {
        SessionRoute {
            auth: AuthSelection {
                profile: self.profile,
                org_name: self.org_name,
            },
            destination: self.destination.or_else(|| {
                self.project
                    .map(|project_name| TraceDestination::ProjectLogs {
                        project_id: None,
                        project_name: Some(project_name),
                    })
            }),
            ..SessionRoute::default()
        }
    }
}

fn host_info() -> HostInfo {
    let exe = std::env::current_exe()
        .map(OsString::from)
        .unwrap_or_else(|_| OsString::from("bt-daemon"));
    HostInfo {
        serve_argv: vec![exe, OsString::from("serve")],
        version: VERSION.to_string(),
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Serve {
            args,
            debug_sink,
            api_url,
            app_url,
        } => {
            let data_dir = paths::data_dir(args.data_dir.as_deref());
            let mut opts = if debug_sink {
                debug_serve_options(VERSION, &data_dir)
            } else {
                let cfg = BraintrustSinkConfig {
                    api_url,
                    app_url,
                    version: VERSION.to_string(),
                };
                braintrust_serve_options(VERSION, cfg, Arc::new(Registry::default_agents()))
            };
            opts.auth_provider = Some(Arc::new(EnvironmentAuthProvider {
                require_token: !debug_sink,
            }));
            if let Err(e) = run_serve(args, opts).await {
                eprintln!("bt-daemon serve: {e}");
                std::process::exit(1);
            }
        }
        Command::Hook { args, route } => {
            // A hook must NEVER fail the agent's turn: log and exit 0 on error.
            if let Err(e) = run_hook(args, route.into_route(), host_info()).await {
                eprintln!("bt-daemon hook (non-fatal): {e}");
            }
            std::process::exit(0);
        }
        Command::Status(args) => match run_status(args).await {
            Ok(Some(status)) => {
                println!("{}", serde_json::to_string_pretty(&status).unwrap());
            }
            Ok(None) => {
                println!("bt-daemon is not running");
            }
            Err(e) => {
                eprintln!("bt-daemon status: {e}");
                std::process::exit(1);
            }
        },
        Command::Import(args) => {
            let data_dir = paths::data_dir(None);
            let opts = debug_serve_options(VERSION, &data_dir);
            if let Err(e) = run_import(args, opts, None).await {
                eprintln!("bt-daemon import: {e}");
                std::process::exit(1);
            }
        }
        Command::Run(args) => {
            let exe = std::env::current_exe()
                .map(OsString::from)
                .unwrap_or_else(|_| OsString::from("bt-daemon"));
            let hook_command = RunHookCommand {
                program: exe,
                args: vec![OsString::from("hook")],
            };
            match run_traced(args, hook_command).await {
                Ok(status) if status.success() => {}
                Ok(status) => std::process::exit(status.code().unwrap_or(1)),
                Err(error) => {
                    eprintln!("bt-daemon run: {error}");
                    std::process::exit(1);
                }
            }
        }
    }
}
