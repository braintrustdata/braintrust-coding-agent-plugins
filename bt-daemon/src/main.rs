//! Standalone `bt-daemon` binary — the isolated-testing front-end over the
//! `bt-daemon` library. Built only with the `cli` feature. Auth is env/flag
//! static-token only; there are no
//! profiles, OAuth, or keychain here (that lives in `bt`). See
//! the crate README's "Dual consumption" section.

use bt_daemon::wire::{BackendAuth, FlushMode, SessionConfig};
use bt_daemon::{
    braintrust_serve_options, paths, run_hook, run_import, run_serve, run_status,
    BraintrustSinkConfig, DebugSinkFactory, HookArgs, HostInfo, ImportArgs, Registry, ServeArgs,
    ServeOptions, StatusArgs,
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
        auth: AuthArgs,
    },
    /// Print daemon/session status.
    Status(StatusArgs),
    /// Import a past Codex or Claude Code session by its resume id.
    Import(ImportArgs),
}

/// Static-token backend auth from env/flags (no profile resolution).
#[derive(Args)]
struct AuthArgs {
    #[arg(long, env = "BRAINTRUST_API_KEY")]
    api_key: Option<String>,
    #[arg(long, env = "BRAINTRUST_API_URL")]
    api_url: Option<String>,
    #[arg(long, env = "BRAINTRUST_APP_URL")]
    app_url: Option<String>,
    #[arg(long = "org", env = "BRAINTRUST_ORG_NAME")]
    org_name: Option<String>,
    #[arg(long = "org-id", env = "BRAINTRUST_ORG_ID")]
    org_id: Option<String>,
    #[arg(long, env = "BRAINTRUST_PROJECT")]
    project: Option<String>,
}

impl AuthArgs {
    fn into_config(self) -> SessionConfig {
        SessionConfig {
            auth: BackendAuth {
                token: self.api_key.unwrap_or_default(),
                api_url: self.api_url,
                app_url: self.app_url,
                org_name: self.org_name,
                org_id: self.org_id,
            },
            project: self.project,
            parent_span_id: None,
            root_span_id: None,
            flush_mode: FlushMode::FireAndForget,
            additional_metadata: None,
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
            let opts = if debug_sink {
                debug_serve_options(VERSION, &data_dir)
            } else {
                let cfg = BraintrustSinkConfig {
                    api_url,
                    app_url,
                    version: VERSION.to_string(),
                };
                braintrust_serve_options(VERSION, cfg, Arc::new(Registry::default_agents()))
            };
            if let Err(e) = run_serve(args, opts).await {
                eprintln!("bt-daemon serve: {e}");
                std::process::exit(1);
            }
        }
        Command::Hook { args, auth } => {
            // A hook must NEVER fail the agent's turn: log and exit 0 on error.
            let config = auth.into_config();
            if let Err(e) = run_hook(args, config, host_info()).await {
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
    }
}
