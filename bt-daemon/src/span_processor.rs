//! Synchronous JavaScript span transforms.
//!
//! Session actors already execute on Tokio's worker pool. Each worker thread
//! lazily owns one QuickJS runtime and module cache, so JavaScript values never
//! cross threads and unrelated workers can transform spans concurrently.

use crate::translate::{SpanOp, SpanRow};
use rquickjs::{CatchResultExt, Context, Function, Module, Persistent, Runtime};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const MEMORY_LIMIT_BYTES: usize = 64 * 1024 * 1024;
const STACK_LIMIT_BYTES: usize = 512 * 1024;
const CALL_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Debug, thiserror::Error)]
#[error("span plugin exceeded its {budget_ms} ms budget during {phase} (total {elapsed_ms} ms; input conversion {input_ms} ms, execution {execution_ms} ms, output conversion {output_ms} ms): {exception}")]
struct PluginTimeout {
    budget_ms: u128,
    phase: &'static str,
    elapsed_ms: u128,
    input_ms: u128,
    execution_ms: u128,
    output_ms: u128,
    exception: String,
}

thread_local! {
    static ENGINE: RefCell<Option<Engine>> = const { RefCell::new(None) };
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum Operation {
    Insert,
    Merge,
}

#[derive(Serialize)]
struct PluginContext<'a> {
    operation: Operation,
    source: &'a str,
    session_id: &'a str,
    env: &'a BTreeMap<String, String>,
}

struct Engine {
    modules: HashMap<PathBuf, CachedModule>,
    failed_plugins: HashMap<PathBuf, FailedPlugin>,
    env: BTreeMap<String, String>,
    last_fingerprint: Option<PluginFingerprint>,
    context: Context,
    started: Instant,
    deadline_ms: Arc<AtomicU64>,
    interrupted: Arc<AtomicBool>,
    // Must drop after every Context and persistent JavaScript value.
    _runtime: Runtime,
}

struct CachedModule {
    fingerprint: PluginFingerprint,
    function: Persistent<Function<'static>>,
}

#[derive(Clone)]
struct FailedPlugin {
    fingerprint: Option<PluginFingerprint>,
    message: String,
    timed_out: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct PluginFingerprint {
    digest: [u8; 32],
}

impl Engine {
    fn new() -> anyhow::Result<Self> {
        let runtime = Runtime::new()?;
        runtime.set_memory_limit(MEMORY_LIMIT_BYTES);
        runtime.set_max_stack_size(STACK_LIMIT_BYTES);
        let started = Instant::now();
        let deadline_ms = Arc::new(AtomicU64::new(0));
        let interrupt_deadline = deadline_ms.clone();
        let interrupt_started = started;
        let interrupted = Arc::new(AtomicBool::new(false));
        let interrupt_fired = interrupted.clone();
        runtime.set_interrupt_handler(Some(Box::new(move || {
            let deadline = interrupt_deadline.load(Ordering::Relaxed);
            let expired =
                deadline != 0 && interrupt_started.elapsed().as_millis() as u64 >= deadline;
            if expired {
                interrupt_fired.store(true, Ordering::Relaxed);
            }
            expired
        })));
        let context = Context::full(&runtime)?;
        Ok(Self {
            modules: HashMap::new(),
            failed_plugins: HashMap::new(),
            env: environment(),
            last_fingerprint: None,
            context,
            started,
            deadline_ms,
            interrupted,
            _runtime: runtime,
        })
    }

    fn load(&mut self, path: &Path) -> anyhow::Result<Persistent<Function<'static>>> {
        self.last_fingerprint = None;
        let source = std::fs::read(path)
            .map_err(|error| anyhow::anyhow!("failed to read {}: {error}", path.display()))?;
        let fingerprint = PluginFingerprint {
            digest: Sha256::digest(&source).into(),
        };
        self.last_fingerprint = Some(fingerprint);
        if let Some(module) = self.modules.get(path) {
            if module.fingerprint == fingerprint {
                return Ok(module.function.clone());
            }
        }
        let digest = Sha256::digest(&source);
        let name = format!("bt-span-plugin:{digest:x}");
        self.arm_deadline(CALL_TIMEOUT);
        let started = Instant::now();
        let function = self.context.with(|ctx| -> anyhow::Result<_> {
            let (module, promise) = Module::declare(ctx.clone(), name, source)
                .catch(&ctx)
                .map_err(|error| anyhow::anyhow!(error.to_string()))?
                .eval()
                .catch(&ctx)
                .map_err(|error| anyhow::anyhow!(error.to_string()))?;
            promise
                .finish::<()>()
                .catch(&ctx)
                .map_err(|error| anyhow::anyhow!(error.to_string()))?;
            let function: Function<'_> = module
                .get("default")
                .catch(&ctx)
                .map_err(|error| anyhow::anyhow!("default export is not a function: {error}"))?;
            Ok(Persistent::save(&ctx, function))
        });
        self.deadline_ms.store(0, Ordering::Relaxed);
        if self.interrupted.load(Ordering::Relaxed) || started.elapsed() >= CALL_TIMEOUT {
            return Err(PluginTimeout {
                budget_ms: CALL_TIMEOUT.as_millis(),
                phase: "module loading",
                elapsed_ms: started.elapsed().as_millis(),
                input_ms: 0,
                execution_ms: 0,
                output_ms: 0,
                exception: function
                    .err()
                    .map(|error| error.to_string())
                    .unwrap_or_default(),
            }
            .into());
        }
        let function = function?;
        self.modules.insert(
            path.to_path_buf(),
            CachedModule {
                fingerprint,
                function: function.clone(),
            },
        );
        Ok(function)
    }

    fn call(
        &mut self,
        path: &Path,
        row: &SpanRow,
        operation: Operation,
        source: &str,
        session_id: &str,
    ) -> anyhow::Result<SpanRow> {
        self.call_with_budget(path, row, operation, source, session_id, CALL_TIMEOUT)
    }

    fn call_with_budget(
        &mut self,
        path: &Path,
        row: &SpanRow,
        operation: Operation,
        source: &str,
        session_id: &str,
        budget: Duration,
    ) -> anyhow::Result<SpanRow> {
        let function = self.load(path)?;
        let context = PluginContext {
            operation,
            source,
            session_id,
            env: &self.env,
        };
        self.arm_deadline(budget);
        let started = Instant::now();
        let mut phase = "input conversion";
        let mut input_ms = 0;
        let mut execution_ms = 0;
        let mut output_ms = 0;
        let result = self.context.with(|ctx| -> anyhow::Result<SpanRow> {
            let function = function.restore(&ctx)?;
            let row = rquickjs_serde::to_value(ctx.clone(), row)?;
            let context = rquickjs_serde::to_value(ctx.clone(), &context)?;
            input_ms = started.elapsed().as_millis();
            anyhow::ensure!(
                started.elapsed() < budget,
                "deadline expired before plugin execution"
            );
            phase = "plugin execution";
            let executing = Instant::now();
            let result = function
                .call::<_, rquickjs::Value<'_>>((row, context))
                .catch(&ctx)
                .map_err(|error| anyhow::anyhow!(error.to_string()));
            execution_ms = executing.elapsed().as_millis();
            let result = result?;
            if result.as_promise().is_some() {
                anyhow::bail!("plugin returned a Promise; span plugins must be synchronous");
            }
            phase = "output conversion";
            let converting = Instant::now();
            let result = rquickjs_serde::from_value_strict(result);
            output_ms = converting.elapsed().as_millis();
            Ok(result?)
        });
        self.deadline_ms.store(0, Ordering::Relaxed);
        if self.interrupted.load(Ordering::Relaxed) || started.elapsed() >= budget {
            if phase == "input conversion" {
                input_ms = started.elapsed().as_millis();
            }
            return Err(PluginTimeout {
                budget_ms: budget.as_millis(),
                phase,
                elapsed_ms: started.elapsed().as_millis(),
                input_ms,
                execution_ms,
                output_ms,
                exception: result
                    .err()
                    .map(|error| error.to_string())
                    .unwrap_or_default(),
            }
            .into());
        }
        result
    }

    fn arm_deadline(&self, budget: Duration) {
        self.interrupted.store(false, Ordering::Relaxed);
        let deadline = self.started.elapsed().saturating_add(budget).as_millis() as u64;
        self.deadline_ms.store(deadline.max(1), Ordering::Relaxed);
    }
}

#[derive(Debug, Clone)]
pub struct PluginFailure {
    pub path: PathBuf,
    pub message: String,
    pub timed_out: bool,
    /// Only the first failure on this worker needs to be logged and persisted.
    pub newly_seen: bool,
}

pub struct ProcessResult {
    /// `None` means the operation was withheld because every configured plugin
    /// is mandatory and one did not complete successfully.
    pub op: Option<SpanOp>,
    pub failure: Option<PluginFailure>,
}

/// Apply an ordered plugin chain on the worker thread currently executing the
/// session actor. Every plugin is mandatory: a failure withholds the operation,
/// and subsequent calls on this worker remain withheld instead of bypassing the
/// failed transform.
pub fn process(
    plugins: &[PathBuf],
    op: &SpanOp,
    source: &str,
    session_id: &str,
) -> anyhow::Result<ProcessResult> {
    process_inner(plugins, op, source, session_id, true)
}

pub(crate) fn process_isolated(
    plugins: &[PathBuf],
    op: &SpanOp,
    source: &str,
    session_id: &str,
) -> anyhow::Result<ProcessResult> {
    process_inner(plugins, op, source, session_id, false)
}

fn process_inner(
    plugins: &[PathBuf],
    op: &SpanOp,
    source: &str,
    session_id: &str,
    use_failed_cache: bool,
) -> anyhow::Result<ProcessResult> {
    if plugins.is_empty() {
        return Ok(ProcessResult {
            op: Some(op.clone()),
            failure: None,
        });
    }
    let (operation, mut row) = match op {
        SpanOp::Insert(row) => (Operation::Insert, row.clone()),
        SpanOp::Merge(row) => (Operation::Merge, row.clone()),
    };
    let original_ids = (
        row.span_id.clone(),
        row.root_span_id.clone(),
        row.parent_span_ids.clone(),
    );
    let mut failure = None;
    ENGINE.with_borrow_mut(|slot| -> anyhow::Result<()> {
        if slot.is_none() {
            *slot = Some(Engine::new()?);
        }
        let engine = slot
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("span processor engine was not initialized"))?;
        for plugin in plugins {
            if let Some(failed) = engine
                .failed_plugins
                .get(plugin)
                .cloned()
                .filter(|failed| use_failed_cache && !failed.timed_out)
            {
                if failed.fingerprint == plugin_fingerprint(plugin) {
                    failure = Some(PluginFailure {
                        path: plugin.clone(),
                        message: failed.message,
                        timed_out: failed.timed_out,
                        newly_seen: false,
                    });
                    break;
                }
                engine.failed_plugins.remove(plugin);
            }
            let candidate = engine.call(plugin, &row, operation, source, session_id);
            let mut candidate = match candidate {
                Ok(candidate)
                    if (
                        candidate.span_id.as_str(),
                        candidate.root_span_id.as_str(),
                        &candidate.parent_span_ids,
                    ) == (
                        original_ids.0.as_str(),
                        original_ids.1.as_str(),
                        &original_ids.2,
                    ) =>
                {
                    candidate
                }
                Ok(_) => {
                    let message = "changed immutable span identity fields".to_owned();
                    engine.failed_plugins.insert(
                        plugin.clone(),
                        failed_plugin(engine.last_fingerprint, &message, false),
                    );
                    failure = Some(PluginFailure {
                        path: plugin.clone(),
                        message,
                        timed_out: false,
                        newly_seen: true,
                    });
                    break;
                }
                Err(error) => {
                    let timed_out = error.downcast_ref::<PluginTimeout>().is_some();
                    let message = error.to_string();
                    engine.failed_plugins.insert(
                        plugin.clone(),
                        failed_plugin(engine.last_fingerprint, &message, timed_out),
                    );
                    failure = Some(PluginFailure {
                        path: plugin.clone(),
                        message,
                        timed_out,
                        newly_seen: true,
                    });
                    break;
                }
            };
            // Turn identity belongs to the translator, not the JSON transform.
            candidate.is_turn = row.is_turn;
            candidate.origin = row.origin.clone();
            candidate.turn_span_id = row.turn_span_id.clone();
            candidate.late_merge_key = row.late_merge_key.clone();
            row = candidate;
        }
        Ok(())
    })?;
    Ok(ProcessResult {
        op: failure.is_none().then_some(match op {
            SpanOp::Insert(_) => SpanOp::Insert(row),
            SpanOp::Merge(_) => SpanOp::Merge(row),
        }),
        failure,
    })
}

fn failed_plugin(
    fingerprint: Option<PluginFingerprint>,
    message: &str,
    timed_out: bool,
) -> FailedPlugin {
    FailedPlugin {
        fingerprint,
        message: message.to_owned(),
        timed_out,
    }
}

fn plugin_fingerprint(path: &Path) -> Option<PluginFingerprint> {
    Some(PluginFingerprint {
        digest: Sha256::digest(std::fs::read(path).ok()?).into(),
    })
}

pub(crate) fn file_digest(path: &Path) -> Option<[u8; 32]> {
    plugin_fingerprint(path).map(|fingerprint| fingerprint.digest)
}

pub(crate) fn failed_digest(path: &Path) -> Option<[u8; 32]> {
    ENGINE.with_borrow(|engine| {
        engine
            .as_ref()?
            .failed_plugins
            .get(path)?
            .fingerprint
            .map(|f| f.digest)
    })
}

// Preserve the bytes actually loaded by predecessors, not a later filesystem
// sample: an edit during a failing call must already qualify for recovery.
pub(crate) fn chain_digests(paths: &[PathBuf], failed: &Path) -> Vec<Option<[u8; 32]>> {
    ENGINE.with_borrow(|engine| {
        let mut reached_failure = false;
        paths
            .iter()
            .map(|path| {
                if path == failed {
                    reached_failure = true;
                    engine
                        .as_ref()
                        .and_then(|engine| engine.failed_plugins.get(path))
                        .and_then(|failed| failed.fingerprint)
                        .map(|fingerprint| fingerprint.digest)
                } else if !reached_failure {
                    engine
                        .as_ref()
                        .and_then(|engine| engine.modules.get(path))
                        .map(|module| module.fingerprint.digest)
                } else {
                    file_digest(path)
                }
            })
            .collect()
    })
}

/// Compile each module and verify that it default-exports a function. Explicit
/// CLI commands call this before persisting or launching with a plugin chain.
pub fn validate(plugins: &[PathBuf]) -> anyhow::Result<()> {
    ENGINE.with_borrow_mut(|slot| -> anyhow::Result<()> {
        if slot.is_none() {
            *slot = Some(Engine::new()?);
        }
        let engine = slot
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("span processor engine was not initialized"))?;
        for plugin in plugins {
            engine.load(plugin)?;
        }
        Ok(())
    })
}

fn environment() -> BTreeMap<String, String> {
    std::env::vars_os()
        .filter_map(|(key, value)| {
            let key = key.into_string().ok()?;
            let value = value.into_string().ok()?;
            // Windows environment variable names are case-insensitive, while
            // JavaScript object properties are not. Use a stable casing there
            // so portable plugins can read conventional names such as PATH.
            #[cfg(windows)]
            let key = key.to_ascii_uppercase();
            Some((key, value))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row() -> SpanRow {
        SpanRow {
            span_id: "span".into(),
            root_span_id: "root".into(),
            name: "original".into(),
            ..SpanRow::default()
        }
    }

    #[test]
    fn composes_plugins_and_exposes_context_env() {
        let dir = tempfile::tempdir().unwrap();
        let first = dir.path().join("first.mjs");
        let second = dir.path().join("second.mjs");
        std::fs::write(
            &first,
            "export default (span, context) => ({...span, name: `${context.source}:${context.env.PATH}:${span.name}`})",
        )
        .unwrap();
        std::fs::write(
            &second,
            "export default span => ({...span, metadata: {second: true}})",
        )
        .unwrap();
        let result = process(&[first, second], &SpanOp::Insert(row()), "codex", "session").unwrap();
        assert!(result.failure.is_none());
        let Some(SpanOp::Insert(processed)) = result.op else {
            panic!("expected insert")
        };
        assert_eq!(
            processed.name,
            format!("codex:{}:original", std::env::var("PATH").unwrap())
        );
        assert_eq!(processed.metadata.unwrap()["second"], true);
    }

    #[test]
    fn conversion_deadline_is_reported_before_the_filter_runs() {
        let dir = tempfile::tempdir().unwrap();
        let plugin = dir.path().join("guard.mjs");
        std::fs::write(
            &plugin,
            "export default () => { throw new Error('filter ran'); }",
        )
        .unwrap();
        let error = Engine::new()
            .unwrap()
            .call_with_budget(
                &plugin,
                &row(),
                Operation::Insert,
                "pi",
                "session",
                Duration::ZERO,
            )
            .unwrap_err();
        let timeout = error.downcast_ref::<PluginTimeout>().unwrap();
        assert_eq!(timeout.phase, "input conversion");
        assert_eq!(timeout.execution_ms, 0);
        assert!(!timeout.exception.contains("filter ran"));
    }

    #[test]
    fn redacts_large_input_and_preserves_property_error_details() {
        let dir = tempfile::tempdir().unwrap();
        let plugin = dir.path().join("redact.mjs");
        std::fs::write(
            &plugin,
            "export default span => {delete span.metadata.username; return span}",
        )
        .unwrap();
        let mut span = row();
        span.input = Some(serde_json::json!((0..10_000)
            .map(|_| "x".repeat(100))
            .collect::<Vec<_>>()));
        span.metadata = Some(serde_json::json!({"username":"private","source":"pi"}));
        let processed = Engine::new()
            .unwrap()
            .call(&plugin, &span, Operation::Insert, "pi", "session")
            .unwrap();
        assert_eq!(processed.input, span.input);
        assert_eq!(processed.metadata, Some(serde_json::json!({"source":"pi"})));
        assert_eq!(span.metadata.unwrap()["username"], "private");

        std::fs::write(
            &plugin,
            "export default span => {span.missing.workspace; return span}",
        )
        .unwrap();
        let result = process(&[plugin], &SpanOp::Insert(row()), "pi", "session").unwrap();
        let failure = result.failure.unwrap();
        assert!(!failure.timed_out);
        assert!(failure.message.contains("workspace"));
        assert!(result.op.is_none());
    }

    #[test]
    fn rejects_identity_changes_and_drops_the_operation() {
        let dir = tempfile::tempdir().unwrap();
        let plugin = dir.path().join("identity.mjs");
        std::fs::write(
            &plugin,
            "export default span => ({...span, span_id: 'different'})",
        )
        .unwrap();
        let result = process(&[plugin], &SpanOp::Insert(row()), "codex", "session").unwrap();
        assert!(result
            .failure
            .as_ref()
            .unwrap()
            .message
            .contains("immutable span identity"));
        assert!(result.op.is_none());
    }

    #[test]
    fn interrupts_runaway_plugins_and_rejects_promises() {
        let dir = tempfile::tempdir().unwrap();
        let runaway = dir.path().join("runaway.mjs");
        std::fs::write(&runaway, "export default span => { while (true) {} }").unwrap();
        let started = Instant::now();
        let result = process(&[runaway], &SpanOp::Insert(row()), "codex", "session").unwrap();
        let failure = result.failure.as_ref().unwrap();
        assert!(failure.timed_out);
        assert!(failure.message.contains("plugin execution"));
        assert!(result.op.is_none());
        assert!(started.elapsed() < Duration::from_secs(2));

        let asynchronous = dir.path().join("async.mjs");
        std::fs::write(
            &asynchronous,
            "export default async span => ({...span, name: 'later'})",
        )
        .unwrap();
        let result = process(&[asynchronous], &SpanOp::Insert(row()), "codex", "session").unwrap();
        assert!(result
            .failure
            .as_ref()
            .unwrap()
            .message
            .contains("must be synchronous"));

        let non_json = dir.path().join("non-json.mjs");
        std::fs::write(&non_json, "export default () => Symbol('not-json')").unwrap();
        let result = process(&[non_json], &SpanOp::Insert(row()), "codex", "session").unwrap();
        assert!(result.failure.is_some());
        assert!(result.op.is_none());
    }

    #[test]
    fn a_failed_plugin_keeps_dropping_without_running_later_plugins() {
        let dir = tempfile::tempdir().unwrap();
        let first = dir.path().join("first.mjs");
        let broken = dir.path().join("broken.mjs");
        let last = dir.path().join("last.mjs");
        std::fs::write(
            &first,
            "export default span => ({...span, name: `first:${span.name}`})",
        )
        .unwrap();
        std::fs::write(
            &broken,
            "export default () => { throw new Error('broken') }",
        )
        .unwrap();
        std::fs::write(
            &last,
            "export default span => ({...span, name: `last:${span.name}`})",
        )
        .unwrap();
        let plugins = [first, broken.clone(), last];

        let first_result = process(&plugins, &SpanOp::Insert(row()), "codex", "session").unwrap();
        let first_failure = first_result.failure.unwrap();
        assert_eq!(first_failure.path, broken);
        assert!(first_failure.newly_seen);
        assert!(first_failure.message.contains("Error: broken"));
        assert!(first_result.op.is_none());

        let second_result = process(&plugins, &SpanOp::Insert(row()), "codex", "session").unwrap();
        let second_failure = second_result.failure.unwrap();
        assert_eq!(second_failure.path, broken);
        assert!(!second_failure.newly_seen);
        assert_eq!(second_failure.message, first_failure.message);
        assert!(second_result.op.is_none());
    }
}
