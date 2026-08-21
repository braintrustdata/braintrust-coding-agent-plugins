//! Synchronous JavaScript span transforms.
//!
//! Session actors already execute on Tokio's worker pool. Each worker thread
//! lazily owns one QuickJS runtime and module cache, so JavaScript values never
//! cross threads and unrelated workers can transform spans concurrently.

use crate::translate::{SpanOp, SpanRow};
use rquickjs::{Context, Function, Module, Persistent, Runtime};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

const MEMORY_LIMIT_BYTES: usize = 64 * 1024 * 1024;
const STACK_LIMIT_BYTES: usize = 512 * 1024;
const CALL_TIMEOUT: Duration = Duration::from_millis(50);

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
    failed_plugins: HashSet<PathBuf>,
    env: BTreeMap<String, String>,
    context: Context,
    started: Instant,
    deadline_ms: Arc<AtomicU64>,
    // Must drop after every Context and persistent JavaScript value.
    _runtime: Runtime,
}

struct CachedModule {
    modified: Option<SystemTime>,
    len: u64,
    function: Persistent<Function<'static>>,
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
        runtime.set_interrupt_handler(Some(Box::new(move || {
            let deadline = interrupt_deadline.load(Ordering::Relaxed);
            deadline != 0 && interrupt_started.elapsed().as_millis() as u64 >= deadline
        })));
        let context = Context::full(&runtime)?;
        Ok(Self {
            modules: HashMap::new(),
            failed_plugins: HashSet::new(),
            env: environment(),
            context,
            started,
            deadline_ms,
            _runtime: runtime,
        })
    }

    fn load(&mut self, path: &Path) -> anyhow::Result<Persistent<Function<'static>>> {
        let metadata = std::fs::metadata(path)
            .map_err(|error| anyhow::anyhow!("failed to inspect {}: {error}", path.display()))?;
        let modified = metadata.modified().ok();
        if let Some(module) = self.modules.get(path) {
            if module.modified == modified && module.len == metadata.len() {
                return Ok(module.function.clone());
            }
        }
        let source = std::fs::read(path)
            .map_err(|error| anyhow::anyhow!("failed to read {}: {error}", path.display()))?;
        let digest = Sha256::digest(&source);
        let name = format!("bt-span-plugin:{digest:x}");
        self.arm_deadline();
        let function = self.context.with(|ctx| -> anyhow::Result<_> {
            let (module, promise) = Module::declare(ctx.clone(), name, source)?.eval()?;
            promise.finish::<()>()?;
            let function: Function<'_> = module
                .get("default")
                .map_err(|error| anyhow::anyhow!("default export is not a function: {error}"))?;
            Ok(Persistent::save(&ctx, function))
        });
        self.deadline_ms.store(0, Ordering::Relaxed);
        let function = function?;
        self.modules.insert(
            path.to_path_buf(),
            CachedModule {
                modified,
                len: metadata.len(),
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
        let function = self.load(path)?;
        let context = PluginContext {
            operation,
            source,
            session_id,
            env: &self.env,
        };
        self.arm_deadline();
        let result = self.context.with(|ctx| -> anyhow::Result<SpanRow> {
            let function = function.restore(&ctx)?;
            let row = rquickjs_serde::to_value(ctx.clone(), row)?;
            let context = rquickjs_serde::to_value(ctx.clone(), &context)?;
            let result = function.call::<_, rquickjs::Value<'_>>((row, context))?;
            if result.as_promise().is_some() {
                anyhow::bail!("plugin returned a Promise; span plugins must be synchronous");
            }
            Ok(rquickjs_serde::from_value_strict(result)?)
        });
        self.deadline_ms.store(0, Ordering::Relaxed);
        result
    }

    fn arm_deadline(&self) {
        let deadline = self
            .started
            .elapsed()
            .saturating_add(CALL_TIMEOUT)
            .as_millis() as u64;
        self.deadline_ms.store(deadline.max(1), Ordering::Relaxed);
    }
}

#[derive(Debug)]
pub struct PluginFailure {
    pub path: PathBuf,
    pub message: String,
}

pub struct ProcessResult {
    pub op: SpanOp,
    pub failures: Vec<PluginFailure>,
}

/// Apply an ordered plugin chain on the worker thread currently executing the
/// session actor. A failing plugin is skipped on subsequent calls handled by
/// this worker, while the rest of the ordered chain continues to run.
pub fn process(
    plugins: &[PathBuf],
    op: &SpanOp,
    source: &str,
    session_id: &str,
) -> anyhow::Result<ProcessResult> {
    if plugins.is_empty() {
        return Ok(ProcessResult {
            op: op.clone(),
            failures: Vec::new(),
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
    let mut failures = Vec::new();
    ENGINE.with_borrow_mut(|slot| -> anyhow::Result<()> {
        if slot.is_none() {
            *slot = Some(Engine::new()?);
        }
        let engine = slot.as_mut().expect("engine initialized");
        for plugin in plugins {
            if engine.failed_plugins.contains(plugin) {
                continue;
            }
            let candidate = engine.call(plugin, &row, operation, source, session_id);
            let candidate = match candidate {
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
                    engine.failed_plugins.insert(plugin.clone());
                    failures.push(PluginFailure {
                        path: plugin.clone(),
                        message,
                    });
                    continue;
                }
                Err(error) => {
                    engine.failed_plugins.insert(plugin.clone());
                    failures.push(PluginFailure {
                        path: plugin.clone(),
                        message: error.to_string(),
                    });
                    continue;
                }
            };
            row = candidate;
        }
        Ok(())
    })?;
    Ok(ProcessResult {
        op: match op {
            SpanOp::Insert(_) => SpanOp::Insert(row),
            SpanOp::Merge(_) => SpanOp::Merge(row),
        },
        failures,
    })
}

/// Compile each module and verify that it default-exports a function. Explicit
/// CLI commands call this before persisting or launching with a plugin chain.
pub fn validate(plugins: &[PathBuf]) -> anyhow::Result<()> {
    ENGINE.with_borrow_mut(|slot| -> anyhow::Result<()> {
        if slot.is_none() {
            *slot = Some(Engine::new()?);
        }
        let engine = slot.as_mut().expect("engine initialized");
        for plugin in plugins {
            engine.load(plugin)?;
        }
        Ok(())
    })
}

fn environment() -> BTreeMap<String, String> {
    std::env::vars_os()
        .filter_map(|(key, value)| Some((key.into_string().ok()?, value.into_string().ok()?)))
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
        assert!(result.failures.is_empty());
        let SpanOp::Insert(processed) = result.op else {
            panic!("expected insert")
        };
        assert_eq!(
            processed.name,
            format!("codex:{}:original", std::env::var("PATH").unwrap())
        );
        assert_eq!(processed.metadata.unwrap()["second"], true);
    }

    #[test]
    fn rejects_identity_changes_without_dropping_the_span() {
        let dir = tempfile::tempdir().unwrap();
        let plugin = dir.path().join("identity.mjs");
        std::fs::write(
            &plugin,
            "export default span => ({...span, span_id: 'different'})",
        )
        .unwrap();
        let result = process(&[plugin], &SpanOp::Insert(row()), "codex", "session").unwrap();
        assert_eq!(result.failures.len(), 1);
        assert!(result.failures[0]
            .message
            .contains("immutable span identity"));
        let SpanOp::Insert(processed) = result.op else {
            panic!("expected insert")
        };
        assert_eq!(processed.span_id, "span");
    }

    #[test]
    fn interrupts_runaway_plugins_and_rejects_promises() {
        let dir = tempfile::tempdir().unwrap();
        let runaway = dir.path().join("runaway.mjs");
        std::fs::write(&runaway, "export default span => { while (true) {} }").unwrap();
        let started = Instant::now();
        let result = process(&[runaway], &SpanOp::Insert(row()), "codex", "session").unwrap();
        assert_eq!(result.failures.len(), 1);
        assert!(started.elapsed() < Duration::from_secs(2));

        let asynchronous = dir.path().join("async.mjs");
        std::fs::write(
            &asynchronous,
            "export default async span => ({...span, name: 'later'})",
        )
        .unwrap();
        let result = process(&[asynchronous], &SpanOp::Insert(row()), "codex", "session").unwrap();
        assert_eq!(result.failures.len(), 1);
        assert!(result.failures[0].message.contains("must be synchronous"));

        let non_json = dir.path().join("non-json.mjs");
        std::fs::write(&non_json, "export default () => Symbol('not-json')").unwrap();
        let result = process(&[non_json], &SpanOp::Insert(row()), "codex", "session").unwrap();
        assert_eq!(result.failures.len(), 1);
    }

    #[test]
    fn skips_only_the_failed_plugin_and_continues_the_chain() {
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
        assert_eq!(first_result.failures.len(), 1);
        assert_eq!(first_result.failures[0].path, broken);
        let SpanOp::Insert(first_row) = first_result.op else {
            panic!("expected insert")
        };
        assert_eq!(first_row.name, "last:first:original");

        let second_result = process(&plugins, &SpanOp::Insert(row()), "codex", "session").unwrap();
        assert!(second_result.failures.is_empty());
        let SpanOp::Insert(second_row) = second_result.op else {
            panic!("expected insert")
        };
        assert_eq!(second_row.name, "last:first:original");
    }
}
