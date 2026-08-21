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
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

const MEMORY_LIMIT_BYTES: usize = 64 * 1024 * 1024;
const STACK_LIMIT_BYTES: usize = 512 * 1024;
const CALL_TIMEOUT: Duration = Duration::from_millis(50);
const MAX_RESULT_BYTES: usize = 8 * 1024 * 1024;

thread_local! {
    static ENGINE: RefCell<Option<Engine>> = const { RefCell::new(None) };
}

#[derive(Serialize)]
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
        context: &PluginContext<'_>,
    ) -> anyhow::Result<SpanRow> {
        let function = self.load(path)?;
        let row_json = serde_json::to_vec(row)?;
        let context_json = serde_json::to_vec(context)?;
        self.arm_deadline();
        let result = self.context.with(|ctx| -> anyhow::Result<Vec<u8>> {
            let function = function.restore(&ctx)?;
            let row = ctx.json_parse(row_json)?;
            let context = ctx.json_parse(context_json)?;
            let result = function.call::<_, rquickjs::Value<'_>>((row, context))?;
            if result.as_promise().is_some() {
                anyhow::bail!("plugin returned a Promise; span plugins must be synchronous");
            }
            let json = ctx
                .json_stringify(result)?
                .ok_or_else(|| anyhow::anyhow!("plugin returned a non-JSON value"))?;
            Ok(json.to_string()?.into_bytes())
        });
        self.deadline_ms.store(0, Ordering::Relaxed);
        let json = result?;
        if json.len() > MAX_RESULT_BYTES {
            anyhow::bail!(
                "plugin returned {} bytes, exceeding the {} byte limit",
                json.len(),
                MAX_RESULT_BYTES
            );
        }
        Ok(serde_json::from_slice(&json)?)
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

/// Apply an ordered plugin chain on the worker thread currently executing the
/// session actor. A plugin failure is reported to the caller, which can fail
/// open with the unmodified translator output.
pub fn process(
    plugins: &[PathBuf],
    op: &SpanOp,
    source: &str,
    session_id: &str,
    env: &BTreeMap<String, String>,
) -> anyhow::Result<SpanOp> {
    if plugins.is_empty() {
        return Ok(op.clone());
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
    let context = PluginContext {
        operation,
        source,
        session_id,
        env,
    };
    ENGINE.with_borrow_mut(|slot| -> anyhow::Result<()> {
        if slot.is_none() {
            *slot = Some(Engine::new()?);
        }
        let engine = slot.as_mut().expect("engine initialized");
        for plugin in plugins {
            row = engine.call(plugin, &row, &context)?;
            if (
                row.span_id.as_str(),
                row.root_span_id.as_str(),
                &row.parent_span_ids,
            ) != (
                original_ids.0.as_str(),
                original_ids.1.as_str(),
                &original_ids.2,
            ) {
                anyhow::bail!(
                    "plugin {} changed immutable span identity fields",
                    plugin.display()
                );
            }
        }
        Ok(())
    })?;
    Ok(match op {
        SpanOp::Insert(_) => SpanOp::Insert(row),
        SpanOp::Merge(_) => SpanOp::Merge(row),
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

pub fn environment() -> BTreeMap<String, String> {
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
            "export default (span, context) => ({...span, name: `${context.source}:${context.env.TEAM}:${span.name}`})",
        )
        .unwrap();
        std::fs::write(
            &second,
            "export default span => ({...span, metadata: {second: true}})",
        )
        .unwrap();
        let env = BTreeMap::from([("TEAM".into(), "platform".into())]);
        let processed = process(
            &[first, second],
            &SpanOp::Insert(row()),
            "codex",
            "session",
            &env,
        )
        .unwrap();
        let SpanOp::Insert(processed) = processed else {
            panic!("expected insert")
        };
        assert_eq!(processed.name, "codex:platform:original");
        assert_eq!(processed.metadata.unwrap()["second"], true);
    }

    #[test]
    fn rejects_identity_changes() {
        let dir = tempfile::tempdir().unwrap();
        let plugin = dir.path().join("identity.mjs");
        std::fs::write(
            &plugin,
            "export default span => ({...span, span_id: 'different'})",
        )
        .unwrap();
        let error = process(
            &[plugin],
            &SpanOp::Insert(row()),
            "codex",
            "session",
            &BTreeMap::new(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("immutable span identity"));
    }

    #[test]
    fn interrupts_runaway_plugins_and_rejects_promises() {
        let dir = tempfile::tempdir().unwrap();
        let runaway = dir.path().join("runaway.mjs");
        std::fs::write(&runaway, "export default span => { while (true) {} }").unwrap();
        let started = Instant::now();
        assert!(process(
            &[runaway],
            &SpanOp::Insert(row()),
            "codex",
            "session",
            &BTreeMap::new(),
        )
        .is_err());
        assert!(started.elapsed() < Duration::from_secs(2));

        let asynchronous = dir.path().join("async.mjs");
        std::fs::write(
            &asynchronous,
            "export default async span => ({...span, name: 'later'})",
        )
        .unwrap();
        let error = process(
            &[asynchronous],
            &SpanOp::Insert(row()),
            "codex",
            "session",
            &BTreeMap::new(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("must be synchronous"));
    }
}
