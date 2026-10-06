//! Rebuildable, route-neutral logical revisions derived from the native WAL.
//! One source session translates each native record once. Route actors stream
//! the committed revisions and apply their own attachment and metadata.

use crate::journal::{self, JournalReader, JournalRecord};
use crate::translate::{AgentTranslator, Registry, SessionCtx, SpanOp, SpanRow};
use crate::wire::{Envelope, SessionConfig};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashSet;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, AsyncWriteExt};

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlKind {
    Checkpoint,
    Finalize,
}

#[derive(Serialize, Deserialize)]
struct ControlRecord {
    through: u64,
    kind: ControlKind,
}

#[derive(Serialize, Deserialize)]
struct Batch {
    ops: Vec<SpanOp>,
    /// SpanOp's late-merge key is sink-only and skipped by normal serde.
    late_merge_keys: Vec<Option<String>>,
}

impl Batch {
    fn new(ops: Vec<SpanOp>) -> Self {
        let late_merge_keys = ops
            .iter()
            .map(|op| row(op).late_merge_key.clone())
            .collect();
        Self {
            ops,
            late_merge_keys,
        }
    }

    fn into_ops(mut self) -> Vec<SpanOp> {
        for (op, key) in self.ops.iter_mut().zip(self.late_merge_keys) {
            row_mut(op).late_merge_key = key;
        }
        self.ops
    }
}

fn row(op: &SpanOp) -> &SpanRow {
    match op {
        SpanOp::Insert(row) | SpanOp::Merge(row) => row,
    }
}

fn row_mut(op: &mut SpanOp) -> &mut SpanRow {
    match op {
        SpanOp::Insert(row) | SpanOp::Merge(row) => row,
    }
}

struct State {
    translator: Box<dyn AgentTranslator>,
    native_through: u64,
    poisoned: bool,
    control_offset: u64,
}

pub struct SourceTranslation {
    source: String,
    session_id: String,
    data_dir: PathBuf,
    journal_path: PathBuf,
    state: tokio::sync::Mutex<State>,
}

impl SourceTranslation {
    pub fn new(
        source: &str,
        session_id: &str,
        translator_session_id: &str,
        data_dir: &Path,
        registry: &Registry,
    ) -> anyhow::Result<Arc<Self>> {
        let translator =
            registry.create_checked_with_session_namespace(source, translator_session_id)?;
        Ok(Arc::new(Self {
            source: source.into(),
            session_id: session_id.into(),
            data_dir: data_dir.into(),
            journal_path: journal::source_journal_path(data_dir, source, session_id),
            state: tokio::sync::Mutex::new(State {
                translator,
                native_through: 0,
                poisoned: false,
                control_offset: 0,
            }),
        }))
    }

    fn dir(&self) -> PathBuf {
        self.data_dir
            .join("derived")
            .join(crate::ids::session_storage_id(
                &self.source,
                &self.session_id,
            ))
    }

    fn event_path(&self, through: u64) -> PathBuf {
        self.dir().join(format!("event-{through:016x}.ndjson"))
    }

    fn control_path(&self, kind: ControlKind, through: u64) -> PathBuf {
        let kind = match kind {
            ControlKind::Checkpoint => "checkpoint",
            ControlKind::Finalize => "finalize",
        };
        self.dir().join(format!("{kind}-{through:016x}.ndjson"))
    }

    fn control_log_path(&self) -> PathBuf {
        self.dir().join("controls.ndjson")
    }

    /// Ensure all native events up to this offset have a committed logical
    /// revision. The mutex serializes translation across every route.
    pub async fn ensure_event(&self, through: u64) -> anyhow::Result<PathBuf> {
        let mut state = self.state.lock().await;
        anyhow::ensure!(
            !state.poisoned,
            "source translator needs a new daemon revision"
        );
        if state.native_through >= through && self.event_path(through).exists() {
            return Ok(self.event_path(through));
        }
        let mut reader =
            JournalReader::open_from(&self.journal_path, state.native_through, through)
                .await?
                .ok_or_else(|| anyhow::anyhow!("native session WAL is missing"))?;
        let current = state.native_through;
        self.apply_controls_through(&mut state, current).await?;
        while let Some(entry) = reader.next_record().await? {
            let JournalRecord::Event(redacted) = entry.record else {
                state.native_through = entry.through;
                continue;
            };
            let mut env = journal::envelope_from_redacted(redacted);
            if crate::translate::canonical_source_name(&env.source) != self.source {
                state.native_through = entry.through;
                continue;
            }
            env.source = self.source.clone();
            env.config = None;
            let path = self.event_path(entry.through);
            if path.exists() {
                self.advance_without_writing(&mut state, &env)?;
            } else if let Err(error) = self.write_event(&mut state, &env, &path).await {
                state.poisoned = true;
                return Err(error);
            }
            state.native_through = entry.through;
            self.apply_controls_through(&mut state, entry.through)
                .await?;
        }
        anyhow::ensure!(
            self.event_path(through).exists(),
            "native WAL ended before requested revision {through}"
        );
        Ok(self.event_path(through))
    }

    async fn apply_controls_through(&self, state: &mut State, through: u64) -> anyhow::Result<()> {
        let path = self.control_log_path();
        let mut file = match tokio::fs::File::open(&path).await {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        file.seek(std::io::SeekFrom::Start(state.control_offset))
            .await?;
        let mut reader = tokio::io::BufReader::new(file);
        loop {
            let mut line = String::new();
            let read = reader.read_line(&mut line).await?;
            if read == 0 {
                break;
            }
            let record: ControlRecord = serde_json::from_str(&line)?;
            if record.through > through {
                break;
            }
            let ctx = self.neutral_ctx();
            let first = match record.kind {
                ControlKind::Checkpoint => state.translator.checkpoint(&ctx),
                ControlKind::Finalize => state.translator.finalize(&ctx),
            };
            let first = match first {
                Ok(first) => first,
                Err(error) => {
                    state.poisoned = true;
                    return Err(error);
                }
            };
            let revision = self.control_path(record.kind, record.through);
            if revision.exists() {
                loop {
                    match state.translator.drain_pending(&ctx) {
                        Ok(Some(_)) => {}
                        Ok(None) => break,
                        Err(error) => {
                            state.poisoned = true;
                            return Err(error);
                        }
                    }
                }
            } else {
                if let Err(error) = self
                    .write_batches(&revision, first, || state.translator.drain_pending(&ctx))
                    .await
                {
                    state.poisoned = true;
                    return Err(error);
                }
            }
            state.control_offset += read as u64;
        }
        Ok(())
    }

    fn neutral_ctx(&self) -> SessionCtx {
        SessionCtx {
            session_id: self.session_id.clone(),
            config: None,
        }
    }

    fn advance_without_writing(&self, state: &mut State, env: &Envelope) -> anyhow::Result<()> {
        let ctx = self.neutral_ctx();
        state.translator.handle(env, &ctx)?;
        while state.translator.drain_pending(&ctx)?.is_some() {}
        Ok(())
    }

    async fn write_event(
        &self,
        state: &mut State,
        env: &Envelope,
        path: &Path,
    ) -> anyhow::Result<()> {
        let ctx = self.neutral_ctx();
        let first = state.translator.handle(env, &ctx)?;
        self.write_batches(path, first, || state.translator.drain_pending(&ctx))
            .await
    }

    async fn write_batches(
        &self,
        path: &Path,
        first: Vec<SpanOp>,
        mut next: impl FnMut() -> anyhow::Result<Option<Vec<SpanOp>>>,
    ) -> anyhow::Result<()> {
        crate::paths::ensure_private_dir(&self.dir())?;
        let temp = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
        let result = async {
            let mut options = tokio::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                options.mode(0o600);
            }
            let mut file = options.open(&temp).await?;
            let mut batch = Some(first);
            while let Some(ops) = batch {
                let mut bytes = serde_json::to_vec(&Batch::new(ops))?;
                bytes.push(b'\n');
                file.write_all(&bytes).await?;
                batch = next()?;
            }
            file.flush().await?;
            file.sync_data().await?;
            tokio::fs::rename(&temp, path).await?;
            Ok::<(), anyhow::Error>(())
        }
        .await;
        if result.is_err() {
            let _ = tokio::fs::remove_file(&temp).await;
        }
        result
    }

    pub async fn ensure_control(&self, kind: ControlKind, through: u64) -> anyhow::Result<PathBuf> {
        let mut state = self.state.lock().await;
        anyhow::ensure!(
            !state.poisoned,
            "source translator needs a new daemon revision"
        );
        anyhow::ensure!(
            state.native_through >= through,
            "source translator has not reached native offset {through}"
        );
        let path = self.control_path(kind, through);
        if path.exists() {
            return Ok(path);
        }
        crate::paths::ensure_private_dir(&self.dir())?;
        let control_path = self.control_log_path();
        let mut options = tokio::fs::OpenOptions::new();
        options.create(true).append(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut control_file = options.open(&control_path).await?;
        let mut line = serde_json::to_vec(&ControlRecord { through, kind })?;
        line.push(b'\n');
        control_file.write_all(&line).await?;
        control_file.flush().await?;
        control_file.sync_data().await?;
        crate::paths::restrict_file_to_owner(&control_path)?;
        let ctx = self.neutral_ctx();
        let first = match kind {
            ControlKind::Checkpoint => state.translator.checkpoint(&ctx),
            ControlKind::Finalize => state.translator.finalize(&ctx),
        };
        let first = match first {
            Ok(first) => first,
            Err(error) => {
                state.poisoned = true;
                return Err(error);
            }
        };
        if let Err(error) = self
            .write_batches(&path, first, || state.translator.drain_pending(&ctx))
            .await
        {
            state.poisoned = true;
            return Err(error);
        }
        state.control_offset += line.len() as u64;
        Ok(path)
    }

    pub fn route_translator(self: &Arc<Self>) -> Box<dyn AgentTranslator> {
        Box::new(DerivedTranslator {
            pending: None,
            roots: HashSet::new(),
        })
    }
}

struct DerivedTranslator {
    pending: Option<BufReader<std::fs::File>>,
    roots: HashSet<String>,
}

impl DerivedTranslator {
    fn read_next(&mut self, ctx: &SessionCtx) -> anyhow::Result<Option<Vec<SpanOp>>> {
        let Some(reader) = self.pending.as_mut() else {
            return Ok(None);
        };
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            self.pending = None;
            return Ok(None);
        }
        let ops = serde_json::from_str::<Batch>(&line)?.into_ops();
        Ok(Some(self.overlay(ops, ctx.config.as_ref())))
    }

    fn overlay(&mut self, mut ops: Vec<SpanOp>, config: Option<&SessionConfig>) -> Vec<SpanOp> {
        let Some(config) = config else {
            return ops;
        };
        let (parent, external_root) = config.attached_span_ids();
        let effective_root = external_root.or_else(|| parent.clone());
        for op in &mut ops {
            let is_insert = matches!(op, SpanOp::Insert(_));
            let row = row_mut(op);
            let is_top_root =
                row.span_id == row.root_span_id && row.parent_span_ids.is_empty() && is_insert;
            if is_top_root {
                self.roots.insert(row.span_id.clone());
            }
            if self.roots.contains(&row.root_span_id) {
                if let Some(root) = &effective_root {
                    row.root_span_id = root.clone();
                }
            }
            if self.roots.contains(&row.span_id) && row.parent_span_ids.is_empty() {
                if let Some(parent) = &parent {
                    row.parent_span_ids = vec![parent.clone()];
                }
            }
            if is_top_root {
                if let Some(additional) = config
                    .additional_metadata
                    .as_ref()
                    .and_then(Value::as_object)
                {
                    let metadata = row
                        .metadata
                        .get_or_insert_with(|| Value::Object(Map::new()));
                    if let Some(metadata) = metadata.as_object_mut() {
                        for (key, value) in additional {
                            if !key.starts_with("_bt_") {
                                metadata.entry(key.clone()).or_insert_with(|| value.clone());
                            }
                        }
                    }
                }
                if let Some(project) = config.project_name() {
                    if let Some(metadata) = row.metadata.as_mut().and_then(Value::as_object_mut) {
                        if metadata.get("source").and_then(Value::as_str) == Some("codex") {
                            metadata
                                .entry("project")
                                .or_insert_with(|| Value::String(project.into()));
                        }
                    }
                }
                if !config.tags.is_empty() {
                    let tags = row.tags.get_or_insert_with(Vec::new);
                    for tag in &config.tags {
                        if !tags.contains(tag) {
                            tags.push(tag.clone());
                        }
                    }
                }
            }
        }
        ops
    }
}

impl AgentTranslator for DerivedTranslator {
    fn set_revision_path(&mut self, path: &Path) {
        self.pending = std::fs::File::open(path).ok().map(BufReader::new);
    }

    fn handle(&mut self, _event: &Envelope, ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        self.read_next(ctx).map(|batch| batch.unwrap_or_default())
    }

    fn drain_pending(&mut self, ctx: &SessionCtx) -> anyhow::Result<Option<Vec<SpanOp>>> {
        self.read_next(ctx)
    }

    fn checkpoint(&mut self, ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        self.read_next(ctx).map(|batch| batch.unwrap_or_default())
    }

    fn finalize(&mut self, ctx: &SessionCtx) -> anyhow::Result<Vec<SpanOp>> {
        self.read_next(ctx).map(|batch| batch.unwrap_or_default())
    }
}
