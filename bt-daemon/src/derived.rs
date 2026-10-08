//! Durable, route-neutral span operation ledger produced from the event WAL.
//! One source session translates each native record once. Route actors stream
//! the committed revisions and apply their own attachment and metadata.

use crate::journal::{self, JournalReader, JournalRecord};
use crate::translate::{AgentTranslator, Registry, SessionCtx, SpanOp, SpanRow};
use crate::wire::{Envelope, SessionConfig};
use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlKind {
    Checkpoint,
    Finalize,
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

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct SpanRevision {
    pub start: u64,
    pub through: u64,
    pub envelope: Option<crate::wire::RedactedEnvelope>,
    pub sequence: u64,
    pub id: String,
}

pub(crate) fn retryable_storage(error: &anyhow::Error) -> bool {
    if let Some(rusqlite::Error::SqliteFailure(error, _)) = error.downcast_ref::<rusqlite::Error>()
    {
        use rusqlite::ErrorCode::*;
        return matches!(
            error.code,
            DatabaseBusy
                | DatabaseLocked
                | SystemIoFailure
                | DiskFull
                | CannotOpen
                | ReadOnly
                | PermissionDenied
        );
    }
    error.downcast_ref::<std::io::Error>().is_some()
}

#[derive(Debug, thiserror::Error)]
#[error("event WAL offset {start}: {message}")]
pub(crate) struct InputFailurePosition {
    pub start: u64,
    pub message: String,
}

struct State {
    translator: Box<dyn AgentTranslator>,
    native_through: u64,
    poisoned: bool,
}

pub struct SourceTranslation {
    source: String,
    session_id: String,
    dir: PathBuf,
    journal_path: PathBuf,
    state: tokio::sync::Mutex<State>,
}

fn open_database(path: &Path) -> anyhow::Result<rusqlite::Connection> {
    let db = rusqlite::Connection::open(path)?;
    let version: i64 = db.pragma_query_value(None, "user_version", |r| r.get(0))?;
    anyhow::ensure!(
        version <= 1,
        "unsupported span ledger schema version {version}"
    );
    db.busy_timeout(std::time::Duration::from_secs(5))?;
    db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA auto_vacuum=INCREMENTAL;
        CREATE TABLE IF NOT EXISTS continuation (
            singleton INTEGER PRIMARY KEY CHECK(singleton=1), through INTEGER NOT NULL,
            version INTEGER NOT NULL, state TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS revisions (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT UNIQUE NOT NULL, start INTEGER NOT NULL, through INTEGER NOT NULL,
            envelope TEXT, kind TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS revision_order ON revisions(through,kind);
        CREATE TABLE IF NOT EXISTS batches (
            revision TEXT NOT NULL, ordinal INTEGER NOT NULL, payload TEXT NOT NULL,
            PRIMARY KEY(revision,ordinal));
        CREATE TABLE IF NOT EXISTS consumers (
            route TEXT PRIMARY KEY, through INTEGER NOT NULL DEFAULT 0, sequence INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS controls(kind TEXT PRIMARY KEY, through INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS aliases(captured TEXT PRIMARY KEY, effective TEXT NOT NULL);")?;
    if version == 0 {
        db.pragma_update(None, "user_version", 1)?;
    }
    crate::paths::restrict_file_to_owner(path)?;
    Ok(db)
}

fn route_key(route: &crate::wire::SessionRoute) -> anyhow::Result<String> {
    let mut route = route.clone();
    route.auth.profile_id = None;
    route.auth.source = route.auth.effective_source();
    Ok(serde_json::to_string(&route)?)
}
fn effective_key(
    db: &rusqlite::Connection,
    route: &crate::wire::SessionRoute,
) -> anyhow::Result<String> {
    let key = route_key(route)?;
    Ok(db
        .query_row(
            "SELECT effective FROM aliases WHERE captured=?1",
            [&key],
            |r| r.get(0),
        )
        .optional()?
        .unwrap_or(key))
}

pub(crate) fn checkpoints(
    data_dir: &Path,
    source: &str,
    session_id: &str,
) -> anyhow::Result<Vec<(crate::wire::SessionRoute, u64)>> {
    let path = data_dir
        .join("derived")
        .join(crate::ids::session_storage_id(source, session_id))
        .join("spans.sqlite");
    if !path.exists() {
        return Ok(Vec::new());
    }
    let db = open_database(&path)?;
    let mut stmt = db.prepare("SELECT route,through FROM consumers")?;
    let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?;
    rows.map(|r| {
        let (route, through) = r?;
        Ok((serde_json::from_str(&route)?, through as u64))
    })
    .collect()
}
pub(crate) fn pending_output(
    data_dir: &Path,
    source: &str,
    session_id: &str,
    route: &crate::wire::SessionRoute,
) -> anyhow::Result<bool> {
    let path = data_dir
        .join("derived")
        .join(crate::ids::session_storage_id(source, session_id))
        .join("spans.sqlite");
    if !path.exists() {
        return Ok(false);
    }
    let db = open_database(&path)?;
    Ok(db.query_row("SELECT EXISTS(SELECT 1 FROM revisions WHERE sequence>COALESCE((SELECT sequence FROM consumers WHERE route=?1),0))",[effective_key(&db,route)?],|r|r.get(0))?)
}

impl SourceTranslation {
    pub fn new(
        source: &str,
        session_id: &str,
        translator_session_id: &str,
        data_dir: &Path,
        registry: &Registry,
    ) -> anyhow::Result<Arc<Self>> {
        let mut translator =
            registry.create_checked_with_session_namespace(source, translator_session_id)?;
        let dir = data_dir
            .join("derived")
            .join(crate::ids::session_storage_id(source, session_id));
        crate::paths::ensure_private_dir(&dir)?;
        let db = open_database(&dir.join("spans.sqlite"))?;
        let saved: Option<(i64, u32, String)> = db
            .query_row(
                "SELECT through,version,state FROM continuation WHERE singleton=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        let native_through = if let Some((through, version, snapshot)) = saved {
            anyhow::ensure!(version == 1, "unsupported translator continuation version");
            translator.restore(serde_json::from_str(&snapshot)?)?;
            through as u64
        } else {
            0
        };
        Ok(Arc::new(Self {
            source: source.into(),
            session_id: session_id.into(),
            dir,
            journal_path: journal::source_journal_path(data_dir, source, session_id),
            state: tokio::sync::Mutex::new(State {
                translator,
                native_through,
                poisoned: false,
            }),
        }))
    }

    fn event_path(&self, through: u64) -> PathBuf {
        self.dir.join(format!("event-{through:016x}"))
    }
    fn control_path(&self, kind: ControlKind, through: u64) -> PathBuf {
        let kind = match kind {
            ControlKind::Checkpoint => "checkpoint",
            ControlKind::Finalize => "finalize",
        };
        self.dir.join(format!("{kind}-{through:016x}"))
    }
    fn neutral_ctx(&self) -> SessionCtx {
        SessionCtx {
            session_id: self.session_id.clone(),
            config: None,
        }
    }

    /// Produce committed revisions from captured input. Only this upstream stage opens the event WAL.
    pub async fn ensure_event(&self, through: u64) -> anyhow::Result<PathBuf> {
        let mut state = self.state.lock().await;
        anyhow::ensure!(
            !state.poisoned,
            "source translator needs a new daemon revision"
        );
        for captured in journal::captured_routes(&self.journal_path)? {
            if let Some(route) = captured.envelope.route {
                self.register(&route, 0)?;
            }
        }
        if state.native_through >= through {
            return Ok(self.event_path(through));
        }
        let mut reader =
            JournalReader::open_from(&self.journal_path, state.native_through, through)
                .await?
                .ok_or_else(|| anyhow::anyhow!("native session WAL is missing"))?;
        while let Some(entry) = reader.next_record().await? {
            let (env, legacy_checkpoint) = match entry.record {
                JournalRecord::Event(redacted) => {
                    let mut env = journal::envelope_from_redacted(redacted);
                    if let Some(route) = env.route.as_ref() {
                        self.register(route, 0)?;
                    }
                    if crate::translate::canonical_source_name(&env.source) == self.source {
                        env.source = self.source.clone();
                        env.config = None;
                        (Some(env), None)
                    } else {
                        (None, None)
                    }
                }
                JournalRecord::DeliveryCheckpoint { route, through } => {
                    (None, Some((route, through)))
                }
            };
            let before = state.native_through;
            let ctx = self.neutral_ctx();
            let rollback = state.translator.snapshot()?;
            let result = async {
                let first = if let Some(env) = &env {
                    state.translator.handle(env, &ctx)?
                } else {
                    Vec::new()
                };
                let scratch = self
                    .stage_batches(first, || state.translator.drain_pending(&ctx))
                    .await?;
                let envelope = env.as_ref().map(|env| {
                    let mut envelope = env.redacted();
                    envelope.payload = Value::Null;
                    envelope.route = None;
                    envelope.capture = None;
                    envelope.managed_run_id = None;
                    envelope
                });
                let snapshot = state.translator.snapshot()?;
                self.commit(
                    &self.event_path(entry.through),
                    (before, entry.through),
                    envelope,
                    "event",
                    snapshot,
                    scratch,
                )
                .await
            }
            .await;
            if let Err(error) = result {
                if retryable_storage(&error) {
                    state.translator.restore(rollback)?;
                } else {
                    state.poisoned = true;
                }
                let message = error.to_string();
                return Err(error.context(InputFailurePosition {
                    start: before,
                    message,
                }));
            }
            state.native_through = entry.through;
            if let Some((route, through)) = legacy_checkpoint {
                let db = open_database(&self.dir.join("spans.sqlite"))?;
                let sequence = db.query_row(
                    "SELECT COALESCE(MAX(sequence),0) FROM revisions WHERE through<=?1",
                    [through as i64],
                    |r| r.get::<_, i64>(0),
                )? as u64;
                drop(db);
                self.register(&route, 0)?;
                self.acknowledge(&route, sequence).await?;
            }
        }
        anyhow::ensure!(
            state.native_through >= through,
            "native WAL ended before requested revision {through}"
        );
        Ok(self.event_path(through))
    }

    // Scratch output is uncommitted and disposable. Import it in one transaction with continuation
    // state, without accumulating a potentially large transcript's operations in memory.
    async fn stage_batches(
        &self,
        first: Vec<SpanOp>,
        mut next: impl FnMut() -> anyhow::Result<Option<Vec<SpanOp>>>,
    ) -> anyhow::Result<PathBuf> {
        let path = self.dir.join(format!("{}.tmp", uuid::Uuid::new_v4()));
        let result = async {
            let mut file = crate::durable::create_private(&path).await?;
            let mut batch = Some(first);
            while let Some(ops) = batch {
                let mut bytes = serde_json::to_vec(&Batch::new(ops))?;
                bytes.push(b'\n');
                file.write_all(&bytes).await?;
                batch = next()?;
            }
            file.flush().await?;
            Ok::<_, anyhow::Error>(())
        }
        .await;
        if let Err(error) = result {
            let _ = tokio::fs::remove_file(&path).await;
            return Err(error);
        }
        Ok(path)
    }

    async fn commit(
        &self,
        path: &Path,
        window: (u64, u64),
        envelope: Option<crate::wire::RedactedEnvelope>,
        kind: &str,
        snapshot: Value,
        scratch: PathBuf,
    ) -> anyhow::Result<()> {
        let (start, through) = window;
        let db_path = self.dir.join("spans.sqlite");
        let id = path.file_name().unwrap().to_string_lossy().into_owned();
        let kind = kind.to_owned();
        let cleanup = scratch.clone();
        let result = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            let mut db = open_database(&db_path)?;
            let tx = db.transaction()?;
            let mut created = false;
            let mut ordinal = 0i64;
            for line in BufReader::new(std::fs::File::open(scratch)?).lines() {
                let line = line?;
                let batch: Batch = serde_json::from_str(&line)?;
                if batch.ops.is_empty() {
                    continue;
                }
                if !created {
                    tx.execute("INSERT INTO revisions(id,start,through,envelope,kind) VALUES(?1,?2,?3,?4,?5)",params![id,start as i64,through as i64,envelope.as_ref().map(serde_json::to_string).transpose()?,kind])?;
                    created = true;
                }
                tx.execute("INSERT INTO batches VALUES(?1,?2,?3)",params![id,ordinal,line])?;
                ordinal += 1;
            }
            tx.execute("INSERT INTO continuation VALUES(1,?1,1,?2) ON CONFLICT(singleton) DO UPDATE SET through=excluded.through,state=excluded.state",
                params![through as i64,serde_json::to_string(&snapshot)?])?;
            if id.starts_with("finalize-") {
                tx.execute("INSERT INTO controls VALUES('finalize',?1) ON CONFLICT(kind) DO UPDATE SET through=excluded.through",[through as i64])?;
            }
            tx.commit()?;
            Ok(())
        }).await?;
        let _ = tokio::fs::remove_file(cleanup).await;
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
        let through = state.native_through;
        let path = match kind {
            ControlKind::Checkpoint => self
                .dir
                .join(format!("checkpoint-{}", uuid::Uuid::new_v4())),
            ControlKind::Finalize => self.control_path(kind, through),
        };
        let exists = matches!(kind, ControlKind::Finalize) && {
            let db = open_database(&self.dir.join("spans.sqlite"))?;
            db.query_row(
                "SELECT 1 FROM controls WHERE kind='finalize' AND through=?1",
                [through as i64],
                |r| r.get::<_, u32>(0),
            )
            .optional()?
            .is_some()
        };
        if exists {
            return Ok(path);
        }
        anyhow::ensure!(
            state.native_through >= through,
            "source translator has not reached native offset {through}"
        );
        let ctx = self.neutral_ctx();
        let rollback = state.translator.snapshot()?;
        let result = async {
            let first = match kind {
                ControlKind::Checkpoint => state.translator.checkpoint(&ctx),
                ControlKind::Finalize => state.translator.finalize(&ctx),
            }?;
            let scratch = self
                .stage_batches(first, || state.translator.drain_pending(&ctx))
                .await?;
            self.commit(
                &path,
                (through, state.native_through),
                None,
                "control",
                state.translator.snapshot()?,
                scratch,
            )
            .await
        }
        .await;
        if let Err(error) = result {
            if retryable_storage(&error) {
                state.translator.restore(rollback)?;
            } else {
                state.poisoned = true;
            }
            return Err(error);
        }
        Ok(path)
    }

    pub(crate) async fn collect_input(
        &self,
        journal: &tokio::sync::Mutex<crate::journal::JournalWriter>,
    ) -> anyhow::Result<()> {
        let state = self.state.lock().await;
        journal
            .lock()
            .await
            .collect_through(state.native_through)
            .await
    }

    /// Automatic parent selection changes the effective route, not the identity of its consumer.
    /// Bind the captured route to that resolved destination so it cannot leave a phantom cursor.
    pub(crate) fn bind_route(
        &self,
        captured: &crate::wire::SessionRoute,
        effective: &crate::wire::SessionRoute,
    ) -> anyhow::Result<()> {
        let captured = route_key(captured)?;
        let effective = route_key(effective)?;
        if captured == effective {
            return Ok(());
        }
        let mut db = open_database(&self.dir.join("spans.sqlite"))?;
        let tx = db.transaction()?;
        tx.execute("INSERT INTO aliases VALUES(?1,?2) ON CONFLICT(captured) DO UPDATE SET effective=excluded.effective",params![captured,effective])?;
        tx.execute("INSERT INTO consumers(route,through,sequence) SELECT ?2,through,sequence FROM consumers WHERE route=?1 ON CONFLICT(route) DO UPDATE SET through=MIN(through,excluded.through),sequence=MIN(sequence,excluded.sequence)",params![captured,effective])?;
        tx.execute("DELETE FROM consumers WHERE route=?1", [captured])?;
        tx.commit()?;
        Ok(())
    }

    /// Durable consumer registration survives actor retirement and daemon crashes.
    pub(crate) fn register(
        &self,
        route: &crate::wire::SessionRoute,
        through: u64,
    ) -> anyhow::Result<u64> {
        let db = open_database(&self.dir.join("spans.sqlite"))?;
        let route = effective_key(&db, route)?;
        db.execute(
            "INSERT OR IGNORE INTO consumers(route,through) VALUES(?1,?2)",
            params![route, through as i64],
        )?;
        Ok(db.query_row(
            "SELECT through FROM consumers WHERE route=?1",
            [route],
            |r| r.get::<_, i64>(0),
        )? as u64)
    }

    pub(crate) async fn acknowledge(
        &self,
        route: &crate::wire::SessionRoute,
        sequence: u64,
    ) -> anyhow::Result<u64> {
        let path = self.dir.join("spans.sqlite");
        let route = route_key(route)?;
        tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
            let mut db=open_database(&path)?;let tx=db.transaction()?;
            tx.execute("UPDATE consumers SET through=MAX(through,COALESCE(CASE WHEN ?2>=COALESCE((SELECT MAX(sequence) FROM revisions),0) THEN (SELECT through FROM continuation WHERE singleton=1) ELSE (SELECT through FROM revisions WHERE sequence=?2) END,through)),sequence=MAX(sequence,?2) WHERE route=?1",params![route,sequence as i64])?;
            let through=tx.query_row("SELECT through FROM consumers WHERE route=?1",[route],|r|r.get::<_,i64>(0))?;
            let floor:Option<i64>=tx.query_row("SELECT MIN(sequence) FROM consumers",[],|r|r.get(0))?;
            if let Some(floor)=floor {
                tx.execute("DELETE FROM batches WHERE revision IN(SELECT id FROM revisions WHERE sequence<=?1)",[floor])?;
                tx.execute("DELETE FROM revisions WHERE sequence<=?1",[floor])?;
            }
            tx.commit()?;
            // Freed pages are reusable immediately. Incremental vacuum bounds disk retained after a large backlog.
            db.execute_batch("PRAGMA incremental_vacuum(128)")?;
            Ok(through as u64)
        }).await?
    }

    pub(crate) async fn consumer_sequence(
        &self,
        route: &crate::wire::SessionRoute,
    ) -> anyhow::Result<u64> {
        let path = self.dir.join("spans.sqlite");
        let route = route_key(route)?;
        tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
            let db = open_database(&path)?;
            Ok(db.query_row(
                "SELECT sequence FROM consumers WHERE route=?1",
                [route],
                |r| r.get::<_, i64>(0),
            )? as u64)
        })
        .await?
    }
    pub(crate) async fn tip(&self) -> anyhow::Result<u64> {
        let path = self.dir.join("spans.sqlite");
        tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
            let db = open_database(&path)?;
            Ok(
                db.query_row("SELECT COALESCE(MAX(sequence),0) FROM revisions", [], |r| {
                    r.get::<_, i64>(0)
                })? as u64,
            )
        })
        .await?
    }
    /// Read one revision at a time so replay memory is bounded independently of session length.
    pub(crate) async fn next_revision(
        &self,
        after: u64,
        tip: u64,
    ) -> anyhow::Result<Option<SpanRevision>> {
        let path = self.dir.join("spans.sqlite");
        tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
            let db=open_database(&path)?;
            let value=db.query_row("SELECT start,through,envelope,id,sequence FROM revisions WHERE sequence>?1 AND sequence<=?2 ORDER BY sequence LIMIT 1",
                params![after as i64,tip as i64],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,i64>(1)?,r.get::<_,Option<String>>(2)?,r.get::<_,String>(3)?,r.get::<_,i64>(4)?))).optional()?;
            value.map(|(start,through,env,id,sequence)| Ok(SpanRevision { start:start as u64,through:through as u64,
                envelope:env.map(|s|serde_json::from_str(&s)).transpose()?,id,sequence:sequence as u64 })).transpose()
        }).await?
    }
    #[cfg(test)]
    async fn revisions(
        &self,
        route: &crate::wire::SessionRoute,
        _through: u64,
    ) -> anyhow::Result<Vec<SpanRevision>> {
        let mut after = self.consumer_sequence(route).await?;
        let tip = self.tip().await?;
        let mut result = Vec::new();
        while let Some(revision) = self.next_revision(after, tip).await? {
            after = revision.sequence;
            result.push(revision);
        }
        Ok(result)
    }
    pub(crate) fn revision_path_for_id(&self, id: &str) -> PathBuf {
        self.dir.join(id)
    }
    pub fn route_translator(self: &Arc<Self>) -> Box<dyn AgentTranslator> {
        Box::new(DerivedTranslator {
            dir: self.dir.clone(),
            pending: None,
            sequence: 0,
        })
    }
}

struct DerivedTranslator {
    dir: PathBuf,
    pending: Option<(String, u64)>,
    sequence: u64,
}
impl DerivedTranslator {
    fn read_next(&mut self, ctx: &SessionCtx) -> anyhow::Result<Option<Vec<SpanOp>>> {
        let Some((id, ordinal)) = self.pending.as_mut() else {
            return Ok(None);
        };
        let db = open_database(&self.dir.join("spans.sqlite"))?;
        if *ordinal == 0 {
            self.sequence = db
                .query_row(
                    "SELECT sequence FROM revisions WHERE id=?1",
                    [id.as_str()],
                    |r| r.get::<_, i64>(0),
                )
                .optional()?
                .unwrap_or(0) as u64;
        }
        let payload: Option<String> = db
            .query_row(
                "SELECT payload FROM batches WHERE revision=?1 AND ordinal=?2",
                params![id.as_str(), *ordinal as i64],
                |r| r.get(0),
            )
            .optional()?;
        let Some(payload) = payload else {
            self.pending = None;
            return Ok(None);
        };
        *ordinal += 1;
        let ops = serde_json::from_str::<Batch>(&payload)?.into_ops();
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
            let is_root = row.span_id == row.root_span_id && row.parent_span_ids.is_empty();
            if let Some(root) = &effective_root {
                row.root_span_id = root.clone();
            }
            if is_root {
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
    fn ledger_sequence(&self) -> Option<u64> {
        Some(self.sequence)
    }
    fn set_revision_path(&mut self, path: &Path) {
        let id = path.file_name().unwrap().to_string_lossy().into_owned();
        self.sequence = 0;
        self.pending = Some((id, 0));
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::SessionRoute;
    fn event(name: &str) -> Envelope {
        Envelope {
            source: "debug".into(),
            source_version: None,
            plugin_version: None,
            session_id: "ledger".into(),
            event: name.into(),
            ts_ms: 1,
            payload: serde_json::json!({"original":"payload"}),
            route: Some(SessionRoute::default()),
            config: None,
            managed_run_id: None,
            capture: None,
        }
    }
    fn source(data: &Path) -> Arc<SourceTranslation> {
        SourceTranslation::new(
            "debug",
            "ledger",
            "ledger",
            data,
            &Registry::default_agents(),
        )
        .unwrap()
    }
    fn read_all(translator: &mut dyn AgentTranslator, path: &Path) -> (Vec<SpanOp>, u64) {
        translator.set_revision_path(path);
        let ctx = SessionCtx {
            session_id: "ledger".into(),
            config: None,
        };
        let mut result = translator.handle(&event("unused"), &ctx).unwrap();
        while let Some(batch) = translator.drain_pending(&ctx).unwrap() {
            result.extend(batch);
        }
        (result, translator.ledger_sequence().unwrap())
    }

    #[tokio::test]
    async fn translation_commit_is_atomic_and_failed_input_survives() {
        let temp = tempfile::tempdir().unwrap();
        let producer = source(temp.path());
        let mut writer = journal::JournalWriter::open_path(&producer.journal_path)
            .await
            .unwrap();
        let through = writer.append(&event("first")).await.unwrap();
        let db = open_database(&producer.dir.join("spans.sqlite")).unwrap();
        db.execute_batch("CREATE TRIGGER fail_commit BEFORE INSERT ON continuation BEGIN SELECT RAISE(ABORT,'injected commit failure'); END;").unwrap();
        assert!(producer.ensure_event(through).await.is_err());
        assert_eq!(
            db.query_row("SELECT COUNT(*) FROM revisions", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            db.query_row("SELECT COUNT(*) FROM batches", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            db.query_row("SELECT COUNT(*) FROM continuation", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert!(std::fs::read_to_string(&producer.journal_path)
            .unwrap()
            .contains("original"));
        db.execute_batch("DROP TRIGGER fail_commit").unwrap();
        drop(db);
        drop(producer);
        let restarted = source(temp.path());
        let path = restarted.ensure_event(through).await.unwrap();
        assert_eq!(
            read_all(restarted.route_translator().as_mut(), &path)
                .0
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn temporary_database_lock_retries_without_losing_translator_state() {
        let temp = tempfile::tempdir().unwrap();
        let producer = source(temp.path());
        let mut writer = journal::JournalWriter::open_path(&producer.journal_path)
            .await
            .unwrap();
        let through = writer.append(&event("first")).await.unwrap();
        let db = open_database(&producer.dir.join("spans.sqlite")).unwrap();
        db.execute_batch("BEGIN IMMEDIATE").unwrap();
        let error = producer.ensure_event(through).await.unwrap_err();
        assert!(retryable_storage(&error), "{error:#}");
        db.execute_batch("ROLLBACK").unwrap();
        let path = producer.ensure_event(through).await.unwrap();
        let (ops, _) = read_all(producer.route_translator().as_mut(), &path);
        assert_eq!(ops.len(), 2);
        assert_eq!(row(&ops[1]).metadata.as_ref().unwrap()["seq"], 0);
    }

    #[tokio::test]
    async fn collected_input_is_not_needed_for_delivery_or_translator_continuation() {
        let temp = tempfile::tempdir().unwrap();
        let producer = source(temp.path());
        let writer = tokio::sync::Mutex::new(
            journal::JournalWriter::open_path(&producer.journal_path)
                .await
                .unwrap(),
        );
        let route = SessionRoute::default();
        producer.register(&route, 0).unwrap();
        let first = writer.lock().await.append(&event("first")).await.unwrap();
        let path = producer.ensure_event(first).await.unwrap();
        producer.collect_input(&writer).await.unwrap();
        assert!(!std::fs::read_to_string(&producer.journal_path)
            .unwrap()
            .contains("original"));
        drop(producer);
        let restarted = source(temp.path());
        let mut consumer = restarted.route_translator();
        let (ops, sequence) = read_all(consumer.as_mut(), &path);
        assert_eq!(ops.len(), 2, "pending spans survive collection and restart");
        restarted.acknowledge(&route, sequence).await.unwrap();
        assert!(restarted.revisions(&route, first).await.unwrap().is_empty());
        let second = writer.lock().await.append(&event("second")).await.unwrap();
        let second_path = restarted.ensure_event(second).await.unwrap();
        let (ops, _) = read_all(consumer.as_mut(), &second_path);
        assert_eq!(
            ops.len(),
            1,
            "continuation must not recreate the session root"
        );
        assert_eq!(row(&ops[0]).metadata.as_ref().unwrap()["seq"], 1);
    }

    #[tokio::test]
    async fn slowest_consumer_protects_event_and_control_operations() {
        let temp = tempfile::tempdir().unwrap();
        let producer = source(temp.path());
        let mut writer = journal::JournalWriter::open_path(&producer.journal_path)
            .await
            .unwrap();
        let route = SessionRoute::default();
        let mut blocked = route.clone();
        blocked.tags.push("blocked".into());
        producer.register(&route, 0).unwrap();
        producer.register(&blocked, 0).unwrap();
        let through = writer.append(&event("first")).await.unwrap();
        let path = producer.ensure_event(through).await.unwrap();
        let (_, first_seq) = read_all(producer.route_translator().as_mut(), &path);
        let control = producer.control_path(ControlKind::Checkpoint, through);
        let merge = SpanOp::Merge(SpanRow {
            span_id: "root".into(),
            root_span_id: "root".into(),
            end_ms: Some(2),
            ..Default::default()
        });
        let scratch = producer
            .stage_batches(vec![merge], || Ok(None))
            .await
            .unwrap();
        let snapshot = producer.state.lock().await.translator.snapshot().unwrap();
        producer
            .commit(
                &control,
                (through, through),
                None,
                "control",
                snapshot,
                scratch,
            )
            .await
            .unwrap();
        let (_, control_seq) = read_all(producer.route_translator().as_mut(), &control);
        assert!(control_seq > first_seq);
        producer.acknowledge(&route, control_seq).await.unwrap();
        assert_eq!(
            producer.revisions(&blocked, through).await.unwrap().len(),
            2
        );
        producer.acknowledge(&blocked, first_seq).await.unwrap();
        assert_eq!(
            producer.revisions(&blocked, through).await.unwrap().len(),
            1,
            "control remains despite the same input offset"
        );
        producer.acknowledge(&blocked, control_seq).await.unwrap();
        let db = open_database(&producer.dir.join("spans.sqlite")).unwrap();
        assert_eq!(
            db.query_row("SELECT COUNT(*) FROM batches", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            db.query_row("SELECT COUNT(*) FROM revisions", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            0
        );
    }
}
