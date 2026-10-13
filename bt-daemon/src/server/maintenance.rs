//! Background maintenance: retention GC, idle-session retirement, the idle
//! watchdog, and draining on shutdown.

use super::*;

/// How long recovery state is kept on disk.
pub(super) const RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// How often that retention is enforced while the daemon keeps running.
pub(super) const GC_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// How long shutdown waits for in-flight work to reach a durable boundary.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// Age-based retention. A partition is deleted only when it holds no pending
/// work, however old; transcript mirrors it still references are kept.
pub(super) async fn collect_garbage(daemon: &Arc<Daemon>) {
    let data_dir = &daemon.data_dir;
    journal::gc_old_journals(data_dir, RETENTION).await;
    journal::gc_old_managed_runs(data_dir, RETENTION).await;
    gc_old_correlation_states(data_dir, RETENTION).await;
    let pipeline = &daemon.pipeline;
    let mut protected_mirrors = HashSet::new();
    if let Ok(rows) = pipeline
        .store
        .read(crate::pipeline::store::partitions)
        .await
    {
        let cutoff = now_ms().saturating_sub(RETENTION.as_millis() as i64);
        for row in rows {
            if row.last_activity_ms > cutoff || pipeline.runtime(&row.partition).is_some() {
                continue;
            }
            let pending = match pipeline_pending(pipeline, &row.partition).await {
                Ok(pending) => pending.work || pending.assembly_from.is_some(),
                Err(_) => true,
            };
            if pending {
                // Held work keeps every input it may still replay.
                protected_mirrors.extend(referenced_mirrors(pipeline, &row.partition).await);
                continue;
            }
            let partition = row.partition.clone();
            if let Err(error) = pipeline
                .store
                .write(move |tx| crate::pipeline::store::delete_partition(tx, &partition))
                .await
            {
                tracing::warn!(partition = %row.partition, %error, "retention delete failed");
                continue;
            }
            if let Err(error) = pipeline.events.delete(&row.partition) {
                tracing::warn!(partition = %row.partition, %error, "event journal not deleted");
            }
        }
    }
    gc_old_mirrors_except(data_dir, &protected_mirrors).await;
}

/// Transcript mirror files referenced by a partition's event journal.
async fn referenced_mirrors(
    pipeline: &Arc<crate::pipeline::runtime::Pipeline>,
    partition: &PartitionId,
) -> HashSet<PathBuf> {
    let mut cursor = pipeline.events.cursor(partition);
    tokio::task::spawn_blocking(move || {
        let mut mirrors = HashSet::new();
        while let Ok(Some((_, input))) = cursor.read_one() {
            if let crate::pipeline::types::TranslationInput::Native(env) = input {
                collect_mirror_paths(&env.payload, &mut mirrors);
            }
        }
        mirrors
    })
    .await
    .unwrap_or_default()
}

fn collect_mirror_paths(value: &Value, mirrors: &mut HashSet<PathBuf>) {
    match value {
        Value::Object(map) => {
            if let Some(Value::String(mirror)) = map.get("mirror") {
                mirrors.insert(PathBuf::from(mirror));
            }
            for value in map.values() {
                collect_mirror_paths(value, mirrors);
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_mirror_paths(value, mirrors);
            }
        }
        _ => {}
    }
}

async fn gc_old_mirrors_except(data_dir: &std::path::Path, protected: &HashSet<PathBuf>) {
    let dir = crate::transcript_mirror::mirror_dir(data_dir);
    let Ok(mut entries) = tokio::fs::read_dir(&dir).await else {
        return;
    };
    let now = SystemTime::now();
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if protected.contains(&path)
            || !matches!(
                path.extension().and_then(|value| value.to_str()),
                Some("jsonl" | "current")
            )
        {
            continue;
        }
        let old = entry
            .metadata()
            .await
            .ok()
            .and_then(|meta| meta.modified().ok())
            .and_then(|modified| now.duration_since(modified).ok())
            .is_some_and(|age| age > RETENTION);
        if old {
            let _ = tokio::fs::remove_file(&path).await;
        }
    }
}

pub(super) async fn gc_old_correlation_states(data_dir: &std::path::Path, max_age: Duration) {
    gc_old_correlation_dir(&data_dir.join("correlation"), max_age).await;
    gc_old_correlation_dir(&data_dir.join("correlation").join("parents"), max_age).await;
}

pub(super) async fn gc_old_correlation_dir(dir: &std::path::Path, max_age: Duration) {
    let Ok(mut entries) = tokio::fs::read_dir(dir).await else {
        return;
    };
    let now = SystemTime::now();
    while let Ok(Some(entry)) = entries.next_entry().await {
        let old = entry
            .metadata()
            .await
            .ok()
            .and_then(|metadata| metadata.modified().ok())
            .and_then(|modified| now.duration_since(modified).ok())
            .is_some_and(|age| age > max_age);
        if old && entry.file_type().await.is_ok_and(|kind| kind.is_file()) {
            let _ = tokio::fs::remove_file(entry.path()).await;
        }
    }
}

/// Retire partitions that have gone quiet. Their durable state stays; a later
/// event or restart resumes them. Partitions that are recovering stay loaded so
/// their recovery conditions keep being evaluated.
pub(super) fn spawn_session_reaper(daemon: Arc<Daemon>, idle_timeout: Duration) {
    if idle_timeout.is_zero() {
        return; // 0 disables retirement (useful in tests)
    }
    tokio::spawn(async move {
        let tick = (idle_timeout / 4).max(Duration::from_secs(1));
        loop {
            tokio::select! {
                _ = daemon.shutdown.notified() => return,
                _ = tokio::time::sleep(tick) => {}
            }
            for runtime in daemon.pipeline.runtimes() {
                if runtime.idle_for() < idle_timeout {
                    continue;
                }
                if daemon.partition_recovering(&runtime)
                    || daemon.partition_has_active_tools(&runtime)
                {
                    continue;
                }
                daemon.retire_partition(&runtime.partition).await;
            }
        }
    });
}

impl Daemon {
    pub(super) fn partition_recovering(
        &self,
        runtime: &crate::pipeline::runtime::PartitionRuntime,
    ) -> bool {
        runtime.translation.borrow().recovering.is_some()
            || runtime
                .branch_list()
                .iter()
                .any(|branch| branch.view.borrow().recovering.is_some())
    }

    pub(super) fn partition_has_active_tools(
        &self,
        runtime: &crate::pipeline::runtime::PartitionRuntime,
    ) -> bool {
        runtime.branch_list().iter().any(|branch| {
            self.correlation
                .has_active_tools(&crate::pipeline::translation::correlation_key(
                    &runtime.partition,
                    &branch.key,
                ))
        })
    }

    /// Finalize a quiet partition's translator for retirement, wait for its
    /// output to reach a durable boundary, then stop its machines.
    pub(super) async fn retire_partition(self: &Arc<Self>, partition: &PartitionId) {
        let keys: Vec<DeliveryKey> = self
            .claude_transcripts
            .lock()
            .unwrap()
            .keys()
            .filter(|key| &key.partition() == partition)
            .cloned()
            .collect();
        for key in &keys {
            self.observe_claude_transcripts(Some(key), false).await;
        }
        self.settle_assembly().await;
        if let Some(runtime) = self
            .pipeline
            .runtime(partition)
            .filter(|runtime| !runtime.retired.load(Ordering::SeqCst))
        {
            match self
                .pipeline
                .control(
                    partition,
                    Some(crate::pipeline::types::FinalizeKind::Retire),
                )
                .await
            {
                Ok(barrier) => {
                    self.pipeline
                        .wait_barrier(&runtime, &barrier, DRAIN_TIMEOUT)
                        .await;
                }
                Err(error) => {
                    tracing::warn!(%partition, %error, "retirement finalize not captured")
                }
            }
            for branch in runtime.branch_list() {
                self.correlation
                    .retire_session(&crate::pipeline::translation::correlation_key(
                        partition,
                        &branch.key,
                    ));
            }
        }
        self.pipeline.retire(partition).await;
        self.claude_transcripts
            .lock()
            .unwrap()
            .retain(|key, _| &key.partition() != partition);
        self.managed_run_sessions.lock().unwrap().retain(|_, keys| {
            keys.retain(|key| &key.partition() != partition);
            !keys.is_empty()
        });
        tracing::info!(%partition, "partition retired");
    }
}

pub(super) fn spawn_gc(daemon: Arc<Daemon>) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = daemon.shutdown.notified() => return,
                _ = tokio::time::sleep(GC_INTERVAL) => {}
            }
            collect_garbage(&daemon).await;
            daemon
                .correlation
                .prune_retired(crate::process::process_is_alive);
        }
    });
}

pub(super) fn spawn_idle_watchdog(daemon: Arc<Daemon>, idle_timeout: Duration) {
    if idle_timeout.is_zero() {
        return; // 0 disables the watchdog (useful in tests)
    }
    tokio::spawn(async move {
        let tick = (idle_timeout / 4).max(Duration::from_secs(1));
        loop {
            tokio::select! {
                _ = daemon.shutdown.notified() => return,
                _ = tokio::time::sleep(tick) => {}
            }
            let idle_for = daemon.last_activity.lock().unwrap().elapsed();
            if idle_for >= idle_timeout
                && daemon.active_work().await == 0
                && !daemon.correlation.has_any_active_tools()
            {
                tracing::info!("idle for {:?}; shutting down", idle_for);
                daemon.trigger_shutdown();
                return;
            }
        }
    });
}

pub(super) async fn drain_all(daemon: &Arc<Daemon>) {
    let mut drained = daemon.drained.lock().await;
    if *drained {
        return;
    }
    daemon.begin_quiesce();
    let _capture_guard = daemon.capture_gate.write().await;
    daemon.observe_claude_transcripts(None, true).await;
    daemon.settle_assembly().await;
    if let Err(error) = resolve_pending_sessions(daemon, |_| true).await {
        tracing::warn!(%error, "pending correlation decisions retained for recovery during shutdown");
    }
    let runtimes = daemon.pipeline.runtimes();
    let mut waits = Vec::new();
    for runtime in &runtimes {
        if daemon.partition_recovering(runtime) || runtime.retired.load(Ordering::SeqCst) {
            continue;
        }
        match daemon
            .pipeline
            .control(
                &runtime.partition,
                Some(crate::pipeline::types::FinalizeKind::Retire),
            )
            .await
        {
            Ok(barrier) => waits.push((runtime.clone(), barrier)),
            Err(error) => {
                tracing::warn!(partition = %runtime.partition, %error, "shutdown finalize not captured")
            }
        }
    }
    for (runtime, barrier) in waits {
        daemon
            .pipeline
            .wait_barrier(&runtime, &barrier, DRAIN_TIMEOUT)
            .await;
    }
    for runtime in runtimes {
        daemon.pipeline.retire(&runtime.partition).await;
    }
    *drained = true;
}

/// Is a live daemon answering at the endpoint? Connect and expect any line
/// back from a well-formed `initialize`.
pub(super) async fn probe_alive(endpoint: &std::path::Path) -> bool {
    let Ok(stream) = transport::connect(endpoint).await else {
        return false;
    };
    let (read_half, mut write_half) = tokio::io::split(stream);
    let init = Request::new(
        crate::wire::RequestId::Int(0),
        method::INITIALIZE,
        serde_json::json!({
            "protocol_version": PROTOCOL_VERSION,
            "client": { "source": "probe" }
        }),
    );
    let Ok(mut line) = Message::Request(init).to_line() else {
        return false;
    };
    line.push('\n');
    if write_half.write_all(line.as_bytes()).await.is_err() {
        return false;
    }
    let mut lines = BufReader::new(read_half).lines();
    matches!(
        tokio::time::timeout(Duration::from_secs(1), lines.next_line()).await,
        Ok(Ok(Some(_)))
    )
}
