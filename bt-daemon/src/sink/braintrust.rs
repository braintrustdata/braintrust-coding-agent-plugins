//! The Braintrust sink: maps sink-neutral [`SpanOp`]s onto `braintrust-sdk-rust`.
//!
//! Multi-profile: a session's backend URLs come from its own config (bt
//! resolves them per profile), so clients are built lazily and cached by
//! `(api_url, app_url)` — sessions on the same instance share a client,
//! sessions on different instances get their own. Within a client, each
//! session's token/org travel per span (`span_builder_with_credentials`) and
//! never leak across sessions. Span ids are the translator's deterministic
//! UUIDv5 strings, reused as the SDK `row_id` merge key so journal replay
//! re-emits idempotently.

use super::{Sink, SinkFactory};
use crate::translate::{SpanOp, SpanRow, SpanType};
use crate::wire::{SessionConfig, TraceDestination};
use braintrust_sdk_rust::{
    BraintrustClient, ParentSpanInfo, SpanComponents, SpanHandle, SpanLog, SpanObjectType,
    SpanOrigin, SpanType as SdkSpanType, DEFAULT_API_URL, DEFAULT_APP_URL,
};
use serde_json::{Map, Value};
use std::collections::{HashMap, VecDeque};
use std::io::Write;
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

/// Daemon-level Braintrust settings. `api_url`/`app_url` are *fallbacks* used
/// when a session's config doesn't carry its own (e.g. the standalone binary's
/// env); `bt` supplies per-session URLs, so these are usually `None`.
#[derive(Debug, Clone, Default)]
pub struct BraintrustSinkConfig {
    pub api_url: Option<String>,
    pub app_url: Option<String>,
    pub version: String,
}

/// Lazily-built, shared-by-URL client pool.
struct ClientCache {
    state: AsyncMutex<ClientCacheState>,
    version: String,
}

const CLIENT_CACHE_CAPACITY: usize = 16;
const QUEUE_FLUSH_BYTE_BUDGET: usize = 8 * 1024 * 1024;

#[derive(Default)]
struct ClientCacheState {
    clients: HashMap<(String, String), Arc<CachedClient>>,
    order: VecDeque<(String, String)>,
}

struct CachedClient {
    client: BraintrustClient,
    queued_bytes: AsyncMutex<usize>,
}

impl CachedClient {
    async fn account_and_flush(&self, bytes: usize) -> anyhow::Result<()> {
        let mut queued_bytes = self.queued_bytes.lock().await;
        *queued_bytes = queued_bytes.saturating_add(bytes);
        if *queued_bytes < QUEUE_FLUSH_BYTE_BUDGET {
            return Ok(());
        }
        self.client
            .flush()
            .await
            .map_err(|error| anyhow::anyhow!("braintrust flush failed: {error}"))?;
        *queued_bytes = 0;
        Ok(())
    }

    async fn flush(&self) -> anyhow::Result<()> {
        let mut queued_bytes = self.queued_bytes.lock().await;
        self.client
            .flush()
            .await
            .map_err(|error| anyhow::anyhow!("braintrust flush failed: {error}"))?;
        *queued_bytes = 0;
        Ok(())
    }
}

impl ClientCacheState {
    fn touch(&mut self, key: &(String, String)) {
        self.order.retain(|candidate| candidate != key);
        self.order.push_back(key.clone());
    }

    fn trim_inactive(&mut self) {
        while self.clients.len() >= CLIENT_CACHE_CAPACITY {
            let Some(index) = self.order.iter().position(|key| {
                self.clients
                    .get(key)
                    .is_some_and(|client| Arc::strong_count(client) == 1)
            }) else {
                // Every client is still referenced by a live session; those are
                // active working state and cannot be evicted safely.
                break;
            };
            if let Some(key) = self.order.remove(index) {
                self.clients.remove(&key);
            }
        }
    }
}

impl ClientCache {
    fn new(version: String) -> Self {
        Self {
            state: AsyncMutex::new(ClientCacheState::default()),
            version,
        }
    }

    async fn get_or_build(
        &self,
        api_url: &str,
        app_url: &str,
    ) -> anyhow::Result<Arc<CachedClient>> {
        let key = (api_url.to_string(), app_url.to_string());
        // Hold the lock across build so two sessions on a new URL don't build
        // duplicate clients. Build is cheap (skip_login: no network).
        let mut state = self.state.lock().await;
        if let Some(c) = state.clients.get(&key).cloned() {
            state.touch(&key);
            return Ok(c);
        }
        let client = BraintrustClient::builder()
            .skip_login(true)
            .span_origin(SpanOrigin::new().version(self.version.clone()))
            .api_url(api_url.to_string())
            .app_url(app_url.to_string())
            .build()
            .await
            .map_err(|e| anyhow::anyhow!("braintrust client build failed: {e}"))?;
        let arc = Arc::new(CachedClient {
            client,
            queued_bytes: AsyncMutex::new(0),
        });
        state.trim_inactive();
        state.clients.insert(key.clone(), arc.clone());
        state.touch(&key);
        Ok(arc)
    }
}

/// Hands out a per-session sink over the shared client pool.
pub struct BraintrustSinkFactory {
    cache: Arc<ClientCache>,
    default_api_url: Option<String>,
    default_app_url: Option<String>,
    version: String,
}

impl BraintrustSinkFactory {
    pub fn new(cfg: BraintrustSinkConfig) -> Self {
        Self {
            cache: Arc::new(ClientCache::new(cfg.version.clone())),
            default_api_url: cfg.api_url,
            default_app_url: cfg.app_url,
            version: cfg.version,
        }
    }
}

impl SinkFactory for BraintrustSinkFactory {
    fn create(
        &self,
        _session_id: &str,
        source: &str,
        plugin_version: Option<&str>,
    ) -> anyhow::Result<Box<dyn Sink>> {
        Ok(Box::new(BraintrustSink {
            cache: self.cache.clone(),
            default_api_url: self.default_api_url.clone(),
            default_app_url: self.default_app_url.clone(),
            version: plugin_version.unwrap_or(&self.version).to_string(),
            source: source.to_string(),
            creds: None,
            urls: None,
            client: None,
            open: HashMap::new(),
        }))
    }
}

/// Per-session resolved credentials + trace-attach settings.
struct Creds {
    token: String,
    org_id: String,
    org_name: Option<String>,
    destination: Option<TraceDestination>,
}

impl Creds {
    fn same_as(&self, other: &Self) -> bool {
        self.token == other.token
            && self.org_id == other.org_id
            && self.org_name == other.org_name
            && serde_json::to_value(&self.destination).ok()
                == serde_json::to_value(&other.destination).ok()
    }
}

struct BraintrustSink {
    cache: Arc<ClientCache>,
    default_api_url: Option<String>,
    default_app_url: Option<String>,
    version: String,
    source: String,
    creds: Option<Creds>,
    /// Resolved `(api_url, app_url)` for this session, from its config.
    urls: Option<(String, String)>,
    /// The client for `urls`, obtained from the cache on first emit.
    client: Option<Arc<CachedClient>>,
    /// Live span handles keyed by deterministic span id, so a later op (e.g.
    /// setting `end`) merges onto the same row the SDK already knows.
    open: HashMap<String, SpanHandle<BraintrustClient>>,
}

impl BraintrustSink {
    fn project(&self, creds: &Creds) -> String {
        if let Some(TraceDestination::ProjectLogs {
            project_name: Some(project_name),
            ..
        }) = &creds.destination
        {
            return project_name.clone();
        }
        self.source.clone()
    }

    fn parent_info(
        &self,
        row: &SpanRow,
        creds: &Creds,
        project: &str,
    ) -> anyhow::Result<ParentSpanInfo> {
        if row.parent_span_ids.is_empty() {
            if let Some(destination) = &creds.destination {
                return root_destination(destination, project);
            }
            Ok(ParentSpanInfo::ProjectName {
                project_name: project.to_string(),
            })
        } else {
            Ok(full_span(
                creds,
                project,
                row.parent_span_ids[0].clone(),
                destination_root(creds).unwrap_or_else(|| row.root_span_id.clone()),
            ))
        }
    }

    async fn ensure_client(&mut self) -> anyhow::Result<Arc<CachedClient>> {
        if let Some(c) = &self.client {
            return Ok(c.clone());
        }
        let urls = self
            .urls
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("session has no config/URLs yet"))?;
        let client = self.cache.get_or_build(&urls.0, &urls.1).await?;
        self.client = Some(client.clone());
        Ok(client)
    }

    fn ensure_handle(&mut self, client: &BraintrustClient, row: &SpanRow) -> anyhow::Result<()> {
        if self.open.contains_key(&row.span_id) {
            return Ok(());
        }
        let creds = self
            .creds
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("session has no credentials/config yet"))?;
        let project = self.project(creds);
        let parent = self.parent_info(row, creds, &project)?;

        let mut builder = client
            .span_builder_with_credentials(creds.token.clone(), creds.org_id.clone())
            .span_type(map_span_type(row.span_type))
            .span_id(row.span_id.clone())
            .row_id(row.span_id.clone())
            .parent_info(parent)
            .span_origin(
                SpanOrigin::new()
                    .name(format!("braintrust.plugin.{}", self.source))
                    .version(self.version.clone())
                    .instrumentation("braintrust-plugin"),
            );
        if let Some(org_name) = &creds.org_name {
            builder = builder.org_name(org_name.clone());
        }
        if let Some(start) = row.start_ms {
            builder = builder.start_time(ms_to_secs(start));
        }
        self.open.insert(row.span_id.clone(), builder.build());
        Ok(())
    }

    fn update_open(&mut self, client: &BraintrustClient, row: &SpanRow) -> anyhow::Result<()> {
        self.ensure_handle(client, row)?;
        let handle = self.open.get(&row.span_id).expect("just inserted");
        handle.log(build_log(row)?);
        if let Some(end) = row.end_ms {
            handle.end_with_time(ms_to_secs(end));
            // SpanHandle retains the complete accumulated input/output. Once a
            // span is terminal, use stateless SDK merges for any unusually late
            // update instead of pinning that payload for the session lifetime.
            self.open.remove(&row.span_id);
        }
        Ok(())
    }

    fn merge_closed(&self, client: &BraintrustClient, row: &SpanRow) -> anyhow::Result<()> {
        let creds = self
            .creds
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("session has no credentials/config yet"))?;
        let project = self.project(creds);
        let components = self.span_components(row, creds, &project).to_str();
        client
            .update_span_with_credentials(
                creds.token.clone(),
                creds.org_id.clone(),
                &components,
                build_log(row)?,
            )
            .map_err(|error| anyhow::anyhow!("braintrust span merge failed: {error}"))
    }

    fn span_components(&self, row: &SpanRow, creds: &Creds, project: &str) -> SpanComponents {
        let destination = creds
            .destination
            .as_ref()
            .map(|destination| destination_components(destination, project))
            .unwrap_or_else(|| {
                let mut args = Map::new();
                args.insert("project_name".into(), Value::String(project.to_string()));
                DestinationComponents {
                    object_type: SpanObjectType::ProjectLogs,
                    object_id: None,
                    compute_object_metadata_args: Some(args),
                    propagated_event: None,
                }
            });
        SpanComponents {
            object_type: destination.object_type,
            object_id: destination.object_id,
            compute_object_metadata_args: destination.compute_object_metadata_args,
            row_id: Some(row.span_id.clone()),
            span_id: Some(row.span_id.clone()),
            root_span_id: Some(destination_root(creds).unwrap_or_else(|| row.root_span_id.clone())),
            span_parents: (!row.parent_span_ids.is_empty()).then(|| row.parent_span_ids.clone()),
            propagated_event: destination.propagated_event,
        }
    }
}

#[async_trait::async_trait]
impl Sink for BraintrustSink {
    fn configure(&mut self, config: &SessionConfig) {
        let api = config
            .auth
            .api_url
            .clone()
            .or_else(|| self.default_api_url.clone())
            .unwrap_or_else(|| DEFAULT_API_URL.to_string());
        let app = config
            .auth
            .app_url
            .clone()
            .or_else(|| self.default_app_url.clone())
            .unwrap_or_else(|| DEFAULT_APP_URL.to_string());
        let new_urls = (api, app);
        let urls_changed = self.urls.as_ref() != Some(&new_urls);
        if urls_changed {
            // A session shouldn't change backend URLs mid-flight; if it does,
            // rebind the client on the next emit. Pre-change open handles stay
            // bound to the old client (pathological; just noted).
            if self.client.is_some() {
                tracing::warn!(
                    source = %self.source,
                    "session changed backend URLs mid-session; rebinding client"
                );
            }
            self.urls = Some(new_urls);
            self.client = None;
        }
        let next_creds = Creds {
            token: config.auth.token.clone(),
            org_id: config.auth.org_id.clone().unwrap_or_default(),
            org_name: config.auth.org_name.clone(),
            destination: config.destination.clone(),
        };
        // Span handles capture their credentials when built. Recreate them
        // after a profile token or routing change; deterministic row ids make
        // subsequent updates merge into the same Braintrust rows.
        if urls_changed
            || self
                .creds
                .as_ref()
                .is_some_and(|old| !old.same_as(&next_creds))
        {
            self.open.clear();
        }
        self.creds = Some(next_creds);
    }

    async fn emit(&mut self, ops: &[SpanOp]) -> anyhow::Result<u64> {
        let client = self.ensure_client().await?;
        let mut n = 0u64;
        for op in ops {
            match op {
                SpanOp::Insert(row) => self.update_open(&client.client, row)?,
                SpanOp::Merge(row) if self.open.contains_key(&row.span_id) => {
                    self.update_open(&client.client, row)?
                }
                SpanOp::Merge(row) => self.merge_closed(&client.client, row)?,
            }
            n += 1;
            client
                .account_and_flush(serialized_len(op).unwrap_or(0))
                .await?;
        }
        Ok(n)
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        match &self.client {
            Some(client) => client.flush().await,
            None => Ok(()),
        }
    }
}

fn full_span(
    creds: &Creds,
    project: &str,
    span_id: String,
    root_span_id: String,
) -> ParentSpanInfo {
    if let Some(destination) = &creds.destination {
        let components = destination_components(destination, project);
        return ParentSpanInfo::FullSpan {
            object_type: components.object_type,
            object_id: components.object_id,
            compute_object_metadata_args: components.compute_object_metadata_args,
            span_id,
            root_span_id,
            span_parents: None,
            propagated_event: components.propagated_event,
        };
    }
    let mut cma = Map::new();
    cma.insert(
        "project_name".to_string(),
        Value::String(project.to_string()),
    );
    ParentSpanInfo::FullSpan {
        object_type: SpanObjectType::ProjectLogs,
        object_id: None,
        compute_object_metadata_args: Some(cma),
        span_id,
        root_span_id,
        span_parents: None,
        propagated_event: None,
    }
}

fn root_destination(
    destination: &TraceDestination,
    project: &str,
) -> anyhow::Result<ParentSpanInfo> {
    Ok(match destination {
        TraceDestination::ProjectLogs {
            project_id: Some(object_id),
            ..
        } => ParentSpanInfo::ProjectLogs {
            object_id: object_id.clone(),
        },
        TraceDestination::ProjectLogs {
            project_name: Some(project_name),
            ..
        } => ParentSpanInfo::ProjectName {
            project_name: project_name.clone(),
        },
        TraceDestination::ProjectLogs { .. } => ParentSpanInfo::ProjectName {
            project_name: project.to_string(),
        },
        TraceDestination::Experiment { experiment_id } => ParentSpanInfo::Experiment {
            object_id: experiment_id.clone(),
        },
        TraceDestination::ParentSpan { components } => components
            .to_parent_span_info_resolving_metadata()
            .map_err(|error| anyhow::anyhow!("invalid parent span destination: {error}"))?,
    })
}

fn destination_root(creds: &Creds) -> Option<String> {
    match &creds.destination {
        Some(TraceDestination::ParentSpan { components }) => components.root_span_id.clone(),
        _ => None,
    }
}

struct DestinationComponents {
    object_type: SpanObjectType,
    object_id: Option<String>,
    compute_object_metadata_args: Option<Map<String, Value>>,
    propagated_event: Option<Map<String, Value>>,
}

fn destination_components(destination: &TraceDestination, project: &str) -> DestinationComponents {
    match destination {
        TraceDestination::ProjectLogs {
            project_id,
            project_name,
        } => {
            let mut args = Map::new();
            if let Some(project_id) = project_id {
                args.insert("project_id".into(), Value::String(project_id.clone()));
            }
            args.insert(
                "project_name".into(),
                Value::String(project_name.as_deref().unwrap_or(project).to_string()),
            );
            DestinationComponents {
                object_type: SpanObjectType::ProjectLogs,
                object_id: project_id.clone(),
                compute_object_metadata_args: Some(args),
                propagated_event: None,
            }
        }
        TraceDestination::Experiment { experiment_id } => DestinationComponents {
            object_type: SpanObjectType::Experiment,
            object_id: Some(experiment_id.clone()),
            compute_object_metadata_args: None,
            propagated_event: None,
        },
        TraceDestination::ParentSpan { components } => DestinationComponents {
            object_type: components.object_type,
            object_id: components.object_id.clone(),
            compute_object_metadata_args: components.compute_object_metadata_args.clone(),
            propagated_event: components.propagated_event.clone(),
        },
    }
}

fn map_span_type(t: SpanType) -> SdkSpanType {
    match t {
        SpanType::Task => SdkSpanType::Task,
        SpanType::Llm => SdkSpanType::Llm,
        SpanType::Tool => SdkSpanType::Tool,
    }
}

fn ms_to_secs(ms: i64) -> f64 {
    ms as f64 / 1000.0
}

fn build_log(row: &SpanRow) -> anyhow::Result<SpanLog> {
    // The span's display name is carried on the log event, not the builder.
    // An empty name means "unchanged" (many merge ops use `..Default::default()`
    // and don't rename the span) — omitting `.name()` avoids overwriting the
    // already-set name with an empty string on merge.
    let mut lb = SpanLog::builder();
    if !row.name.is_empty() {
        lb = lb.name(row.name.clone());
    }
    if let Some(input) = &row.input {
        lb = lb.input(input.clone());
    }
    if let Some(output) = &row.output {
        lb = lb.output(output.clone());
    }
    if let Some(Value::Object(md)) = &row.metadata {
        lb = lb.metadata(md.clone());
    }
    let mut metrics = row
        .metrics
        .as_ref()
        .and_then(Value::as_object)
        .map(|metrics| {
            metrics
                .iter()
                .filter_map(|(k, v)| v.as_f64().map(|f| (k.clone(), f)))
                .collect::<HashMap<String, f64>>()
        })
        .unwrap_or_default();
    // Stateless updates do not have a SpanHandle on which to call `end`, so
    // carry terminal timing in the merge payload itself.
    if let Some(end) = row.end_ms {
        metrics.insert("end".into(), ms_to_secs(end));
    }
    if !metrics.is_empty() {
        lb = lb.metrics(metrics);
    }
    if let Some(err) = &row.error {
        lb = lb.error(Value::String(err.clone()));
    }
    if let Some(tags) = &row.tags {
        if !tags.is_empty() {
            lb = lb.tags(tags.clone());
        }
    }
    lb.build()
        .map_err(|e| anyhow::anyhow!("span log build failed: {e}"))
}

#[derive(Default)]
struct ByteCounter(usize);

impl Write for ByteCounter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self.0.saturating_add(bytes.len());
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn serialized_len(op: &SpanOp) -> serde_json::Result<usize> {
    let mut counter = ByteCounter::default();
    serde_json::to_writer(&mut counter, op)?;
    Ok(counter.0)
}
