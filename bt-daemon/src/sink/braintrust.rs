//! The Braintrust sink: maps sink-neutral [`SpanOp`]s onto `braintrust-sdk-rust`.
//!
//! Each routing branch tracks its own writes on the shared SDK delivery service.
//! Credentials travel with each span, and deterministic row IDs make journal
//! replay idempotent.

use super::{Sink, SinkFactory};
use crate::translate::{SpanOp, SpanRow, SpanType};
use crate::wire::{SessionConfig, TraceDestination};
use braintrust_api_client::backend::{
    extensions::{AttachmentRequest, AttachmentUploadStatus},
    BackendClient, HttpError,
};
use braintrust_sdk_rust::{
    BraintrustClient, Completion, DeliveryReceipt, ParentSpanInfo, SpanComponents, SpanHandle,
    SpanLog, SpanObjectType, SpanType as SdkSpanType, WriteError, DEFAULT_API_URL, DEFAULT_APP_URL,
};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::collections::{HashMap, VecDeque};
use std::io::Write;

/// Daemon-level Braintrust settings. `api_url`/`app_url` are *fallbacks* used
/// when a session's config doesn't carry its own (e.g. the standalone binary's
/// env); `bt` supplies per-session URLs, so these are usually `None`.
#[derive(Debug, Clone, Default)]
pub struct BraintrustSinkConfig {
    pub api_url: Option<String>,
    pub app_url: Option<String>,
}

const QUEUE_FLUSH_BYTE_BUDGET: usize = 8 * 1024 * 1024;
type SpanWrite = Completion<DeliveryReceipt, WriteError>;

#[cfg(any(test, feature = "test-support"))]
fn configure_test_sdk_runtime() -> braintrust_sdk_rust::Result<()> {
    use braintrust_sdk_rust::{configure_service, ServiceConfig, ServiceRuntime};

    // Test cases have independent Tokio runtimes. Give the process-wide SDK
    // service a runtime that survives the first test to finish.
    configure_service(
        ServiceConfig::builder()
            .runtime(ServiceRuntime::Dedicated)
            .build(),
    )
}

/// Hands out a sink for each routing branch.
pub struct BraintrustSinkFactory {
    default_api_url: Option<String>,
    default_app_url: Option<String>,
}

impl BraintrustSinkFactory {
    pub fn new(cfg: BraintrustSinkConfig) -> Self {
        Self {
            default_api_url: cfg.api_url,
            default_app_url: cfg.app_url,
        }
    }
    fn create_sink(&self, source: &str) -> BraintrustSink {
        BraintrustSink {
            default_api_url: self.default_api_url.clone(),
            default_app_url: self.default_app_url.clone(),
            source: source.to_string(),
            creds: None,
            urls: None,
            client: None,
            pending_writes: VecDeque::new(),
            write_failure: None,
            queued_bytes: 0,
            attachment_uploads: None,
            open: HashMap::new(),
            pending_roots: HashMap::new(),
            delivered_root: None,
        }
    }
}

impl SinkFactory for BraintrustSinkFactory {
    fn create(&self, _session_id: &str, source: &str) -> anyhow::Result<Box<dyn Sink>> {
        Ok(Box::new(self.create_sink(source)))
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

/// Only completed keys are retained, never attachment bytes. This bounded
/// optimization may miss after eviction or restart; check conflicts with the API.
struct AttachmentUploads {
    backend: BackendClient,
    completed: crate::translate::recent::RecentSet<String>,
}

#[derive(Deserialize)]
struct AttachmentConflict {
    #[serde(rename = "Message")]
    message: String,
}

impl AttachmentUploads {
    async fn upload(&mut self, request: &AttachmentRequest, data: &[u8]) -> anyhow::Result<()> {
        if let Err(error) = self.backend.upload_attachment(request, data.to_vec()).await {
            let duplicate = error.downcast_ref::<HttpError>().is_some_and(|http| {
                http.status.as_u16() == 409
                    && serde_json::from_str::<AttachmentConflict>(&http.body)
                        .is_ok_and(|body| body.message.starts_with("Attachment key already exists"))
            });
            if !duplicate {
                return Err(error);
            }

            // A key is reserved before the object is uploaded. Require both
            // a completed status and an object of the expected size.
            let metadata = self.backend.get_attachment(request).await?;
            if !matches!(metadata.status, AttachmentUploadStatus::Done)
                || metadata.content_length != Some(data.len() as u64)
            {
                return Err(error);
            }
        }
        self.completed.insert(request.key.clone());
        Ok(())
    }
}

struct BraintrustSink {
    default_api_url: Option<String>,
    default_app_url: Option<String>,
    source: String,
    creds: Option<Creds>,
    /// Resolved `(api_url, app_url)` for this session, from its config.
    urls: Option<(String, String)>,
    /// The SDK client bound to `urls`, built on first emit.
    client: Option<BraintrustClient>,
    pending_writes: VecDeque<SpanWrite>,
    write_failure: Option<WriteError>,
    queued_bytes: usize,
    attachment_uploads: Option<AttachmentUploads>,
    /// Live span handles keyed by deterministic span id, so a later op (e.g.
    /// setting `end`) merges onto the same row the SDK already knows.
    open: HashMap<String, SpanHandle<BraintrustClient>>,
    /// Roots marked by translators as provisional, keyed by their local span
    /// id. Each is exported only once direct descendant work arrives.
    pending_roots: HashMap<String, Vec<SpanOp>>,
    /// Root span id of the trace that most recently received a row, which
    /// is also that root's deterministic row id in Braintrust.
    delivered_root: Option<String>,
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

    fn ensure_client(&mut self) -> anyhow::Result<BraintrustClient> {
        if let Some(c) = &self.client {
            return Ok(c.clone());
        }
        let urls = self
            .urls
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("session has no config/URLs yet"))?;
        #[cfg(any(test, feature = "test-support"))]
        configure_test_sdk_runtime()?;
        let client = BraintrustClient::builder()
            .api_url(urls.0.clone())
            .app_url(urls.1.clone())
            .build_without_login()?;
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
            .parent_info(parent);
        if let Some(org_name) = &creds.org_name {
            builder = builder.org_name(org_name.clone());
        }
        if let Some(start) = row.start_ms {
            builder = builder.start_time(ms_to_secs(start));
        }
        self.open.insert(row.span_id.clone(), builder.build());
        Ok(())
    }

    fn update_open(
        &mut self,
        client: &BraintrustClient,
        row: &SpanRow,
    ) -> anyhow::Result<SpanWrite> {
        self.ensure_handle(client, row)?;
        let handle = self.open.get(&row.span_id).ok_or_else(|| {
            anyhow::anyhow!("span handle was not initialized for {}", row.span_id)
        })?;
        let write = handle.log(build_log(row, &self.source, false)?);
        if row.end_ms.is_some() && write.submission().is_ok() {
            // SpanHandle retains the complete accumulated input/output. Once a
            // span is terminal, use stateless SDK merges for any unusually late
            // update instead of pinning that payload for the session lifetime.
            self.open.remove(&row.span_id);
        }
        Ok(write)
    }

    fn merge_closed(&self, client: &BraintrustClient, row: &SpanRow) -> anyhow::Result<SpanWrite> {
        let creds = self
            .creds
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("session has no credentials/config yet"))?;
        let project = self.project(creds);
        let mut components = self.span_components(row, creds, &project);
        if row.origin.is_none() {
            // An attached parent's provenance is not the existing span's origin.
            // Legacy spans have no birth snapshot with which to override it.
            if let Some(context) = components
                .propagated_event
                .as_mut()
                .and_then(|event| event.get_mut("context"))
                .and_then(Value::as_object_mut)
            {
                context.remove("span_origin");
            }
        }
        client
            .update_span_with_credentials_tracked(
                creds.token.clone(),
                creds.org_id.clone(),
                &components.to_str(),
                build_log(row, &self.source, true)?,
            )
            .map_err(anyhow::Error::new)
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

    async fn emit_op(&mut self, op: &SpanOp) -> anyhow::Result<()> {
        let (SpanOp::Insert(row) | SpanOp::Merge(row)) = op;
        let prepared;
        let op = if row.attachments.is_empty() {
            op
        } else {
            let creds = self
                .creds
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("missing attachment credentials"))?;
            let (api, app) = self
                .urls
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("missing attachment backend"))?;
            if self.attachment_uploads.is_none() {
                self.attachment_uploads = Some(AttachmentUploads {
                    backend: BackendClient::new(
                        api,
                        app,
                        &creds.token,
                        &creds.org_id,
                        creds.org_name.as_deref().unwrap_or_default(),
                        concat!("bt-daemon/", env!("CARGO_PKG_VERSION")),
                    )?,
                    completed: Default::default(),
                });
            }
            let uploads = self
                .attachment_uploads
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("missing attachment upload client"))?;
            let mut references = HashMap::new();
            for attachment in &row.attachments {
                let key = uuid::Uuid::new_v5(
                    &uuid::Uuid::NAMESPACE_URL,
                    format!("{api}/{}/{}", creds.org_id, attachment.digest).as_bytes(),
                )
                .to_string();
                let reference =
                    crate::attachments::AttachmentReference::uploaded(key.clone(), attachment);
                if !uploads.completed.contains(&key) {
                    uploads
                        .upload(
                            &AttachmentRequest::new(
                                key,
                                attachment.filename.clone(),
                                attachment.content_type.clone(),
                                creds.org_id.clone(),
                            ),
                            &attachment.data,
                        )
                        .await?;
                }
                references.insert(attachment.digest.clone(), reference);
            }
            let mut row = row.clone();
            for value in [&mut row.input, &mut row.output].into_iter().flatten() {
                crate::attachments::materialize(value, &references);
            }
            let metadata = row.metadata.get_or_insert_with(|| json!({}));
            if let Some(metadata) = metadata.as_object_mut() {
                metadata.insert(
                    "braintrust.attachments".into(),
                    json!(references.into_values().collect::<Vec<_>>()),
                );
            }
            row.attachments.clear();
            prepared = match op {
                SpanOp::Insert(_) => SpanOp::Insert(row),
                SpanOp::Merge(_) => SpanOp::Merge(row),
            };
            &prepared
        };
        let bytes = serialized_len(op)?;
        let client = self.ensure_client()?;
        let write = match op {
            SpanOp::Insert(row) => self.update_open(&client, row)?,
            SpanOp::Merge(row) if self.open.contains_key(&row.span_id) => {
                self.update_open(&client, row)?
            }
            SpanOp::Merge(row) => self.merge_closed(&client, row)?,
        };
        write.submission().as_ref().map_err(Clone::clone)?;
        self.pending_writes.push_back(write);
        self.queued_bytes = self.queued_bytes.saturating_add(bytes);
        if self.queued_bytes >= QUEUE_FLUSH_BYTE_BUDGET {
            self.flush_writes().await?;
        }
        Ok(())
    }

    async fn flush_writes(&mut self) -> anyhow::Result<()> {
        // A client flush only sees writes still in flight at its cutoff. A
        // receipt also reports failures that finished before the daemon flush.
        while let Some(write) = self.pending_writes.front_mut() {
            if let Err(error) = write.await {
                self.write_failure.get_or_insert(error);
            }
            self.pending_writes.pop_front();
        }
        self.queued_bytes = 0;
        match self.write_failure.take() {
            Some(error) => Err(error.into()),
            None => Ok(()),
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
            // Rebind the client on the next emit. Pending receipts still
            // report writes submitted to the previous backend.
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
            self.attachment_uploads = None;
        }
        self.creds = Some(next_creds);
    }

    async fn emit(&mut self, ops: &[SpanOp]) -> anyhow::Result<u64> {
        let mut n = 0u64;
        for op in ops {
            let op_id = op_span_id(op);
            if let Some(pending) = self.pending_roots.get_mut(op_id) {
                pending.push(op.clone());
                continue;
            }
            if let Some(root_id) = direct_pending_parent(op, &self.pending_roots) {
                let pending = self
                    .pending_roots
                    .remove(&root_id)
                    .ok_or_else(|| anyhow::anyhow!("pending root {root_id} is missing"))?;
                for pending_op in pending {
                    self.emit_op(&pending_op).await?;
                    n += 1;
                }
            }
            if is_provisional_root(op) {
                self.pending_roots
                    .insert(op_id.to_string(), vec![op.clone()]);
                continue;
            }
            self.emit_op(op).await?;
            n += 1;
            let root = op_root_span_id(op);
            if self.delivered_root.as_deref() != Some(root) {
                self.delivered_root = Some(root.to_string());
            }
        }
        Ok(n)
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        self.flush_writes().await
    }

    fn has_pending_delivery(&self) -> bool {
        !self.pending_roots.is_empty()
    }

    fn permalink(&self) -> Option<String> {
        let creds = self.creds.as_ref()?;
        // Logs URLs address projects by name. Experiment and attached-parent
        // destinations, and routes that only know a project id, have none.
        match &creds.destination {
            None
            | Some(TraceDestination::ProjectLogs {
                project_name: Some(_),
                ..
            })
            | Some(TraceDestination::ProjectLogs {
                project_id: None, ..
            }) => {}
            Some(_) => return None,
        }
        let org_name = creds.org_name.as_deref()?;
        let root = self.delivered_root.as_deref()?;
        let (_, app_url) = self.urls.as_ref()?;
        Some(format!(
            "{}/app/{}/p/{}/logs?r={}",
            app_url.trim_end_matches('/'),
            encode_url_component(org_name),
            encode_url_component(&self.project(creds)),
            encode_url_component(root),
        ))
    }
}

fn op_span_id(op: &SpanOp) -> &str {
    match op {
        SpanOp::Insert(row) | SpanOp::Merge(row) => &row.span_id,
    }
}

fn op_root_span_id(op: &SpanOp) -> &str {
    match op {
        SpanOp::Insert(row) | SpanOp::Merge(row) => &row.root_span_id,
    }
}

/// Percent-encodes everything outside the RFC 3986 unreserved set.
fn encode_url_component(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push('%');
            encoded.push(char::from(HEX[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX[usize::from(byte & 0x0F)]));
        }
    }
    encoded
}

fn direct_pending_parent(
    op: &SpanOp,
    pending_roots: &HashMap<String, Vec<SpanOp>>,
) -> Option<String> {
    let row = match op {
        SpanOp::Insert(row) | SpanOp::Merge(row) => row,
    };
    row.parent_span_ids
        .iter()
        .find(|parent| pending_roots.contains_key(*parent))
        .cloned()
}

fn is_provisional_root(op: &SpanOp) -> bool {
    let SpanOp::Insert(row) = op else {
        return false;
    };
    row.metadata
        .as_ref()
        .and_then(Value::as_object)
        .and_then(|metadata| metadata.get("_bt_defer_root"))
        .and_then(Value::as_bool)
        == Some(true)
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
        // Braintrust's span type enum has no dedicated wait value. Keep waits
        // as task spans so their elapsed time cannot be mistaken for tool time.
        SpanType::Wait => SdkSpanType::Task,
        SpanType::Llm => SdkSpanType::Llm,
        SpanType::Tool => SdkSpanType::Tool,
    }
}

fn ms_to_secs(ms: i64) -> f64 {
    ms as f64 / 1000.0
}

fn build_log(row: &SpanRow, source: &str, is_merge: bool) -> anyhow::Result<SpanLog> {
    let mut builder = SpanLog::builder();
    if let Some(origin) = &row.origin {
        // Translation supplies the immutable birth snapshot on every span op.
        // Explicit null prevents SDK version defaults on spans with unknown
        // plugin versions, including stateless merges after close or recovery.
        builder = builder.context(json!({
            "span_origin": {
                "name": format!("braintrust.plugin.{source}"),
                "version": origin.plugin_version,
                "bt": { "version": origin.bt_version },
                (source): { "version": origin.source_version },
                "instrumentation": { "name": "braintrust-plugin" },
            },
        }));
    } else if is_merge {
        // An update without a saved snapshot must leave existing provenance
        // untouched, not replace legacy plugin attribution with SDK defaults.
        builder = builder.skip_span_origin();
    }

    // The span's display name is carried on the log event, not the builder.
    // An empty name means "unchanged" (many merge ops use `..Default::default()`
    // and don't rename the span) — omitting `.name()` avoids overwriting the
    // already-set name with an empty string on merge.
    if !row.name.is_empty() {
        builder = builder.name(row.name.clone());
    }
    if let Some(input) = &row.input {
        builder = builder.input(input.clone());
    }
    if let Some(output) = &row.output {
        builder = builder.output(output.clone());
    }
    let mut metadata = row
        .metadata
        .as_ref()
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    metadata.remove("_bt_defer_root");
    builder = builder.metadata(metadata);
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
    // Submit terminal timing with the same tracked write as the span data.
    if let Some(end) = row.end_ms {
        metrics.insert("end".into(), ms_to_secs(end));
    }
    if !metrics.is_empty() {
        builder = builder.metrics(metrics);
    }
    if let Some(err) = &row.error {
        builder = builder.error(Value::String(err.clone()));
    }
    if let Some(tags) = &row.tags {
        if !tags.is_empty() {
            builder = builder.tags(tags.clone());
        }
    }
    builder
        .build()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::translate::OriginSnapshot;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn flush_reports_a_merge_rejected_before_the_flush() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/version"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/logs3"))
            .respond_with(ResponseTemplate::new(401))
            .expect(1)
            .mount(&server)
            .await;
        let factory = BraintrustSinkFactory::new(BraintrustSinkConfig::default());
        let mut sink = factory.create_sink("codex");
        sink.configure(&SessionConfig {
            auth: crate::wire::BackendAuth {
                token: "sk-test".into(),
                api_url: Some(server.uri()),
                app_url: Some(server.uri()),
                org_id: Some("org-test".into()),
                ..Default::default()
            },
            destination: Some(TraceDestination::ProjectLogs {
                project_id: Some("proj-test".into()),
                project_name: None,
            }),
            flush_mode: Default::default(),
            additional_metadata: None,
            tags: Default::default(),
            span_plugins: Vec::new(),
        });
        sink.emit(&[SpanOp::Merge(SpanRow {
            span_id: "root".into(),
            root_span_id: "root".into(),
            end_ms: Some(2_000),
            ..Default::default()
        })])
        .await
        .unwrap();

        // Wait for delivery through the SDK. Its first flush can return the
        // rejection if the write is still in flight when it captures its cutoff.
        let client = sink.ensure_client().unwrap();
        let _delivery_barrier = client.flush().await;
        assert_eq!(client.flush().await.unwrap().delivered(), 0);

        let error = sink.flush().await.unwrap_err();
        assert!(error.chain().any(|cause| matches!(
            cause.downcast_ref::<braintrust_sdk_rust::BraintrustError>(),
            Some(braintrust_sdk_rust::BraintrustError::Api { status: 401, .. })
        )));
    }

    #[tokio::test]
    async fn root_context_retains_sdk_environment_on_insert_and_stateless_merge() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/version"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/logs3"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .mount(&server)
            .await;
        configure_test_sdk_runtime().unwrap();
        let client = BraintrustClient::builder()
            .skip_login(true)
            .api_url(server.uri())
            .app_url(server.uri())
            .environment("test", Some("sink-test"))
            .build()
            .await
            .unwrap();
        let row = SpanRow {
            span_id: "root".into(),
            root_span_id: "root".into(),
            origin: Some(
                OriginSnapshot {
                    plugin_version: None,
                    bt_version: "birth-bt".into(),
                    source_version: None,
                }
                .into(),
            ),
            ..Default::default()
        };
        let handle = client
            .span_builder_with_credentials("sk-test", "org-test")
            .span_id(row.span_id.clone())
            .row_id(row.span_id.clone())
            .parent_info(ParentSpanInfo::ProjectLogs {
                object_id: "proj-test".into(),
            })
            .build();
        handle
            .log(build_log(&row, "codex", false).unwrap())
            .await
            .unwrap();

        let mut components = SpanComponents::new(SpanObjectType::ProjectLogs);
        components.object_id = Some("proj-test".into());
        components.row_id = Some(row.span_id.clone());
        components.span_id = Some(row.span_id.clone());
        components.root_span_id = Some(row.root_span_id.clone());
        client
            .update_span_with_credentials_tracked(
                "sk-test",
                "org-test",
                &components.to_str(),
                build_log(&row, "codex", true).unwrap(),
            )
            .unwrap()
            .await
            .unwrap();

        let requests = server.received_requests().await.unwrap();
        let rows: Vec<Value> = requests
            .iter()
            .filter(|request| request.url.path() == "/logs3")
            .flat_map(|request| {
                serde_json::from_slice::<Value>(&request.body).unwrap()["rows"]
                    .as_array()
                    .unwrap()
                    .clone()
            })
            .collect();
        assert!(rows.len() >= 2);
        for row in rows {
            let origin = &row["context"]["span_origin"];
            assert_eq!(origin["name"], "braintrust.plugin.codex");
            assert_eq!(origin.get("version"), Some(&Value::Null));
            assert_eq!(origin["bt"]["version"], "birth-bt");
            assert_eq!(
                origin["environment"],
                json!({"type": "test", "name": "sink-test"})
            );
        }
    }
}
