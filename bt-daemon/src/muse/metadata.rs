//! Authored Braintrust row shapes. Serialization into SpanRow is the final JSON
//! boundary; lifecycle and metric policy never navigate these JSON values.
use super::field::Field;
use super::hooks::*;
use serde::Serialize;
use serde_json::Value;

#[derive(Serialize)]
pub(crate) struct RootMetadata<'a> {
    pub source: &'static str,
    pub session_id: &'a str,
    pub username: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub muse_version: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub adapter_version: Option<&'a str>,
    #[serde(flatten)]
    pub session: &'a SessionMetadata,
}
#[derive(Serialize)]
pub(crate) struct ModelInput<'a> {
    pub messages: &'a Option<Vec<NativeMessage>>,
    pub tools: &'a Option<Vec<ToolDescription>>,
}
#[derive(Serialize)]
pub(crate) struct ModelStartMetadata<'a> {
    #[serde(skip_serializing_if = "Field::is_missing")]
    pub model: &'a Field<String>,
    #[serde(skip_serializing_if = "Field::is_missing")]
    pub provider: &'a Field<String>,
    #[serde(flatten)]
    pub details: &'a ModelDetails,
    #[serde(skip_serializing_if = "Field::is_missing")]
    pub request_record_id: &'a Field<String>,
    #[serde(skip_serializing_if = "Field::is_missing")]
    pub input_summary: &'a Field<InputSummary>,
}
#[derive(Serialize)]
pub(crate) struct ModelEndMetadata<'a> {
    #[serde(skip_serializing_if = "Field::is_missing")]
    pub finish_reason: &'a Field<String>,
    #[serde(skip_serializing_if = "Field::is_missing")]
    pub response_id: &'a Field<String>,
    #[serde(skip_serializing_if = "Field::is_missing")]
    pub tool_call_count: &'a Field<u64>,
    #[serde(skip_serializing_if = "Field::is_missing")]
    pub status: &'a Field<OperationStatus>,
    #[serde(skip_serializing_if = "Field::is_missing")]
    pub options: &'a Field<ProviderOptions>,
}
#[derive(Serialize)]
pub(crate) struct ToolStartMetadata<'a> {
    pub tool_name: &'a str,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub native_start_missing: bool,
}
#[derive(Serialize)]
pub(crate) struct ToolEndMetadata {
    pub tool_outcome: ToolOutcomeLabel,
}
#[derive(Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum ToolOutcomeLabel {
    Success,
    Error,
}
#[derive(Serialize)]
pub(crate) struct SubagentMetadata<'a> {
    pub subagent_id: &'a Option<String>,
    pub child_session_id: &'a Option<String>,
}
#[derive(Serialize)]
pub(crate) struct CompactMetadata<'a> {
    pub trigger: &'a Option<String>,
    pub native_operation_id_available: bool,
}
#[derive(Serialize)]
pub(crate) struct SessionEndMetadata<'a> {
    pub end_reason: &'a Field<ErrorDetail>,
}
#[derive(Serialize)]
pub(crate) struct UnpairedMetadata {
    pub unpaired: bool,
}
#[derive(Serialize)]
pub(crate) struct PermissionMetadata<'a> {
    pub tool_name: &'a Option<String>,
}
#[derive(Serialize)]
pub(crate) struct NotificationMetadata<'a> {
    pub tool_name: &'a Option<String>,
    pub notification_type: &'a Option<String>,
    pub title: &'a Option<String>,
    pub message: &'a Option<String>,
}
#[derive(Serialize)]
pub(crate) struct CompletionFingerprint<'a> {
    pub output: &'a Option<Value>,
    pub metrics: &'a Option<Value>,
    pub error: &'a Option<String>,
    pub metadata: &'a Option<Value>,
}
