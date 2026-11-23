//! OpenCode permission variants own their normalized request and reply precedence.
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Default, Deserialize, Serialize)]
struct PermissionToolRecord {
    #[serde(default, rename = "sessionID")]
    session_id: Option<String>,
    #[serde(default, rename = "callID")]
    call_id: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    tool: Option<String>,
    #[serde(default)]
    input: Option<Value>,
}

#[derive(Clone, Deserialize)]
#[serde(untagged)]
#[derive(Serialize)]
enum PermissionTool {
    Name(String),
    Record(PermissionToolRecord),
}

#[derive(Clone, Default, Deserialize, Serialize)]
struct PermissionFields {
    #[serde(default)]
    id: Option<String>,
    #[serde(default, rename = "sessionID")]
    session_id: Option<String>,
    #[serde(default, rename = "callID")]
    call_id: Option<String>,
    #[serde(default)]
    tool: Option<PermissionTool>,
    #[serde(default, rename = "toolName")]
    tool_name: Option<String>,
    #[serde(default, rename = "permission")]
    permission: Option<String>,
    #[serde(default)]
    input: Option<Value>,
    #[serde(default)]
    args: Option<Value>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default, rename = "type", alias = "permissionType")]
    permission_type: Option<String>,
}

#[derive(Deserialize)]
#[serde(untagged)]
#[derive(Serialize)]
enum PermissionPayload {
    Record(Box<PermissionFields>),
    Kind(String),
}

#[derive(Default, Deserialize, Serialize)]
pub(super) struct PermissionProperties {
    #[serde(default)]
    permission: Option<PermissionPayload>,
    #[serde(default)]
    info: Option<PermissionFields>,
    #[serde(default, rename = "permissionID")]
    permission_id: Option<String>,
    #[serde(default, rename = "requestID")]
    request_id: Option<String>,
    #[serde(default)]
    reply: Option<String>,
    #[serde(default)]
    response: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(flatten)]
    direct: PermissionFields,
}

impl PermissionTool {
    fn record(&self) -> Option<&PermissionToolRecord> {
        match self {
            Self::Record(record) => Some(record),
            Self::Name(_) => None,
        }
    }

    fn name(&self) -> Option<&str> {
        match self {
            Self::Name(name) => Some(name),
            Self::Record(_) => None,
        }
    }
}

impl PermissionProperties {
    pub(super) fn decode(payload: &Value) -> Option<Self> {
        super::decode(payload.get("properties").unwrap_or(payload))
    }
    pub(super) fn reply(&self) -> &str {
        self.reply
            .as_deref()
            .or(self.response.as_deref())
            .or(self.status.as_deref())
            .unwrap_or("")
    }
    pub(super) fn request(&self) -> PermissionRequest {
        let permission_kind = match &self.permission {
            Some(PermissionPayload::Kind(kind)) => Some(kind.clone()),
            _ => None,
        };
        let source = match &self.permission {
            Some(PermissionPayload::Record(record)) => record,
            _ => self.info.as_ref().unwrap_or(&self.direct),
        };
        let tool_record = source.tool.as_ref().and_then(PermissionTool::record);
        PermissionRequest {
            id: source
                .id
                .clone()
                .or_else(|| self.direct.id.clone())
                .or_else(|| self.permission_id.clone())
                .or_else(|| self.request_id.clone()),
            session_id: source
                .session_id
                .clone()
                .or_else(|| self.direct.session_id.clone())
                .or_else(|| tool_record.and_then(|tool| tool.session_id.clone())),
            call_id: source
                .call_id
                .clone()
                .or_else(|| tool_record.and_then(|tool| tool.call_id.clone())),
            tool: source
                .tool
                .as_ref()
                .and_then(PermissionTool::name)
                .map(str::to_owned)
                .or_else(|| source.tool_name.clone())
                .or_else(|| tool_record.and_then(|tool| tool.name.clone()))
                .or_else(|| tool_record.and_then(|tool| tool.tool.clone()))
                // OpenCode's native permission events put the requested tool
                // kind in `permission`, separate from `tool.callID`.
                .or_else(|| source.permission.clone())
                .or(permission_kind),
            input: source
                .input
                .clone()
                .or_else(|| source.args.clone())
                .or_else(|| tool_record.and_then(|tool| tool.input.clone())),
            title: source.title.clone(),
            permission_type: source.permission_type.clone(),
        }
    }
}
#[derive(Clone, Default, Serialize, Deserialize)]
pub(super) struct PermissionRequest {
    pub(super) id: Option<String>,
    pub(super) session_id: Option<String>,
    pub(super) call_id: Option<String>,
    pub(super) tool: Option<String>,
    pub(super) input: Option<Value>,
    pub(super) title: Option<String>,
    pub(super) permission_type: Option<String>,
}
