//! The normalized request configuration retained by Pi, independent of the
//! provider-native payload. Unknown built-in tools remain provider-owned JSON;
//! function definitions and their arbitrary parameter schemas are distinguished.
use crate::json::{Extensions, Field};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{value::RawValue, Value};

#[derive(Default, Serialize, Deserialize)]
pub(crate) struct RequestConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    top_p: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    frequency_penalty: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    presence_penalty: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stop: Option<Stop>,
    #[serde(skip_serializing_if = "Option::is_none")]
    response_format: Option<ResponseFormat>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    tools: Vec<ToolDefinition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_choice: Option<ToolChoice>,
    #[serde(skip_serializing_if = "Option::is_none")]
    parallel_tool_calls: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tool_calls: Option<u64>,
}
impl RequestConfig {
    pub fn is_empty(&self) -> bool {
        self.temperature.is_none()
            && self.top_p.is_none()
            && self.max_tokens.is_none()
            && self.frequency_penalty.is_none()
            && self.presence_penalty.is_none()
            && self.stop.is_none()
            && self.response_format.is_none()
            && self.tools.is_empty()
            && self.tool_choice.is_none()
            && self.parallel_tool_calls.is_none()
            && self.max_tool_calls.is_none()
    }
}
#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum Stop {
    One(String),
    Many(Vec<String>),
}
#[derive(Serialize)]
#[serde(untagged)]
enum ToolDefinition {
    Function(FunctionTool),
    BuiltIn(Extensions),
}
#[derive(Serialize, Deserialize)]
struct FunctionTool {
    #[serde(rename = "type")]
    kind: FunctionType,
    function: FunctionDefinition,
    #[serde(flatten, deserialize_with = "crate::json::deserialize_extensions")]
    extensions: Extensions,
}
#[derive(Serialize, Deserialize)]
enum FunctionType {
    #[serde(rename = "function")]
    Function,
}
#[derive(Serialize, Deserialize)]
struct FunctionDefinition {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    /// Tool-authored JSON Schema, intentionally unrestricted.
    #[serde(skip_serializing_if = "Option::is_none")]
    parameters: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    strict: Option<bool>,
    #[serde(flatten, deserialize_with = "crate::json::deserialize_extensions")]
    extensions: Extensions,
}
impl<'de> Deserialize<'de> for ToolDefinition {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(d)?;
        if value.get("type").and_then(Value::as_str) == Some("function") {
            serde_json::from_value(value)
                .map(Self::Function)
                .map_err(serde::de::Error::custom)
        } else {
            serde_json::from_value(value)
                .map(Self::BuiltIn)
                .map_err(serde::de::Error::custom)
        }
    }
}
#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum ToolChoice {
    Mode(ChoiceMode),
    Named(NamedChoice),
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum ChoiceMode {
    Auto,
    None,
    Required,
}
#[derive(Serialize, Deserialize)]
struct NamedChoice {
    #[serde(rename = "type")]
    kind: FunctionType,
    function: FunctionName,
}
#[derive(Serialize, Deserialize)]
struct FunctionName {
    name: String,
}

#[derive(Serialize)]
#[serde(untagged)]
enum ResponseFormat {
    Known(KnownResponseFormat),
    Other(Box<RawValue>),
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum KnownResponseFormat {
    Text {
        #[serde(flatten, deserialize_with = "crate::json::deserialize_extensions")]
        extensions: Extensions,
    },
    JsonObject {
        #[serde(flatten, deserialize_with = "crate::json::deserialize_extensions")]
        extensions: Extensions,
    },
    JsonSchema {
        json_schema: SchemaDefinition,
        #[serde(flatten, deserialize_with = "crate::json::deserialize_extensions")]
        extensions: Extensions,
    },
}
#[derive(Serialize, Deserialize)]
struct SchemaDefinition {
    name: String,
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    description: Field<String>,
    #[serde(default, skip_serializing_if = "Field::is_missing")]
    strict: Field<bool>,
    /// Provider/user-authored JSON Schema, intentionally unrestricted.
    schema: Value,
    #[serde(flatten, deserialize_with = "crate::json::deserialize_extensions")]
    extensions: Extensions,
}
impl<'de> Deserialize<'de> for ResponseFormat {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(d)?;
        if matches!(
            value.get("type").and_then(Value::as_str),
            Some("text" | "json_object" | "json_schema")
        ) {
            serde_json::from_value(value)
                .map(Self::Known)
                .map_err(serde::de::Error::custom)
        } else {
            serde_json::value::to_raw_value(&value)
                .map(Self::Other)
                .map_err(serde::de::Error::custom)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn request_variants_preserve_schemas_and_provider_builtins() {
        let raw = json!({"max_tokens":100,"stop":["END"],"tool_choice":{"type":"function","function":{"name":"read"}},
            "tools":[{"type":"function","function":{"name":"read","parameters":{"type":"object","future":true}}},{"type":"web_search","future":true}],
            "response_format":{"type":"json_schema","json_schema":{"name":"answer","schema":{"type":"object","future":true}}}});
        let config: RequestConfig = serde_json::from_value(raw.clone()).unwrap();
        assert!(matches!(&config.tools[0], ToolDefinition::Function(_)));
        assert!(matches!(&config.tools[1], ToolDefinition::BuiltIn(_)));
        assert_eq!(serde_json::to_value(config).unwrap(), raw);
        for format in [
            json!({"type":"text","future":null}),
            json!({"type":"json_object","future":true}),
            json!({"type":"json_schema","json_schema":{"name":"answer","description":null,"strict":null,"schema":{"type":"object"}},"future":true}),
            json!({"type":"future","settings":[null,42]}),
        ] {
            let raw = json!({"response_format":format});
            let parsed: RequestConfig = serde_json::from_value(raw.clone()).unwrap();
            assert_eq!(serde_json::to_value(parsed).unwrap(), raw);
        }
        for raw in [
            json!({"tools":[{"type":"function","function":{"name":42}}]}),
            json!({"max_tokens":"100"}),
            json!({"response_format":{"type":"json_schema","json_schema":{"name":42,"schema":{}}}}),
        ] {
            assert!(serde_json::from_value::<RequestConfig>(raw).is_err());
        }
    }
}
