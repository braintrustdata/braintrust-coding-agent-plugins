use super::{ResponseFormat, Stop, ToolChoice, ToolDefinition};
use serde::{Deserialize, Serialize};

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
