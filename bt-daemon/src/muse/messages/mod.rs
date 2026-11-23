//! Native hook message and provider payload contracts.
pub(crate) use super::provider_options::ProviderOptions;

mod assistant_tool_call;
mod base64_image_source;
mod content_block;
mod function_call;
mod function_call_type;
mod image_detail;
mod image_source;
mod image_url;
mod input_summary;
mod known_content_block;
mod message_content;
mod message_phase;
mod message_role;
mod model_output;
mod native_message;
mod tool_arguments;
mod tool_description;
mod tool_input;
mod tool_output;
mod url_image_source;

pub(crate) use assistant_tool_call::AssistantToolCall;
pub(crate) use base64_image_source::Base64ImageSource;
pub(crate) use content_block::ContentBlock;
pub(crate) use function_call::FunctionCall;
pub(crate) use function_call_type::FunctionCallType;
pub(crate) use image_detail::ImageDetail;
pub(crate) use image_source::ImageSource;
pub(crate) use image_url::ImageUrl;
pub(crate) use input_summary::InputSummary;
pub(crate) use known_content_block::KnownContentBlock;
pub(crate) use message_content::MessageContent;
pub(crate) use message_phase::MessagePhase;
pub(crate) use message_role::MessageRole;
pub(crate) use model_output::ModelOutput;
pub(crate) use native_message::NativeMessage;
pub(crate) use tool_arguments::ToolArguments;
pub(crate) use tool_description::ToolDescription;
pub(crate) use tool_input::ToolInput;
pub(crate) use tool_output::ToolOutput;
pub(crate) use url_image_source::UrlImageSource;

#[cfg(test)]
mod tests;
