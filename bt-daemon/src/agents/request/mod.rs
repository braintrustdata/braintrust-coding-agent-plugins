//! The normalized request configuration retained by Pi, independent of the
//! provider-native payload. Unknown built-in tools remain provider-owned JSON;
//! function definitions and their arbitrary parameter schemas are distinguished.

mod function_definition;
mod function_tool;
mod request_config;
mod response_format;
mod schema_definition;
mod stop;
mod tool_choice;
mod tool_definition;

use function_definition::FunctionDefinition;
use function_tool::FunctionTool;
use function_tool::FunctionType;
pub(crate) use request_config::RequestConfig;
use response_format::ResponseFormat;
use schema_definition::SchemaDefinition;
use stop::Stop;
use tool_choice::ToolChoice;
use tool_definition::ToolDefinition;
