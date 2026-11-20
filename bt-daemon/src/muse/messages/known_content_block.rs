use super::{ImageSource, ImageUrl, MessagePhase};
use crate::json::{Extensions, Field};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum KnownContentBlock {
    Text {
        text: String,
        #[serde(flatten, deserialize_with = "crate::json::deserialize_extensions")]
        extensions: Extensions,
    },
    AssistantMessage {
        text: String,
        #[serde(default, skip_serializing_if = "Field::is_missing")]
        provider_item_id: Field<String>,
        #[serde(default, skip_serializing_if = "Field::is_missing")]
        phase: Field<MessagePhase>,
        #[serde(flatten, deserialize_with = "crate::json::deserialize_extensions")]
        extensions: Extensions,
    },
    Image {
        source: ImageSource,
        #[serde(flatten, deserialize_with = "crate::json::deserialize_extensions")]
        extensions: Extensions,
    },
    ImageUrl {
        image_url: ImageUrl,
        #[serde(flatten, deserialize_with = "crate::json::deserialize_extensions")]
        extensions: Extensions,
    },
}
