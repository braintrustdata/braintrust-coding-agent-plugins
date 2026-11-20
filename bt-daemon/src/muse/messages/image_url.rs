use super::ImageDetail;
use crate::json::{Extensions, Field};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum ImageUrl {
    Url(String),
    Detailed {
        url: String,
        #[serde(default, skip_serializing_if = "Field::is_missing")]
        detail: Field<ImageDetail>,
        #[serde(flatten, deserialize_with = "crate::json::deserialize_extensions")]
        extensions: Extensions,
    },
}
