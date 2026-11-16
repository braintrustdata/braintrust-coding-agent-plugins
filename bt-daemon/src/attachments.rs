//! Sink-neutral captured attachments. Bytes are immutable snapshots, never lazy paths.
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attachment {
    pub digest: String,
    pub filename: String,
    pub content_type: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub data: Vec<u8>,
}

impl Attachment {
    pub fn new(filename: String, content_type: String, data: Vec<u8>) -> Self {
        Self {
            digest: format!("{:x}", Sha256::digest(&data)),
            filename,
            content_type,
            data,
        }
    }
}

/// Extract explicit inline media and native file blocks, preserving their position.
/// Ordinary path strings and remote URLs are never dereferenced.
pub(crate) fn capture(value: &mut Value, attachments: &mut Vec<Attachment>) {
    let source = match value {
        Value::String(url) if url.starts_with("data:") => decode_url(url),
        Value::Object(map) => {
            let kind = map.get("type").and_then(Value::as_str).unwrap_or_default();
            if matches!(kind, "image" | "document") && map.get("source").is_some() {
                let source = &map["source"];
                source.get("data").and_then(Value::as_str).and_then(|data| {
                    let mime = source.get("media_type")?.as_str()?;
                    Some((
                        mime.to_owned(),
                        base64::engine::general_purpose::STANDARD
                            .decode(data)
                            .ok()?,
                    ))
                })
            } else if matches!(kind, "image" | "file" | "audio" | "video" | "image_url") {
                let mime = map
                    .get("mimeType")
                    .or_else(|| map.get("mime"))
                    .and_then(Value::as_str);
                mime.and_then(|mime| {
                    map.get("data").and_then(Value::as_str).and_then(|data| {
                        Some((
                            mime.to_owned(),
                            base64::engine::general_purpose::STANDARD
                                .decode(data)
                                .ok()?,
                        ))
                    })
                })
                .or_else(|| {
                    let url = map
                        .get("uri")
                        .or_else(|| map.get("url"))
                        .or_else(|| map.get("image_url").and_then(|v| v.get("url").or(Some(v))))?
                        .as_str()?;
                    if url.starts_with("data:") {
                        return decode_url(url);
                    }
                    let path = reqwest::Url::parse(url).ok()?.to_file_path().ok()?;
                    let data = std::fs::read(path).ok()?;
                    Some((mime.unwrap_or("application/octet-stream").to_owned(), data))
                })
            } else {
                None
            }
        }
        _ => None,
    };
    if let Some((mime, data)) = source {
        let filename = value
            .get("filename")
            .or_else(|| value.get("name"))
            .and_then(Value::as_str)
            .unwrap_or("attachment")
            .to_owned();
        let attachment = Attachment::new(filename, mime, data);
        *value = json!({"type":"_bt_attachment", "digest":attachment.digest, "filename":attachment.filename, "content_type":attachment.content_type});
        if !attachments.iter().any(|a| a.digest == attachment.digest) {
            attachments.push(attachment);
        }
        return;
    }
    match value {
        Value::Array(values) => values.iter_mut().for_each(|v| capture(v, attachments)),
        Value::Object(values) => values.values_mut().for_each(|v| capture(v, attachments)),
        _ => {}
    }
}

fn decode_url(url: &str) -> Option<(String, Vec<u8>)> {
    let (header, data) = url.strip_prefix("data:")?.split_once(',')?;
    let mime = header.strip_suffix(";base64")?;
    Some((
        mime.to_owned(),
        base64::engine::general_purpose::STANDARD
            .decode(data)
            .ok()?,
    ))
}

pub(crate) fn materialize(
    value: &mut Value,
    references: &std::collections::HashMap<String, Value>,
) {
    if value.get("type").and_then(Value::as_str) == Some("_bt_attachment") {
        if let Some(reference) = value
            .get("digest")
            .and_then(Value::as_str)
            .and_then(|id| references.get(id))
        {
            let mut replacement = reference.clone();
            for key in ["filename", "content_type"] {
                if let Some(field) = value.get(key) {
                    replacement[key] = field.clone();
                }
            }
            *value = replacement;
        }
        return;
    }
    match value {
        Value::Array(values) => values.iter_mut().for_each(|v| materialize(v, references)),
        Value::Object(values) => values.values_mut().for_each(|v| materialize(v, references)),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn captures_documents_images_and_arbitrary_binary_without_fetching_urls() {
        let mut value = json!([
            {"type":"document","source":{"type":"base64","media_type":"application/pdf","data":"YWJj"}},
            {"type":"image","mimeType":"image/png","data":"YWJj"},
            {"type":"file","mime":"application/octet-stream","data":"AAEC"},
            {"type":"image_url","image_url":{"url":"https://example.test/image.png"}}
        ]);
        let mut attachments = Vec::new();
        capture(&mut value, &mut attachments);
        assert_eq!(attachments.len(), 2);
        assert_eq!(attachments[0].data, b"abc");
        assert_eq!(attachments[1].data, [0, 1, 2]);
        assert_eq!(
            value[3]["image_url"]["url"],
            "https://example.test/image.png"
        );
        let references = attachments
            .into_iter()
            .map(|a| {
                (
                    a.digest,
                    json!({"type":"braintrust_attachment","key":"uploaded"}),
                )
            })
            .collect();
        materialize(&mut value, &references);
        assert_eq!(value[0]["type"], "braintrust_attachment");
        assert_eq!(value[1]["content_type"], "image/png");
    }
    #[test]
    fn snapshots_native_local_file_urls() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("artifact.bin");
        std::fs::write(&path, [0, 255, 1]).unwrap();
        let mut value = json!({"type":"file","mime":"application/octet-stream","uri":reqwest::Url::from_file_path(&path).unwrap().as_str()});
        let mut attachments = Vec::new();
        capture(&mut value, &mut attachments);
        std::fs::remove_file(path).unwrap();
        assert_eq!(attachments[0].data, [0, 255, 1]);
    }
}
