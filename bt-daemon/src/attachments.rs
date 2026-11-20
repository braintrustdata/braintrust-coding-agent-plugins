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

#[derive(Debug, Clone, Serialize)]
pub(crate) struct AttachmentReference {
    #[serde(rename = "type")]
    kind: AttachmentReferenceType,
    key: String,
    filename: crate::json::Field<String>,
    content_type: crate::json::Field<String>,
}
#[derive(Debug, Clone, Serialize)]
enum AttachmentReferenceType {
    #[serde(rename = "braintrust_attachment")]
    BraintrustAttachment,
}
impl AttachmentReference {
    pub fn uploaded(key: String, attachment: &Attachment) -> Self {
        Self {
            kind: AttachmentReferenceType::BraintrustAttachment,
            key,
            filename: crate::json::Field::Value(attachment.filename.clone()),
            content_type: crate::json::Field::Value(attachment.content_type.clone()),
        }
    }
}

/// Recognize native attachment-bearing content without opening files or fetching URLs.
pub(crate) fn contains(value: &Value) -> bool {
    match value {
        Value::String(text) => text.starts_with("data:") && text.contains(";base64,"),
        Value::Array(items) => items.iter().any(contains),
        Value::Object(fields) => {
            matches!(
                fields.get("type").and_then(Value::as_str),
                Some(
                    "image"
                        | "document"
                        | "file"
                        | "input_file"
                        | "input_image"
                        | "image_url"
                        | "audio"
                        | "video"
                )
            ) || fields.values().any(contains)
        }
        _ => false,
    }
}

/// Extract explicit inline media and native file blocks, preserving their position.
/// Ordinary path strings and remote URLs are never dereferenced.
pub(crate) fn capture(value: &mut Value, attachments: &mut Vec<Attachment>) {
    let source = match value {
        Value::String(url) if url.starts_with("data:") => decode_url(url),
        Value::Object(map) => {
            let kind = map.get("type").and_then(Value::as_str).unwrap_or_default();
            if kind == "image" && map.get("file").is_some() {
                let file = &map["file"];
                file.get("base64").and_then(Value::as_str).and_then(|data| {
                    Some((
                        file.get("type")?.as_str()?.to_owned(),
                        base64::engine::general_purpose::STANDARD
                            .decode(data)
                            .ok()?,
                    ))
                })
            } else if kind == "input_file" {
                map.get("file_data")
                    .and_then(Value::as_str)
                    .and_then(|data| {
                        decode_url(data).or_else(|| {
                            let mime = map
                                .get("content_type")
                                .and_then(Value::as_str)
                                .map(str::to_owned)
                                .unwrap_or_else(|| {
                                    mime_guess::from_path(
                                        map.get("filename")
                                            .and_then(Value::as_str)
                                            .unwrap_or_default(),
                                    )
                                    .first_or_octet_stream()
                                    .to_string()
                                });
                            Some((
                                mime,
                                base64::engine::general_purpose::STANDARD
                                    .decode(data)
                                    .ok()?,
                            ))
                        })
                    })
            } else if matches!(kind, "image" | "document") && map.get("source").is_some() {
                let source = &map["source"];
                source.get("data").and_then(Value::as_str).and_then(|data| {
                    let mime = source.get("media_type")?.as_str()?;
                    let bytes = if source.get("type").and_then(Value::as_str) == Some("text") {
                        data.as_bytes().to_vec()
                    } else {
                        base64::engine::general_purpose::STANDARD
                            .decode(data)
                            .ok()?
                    };
                    Some((mime.to_owned(), bytes))
                })
            } else if matches!(
                kind,
                "image" | "file" | "audio" | "video" | "image_url" | "input_image"
            ) {
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
                    let path = url::Url::parse(url).ok()?.to_file_path().ok()?;
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
            .or_else(|| value.get("title"))
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
    references: &std::collections::HashMap<String, AttachmentReference>,
) {
    if value.get("type").and_then(Value::as_str) == Some("_bt_attachment") {
        if let Some(reference) = value
            .get("digest")
            .and_then(Value::as_str)
            .and_then(|id| references.get(id))
        {
            #[derive(Deserialize)]
            struct Marker {
                #[serde(default)]
                filename: crate::json::Field<String>,
                #[serde(default)]
                content_type: crate::json::Field<String>,
            }
            let Ok(marker) = Marker::deserialize(&*value) else {
                return;
            };
            let mut replacement = reference.clone();
            if !marker.filename.is_missing() {
                replacement.filename = marker.filename;
            }
            if !marker.content_type.is_missing() {
                replacement.content_type = marker.content_type;
            }
            *value = json!(replacement);
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
                let reference = AttachmentReference::uploaded("uploaded".into(), &a);
                (a.digest, reference)
            })
            .collect();
        materialize(&mut value, &references);
        assert_eq!(value[0]["type"], "braintrust_attachment");
        assert_eq!(value[1]["content_type"], "image/png");
    }
    #[test]
    fn captures_native_documents_and_retains_unavailable_file_ids() {
        for (mut value, mime, filename, bytes) in [
            (
                json!({"type":"image","file":{"type":"image/png","base64":"aW1hZ2UgYnl0ZXM="}}),
                "image/png",
                "attachment",
                b"image bytes".to_vec(),
            ),
            (
                json!({"type":"input_file","filename":"report.pdf","file_data":"cGRmIGJ5dGVz"}),
                "application/pdf",
                "report.pdf",
                b"pdf bytes".to_vec(),
            ),
            (
                json!({"type":"input_file","filename":"binary.custom","content_type":"application/x-custom","file_data":"AAEC/w=="}),
                "application/x-custom",
                "binary.custom",
                vec![0, 1, 2, 255],
            ),
            (
                json!({"type":"document","title":"notes.txt","source":{"type":"text","media_type":"text/plain","data":"raw notes"}}),
                "text/plain",
                "notes.txt",
                b"raw notes".to_vec(),
            ),
        ] {
            let mut attachments = Vec::new();
            capture(&mut value, &mut attachments);
            assert_eq!(attachments.len(), 1);
            assert_eq!(attachments[0].content_type, mime);
            assert_eq!(attachments[0].filename, filename);
            assert_eq!(attachments[0].data, bytes);
        }
        let mut opaque = json!({"type":"input_file","file_id":"hosted-file"});
        let original = opaque.clone();
        let mut attachments = Vec::new();
        capture(&mut opaque, &mut attachments);
        assert_eq!(opaque, original);
        assert!(attachments.is_empty());
    }

    #[test]
    fn local_image_urls_keep_the_native_content_type() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("image.png");
        std::fs::write(&path, b"image bytes").unwrap();
        let mut value = json!({"type":"image_url","mime":"image/png","image_url":{"url":url::Url::from_file_path(&path).unwrap().as_str()}});
        let mut attachments = Vec::new();
        capture(&mut value, &mut attachments);
        assert_eq!(attachments[0].content_type, "image/png");
        assert_eq!(value["content_type"], "image/png");
        assert_eq!(attachments[0].data, b"image bytes");
    }

    #[test]
    fn snapshots_native_local_file_urls() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("artifact.bin");
        std::fs::write(&path, [0, 255, 1]).unwrap();
        let mut value = json!({"type":"file","mime":"application/octet-stream","uri":url::Url::from_file_path(&path).unwrap().as_str()});
        let mut attachments = Vec::new();
        capture(&mut value, &mut attachments);
        std::fs::remove_file(path).unwrap();
        assert_eq!(attachments[0].data, [0, 255, 1]);
    }
}
