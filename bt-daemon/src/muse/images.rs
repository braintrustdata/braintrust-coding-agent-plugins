//! Recover image bytes from Muse's content-addressed session assets, never source paths.
use base64::Engine;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

const MAX_BYTES: u64 = 8 * 1024 * 1024;

fn session_dir(session: &str) -> Option<PathBuf> {
    let id = uuid::Uuid::parse_str(session).ok()?;
    let millis = (id.as_u128() >> 80) as i64;
    let date = chrono::DateTime::from_timestamp_millis(millis)?;
    let root = std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(
                std::env::var_os("HOME")
                    .or_else(|| std::env::var_os("USERPROFILE"))
                    .unwrap_or_default(),
            )
            .join(".local/share")
        });
    Some(
        root.join("muse/sessions")
            .join(date.format("%Y/%m/%d").to_string())
            .join(session),
    )
}

pub(crate) fn content(session: &str, turn: &str) -> Option<Value> {
    let dir = session_dir(session)?;
    content_at(&dir, turn)
}

fn content_at(dir: &std::path::Path, turn: &str) -> Option<Value> {
    let mut file = std::fs::File::open(dir.join("session.jsonl")).ok()?;
    let len = file.metadata().ok()?.len();
    let start = len.saturating_sub(MAX_BYTES);
    file.seek(SeekFrom::Start(start)).ok()?;
    let mut bytes = Vec::new();
    file.take(MAX_BYTES).read_to_end(&mut bytes).ok()?;
    let lines = bytes.split(|b| *b == b'\n');
    for line in lines.rev() {
        let Ok(record) = serde_json::from_slice::<Value>(line) else {
            continue;
        };
        if record["payload_type"] != "runtime.user_intent.accepted" {
            continue;
        }
        let payload = &record["payload"];
        if payload["intent_id"] != turn {
            continue;
        }
        return asset_content(dir, payload["refill_blocks"].as_array()?);
    }
    None
}

fn asset_content(dir: &std::path::Path, blocks: &[Value]) -> Option<Value> {
    let mut content = Vec::new();
    let mut found = false;
    let mut total = 0;
    for block in blocks {
        if block["kind"] == "text" {
            if let Some(text) = block["text"].as_str() {
                content.push(json!({"type":"text","text":text}));
            }
        } else if block["kind"] == "asset" {
            let asset = &block["asset"];
            let digest = asset["digest"].as_str()?;
            let mime = asset["media_type"].as_str()?;
            let size = asset["byte_length"].as_u64()?;
            total += size.min(MAX_BYTES + 1);
            if asset["schema_version"] != 2
                || asset["algorithm"] != "sha256"
                || digest.len() != 64
                || !digest.bytes().all(|b| b.is_ascii_hexdigit())
                || !mime.starts_with("image/")
                || total > MAX_BYTES
            {
                return None;
            }
            let path = dir
                .join("input-assets/v1/sha256")
                .join(&digest[..2])
                .join(digest);
            let file = std::fs::File::open(path).ok()?;
            if !file.metadata().ok()?.is_file() {
                return None;
            }
            let mut bytes = Vec::new();
            file.take(MAX_BYTES + 1).read_to_end(&mut bytes).ok()?;
            if bytes.len() as u64 != size || format!("{:x}", Sha256::digest(&bytes)) != digest {
                return None;
            }
            let data = base64::engine::general_purpose::STANDARD.encode(bytes);
            content.push(json!({"type":"image_url","image_url":{"url":format!("data:{mime};base64,{data}")}}));
            found = true;
        }
    }
    found.then_some(Value::Array(content))
}

pub(crate) fn enrich(payload: &mut Value) {
    if !payload["prompt"]
        .as_str()
        .is_some_and(|text| text.contains("[Image #"))
    {
        return;
    }
    let Some(session) = payload["session_id"].as_str() else {
        return;
    };
    let Some(turn) = payload["turn_id"].as_str() else {
        return;
    };
    if let Some(content) = content(session, turn) {
        payload["image_content"] = content;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn captured_asset_uses_durable_bytes_and_rejects_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let bytes = base64::engine::general_purpose::STANDARD.decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=").unwrap();
        let digest = format!("{:x}", Sha256::digest(&bytes));
        let parent = dir.path().join("input-assets/v1/sha256").join(&digest[..2]);
        std::fs::create_dir_all(&parent).unwrap();
        let path = parent.join(&digest);
        std::fs::write(&path, &bytes).unwrap();
        // Captured Muse 1.4.14 runtime.user_intent.accepted refill blocks.
        let blocks = json!([
            {"kind":"text","text":"[Image #1] Describe this image"},
            {"kind":"asset","asset":{"schema_version":2,"algorithm":"sha256",
                "digest":digest,"media_type":"image/png","byte_length":68,
                "width":1,"height":1,"source_path":"/never/read/the/original.png"}}
        ]);
        let content = asset_content(dir.path(), blocks.as_array().unwrap()).unwrap();
        assert_eq!(content[0]["text"], "[Image #1] Describe this image");
        assert_eq!(content[1]["type"], "image_url");
        assert!(content[1]["image_url"]["url"]
            .as_str()
            .unwrap()
            .starts_with("data:image/png;base64,"));
        let record = json!({"payload_type":"runtime.user_intent.accepted", "payload":{"intent_id":"turn-image", "refill_blocks":blocks}});
        std::fs::write(dir.path().join("session.jsonl"), format!("{}\n", record)).unwrap();
        assert_eq!(content_at(dir.path(), "turn-image"), Some(content));
        assert!(content_at(dir.path(), "another-turn").is_none());
        std::fs::write(&path, vec![0; 68]).unwrap();
        assert!(asset_content(dir.path(), blocks.as_array().unwrap()).is_none());
        std::fs::remove_file(&path).unwrap();
        assert!(asset_content(dir.path(), blocks.as_array().unwrap()).is_none());
        let mut invalid = blocks.clone();
        invalid[1]["asset"]["digest"] = json!("../../secret");
        assert!(asset_content(dir.path(), invalid.as_array().unwrap()).is_none());
    }
}
