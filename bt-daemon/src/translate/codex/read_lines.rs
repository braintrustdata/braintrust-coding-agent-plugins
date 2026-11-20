//! Bounded rollout reads preserve partial lines and capture-time cutoffs.
use super::rollout_record::parse_ts;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize)]
pub(super) struct ReadLines {
    pub(super) lines: Vec<String>,
    pub(super) complete: bool,
}

pub(super) fn read_new_lines(
    path: &str,
    offset: &mut u64,
    through_ms: Option<i64>,
    through_bytes: Option<u64>,
    byte_budget: usize,
) -> ReadLines {
    use std::io::{BufRead, BufReader, Seek, SeekFrom};
    let Ok(mut f) = std::fs::File::open(path) else {
        return ReadLines {
            lines: Vec::new(),
            complete: true,
        };
    };
    if let Ok(meta) = f.metadata() {
        if *offset > meta.len() {
            *offset = 0;
        }
    }
    if f.seek(SeekFrom::Start(*offset)).is_err() {
        return ReadLines {
            lines: Vec::new(),
            complete: true,
        };
    }
    let mut reader = BufReader::new(f);
    let mut lines = Vec::new();
    let mut consumed = 0usize;
    loop {
        if through_bytes.is_some_and(|limit| *offset >= limit) {
            break;
        }
        if consumed >= byte_budget && !lines.is_empty() {
            return ReadLines {
                lines,
                complete: false,
            };
        }
        let mut line = String::new();
        let Ok(bytes) = reader.read_line(&mut line) else {
            break;
        };
        if bytes == 0 || !line.ends_with('\n') {
            break;
        }
        if through_bytes.is_some_and(|limit| offset.saturating_add(bytes as u64) > limit) {
            break;
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if let Some(limit) = through_ms {
            if serde_json::from_str::<Value>(trimmed)
                .ok()
                .and_then(|record| parse_ts(&record))
                .is_some_and(|timestamp| timestamp > limit)
            {
                break;
            }
        }
        *offset += bytes as u64;
        consumed += bytes;
        if !trimmed.trim().is_empty() {
            lines.push(trimmed.to_string());
        }
    }
    ReadLines {
        lines,
        complete: true,
    }
}
