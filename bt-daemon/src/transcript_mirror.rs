//! Daemon-owned append-only mirrors of agent transcript files.
//!
//! Claude transcript files are external mutable state, so a journaled
//! lifecycle event must stay replayable even after the agent rewrites or
//! deletes the path it came from. Embedding the whole transcript in every
//! lifecycle event bought that durability at quadratic cost: a session
//! re-journaled its entire (growing) transcript on every turn, so an 18 MB
//! transcript produced a 1.6 GB journal that replay then had to hold in
//! memory all at once.
//!
//! Mirroring stores each transcript byte exactly once. The journal carries
//! only a reference — the mirror path plus the high-water offset that existed
//! when the event was accepted — so replay reads the same bytes the live run
//! saw, straight off disk, without the daemon ever holding a transcript in
//! memory.

use std::path::{Path, PathBuf};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use uuid::Uuid;

/// Namespace for mirror file names (distinct from the span-id namespace).
const NAMESPACE: Uuid = Uuid::from_u128(0x3d51_9a02_7c64_4b8f_9e17_a2c5_0d63_88f1);

pub fn mirror_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("transcripts")
}

/// A stable per-(session, transcript path) mirror file name. Keyed by both so
/// one session's main and subagent transcripts never collide, and so two
/// sessions reading the same path keep independent mirrors.
pub fn mirror_path(data_dir: &Path, session_id: &str, source: &str) -> PathBuf {
    let name = format!("{session_id}\u{1f}{source}");
    let digest = Uuid::new_v5(&NAMESPACE, name.as_bytes())
        .simple()
        .to_string();
    mirror_dir(data_dir).join(format!("{digest}.jsonl"))
}

/// Append everything written to `source` since the last capture, returning the
/// mirror path and the mirror's resulting length. That length is the exact
/// high-water offset the caller should journal: replay bounded by it sees the
/// transcript as of this moment and no further.
///
/// If `source` is shorter than the mirror (the agent rewrote or truncated it),
/// the mirror is rebuilt from scratch so it never interleaves two generations.
pub async fn capture(
    data_dir: &Path,
    session_id: &str,
    source: &str,
) -> anyhow::Result<(PathBuf, u64)> {
    let path = mirror_path(data_dir, session_id, source);
    tokio::fs::create_dir_all(mirror_dir(data_dir)).await?;

    let mirrored = tokio::fs::metadata(&path)
        .await
        .map(|meta| meta.len())
        .unwrap_or(0);
    let mut input = tokio::fs::File::open(source).await?;
    let source_len = input.metadata().await?.len();

    // A shorter source means the file was replaced; start the mirror over.
    let restart = source_len < mirrored;
    let from = if restart { 0 } else { mirrored };
    if from >= source_len {
        return Ok((path, mirrored));
    }

    input.seek(std::io::SeekFrom::Start(from)).await?;
    let mut mirror = tokio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(restart)
        .append(!restart)
        .open(&path)
        .await?;
    // Streamed, never buffered whole: the delta is copied through a small
    // fixed buffer, so mirroring an arbitrarily large transcript costs
    // arbitrarily little memory. The copy is unbounded in bytes on purpose —
    // the mirror is the durable record and must stay complete.
    let copied = tokio::io::copy(&mut input, &mut mirror).await?;
    mirror.flush().await?;
    Ok((path, from + copied))
}

/// Best-effort age-based collection, mirroring journal GC. Mirrors are only
/// useful for as long as their journal survives.
pub async fn gc_old_mirrors(data_dir: &Path, max_age: std::time::Duration) {
    let dir = mirror_dir(data_dir);
    let Ok(mut entries) = tokio::fs::read_dir(&dir).await else {
        return;
    };
    let now = std::time::SystemTime::now();
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("jsonl") {
            continue;
        }
        let old = entry
            .metadata()
            .await
            .ok()
            .and_then(|meta| meta.modified().ok())
            .and_then(|modified| now.duration_since(modified).ok())
            .is_some_and(|age| age > max_age);
        if old {
            let _ = tokio::fs::remove_file(&path).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn capture_is_incremental_and_reports_the_high_water_offset() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("t.jsonl");
        tokio::fs::write(&source, b"one\n").await.unwrap();

        let (mirror, first) = capture(tmp.path(), "s1", source.to_str().unwrap())
            .await
            .unwrap();
        assert_eq!(first, 4);

        tokio::fs::write(&source, b"one\ntwo\n").await.unwrap();
        let (_, second) = capture(tmp.path(), "s1", source.to_str().unwrap())
            .await
            .unwrap();
        assert_eq!(second, 8);
        assert_eq!(tokio::fs::read(&mirror).await.unwrap(), b"one\ntwo\n");
    }

    #[tokio::test]
    async fn a_truncated_source_restarts_the_mirror() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("t.jsonl");
        tokio::fs::write(&source, b"aaaa\nbbbb\n").await.unwrap();
        capture(tmp.path(), "s1", source.to_str().unwrap())
            .await
            .unwrap();

        tokio::fs::write(&source, b"cc\n").await.unwrap();
        let (mirror, len) = capture(tmp.path(), "s1", source.to_str().unwrap())
            .await
            .unwrap();
        assert_eq!(len, 3);
        assert_eq!(tokio::fs::read(&mirror).await.unwrap(), b"cc\n");
    }

    #[tokio::test]
    async fn separate_sessions_and_paths_get_separate_mirrors() {
        let tmp = tempfile::tempdir().unwrap();
        assert_ne!(
            mirror_path(tmp.path(), "s1", "/a.jsonl"),
            mirror_path(tmp.path(), "s2", "/a.jsonl")
        );
        assert_ne!(
            mirror_path(tmp.path(), "s1", "/a.jsonl"),
            mirror_path(tmp.path(), "s1", "/b.jsonl")
        );
    }
}
