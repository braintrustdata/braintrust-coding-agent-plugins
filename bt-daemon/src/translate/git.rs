use super::{SpanOp, SpanRow};
use serde_json::{Map, Value};
use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime};

const CACHE_CAPACITY: usize = 256;
const NEGATIVE_TTL: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, PartialEq, Eq)]
struct FileStamp {
    len: u64,
    modified: Option<SystemTime>,
    contents: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct GitFingerprint {
    head: Option<FileStamp>,
    head_ref: Option<FileStamp>,
    packed_refs: Option<FileStamp>,
    config: Option<FileStamp>,
    worktree_config: Option<FileStamp>,
}

#[derive(Clone)]
struct RepoEntry {
    git_dir: PathBuf,
    common_dir: PathBuf,
    fingerprint: GitFingerprint,
    metadata: Map<String, Value>,
}

struct NegativeEntry {
    observed_at: Instant,
}

#[derive(Default)]
struct CacheState {
    cwd_to_repo: HashMap<PathBuf, PathBuf>,
    cwd_order: VecDeque<PathBuf>,
    repos: HashMap<PathBuf, RepoEntry>,
    repo_order: VecDeque<PathBuf>,
    negative: HashMap<PathBuf, NegativeEntry>,
    negative_order: VecDeque<PathBuf>,
}

impl CacheState {
    fn trim_cwds(&mut self) {
        while self.cwd_to_repo.len() > CACHE_CAPACITY {
            let Some(key) = self.cwd_order.pop_front() else {
                break;
            };
            self.cwd_to_repo.remove(&key);
        }
    }

    fn trim_repos(&mut self) {
        while self.repos.len() > CACHE_CAPACITY {
            let Some(key) = self.repo_order.pop_front() else {
                break;
            };
            self.repos.remove(&key);
        }
    }

    fn trim_negative(&mut self) {
        while self.negative.len() > CACHE_CAPACITY {
            let Some(key) = self.negative_order.pop_front() else {
                break;
            };
            self.negative.remove(&key);
        }
    }
}

/// Daemon-wide Git metadata cache shared by all production translators.
pub(super) struct GitMetadataCache {
    state: Mutex<CacheState>,
}

impl Default for GitMetadataCache {
    fn default() -> Self {
        Self {
            state: Mutex::new(CacheState::default()),
        }
    }
}

impl GitMetadataCache {
    pub(super) fn metadata(&self, cwd: &str) -> Map<String, Value> {
        if cwd.is_empty() {
            return Map::new();
        }
        let cwd = canonical_or_original(Path::new(cwd));
        let mut state = self.state.lock().unwrap();

        if let Some(negative) = state.negative.get(&cwd) {
            if negative.observed_at.elapsed() < NEGATIVE_TTL {
                return Map::new();
            }
            state.negative.remove(&cwd);
            state.negative_order.retain(|key| key != &cwd);
        }

        if let Some(repo_root) = state.cwd_to_repo.get(&cwd).cloned() {
            touch(&mut state.cwd_order, &cwd);
            if let Some(entry) = state.repos.get(&repo_root).cloned() {
                if fingerprint(&entry.git_dir, &entry.common_dir) == entry.fingerprint {
                    touch(&mut state.repo_order, &repo_root);
                    return entry.metadata;
                }
            }
            if let Some(entry) = inspect_repo(&cwd) {
                let metadata = entry.metadata.clone();
                state.repos.insert(repo_root.clone(), entry);
                touch(&mut state.repo_order, &repo_root);
                state.trim_repos();
                return metadata;
            }
            state.cwd_to_repo.remove(&cwd);
        }

        let Some((repo_root, entry)) = discover_repo(&cwd) else {
            state.negative.insert(
                cwd.clone(),
                NegativeEntry {
                    observed_at: Instant::now(),
                },
            );
            touch(&mut state.negative_order, &cwd);
            state.trim_negative();
            return Map::new();
        };
        let metadata = entry.metadata.clone();
        state.cwd_to_repo.insert(cwd.clone(), repo_root.clone());
        touch(&mut state.cwd_order, &cwd);
        state.trim_cwds();
        state.repos.insert(repo_root.clone(), entry);
        touch(&mut state.repo_order, &repo_root);
        state.trim_repos();
        metadata
    }

    pub(super) fn enrich_rows(&self, cwd: Option<&str>, ops: &mut [SpanOp]) {
        let Some(cwd) = cwd else { return };
        let metadata = self.metadata(cwd);
        if metadata.is_empty() {
            return;
        }
        for op in ops {
            match op {
                SpanOp::Insert(row) => merge_metadata(row, &metadata),
                SpanOp::Merge(row) if row.metadata.is_some() => merge_metadata(row, &metadata),
                SpanOp::Merge(_) => {}
            }
        }
    }
}

fn merge_metadata(row: &mut SpanRow, git: &Map<String, Value>) {
    let metadata = row
        .metadata
        .get_or_insert_with(|| Value::Object(Map::new()));
    let Some(metadata) = metadata.as_object_mut() else {
        return;
    };
    for (key, value) in git {
        metadata.insert(key.clone(), value.clone());
    }
}

fn discover_repo(cwd: &Path) -> Option<(PathBuf, RepoEntry)> {
    let root = canonical_or_original(Path::new(&git(cwd, &["rev-parse", "--show-toplevel"])?));
    inspect_repo(cwd).map(|entry| (root, entry))
}

fn inspect_repo(cwd: &Path) -> Option<RepoEntry> {
    let git_dir = absolute_git_path(cwd, &git(cwd, &["rev-parse", "--absolute-git-dir"])?);
    let common_dir = absolute_git_path(cwd, &git(cwd, &["rev-parse", "--git-common-dir"])?);
    let mut metadata = Map::new();
    if let Some(origin) = git(cwd, &["remote", "get-url", "origin"]) {
        metadata.insert(
            "git_origin_url".into(),
            Value::String(redact_remote(origin)),
        );
    }
    if let Some(branch) = git(cwd, &["symbolic-ref", "--quiet", "--short", "HEAD"]) {
        metadata.insert("git_branch".into(), Value::String(branch));
    }
    if let Some(commit) = git(cwd, &["rev-parse", "HEAD"]) {
        metadata.insert("git_commit_sha".into(), Value::String(commit));
    }
    Some(RepoEntry {
        fingerprint: fingerprint(&git_dir, &common_dir),
        git_dir,
        common_dir,
        metadata,
    })
}

fn git(cwd: &Path, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(cwd)
        .args(args)
        .env("GIT_OPTIONAL_LOCKS", "0")
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?.trim().to_string();
    (!value.is_empty()).then_some(value)
}

fn absolute_git_path(cwd: &Path, value: &str) -> PathBuf {
    let path = PathBuf::from(value);
    if path.is_absolute() {
        canonical_or_original(&path)
    } else {
        canonical_or_original(&cwd.join(path))
    }
}

fn fingerprint(git_dir: &Path, common_dir: &Path) -> GitFingerprint {
    let head_path = git_dir.join("HEAD");
    let head_ref = std::fs::read_to_string(&head_path)
        .ok()
        .and_then(|head| head.strip_prefix("ref: ").map(str::trim).map(str::to_owned))
        .filter(|name| !name.split('/').any(|part| part == ".."))
        .and_then(|name| stamp(&common_dir.join(name), true));
    GitFingerprint {
        head: stamp(&head_path, true),
        head_ref,
        packed_refs: stamp(&common_dir.join("packed-refs"), false),
        config: stamp(&common_dir.join("config"), false),
        worktree_config: stamp(&git_dir.join("config.worktree"), false),
    }
}

fn stamp(path: &Path, include_contents: bool) -> Option<FileStamp> {
    let metadata = std::fs::metadata(path).ok()?;
    Some(FileStamp {
        len: metadata.len(),
        modified: metadata.modified().ok(),
        contents: include_contents.then(|| std::fs::read(path).unwrap_or_default()),
    })
}

fn canonical_or_original(path: &Path) -> PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

fn redact_remote(remote: String) -> String {
    let Some(scheme) = remote.find("://") else {
        return remote;
    };
    let authority_start = scheme + 3;
    let authority_end = remote[authority_start..]
        .find('/')
        .map(|offset| authority_start + offset)
        .unwrap_or(remote.len());
    if let Some(at) = remote[authority_start..authority_end].rfind('@') {
        let at = authority_start + at;
        return format!("{}{}", &remote[..authority_start], &remote[at + 1..]);
    }
    remote
}

fn touch<K: PartialEq + Clone>(order: &mut VecDeque<K>, key: &K) {
    if let Some(index) = order.iter().position(|candidate| candidate == key) {
        order.remove(index);
    }
    order.push_back(key.clone());
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn run(repo: &Path, args: &[&str]) {
        assert!(Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(args)
            .status()
            .unwrap()
            .success());
    }

    #[test]
    fn invalidates_head_and_origin() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path().join("repo");
        fs::create_dir(&repo).unwrap();
        run(&repo, &["init", "-b", "main"]);
        run(&repo, &["config", "user.email", "test@example.com"]);
        run(&repo, &["config", "user.name", "Test"]);
        fs::write(repo.join("a"), "a").unwrap();
        run(&repo, &["add", "a"]);
        run(&repo, &["commit", "-m", "a"]);
        run(
            &repo,
            &[
                "remote",
                "add",
                "origin",
                "https://secret@example.com/acme/a.git",
            ],
        );
        let subdir = repo.join("subdir");
        fs::create_dir(&subdir).unwrap();

        let cache = GitMetadataCache::default();
        let first = cache.metadata(subdir.to_str().unwrap());
        assert_eq!(first["git_branch"], "main");
        assert_eq!(first["git_origin_url"], "https://example.com/acme/a.git");

        run(&repo, &["checkout", "-b", "feature"]);
        fs::write(repo.join("b"), "b").unwrap();
        run(&repo, &["add", "b"]);
        run(&repo, &["commit", "-m", "b"]);
        run(
            &repo,
            &[
                "remote",
                "set-url",
                "origin",
                "https://example.com/acme/b.git",
            ],
        );
        let second = cache.metadata(subdir.to_str().unwrap());
        assert_eq!(second["git_branch"], "feature");
        assert_eq!(second["git_origin_url"], "https://example.com/acme/b.git");
        assert_ne!(first["git_commit_sha"], second["git_commit_sha"]);
    }
}
