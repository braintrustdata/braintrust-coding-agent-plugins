#![allow(dead_code)]
use serde_json::Value;
use std::path::{Path, PathBuf};

pub fn catalog(data: &Path, source: &str, session: &str) -> Value {
    let journal = bt_daemon::source_journal_path(data, source, session);
    let path = data
        .join("journal-control")
        .join(journal.file_name().unwrap())
        .with_extension("routes.json");
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

pub fn database(data: &Path, source: &str, session: &str) -> PathBuf {
    let journal = bt_daemon::source_journal_path(data, source, session);
    let stem = journal.file_stem().unwrap().to_str().unwrap();
    data.join("derived")
        .join(stem.rsplit("--").next().unwrap())
        .join("spans.sqlite")
}

pub fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}
