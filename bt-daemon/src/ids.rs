//! Deterministic span-id derivation.
//!
//! Translators derive span ids as UUIDv5 over stable keys so that replaying a
//! session's journal re-creates the same ids, and the re-emit merges
//! server-side (`_is_merge`) instead of duplicating. The exact string format
//! the Braintrust sink requires is reconciled in the sink layer (some SDK
//! paths want hex span ids); this module is the single place that mints them.

use uuid::Uuid;

/// Fixed namespace for all bt-daemon span ids ("btdaemon-span-id-ns" hashed to
/// a v4 uuid, pinned as a constant so it never changes across builds).
const NAMESPACE: Uuid = Uuid::from_u128(0x8f2b_4e11_9c7a_4d3e_b6a1_5f0c_2d84_71ae);

const SEP: char = '\u{1f}'; // ASCII unit separator; will not appear in ids/keys.

/// A stable namespace for one agent's native session. Source qualification
/// prevents two agents that happen to choose the same native session id from
/// sharing span ids, actor locks, or journals.
pub fn session_namespace(source: &str, session_id: &str) -> String {
    format!("{source}{SEP}{session_id}")
}

pub fn native_session_id(namespace: &str) -> &str {
    namespace
        .split_once(SEP)
        .map(|(_, session_id)| session_id)
        .unwrap_or(namespace)
}

/// Collision-resistant suffix for source-qualified on-disk session state.
pub fn session_storage_id(source: &str, session_id: &str) -> String {
    Uuid::new_v5(&NAMESPACE, session_namespace(source, session_id).as_bytes()).to_string()
}

/// A deterministic span id for `key` within `session_id`. `key` should encode
/// the logical span identity, e.g. `turn:{turn_id}` or `tool:{call_id}`.
pub fn span_id(session_id: &str, key: &str) -> String {
    let name = format!("{session_id}{SEP}{key}");
    Uuid::new_v5(&NAMESPACE, name.as_bytes()).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_and_distinct() {
        let a1 = span_id("s1", "turn:1");
        let a2 = span_id("s1", "turn:1");
        let b = span_id("s1", "turn:2");
        let c = span_id("s2", "turn:1");
        assert_eq!(a1, a2, "same inputs must be stable across calls");
        assert_ne!(a1, b);
        assert_ne!(a1, c);
    }

    #[test]
    fn source_qualified_sessions_do_not_collide() {
        let codex = session_namespace("codex", "same");
        let claude = session_namespace("claude-code", "same");
        assert_ne!(span_id(&codex, "root"), span_id(&claude, "root"));
        assert_ne!(
            session_storage_id("codex", "same"),
            session_storage_id("claude-code", "same")
        );
    }
}
