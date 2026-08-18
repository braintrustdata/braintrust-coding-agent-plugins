//! Small bounded caches for replay/deduplication state.
//!
//! Journals and native transcripts are the durable source of truth. Translators
//! only need a recent window of completed identifiers to absorb duplicate or
//! slightly reordered events; keeping every identifier for the whole session
//! makes payload-free bookkeeping grow without bound.

use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::Hash;

pub(super) const RECENT_ID_CAPACITY: usize = 4_096;

pub(super) struct RecentSet<K> {
    values: HashSet<K>,
    order: VecDeque<K>,
    capacity: usize,
}

impl<K> Default for RecentSet<K> {
    fn default() -> Self {
        Self {
            values: HashSet::new(),
            order: VecDeque::new(),
            capacity: RECENT_ID_CAPACITY,
        }
    }
}

impl<K: Eq + Hash + Clone> RecentSet<K> {
    pub(super) fn contains(&self, value: &K) -> bool {
        self.values.contains(value)
    }

    pub(super) fn insert(&mut self, value: K) -> bool {
        if !self.values.insert(value.clone()) {
            return false;
        }
        self.order.push_back(value);
        while self.values.len() > self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.values.remove(&oldest);
            }
        }
        true
    }

    pub(super) fn remove(&mut self, value: &K) -> bool {
        let removed = self.values.remove(value);
        if removed {
            self.order.retain(|candidate| candidate != value);
        }
        removed
    }

    pub(super) fn clear(&mut self) {
        self.values.clear();
        self.order.clear();
    }
}

pub(super) struct RecentMap<K, V> {
    values: HashMap<K, V>,
    order: VecDeque<K>,
    capacity: usize,
}

impl<K, V> Default for RecentMap<K, V> {
    fn default() -> Self {
        Self {
            values: HashMap::new(),
            order: VecDeque::new(),
            capacity: RECENT_ID_CAPACITY,
        }
    }
}

impl<K: Eq + Hash + Clone, V> RecentMap<K, V> {
    #[cfg(test)]
    pub(super) fn get(&self, key: &K) -> Option<&V> {
        self.values.get(key)
    }

    pub(super) fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.order.retain(|candidate| candidate != &key);
        self.order.push_back(key.clone());
        let previous = self.values.insert(key, value);
        while self.values.len() > self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.values.remove(&oldest);
            }
        }
        previous
    }

    pub(super) fn remove(&mut self, key: &K) -> Option<V> {
        let removed = self.values.remove(key);
        if removed.is_some() {
            self.order.retain(|candidate| candidate != key);
        }
        removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recent_collections_evict_oldest_entries() {
        let mut set = RecentSet {
            capacity: 2,
            ..Default::default()
        };
        assert!(set.insert("a"));
        assert!(set.insert("b"));
        assert!(set.insert("c"));
        assert!(!set.contains(&"a"));
        assert!(set.contains(&"b"));
        assert!(set.contains(&"c"));

        let mut map = RecentMap {
            capacity: 2,
            ..Default::default()
        };
        map.insert("a", 1);
        map.insert("b", 2);
        map.insert("c", 3);
        assert!(map.get(&"a").is_none());
        assert_eq!(map.get(&"b"), Some(&2));
        assert_eq!(map.get(&"c"), Some(&3));
    }
}
