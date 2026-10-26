//! Provenance is owned by live operations, never by the lookup cache.
use super::OriginSnapshot;
use std::{
    collections::HashMap,
    sync::{Arc, Weak},
};

#[derive(Default)]
pub(crate) struct WeakOrigins(HashMap<String, Weak<OriginSnapshot>>);

impl WeakOrigins {
    pub fn get(&mut self, span: &str) -> Option<Arc<OriginSnapshot>> {
        let origin = self.0.get(span).and_then(Weak::upgrade);
        if origin.is_none() {
            self.0.remove(span);
        }
        origin
    }

    pub fn insert(&mut self, span: String, origin: &Arc<OriginSnapshot>) {
        // Weak values alone still leak keys. Sweep dead entries as new work
        // arrives, keeping the index proportional to currently live owners.
        self.0.retain(|_, origin| origin.strong_count() > 0);
        self.0.insert(span, Arc::downgrade(origin));
    }

    pub fn remove(&mut self, span: &str) {
        self.0.remove(span);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_releases_origins_and_dead_keys_but_preserves_live_operations() {
        let mut cache = WeakOrigins::default();
        let live = Arc::new(OriginSnapshot {
            plugin_version: None,
            bt_version: "original".into(),
            source_version: None,
        });
        cache.insert("live".into(), &live);
        for i in 0..10_000 {
            let origin = Arc::new((*live).clone());
            let weak = Arc::downgrade(&origin);
            cache.insert(i.to_string(), &origin);
            drop(origin);
            assert!(weak.upgrade().is_none());
        }
        assert!(cache.0.len() <= 2);
        assert!(Arc::ptr_eq(&cache.get("live").unwrap(), &live));
        drop(live);
        assert!(cache.get("live").is_none());
    }
}
