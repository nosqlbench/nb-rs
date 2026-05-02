// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Race-free per-key once-init cache.
//!
//! Generalizes the "cached shared resource" pattern that GK node
//! functions repeatedly need: a global, expensive-to-construct
//! handle (a vectordata `TestDataGroup`, a typed reader, an HTTP
//! client, a parsed schema, a compiled regex, …) that should be
//! built **at most once** per unique key, regardless of how many
//! fibers race to the access point.
//!
//! ## Why a dedicated type
//!
//! The naïve form — `Mutex<HashMap<K, V>>` with
//! `lock → check missing → unlock → load → relock → insert` —
//! exhibits a TOCTOU bug under concurrency: N fibers all see
//! "missing" simultaneously, each runs the (expensive) loader,
//! and the last writer wins. nb-rs hit exactly this in
//! `nbrs-variates::nodes::vectors`: 20 fibers all opened their
//! own `vectordata::Storage` per facet, each constructing a
//! fresh `reqwest::blocking::Client` (≈ load-native-certs +
//! TLS-bootstrap), driving the per-cycle reqwest cost into
//! observable flamegraph dominance.
//!
//! [`OnceCache`] caches an `Arc<OnceLock<Result<V, String>>>`
//! per key, holds the outer `Mutex` only long enough to insert
//! the slot, then dispatches the actual loader through
//! [`OnceLock::get_or_init`]. Concurrent callers for the same
//! key block on the OnceLock and reuse the cached `Result` —
//! exactly one loader run per (key, lifetime).
//!
//! ## Failure semantics
//!
//! Failed loads are **sticky**: every concurrent caller for the
//! same key sees the same `Err` rather than triggering a retry
//! storm. This is deliberate — the caller in nb-rs treats a load
//! failure as a workload-config issue (missing dataset, bad URL,
//! permission), and re-attempting per fiber wouldn't change the
//! diagnostic. If a future caller wants retry semantics, it
//! should clear the slot via a `purge` method (not yet
//! exposed; add when needed).
//!
//! ## Hot-path cost
//!
//! After the first successful load, every subsequent
//! `get_or_init` is: one outer `Mutex::lock` (for the
//! `HashMap::entry` lookup), one `Arc::clone`, one
//! `OnceLock::get_or_init` (returns immediately because the
//! slot is initialized), and one `Result::clone` (the V is
//! typically `Arc<…>` — a refcount bump). No load runs, no I/O
//! happens. This is comparable to a plain `Mutex<HashMap>` read
//! and not on any hot path nb-rs cares about (cycle-time reads
//! go through pre-resolved handles, not through the cache).

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex, OnceLock};

/// Per-key once-init cache. See module docs for the rationale
/// and pattern.
///
/// `K` is the cache key (hashable, cloneable, equatable). `V`
/// is the cached value type — typically `Arc<Something>` so
/// the clone on hit is just a refcount bump.
pub struct OnceCache<K: Eq + Hash + Clone, V: Clone> {
    inner: Mutex<HashMap<K, Arc<OnceLock<Result<V, String>>>>>,
}

impl<K: Eq + Hash + Clone, V: Clone> Default for OnceCache<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Eq + Hash + Clone, V: Clone> OnceCache<K, V> {
    /// Empty cache. Use with `LazyLock`/`OnceLock` for a static.
    pub fn new() -> Self {
        Self { inner: Mutex::new(HashMap::new()) }
    }

    /// Get the value for `key`, computing it via `init` exactly
    /// once across all concurrent callers. Subsequent callers
    /// for the same `key` see the cached `Result` (including
    /// errors — see module docs §"Failure semantics").
    ///
    /// `init` may take seconds (HTTPS download, cert load, file
    /// open). The outer `Mutex` is only held long enough to
    /// install the per-key slot; `init` runs without it,
    /// guarded only by the per-key `OnceLock`.
    pub fn get_or_init<F>(&self, key: K, init: F) -> Result<V, String>
    where F: FnOnce() -> Result<V, String>
    {
        let slot: Arc<OnceLock<Result<V, String>>> = {
            let mut map = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            map.entry(key).or_insert_with(|| Arc::new(OnceLock::new())).clone()
        };
        slot.get_or_init(init).clone()
    }

    /// Number of distinct keys currently in the cache. Includes
    /// keys whose load is in progress or has failed. Diagnostic
    /// only — the cache should not be inspected for routing or
    /// behavior decisions.
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Whether the cache holds no keys.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::thread;

    #[test]
    fn first_caller_runs_loader_subsequent_callers_reuse_value() {
        let cache: OnceCache<&'static str, Arc<String>> = OnceCache::new();
        let calls = AtomicU32::new(0);

        let v1 = cache.get_or_init("k", || {
            calls.fetch_add(1, Ordering::Relaxed);
            Ok(Arc::new("loaded".into()))
        }).unwrap();
        let v2 = cache.get_or_init("k", || {
            calls.fetch_add(1, Ordering::Relaxed);
            Ok(Arc::new("DIFFERENT".into()))
        }).unwrap();

        assert_eq!(*v1, "loaded");
        assert_eq!(*v2, "loaded");
        assert!(Arc::ptr_eq(&v1, &v2), "second caller should see the cached arc");
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn concurrent_callers_share_one_loader_run() {
        // The TOCTOU regression test: 32 threads all race for the
        // same key. The naïve lock-check-release-load pattern
        // would fire the loader 32 times. `OnceCache` should
        // fire it exactly once.
        let cache: Arc<OnceCache<&'static str, Arc<String>>> = Arc::new(OnceCache::new());
        let calls = Arc::new(AtomicU32::new(0));

        let handles: Vec<_> = (0..32).map(|_| {
            let cache = cache.clone();
            let calls = calls.clone();
            thread::spawn(move || {
                cache.get_or_init("hot-key", || {
                    calls.fetch_add(1, Ordering::Relaxed);
                    // Tiny stall to widen the race window.
                    thread::sleep(std::time::Duration::from_millis(5));
                    Ok(Arc::new("only-once".into()))
                })
            })
        }).collect();

        for h in handles {
            let v = h.join().unwrap().unwrap();
            assert_eq!(*v, "only-once");
        }
        assert_eq!(calls.load(Ordering::Relaxed), 1,
            "loader should run exactly once across all concurrent callers");
    }

    #[test]
    fn distinct_keys_load_independently() {
        let cache: OnceCache<u32, u32> = OnceCache::new();
        let v1 = cache.get_or_init(1, || Ok(10)).unwrap();
        let v2 = cache.get_or_init(2, || Ok(20)).unwrap();
        assert_eq!(v1, 10);
        assert_eq!(v2, 20);
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn failed_load_is_sticky() {
        let cache: OnceCache<&'static str, u32> = OnceCache::new();
        let calls = AtomicU32::new(0);

        let r1 = cache.get_or_init("bad", || {
            calls.fetch_add(1, Ordering::Relaxed);
            Err("nope".into())
        });
        let r2 = cache.get_or_init("bad", || {
            calls.fetch_add(1, Ordering::Relaxed);
            Ok(42)
        });

        assert_eq!(r1.unwrap_err(), "nope");
        assert_eq!(r2.unwrap_err(), "nope");
        assert_eq!(calls.load(Ordering::Relaxed), 1,
            "second caller must NOT retry; the failure is sticky");
    }
}
