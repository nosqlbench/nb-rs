// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Dimensional metric labels.
//!
//! Every metric carries a set of key-value labels for identification.
//! Labels are immutable, `Arc`-shared for cheap cloning, and compose
//! hierarchically (child inherits parent).

use std::fmt;
use std::sync::Arc;

/// An immutable set of key-value label pairs.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Labels {
    pairs: Arc<Vec<(String, String)>>,
}

impl Default for Labels {
    fn default() -> Self {
        Self::empty()
    }
}

impl Labels {
    pub fn empty() -> Self {
        Self { pairs: Arc::new(Vec::new()) }
    }

    pub fn of(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self { pairs: Arc::new(vec![(key.into(), value.into())]) }
    }

    pub fn with(&self, key: impl Into<String>, value: impl Into<String>) -> Self {
        let mut pairs = (*self.pairs).clone();
        let key = key.into();
        if let Some(pos) = pairs.iter().position(|(k, _)| k == &key) {
            pairs[pos].1 = value.into();
        } else {
            pairs.push((key, value.into()));
        }
        Self { pairs: Arc::new(pairs) }
    }

    pub fn extend(&self, child: &Labels) -> Labels {
        let mut pairs = (*self.pairs).clone();
        for (k, v) in child.pairs.iter() {
            if let Some(pos) = pairs.iter().position(|(pk, _)| pk == k) {
                pairs[pos].1 = v.clone();
            } else {
                pairs.push((k.clone(), v.clone()));
            }
        }
        Labels { pairs: Arc::new(pairs) }
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.pairs.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str())
    }

    pub fn len(&self) -> usize { self.pairs.len() }
    pub fn is_empty(&self) -> bool { self.pairs.is_empty() }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.pairs.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    pub fn to_prometheus(&self) -> String {
        if self.pairs.is_empty() { return String::new(); }
        let inner: Vec<String> = self.pairs.iter()
            .map(|(k, v)| format!("{k}=\"{v}\""))
            .collect();
        format!("{{{}}}", inner.join(","))
    }

    pub fn to_dotted(&self) -> String {
        self.pairs.iter().map(|(_, v)| v.as_str()).collect::<Vec<_>>().join(".")
    }

    pub fn identity_hash(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut h = DefaultHasher::new();
        for (k, v) in self.pairs.iter() { k.hash(&mut h); v.hash(&mut h); }
        h.finish()
    }
}

impl fmt::Display for Labels {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_prometheus())
    }
}

/// Semantic category for metric filtering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricCategory {
    Core,
    Progress,
    Errors,
    Driver,
    Internals,
    Verification,
    Config,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn labels_with_override() {
        let l = Labels::of("a", "1").with("a", "2");
        assert_eq!(l.get("a"), Some("2"));
        assert_eq!(l.len(), 1);
    }

    #[test]
    fn labels_extend_child_wins() {
        let parent = Labels::of("session", "s1").with("activity", "write");
        let child = Labels::of("name", "timer1").with("activity", "read");
        let merged = parent.extend(&child);
        assert_eq!(merged.get("session"), Some("s1"));
        assert_eq!(merged.get("activity"), Some("read"));
        assert_eq!(merged.get("name"), Some("timer1"));
    }

    #[test]
    fn labels_prometheus_format() {
        let l = Labels::of("session", "abc").with("name", "ops_total");
        assert_eq!(l.to_prometheus(), r#"{session="abc",name="ops_total"}"#);
    }

    #[test]
    fn labels_clone_shares_arc() {
        let l = Labels::of("a", "1").with("b", "2");
        let l2 = l.clone();
        assert!(Arc::ptr_eq(&l.pairs, &l2.pairs));
    }
}
