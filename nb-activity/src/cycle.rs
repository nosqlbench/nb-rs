// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Cycle source: distributes cycle numbers to concurrent async tasks.

use std::sync::atomic::{AtomicU64, Ordering};

/// Thread-safe cycle counter. Each `next()` call returns the next
/// cycle number, or `None` when the range is exhausted.
pub struct CycleSource {
    current: AtomicU64,
    end: u64,
}

impl CycleSource {
    /// Create a cycle source for the range [start, end).
    pub fn new(start: u64, end: u64) -> Self {
        Self {
            current: AtomicU64::new(start),
            end,
        }
    }

    /// Get the next cycle, or None if exhausted.
    pub fn next(&self) -> Option<u64> {
        let cycle = self.current.fetch_add(1, Ordering::Relaxed);
        if cycle < self.end { Some(cycle) } else { None }
    }

    /// How many cycles remain (approximate — racy under concurrency).
    pub fn remaining(&self) -> u64 {
        let current = self.current.load(Ordering::Relaxed);
        self.end.saturating_sub(current)
    }

    /// Total cycles in the range.
    pub fn total(&self) -> u64 {
        self.end
    }

    /// Reset to start over.
    pub fn reset(&self, start: u64) {
        self.current.store(start, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cycle_source_sequential() {
        let cs = CycleSource::new(0, 5);
        assert_eq!(cs.next(), Some(0));
        assert_eq!(cs.next(), Some(1));
        assert_eq!(cs.next(), Some(2));
        assert_eq!(cs.next(), Some(3));
        assert_eq!(cs.next(), Some(4));
        assert_eq!(cs.next(), None);
        assert_eq!(cs.next(), None);
    }

    #[test]
    fn cycle_source_offset() {
        let cs = CycleSource::new(100, 103);
        assert_eq!(cs.next(), Some(100));
        assert_eq!(cs.next(), Some(101));
        assert_eq!(cs.next(), Some(102));
        assert_eq!(cs.next(), None);
    }

    #[test]
    fn cycle_source_remaining() {
        let cs = CycleSource::new(0, 10);
        assert_eq!(cs.remaining(), 10);
        cs.next();
        cs.next();
        assert_eq!(cs.remaining(), 8);
    }

    #[test]
    fn cycle_source_concurrent() {
        use std::sync::Arc;
        use std::thread;

        let cs = Arc::new(CycleSource::new(0, 1000));
        let mut handles = Vec::new();

        for _ in 0..4 {
            let cs = cs.clone();
            handles.push(thread::spawn(move || {
                let mut count = 0u64;
                while cs.next().is_some() { count += 1; }
                count
            }));
        }

        let total: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
        assert_eq!(total, 1000);
    }
}
