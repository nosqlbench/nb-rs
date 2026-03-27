// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Timer: histogram + counter for per-operation latency recording.

use std::sync::atomic::{AtomicU64, Ordering};
use hdrhistogram::Histogram as HdrHistogram;
use crate::labels::Labels;
use crate::instruments::histogram::Histogram;

pub struct Timer {
    labels: Labels,
    histogram: Histogram,
    count: AtomicU64,
}

/// Snapshot of a timer's state for a single interval.
pub struct TimerSnapshot {
    pub histogram: HdrHistogram<u64>,
    pub count: u64,
}

impl Timer {
    pub fn new(labels: Labels) -> Self {
        Self {
            labels: labels.clone(),
            histogram: Histogram::new(labels),
            count: AtomicU64::new(0),
        }
    }

    /// Record a duration in nanoseconds.
    pub fn record(&self, duration_nanos: u64) {
        self.histogram.record(duration_nanos);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Snapshot: returns the delta histogram and the current count.
    ///
    /// The count returned is the absolute total (not delta) — reporters
    /// compute deltas by comparing with their previous snapshot.
    pub fn snapshot(&self) -> TimerSnapshot {
        TimerSnapshot {
            histogram: self.histogram.snapshot(),
            count: self.count.load(Ordering::Relaxed),
        }
    }

    /// Current total count (without snapshotting).
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    pub fn labels(&self) -> &Labels {
        &self.labels
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timer_record_and_snapshot() {
        let t = Timer::new(Labels::of("name", "servicetime"));
        t.record(1_000_000);
        t.record(2_000_000);

        let snap = t.snapshot();
        assert_eq!(snap.histogram.len(), 2);
        assert_eq!(snap.count, 2);
    }

    #[test]
    fn timer_delta_histogram() {
        let t = Timer::new(Labels::of("name", "test"));
        t.record(1_000);
        let snap1 = t.snapshot();
        assert_eq!(snap1.histogram.len(), 1);

        t.record(2_000);
        t.record(3_000);
        let snap2 = t.snapshot();
        assert_eq!(snap2.histogram.len(), 2); // delta: only new records
        assert_eq!(snap2.count, 3); // count is cumulative
    }

    #[test]
    fn timer_count_monotonic() {
        let t = Timer::new(Labels::of("name", "c"));
        assert_eq!(t.count(), 0);
        t.record(100);
        assert_eq!(t.count(), 1);
        t.record(200);
        assert_eq!(t.count(), 2);
    }
}
