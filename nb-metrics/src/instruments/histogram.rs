// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Delta HDR Histogram for latency/value distribution recording.
//!
//! Each `snapshot()` call returns the data accumulated since the last
//! snapshot (delta semantics). The Recorder's interval is swapped
//! atomically, so the hot path (`record()`) and the snapshot path
//! don't contend.

use std::sync::Mutex;
use hdrhistogram::Histogram as HdrHistogram;
use crate::labels::Labels;

/// Default significant digits for HDR Histograms (0.1% error).
const SIGNIFICANT_DIGITS: u8 = 3;

/// Maximum trackable value in nanoseconds (~1 hour).
const MAX_VALUE: u64 = 3_600_000_000_000;

pub struct Histogram {
    labels: Labels,
    /// The accumulating histogram. Protected by mutex for the swap.
    current: Mutex<HdrHistogram<u64>>,
}

impl Histogram {
    pub fn new(labels: Labels) -> Self {
        Self {
            labels,
            current: Mutex::new(
                HdrHistogram::new_with_bounds(1, MAX_VALUE, SIGNIFICANT_DIGITS)
                    .expect("failed to create HDR histogram")
            ),
        }
    }

    /// Record a value (typically nanoseconds).
    pub fn record(&self, value: u64) {
        let value = value.min(MAX_VALUE);
        let mut h = self.current.lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Err(e) = h.record(value) {
            eprintln!("warning: histogram record failed for value {value}: {e}");
        }
    }

    /// Swap out the current histogram and return the delta.
    ///
    /// The returned histogram contains all data since the last
    /// `snapshot()` call. The internal histogram is reset.
    pub fn snapshot(&self) -> HdrHistogram<u64> {
        let mut current = self.current.lock()
            .unwrap_or_else(|e| e.into_inner());
        let snapshot = current.clone();
        current.reset();
        snapshot
    }

    pub fn labels(&self) -> &Labels {
        &self.labels
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_record_and_snapshot() {
        let h = Histogram::new(Labels::of("name", "latency"));
        h.record(1_000_000); // 1ms
        h.record(2_000_000); // 2ms
        h.record(3_000_000); // 3ms

        let snap = h.snapshot();
        assert_eq!(snap.len(), 3);
        assert!(snap.min() >= 999_000); // HDR bucketing
        assert!(snap.max() <= 3_100_000);
    }

    #[test]
    fn histogram_delta_semantics() {
        let h = Histogram::new(Labels::of("name", "test"));
        h.record(1_000);
        h.record(2_000);

        let snap1 = h.snapshot();
        assert_eq!(snap1.len(), 2);

        // After snapshot, histogram is reset
        h.record(3_000);
        let snap2 = h.snapshot();
        assert_eq!(snap2.len(), 1); // only the new record
    }

    #[test]
    fn histogram_empty_snapshot() {
        let h = Histogram::new(Labels::of("name", "empty"));
        let snap = h.snapshot();
        assert_eq!(snap.len(), 0);
    }

    #[test]
    fn histogram_quantiles() {
        let h = Histogram::new(Labels::of("name", "q"));
        for i in 1..=1000 {
            h.record(i * 1000); // 1µs to 1ms
        }
        let snap = h.snapshot();
        let p50 = snap.value_at_quantile(0.5);
        let p99 = snap.value_at_quantile(0.99);
        assert!(p50 > 400_000 && p50 < 600_000, "p50={p50}");
        assert!(p99 > 980_000 && p99 < 1_100_000, "p99={p99}");
    }
}
