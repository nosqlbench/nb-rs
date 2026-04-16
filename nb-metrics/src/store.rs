// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! In-process metrics store for cumulative and last-window views.
//!
//! The store is a [`SnapshotConsumer`]-compatible component that sits
//! on the scheduler pipeline at base cadence. It maintains two views
//! per component:
//!
//! - **Cumulative**: merged total across the component's entire lifetime.
//! - **Last window**: the most recent delta snapshot, cached for fast access.
//!
//! All in-process readers (GK `metric()` / `metric_window()`, summary
//! report, TUI) query this store. External reporters (SQLite, CSV)
//! are separate consumers on the same pipeline — they don't maintain
//! queryable state.
//!
//! Thread safety: wrapped as `Arc<RwLock<InProcessMetricsStore>>`.
//! The scheduler thread is the only writer (at base cadence). All
//! other consumers take read locks.

use std::collections::HashMap;

use crate::frame::{MetricsFrame, Sample};
use crate::labels::Labels;

/// In-process queryable metrics state.
///
/// Keyed by label identity hash for O(1) lookup. Each entry stores
/// the effective labels (for display/filtering) alongside the frame.
pub struct InProcessMetricsStore {
    /// Per-component cumulative views. Built by merging every delta
    /// via [`MetricsFrame::coalesce`].
    cumulative: HashMap<u64, (Labels, MetricsFrame)>,
    /// Per-component last window (most recent delta). Replaced on
    /// every ingest — always reflects the latest capture interval.
    last_window: HashMap<u64, (Labels, MetricsFrame)>,
}

impl InProcessMetricsStore {
    /// Create an empty store.
    pub fn new() -> Self {
        Self {
            cumulative: HashMap::new(),
            last_window: HashMap::new(),
        }
    }

    /// Ingest a delta snapshot from a component.
    ///
    /// 1. Caches the delta as the component's `last_window`.
    /// 2. Merges the delta into the component's `cumulative` view
    ///    via [`MetricsFrame::coalesce`].
    pub fn ingest_delta(&mut self, labels: &Labels, delta: MetricsFrame) {
        let key = labels.identity_hash();

        // Update last window (replace)
        self.last_window.insert(key, (labels.clone(), delta.clone()));

        // Merge into cumulative.
        // Counters are summed, timers' histograms are merged (both correct
        // for accumulation). Gauges are point-in-time values that should
        // be replaced, not averaged — so after coalescing we overwrite
        // gauge samples with the latest delta's gauge values.
        if let Some((_, existing)) = self.cumulative.get(&key) {
            let mut merged = MetricsFrame::coalesce(&[existing.clone(), delta.clone()]);
            // Overwrite gauge values with the latest delta's gauges
            for delta_sample in &delta.samples {
                if let Sample::Gauge { labels: dl, value: dv } = delta_sample {
                    for merged_sample in &mut merged.samples {
                        if let Sample::Gauge { labels: ml, value: mv } = merged_sample {
                            if ml == dl {
                                *mv = *dv;
                            }
                        }
                    }
                    // If gauge not in merged (first appearance), add it
                    if !merged.samples.iter().any(|s| matches!(s, Sample::Gauge { labels: ml, .. } if ml == dl)) {
                        merged.samples.push(delta_sample.clone());
                    }
                }
            }
            self.cumulative.insert(key, (labels.clone(), merged));
        } else {
            self.cumulative.insert(key, (labels.clone(), delta));
        }
    }

    /// Flush a retiring component's final delta into its cumulative view.
    ///
    /// Called during lifecycle retirement before the component transitions
    /// to Stopped. Ensures no data is lost between the last capture tick
    /// and the actual completion.
    pub fn flush_component(&mut self, labels: &Labels, final_delta: MetricsFrame) {
        self.ingest_delta(labels, final_delta);
    }

    /// Query cumulative views matching a filter predicate.
    pub fn query_cumulative<F>(&self, filter: F) -> Vec<(&Labels, &MetricsFrame)>
    where
        F: Fn(&Labels) -> bool,
    {
        self.cumulative.values()
            .filter(|(labels, _)| filter(labels))
            .map(|(labels, frame)| (labels, frame))
            .collect()
    }

    /// Query last-window views matching a filter predicate.
    pub fn query_last_window<F>(&self, filter: F) -> Vec<(&Labels, &MetricsFrame)>
    where
        F: Fn(&Labels) -> bool,
    {
        self.last_window.values()
            .filter(|(labels, _)| filter(labels))
            .map(|(labels, frame)| (labels, frame))
            .collect()
    }

    /// Number of components tracked.
    pub fn component_count(&self) -> usize {
        self.cumulative.len()
    }
}

impl Default for InProcessMetricsStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::Sample;
    use std::time::{Duration, Instant};

    fn counter_frame(label_key: &str, label_val: &str, name: &str, value: u64) -> MetricsFrame {
        MetricsFrame {
            captured_at: Instant::now(),
            interval: Duration::from_secs(1),
            samples: vec![Sample::Counter {
                labels: Labels::of(label_key, label_val).with("name", name),
                value,
            }],
        }
    }

    fn timer_frame(label_key: &str, label_val: &str, values: &[u64]) -> MetricsFrame {
        let mut h = hdrhistogram::Histogram::new_with_bounds(1, 3_600_000_000_000, 3).unwrap();
        for &v in values { let _ = h.record(v); }
        MetricsFrame {
            captured_at: Instant::now(),
            interval: Duration::from_secs(1),
            samples: vec![Sample::Timer {
                labels: Labels::of(label_key, label_val).with("name", "latency"),
                count: values.len() as u64,
                histogram: h,
            }],
        }
    }

    #[test]
    fn ingest_delta_creates_cumulative() {
        let mut store = InProcessMetricsStore::new();
        let labels = Labels::of("phase", "load");
        store.ingest_delta(&labels, counter_frame("phase", "load", "ops", 100));

        let results = store.query_cumulative(|_| true);
        assert_eq!(results.len(), 1);
        match &results[0].1.samples[0] {
            Sample::Counter { value, .. } => assert_eq!(*value, 100),
            _ => panic!("expected counter"),
        }
    }

    #[test]
    fn ingest_multiple_deltas_merges_cumulative() {
        let mut store = InProcessMetricsStore::new();
        let labels = Labels::of("phase", "load");

        store.ingest_delta(&labels, counter_frame("phase", "load", "ops", 100));
        store.ingest_delta(&labels, counter_frame("phase", "load", "ops", 50));

        let results = store.query_cumulative(|_| true);
        assert_eq!(results.len(), 1);
        match &results[0].1.samples[0] {
            Sample::Counter { value, .. } => assert_eq!(*value, 150), // summed
            _ => panic!("expected counter"),
        }
    }

    #[test]
    fn last_window_returns_most_recent_delta() {
        let mut store = InProcessMetricsStore::new();
        let labels = Labels::of("phase", "load");

        store.ingest_delta(&labels, counter_frame("phase", "load", "ops", 100));
        store.ingest_delta(&labels, counter_frame("phase", "load", "ops", 50));

        let results = store.query_last_window(|_| true);
        assert_eq!(results.len(), 1);
        match &results[0].1.samples[0] {
            Sample::Counter { value, .. } => assert_eq!(*value, 50), // last delta only
            _ => panic!("expected counter"),
        }
    }

    #[test]
    fn timer_cumulative_merges_histograms() {
        let mut store = InProcessMetricsStore::new();
        let labels = Labels::of("phase", "search");

        store.ingest_delta(&labels, timer_frame("phase", "search", &[1000, 2000]));
        store.ingest_delta(&labels, timer_frame("phase", "search", &[3000, 4000, 5000]));

        let results = store.query_cumulative(|_| true);
        assert_eq!(results.len(), 1);
        match &results[0].1.samples[0] {
            Sample::Timer { histogram, .. } => {
                assert_eq!(histogram.len(), 5); // all 5 observations merged
            }
            _ => panic!("expected timer"),
        }
    }

    #[test]
    fn query_with_filter() {
        let mut store = InProcessMetricsStore::new();
        store.ingest_delta(
            &Labels::of("phase", "load"),
            counter_frame("phase", "load", "ops", 100),
        );
        store.ingest_delta(
            &Labels::of("phase", "search"),
            counter_frame("phase", "search", "ops", 200),
        );

        let load_only = store.query_cumulative(|l| l.get("phase") == Some("load"));
        assert_eq!(load_only.len(), 1);
        assert_eq!(load_only[0].0.get("phase"), Some("load"));
    }

    #[test]
    fn flush_component_merges_final_delta() {
        let mut store = InProcessMetricsStore::new();
        let labels = Labels::of("phase", "load");

        store.ingest_delta(&labels, counter_frame("phase", "load", "ops", 100));
        store.flush_component(&labels, counter_frame("phase", "load", "ops", 25));

        let results = store.query_cumulative(|_| true);
        match &results[0].1.samples[0] {
            Sample::Counter { value, .. } => assert_eq!(*value, 125),
            _ => panic!("expected counter"),
        }
    }

    #[test]
    fn multiple_components_tracked_independently() {
        let mut store = InProcessMetricsStore::new();
        store.ingest_delta(
            &Labels::of("phase", "a"),
            counter_frame("phase", "a", "ops", 10),
        );
        store.ingest_delta(
            &Labels::of("phase", "b"),
            counter_frame("phase", "b", "ops", 20),
        );

        assert_eq!(store.component_count(), 2);
        let all = store.query_cumulative(|_| true);
        assert_eq!(all.len(), 2);
    }
}
