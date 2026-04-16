// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Metrics snapshot scheduler with hierarchical frame coalescing.
//!
//! A dedicated thread captures frames at the base interval from the
//! component tree. Each reporter is registered at its own interval
//! (must be an exact multiple of the base). Schedule nodes accumulate
//! and coalesce frames for slower reporters.
//!
//! The in-process metrics store is fed at every base tick, maintaining
//! per-component cumulative and last-window views for GK, summary
//! report, and other in-process consumers.

use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use std::thread;

use crate::frame::MetricsFrame;
use crate::labels::Labels;
use crate::store::InProcessMetricsStore;

/// Trait for metrics reporters (external consumers: SQLite, CSV, etc.).
pub trait Reporter: Send + 'static {
    fn report(&mut self, frame: &MetricsFrame);
    fn flush(&mut self) {}
}

/// Capture function that produces per-component delta frames
/// from the component tree.
///
/// Returns one `(effective_labels, delta_frame)` per RUNNING
/// component that has instruments with data.
pub type CaptureFunc = Box<dyn Fn() -> Vec<(Labels, MetricsFrame)> + Send>;

/// A node in the schedule tree that accumulates and coalesces frames.
struct ScheduleNode {
    interval: Duration,
    accumulated: Vec<MetricsFrame>,
    accumulated_duration: Duration,
    reporters: Vec<Box<dyn Reporter>>,
    children: Vec<ScheduleNode>,
}

impl ScheduleNode {
    fn new(interval: Duration) -> Self {
        Self {
            interval,
            accumulated: Vec::new(),
            accumulated_duration: Duration::ZERO,
            reporters: Vec::new(),
            children: Vec::new(),
        }
    }

    /// Ingest a combined frame. Accumulate, and when the
    /// interval is satisfied, coalesce and emit.
    fn ingest(&mut self, frame: MetricsFrame) {
        self.accumulated_duration += frame.interval;
        self.accumulated.push(frame);

        if self.accumulated_duration >= self.interval {
            let coalesced = MetricsFrame::coalesce(&self.accumulated);
            self.accumulated.clear();
            self.accumulated_duration = Duration::ZERO;

            for reporter in &mut self.reporters {
                reporter.report(&coalesced);
            }
            for child in &mut self.children {
                child.ingest(coalesced.clone());
            }
        }
    }
}

/// Configuration for the snapshot scheduler.
pub struct SchedulerConfig {
    pub base_interval: Duration,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self { base_interval: Duration::from_secs(1) }
    }
}

/// Builder for constructing a scheduler with reporters.
pub struct SchedulerBuilder {
    config: SchedulerConfig,
    reporters: Vec<(Duration, Box<dyn Reporter>)>,
    store: Option<Arc<RwLock<InProcessMetricsStore>>>,
}

impl Default for SchedulerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl SchedulerBuilder {
    pub fn new() -> Self {
        Self {
            config: SchedulerConfig::default(),
            reporters: Vec::new(),
            store: None,
        }
    }

    pub fn base_interval(mut self, interval: Duration) -> Self {
        self.config.base_interval = interval;
        self
    }

    pub fn add_reporter(mut self, interval: Duration, reporter: impl Reporter) -> Self {
        self.reporters.push((interval, Box::new(reporter)));
        self
    }

    /// Set the in-process metrics store. Fed at every base tick.
    pub fn with_store(mut self, store: Arc<RwLock<InProcessMetricsStore>>) -> Self {
        self.store = Some(store);
        self
    }

    /// Build the schedule tree and return a handle.
    ///
    /// The scheduler is not yet running — call `start()` on the handle.
    pub fn build(self, capture: CaptureFunc) -> SchedulerHandle {
        let base = self.config.base_interval;
        let mut root = ScheduleNode::new(base);

        let mut by_interval: std::collections::BTreeMap<Duration, Vec<Box<dyn Reporter>>> =
            std::collections::BTreeMap::new();
        for (interval, reporter) in self.reporters {
            by_interval.entry(interval).or_default().push(reporter);
        }

        for (interval, reporters) in by_interval {
            if interval == base {
                root.reporters.extend(reporters);
            } else {
                assert!(
                    interval.as_millis() % base.as_millis() == 0,
                    "reporter interval {:?} must be an exact multiple of base {:?}",
                    interval, base
                );
                let mut node = ScheduleNode::new(interval);
                node.reporters = reporters;
                root.children.push(node);
            }
        }

        SchedulerHandle {
            root: Arc::new(Mutex::new(root)),
            capture,
            base_interval: base,
            running: Arc::new(Mutex::new(false)),
            store: self.store.unwrap_or_else(|| {
                Arc::new(RwLock::new(InProcessMetricsStore::new()))
            }),
        }
    }
}

/// Handle to a running (or startable) scheduler.
pub struct SchedulerHandle {
    root: Arc<Mutex<ScheduleNode>>,
    capture: CaptureFunc,
    base_interval: Duration,
    running: Arc<Mutex<bool>>,
    store: Arc<RwLock<InProcessMetricsStore>>,
}

impl SchedulerHandle {
    /// Reference to the in-process metrics store.
    pub fn store(&self) -> &Arc<RwLock<InProcessMetricsStore>> {
        &self.store
    }

    /// Flush a retiring component's final delta into the store.
    ///
    /// Called from the executor thread when a phase completes,
    /// outside the scheduler tick loop.
    pub fn flush_component(&self, labels: &Labels, final_delta: MetricsFrame) {
        if let Ok(mut store) = self.store.write() {
            store.flush_component(labels, final_delta);
        }
    }

    /// Start the scheduler on a dedicated thread.
    ///
    /// Returns a `StopHandle` that can be used to shut down.
    pub fn start(self) -> StopHandle {
        let root = self.root.clone();
        let root_for_stop = self.root;
        let capture = self.capture;
        let interval = self.base_interval;
        let running = self.running.clone();
        let store = self.store.clone();

        *running.lock().unwrap_or_else(|e| e.into_inner()) = true;

        let stop_running = running.clone();
        let handle = thread::spawn(move || {
            let mut next_tick = Instant::now() + interval;
            loop {
                if !*stop_running.lock().unwrap_or_else(|e| e.into_inner()) {
                    break;
                }

                let now = Instant::now();
                if now < next_tick {
                    thread::sleep(next_tick - now);
                }
                next_tick += interval;

                // Capture per-component deltas from the tree
                let component_frames = (capture)();

                // Feed each component's delta to the in-process store
                if let Ok(mut s) = store.write() {
                    for (labels, frame) in &component_frames {
                        s.ingest_delta(labels, frame.clone());
                    }
                }

                // Merge all component frames into one combined frame
                // for the cadence tree (external reporters)
                let all_frames: Vec<MetricsFrame> = component_frames.into_iter()
                    .map(|(_, frame)| frame)
                    .collect();
                let combined = if all_frames.is_empty() {
                    MetricsFrame {
                        captured_at: Instant::now(),
                        interval,
                        samples: Vec::new(),
                    }
                } else {
                    let mut merged = MetricsFrame::coalesce(&all_frames);
                    // Ensure the interval reflects the scheduler interval,
                    // not the sum from coalesce (which sums intervals)
                    merged.interval = interval;
                    merged
                };

                let mut root = root.lock().unwrap_or_else(|e| e.into_inner());

                // Deliver to root-level reporters
                for reporter in &mut root.reporters {
                    reporter.report(&combined);
                }

                // Feed to children for coalescing
                for child in &mut root.children {
                    child.ingest(combined.clone());
                }
            }

            // Final capture before shutdown: ensures short-lived phases
            // that completed between ticks get their data to reporters.
            {
                let component_frames = (capture)();
                if let Ok(mut s) = store.write() {
                    for (labels, frame) in &component_frames {
                        s.ingest_delta(labels, frame.clone());
                    }
                }
                let all_frames: Vec<MetricsFrame> = component_frames.into_iter()
                    .map(|(_, frame)| frame)
                    .collect();
                if !all_frames.is_empty() {
                    let mut merged = MetricsFrame::coalesce(&all_frames);
                    merged.interval = interval;
                    let mut root_node = root.lock().unwrap_or_else(|e| e.into_inner());
                    for reporter in &mut root_node.reporters {
                        reporter.report(&merged);
                    }
                    for child in &mut root_node.children {
                        child.ingest(merged.clone());
                    }
                }
            }
            // Flush all reporters on shutdown
            flush_tree(&mut root.lock().unwrap_or_else(|e| e.into_inner()));
        });

        StopHandle {
            running: self.running,
            store: self.store,
            root: root_for_stop,
            thread: Some(handle),
        }
    }
}

fn flush_tree(node: &mut ScheduleNode) {
    for reporter in &mut node.reporters {
        reporter.flush();
    }
    for child in &mut node.children {
        flush_tree(child);
    }
}

/// Handle to stop a running scheduler.
pub struct StopHandle {
    running: Arc<Mutex<bool>>,
    store: Arc<RwLock<InProcessMetricsStore>>,
    root: Arc<Mutex<ScheduleNode>>,
    thread: Option<thread::JoinHandle<()>>,
}

impl StopHandle {
    /// Stop the scheduler and join the capture thread.
    pub fn stop(&mut self) {
        *self.running.lock().unwrap_or_else(|e| e.into_inner()) = false;
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
    }

    /// Reference to the in-process metrics store.
    ///
    /// Remains valid and queryable after the scheduler is stopped.
    pub fn store(&self) -> &Arc<RwLock<InProcessMetricsStore>> {
        &self.store
    }

    /// Deliver a frame directly to all reporters on the root node.
    ///
    /// Used for lifecycle flush: the executor captures a final delta
    /// and needs it delivered to SQLite (and other reporters) outside
    /// the scheduler tick loop.
    pub fn report_frame(&self, frame: &MetricsFrame) {
        let mut root = self.root.lock().unwrap_or_else(|e| e.into_inner());
        for reporter in &mut root.reporters {
            reporter.report(frame);
        }
        for child in &mut root.children {
            child.ingest(frame.clone());
        }
    }
}

impl Drop for StopHandle {
    fn drop(&mut self) {
        *self.running.lock().unwrap_or_else(|e| e.into_inner()) = false;
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use crate::frame::Sample;

    struct CountingReporter {
        count: Arc<AtomicU64>,
    }

    impl Reporter for CountingReporter {
        fn report(&mut self, _frame: &MetricsFrame) {
            self.count.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn mock_capture() -> Vec<(Labels, MetricsFrame)> {
        vec![(
            Labels::of("phase", "test"),
            MetricsFrame {
                captured_at: Instant::now(),
                interval: Duration::from_millis(100),
                samples: vec![Sample::Counter {
                    labels: Labels::of("name", "ops"),
                    value: 10,
                }],
            },
        )]
    }

    #[test]
    fn scheduler_builds_and_reports() {
        let count = Arc::new(AtomicU64::new(0));
        let c = count.clone();
        let handle = SchedulerBuilder::new()
            .base_interval(Duration::from_millis(100))
            .add_reporter(Duration::from_millis(100), CountingReporter { count: c })
            .build(Box::new(mock_capture));

        let mut stop = handle.start();
        thread::sleep(Duration::from_millis(350));
        stop.stop();

        let c = count.load(Ordering::Relaxed);
        assert!(c >= 2 && c <= 5, "expected ~3 reports, got {c}");
    }

    #[test]
    fn scheduler_feeds_in_process_store() {
        let store = Arc::new(RwLock::new(InProcessMetricsStore::new()));
        let handle = SchedulerBuilder::new()
            .base_interval(Duration::from_millis(100))
            .with_store(store.clone())
            .build(Box::new(mock_capture));

        let mut stop = handle.start();
        thread::sleep(Duration::from_millis(350));
        stop.stop();

        let s = store.read().unwrap();
        assert_eq!(s.component_count(), 1);

        // Cumulative should have merged multiple deltas
        let cum = s.query_cumulative(|_| true);
        assert_eq!(cum.len(), 1);
        match &cum[0].1.samples[0] {
            Sample::Counter { value, .. } => {
                // ~3 ticks × 10 per tick = ~30
                assert!(*value >= 20, "cumulative counter should be ≥20, got {value}");
            }
            _ => panic!("expected counter"),
        }

        // Last window should be exactly one tick's worth
        let lw = s.query_last_window(|_| true);
        assert_eq!(lw.len(), 1);
        match &lw[0].1.samples[0] {
            Sample::Counter { value, .. } => assert_eq!(*value, 10),
            _ => panic!("expected counter"),
        }
    }

    #[test]
    fn scheduler_coalesces_for_slow_reporter() {
        let fast_count = Arc::new(AtomicU64::new(0));
        let slow_count = Arc::new(AtomicU64::new(0));
        let fc = fast_count.clone();
        let sc = slow_count.clone();

        let handle = SchedulerBuilder::new()
            .base_interval(Duration::from_millis(50))
            .add_reporter(Duration::from_millis(50), CountingReporter { count: fc })
            .add_reporter(Duration::from_millis(200), CountingReporter { count: sc })
            .build(Box::new(|| vec![(
                Labels::of("phase", "test"),
                MetricsFrame {
                    captured_at: Instant::now(),
                    interval: Duration::from_millis(50),
                    samples: Vec::new(),
                },
            )]));

        let mut stop = handle.start();
        thread::sleep(Duration::from_millis(450));
        stop.stop();

        let fast = fast_count.load(Ordering::Relaxed);
        let slow = slow_count.load(Ordering::Relaxed);
        assert!(fast >= 6, "fast should get many reports, got {fast}");
        assert!(slow >= 1 && slow <= 3, "slow should get ~2, got {slow}");
    }

    #[test]
    fn flush_component_accessible_from_outside() {
        let store = Arc::new(RwLock::new(InProcessMetricsStore::new()));
        let handle = SchedulerBuilder::new()
            .with_store(store.clone())
            .build(Box::new(|| Vec::new()));

        // Flush without starting — simulates lifecycle retirement
        let labels = Labels::of("phase", "done");
        let frame = MetricsFrame {
            captured_at: Instant::now(),
            interval: Duration::from_secs(1),
            samples: vec![Sample::Counter {
                labels: Labels::of("name", "final_ops"),
                value: 42,
            }],
        };
        handle.flush_component(&labels, frame);

        let s = store.read().unwrap();
        let cum = s.query_cumulative(|_| true);
        assert_eq!(cum.len(), 1);
    }
}
