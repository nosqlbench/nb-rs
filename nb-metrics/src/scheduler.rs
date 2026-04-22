// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Metrics snapshot scheduler with hierarchical frame coalescing.
//!
//! A dedicated thread captures frames at the base interval from the
//! component tree. Each reporter is registered at its own interval
//! (must be an exact multiple of the base). Schedule nodes accumulate
//! and coalesce frames for slower reporters.
//!
//! At every tick the scheduler also feeds the installed
//! [`CadenceReporter`] (SRD-42), which owns the windowed snapshot
//! store read by every consumer through
//! [`crate::metrics_query::MetricsQuery`].

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::thread;

use crate::cadence_reporter::CadenceReporter;
use crate::labels::Labels;
use crate::snapshot::MetricSet;

/// Trait for metrics reporters (external consumers: SQLite, CSV, etc.).
pub trait Reporter: Send + 'static {
    fn report(&mut self, snapshot: &MetricSet);
    fn flush(&mut self) {}
}

/// Capture function that produces per-component delta snapshots
/// from the component tree.
///
/// Returns one `(effective_labels, delta_snapshot)` per RUNNING
/// component that has instruments with data.
pub type CaptureFunc = Box<dyn Fn() -> Vec<(Labels, MetricSet)> + Send>;

/// A node in the schedule tree that accumulates and coalesces snapshots.
struct ScheduleNode {
    interval: Duration,
    accumulated: Vec<MetricSet>,
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

    /// Ingest a combined snapshot. Accumulate, and when the
    /// interval is satisfied, coalesce and emit.
    fn ingest(&mut self, snapshot: MetricSet) {
        self.accumulated_duration += snapshot.interval();
        self.accumulated.push(snapshot);

        if self.accumulated_duration >= self.interval {
            let coalesced = MetricSet::coalesce(&self.accumulated);
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
    cadence_reporter: Option<Arc<CadenceReporter>>,
    cadence_tree: Option<crate::cadence::CadenceTree>,
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
            cadence_reporter: None,
            cadence_tree: None,
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

    /// Install the cadence reporter that owns the windowed snapshot
    /// store. On every scheduler tick, captured per-component
    /// snapshots are fed into this reporter, which cascades them
    /// up the cadence tree and publishes closed windows to
    /// [`crate::metrics_query::MetricsQuery`] readers.
    pub fn with_cadence_reporter(mut self, reporter: Arc<CadenceReporter>) -> Self {
        self.cadence_reporter = Some(reporter);
        self
    }

    /// Install a cadence tree (SRD-42 §"Tree Construction"). When set,
    /// `build()` constructs a chained schedule where each layer feeds
    /// the next via [`ScheduleNode::ingest`] rather than coalescing
    /// from base frames independently. Hidden layers participate in
    /// accumulation but have no reporters of their own.
    ///
    /// Reporters at intervals matching a tree layer attach at that
    /// layer; reporters at intervals outside the tree continue to
    /// attach as flat children of root (backward-compatible).
    pub fn with_cadence_tree(mut self, tree: crate::cadence::CadenceTree) -> Self {
        self.cadence_tree = Some(tree);
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

        // Reporters that match the base interval always live on root.
        if let Some(reps) = by_interval.remove(&base) {
            root.reporters.extend(reps);
        }

        // If a cadence tree was provided, build the chained sub-tree.
        // Walking layers largest → smallest builds the chain from the
        // leaf inward, so each node owns its single child.
        if let Some(tree) = self.cadence_tree {
            let mut chain: Option<ScheduleNode> = None;
            for layer in tree.layers().iter().rev() {
                if layer.interval == base {
                    // Base-interval "layer" is just the root itself —
                    // any reporters at that interval are already on
                    // root. Skip without nesting.
                    continue;
                }
                assert!(
                    layer.interval.as_millis() % base.as_millis() == 0,
                    "cadence layer {:?} must be an exact multiple of base {:?}",
                    layer.interval, base,
                );
                let mut node = ScheduleNode::new(layer.interval);
                if !layer.hidden {
                    if let Some(reps) = by_interval.remove(&layer.interval) {
                        node.reporters = reps;
                    }
                }
                if let Some(child) = chain.take() {
                    node.children.push(child);
                }
                chain = Some(node);
            }
            if let Some(top) = chain {
                root.children.push(top);
            }
        }

        // Reporters not consumed by the tree (intervals outside it,
        // or no tree at all) attach as flat children of root — same
        // behavior as before this layering existed.
        for (interval, reporters) in by_interval {
            assert!(
                interval.as_millis() % base.as_millis() == 0,
                "reporter interval {:?} must be an exact multiple of base {:?}",
                interval, base
            );
            let mut node = ScheduleNode::new(interval);
            node.reporters = reporters;
            root.children.push(node);
        }

        SchedulerHandle {
            root: Arc::new(Mutex::new(root)),
            capture,
            base_interval: base,
            running: Arc::new(Mutex::new(false)),
            cadence_reporter: self.cadence_reporter,
        }
    }
}

/// Handle to a running (or startable) scheduler.
pub struct SchedulerHandle {
    root: Arc<Mutex<ScheduleNode>>,
    capture: CaptureFunc,
    base_interval: Duration,
    running: Arc<Mutex<bool>>,
    cadence_reporter: Option<Arc<CadenceReporter>>,
}

impl SchedulerHandle {
    /// Reference to the installed cadence reporter, if any.
    pub fn cadence_reporter(&self) -> Option<&Arc<CadenceReporter>> {
        self.cadence_reporter.as_ref()
    }

    /// Flush a retiring component's final delta through the
    /// cadence reporter (if present). Called from the executor
    /// thread when a phase completes, outside the scheduler tick
    /// loop.
    pub fn flush_component(&self, labels: &Labels, final_delta: MetricSet) {
        if let Some(reporter) = &self.cadence_reporter {
            reporter.ingest(labels, final_delta);
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
        let cadence_reporter = self.cadence_reporter.clone();
        let cadence_reporter_for_stop = self.cadence_reporter.clone();

        let (frame_tx, frame_rx) = std::sync::mpsc::channel::<MetricSet>();

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

                // Drain async snapshot channel (lifecycle flushes from executor)
                while let Ok(snapshot) = frame_rx.try_recv() {
                    let mut root = root.lock().unwrap_or_else(|e| e.into_inner());
                    for reporter in &mut root.reporters {
                        reporter.report(&snapshot);
                    }
                    for child in &mut root.children {
                        child.ingest(snapshot.clone());
                    }
                }

                // Capture per-component deltas from the tree
                let component_snapshots = (capture)();

                // Feed each per-component delta into the cadence
                // reporter (single writer of windowed snapshots).
                if let Some(ref cr) = cadence_reporter {
                    for (labels, snapshot) in &component_snapshots {
                        cr.ingest(labels, snapshot.clone());
                    }
                }

                // Merge all component snapshots into one combined snapshot
                // for the scheduler-tree reporters (CSV / SQLite / etc.).
                let all_snapshots: Vec<MetricSet> = component_snapshots.into_iter()
                    .map(|(_, snapshot)| snapshot)
                    .collect();
                let combined = if all_snapshots.is_empty() {
                    MetricSet::new(interval)
                } else {
                    let mut merged = MetricSet::coalesce(&all_snapshots);
                    // Ensure the interval reflects the scheduler interval,
                    // not the sum from coalesce (which sums intervals)
                    merged.set_interval(interval);
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
                let component_snapshots = (capture)();
                if let Some(ref cr) = cadence_reporter {
                    for (labels, snapshot) in &component_snapshots {
                        cr.ingest(labels, snapshot.clone());
                    }
                }
                let all_snapshots: Vec<MetricSet> = component_snapshots.into_iter()
                    .map(|(_, snapshot)| snapshot)
                    .collect();
                if !all_snapshots.is_empty() {
                    let mut merged = MetricSet::coalesce(&all_snapshots);
                    merged.set_interval(interval);
                    let mut root_node = root.lock().unwrap_or_else(|e| e.into_inner());
                    for reporter in &mut root_node.reporters {
                        reporter.report(&merged);
                    }
                    for child in &mut root_node.children {
                        child.ingest(merged.clone());
                    }
                }
                // Force-close any unpromoted partials so the
                // trailing window is not lost.
                if let Some(ref cr) = cadence_reporter {
                    cr.shutdown_flush();
                }
            }
            // Drain any remaining async snapshots before final flush
            while let Ok(snapshot) = frame_rx.try_recv() {
                let mut r = root.lock().unwrap_or_else(|e| e.into_inner());
                for reporter in &mut r.reporters {
                    reporter.report(&snapshot);
                }
                for child in &mut r.children {
                    child.ingest(snapshot.clone());
                }
            }
            // Flush all reporters on shutdown
            flush_tree(&mut root.lock().unwrap_or_else(|e| e.into_inner()));
        });

        StopHandle {
            running: self.running,
            cadence_reporter: cadence_reporter_for_stop,
            root: root_for_stop,
            thread: Some(handle),
            frame_tx,
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
    cadence_reporter: Option<Arc<CadenceReporter>>,
    #[allow(dead_code)] // retained for future direct-flush access
    root: Arc<Mutex<ScheduleNode>>,
    thread: Option<thread::JoinHandle<()>>,
    /// Channel for async frame delivery — the executor sends frames
    /// here instead of writing to reporters inline. The scheduler
    /// thread drains this channel on each tick.
    frame_tx: std::sync::mpsc::Sender<MetricSet>,
}

impl StopHandle {
    /// Stop the scheduler and join the capture thread.
    pub fn stop(&mut self) {
        *self.running.lock().unwrap_or_else(|e| e.into_inner()) = false;
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
    }

    /// Reference to the cadence reporter, if any.
    ///
    /// Remains valid and queryable after the scheduler is stopped.
    pub fn cadence_reporter(&self) -> Option<&Arc<CadenceReporter>> {
        self.cadence_reporter.as_ref()
    }

    /// Deliver a frame to reporters asynchronously.
    ///
    /// The frame is enqueued on a channel and processed by the
    /// scheduler thread on its next tick. This never blocks the
    /// caller — safe to call from tokio worker threads.
    pub fn report_frame(&self, snapshot: &MetricSet) {
        let _ = self.frame_tx.send(snapshot.clone());
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
    use crate::snapshot::MetricValue;

    struct CountingReporter {
        count: Arc<AtomicU64>,
    }

    impl Reporter for CountingReporter {
        fn report(&mut self, _snapshot: &MetricSet) {
            self.count.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn mock_capture() -> Vec<(Labels, MetricSet)> {
        let mut s = MetricSet::new(Duration::from_millis(100));
        s.insert_counter("ops", Labels::default(), 10, Instant::now());
        vec![(Labels::of("phase", "test"), s)]
    }

    fn empty_snapshot(interval: Duration) -> MetricSet {
        MetricSet::new(interval)
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
    fn scheduler_feeds_cadence_reporter() {
        use crate::cadence::{Cadences, CadenceTree};

        let tree = CadenceTree::plan_default(Cadences::new(&[
            Duration::from_millis(100),
        ]).unwrap());
        let reporter = Arc::new(CadenceReporter::new(tree));
        let handle = SchedulerBuilder::new()
            .base_interval(Duration::from_millis(100))
            .with_cadence_reporter(reporter.clone())
            .build(Box::new(mock_capture));

        let mut stop = handle.start();
        thread::sleep(Duration::from_millis(350));
        stop.stop();

        // Reporter received ingests — has the component tracked.
        let components = reporter.component_labels();
        assert_eq!(components.len(), 1);
        // The 100ms cadence should have at least one closed snapshot.
        let component = &components[0];
        let latest = reporter.latest(component, Duration::from_millis(100))
            .expect("cadence reporter should have a closed 100ms snapshot");
        let ops_total = match latest.family("ops").unwrap()
            .metrics().next().unwrap().point().unwrap().value() {
            MetricValue::Counter(c) => c.total,
            _ => panic!("expected counter"),
        };
        assert_eq!(ops_total, 10, "one tick = 10");
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
                empty_snapshot(Duration::from_millis(50)),
            )]));

        let mut stop = handle.start();
        thread::sleep(Duration::from_millis(450));
        stop.stop();

        let fast = fast_count.load(Ordering::Relaxed);
        let slow = slow_count.load(Ordering::Relaxed);
        assert!(fast >= 6, "fast should get many reports, got {fast}");
        assert!(slow >= 1 && slow <= 3, "slow should get ~2, got {slow}");
    }

    /// With a CadenceTree installed, a slow reporter at the largest
    /// declared cadence is fed *through* the chain (root → smallest
    /// → … → largest). Functionally indistinguishable from the flat
    /// arrangement at the consumer level — same number of reports,
    /// same coalesced data — but internally the largest layer's
    /// accumulation is bounded by the next-smaller cadence, not by
    /// every base frame.
    #[test]
    fn scheduler_chained_tree_delivers_to_largest_cadence() {
        use crate::cadence::{Cadences, CadenceTree};

        let small_count = Arc::new(AtomicU64::new(0));
        let large_count = Arc::new(AtomicU64::new(0));
        let sc = small_count.clone();
        let lc = large_count.clone();

        // Cadences: 100ms (smallest declared) and 400ms (largest).
        // Ratio 4 — well under default fan-in, no hidden inserts.
        let tree = CadenceTree::plan_default(
            Cadences::new(&[
                Duration::from_millis(100),
                Duration::from_millis(400),
            ]).unwrap(),
        );

        let handle = SchedulerBuilder::new()
            .base_interval(Duration::from_millis(100))
            .with_cadence_tree(tree)
            .add_reporter(Duration::from_millis(100), CountingReporter { count: sc })
            .add_reporter(Duration::from_millis(400), CountingReporter { count: lc })
            .build(Box::new(|| vec![(
                Labels::of("phase", "test"),
                empty_snapshot(Duration::from_millis(100)),
            )]));

        let mut stop = handle.start();
        thread::sleep(Duration::from_millis(900));
        stop.stop();

        let small = small_count.load(Ordering::Relaxed);
        let large = large_count.load(Ordering::Relaxed);
        // ~9 base ticks → smallest fires every tick (≥6) and
        // largest fires every 4 (≥1, ≤3).
        assert!(small >= 6, "smallest cadence reports = {small}");
        assert!(large >= 1 && large <= 3, "largest cadence reports = {large}");
    }

    /// Hidden intermediate layers (auto-inserted by the planner)
    /// participate in accumulation but never deliver to a reporter.
    /// Verify that a reporter only at the *largest* declared cadence
    /// still gets its expected report count even when a hidden
    /// layer sits between it and the smallest cadence.
    #[test]
    fn scheduler_hidden_layers_pass_through_to_visible_reporters() {
        use crate::cadence::{Cadences, CadenceTree};

        let large_count = Arc::new(AtomicU64::new(0));
        let lc = large_count.clone();

        // 50ms → 1500ms is ratio 30 — exceeds default K=20, so the
        // planner inserts a hidden intermediate. Ensures the chain
        // flows through it correctly.
        let tree = CadenceTree::plan_default(
            Cadences::new(&[
                Duration::from_millis(50),
                Duration::from_millis(1500),
            ]).unwrap(),
        );
        // Sanity check the planner actually inserted one.
        let inserted: Vec<_> = tree.hidden().collect();
        assert!(!inserted.is_empty(), "test relies on hidden insertion");

        let handle = SchedulerBuilder::new()
            .base_interval(Duration::from_millis(50))
            .with_cadence_tree(tree)
            .add_reporter(Duration::from_millis(1500), CountingReporter { count: lc })
            .build(Box::new(|| vec![(
                Labels::of("phase", "test"),
                empty_snapshot(Duration::from_millis(50)),
            )]));

        let mut stop = handle.start();
        thread::sleep(Duration::from_millis(3300));
        stop.stop();

        let large = large_count.load(Ordering::Relaxed);
        assert!(large >= 1, "largest reporter saw 0 frames — chain broken");
    }

    #[test]
    fn flush_component_routes_to_cadence_reporter() {
        use crate::cadence::{Cadences, CadenceTree};

        let tree = CadenceTree::plan_default(Cadences::new(&[
            Duration::from_secs(1),
        ]).unwrap());
        let reporter = Arc::new(CadenceReporter::new(tree));
        let handle = SchedulerBuilder::new()
            .with_cadence_reporter(reporter.clone())
            .build(Box::new(|| Vec::new()));

        // Flush without starting — simulates lifecycle retirement
        let labels = Labels::of("phase", "done");
        let mut snapshot = MetricSet::new(Duration::from_secs(1));
        snapshot.insert_counter("final_ops", Labels::default(), 42, Instant::now());
        handle.flush_component(&labels, snapshot);

        // The flush went straight into the reporter's smallest
        // cadence accumulator and promoted (interval matched).
        let latest = reporter.latest(&labels, Duration::from_secs(1))
            .expect("flush should produce a closed snapshot");
        assert!(latest.family("final_ops").is_some());
    }
}
