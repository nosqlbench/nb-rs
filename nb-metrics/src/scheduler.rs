// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Metrics snapshot scheduler with hierarchical frame coalescing.
//!
//! A dedicated thread captures frames at the base interval. Each
//! reporter is registered at its own interval (must be an exact
//! multiple of the base). Schedule nodes accumulate and coalesce
//! frames for slower reporters. Delivery to reporters is concurrent.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::thread;

use crate::frame::MetricsFrame;

/// Trait for metrics reporters.
pub trait Reporter: Send + 'static {
    fn report(&mut self, frame: &MetricsFrame);
    fn flush(&mut self) {}
}

/// A capture function that produces a frame from live instruments.
pub type CaptureFunc = Box<dyn Fn() -> MetricsFrame + Send>;

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

    /// Ingest a frame from the parent. Accumulate, and when the
    /// interval is satisfied, coalesce and emit.
    fn ingest(&mut self, frame: MetricsFrame) {
        self.accumulated_duration += frame.interval;
        self.accumulated.push(frame);

        if self.accumulated_duration >= self.interval {
            let coalesced = MetricsFrame::coalesce(&self.accumulated);
            self.accumulated.clear();
            self.accumulated_duration = Duration::ZERO;

            // Deliver to reporters
            for reporter in &mut self.reporters {
                reporter.report(&coalesced);
            }

            // Cascade to children
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
}

impl SchedulerBuilder {
    pub fn new() -> Self {
        Self {
            config: SchedulerConfig::default(),
            reporters: Vec::new(),
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

    /// Build the schedule tree and return a handle.
    ///
    /// The scheduler is not yet running — call `start()` on the handle.
    pub fn build(self, capture: CaptureFunc) -> SchedulerHandle {
        // Build the tree: root node at base interval, child nodes for
        // each distinct reporter interval.
        let base = self.config.base_interval;
        let mut root = ScheduleNode::new(base);

        // Group reporters by interval
        let mut by_interval: std::collections::BTreeMap<Duration, Vec<Box<dyn Reporter>>> =
            std::collections::BTreeMap::new();
        for (interval, reporter) in self.reporters {
            by_interval.entry(interval).or_default().push(reporter);
        }

        // Build nodes. Reporters at base interval go on the root.
        // Others become child nodes.
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
        }
    }
}

/// Handle to a running (or startable) scheduler.
pub struct SchedulerHandle {
    root: Arc<Mutex<ScheduleNode>>,
    capture: CaptureFunc,
    base_interval: Duration,
    running: Arc<Mutex<bool>>,
}

impl SchedulerHandle {
    /// Start the scheduler on a dedicated thread.
    ///
    /// Returns a `StopHandle` that can be used to shut down.
    pub fn start(self) -> StopHandle {
        let root = self.root;
        let capture = self.capture;
        let interval = self.base_interval;
        let running = self.running.clone();

        *running.lock().unwrap() = true;

        let stop_running = running.clone();
        let handle = thread::spawn(move || {
            let mut next_tick = Instant::now() + interval;
            loop {
                if !*stop_running.lock().unwrap() {
                    break;
                }

                let now = Instant::now();
                if now < next_tick {
                    thread::sleep(next_tick - now);
                }
                next_tick += interval;

                let frame = (capture)();
                let mut root = root.lock().unwrap();

                // Deliver to root reporters
                for reporter in &mut root.reporters {
                    reporter.report(&frame);
                }

                // Feed to children for coalescing
                for child in &mut root.children {
                    child.ingest(frame.clone());
                }
            }

            // Flush all reporters
            flush_tree(&mut root.lock().unwrap());
        });

        StopHandle { running: self.running, thread: Some(handle) }
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
    thread: Option<thread::JoinHandle<()>>,
}

impl StopHandle {
    pub fn stop(mut self) {
        *self.running.lock().unwrap() = false;
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for StopHandle {
    fn drop(&mut self) {
        *self.running.lock().unwrap() = false;
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct CountingReporter {
        count: Arc<AtomicU64>,
    }

    impl Reporter for CountingReporter {
        fn report(&mut self, _frame: &MetricsFrame) {
            self.count.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn scheduler_builds() {
        let count = Arc::new(AtomicU64::new(0));
        let c = count.clone();
        let handle = SchedulerBuilder::new()
            .base_interval(Duration::from_millis(100))
            .add_reporter(Duration::from_millis(100), CountingReporter { count: c })
            .build(Box::new(|| MetricsFrame {
                captured_at: Instant::now(),
                interval: Duration::from_millis(100),
                samples: Vec::new(),
            }));

        let stop = handle.start();
        thread::sleep(Duration::from_millis(350));
        stop.stop();

        // Should have received ~3 reports in 350ms at 100ms intervals
        let c = count.load(Ordering::Relaxed);
        assert!(c >= 2 && c <= 5, "expected ~3 reports, got {c}");
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
            .build(Box::new(|| MetricsFrame {
                captured_at: Instant::now(),
                interval: Duration::from_millis(50),
                samples: Vec::new(),
            }));

        let stop = handle.start();
        thread::sleep(Duration::from_millis(450));
        stop.stop();

        let fast = fast_count.load(Ordering::Relaxed);
        let slow = slow_count.load(Ordering::Relaxed);
        // Fast: ~8-9 reports. Slow: ~2 reports (at 200ms intervals).
        assert!(fast >= 6, "fast should get many reports, got {fast}");
        assert!(slow >= 1 && slow <= 3, "slow should get ~2, got {slow}");
    }
}
