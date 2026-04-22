// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Cadence reporter — single writer of windowed snapshots
//! (SRD-42 §"Wire-Up → Cadence reporter").
//!
//! On every smallest-cadence tick the reporter:
//!
//! 1. Receives the captured per-component [`MetricSet`]s from the
//!    scheduler.
//! 2. Folds them into the smallest-cadence accumulator.
//! 3. When that accumulator's interval is satisfied, seals it into an
//!    immutable `Arc<MetricSet>`, publishes it into the per-cadence
//!    store, and folds it into the next-larger cadence's prebuffer
//!    (per the streaming cascade in SRD-42 §"Streaming coalesce
//!    semantics").
//!
//! The published snapshots are read by [`crate::metrics_query::MetricsQuery`]
//! — the single read API for every consumer.
//!
//! ## Phase 7b status
//!
//! This module is the sole home for the windowed snapshot store.
//! The legacy per-component `WindowedMetrics` and `InProcessMetricsStore`
//! are deleted as of Phase 7b; all writes go through this type and
//! all reads go through [`crate::metrics_query::MetricsQuery`].

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{SyncSender, TrySendError};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::cadence::{CadenceLayer, CadenceTree, Cadences};
use crate::labels::Labels;
use crate::scheduler::Reporter;
use crate::snapshot::MetricSet;

/// Maximum number of historical closed snapshots retained per
/// `(component_path, cadence)` ring. Bounds memory regardless of
/// run length. See SRD-42 §"Open Questions → Histogram retention
/// for past() queries".
pub const HISTORY_RING_CAP: usize = 32;

/// Default bounded-channel capacity for subscription dispatch. A
/// subscriber that can't drain this many snapshots in
/// `2 × cadence_interval` has fallen behind enough to trip the
/// timeout path.
pub const DEFAULT_SUBSCRIPTION_CHANNEL_CAPACITY: usize = 8;

// =========================================================================
// CadenceReporter — the single writer
// =========================================================================

/// Per-component, per-cadence streaming accumulator + retention ring.
///
/// One instance per `(component_path, cadence)` pair. Owns:
///
/// - the in-flight prebuffer for this cadence (accumulating folds
///   from the next-smaller cadence — or the smallest cadence's
///   accumulating window when this is the smallest cadence),
/// - the most-recently-closed snapshot, exposed as `Arc` to readers,
/// - a bounded ring of past closed snapshots for `recent_window` and
///   `past(span)` queries.
struct CadenceWindow {
    cadence: Duration,
    /// Accumulated duration in the prebuffer.
    accumulated: Duration,
    /// In-flight prebuffer being assembled toward the next close.
    /// `None` until the first input arrives.
    prebuffer: Option<MetricSet>,
    /// Latest closed (immutable) snapshot.
    latest: Option<Arc<MetricSet>>,
    /// Bounded ring of past closed snapshots, newest at the back.
    ring: VecDeque<Arc<MetricSet>>,
}

impl CadenceWindow {
    fn new(cadence: Duration) -> Self {
        Self {
            cadence,
            accumulated: Duration::ZERO,
            prebuffer: None,
            latest: None,
            ring: VecDeque::new(),
        }
    }

    /// Fold an incoming snapshot into the prebuffer. Returns the
    /// just-closed snapshot if this fold completed the cadence's
    /// interval (caller propagates upstream), else `None`.
    fn ingest(&mut self, snapshot: MetricSet) -> Option<Arc<MetricSet>> {
        self.accumulated += snapshot.interval();
        match &mut self.prebuffer {
            None => self.prebuffer = Some(snapshot),
            Some(buf) => {
                let merged = MetricSet::coalesce(&[buf.clone(), snapshot]);
                *buf = merged;
            }
        }

        if self.accumulated >= self.cadence {
            let mut closed = self.prebuffer.take().expect("prebuffer present after fold");
            // Stamp the cadence interval explicitly so consumers
            // never see "interval grew past cadence" rounding.
            closed.set_interval(self.cadence);
            let arc = Arc::new(closed);
            self.latest = Some(arc.clone());
            self.ring.push_back(arc.clone());
            while self.ring.len() > HISTORY_RING_CAP {
                self.ring.pop_front();
            }
            self.accumulated = Duration::ZERO;
            Some(arc)
        } else {
            None
        }
    }

    /// Force-close whatever's accumulated so the trailing partial
    /// window is published with `interval < cadence`. Used at
    /// shutdown per SRD-42 §"Streaming coalesce → Shutdown".
    fn force_close(&mut self) -> Option<Arc<MetricSet>> {
        let buf = self.prebuffer.take()?;
        if buf.is_empty() {
            return None;
        }
        // `interval` already reflects accumulated time from the
        // input snapshots' interval sum.
        let arc = Arc::new(buf);
        self.latest = Some(arc.clone());
        self.ring.push_back(arc.clone());
        while self.ring.len() > HISTORY_RING_CAP {
            self.ring.pop_front();
        }
        self.accumulated = Duration::ZERO;
        Some(arc)
    }

    /// Latest closed snapshot for this cadence, if any.
    fn latest(&self) -> Option<Arc<MetricSet>> { self.latest.clone() }

    /// Read-only ring of past closed snapshots, oldest first.
    fn ring(&self) -> impl Iterator<Item = &Arc<MetricSet>> {
        self.ring.iter()
    }

    /// Read-only access to the in-flight prebuffer (clone-friendly
    /// since `MetricSet` is `Clone`). Used by `session_lifetime` to
    /// peek partials without disturbing the cascade.
    fn prebuffer_clone(&self) -> Option<MetricSet> {
        self.prebuffer.clone()
    }
}

// =========================================================================
// Subscriptions — async push dispatch (SRD-42 §"Notification dispatch")
// =========================================================================

/// Opaque identifier for a subscription registered via
/// [`CadenceReporter::subscribe`]. Used to [`unsubscribe`](CadenceReporter::unsubscribe)
/// the subscriber later.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SubscriberId(u64);

/// Errors returned by [`CadenceReporter::subscribe`].
#[derive(Debug)]
pub enum SubscribeError {
    /// The requested cadence is not one of the reporter's declared
    /// or hidden layer intervals.
    UnknownCadence(Duration),
    /// Failed to spawn the dispatch thread for the subscription.
    SpawnFailed(String),
}

impl std::fmt::Display for SubscribeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownCadence(d) =>
                write!(f, "cadence {d:?} is not a layer of this reporter"),
            Self::SpawnFailed(e) =>
                write!(f, "failed to spawn subscription dispatch thread: {e}"),
        }
    }
}

impl std::error::Error for SubscribeError {}

/// Per-subscription configuration.
#[derive(Clone)]
pub struct SubscriptionOpts {
    /// Bounded channel capacity for this subscription. When the
    /// channel is full, `try_send` fails and the snapshot is dropped
    /// for this subscriber (cascade continues). Default:
    /// [`DEFAULT_SUBSCRIPTION_CHANNEL_CAPACITY`].
    pub channel_capacity: usize,
    /// Timeout — if `Instant::now() - last_successful_delivery`
    /// exceeds this, fire `on_timeout` once and arm re-firing only
    /// after the next successful delivery. Default: `2 ×
    /// cadence_interval` per SRD-42.
    pub timeout: Option<Duration>,
    /// Called (off the scheduler thread, on the dispatch thread)
    /// when `timeout` expires. Receives a [`TimeoutEvent`] with the
    /// cadence, subscriber id, undelivered-snapshot age, and
    /// consecutive-drop count so the cadence manager can decide
    /// whether to log, escalate, or unsubscribe.
    pub on_timeout: Option<TimeoutCallback>,
}

impl Default for SubscriptionOpts {
    fn default() -> Self {
        Self {
            channel_capacity: DEFAULT_SUBSCRIPTION_CHANNEL_CAPACITY,
            timeout: None,
            on_timeout: None,
        }
    }
}

/// Error callback invoked when a subscription's delivery timeout
/// expires. Runs on the dispatcher thread — must not block.
pub type TimeoutCallback = Arc<dyn Fn(TimeoutEvent) + Send + Sync>;

/// Delivery-timeout event surfaced to a subscription's
/// [`SubscriptionOpts::on_timeout`] callback.
#[derive(Clone, Debug)]
pub struct TimeoutEvent {
    pub subscriber_id: SubscriberId,
    pub cadence: Duration,
    /// Age of the snapshot that failed to deliver (relative to
    /// `Instant::now()`).
    pub snapshot_age: Duration,
    /// Count of consecutive `try_send` failures for this subscription
    /// since the last successful delivery.
    pub consecutive_drops: u64,
}

/// Per-subscription mutable bookkeeping. Shared between the reporter
/// (which calls `try_send`) and the dispatch thread (which updates
/// `last_delivered`).
struct SubscriptionState {
    last_delivered: Mutex<Instant>,
    consecutive_drops: AtomicU64,
    timeout_fired: AtomicBool,
}

impl SubscriptionState {
    fn new() -> Self {
        Self {
            last_delivered: Mutex::new(Instant::now()),
            consecutive_drops: AtomicU64::new(0),
            timeout_fired: AtomicBool::new(false),
        }
    }

    fn mark_delivered(&self) {
        if let Ok(mut t) = self.last_delivered.lock() {
            *t = Instant::now();
        }
        self.consecutive_drops.store(0, Ordering::Relaxed);
        self.timeout_fired.store(false, Ordering::Relaxed);
    }
}

/// A registered subscription. Holds the sender the cascade writes to
/// plus bookkeeping for timeout escalation.
struct Subscription {
    id: SubscriberId,
    cadence: Duration,
    sender: SyncSender<Arc<MetricSet>>,
    state: Arc<SubscriptionState>,
    opts: SubscriptionOpts,
    /// Handle to the dispatch thread. `Some` until unsubscribe or
    /// drop, when we take it to join.
    worker: Option<JoinHandle<()>>,
}

/// Component path key — derived from a `Labels` value by joining
/// every `(k, v)` pair into a stable string.
pub fn component_path_of(labels: &Labels) -> String {
    let mut parts: Vec<String> = labels.iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect();
    parts.sort_unstable();
    parts.join(",")
}

/// The cadence reporter: per-component + per-cadence accumulator,
/// store, and ring.
pub struct CadenceReporter {
    /// The cadence tree the scheduler is built against. Layers in
    /// order from smallest to largest.
    layers: Vec<CadenceLayer>,
    /// User-declared cadences (subset of `layers`, in declaration
    /// order). Used by consumers that want to enumerate
    /// human-visible cadences.
    declared: Cadences,
    /// `(component_path, layer_index) → window` map. Layer index
    /// matches `self.layers`.
    windows: RwLock<HashMap<(String, usize), CadenceWindow>>,
    /// Per-component-path original `Labels` value. Stored on first
    /// ingest so readers can iterate components without losing
    /// label structure to path serialization.
    component_labels_by_path: RwLock<HashMap<String, Labels>>,
    /// Active subscriptions, keyed by subscriber id. Each subscription
    /// has a dedicated dispatch thread and a bounded channel so a
    /// slow subscriber can never stall the cascade.
    subscriptions: Mutex<HashMap<SubscriberId, Subscription>>,
    /// Monotonic id generator for subscriptions.
    next_subscriber_id: AtomicU64,
    /// Run start instant — used by `session_lifetime` queries to
    /// build the result's `interval`.
    started_at: Instant,
}

impl CadenceReporter {
    /// Build a new reporter from a planned cadence tree.
    pub fn new(tree: CadenceTree) -> Self {
        let layers = tree.layers().to_vec();
        let declared = tree.declared().clone();
        Self {
            layers,
            declared,
            windows: RwLock::new(HashMap::new()),
            component_labels_by_path: RwLock::new(HashMap::new()),
            subscriptions: Mutex::new(HashMap::new()),
            next_subscriber_id: AtomicU64::new(1),
            started_at: Instant::now(),
        }
    }

    /// All layers (declared + hidden), smallest first.
    pub fn layers(&self) -> &[CadenceLayer] { &self.layers }

    /// User-declared cadences in original order.
    pub fn declared_cadences(&self) -> &Cadences { &self.declared }

    /// Wall-clock instant the reporter was constructed (run start).
    pub fn started_at(&self) -> Instant { self.started_at }

    /// Ingest a per-component snapshot at the smallest cadence.
    /// Cascades the close-then-promote chain per SRD-42, fanning out
    /// every closed snapshot to subscribers at that cadence via
    /// [`Self::subscribe`].
    pub fn ingest(&self, labels: &Labels, snapshot: MetricSet) {
        let path = component_path_of(labels);
        // Record the original Labels on first sight so readers can
        // enumerate components without losing structure.
        if let Ok(mut map) = self.component_labels_by_path.write() {
            map.entry(path.clone()).or_insert_with(|| labels.clone());
        }

        // Collect closed snapshots per cadence as we cascade, then
        // release the windows lock before fanning out — dispatch
        // touches subscriptions under a separate lock.
        let mut closed_by_cadence: Vec<(Duration, Arc<MetricSet>)> = Vec::new();
        {
            let mut windows = self.windows.write()
                .unwrap_or_else(|e| e.into_inner());

            let smallest_idx = 0usize;
            let mut to_propagate: Option<(usize, Arc<MetricSet>)> = None;

            let key = (path.clone(), smallest_idx);
            let entry = windows.entry(key)
                .or_insert_with(|| CadenceWindow::new(self.layers[smallest_idx].interval));
            if let Some(closed) = entry.ingest(snapshot) {
                closed_by_cadence.push((self.layers[smallest_idx].interval, closed.clone()));
                to_propagate = Some((smallest_idx + 1, closed));
            }

            while let Some((idx, snapshot_arc)) = to_propagate.take() {
                if idx >= self.layers.len() {
                    break;
                }
                let key = (path.clone(), idx);
                let entry = windows.entry(key)
                    .or_insert_with(|| CadenceWindow::new(self.layers[idx].interval));
                if let Some(closed) = entry.ingest((*snapshot_arc).clone()) {
                    closed_by_cadence.push((self.layers[idx].interval, closed.clone()));
                    to_propagate = Some((idx + 1, closed));
                }
            }
        }

        for (cadence, arc) in closed_by_cadence {
            self.fanout(cadence, arc);
        }
    }

    /// Register a push subscriber for the given cadence. The
    /// subscriber runs on a dedicated dispatch thread so slow
    /// receivers can never stall the cascade. Returns a
    /// [`SubscriberId`] for later [`Self::unsubscribe`].
    ///
    /// `cadence` MUST match one of this reporter's declared or
    /// hidden layer intervals — unknown cadences return an error.
    pub fn subscribe(
        self: &Arc<Self>,
        cadence: Duration,
        mut reporter: Box<dyn Reporter>,
        mut opts: SubscriptionOpts,
    ) -> Result<SubscriberId, SubscribeError> {
        if !self.layers.iter().any(|l| l.interval == cadence) {
            return Err(SubscribeError::UnknownCadence(cadence));
        }
        if opts.timeout.is_none() {
            opts.timeout = Some(cadence.saturating_mul(2));
        }

        let id = SubscriberId(self.next_subscriber_id.fetch_add(1, Ordering::Relaxed));
        let (sender, receiver) =
            std::sync::mpsc::sync_channel::<Arc<MetricSet>>(opts.channel_capacity);
        let state = Arc::new(SubscriptionState::new());
        let state_for_worker = state.clone();

        let worker = thread::Builder::new()
            .name(format!("nb-metrics-subscriber-{}", id.0))
            .spawn(move || {
                while let Ok(snapshot) = receiver.recv() {
                    reporter.report(&snapshot);
                    state_for_worker.mark_delivered();
                }
                reporter.flush();
            })
            .map_err(|e| SubscribeError::SpawnFailed(e.to_string()))?;

        let sub = Subscription {
            id,
            cadence,
            sender,
            state,
            opts,
            worker: Some(worker),
        };
        self.subscriptions.lock().unwrap_or_else(|e| e.into_inner()).insert(id, sub);
        Ok(id)
    }

    /// Drop a subscription. The dispatch thread drains any pending
    /// snapshots, calls `Reporter::flush`, and exits.
    pub fn unsubscribe(&self, id: SubscriberId) {
        let mut sub = {
            let mut map = self.subscriptions.lock()
                .unwrap_or_else(|e| e.into_inner());
            map.remove(&id)
        };
        if let Some(sub) = sub.as_mut() {
            // Dropping the sender by letting `sub` go out of scope
            // at the end of this block — but we want to join the
            // worker explicitly for a clean shutdown.
            let worker = sub.worker.take();
            // Drop the sender to close the channel.
            // (Need to drop before join or the worker never exits.)
            drop(std::mem::replace(
                &mut sub.sender,
                std::sync::mpsc::sync_channel::<Arc<MetricSet>>(1).0,
            ));
            if let Some(handle) = worker {
                let _ = handle.join();
            }
        }
    }

    /// Deliver `snapshot` to every subscription at the given cadence
    /// via non-blocking `try_send`. A subscriber whose channel is
    /// full has its consecutive-drop counter incremented; if the
    /// last successful delivery is older than its configured
    /// `timeout`, the subscription's `on_timeout` callback fires
    /// (once per stall; re-arms after the next successful delivery).
    fn fanout(&self, cadence: Duration, snapshot: Arc<MetricSet>) {
        let Ok(map) = self.subscriptions.lock() else { return };
        for sub in map.values() {
            if sub.cadence != cadence { continue; }
            match sub.sender.try_send(snapshot.clone()) {
                Ok(()) => {
                    // Delivery success is marked by the dispatch
                    // thread *after* `reporter.report()` returns
                    // (see `subscribe`). We don't reset counters
                    // here — a channel with capacity > 1 may buffer
                    // snapshots without the worker having processed
                    // them yet.
                }
                Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                    let drops = sub.state.consecutive_drops.fetch_add(1, Ordering::Relaxed) + 1;
                    if let Some(timeout) = sub.opts.timeout {
                        let last = sub.state.last_delivered.lock()
                            .map(|g| *g).unwrap_or_else(|_| Instant::now());
                        let age = last.elapsed();
                        if age >= timeout
                            && !sub.state.timeout_fired.swap(true, Ordering::Relaxed)
                        {
                            if let Some(cb) = &sub.opts.on_timeout {
                                cb(TimeoutEvent {
                                    subscriber_id: sub.id,
                                    cadence: sub.cadence,
                                    snapshot_age: age,
                                    consecutive_drops: drops,
                                });
                            } else {
                                crate::diag::warn(&format!(
                                    "metrics subscription {:?} at cadence {:?} has stalled for \
                                     {age:?} ({drops} consecutive drops)",
                                    sub.id, sub.cadence,
                                ));
                            }
                        }
                    }
                }
            }
        }
    }

    /// Full shutdown: flush trailing partials through the cascade,
    /// fan them out to every subscriber, then drop + join every
    /// subscriber worker so channels drain and sinks call their
    /// `Reporter::flush` on the way out. Callers that read from a
    /// reporter sink (e.g. SQLite for a summary report) MUST call
    /// this before reading — otherwise the last window of data is
    /// still in transit.
    pub fn shutdown(&self) {
        self.shutdown_flush();
        let mut subs = {
            let mut map = self.subscriptions.lock()
                .unwrap_or_else(|e| e.into_inner());
            std::mem::take(&mut *map)
        };
        for (_id, mut sub) in subs.drain() {
            let worker = sub.worker.take();
            drop(std::mem::replace(
                &mut sub.sender,
                std::sync::mpsc::sync_channel::<Arc<MetricSet>>(1).0,
            ));
            if let Some(handle) = worker {
                let _ = handle.join();
            }
        }
    }

    /// Close every window for the given path, promote partials up the
    /// cascade, and fan the resulting closed snapshots out to every
    /// subscriber via blocking send. Intended to be called at phase-
    /// end lifecycle boundaries — once a phase's labels will never
    /// receive another ingest, the window may as well publish now
    /// instead of idling until the next cadence tick (or, worse,
    /// until session shutdown).
    ///
    /// Blocking send is deliberate: phase-end is a lifecycle boundary
    /// that must be observable in the summary / downstream sinks. If
    /// a subscriber can't keep up, we'd rather back-pressure the
    /// phase executor than drop its data.
    pub fn close_path(&self, labels: &Labels) {
        let path = component_path_of(labels);
        let closed = self.force_close_path(&path);
        for (cadence, arc) in closed {
            self.fanout_blocking(cadence, arc);
        }
    }

    /// Force-close every prebuffer in cascade order at shutdown.
    /// Trailing partials are published with `interval < cadence`, and
    /// crucially — fanned out to every subscriber at their cadence so
    /// sinks like the SQLite reporter actually see the final window
    /// of activity.
    ///
    /// This is a belt-and-suspenders safety net: the primary delivery
    /// path is [`Self::close_path`] at phase completion. Any windows
    /// still holding data at shutdown (e.g. the top-level activity
    /// component, or a run aborted before phase-end ran) get
    /// published here. Delivery uses blocking `send` for the same
    /// reason as `close_path`.
    pub fn shutdown_flush(&self) {
        let paths: Vec<String> = {
            let windows = self.windows.read()
                .unwrap_or_else(|e| e.into_inner());
            let mut set = std::collections::HashSet::new();
            for (p, _) in windows.keys() {
                set.insert(p.clone());
            }
            set.into_iter().collect()
        };

        let mut closed_by_cadence: Vec<(Duration, Arc<MetricSet>)> = Vec::new();
        for path in &paths {
            closed_by_cadence.extend(self.force_close_path(path));
        }

        for (cadence, arc) in closed_by_cadence {
            self.fanout_blocking(cadence, arc);
        }
    }

    /// Internal helper: force-close every cascade layer's window for
    /// a single path, propagating partials up the cascade. Returns
    /// the closed snapshots in ascending cadence order so callers can
    /// fan them out after dropping the windows lock.
    ///
    /// Holding the windows write-lock during delivery would deadlock
    /// against subscriber dispatch threads that re-enter the reporter
    /// (e.g. to read prebuffer state), so delivery is *always* done
    /// by the caller after this returns.
    fn force_close_path(&self, path: &str) -> Vec<(Duration, Arc<MetricSet>)> {
        let mut closed: Vec<(Duration, Arc<MetricSet>)> = Vec::new();
        let mut windows = self.windows.write()
            .unwrap_or_else(|e| e.into_inner());

        let mut to_propagate: Option<(usize, Arc<MetricSet>)> = None;
        for idx in 0..self.layers.len() {
            let key = (path.to_string(), idx);
            if let Some((carry_idx, carry)) = to_propagate.take() {
                if carry_idx == idx {
                    let entry = windows.entry(key.clone())
                        .or_insert_with(|| CadenceWindow::new(self.layers[idx].interval));
                    let _ = entry.ingest((*carry).clone());
                }
            }
            if let Some(window) = windows.get_mut(&key) {
                if let Some(snap) = window.force_close() {
                    closed.push((self.layers[idx].interval, snap.clone()));
                    if idx + 1 < self.layers.len() {
                        to_propagate = Some((idx + 1, snap));
                    }
                }
            }
        }
        closed
    }

    /// Blocking variant of [`Self::fanout`] used by shutdown. Each
    /// subscriber receives the snapshot via `Sender::send`, which
    /// blocks until channel capacity is available — guaranteeing
    /// delivery as long as the subscriber is alive.
    fn fanout_blocking(&self, cadence: Duration, snapshot: Arc<MetricSet>) {
        let senders: Vec<std::sync::mpsc::SyncSender<Arc<MetricSet>>> = {
            let Ok(map) = self.subscriptions.lock() else { return };
            map.values()
                .filter(|sub| sub.cadence == cadence)
                .map(|sub| sub.sender.clone())
                .collect()
        };
        for sender in senders {
            let _ = sender.send(snapshot.clone());
        }
    }

    /// Latest closed snapshot for `(labels, cadence)`, if any.
    pub fn latest(&self, labels: &Labels, cadence: Duration) -> Option<Arc<MetricSet>> {
        let path = component_path_of(labels);
        let idx = self.layer_index(cadence)?;
        let windows = self.windows.read().ok()?;
        windows.get(&(path, idx)).and_then(|w| w.latest())
    }

    /// Clone of the in-flight prebuffer for `(labels, cadence)`.
    /// Used by `session_lifetime` to peek partials without
    /// disturbing the cascade.
    pub fn prebuffer(&self, labels: &Labels, cadence: Duration) -> Option<MetricSet> {
        let path = component_path_of(labels);
        let idx = self.layer_index(cadence)?;
        let windows = self.windows.read().ok()?;
        windows.get(&(path, idx)).and_then(|w| w.prebuffer_clone())
    }

    /// Read the past-snapshot ring for `(labels, cadence)`. Returns
    /// the snapshots oldest-first so callers can `rev().take(n)` to
    /// merge the most-recent N.
    pub fn ring(&self, labels: &Labels, cadence: Duration) -> Vec<Arc<MetricSet>> {
        let path = component_path_of(labels);
        let Some(idx) = self.layer_index(cadence) else { return Vec::new() };
        let Ok(windows) = self.windows.read() else { return Vec::new() };
        windows.get(&(path, idx))
            .map(|w| w.ring().cloned().collect())
            .unwrap_or_default()
    }

    /// All `(component_labels)` keys currently tracked. Used by
    /// consumers (summary report, exposition) to enumerate which
    /// components have data. Returns the original `Labels` recorded
    /// at the first ingest for each component path.
    pub fn component_labels(&self) -> Vec<Labels> {
        let Ok(map) = self.component_labels_by_path.read() else { return Vec::new() };
        map.values().cloned().collect()
    }

    fn layer_index(&self, cadence: Duration) -> Option<usize> {
        self.layers.iter().position(|l| l.interval == cadence)
    }
}

impl Drop for CadenceReporter {
    fn drop(&mut self) {
        // Drain subscriptions — dropping each sender closes the
        // channel so the dispatch thread exits cleanly and flushes
        // the reporter.
        let mut subs = self.subscriptions.get_mut()
            .map(std::mem::take)
            .unwrap_or_default();
        for (_id, mut sub) in subs.drain() {
            let worker = sub.worker.take();
            drop(std::mem::replace(
                &mut sub.sender,
                std::sync::mpsc::sync_channel::<Arc<MetricSet>>(1).0,
            ));
            if let Some(handle) = worker {
                let _ = handle.join();
            }
        }
    }
}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::MetricValue;

    fn counter_set(interval: Duration, value: u64) -> MetricSet {
        let mut s = MetricSet::new(interval);
        s.insert_counter("ops", Labels::default(), value, Instant::now());
        s
    }

    fn first_counter_total(snap: &MetricSet) -> u64 {
        let f = snap.family("ops").expect("ops family");
        let m = f.metrics().next().expect("series");
        match m.point().unwrap().value() {
            MetricValue::Counter(c) => c.total,
            _ => panic!("not a counter"),
        }
    }

    #[test]
    fn ingest_promotes_at_smallest_cadence_boundary() {
        let cadences = Cadences::new(&[
            Duration::from_millis(100),
            Duration::from_millis(400),
        ]).unwrap();
        let tree = CadenceTree::plan_default(cadences);
        let reporter = CadenceReporter::new(tree);
        let labels = Labels::of("phase", "load");

        // 4× 100ms snapshots @ 5 each → expect 100ms latest after each,
        // 400ms latest after the 4th.
        for _ in 0..4 {
            reporter.ingest(&labels, counter_set(Duration::from_millis(100), 5));
        }

        let latest_100 = reporter.latest(&labels, Duration::from_millis(100))
            .expect("100ms cadence should have a latest");
        assert_eq!(first_counter_total(&latest_100), 5);

        let latest_400 = reporter.latest(&labels, Duration::from_millis(400))
            .expect("400ms cadence should have promoted after 4 ticks");
        assert_eq!(first_counter_total(&latest_400), 20);
    }

    #[test]
    fn ring_caps_at_history_ring_cap() {
        let cadences = Cadences::new(&[Duration::from_millis(50)]).unwrap();
        let tree = CadenceTree::plan_default(cadences);
        let reporter = CadenceReporter::new(tree);
        let labels = Labels::of("phase", "x");

        for i in 0..(HISTORY_RING_CAP + 5) {
            reporter.ingest(&labels, counter_set(Duration::from_millis(50), (i as u64) + 1));
        }

        let ring = reporter.ring(&labels, Duration::from_millis(50));
        assert_eq!(ring.len(), HISTORY_RING_CAP);
        // Newest at the back: total should be HISTORY_RING_CAP + 5.
        let newest_total = first_counter_total(ring.last().unwrap());
        assert_eq!(newest_total, (HISTORY_RING_CAP as u64) + 5);
    }

    #[test]
    fn force_close_publishes_partial_at_shutdown() {
        let cadences = Cadences::new(&[Duration::from_millis(1000)]).unwrap();
        let tree = CadenceTree::plan_default(cadences);
        let reporter = CadenceReporter::new(tree);
        let labels = Labels::of("phase", "trail");

        // Only 200ms of data — won't naturally promote at 1000ms.
        reporter.ingest(&labels, counter_set(Duration::from_millis(200), 3));
        assert!(reporter.latest(&labels, Duration::from_millis(1000)).is_none());

        reporter.shutdown_flush();
        let partial = reporter.latest(&labels, Duration::from_millis(1000))
            .expect("shutdown must publish trailing partial");
        assert_eq!(first_counter_total(&partial), 3);
        assert!(partial.interval() < Duration::from_millis(1000),
            "partial interval must be < cadence: {:?}", partial.interval());
    }

    #[test]
    fn prebuffer_visible_for_in_flight_data() {
        let cadences = Cadences::new(&[Duration::from_millis(1000)]).unwrap();
        let tree = CadenceTree::plan_default(cadences);
        let reporter = CadenceReporter::new(tree);
        let labels = Labels::of("phase", "p");

        reporter.ingest(&labels, counter_set(Duration::from_millis(300), 7));
        reporter.ingest(&labels, counter_set(Duration::from_millis(300), 8));

        let pb = reporter.prebuffer(&labels, Duration::from_millis(1000))
            .expect("prebuffer present");
        assert_eq!(first_counter_total(&pb), 15);
        // Latest still empty — no full cadence elapsed.
        assert!(reporter.latest(&labels, Duration::from_millis(1000)).is_none());
    }

    // ---- subscription tests -----------------------------------

    struct CountingReporter {
        count: Arc<AtomicU64>,
    }
    impl crate::scheduler::Reporter for CountingReporter {
        fn report(&mut self, _snapshot: &MetricSet) {
            self.count.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn subscribe_receives_snapshots_on_dispatch_thread() {
        let cadences = Cadences::new(&[Duration::from_millis(100)]).unwrap();
        let reporter = Arc::new(CadenceReporter::new(CadenceTree::plan_default(cadences)));
        let labels = Labels::of("phase", "sub");

        let count = Arc::new(AtomicU64::new(0));
        let _id = reporter.subscribe(
            Duration::from_millis(100),
            Box::new(CountingReporter { count: count.clone() }),
            SubscriptionOpts::default(),
        ).unwrap();

        for _ in 0..5 {
            reporter.ingest(&labels, counter_set(Duration::from_millis(100), 1));
        }
        // Drain: wait up to 500ms for the dispatch thread to catch up.
        let deadline = Instant::now() + Duration::from_millis(500);
        while count.load(Ordering::Relaxed) < 5 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(count.load(Ordering::Relaxed), 5);
    }

    #[test]
    fn subscribe_rejects_unknown_cadence() {
        let cadences = Cadences::new(&[Duration::from_millis(100)]).unwrap();
        let reporter = Arc::new(CadenceReporter::new(CadenceTree::plan_default(cadences)));

        let err = reporter.subscribe(
            Duration::from_millis(250),
            Box::new(CountingReporter { count: Arc::new(AtomicU64::new(0)) }),
            SubscriptionOpts::default(),
        ).unwrap_err();
        assert!(matches!(err, SubscribeError::UnknownCadence(_)));
    }

    /// A reporter that blocks on a parking lot — simulates a slow
    /// HTTP sink.
    struct SlowReporter {
        block: Arc<AtomicBool>,
    }
    impl crate::scheduler::Reporter for SlowReporter {
        fn report(&mut self, _snapshot: &MetricSet) {
            while self.block.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(5));
            }
        }
    }

    #[test]
    fn stalled_subscriber_fires_timeout_without_blocking_cascade() {
        let cadences = Cadences::new(&[Duration::from_millis(50)]).unwrap();
        let reporter = Arc::new(CadenceReporter::new(CadenceTree::plan_default(cadences)));
        let labels = Labels::of("phase", "stall");

        let block = Arc::new(AtomicBool::new(true));
        let fired = Arc::new(AtomicU64::new(0));
        let fired_for_cb = fired.clone();

        let opts = SubscriptionOpts {
            channel_capacity: 1, // fill fast
            timeout: Some(Duration::from_millis(100)),
            on_timeout: Some(Arc::new(move |_ev| {
                fired_for_cb.fetch_add(1, Ordering::Relaxed);
            })),
        };
        let _id = reporter.subscribe(
            Duration::from_millis(50),
            Box::new(SlowReporter { block: block.clone() }),
            opts,
        ).unwrap();

        // Pump ingests — the cascade must keep running even though
        // the subscriber is blocked on its first snapshot.
        let start = Instant::now();
        for _ in 0..20 {
            reporter.ingest(&labels, counter_set(Duration::from_millis(50), 1));
            std::thread::sleep(Duration::from_millis(20));
        }
        // Cascade wall-clock should be roughly 20 * 20ms = 400ms; if
        // the subscriber were synchronous it would be >20 * cadence.
        assert!(start.elapsed() < Duration::from_secs(2),
            "cascade took {:?} — subscriber must have blocked it", start.elapsed());
        // Timeout callback should have fired at least once.
        assert!(fired.load(Ordering::Relaxed) >= 1,
            "expected timeout callback to fire; got {}", fired.load(Ordering::Relaxed));

        // Unblock so the worker can drain before shutdown.
        block.store(false, Ordering::Relaxed);
    }

    #[test]
    fn unsubscribe_stops_delivery() {
        let cadences = Cadences::new(&[Duration::from_millis(50)]).unwrap();
        let reporter = Arc::new(CadenceReporter::new(CadenceTree::plan_default(cadences)));
        let labels = Labels::of("phase", "unsub");

        let count = Arc::new(AtomicU64::new(0));
        let id = reporter.subscribe(
            Duration::from_millis(50),
            Box::new(CountingReporter { count: count.clone() }),
            SubscriptionOpts::default(),
        ).unwrap();

        reporter.ingest(&labels, counter_set(Duration::from_millis(50), 1));
        // Wait briefly for delivery.
        let deadline = Instant::now() + Duration::from_millis(200);
        while count.load(Ordering::Relaxed) < 1 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(count.load(Ordering::Relaxed), 1);

        reporter.unsubscribe(id);

        reporter.ingest(&labels, counter_set(Duration::from_millis(50), 1));
        reporter.ingest(&labels, counter_set(Duration::from_millis(50), 1));
        std::thread::sleep(Duration::from_millis(100));
        // Still only 1 — unsubscribe dropped the sender.
        assert_eq!(count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn separate_components_keyed_independently() {
        let cadences = Cadences::new(&[Duration::from_millis(100)]).unwrap();
        let tree = CadenceTree::plan_default(cadences);
        let reporter = CadenceReporter::new(tree);

        reporter.ingest(&Labels::of("phase", "a"), counter_set(Duration::from_millis(100), 1));
        reporter.ingest(&Labels::of("phase", "b"), counter_set(Duration::from_millis(100), 99));

        assert_eq!(first_counter_total(&reporter.latest(&Labels::of("phase", "a"), Duration::from_millis(100)).unwrap()), 1);
        assert_eq!(first_counter_total(&reporter.latest(&Labels::of("phase", "b"), Duration::from_millis(100)).unwrap()), 99);
    }
}
