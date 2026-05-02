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
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;

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

// =========================================================================
// Lock-free architecture (SRD-42 §"Lock-free consolidation lifecycle")
// =========================================================================
//
// The reporter follows the single-writer actor pattern:
//
// - All mutation of the windows map happens inside a single owner
//   thread that drains a non-blocking command channel.
// - Public `ingest`/`close_path` calls send commands and return
//   immediately (or, for `close_path`, optionally wait on an ack
//   channel — the deterministic-publish lifecycle boundary).
// - Reads (`latest`/`prebuffer`/`ring`/`component_labels`) hit an
//   `ArcSwap<ReaderState>` that the owner publishes after each
//   batch of commands. No lock; readers never block writers and
//   writers never block readers.
//
// The historic `windows: RwLock<HashMap<…>>` and
// `component_labels_by_path: RwLock<HashMap<…>>` are gone —
// those caused the post-fiber phase-transition hangs when a TUI
// reader's read guard happened to coincide with an ingest's
// write request.

/// Snapshot of one window's externally-observable state, published
/// atomically by the owner thread for read paths.
#[derive(Clone)]
struct WindowReaderView {
    latest: Option<Arc<MetricSet>>,
    prebuffer: Option<Arc<MetricSet>>,
    ring: Arc<Vec<Arc<MetricSet>>>,
}

impl Default for WindowReaderView {
    fn default() -> Self {
        Self {
            latest: None,
            prebuffer: None,
            ring: Arc::new(Vec::new()),
        }
    }
}

/// Atomically-published reader state. The owner thread builds a
/// fresh instance after each command batch and stores it via
/// [`ArcSwap`]. Readers do `state.load_full()` — one atomic load,
/// one Arc clone, no lock.
#[derive(Default)]
struct ReaderState {
    component_labels: HashMap<String, Labels>,
    windows: HashMap<(String, usize), Arc<WindowReaderView>>,
}

/// Commands sent to the owner thread.
///
/// Every variant carries an optional `ack` channel. Public APIs
/// that want deterministic-publish semantics (current callers
/// effectively all do — every ingest is at a phase-boundary, not
/// the cycle hot path) attach an ack and wait for it before
/// returning. The owner sends `()` after the corresponding state
/// is published to the reader-state ArcSwap.
enum Cmd {
    Ingest {
        path: String,
        labels: Labels,
        snapshot: MetricSet,
        ack: Option<crossbeam_channel::Sender<()>>,
    },
    ClosePath {
        path: String,
        ack: Option<crossbeam_channel::Sender<()>>,
    },
    /// Force-close every prebuffer and fan out to every subscriber.
    /// Used by `shutdown_flush`; sender waits on `ack`.
    ShutdownFlushAll {
        ack: crossbeam_channel::Sender<()>,
    },
    /// No-op barrier used by tests / explicit synchronization
    /// callers. The owner thread acks after publishing the
    /// reader-state, guaranteeing all prior FIFO commands have
    /// been processed.
    Barrier {
        ack: crossbeam_channel::Sender<()>,
    },
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
    /// Owner-thread command channel. Non-blocking from the writer's
    /// perspective (unbounded; allocations bounded by the ingest
    /// rate, which is one message per phase boundary in practice —
    /// not the cycle hot path).
    cmd_tx: crossbeam_channel::Sender<Cmd>,
    /// Atomically-published reader state. Owner publishes; readers
    /// `load_full`.
    state: Arc<ArcSwap<ReaderState>>,
    /// Active subscriptions, keyed by subscriber id. Each
    /// subscription has a dedicated dispatch thread and a bounded
    /// channel so a slow subscriber can never stall the cascade.
    /// Shared `Arc<Mutex<…>>` because the owner thread reads it
    /// during fanout and the public subscribe/unsubscribe API
    /// writes it from the caller thread. Subscribe/unsubscribe is
    /// not on the hot path — the brief mutex acquire there is
    /// fine.
    subscriptions: Arc<Mutex<HashMap<SubscriberId, Subscription>>>,
    /// Monotonic id generator for subscriptions.
    next_subscriber_id: AtomicU64,
    /// Run start instant — used by `session_lifetime` queries to
    /// build the result's `interval`.
    started_at: Instant,
    /// Owner thread join handle. Taken in `Drop`.
    owner_thread: Mutex<Option<JoinHandle<()>>>,
}

impl CadenceReporter {
    /// Build a new reporter from a planned cadence tree.
    ///
    /// Spawns the owner thread that processes ingest/close commands
    /// in serial order. Public `ingest`/`close_path` calls send via
    /// `cmd_tx` and never take any lock related to the windows map.
    pub fn new(tree: CadenceTree) -> Self {
        let layers = tree.layers().to_vec();
        let declared = tree.declared().clone();

        let state: Arc<ArcSwap<ReaderState>> =
            Arc::new(ArcSwap::from_pointee(ReaderState::default()));
        let subscriptions: Arc<Mutex<HashMap<SubscriberId, Subscription>>> =
            Arc::new(Mutex::new(HashMap::new()));

        // Unbounded sender so `ingest` never blocks. Backpressure
        // (if needed) is the user's responsibility — but with the
        // typical "one ingest per phase boundary" pattern the
        // queue depth stays trivially bounded.
        let (cmd_tx, cmd_rx) = crossbeam_channel::unbounded::<Cmd>();

        let owner_layers = layers.clone();
        let owner_state = state.clone();
        let owner_subs = subscriptions.clone();
        let handle = thread::Builder::new()
            .name("nbrs-cadence-consolidator".into())
            .spawn(move || {
                run_owner(cmd_rx, owner_layers, owner_state, owner_subs)
            })
            .expect("spawn cadence consolidator thread");

        Self {
            layers,
            declared,
            cmd_tx,
            state,
            subscriptions,
            next_subscriber_id: AtomicU64::new(1),
            started_at: Instant::now(),
            owner_thread: Mutex::new(Some(handle)),
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
    ///
    /// **Fire-and-forget, lock-free, never blocks**: enqueues an
    /// `Ingest` command and returns immediately. The owner thread
    /// processes commands in FIFO order, so any subsequent call
    /// (`close_path`, another `ingest`, etc.) will observe this
    /// command's effects in turn. `latest`/`prebuffer`/`ring` may
    /// race against an in-flight ingest — that's the trade-off
    /// for keeping the writer fully non-blocking on the tokio
    /// hot path. Tests that need synchronous semantics call
    /// [`Self::flush_for_tests`].
    ///
    /// CRITICAL: this method must NEVER block on a sync primitive.
    /// Earlier versions waited on a `crossbeam_channel::Receiver`
    /// for an ack — that blocks the OS thread (and therefore a
    /// tokio worker if called from async context), which led to
    /// runtime starvation and the post-fiber drain-loop hang.
    /// See `feedback_no_blocking_in_async.md` in the project
    /// memory.
    pub fn ingest(&self, labels: &Labels, snapshot: MetricSet) {
        let path = component_path_of(labels);
        // Unbounded sender — never blocks; only fails if the owner
        // thread has dropped (during shutdown). In that case
        // dropping the snapshot is correct behavior.
        let _ = self.cmd_tx.send(Cmd::Ingest {
            path,
            labels: labels.clone(),
            snapshot,
            ack: None,
        });
    }

    /// Test-only synchronous barrier. Sends a `Barrier` command
    /// and waits for the owner to ack — a guarantee that every
    /// prior FIFO command has been processed AND its effects are
    /// visible via [`Self::latest`] / [`Self::ring`] /
    /// [`Self::prebuffer`].
    ///
    /// **Production code MUST NOT call this** — it blocks the
    /// OS thread, which negates the lock-free design of
    /// [`Self::ingest`] / [`Self::close_path`]. Production
    /// callers don't need the synchronous barrier; the owner
    /// processes commands in FIFO order, so a follow-up call
    /// (next phase's ingest, shutdown_flush, etc.) sees prior
    /// effects naturally.
    pub fn flush_for_tests(&self) {
        let (ack_tx, ack_rx) = crossbeam_channel::bounded::<()>(1);
        let _ = self.cmd_tx.send(Cmd::Barrier { ack: ack_tx });
        let _ = ack_rx.recv();
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
            .name(format!("nbrs-metrics-subscriber-{}", id.0))
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

    /// Full shutdown: flush trailing partials through the cascade,
    /// fan them out to every subscriber, then drop + join every
    /// subscriber worker so channels drain and sinks call their
    /// `Reporter::flush` on the way out. Callers that read from a
    /// reporter sink (e.g. SQLite for a summary report) MUST call
    /// this before reading — otherwise the last window of data is
    /// still in transit.
    pub fn shutdown(&self) {
        // Wait for the owner to flush, then drop the cmd channel
        // so the owner exits.
        self.shutdown_flush();
        // Closing the cmd_tx side terminates the owner loop.
        // We can't move out of `&self`, so instead send a sentinel
        // by dropping the last cloned sender. Since we hold one
        // sender here, simply joining the thread relies on Drop.
        // Instead, have the owner observe channel closure: we
        // close by dropping all senders in the public Drop impl.
        // Here we just join the worker channels.
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
    /// subscriber. Intended to be called at phase-end lifecycle
    /// boundaries — once a phase's labels will never receive another
    /// ingest, the window may as well publish now instead of idling
    /// until the next cadence tick.
    ///
    /// **Fire-and-forget, lock-free, never blocks**: same contract
    /// as [`Self::ingest`]. The deterministic-publish guarantee at
    /// session shutdown is provided by [`Self::shutdown_flush`],
    /// which is the only call in the lifecycle that's allowed to
    /// block — and it's called once at the end of the run, not
    /// from any tokio worker on the hot path.
    pub fn close_path(&self, labels: &Labels) {
        let path = component_path_of(labels);
        let _ = self.cmd_tx.send(Cmd::ClosePath {
            path,
            ack: None,
        });
    }

    /// Force-close every prebuffer in cascade order at shutdown.
    /// Trailing partials are published with `interval < cadence`,
    /// and crucially — fanned out to every subscriber at their
    /// cadence so sinks like the SQLite reporter actually see the
    /// final window of activity.
    ///
    /// **Blocks the calling thread** until the owner thread has
    /// processed every queued command (every prior ingest /
    /// close_path) AND the final force-close has fanned out. This
    /// is the ONLY place in the public API that synchronously
    /// waits, and it's intended to be called once at session end
    /// from the tokio runtime's main task, AFTER all other tokio
    /// tasks have completed — so blocking the OS thread for the
    /// duration of the flush is safe.
    pub fn shutdown_flush(&self) {
        let (ack_tx, ack_rx) = crossbeam_channel::bounded::<()>(1);
        let _ = self.cmd_tx.send(Cmd::ShutdownFlushAll { ack: ack_tx });
        let _ = ack_rx.recv();
    }

    /// Latest closed snapshot for `(labels, cadence)`, if any.
    /// **Lock-free**: one atomic `ArcSwap::load_full` + a HashMap
    /// lookup against the published reader state.
    pub fn latest(&self, labels: &Labels, cadence: Duration) -> Option<Arc<MetricSet>> {
        let path = component_path_of(labels);
        let idx = self.layer_index(cadence)?;
        let state = self.state.load_full();
        state.windows.get(&(path, idx))?.latest.clone()
    }

    /// Clone of the in-flight prebuffer for `(labels, cadence)`.
    /// Used by `session_lifetime` to peek partials without
    /// disturbing the cascade.
    pub fn prebuffer(&self, labels: &Labels, cadence: Duration) -> Option<MetricSet> {
        let path = component_path_of(labels);
        let idx = self.layer_index(cadence)?;
        let state = self.state.load_full();
        state.windows.get(&(path, idx))?
            .prebuffer.as_ref()
            .map(|arc| (**arc).clone())
    }

    /// Read the past-snapshot ring for `(labels, cadence)`. Returns
    /// the snapshots oldest-first so callers can `rev().take(n)` to
    /// merge the most-recent N.
    pub fn ring(&self, labels: &Labels, cadence: Duration) -> Vec<Arc<MetricSet>> {
        let path = component_path_of(labels);
        let Some(idx) = self.layer_index(cadence) else { return Vec::new() };
        let state = self.state.load_full();
        state.windows.get(&(path, idx))
            .map(|w| (*w.ring).clone())
            .unwrap_or_default()
    }

    /// All `(component_labels)` keys currently tracked.
    pub fn component_labels(&self) -> Vec<Labels> {
        let state = self.state.load_full();
        state.component_labels.values().cloned().collect()
    }

    fn layer_index(&self, cadence: Duration) -> Option<usize> {
        self.layers.iter().position(|l| l.interval == cadence)
    }
}

impl Drop for CadenceReporter {
    fn drop(&mut self) {
        // Closing the cmd channel by dropping the sender (we can't
        // explicitly close it here since we're still holding a
        // clone via `self.cmd_tx`). The owner thread observes
        // the channel closure when its `recv()` returns Err.
        //
        // We replace `cmd_tx` with a freshly-disconnected channel's
        // sender (cheap, no remaining receivers) so the original
        // sender is dropped, dropping the channel's strong-sender
        // count to zero and waking the owner's blocking recv.
        let (dummy_tx, _dummy_rx) = crossbeam_channel::bounded::<Cmd>(1);
        let _ = std::mem::replace(&mut self.cmd_tx, dummy_tx);
        // _dummy_rx is dropped here, so the dummy_tx's recv side is
        // gone too — but this scope's `cmd_tx` is now disconnected,
        // which is fine: nothing else will send.

        // Join the owner thread.
        if let Ok(mut guard) = self.owner_thread.lock() {
            if let Some(handle) = guard.take() {
                let _ = handle.join();
            }
        }

        // Drain subscriptions — dropping each sender closes the
        // channel so the dispatch thread exits cleanly and flushes
        // the reporter.
        let mut subs = self.subscriptions.lock()
            .map(|mut g| std::mem::take(&mut *g))
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
// Owner thread — single writer of the windows map
// =========================================================================

/// Drive the cadence consolidation lifecycle from a single thread.
/// Drains the command channel, mutates the windows map exclusively
/// (no lock), and republishes a fresh `ReaderState` after each batch
/// of commands so readers always see a consistent view.
fn run_owner(
    cmd_rx: crossbeam_channel::Receiver<Cmd>,
    layers: Vec<CadenceLayer>,
    state_pub: Arc<ArcSwap<ReaderState>>,
    subscriptions: Arc<Mutex<HashMap<SubscriberId, Subscription>>>,
) {
    let mut windows: HashMap<(String, usize), CadenceWindow> = HashMap::new();
    let mut component_labels: HashMap<String, Labels> = HashMap::new();

    'outer: loop {
        // Block on the first message of a batch.
        let mut cmd = match cmd_rx.recv() {
            Ok(c) => c,
            Err(_) => break 'outer, // channel closed → shutdown
        };

        // Drain commands without blocking; publish once at the end
        // of the batch so readers see all changes atomically. Acks
        // are collected and fired AFTER the publish so callers
        // observe the state they requested.
        let mut acks: Vec<crossbeam_channel::Sender<()>> = Vec::new();
        loop {
            match cmd {
                Cmd::Ingest { path, labels, snapshot, ack } => {
                    component_labels.entry(path.clone()).or_insert(labels);
                    let closed_by_cadence = ingest_cascade(
                        &mut windows, &layers, path, snapshot,
                    );
                    fanout_owner(&subscriptions, &closed_by_cadence);
                    if let Some(a) = ack { acks.push(a); }
                }
                Cmd::ClosePath { path, ack } => {
                    let closed_by_cadence = close_path_cascade(
                        &mut windows, &layers, &path,
                    );
                    fanout_owner(&subscriptions, &closed_by_cadence);
                    if let Some(a) = ack { acks.push(a); }
                }
                Cmd::ShutdownFlushAll { ack } => {
                    let paths: Vec<String> = {
                        let mut set: std::collections::HashSet<String> =
                            std::collections::HashSet::new();
                        for (p, _) in windows.keys() { set.insert(p.clone()); }
                        set.into_iter().collect()
                    };
                    let mut all_closed = Vec::new();
                    for path in &paths {
                        all_closed.extend(close_path_cascade(
                            &mut windows, &layers, path,
                        ));
                    }
                    fanout_owner(&subscriptions, &all_closed);
                    acks.push(ack);
                }
                Cmd::Barrier { ack } => {
                    // No state mutation — just synchronization.
                    // The ack fires after the post-batch publish,
                    // so the caller sees the cumulative effect of
                    // every prior FIFO command.
                    acks.push(ack);
                }
            }

            cmd = match cmd_rx.try_recv() {
                Ok(c) => c,
                Err(_) => break,
            };
        }

        publish_reader_state(&windows, &component_labels, &state_pub);

        // Fire all acks AFTER publish so each waiter sees its own
        // command's effects in the reader state.
        for ack in acks.drain(..) {
            let _ = ack.send(());
        }
    }

    // Final publish so any remaining state is visible to late readers
    // (e.g., a shutdown query after the channel closed).
    publish_reader_state(&windows, &component_labels, &state_pub);
}

/// Apply a single ingest to the cascade. Returns the closed
/// snapshots in ascending-cadence order.
fn ingest_cascade(
    windows: &mut HashMap<(String, usize), CadenceWindow>,
    layers: &[CadenceLayer],
    path: String,
    snapshot: MetricSet,
) -> Vec<(Duration, Arc<MetricSet>)> {
    let mut closed_by_cadence: Vec<(Duration, Arc<MetricSet>)> = Vec::new();
    let smallest_idx = 0usize;
    let mut to_propagate: Option<(usize, Arc<MetricSet>)> = None;

    let key = (path.clone(), smallest_idx);
    let entry = windows.entry(key)
        .or_insert_with(|| CadenceWindow::new(layers[smallest_idx].interval));
    if let Some(closed) = entry.ingest(snapshot) {
        closed_by_cadence.push((layers[smallest_idx].interval, closed.clone()));
        to_propagate = Some((smallest_idx + 1, closed));
    }

    while let Some((idx, snapshot_arc)) = to_propagate.take() {
        if idx >= layers.len() { break; }
        let key = (path.clone(), idx);
        let entry = windows.entry(key)
            .or_insert_with(|| CadenceWindow::new(layers[idx].interval));
        if let Some(closed) = entry.ingest((*snapshot_arc).clone()) {
            closed_by_cadence.push((layers[idx].interval, closed.clone()));
            to_propagate = Some((idx + 1, closed));
        }
    }
    closed_by_cadence
}

/// Force-close every cascade layer's window for one path, promoting
/// partials. Returns closed snapshots in ascending-cadence order.
fn close_path_cascade(
    windows: &mut HashMap<(String, usize), CadenceWindow>,
    layers: &[CadenceLayer],
    path: &str,
) -> Vec<(Duration, Arc<MetricSet>)> {
    let mut closed: Vec<(Duration, Arc<MetricSet>)> = Vec::new();
    let mut to_propagate: Option<(usize, Arc<MetricSet>)> = None;
    for idx in 0..layers.len() {
        let key = (path.to_string(), idx);
        if let Some((carry_idx, carry)) = to_propagate.take() {
            if carry_idx == idx {
                let entry = windows.entry(key.clone())
                    .or_insert_with(|| CadenceWindow::new(layers[idx].interval));
                let _ = entry.ingest((*carry).clone());
            }
        }
        if let Some(window) = windows.get_mut(&key) {
            if let Some(snap) = window.force_close() {
                closed.push((layers[idx].interval, snap.clone()));
                if idx + 1 < layers.len() {
                    to_propagate = Some((idx + 1, snap));
                }
            }
        }
    }
    closed
}

/// Owner-side fanout: deliver each closed snapshot to subscribers at
/// its cadence via non-blocking `try_send`. The owner thread is the
/// sole writer of windows, so even the historical "blocking send at
/// phase-end" semantics don't need to be blocking here — the
/// deterministic publication boundary is the ack channel returned to
/// `close_path`'s caller.
fn fanout_owner(
    subscriptions: &Arc<Mutex<HashMap<SubscriberId, Subscription>>>,
    closed: &[(Duration, Arc<MetricSet>)],
) {
    if closed.is_empty() { return; }
    let Ok(map) = subscriptions.lock() else { return };
    for (cadence, snapshot) in closed {
        for sub in map.values() {
            if sub.cadence != *cadence { continue; }
            match sub.sender.try_send(snapshot.clone()) {
                Ok(()) => {}
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
}

/// Build a fresh `ReaderState` from the owner's private windows map
/// and publish it via `ArcSwap`. Readers see this as a single
/// atomic transition.
fn publish_reader_state(
    windows: &HashMap<(String, usize), CadenceWindow>,
    component_labels: &HashMap<String, Labels>,
    state_pub: &Arc<ArcSwap<ReaderState>>,
) {
    let mut win_views: HashMap<(String, usize), Arc<WindowReaderView>> =
        HashMap::with_capacity(windows.len());
    for (key, win) in windows.iter() {
        let view = WindowReaderView {
            latest: win.latest(),
            prebuffer: win.prebuffer_clone().map(Arc::new),
            ring: Arc::new(win.ring().cloned().collect()),
        };
        win_views.insert(key.clone(), Arc::new(view));
    }
    let new_state = ReaderState {
        component_labels: component_labels.clone(),
        windows: win_views,
    };
    state_pub.store(Arc::new(new_state));
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
        reporter.flush_for_tests();

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
        reporter.flush_for_tests();

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
        reporter.flush_for_tests();
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
        reporter.flush_for_tests();

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
        reporter.flush_for_tests();

        assert_eq!(first_counter_total(&reporter.latest(&Labels::of("phase", "a"), Duration::from_millis(100)).unwrap()), 1);
        assert_eq!(first_counter_total(&reporter.latest(&Labels::of("phase", "b"), Duration::from_millis(100)).unwrap()), 99);
    }
}
