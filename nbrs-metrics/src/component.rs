// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Runtime component tree for metrics ownership and dimensional labels.
//!
//! Every GK context layer (session, scenario, phase, dispenser) is a
//! [`Component`] in a parent-child tree. Labels inherit downward —
//! a phase component's effective labels include all ancestor labels.
//! Properties walk upward — a child can query a prop set on any ancestor.
//!
//! ## Instrument ownership (consolidated 2026-05)
//!
//! Each component carries a single `Vec<RegisteredInstrument>` — the
//! canonical store for every instrument hung on the node. Per-cycle
//! callers (op-dispenser wrappers, the activity executor) hold typed
//! `Arc<...>` references captured at registration time and never
//! look up by family name on the hot path. The [`Component::find_instrument`]
//! linear scan exists for diagnostics / introspection only.
//!
//! Dynamic instruments whose existence isn't known at init —
//! per-error-type counters allocated on first sighting — register
//! through the [`DynamicCapture`] hook installed via
//! [`Component::set_dynamic_capture`]. Capture walks the registry
//! first, then invokes the dynamic hook (if any).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::{Duration, Instant};

use crate::instruments::counter::Counter;
use crate::instruments::gauge::ValueGauge;
use crate::instruments::histogram::Histogram;
use crate::instruments::timer::Timer;
use crate::labels::Labels;
use crate::snapshot::MetricSet;

/// Lifecycle state of a component.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComponentState {
    /// Component is being initialized.
    Starting,
    /// Component is actively running. Instruments are captured.
    Running,
    /// Component is shutting down. Final flush pending.
    Stopping,
    /// Component is done. Instruments no longer captured.
    /// Cumulative view remains queryable in the store until detach.
    Stopped,
}

/// A typed instrument reference owned by a [`Component`].
///
/// One variant per kind matches the [`crate::snapshot::MetricType`]
/// axis (counter / gauge / histogram / timer). Capture dispatches
/// on the variant to call the right kind-specific snapshot method.
#[derive(Clone)]
pub enum InstrumentRef {
    Counter(Arc<Counter>),
    Gauge(Arc<ValueGauge>),
    Histogram(Arc<Histogram>),
    Timer(Arc<Timer>),
}

impl InstrumentRef {
    /// Labels recorded on the underlying instrument. The `name=...`
    /// pair (used by [`split_name_label`]) is preserved here so
    /// existing snapshots keep their shape.
    pub fn labels(&self) -> &Labels {
        match self {
            Self::Counter(c) => c.labels(),
            Self::Gauge(g) => g.labels(),
            Self::Histogram(h) => h.labels(),
            Self::Timer(t) => t.labels(),
        }
    }
}

/// One registry entry: the bare family name, optional OpenMetrics
/// unit, and the typed instrument.
///
/// `family` is the bare name as given to
/// [`Component::register_instrument`]. The `_<unit>` suffix per
/// SRD-40a §4.3 is applied at capture time (in
/// [`crate::snapshot::MetricSet::insert_metric_with_unit`]) so the
/// `metric_family.name` ends up suffixed and `metric_family.unit`
/// holds the unit. Unit `None` means the family is published as-is.
pub struct RegisteredInstrument {
    pub family: String,
    pub unit: Option<String>,
    pub instrument: InstrumentRef,
}

/// Hook for components that own a dynamically-extending set of
/// instruments — e.g. per-error-type counters allocated lazily.
///
/// The registry-side `Vec<RegisteredInstrument>` is the canonical
/// store for instruments known at init. Anything that needs to
/// register more instruments after `register_on` has run installs
/// a `DynamicCapture` via [`Component::set_dynamic_capture`]; the
/// component's capture path invokes it after walking the registry
/// so the dynamic samples ride the same cadence pipeline.
pub trait DynamicCapture: Send + Sync {
    /// Append the dynamic instruments' current samples into `out`.
    /// `drain` mirrors the registry walk: `true` for the cadence
    /// reporter's per-tick path (drain histograms, etc.); `false`
    /// for the non-mutating "current" path.
    fn capture_into(&self, out: &mut MetricSet, now: Instant, drain: bool);
}

/// A node in the runtime component tree.
///
/// Components form a hierarchy: Session → Scenario → Phase → Dispenser.
/// Each component carries its own labels, inheritable properties, and
/// its own instrument registry.
pub struct Component {
    /// This component's own labels (e.g., `phase="rampup"`).
    labels: Labels,
    /// Effective labels = all ancestor labels merged with own labels.
    /// Computed on [`attach`] and cached.
    effective_labels: Labels,
    /// Inheritable properties. Queried via walk-up to first ancestor
    /// that has the key set. Used for `hdr_digits`, `base_interval`, etc.
    props: HashMap<String, String>,
    /// Weak reference to parent for prop walk-up.
    parent: Option<Weak<RwLock<Component>>>,
    /// Child components. Populated at runtime as phases start.
    children: Vec<Arc<RwLock<Component>>>,
    /// Lifecycle state. Only RUNNING components are captured.
    state: ComponentState,
    /// Canonical instrument store for this component.
    ///
    /// Hot-path callers hold typed `Arc<...>` references obtained
    /// at registration time and never look up by name per cycle.
    /// Family-name lookup ([`find_instrument`]) is a linear scan and
    /// is reserved for diagnostics / introspection — see the
    /// [`find_instrument`] doc-comment.
    instruments: Vec<RegisteredInstrument>,
    /// Optional hook for instruments whose existence isn't known at
    /// init (per-error-type counters, etc.). Capture walks
    /// `instruments` first, then invokes this if present. See
    /// [`DynamicCapture`].
    dynamic_capture: Option<Arc<dyn DynamicCapture>>,
    /// Per-counter previous-snapshot baseline for delta computation.
    /// Keyed by counter labels' `identity_hash`. Populated lazily on
    /// each `capture_delta` call.
    prev_counters: Mutex<HashMap<u64, u64>>,
    /// Dynamic-controls declared on this component (SRD 23).
    /// Empty unless the code that instantiates the component
    /// explicitly declares a control via
    /// `component.controls().declare(...)`.
    controls: crate::controls::ControlRegistry,
}

impl Component {
    /// Create a new detached component with the given labels and props.
    ///
    /// The component starts in [`ComponentState::Starting`]. Call
    /// [`attach`] to wire it into the tree and compute effective labels.
    pub fn new(labels: Labels, props: HashMap<String, String>) -> Self {
        Self {
            effective_labels: labels.clone(),
            labels,
            props,
            parent: None,
            children: Vec::new(),
            state: ComponentState::Starting,
            instruments: Vec::new(),
            dynamic_capture: None,
            prev_counters: Mutex::new(HashMap::new()),
            controls: crate::controls::ControlRegistry::new(),
        }
    }

    /// Create a root component (session level). No parent.
    pub fn root(labels: Labels, props: HashMap<String, String>) -> Arc<RwLock<Self>> {
        let mut component = Self::new(labels, props);
        component.state = ComponentState::Running;
        Arc::new(RwLock::new(component))
    }

    /// This component's own labels (not including ancestors).
    pub fn labels(&self) -> &Labels {
        &self.labels
    }

    /// Effective labels: all ancestor labels merged with own labels.
    pub fn effective_labels(&self) -> &Labels {
        &self.effective_labels
    }

    /// Current lifecycle state.
    pub fn state(&self) -> ComponentState {
        self.state
    }

    /// Transition to a new lifecycle state.
    pub fn set_state(&mut self, state: ComponentState) {
        self.state = state;
    }

    /// Register an instrument under `family` on this component.
    ///
    /// Returns `Err` when `family` is already registered on this
    /// component — duplicate-family declarations on the same
    /// dimensional cell surface as a workload error here, before
    /// any cycle runs (SRD-40b §7.2). The component's
    /// `effective_labels` define the dimensional cell; the same
    /// family on a different component is a different cell and
    /// produces no collision.
    ///
    /// The collision check is a linear scan over the registry
    /// Vec — see the storage-shape note on [`Self::instruments`].
    pub fn register_instrument(
        &mut self,
        family: impl Into<String>,
        instrument: InstrumentRef,
    ) -> Result<(), String> {
        self.register_instrument_with_unit(family, None, instrument)
    }

    /// Variant of [`Self::register_instrument`] that records an
    /// OpenMetrics unit (`ms`, `bytes`, …).
    ///
    /// At capture time the unit drives the `_<unit>` suffix on
    /// `metric_family.name` and populates the `unit` column per
    /// SRD-40a §4.3 / SRD-40b §1. `None` is identical to the
    /// no-unit `register_instrument` path.
    pub fn register_instrument_with_unit(
        &mut self,
        family: impl Into<String>,
        unit: Option<String>,
        instrument: InstrumentRef,
    ) -> Result<(), String> {
        let family = family.into();
        if self.instruments.iter().any(|ri| ri.family == family) {
            return Err(format!(
                "duplicate family name on dimensionally-same metric \
                 context: {family}{}",
                self.effective_labels.to_prometheus(),
            ));
        }
        self.instruments.push(RegisteredInstrument {
            family,
            unit,
            instrument,
        });
        Ok(())
    }

    /// Read-only view of every registered instrument on this
    /// component, in insertion order. Walked by the cadence
    /// reporter on every tick.
    pub fn instruments(&self) -> &[RegisteredInstrument] {
        &self.instruments
    }

    /// Linear scan by family name — diagnostic / rare-path only.
    ///
    /// Hot-path callers must use the typed `Arc<...>` they
    /// captured at registration time. The Vec storage and linear
    /// scan are deliberate: registration is once-at-init,
    /// per-cycle access is pre-bound, and a HashMap probe would
    /// add API + Hash bound for ~40 ns saved once per workload load.
    ///
    /// If you find yourself reaching for this on a per-cycle code
    /// path, that's a design bug in the caller — pre-bind the
    /// `Arc<...>` you got from [`register_instrument`] instead.
    pub fn find_instrument(&self, family: &str) -> Option<&InstrumentRef> {
        self.instruments
            .iter()
            .find(|ri| ri.family == family)
            .map(|ri| &ri.instrument)
    }

    /// Install a [`DynamicCapture`] hook for instruments whose
    /// existence isn't known at init time. Replaces any prior
    /// installation. See the trait doc.
    pub fn set_dynamic_capture(&mut self, hook: Arc<dyn DynamicCapture>) {
        self.dynamic_capture = Some(hook);
    }

    /// Capture a delta snapshot covering `interval`.
    ///
    /// Resets internal delta accumulators (histograms drain;
    /// counter baselines advance). Called by the scheduler on
    /// every tick — the result feeds the cadence reporter's
    /// smallest-cadence accumulator.
    pub fn capture_delta(&self, interval: Duration) -> MetricSet {
        let now = Instant::now();
        let mut out = MetricSet::at(now, interval);
        self.capture_registry_into(&mut out, now, true);
        if let Some(hook) = &self.dynamic_capture {
            hook.capture_into(&mut out, now, true);
        }
        out
    }

    /// Capture a non-mutating snapshot of current state.
    ///
    /// - Counters: absolute totals (atomic load).
    /// - Gauges: current value.
    /// - Histograms / Timers: non-draining clone (`peek_snapshot`).
    ///
    /// Never touches internal accumulators — callers may invoke
    /// this arbitrarily often without perturbing the scheduler's
    /// per-tick cascade.
    pub fn capture_current(&self) -> MetricSet {
        let now = Instant::now();
        let mut out = MetricSet::at(now, Duration::ZERO);
        self.capture_registry_into(&mut out, now, false);
        if let Some(hook) = &self.dynamic_capture {
            hook.capture_into(&mut out, now, false);
        }
        out
    }

    /// Walk the registered instruments and emit their samples into
    /// `out`. `drain=true` drains histograms and advances counter
    /// baselines (delta semantics); `drain=false` peeks without
    /// disturbing reservoirs (current semantics).
    fn capture_registry_into(&self, out: &mut MetricSet, now: Instant, drain: bool) {
        for ri in &self.instruments {
            let family = ri.family.clone();
            let unit = ri.unit.as_deref();
            match &ri.instrument {
                InstrumentRef::Counter(c) => {
                    let lbl = strip_name_label(c.labels());
                    let absolute = c.get();
                    let value = if drain {
                        self.counter_delta(c.labels().identity_hash(), absolute)
                    } else {
                        absolute
                    };
                    out.insert_counter_with_unit(family, unit, lbl, value, now);
                }
                InstrumentRef::Gauge(g) => {
                    let lbl = strip_name_label(g.labels());
                    out.insert_gauge_with_unit(family, unit, lbl, g.get(), now);
                }
                InstrumentRef::Histogram(h) => {
                    let lbl = strip_name_label(h.labels());
                    let reservoir = if drain { h.snapshot() } else { h.peek_snapshot() };
                    out.insert_histogram_with_unit(family, unit, lbl, reservoir, now);
                }
                InstrumentRef::Timer(t) => {
                    let lbl = strip_name_label(t.labels());
                    let snap = if drain { t.snapshot() } else { t.peek_snapshot() };
                    out.insert_histogram_with_unit(family, unit, lbl, snap.histogram, now);
                }
            }
        }
    }

    /// Compute the delta for a counter: current minus previous,
    /// updating the stored baseline. Mirrors the sidecar that
    /// `ActivityMetrics` used to maintain. `identity_hash` keys
    /// the per-counter prev-value cell.
    fn counter_delta(&self, identity_hash: u64, current: u64) -> u64 {
        let mut prev = self
            .prev_counters
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let previous = prev.insert(identity_hash, current).unwrap_or(0);
        current.saturating_sub(previous)
    }

    /// Get a property by name, walking up the tree.
    ///
    /// Checks this component's props first, then each ancestor in
    /// order until found. Returns `None` if no ancestor has the key.
    pub fn get_prop(&self, name: &str) -> Option<String> {
        if let Some(value) = self.props.get(name) {
            return Some(value.clone());
        }
        if let Some(ref parent_weak) = self.parent {
            if let Some(parent_arc) = parent_weak.upgrade() {
                if let Ok(parent) = parent_arc.read() {
                    return parent.get_prop(name);
                }
            }
        }
        None
    }

    /// Set a property on this component.
    pub fn set_prop(&mut self, name: &str, value: &str) {
        self.props.insert(name.to_string(), value.to_string());
    }

    /// Number of child components.
    pub fn child_count(&self) -> usize {
        self.children.len()
    }

    /// Iterator over this component's direct children.
    pub fn children(&self) -> impl Iterator<Item = &Arc<RwLock<Component>>> {
        self.children.iter()
    }

    /// Borrow this component's dynamic-controls registry. Every
    /// component carries one; empty until something declares a
    /// control on it. See SRD 23.
    pub fn controls(&self) -> &crate::controls::ControlRegistry {
        &self.controls
    }

    /// Resolve a typed control by name, walking up the parent
    /// chain. This component's registry is checked first; then
    /// each ancestor in order. An ancestor declaration is only
    /// honored if its [`BranchScope`] is `Subtree` —
    /// [`BranchScope::Local`] declarations do not propagate to
    /// descendants. Returns `None` if no in-scope declaration
    /// matches the `<name, T>` pair.
    ///
    /// Mirrors [`Self::get_prop`] but for typed controls (SRD 23
    /// §"Branch-scoped and final controls").
    pub fn find_control_up<T>(&self, name: &str)
        -> Option<crate::controls::Control<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        if let Some(c) = self.controls.get::<T>(name) {
            return Some(c);
        }
        if let Some(ref parent_weak) = self.parent {
            if let Some(parent_arc) = parent_weak.upgrade() {
                if let Ok(parent) = parent_arc.read() {
                    return parent.find_control_up_subtree::<T>(name);
                }
            }
        }
        None
    }

    /// Ancestor-side recursion. Honors [`BranchScope::Subtree`]
    /// on an ancestor's declaration; otherwise keeps walking.
    fn find_control_up_subtree<T>(&self, name: &str)
        -> Option<crate::controls::Control<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        if let Some(erased) = self.controls.get_erased(name) {
            if erased.branch_scope() == crate::controls::BranchScope::Subtree {
                if let Some(c) = self.controls.get::<T>(name) {
                    return Some(c);
                }
            }
        }
        if let Some(ref parent_weak) = self.parent {
            if let Some(parent_arc) = parent_weak.upgrade() {
                if let Ok(parent) = parent_arc.read() {
                    return parent.find_control_up_subtree::<T>(name);
                }
            }
        }
        None
    }

    /// Erased variant of [`Self::find_control_up`] — returns
    /// just the enumeration handle, useful for diagnostics
    /// (`dryrun=controls`, TUI surfaces) that don't need the
    /// typed value.
    pub fn find_control_erased_up(&self, name: &str)
        -> Option<std::sync::Arc<dyn crate::controls::ErasedControl>>
    {
        if let Some(erased) = self.controls.get_erased(name) {
            return Some(erased);
        }
        if let Some(ref parent_weak) = self.parent {
            if let Some(parent_arc) = parent_weak.upgrade() {
                if let Ok(parent) = parent_arc.read() {
                    return parent.find_control_erased_up_subtree(name);
                }
            }
        }
        None
    }

    fn find_control_erased_up_subtree(&self, name: &str)
        -> Option<std::sync::Arc<dyn crate::controls::ErasedControl>>
    {
        if let Some(erased) = self.controls.get_erased(name) {
            if erased.branch_scope() == crate::controls::BranchScope::Subtree {
                return Some(erased);
            }
        }
        if let Some(ref parent_weak) = self.parent {
            if let Some(parent_arc) = parent_weak.upgrade() {
                if let Ok(parent) = parent_arc.read() {
                    return parent.find_control_erased_up_subtree(name);
                }
            }
        }
        None
    }

    /// Count of `Running`-state descendants (this component's
    /// children, grandchildren, …). Used by callers that want a
    /// structural "how many phases are in flight?" query against
    /// the live component tree — e.g. the TUI's Focus-LOD
    /// placeholder decision (SRD 62 §"Scenario done?").
    ///
    /// The component itself is NOT counted — the query is meant
    /// to traverse from an activity root into its phases.
    pub fn running_descendant_count(&self) -> usize {
        let mut count = 0;
        for child in &self.children {
            if let Ok(c) = child.read() {
                if c.state == ComponentState::Running { count += 1; }
                count += c.running_descendant_count();
            }
        }
        count
    }
}

/// Strip the legacy `name=...` label from an instrument's `Labels`,
/// returning the dimensional residual that goes onto the captured
/// `MetricFamily` row. The family name itself is provided
/// separately by [`RegisteredInstrument::family`] — historical
/// instruments embedded the family name as a `name=...` label, but
/// that pair must NOT appear on the metric's `LabelSet` (label-set
/// uniqueness within a family per OpenMetrics §4.5.1 would
/// otherwise be polluted).
fn strip_name_label(labels: &Labels) -> Labels {
    let mut out = Labels::default();
    for (k, v) in labels.iter() {
        if k != "name" {
            out = out.with(k, v);
        }
    }
    out
}

/// SRD-40b §11 / SRD-42 §"Component lifecycle: scope_close flush" —
/// fused teardown helper. Captures a final delta from this component's
/// instruments, fires the cadence reporter's `scope_close` (which
/// marks the partial, ingests, and closes the path), and transitions
/// the component to [`ComponentState::Stopped`].
///
/// **Only acts on `Running` components.** Components that are
/// `Starting`, `Stopping`, or already `Stopped` return without
/// touching the reporter — calling `scope_close` twice on the same
/// component is a no-op on the second call, matching SRD-40 §
/// component lifecycle.
///
/// Components with no registered instruments still close the path so
/// any in-flight prebuffer at that label set (e.g. ingests routed via
/// a sibling layer) flushes through the cascade.
pub fn scope_close(
    component: &Arc<RwLock<Component>>,
    cadence_reporter: &crate::cadence_reporter::CadenceReporter,
    interval: Duration,
) {
    // Read-capture the delta first, then take a write guard to
    // transition state. Read guard is released between the two so
    // the write doesn't deadlock.
    let (labels, delta) = {
        let g = component.read().unwrap_or_else(|e| e.into_inner());
        if g.state != ComponentState::Running {
            return;
        }
        let delta = g.capture_delta(interval);
        (g.effective_labels.clone(), delta)
    };

    cadence_reporter.scope_close(&labels, delta);

    // Transition to Stopped so a subsequent capture pass skips this
    // component (capture_tree only walks Running) and a second
    // scope_close call is a no-op.
    let mut g = component.write().unwrap_or_else(|e| e.into_inner());
    g.state = ComponentState::Stopped;
}

// =========================================================================
// Selector-based lookup (SRD 24)
// =========================================================================

/// Collect every component in a subtree whose `effective_labels`
/// match the selector. Order is pre-order DFS: root first, then
/// each child's subtree in insertion order.
///
/// Selector-based lookup takes an `Arc<RwLock<Component>>` root
/// (rather than `&Component`) because the results must also be
/// `Arc<RwLock<Component>>` — `find`'s Vec is a list of live
/// handles callers can then mutate, not a snapshot. Scoping a
/// query to a subtree is expressed by passing that subtree's
/// `Arc` as the root.
///
/// The query is read-only against every visited component: a
/// failed `read()` (e.g. poisoned lock) is treated as "this
/// subtree is opaque for this query" and silently skipped.
/// Collection order is stable across calls as long as no
/// components are attached or detached mid-traversal.
pub fn find(
    root: &Arc<RwLock<Component>>,
    sel: &crate::selector::Selector,
) -> Vec<Arc<RwLock<Component>>> {
    let mut out = Vec::new();
    find_into(root, sel, &mut out);
    out
}

fn find_into(
    root: &Arc<RwLock<Component>>,
    sel: &crate::selector::Selector,
    out: &mut Vec<Arc<RwLock<Component>>>,
) {
    let Ok(guard) = root.read() else { return };
    if sel.matches(&guard.effective_labels) {
        out.push(root.clone());
    }
    let children = guard.children.clone();
    drop(guard);
    for child in &children {
        find_into(child, sel, out);
    }
}

/// Expect exactly one match. Returns
/// [`crate::selector::LookupError::NotFound`] or
/// [`crate::selector::LookupError::Ambiguous`] otherwise.
///
/// Short-circuits on the second hit — the Vec-returning [`find`]
/// is preferable when all matches are wanted.
pub fn find_one(
    root: &Arc<RwLock<Component>>,
    sel: &crate::selector::Selector,
) -> Result<Arc<RwLock<Component>>, crate::selector::LookupError> {
    let mut first: Option<Arc<RwLock<Component>>> = None;
    let mut count = 0usize;
    find_one_walk(root, sel, &mut first, &mut count);
    match first {
        None => Err(crate::selector::LookupError::NotFound),
        Some(c) if count == 1 => Ok(c),
        Some(_) => Err(crate::selector::LookupError::Ambiguous { count }),
    }
}

fn find_one_walk(
    root: &Arc<RwLock<Component>>,
    sel: &crate::selector::Selector,
    first: &mut Option<Arc<RwLock<Component>>>,
    count: &mut usize,
) {
    let Ok(guard) = root.read() else { return };
    if sel.matches(&guard.effective_labels) {
        *count += 1;
        if first.is_none() {
            *first = Some(root.clone());
        }
        // Keep walking so `count` reflects the total — callers
        // rely on the Ambiguous count.
    }
    let children = guard.children.clone();
    drop(guard);
    for child in &children {
        find_one_walk(child, sel, first, count);
    }
}

/// True if any component in the subtree matches. Short-circuits
/// on the first hit.
pub fn any(
    root: &Arc<RwLock<Component>>,
    sel: &crate::selector::Selector,
) -> bool {
    any_walk(root, sel)
}

fn any_walk(
    root: &Arc<RwLock<Component>>,
    sel: &crate::selector::Selector,
) -> bool {
    let Ok(guard) = root.read() else { return false };
    if sel.matches(&guard.effective_labels) {
        return true;
    }
    let children = guard.children.clone();
    drop(guard);
    children.iter().any(|c| any_walk(c, sel))
}

/// Count every matching component in the subtree.
pub fn count(
    root: &Arc<RwLock<Component>>,
    sel: &crate::selector::Selector,
) -> usize {
    let mut n = 0usize;
    count_walk(root, sel, &mut n);
    n
}

fn count_walk(
    root: &Arc<RwLock<Component>>,
    sel: &crate::selector::Selector,
    n: &mut usize,
) {
    let Ok(guard) = root.read() else { return };
    if sel.matches(&guard.effective_labels) {
        *n += 1;
    }
    let children = guard.children.clone();
    drop(guard);
    for child in &children {
        count_walk(child, sel, n);
    }
}

/// Attach a child component to a parent.
///
/// Computes the child's effective labels by extending the parent's
/// effective labels with the child's own labels. Adds the child to
/// the parent's children list and sets the child's parent reference.
pub fn attach(
    parent: &Arc<RwLock<Component>>,
    child: &Arc<RwLock<Component>>,
) {
    let parent_effective = {
        let p = parent.read().unwrap_or_else(|e| e.into_inner());
        p.effective_labels.clone()
    };
    let mut c = child.write().unwrap_or_else(|e| e.into_inner());
    c.effective_labels = parent_effective.extend(&c.labels);
    c.parent = Some(Arc::downgrade(parent));
    drop(c);

    let mut p = parent.write().unwrap_or_else(|e| e.into_inner());
    p.children.push(child.clone());
}

/// Detach a child component from its parent.
///
/// Removes the child from the parent's children list and clears
/// the child's parent reference.
pub fn detach(
    parent: &Arc<RwLock<Component>>,
    child: &Arc<RwLock<Component>>,
) {
    let mut p = parent.write().unwrap_or_else(|e| e.into_inner());
    p.children.retain(|c| !Arc::ptr_eq(c, child));
    let mut c = child.write().unwrap_or_else(|e| e.into_inner());
    c.parent = None;
}

/// Walk the component tree and capture delta snapshots from all
/// RUNNING components.
///
/// Returns one `(effective_labels, snapshot)` pair per captured
/// component. Draining semantics — used by the scheduler tick.
pub fn capture_tree(
    root: &Arc<RwLock<Component>>,
    interval: Duration,
) -> Vec<(Labels, MetricSet)> {
    let mut results = Vec::new();
    capture_recursive(root, interval, &mut results);
    results
}

fn capture_recursive(
    node: &Arc<RwLock<Component>>,
    interval: Duration,
    results: &mut Vec<(Labels, MetricSet)>,
) {
    // Take a read guard, snapshot the values we need, drop the
    // guard before recursing so child locks don't nest on ours.
    let Ok(guard) = node.read() else { return };
    let state = guard.state;
    let effective_labels = guard.effective_labels.clone();
    let children = guard.children.clone();

    if state == ComponentState::Running {
        let snapshot = guard.capture_delta(interval);
        if !snapshot.is_empty() {
            results.push((effective_labels.clone(), snapshot));
        }
        // Reified control gauges — one per declared control that
        // has a numeric projection. Published at every tick so
        // they flow through the same sinks as regular metrics.
        let control_gauges = guard.controls.snapshot_gauges(
            &effective_labels,
            Instant::now(),
        );
        if !control_gauges.is_empty() {
            results.push((effective_labels, control_gauges));
        }
    }

    drop(guard);
    for child in &children {
        capture_recursive(child, interval, results);
    }
}

/// Non-mutating counterpart of [`capture_tree`]. Walks every RUNNING
/// component and returns absolute/peeked snapshots via
/// [`Component::capture_current`]. Safe to call arbitrarily often —
/// doesn't drain histograms or advance counter baselines.
pub fn capture_tree_current(
    root: &Arc<RwLock<Component>>,
) -> Vec<(Labels, MetricSet)> {
    let mut results = Vec::new();
    capture_current_recursive(root, &mut results);
    results
}

fn capture_current_recursive(
    node: &Arc<RwLock<Component>>,
    results: &mut Vec<(Labels, MetricSet)>,
) {
    let Ok(guard) = node.read() else { return };
    let state = guard.state;
    let effective_labels = guard.effective_labels.clone();
    let children = guard.children.clone();

    if state == ComponentState::Running {
        let snapshot = guard.capture_current();
        if !snapshot.is_empty() {
            results.push((effective_labels.clone(), snapshot));
        }
        let control_gauges = guard.controls.snapshot_gauges(
            &effective_labels,
            Instant::now(),
        );
        if !control_gauges.is_empty() {
            results.push((effective_labels, control_gauges));
        }
    }

    drop(guard);
    for child in &children {
        capture_current_recursive(child, results);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn new_counter(family: &str) -> Arc<Counter> {
        Arc::new(Counter::new(Labels::of("name", family)))
    }

    // ── SRD-40b §7.2: register_instrument duplicate detection ──

    #[test]
    fn register_instrument_first_time_succeeds() {
        let mut c = Component::new(Labels::empty(), HashMap::new());
        assert!(c.register_instrument(
            "recall_at_10",
            InstrumentRef::Counter(new_counter("recall_at_10")),
        ).is_ok());
        assert!(c.find_instrument("recall_at_10").is_some());
    }

    #[test]
    fn register_instrument_duplicate_errors() {
        let mut c = Component::new(Labels::empty(), HashMap::new());
        c.register_instrument(
            "recall_at_10",
            InstrumentRef::Counter(new_counter("recall_at_10")),
        ).unwrap();
        let err = c.register_instrument(
            "recall_at_10",
            InstrumentRef::Counter(new_counter("recall_at_10")),
        ).unwrap_err();
        assert!(err.contains("duplicate family"),
            "wrong message: {err}");
        assert!(err.contains("recall_at_10"),
            "family name not in error: {err}");
    }

    #[test]
    fn register_instrument_distinct_names_succeed() {
        let mut c = Component::new(Labels::empty(), HashMap::new());
        c.register_instrument("a", InstrumentRef::Counter(new_counter("a"))).unwrap();
        c.register_instrument("b", InstrumentRef::Counter(new_counter("b"))).unwrap();
        c.register_instrument("c", InstrumentRef::Counter(new_counter("c"))).unwrap();
        assert_eq!(c.instruments().len(), 3);
    }

    #[test]
    fn register_instrument_error_carries_label_context() {
        // SRD-40b §7's contract: the error message names the
        // dimensional cell so the workload author can see WHICH
        // op-template's label set produced the collision.
        let labels = Labels::of("phase", "pvs_query")
            .with("op", "select_ann");
        let mut c = Component::new(labels, HashMap::new());
        c.register_instrument(
            "overscan",
            InstrumentRef::Counter(new_counter("overscan")),
        ).unwrap();
        let err = c.register_instrument(
            "overscan",
            InstrumentRef::Counter(new_counter("overscan")),
        ).unwrap_err();
        assert!(err.contains("phase"), "missing phase label: {err}");
        assert!(err.contains("pvs_query"), "missing phase value: {err}");
        assert!(err.contains("op"), "missing op label: {err}");
    }

    #[test]
    fn register_instrument_isolated_per_component() {
        // Two components — registering the same family on each
        // is OK; dimensional uniqueness comes from the
        // component-tree structure, not a global registry.
        let mut a = Component::new(Labels::of("op", "foo"), HashMap::new());
        let mut b = Component::new(Labels::of("op", "bar"), HashMap::new());
        assert!(a.register_instrument(
            "overscan",
            InstrumentRef::Counter(new_counter("overscan")),
        ).is_ok());
        assert!(b.register_instrument(
            "overscan",
            InstrumentRef::Counter(new_counter("overscan")),
        ).is_ok());
    }

    // Test helper: register a counter that records a fixed value.
    fn install_counter(c: &mut Component, family: &str, value: u64) -> Arc<Counter> {
        let counter = new_counter(family);
        counter.inc_by(value);
        c.register_instrument(
            family,
            InstrumentRef::Counter(counter.clone()),
        ).unwrap();
        counter
    }

    // ── DynamicCapture hook ──

    struct DynamicCounter {
        inner: AtomicU64,
    }
    impl DynamicCapture for DynamicCounter {
        fn capture_into(&self, out: &mut MetricSet, now: Instant, _drain: bool) {
            let v = self.inner.load(Ordering::Relaxed);
            out.insert_counter("dynamic_counter", Labels::default(), v, now);
        }
    }

    #[test]
    fn dynamic_capture_runs_after_registry() {
        let mut c = Component::new(Labels::empty(), HashMap::new());
        install_counter(&mut c, "static_counter", 5);
        c.set_dynamic_capture(Arc::new(DynamicCounter {
            inner: AtomicU64::new(7),
        }));
        let snap = c.capture_current();
        assert!(snap.family("static_counter").is_some());
        assert!(snap.family("dynamic_counter").is_some());
    }

    #[test]
    fn component_attach_computes_effective_labels() {
        let root = Component::root(
            Labels::of("session", "s1"),
            HashMap::new(),
        );
        let child = Arc::new(RwLock::new(
            Component::new(Labels::of("phase", "rampup"), HashMap::new()),
        ));
        attach(&root, &child);

        let c = child.read().unwrap();
        let eff = c.effective_labels();
        assert_eq!(eff.get("session"), Some("s1"));
        assert_eq!(eff.get("phase"), Some("rampup"));
    }

    #[test]
    fn prop_walk_up_inheritance() {
        let mut root_props = HashMap::new();
        root_props.insert("hdr_digits".to_string(), "4".to_string());
        let root = Component::root(Labels::of("session", "s1"), root_props);

        let child = Arc::new(RwLock::new(
            Component::new(Labels::of("phase", "rampup"), HashMap::new()),
        ));
        attach(&root, &child);

        let c = child.read().unwrap();
        assert_eq!(c.get_prop("hdr_digits").as_deref(), Some("4"));
        assert_eq!(c.get_prop("nonexistent").as_deref(), None);
    }

    #[test]
    fn prop_child_overrides_parent() {
        let mut root_props = HashMap::new();
        root_props.insert("hdr_digits".to_string(), "3".to_string());
        let root = Component::root(Labels::of("session", "s1"), root_props);

        let mut child_props = HashMap::new();
        child_props.insert("hdr_digits".to_string(), "4".to_string());
        let child = Arc::new(RwLock::new(
            Component::new(Labels::of("phase", "rampup"), child_props),
        ));
        attach(&root, &child);

        let c = child.read().unwrap();
        assert_eq!(c.get_prop("hdr_digits").as_deref(), Some("4"));
    }

    #[test]
    fn detach_removes_child() {
        let root = Component::root(Labels::of("session", "s1"), HashMap::new());
        let child = Arc::new(RwLock::new(
            Component::new(Labels::of("phase", "rampup"), HashMap::new()),
        ));
        attach(&root, &child);
        assert_eq!(root.read().unwrap().child_count(), 1);

        detach(&root, &child);
        assert_eq!(root.read().unwrap().child_count(), 0);
    }

    #[test]
    fn capture_tree_collects_running_components() {
        let root = Component::root(Labels::of("session", "s1"), HashMap::new());

        // Running child with a registered counter.
        let child1 = Arc::new(RwLock::new(
            Component::new(Labels::of("phase", "load"), HashMap::new()),
        ));
        attach(&root, &child1);
        {
            let mut c = child1.write().unwrap();
            c.set_state(ComponentState::Running);
            install_counter(&mut c, "test_counter", 42);
        }

        // Stopped child with a registered counter — must NOT be captured.
        let child2 = Arc::new(RwLock::new(
            Component::new(Labels::of("phase", "done"), HashMap::new()),
        ));
        attach(&root, &child2);
        {
            let mut c = child2.write().unwrap();
            c.set_state(ComponentState::Stopped);
            install_counter(&mut c, "test_counter", 99);
        }

        let captured = capture_tree(&root, Duration::from_secs(1));
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].0.get("phase"), Some("load"));
    }

    #[test]
    fn capture_tree_walks_nested_children() {
        let root = Component::root(Labels::of("session", "s1"), HashMap::new());

        let scenario = Arc::new(RwLock::new(
            Component::new(Labels::of("scenario", "default"), HashMap::new()),
        ));
        attach(&root, &scenario);
        scenario.write().unwrap().set_state(ComponentState::Running);

        let phase = Arc::new(RwLock::new(
            Component::new(Labels::of("phase", "search"), HashMap::new()),
        ));
        attach(&scenario, &phase);
        {
            let mut p = phase.write().unwrap();
            p.set_state(ComponentState::Running);
            install_counter(&mut p, "test_counter", 10);
        }

        let captured = capture_tree(&root, Duration::from_secs(1));
        assert_eq!(captured.len(), 1);
        let eff = &captured[0].0;
        assert_eq!(eff.get("session"), Some("s1"));
        assert_eq!(eff.get("scenario"), Some("default"));
        assert_eq!(eff.get("phase"), Some("search"));
    }

    // =====================================================================
    // Selector-based lookup (SRD 24)
    // =====================================================================

    /// Fixture: a small session tree with two activity subtrees,
    /// each holding a handful of phases with distinct label shapes.
    fn sample_tree() -> Arc<RwLock<Component>> {
        let root = Component::root(
            Labels::empty()
                .with("type", "session")
                .with("session", "test-session"),
            HashMap::new(),
        );
        // Activity A: rampup + two ann_query phases at different k.
        let activity_a = Arc::new(RwLock::new(Component::new(
            Labels::empty().with("type", "activity").with("name", "a"),
            HashMap::new(),
        )));
        attach(&root, &activity_a);
        let rampup = Arc::new(RwLock::new(Component::new(
            Labels::empty().with("type", "phase").with("name", "rampup")
                .with("profile", "label_00"),
            HashMap::new(),
        )));
        attach(&activity_a, &rampup);
        for k in ["10", "100"] {
            let aq = Arc::new(RwLock::new(Component::new(
                Labels::empty().with("type", "phase").with("name", "ann_query")
                    .with("profile", "label_00").with("k", k),
                HashMap::new(),
            )));
            attach(&activity_a, &aq);
        }
        // Activity B: one phase with a different profile shape.
        let activity_b = Arc::new(RwLock::new(Component::new(
            Labels::empty().with("type", "activity").with("name", "b"),
            HashMap::new(),
        )));
        attach(&root, &activity_b);
        let teardown = Arc::new(RwLock::new(Component::new(
            Labels::empty().with("type", "phase").with("name", "teardown")
                .with("profile", "label_99"),
            HashMap::new(),
        )));
        attach(&activity_b, &teardown);
        root
    }

    #[test]
    fn find_returns_every_match_in_preorder() {
        let root = sample_tree();
        let sel = crate::selector::Selector::new().eq("type", "phase");
        let hits = find(&root, &sel);
        assert_eq!(hits.len(), 4);
        let names: Vec<String> = hits.iter()
            .filter_map(|c| c.read().ok().and_then(|g|
                g.effective_labels().get("name").map(|s| s.to_string())
            ))
            .collect();
        assert_eq!(
            names,
            vec!["rampup", "ann_query", "ann_query", "teardown"],
        );
    }

    #[test]
    fn find_with_empty_selector_returns_everything() {
        let root = sample_tree();
        let all = find(&root, &crate::selector::Selector::new());
        // session root + 2 activities + (rampup + 2 ann_query + teardown) = 7
        assert_eq!(all.len(), 7);
    }

    #[test]
    fn find_with_glob_and_eq_conjunction() {
        let root = sample_tree();
        let sel = crate::selector::Selector::new()
            .eq("type", "phase")
            .glob("name", "ann_*");
        let hits = find(&root, &sel);
        assert_eq!(hits.len(), 2);
        for h in &hits {
            let g = h.read().unwrap();
            assert_eq!(g.effective_labels().get("name"), Some("ann_query"));
        }
    }

    #[test]
    fn find_with_present_and_absent_clauses() {
        let root = sample_tree();
        let with_k = find(&root, &crate::selector::Selector::new()
            .eq("type", "phase").present("k"));
        assert_eq!(with_k.len(), 2);

        let without_k = find(&root, &crate::selector::Selector::new()
            .eq("type", "phase").absent("k"));
        assert_eq!(without_k.len(), 2);
    }

    #[test]
    fn find_one_exact_match() {
        let root = sample_tree();
        let sel = crate::selector::Selector::new()
            .eq("type", "phase").eq("name", "rampup");
        let c = find_one(&root, &sel).unwrap();
        assert_eq!(
            c.read().unwrap().effective_labels().get("name"),
            Some("rampup"),
        );
    }

    #[test]
    fn find_one_not_found() {
        let root = sample_tree();
        let sel = crate::selector::Selector::new().eq("name", "nowhere");
        match find_one(&root, &sel) {
            Err(crate::selector::LookupError::NotFound) => {}
            Err(other) => panic!("expected NotFound, got {other:?}"),
            Ok(_) => panic!("expected NotFound, got a match"),
        }
    }

    #[test]
    fn find_one_ambiguous_reports_count() {
        let root = sample_tree();
        let sel = crate::selector::Selector::new()
            .eq("type", "phase").eq("name", "ann_query");
        match find_one(&root, &sel) {
            Err(crate::selector::LookupError::Ambiguous { count }) => {
                assert_eq!(count, 2);
            }
            Err(other) => panic!("expected Ambiguous, got {other:?}"),
            Ok(_) => panic!("expected Ambiguous, got a single match"),
        }
    }

    #[test]
    fn any_short_circuits_on_first_hit() {
        let root = sample_tree();
        assert!(any(&root, &crate::selector::Selector::new().eq("name", "rampup")));
        assert!(!any(&root, &crate::selector::Selector::new().eq("name", "zzz")));
    }

    #[test]
    fn count_matches_len_of_find() {
        let root = sample_tree();
        let sel = crate::selector::Selector::new().eq("type", "phase");
        assert_eq!(count(&root, &sel), find(&root, &sel).len());
    }

    #[test]
    fn query_from_subtree_is_scoped() {
        let root = sample_tree();
        let activity_a = root.read().unwrap().children.first().unwrap().clone();
        let hits = find(&activity_a,
            &crate::selector::Selector::new().eq("type", "phase"));
        assert_eq!(hits.len(), 3);
    }

    #[test]
    fn effective_labels_include_inherited_session_label() {
        let root = sample_tree();
        let sel = crate::selector::Selector::new()
            .eq("session", "test-session")
            .eq("type", "phase");
        assert_eq!(count(&root, &sel), 4);
    }

    #[test]
    fn selector_macro_drives_find() {
        let root = sample_tree();
        let hits = find(&root, &crate::selector!(type = "phase", name = "teardown"));
        assert_eq!(hits.len(), 1);
    }

    // =====================================================================
    // Controls on components (SRD 23)
    // =====================================================================

    #[tokio::test]
    async fn controls_declare_and_lookup_through_component() {
        let root = Component::root(Labels::of("session", "s"), HashMap::new());
        {
            let guard = root.read().unwrap();
            guard.controls().declare(
                crate::controls::ControlBuilder::new("concurrency", 16u32).build(),
            );
        }
        let c: crate::controls::Control<u32> = {
            let guard = root.read().unwrap();
            guard.controls().get::<u32>("concurrency").unwrap()
        };
        c.set(32, crate::controls::ControlOrigin::Test).await.unwrap();
        let reread: crate::controls::Control<u32> = {
            let guard = root.read().unwrap();
            guard.controls().get::<u32>("concurrency").unwrap()
        };
        assert_eq!(reread.value(), 32);
    }

    #[tokio::test]
    async fn reified_control_gauges_flow_through_capture_tree() {
        let root = Component::root(
            Labels::empty().with("type", "session").with("session", "s1"),
            HashMap::new(),
        );
        let phase = Arc::new(RwLock::new(Component::new(
            Labels::empty().with("type", "phase").with("name", "rampup"),
            HashMap::new(),
        )));
        attach(&root, &phase);
        {
            let p = phase.write().unwrap();
            p.controls().declare(
                crate::controls::ControlBuilder::new("concurrency", 8u32)
                    .reify_as_gauge(|v| Some(*v as f64))
                    .build(),
            );
        }
        phase.write().unwrap().set_state(ComponentState::Running);

        let c: crate::controls::Control<u32> = phase.read().unwrap()
            .controls().get::<u32>("concurrency").unwrap();
        c.set(64, crate::controls::ControlOrigin::Test).await.unwrap();

        let captured = capture_tree(&root, Duration::from_secs(1));
        let mut found_value: Option<f64> = None;
        for (labels, set) in &captured {
            if labels.get("name") != Some("rampup") { continue; }
            if let Some(fam) = set.family("control_concurrency") {
                if let Some(m) = fam.metrics().next() {
                    if let Some(p) = m.point() {
                        if let crate::snapshot::MetricValue::Gauge(g) = p.value() {
                            found_value = Some(g.value);
                        }
                    }
                    assert_eq!(m.labels().get("name"), Some("rampup"));
                    assert_eq!(m.labels().get("control"), Some("concurrency"));
                }
            }
        }
        assert_eq!(found_value, Some(64.0));

        let current = capture_tree_current(&root);
        let mut saw_via_current = false;
        for (_, set) in &current {
            if set.family("control_concurrency").is_some() {
                saw_via_current = true;
            }
        }
        assert!(saw_via_current);
    }

    #[test]
    fn dryrun_controls_enumeration_over_tree() {
        let root = sample_tree();
        let phase_hits = find(&root,
            &crate::selector::Selector::new().eq("type", "phase"));
        for (idx, phase) in phase_hits.iter().enumerate() {
            let guard = phase.read().unwrap();
            guard.controls().declare(
                crate::controls::ControlBuilder::new(
                    "concurrency",
                    (10 * (idx + 1)) as u32,
                ).build(),
            );
        }

        let mut entries: Vec<(String, String)> = Vec::new();
        for c in find(&root, &crate::selector::Selector::new()) {
            let guard = c.read().unwrap();
            let labels = guard.effective_labels().clone();
            for ctl in guard.controls().list() {
                entries.push((
                    format!(
                        "{}/{}",
                        labels.get("name").unwrap_or("-"),
                        ctl.name(),
                    ),
                    ctl.value_string(),
                ));
            }
        }

        assert_eq!(entries.len(), phase_hits.len());
        for (key, value) in &entries {
            assert!(key.ends_with("/concurrency"), "key = {key}");
            assert!(value.parse::<u32>().is_ok(), "value = {value}");
        }
    }

    // ---- Branch-scoped control walk-up (SRD 23) ------------------

    #[test]
    fn branch_scope_subtree_resolves_from_descendant() {
        use crate::controls::{BranchScope, ControlBuilder};
        let root = Component::root(
            Labels::empty().with("type", "session").with("session", "s1"),
            HashMap::new(),
        );
        let phase = Arc::new(RwLock::new(Component::new(
            Labels::empty().with("type", "phase").with("name", "rampup"),
            HashMap::new(),
        )));
        attach(&root, &phase);

        root.read().unwrap().controls().declare(
            ControlBuilder::new("hdr_sigdigs", 3u32)
                .branch_scope(BranchScope::Subtree)
                .build(),
        );

        let resolved = phase.read().unwrap()
            .find_control_up::<u32>("hdr_sigdigs");
        assert!(resolved.is_some(), "Subtree-scoped control should be visible to descendant");
        assert_eq!(resolved.unwrap().value(), 3u32);
    }

    #[test]
    fn branch_scope_local_does_not_leak_to_descendants() {
        use crate::controls::{BranchScope, ControlBuilder};
        let root = Component::root(
            Labels::empty().with("type", "session").with("session", "s1"),
            HashMap::new(),
        );
        let phase = Arc::new(RwLock::new(Component::new(
            Labels::empty().with("type", "phase").with("name", "rampup"),
            HashMap::new(),
        )));
        attach(&root, &phase);

        root.read().unwrap().controls().declare(
            ControlBuilder::new("private", 99u32)
                .branch_scope(BranchScope::Local)
                .build(),
        );

        let leaked = phase.read().unwrap()
            .find_control_up::<u32>("private");
        assert!(leaked.is_none(),
            "Local-scoped control must not be visible to descendants");
    }

    #[test]
    fn nearest_declaration_wins_during_walk_up() {
        use crate::controls::{BranchScope, ControlBuilder};
        let root = Component::root(
            Labels::empty().with("type", "session").with("session", "s1"),
            HashMap::new(),
        );
        let phase = Arc::new(RwLock::new(Component::new(
            Labels::empty().with("type", "phase").with("name", "rampup"),
            HashMap::new(),
        )));
        attach(&root, &phase);

        root.read().unwrap().controls().declare(
            ControlBuilder::new("hdr_sigdigs", 3u32)
                .branch_scope(BranchScope::Subtree)
                .build(),
        );
        phase.read().unwrap().controls().declare(
            ControlBuilder::new("hdr_sigdigs", 5u32).build(),
        );

        let v = phase.read().unwrap()
            .find_control_up::<u32>("hdr_sigdigs")
            .unwrap()
            .value();
        assert_eq!(v, 5u32, "phase override should win over session default");
    }

    // =====================================================================
    // SRD-40b §11 / SRD-42 §"Component lifecycle: scope_close flush"
    // =====================================================================

    #[test]
    fn component_scope_close_flushes_running_component_marks_partial_and_stops() {
        use crate::cadence::{Cadences, CadenceTree};
        use crate::cadence_reporter::CadenceReporter;

        let tree = CadenceTree::plan_default(
            Cadences::new(&[Duration::from_secs(1)]).unwrap(),
        );
        let reporter = CadenceReporter::new(tree);

        // Build a phase component with a registered counter holding N=42.
        let root = Component::root(Labels::of("session", "s1"), HashMap::new());
        let phase = Arc::new(RwLock::new(
            Component::new(Labels::of("phase", "short"), HashMap::new()),
        ));
        attach(&root, &phase);
        {
            let mut p = phase.write().unwrap();
            p.set_state(ComponentState::Running);
            install_counter(&mut p, "test_counter", 42);
        }

        scope_close(&phase, &reporter, Duration::from_millis(150));
        reporter.flush_for_tests();

        // Component is now Stopped — second call must be a no-op.
        assert_eq!(phase.read().unwrap().state(), ComponentState::Stopped);
        scope_close(&phase, &reporter, Duration::from_millis(150));
        reporter.flush_for_tests();

        let labels = phase.read().unwrap().effective_labels().clone();
        let latest = reporter.latest(&labels, Duration::from_secs(1))
            .expect("scope_close must publish the partial");
        assert!(latest.is_partial(), "snapshot must be marked partial");
        let f = latest.family("test_counter").expect("test_counter family present");
        let m = f.metrics().next().unwrap();
        match m.point().unwrap().value() {
            crate::snapshot::MetricValue::Counter(c) => assert_eq!(c.total, 42),
            v => panic!("expected counter, got {v:?}"),
        }
    }

    #[test]
    fn component_scope_close_skips_non_running_states() {
        use crate::cadence::{Cadences, CadenceTree};
        use crate::cadence_reporter::CadenceReporter;

        let tree = CadenceTree::plan_default(
            Cadences::new(&[Duration::from_secs(1)]).unwrap(),
        );
        let reporter = CadenceReporter::new(tree);

        let root = Component::root(Labels::of("session", "s1"), HashMap::new());
        let phase = Arc::new(RwLock::new(
            Component::new(Labels::of("phase", "starting"), HashMap::new()),
        ));
        attach(&root, &phase);
        assert_eq!(phase.read().unwrap().state(), ComponentState::Starting);
        {
            let mut p = phase.write().unwrap();
            install_counter(&mut p, "test_counter", 99);
        }

        scope_close(&phase, &reporter, Duration::from_millis(150));
        reporter.flush_for_tests();

        assert_eq!(phase.read().unwrap().state(), ComponentState::Starting);
        let labels = phase.read().unwrap().effective_labels().clone();
        assert!(reporter.latest(&labels, Duration::from_secs(1)).is_none(),
            "scope_close on a non-Running component must not publish");
    }
}
