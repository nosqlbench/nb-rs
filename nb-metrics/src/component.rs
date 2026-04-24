// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Runtime component tree for metrics ownership and dimensional labels.
//!
//! Every GK context layer (session, scenario, phase, dispenser) is a
//! [`Component`] in a parent-child tree. Labels inherit downward —
//! a phase component's effective labels include all ancestor labels.
//! Properties walk upward — a child can query a prop set on any ancestor.
//!
//! Instruments hang off components via the [`InstrumentSet`] trait.
//! The scheduler walks the tree to capture delta snapshots from all
//! RUNNING components.

use std::collections::HashMap;
use std::sync::{Arc, RwLock, Weak};
use std::time::Duration;

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

/// A set of instruments that can produce snapshots of their
/// current state.
///
/// Implemented by `ActivityMetrics` in nb-activity. The component
/// tree does not know about specific instrument types — it only
/// asks for a [`MetricSet`].
///
/// Two read modes — one draining, one not — because the scheduler's
/// cascade coalesce needs delta semantics to feed the
/// [`crate::cadence_reporter::CadenceReporter`] correctly, while
/// interactive pull readers (TUI, summary, GK metric nodes) need
/// non-mutating reads that never touch counter accumulators or
/// drain histogram reservoirs.
pub trait InstrumentSet: Send + Sync {
    /// Capture a delta snapshot covering the given interval.
    ///
    /// Resets internal delta accumulators (histograms drain,
    /// counter baselines advance). Called by the scheduler on every
    /// tick — the result feeds the cadence reporter's smallest-cadence
    /// accumulator.
    fn capture_delta(&self, interval: Duration) -> MetricSet;

    /// Capture a non-mutating snapshot of current state.
    ///
    /// - Counters: absolute totals (atomic load).
    /// - Gauges: current value.
    /// - Histograms: non-draining clone (`peek_snapshot`).
    ///
    /// Never touches internal accumulators — callers may invoke
    /// this arbitrarily often without perturbing the scheduler's
    /// per-tick cascade. Used by [`crate::metrics_query::MetricsQuery::now`]
    /// and the memoized [`crate::metrics_query::MetricHandle`] read
    /// path.
    fn capture_current(&self) -> MetricSet;
}

/// A node in the runtime component tree.
///
/// Components form a hierarchy: Session → Scenario → Phase → Dispenser.
/// Each component carries its own labels, inheritable properties, and
/// an optional instrument set for metrics capture.
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
    /// Instruments owned by this component. None for structural-only
    /// nodes (session, scenario) that don't directly record metrics.
    instruments: Option<Arc<dyn InstrumentSet>>,
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
            instruments: None,
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

    /// Set the instrument set for this component.
    pub fn set_instruments(&mut self, instruments: Arc<dyn InstrumentSet>) {
        self.instruments = Some(instruments);
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
/// RUNNING components that have instruments.
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
    let guard = node.read().ok();
    let (state, effective_labels, instruments, children, control_gauges) = match guard {
        Some(n) => {
            let controls_snap = n.controls.snapshot_gauges(
                &n.effective_labels,
                std::time::Instant::now(),
            );
            (
                n.state,
                n.effective_labels.clone(),
                n.instruments.clone(),
                n.children.clone(),
                controls_snap,
            )
        }
        None => return,
    };

    if state == ComponentState::Running {
        if let Some(ref instr) = instruments {
            let snapshot = instr.capture_delta(interval);
            if !snapshot.is_empty() {
                results.push((effective_labels.clone(), snapshot));
            }
        }
        // Reified control gauges — one per declared control that
        // has a numeric projection. Published at every tick so
        // they flow through the same sinks as regular metrics.
        if !control_gauges.is_empty() {
            results.push((effective_labels, control_gauges));
        }
    }

    for child in &children {
        capture_recursive(child, interval, results);
    }
}

/// Non-mutating counterpart of [`capture_tree`]. Walks every RUNNING
/// component and returns absolute/peeked snapshots via
/// [`InstrumentSet::capture_current`]. Safe to call arbitrarily
/// often — doesn't drain histograms or advance counter baselines.
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
    let guard = node.read().ok();
    let (state, effective_labels, instruments, children, control_gauges) = match guard {
        Some(n) => {
            let controls_snap = n.controls.snapshot_gauges(
                &n.effective_labels,
                std::time::Instant::now(),
            );
            (
                n.state,
                n.effective_labels.clone(),
                n.instruments.clone(),
                n.children.clone(),
                controls_snap,
            )
        }
        None => return,
    };

    if state == ComponentState::Running {
        if let Some(ref instr) = instruments {
            let snapshot = instr.capture_current();
            if !snapshot.is_empty() {
                results.push((effective_labels.clone(), snapshot));
            }
        }
        if !control_gauges.is_empty() {
            results.push((effective_labels, control_gauges));
        }
    }

    for child in &children {
        capture_current_recursive(child, results);
    }
}

/// Walk the component tree and collect `(effective_labels, instruments)`
/// pairs for every RUNNING component. Used by
/// [`crate::metrics_query::MetricsQuery::resolve`] to memoize the
/// set of instrument sets a handle pulls from — subsequent reads
/// skip the tree walk.
pub fn collect_running_instruments(
    root: &Arc<RwLock<Component>>,
) -> Vec<(Labels, Arc<dyn InstrumentSet>)> {
    let mut out = Vec::new();
    collect_recursive(root, &mut out);
    out
}

fn collect_recursive(
    node: &Arc<RwLock<Component>>,
    out: &mut Vec<(Labels, Arc<dyn InstrumentSet>)>,
) {
    let (state, effective_labels, instruments, children) = {
        let n = node.read().unwrap_or_else(|e| e.into_inner());
        (
            n.state,
            n.effective_labels.clone(),
            n.instruments.clone(),
            n.children.clone(),
        )
    };
    if state == ComponentState::Running {
        if let Some(instr) = instruments {
            out.push((effective_labels, instr));
        }
    }
    for child in &children {
        collect_recursive(child, out);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockInstruments {
        value: std::sync::atomic::AtomicU64,
    }

    impl InstrumentSet for MockInstruments {
        fn capture_delta(&self, interval: Duration) -> MetricSet {
            let v = self.value.load(std::sync::atomic::Ordering::Relaxed);
            let mut s = MetricSet::new(interval);
            s.insert_counter(
                "test_counter",
                Labels::default(),
                v,
                std::time::Instant::now(),
            );
            s
        }
        fn capture_current(&self) -> MetricSet {
            // Tests don't distinguish current vs delta — fine to
            // reuse. Real impls (ActivityMetrics) differentiate.
            self.capture_delta(Duration::ZERO)
        }
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

        // Running child with instruments
        let child1 = Arc::new(RwLock::new(
            Component::new(Labels::of("phase", "load"), HashMap::new()),
        ));
        attach(&root, &child1);
        {
            let mut c = child1.write().unwrap();
            c.set_state(ComponentState::Running);
            c.set_instruments(Arc::new(MockInstruments {
                value: std::sync::atomic::AtomicU64::new(42),
            }));
        }

        // Stopped child with instruments — should NOT be captured
        let child2 = Arc::new(RwLock::new(
            Component::new(Labels::of("phase", "done"), HashMap::new()),
        ));
        attach(&root, &child2);
        {
            let mut c = child2.write().unwrap();
            c.set_state(ComponentState::Stopped);
            c.set_instruments(Arc::new(MockInstruments {
                value: std::sync::atomic::AtomicU64::new(99),
            }));
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
            p.set_instruments(Arc::new(MockInstruments {
                value: std::sync::atomic::AtomicU64::new(10),
            }));
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
    /// Returns the session root + quick accessors to named nodes
    /// so individual tests don't rebuild.
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
        // Every component with type=phase — expect 4 across both activities.
        let sel = crate::selector::Selector::new().eq("type", "phase");
        let hits = find(&root, &sel);
        assert_eq!(hits.len(), 4);
        // Pre-order DFS: activity_a's rampup + ann_queries first,
        // then activity_b's teardown. Verify by reading names.
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
        // Phases that carry a `k` label (only ann_queries do).
        let with_k = find(&root, &crate::selector::Selector::new()
            .eq("type", "phase").present("k"));
        assert_eq!(with_k.len(), 2);

        // Phases that do NOT carry a `k` label.
        let without_k = find(&root, &crate::selector::Selector::new()
            .eq("type", "phase").absent("k"));
        assert_eq!(without_k.len(), 2); // rampup + teardown
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
        // `Component` doesn't implement Debug / PartialEq, so we
        // can't `assert_eq!(find_one(...), Err(...))` directly
        // — inspect the `Err` arm with a match instead.
        match find_one(&root, &sel) {
            Err(crate::selector::LookupError::NotFound) => {}
            Err(other) => panic!("expected NotFound, got {other:?}"),
            Ok(_) => panic!("expected NotFound, got a match"),
        }
    }

    #[test]
    fn find_one_ambiguous_reports_count() {
        let root = sample_tree();
        // Two ann_query phases share these labels.
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
        // Start the lookup from activity_a; activity_b's teardown
        // must NOT appear.
        let activity_a = root.read().unwrap().children.first().unwrap().clone();
        let hits = find(&activity_a,
            &crate::selector::Selector::new().eq("type", "phase"));
        assert_eq!(hits.len(), 3); // rampup + 2 ann_queries, no teardown
    }

    #[test]
    fn effective_labels_include_inherited_session_label() {
        // Selectors compose with parent-inherited labels because
        // they match on `effective_labels`, which is the merged
        // set.
        let root = sample_tree();
        // Filter by a label defined on the session root — every
        // descendant inherits it.
        let sel = crate::selector::Selector::new()
            .eq("session", "test-session")
            .eq("type", "phase");
        assert_eq!(count(&root, &sel), 4);
    }

    #[test]
    fn selector_macro_drives_find() {
        // End-to-end use of the `selector!` macro against the
        // tree, for the exact call-site ergonomics SRD 24 shows.
        let root = sample_tree();
        let hits = find(&root, &crate::selector!(type = "phase", name = "teardown"));
        assert_eq!(hits.len(), 1);
    }

    // =====================================================================
    // Controls on components (SRD 23)
    // =====================================================================

    /// Controls declared on a component are reachable via
    /// `component.controls()` — proving the registry is wired
    /// through the Component API, not just present as a field.
    #[tokio::test]
    async fn controls_declare_and_lookup_through_component() {
        let root = Component::root(Labels::of("session", "s"), HashMap::new());
        {
            let guard = root.read().unwrap();
            guard.controls().declare(
                crate::controls::ControlBuilder::new("concurrency", 16u32).build(),
            );
        }
        // Look up and mutate.
        let c: crate::controls::Control<u32> = {
            let guard = root.read().unwrap();
            guard.controls().get::<u32>("concurrency").unwrap()
        };
        c.set(32, crate::controls::ControlOrigin::Test).await.unwrap();
        // Re-reading from the component's registry yields the
        // same Arc-shared state.
        let reread: crate::controls::Control<u32> = {
            let guard = root.read().unwrap();
            guard.controls().get::<u32>("concurrency").unwrap()
        };
        assert_eq!(reread.value(), 32);
    }

    #[tokio::test]
    async fn reified_control_gauges_flow_through_capture_tree() {
        // Full integration: declare a reified control on a
        // running component → capture_tree → the captured
        // MetricSets include the control's gauge.
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
            // Mark the phase Running so capture walks it.
        }
        phase.write().unwrap().set_state(ComponentState::Running);

        // Mutate the control after the tree is set up.
        let c: crate::controls::Control<u32> = phase.read().unwrap()
            .controls().get::<u32>("concurrency").unwrap();
        c.set(64, crate::controls::ControlOrigin::Test).await.unwrap();

        // `capture_tree` should now surface a `control.concurrency`
        // gauge with the updated value and the phase's labels.
        let captured = capture_tree(&root, Duration::from_secs(1));
        let mut found_value: Option<f64> = None;
        for (labels, set) in &captured {
            if labels.get("name") != Some("rampup") { continue; }
            if let Some(fam) = set.family("control.concurrency") {
                if let Some(m) = fam.metrics().next() {
                    if let Some(p) = m.point() {
                        if let crate::snapshot::MetricValue::Gauge(g) = p.value() {
                            found_value = Some(g.value);
                        }
                    }
                    // The family's metric should carry both the
                    // inherited phase labels AND the `control=...`
                    // dimension the registry adds.
                    assert_eq!(m.labels().get("name"), Some("rampup"));
                    assert_eq!(m.labels().get("control"), Some("concurrency"));
                }
            }
        }
        assert_eq!(found_value, Some(64.0));

        // `capture_tree_current` (non-draining) must also surface it.
        let current = capture_tree_current(&root);
        let mut saw_via_current = false;
        for (_, set) in &current {
            if set.family("control.concurrency").is_some() {
                saw_via_current = true;
            }
        }
        assert!(saw_via_current);
    }

    /// `dryrun=controls`-style enumeration — walk the tree,
    /// ask each component for its declared controls, and render
    /// the set. Validates that the selector + lookup + controls
    /// combination covers the discovery UX SRD 23 calls out.
    #[test]
    fn dryrun_controls_enumeration_over_tree() {
        let root = sample_tree();
        // Declare a control on each phase — different value types
        // per phase so the erased rendering has to cope.
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

        // Enumerate: walk every component, list its controls.
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

        // Every declared control is reachable through the walk.
        assert_eq!(entries.len(), phase_hits.len());
        // Values round-trip through the erased render.
        for (key, value) in &entries {
            assert!(key.ends_with("/concurrency"), "key = {key}");
            assert!(value.parse::<u32>().is_ok(), "value = {value}");
        }
    }

    // ---- Branch-scoped control walk-up (SRD 23) ------------------

    #[test]
    fn branch_scope_subtree_resolves_from_descendant() {
        use crate::controls::{BranchScope, ControlBuilder};

        // session has a Subtree-scoped hdr_sigdigs; phase does
        // not declare it. A descendant read walks up and finds
        // the session's declaration.
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

        // Default BranchScope::Local — phase should NOT see it.
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

        // Session declares hdr_sigdigs=3 Subtree; phase
        // re-declares it Local=5. The descendant read should
        // return the phase's value (nearest wins).
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
}
