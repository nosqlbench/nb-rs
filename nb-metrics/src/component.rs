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
        if let Some(ref instr) = instruments {
            let snapshot = instr.capture_delta(interval);
            if !snapshot.is_empty() {
                results.push((effective_labels, snapshot));
            }
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
        if let Some(ref instr) = instruments {
            let snapshot = instr.capture_current();
            if !snapshot.is_empty() {
                results.push((effective_labels, snapshot));
            }
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
}
