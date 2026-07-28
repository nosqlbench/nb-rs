// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Dimensional cells materialised from data.
//!
//! A metric whose series are one-per-instance of some dimension — a compaction
//! tier, a keyspace, a node — does NOT get there by attaching labels to an
//! instrument. The label set of a [`Component`] *is* the dimensional cell:
//!
//! > The component's `effective_labels` define the dimensional cell; the same
//! > family on a different component is a different cell and produces no
//! > collision.
//!
//! So one series per instance is one CHILD COMPONENT per instance, with the
//! family registered exactly once on each. The duplicate-family rejection in
//! [`Component::register_instrument`] is untouched and keeps catching what it
//! exists to catch: two different instruments claiming one name inside one
//! dimensional context.
//!
//! This registry is the resolve-or-create for those children. It is
//! deliberately generic — it knows about coordinates and components, not about
//! metrics — and it hangs off the component tree, so a cell's lifetime is its
//! parent's: cells created under a phase component die when that phase's
//! subtree is dropped.
//!
//! Coordinates are whole. A two-dimension coordinate resolves to ONE child
//! carrying both labels, never to nested children — nesting would impose an
//! arbitrary order on co-equal dimensions, and `a` inside `b` is a different
//! tree shape from `b` inside `a` for no reason the data supports.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use crate::component::Component;
use crate::labels::Labels;

/// Resolve-or-create map from a coordinate to the child component that
/// represents it. Interior-mutable so it is reachable through a read guard on
/// the parent, mirroring [`Component::controls`].
#[derive(Default)]
pub struct CellMap {
    /// Keyed by the coordinate's canonical rendering. `Labels::to_prometheus`
    /// is order-stable, so two spellings of one coordinate map to one cell.
    inner: Mutex<HashMap<String, Arc<RwLock<Component>>>>,
}

impl CellMap {
    pub fn new() -> Self {
        Self { inner: Mutex::new(HashMap::new()) }
    }

    /// The cell for `coord` under `parent`, creating and attaching it on first
    /// sight. Idempotent: the same coordinate yields the same component for as
    /// long as the parent lives, so a caller may resolve per cycle and get a
    /// registry write only once.
    ///
    /// `coord` carries only the dimension labels this cell adds. Inherited
    /// labels come from the parent via [`crate::component::attach`], which also
    /// enforces that no ancestor already owns these names.
    pub fn resolve(
        &self,
        parent: &Arc<RwLock<Component>>,
        coord: &Labels,
    ) -> Arc<RwLock<Component>> {
        let key = coord.to_prometheus();
        let mut map = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(existing) = map.get(&key) {
            return existing.clone();
        }
        let child = Arc::new(RwLock::new(Component::new(
            coord.clone(),
            HashMap::new(),
        )));
        crate::component::attach(parent, &child);
        map.insert(key, child.clone());
        child
    }

    /// Number of distinct coordinates materialised so far. For diagnostics and
    /// tests — there is deliberately no cap: how many series a dimension has is
    /// a modelling decision, and silently dropping data would be worse than
    /// having many.
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parent() -> Arc<RwLock<Component>> {
        Arc::new(RwLock::new(Component::new(
            Labels::of("phase", "finalize"),
            HashMap::new(),
        )))
    }

    /// The same coordinate must resolve to the SAME component, or a per-cycle
    /// resolve would attach a new child every cycle and re-register the family
    /// on each — the registry write this design exists to avoid.
    #[test]
    fn a_coordinate_resolves_to_one_stable_cell() {
        let p = parent();
        let cells = CellMap::new();
        let a = cells.resolve(&p, &Labels::of("tier", "24"));
        let b = cells.resolve(&p, &Labels::of("tier", "24"));
        assert!(Arc::ptr_eq(&a, &b), "one coordinate must be one cell");
        assert_eq!(cells.len(), 1);
    }

    /// Distinct values are distinct cells — which is what makes one family
    /// registrable on each without colliding.
    #[test]
    fn distinct_values_are_distinct_cells() {
        let p = parent();
        let cells = CellMap::new();
        let a = cells.resolve(&p, &Labels::of("tier", "24"));
        let b = cells.resolve(&p, &Labels::of("tier", "25"));
        assert!(!Arc::ptr_eq(&a, &b));
        assert_eq!(cells.len(), 2);

        // The registry invariant this whole design protects: the same family
        // registers cleanly on both, because they are different cells.
        for c in [&a, &b] {
            let g = c.write().unwrap();
            let mut g = g;
            g.register_instrument(
                "compaction_bytes_out",
                crate::component::InstrumentRef::Gauge(Arc::new(
                    crate::instruments::gauge::ValueGauge::new(Labels::default()),
                )),
            )
            .expect("same family on a different cell must not collide");
        }
    }

    /// A cell inherits its parent's labels and adds its own, so the emitted
    /// series carries the full coordinate.
    #[test]
    fn a_cell_inherits_the_parent_dimensions() {
        let p = parent();
        let cells = CellMap::new();
        let c = cells.resolve(&p, &Labels::of("tier", "24"));
        let eff = c.read().unwrap().effective_labels().to_prometheus();
        assert!(eff.contains("phase=") && eff.contains("tier="),
            "cell must carry inherited + own dimensions, got {eff}");
    }

    /// Two dimensions are ONE cell carrying both, not a nesting.
    #[test]
    fn a_multi_dimension_coordinate_is_a_single_child() {
        let p = parent();
        let cells = CellMap::new();
        let coord = Labels::of("tier", "24").with("keyspace", "baselines");
        let c = cells.resolve(&p, &coord);
        assert_eq!(c.read().unwrap().child_count(), 0,
            "a coordinate must not nest one dimension inside another");
        let eff = c.read().unwrap().effective_labels().to_prometheus();
        assert!(eff.contains("tier=") && eff.contains("keyspace="),
            "both dimensions belong to one cell, got {eff}");
        assert_eq!(p.read().unwrap().child_count(), 1);
    }
}
