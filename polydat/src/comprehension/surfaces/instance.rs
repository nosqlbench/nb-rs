// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `KernelScope` trait + `ScopedKernelInstance` — the
//! second-order surface's value type (spec §9.5).
//!
//! `KernelScope` is the algebra-layer abstraction over
//! "a thing that can be scoped to a coordinate tuple."
//! nb-rs's `GkKernel` implements it; tests can implement it
//! with a lightweight mock so the surfaces can be exercised
//! without pulling in the full GK runtime.

use crate::comprehension::strategies::Tuple;

/// A parent value that can be scoped to a coordinate tuple,
/// producing an instance of `Self::Scoped`. Spec §9.5.3 names
/// this the "one-shot scope function."
///
/// For polydat's GK kernels this is `bind_outer_scope` +
/// per-tuple `set_input` followed by `evaluate_inits`. For
/// tests it can be any type that derives a scoped value from
/// a tuple.
pub trait KernelScope {
    /// The scoped value produced by `scope`.
    type Scoped;

    /// Apply `coords` to this parent and produce a scoped
    /// instance. Pure function — same `(parent, coords)`
    /// always produces equivalent output.
    fn scope(&self, coords: &Tuple) -> Self::Scoped;
}

/// The result of scoping a parent kernel against a coord
/// tuple. Wraps the scoped value (`Scoped`) and the
/// originating coord tuple for traceability.
#[derive(Debug, Clone, PartialEq)]
pub struct ScopedKernelInstance<S> {
    pub coords: Tuple,
    pub scoped: S,
}

impl<S> ScopedKernelInstance<S> {
    pub fn new(coords: Tuple, scoped: S) -> Self {
        Self { coords, scoped }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::comprehension::strategies::TupleValue;

    /// Test mock: a parent kernel that records its name and,
    /// when scoped, returns a `MockScoped` carrying the parent
    /// name + the coord tuple.
    #[derive(Debug, Clone)]
    struct MockKernel {
        name: String,
    }

    #[derive(Debug, Clone, PartialEq)]
    struct MockScoped {
        parent_name: String,
        coords: Tuple,
    }

    impl KernelScope for MockKernel {
        type Scoped = MockScoped;
        fn scope(&self, coords: &Tuple) -> MockScoped {
            MockScoped {
                parent_name: self.name.clone(),
                coords: coords.clone(),
            }
        }
    }

    #[test]
    fn mock_kernel_scope() {
        let parent = MockKernel { name: "phase_x".into() };
        let coords = Tuple::new().with("k", TupleValue::I64(42));
        let scoped = parent.scope(&coords);
        assert_eq!(scoped.parent_name, "phase_x");
        assert_eq!(scoped.coords.bindings[0].0, "k");
    }

    #[test]
    fn scoped_instance_construction() {
        let coords = Tuple::new().with("limit", TupleValue::I64(100));
        let instance = ScopedKernelInstance::new(coords.clone(), "scoped_value".to_string());
        assert_eq!(instance.coords, coords);
        assert_eq!(instance.scoped, "scoped_value");
    }
}
