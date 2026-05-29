// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `scope_once` — non-streamed surface (spec §9.5.3).
//!
//! Pure function: takes a single coordinate tuple and produces
//! a single scoped kernel instance. Used for replay,
//! debugging, and point queries where the coord tuple is
//! already known (typically from a checkpoint log or a UI
//! emission). Does not consult or advance any dispense
//! cursor.

use crate::iteration::comprehension::strategies::Tuple;

use super::instance::{KernelScope, ScopedKernelInstance};

/// Public entry — apply the parent's `scope` to the given
/// coords and wrap in a [`ScopedKernelInstance`]. Pure.
///
/// Available as a standalone function and as a method on
/// [`crate::iteration::comprehension::surfaces::CompiledComprehension::scope_once`];
/// neither form consults the program's dispense cursor.
pub fn scope_once<K: KernelScope>(
    parent: &K,
    coords: &Tuple,
) -> ScopedKernelInstance<K::Scoped> {
    scope_once_with(parent, coords)
}

/// Internal helper used by both the standalone function and
/// the `CompiledComprehension::scope_once` method. Kept as a
/// distinct symbol so call-site search clearly distinguishes
/// the two entry points.
pub(crate) fn scope_once_with<K: KernelScope>(
    parent: &K,
    coords: &Tuple,
) -> ScopedKernelInstance<K::Scoped> {
    let scoped = parent.scope(coords);
    ScopedKernelInstance::new(coords.clone(), scoped)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iteration::comprehension::strategies::TupleValue;

    #[derive(Debug, Clone)]
    struct MockKernel(String);

    impl KernelScope for MockKernel {
        type Scoped = (String, Tuple);
        fn scope(&self, coords: &Tuple) -> Self::Scoped {
            (self.0.clone(), coords.clone())
        }
    }

    #[test]
    fn scope_once_produces_instance() {
        let parent = MockKernel("parent_kernel".into());
        let coords = Tuple::new()
            .with("k", TupleValue::I64(7))
            .with("limit", TupleValue::I64(100));
        let instance = scope_once(&parent, &coords);
        assert_eq!(instance.coords.bindings.len(), 2);
        assert_eq!(instance.scoped.0, "parent_kernel");
        assert_eq!(instance.scoped.1, coords);
    }

    #[test]
    fn scope_once_does_not_modify_inputs() {
        let parent = MockKernel("p".into());
        let coords = Tuple::new().with("k", TupleValue::I64(1));
        let _ = scope_once(&parent, &coords);
        let _ = scope_once(&parent, &coords);
        // Both produce equivalent outputs (no internal state).
        let a = scope_once(&parent, &coords);
        let b = scope_once(&parent, &coords);
        assert_eq!(a.coords, b.coords);
    }
}
