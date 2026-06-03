// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Bridge between the algebra-layer [`KernelScope`] surface
//! and polydat's [`PolydatKernel`] scope-binding primitives.
//!
//! Implements `KernelScope` for a `PolydatKernelScope` wrapper that
//! holds a `(canonical, parent)` `PolydatKernel` pair. Each
//! `scope(coords)` call delegates to
//! [`PolydatKernel::for_iteration`] — the established polydat
//! primitive for "materialize a fresh per-iteration child of
//! `parent` based on `canonical` with these bindings."
//!
//! This is the load-bearing bridge for the PR 9 migration:
//! once `KernelScope for PolydatKernelScope` exists, the algebra-
//! layer [`ScopedKernelStream<PolydatKernelScope>`] can replace
//! the legacy `ComprehensionIter` everywhere nb-activity
//! drives comprehension dispatch.

use std::sync::Arc;

use crate::iteration::comprehension::strategies::{Tuple, TupleValue};
use crate::kernel::PolydatKernel;
use crate::ast::Value;

use super::instance::KernelScope;

/// Wrapper around a `(canonical, parent)` `PolydatKernel` pair that
/// implements [`KernelScope`].
///
/// - `canonical` is the comprehension's prototype kernel —
///   built once at scope-synthesis time, materialized fresh
///   per iteration via `for_iteration`.
/// - `parent` is the enclosing scope's kernel — provides the
///   outer scope chain that `materialize_subscope` wires
///   into every iteration's child.
///
/// `scope(coords)` converts the algebra-layer `Tuple` to a
/// polydat `[(String, Value)]` bindings slice and calls
/// `PolydatKernel::for_iteration(&canonical, &parent, &bindings)`.
#[derive(Debug, Clone)]
pub struct PolydatKernelScope {
    canonical: Arc<PolydatKernel>,
    parent: Arc<PolydatKernel>,
}

impl PolydatKernelScope {
    /// Construct the scope wrapper from the comprehension's
    /// canonical kernel and the enclosing parent kernel.
    pub fn new(canonical: Arc<PolydatKernel>, parent: Arc<PolydatKernel>) -> Self {
        Self { canonical, parent }
    }

    /// Access the canonical kernel — useful when the consumer
    /// needs to share metadata (input manifest, scope
    /// coordinates) without invoking `scope`.
    pub fn canonical(&self) -> &Arc<PolydatKernel> {
        &self.canonical
    }

    /// Access the parent kernel.
    pub fn parent(&self) -> &Arc<PolydatKernel> {
        &self.parent
    }
}

impl KernelScope for PolydatKernelScope {
    /// Each scope produces a fresh `Arc<PolydatKernel>` — the
    /// per-iteration child kernel. Consumers can clone the Arc
    /// cheaply if they need multiple references.
    type Scoped = Arc<PolydatKernel>;

    fn scope(&self, coords: &Tuple) -> Arc<PolydatKernel> {
        let bindings: Vec<(String, Value)> = coords
            .bindings
            .iter()
            .map(|(name, val)| (name.clone(), tuple_value_to_polydat_value(val)))
            .collect();
        PolydatKernel::for_iteration(&self.canonical, &self.parent, &bindings)
    }
}

/// Convert an algebra-layer [`TupleValue`] to a polydat
/// [`Value`].
///
/// The algebra layer's `TupleValue::I64` doesn't exist in
/// polydat's `Value`; integers there are unconditionally
/// `U64`. We bitcast `i64 as u64` — for the comprehension
/// use case (iteration coordinates, typically non-negative
/// integers), this preserves the bit pattern; consumers that
/// care about signedness should use `as i64` to recover.
pub fn tuple_value_to_polydat_value(val: &TupleValue) -> Value {
    match val {
        TupleValue::U64(n) => Value::U64(*n),
        TupleValue::I64(n) => Value::U64(*n as u64),
        TupleValue::F64(f) => Value::F64(*f),
        TupleValue::Str(s) => Value::Str(Arc::from(s.as_str())),
        TupleValue::Bool(b) => Value::Bool(*b),
    }
}

/// Reverse conversion — polydat `Value` to algebra-layer
/// `TupleValue`. Only the subset of `Value` variants that
/// have a corresponding `TupleValue` are converted; richer
/// `Value` variants (`Bytes`, `Json`, `Ext`, `Handle`,
/// `Vec*`, etc.) return `None` so the caller can handle
/// the unsupported case explicitly.
pub fn polydat_value_to_tuple_value(val: &Value) -> Option<TupleValue> {
    match val {
        Value::U64(n) => Some(TupleValue::U64(*n)),
        Value::F64(f) => Some(TupleValue::F64(*f)),
        Value::Bool(b) => Some(TupleValue::Bool(*b)),
        Value::Str(s) => Some(TupleValue::Str(s.to_string())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tuple_value_to_polydat_round_trips_u64() {
        let tv = TupleValue::U64(42);
        let pv = tuple_value_to_polydat_value(&tv);
        let back = polydat_value_to_tuple_value(&pv).unwrap();
        assert_eq!(tv, back);
    }

    #[test]
    fn tuple_value_to_polydat_round_trips_f64() {
        let tv = TupleValue::F64(3.14);
        let pv = tuple_value_to_polydat_value(&tv);
        let back = polydat_value_to_tuple_value(&pv).unwrap();
        assert_eq!(tv, back);
    }

    #[test]
    fn tuple_value_to_polydat_round_trips_string() {
        let tv = TupleValue::Str("hello".into());
        let pv = tuple_value_to_polydat_value(&tv);
        let back = polydat_value_to_tuple_value(&pv).unwrap();
        assert_eq!(tv, back);
    }

    #[test]
    fn tuple_value_to_polydat_round_trips_bool() {
        let tv = TupleValue::Bool(true);
        let pv = tuple_value_to_polydat_value(&tv);
        let back = polydat_value_to_tuple_value(&pv).unwrap();
        assert_eq!(tv, back);
    }

    #[test]
    fn i64_converts_via_bitcast() {
        let tv = TupleValue::I64(123);
        let pv = tuple_value_to_polydat_value(&tv);
        match pv {
            Value::U64(n) => assert_eq!(n, 123u64),
            other => panic!("expected U64, got {other:?}"),
        }
    }

    #[test]
    fn polydat_value_returns_none_for_unsupported_variants() {
        let pv = Value::Bytes(Arc::from(&b"abc"[..]));
        assert!(polydat_value_to_tuple_value(&pv).is_none());
    }
}
