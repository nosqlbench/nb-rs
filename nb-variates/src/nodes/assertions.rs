// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Type and value assertion nodes.
//!
//! GK's runtime contract: node `eval` trusts its inputs. Bad input
//! panics, by design, because the hot path stays branch-free. The
//! "guarded" version of a node that would otherwise panic is built
//! as an *assembly* of two functions — the original node, and an
//! assertion node spliced in front of one of its inputs (SRD 15
//! §"Type and Value Assertion Nodes"). The assertion runs the
//! check; the downstream node still trusts its inputs.
//!
//! Two families:
//!
//! * **Type assertions** — one per supported [`PortType`]. They
//!   confirm the runtime [`Value`] variant matches the static
//!   port type and pass it through. Useful when provenance can't
//!   prove the wire already carries the right type (dynamic JSON
//!   navigation, `Ext` unwraps, cross-adapter values).
//!
//! * **Value assertions** — one per `PortType`, parameterised by
//!   a [`ConstConstraint`]. Pass the value through if the
//!   constraint holds, otherwise panic with a structured message.
//!   The same vocabulary the const-constraint metadata uses on
//!   `ParamSpec` is reused on `Port` (SRD 15 §"Strict Wire Mode")
//!   and on these nodes.
//!
//! Auto-insertion is the compiler's job (M2 §"Strict Wire Mode")
//! — these nodes are also user-callable from GK source for ad-hoc
//! guards.

use crate::dsl::const_constraints::ConstConstraint;
use crate::node::{GkNode, NodeMeta, Port, PortType, Slot, Value};

// =========================================================================
// Type assertions: one per PortType
// =========================================================================

/// Pass-through guard that confirms the runtime value variant
/// matches a declared `PortType`. Panics on mismatch.
///
/// Constructed with [`assert_type_node`] from the compiler when
/// strict wire mode can't statically prove the source's runtime
/// variant. End users rarely instantiate these directly.
pub struct AssertType {
    meta: NodeMeta,
    expected: PortType,
}

impl AssertType {
    pub fn new(typ: PortType) -> Self {
        let name = match typ {
            PortType::U64 => "assert_u64",
            PortType::F64 => "assert_f64",
            PortType::Bool => "assert_bool",
            PortType::Str => "assert_str",
            PortType::Bytes => "assert_bytes",
            PortType::Json => "assert_json",
            PortType::U32 => "assert_u32",
            PortType::I32 => "assert_i32",
            PortType::I64 => "assert_i64",
            PortType::F32 => "assert_f32",
            PortType::Ext => "assert_ext",
        };
        Self {
            meta: NodeMeta {
                name: name.into(),
                outs: vec![Port::new("output", typ)],
                ins: vec![Slot::Wire(Port::new("input", typ))],
            },
            expected: typ,
        }
    }

    /// Returns the `PortType` this node asserts against.
    pub fn expected(&self) -> PortType {
        self.expected
    }
}

impl GkNode for AssertType {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let v = &inputs[0];
        if !value_matches(v, self.expected) {
            panic!(
                "{}: expected runtime value of type {:?}, got {:?}",
                self.meta.name, self.expected, v
            );
        }
        outputs[0] = v.clone();
    }
}

fn value_matches(v: &Value, typ: PortType) -> bool {
    match (v, typ) {
        (Value::U64(_), PortType::U64) => true,
        (Value::F64(_), PortType::F64) => true,
        (Value::Bool(_), PortType::Bool) => true,
        (Value::Str(_), PortType::Str) => true,
        (Value::Bytes(_), PortType::Bytes) => true,
        (Value::Json(_), PortType::Json) => true,
        // Narrow-int variants ride in the wider variant per the
        // PortType doc on `node.rs` — accept the natural carrier.
        (Value::U64(_), PortType::U32) => true,
        (Value::U64(_), PortType::I32) => true,
        (Value::U64(_), PortType::I64) => true,
        (Value::F64(_), PortType::F32) => true,
        // Ext is opaque; we accept any concrete reflection.
        (Value::Ext(_), PortType::Ext) => true,
        _ => false,
    }
}

// =========================================================================
// Value assertions: type + constraint pair
// =========================================================================

/// Runtime value-constraint guard. Holds a [`ConstConstraint`]
/// the value must satisfy each cycle. Panics with a structured
/// message on violation; passes the value through otherwise.
///
/// Constructed with [`assert_value_node`] from the compiler when
/// the source can't statically be proven to deliver a value
/// satisfying the sink's constraint. Reuses the same
/// `ConstConstraint` vocabulary the const-validator uses, so the
/// two layers speak one language.
pub struct AssertValue {
    meta: NodeMeta,
    typ: PortType,
    constraint: ConstConstraint,
}

impl AssertValue {
    pub fn new(typ: PortType, constraint: ConstConstraint) -> Self {
        let name = match (&typ, &constraint) {
            (PortType::U64, ConstConstraint::NonZeroU64) => "assert_u64_nonzero",
            (PortType::U64, ConstConstraint::RangeU64 { .. }) => "assert_u64_range",
            (PortType::U64, ConstConstraint::AllowedU64(_)) => "assert_u64_allowed",
            (PortType::F64, ConstConstraint::RangeF64 { .. }) => "assert_f64_range",
            (PortType::Str, ConstConstraint::NonEmptyStr) => "assert_str_non_empty",
            (PortType::Str, ConstConstraint::StrParser(_)) => "assert_str_parses",
            // Catch-all for combinations we haven't dedicated a
            // distinct DSL name to yet.
            _ => "assert_value",
        };
        Self {
            meta: NodeMeta {
                name: name.into(),
                outs: vec![Port::new("output", typ)],
                ins: vec![Slot::Wire(Port::new("input", typ))],
            },
            typ,
            constraint,
        }
    }

    pub fn constraint(&self) -> &ConstConstraint {
        &self.constraint
    }

    pub fn port_type(&self) -> PortType {
        self.typ
    }
}

impl GkNode for AssertValue {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        // Re-route the constraint check through `ConstConstraint::check`
        // by lifting the value into a `ConstArg` shaped tuple. Avoids
        // duplicating the per-variant logic between assembly and
        // runtime.
        let arg = match &inputs[0] {
            Value::U64(v) => crate::dsl::factory::ConstArg::Int(*v),
            Value::F64(v) => crate::dsl::factory::ConstArg::Float(*v),
            Value::Str(s) => crate::dsl::factory::ConstArg::Str(s.clone()),
            other => panic!(
                "{}: unsupported runtime value variant {:?}",
                self.meta.name, other
            ),
        };
        if let Err(msg) = self.constraint.check(&arg, "value") {
            panic!("{}: {msg}", self.meta.name);
        }
        outputs[0] = inputs[0].clone();
    }
}

// =========================================================================
// Helpers used by the compiler when auto-wiring assertions
// =========================================================================

/// Construct the right type assertion node for a given `PortType`.
pub fn assert_type_node(typ: PortType) -> Box<dyn GkNode> {
    Box::new(AssertType::new(typ))
}

/// Construct a value assertion node for the given (type, constraint) pair.
pub fn assert_value_node(typ: PortType, constraint: ConstConstraint) -> Box<dyn GkNode> {
    Box::new(AssertValue::new(typ, constraint))
}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assert_u64_passes_u64_through() {
        let node = AssertType::new(PortType::U64);
        let mut out = [Value::None];
        node.eval(&[Value::U64(42)], &mut out);
        assert_eq!(out[0].as_u64(), 42);
    }

    #[test]
    #[should_panic(expected = "expected runtime value of type U64")]
    fn assert_u64_panics_on_string() {
        let node = AssertType::new(PortType::U64);
        let mut out = [Value::None];
        node.eval(&[Value::Str("not a number".into())], &mut out);
    }

    #[test]
    fn assert_value_nonzero_passes_nonzero() {
        let node = AssertValue::new(PortType::U64, ConstConstraint::NonZeroU64);
        let mut out = [Value::None];
        node.eval(&[Value::U64(7)], &mut out);
        assert_eq!(out[0].as_u64(), 7);
    }

    #[test]
    #[should_panic(expected = "must be non-zero")]
    fn assert_value_nonzero_panics_on_zero() {
        let node = AssertValue::new(PortType::U64, ConstConstraint::NonZeroU64);
        let mut out = [Value::None];
        node.eval(&[Value::U64(0)], &mut out);
    }

    #[test]
    fn assert_value_range_f64_passes_unit_interval() {
        let node = AssertValue::new(
            PortType::F64,
            ConstConstraint::RangeF64 { min: 0.0, max: 1.0 },
        );
        let mut out = [Value::None];
        node.eval(&[Value::F64(0.5)], &mut out);
        assert_eq!(out[0].as_f64(), 0.5);
    }

    #[test]
    #[should_panic(expected = "must be in [0, 1]")]
    fn assert_value_range_f64_panics_on_out_of_range() {
        let node = AssertValue::new(
            PortType::F64,
            ConstConstraint::RangeF64 { min: 0.0, max: 1.0 },
        );
        let mut out = [Value::None];
        node.eval(&[Value::F64(1.5)], &mut out);
    }
}
