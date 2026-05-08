// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `exactly_one_value` — explicit unwrap of a unary structural body
//! (SRD-66 §"Surface 4").
//!
//! The motivating use case has a CQL `describe keyspace` op whose
//! body is a single row × single text column. To regex-match the
//! schema text, the workload asserts unary shape and unwraps. No
//! implicit modal projection — identical workload source against a
//! non-unary body must surface a clear shape diagnostic, not silently
//! diverge from intent.
//!
//! Push 1 implements the assertion against the existing `Value`
//! variants (Str, Bool, U64, F64, VecF32, VecI32, None). Push 2 will
//! settle the structural body type (`Json` or similar) and extend
//! this node to walk row × column structure with the diagnostic
//! format from SRD-66 §"Surface 4 §Semantics":
//!
//! ```text
//! exactly_one_value: expected unary structure (1 row × 1 column),
//!                    found <r> rows × <c> columns
//! ```

use crate::node::{GkNode, NodeMeta, Port, PortType, Slot, Value};

/// Assert that the input value is a unary structure and return its
/// single cell. See module docs.
pub struct ExactlyOneValue {
    meta: NodeMeta,
}

impl Default for ExactlyOneValue {
    fn default() -> Self {
        Self::new()
    }
}

impl ExactlyOneValue {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "exactly_one_value".into(),
                outs: vec![Port::new("output", PortType::Str)],
                // Body wire's port type is a placeholder. The assembler
                // does not currently surface a "structural body" type;
                // Push 2 will revisit this when the body wire's GK type
                // is settled. For Push 1 we pass-through whatever type
                // the upstream wire produces (the eval method inspects
                // the actual `Value` variant).
                ins: vec![Slot::Wire(Port::new("body", PortType::Str))],
            },
        }
    }
}

impl GkNode for ExactlyOneValue {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let body = &inputs[0];
        outputs[0] = match body {
            // Already-scalar values pass through unchanged. They came
            // from a body projection that already collapsed the row ×
            // column structure (e.g. a CQL `body.to_text()` on a unary
            // result), so the assertion is trivially satisfied.
            Value::Str(_) | Value::Bool(_) | Value::U64(_) | Value::F64(_) => body.clone(),

            // Typed vector carriers: the structural shape is "1 row × 1
            // column" iff the slice has exactly one element. Unwrap the
            // single cell to its scalar carrier (F64 for f32, U64 for
            // i32 — the standard widening when these vectors flow
            // through the GK kernel as scalars).
            Value::VecF32(arc) => {
                if arc.len() != 1 {
                    panic!(
                        "exactly_one_value: expected unary structure \
                         (1 row × 1 column), found vec_f32 of length {}",
                        arc.len()
                    );
                }
                Value::F64(arc[0] as f64)
            }
            Value::VecI32(arc) => {
                if arc.len() != 1 {
                    panic!(
                        "exactly_one_value: expected unary structure \
                         (1 row × 1 column), found vec_i32 of length {}",
                        arc.len()
                    );
                }
                Value::U64(arc[0] as u64)
            }

            Value::None => panic!(
                "exactly_one_value: empty body (Value::None); the upstream \
                 op produced no result to unwrap"
            ),

            // TODO(SRD-66 Push 2): once a `Json`/structural body Value
            // variant lands, walk row × column structure and panic with
            // the SRD-66 §"Surface 4" shape diagnostic naming actual
            // dimensions. The Bytes/Json/Ext/Handle paths below should
            // be replaced by structural inspection when that variant
            // settles. For Push 1 we accept these as scalar pass-through
            // — the typical case is the upstream `body` projection has
            // already collapsed shape into one of these scalar carriers.
            Value::Bytes(_) | Value::Json(_) | Value::Ext(_) | Value::Handle(_) => body.clone(),
        };
    }
}

// ---------------------------------------------------------------------------
// Signature declaration for the DSL registry
// ---------------------------------------------------------------------------

use crate::dsl::registry::{Arity, FuncCategory, FuncSig, ParamSpec};
use crate::node::SlotType;

pub fn signatures() -> &'static [FuncSig] {
    use FuncCategory as C;
    &[FuncSig {
        name: "exactly_one_value",
        category: C::Diagnostic,
        outputs: 1,
        description: "assert body has unary structure (1 row × 1 column) and return its single cell",
        help: "exactly_one_value(body) -> V\n\
               \n\
               Inspect a structural body and return its single cell value;\n\
               panic with a shape diagnostic if the body has zero or\n\
               multiple rows / columns. Use to assertively unwrap a unary\n\
               result (e.g. CQL `describe keyspace`) before applying a\n\
               regex or other scalar predicate, instead of relying on an\n\
               implicit modal projection. Push 1 supports scalar carriers\n\
               and length-1 typed vectors; Push 2 extends to structural\n\
               row × column bodies.\n\
               \n\
               Example:\n  \
                 has_sai := regex_match(exactly_one_value(body),\n  \
                                        \"^TABLE\\\\s+system_views\\\\.sai\")",
        identity: None,
        variadic_ctor: None,
        params: &[ParamSpec {
            name: "body",
            slot_type: SlotType::Wire,
            required: true,
            example: "body",
            constraint: None,
        }],
        arity: Arity::Fixed,
        commutativity: crate::node::Commutativity::Positional,
        default_resolver: None,
    }]
}

pub(crate) fn build_node(
    name: &str,
    _wires: &[crate::assembly::WireRef],
    _consts: &[crate::dsl::factory::ConstArg],
) -> Option<Result<Box<dyn crate::node::GkNode>, String>> {
    match name {
        "exactly_one_value" => Some(Ok(Box::new(ExactlyOneValue::new()))),
        _ => None,
    }
}

crate::register_nodes!(signatures, build_node);

#[cfg(test)]
mod tests {
    use super::*;

    fn run(input: Value) -> Value {
        let node = ExactlyOneValue::new();
        let mut out = [Value::None];
        node.eval(&[input], &mut out);
        out.into_iter().next().unwrap()
    }

    #[test]
    fn passes_through_str() {
        let v = run(Value::Str("hello".into()));
        assert_eq!(v.as_str(), "hello");
    }

    #[test]
    fn passes_through_bool() {
        let v = run(Value::Bool(true));
        assert!(v.as_bool());
    }

    #[test]
    fn passes_through_u64() {
        let v = run(Value::U64(42));
        assert_eq!(v.as_u64(), 42);
    }

    #[test]
    fn passes_through_f64() {
        let v = run(Value::F64(2.5));
        assert_eq!(v.as_f64(), 2.5);
    }

    #[test]
    fn unwraps_singleton_vec_f32() {
        // Use SliceArc::from a small Vec<f32>.
        let v = Value::VecF32(crate::node::SliceArc::from_vec(vec![1.5_f32]));
        let out = run(v);
        assert_eq!(out.as_f64(), 1.5);
    }

    #[test]
    #[should_panic(expected = "expected unary structure")]
    fn rejects_multi_element_vec_f32() {
        let v = Value::VecF32(crate::node::SliceArc::from_vec(vec![1.0_f32, 2.0]));
        run(v);
    }

    #[test]
    #[should_panic(expected = "exactly_one_value: empty body")]
    fn rejects_none() {
        run(Value::None);
    }
}
