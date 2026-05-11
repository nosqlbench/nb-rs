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
        if crate::nodes::debug_nodes_enabled() {
            // Per-cycle visibility into what the structural unwrap
            // saw and produced. Body's display form is truncated for
            // long Json arrays so the trace stays scannable.
            let body_disp = body.to_display_string();
            let snippet: String = body_disp.chars().take(400).collect();
            let ellipsis = if body_disp.len() > snippet.len() { "…" } else { "" };
            eprintln!(
                "[DEBUG] exactly_one_value: body.variant={:?} body.len={} snippet={}{ellipsis}",
                body.port_type(),
                body_disp.len(),
                snippet,
            );
        }
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

            // SRD-66 §"Surface 4 §Semantics" — structural body
            // walk: an `Array` is the row dimension, an `Object`
            // is the column dimension within each row, and the
            // single leaf cell is what we return. Unary shape =
            // 1 row × 1 column × 1 leaf. The path matches CQL
            // adapter projection: `[{"create_statement":"..."}]`
            // is the canonical describe-keyspace shape — one row
            // (array len 1), one column (object key count 1),
            // one leaf (the schema text).
            //
            // The leaf maps back to the matching Value variant:
            // JSON String → Value::Str, JSON Number → Value::F64
            // / Value::U64 (per int-ness), JSON Bool → Value::Bool,
            // JSON Null → panic (no value to unwrap).
            Value::Json(j) => unwrap_unary_json(j),

            // Other carriers — not part of the structural body
            // shape. Pass through (the upstream projection
            // already collapsed shape into a scalar carrier).
            // Bytes / Ext / Handle would never ride the `body`
            // wire (PortType::Json) but kept for completeness in
            // case `exactly_one_value` is applied to non-body
            // wires.
            Value::Bytes(_) | Value::Ext(_) | Value::Handle(_) => body.clone(),
        };
    }
}

/// Walk a JSON value asserting unary row × column × leaf shape.
/// Returns the matching `Value` variant for the leaf cell.
///
/// The shape diagnostic names actual dimensions when the input
/// doesn't match the unary contract.
fn unwrap_unary_json(j: &serde_json::Value) -> Value {
    use serde_json::Value as J;
    // Row dimension: an array. Length 0 / >1 → shape error.
    let row = match j {
        J::Array(arr) => match arr.len() {
            0 => panic!(
                "exactly_one_value: expected unary structure (1 row × 1 column), \
                 found 0 rows"
            ),
            1 => &arr[0],
            n => panic!(
                "exactly_one_value: expected unary structure (1 row × 1 column), \
                 found {n} rows"
            ),
        },
        // No row wrapper — treat the whole value as the single
        // row and continue to column inspection. Adapters that
        // produce a single-row unwrapped projection (rare; CQL
        // doesn't) take this path naturally.
        other => other,
    };
    // Column dimension: an object. Length 0 / >1 → shape error.
    let leaf = match row {
        J::Object(obj) => match obj.len() {
            0 => panic!(
                "exactly_one_value: expected unary structure (1 row × 1 column), \
                 found 1 row × 0 columns"
            ),
            1 => obj.values().next().expect("len==1"),
            n => panic!(
                "exactly_one_value: expected unary structure (1 row × 1 column), \
                 found 1 row × {n} columns"
            ),
        },
        // No column wrapper — the row IS the leaf. Common for
        // non-tabular bodies (e.g. a HTTP body that's a bare
        // string).
        other => other,
    };
    match leaf {
        J::String(s) => Value::Str(s.clone()),
        J::Bool(b) => Value::Bool(*b),
        J::Number(n) => {
            if let Some(u) = n.as_u64() {
                Value::U64(u)
            } else if let Some(f) = n.as_f64() {
                Value::F64(f)
            } else {
                panic!("exactly_one_value: numeric leaf is not representable as u64 or f64: {n}")
            }
        }
        J::Null => panic!(
            "exactly_one_value: leaf cell is null; expected a non-null value"
        ),
        // Nested structural leaf — the body has more than two
        // levels of nesting. Not a unary shape per the SRD; the
        // diagnostic names what was found.
        J::Array(_) | J::Object(_) => panic!(
            "exactly_one_value: leaf cell is itself structural ({}); \
             expected a scalar (string, number, or boolean)",
            describe_json_kind(leaf)
        ),
    }
}

fn describe_json_kind(j: &serde_json::Value) -> &'static str {
    use serde_json::Value as J;
    match j {
        J::Null => "null",
        J::Bool(_) => "bool",
        J::Number(_) => "number",
        J::String(_) => "string",
        J::Array(_) => "array",
        J::Object(_) => "object",
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
        output_type: crate::dsl::registry::OutputType::Fixed,
    }]
}

pub(crate) fn build_node(
    name: &str,
    _wires: &[crate::assembly::WireRef], _wire_types: &[crate::node::PortType],
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

    // ---------------------------------------------------------------
    // SRD-66 §"Surface 4 §Semantics" — structural Json walk
    // ---------------------------------------------------------------

    #[test]
    fn unwraps_unary_json_describe_keyspace_shape() {
        // Canonical CQL `DESCRIBE KEYSPACE` projection: array of
        // one row, each row an object with one text column.
        let j = serde_json::json!([
            {"create_statement": "VIRTUAL TABLE system_views.sai_column_indexes (\n  ...\n)"}
        ]);
        let out = run(Value::Json(j));
        let s = out.as_str();
        assert!(s.starts_with("VIRTUAL TABLE"), "got: {s:?}");
    }

    #[test]
    fn unwraps_unary_json_string_leaf() {
        // Bare scalar wrapped in array → object → string.
        let j = serde_json::json!([{"value": "hello"}]);
        let out = run(Value::Json(j));
        assert_eq!(out.as_str(), "hello");
    }

    #[test]
    fn unwraps_unary_json_numeric_leaf() {
        let j = serde_json::json!([{"n": 42}]);
        let out = run(Value::Json(j));
        assert_eq!(out.as_u64(), 42);
    }

    #[test]
    fn unwraps_unary_json_bool_leaf() {
        let j = serde_json::json!([{"b": true}]);
        let out = run(Value::Json(j));
        assert!(out.as_bool());
    }

    #[test]
    #[should_panic(expected = "found 0 rows")]
    fn rejects_empty_json_array() {
        run(Value::Json(serde_json::json!([])));
    }

    #[test]
    #[should_panic(expected = "found 2 rows")]
    fn rejects_multi_row_json() {
        let j = serde_json::json!([{"a": 1}, {"a": 2}]);
        run(Value::Json(j));
    }

    #[test]
    #[should_panic(expected = "found 1 row × 2 columns")]
    fn rejects_multi_column_json() {
        let j = serde_json::json!([{"a": 1, "b": 2}]);
        run(Value::Json(j));
    }

    #[test]
    #[should_panic(expected = "leaf cell is null")]
    fn rejects_json_null_leaf() {
        let j = serde_json::json!([{"a": null}]);
        run(Value::Json(j));
    }

    #[test]
    #[should_panic(expected = "leaf cell is itself structural")]
    fn rejects_json_nested_structural_leaf() {
        let j = serde_json::json!([{"a": {"nested": 1}}]);
        run(Value::Json(j));
    }
}
