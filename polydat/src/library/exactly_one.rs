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

use crate::ast::Value;

/// Assert that the input value is a unary structure and return its
/// single cell as a String. See module docs.
///
/// **Output is always `Str`** (the function returns `String`, so the
/// macro emits a fixed `PortType::Str` output port). The function
/// accepts any `Value` variant the upstream wire produces and
/// renders the unwrapped leaf to its display form. This matches the
/// load-bearing workload pattern `schema_text := exactly_one_value(body)`
/// where downstream nodes (regex_match, format, …) take `&str`.
///
/// A polymorphic-output variant that preserves leaf types
/// (Bool/U64/F64/…) would require a port-type declaration that
/// reflects the runtime unwrap result rather than the input port,
/// which the macro can't express today without generic-over-Wire
/// instantiation. When such a variant is needed it becomes its own
/// node.
#[crate::polydat_node(category = Diagnostic)]
fn exactly_one_value(body: Value) -> String {
    if crate::library::debug_nodes_enabled() {
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
    let leaf_value: Value = match &body {
        // Already-scalar values pass through unchanged. They came
        // from a body projection that already collapsed the row ×
        // column structure.
        Value::Str(_) | Value::Bool(_) | Value::U64(_) | Value::F64(_) => body.clone(),

        // Typed vector carriers: the structural shape is "1 row × 1
        // column" iff the slice has exactly one element.
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
        Value::VecF64(arc) => {
            if arc.len() != 1 {
                panic!(
                    "exactly_one_value: expected unary structure \
                     (1 row × 1 column), found vec_f64 of length {}",
                    arc.len()
                );
            }
            Value::F64(arc[0])
        }
        Value::VecI64(arc) => {
            if arc.len() != 1 {
                panic!(
                    "exactly_one_value: expected unary structure \
                     (1 row × 1 column), found vec_i64 of length {}",
                    arc.len()
                );
            }
            Value::U64(arc[0] as u64)
        }
        Value::VecF16(arc) => {
            if arc.len() != 1 {
                panic!(
                    "exactly_one_value: expected unary structure \
                     (1 row × 1 column), found vec_f16 of length {}",
                    arc.len()
                );
            }
            Value::F64(arc[0].to_f32() as f64)
        }
        Value::VecI16(arc) => {
            if arc.len() != 1 {
                panic!(
                    "exactly_one_value: expected unary structure \
                     (1 row × 1 column), found vec_i16 of length {}",
                    arc.len()
                );
            }
            Value::U64(arc[0] as u64)
        }

        Value::None => panic!(
            "exactly_one_value: empty body (Value::None); the upstream \
             op produced no result to unwrap"
        ),

        // Structural body walk: array = row dim, object = column
        // dim, leaf = result. Unary shape = 1 × 1 × 1.
        Value::Json(j) => unwrap_unary_json(j),

        // Other carriers pass through (already collapsed).
        Value::Bytes(_) | Value::Ext(_) | Value::Handle(_) => body.clone(),
    };
    // Render to String for the declared Str output port. Non-Str
    // leaves render via the Value display form (Bool → "true"/"false",
    // U64 → "42", F64 → "3.14", etc.).
    match leaf_value {
        Value::Str(s) => s.to_string(),
        other => other.to_display_string(),
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
        J::String(s) => Value::Str(s.as_str().into()),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::PolydatNode;

    fn run(input: Value) -> Value {
        // Macro emits `ExactlyOneValue::new(input_type)` for a
        // PolyWire-in node; pass the input's port type so the
        // input slot's port-type metadata matches the upstream
        // wire. (Output port is fixed `Str` regardless of input.)
        let node = ExactlyOneValue::new(input.port_type());
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
        // Bool leaves render to "true"/"false" (Str output).
        let v = run(Value::Bool(true));
        assert_eq!(v.as_str(), "true");
    }

    #[test]
    fn passes_through_u64() {
        // Numeric leaves render to their display form.
        let v = run(Value::U64(42));
        assert_eq!(v.as_str(), "42");
    }

    #[test]
    fn passes_through_f64() {
        let v = run(Value::F64(2.5));
        assert_eq!(v.as_str(), "2.5");
    }

    #[test]
    fn unwraps_singleton_vec_f32() {
        let v = Value::VecF32(crate::ast::SliceArc::from_vec(vec![1.5_f32]));
        let out = run(v);
        assert_eq!(out.as_str(), "1.5");
    }

    #[test]
    #[should_panic(expected = "expected unary structure")]
    fn rejects_multi_element_vec_f32() {
        let v = Value::VecF32(crate::ast::SliceArc::from_vec(vec![1.0_f32, 2.0]));
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
        let j = serde_json::json!([
            {"create_statement": "VIRTUAL TABLE system_views.sai_column_indexes (\n  ...\n)"}
        ]);
        let out = run(Value::Json(std::sync::Arc::new(j)));
        let s = out.as_str();
        assert!(s.starts_with("VIRTUAL TABLE"), "got: {s:?}");
    }

    #[test]
    fn unwraps_unary_json_string_leaf() {
        let j = serde_json::json!([{"value": "hello"}]);
        let out = run(Value::Json(std::sync::Arc::new(j)));
        assert_eq!(out.as_str(), "hello");
    }

    #[test]
    fn unwraps_unary_json_numeric_leaf() {
        let j = serde_json::json!([{"n": 42}]);
        let out = run(Value::Json(std::sync::Arc::new(j)));
        assert_eq!(out.as_str(), "42");
    }

    #[test]
    fn unwraps_unary_json_bool_leaf() {
        let j = serde_json::json!([{"b": true}]);
        let out = run(Value::Json(std::sync::Arc::new(j)));
        assert_eq!(out.as_str(), "true");
    }

    #[test]
    #[should_panic(expected = "found 0 rows")]
    fn rejects_empty_json_array() {
        run(Value::Json(std::sync::Arc::new(serde_json::json!([]))));
    }

    #[test]
    #[should_panic(expected = "found 2 rows")]
    fn rejects_multi_row_json() {
        let j = serde_json::json!([{"a": 1}, {"a": 2}]);
        run(Value::Json(std::sync::Arc::new(j)));
    }

    #[test]
    #[should_panic(expected = "found 1 row × 2 columns")]
    fn rejects_multi_column_json() {
        let j = serde_json::json!([{"a": 1, "b": 2}]);
        run(Value::Json(std::sync::Arc::new(j)));
    }

    #[test]
    #[should_panic(expected = "leaf cell is null")]
    fn rejects_json_null_leaf() {
        let j = serde_json::json!([{"a": null}]);
        run(Value::Json(std::sync::Arc::new(j)));
    }

    #[test]
    #[should_panic(expected = "leaf cell is itself structural")]
    fn rejects_json_nested_structural_leaf() {
        let j = serde_json::json!([{"a": {"nested": 1}}]);
        run(Value::Json(std::sync::Arc::new(j)));
    }
}
