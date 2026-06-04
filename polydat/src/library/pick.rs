// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `pick` — branched-dispatch primitive (SRD-66 §"Surface 3").
//!
//! Signature: `pick(b0, b1, …, bN-1, v0, v1, …, vN-1) -> V`
//!
//! Exactly one of the N selector booleans must be true at eval; the
//! corresponding value is returned. Zero true and multiple-true both
//! panic with a clear diagnostic — workload authors get a hard signal
//! when their probe assumptions break, never a silent default.
//!
//! The split-halves call shape (all booleans first, then all values)
//! was chosen over interleaved pairs so long lists scan cleanly and
//! a missing pair surfaces as "odd total args" at compile time. See
//! SRD-66 §"Why not pair-wise `(b, v)` interleaving?" for rationale.

use crate::ast::Value;

/// Static guidance suffix appended to every `pick` panic, per
/// SRD-66 §"Diagnostic guidance".
const PICK_HINT: &str =
    "\n  hint: did the probe phase that sets these booleans run before \
this phase? Check scenario-tree DFS order or declare a `detect_*` \
phase ahead of consumers.";

/// Branched-dispatch primitive: select the value whose paired
/// selector is true. SRD-80b split-halves variadic — the macro
/// recognises two consecutive `&[T]` variadic args as a
/// split-halves shape, emits `selectors.len()` Bool slots
/// followed by `values.len()` polymorphic slots (each pair
/// `(b_i, v_i)` shares an index), and slices `inputs` at the
/// midpoint at eval time.
#[crate::polydat_node(category = Comparison, variadic_min = 1)]
fn pick(selectors: &[bool], values: &[Value]) -> Value {
    let n = selectors.len();
    debug_assert_eq!(n, values.len(),
        "pick arity mismatch at eval: selectors={} values={}",
        n, values.len());

    if crate::library::debug_nodes_enabled() {
        let sels: Vec<String> = selectors.iter().enumerate()
            .map(|(i, s)| format!("b{i}={s}"))
            .collect();
        let vals: Vec<String> = values.iter().enumerate()
            .map(|(i, v)| format!("v{i}={}", v.to_display_string()))
            .collect();
        crate::library::support::audit::debug(&format!(
            "pick: selectors=[{}] values=[{}]",
            sels.join(", "),
            vals.join(", "),
        ));
    }

    let mut matched: Vec<usize> = Vec::new();
    for (i, &sel) in selectors.iter().enumerate() {
        if sel { matched.push(i); }
    }

    if matched.is_empty() {
        panic!(
            "pick: no selector matched (all N={n} booleans false); \
             workload author guarantees one of {{b0, …, bN-1}} is \
             true at this point{PICK_HINT}"
        );
    }
    if matched.len() > 1 {
        let positions: Vec<String> = matched.iter().map(|i| format!("b{i}")).collect();
        panic!(
            "pick: multiple selectors matched (positions {}); \
             selectors must be mutually exclusive{PICK_HINT}",
            positions.join(", ")
        );
    }

    // Validate the value-half is uniform-typed across positions.
    let first_pt = values[0].port_type();
    for (i, v) in values.iter().enumerate().skip(1) {
        let vpt = v.port_type();
        if vpt != first_pt {
            panic!(
                "pick: value v{i} has type {vpt:?} but v0 has type {first_pt:?}; \
                 all value inputs must share a common type{PICK_HINT}"
            );
        }
    }

    values[matched[0]].clone()
}

// ---------------------------------------------------------------------------
// Signature declaration for the DSL registry
// ---------------------------------------------------------------------------

// `pick` is registered via the macro's `inventory::submit!`
// channel. The split-halves shape is recognised by the macro
// detecting two consecutive `&[T]` variadic args; FuncSig
// `Arity::VariadicWires { min_wires: 2 * variadic_min }` enforces
// total wire count = 2 × pairs. Odd-arity workload calls fall
// through to the standard assembler-side variadic arity check.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::PolydatNode;

    fn run(node: &Pick, inputs: Vec<Value>) -> Value {
        let mut out = [Value::None];
        node.eval(&inputs, &mut out);
        out.into_iter().next().unwrap()
    }

    // Pick::new takes the per-half (pairs) count. The macro
    // emits a (n_wires: usize) ctor param interpreted as pairs
    // in split-halves mode; runtime inputs slice at the
    // midpoint, so each call must supply exactly 2 * pairs
    // input values.

    #[test]
    fn pick_true_first_returns_first_value() {
        let node = Pick::new(2);
        let v = run(
            &node,
            vec![
                Value::Bool(true),
                Value::Bool(false),
                Value::Str("a".into()),
                Value::Str("b".into()),
            ],
        );
        assert_eq!(v.as_str(), "a");
    }

    #[test]
    fn pick_true_second_returns_second_value() {
        let node = Pick::new(2);
        let v = run(
            &node,
            vec![
                Value::Bool(false),
                Value::Bool(true),
                Value::Str("a".into()),
                Value::Str("b".into()),
            ],
        );
        assert_eq!(v.as_str(), "b");
    }

    #[test]
    #[should_panic(expected = "pick: no selector matched")]
    fn pick_zero_selectors_panics() {
        let node = Pick::new(2);
        run(
            &node,
            vec![
                Value::Bool(false),
                Value::Bool(false),
                Value::Str("a".into()),
                Value::Str("b".into()),
            ],
        );
    }

    #[test]
    #[should_panic(expected = "pick: multiple selectors matched")]
    fn pick_multiple_selectors_panics() {
        let node = Pick::new(2);
        run(
            &node,
            vec![
                Value::Bool(true),
                Value::Bool(true),
                Value::Str("a".into()),
                Value::Str("b".into()),
            ],
        );
    }

    #[test]
    #[should_panic(expected = "pick: value v1 has type")]
    fn pick_mixed_value_types_panics_at_eval() {
        let node = Pick::new(2);
        run(
            &node,
            vec![
                Value::Bool(true),
                Value::Bool(false),
                Value::U64(1),
                Value::Str("b".into()),
            ],
        );
    }

    #[test]
    fn pick_variadic_n_works_for_2_3_4() {
        // pairs = 3 → 6 total wires.
        let node = Pick::new(3);
        let v = run(
            &node,
            vec![
                Value::Bool(false),
                Value::Bool(false),
                Value::Bool(true),
                Value::Str("x".into()),
                Value::Str("y".into()),
                Value::Str("z".into()),
            ],
        );
        assert_eq!(v.as_str(), "z");

        // pairs = 4 → 8 total wires.
        let node = Pick::new(4);
        let v = run(
            &node,
            vec![
                Value::Bool(false),
                Value::Bool(true),
                Value::Bool(false),
                Value::Bool(false),
                Value::U64(10),
                Value::U64(20),
                Value::U64(30),
                Value::U64(40),
            ],
        );
        assert_eq!(v.as_u64(), 20);
    }

    #[test]
    fn pick_meta_has_correct_slot_count() {
        use crate::ast::{PortType, Slot};
        // pairs=3 → 6 total wire slots.
        let node = Pick::new(3);
        assert_eq!(node.meta().ins.len(), 6);
        // First N=3 slots are bool, last N=3 are placeholder
        // (PortType::Str — macro's PolyWire variadic default).
        for i in 0..3 {
            match &node.meta().ins[i] {
                Slot::Wire(p) => assert_eq!(p.typ, PortType::Bool, "selector {i} should be Bool"),
                _ => panic!("expected wire slot at {i}"),
            }
        }
    }
}
