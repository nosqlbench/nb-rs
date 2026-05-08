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

use crate::node::{Commutativity, GkNode, NodeMeta, Port, PortType, Slot, Value};

/// Variadic boolean-selector branch node.
///
/// Constructed with `n` total wire inputs (must be even). The first
/// half are Bool selectors; the second half are uniform-typed values.
/// The assembler skips type-checking for `pick` (analogous to printf)
/// because the value-half port type is not known at variadic-ctor
/// time; eval validates types and the selector cardinality.
pub struct PickN {
    meta: NodeMeta,
    n: usize,
}

impl PickN {
    /// Build a `pick` node with `total_wires` inputs. `total_wires`
    /// must be even and at least 2.
    pub fn new(total_wires: usize) -> Self {
        assert!(
            total_wires >= 2 && total_wires.is_multiple_of(2),
            "pick requires an even number of inputs >= 2 (got {total_wires})"
        );
        let n = total_wires / 2;
        let mut slots: Vec<Slot> = Vec::with_capacity(total_wires);
        for i in 0..n {
            slots.push(Slot::Wire(Port::new(format!("b{i}"), PortType::Bool)));
        }
        // Value-half port types are placeholders. The assembler skips
        // type-check for `pick`, so the source's actual `Value` flows
        // through unmodified; eval sees the real types and enforces
        // uniformity. We use U64 here as a neutral placeholder (any
        // PortType would do; printf uses U64 for the same reason).
        for i in 0..n {
            slots.push(Slot::Wire(Port::new(format!("v{i}"), PortType::U64)));
        }
        Self {
            meta: NodeMeta {
                name: "pick".into(),
                outs: vec![Port::new("output", PortType::U64)],
                ins: slots,
            },
            n,
        }
    }
}

/// Static guidance suffix appended to every `pick` panic, per
/// SRD-66 §"Diagnostic guidance".
const PICK_HINT: &str =
    "\n  hint: did the probe phase that sets these booleans run before \
this phase? Check scenario-tree DFS order or declare a `detect_*` \
phase ahead of consumers.";

impl GkNode for PickN {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    fn commutativity(&self) -> Commutativity {
        // Selectors and values are paired by index — order matters.
        Commutativity::Positional
    }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let n = self.n;
        debug_assert_eq!(inputs.len(), 2 * n, "pick arity mismatch at eval");

        // Validate selectors are Bool.
        for (i, sel) in inputs.iter().take(n).enumerate() {
            if !matches!(sel, Value::Bool(_)) {
                panic!(
                    "pick: selector b{i} has non-bool type {:?}; selectors \
                     must be Bool{PICK_HINT}",
                    sel.port_type()
                );
            }
        }

        // Count which selectors are true.
        let mut matched: Vec<usize> = Vec::new();
        for (i, sel) in inputs.iter().take(n).enumerate() {
            if sel.as_bool() {
                matched.push(i);
            }
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
        // The assembler skipped type-check, so we enforce it here.
        let values = &inputs[n..];
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

        let chosen = matched[0];
        outputs[0] = values[chosen].clone();
    }
}

// ---------------------------------------------------------------------------
// Signature declaration for the DSL registry
// ---------------------------------------------------------------------------

use crate::dsl::registry::{Arity, FuncCategory, FuncSig, ParamSpec};
use crate::node::SlotType;

/// Signatures for the `pick` node.
pub fn signatures() -> &'static [FuncSig] {
    use FuncCategory as C;
    &[FuncSig {
        name: "pick",
        category: C::Comparison,
        outputs: 1,
        description: "branched dispatch: N booleans + N values; return value paired with the unique true selector",
        help: "pick(b0, b1, …, bN-1, v0, v1, …, vN-1) -> V\n\
               \n\
               Exactly one of the N selector booleans must be true; the\n\
               corresponding value is returned. The split-halves shape\n\
               (selectors first, then values) lets long lists scan as two\n\
               readable groups and surfaces missing pairs as 'odd total\n\
               args' at compile time.\n\
               \n\
               Eval-time errors:\n  \
                 - zero matched   → 'pick: no selector matched (all N=… false)'\n  \
                 - multiple match → 'pick: multiple selectors matched (b1, b3, …)'\n\
               \n\
               Compile-time errors:\n  \
                 - odd argument count\n  \
                 - mixed value-half types (caught at eval)\n\
               \n\
               Example:\n  \
                 target := pick(has_sai, has_idx,\n  \
                                \"system_views.sai_column_indexes\",\n  \
                                \"system_views.indexes\")",
        identity: None,
        variadic_ctor: Some(|n| Box::new(PickN::new(n))),
        params: &[
            ParamSpec {
                name: "input",
                slot_type: SlotType::Wire,
                required: true,
                example: "true",
                constraint: None,
            },
        ],
        arity: Arity::VariadicWires { min_wires: 2 },
        commutativity: Commutativity::Positional,
        default_resolver: None,
    }]
}

/// Per-module dispatch hook. Validates even arg count up-front and
/// hands construction to `PickN::new`. Returns `Some(Err)` for an odd
/// total so the user gets a structured compile error rather than the
/// constructor's `assert!`.
pub(crate) fn build_node(
    name: &str,
    wires: &[crate::assembly::WireRef],
    _consts: &[crate::dsl::factory::ConstArg],
) -> Option<Result<Box<dyn crate::node::GkNode>, String>> {
    if name != "pick" {
        return None;
    }
    let n = wires.len();
    if n < 2 {
        return Some(Err(
            "pick requires at least one selector + one value (min 2 inputs)".into(),
        ));
    }
    if !n.is_multiple_of(2) {
        return Some(Err(
            "pick requires an even number of inputs (N booleans + N values)".into(),
        ));
    }
    Some(Ok(Box::new(PickN::new(n))))
}

crate::register_nodes!(signatures, build_node);

#[cfg(test)]
mod tests {
    use super::*;

    fn run(node: &PickN, inputs: Vec<Value>) -> Value {
        let mut out = [Value::None];
        node.eval(&inputs, &mut out);
        out.into_iter().next().unwrap()
    }

    #[test]
    fn pick_true_first_returns_first_value() {
        let node = PickN::new(4);
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
        let node = PickN::new(4);
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
        let node = PickN::new(4);
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
        let node = PickN::new(4);
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
        let node = PickN::new(4);
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

    fn build_err(wire_count: usize) -> String {
        let wires: Vec<crate::assembly::WireRef> = (0..wire_count)
            .map(|i| crate::assembly::WireRef::input(format!("w{i}")))
            .collect();
        match build_node("pick", &wires, &[]) {
            Some(Err(msg)) => msg,
            Some(Ok(_)) => panic!("expected build_node to error for {wire_count} wires"),
            None => panic!("pick did not handle the name"),
        }
    }

    #[test]
    fn pick_build_node_rejects_odd_arity() {
        // Three wires (one selector + two values) should be rejected
        // with a structured error rather than the ctor's assert.
        let err = build_err(3);
        assert!(err.contains("even number of inputs"), "got: {err}");
    }

    #[test]
    fn pick_build_node_rejects_too_few() {
        let err = build_err(1);
        assert!(err.contains("at least"), "got: {err}");
    }

    #[test]
    fn pick_variadic_n_works_for_4_6_8() {
        // 4-wire form (N=2): already covered above.
        // 6-wire form (N=3).
        let node = PickN::new(6);
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

        // 8-wire form (N=4).
        let node = PickN::new(8);
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
        let node = PickN::new(6);
        assert_eq!(node.meta().ins.len(), 6);
        // First N=3 slots are bool, last N=3 are placeholder u64.
        for i in 0..3 {
            match &node.meta().ins[i] {
                Slot::Wire(p) => assert_eq!(p.typ, PortType::Bool, "selector {i} should be Bool"),
                _ => panic!("expected wire slot at {i}"),
            }
        }
    }
}
