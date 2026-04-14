// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Convenience weighted output selection nodes.
//!
//! These are "fat" convenience nodes that combine alias sampling with
//! value lookup in one step. They parse an inline spec string at init
//! time and perform weighted selection at cycle time.

use crate::node::{Commutativity, CompiledU64Op, GkNode, NodeMeta, Port, PortType, Slot, Value};
use crate::sampling::alias::AliasTableU64;
use crate::fusion::{DecomposedGraph, DecomposedWire, FusedNode};

/// Parse a weighted spec like "alpha:0.3;beta:0.5;gamma:0.2"
/// into parallel vectors of values and weights.
fn parse_weighted_str_spec(spec: &str) -> (Vec<String>, Vec<f64>) {
    let mut values = Vec::new();
    let mut weights = Vec::new();
    for elem in spec.split([';', ',']) {
        let elem = elem.trim();
        if elem.is_empty() { continue; }
        let parts: Vec<&str> = elem.splitn(2, ':').collect();
        assert_eq!(parts.len(), 2, "expected 'value:weight', got '{elem}'");
        values.push(parts[0].to_string());
        weights.push(parts[1].parse::<f64>().expect("invalid weight"));
    }
    (values, weights)
}

fn parse_weighted_u64_spec(spec: &str) -> (Vec<u64>, Vec<f64>) {
    let mut values = Vec::new();
    let mut weights = Vec::new();
    for elem in spec.split([';', ',']) {
        let elem = elem.trim();
        if elem.is_empty() { continue; }
        let parts: Vec<&str> = elem.splitn(2, ':').collect();
        assert_eq!(parts.len(), 2, "expected 'value:weight', got '{elem}'");
        values.push(parts[0].parse::<u64>().expect("invalid value"));
        weights.push(parts[1].parse::<f64>().expect("invalid weight"));
    }
    (values, weights)
}

/// Weighted string selection from an inline spec.
///
/// Signature: `(input: u64) -> (String)`
///
/// Spec format: `"alpha:0.3;beta:0.5;gamma:0.2"`
/// Input should be hashed for uniform distribution.
pub struct WeightedStrings {
    meta: NodeMeta,
    values: Vec<String>,
    table: AliasTableU64,
}

impl WeightedStrings {
    pub fn new(spec: &str) -> Self {
        let (values, weights) = parse_weighted_str_spec(spec);
        let table = AliasTableU64::from_weights(&weights);
        Self {
            meta: NodeMeta {
                name: "weighted_strings".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::u64("input"))],
            },
            values,
            table,
        }
    }
}

impl GkNode for WeightedStrings {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let idx = self.table.sample(inputs[0].as_u64()) as usize;
        outputs[0] = Value::Str(self.values[idx].clone());
    }
}

/// Weighted u64 selection from an inline spec.
///
/// Signature: `(input: u64) -> (u64)`
///
/// Spec format: `"10:0.5;20:0.3;30:0.2"`
pub struct WeightedU64 {
    meta: NodeMeta,
    values: Vec<u64>,
    table: AliasTableU64,
}

impl WeightedU64 {
    pub fn new(spec: &str) -> Self {
        let (values, weights) = parse_weighted_u64_spec(spec);
        let table = AliasTableU64::from_weights(&weights);
        Self {
            meta: NodeMeta {
                name: "weighted_u64".into(),
                outs: vec![Port::u64("output")],
                ins: vec![Slot::Wire(Port::u64("input"))],
            },
            values,
            table,
        }
    }
}

impl GkNode for WeightedU64 {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let idx = self.table.sample(inputs[0].as_u64()) as usize;
        outputs[0] = Value::U64(self.values[idx]);
    }
}

/// Weighted u64 selection from inline weight/value pairs.
///
/// Signature: `weighted_pick(input: u64, w0: f64, v0: u64, ...) -> (u64)`
///
/// This is the reference implementation exercising all metadata features:
/// wire input, constant inputs, commutativity, Phase 2 compiled closure,
/// JIT constants, and fusion equivalence contract.
///
/// The weight/value pairs are interleaved constants using
/// `Arity::VariadicGroup`. Reordering pairs does not change the output
/// (the alias table normalizes weights), so `commutativity()` returns
/// `Groups` where each (weight, value) pair index set commutes with
/// the others.
///
/// DSL syntax: `weighted_pick(hash(cycle), 0.5, 10, 0.3, 20, 0.2, 30)`
///
/// Internally builds an alias table at construction for O(1) sampling.
///
/// JIT level: P2 (compiled_u64 closure with captured alias table arrays).
pub struct WeightedPick {
    meta: NodeMeta,
    weights: Vec<f64>,
    values: Vec<u64>,
    table: AliasTableU64,
}

impl WeightedPick {
    /// Create from explicit weight/value pairs.
    pub fn new(pairs: &[(f64, u64)]) -> Self {
        assert!(!pairs.is_empty(), "weighted_pick requires at least one pair");
        let weights: Vec<f64> = pairs.iter().map(|(w, _)| *w).collect();
        let values: Vec<u64> = pairs.iter().map(|(_, v)| *v).collect();
        let table = AliasTableU64::from_weights(&weights);

        // Build ins: wire input + interleaved (weight, value) constant pairs
        let mut ins = vec![Slot::Wire(Port::u64("input"))];
        for (i, &(w, v)) in pairs.iter().enumerate() {
            ins.push(Slot::const_f64(format!("w{i}"), w));
            ins.push(Slot::const_u64(format!("v{i}"), v));
        }

        Self {
            meta: NodeMeta {
                name: "weighted_pick".into(),
                ins,
                outs: vec![Port::u64("output")],
            },
            weights,
            values,
            table,
        }
    }
}

impl GkNode for WeightedPick {
    fn meta(&self) -> &NodeMeta { &self.meta }

    /// Commutativity: the weight/value pairs are interchangeable with
    /// each other (reordering pairs doesn't change the output since the
    /// alias table normalizes). Each pair occupies two consecutive slot
    /// indices: (1,2), (3,4), (5,6), etc. The wire at index 0 is
    /// positional. The groups express: "pair 0 can swap with pair 1",
    /// not "weight can swap with value within a pair".
    fn commutativity(&self) -> Commutativity {
        let n = self.values.len();
        if n <= 1 {
            return Commutativity::Positional;
        }
        // Each pair occupies slots [1+2i, 2+2i].
        // All pair-start indices commute as a set, and each pair's
        // two slots move together. We express this as one group
        // containing all the pair-start slot indices — when the
        // matcher permutes this group, it must move both slots of
        // each pair together. This requires a "pair-swap" rather than
        // individual-slot permutation. For now, express as Positional
        // since our Groups model operates on individual indices.
        //
        // TODO: Extend Groups to support pair-wise commutation.
        // For now, the FuncSig carries commutativity for the DSL level.
        Commutativity::Positional
    }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let idx = self.table.sample(inputs[0].as_u64()) as usize;
        outputs[0] = Value::U64(self.values[idx]);
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        let values = self.values.clone();
        let biases = self.table.biases().to_vec();
        let primaries = self.table.primaries().to_vec();
        let aliases = self.table.aliases().to_vec();
        let n = values.len();
        Some(Box::new(move |inputs, outputs| {
            let input = inputs[0];
            let slot = (input as usize) % n;
            let bias_test = ((input >> 32) as f64) / (u32::MAX as f64);
            let index = if bias_test < biases[slot] {
                primaries[slot]
            } else {
                aliases[slot]
            };
            outputs[0] = values[index as usize];
        }))
    }

    fn jit_constants(&self) -> Vec<u64> {
        // Expose array pointers and length for JIT extern call.
        // Safety: these pointers are into self.values and self.table,
        // which live in GkProgram behind Arc — never moved or freed
        // during the JIT kernel's lifetime.
        vec![
            self.values.as_ptr() as u64,
            self.table.biases().as_ptr() as u64,
            self.table.primaries().as_ptr() as u64,
            self.table.aliases().as_ptr() as u64,
            self.values.len() as u64,
        ]
    }
}

impl FusedNode for WeightedPick {
    /// `weighted_pick(input, w0, v0, w1, v1, ...)` is equivalent to
    /// `weighted_u64(input, "v0:w0;v1:w1;...")`.
    fn decomposed(&self) -> DecomposedGraph {
        // Build the equivalent spec string for WeightedU64
        let spec: String = self.values.iter().zip(self.weights.iter())
            .map(|(v, w)| format!("{v}:{w}"))
            .collect::<Vec<_>>()
            .join(";");
        let mut g = DecomposedGraph::new(1);
        let wu = g.add_node(
            Box::new(WeightedU64::new(&spec)),
            vec![DecomposedWire::Input(0)],
        );
        g.set_outputs(vec![DecomposedWire::Node(wu, 0)]);
        g
    }
}

/// Dynamic weighted selection where the weight spec is a wire input.
///
/// Unlike `WeightedStrings` (which parses weights at init time and
/// builds the alias table once), this node accepts the weight spec
/// as a runtime wire input. Changing the spec rebuilds the alias
/// table — an O(n) operation for n categories.
///
/// The `weights_spec` input is marked `WireCost::Config` to signal
/// that it is expensive to change per-cycle. The compiler warns when
/// this port is wired to a cycle-time source.
///
/// Typical use: wire `weights_spec` to an init-time constant or a
/// rarely-changing captured value. Wire `selector` to a per-cycle
/// hash for O(1) lookup.
///
/// Signature: `(selector: u64, weights_spec: String) -> (String)`
///
/// Spec format: `"alpha:0.3;beta:0.5;gamma:0.2"`
pub struct DynamicWeightedSelect {
    meta: NodeMeta,
    /// Cached alias table. Rebuilt when weights_spec changes.
    cached_spec: std::cell::RefCell<String>,
    cached_values: std::cell::RefCell<Vec<String>>,
    cached_table: std::cell::RefCell<Option<AliasTableU64>>,
}

impl DynamicWeightedSelect {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "dynamic_weighted_select".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![
                    Slot::Wire(Port::u64("selector")),
                    Slot::Wire(Port::str("weights_spec").config()),
                ],
            },
            cached_spec: std::cell::RefCell::new(String::new()),
            cached_values: std::cell::RefCell::new(Vec::new()),
            cached_table: std::cell::RefCell::new(None),
        }
    }

    fn rebuild_if_needed(&self, spec: &str) {
        let mut cached_spec = self.cached_spec.borrow_mut();
        if *cached_spec == spec {
            return; // no change
        }
        let (values, weights) = parse_weighted_str_spec(spec);
        let table = AliasTableU64::from_weights(&weights);
        *cached_spec = spec.to_string();
        *self.cached_values.borrow_mut() = values;
        *self.cached_table.borrow_mut() = Some(table);
    }
}

impl GkNode for DynamicWeightedSelect {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let selector = inputs[0].as_u64();
        let spec = inputs[1].as_str();
        self.rebuild_if_needed(spec);
        let values = self.cached_values.borrow();
        let table = self.cached_table.borrow();
        if let Some(ref table) = *table {
            let idx = table.sample(selector) as usize;
            outputs[0] = Value::Str(values[idx].clone());
        } else {
            outputs[0] = Value::Str(String::new());
        }
    }
}

// Safety: DynamicWeightedSelect uses RefCell internally but is only
// accessed from a single fiber's eval path (no concurrent access).
// GkNode requires Send + Sync for the program Arc, but evaluation
// is always single-threaded per GkState.
unsafe impl Send for DynamicWeightedSelect {}
unsafe impl Sync for DynamicWeightedSelect {}

// ---------------------------------------------------------------------------
// Signature declarations for the DSL registry
// ---------------------------------------------------------------------------

use crate::dsl::registry::{Arity, FuncCategory, FuncSig, ParamSpec};
use crate::node::SlotType;

/// Signatures for weighted categorical selection nodes.
pub fn signatures() -> &'static [FuncSig] {
    use FuncCategory as C;
    &[
        FuncSig {
            name: "weighted_strings", category: C::Weighted,
            outputs: 1, description: "weighted string selection from inline spec",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "spec", slot_type: SlotType::ConstStr, required: true, example: "\"1:10,2:20,3:30\"" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Weighted string selection from a compact spec string.\nSpec format: \"value:weight,value:weight,...\" — weights are relative.\nParameters:\n  input — u64 wire input (typically hashed)\n  spec  — comma-separated value:weight pairs\nExample: weighted_strings(hash(cycle), \"red:3,green:2,blue:1\")",
        },
        FuncSig {
            name: "weighted_u64", category: C::Weighted,
            outputs: 1, description: "weighted u64 selection from inline spec",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "spec", slot_type: SlotType::ConstStr, required: true, example: "\"1:10,2:20,3:30\"" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Weighted u64 selection from a compact spec string.\nSpec format: \"value:weight,value:weight,...\" — values are parsed as u64.\nParameters:\n  input — u64 wire input (typically hashed)\n  spec  — comma-separated value:weight pairs (e.g. \"10:0.5,20:0.3,30:0.2\")\nExample: weighted_u64(hash(cycle), \"100:5,200:3,300:2\")",
        },
        FuncSig {
            name: "weighted_pick", category: C::Weighted,
            outputs: 1, description: "weighted u64 selection from inline weight/value pairs",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "weight", slot_type: SlotType::ConstF64, required: true, example: "1.0" },
                ParamSpec { name: "value", slot_type: SlotType::ConstU64, required: true, example: "100" },
            ],
            arity: Arity::VariadicGroup {
                group: &[SlotType::ConstF64, SlotType::ConstU64],
                min_repeats: 1,
            },
            commutativity: crate::node::Commutativity::Positional,
            help: "Weighted categorical selection from inline weight/value pairs.\nUses the alias method for O(1) lookup after initialization.\nParameters:\n  input      — u64 wire input (typically hashed)\n  weight,val — repeating pairs: f64 weight, u64 value\nWeights are relative (need not sum to 1).\nExample: weighted_pick(hash(cycle), 3.0, 100, 1.0, 200, 1.0, 300)\nTheory: the alias method pre-computes a table so each lookup is\nconstant-time regardless of the number of categories.",
        },
        FuncSig {
            name: "dynamic_weighted_select", category: C::Weighted,
            outputs: 1, description: "weighted string selection with dynamic weight spec (Config wire)",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "selector", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "weights_spec", slot_type: SlotType::Wire, required: true, example: "cycle" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Dynamic weighted string selection where the weight spec is a wire input.\n\
                   The weights_spec input is a Config wire — changing it rebuilds the alias table (O(n)).\n\
                   Wire weights_spec to an init-time constant for normal use. Wiring to a cycle-time\n\
                   source triggers a compiler warning.\n\n\
                   Parameters:\n  selector     — u64 wire input (typically hashed)\n  \
                   weights_spec — String wire input (e.g. \"alpha:0.3;beta:0.5;gamma:0.2\")\n\n\
                   Example: dynamic_weighted_select(hash(cycle), my_weights)",
        },
    ]
}

/// Try to build a weighted-selection node from a function name and const args.
///
/// Returns `None` if the name is not handled by this module.
pub(crate) fn build_node(name: &str, _wires: &[crate::assembly::WireRef], consts: &[crate::dsl::factory::ConstArg]) -> Option<Result<Box<dyn crate::node::GkNode>, String>> {
    match name {
        "weighted_strings" => Some(Ok(Box::new(WeightedStrings::new(
            consts.first().map(|c| c.as_str()).unwrap_or(""),
        )))),
        "weighted_u64" => Some(Ok(Box::new(WeightedU64::new(
            consts.first().map(|c| c.as_str()).unwrap_or(""),
        )))),
        "weighted_pick" => {
            let pairs: Vec<(f64, u64)> = consts.chunks(2)
                .map(|chunk| {
                    let w = chunk.first().map(|c| c.as_f64()).unwrap_or(1.0);
                    let v = chunk.get(1).map(|c| c.as_u64()).unwrap_or(0);
                    (w, v)
                })
                .collect();
            Some(Ok(Box::new(WeightedPick::new(&pairs))))
        }
        "dynamic_weighted_select" => Some(Ok(Box::new(DynamicWeightedSelect::new()))),
        _ => None,
    }
}


crate::register_nodes!(signatures, build_node);
#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::ConstValue;
    use xxhash_rust::xxh3::xxh3_64;

    #[test]
    fn weighted_strings_valid_outputs() {
        let node = WeightedStrings::new("alpha:0.3;beta:0.5;gamma:0.2");
        let valid = ["alpha", "beta", "gamma"];
        let mut out = [Value::None];
        for i in 0..1000u64 {
            node.eval(&[Value::U64(xxh3_64(&i.to_le_bytes()))], &mut out);
            assert!(valid.contains(&out[0].as_str()));
        }
    }

    #[test]
    fn weighted_strings_respects_weights() {
        let node = WeightedStrings::new("rare:0.01;common:0.99");
        let mut common_count = 0u64;
        let mut out = [Value::None];
        let n = 10_000u64;
        for i in 0..n {
            node.eval(&[Value::U64(xxh3_64(&i.to_le_bytes()))], &mut out);
            if out[0].as_str() == "common" {
                common_count += 1;
            }
        }
        let ratio = common_count as f64 / n as f64;
        assert!(ratio > 0.90, "common should dominate, got {ratio}");
    }

    #[test]
    fn weighted_u64_valid_outputs() {
        let node = WeightedU64::new("10:0.5;20:0.3;30:0.2");
        let valid = [10u64, 20, 30];
        let mut out = [Value::None];
        for i in 0..1000u64 {
            node.eval(&[Value::U64(xxh3_64(&i.to_le_bytes()))], &mut out);
            assert!(valid.contains(&out[0].as_u64()));
        }
    }

    // --- WeightedPick tests ---

    #[test]
    fn weighted_pick_valid_outputs() {
        let node = WeightedPick::new(&[(0.5, 10), (0.3, 20), (0.2, 30)]);
        let valid = [10u64, 20, 30];
        let mut out = [Value::None];
        for i in 0..1000u64 {
            node.eval(&[Value::U64(xxh3_64(&i.to_le_bytes()))], &mut out);
            assert!(valid.contains(&out[0].as_u64()),
                "unexpected output {} at seed {i}", out[0].as_u64());
        }
    }

    #[test]
    fn weighted_pick_respects_weights() {
        let node = WeightedPick::new(&[(0.99, 1), (0.01, 2)]);
        let mut count_1 = 0u64;
        let mut out = [Value::None];
        let n = 10_000u64;
        for i in 0..n {
            node.eval(&[Value::U64(xxh3_64(&i.to_le_bytes()))], &mut out);
            if out[0].as_u64() == 1 { count_1 += 1; }
        }
        let ratio = count_1 as f64 / n as f64;
        assert!(ratio > 0.90, "value 1 (weight 0.99) should dominate, got {ratio}");
    }

    #[test]
    fn weighted_pick_single_pair() {
        let node = WeightedPick::new(&[(1.0, 42)]);
        let mut out = [Value::None];
        for i in 0..100u64 {
            node.eval(&[Value::U64(i)], &mut out);
            assert_eq!(out[0].as_u64(), 42);
        }
    }

    #[test]
    fn weighted_pick_equal_weights() {
        let node = WeightedPick::new(&[(1.0, 10), (1.0, 20), (1.0, 30)]);
        let mut counts = [0u64; 3];
        let mut out = [Value::None];
        let n = 30_000u64;
        for i in 0..n {
            node.eval(&[Value::U64(xxh3_64(&i.to_le_bytes()))], &mut out);
            match out[0].as_u64() {
                10 => counts[0] += 1,
                20 => counts[1] += 1,
                30 => counts[2] += 1,
                v => panic!("unexpected value {v}"),
            }
        }
        // Each should be roughly 1/3
        for (i, c) in counts.iter().enumerate() {
            let ratio = *c as f64 / n as f64;
            assert!(ratio > 0.25 && ratio < 0.42,
                "value at index {i} has ratio {ratio}, expected ~0.33");
        }
    }

    #[test]
    fn weighted_pick_compiled_matches_eval() {
        let node = WeightedPick::new(&[(0.5, 10), (0.3, 20), (0.2, 30)]);
        let compiled = node.compiled_u64().expect("should compile");
        for i in 0..10_000u64 {
            let input = xxh3_64(&i.to_le_bytes());
            let mut eval_out = [Value::None];
            node.eval(&[Value::U64(input)], &mut eval_out);
            let mut compiled_out = [0u64];
            compiled(&[input], &mut compiled_out);
            assert_eq!(eval_out[0].as_u64(), compiled_out[0],
                "eval vs compiled mismatch at seed {i}");
        }
    }

    #[test]
    fn weighted_pick_slot_consistency() {
        let node = WeightedPick::new(&[(0.5, 10), (0.3, 20), (0.2, 30)]);

        // jit_constants() returns array pointers + length for JIT extern call.
        let from_trait = node.jit_constants();
        assert_eq!(from_trait.len(), 5); // values_ptr, biases_ptr, primaries_ptr, aliases_ptr, n
        assert_eq!(from_trait[4], 3); // n = 3 pairs

        // jit_constants_from_slots() returns ALL typed constants: interleaved
        // weights (as f64 bits) and values.
        let from_slots = node.meta().jit_constants_from_slots();
        assert_eq!(from_slots.len(), 6); // w0, v0, w1, v1, w2, v2
        assert_eq!(from_slots[1], 10);
        assert_eq!(from_slots[3], 20);
        assert_eq!(from_slots[5], 30);
        assert_eq!(from_slots[0], 0.5f64.to_bits());
    }

    #[test]
    fn weighted_pick_equivalence_with_weighted_u64() {
        // weighted_pick(input, 0.5, 10, 0.3, 20, 0.2, 30) should match
        // weighted_u64(input, "10:0.5;20:0.3;30:0.2")
        let fused = WeightedPick::new(&[(0.5, 10), (0.3, 20), (0.2, 30)]);
        let decomposed = fused.decomposed();
        for i in 0..10_000u64 {
            let input = xxh3_64(&i.to_le_bytes());
            let mut fused_out = [Value::None];
            fused.eval(&[Value::U64(input)], &mut fused_out);
            let decomposed_out = decomposed.eval(&[Value::U64(input)]);
            assert_eq!(fused_out[0].as_u64(), decomposed_out[0].as_u64(),
                "equivalence failed at seed {i}");
        }
    }

    // --- DynamicWeightedSelect tests ---

    #[test]
    fn dynamic_weighted_select_basic() {
        let node = DynamicWeightedSelect::new();
        let spec = "alpha:0.3;beta:0.5;gamma:0.2";
        let valid = ["alpha", "beta", "gamma"];
        let mut out = [Value::None];
        for i in 0..100u64 {
            node.eval(
                &[Value::U64(xxh3_64(&i.to_le_bytes())), Value::Str(spec.into())],
                &mut out,
            );
            assert!(valid.contains(&out[0].as_str()), "unexpected: {}", out[0].as_str());
        }
    }

    #[test]
    fn dynamic_weighted_select_caches_table() {
        let node = DynamicWeightedSelect::new();
        let spec = "a:0.5;b:0.5";
        let mut out = [Value::None];

        // First call builds the table
        node.eval(&[Value::U64(42), Value::Str(spec.into())], &mut out);
        let first = out[0].as_str().to_string();

        // Same spec → same table (cached), same result for same input
        node.eval(&[Value::U64(42), Value::Str(spec.into())], &mut out);
        assert_eq!(out[0].as_str(), first);

        // Different spec → rebuilds table
        node.eval(&[Value::U64(42), Value::Str("x:1.0".into())], &mut out);
        assert_eq!(out[0].as_str(), "x");
    }

    #[test]
    fn dynamic_weighted_select_config_wire_annotation() {
        let node = DynamicWeightedSelect::new();
        let meta = node.meta();
        // Second input (weights_spec) should be marked Config
        let wire_inputs = meta.wire_inputs();
        assert_eq!(wire_inputs.len(), 2);
        assert_eq!(wire_inputs[0].wire_cost, crate::node::WireCost::Data);
        assert_eq!(wire_inputs[1].wire_cost, crate::node::WireCost::Config);
    }

    #[test]
    fn dynamic_weighted_select_e2e_init_config() {
        // Init-time config wire: no warning expected
        use crate::dsl::events::CompileEventLog;

        let source = r#"
            coordinates := (cycle)
            init spec = "alpha:0.3;beta:0.7"
            result := dynamic_weighted_select(hash(cycle), spec)
        "#;
        let mut log = CompileEventLog::new();
        let _k = crate::dsl::compile::compile_gk_with_log(source, &mut log).unwrap();

        let warnings: Vec<_> = log.events().iter().filter(|e|
            matches!(e, crate::dsl::events::CompileEvent::ConfigWireCycleWarning { .. })
        ).collect();
        assert!(warnings.is_empty(), "init-time config should not warn");
    }

    #[test]
    fn dynamic_weighted_select_e2e_cycle_config_warns() {
        // Cycle-time config wire: should warn
        use crate::dsl::events::CompileEventLog;

        // Spec derived from cycle → cycle-time → config wire warning
        let source = r#"
            coordinates := (cycle)
            spec := format_u64(hash(cycle), 10)
            result := dynamic_weighted_select(hash(cycle), spec)
        "#;
        let mut log = CompileEventLog::new();
        let _k = crate::dsl::compile::compile_gk_with_log(source, &mut log).unwrap();

        let warnings: Vec<_> = log.events().iter().filter(|e|
            matches!(e, crate::dsl::events::CompileEvent::ConfigWireCycleWarning { .. })
        ).collect();
        assert_eq!(warnings.len(), 1, "cycle-time config should warn: {warnings:?}");
    }

    #[test]
    fn dynamic_weighted_select_strict_rejects_cycle_config() {
        // In strict mode, Config wire from cycle source is a hard error.
        use crate::assembly::{GkAssembler, WireRef};
        use crate::nodes::hash::Hash64;
        use crate::nodes::convert::U64ToString;
        use crate::dsl::events::CompileEventLog;

        let mut asm = GkAssembler::new(vec!["cycle".into()]);
        asm.add_node("hashed", Box::new(Hash64::new()), vec![WireRef::input("cycle")]);
        asm.add_node("spec", Box::new(U64ToString::default()), vec![WireRef::node("hashed")]);
        asm.add_node("dws", Box::new(DynamicWeightedSelect::new()), vec![
            WireRef::node("hashed"),  // selector ← cycle (Data, ok)
            WireRef::node("spec"),    // weights_spec ← cycle (Config, BAD)
        ]);
        asm.add_output("result", WireRef::node("dws"));

        // Non-strict compile: should succeed with warning
        let mut log = CompileEventLog::new();
        let _kernel = asm.compile_with_log(Some(&mut log)).unwrap();
        let warnings: Vec<_> = log.events().iter().filter(|e|
            matches!(e, crate::dsl::events::CompileEvent::ConfigWireCycleWarning { .. })
        ).collect();
        assert_eq!(warnings.len(), 1, "should warn in non-strict");

        // Strict compile: rebuild and fold with strict=true
        let mut asm2 = GkAssembler::new(vec!["cycle".into()]);
        asm2.add_node("hashed", Box::new(Hash64::new()), vec![WireRef::input("cycle")]);
        asm2.add_node("spec", Box::new(U64ToString::default()), vec![WireRef::node("hashed")]);
        asm2.add_node("dws", Box::new(DynamicWeightedSelect::new()), vec![
            WireRef::node("hashed"),
            WireRef::node("spec"),
        ]);
        asm2.add_output("result", WireRef::node("dws"));

        let result = asm2.compile_strict(true);
        assert!(result.is_err(), "strict mode should reject cycle-time config wire");
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("strict") || msg.contains("config"),
            "error should mention strict or config: {msg}");
    }

    #[test]
    fn weighted_pick_metadata_complete() {
        let node = WeightedPick::new(&[(0.5, 10), (0.3, 20)]);
        let meta = node.meta();

        // Name
        assert_eq!(meta.name, "weighted_pick");

        // Ins: 1 wire + 4 constants (w0, v0, w1, v1)
        assert_eq!(meta.ins.len(), 5);
        assert!(matches!(meta.ins[0], Slot::Wire(_)));
        assert!(matches!(&meta.ins[1], Slot::Const { value: ConstValue::F64(_), .. }));
        assert!(matches!(&meta.ins[2], Slot::Const { value: ConstValue::U64(10), .. }));
        assert!(matches!(&meta.ins[3], Slot::Const { value: ConstValue::F64(_), .. }));
        assert!(matches!(&meta.ins[4], Slot::Const { value: ConstValue::U64(20), .. }));

        // Outs: 1 u64
        assert_eq!(meta.outs.len(), 1);

        // Wire inputs
        assert_eq!(meta.wire_inputs().len(), 1);

        // Const slots
        let consts = meta.const_slots();
        assert_eq!(consts.len(), 4); // w0, v0, w1, v1
    }
}
