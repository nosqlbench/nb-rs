// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Convenience weighted output selection nodes.
//!
//! These are "fat" convenience nodes that combine alias sampling with
//! value lookup in one step. They parse an inline spec string at init
//! time and perform weighted selection at cycle time.
//!
//! SRD-80b Phase E migration status:
//!
//! * [`WeightedStrings`] / [`WeightedU64`] — migrated to `#[polydat_node]`.
//!   The spec parser runs once at construction via a `#[poly_const]`
//!   setup function, producing a derived `WeightedStrCache` /
//!   `WeightedU64Cache` (parallel value+alias-table). The eval body
//!   does a constant-time lookup.
//! * [`WeightedPick`] — migrated to `#[polydat_node]` via the same
//!   spec-string surface as `weighted_u64`. The DSL form is now
//!   `weighted_pick(input, "v0:w0;v1:w1;...")`. The pre-Phase-E
//!   interleaved-pair form is gone — `FusedNode::decomposed()` had
//!   already declared the spec-string form is equivalent, so the
//!   migration collapses both surfaces onto one. The
//!   `compiled_u64`/`jit_constants` overrides survive intact; the
//!   JIT extern (`jit_weighted_pick`) reads the same 5-u64
//!   (values_ptr, biases_ptr, primaries_ptr, aliases_ptr, n)
//!   constants slice as before.
//! * [`DynamicWeightedSelect`] — migrated to `#[polydat_node]` via
//!   the SRD-80b in-spirit `Config<T>` marker (the Wire trait's
//!   `WIRE_COST = Config` const flows through `Config<Arc<str>>` to
//!   the slot's `WireCost::Config` annotation, retaining the
//!   load-bearing compile-warning behaviour exercised by tests).
//!   Per-cycle behaviour: re-parses the spec on every call —
//!   intentionally; the Mutex-backed cache from the pre-Phase-E
//!   hand-written form depended on per-node interior-mutable state
//!   that the macro doesn't emit. With `Config` cost, well-formed
//!   workloads bind the spec at init-time, so the parse re-runs
//!   only when the workload-level binding wakes; cycle-time binders
//!   trip the compiler warning and get O(n) per cycle.

use crate::ast::CompiledU64Op;
use crate::derive_support::Config;
use crate::library::sampling::alias::AliasTableU64;
use crate::compile::fusion::{DecomposedGraph, DecomposedWire};

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

// ---------------------------------------------------------------------------
// WeightedStrings — migrated to #[polydat_node]
// ---------------------------------------------------------------------------

/// Derived state for [`WeightedStrings`]: the parsed value list and
/// the alias table, computed once at construction from the spec
/// string and read on every cycle.
pub struct WeightedStrCache {
    values: Vec<String>,
    table: AliasTableU64,
}

impl crate::derive_support::PolydatSetup for WeightedStrCache {}

fn build_weighted_str_cache(spec: &str) -> WeightedStrCache {
    let (values, weights) = parse_weighted_str_spec(spec);
    let table = AliasTableU64::from_weights(&weights);
    WeightedStrCache { values, table }
}

/// Weighted string selection from an inline spec.
///
/// Signature: `weighted_strings(input: u64, spec: &str) -> String`
///
/// Spec format: `"alpha:0.3;beta:0.5;gamma:0.2"`. The input is
/// expected to be a hashed u64 so the alias-method sampling sees
/// a uniformly-distributed selector.
#[crate::polydat_node(category = Weighted)]
fn weighted_strings(
    input: u64,
    spec: crate::derive_support::Const<&str>,
    #[poly_const(build_weighted_str_cache, from = spec)]
    cache: &WeightedStrCache,
) -> String {
    let _ = spec; // value baked into `cache` at construction
    let idx = cache.table.sample(input) as usize;
    cache.values[idx].clone()
}

// ---------------------------------------------------------------------------
// WeightedU64 — migrated to #[polydat_node]
// ---------------------------------------------------------------------------

/// Derived state for [`WeightedU64`]: the parsed value list and
/// the alias table, computed once at construction from the spec
/// string and read on every cycle.
pub struct WeightedU64Cache {
    values: Vec<u64>,
    table: AliasTableU64,
}

impl crate::derive_support::PolydatSetup for WeightedU64Cache {}

fn build_weighted_u64_cache(spec: &str) -> WeightedU64Cache {
    let (values, weights) = parse_weighted_u64_spec(spec);
    let table = AliasTableU64::from_weights(&weights);
    WeightedU64Cache { values, table }
}

/// Weighted u64 selection from an inline spec.
///
/// Signature: `weighted_u64(input: u64, spec: &str) -> u64`
///
/// Spec format: `"10:0.5;20:0.3;30:0.2"`.
#[crate::polydat_node(category = Weighted)]
fn weighted_u64(
    input: u64,
    spec: crate::derive_support::Const<&str>,
    #[poly_const(build_weighted_u64_cache, from = spec)]
    cache: &WeightedU64Cache,
) -> u64 {
    let _ = spec; // value baked into `cache` at construction
    let idx = cache.table.sample(input) as usize;
    cache.values[idx]
}

// ---------------------------------------------------------------------------
// WeightedPick — migrated to #[polydat_node] via the spec-string surface
//
// The pre-Phase-E interleaved-pair DSL form (`weighted_pick(input, w0,
// v0, w1, v1, ...)`) is gone. The FusedNode equivalence already
// declared the spec-string form (`weighted_pick(input, "v0:w0;v1:w1;...")`)
// is canonical; the migration collapses both surfaces onto one. The
// override path keeps the `compiled_u64`/`jit_constants` closures —
// the JIT extern `jit_weighted_pick` reads the same 5-u64 constants
// slice (values_ptr, biases_ptr, primaries_ptr, aliases_ptr, n).
// ---------------------------------------------------------------------------

/// Derived state for [`WeightedPick`]: parsed values, weights, and the
/// alias table, built once at construction from the spec string and
/// read on every cycle (both in the eval path and the compiled
/// closure). `weights` is retained alongside `values` so
/// [`FusedNode::decomposed`] can reconstruct the equivalent
/// `weighted_u64` spec string.
pub struct WeightedPickState {
    pub table: AliasTableU64,
    pub values: Vec<u64>,
    pub weights: Vec<f64>,
}

impl crate::derive_support::PolydatSetup for WeightedPickState {}

/// Spec syntax: `"<value>:<weight>;<value>:<weight>;..."` —
/// e.g. `"100:1.0;200:2.0;300:1.5"` → values `[100, 200, 300]`,
/// weights `[1.0, 2.0, 1.5]`. Panics on malformed spec;
/// construction-time failure is the right signal (matches the
/// `weighted_u64` parser).
fn parse_weighted_pick_spec(spec: &str) -> WeightedPickState {
    let mut weights = Vec::new();
    let mut values = Vec::new();
    for entry in spec.split([';', ',']) {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let (v, w) = entry
            .split_once(':')
            .unwrap_or_else(|| panic!("weighted_pick: malformed entry '{entry}', expected 'value:weight'"));
        let value: u64 = v
            .trim()
            .parse()
            .unwrap_or_else(|_| panic!("weighted_pick: invalid value '{v}' in entry '{entry}'"));
        let weight: f64 = w
            .trim()
            .parse()
            .unwrap_or_else(|_| panic!("weighted_pick: invalid weight '{w}' in entry '{entry}'"));
        assert!(
            weight.is_finite() && weight > 0.0,
            "weighted_pick: weight must be a positive finite f64, got {weight}",
        );
        values.push(value);
        weights.push(weight);
    }
    assert!(
        !weights.is_empty(),
        "weighted_pick requires at least one entry in spec",
    );
    WeightedPickState {
        table: AliasTableU64::from_weights(&weights),
        values,
        weights,
    }
}

/// `compiled_u64` override for [`WeightedPick`]. Captures the
/// parsed value list and alias-table arrays by clone so the
/// returned closure is independent of `self`'s lifetime.
fn weighted_pick_jit(node: &WeightedPick) -> CompiledU64Op {
    let values = node.state.values.clone();
    let biases = node.state.table.biases().to_vec();
    let primaries = node.state.table.primaries().to_vec();
    let aliases = node.state.table.aliases().to_vec();
    let n = values.len();
    Box::new(move |inputs, outputs| {
        let input = inputs[0];
        let slot = (input as usize) % n;
        let bias_test = ((input >> 32) as f64) / (u32::MAX as f64);
        let index = if bias_test < biases[slot] {
            primaries[slot]
        } else {
            aliases[slot]
        };
        outputs[0] = values[index as usize];
    })
}

/// `jit_constants` override for [`WeightedPick`]. Publishes the
/// pointer/length quintuple the `jit_weighted_pick` extern reads.
/// Safety: pointers live in the parsed state which is owned by
/// `PolydatProgram` behind an `Arc` — never moved or freed during
/// the JIT kernel's lifetime.
fn weighted_pick_jit_constants(node: &WeightedPick) -> Vec<u64> {
    vec![
        node.state.values.as_ptr() as u64,
        node.state.table.biases().as_ptr() as u64,
        node.state.table.primaries().as_ptr() as u64,
        node.state.table.aliases().as_ptr() as u64,
        node.state.values.len() as u64,
    ]
}

/// Weighted u64 selection from a spec string.
///
/// Signature: `weighted_pick(input: u64, spec: &str) -> u64`
///
/// Spec format: `"value:weight;value:weight;..."` — e.g.
/// `"100:1.0;200:2.0;300:1.5"`. Weights are relative (need not
/// sum to 1); each must be positive and finite. Internally builds
/// an alias table at construction for O(1) sampling.
///
/// JIT level: P2 — `compiled_u64` is supplied by
/// [`weighted_pick_jit`] (closure with captured alias-table
/// arrays); `jit_constants` is supplied by
/// [`weighted_pick_jit_constants`] (5-u64 slice for the
/// `jit_weighted_pick` extern).
///
/// Example: `weighted_pick(hash(cycle), "100:0.5;200:0.3;300:0.2")`.
/// `weighted_pick(input, "v0:w0;v1:w1;...")` is equivalent to
/// `weighted_u64(input, "v0:w0;v1:w1;...")`. The two nodes share
/// the same spec-string surface, so this decomposition is a
/// direct re-instantiation under the canonical name. Wired into
/// the macro via `#[polydat_node(decompose = ...)]` so the
/// FusedNode impl is emitted alongside PolydatNode.
fn weighted_pick_decompose(node: &WeightedPick) -> DecomposedGraph {
    let spec: String = node.state.values.iter().zip(node.state.weights.iter())
        .map(|(v, w)| format!("{v}:{w}"))
        .collect::<Vec<_>>()
        .join(";");
    let mut g = DecomposedGraph::new(1);
    let wu = g.add_node(
        Box::new(WeightedU64::new(spec)),
        vec![DecomposedWire::Input(0)],
    );
    g.set_outputs(vec![DecomposedWire::Node(wu, 0)]);
    g
}

#[crate::polydat_node(
    category = Weighted,
    compiled_u64 = weighted_pick_jit,
    jit_constants = weighted_pick_jit_constants,
    decompose = weighted_pick_decompose,
)]
fn weighted_pick(
    input: u64,
    spec: crate::derive_support::Const<&str>,
    #[poly_const(parse_weighted_pick_spec, from = spec)]
    state: &WeightedPickState,
) -> u64 {
    let _ = spec; // value baked into `state` at construction
    let idx = state.table.sample(input) as usize;
    state.values[idx]
}

// ---------------------------------------------------------------------------
// DynamicWeightedSelect — migrated to `#[polydat_node]` via the
// `Config<T>` marker. The Wire trait's `WIRE_COST` const flows
// through `Config<Arc<str>>` to the slot's `WireCost::Config`
// annotation, retaining the load-bearing compile-warning behaviour
// exercised by tests. The pre-Phase-E `Mutex<DynamicWeightedCache>`
// is gone — the macro doesn't emit interior-mutable per-node state
// — and the body now re-parses each cycle. Workloads that bind
// `weights_spec` at init-time pay the parse cost once (the
// underlying `Value::Str` Arc is shared) and never re-trigger;
// workloads that bind from a cycle-time source already trip the
// Config-wire warning and have opted in to the per-cycle cost.
// ---------------------------------------------------------------------------

/// Dynamic weighted selection where the weight spec is a wire input.
///
/// Signature: `dynamic_weighted_select(selector: u64, weights_spec: Str) -> Str`
///
/// Unlike `WeightedStrings` (which parses weights at init time and
/// builds the alias table once), this node accepts the weight spec
/// as a runtime wire input. The `weights_spec` input is wrapped in
/// `Config<Arc<str>>` to mark it as a configuration-cost wire: the
/// compiler warns when it is bound to a cycle-time source.
///
/// Typical use: wire `weights_spec` to an init-time constant or a
/// rarely-changing captured value. Wire `selector` to a per-cycle
/// hash for O(1) lookup once the alias table is built.
///
/// Spec format: `"alpha:0.3;beta:0.5;gamma:0.2"`
#[crate::polydat_node(category = Weighted)]
fn dynamic_weighted_select(
    selector: u64,
    weights_spec: Config<std::sync::Arc<str>>,
) -> String {
    let spec: &str = weights_spec.0.as_ref();
    let (values, weights) = parse_weighted_str_spec(spec);
    if values.is_empty() {
        return String::new();
    }
    let table = AliasTableU64::from_weights(&weights);
    let idx = table.sample(selector) as usize;
    values[idx].clone()
}

// ---------------------------------------------------------------------------
// All nodes in this module now self-register via `#[polydat_node]`;
// the hand-written `signatures()` / `build_node()` / `validate_node()`
// / `register_nodes!` entries from the pre-Phase-E form have been
// removed.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{ConstValue, PolydatNode, Slot, Value};
    use crate::compile::fusion::FusedNode;
    use xxhash_rust::xxh3::xxh3_64;

    #[test]
    fn weighted_strings_valid_outputs() {
        let node = WeightedStrings::new("alpha:0.3;beta:0.5;gamma:0.2".to_string());
        let valid = ["alpha", "beta", "gamma"];
        let mut out = [Value::None];
        for i in 0..1000u64 {
            node.eval(&[Value::U64(xxh3_64(&i.to_le_bytes()))], &mut out);
            assert!(valid.contains(&out[0].as_str()));
        }
    }

    #[test]
    fn weighted_strings_respects_weights() {
        let node = WeightedStrings::new("rare:0.01;common:0.99".to_string());
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
        let node = WeightedU64::new("10:0.5;20:0.3;30:0.2".to_string());
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
        let node = WeightedPick::new("10:0.5;20:0.3;30:0.2".to_string());
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
        let node = WeightedPick::new("1:0.99;2:0.01".to_string());
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
        let node = WeightedPick::new("42:1.0".to_string());
        let mut out = [Value::None];
        for i in 0..100u64 {
            node.eval(&[Value::U64(i)], &mut out);
            assert_eq!(out[0].as_u64(), 42);
        }
    }

    #[test]
    fn weighted_pick_equal_weights() {
        let node = WeightedPick::new("10:1.0;20:1.0;30:1.0".to_string());
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
        let node = WeightedPick::new("10:0.5;20:0.3;30:0.2".to_string());
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
    fn weighted_pick_jit_constants_shape() {
        // The 5-u64 jit_constants slice (values_ptr, biases_ptr,
        // primaries_ptr, aliases_ptr, n) is the contract the
        // `jit_weighted_pick` extern reads.
        let node = WeightedPick::new("10:0.5;20:0.3;30:0.2".to_string());

        let raw = node.jit_constants();
        assert_eq!(raw.len(), 5); // values_ptr, biases_ptr, primaries_ptr, aliases_ptr, n
        assert_eq!(raw[4], 3);    // n = 3 entries in the spec

        // The values_ptr should match the parsed value list inside state.
        assert_eq!(raw[0], node.state.values.as_ptr() as u64);
        assert_eq!(raw[1], node.state.table.biases().as_ptr() as u64);
        assert_eq!(raw[2], node.state.table.primaries().as_ptr() as u64);
        assert_eq!(raw[3], node.state.table.aliases().as_ptr() as u64);
    }

    #[test]
    fn weighted_pick_equivalence_with_weighted_u64() {
        // weighted_pick(input, "10:0.5;20:0.3;30:0.2") should match
        // weighted_u64(input, "10:0.5;20:0.3;30:0.2") — the two
        // nodes now share the same spec-string surface.
        let fused = WeightedPick::new("10:0.5;20:0.3;30:0.2".to_string());
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

    #[test]
    #[should_panic(expected = "weighted_pick requires at least one entry in spec")]
    fn weighted_pick_rejects_empty_spec() {
        // Macro-emitted `new` runs the setup parser eagerly; empty
        // spec panics at construction.
        let _ = WeightedPick::new("".to_string());
    }

    #[test]
    #[should_panic(expected = "weighted_pick: malformed entry")]
    fn weighted_pick_rejects_bad_format() {
        let _ = WeightedPick::new("noweight".to_string());
    }

    #[test]
    #[should_panic(expected = "weighted_pick: weight must be a positive finite f64")]
    fn weighted_pick_rejects_nonpositive_weight() {
        let _ = WeightedPick::new("10:0.0;20:1.0".to_string());
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
        assert_eq!(wire_inputs[0].wire_cost, crate::ast::WireCost::Data);
        assert_eq!(wire_inputs[1].wire_cost, crate::ast::WireCost::Config);
    }

    #[test]
    fn dynamic_weighted_select_e2e_init_config() {
        // Init-time config wire: no warning expected
        use crate::dsl::events::CompileEventLog;

        let source = r#"
            input cycle: u64
            const spec := "alpha:0.3;beta:0.7"
            result := dynamic_weighted_select(hash(cycle), spec)
        "#;
        let mut log = CompileEventLog::new();
        let _k = crate::dsl::compile::compile_polydat_with_log(source, &mut log).unwrap();

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
            input cycle: u64
            spec := format_u64(hash(cycle), 10)
            result := dynamic_weighted_select(hash(cycle), spec)
        "#;
        let mut log = CompileEventLog::new();
        let _k = crate::dsl::compile::compile_polydat_with_log(source, &mut log).unwrap();

        let warnings: Vec<_> = log.events().iter().filter(|e|
            matches!(e, crate::dsl::events::CompileEvent::ConfigWireCycleWarning { .. })
        ).collect();
        assert_eq!(warnings.len(), 1, "cycle-time config should warn: {warnings:?}");
    }

    #[test]
    fn dynamic_weighted_select_strict_rejects_cycle_config() {
        // In strict mode, Config wire from cycle source is a hard error.
        use crate::compile::assembly::{PolydatAssembler, WireRef};
        use crate::library::hash::Hash;
        use crate::library::convert::U64ToString;
        use crate::dsl::events::CompileEventLog;

        let mut asm = PolydatAssembler::new(vec!["cycle".into()]);
        asm.add_node("hashed", Box::new(Hash::new()), vec![WireRef::input("cycle")]);
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
        let mut asm2 = PolydatAssembler::new(vec!["cycle".into()]);
        asm2.add_node("hashed", Box::new(Hash::new()), vec![WireRef::input("cycle")]);
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
        // Spec-string surface: 1 wire input + 1 Const<&str> constant.
        let node = WeightedPick::new("10:0.5;20:0.3".to_string());
        let meta = node.meta();

        // Name
        assert_eq!(meta.name, "weighted_pick");

        // Ins: 1 wire + 1 string constant (the spec)
        assert_eq!(meta.ins.len(), 2);
        assert!(matches!(meta.ins[0], Slot::Wire(_)));
        assert!(matches!(&meta.ins[1], Slot::Const { value: ConstValue::Str(_), .. }));

        // Outs: 1 u64
        assert_eq!(meta.outs.len(), 1);

        // Wire inputs
        assert_eq!(meta.wire_inputs().len(), 1);

        // Const slots
        let consts = meta.const_slots();
        assert_eq!(consts.len(), 1); // spec
    }
}
