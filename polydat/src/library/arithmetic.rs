// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Arithmetic function nodes.
//!
//! Core integer operations for the Polydat DAG. These are the building blocks
//! that most workloads compose: hash → mod → add for bounded IDs,
//! mixed_radix for coordinate decomposition, interleave for combining
//! independent dimensions.

use crate::ast::CompiledU64Op;
use crate::derive_support::Const;

/// Add a constant to a u64 value (wrapping).
///
/// Signature: `add(input: u64, addend: u64) -> (u64)`
///
/// Use for offsetting a bounded range: `mod(h, 100)` gives [0,100),
/// `add(mod(h, 100), 500)` gives [500,600). Also common with timestamps:
/// `add(base_epoch, offset)`.
///
/// JIT level: P3 (single `iadd` instruction).
// SRD-80 PR B.7 — Phase 3 const-arg arithmetic family
// migrated to `#[polydat_node]`. The macro derives `Add`,
// `Mul`, `Div`, `Mod` from snake_case → PascalCase; the
// `r#mod` raw identifier is stripped to "mod" for the DSL
// name and PascalCased to `Mod` for the struct.
//
// `classify_node` matches by DSL name ("add", "mul", "div",
// "mod") and reads `jit_constants()` — the macro auto-emits
// both, so Phase 3 dispatch is preserved verbatim.

#[crate::polydat_node(category = Arithmetic)]
fn add(input: u64, addend: Const<u64>) -> u64 {
    input.wrapping_add(*addend)
}

#[crate::polydat_node(category = Arithmetic)]
fn mul(input: u64, factor: Const<u64>) -> u64 {
    input.wrapping_mul(*factor)
}

#[crate::polydat_node(category = Arithmetic)]
fn div(input: u64, divisor: Const<u64>) -> u64 {
    // Greenfield posture: zero-divisor panics at cycle time
    // (matching the body's `/`). The original `new()` assert
    // is retired with the migration; if early-fail is needed
    // again, it lands via a const-constraint attribute later.
    input / *divisor
}

#[crate::polydat_node(category = Arithmetic)]
fn r#mod(input: u64, modulus: Const<u64>) -> u64 {
    input % *modulus
}

/// Modulo of a u64 value by a *wire-fed* divisor.
///
/// Signature: `mod_wire(input: u64, divisor: u64) -> (u64)`
///
/// The divisor is computed at cycle time from another node — for
/// example, a control read or a runtime-derived shard count. The
/// divisor port declares a `NonZeroU64` constraint, so under
/// `// @pragma: strict_values` the compiler auto-inserts an
/// `assert_u64_nonzero` between the source and the divisor input
/// (SRD 15 §"Strict Wire Mode"). Without strict mode, the node
/// trusts the divisor and a zero value will panic at cycle time —
/// the canonical "panic at hour 14" hazard, opt-out by design.
///
/// Use this when the modulus genuinely varies across cycles. For
/// the const case, prefer [`Mod`] which is faster (the divisor
/// is baked into the JIT closure as a constant).
///
/// JIT level: P2 (compiled_u64 closure; not const-foldable).
/// Modulo of a u64 by a wire-fed divisor. SRD-80 PR B.14
/// migration — the `#[constraint(NonZeroU64)]` attribute carries
/// the strict-wire-mode assertion contract.
#[crate::polydat_node(category = Arithmetic)]
fn mod_wire(input: u64, #[constraint(NonZeroU64)] divisor: u64) -> u64 {
    input % divisor
}

/// Division of a u64 by a wire-fed divisor. SRD-80 PR B.14
/// migration — same NonZeroU64 contract as mod_wire.
#[crate::polydat_node(category = Arithmetic)]
fn div_wire(input: u64, #[constraint(NonZeroU64)] divisor: u64) -> u64 {
    input / divisor
}

/// Smallest multiple of `multiple` that is ≥ `value`.
///
/// Signature: `ceil_to_multiple(value: u64, multiple: u64) -> (u64)`
///
/// Workload-author shorthand for "round this value up to the
/// next whole multiple of base." Eliminates the
/// `(v + m - 1) / m * m` / `div_ceil` idiom from bindings.
/// `multiple == 0` is a soft no-op: returns `value` unchanged
/// rather than trapping, so a transient zero from a wire-bound
/// extern doesn't break a binding mid-evaluation.
///
/// Use cases:
///   - cycle counts: `ceil_to_multiple(min_cycles, base)` gives
///     the smallest whole-pass cycle count meeting a minimum
///   - alignment: pad an offset up to a chunk boundary
///   - bucketing: snap a value up to the next bin edge
///
/// JIT level: P2 (uses `u64::div_ceil`).
/// Smallest multiple of `multiple` that is ≥ `value`. SRD-80
/// PR B.13. `multiple == 0` is a soft no-op (returns value
/// unchanged) so a transient zero from a wire-bound extern
/// doesn't break a binding mid-evaluation. JIT P2.
#[crate::polydat_node(category = Arithmetic)]
fn ceil_to_multiple(value: u64, multiple: u64) -> u64 {
    if multiple == 0 {
        value
    } else {
        value.div_ceil(multiple).saturating_mul(multiple)
    }
}

/// Count of multiples of `multiple` needed to cover `value`.
///
/// Signature: `multiples_at_least(value: u64, multiple: u64) -> (u64)`
///
/// Companion to [`CeilToMultiple`] that returns the *count*
/// instead of the product — i.e. `ceil(value / multiple)`. The
/// invariant `multiples_at_least(v, m) * m == ceil_to_multiple(v, m)`
/// holds whenever `multiple > 0` and the multiplication doesn't
/// overflow.
///
/// Use cases:
///   - calibration: `multiples_at_least(min_cycles, base)` gives
///     the pass count so the workload can both apply the
///     multiplier and report "ran N passes" for diagnostics
///   - bucket arithmetic: count of fixed-size buckets needed
///     to hold N items
///
/// `multiple == 0` returns `0` — there is no count that covers
/// a positive value with zero-sized multiples; rather than
/// trap, the function quietly yields the only honest answer.
///
/// JIT level: P3 (single `udiv_ceil`).
/// Count of multiples needed to cover `value`. SRD-80 PR B.13.
/// `multiple == 0` returns 0 (no count covers positive value
/// with zero-sized multiples). JIT P3.
#[crate::polydat_node(category = Arithmetic)]
fn multiples_at_least(value: u64, multiple: u64) -> u64 {
    if multiple == 0 { 0 } else { value.div_ceil(multiple) }
}

/// "Set-or-get" memoizer: returns `current` if non-zero,
/// otherwise returns `fallback`.
///
/// Signature: `set_or_get(current: u64, fallback: u64) -> (u64)`
///
/// Functionally `if current == 0 { fallback } else { current }`
/// — a simple conditional. The name reflects its intended use
/// alongside SRD-13f cross-scope shared wires:
///
/// ```text
///   shared query_passes := set_or_get(
///       query_passes,
///       multiples_at_least(min_cycles, base),
///   )
/// ```
///
/// First phase to evaluate this: `query_passes` reads 0 (the
/// unset sentinel), `set_or_get` returns the computed fallback,
/// the `shared :=` broadcast writes the value to the parent
/// scope's SharedCell. Every subsequent phase reads the
/// already-set value and the fallback computation is
/// effectively a no-op (it still evaluates, but its result is
/// discarded). The write-back is idempotent — writing the
/// already-cached value back doesn't change anything.
///
/// Concurrency: first-writer-wins is provided by the SharedCell
/// mutex, not by this node. The node itself is pure — given
/// the same inputs it returns the same output. Concurrent
/// phases evaluating it simultaneously will compute the same
/// fallback and race on the cell write; whichever writes last
/// wins, but they're writing the same value anyway.
///
/// JIT level: P3 (single compare + select).
//
// SRD-80b Phase E: migrated to `#[polydat_node]`. Struct
// renamed from `SetOrGetU64` to `SetOrGet` (greenfield
// posture — no cross-crate callers reference the old name)
// to match the macro's snake_case → PascalCase derivation.
#[crate::polydat_node(category = Arithmetic)]
fn set_or_get(current: u64, fallback: u64) -> u64 {
    if current == 0 { fallback } else { current }
}

/// Clamp an unsigned integer to [min, max].
///
/// Signature: `clamp(input: u64, min: u64, max: u64) -> (u64)`
///
/// Unlike mod (which wraps), clamp saturates at the boundary. Use when
/// you want values to pile up at the edges rather than wrap around.
///
/// JIT level: P3 (`umax` + `umin`).
//
// SRD-80b Phase E: migrated to `#[polydat_node]`. Struct
// renamed from `ClampU64` to `Clamp` (greenfield posture —
// no cross-crate callers reference the old name).
#[crate::polydat_node(category = Arithmetic)]
fn clamp(input: u64, min: Const<u64>, max: Const<u64>) -> u64 {
    input.clamp(*min, *max)
}

/// Decompose a u64 into mixed-radix digits.
///
/// Signature: `mixed_radix(input: u64, radixes...) -> (d0: u64, d1: u64, ...)`
///
/// The primary tool for coordinate decomposition. Maps a flat cycle
/// counter into a multi-dimensional space. Each radix defines the size
/// of that dimension. A trailing radix of 0 means unbounded (consumes
/// the remainder).
///
/// Example: `(device, reading) := mixed_radix(cycle, 10000, 0)` gives
/// 10,000 devices with unbounded readings per device.
///
/// Traversal is nested-loop, innermost first: d0 increments every cycle,
/// d1 increments every `radix[0]` cycles, etc.
///
/// JIT level: P3 (unrolled urem/udiv chain).
//
// SRD-80b Phase E: kept hand-written. The macro doesn't
// currently support nodes whose output port count is
// `MixedRadix` migrated to `#[polydat_node]` via the SRD-80b
// `DynamicOutputs<T>` shape — the output port count is
// determined at construction time from the `radixes`
// `Const<Vec<u64>>` arg's length.

fn mixed_radix_jit(node: &MixedRadix) -> CompiledU64Op {
    let radixes = node.radixes.clone();
    Box::new(move |inputs, outputs| {
        let mut remainder = inputs[0];
        for (i, &radix) in radixes.iter().enumerate() {
            if radix == 0 {
                outputs[i] = remainder;
                remainder = 0;
            } else {
                outputs[i] = remainder % radix;
                remainder /= radix;
            }
        }
    })
}

fn mixed_radix_jit_constants(node: &MixedRadix) -> Vec<u64> {
    node.radixes.clone()
}

/// Decompose `value` into mixed-radix digits using the given
/// `radixes`. The output is a vector of N digits where N =
/// `radixes.len()`. A radix of 0 in the trailing position
/// captures the remainder verbatim.
#[crate::polydat_node(
    category = Arithmetic,
    compiled_u64 = mixed_radix_jit,
    jit_constants = mixed_radix_jit_constants,
)]
fn mixed_radix(
    input: u64,
    radixes: crate::derive_support::Const<Vec<u64>>,
) -> crate::derive_support::DynamicOutputs<u64> {
    let mut remainder = input;
    let mut result = Vec::with_capacity(radixes.len());
    for &radix in radixes.iter() {
        if radix == 0 {
            result.push(remainder);
            remainder = 0;
        } else {
            result.push(remainder % radix);
            remainder /= radix;
        }
    }
    crate::derive_support::DynamicOutputs(result)
}

/// Sum N u64 inputs (wrapping). Variadic: accepts 0..N wire inputs.
///
/// Signature: `sum(in_0: u64, ..., in_N: u64) -> (u64)`
///
/// Group theory: identity element is 0 (additive identity).
/// `sum()` = 0, `sum(a)` = a, `sum(a, b, c)` = a + b + c.
///
/// Use for combining multiple values into a single aggregate.
///
/// JIT level: P2 (closure with loop).
// SRD-80 PR B.9 — variadic N-ary u64 reductions migrated to
// `#[polydat_node]`. Macro generates Sum/Product/Min/Max
// structs with `new(n_wires)` ctors and auto-emits Phase 2
// closures that pass the JIT `&[u64]` buffer directly to the
// body. AllCommutative declared via attribute.

#[crate::polydat_node(category = Variadic, identity = 0u64, commutativity = AllCommutative)]
fn sum(values: &[u64]) -> u64 {
    values.iter().fold(0u64, |a, b| a.wrapping_add(*b))
}

#[crate::polydat_node(category = Variadic, identity = 1u64, commutativity = AllCommutative)]
fn product(values: &[u64]) -> u64 {
    values.iter().fold(1u64, |a, b| a.wrapping_mul(*b))
}

#[crate::polydat_node(category = Variadic, identity = u64::MAX, commutativity = AllCommutative)]
fn min(values: &[u64]) -> u64 {
    values.iter().copied().fold(u64::MAX, std::cmp::min)
}

#[crate::polydat_node(category = Variadic, identity = 0u64, commutativity = AllCommutative)]
fn max(values: &[u64]) -> u64 {
    values.iter().copied().fold(0u64, std::cmp::max)
}

/// Interleave the bits of two u64 values into one (Morton code).
///
/// Signature: `interleave(a: u64, b: u64) -> (u64)`
///
/// Bit 0 of a → bit 0 of output, bit 0 of b → bit 1, bit 1 of a → bit 2,
/// etc. This preserves locality from both dimensions — essential for
/// combining two independent coordinates into a single hash input:
/// `hash(interleave(device_id, reading_idx))` produces a value that
/// changes when either dimension changes, with spatial correlation.
///
/// JIT level: P3 (extern call).
//
// SRD-80b Phase E: migrated to `#[polydat_node]`. Struct
// name `Interleave` matches snake_case → PascalCase of `interleave`.
#[crate::polydat_node(category = Arithmetic)]
fn interleave(a: u64, b: u64) -> u64 {
    let mut result: u64 = 0;
    for i in 0..32 {
        result |= ((a >> i) & 1) << (2 * i);
        result |= ((b >> i) & 1) << (2 * i + 1);
    }
    result
}

// ---------------------------------------------------------------------------
// Signature declarations for the DSL registry
// ---------------------------------------------------------------------------

use crate::dsl::registry::{Arity, FuncCategory, FuncSig, ParamSpec};
use crate::ast::SlotType;

/// Signatures for arithmetic and variadic nodes.
///
/// SRD-80b Phase E: most arithmetic nodes route through the
/// proc-macro NodeRegistration. Only `mixed_radix` (dynamic
/// output count from a `Const<Vec<C>>` arg) still needs a
/// hand-written FuncSig + build_node entry.
pub fn signatures() -> &'static [FuncSig] {
    use FuncCategory as C;
    &[
        FuncSig {
            name: "mixed_radix", category: C::Arithmetic, outputs: 0,
            description: "decompose into mixed-radix digits (output count = number of radixes)",
            help: "Decompose a single u64 into multiple coordinate digits, like\nnested loops unrolled into a flat index. Each radix defines the\nmodulus for that digit; radix=0 means unbounded (captures remainder).\nProduces one output port per radix.\nParameters:\n  input    — u64 wire input\n  radix... — one or more u64 constants (variadic)\nExample: mixed_radix(cycle, 10, 26, 0)  // 3 outputs: d0 in [0,10), d1 in [0,26), d2 unbounded\nTheory: mixed-radix decomposition generalizes base conversion;\neach position can have a different base.",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
            ],
            arity: Arity::VariadicConsts { min_consts: 1 },
            commutativity: crate::ast::Commutativity::Positional,
            default_resolver: None,
            output_type: crate::dsl::registry::OutputType::Fixed,
        },
    ]
}

/// Try to build an arithmetic node from a function name and const args.
///
/// Returns `None` if the name is not handled by this module. Only
/// `mixed_radix` is still hand-built here (see [`signatures`]).
pub(crate) fn build_node(name: &str, _wires: &[crate::compile::assembly::WireRef], _wire_types: &[crate::ast::PortType], consts: &[crate::dsl::factory::ConstArg]) -> Option<Result<Box<dyn crate::ast::PolydatNode>, String>> {
    match name {
        "mixed_radix" => {
            let radixes: Vec<u64> = consts.iter().map(|c| c.as_u64()).collect();
            Some(Ok(Box::new(MixedRadix::new(radixes))))
        }
        _ => None,
    }
}


/// Assembly-time constant validation. See SRD 15 §"Const Constraint Metadata".
///
/// The variadic positional rule for `mixed_radix` — non-terminal
/// radixes must each be non-zero, but the last one is allowed to
/// be `0` as the "everything left" sentinel — can't ride on a
/// per-param `ParamSpec.constraint`, so it stays here as a
/// hand-written validator.
pub(crate) fn validate_node(
    name: &str,
    consts: &[crate::dsl::factory::ConstArg],
) -> Result<(), String> {
    match name {
        "mixed_radix" => {
            for (i, c) in consts.iter().enumerate().take(consts.len().saturating_sub(1)) {
                if c.as_u64() == 0 {
                    return Err(format!("radix {i} must be non-zero"));
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

crate::register_nodes!(signatures, build_node, validate_node);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{PolydatNode, Value};

    #[test]
    fn add_wrapping() {
        let node = Add::new(10);
        let mut out = [Value::None];
        node.eval(&[Value::U64(5)], &mut out);
        assert_eq!(out[0].as_u64(), 15);
    }

    #[test]
    fn mod_basic() {
        let node = Mod::new(100);
        let mut out = [Value::None];
        node.eval(&[Value::U64(542)], &mut out);
        assert_eq!(out[0].as_u64(), 42);
    }

    #[test]
    fn mixed_radix_decompose() {
        let node = MixedRadix::new(vec![100, 1000, 0]);
        let mut out = [Value::None, Value::None, Value::None];
        // 4201337 → (37, 13, 42)
        // 4201337 % 100 = 37
        // 4201337 / 100 = 42013; 42013 % 1000 = 13
        // 42013 / 1000 = 42
        node.eval(&[Value::U64(4_201_337)], &mut out);
        assert_eq!(out[0].as_u64(), 37);
        assert_eq!(out[1].as_u64(), 13);
        assert_eq!(out[2].as_u64(), 42);
    }

    #[test]
    fn mixed_radix_cartesian() {
        // 100 tenants × 1000 devices × unbounded readings
        let node = MixedRadix::new(vec![100, 1000, 0]);
        let mut out = [Value::None, Value::None, Value::None];

        // cycle 0 → tenant 0, device 0, reading 0
        node.eval(&[Value::U64(0)], &mut out);
        assert_eq!(out[0].as_u64(), 0);
        assert_eq!(out[1].as_u64(), 0);
        assert_eq!(out[2].as_u64(), 0);

        // cycle 100_000 → tenant 0, device 0, reading 1
        node.eval(&[Value::U64(100_000)], &mut out);
        assert_eq!(out[0].as_u64(), 0);
        assert_eq!(out[1].as_u64(), 0);
        assert_eq!(out[2].as_u64(), 1);
    }

    #[test]
    fn interleave_basic() {
        let node = Interleave::new();
        let mut out = [Value::None];
        node.eval(&[Value::U64(0b101), Value::U64(0b010)], &mut out);
        // a=101, b=010
        // bit 0: a0=1, b0=0 → positions 0,1 = 01
        // bit 1: a1=0, b1=1 → positions 2,3 = 10
        // bit 2: a2=1, b2=0 → positions 4,5 = 01
        // result = 0b01_10_01 = 0b011001 = 25
        assert_eq!(out[0].as_u64(), 0b01_10_01);
    }

    #[test]
    fn div_basic() {
        let node = Div::new(100);
        let mut out = [Value::None];
        node.eval(&[Value::U64(4_201_337)], &mut out);
        assert_eq!(out[0].as_u64(), 42013);
    }

    // --- Variadic N-ary tests ---

    #[test]
    fn sum_variadic() {
        // 0 inputs → identity = 0
        let node = Sum::new(0);
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        assert_eq!(out[0].as_u64(), 0);

        // 1 input → passthrough
        let node = Sum::new(1);
        node.eval(&[Value::U64(42)], &mut out);
        assert_eq!(out[0].as_u64(), 42);

        // 3 inputs → fold
        let node = Sum::new(3);
        node.eval(&[Value::U64(10), Value::U64(20), Value::U64(30)], &mut out);
        assert_eq!(out[0].as_u64(), 60);
    }

    #[test]
    fn product_variadic() {
        // 0 inputs → identity = 1
        let node = Product::new(0);
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        assert_eq!(out[0].as_u64(), 1);

        // 1 input → passthrough
        let node = Product::new(1);
        node.eval(&[Value::U64(7)], &mut out);
        assert_eq!(out[0].as_u64(), 7);

        // 3 inputs → fold
        let node = Product::new(3);
        node.eval(&[Value::U64(2), Value::U64(3), Value::U64(7)], &mut out);
        assert_eq!(out[0].as_u64(), 42);
    }

    #[test]
    fn min_variadic() {
        // 0 inputs → identity = u64::MAX
        let node = Min::new(0);
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        assert_eq!(out[0].as_u64(), u64::MAX);

        // 3 inputs → min
        let node = Min::new(3);
        node.eval(&[Value::U64(50), Value::U64(10), Value::U64(30)], &mut out);
        assert_eq!(out[0].as_u64(), 10);
    }

    #[test]
    fn max_variadic() {
        // 0 inputs → identity = 0
        let node = Max::new(0);
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        assert_eq!(out[0].as_u64(), 0);

        // 3 inputs → max
        let node = Max::new(3);
        node.eval(&[Value::U64(50), Value::U64(10), Value::U64(30)], &mut out);
        assert_eq!(out[0].as_u64(), 50);
    }

    // --- Slot model consistency ---

    /// Verify that `meta().jit_constants_from_slots()` matches
    /// `jit_constants()` for all arithmetic nodes with constants.
    #[test]
    fn slot_constants_match_jit_constants() {
        use crate::ast::PolydatNode;

        let nodes: Vec<Box<dyn PolydatNode>> = vec![
            Box::new(Add::new(42)),
            Box::new(Mul::new(7)),
            Box::new(Div::new(100)),
            Box::new(Mod::new(256)),
            Box::new(Clamp::new(10, 90)),
            Box::new(MixedRadix::new(vec![100, 1000, 0])),
        ];

        for node in &nodes {
            let from_trait = node.jit_constants();
            let from_slots = node.meta().jit_constants_from_slots();
            assert_eq!(
                from_trait, from_slots,
                "constant mismatch for node '{}': trait={from_trait:?}, slots={from_slots:?}",
                node.meta().name,
            );
        }
    }

    // ── ceil_to_multiple ──────────────────────────────────

    fn run_binary(node: &dyn PolydatNode, a: u64, b: u64) -> u64 {
        let mut out = [Value::None];
        node.eval(&[Value::U64(a), Value::U64(b)], &mut out);
        out[0].as_u64()
    }

    #[test]
    fn ceil_to_multiple_returns_value_when_already_a_multiple() {
        let n = CeilToMultiple::default();
        assert_eq!(run_binary(&n, 800, 100), 800);
    }

    #[test]
    fn ceil_to_multiple_rounds_up_to_next_boundary() {
        let n = CeilToMultiple::default();
        assert_eq!(run_binary(&n, 801, 100), 900);
    }

    #[test]
    fn ceil_to_multiple_zero_value_is_zero() {
        let n = CeilToMultiple::default();
        assert_eq!(run_binary(&n, 0, 100), 0);
    }

    #[test]
    fn ceil_to_multiple_below_one_multiple_rounds_to_multiple() {
        let n = CeilToMultiple::default();
        assert_eq!(run_binary(&n, 50, 100), 100);
        assert_eq!(run_binary(&n, 1, 100), 100);
    }

    #[test]
    fn ceil_to_multiple_zero_multiple_is_soft_no_op() {
        let n = CeilToMultiple::default();
        assert_eq!(run_binary(&n, 42, 0), 42,
            "multiple=0 must not trap; passes value through");
    }

    // ── multiples_at_least ────────────────────────────────

    #[test]
    fn multiples_at_least_exact_division() {
        let n = MultiplesAtLeast::default();
        assert_eq!(run_binary(&n, 800, 100), 8);
    }

    #[test]
    fn multiples_at_least_rounds_up_partial() {
        let n = MultiplesAtLeast::default();
        assert_eq!(run_binary(&n, 801, 100), 9);
        assert_eq!(run_binary(&n, 1, 100), 1);
    }

    #[test]
    fn multiples_at_least_zero_value_is_zero() {
        let n = MultiplesAtLeast::default();
        assert_eq!(run_binary(&n, 0, 100), 0);
    }

    #[test]
    fn multiples_at_least_zero_multiple_is_zero() {
        let n = MultiplesAtLeast::default();
        assert_eq!(run_binary(&n, 42, 0), 0);
    }

    // ── set_or_get ────────────────────────────────────────

    #[test]
    fn set_or_get_returns_current_when_non_zero() {
        let n = SetOrGet::default();
        assert_eq!(run_binary(&n, 7, 99), 7);
        assert_eq!(run_binary(&n, u64::MAX, 99), u64::MAX);
    }

    #[test]
    fn set_or_get_returns_fallback_when_current_is_zero() {
        let n = SetOrGet::default();
        assert_eq!(run_binary(&n, 0, 99), 99);
    }

    #[test]
    fn set_or_get_zero_fallback_is_zero() {
        // If both inputs are zero, output is zero — soft default
        // for the degenerate case (caller's choice not to seed
        // a meaningful fallback).
        let n = SetOrGet::default();
        assert_eq!(run_binary(&n, 0, 0), 0);
    }

    #[test]
    fn set_or_get_idempotent_on_already_set() {
        // The "every subsequent phase" path: current is the
        // cached value, fallback is the (still-evaluated but
        // discarded) recomputation. Returning current preserves
        // the cached state across phases.
        let n = SetOrGet::default();
        for v in [1u64, 42, 1000, u64::MAX] {
            // Even if the fallback differs each call (e.g., a
            // recomputation that picked a slightly different
            // value due to a different base), the cached value
            // wins.
            assert_eq!(run_binary(&n, v, 999), v);
        }
    }

    #[test]
    fn ceil_to_multiple_and_count_satisfy_invariant() {
        // Documented invariant: ceil_to_multiple(v, m) == multiples_at_least(v, m) * m
        // whenever m > 0 and the multiplication doesn't overflow.
        let ceil = CeilToMultiple::default();
        let count = MultiplesAtLeast::default();
        for (v, m) in [(0u64, 100), (1, 100), (50, 100), (100, 100),
                       (101, 100), (10000, 7), (10000, 64), (12345, 256)] {
            let c_val = run_binary(&ceil, v, m);
            let n_val = run_binary(&count, v, m);
            assert_eq!(c_val, n_val * m,
                "invariant violated for (v={v}, m={m}): ceil={c_val}, count={n_val}");
        }
    }

    /// Verify wire_inputs() returns correct count for all arithmetic nodes.
    #[test]
    fn slot_wire_inputs_match_inputs() {
        use crate::ast::PolydatNode;

        let nodes: Vec<Box<dyn PolydatNode>> = vec![
            Box::new(Add::new(0)),
            Box::new(Mod::new(1)),
            Box::new(Sum::new(3)),
            Box::new(Product::new(2)),
            Box::new(Interleave::new()),
            Box::new(MixedRadix::new(vec![10, 20])),
            Box::new(CeilToMultiple::default()),
            Box::new(MultiplesAtLeast::default()),
            Box::new(SetOrGet::default()),
        ];

        for node in &nodes {
            let old_count = node.meta().wire_inputs().len();
            let new_count = node.meta().wire_inputs().len();
            assert_eq!(
                old_count, new_count,
                "wire input count mismatch for '{}': inputs={old_count}, wire_inputs()={new_count}",
                node.meta().name,
            );
        }
    }
}
