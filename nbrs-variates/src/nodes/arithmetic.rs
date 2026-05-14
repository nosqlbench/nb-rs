// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Arithmetic function nodes.
//!
//! Core integer operations for the GK DAG. These are the building blocks
//! that most workloads compose: hash → mod → add for bounded IDs,
//! mixed_radix for coordinate decomposition, interleave for combining
//! independent dimensions.

use crate::node::{
    Commutativity, CompiledU64Op,
    GkNode, NodeMeta, Port, Slot, Value,
};

/// Add a constant to a u64 value (wrapping).
///
/// Signature: `add(input: u64, addend: u64) -> (u64)`
///
/// Use for offsetting a bounded range: `mod(h, 100)` gives [0,100),
/// `add(mod(h, 100), 500)` gives [500,600). Also common with timestamps:
/// `add(base_epoch, offset)`.
///
/// JIT level: P3 (single `iadd` instruction).
pub struct AddU64 {
    meta: NodeMeta,
    addend: u64,
}

impl AddU64 {
    pub fn new(addend: u64) -> Self {
        Self {
            meta: NodeMeta {
                name: "add".into(),
                outs: vec![Port::u64("output")],
                ins: vec![
                    Slot::Wire(Port::u64("input")),
                    Slot::const_u64("addend", addend),
                ],
            },
            addend,
        }
    }
}

impl GkNode for AddU64 {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(inputs[0].as_u64().wrapping_add(self.addend));
    }
    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        let addend = self.addend;
        Some(Box::new(move |inputs, outputs| { outputs[0] = inputs[0].wrapping_add(addend); }))
    }
    fn jit_constants(&self) -> Vec<u64> { vec![self.addend] }
}

/// Multiply a u64 value by a constant (wrapping).
///
/// Signature: `mul(input: u64, factor: u64) -> (u64)`
///
/// Use for scaling counters to time intervals: `mul(reading_idx, 1000)`
/// converts a reading index to millisecond offsets. Wraps at 2^64.
///
/// JIT level: P3 (single `imul` instruction).
pub struct MulU64 {
    meta: NodeMeta,
    factor: u64,
}

impl MulU64 {
    pub fn new(factor: u64) -> Self {
        Self {
            meta: NodeMeta {
                name: "mul".into(),
                outs: vec![Port::u64("output")],
                ins: vec![
                    Slot::Wire(Port::u64("input")),
                    Slot::const_u64("factor", factor),
                ],
            },
            factor,
        }
    }
}

impl GkNode for MulU64 {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(inputs[0].as_u64().wrapping_mul(self.factor));
    }
    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        let factor = self.factor;
        Some(Box::new(move |inputs, outputs| { outputs[0] = inputs[0].wrapping_mul(factor); }))
    }
    fn jit_constants(&self) -> Vec<u64> { vec![self.factor] }
}

/// Divide a u64 value by a constant (integer division, toward zero).
///
/// Signature: `div(input: u64, divisor: u64) -> (u64)`
///
/// Use for coarsening: `div(cycle, 100)` groups 100 consecutive cycles
/// into one bucket. Panics at construction if divisor is 0.
///
/// JIT level: P3 (single `udiv` instruction).
pub struct DivU64 {
    meta: NodeMeta,
    divisor: u64,
}

impl DivU64 {
    pub fn new(divisor: u64) -> Self {
        assert!(divisor != 0, "divisor must not be zero");
        Self {
            meta: NodeMeta {
                name: "div".into(),
                outs: vec![Port::u64("output")],
                ins: vec![
                    Slot::Wire(Port::u64("input")),
                    Slot::const_u64("divisor", divisor),
                ],
            },
            divisor,
        }
    }
}

impl GkNode for DivU64 {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(inputs[0].as_u64() / self.divisor);
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        let divisor = self.divisor;
        Some(Box::new(move |inputs, outputs| { outputs[0] = inputs[0] / divisor; }))
    }
    fn jit_constants(&self) -> Vec<u64> { vec![self.divisor] }
}

/// Modulo of a u64 value by a constant. Result in [0, modulus).
///
/// Signature: `mod(input: u64, modulus: u64) -> (u64)`
///
/// The most common operation after hash. `mod(hash(cycle), N)` gives a
/// uniformly distributed integer in [0, N). Also used for cyclic patterns:
/// `mod(cycle, period)` repeats every `period` cycles.
///
/// JIT level: P3 (single `urem` instruction).
pub struct ModU64 {
    meta: NodeMeta,
    modulus: u64,
}

impl ModU64 {
    pub fn new(modulus: u64) -> Self {
        assert!(modulus != 0, "modulus must not be zero");
        Self {
            meta: NodeMeta {
                name: "mod".into(),
                outs: vec![Port::u64("output")],
                ins: vec![
                    Slot::Wire(Port::u64("input")),
                    Slot::const_u64("modulus", modulus),
                ],
            },
            modulus,
        }
    }
}

impl GkNode for ModU64 {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(inputs[0].as_u64() % self.modulus);
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        let modulus = self.modulus;
        Some(Box::new(move |inputs, outputs| {
            outputs[0] = inputs[0] % modulus;
        }))
    }

    fn jit_constants(&self) -> Vec<u64> {
        vec![self.modulus]
    }
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
/// the const case, prefer [`ModU64`] which is faster (the divisor
/// is baked into the JIT closure as a constant).
///
/// JIT level: P2 (compiled_u64 closure; not const-foldable).
pub struct ModWireU64 {
    meta: NodeMeta,
}

impl Default for ModWireU64 {
    fn default() -> Self { Self::new() }
}

impl ModWireU64 {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "mod_wire".into(),
                outs: vec![Port::u64("output")],
                ins: vec![
                    Slot::Wire(Port::u64("input")),
                    // The divisor wire declares its non-zero
                    // contract here. Strict-wire mode auto-inserts
                    // an `assert_u64_nonzero` upstream when the
                    // source can't statically guarantee it.
                    Slot::Wire(
                        Port::u64("divisor")
                            .with_constraint(crate::dsl::const_constraints::ConstConstraint::NonZeroU64),
                    ),
                ],
            },
        }
    }
}

impl GkNode for ModWireU64 {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let divisor = inputs[1].as_u64();
        // Trust the divisor by contract. Bad input is the user's
        // problem unless they opted into strict_values, in which
        // case the upstream assertion has already rejected zero.
        outputs[0] = Value::U64(inputs[0].as_u64() % divisor);
    }
    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        Some(Box::new(|inputs, outputs| {
            outputs[0] = inputs[0] % inputs[1];
        }))
    }
}

/// Division of a u64 value by a *wire-fed* divisor.
///
/// Signature: `div_wire(input: u64, divisor: u64) -> (u64)`
///
/// Sibling of [`ModWireU64`] for integer division. Same wire-input
/// contract: divisor must be non-zero. Strict-wire mode auto-wires
/// the assertion. JIT level: P2.
pub struct DivWireU64 {
    meta: NodeMeta,
}

impl Default for DivWireU64 {
    fn default() -> Self { Self::new() }
}

impl DivWireU64 {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "div_wire".into(),
                outs: vec![Port::u64("output")],
                ins: vec![
                    Slot::Wire(Port::u64("input")),
                    Slot::Wire(
                        Port::u64("divisor")
                            .with_constraint(crate::dsl::const_constraints::ConstConstraint::NonZeroU64),
                    ),
                ],
            },
        }
    }
}

impl GkNode for DivWireU64 {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(inputs[0].as_u64() / inputs[1].as_u64());
    }
    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        Some(Box::new(|inputs, outputs| {
            outputs[0] = inputs[0] / inputs[1];
        }))
    }
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
pub struct CeilToMultipleU64 {
    meta: NodeMeta,
}

impl Default for CeilToMultipleU64 {
    fn default() -> Self { Self::new() }
}

impl CeilToMultipleU64 {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "ceil_to_multiple".into(),
                outs: vec![Port::u64("output")],
                ins: vec![
                    Slot::Wire(Port::u64("value")),
                    Slot::Wire(Port::u64("multiple")),
                ],
            },
        }
    }
}

impl GkNode for CeilToMultipleU64 {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let value = inputs[0].as_u64();
        let multiple = inputs[1].as_u64();
        outputs[0] = Value::U64(if multiple == 0 {
            value
        } else {
            value.div_ceil(multiple).saturating_mul(multiple)
        });
    }
    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        Some(Box::new(|inputs, outputs| {
            let value = inputs[0];
            let multiple = inputs[1];
            outputs[0] = if multiple == 0 {
                value
            } else {
                value.div_ceil(multiple).saturating_mul(multiple)
            };
        }))
    }
}

/// Count of multiples of `multiple` needed to cover `value`.
///
/// Signature: `multiples_at_least(value: u64, multiple: u64) -> (u64)`
///
/// Companion to [`CeilToMultipleU64`] that returns the *count*
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
pub struct MultiplesAtLeastU64 {
    meta: NodeMeta,
}

impl Default for MultiplesAtLeastU64 {
    fn default() -> Self { Self::new() }
}

impl MultiplesAtLeastU64 {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "multiples_at_least".into(),
                outs: vec![Port::u64("output")],
                ins: vec![
                    Slot::Wire(Port::u64("value")),
                    Slot::Wire(Port::u64("multiple")),
                ],
            },
        }
    }
}

impl GkNode for MultiplesAtLeastU64 {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let value = inputs[0].as_u64();
        let multiple = inputs[1].as_u64();
        outputs[0] = Value::U64(if multiple == 0 { 0 } else { value.div_ceil(multiple) });
    }
    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        Some(Box::new(|inputs, outputs| {
            let value = inputs[0];
            let multiple = inputs[1];
            outputs[0] = if multiple == 0 { 0 } else { value.div_ceil(multiple) };
        }))
    }
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
pub struct SetOrGetU64 {
    meta: NodeMeta,
}

impl Default for SetOrGetU64 {
    fn default() -> Self { Self::new() }
}

impl SetOrGetU64 {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "set_or_get".into(),
                outs: vec![Port::u64("output")],
                ins: vec![
                    Slot::Wire(Port::u64("current")),
                    Slot::Wire(Port::u64("fallback")),
                ],
            },
        }
    }
}

impl GkNode for SetOrGetU64 {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let current = inputs[0].as_u64();
        let fallback = inputs[1].as_u64();
        outputs[0] = Value::U64(if current == 0 { fallback } else { current });
    }
    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        Some(Box::new(|inputs, outputs| {
            let current = inputs[0];
            let fallback = inputs[1];
            outputs[0] = if current == 0 { fallback } else { current };
        }))
    }
}

/// Clamp an unsigned integer to [min, max].
///
/// Signature: `clamp(input: u64, min: u64, max: u64) -> (u64)`
///
/// Unlike mod (which wraps), clamp saturates at the boundary. Use when
/// you want values to pile up at the edges rather than wrap around.
///
/// JIT level: P3 (`umax` + `umin`).
pub struct ClampU64 {
    meta: NodeMeta,
    min: u64,
    max: u64,
}

impl ClampU64 {
    pub fn new(min: u64, max: u64) -> Self {
        Self {
            meta: NodeMeta {
                name: "clamp".into(),
                outs: vec![Port::u64("output")],
                ins: vec![
                    Slot::Wire(Port::u64("input")),
                    Slot::const_u64("min", min),
                    Slot::const_u64("max", max),
                ],
            },
            min,
            max,
        }
    }
}

impl GkNode for ClampU64 {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(inputs[0].as_u64().clamp(self.min, self.max));
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        let min = self.min;
        let max = self.max;
        Some(Box::new(move |inputs, outputs| { outputs[0] = inputs[0].clamp(min, max); }))
    }
    fn jit_constants(&self) -> Vec<u64> { vec![self.min, self.max] }
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
pub struct MixedRadix {
    meta: NodeMeta,
    radixes: Vec<u64>,
}

impl MixedRadix {
    pub fn new(radixes: Vec<u64>) -> Self {
        let outputs: Vec<Port> = radixes
            .iter()
            .enumerate()
            .map(|(i, _)| Port::u64(format!("d{i}")))
            .collect();
        let slots = vec![
            Slot::Wire(Port::u64("input")),
            Slot::const_vec_u64("radixes", radixes.clone()),
        ];
        Self {
            meta: NodeMeta {
                name: "mixed_radix".into(),
                outs: outputs,
                ins: slots,
            },
            radixes,
        }
    }
}

impl GkNode for MixedRadix {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let mut remainder = inputs[0].as_u64();
        for (i, &radix) in self.radixes.iter().enumerate() {
            if radix == 0 {
                outputs[i] = Value::U64(remainder);
                remainder = 0;
            } else {
                outputs[i] = Value::U64(remainder % radix);
                remainder /= radix;
            }
        }
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        let radixes = self.radixes.clone();
        Some(Box::new(move |inputs, outputs| {
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
        }))
    }

    fn jit_constants(&self) -> Vec<u64> { self.radixes.clone() }
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
pub struct SumN {
    meta: NodeMeta,
}

impl SumN {
    pub fn new(n: usize) -> Self {
        let slots: Vec<Slot> = (0..n)
            .map(|i| Slot::Wire(Port::u64(format!("in_{i}"))))
            .collect();
        Self {
            meta: NodeMeta {
                name: "sum".into(),
                outs: vec![Port::u64("output")],
                ins: slots,
            },
        }
    }
}

impl GkNode for SumN {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    fn commutativity(&self) -> Commutativity { Commutativity::AllCommutative }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let mut acc: u64 = 0;
        for input in inputs {
            acc = acc.wrapping_add(input.as_u64());
        }
        outputs[0] = Value::U64(acc);
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        Some(Box::new(|inputs, outputs| {
            let mut acc: u64 = 0;
            for &v in inputs {
                acc = acc.wrapping_add(v);
            }
            outputs[0] = acc;
        }))
    }
}

/// Multiply N u64 inputs (wrapping). Variadic: accepts 0..N wire inputs.
///
/// Signature: `product(in_0: u64, ..., in_N: u64) -> (u64)`
///
/// Group theory: identity element is 1 (multiplicative identity).
/// `product()` = 1, `product(a)` = a, `product(a, b)` = a * b.
///
/// JIT level: P2 (closure with loop).
pub struct ProductN {
    meta: NodeMeta,
}

impl ProductN {
    pub fn new(n: usize) -> Self {
        let slots: Vec<Slot> = (0..n)
            .map(|i| Slot::Wire(Port::u64(format!("in_{i}"))))
            .collect();
        Self {
            meta: NodeMeta {
                name: "product".into(),
                outs: vec![Port::u64("output")],
                ins: slots,
            },
        }
    }
}

impl GkNode for ProductN {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn commutativity(&self) -> Commutativity { Commutativity::AllCommutative }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let mut acc: u64 = 1;
        for input in inputs {
            acc = acc.wrapping_mul(input.as_u64());
        }
        outputs[0] = Value::U64(acc);
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        Some(Box::new(|inputs, outputs| {
            let mut acc: u64 = 1;
            for &v in inputs { acc = acc.wrapping_mul(v); }
            outputs[0] = acc;
        }))
    }
}

/// Minimum of N u64 inputs. Variadic: accepts 0..N wire inputs.
///
/// Signature: `min(in_0: u64, ..., in_N: u64) -> (u64)`
///
/// Lattice: identity element is u64::MAX (top element).
/// `min()` = u64::MAX, `min(a)` = a, `min(a, b, c)` = smallest.
///
/// JIT level: P2 (closure with loop).
pub struct MinN {
    meta: NodeMeta,
}

impl MinN {
    pub fn new(n: usize) -> Self {
        let slots: Vec<Slot> = (0..n)
            .map(|i| Slot::Wire(Port::u64(format!("in_{i}"))))
            .collect();
        Self {
            meta: NodeMeta {
                name: "min".into(),
                outs: vec![Port::u64("output")],
                ins: slots,
            },
        }
    }
}

impl GkNode for MinN {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn commutativity(&self) -> Commutativity { Commutativity::AllCommutative }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let mut acc: u64 = u64::MAX;
        for input in inputs {
            acc = acc.min(input.as_u64());
        }
        outputs[0] = Value::U64(acc);
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        Some(Box::new(|inputs, outputs| {
            let mut acc: u64 = u64::MAX;
            for &v in inputs { acc = acc.min(v); }
            outputs[0] = acc;
        }))
    }
}

/// Maximum of N u64 inputs. Variadic: accepts 0..N wire inputs.
///
/// Signature: `max(in_0: u64, ..., in_N: u64) -> (u64)`
///
/// Lattice: identity element is 0 (bottom element).
/// `max()` = 0, `max(a)` = a, `max(a, b, c)` = largest.
///
/// JIT level: P2 (closure with loop).
pub struct MaxN {
    meta: NodeMeta,
}

impl MaxN {
    pub fn new(n: usize) -> Self {
        let slots: Vec<Slot> = (0..n)
            .map(|i| Slot::Wire(Port::u64(format!("in_{i}"))))
            .collect();
        Self {
            meta: NodeMeta {
                name: "max".into(),
                outs: vec![Port::u64("output")],
                ins: slots,
            },
        }
    }
}

impl GkNode for MaxN {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn commutativity(&self) -> Commutativity { Commutativity::AllCommutative }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let mut acc: u64 = 0;
        for input in inputs {
            acc = acc.max(input.as_u64());
        }
        outputs[0] = Value::U64(acc);
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        Some(Box::new(|inputs, outputs| {
            let mut acc: u64 = 0;
            for &v in inputs { acc = acc.max(v); }
            outputs[0] = acc;
        }))
    }
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
pub struct Interleave {
    meta: NodeMeta,
}

impl Default for Interleave {
    fn default() -> Self {
        Self::new()
    }
}

impl Interleave {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "interleave".into(),
                outs: vec![Port::u64("output")],
                ins: vec![
                    Slot::Wire(Port::u64("a")),
                    Slot::Wire(Port::u64("b")),
                ],
            },
        }
    }
}

impl GkNode for Interleave {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let a = inputs[0].as_u64();
        let b = inputs[1].as_u64();
        let mut result: u64 = 0;
        for i in 0..32 {
            result |= ((a >> i) & 1) << (2 * i);
            result |= ((b >> i) & 1) << (2 * i + 1);
        }
        outputs[0] = Value::U64(result);
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        Some(Box::new(|inputs, outputs| {
            let (a, b) = (inputs[0], inputs[1]);
            let mut result: u64 = 0;
            for i in 0..32 {
                result |= ((a >> i) & 1) << (2 * i);
                result |= ((b >> i) & 1) << (2 * i + 1);
            }
            outputs[0] = result;
        }))
    }
}

// ---------------------------------------------------------------------------
// Signature declarations for the DSL registry
// ---------------------------------------------------------------------------

use crate::dsl::registry::{Arity, FuncCategory, FuncSig, ParamSpec};
use crate::node::SlotType;

/// Signatures for arithmetic and variadic nodes.
pub fn signatures() -> &'static [FuncSig] {
    use FuncCategory as C;
    &[
        // --- Variadic arithmetic ---
        FuncSig {
            name: "sum", category: C::Variadic, outputs: 1,
            description: "sum of N inputs (wrapping); identity = 0",
            help: "Wrapping addition of N wire inputs. With zero inputs returns 0.\nUseful for combining multiple independently generated components.\nParameters:\n  input... — any number of u64 wire inputs\nExample: sum(hash(cycle), hash(add(cycle, 1000)))\nIdentity element is 0. Overflow wraps at 2^64.",
            identity: Some(0),
            variadic_ctor: Some(|n| Box::new(SumN::new(n))),
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: false, example: "cycle", constraint: None }],
            arity: Arity::VariadicWires { min_wires: 0 },
            commutativity: crate::node::Commutativity::AllCommutative,
            default_resolver: None,
            output_type: crate::dsl::registry::OutputType::Fixed,
        },
        FuncSig {
            name: "product", category: C::Variadic, outputs: 1,
            description: "product of N inputs (wrapping); identity = 1",
            help: "Wrapping multiplication of N wire inputs. With zero inputs returns 1.\nUseful for combining independent scaling factors.\nParameters:\n  input... — any number of u64 wire inputs\nExample: product(hash(cycle), mod(cycle, 10))\nIdentity element is 1. Overflow wraps at 2^64.",
            identity: Some(1),
            variadic_ctor: Some(|n| Box::new(ProductN::new(n))),
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: false, example: "cycle", constraint: None }],
            arity: Arity::VariadicWires { min_wires: 0 },
            commutativity: crate::node::Commutativity::AllCommutative,
            default_resolver: None,
            output_type: crate::dsl::registry::OutputType::Fixed,
        },
        FuncSig {
            name: "min", category: C::Variadic, outputs: 1,
            description: "minimum of N inputs; identity = u64::MAX",
            help: "Returns the smallest of N wire inputs. With zero inputs returns u64::MAX.\nUseful for clamping to the lowest of several generated bounds.\nParameters:\n  input... — any number of u64 wire inputs\nExample: min(hash(cycle), mod(cycle, 1000))\nIdentity element is u64::MAX.",
            identity: Some(u64::MAX),
            variadic_ctor: Some(|n| Box::new(MinN::new(n))),
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: false, example: "cycle", constraint: None }],
            arity: Arity::VariadicWires { min_wires: 0 },
            commutativity: crate::node::Commutativity::AllCommutative,
            default_resolver: None,
            output_type: crate::dsl::registry::OutputType::Fixed,
        },
        FuncSig {
            name: "max", category: C::Variadic, outputs: 1,
            description: "maximum of N inputs; identity = 0",
            help: "Returns the largest of N wire inputs. With zero inputs returns 0.\nUseful for selecting the highest of several generated values.\nParameters:\n  input... — any number of u64 wire inputs\nExample: max(hash(cycle), mod(cycle, 500))\nIdentity element is 0.",
            identity: Some(0),
            variadic_ctor: Some(|n| Box::new(MaxN::new(n))),
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: false, example: "cycle", constraint: None }],
            arity: Arity::VariadicWires { min_wires: 0 },
            commutativity: crate::node::Commutativity::AllCommutative,
            default_resolver: None,
            output_type: crate::dsl::registry::OutputType::Fixed,
        },

        // --- Arithmetic ---
        FuncSig {
            name: "add", category: C::Arithmetic,
            outputs: 1, description: "add a constant (wrapping)",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
                ParamSpec { name: "addend", slot_type: SlotType::ConstU64, required: true, example: "10", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Add a constant to a u64 value using wrapping arithmetic.\nUseful for offsetting ranges or shifting cycle ordinals.\nParameters:\n  input  — u64 wire input\n  addend — constant to add (wraps at 2^64)\nExample: add(hash(cycle), 1000000)",
            default_resolver: None,
            output_type: crate::dsl::registry::OutputType::Fixed,
        },
        FuncSig {
            name: "mul", category: C::Arithmetic,
            outputs: 1, description: "multiply by a constant (wrapping)",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
                ParamSpec { name: "factor", slot_type: SlotType::ConstU64, required: true, example: "10", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Multiply a u64 value by a constant using wrapping arithmetic.\nUseful for scaling counters or spreading values across a stride.\nParameters:\n  input  — u64 wire input\n  factor — constant multiplier (wraps at 2^64)\nExample: mul(cycle, 7)",
            default_resolver: None,
            output_type: crate::dsl::registry::OutputType::Fixed,
        },
        FuncSig {
            name: "div", category: C::Arithmetic,
            outputs: 1, description: "divide by a constant",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
                ParamSpec { name: "divisor", slot_type: SlotType::ConstU64, required: true, example: "10",
                    constraint: Some(crate::dsl::const_constraints::ConstConstraint::NonZeroU64) },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Integer division by a constant (truncating toward zero).\nUseful for coarsening values — e.g., grouping cycles into blocks.\nParameters:\n  input   — u64 wire input\n  divisor — constant divisor (must be > 0)\nExample: div(cycle, 100)  // groups into blocks of 100",
            default_resolver: None,
            output_type: crate::dsl::registry::OutputType::Fixed,
        },
        FuncSig {
            name: "mod", category: C::Arithmetic,
            outputs: 1, description: "modulo by a constant",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
                ParamSpec { name: "modulus", slot_type: SlotType::ConstU64, required: true, example: "1000",
                    constraint: Some(crate::dsl::const_constraints::ConstConstraint::NonZeroU64) },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Modular reduction: output = input % modulus, producing [0, K).\nThe most common operation after hash — bounds a hashed value\ninto a usable integer range.\nParameters:\n  input   — u64 wire input (typically hashed)\n  modulus — upper bound (exclusive, must be > 0)\nExample: mod(hash(cycle), 1000)  // yields 0..999",
            default_resolver: None,
            output_type: crate::dsl::registry::OutputType::Fixed,
        },
        FuncSig {
            name: "mod_wire", category: C::Arithmetic,
            outputs: 1, description: "modulo by a wire-fed divisor",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
                ParamSpec { name: "divisor", slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Modulo by a wire-fed divisor. Sibling of `mod` for cases where\nthe divisor varies per cycle (e.g. driven by a control or a\nruntime-derived shard count). The divisor port declares a\n`NonZeroU64` constraint, so under `// @pragma: strict_values`\nthe compiler auto-inserts an `assert_u64_nonzero` upstream.\nWithout strict mode the node trusts the divisor; a zero panics.\nParameters:\n  input   — u64 wire input\n  divisor — u64 wire input (non-zero)\nExample: shard := mod_wire(cycle, concurrency())",
            default_resolver: None,
            output_type: crate::dsl::registry::OutputType::Fixed,
        },
        FuncSig {
            name: "div_wire", category: C::Arithmetic,
            outputs: 1, description: "divide by a wire-fed divisor",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
                ParamSpec { name: "divisor", slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Integer division by a wire-fed divisor. Sibling of `div`. Same\nnon-zero contract on the `divisor` wire. Use when the divisor\ngenuinely varies per cycle.\nParameters:\n  input   — u64 wire input\n  divisor — u64 wire input (non-zero)\nExample: bucket := div_wire(cycle, partition_size())",
            default_resolver: None,
            output_type: crate::dsl::registry::OutputType::Fixed,
        },
        FuncSig {
            name: "ceil_to_multiple", category: C::Arithmetic,
            outputs: 1, description: "smallest multiple of `multiple` that is ≥ `value`",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "value", slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
                ParamSpec { name: "multiple", slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Round `value` UP to the nearest multiple of `multiple`.\nUseful for cycle counts (smallest whole-pass cycle count\nmeeting a minimum), alignment (pad to chunk boundary),\nbucketing (snap to next bin edge).\nReturns `value` unchanged if `multiple == 0` (soft no-op so a\ntransient zero from a wire-bound extern doesn't trap).\nParameters:\n  value    — u64 wire input\n  multiple — u64 wire input (multiplier base)\nExample: cycles := ceil_to_multiple(min_cycles, base)",
            default_resolver: None,
            output_type: crate::dsl::registry::OutputType::Fixed,
        },
        FuncSig {
            name: "multiples_at_least", category: C::Arithmetic,
            outputs: 1, description: "count of `multiple`s needed to cover `value`",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "value", slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
                ParamSpec { name: "multiple", slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Number of `multiple`-sized chunks needed to cover `value`:\nceil(value / multiple). Companion to ceil_to_multiple — returns\nthe count instead of the rounded value.\nReturns 0 if `multiple == 0`.\nParameters:\n  value    — u64 wire input\n  multiple — u64 wire input (chunk size)\nExample: passes := multiples_at_least(min_cycles, base)",
            default_resolver: None,
            output_type: crate::dsl::registry::OutputType::Fixed,
        },
        FuncSig {
            name: "set_or_get", category: C::Arithmetic,
            outputs: 1, description: "first non-zero of (current, fallback)",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "current", slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
                ParamSpec { name: "fallback", slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Returns `current` if non-zero, else `fallback`. Use with\nSRD-13f shared wires for cross-scope memoization:\n  shared X := set_or_get(X, expensive_computation())\nFirst evaluation: X reads 0, returns fallback, broadcast\nwrites it to the parent's SharedCell. Subsequent\nevaluations: X reads the cached value, fallback is\ndiscarded. Concurrency-safe via the SharedCell mutex.\nParameters:\n  current  — u64 wire input (typically a shared-bound slot)\n  fallback — u64 wire input (computed value to use when current==0)\nExample: passes := set_or_get(query_passes, multiples_at_least(min, base))",
            default_resolver: None,
            output_type: crate::dsl::registry::OutputType::Fixed,
        },
        FuncSig {
            name: "clamp", category: C::Arithmetic,
            outputs: 1, description: "clamp u64 to [min, max]",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
                ParamSpec { name: "min", slot_type: SlotType::ConstU64, required: true, example: "100", constraint: None },
                ParamSpec { name: "max", slot_type: SlotType::ConstU64, required: true, example: "100", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Saturating clamp: values below min become min, above max become max.\nUnlike mod (which wraps), clamp preserves relative ordering within\nthe valid range. Use when you need hard bounds without wrap-around.\nParameters:\n  input — u64 wire input\n  min   — lower bound (inclusive)\n  max   — upper bound (inclusive)\nExample: clamp(hash(cycle), 10, 500)",
            default_resolver: None,
            output_type: crate::dsl::registry::OutputType::Fixed,
        },
        FuncSig {
            name: "interleave", category: C::Arithmetic,
            outputs: 1, description: "interleave bits of two u64 values",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "a", slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
                ParamSpec { name: "b", slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Interleave the bits of two u64 values into a single u64 (Morton code).\nBit 0 of a goes to bit 0, bit 0 of b goes to bit 1, bit 1 of a to bit 2, etc.\nUseful for combining two independent coordinates into one value\nthat preserves spatial locality.\nParameters:\n  a — first u64 wire input (even bits in output)\n  b — second u64 wire input (odd bits in output)\nExample: hash(interleave(x_coord, y_coord))",
            default_resolver: None,
            output_type: crate::dsl::registry::OutputType::Fixed,
        },
        FuncSig {
            name: "mixed_radix", category: C::Arithmetic, outputs: 0,
            description: "decompose into mixed-radix digits (output count = number of radixes)",
            help: "Decompose a single u64 into multiple coordinate digits, like\nnested loops unrolled into a flat index. Each radix defines the\nmodulus for that digit; radix=0 means unbounded (captures remainder).\nProduces one output port per radix.\nParameters:\n  input    — u64 wire input\n  radix... — one or more u64 constants (variadic)\nExample: mixed_radix(cycle, 10, 26, 0)  // 3 outputs: d0 in [0,10), d1 in [0,26), d2 unbounded\nTheory: mixed-radix decomposition generalizes base conversion;\neach position can have a different base.",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
            ],
            arity: Arity::VariadicConsts { min_consts: 1 },
            commutativity: crate::node::Commutativity::Positional,
            default_resolver: None,
            output_type: crate::dsl::registry::OutputType::Fixed,
        },
        FuncSig {
            name: "identity", category: C::Arithmetic, outputs: 1,
            description: "passthrough",
            help: "Passes the input value through unchanged.\nUseful for debugging, naming intermediate values, or as a\nplaceholder during graph construction.\nParameters:\n  input — any wire value\nExample: identity(hash(cycle))  // same as hash(cycle)",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            default_resolver: None,
            output_type: crate::dsl::registry::OutputType::Fixed,
        },
    ]
}

/// Try to build an arithmetic node from a function name and const args.
///
/// Returns `None` if the name is not handled by this module.
pub(crate) fn build_node(name: &str, _wires: &[crate::assembly::WireRef], _wire_types: &[crate::node::PortType], consts: &[crate::dsl::factory::ConstArg]) -> Option<Result<Box<dyn crate::node::GkNode>, String>> {
    match name {
        "add" => Some(Ok(Box::new(AddU64::new(consts.first().map(|c| c.as_u64()).unwrap_or(0))))),
        "mul" => Some(Ok(Box::new(MulU64::new(consts.first().map(|c| c.as_u64()).unwrap_or(1))))),
        "div" => Some(Ok(Box::new(DivU64::new(consts.first().map(|c| c.as_u64()).unwrap_or(1))))),
        "mod" => Some(Ok(Box::new(ModU64::new(consts.first().map(|c| c.as_u64()).unwrap_or(1))))),
        "mod_wire" => Some(Ok(Box::new(ModWireU64::new()))),
        "div_wire" => Some(Ok(Box::new(DivWireU64::new()))),
        "ceil_to_multiple" => Some(Ok(Box::new(CeilToMultipleU64::new()))),
        "multiples_at_least" => Some(Ok(Box::new(MultiplesAtLeastU64::new()))),
        "set_or_get" => Some(Ok(Box::new(SetOrGetU64::new()))),
        "clamp" => Some(Ok(Box::new(ClampU64::new(
            consts.first().map(|c| c.as_u64()).unwrap_or(0),
            consts.get(1).map(|c| c.as_u64()).unwrap_or(u64::MAX),
        )))),
        "interleave" => Some(Ok(Box::new(Interleave::new()))),
        "mixed_radix" => {
            let radixes: Vec<u64> = consts.iter().map(|c| c.as_u64()).collect();
            Some(Ok(Box::new(MixedRadix::new(radixes))))
        }
        _ => None,
    }
}


/// Assembly-time constant validation. See SRD 15 §"Const Constraint Metadata".
///
/// `div` and `mod` declare `NonZeroU64` on their constant param;
/// Pass 1 enforces those before this validator runs. The only
/// rule left here is `mixed_radix`'s variadic positional check —
/// the non-terminal radixes must each be non-zero, but the last
/// one is allowed to be `0` as the "everything left" sentinel,
/// and that variadic positional rule can't ride on a per-param
/// `ParamSpec.constraint`.
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

    #[test]
    fn add_wrapping() {
        let node = AddU64::new(10);
        let mut out = [Value::None];
        node.eval(&[Value::U64(5)], &mut out);
        assert_eq!(out[0].as_u64(), 15);
    }

    #[test]
    fn mod_basic() {
        let node = ModU64::new(100);
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
        let node = DivU64::new(100);
        let mut out = [Value::None];
        node.eval(&[Value::U64(4_201_337)], &mut out);
        assert_eq!(out[0].as_u64(), 42013);
    }

    // --- Variadic N-ary tests ---

    #[test]
    fn sum_variadic() {
        // 0 inputs → identity = 0
        let node = SumN::new(0);
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        assert_eq!(out[0].as_u64(), 0);

        // 1 input → passthrough
        let node = SumN::new(1);
        node.eval(&[Value::U64(42)], &mut out);
        assert_eq!(out[0].as_u64(), 42);

        // 3 inputs → fold
        let node = SumN::new(3);
        node.eval(&[Value::U64(10), Value::U64(20), Value::U64(30)], &mut out);
        assert_eq!(out[0].as_u64(), 60);
    }

    #[test]
    fn product_variadic() {
        // 0 inputs → identity = 1
        let node = ProductN::new(0);
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        assert_eq!(out[0].as_u64(), 1);

        // 1 input → passthrough
        let node = ProductN::new(1);
        node.eval(&[Value::U64(7)], &mut out);
        assert_eq!(out[0].as_u64(), 7);

        // 3 inputs → fold
        let node = ProductN::new(3);
        node.eval(&[Value::U64(2), Value::U64(3), Value::U64(7)], &mut out);
        assert_eq!(out[0].as_u64(), 42);
    }

    #[test]
    fn min_variadic() {
        // 0 inputs → identity = u64::MAX
        let node = MinN::new(0);
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        assert_eq!(out[0].as_u64(), u64::MAX);

        // 3 inputs → min
        let node = MinN::new(3);
        node.eval(&[Value::U64(50), Value::U64(10), Value::U64(30)], &mut out);
        assert_eq!(out[0].as_u64(), 10);
    }

    #[test]
    fn max_variadic() {
        // 0 inputs → identity = 0
        let node = MaxN::new(0);
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        assert_eq!(out[0].as_u64(), 0);

        // 3 inputs → max
        let node = MaxN::new(3);
        node.eval(&[Value::U64(50), Value::U64(10), Value::U64(30)], &mut out);
        assert_eq!(out[0].as_u64(), 50);
    }

    // --- Slot model consistency ---

    /// Verify that `meta().jit_constants_from_slots()` matches
    /// `jit_constants()` for all arithmetic nodes with constants.
    #[test]
    fn slot_constants_match_jit_constants() {
        use crate::node::GkNode;

        let nodes: Vec<Box<dyn GkNode>> = vec![
            Box::new(AddU64::new(42)),
            Box::new(MulU64::new(7)),
            Box::new(DivU64::new(100)),
            Box::new(ModU64::new(256)),
            Box::new(ClampU64::new(10, 90)),
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

    fn run_binary(node: &dyn GkNode, a: u64, b: u64) -> u64 {
        let mut out = [Value::None];
        node.eval(&[Value::U64(a), Value::U64(b)], &mut out);
        out[0].as_u64()
    }

    #[test]
    fn ceil_to_multiple_returns_value_when_already_a_multiple() {
        let n = CeilToMultipleU64::new();
        assert_eq!(run_binary(&n, 800, 100), 800);
    }

    #[test]
    fn ceil_to_multiple_rounds_up_to_next_boundary() {
        let n = CeilToMultipleU64::new();
        assert_eq!(run_binary(&n, 801, 100), 900);
    }

    #[test]
    fn ceil_to_multiple_zero_value_is_zero() {
        let n = CeilToMultipleU64::new();
        assert_eq!(run_binary(&n, 0, 100), 0);
    }

    #[test]
    fn ceil_to_multiple_below_one_multiple_rounds_to_multiple() {
        let n = CeilToMultipleU64::new();
        assert_eq!(run_binary(&n, 50, 100), 100);
        assert_eq!(run_binary(&n, 1, 100), 100);
    }

    #[test]
    fn ceil_to_multiple_zero_multiple_is_soft_no_op() {
        let n = CeilToMultipleU64::new();
        assert_eq!(run_binary(&n, 42, 0), 42,
            "multiple=0 must not trap; passes value through");
    }

    // ── multiples_at_least ────────────────────────────────

    #[test]
    fn multiples_at_least_exact_division() {
        let n = MultiplesAtLeastU64::new();
        assert_eq!(run_binary(&n, 800, 100), 8);
    }

    #[test]
    fn multiples_at_least_rounds_up_partial() {
        let n = MultiplesAtLeastU64::new();
        assert_eq!(run_binary(&n, 801, 100), 9);
        assert_eq!(run_binary(&n, 1, 100), 1);
    }

    #[test]
    fn multiples_at_least_zero_value_is_zero() {
        let n = MultiplesAtLeastU64::new();
        assert_eq!(run_binary(&n, 0, 100), 0);
    }

    #[test]
    fn multiples_at_least_zero_multiple_is_zero() {
        let n = MultiplesAtLeastU64::new();
        assert_eq!(run_binary(&n, 42, 0), 0);
    }

    // ── set_or_get ────────────────────────────────────────

    #[test]
    fn set_or_get_returns_current_when_non_zero() {
        let n = SetOrGetU64::new();
        assert_eq!(run_binary(&n, 7, 99), 7);
        assert_eq!(run_binary(&n, u64::MAX, 99), u64::MAX);
    }

    #[test]
    fn set_or_get_returns_fallback_when_current_is_zero() {
        let n = SetOrGetU64::new();
        assert_eq!(run_binary(&n, 0, 99), 99);
    }

    #[test]
    fn set_or_get_zero_fallback_is_zero() {
        // If both inputs are zero, output is zero — soft default
        // for the degenerate case (caller's choice not to seed
        // a meaningful fallback).
        let n = SetOrGetU64::new();
        assert_eq!(run_binary(&n, 0, 0), 0);
    }

    #[test]
    fn set_or_get_idempotent_on_already_set() {
        // The "every subsequent phase" path: current is the
        // cached value, fallback is the (still-evaluated but
        // discarded) recomputation. Returning current preserves
        // the cached state across phases.
        let n = SetOrGetU64::new();
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
        let ceil = CeilToMultipleU64::new();
        let count = MultiplesAtLeastU64::new();
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
        use crate::node::GkNode;

        let nodes: Vec<Box<dyn GkNode>> = vec![
            Box::new(AddU64::new(0)),
            Box::new(ModU64::new(1)),
            Box::new(SumN::new(3)),
            Box::new(ProductN::new(2)),
            Box::new(Interleave::new()),
            Box::new(MixedRadix::new(vec![10, 20])),
            Box::new(CeilToMultipleU64::new()),
            Box::new(MultiplesAtLeastU64::new()),
            Box::new(SetOrGetU64::new()),
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
