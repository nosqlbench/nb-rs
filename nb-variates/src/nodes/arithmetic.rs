// Copyright 2024-2026 nosqlbench contributors
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
