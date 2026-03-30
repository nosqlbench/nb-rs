// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Arithmetic function nodes.

use crate::node::{CompiledU64Op, GkNode, NodeMeta, Port, Value};

/// Add a constant to a u64 value (wrapping).
///
/// Signature: `(input: u64) -> (u64)`
/// Param: `addend: u64`
pub struct AddU64 {
    meta: NodeMeta,
    addend: u64,
}

impl AddU64 {
    pub fn new(addend: u64) -> Self {
        Self {
            meta: NodeMeta {
                name: "add".into(),
                inputs: vec![Port::u64("input")],
                outputs: vec![Port::u64("output")],
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
/// Signature: `(input: u64) -> (u64)`
/// Param: `factor: u64`
pub struct MulU64 {
    meta: NodeMeta,
    factor: u64,
}

impl MulU64 {
    pub fn new(factor: u64) -> Self {
        Self {
            meta: NodeMeta {
                name: "mul".into(),
                inputs: vec![Port::u64("input")],
                outputs: vec![Port::u64("output")],
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

/// Divide a u64 value by a constant (integer division).
///
/// Signature: `(input: u64) -> (u64)`
/// Param: `divisor: u64`
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
                inputs: vec![Port::u64("input")],
                outputs: vec![Port::u64("output")],
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

/// Modulo of a u64 value by a constant.
///
/// Signature: `(input: u64) -> (u64)`
/// Param: `modulus: u64`
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
                inputs: vec![Port::u64("input")],
                outputs: vec![Port::u64("output")],
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

/// Clamp a u64 value to `[min, max]`.
///
/// Signature: `(input: u64) -> (u64)`
/// Params: `min: u64, max: u64`
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
                inputs: vec![Port::u64("input")],
                outputs: vec![Port::u64("output")],
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
/// Signature: `(input: u64) -> (u64, u64, ...)`
/// Param: `radixes: Vec<u64>` — the radix for each output position.
///   A radix of 0 means "unbounded" (consumes the remainder).
///
/// Example: `MixedRadix([100, 1000, 0])` decomposes `cycle` into
/// `(cycle % 100, (cycle / 100) % 1000, cycle / 100000)`.
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
        Self {
            meta: NodeMeta {
                name: "mixed_radix".into(),
                inputs: vec![Port::u64("input")],
                outputs,
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

/// Sum N u64 inputs into one (wrapping).
///
/// Signature: `(in_0: u64, in_1: u64, ..., in_N: u64) -> (u64)`
pub struct SumN {
    meta: NodeMeta,
}

impl SumN {
    pub fn new(n: usize) -> Self {
        let inputs: Vec<Port> = (0..n).map(|i| Port::u64(format!("in_{i}"))).collect();
        Self {
            meta: NodeMeta {
                name: "sum".into(),
                inputs,
                outputs: vec![Port::u64("output")],
            },
        }
    }
}

impl GkNode for SumN {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

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

/// Interleave bits from two u64 values into one.
///
/// Signature: `(a: u64, b: u64) -> (u64)`
pub struct Interleave {
    meta: NodeMeta,
}

impl Interleave {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "interleave".into(),
                inputs: vec![Port::u64("a"), Port::u64("b")],
                outputs: vec![Port::u64("output")],
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
}
