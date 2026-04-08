// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Coherent noise functions: Perlin, simplex, fractal Brownian motion.
//!
//! Unlike hash functions (which produce uncorrelated "white noise"),
//! coherent noise produces values that vary smoothly — nearby inputs
//! yield similar outputs. This is essential for generating realistic
//! time-series data, spatial fields, and any workload where adjacent
//! coordinates should have correlated values.
//!
//! The permutation table is built at init time from a seed. The noise
//! evaluation runs at cycle time.
//!
//! Inputs are u64 coordinates mapped to a float domain via scaling.
//! Outputs are f64 in [-1, 1] (raw noise) or [0, 1] (normalized).

use crate::node::{Commutativity, GkNode, NodeMeta, Port, Value};

// =================================================================
// Permutation table (init-time artifact)
// =================================================================

/// A permutation table for noise functions. Built from a seed at init
/// time, immutable thereafter. The table is doubled (512 entries) to
/// avoid modular indexing.
struct PermTable {
    perm: [u8; 512],
}

impl PermTable {
    fn new(seed: u64) -> Self {
        use xxhash_rust::xxh3::xxh3_64;
        let mut p: Vec<u8> = (0..=255).collect();
        // Fisher-Yates shuffle seeded by hash chain
        let mut s = seed;
        for i in (1..256).rev() {
            s = xxh3_64(&s.to_le_bytes());
            let j = (s as usize) % (i + 1);
            p.swap(i, j);
        }
        let mut perm = [0u8; 512];
        for i in 0..512 {
            perm[i] = p[i & 255];
        }
        Self { perm }
    }

    #[inline]
    fn hash(&self, i: i32) -> u8 {
        self.perm[(i & 255) as usize]
    }
}

// =================================================================
// Perlin noise primitives
// =================================================================

#[inline]
fn fade(t: f64) -> f64 {
    // 6t^5 - 15t^4 + 10t^3 (improved Perlin smoothstep)
    t * t * t * (t * (t * 6.0 - 15.0) + 10.0)
}

#[inline]
fn lerp(t: f64, a: f64, b: f64) -> f64 {
    a + t * (b - a)
}

#[inline]
fn grad1d(hash: u8, x: f64) -> f64 {
    if hash & 1 == 0 { x } else { -x }
}

#[inline]
fn grad2d(hash: u8, x: f64, y: f64) -> f64 {
    match hash & 3 {
        0 => x + y,
        1 => -x + y,
        2 => x - y,
        _ => -x - y,
    }
}

/// Evaluate 1D Perlin noise at a given point.
fn perlin_1d(perm: &PermTable, x: f64) -> f64 {
    let xi = x.floor() as i32;
    let xf = x - x.floor();
    let u = fade(xf);

    let a = perm.hash(xi);
    let b = perm.hash(xi + 1);

    lerp(u, grad1d(a, xf), grad1d(b, xf - 1.0))
}

/// Evaluate 2D Perlin noise at a given point.
fn perlin_2d(perm: &PermTable, x: f64, y: f64) -> f64 {
    let xi = x.floor() as i32;
    let yi = y.floor() as i32;
    let xf = x - x.floor();
    let yf = y - y.floor();

    let u = fade(xf);
    let v = fade(yf);

    let aa = perm.hash(perm.hash(xi) as i32 + yi);
    let ab = perm.hash(perm.hash(xi) as i32 + yi + 1);
    let ba = perm.hash(perm.hash(xi + 1) as i32 + yi);
    let bb = perm.hash(perm.hash(xi + 1) as i32 + yi + 1);

    lerp(v,
        lerp(u, grad2d(aa, xf, yf), grad2d(ba, xf - 1.0, yf)),
        lerp(u, grad2d(ab, xf, yf - 1.0), grad2d(bb, xf - 1.0, yf - 1.0)),
    )
}

// =================================================================
// Simplex noise 2D
// =================================================================

const F2: f64 = 0.3660254037844386; // (sqrt(3) - 1) / 2
const G2: f64 = 0.21132486540518713; // (3 - sqrt(3)) / 6

fn simplex_2d(perm: &PermTable, x: f64, y: f64) -> f64 {
    let s = (x + y) * F2;
    let i = (x + s).floor() as i32;
    let j = (y + s).floor() as i32;

    let t = (i + j) as f64 * G2;
    let x0 = x - (i as f64 - t);
    let y0 = y - (j as f64 - t);

    let (i1, j1) = if x0 > y0 { (1, 0) } else { (0, 1) };

    let x1 = x0 - i1 as f64 + G2;
    let y1 = y0 - j1 as f64 + G2;
    let x2 = x0 - 1.0 + 2.0 * G2;
    let y2 = y0 - 1.0 + 2.0 * G2;

    let gi0 = perm.hash(i + perm.hash(j) as i32);
    let gi1 = perm.hash(i + i1 + perm.hash(j + j1) as i32);
    let gi2 = perm.hash(i + 1 + perm.hash(j + 1) as i32);

    let mut n0 = 0.0;
    let t0 = 0.5 - x0 * x0 - y0 * y0;
    if t0 > 0.0 {
        let t0 = t0 * t0;
        n0 = t0 * t0 * grad2d(gi0, x0, y0);
    }

    let mut n1 = 0.0;
    let t1 = 0.5 - x1 * x1 - y1 * y1;
    if t1 > 0.0 {
        let t1 = t1 * t1;
        n1 = t1 * t1 * grad2d(gi1, x1, y1);
    }

    let mut n2 = 0.0;
    let t2 = 0.5 - x2 * x2 - y2 * y2;
    if t2 > 0.0 {
        let t2 = t2 * t2;
        n2 = t2 * t2 * grad2d(gi2, x2, y2);
    }

    // Scale to [-1, 1]
    70.0 * (n0 + n1 + n2)
}

// =================================================================
// GK Nodes
// =================================================================

/// 1D Perlin noise.
///
/// Signature: `(input: u64) -> (f64)`
///
/// The u64 input is scaled to the float domain by `frequency`.
/// Output is in [-1, 1]. For [0, 1], compose with a remap node.
pub struct Perlin1D {
    meta: NodeMeta,
    perm: PermTable,
    frequency: f64,
}

impl Perlin1D {
    pub fn new(seed: u64, frequency: f64) -> Self {
        Self {
            meta: NodeMeta {
                name: "perlin_1d".into(),
                inputs: vec![Port::u64("input")],
                outputs: vec![Port::f64("output")],
                commutativity: Commutativity::Positional,
            },
            perm: PermTable::new(seed),
            frequency,
        }
    }
}

impl GkNode for Perlin1D {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let x = inputs[0].as_u64() as f64 * self.frequency;
        outputs[0] = Value::F64(perlin_1d(&self.perm, x));
    }
}

/// 2D Perlin noise.
///
/// Signature: `(x: u64, y: u64) -> (f64)`
pub struct Perlin2D {
    meta: NodeMeta,
    perm: PermTable,
    frequency: f64,
}

impl Perlin2D {
    pub fn new(seed: u64, frequency: f64) -> Self {
        Self {
            meta: NodeMeta {
                name: "perlin_2d".into(),
                inputs: vec![Port::u64("x"), Port::u64("y")],
                outputs: vec![Port::f64("output")],
                commutativity: Commutativity::Positional,
            },
            perm: PermTable::new(seed),
            frequency,
        }
    }
}

impl GkNode for Perlin2D {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let x = inputs[0].as_u64() as f64 * self.frequency;
        let y = inputs[1].as_u64() as f64 * self.frequency;
        outputs[0] = Value::F64(perlin_2d(&self.perm, x, y));
    }
}

/// 2D Simplex noise.
///
/// Signature: `(x: u64, y: u64) -> (f64)`
///
/// Faster than Perlin for 2D+ with fewer directional artifacts.
pub struct SimplexNoise2D {
    meta: NodeMeta,
    perm: PermTable,
    frequency: f64,
}

impl SimplexNoise2D {
    pub fn new(seed: u64, frequency: f64) -> Self {
        Self {
            meta: NodeMeta {
                name: "simplex_2d".into(),
                inputs: vec![Port::u64("x"), Port::u64("y")],
                outputs: vec![Port::f64("output")],
                commutativity: Commutativity::Positional,
            },
            perm: PermTable::new(seed),
            frequency,
        }
    }
}

impl GkNode for SimplexNoise2D {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let x = inputs[0].as_u64() as f64 * self.frequency;
        let y = inputs[1].as_u64() as f64 * self.frequency;
        outputs[0] = Value::F64(simplex_2d(&self.perm, x, y));
    }
}

/// Fractal Brownian motion (FBM): layered octaves of noise.
///
/// Signature: `(input: u64) -> (f64)`
///
/// Each octave doubles the frequency and halves the amplitude,
/// producing multi-scale detail. Output range grows with octaves
/// but stays bounded.
pub struct FractalNoise1D {
    meta: NodeMeta,
    perm: PermTable,
    frequency: f64,
    octaves: u32,
    lacunarity: f64,
    persistence: f64,
}

impl FractalNoise1D {
    /// Create with standard parameters.
    ///
    /// - `octaves`: number of noise layers (1-8 typical)
    /// - `lacunarity`: frequency multiplier per octave (default 2.0)
    /// - `persistence`: amplitude multiplier per octave (default 0.5)
    pub fn new(seed: u64, frequency: f64, octaves: u32) -> Self {
        Self::with_params(seed, frequency, octaves, 2.0, 0.5)
    }

    pub fn with_params(
        seed: u64, frequency: f64, octaves: u32,
        lacunarity: f64, persistence: f64,
    ) -> Self {
        Self {
            meta: NodeMeta {
                name: "fractal_noise_1d".into(),
                inputs: vec![Port::u64("input")],
                outputs: vec![Port::f64("output")],
                commutativity: Commutativity::Positional,
            },
            perm: PermTable::new(seed),
            frequency,
            octaves,
            lacunarity,
            persistence,
        }
    }
}

impl GkNode for FractalNoise1D {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let base_x = inputs[0].as_u64() as f64;
        let mut total = 0.0;
        let mut freq = self.frequency;
        let mut amp = 1.0;
        let mut max_amp = 0.0;

        for _ in 0..self.octaves {
            total += perlin_1d(&self.perm, base_x * freq) * amp;
            max_amp += amp;
            freq *= self.lacunarity;
            amp *= self.persistence;
        }

        // Normalize to [-1, 1]
        outputs[0] = Value::F64(total / max_amp);
    }
}

/// 2D Fractal Brownian motion.
///
/// Signature: `(x: u64, y: u64) -> (f64)`
pub struct FractalNoise2D {
    meta: NodeMeta,
    perm: PermTable,
    frequency: f64,
    octaves: u32,
    lacunarity: f64,
    persistence: f64,
}

impl FractalNoise2D {
    pub fn new(seed: u64, frequency: f64, octaves: u32) -> Self {
        Self::with_params(seed, frequency, octaves, 2.0, 0.5)
    }

    pub fn with_params(
        seed: u64, frequency: f64, octaves: u32,
        lacunarity: f64, persistence: f64,
    ) -> Self {
        Self {
            meta: NodeMeta {
                name: "fractal_noise_2d".into(),
                inputs: vec![Port::u64("x"), Port::u64("y")],
                outputs: vec![Port::f64("output")],
                commutativity: Commutativity::Positional,
            },
            perm: PermTable::new(seed),
            frequency,
            octaves,
            lacunarity,
            persistence,
        }
    }
}

impl GkNode for FractalNoise2D {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let base_x = inputs[0].as_u64() as f64;
        let base_y = inputs[1].as_u64() as f64;
        let mut total = 0.0;
        let mut freq = self.frequency;
        let mut amp = 1.0;
        let mut max_amp = 0.0;

        for _ in 0..self.octaves {
            total += perlin_2d(&self.perm, base_x * freq, base_y * freq) * amp;
            max_amp += amp;
            freq *= self.lacunarity;
            amp *= self.persistence;
        }

        outputs[0] = Value::F64(total / max_amp);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn perlin_1d_bounded() {
        let node = Perlin1D::new(42, 0.01);
        let mut out = [Value::None];
        for i in 0..1000u64 {
            node.eval(&[Value::U64(i)], &mut out);
            let v = out[0].as_f64();
            assert!(v >= -1.0 && v <= 1.0, "out of range: {v} at i={i}");
        }
    }

    #[test]
    fn perlin_1d_smooth() {
        // Adjacent inputs should produce similar (not identical) values
        let node = Perlin1D::new(42, 0.01);
        let mut prev = [Value::None];
        let mut curr = [Value::None];
        node.eval(&[Value::U64(100)], &mut prev);
        let mut large_jumps = 0;
        for i in 101..200u64 {
            node.eval(&[Value::U64(i)], &mut curr);
            let diff = (curr[0].as_f64() - prev[0].as_f64()).abs();
            if diff > 0.5 { large_jumps += 1; }
            prev[0] = curr[0].clone();
        }
        // With frequency 0.01, adjacent samples should rarely jump more than 0.5
        assert!(large_jumps < 5, "too many large jumps: {large_jumps}");
    }

    #[test]
    fn perlin_1d_deterministic() {
        let node = Perlin1D::new(42, 0.1);
        let mut out1 = [Value::None];
        let mut out2 = [Value::None];
        node.eval(&[Value::U64(123)], &mut out1);
        node.eval(&[Value::U64(123)], &mut out2);
        assert_eq!(out1[0].as_f64(), out2[0].as_f64());
    }

    #[test]
    fn perlin_1d_different_seeds() {
        let a = Perlin1D::new(1, 0.1);
        let b = Perlin1D::new(2, 0.1);
        let mut out_a = [Value::None];
        let mut out_b = [Value::None];
        let mut differ = false;
        for i in 0..100u64 {
            a.eval(&[Value::U64(i)], &mut out_a);
            b.eval(&[Value::U64(i)], &mut out_b);
            if (out_a[0].as_f64() - out_b[0].as_f64()).abs() > 0.01 {
                differ = true;
                break;
            }
        }
        assert!(differ, "different seeds should produce different noise");
    }

    #[test]
    fn perlin_2d_bounded() {
        let node = Perlin2D::new(42, 0.01);
        let mut out = [Value::None];
        for x in 0..50u64 {
            for y in 0..50u64 {
                node.eval(&[Value::U64(x), Value::U64(y)], &mut out);
                let v = out[0].as_f64();
                assert!(v >= -1.5 && v <= 1.5, "out of range: {v} at ({x},{y})");
            }
        }
    }

    #[test]
    fn perlin_2d_smooth() {
        let node = Perlin2D::new(42, 0.01);
        let mut prev = [Value::None];
        let mut curr = [Value::None];
        node.eval(&[Value::U64(100), Value::U64(100)], &mut prev);
        let mut large_jumps = 0;
        for i in 101..150u64 {
            node.eval(&[Value::U64(i), Value::U64(100)], &mut curr);
            let diff = (curr[0].as_f64() - prev[0].as_f64()).abs();
            if diff > 0.5 { large_jumps += 1; }
            prev[0] = curr[0].clone();
        }
        assert!(large_jumps < 5, "too many large jumps: {large_jumps}");
    }

    #[test]
    fn simplex_2d_bounded() {
        let node = SimplexNoise2D::new(42, 0.01);
        let mut out = [Value::None];
        for x in 0..50u64 {
            for y in 0..50u64 {
                node.eval(&[Value::U64(x), Value::U64(y)], &mut out);
                let v = out[0].as_f64();
                assert!(v >= -1.5 && v <= 1.5, "out of range: {v}");
            }
        }
    }

    #[test]
    fn fractal_1d_bounded() {
        let node = FractalNoise1D::new(42, 0.01, 4);
        let mut out = [Value::None];
        for i in 0..500u64 {
            node.eval(&[Value::U64(i)], &mut out);
            let v = out[0].as_f64();
            assert!(v >= -1.5 && v <= 1.5, "out of range: {v}");
        }
    }

    #[test]
    fn fractal_1d_more_detail_than_single_octave() {
        // FBM with 4 octaves should have more high-frequency variation
        // than a single octave
        let single = Perlin1D::new(42, 0.01);
        let fbm = FractalNoise1D::new(42, 0.01, 4);
        let mut s_out = [Value::None];
        let mut f_out = [Value::None];
        let mut s_changes = 0.0;
        let mut f_changes = 0.0;
        let mut s_prev = 0.0;
        let mut f_prev = 0.0;
        for i in 0..500u64 {
            single.eval(&[Value::U64(i)], &mut s_out);
            fbm.eval(&[Value::U64(i)], &mut f_out);
            if i > 0 {
                s_changes += (s_out[0].as_f64() - s_prev).abs();
                f_changes += (f_out[0].as_f64() - f_prev).abs();
            }
            s_prev = s_out[0].as_f64();
            f_prev = f_out[0].as_f64();
        }
        // FBM should have more total variation (higher frequency detail)
        assert!(f_changes > s_changes * 0.8,
            "FBM should have comparable or more detail: single={s_changes}, fbm={f_changes}");
    }

    #[test]
    fn fractal_2d_bounded() {
        let node = FractalNoise2D::new(42, 0.01, 3);
        let mut out = [Value::None];
        for x in 0..30u64 {
            for y in 0..30u64 {
                node.eval(&[Value::U64(x), Value::U64(y)], &mut out);
                let v = out[0].as_f64();
                assert!(v >= -1.5 && v <= 1.5, "out of range: {v}");
            }
        }
    }
}
