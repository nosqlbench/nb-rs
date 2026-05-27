// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Illustration: a single integer `cycle` is a 1-D parameter space.
//! `mixed_radix` projects it onto an N-D space — useful when you want
//! "100 devices × 500 readings each" but only have one cycle counter
//! to drive everything.
//!
//! The projection unwinds in row-major order over the supplied radix
//! list: the first dimension fills before the second advances.

fn main() {
    let mut kernel = polydat::dsl::compile_gk(r#"
        input cycle: u64
        // 100 × 500 = 50,000 distinct (device, reading) pairs.
        (device, reading) := mixed_radix(cycle, 100, 0)
    "#).expect("compile failed");

    println!("cycle  device  reading");
    println!("-----  ------  -------");
    for cycle in [0u64, 1, 99, 100, 49_999, 50_000] {
        kernel.set_inputs(&[cycle]);
        let d = kernel.pull("device").as_u64();
        let r = kernel.pull("reading").as_u64();
        println!("{cycle:>5}  {d:>6}  {r:>7}");
    }
}
