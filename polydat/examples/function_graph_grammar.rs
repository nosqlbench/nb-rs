// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Illustration: GK source IS a grammar for declaring function graphs.
//!
//! Each `name := expr` line is a graph node; expressions reference
//! other nodes by name, forming directed edges. The compiler
//! topologically sorts the result and refuses cycles. Wide and deep
//! graphs work the same way — the compiler enforces port-type
//! contracts between every wire connection.

fn main() {
    let mut kernel = polydat::dsl::compile_gk(r#"
        input cycle: u64

        // Decompose one coordinate into two dimensions (device, reading).
        (device, reading) := mixed_radix(cycle, 100, 0)

        // Derive an identity hash and split it into independent streams.
        device_h := hash(device)
        h_temp := hash(device_h)
        h_humid := hash(h_temp)

        // Convert each independent stream into a unit-interval quantile.
        q_temp := unit_interval(h_temp)
        q_humid := unit_interval(h_humid)
    "#).expect("compile failed");

    kernel.set_inputs(&[12_345]);
    let d = kernel.pull("device").as_u64();
    let r = kernel.pull("reading").as_u64();
    let qt = kernel.pull("q_temp").as_f64();
    let qh = kernel.pull("q_humid").as_f64();

    println!("cycle=12345 → device={d} reading={r}");
    println!("  q_temp  = {qt:.6}");
    println!("  q_humid = {qh:.6}");
    assert!(d < 100, "device must be in [0, 100)");
    assert!((0.0..1.0).contains(&qt), "q_temp must be in [0, 1)");
    assert!((0.0..1.0).contains(&qh), "q_humid must be in [0, 1)");
}
