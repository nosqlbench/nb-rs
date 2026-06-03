// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Illustration: polydat's `Value` enum reifies the port-type lattice
//! at the runtime boundary.
//!
//! The compiler enforces port-type contracts when wiring nodes
//! (e.g., `mod` accepts two u64 inputs, returns u64). The runtime
//! carries the typed payload through to the consumer, who reads it
//! through a typed accessor. Connecting incompatible port types is a
//! compile error, not a silent coercion.

use polydat::ast::Value;

fn main() {
    let mut kernel = polydat::dsl::compile_polydat(r#"
        input cycle: u64
        n := mod(hash(cycle), 1000)             // u64
        p := unit_interval(hash(cycle))         // f64 in [0.0, 1.0)
        label := number_to_words(n)             // Str
        // Polydat's comparison family returns u64 (0/1) rather than
        // Bool — Bool exists in the Value enum but the comparison
        // nodes pick u64 for compactness and fast-path math. So
        // `is_big` below is a u64, not a Bool.
        is_big := u64_gt(n, 500)                // u64 (0 or 1)
    "#).expect("compile failed");

    kernel.set_inputs(&[42]);

    let n = kernel.pull("n").clone();
    let p = kernel.pull("p").clone();
    let label = kernel.pull("label").clone();
    let is_big = kernel.pull("is_big").clone();

    // Each output is typed at the boundary — the consumer
    // pattern-matches on the Value variant or uses a typed accessor
    // (`as_u64()`, `as_f64()`, etc.).
    match &n {
        Value::U64(v) => println!("n      = U64({v})"),
        other => panic!("expected U64, got {other:?}"),
    }
    match &p {
        Value::F64(v) => println!("p      = F64({v:.6})"),
        other => panic!("expected F64, got {other:?}"),
    }
    match &label {
        Value::Str(s) => println!("label  = Str({s:?})"),
        other => panic!("expected Str, got {other:?}"),
    }
    match &is_big {
        Value::U64(v) => println!("is_big = U64({v})  // 0 or 1, comparison-as-u64"),
        other => panic!("expected U64, got {other:?}"),
    }
}
