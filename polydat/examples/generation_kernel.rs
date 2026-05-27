// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Illustration: a compiled kernel is a pure function of its
//! coordinate inputs.
//!
//! Same coordinate in → same outputs out, every time, with no shared
//! state. That property is what lets a multi-thread benchmark generate
//! billions of distinct reproducible variates in parallel — each
//! thread gets its own `GkState`, the immutable `GkProgram` is shared
//! via `Arc`.

fn main() {
    let kernel = polydat::dsl::compile_gk(r#"
        input cycle: u64
        user_id := mod(hash(cycle), 1000000)
    "#).expect("compile failed");

    let program = kernel.into_program();
    let threads = 2;

    // Each thread evaluates the same coordinate independently and
    // collects the result; the determinism contract says all threads
    // produce the same value for the same coord.
    let results: Vec<u64> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..threads).map(|_| {
            let program = program.clone();
            s.spawn(move || {
                let mut state = program.create_state();
                state.set_inputs(&[42]);
                state.pull(&program, "user_id").as_u64()
            })
        }).collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    println!("two threads, coord=42 → results: {results:?}");
    let first = results[0];
    for (i, v) in results.iter().enumerate() {
        assert_eq!(*v, first, "thread {i} disagreed: {v} vs {first}");
    }
    println!("all threads agreed: user_id = {first}");
}
