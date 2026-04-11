// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Multi-threaded usage: shared GkProgram, per-thread GkState.

fn main() {
    let kernel = nb_variates::dsl::compile_gk(r#"
        coordinates := (cycle)
        user_id := mod(hash(cycle), 1000000)
    "#).expect("compile failed");

    // The program is immutable and shared via Arc.
    let program = kernel.into_program();
    let threads = 4;
    let cycles_per_thread = 1_000_000;

    let start = std::time::Instant::now();

    std::thread::scope(|s| {
        for tid in 0..threads {
            let program = program.clone();
            s.spawn(move || {
                // Each thread creates its own state — no locks.
                let mut state = program.create_state();
                let base = tid as u64 * cycles_per_thread;
                for c in base..base + cycles_per_thread {
                    state.set_coordinates(&[c]);
                    state.pull(&program, "user_id");
                }
            });
        }
    });

    let elapsed = start.elapsed();
    let total = threads * cycles_per_thread as usize;
    let per_cycle = elapsed.as_nanos() as f64 / total as f64;
    println!("{total} cycles across {threads} threads in {elapsed:.2?} ({per_cycle:.1} ns/cycle)");
}
