// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Stress test: concurrent vectordata reads.
//!
//! Verifies that the same vector data is returned regardless of
//! concurrency level. Detects race conditions in mmap readers,
//! caching, or dataset resolution.

use nb_variates::dsl::compile::compile_gk;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Read a specific vector index single-threaded and return its string.
fn read_vector_single(source: &str, index: u64) -> String {
    let src = format!(
        "inputs := (cycle)\nvec := vector_at(cycle, \"{source}\")"
    );
    let mut kernel = compile_gk(&src).unwrap();
    kernel.set_inputs(&[index]);
    kernel.pull("vec").to_display_string()
}

/// Read vectors from multiple threads and check consistency.
#[test]
#[ignore] // requires sift1m dataset
fn concurrent_reads_match_sequential() {
    let source = "sift1m:label_01";
    let test_indices: Vec<u64> = vec![0, 1, 100, 1000, 5000, 7847, 10000, 50000, 83000];

    // Phase 1: sequential baseline — read each index once
    let baseline: Vec<(u64, String)> = test_indices.iter()
        .map(|&idx| {
            let val = read_vector_single(source, idx);
            assert!(!val.starts_with("[]"), "index {idx} returned empty vector in sequential read");
            (idx, val)
        })
        .collect();

    eprintln!("baseline: {} indices read, all non-empty", baseline.len());

    // Phase 2: concurrent reads — N threads all reading the same indices
    let thread_count = 16;
    let iterations = 10;
    let mut handles = Vec::new();

    for thread_id in 0..thread_count {
        let baseline = baseline.clone();
        let source = source.to_string();
        handles.push(std::thread::spawn(move || {
            let src = format!(
                "inputs := (cycle)\nvec := vector_at(cycle, \"{source}\")"
            );
            let mut kernel = compile_gk(&src).unwrap();

            let mut mismatches = Vec::new();
            for _iter in 0..iterations {
                for &(idx, ref expected) in &baseline {
                    kernel.set_inputs(&[idx]);
                    let actual = kernel.pull("vec").to_display_string();
                    if actual != *expected {
                        mismatches.push((thread_id, idx, actual.len(), expected.len()));
                    }
                    if actual.starts_with("[]") || actual == "[]" {
                        mismatches.push((thread_id, idx, 0, expected.len()));
                    }
                }
            }
            mismatches
        }));
    }

    let mut total_mismatches = 0;
    for handle in handles {
        let mismatches = handle.join().unwrap();
        for (tid, idx, actual_len, expected_len) in &mismatches {
            eprintln!("MISMATCH thread={tid} index={idx} actual_len={actual_len} expected_len={expected_len}");
        }
        total_mismatches += mismatches.len();
    }

    assert_eq!(total_mismatches, 0,
        "{total_mismatches} mismatches across {thread_count} threads x {iterations} iterations");
}

/// Stress test: shared kernel, concurrent fiber-like eval.
/// This mirrors the actual production path where all fibers share
/// one Arc<GkProgram> and each has its own GkState.
#[test]
#[ignore] // requires sift1m dataset
fn shared_kernel_concurrent_eval() {
    use std::sync::Arc;
    let source = "sift1m:label_01";
    let src = format!(
        "inputs := (cycle)\nvec := vector_at(cycle, \"{source}\")"
    );
    let kernel = compile_gk(&src).unwrap();
    let program = kernel.into_program();

    let thread_count = 32;
    let reads_per_thread = 1000;
    let empty_count = std::sync::atomic::AtomicU64::new(0);
    let total_reads = std::sync::atomic::AtomicU64::new(0);

    std::thread::scope(|s| {
        for _tid in 0..thread_count {
            let program = program.clone();
            let empty_count = &empty_count;
            let total_reads = &total_reads;
            s.spawn(move || {
                // Each fiber creates its own state (like FiberBuilder)
                let mut state = program.create_state();
                for i in 0..reads_per_thread {
                    let idx = (i * 7 + 13) as u64 % 83775;
                    state.set_inputs(&[idx]);
                    let val = state.pull(&program, "vec");
                    total_reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if val.to_display_string() == "[]" {
                        empty_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            });
        }
    });

    let total = total_reads.load(std::sync::atomic::Ordering::Relaxed);
    let empties = empty_count.load(std::sync::atomic::Ordering::Relaxed);
    eprintln!("shared kernel: {total} reads, {empties} empty");
    assert_eq!(empties, 0, "{empties} empty vectors in shared-kernel test");
}

/// Tokio async stress test: matches production execution model.
/// 100 tokio tasks sharing one Arc<GkProgram>, each with its own GkState,
/// reading vectors concurrently — same as the activity executor.
/// Helper: compile kernel on a blocking thread to avoid nested runtime.
fn compile_shared_program(source: &str) -> Arc<nb_variates::kernel::GkProgram> {
    let src = format!(
        "inputs := (cycle)\nvec := vector_at(cycle, \"{source}\")"
    );
    let kernel = compile_gk(&src).unwrap();
    kernel.into_program()
}

/// High-concurrency thread pool test with work-stealing pattern.
/// 100 tasks on a thread pool, sharing one program, each with own state.
/// Uses crossbeam-style scoped threads to avoid Arc overhead on the pool.
#[test]
#[ignore] // requires sift1m dataset
fn threadpool_100_tasks_shared_kernel() {
    let program = compile_shared_program("sift1m:label_01");

    let task_count = 100;
    let reads_per_task = 500;
    let empty_count = AtomicU64::new(0);
    let total_reads = AtomicU64::new(0);

    // Use scoped threads to run 100 concurrent "fibers"
    std::thread::scope(|s| {
        for tid in 0..task_count {
            let program = &program;
            let empty_count = &empty_count;
            let total_reads = &total_reads;
            s.spawn(move || {
                let mut state = program.create_state();
                for i in 0..reads_per_task {
                    let idx = ((tid * reads_per_task + i) * 7 + 13) as u64 % 83775;
                    state.set_inputs(&[idx]);
                    let val = state.pull(program, "vec");
                    total_reads.fetch_add(1, Ordering::Relaxed);
                    if val.to_display_string() == "[]" {
                        empty_count.fetch_add(1, Ordering::Relaxed);
                        eprintln!("EMPTY at task={tid} iter={i} idx={idx}");
                    }
                }
            });
        }
    });

    let total = total_reads.load(Ordering::Relaxed);
    let empties = empty_count.load(Ordering::Relaxed);
    eprintln!("100-task threadpool: {total} reads, {empties} empty");
    assert_eq!(empties, 0, "{empties} empty vectors in 100-task test ({total} total reads)");
}

/// Stress test with interleaved sleep to simulate I/O latency.
/// Tasks read a vector, sleep briefly (simulating network round trip),
/// then read the next. This creates maximum interleaving.
#[test]
#[ignore] // requires sift1m dataset
fn interleaved_reads_with_sleep() {
    let program = compile_shared_program("sift1m:label_01");

    let task_count = 50;
    let reads_per_task = 200;
    let empty_count = AtomicU64::new(0);
    let total_reads = AtomicU64::new(0);

    std::thread::scope(|s| {
        for tid in 0..task_count {
            let program = &program;
            let empty_count = &empty_count;
            let total_reads = &total_reads;
            s.spawn(move || {
                let mut state = program.create_state();
                for i in 0..reads_per_task {
                    let idx = ((tid * reads_per_task + i) * 13 + 7) as u64 % 83775;
                    state.set_inputs(&[idx]);
                    let val = state.pull(program, "vec");
                    total_reads.fetch_add(1, Ordering::Relaxed);
                    if val.to_display_string() == "[]" {
                        empty_count.fetch_add(1, Ordering::Relaxed);
                        eprintln!("EMPTY at task={tid} iter={i} idx={idx}");
                    }
                    // Simulate I/O latency — forces OS thread scheduling interleaving
                    if i % 5 == 0 {
                        std::thread::sleep(std::time::Duration::from_micros(100));
                    }
                }
            });
        }
    });

    let total = total_reads.load(Ordering::Relaxed);
    let empties = empty_count.load(Ordering::Relaxed);
    eprintln!("interleaved: {total} reads, {empties} empty");
    assert_eq!(empties, 0, "{empties} empty vectors in interleaved test ({total} total reads)");
}

/// Stress test: many threads hitting different indices simultaneously.
#[test]
#[ignore] // requires sift1m dataset
fn high_contention_reads() {
    let source = "sift1m:label_01";
    let thread_count = 32;
    let reads_per_thread = 1000;

    // First: get the expected dim from a single read
    let single = read_vector_single(source, 0);
    let expected_elements: usize = single.matches(',').count() + 1; // comma-separated inside []
    eprintln!("expected elements per vector: {expected_elements}");

    let empty_count = std::sync::atomic::AtomicU64::new(0);
    let wrong_dim_count = std::sync::atomic::AtomicU64::new(0);
    let total_reads = std::sync::atomic::AtomicU64::new(0);

    std::thread::scope(|s| {
        for _tid in 0..thread_count {
            let empty_count = &empty_count;
            let wrong_dim_count = &wrong_dim_count;
            let total_reads = &total_reads;
            s.spawn(move || {
                let src = format!(
                    "inputs := (cycle)\nvec := vector_at(cycle, \"{source}\")"
                );
                let mut kernel = compile_gk(&src).unwrap();
                for i in 0..reads_per_thread {
                    let idx = (i * 7 + 13) as u64 % 83775; // spread across dataset
                    kernel.set_inputs(&[idx]);
                    let val = kernel.pull("vec").to_display_string();
                    total_reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                    if val == "[]" || val.starts_with("[]") {
                        empty_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    } else {
                        let elements = val.matches(',').count() + 1;
                        if elements != expected_elements {
                            wrong_dim_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                }
            });
        }
    });

    let total = total_reads.load(std::sync::atomic::Ordering::Relaxed);
    let empties = empty_count.load(std::sync::atomic::Ordering::Relaxed);
    let wrong = wrong_dim_count.load(std::sync::atomic::Ordering::Relaxed);

    eprintln!("total reads: {total}, empty: {empties}, wrong dim: {wrong}");
    assert_eq!(empties, 0, "{empties} empty vectors out of {total} reads");
    assert_eq!(wrong, 0, "{wrong} wrong-dimension vectors out of {total} reads");
}
