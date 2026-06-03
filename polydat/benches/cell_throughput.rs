// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `SharedCell` throughput & overhead benchmarks for the
//! cross-fiber validity-tracking mechanism specified in
//! `polydat/docs/design/cross_fiber_invalidation.md`.
//!
//! Six bench groups:
//!
//! 1. **Microbench — uncontended cell ops.** Single-thread
//!    `publish` / `snapshot` cost (mutex + 2 Release stores
//!    on publish; mutex + 1 Acquire load on snapshot).
//!
//! 2. **Multi-writer contention rate.** N writers hammering
//!    a single cell. Measures the cell's effective atomic
//!    write rate under contention — every `fetch_add(Release)`
//!    on revision is a serializable read-modify-write, so the
//!    revision after `N * K` writes must equal `N * K`. Bench
//!    measures total writes / wall-clock.
//!
//! 3. **Pull on clean cone.** Steady-state pull cost when no
//!    cell has been written since the last observation —
//!    measures the bulk-mask early-out path. Parameterized
//!    by cells-per-scope.
//!
//! 4. **Pull on dirty cone — single cell.** One cell published
//!    between pulls. Measures (publish + bulk-mask hit + per-
//!    cell drill + re-eval) per iter.
//!
//! 5. **Pull across multiple scopes.** Cells split across N
//!    distinct intent-word Arcs so the cone has N groups.
//!    Measures bulk-mask scaling with group count.
//!
//! 6. **Wide spill.** >64 cells in one scope, forcing the
//!    spill-to-`Vec<Arc<AtomicU64>>` path. Measures the cone
//!    walker's per-word check cost when the scope's intent
//!    state grows.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Instant;

use criterion::{
    BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main,
};

use polydat::ast::Value;
use polydat::compile::assembly::{PolydatAssembler, WireRef};
use polydat::kernel::{PolydatKernel, SharedCell, SharedCellInner};
use polydat::library::arithmetic::SumN;

// =================================================================
// Helpers
// =================================================================

/// Build a `SharedCell` in a fresh, standalone scope —
/// a brand-new `Arc<AtomicU64>` intent word with this
/// cell at bit 0. Useful for microbenchmarks where we
/// don't care about cone grouping.
fn standalone_cell(initial: Value) -> SharedCell {
    let intent = Arc::new(AtomicU64::new(0));
    Arc::new(SharedCellInner::new(initial, intent, 0))
}

/// Build a `SharedCell` whose intent bit lives in the
/// caller-provided `intent_word` at `bit`. Lets the
/// caller control cone-grouping shape directly: cells
/// sharing the same `Arc<AtomicU64>` end up in one
/// group (compared via `Arc::ptr_eq`); cells with
/// different Arcs end up in separate groups.
fn cell_in_scope(intent_word: Arc<AtomicU64>, bit: u8, initial: Value) -> SharedCell {
    Arc::new(SharedCellInner::new(initial, intent_word, bit))
}

/// Build a kernel with `n_inputs` cell-attachable input
/// slots (named `in0`…`in{n-1}`) feeding a single
/// `SumN` node whose output is "out". The caller
/// attaches cells to whichever slots they want to
/// exercise.
fn build_n_input_kernel(n_inputs: usize) -> PolydatKernel {
    let input_names: Vec<String> = (0..n_inputs).map(|i| format!("in{i}")).collect();
    let mut asm = PolydatAssembler::new(input_names.clone());
    let refs: Vec<WireRef> = input_names.iter().map(|n| WireRef::input(n)).collect();
    asm.add_node("sum", Box::new(SumN::new(n_inputs)), refs);
    asm.add_output("out", WireRef::node("sum"));
    asm.compile().unwrap()
}

/// Warm up a kernel so its cone metadata is built and
/// `node_clean` is true. Bench loops can then measure
/// the steady-state hot path.
fn warm_up(kernel: &mut PolydatKernel) {
    black_box(kernel.pull("out")); // first pull: full eval; sets node_clean
    black_box(kernel.pull("out")); // second pull: lazy-builds the cone, returns clean
}

// =================================================================
// 1. Microbench — uncontended cell ops
// =================================================================

fn bench_cell_publish_uncontended(c: &mut Criterion) {
    let cell = standalone_cell(Value::U64(0));
    c.bench_function("cell/publish_uncontended_u64", |b| {
        let mut v = 0u64;
        b.iter(|| {
            cell.publish(Value::U64(v));
            v = v.wrapping_add(1);
        });
    });
}

fn bench_cell_snapshot_uncontended(c: &mut Criterion) {
    let cell = standalone_cell(Value::U64(42));
    c.bench_function("cell/snapshot_uncontended_u64", |b| {
        b.iter(|| {
            black_box(cell.snapshot());
        });
    });
}

// =================================================================
// 2. Multi-writer contention rate
// =================================================================

/// Measure sustained write rate when N writers contend
/// on a single cell. Uses `iter_custom` to spin up the
/// writer threads once per iter batch; the timer covers
/// only the period between "all writers ready" and
/// "all writers done."
fn bench_cell_publish_contended(c: &mut Criterion) {
    let mut group = c.benchmark_group("cell/publish_contended");
    for &n_writers in &[1usize, 2, 4, 8] {
        group.throughput(Throughput::Elements(n_writers as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(n_writers),
            &n_writers,
            |b, &n_writers| {
                b.iter_custom(|iters| {
                    // Cap per-writer work so very large `iters`
                    // don't blow out wall-clock budget for the
                    // contended cases.
                    let per_writer = (iters / n_writers as u64).max(1);
                    let cell = standalone_cell(Value::U64(0));
                    let start = Arc::new(std::sync::Barrier::new(n_writers + 1));
                    let finish = Arc::new(std::sync::Barrier::new(n_writers + 1));
                    let handles: Vec<_> = (0..n_writers)
                        .map(|_| {
                            let cell = cell.clone();
                            let start = start.clone();
                            let finish = finish.clone();
                            thread::spawn(move || {
                                start.wait();
                                for i in 0..per_writer {
                                    cell.publish(Value::U64(i));
                                }
                                finish.wait();
                            })
                        })
                        .collect();
                    start.wait();
                    let t0 = Instant::now();
                    finish.wait();
                    let dur = t0.elapsed();
                    for h in handles {
                        h.join().unwrap();
                    }
                    // Sanity: revision must equal total writes
                    // exactly — every fetch_add was atomic.
                    let observed = cell.revision.load(Ordering::Acquire);
                    let expected = per_writer * n_writers as u64;
                    assert_eq!(
                        observed, expected,
                        "atomic write rate test: lost increments \
                         (n_writers={n_writers}, per_writer={per_writer}, \
                          revision={observed}, expected={expected})"
                    );
                    dur
                });
            },
        );
    }
    group.finish();
}

// =================================================================
// 3. Pull on a clean cone (bulk-mask early-out)
// =================================================================

/// Build a kernel with `n_cells` cells all in one scope
/// (same intent word, bits 0..n), warm up, then
/// repeatedly pull. Steady-state cost = bulk-mask check
/// (1 Acquire load + 1 AND) + per-cell drill-down
/// (which short-circuits when the cells haven't moved).
fn bench_pull_clean_one_scope(c: &mut Criterion) {
    let mut group = c.benchmark_group("cell/pull_clean_one_scope");
    for &n_cells in &[1usize, 8, 32, 64] {
        group.throughput(Throughput::Elements(1));
        let mut kernel = build_n_input_kernel(n_cells);
        let intent = Arc::new(AtomicU64::new(0));
        for i in 0..n_cells {
            let cell = cell_in_scope(intent.clone(), i as u8, Value::U64(i as u64));
            kernel.state().attach_shared_cell(i, cell);
        }
        warm_up(&mut kernel);
        group.bench_with_input(
            BenchmarkId::from_parameter(n_cells),
            &n_cells,
            |b, _| {
                b.iter(|| {
                    black_box(kernel.pull("out"));
                });
            },
        );
    }
    group.finish();
}

// =================================================================
// 4. Pull on a dirty cone — single cell published per iter
// =================================================================

/// Bench (publish + pull) cycle: one cell of N is dirtied
/// between pulls, the cone walker detects it via per-cell
/// revision compare, and the cone re-evaluates.
fn bench_pull_dirty_one_cell(c: &mut Criterion) {
    let mut group = c.benchmark_group("cell/pull_dirty_one_cell");
    for &n_cells in &[1usize, 8, 32, 64] {
        group.throughput(Throughput::Elements(1));
        let mut kernel = build_n_input_kernel(n_cells);
        let intent = Arc::new(AtomicU64::new(0));
        let mut cells: Vec<SharedCell> = Vec::with_capacity(n_cells);
        for i in 0..n_cells {
            let cell = cell_in_scope(intent.clone(), i as u8, Value::U64(i as u64));
            kernel.state().attach_shared_cell(i, cell.clone());
            cells.push(cell);
        }
        warm_up(&mut kernel);
        let mut v = 0u64;
        group.bench_with_input(
            BenchmarkId::from_parameter(n_cells),
            &n_cells,
            |b, _| {
                b.iter(|| {
                    cells[0].publish(Value::U64(v));
                    v = v.wrapping_add(1);
                    black_box(kernel.pull("out"));
                });
            },
        );
    }
    group.finish();
}

// =================================================================
// 5. Pull across multiple scopes (multi-group cone)
// =================================================================

/// Cells split across `n_scopes` distinct intent-word
/// Arcs — one cell per scope. The cone walker produces
/// `n_scopes` groups; each group is one bulk-mask check
/// + (when clean) zero drill-down. Measures group-count
/// scaling.
fn bench_pull_clean_multi_scope(c: &mut Criterion) {
    let mut group = c.benchmark_group("cell/pull_clean_multi_scope");
    for &n_scopes in &[1usize, 2, 4, 8, 16] {
        group.throughput(Throughput::Elements(1));
        let mut kernel = build_n_input_kernel(n_scopes);
        for i in 0..n_scopes {
            let intent = Arc::new(AtomicU64::new(0));
            let cell = cell_in_scope(intent, 0, Value::U64(i as u64));
            kernel.state().attach_shared_cell(i, cell);
        }
        warm_up(&mut kernel);
        group.bench_with_input(
            BenchmarkId::from_parameter(n_scopes),
            &n_scopes,
            |b, _| {
                b.iter(|| {
                    black_box(kernel.pull("out"));
                });
            },
        );
    }
    group.finish();
}

// =================================================================
// 6. Wide spill — >64 cells in one scope, multiple words
// =================================================================

/// Forces the spill case (>64 cells per scope). Cells go
/// into multiple `Arc<AtomicU64>` words. Each word becomes
/// its own cone group (cells in the same word group by
/// Arc::ptr_eq; cells in adjacent words have different
/// Arcs even within the same scope). Measures whether the
/// >64 path degrades linearly with word count.
fn bench_pull_clean_wide_spill(c: &mut Criterion) {
    let mut group = c.benchmark_group("cell/pull_clean_wide_spill");
    for &n_cells in &[64usize, 128, 256] {
        group.throughput(Throughput::Elements(1));
        let mut kernel = build_n_input_kernel(n_cells);
        // Per cross_fiber_invalidation.md §9.1: one fresh
        // Arc<AtomicU64> per 64 cells. Allocate the words
        // up front and dole out bits.
        let n_words = n_cells.div_ceil(64);
        let words: Vec<Arc<AtomicU64>> = (0..n_words)
            .map(|_| Arc::new(AtomicU64::new(0)))
            .collect();
        for i in 0..n_cells {
            let word_idx = i / 64;
            let bit = (i % 64) as u8;
            let cell = cell_in_scope(words[word_idx].clone(), bit, Value::U64(i as u64));
            kernel.state().attach_shared_cell(i, cell);
        }
        warm_up(&mut kernel);
        group.bench_with_input(
            BenchmarkId::from_parameter(n_cells),
            &n_cells,
            |b, _| {
                b.iter(|| {
                    black_box(kernel.pull("out"));
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_cell_publish_uncontended,
    bench_cell_snapshot_uncontended,
    bench_cell_publish_contended,
    bench_pull_clean_one_scope,
    bench_pull_dirty_one_cell,
    bench_pull_clean_multi_scope,
    bench_pull_clean_wide_spill,
);
criterion_main!(benches);
