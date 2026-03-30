// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! GK throughput benchmarks — Phase 1 (runtime) vs Phase 2 (compiled).
//!
//! Three targeted topologies, each benchmarked in both modes:
//!
//! 1. **Baseline: single identity node**
//! 2. **Deep chain: N-stage unary identity pipeline**
//! 3. **Wide fan-in: N inputs → one sum node**

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main, black_box};

use nb_variates::assembly::{GkAssembler, WireRef};
use nb_variates::compiled::CompiledKernel;
use nb_variates::kernel::GkKernel;
use nb_variates::nodes::arithmetic::SumN;
use nb_variates::nodes::identity::Identity;

// =================================================================
// Builder helpers (shared by Phase 1 and Phase 2)
// =================================================================

fn asm_single_identity() -> GkAssembler {
    let mut asm = GkAssembler::new(vec!["cycle".into()]);
    asm.add_node("id", Box::new(Identity::new()), vec![WireRef::coord("cycle")]);
    asm.add_output("out", WireRef::node("id"));
    asm
}

fn asm_identity_chain(depth: usize) -> GkAssembler {
    let mut asm = GkAssembler::new(vec!["cycle".into()]);
    asm.add_node("id_0", Box::new(Identity::new()), vec![WireRef::coord("cycle")]);
    for i in 1..depth {
        let name = format!("id_{i}");
        let prev = format!("id_{}", i - 1);
        asm.add_node(&name, Box::new(Identity::new()), vec![WireRef::node(prev)]);
    }
    let last = format!("id_{}", depth - 1);
    asm.add_output("out", WireRef::node(last));
    asm
}

fn asm_wide_sum(width: usize) -> GkAssembler {
    let coord_names: Vec<String> = (0..width).map(|i| format!("c{i}")).collect();
    let mut asm = GkAssembler::new(coord_names.clone());
    let inputs: Vec<WireRef> = coord_names.iter().map(|n| WireRef::coord(n)).collect();
    asm.add_node("sum", Box::new(SumN::new(width)), inputs);
    asm.add_output("out", WireRef::node("sum"));
    asm
}

// =================================================================
// Phase 1 (runtime) benchmarks
// =================================================================

fn bench_p1_single_identity(c: &mut Criterion) {
    let mut kernel = asm_single_identity().compile().unwrap();
    c.bench_function("p1/single_identity", |b| {
        let mut cycle = 0u64;
        b.iter(|| {
            kernel.set_coordinates(&[cycle]);
            black_box(kernel.pull("out"));
            cycle = cycle.wrapping_add(1);
        });
    });
}

fn bench_p1_identity_chain(c: &mut Criterion) {
    let mut group = c.benchmark_group("p1/identity_chain");
    for depth in [1, 2, 4, 8, 16] {
        let mut kernel = asm_identity_chain(depth).compile().unwrap();
        group.bench_with_input(BenchmarkId::from_parameter(depth), &depth, |b, _| {
            let mut cycle = 0u64;
            b.iter(|| {
                kernel.set_coordinates(&[cycle]);
                black_box(kernel.pull("out"));
                cycle = cycle.wrapping_add(1);
            });
        });
    }
    group.finish();
}

fn bench_p1_wide_sum(c: &mut Criterion) {
    let mut group = c.benchmark_group("p1/wide_sum");
    for width in [1, 2, 4, 6, 8, 10] {
        let mut kernel = asm_wide_sum(width).compile().unwrap();
        let coords: Vec<u64> = (0..width as u64).collect();
        group.bench_with_input(BenchmarkId::from_parameter(width), &width, |b, _| {
            let mut base = 0u64;
            b.iter(|| {
                let c: Vec<u64> = coords.iter().map(|x| x.wrapping_add(base)).collect();
                kernel.set_coordinates(&c);
                black_box(kernel.pull("out"));
                base = base.wrapping_add(1);
            });
        });
    }
    group.finish();
}

// =================================================================
// Phase 2 (compiled) benchmarks
// =================================================================

fn bench_p2_single_identity(c: &mut Criterion) {
    let mut kernel = asm_single_identity().try_compile().unwrap();
    let out_slot = kernel.resolve_output("out").unwrap();
    c.bench_function("p2/single_identity", |b| {
        let mut cycle = 0u64;
        b.iter(|| {
            kernel.eval(&[cycle]);
            black_box(kernel.get_slot(out_slot));
            cycle = cycle.wrapping_add(1);
        });
    });
}

fn bench_p2_identity_chain(c: &mut Criterion) {
    let mut group = c.benchmark_group("p2/identity_chain");
    for depth in [1, 2, 4, 8, 16] {
        let mut kernel = asm_identity_chain(depth).try_compile().unwrap();
        let out_slot = kernel.resolve_output("out").unwrap();
        group.bench_with_input(BenchmarkId::from_parameter(depth), &depth, |b, _| {
            let mut cycle = 0u64;
            b.iter(|| {
                kernel.eval(&[cycle]);
                black_box(kernel.get_slot(out_slot));
                cycle = cycle.wrapping_add(1);
            });
        });
    }
    group.finish();
}

fn bench_p2_wide_sum(c: &mut Criterion) {
    let mut group = c.benchmark_group("p2/wide_sum");
    for width in [1, 2, 4, 6, 8, 10] {
        let mut kernel = asm_wide_sum(width).try_compile().unwrap();
        let out_slot = kernel.resolve_output("out").unwrap();
        let coords: Vec<u64> = (0..width as u64).collect();
        group.bench_with_input(BenchmarkId::from_parameter(width), &width, |b, _| {
            let mut base = 0u64;
            b.iter(|| {
                let c: Vec<u64> = coords.iter().map(|x| x.wrapping_add(base)).collect();
                kernel.eval(&c);
                black_box(kernel.get_slot(out_slot));
                base = base.wrapping_add(1);
            });
        });
    }
    group.finish();
}

// =================================================================
// Phase 3 (JIT) benchmarks — only with `jit` feature
// =================================================================

#[cfg(feature = "jit")]
fn bench_p3_single_identity(c: &mut Criterion) {
    let mut kernel = asm_single_identity().try_compile_jit().unwrap();
    let out_slot = kernel.resolve_output("out").unwrap();
    c.bench_function("p3/single_identity", |b| {
        let mut cycle = 0u64;
        b.iter(|| {
            kernel.eval(&[cycle]);
            black_box(kernel.get_slot(out_slot));
            cycle = cycle.wrapping_add(1);
        });
    });
}

#[cfg(feature = "jit")]
fn bench_p3_identity_chain(c: &mut Criterion) {
    let mut group = c.benchmark_group("p3/identity_chain");
    for depth in [1, 2, 4, 8, 16] {
        let mut kernel = asm_identity_chain(depth).try_compile_jit().unwrap();
        let out_slot = kernel.resolve_output("out").unwrap();
        group.bench_with_input(BenchmarkId::from_parameter(depth), &depth, |b, _| {
            let mut cycle = 0u64;
            b.iter(|| {
                kernel.eval(&[cycle]);
                black_box(kernel.get_slot(out_slot));
                cycle = cycle.wrapping_add(1);
            });
        });
    }
    group.finish();
}

// =================================================================

// =================================================================
// Hybrid benchmarks
// =================================================================

fn bench_hybrid_single_identity(c: &mut Criterion) {
    let mut kernel = asm_single_identity().compile_hybrid().unwrap();
    let out_slot = kernel.resolve_output("out").unwrap();
    c.bench_function("hybrid/single_identity", |b| {
        let mut cycle = 0u64;
        b.iter(|| {
            kernel.eval(&[cycle]);
            black_box(kernel.get_slot(out_slot));
            cycle = cycle.wrapping_add(1);
        });
    });
}

fn bench_hybrid_identity_chain(c: &mut Criterion) {
    let mut group = c.benchmark_group("hybrid/identity_chain");
    for depth in [1, 2, 4, 8, 16] {
        let mut kernel = asm_identity_chain(depth).compile_hybrid().unwrap();
        let out_slot = kernel.resolve_output("out").unwrap();
        group.bench_with_input(BenchmarkId::from_parameter(depth), &depth, |b, _| {
            let mut cycle = 0u64;
            b.iter(|| {
                kernel.eval(&[cycle]);
                black_box(kernel.get_slot(out_slot));
                cycle = cycle.wrapping_add(1);
            });
        });
    }
    group.finish();
}

// =================================================================

#[cfg(not(feature = "jit"))]
criterion_group!(
    benches,
    bench_p1_single_identity,
    bench_p1_identity_chain,
    bench_p1_wide_sum,
    bench_p2_single_identity,
    bench_p2_identity_chain,
    bench_p2_wide_sum,
    bench_hybrid_single_identity,
    bench_hybrid_identity_chain,
);

#[cfg(feature = "jit")]
criterion_group!(
    benches,
    bench_p1_single_identity,
    bench_p1_identity_chain,
    bench_p1_wide_sum,
    bench_p2_single_identity,
    bench_p2_identity_chain,
    bench_p2_wide_sum,
    bench_p3_single_identity,
    bench_p3_identity_chain,
    bench_hybrid_single_identity,
    bench_hybrid_identity_chain,
);
criterion_main!(benches);
