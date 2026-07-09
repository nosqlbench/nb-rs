// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Polydat throughput benchmarks — Phase 1 (runtime) vs Phase 2 (compiled).
//!
//! Three targeted topologies, each benchmarked in both modes:
//!
//! 1. **Baseline: single identity node**
//! 2. **Deep chain: N-stage unary identity pipeline**
//! 3. **Wide fan-in: N inputs → one sum node**

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main, black_box};

use polydat::compile::assembly::{PolydatAssembler, WireRef};
use polydat::library::arithmetic::{Add, Sum};

// =================================================================
// Builder helpers (shared by Phase 1 and Phase 2)
// =================================================================

// The passthrough stages use `add 0`, not `identity`: identity is
// polymorphic over `Value` (SRD-80 PR B.8) with no P2/P3 lowering,
// which silently broke the compiled-tier benches. `add 0` is the
// same one-node-deep work at every tier.
fn asm_single_identity() -> PolydatAssembler {
    let mut asm = PolydatAssembler::new(vec!["cycle".into()]);
    asm.add_node("id", Box::new(Add::new(0)), vec![WireRef::input("cycle")]);
    asm.add_output("out", WireRef::node("id"));
    asm
}

fn asm_identity_chain(depth: usize) -> PolydatAssembler {
    let mut asm = PolydatAssembler::new(vec!["cycle".into()]);
    asm.add_node("id_0", Box::new(Add::new(0)), vec![WireRef::input("cycle")]);
    for i in 1..depth {
        let name = format!("id_{i}");
        let prev = format!("id_{}", i - 1);
        asm.add_node(&name, Box::new(Add::new(0)), vec![WireRef::node(prev)]);
    }
    let last = format!("id_{}", depth - 1);
    asm.add_output("out", WireRef::node(last));
    asm
}

fn asm_wide_sum(width: usize) -> PolydatAssembler {
    let coord_names: Vec<String> = (0..width).map(|i| format!("c{i}")).collect();
    let mut asm = PolydatAssembler::new(coord_names.clone());
    let inputs: Vec<WireRef> = coord_names.iter().map(WireRef::input).collect();
    asm.add_node("sum", Box::new(Sum::new(width)), inputs);
    asm.add_output("out", WireRef::node("sum"));
    asm
}

// =================================================================
// Phase 1 (runtime) benchmarks
// =================================================================

/// Compile pinned to the interpreter (`jit=off`). Since SRD-105
/// made `Auto` the process default, an unpinned compile would fuse
/// eligible chains into cones — these groups measure the
/// interpreter tier specifically.
fn compile_off(mut asm: PolydatAssembler) -> polydat::kernel::PolydatKernel {
    asm.set_jit_mode(polydat::JitMode::Off);
    asm.compile().unwrap()
}

fn bench_p1_single_identity(c: &mut Criterion) {
    let mut kernel = compile_off(asm_single_identity());
    c.bench_function("p1/single_identity", |b| {
        let mut cycle = 0u64;
        b.iter(|| {
            kernel.set_inputs(&[cycle]);
            black_box(kernel.pull("out"));
            cycle = cycle.wrapping_add(1);
        });
    });
}

fn bench_p1_identity_chain(c: &mut Criterion) {
    let mut group = c.benchmark_group("p1/identity_chain");
    for depth in [1, 2, 4, 8, 16] {
        let mut kernel = compile_off(asm_identity_chain(depth));
        group.bench_with_input(BenchmarkId::from_parameter(depth), &depth, |b, _| {
            let mut cycle = 0u64;
            b.iter(|| {
                kernel.set_inputs(&[cycle]);
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
        let mut kernel = compile_off(asm_wide_sum(width));
        let coords: Vec<u64> = (0..width as u64).collect();
        group.bench_with_input(BenchmarkId::from_parameter(width), &width, |b, _| {
            let mut base = 0u64;
            b.iter(|| {
                let c: Vec<u64> = coords.iter().map(|x| x.wrapping_add(base)).collect();
                kernel.set_inputs(&c);
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

// =================================================================
// Invalidation strategy benchmarks
// =================================================================

use polydat::library::hash::Hash;
use polydat::library::arithmetic::Mod;

fn asm_hash_chain(depth: usize) -> PolydatAssembler {
    let mut asm = PolydatAssembler::new(vec!["cycle".into()]);
    asm.add_node("h0", Box::new(Hash::new()), vec![WireRef::input("cycle")]);
    for i in 1..depth {
        let name = format!("h{i}");
        let prev = format!("h{}", i - 1);
        asm.add_node(&name, Box::new(Mod::new(1_000_000)), vec![WireRef::node(prev)]);
    }
    let last = format!("h{}", depth - 1);
    asm.add_output("out", WireRef::node(last));
    asm
}

fn bench_invalidation_strategy(c: &mut Criterion) {
    let mut group = c.benchmark_group("invalidation");
    for depth in [5, 20, 50] {
        let kernel = asm_hash_chain(depth).compile().unwrap();
        let program = kernel.into_program();

        // Engine 1: Dependent List
        group.bench_with_input(
            BenchmarkId::new("dependent_list", depth),
            &depth,
            |b, _| {
                let mut state = program.create_state();
                let mut cycle = 0u64;
                b.iter(|| {
                    state.set_inputs(&[cycle]);
                    state.pull(&program, "out");
                    black_box(());
                    cycle = cycle.wrapping_add(1);
                });
            },
        );

        // Engine 2: Provenance Scan
        group.bench_with_input(
            BenchmarkId::new("provenance_scan", depth),
            &depth,
            |b, _| {
                let mut state = program.create_provscan_state();
                let mut cycle = 0u64;
                b.iter(|| {
                    state.set_inputs(&[cycle]);
                    state.pull(&program, "out");
                    black_box(());
                    cycle = cycle.wrapping_add(1);
                });
            },
        );
    }
    group.finish();
}

// =================================================================
// SRD-105 cone-mode benchmarks — the production engine question:
// what does auto-mode cone extraction buy over the interpreter on
// per-cycle graphs shaped like real workload bindings? These
// compile through the full DSL pipeline (same path production
// kernels take) under jit=off vs jit=auto.
// =================================================================

fn compile_dsl_mode(src: &str, mode: polydat::JitMode) -> polydat::kernel::PolydatKernel {
    let mut asm = polydat::dsl::compile::compile_polydat_to_assembler(src)
        .expect("assemble");
    asm.set_jit_mode(mode);
    asm.compile().expect("compile")
}

fn hash_mod_chain_src(depth: usize) -> String {
    let mut s = String::from("input cycle: u64\nn0 := hash(cycle)\n");
    for i in 1..depth {
        s.push_str(&format!("n{i} := hash(mod(n{}, 1000003))\n", i - 1));
    }
    s.push_str(&format!("out := mod(n{}, 1000000)\n", depth - 1));
    s
}

/// Binding set shaped like a real workload op-template: id
/// derivation, bucketing, an f64 ratio leg, and a small gate.
const WORKLOAD_BINDINGS_SRC: &str = "\
input cycle: u64
uid := mod(hash(cycle), 1000000)
bucket := mod(hash(uid), 100)
ratio := (to_f64(bucket) / 100.0)
scaled := ((ratio * 3.5) + 1.0)
gate := (uid % 7)
out := (uid + gate)
";

/// Vector pipeline: generation + reduction dominate; scalar node
/// overhead (what slice-slot boundary transport could remove) is
/// the delta between modes. This bounds the slice-slot ABI win.
fn vector_pipeline_src(dim: usize) -> String {
    format!(
        "input cycle: u64\n\
         a := hash_vec(cycle, {dim})\n\
         b := hash_vec(add(cycle, 1), {dim})\n\
         out := vec_dot(a, b)\n"
    )
}

fn bench_cone_hash_mod_chain(c: &mut Criterion) {
    let mut group = c.benchmark_group("cone/hash_mod_chain");
    for depth in [4usize, 16, 64] {
        for (label, mode) in [("off", polydat::JitMode::Off), ("auto", polydat::JitMode::Auto)] {
            let mut kernel = compile_dsl_mode(&hash_mod_chain_src(depth), mode);
            group.bench_with_input(
                BenchmarkId::new(label, depth), &depth, |b, _| {
                    let mut cycle = 0u64;
                    b.iter(|| {
                        kernel.set_inputs(&[cycle]);
                        black_box(kernel.pull("out"));
                        cycle = cycle.wrapping_add(1);
                    });
                });
        }
    }
    group.finish();
}

fn bench_cone_workload_bindings(c: &mut Criterion) {
    let mut group = c.benchmark_group("cone/workload_bindings");
    for (label, mode) in [("off", polydat::JitMode::Off), ("auto", polydat::JitMode::Auto)] {
        let mut kernel = compile_dsl_mode(WORKLOAD_BINDINGS_SRC, mode);
        group.bench_function(label, |b| {
            let mut cycle = 0u64;
            b.iter(|| {
                kernel.set_inputs(&[cycle]);
                black_box(kernel.pull("out"));
                black_box(kernel.pull("scaled"));
                cycle = cycle.wrapping_add(1);
            });
        });
    }
    group.finish();
}

fn bench_cone_vector_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("cone/vector_pipeline");
    group.sample_size(30);
    for dim in [128usize, 768] {
        for (label, mode) in [("off", polydat::JitMode::Off), ("auto", polydat::JitMode::Auto)] {
            let mut kernel = compile_dsl_mode(&vector_pipeline_src(dim), mode);
            group.bench_with_input(
                BenchmarkId::new(label, dim), &dim, |b, _| {
                    let mut cycle = 0u64;
                    b.iter(|| {
                        kernel.set_inputs(&[cycle]);
                        black_box(kernel.pull("out"));
                        cycle = cycle.wrapping_add(1);
                    });
                });
        }
    }
    group.finish();
}

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
    bench_invalidation_strategy,
    bench_cone_hash_mod_chain,
    bench_cone_workload_bindings,
    bench_cone_vector_pipeline,
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
    bench_invalidation_strategy,
    bench_cone_hash_mod_chain,
    bench_cone_workload_bindings,
    bench_cone_vector_pipeline,
);
criterion_main!(benches);
