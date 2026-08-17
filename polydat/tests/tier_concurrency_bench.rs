// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Comprehensive P1 / P2 / P3 baseline microbenchmark across all registered Polydat functions
//! at Concurrency 1, 4, and 16.

use std::collections::HashMap;
use std::time::Instant;

use polydat::ast::{PortType, SlotType, Value};
use polydat::compile::assembly::WireRef;
use polydat::compile::jit::{classify_node, compile_jit_raw, JitOp};
use polydat::dsl::factory::{build_node, ConstArg};
use polydat::dsl::registry::registry;

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct BenchResult {
    name: String,
    category: String,
    tier: &'static str,
    // (concurrency, throughput_mops, latency_ns)
    concurrency_metrics: Vec<(usize, f64, f64)>,
}

fn get_wire_types_and_vals(name: &str, count: usize) -> (Vec<PortType>, Vec<Value>) {
    let pt = if name.contains("_str_to") || name.starts_with("str_") || name.contains("string") || name.contains("trim") || name.contains("lower") || name.contains("upper") {
        PortType::Str
    } else if name.contains("f64") || name.contains("f32") || name.contains("sin") || name.contains("cos") || name.contains("tan") || name.contains("lerp") || name.contains("noise") {
        PortType::F64
    } else if name.contains("i64") || name.contains("i32") || name.contains("i16") || name.contains("i8") {
        PortType::I64
    } else if name.contains("bool") {
        PortType::Bool
    } else {
        PortType::U64
    };

    let val = match pt {
        PortType::Str => Value::Str("sample_data_text".into()),
        PortType::F64 => Value::F64(12.345),
        PortType::I64 => Value::I64(12345),
        PortType::Bool => Value::Bool(true),
        _ => Value::U64(12345),
    };

    (vec![pt; count], vec![val; count])
}

fn mock_const_args(name: &str, slot_type: &SlotType) -> ConstArg {
    match slot_type {
        SlotType::ConstU64 => ConstArg::Int(42),
        SlotType::ConstF64 => ConstArg::Float(1.5),
        SlotType::ConstStr => {
            if name.contains("weighted") || name.contains("pick") {
                ConstArg::Str("alpha:10;beta:20".into())
            } else if name.contains("combinations") {
                ConstArg::Str("A-Z;0-9;a-z".into())
            } else if name.contains("regex") || name.contains("matches") {
                ConstArg::Str("^[0-9]+$".into())
            } else if name.contains("date") || name.contains("time") {
                ConstArg::Str("%Y-%m-%d".into())
            } else if name.contains("format") {
                ConstArg::Str("val_{}".into())
            } else {
                ConstArg::Str("sample_data_text".into())
            }
        }
        SlotType::ConstVecU64 => ConstArg::Int(10),
        SlotType::ConstVecF64 => ConstArg::Float(1.0),
        SlotType::ConstVec => ConstArg::Int(10),
        SlotType::Wire => ConstArg::Int(0),
    }
}

#[test]
fn run_all_functions_concurrency_benchmarks() {
    let reg = registry();
    println!("\n=========================================================================");
    println!("  RUNNING P1 / P2 / P3 MICROBENCHMARK ACROSS ALL REGISTERED FUNCTIONS   ");
    println!("  CONCURRENCY LEVELS: 1, 4, 16 THREADS                                  ");
    println!("=========================================================================\n");

    let concurrencies = [1usize, 4usize, 16usize];
    let iters_per_thread = 10_000usize;

    let mut category_stats: HashMap<String, Vec<(f64, f64, f64, f64, f64, f64, f64, f64, f64)>> = HashMap::new();
    let mut total_p1_lat = [0.0; 3];
    let mut total_p2_lat = [0.0; 3];
    let mut total_p3_lat = [0.0; 3];
    let mut total_p1_thru = [0.0; 3];
    let mut total_p2_thru = [0.0; 3];
    let mut total_p3_thru = [0.0; 3];
    let mut benchmarked_count = 0usize;

    for sig in &reg {
        if sig.name.starts_with("log_") {
            continue;
        }
        let consts: Vec<ConstArg> = sig
            .params
            .iter()
            .map(|p| mock_const_args(sig.name, &p.slot_type))
            .collect();

        let wire_count = sig.wire_input_count().max(1);
        let (in_wire_types, in_vals) = get_wire_types_and_vals(sig.name, wire_count);
        let wires: Vec<WireRef> = (0..wire_count)
            .map(|i| WireRef::Input(format!("c{i}")))
            .collect();

        let Ok(node) = build_node(sig.name, &wires, &in_wire_types, &consts) else {
            continue;
        };

        let mut test_out = vec![Value::None; sig.outputs.max(1)];
        let in_vals_probe = in_vals.clone();
        let consts_probe = consts.clone();
        let in_wire_types_probe = in_wire_types.clone();
        let wires_probe = wires.clone();
        let probe_ok = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let test_node = build_node(sig.name, &wires_probe, &in_wire_types_probe, &consts_probe).unwrap();
            test_node.eval(&in_vals_probe, &mut test_out);
        })).is_ok();
        if !probe_ok {
            continue;
        }

        let p2_eligible = node.compiled_u64().is_some();
        if p2_eligible {
            let consts_probe = consts.clone();
            let in_wire_types_probe = in_wire_types.clone();
            let wires_probe = wires.clone();
            let p2_probe_ok = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let test_node = build_node(sig.name, &wires_probe, &in_wire_types_probe, &consts_probe).unwrap();
                let closure = test_node.compiled_u64().unwrap();
                let in_buf = [12345u64, 0, 0, 0, 0, 0, 0, 0];
                let mut out_buf = [0u64; 8];
                closure(&in_buf, &mut out_buf);
            })).is_ok();
            if !p2_probe_ok {
                continue;
            }
        }

        let p3_op = classify_node(node.as_ref());
        let p3_eligible = p3_op != JitOp::Fallback;

        benchmarked_count += 1;
        let category = format!("{:?}", sig.category);

        // --- Benchmark P1: Interpreter (Eval with Value boxing) ---
        let mut p1_lat = [0.0; 3];
        let mut p1_thru = [0.0; 3];
        for (c_idx, &concurrency) in concurrencies.iter().enumerate() {
            let start = Instant::now();
            std::thread::scope(|s| {
                for _ in 0..concurrency {
                    let in_vals_clone = in_vals.clone();
                    let consts_clone = consts.clone();
                    let wires_clone = wires.clone();
                    let wire_types_clone = in_wire_types.clone();
                    s.spawn(move || {
                        let local_node = build_node(sig.name, &wires_clone, &wire_types_clone, &consts_clone).unwrap();
                        let mut out_vals = vec![Value::None; sig.outputs.max(1)];
                        for _cycle in 0..iters_per_thread {
                            local_node.eval(&in_vals_clone, &mut out_vals);
                            std::hint::black_box(&out_vals);
                        }
                    });
                }
            });
            let elapsed = start.elapsed().as_secs_f64();
            let total_ops = (concurrency * iters_per_thread) as f64;
            p1_thru[c_idx] = (total_ops / elapsed) / 1_000_000.0;
            p1_lat[c_idx] = (elapsed / total_ops) * 1_000_000_000.0;
        }

        // --- Benchmark P2: Captured Closure ---
        let mut p2_lat = [0.0; 3];
        let mut p2_thru = [0.0; 3];
        if p2_eligible {
            for (c_idx, &concurrency) in concurrencies.iter().enumerate() {
                let start = Instant::now();
                std::thread::scope(|s| {
                    for _ in 0..concurrency {
                        let consts_clone = consts.clone();
                        let wires_clone = wires.clone();
                        let wire_types_clone = in_wire_types.clone();
                        s.spawn(move || {
                            let local_node = build_node(sig.name, &wires_clone, &wire_types_clone, &consts_clone).unwrap();
                            let closure = local_node.compiled_u64().unwrap();
                            let in_buf = [12345u64, 0, 0, 0, 0, 0, 0, 0];
                            let mut out_buf = [0u64; 8];
                            for _ in 0..iters_per_thread {
                                closure(&in_buf, &mut out_buf);
                                std::hint::black_box(&out_buf);
                            }
                        });
                    }
                });
                let elapsed = start.elapsed().as_secs_f64();
                let total_ops = (concurrency * iters_per_thread) as f64;
                p2_thru[c_idx] = (total_ops / elapsed) / 1_000_000.0;
                p2_lat[c_idx] = (elapsed / total_ops) * 1_000_000_000.0;
            }
        } else {
            p2_lat = p1_lat;
            p2_thru = p1_thru;
        }

        // --- Benchmark P3: Cranelift Native JIT ---
        let mut p3_lat = [0.0; 3];
        let mut p3_thru = [0.0; 3];
        if p3_eligible {
            let req_inputs = match &p3_op {
                JitOp::StrConcat | JitOp::U64Add2 | JitOp::U64Sub2 | JitOp::U64Mul2 | JitOp::U64Div2 | JitOp::U64Mod2
                | JitOp::F64Add | JitOp::F64Sub | JitOp::F64Mul | JitOp::F64Div | JitOp::F64Mod | JitOp::Interleave => 2,
                _ => in_wire_types.len().max(1),
            };
            let in_slots: Vec<usize> = (0..req_inputs).collect();
            let in_count = in_slots.len();
            let out_slots: Vec<usize> = (in_count..in_count + sig.outputs.max(1)).collect();
            let total_slots = in_count + out_slots.len();
            let steps = vec![(p3_op.clone(), in_slots.clone(), out_slots.clone())];
            let mut out_map = HashMap::new();
            out_map.insert("out".into(), in_count);
            let coords = vec![12345u64; in_count];

            let compile_probe = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let mut k = compile_jit_raw(in_count, total_slots, steps.clone(), out_map.clone(), Vec::new()).unwrap();
                k.eval(&coords);
            })).is_ok();

            if compile_probe {
                for (c_idx, &concurrency) in concurrencies.iter().enumerate() {
                    let start = Instant::now();
                    std::thread::scope(|s| {
                        for _ in 0..concurrency {
                            let steps_clone = steps.clone();
                            let out_map_clone = out_map.clone();
                            let coords_clone = coords.clone();
                            s.spawn(move || {
                                let mut local_kernel = compile_jit_raw(in_count, total_slots, steps_clone, out_map_clone, Vec::new()).unwrap();
                                for _ in 0..iters_per_thread {
                                    local_kernel.eval(&coords_clone);
                                    std::hint::black_box(local_kernel.get("out"));
                                }
                            });
                        }
                    });
                    let elapsed = start.elapsed().as_secs_f64();
                    let total_ops = (concurrency * iters_per_thread) as f64;
                    p3_thru[c_idx] = (total_ops / elapsed) / 1_000_000.0;
                    p3_lat[c_idx] = (elapsed / total_ops) * 1_000_000_000.0;
                }
            } else {
                p3_lat = p2_lat;
                p3_thru = p2_thru;
            }
        } else {
            p3_lat = p2_lat;
            p3_thru = p2_thru;
        }

        for i in 0..3 {
            total_p1_lat[i] += p1_lat[i];
            total_p2_lat[i] += p2_lat[i];
            total_p3_lat[i] += p3_lat[i];
            total_p1_thru[i] += p1_thru[i];
            total_p2_thru[i] += p2_thru[i];
            total_p3_thru[i] += p3_thru[i];
        }

        let cat_entry = category_stats.entry(category).or_default();
        cat_entry.push((
            p1_lat[0], p2_lat[0], p3_lat[0],
            p1_lat[1], p2_lat[1], p3_lat[1],
            p1_lat[2], p2_lat[2], p3_lat[2],
        ));
    }

    println!("| Category | P1 (C=1) | P2 (C=1) | P3 (C=1) | P1 (C=4) | P2 (C=4) | P3 (C=4) | P1 (C=16) | P2 (C=16) | P3 (C=16) | Speedup (P3/P1) |");
    println!("|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|");

    let mut cat_names: Vec<_> = category_stats.keys().cloned().collect();
    cat_names.sort();

    for cat in cat_names {
        let rows = &category_stats[&cat];
        let n = rows.len() as f64;
        let avg_p1_1 = rows.iter().map(|r| r.0).sum::<f64>() / n;
        let avg_p2_1 = rows.iter().map(|r| r.1).sum::<f64>() / n;
        let avg_p3_1 = rows.iter().map(|r| r.2).sum::<f64>() / n;

        let avg_p1_4 = rows.iter().map(|r| r.3).sum::<f64>() / n;
        let avg_p2_4 = rows.iter().map(|r| r.4).sum::<f64>() / n;
        let avg_p3_4 = rows.iter().map(|r| r.5).sum::<f64>() / n;

        let avg_p1_16 = rows.iter().map(|r| r.6).sum::<f64>() / n;
        let avg_p2_16 = rows.iter().map(|r| r.7).sum::<f64>() / n;
        let avg_p3_16 = rows.iter().map(|r| r.8).sum::<f64>() / n;

        let speedup = avg_p1_1 / avg_p3_1.max(0.001);
        println!(
            "| **{}** | {:.1}ns | {:.1}ns | {:.1}ns | {:.1}ns | {:.1}ns | {:.1}ns | {:.1}ns | {:.1}ns | {:.1}ns | **{:.1}x** |",
            cat, avg_p1_1, avg_p2_1, avg_p3_1, avg_p1_4, avg_p2_4, avg_p3_4, avg_p1_16, avg_p2_16, avg_p3_16, speedup
        );
    }

    let n = benchmarked_count as f64;
    println!("|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|");
    println!(
        "| **OVERALL AVERAGE** | **{:.1}ns** | **{:.1}ns** | **{:.1}ns** | **{:.1}ns** | **{:.1}ns** | **{:.1}ns** | **{:.1}ns** | **{:.1}ns** | **{:.1}ns** | **{:.1}x** |",
        total_p1_lat[0] / n, total_p2_lat[0] / n, total_p3_lat[0] / n,
        total_p1_lat[1] / n, total_p2_lat[1] / n, total_p3_lat[1] / n,
        total_p1_lat[2] / n, total_p2_lat[2] / n, total_p3_lat[2] / n,
        (total_p1_lat[0] / n) / ((total_p3_lat[0] / n).max(0.001))
    );

    println!("\n=== AGGREGATE THROUGHPUT SCALING (MOPS/SEC) ===");
    println!("- Concurrency 1:  P1 = {:.2} Mops/s | P2 = {:.2} Mops/s | P3 = {:.2} Mops/s", total_p1_thru[0] / n, total_p2_thru[0] / n, total_p3_thru[0] / n);
    println!("- Concurrency 4:  P1 = {:.2} Mops/s | P2 = {:.2} Mops/s | P3 = {:.2} Mops/s", total_p1_thru[1] / n, total_p2_thru[1] / n, total_p3_thru[1] / n);
    println!("- Concurrency 16: P1 = {:.2} Mops/s | P2 = {:.2} Mops/s | P3 = {:.2} Mops/s", total_p1_thru[2] / n, total_p2_thru[2] / n, total_p3_thru[2] / n);
    println!("================================================\n");
}
