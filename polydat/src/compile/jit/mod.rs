// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Phase 3: Cranelift JIT compilation of Polydat Kernels.
//!
//! Generates native machine code from the DAG. The entire kernel
//! becomes a single function: `fn(coords: *const u64, buffer: *mut u64)`.
//! No closures, no function pointers, no gather/scatter — direct
//! buffer reads and writes with inlined arithmetic.
//!
//! The buffer is `Vec<u64>`. For f64 values, they are stored as their
//! bit representation (`f64::to_bits()` / `f64::from_bits()`). The JIT
//! uses Cranelift `bitcast` (free, no instruction emitted) to convert
//! between i64 and f64 representations when crossing type boundaries.
//!
//! Feature-gated behind `jit`.
//!
//! For nodes that can't be JIT-compiled inline (hash, shuffle, interleave),
//! we emit a call to an extern function. Simple ops are fully inlined,
//! complex ops are extern calls with zero overhead beyond the call itself.

#[cfg(feature = "jit")]
mod kernels;
#[cfg(feature = "jit")]
mod codegen;
#[cfg(feature = "jit")]
pub mod simd;

#[cfg(feature = "jit")]
pub use kernels::*;
#[cfg(feature = "jit")]
pub use codegen::*;

#[cfg(all(test, feature = "jit"))]
mod tests {
    use super::*;

    #[test]
    fn test_inventory_tier_distribution() {
        use crate::dsl::registry::registry;
        use crate::dsl::factory::{build_node, ConstArg};
        use crate::compile::assembly::WireRef;
        use crate::ast::PortType;

        let reg = registry();
        let total = reg.len();

        let mut p1_count = 0;
        let mut p2_count = 0;
        let mut p3_count = 0;
        let mut unbuilt = 0;

        for sig in &reg {
            let mut consts = Vec::new();
            for p in sig.params {
                match p.slot_type {
                    crate::ast::SlotType::ConstU64 => consts.push(ConstArg::Int(1)),
                    crate::ast::SlotType::ConstF64 => consts.push(ConstArg::Float(1.0)),
                    crate::ast::SlotType::ConstStr => consts.push(ConstArg::Str("test".into())),
                    crate::ast::SlotType::ConstVecU64 => consts.push(ConstArg::Int(1)),
                    crate::ast::SlotType::ConstVecF64 => consts.push(ConstArg::Float(1.0)),
                    crate::ast::SlotType::ConstVec => consts.push(ConstArg::Int(1)),
                    crate::ast::SlotType::Wire => {}
                }
            }
            let wires = vec![WireRef::Input("cycle".to_string()); sig.wire_input_count().max(1)];
            let wire_types = vec![PortType::U64; wires.len()];

            let node_res = build_node(sig.name, &wires, &wire_types, &consts);
            if let Ok(node) = node_res {
                let p2_eligible = node.compiled_u64().is_some();
                let p3_eligible = classify_node(node.as_ref()) != JitOp::Fallback;
                if p3_eligible {
                    p3_count += 1;
                } else if p2_eligible {
                    p2_count += 1;
                } else {
                    p1_count += 1;
                }
            } else {
                unbuilt += 1;
                p1_count += 1;
            }
        }

        println!("\n=== COMPILER OPTIMIZATION INVENTORY SUMMARY ===");
        println!("Total Registered Functions: {total}");
        println!("Phase 3 (Full Native JIT):  {p3_count} ({:.1}%)", (p3_count as f64 / total as f64) * 100.0);
        println!("Phase 2 (Captured Closure): {p2_count} ({:.1}%)", (p2_count as f64 / total as f64) * 100.0);
        println!("Phase 1 (Interpreter Cones):{p1_count} ({:.1}%) (unbuilt fallback: {unbuilt})", (p1_count as f64 / total as f64) * 100.0);
        println!("===============================================\n");
    }
}
