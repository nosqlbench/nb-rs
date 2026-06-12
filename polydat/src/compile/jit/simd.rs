// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Cranelift-SIMD compute kernels for element-wise vector math
//! (type_system_alignment.md §8.2, execution level).
//!
//! Four f32-lane kernels are compiled once per process through the
//! same cranelift JIT engine the scalar kernels use, with
//! `enable_simd` on. Each kernel processes the slice body in
//! `F32X4` chunks (unaligned 128-bit loads — `SliceArc<f32>` data
//! is only 4-aligned) and finishes the remainder in a scalar loop:
//!
//! - `dot_f32(a, b, len) -> f32` — F32X4 multiply-accumulate,
//!   horizontal reduce, scalar tail.
//! - `l2sq_f32(a, b, len) -> f32` — squared-difference
//!   accumulate; callers take the square root.
//! - `add_f32(a, b, out, len)` — element-wise sum.
//! - `scale_f32(a, k, out, len)` — scalar broadcast multiply
//!   (`splat`).
//!
//! Consumers are the `vec_*` library nodes in
//! `crate::library::vector_math`, which fall back to scalar Rust
//! loops when the JIT feature is off or ISA construction fails.
//! SIMD accumulation reassociates floating-point addition, so
//! results may differ from the scalar reference in the final
//! ulps; the equivalence tests compare with relative tolerance.

use std::sync::OnceLock;

use cranelift_codegen::ir::{self, types, AbiParam, InstBuilder};
use cranelift_codegen::ir::condcodes::IntCC;
use cranelift_codegen::settings::{self, Configurable};
use cranelift_frontend::{FunctionBuilder, FunctionBuilderContext};
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{Linkage, Module};

/// Finalized SIMD kernel entry points. The owning [`JITModule`] is
/// kept alive alongside the pointers (dropping it would unmap the
/// code pages).
pub struct SimdKernels {
    pub dot_f32: unsafe extern "C" fn(*const f32, *const f32, u64) -> f32,
    pub l2sq_f32: unsafe extern "C" fn(*const f32, *const f32, u64) -> f32,
    pub add_f32: unsafe extern "C" fn(*const f32, *const f32, *mut f32, u64),
    pub scale_f32: unsafe extern "C" fn(*const f32, f32, *mut f32, u64),
    _module: JITModule,
}

// SAFETY: the function pointers are immutable after construction
// and the generated code is reentrant (no globals); JITModule is
// only held to keep the mapping alive.
unsafe impl Send for SimdKernels {}
unsafe impl Sync for SimdKernels {}

static KERNELS: OnceLock<Option<SimdKernels>> = OnceLock::new();

/// The process-wide SIMD kernel set, compiled on first use.
/// `None` when the host ISA can't be constructed with SIMD
/// enabled — callers fall back to their scalar loops.
pub fn kernels() -> Option<&'static SimdKernels> {
    KERNELS.get_or_init(|| compile_kernels().ok()).as_ref()
}

/// Which reduction/elementwise body a kernel uses.
#[derive(Clone, Copy, PartialEq)]
enum KernelKind {
    /// acc += a[i] * b[i]; returns acc.
    Dot,
    /// d = a[i] - b[i]; acc += d * d; returns acc.
    L2Sq,
    /// out[i] = a[i] + b[i].
    Add,
    /// out[i] = a[i] * k.
    Scale,
}

fn compile_kernels() -> Result<SimdKernels, String> {
    let mut flag_builder = settings::builder();
    flag_builder.set("opt_level", "speed").unwrap();
    // SIMD types (F32X4 et al.) are unconditional in cranelift
    // 0.116 — the former `enable_simd` flag is gone; per-ISA
    // capability comes from the host feature detection in
    // `isa::lookup(host)`.
    let isa_builder = cranelift_codegen::isa::lookup(target_lexicon::Triple::host())
        .map_err(|e| format!("ISA lookup failed: {e}"))?;
    let isa = isa_builder
        .finish(settings::Flags::new(flag_builder))
        .map_err(|e| format!("ISA build failed: {e}"))?;

    let jit_builder = JITBuilder::with_isa(isa, cranelift_module::default_libcall_names());
    let mut module = JITModule::new(jit_builder);
    let mut fn_ctx = FunctionBuilderContext::new();

    let dot_id = build_kernel(&mut module, &mut fn_ctx, "simd_dot_f32", KernelKind::Dot)?;
    let l2_id = build_kernel(&mut module, &mut fn_ctx, "simd_l2sq_f32", KernelKind::L2Sq)?;
    let add_id = build_kernel(&mut module, &mut fn_ctx, "simd_add_f32", KernelKind::Add)?;
    let scale_id = build_kernel(&mut module, &mut fn_ctx, "simd_scale_f32", KernelKind::Scale)?;

    module
        .finalize_definitions()
        .map_err(|e| format!("finalize: {e}"))?;

    // SAFETY: signatures below match exactly what build_kernel
    // declared for each kind.
    unsafe {
        Ok(SimdKernels {
            dot_f32: std::mem::transmute::<*const u8, unsafe extern "C" fn(*const f32, *const f32, u64) -> f32>(
                module.get_finalized_function(dot_id),
            ),
            l2sq_f32: std::mem::transmute::<*const u8, unsafe extern "C" fn(*const f32, *const f32, u64) -> f32>(
                module.get_finalized_function(l2_id),
            ),
            add_f32: std::mem::transmute::<*const u8, unsafe extern "C" fn(*const f32, *const f32, *mut f32, u64)>(
                module.get_finalized_function(add_id),
            ),
            scale_f32: std::mem::transmute::<*const u8, unsafe extern "C" fn(*const f32, f32, *mut f32, u64)>(
                module.get_finalized_function(scale_id),
            ),
            _module: module,
        })
    }
}

/// Build one kernel function. Reducing kinds (Dot/L2Sq) take
/// `(a: i64, b: i64, len: i64) -> f32`; element-wise kinds write
/// through an out pointer and return nothing — Add is
/// `(a, b, out, len)`, Scale is `(a, k: f32, out, len)`.
fn build_kernel(
    module: &mut JITModule,
    fn_ctx: &mut FunctionBuilderContext,
    name: &str,
    kind: KernelKind,
) -> Result<cranelift_module::FuncId, String> {
    let mut sig = module.make_signature();
    match kind {
        KernelKind::Dot | KernelKind::L2Sq => {
            sig.params.push(AbiParam::new(types::I64)); // a
            sig.params.push(AbiParam::new(types::I64)); // b
            sig.params.push(AbiParam::new(types::I64)); // len
            sig.returns.push(AbiParam::new(types::F32));
        }
        KernelKind::Add => {
            sig.params.push(AbiParam::new(types::I64)); // a
            sig.params.push(AbiParam::new(types::I64)); // b
            sig.params.push(AbiParam::new(types::I64)); // out
            sig.params.push(AbiParam::new(types::I64)); // len
        }
        KernelKind::Scale => {
            sig.params.push(AbiParam::new(types::I64)); // a
            sig.params.push(AbiParam::new(types::F32)); // k
            sig.params.push(AbiParam::new(types::I64)); // out
            sig.params.push(AbiParam::new(types::I64)); // len
        }
    }

    let func_id = module
        .declare_function(name, Linkage::Local, &sig)
        .map_err(|e| format!("declare {name}: {e}"))?;

    let mut ctx = module.make_context();
    ctx.func.signature = sig;

    {
        let mut b = FunctionBuilder::new(&mut ctx.func, fn_ctx);
        let entry = b.create_block();
        b.append_block_params_for_function_params(entry);
        b.switch_to_block(entry);
        b.seal_block(entry);

        let params: Vec<ir::Value> = b.block_params(entry).to_vec();
        let (a_ptr, b_or_k, out_ptr, len) = match kind {
            KernelKind::Dot | KernelKind::L2Sq => (params[0], params[1], None, params[2]),
            KernelKind::Add => (params[0], params[1], Some(params[2]), params[3]),
            KernelKind::Scale => (params[0], params[1], Some(params[2]), params[3]),
        };

        // Unaligned 128-bit access: SliceArc<f32> data is only
        // guaranteed 4-aligned.
        let mf = ir::MemFlags::new();
        let reducing = matches!(kind, KernelKind::Dot | KernelKind::L2Sq);

        // n4 = len & !3 — the SIMD-chunked prefix.
        let n4 = b.ins().band_imm(len, !3i64);
        let zero_i = b.ins().iconst(types::I64, 0);
        let zero_f = b.ins().f32const(0.0);
        let vzero = b.ins().splat(types::F32X4, zero_f);
        // Scale's broadcast operand.
        let vk = if kind == KernelKind::Scale {
            Some(b.ins().splat(types::F32X4, b_or_k))
        } else {
            None
        };

        // ── Vector loop ──
        // head(i: i64, acc: f32x4) — acc unused (carried zero) for
        // the element-wise kinds; keeping one block shape for all
        // four kernels keeps this builder small.
        let vhead = b.create_block();
        b.append_block_param(vhead, types::I64);
        b.append_block_param(vhead, types::F32X4);
        let vbody = b.create_block();
        let vexit = b.create_block();
        b.append_block_param(vexit, types::I64);
        b.append_block_param(vexit, types::F32X4);

        b.ins().jump(vhead, &[zero_i, vzero]);

        b.switch_to_block(vhead);
        let vi = b.block_params(vhead)[0];
        let vacc = b.block_params(vhead)[1];
        let done4 = b.ins().icmp(IntCC::UnsignedGreaterThanOrEqual, vi, n4);
        b.ins().brif(
            done4,
            vexit,
            &[vi, vacc],
            vbody,
            &[],
        );

        b.switch_to_block(vbody);
        let byte_off = b.ins().ishl_imm(vi, 2); // i * sizeof(f32)
        let a_addr = b.ins().iadd(a_ptr, byte_off);
        let va = b.ins().load(types::F32X4, mf, a_addr, 0);
        let (next_acc, store_val) = match kind {
            KernelKind::Dot => {
                let b_addr = b.ins().iadd(b_or_k, byte_off);
                let vb = b.ins().load(types::F32X4, mf, b_addr, 0);
                let prod = b.ins().fmul(va, vb);
                (b.ins().fadd(vacc, prod), None)
            }
            KernelKind::L2Sq => {
                let b_addr = b.ins().iadd(b_or_k, byte_off);
                let vb = b.ins().load(types::F32X4, mf, b_addr, 0);
                let d = b.ins().fsub(va, vb);
                let sq = b.ins().fmul(d, d);
                (b.ins().fadd(vacc, sq), None)
            }
            KernelKind::Add => {
                let b_addr = b.ins().iadd(b_or_k, byte_off);
                let vb = b.ins().load(types::F32X4, mf, b_addr, 0);
                (vacc, Some(b.ins().fadd(va, vb)))
            }
            KernelKind::Scale => (vacc, Some(b.ins().fmul(va, vk.unwrap()))),
        };
        if let Some(v) = store_val {
            let out_addr = b.ins().iadd(out_ptr.unwrap(), byte_off);
            b.ins().store(mf, v, out_addr, 0);
        }
        let vi_next = b.ins().iadd_imm(vi, 4);
        b.ins().jump(vhead, &[vi_next, next_acc]);
        b.seal_block(vhead);
        b.seal_block(vbody);

        // ── Horizontal reduce (reducing kinds) ──
        b.switch_to_block(vexit);
        b.seal_block(vexit);
        let ti = b.block_params(vexit)[0];
        let facc = b.block_params(vexit)[1];
        let red = if reducing {
            let l0 = b.ins().extractlane(facc, 0);
            let l1 = b.ins().extractlane(facc, 1);
            let l2 = b.ins().extractlane(facc, 2);
            let l3 = b.ins().extractlane(facc, 3);
            let s01 = b.ins().fadd(l0, l1);
            let s23 = b.ins().fadd(l2, l3);
            b.ins().fadd(s01, s23)
        } else {
            zero_f
        };

        // ── Scalar tail loop: head(i: i64, s: f32) ──
        let shead = b.create_block();
        b.append_block_param(shead, types::I64);
        b.append_block_param(shead, types::F32);
        let sbody = b.create_block();
        let sexit = b.create_block();
        b.append_block_param(sexit, types::F32);

        b.ins().jump(shead, &[ti, red]);

        b.switch_to_block(shead);
        let si = b.block_params(shead)[0];
        let ss = b.block_params(shead)[1];
        let done = b.ins().icmp(IntCC::UnsignedGreaterThanOrEqual, si, len);
        b.ins().brif(done, sexit, &[ss], sbody, &[]);

        b.switch_to_block(sbody);
        let s_off = b.ins().ishl_imm(si, 2);
        let sa_addr = b.ins().iadd(a_ptr, s_off);
        let sa = b.ins().load(types::F32, mf, sa_addr, 0);
        let s_next = match kind {
            KernelKind::Dot => {
                let sb_addr = b.ins().iadd(b_or_k, s_off);
                let sb = b.ins().load(types::F32, mf, sb_addr, 0);
                let p = b.ins().fmul(sa, sb);
                b.ins().fadd(ss, p)
            }
            KernelKind::L2Sq => {
                let sb_addr = b.ins().iadd(b_or_k, s_off);
                let sb = b.ins().load(types::F32, mf, sb_addr, 0);
                let d = b.ins().fsub(sa, sb);
                let sq = b.ins().fmul(d, d);
                b.ins().fadd(ss, sq)
            }
            KernelKind::Add => {
                let sb_addr = b.ins().iadd(b_or_k, s_off);
                let sb = b.ins().load(types::F32, mf, sb_addr, 0);
                let v = b.ins().fadd(sa, sb);
                let out_addr = b.ins().iadd(out_ptr.unwrap(), s_off);
                b.ins().store(mf, v, out_addr, 0);
                ss
            }
            KernelKind::Scale => {
                let v = b.ins().fmul(sa, b_or_k);
                let out_addr = b.ins().iadd(out_ptr.unwrap(), s_off);
                b.ins().store(mf, v, out_addr, 0);
                ss
            }
        };
        let si_next = b.ins().iadd_imm(si, 1);
        b.ins().jump(shead, &[si_next, s_next]);
        b.seal_block(shead);
        b.seal_block(sbody);

        b.switch_to_block(sexit);
        b.seal_block(sexit);
        let result = b.block_params(sexit)[0];
        if reducing {
            b.ins().return_(&[result]);
        } else {
            b.ins().return_(&[]);
        }
        b.finalize();
    }

    module
        .define_function(func_id, &mut ctx)
        .map_err(|e| format!("define {name}: {e}"))?;
    module.clear_context(&mut ctx);
    Ok(func_id)
}

#[cfg(test)]
mod tests {
    /// Deterministic pseudo-random test vectors (no Math::random
    /// in tests — keep them replayable).
    fn test_vec(n: usize, seed: u64) -> Vec<f32> {
        (0..n)
            .map(|i| {
                let h = xxhash_rust::xxh3::xxh3_64(&(seed ^ i as u64).to_le_bytes());
                // map to [-1, 1)
                (h as f64 / u64::MAX as f64 * 2.0 - 1.0) as f32
            })
            .collect()
    }

    fn assert_close(simd: f32, reference: f32, what: &str) {
        let denom = reference.abs().max(1e-6);
        let rel = ((simd - reference).abs()) / denom;
        assert!(
            rel < 1e-4,
            "{what}: simd={simd} reference={reference} rel_err={rel}"
        );
    }

    #[test]
    fn simd_kernels_match_scalar_reference() {
        let Some(k) = super::kernels() else {
            panic!("SIMD kernels failed to compile on this host — \
                    the cranelift ISA should support enable_simd");
        };
        // Cover: empty, sub-chunk, exact-chunk, chunk+tail sizes.
        for &n in &[0usize, 3, 8, 1029] {
            let a = test_vec(n, 0xA);
            let b = test_vec(n, 0xB);

            let dot_ref: f32 = a.iter().zip(&b).map(|(x, y)| x * y).sum();
            let l2_ref: f32 = a.iter().zip(&b).map(|(x, y)| (x - y) * (x - y)).sum();
            let (dot, l2) = unsafe {
                (
                    (k.dot_f32)(a.as_ptr(), b.as_ptr(), n as u64),
                    (k.l2sq_f32)(a.as_ptr(), b.as_ptr(), n as u64),
                )
            };
            assert_close(dot, dot_ref, &format!("dot n={n}"));
            assert_close(l2, l2_ref, &format!("l2sq n={n}"));

            let mut out = vec![0.0f32; n];
            unsafe { (k.add_f32)(a.as_ptr(), b.as_ptr(), out.as_mut_ptr(), n as u64) };
            for i in 0..n {
                assert_eq!(out[i], a[i] + b[i], "add lane {i} n={n}");
            }
            unsafe { (k.scale_f32)(a.as_ptr(), 2.5, out.as_mut_ptr(), n as u64) };
            for i in 0..n {
                assert_eq!(out[i], a[i] * 2.5, "scale lane {i} n={n}");
            }
        }
    }
}
