// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Phase 3: Cranelift JIT compilation of GK kernels.
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
mod inner {
    use std::collections::HashMap;
    use std::mem;

    use cranelift_codegen::ir::{self, AbiParam, InstBuilder, types};
    use cranelift_codegen::settings::{self, Configurable};
    use cranelift_frontend::{FunctionBuilder, FunctionBuilderContext};
    use cranelift_jit::{JITBuilder, JITModule};
    use cranelift_module::{Linkage, Module};

    use crate::node::GkNode;

    /// A JIT-compiled GK kernel.
    ///
    /// The entire DAG is compiled to a single native function.
    /// `eval()` calls directly into generated machine code.
    pub struct JitKernel {
        /// The flat u64 buffer (same layout as CompiledKernel).
        buffer: Vec<u64>,
        coord_count: usize,
        output_map: HashMap<String, usize>,
        /// The JIT-compiled function pointer.
        code_fn: unsafe fn(*const u64, *mut u64),
        /// Keep the module alive so the code isn't freed.
        _module: JITModule,
    }

    impl JitKernel {
        /// Evaluate the kernel with the given coordinates.
        #[inline]
        pub fn eval(&mut self, coords: &[u64]) {
            self.buffer[..self.coord_count].copy_from_slice(coords);
            unsafe {
                (self.code_fn)(self.buffer.as_ptr(), self.buffer.as_mut_ptr());
            }
        }

        /// Read a named output after eval().
        #[inline]
        pub fn get(&self, name: &str) -> u64 {
            self.buffer[self.output_map[name]]
        }

        /// Read by pre-resolved slot index.
        #[inline]
        pub fn get_slot(&self, slot: usize) -> u64 {
            self.buffer[slot]
        }

        /// Resolve output name to slot.
        pub fn resolve_output(&self, name: &str) -> Option<usize> {
            self.output_map.get(name).copied()
        }

        /// Decompose into raw parts for hybrid kernel integration.
        pub fn into_parts(self) -> (unsafe fn(*const u64, *mut u64), JITModule) {
            (self.code_fn, self._module)
        }
    }

    /// Extern function: xxhash3 of a u64 (called from JIT code).
    extern "C" fn jit_xxh3_hash(value: u64) -> u64 {
        xxhash_rust::xxh3::xxh3_64(&value.to_le_bytes())
    }

    /// Extern function: interleave bits of two u64 values (called from JIT code).
    extern "C" fn jit_interleave(a: u64, b: u64) -> u64 {
        let mut result: u64 = 0;
        for i in 0..32 {
            result |= ((a >> i) & 1) << (2 * i);
            result |= ((b >> i) & 1) << (2 * i + 1);
        }
        result
    }

    /// Extern function: interpolating LUT sample (called from JIT code).
    ///
    /// Input is f64 bits in [0,1]. LUT pointer + length are baked constants.
    /// Returns f64 result as u64 bits.
    extern "C" fn jit_lut_sample(input_bits: u64, lut_ptr: u64, lut_len: u64) -> u64 {
        let u = f64::from_bits(input_bits).clamp(0.0, 1.0);
        let n = (lut_len - 1) as f64;
        let pos = u * n;
        let idx = (pos as usize).min(lut_len as usize - 2);
        let frac = pos - idx as f64;
        let result = unsafe {
            let ptr = lut_ptr as *const f64;
            let a = *ptr.add(idx);
            let b = *ptr.add(idx + 1);
            a * (1.0 - frac) + b * frac
        };
        result.to_bits()
    }

    /// Extern function: LFSR shuffle (called from JIT code).
    extern "C" fn jit_shuffle(input: u64, feedback: u64, size: u64, min: u64) -> u64 {
        let mut register = (input % size) + 1;
        loop {
            let lsb = register & 1;
            register >>= 1;
            if lsb != 0 {
                register ^= feedback;
            }
            if register <= size {
                break;
            }
        }
        (register - 1) + min
    }

    /// Description of a JIT step — what operation to generate.
    ///
    /// For f64 operations, values are stored in the u64 buffer as their
    /// bit representation. Cranelift `bitcast` converts between i64/f64.
    #[derive(Debug, Clone)]
    pub(crate) enum JitOp {
        // --- u64 integer ops ---
        /// output[0] = input[0]  (identity / copy)
        Identity,
        /// output[0] = input[0] + constant
        AddConst(u64),
        /// output[0] = input[0] * constant
        MulConst(u64),
        /// output[0] = input[0] / constant
        DivConst(u64),
        /// output[0] = input[0] % constant
        ModConst(u64),
        /// output[0] = clamp(input[0], min, max)  (unsigned)
        ClampConst(u64, u64),
        /// output[0] = interleave_bits(input[0], input[1])  (extern call)
        Interleave,
        /// output[i] = mixed-radix decomposition of input[0]  (inline urem/udiv)
        MixedRadixConst(Vec<u64>),
        /// output[0] = xxh3_hash(input[0])  (extern call)
        Hash,
        /// output[0] = shuffle(input[0])  (extern call: feedback, size, min)
        ShuffleConst(u64, u64, u64),

        // --- f64 ops (values stored as u64 bits in buffer) ---
        /// output[0] = input[0] as f64 / u64::MAX as f64  (u64 → f64 bits)
        UnitInterval,
        /// output[0] = f64::from_bits(input[0]) as u64  (f64 bits → u64, truncate)
        F64ToU64,
        /// output[0] = f64::from_bits(input[0]).round() as u64
        RoundToU64,
        /// output[0] = f64::from_bits(input[0]).floor() as u64
        FloorToU64,
        /// output[0] = f64::from_bits(input[0]).ceil() as u64
        CeilToU64,
        /// output[0] = clamp(f64::from_bits(input[0]), min, max)  → f64 bits
        ClampF64Const(u64, u64), // min.to_bits(), max.to_bits()
        /// output[0] = a + (b - a) * f64::from_bits(input[0])  → f64 bits
        LerpConst(u64, u64), // a.to_bits(), b.to_bits()
        /// output[0] = min + range * (input[0] as f64 / MAX)  → f64 bits  (u64 input)
        ScaleRangeConst(u64, u64), // min.to_bits(), range.to_bits()
        /// output[0] = round(f64::from_bits(input[0]) / step) * step  → f64 bits
        QuantizeConst(u64), // step.to_bits()
        /// output[0] = discretize(f64 input, range, buckets)  → u64
        DiscretizeConst(u64, u64), // range.to_bits(), buckets
        /// output[0] = lut_sample(f64 input, lut_ptr, lut_len)  → f64 bits  (extern call)
        LutSampleConst(u64, u64), // lut_ptr as u64, lut_len

        /// Fallback: call the Phase 2 closure
        Fallback,
    }

    /// Classify a GK node into a JIT-able operation.
    ///
    /// Uses `jit_constants()` to extract assembly-time constants
    /// directly from the node — no probing hacks needed.
    pub(crate) fn classify_node(node: &dyn GkNode) -> JitOp {
        let name = node.meta().name.as_str();
        let consts = node.jit_constants();

        match name {
            "identity" => JitOp::Identity,
            "hash" => JitOp::Hash,
            "add" => {
                if let Some(&c) = consts.first() {
                    JitOp::AddConst(c)
                } else {
                    JitOp::Fallback
                }
            }
            "mul" => {
                if let Some(&c) = consts.first() {
                    JitOp::MulConst(c)
                } else {
                    JitOp::Fallback
                }
            }
            "div" => {
                if let Some(&c) = consts.first() {
                    JitOp::DivConst(c)
                } else {
                    JitOp::Fallback
                }
            }
            "mod" => {
                if let Some(&c) = consts.first() {
                    JitOp::ModConst(c)
                } else {
                    JitOp::Fallback
                }
            }
            "clamp" => {
                if consts.len() >= 2 {
                    JitOp::ClampConst(consts[0], consts[1])
                } else {
                    JitOp::Fallback
                }
            }
            "interleave" => JitOp::Interleave,
            "mixed_radix" => {
                if consts.is_empty() {
                    JitOp::Fallback
                } else {
                    JitOp::MixedRadixConst(consts)
                }
            }
            "shuffle" => {
                if consts.len() >= 3 {
                    JitOp::ShuffleConst(consts[0], consts[1], consts[2])
                } else {
                    JitOp::Fallback
                }
            }
            // f64 ops
            "unit_interval" => JitOp::UnitInterval,
            "f64_to_u64" => JitOp::F64ToU64,
            "round_to_u64" => JitOp::RoundToU64,
            "floor_to_u64" => JitOp::FloorToU64,
            "ceil_to_u64" => JitOp::CeilToU64,
            "clamp_f64" => {
                if consts.len() >= 2 {
                    JitOp::ClampF64Const(consts[0], consts[1])
                } else {
                    JitOp::Fallback
                }
            }
            "lerp" => {
                if consts.len() >= 2 {
                    JitOp::LerpConst(consts[0], consts[1])
                } else {
                    JitOp::Fallback
                }
            }
            "scale_range" => {
                if consts.len() >= 2 {
                    JitOp::ScaleRangeConst(consts[0], consts[1])
                } else {
                    JitOp::Fallback
                }
            }
            "quantize" => {
                if let Some(&c) = consts.first() {
                    JitOp::QuantizeConst(c)
                } else {
                    JitOp::Fallback
                }
            }
            "discretize" => {
                if consts.len() >= 2 {
                    JitOp::DiscretizeConst(consts[0], consts[1])
                } else {
                    JitOp::Fallback
                }
            }
            "lut_sample" => {
                if consts.len() >= 2 {
                    JitOp::LutSampleConst(consts[0], consts[1])
                } else {
                    JitOp::Fallback
                }
            }
            _ => JitOp::Fallback,
        }
    }

    /// Compile a set of JIT steps into native code.
    ///
    /// Each step has: jit_op, input_slots (buffer indices), output_slots.
    /// The generated function reads coords from the buffer, executes
    /// all steps in order, and writes results to the buffer.
    pub(crate) fn compile_jit(
        coord_count: usize,
        total_slots: usize,
        steps: Vec<(JitOp, Vec<usize>, Vec<usize>)>,
        output_map: HashMap<String, usize>,
    ) -> Result<JitKernel, String> {
        let mut flag_builder = settings::builder();
        flag_builder.set("opt_level", "speed").unwrap();
        let isa_builder = cranelift_codegen::isa::lookup(target_lexicon::Triple::host())
            .map_err(|e| format!("ISA lookup failed: {e}"))?;
        let isa = isa_builder.finish(settings::Flags::new(flag_builder))
            .map_err(|e| format!("ISA build failed: {e}"))?;

        let mut jit_builder = JITBuilder::with_isa(isa, cranelift_module::default_libcall_names());

        // Register extern functions
        jit_builder.symbol("jit_xxh3_hash", jit_xxh3_hash as *const u8);
        jit_builder.symbol("jit_interleave", jit_interleave as *const u8);
        jit_builder.symbol("jit_shuffle", jit_shuffle as *const u8);
        jit_builder.symbol("jit_lut_sample", jit_lut_sample as *const u8);

        let mut module = JITModule::new(jit_builder);

        // Declare extern: hash(u64) -> u64
        let hash_func_id = {
            let mut sig = module.make_signature();
            sig.params.push(AbiParam::new(types::I64));
            sig.returns.push(AbiParam::new(types::I64));
            module.declare_function("jit_xxh3_hash", Linkage::Import, &sig)
                .map_err(|e| format!("declare hash: {e}"))?
        };

        // Declare extern: interleave(u64, u64) -> u64
        let interleave_func_id = {
            let mut sig = module.make_signature();
            sig.params.push(AbiParam::new(types::I64));
            sig.params.push(AbiParam::new(types::I64));
            sig.returns.push(AbiParam::new(types::I64));
            module.declare_function("jit_interleave", Linkage::Import, &sig)
                .map_err(|e| format!("declare interleave: {e}"))?
        };

        // Declare extern: shuffle(u64, u64, u64, u64) -> u64
        let shuffle_func_id = {
            let mut sig = module.make_signature();
            for _ in 0..4 { sig.params.push(AbiParam::new(types::I64)); }
            sig.returns.push(AbiParam::new(types::I64));
            module.declare_function("jit_shuffle", Linkage::Import, &sig)
                .map_err(|e| format!("declare shuffle: {e}"))?
        };

        // Declare extern: lut_sample(u64, u64, u64) -> u64
        let lut_sample_func_id = {
            let mut sig = module.make_signature();
            for _ in 0..3 { sig.params.push(AbiParam::new(types::I64)); }
            sig.returns.push(AbiParam::new(types::I64));
            module.declare_function("jit_lut_sample", Linkage::Import, &sig)
                .map_err(|e| format!("declare lut_sample: {e}"))?
        };

        // Define the kernel function: fn(coords: *const u64, buffer: *mut u64)
        let mut sig = module.make_signature();
        sig.params.push(AbiParam::new(types::I64)); // coords ptr
        sig.params.push(AbiParam::new(types::I64)); // buffer ptr
        let func_id = module.declare_function("gk_kernel", Linkage::Local, &sig)
            .map_err(|e| format!("declare kernel: {e}"))?;

        let mut ctx = module.make_context();
        ctx.func.signature = sig;

        let mut fb_ctx = FunctionBuilderContext::new();
        {
            let mut builder = FunctionBuilder::new(&mut ctx.func, &mut fb_ctx);
            let block = builder.create_block();
            builder.append_block_params_for_function_params(block);
            builder.switch_to_block(block);
            builder.seal_block(block);

            let _coords_ptr = builder.block_params(block)[0];
            let buffer_ptr = builder.block_params(block)[1];

            // Import extern functions for calls
            let hash_func_ref = module.declare_func_in_func(hash_func_id, builder.func);
            let interleave_func_ref = module.declare_func_in_func(interleave_func_id, builder.func);
            let shuffle_func_ref = module.declare_func_in_func(shuffle_func_id, builder.func);
            let lut_sample_func_ref = module.declare_func_in_func(lut_sample_func_id, builder.func);

            // Generate code for each step
            for (jit_op, input_slots, output_slots) in &steps {
                match jit_op {
                    JitOp::Identity => {
                        let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                        store_slot(&mut builder, buffer_ptr, output_slots[0], val);
                    }
                    JitOp::AddConst(c) => {
                        let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                        let c_val = builder.ins().iconst(types::I64, *c as i64);
                        let result = builder.ins().iadd(val, c_val);
                        store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                    }
                    JitOp::MulConst(c) => {
                        let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                        let c_val = builder.ins().iconst(types::I64, *c as i64);
                        let result = builder.ins().imul(val, c_val);
                        store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                    }
                    JitOp::DivConst(c) => {
                        let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                        let c_val = builder.ins().iconst(types::I64, *c as i64);
                        let result = builder.ins().udiv(val, c_val);
                        store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                    }
                    JitOp::ModConst(c) => {
                        let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                        let c_val = builder.ins().iconst(types::I64, *c as i64);
                        let result = builder.ins().urem(val, c_val);
                        store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                    }
                    JitOp::ClampConst(min, max) => {
                        let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                        let min_val = builder.ins().iconst(types::I64, *min as i64);
                        let max_val = builder.ins().iconst(types::I64, *max as i64);
                        let clamped_lo = builder.ins().umax(val, min_val);
                        let clamped = builder.ins().umin(clamped_lo, max_val);
                        store_slot(&mut builder, buffer_ptr, output_slots[0], clamped);
                    }
                    JitOp::Interleave => {
                        let a = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                        let b = load_slot(&mut builder, buffer_ptr, input_slots[1]);
                        let call = builder.ins().call(interleave_func_ref, &[a, b]);
                        let result = builder.inst_results(call)[0];
                        store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                    }
                    JitOp::MixedRadixConst(radixes) => {
                        // Unrolled: for each radix, emit urem + udiv
                        let mut remainder = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                        for (i, &radix) in radixes.iter().enumerate() {
                            if radix == 0 {
                                // Unbounded: output = remainder
                                store_slot(&mut builder, buffer_ptr, output_slots[i], remainder);
                            } else {
                                let r = builder.ins().iconst(types::I64, radix as i64);
                                let digit = builder.ins().urem(remainder, r);
                                store_slot(&mut builder, buffer_ptr, output_slots[i], digit);
                                remainder = builder.ins().udiv(remainder, r);
                            }
                        }
                    }
                    JitOp::Hash => {
                        let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                        let call = builder.ins().call(hash_func_ref, &[val]);
                        let result = builder.inst_results(call)[0];
                        store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                    }
                    JitOp::ShuffleConst(feedback, size, min) => {
                        let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                        let fb = builder.ins().iconst(types::I64, *feedback as i64);
                        let sz = builder.ins().iconst(types::I64, *size as i64);
                        let mn = builder.ins().iconst(types::I64, *min as i64);
                        let call = builder.ins().call(shuffle_func_ref, &[val, fb, sz, mn]);
                        let result = builder.inst_results(call)[0];
                        store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                    }

                    // --- f64 ops ---
                    JitOp::UnitInterval => {
                        // u64 → f64: input as f64 / u64::MAX as f64
                        let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                        let fval = builder.ins().fcvt_from_uint(types::F64, val);
                        let max_f = builder.ins().f64const(u64::MAX as f64);
                        let result = builder.ins().fdiv(fval, max_f);
                        store_slot_f64(&mut builder, buffer_ptr, output_slots[0], result);
                    }
                    JitOp::F64ToU64 => {
                        let fval = load_slot_f64(&mut builder, buffer_ptr, input_slots[0]);
                        let result = builder.ins().fcvt_to_uint_sat(types::I64, fval);
                        store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                    }
                    JitOp::RoundToU64 => {
                        let fval = load_slot_f64(&mut builder, buffer_ptr, input_slots[0]);
                        let rounded = builder.ins().nearest(fval);
                        let result = builder.ins().fcvt_to_uint_sat(types::I64, rounded);
                        store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                    }
                    JitOp::FloorToU64 => {
                        let fval = load_slot_f64(&mut builder, buffer_ptr, input_slots[0]);
                        let floored = builder.ins().floor(fval);
                        let result = builder.ins().fcvt_to_uint_sat(types::I64, floored);
                        store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                    }
                    JitOp::CeilToU64 => {
                        let fval = load_slot_f64(&mut builder, buffer_ptr, input_slots[0]);
                        let ceiled = builder.ins().ceil(fval);
                        let result = builder.ins().fcvt_to_uint_sat(types::I64, ceiled);
                        store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                    }
                    JitOp::ClampF64Const(min_bits, max_bits) => {
                        let fval = load_slot_f64(&mut builder, buffer_ptr, input_slots[0]);
                        let fmin = builder.ins().f64const(f64::from_bits(*min_bits));
                        let fmax = builder.ins().f64const(f64::from_bits(*max_bits));
                        let clamped = builder.ins().fmax(fval, fmin);
                        let clamped = builder.ins().fmin(clamped, fmax);
                        store_slot_f64(&mut builder, buffer_ptr, output_slots[0], clamped);
                    }
                    JitOp::LerpConst(a_bits, b_bits) => {
                        // a + t * (b - a)
                        let t = load_slot_f64(&mut builder, buffer_ptr, input_slots[0]);
                        let a = builder.ins().f64const(f64::from_bits(*a_bits));
                        let b = builder.ins().f64const(f64::from_bits(*b_bits));
                        let diff = builder.ins().fsub(b, a);
                        let scaled = builder.ins().fmul(t, diff);
                        let result = builder.ins().fadd(a, scaled);
                        store_slot_f64(&mut builder, buffer_ptr, output_slots[0], result);
                    }
                    JitOp::ScaleRangeConst(min_bits, range_bits) => {
                        // min + range * (input as f64 / u64::MAX as f64)
                        let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                        let fval = builder.ins().fcvt_from_uint(types::F64, val);
                        let max_f = builder.ins().f64const(u64::MAX as f64);
                        let t = builder.ins().fdiv(fval, max_f);
                        let fmin = builder.ins().f64const(f64::from_bits(*min_bits));
                        let frange = builder.ins().f64const(f64::from_bits(*range_bits));
                        let scaled = builder.ins().fmul(t, frange);
                        let result = builder.ins().fadd(fmin, scaled);
                        store_slot_f64(&mut builder, buffer_ptr, output_slots[0], result);
                    }
                    JitOp::QuantizeConst(step_bits) => {
                        // round(val / step) * step
                        let fval = load_slot_f64(&mut builder, buffer_ptr, input_slots[0]);
                        let step = builder.ins().f64const(f64::from_bits(*step_bits));
                        let divided = builder.ins().fdiv(fval, step);
                        let rounded = builder.ins().nearest(divided);
                        let result = builder.ins().fmul(rounded, step);
                        store_slot_f64(&mut builder, buffer_ptr, output_slots[0], result);
                    }

                    JitOp::LutSampleConst(lut_ptr, lut_len) => {
                        // Extern call: jit_lut_sample(input_bits, lut_ptr, lut_len) -> f64 bits
                        let input = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                        let ptr_val = builder.ins().iconst(types::I64, *lut_ptr as i64);
                        let len_val = builder.ins().iconst(types::I64, *lut_len as i64);
                        let call = builder.ins().call(lut_sample_func_ref, &[input, ptr_val, len_val]);
                        let result = builder.inst_results(call)[0];
                        store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                    }
                    JitOp::DiscretizeConst(range_bits, buckets) => {
                        // clamp(input, 0.0, range - eps) / range * buckets → u64
                        let fval = load_slot_f64(&mut builder, buffer_ptr, input_slots[0]);
                        let range = f64::from_bits(*range_bits);
                        let fzero = builder.ins().f64const(0.0);
                        let frange_m_eps = builder.ins().f64const(range - f64::EPSILON);
                        let frange = builder.ins().f64const(range);
                        let fbuckets = builder.ins().f64const(*buckets as f64);
                        let clamped = builder.ins().fmax(fval, fzero);
                        let clamped = builder.ins().fmin(clamped, frange_m_eps);
                        let divided = builder.ins().fdiv(clamped, frange);
                        let scaled = builder.ins().fmul(divided, fbuckets);
                        let as_u64 = builder.ins().fcvt_to_uint_sat(types::I64, scaled);
                        let max_bucket = builder.ins().iconst(types::I64, (*buckets - 1) as i64);
                        let result = builder.ins().umin(as_u64, max_bucket);
                        store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                    }

                    JitOp::Fallback => {
                        // Can't JIT this node — skip (caller should
                        // not include fallback ops in JIT steps)
                    }
                }
            }

            builder.ins().return_(&[]);
            builder.finalize();
        }

        module.define_function(func_id, &mut ctx)
            .map_err(|e| format!("define function: {e}"))?;
        module.clear_context(&mut ctx);
        module.finalize_definitions()
            .map_err(|e| format!("finalize: {e}"))?;

        let code_ptr = module.get_finalized_function(func_id);
        let code_fn: unsafe fn(*const u64, *mut u64) = unsafe { mem::transmute(code_ptr) };

        Ok(JitKernel {
            buffer: vec![0u64; total_slots],
            coord_count,
            output_map,
            code_fn,
            _module: module,
        })
    }

    /// Load a u64 from buffer[slot].
    fn load_slot(
        builder: &mut FunctionBuilder,
        buffer_ptr: ir::Value,
        slot: usize,
    ) -> ir::Value {
        let offset = (slot * 8) as i32;
        builder.ins().load(types::I64, ir::MemFlags::trusted(), buffer_ptr, offset)
    }

    /// Store a u64 to buffer[slot].
    fn store_slot(
        builder: &mut FunctionBuilder,
        buffer_ptr: ir::Value,
        slot: usize,
        value: ir::Value,
    ) {
        let offset = (slot * 8) as i32;
        builder.ins().store(ir::MemFlags::trusted(), value, buffer_ptr, offset);
    }

    /// Load an f64 from buffer[slot] (bitcast from i64).
    fn load_slot_f64(
        builder: &mut FunctionBuilder,
        buffer_ptr: ir::Value,
        slot: usize,
    ) -> ir::Value {
        let i64_val = load_slot(builder, buffer_ptr, slot);
        builder.ins().bitcast(types::F64, ir::MemFlags::new(), i64_val)
    }

    /// Store an f64 to buffer[slot] (bitcast to i64).
    fn store_slot_f64(
        builder: &mut FunctionBuilder,
        buffer_ptr: ir::Value,
        slot: usize,
        value: ir::Value,
    ) {
        let i64_val = builder.ins().bitcast(types::I64, ir::MemFlags::new(), value);
        store_slot(builder, buffer_ptr, slot, i64_val);
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn jit_identity() {
            let steps = vec![
                (JitOp::Identity, vec![0], vec![1]),
            ];
            let mut output_map = HashMap::new();
            output_map.insert("out".into(), 1);
            let mut kernel = compile_jit(1, 2, steps, output_map).unwrap();
            kernel.eval(&[42]);
            assert_eq!(kernel.get("out"), 42);
        }

        #[test]
        fn jit_add_const() {
            let steps = vec![
                (JitOp::AddConst(100), vec![0], vec![1]),
            ];
            let mut output_map = HashMap::new();
            output_map.insert("out".into(), 1);
            let mut kernel = compile_jit(1, 2, steps, output_map).unwrap();
            kernel.eval(&[5]);
            assert_eq!(kernel.get("out"), 105);
        }

        #[test]
        fn jit_mul_const() {
            let steps = vec![
                (JitOp::MulConst(7), vec![0], vec![1]),
            ];
            let mut output_map = HashMap::new();
            output_map.insert("out".into(), 1);
            let mut kernel = compile_jit(1, 2, steps, output_map).unwrap();
            kernel.eval(&[6]);
            assert_eq!(kernel.get("out"), 42);
        }

        #[test]
        fn jit_mod_const() {
            let steps = vec![
                (JitOp::ModConst(100), vec![0], vec![1]),
            ];
            let mut output_map = HashMap::new();
            output_map.insert("out".into(), 1);
            let mut kernel = compile_jit(1, 2, steps, output_map).unwrap();
            kernel.eval(&[542]);
            assert_eq!(kernel.get("out"), 42);
        }

        #[test]
        fn jit_hash() {
            let steps = vec![
                (JitOp::Hash, vec![0], vec![1]),
            ];
            let mut output_map = HashMap::new();
            output_map.insert("out".into(), 1);
            let mut kernel = compile_jit(1, 2, steps, output_map).unwrap();

            kernel.eval(&[42]);
            let v1 = kernel.get("out");

            // Verify it matches the Rust xxh3 implementation
            let expected = xxhash_rust::xxh3::xxh3_64(&42u64.to_le_bytes());
            assert_eq!(v1, expected);
        }

        #[test]
        fn jit_hash_deterministic() {
            let steps = vec![(JitOp::Hash, vec![0], vec![1])];
            let mut output_map = HashMap::new();
            output_map.insert("out".into(), 1);
            let mut kernel = compile_jit(1, 2, steps, output_map).unwrap();

            kernel.eval(&[42]);
            let v1 = kernel.get("out");
            kernel.eval(&[42]);
            let v2 = kernel.get("out");
            assert_eq!(v1, v2);
        }

        #[test]
        fn jit_chain_hash_mod() {
            // hash(cycle) → mod(result, 1000000)
            let steps = vec![
                (JitOp::Hash, vec![0], vec![1]),      // slot 1 = hash(coord 0)
                (JitOp::ModConst(1_000_000), vec![1], vec![2]), // slot 2 = slot 1 % 1M
            ];
            let mut output_map = HashMap::new();
            output_map.insert("user_id".into(), 2);
            let mut kernel = compile_jit(1, 3, steps, output_map).unwrap();

            kernel.eval(&[42]);
            let uid = kernel.get("user_id");
            assert!(uid < 1_000_000, "got {uid}");
        }

        #[test]
        fn jit_clamp_const() {
            let steps = vec![
                (JitOp::ClampConst(10, 50), vec![0], vec![1]),
            ];
            let mut output_map = HashMap::new();
            output_map.insert("out".into(), 1);
            let mut kernel = compile_jit(1, 2, steps, output_map).unwrap();

            kernel.eval(&[5]);
            assert_eq!(kernel.get("out"), 10); // below min

            kernel.eval(&[30]);
            assert_eq!(kernel.get("out"), 30); // in range

            kernel.eval(&[100]);
            assert_eq!(kernel.get("out"), 50); // above max
        }

        #[test]
        fn jit_interleave() {
            let steps = vec![
                (JitOp::Interleave, vec![0, 1], vec![2]),
            ];
            let mut output_map = HashMap::new();
            output_map.insert("out".into(), 2);
            let mut kernel = compile_jit(2, 3, steps, output_map).unwrap();

            kernel.eval(&[0b101, 0b010]);
            // Same as the Interleave node test: result = 0b011001
            assert_eq!(kernel.get("out"), 0b01_10_01);
        }

        #[test]
        fn jit_mixed_radix() {
            // 100 × 1000 × unbounded
            let steps = vec![
                (JitOp::MixedRadixConst(vec![100, 1000, 0]), vec![0], vec![1, 2, 3]),
            ];
            let mut output_map = HashMap::new();
            output_map.insert("d0".into(), 1);
            output_map.insert("d1".into(), 2);
            output_map.insert("d2".into(), 3);
            let mut kernel = compile_jit(1, 4, steps, output_map).unwrap();

            // 4201337 → (37, 13, 42)
            kernel.eval(&[4_201_337]);
            assert_eq!(kernel.get("d0"), 37);
            assert_eq!(kernel.get("d1"), 13);
            assert_eq!(kernel.get("d2"), 42);
        }

        #[test]
        fn jit_shuffle() {
            // Create a real Shuffle to get its constants
            use crate::sampling::metashift::Shuffle;
            use crate::node::GkNode;
            let node = Shuffle::new(0, 1000);
            let consts = node.jit_constants();

            let steps = vec![
                (JitOp::ShuffleConst(consts[0], consts[1], consts[2]), vec![0], vec![1]),
            ];
            let mut output_map = HashMap::new();
            output_map.insert("out".into(), 1);
            let mut kernel = compile_jit(1, 2, steps, output_map).unwrap();

            // Verify same result as the node
            kernel.eval(&[42]);
            let jit_result = kernel.get("out");

            let mut out = [crate::node::Value::None];
            node.eval(&[crate::node::Value::U64(42)], &mut out);
            assert_eq!(jit_result, out[0].as_u64());
        }

        #[test]
        fn jit_unit_interval() {
            let steps = vec![
                (JitOp::UnitInterval, vec![0], vec![1]),
            ];
            let mut output_map = HashMap::new();
            output_map.insert("out".into(), 1);
            let mut kernel = compile_jit(1, 2, steps, output_map).unwrap();

            kernel.eval(&[0]);
            let v = f64::from_bits(kernel.get("out"));
            assert!((v - 0.0).abs() < 1e-10);

            kernel.eval(&[u64::MAX]);
            let v = f64::from_bits(kernel.get("out"));
            assert!((v - 1.0).abs() < 1e-10);
        }

        #[test]
        fn jit_f64_to_u64() {
            // Store 3.7 as f64 bits in coord slot, convert to u64
            let steps = vec![
                (JitOp::F64ToU64, vec![0], vec![1]),
            ];
            let mut output_map = HashMap::new();
            output_map.insert("out".into(), 1);
            let mut kernel = compile_jit(1, 2, steps, output_map).unwrap();

            kernel.eval(&[3.7f64.to_bits()]);
            assert_eq!(kernel.get("out"), 3); // truncate toward zero
        }

        #[test]
        fn jit_round_to_u64() {
            let steps = vec![(JitOp::RoundToU64, vec![0], vec![1])];
            let mut output_map = HashMap::new();
            output_map.insert("out".into(), 1);
            let mut kernel = compile_jit(1, 2, steps, output_map).unwrap();

            kernel.eval(&[3.7f64.to_bits()]);
            assert_eq!(kernel.get("out"), 4);

            kernel.eval(&[3.2f64.to_bits()]);
            assert_eq!(kernel.get("out"), 3);
        }

        #[test]
        fn jit_clamp_f64() {
            let steps = vec![
                (JitOp::ClampF64Const(0.0f64.to_bits(), 1.0f64.to_bits()), vec![0], vec![1]),
            ];
            let mut output_map = HashMap::new();
            output_map.insert("out".into(), 1);
            let mut kernel = compile_jit(1, 2, steps, output_map).unwrap();

            kernel.eval(&[(-0.5f64).to_bits()]);
            assert_eq!(f64::from_bits(kernel.get("out")), 0.0);

            kernel.eval(&[0.5f64.to_bits()]);
            assert_eq!(f64::from_bits(kernel.get("out")), 0.5);

            kernel.eval(&[1.5f64.to_bits()]);
            assert_eq!(f64::from_bits(kernel.get("out")), 1.0);
        }

        #[test]
        fn jit_lerp() {
            let steps = vec![
                (JitOp::LerpConst(10.0f64.to_bits(), 20.0f64.to_bits()), vec![0], vec![1]),
            ];
            let mut output_map = HashMap::new();
            output_map.insert("out".into(), 1);
            let mut kernel = compile_jit(1, 2, steps, output_map).unwrap();

            kernel.eval(&[0.0f64.to_bits()]);
            assert_eq!(f64::from_bits(kernel.get("out")), 10.0);

            kernel.eval(&[1.0f64.to_bits()]);
            assert_eq!(f64::from_bits(kernel.get("out")), 20.0);

            kernel.eval(&[0.5f64.to_bits()]);
            assert_eq!(f64::from_bits(kernel.get("out")), 15.0);
        }

        #[test]
        fn jit_scale_range() {
            let steps = vec![
                (JitOp::ScaleRangeConst(10.0f64.to_bits(), 10.0f64.to_bits()), vec![0], vec![1]),
            ];
            let mut output_map = HashMap::new();
            output_map.insert("out".into(), 1);
            let mut kernel = compile_jit(1, 2, steps, output_map).unwrap();

            kernel.eval(&[0]);
            let v = f64::from_bits(kernel.get("out"));
            assert!((v - 10.0).abs() < 0.001);

            kernel.eval(&[u64::MAX]);
            let v = f64::from_bits(kernel.get("out"));
            assert!((v - 20.0).abs() < 0.001);
        }

        #[test]
        fn jit_quantize() {
            let steps = vec![
                (JitOp::QuantizeConst(10.0f64.to_bits()), vec![0], vec![1]),
            ];
            let mut output_map = HashMap::new();
            output_map.insert("out".into(), 1);
            let mut kernel = compile_jit(1, 2, steps, output_map).unwrap();

            kernel.eval(&[13.0f64.to_bits()]);
            assert_eq!(f64::from_bits(kernel.get("out")), 10.0);

            kernel.eval(&[17.0f64.to_bits()]);
            assert_eq!(f64::from_bits(kernel.get("out")), 20.0);
        }

        #[test]
        fn jit_discretize() {
            let steps = vec![
                (JitOp::DiscretizeConst(100.0f64.to_bits(), 10), vec![0], vec![1]),
            ];
            let mut output_map = HashMap::new();
            output_map.insert("out".into(), 1);
            let mut kernel = compile_jit(1, 2, steps, output_map).unwrap();

            kernel.eval(&[0.0f64.to_bits()]);
            assert_eq!(kernel.get("out"), 0);

            kernel.eval(&[55.0f64.to_bits()]);
            assert_eq!(kernel.get("out"), 5);

            kernel.eval(&[99.0f64.to_bits()]);
            assert_eq!(kernel.get("out"), 9);

            // Clamp above range
            kernel.eval(&[200.0f64.to_bits()]);
            assert_eq!(kernel.get("out"), 9);
        }

        #[test]
        fn jit_lut_sample() {
            // Build a simple linear LUT: f(x) = x * 100
            use crate::sampling::lut::LutF64;
            let lut = LutF64::from_fn(|p| p * 100.0, 1000);
            let lut_ptr = lut.as_ptr() as u64;
            let lut_len = lut.len() as u64;

            let steps = vec![
                (JitOp::LutSampleConst(lut_ptr, lut_len), vec![0], vec![1]),
            ];
            let mut output_map = HashMap::new();
            output_map.insert("out".into(), 1);
            let mut kernel = compile_jit(1, 2, steps, output_map).unwrap();

            // Input 0.5 → should give ~50.0
            kernel.eval(&[0.5f64.to_bits()]);
            let v = f64::from_bits(kernel.get("out"));
            assert!((v - 50.0).abs() < 0.1, "got {v}");

            // Input 0.0 → should give 0.0
            kernel.eval(&[0.0f64.to_bits()]);
            let v = f64::from_bits(kernel.get("out"));
            assert!((v - 0.0).abs() < 0.1, "got {v}");

            // Input 1.0 → should give 100.0
            kernel.eval(&[1.0f64.to_bits()]);
            let v = f64::from_bits(kernel.get("out"));
            assert!((v - 100.0).abs() < 0.1, "got {v}");
        }

        #[test]
        fn jit_lut_normal_distribution() {
            // Build a normal distribution LUT and verify JIT gives same results as P1
            use crate::sampling::icd;
            let lut = icd::dist_normal(0.0, 1.0, icd::DEFAULT_RESOLUTION);
            let lut_ptr = lut.as_ptr() as u64;
            let lut_len = lut.len() as u64;

            let steps = vec![
                (JitOp::LutSampleConst(lut_ptr, lut_len), vec![0], vec![1]),
            ];
            let mut output_map = HashMap::new();
            output_map.insert("out".into(), 1);
            let mut kernel = compile_jit(1, 2, steps, output_map).unwrap();

            // Median of standard normal = 0.0
            kernel.eval(&[0.5f64.to_bits()]);
            let v = f64::from_bits(kernel.get("out"));
            assert!((v - 0.0).abs() < 0.01, "median should be ~0, got {v}");

            // p=0.5 + 1σ ≈ 0.8413 → should give ~1.0
            kernel.eval(&[0.8413f64.to_bits()]);
            let v = f64::from_bits(kernel.get("out"));
            assert!((v - 1.0).abs() < 0.05, "1σ should be ~1.0, got {v}");
        }

        #[test]
        fn jit_chain_unit_interval_lerp() {
            // u64 → unit_interval → lerp(100, 200)
            let steps = vec![
                (JitOp::UnitInterval, vec![0], vec![1]),
                (JitOp::LerpConst(100.0f64.to_bits(), 200.0f64.to_bits()), vec![1], vec![2]),
            ];
            let mut output_map = HashMap::new();
            output_map.insert("out".into(), 2);
            let mut kernel = compile_jit(1, 3, steps, output_map).unwrap();

            kernel.eval(&[0]);
            let v = f64::from_bits(kernel.get("out"));
            assert!((v - 100.0).abs() < 0.001);

            kernel.eval(&[u64::MAX]);
            let v = f64::from_bits(kernel.get("out"));
            assert!((v - 200.0).abs() < 0.001);
        }

        #[test]
        fn jit_multi_step_chain() {
            // cycle → add(10) → mul(3) → mod(100)
            let steps = vec![
                (JitOp::AddConst(10), vec![0], vec![1]),
                (JitOp::MulConst(3), vec![1], vec![2]),
                (JitOp::ModConst(100), vec![2], vec![3]),
            ];
            let mut output_map = HashMap::new();
            output_map.insert("out".into(), 3);
            let mut kernel = compile_jit(1, 4, steps, output_map).unwrap();

            kernel.eval(&[5]);
            // (5 + 10) * 3 = 45, 45 % 100 = 45
            assert_eq!(kernel.get("out"), 45);
        }
    }
}

#[cfg(feature = "jit")]
pub use inner::*;
