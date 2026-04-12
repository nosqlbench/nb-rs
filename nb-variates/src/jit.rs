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
        buffer: Vec<u64>,
        coord_count: usize,
        output_map: HashMap<String, usize>,
        /// Plain eval function (no provenance).
        code_fn: unsafe fn(*const u64, *mut u64),
        /// Provenance-aware eval function (with clean ptr).
        code_fn_prov: Option<unsafe fn(*const u64, *mut u64, *mut u8)>,
        _module: JITModule,
        _nodes: Vec<Box<dyn GkNode>>,
        /// Provenance state (only when compiled with provenance).
        provenance: Option<JitProvenanceState>,
    }

    struct JitProvenanceState {
        node_clean: Vec<u8>, // u8 for C ABI compat
        input_dependents: Vec<Vec<usize>>,
        /// Per-slot provenance for pull-side guard.
        slot_provenance: Vec<u64>,
        changed_mask: u64,
    }

    impl JitKernel {
        #[inline]
        fn set_inputs_internal(&mut self, coords: &[u64]) {
            if let Some(ref mut prov) = self.provenance {
                prov.changed_mask = 0;
                for i in 0..coords.len().min(self.coord_count) {
                    if self.buffer[i] != coords[i] {
                        self.buffer[i] = coords[i];
                        prov.changed_mask |= 1u64 << i;
                        if i < prov.input_dependents.len() {
                            for &step_idx in &prov.input_dependents[i] {
                                prov.node_clean[step_idx] = 0;
                            }
                        }
                    }
                }
            } else {
                self.buffer[..self.coord_count.min(coords.len())]
                    .copy_from_slice(&coords[..self.coord_count.min(coords.len())]);
            }
        }

        #[inline]
        fn run_jit(&mut self) {
            if let Some(ref mut prov) = self.provenance {
                unsafe {
                    (self.code_fn_prov.unwrap())(
                        self.buffer.as_ptr(),
                        self.buffer.as_mut_ptr(),
                        prov.node_clean.as_mut_ptr(),
                    );
                }
            } else {
                unsafe {
                    (self.code_fn)(self.buffer.as_ptr(), self.buffer.as_mut_ptr());
                }
            }
        }

        /// Set inputs + evaluate all nodes eagerly.
        #[inline]
        pub fn eval(&mut self, coords: &[u64]) {
            self.set_inputs_internal(coords);
            self.run_jit();
        }

        /// Set inputs + evaluate ONLY if the requested output's cone
        /// was affected. Pull-side guard skips JIT call entirely when
        /// the output is clean.
        #[inline]
        pub fn eval_for_slot(&mut self, coords: &[u64], slot: usize) -> u64 {
            self.set_inputs_internal(coords);
            if let Some(ref prov) = self.provenance {
                if slot < prov.slot_provenance.len() {
                    if prov.slot_provenance[slot] & prov.changed_mask == 0 {
                        return self.buffer[slot]; // pull-side skip
                    }
                }
            }
            self.run_jit();
            self.buffer[slot]
        }

        #[inline]
        pub fn get(&self, name: &str) -> u64 {
            self.buffer[self.output_map[name]]
        }

        #[inline]
        pub fn get_slot(&self, slot: usize) -> u64 {
            self.buffer[slot]
        }

        pub fn coord_count(&self) -> usize { self.coord_count }

        /// Resolve output name to slot.
        pub fn resolve_output(&self, name: &str) -> Option<usize> {
            self.output_map.get(name).copied()
        }

        /// Set provenance dependents for the JIT kernel.
        pub fn set_provenance_dependents(&mut self, input_dependents: Vec<Vec<usize>>) {
            if let Some(ref mut prov) = self.provenance {
                // Compute per-step provenance from dependents
                let step_count = prov.node_clean.len();
                let mut step_prov = vec![0u64; step_count];
                for (input_idx, deps) in input_dependents.iter().enumerate() {
                    for &step_idx in deps {
                        if step_idx < step_count {
                            step_prov[step_idx] |= 1u64 << input_idx;
                        }
                    }
                }
                // Map to slot provenance
                // TODO: needs slot_bases from assembly. For now, approximate:
                // each step produces one output at slot coord_count + step_idx
                let mut slot_prov = vec![0u64; self.buffer.len()];
                for i in 0..self.coord_count.min(64) {
                    slot_prov[i] = 1u64 << i;
                }
                for i in 0..step_count {
                    let slot = self.coord_count + i; // approximate
                    if slot < slot_prov.len() {
                        slot_prov[slot] = step_prov[i];
                    }
                }
                prov.slot_provenance = slot_prov;
                prov.input_dependents = input_dependents;
            }
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

    // Extern functions for math operations (called from JIT code).
    extern "C" fn jit_sin(bits: u64) -> u64 { f64::from_bits(bits).sin().to_bits() }
    extern "C" fn jit_cos(bits: u64) -> u64 { f64::from_bits(bits).cos().to_bits() }
    extern "C" fn jit_tan(bits: u64) -> u64 { f64::from_bits(bits).tan().to_bits() }
    extern "C" fn jit_asin(bits: u64) -> u64 { f64::from_bits(bits).asin().to_bits() }
    extern "C" fn jit_acos(bits: u64) -> u64 { f64::from_bits(bits).acos().to_bits() }
    extern "C" fn jit_atan(bits: u64) -> u64 { f64::from_bits(bits).atan().to_bits() }
    extern "C" fn jit_sqrt(bits: u64) -> u64 { f64::from_bits(bits).sqrt().to_bits() }
    extern "C" fn jit_abs_f64(bits: u64) -> u64 { f64::from_bits(bits).abs().to_bits() }
    extern "C" fn jit_ln(bits: u64) -> u64 { f64::from_bits(bits).ln().to_bits() }
    extern "C" fn jit_exp(bits: u64) -> u64 { f64::from_bits(bits).exp().to_bits() }
    extern "C" fn jit_atan2(y_bits: u64, x_bits: u64) -> u64 {
        f64::from_bits(y_bits).atan2(f64::from_bits(x_bits)).to_bits()
    }
    extern "C" fn jit_pow(base_bits: u64, exp_bits: u64) -> u64 {
        f64::from_bits(base_bits).powf(f64::from_bits(exp_bits)).to_bits()
    }

    /// Extern function: weighted pick via alias table (called from JIT code).
    ///
    /// Performs O(1) alias sampling and value lookup. All array pointers
    /// are baked as i64 immediates in the JIT code.
    extern "C" fn jit_weighted_pick(
        input: u64,
        values_ptr: u64,
        biases_ptr: u64,
        primaries_ptr: u64,
        aliases_ptr: u64,
        n: u64,
    ) -> u64 {
        let n = n as usize;
        let slot = (input as usize) % n;
        let bias_test = ((input >> 32) as f64) / (u32::MAX as f64);
        unsafe {
            let biases = std::slice::from_raw_parts(biases_ptr as *const f64, n);
            let primaries = std::slice::from_raw_parts(primaries_ptr as *const u64, n);
            let aliases = std::slice::from_raw_parts(aliases_ptr as *const u64, n);
            let values = std::slice::from_raw_parts(values_ptr as *const u64, n);
            let index = if bias_test < biases[slot] {
                primaries[slot]
            } else {
                aliases[slot]
            };
            values[index as usize]
        }
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
        /// output[0] = weighted_pick(input, values_ptr, biases_ptr, primaries_ptr, aliases_ptr, n)
        WeightedPickConst(u64, u64, u64, u64, u64), // values_ptr, biases_ptr, primaries_ptr, aliases_ptr, n

        /// Unary f64 math function via extern call. The u8 identifies which function.
        /// 0=sin 1=cos 2=tan 3=asin 4=acos 5=atan 6=sqrt 7=abs 8=ln 9=exp
        MathUnary(u8),
        /// Binary f64 math function via extern call.
        /// 0=atan2 1=pow
        MathBinary(u8),

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
            // Math functions
            "sin" => JitOp::MathUnary(0),
            "cos" => JitOp::MathUnary(1),
            "tan" => JitOp::MathUnary(2),
            "asin" => JitOp::MathUnary(3),
            "acos" => JitOp::MathUnary(4),
            "atan" => JitOp::MathUnary(5),
            "sqrt" => JitOp::MathUnary(6),
            "abs_f64" => JitOp::MathUnary(7),
            "ln" => JitOp::MathUnary(8),
            "exp" => JitOp::MathUnary(9),
            "atan2" => JitOp::MathBinary(0),
            "pow" => JitOp::MathBinary(1),

            "weighted_pick" => {
                if consts.len() >= 5 {
                    JitOp::WeightedPickConst(consts[0], consts[1], consts[2], consts[3], consts[4])
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
        nodes: Vec<Box<dyn GkNode>>,
    ) -> Result<JitKernel, String> {
        compile_jit_impl(coord_count, total_slots, steps, output_map, nodes, false)
    }

    pub(crate) fn compile_jit_with_provenance(
        coord_count: usize,
        total_slots: usize,
        steps: Vec<(JitOp, Vec<usize>, Vec<usize>)>,
        output_map: HashMap<String, usize>,
        nodes: Vec<Box<dyn GkNode>>,
    ) -> Result<JitKernel, String> {
        compile_jit_impl(coord_count, total_slots, steps, output_map, nodes, true)
    }

    fn compile_jit_impl(
        coord_count: usize,
        total_slots: usize,
        steps: Vec<(JitOp, Vec<usize>, Vec<usize>)>,
        output_map: HashMap<String, usize>,
        nodes: Vec<Box<dyn GkNode>>,
        provenance: bool,
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
        jit_builder.symbol("jit_weighted_pick", jit_weighted_pick as *const u8);
        // Math externs
        jit_builder.symbol("jit_sin", jit_sin as *const u8);
        jit_builder.symbol("jit_cos", jit_cos as *const u8);
        jit_builder.symbol("jit_tan", jit_tan as *const u8);
        jit_builder.symbol("jit_asin", jit_asin as *const u8);
        jit_builder.symbol("jit_acos", jit_acos as *const u8);
        jit_builder.symbol("jit_atan", jit_atan as *const u8);
        jit_builder.symbol("jit_sqrt", jit_sqrt as *const u8);
        jit_builder.symbol("jit_abs_f64", jit_abs_f64 as *const u8);
        jit_builder.symbol("jit_ln", jit_ln as *const u8);
        jit_builder.symbol("jit_exp", jit_exp as *const u8);
        jit_builder.symbol("jit_atan2", jit_atan2 as *const u8);
        jit_builder.symbol("jit_pow", jit_pow as *const u8);

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

        // Declare extern: weighted_pick(u64, u64, u64, u64, u64, u64) -> u64
        let weighted_pick_func_id = {
            let mut sig = module.make_signature();
            for _ in 0..6 { sig.params.push(AbiParam::new(types::I64)); }
            sig.returns.push(AbiParam::new(types::I64));
            module.declare_function("jit_weighted_pick", Linkage::Import, &sig)
                .map_err(|e| format!("declare weighted_pick: {e}"))?
        };

        // Declare math externs: unary (u64) -> u64
        let math_unary_names = [
            "jit_sin", "jit_cos", "jit_tan", "jit_asin", "jit_acos",
            "jit_atan", "jit_sqrt", "jit_abs_f64", "jit_ln", "jit_exp",
        ];
        let mut math_unary_ids = Vec::new();
        for name in &math_unary_names {
            let mut sig = module.make_signature();
            sig.params.push(AbiParam::new(types::I64));
            sig.returns.push(AbiParam::new(types::I64));
            math_unary_ids.push(
                module.declare_function(name, Linkage::Import, &sig)
                    .map_err(|e| format!("declare {name}: {e}"))?
            );
        }

        // Declare math externs: binary (u64, u64) -> u64
        let math_binary_names = ["jit_atan2", "jit_pow"];
        let mut math_binary_ids = Vec::new();
        for name in &math_binary_names {
            let mut sig = module.make_signature();
            sig.params.push(AbiParam::new(types::I64));
            sig.params.push(AbiParam::new(types::I64));
            sig.returns.push(AbiParam::new(types::I64));
            math_binary_ids.push(
                module.declare_function(name, Linkage::Import, &sig)
                    .map_err(|e| format!("declare {name}: {e}"))?
            );
        }

        // Function signature depends on provenance mode:
        // Without: fn(coords: *const u64, buffer: *mut u64)
        // With:    fn(coords: *const u64, buffer: *mut u64, clean: *mut u8)
        let mut sig = module.make_signature();
        sig.params.push(AbiParam::new(types::I64)); // coords ptr
        sig.params.push(AbiParam::new(types::I64)); // buffer ptr
        if provenance {
            sig.params.push(AbiParam::new(types::I64)); // clean ptr
        }
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
            let clean_ptr = if provenance { Some(builder.block_params(block)[2]) } else { None };

            // Import extern functions for calls
            let hash_func_ref = module.declare_func_in_func(hash_func_id, builder.func);
            let interleave_func_ref = module.declare_func_in_func(interleave_func_id, builder.func);
            let shuffle_func_ref = module.declare_func_in_func(shuffle_func_id, builder.func);
            let lut_sample_func_ref = module.declare_func_in_func(lut_sample_func_id, builder.func);
            let weighted_pick_func_ref = module.declare_func_in_func(weighted_pick_func_id, builder.func);
            let math_unary_refs: Vec<_> = math_unary_ids.iter()
                .map(|id| module.declare_func_in_func(*id, builder.func))
                .collect();
            let math_binary_refs: Vec<_> = math_binary_ids.iter()
                .map(|id| module.declare_func_in_func(*id, builder.func))
                .collect();

            // Generate code for each step
            for (step_idx, (jit_op, input_slots, output_slots)) in steps.iter().enumerate() {
                // Provenance guard: if clean[step_idx] != 0, skip this node
                let skip_block = if let Some(cp) = clean_ptr {
                    let skip = builder.create_block();
                    let cont = builder.create_block();
                    // Load clean[step_idx] (u8)
                    let offset = builder.ins().iconst(types::I64, step_idx as i64);
                    let addr = builder.ins().iadd(cp, offset);
                    let flag = builder.ins().load(types::I8, ir::MemFlags::new(), addr, 0);
                    let zero = builder.ins().iconst(types::I8, 0);
                    let is_clean = builder.ins().icmp(ir::condcodes::IntCC::NotEqual, flag, zero);
                    builder.ins().brif(is_clean, skip, &[], cont, &[]);
                    builder.switch_to_block(cont);
                    builder.seal_block(cont);
                    Some(skip)
                } else {
                    None
                };
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

                    JitOp::WeightedPickConst(values_ptr, biases_ptr, primaries_ptr, aliases_ptr, n) => {
                        // Extern call: jit_weighted_pick(input, values_ptr, biases_ptr, primaries_ptr, aliases_ptr, n)
                        let input = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                        let v_ptr = builder.ins().iconst(types::I64, *values_ptr as i64);
                        let b_ptr = builder.ins().iconst(types::I64, *biases_ptr as i64);
                        let p_ptr = builder.ins().iconst(types::I64, *primaries_ptr as i64);
                        let a_ptr = builder.ins().iconst(types::I64, *aliases_ptr as i64);
                        let n_val = builder.ins().iconst(types::I64, *n as i64);
                        let call = builder.ins().call(
                            weighted_pick_func_ref,
                            &[input, v_ptr, b_ptr, p_ptr, a_ptr, n_val],
                        );
                        let result = builder.inst_results(call)[0];
                        store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                    }

                    JitOp::MathUnary(idx) => {
                        let input = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                        let func_ref = math_unary_refs[*idx as usize];
                        let call = builder.ins().call(func_ref, &[input]);
                        let result = builder.inst_results(call)[0];
                        store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                    }

                    JitOp::MathBinary(idx) => {
                        let a = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                        let b = load_slot(&mut builder, buffer_ptr, input_slots[1]);
                        let func_ref = math_binary_refs[*idx as usize];
                        let call = builder.ins().call(func_ref, &[a, b]);
                        let result = builder.inst_results(call)[0];
                        store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                    }

                    JitOp::Fallback => {
                        // Can't JIT this node — skip (caller should
                        // not include fallback ops in JIT steps)
                    }
                }

                // Provenance: set clean[step_idx] = 1, then jump to skip block
                if let (Some(cp), Some(skip)) = (clean_ptr, skip_block) {
                    let offset = builder.ins().iconst(types::I64, step_idx as i64);
                    let addr = builder.ins().iadd(cp, offset);
                    let one = builder.ins().iconst(types::I8, 1);
                    builder.ins().store(ir::MemFlags::new(), one, addr, 0);
                    builder.ins().jump(skip, &[]);
                    builder.switch_to_block(skip);
                    builder.seal_block(skip);
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
        let step_count = steps.len();

        if provenance {
            let code_fn_prov: unsafe fn(*const u64, *mut u64, *mut u8) =
                unsafe { mem::transmute(code_ptr) };
            // Dummy plain fn for into_parts compatibility
            let dummy: unsafe fn(*const u64, *mut u64) =
                unsafe { mem::transmute(code_ptr) };
            Ok(JitKernel {
                buffer: vec![0u64; total_slots],
                coord_count,
                output_map,
                code_fn: dummy,
                code_fn_prov: Some(code_fn_prov),
                _module: module,
                _nodes: nodes,
                provenance: Some(JitProvenanceState {
                    node_clean: vec![0u8; step_count],
                    input_dependents: Vec::new(),
                    slot_provenance: Vec::new(), // set via set_provenance_dependents
                    changed_mask: u64::MAX,
                }),
            })
        } else {
            let code_fn: unsafe fn(*const u64, *mut u64) =
                unsafe { mem::transmute(code_ptr) };
            Ok(JitKernel {
                buffer: vec![0u64; total_slots],
                coord_count,
                output_map,
                code_fn,
                code_fn_prov: None,
                _module: module,
                _nodes: nodes,
                provenance: None,
            })
        }
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
            let mut kernel = compile_jit(1, 2, steps, output_map, Vec::new()).unwrap();
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
            let mut kernel = compile_jit(1, 2, steps, output_map, Vec::new()).unwrap();
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
            let mut kernel = compile_jit(1, 2, steps, output_map, Vec::new()).unwrap();
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
            let mut kernel = compile_jit(1, 2, steps, output_map, Vec::new()).unwrap();
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
            let mut kernel = compile_jit(1, 2, steps, output_map, Vec::new()).unwrap();

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
            let mut kernel = compile_jit(1, 2, steps, output_map, Vec::new()).unwrap();

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
            let mut kernel = compile_jit(1, 3, steps, output_map, Vec::new()).unwrap();

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
            let mut kernel = compile_jit(1, 2, steps, output_map, Vec::new()).unwrap();

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
            let mut kernel = compile_jit(2, 3, steps, output_map, Vec::new()).unwrap();

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
            let mut kernel = compile_jit(1, 4, steps, output_map, Vec::new()).unwrap();

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
            let mut kernel = compile_jit(1, 2, steps, output_map, Vec::new()).unwrap();

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
            let mut kernel = compile_jit(1, 2, steps, output_map, Vec::new()).unwrap();

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
            let mut kernel = compile_jit(1, 2, steps, output_map, Vec::new()).unwrap();

            kernel.eval(&[3.7f64.to_bits()]);
            assert_eq!(kernel.get("out"), 3); // truncate toward zero
        }

        #[test]
        fn jit_round_to_u64() {
            let steps = vec![(JitOp::RoundToU64, vec![0], vec![1])];
            let mut output_map = HashMap::new();
            output_map.insert("out".into(), 1);
            let mut kernel = compile_jit(1, 2, steps, output_map, Vec::new()).unwrap();

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
            let mut kernel = compile_jit(1, 2, steps, output_map, Vec::new()).unwrap();

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
            let mut kernel = compile_jit(1, 2, steps, output_map, Vec::new()).unwrap();

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
            let mut kernel = compile_jit(1, 2, steps, output_map, Vec::new()).unwrap();

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
            let mut kernel = compile_jit(1, 2, steps, output_map, Vec::new()).unwrap();

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
            let mut kernel = compile_jit(1, 2, steps, output_map, Vec::new()).unwrap();

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
            let mut kernel = compile_jit(1, 2, steps, output_map, Vec::new()).unwrap();

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
            let mut kernel = compile_jit(1, 2, steps, output_map, Vec::new()).unwrap();

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
            let mut kernel = compile_jit(1, 3, steps, output_map, Vec::new()).unwrap();

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
            let mut kernel = compile_jit(1, 4, steps, output_map, Vec::new()).unwrap();

            kernel.eval(&[5]);
            // (5 + 10) * 3 = 45, 45 % 100 = 45
            assert_eq!(kernel.get("out"), 45);
        }
    }
}

#[cfg(feature = "jit")]
pub use inner::*;
