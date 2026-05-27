// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! JIT codegen: Cranelift IR generation, operation classification, and
//! extern "C" runtime helpers called from JIT-compiled code.
//!
//! `JitOp` classifies each DAG node into an inline IR pattern or an
//! extern call. `compile_jit_impl` lowers a slice of `(JitOp, inputs,
//! outputs)` steps into a single native function via Cranelift.
//! The four `compile_jit_*` constructors wrap the result in the
//! appropriate kernel struct from `kernels`.

use std::collections::HashMap;
use std::mem;

use cranelift_codegen::ir::{self, AbiParam, InstBuilder, types};
use cranelift_codegen::settings::{self, Configurable};
use cranelift_frontend::{FunctionBuilder, FunctionBuilderContext};
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{Linkage, Module};

use crate::node::GkNode;

use super::kernels::{
    JitCore, JitKernelPull, JitKernelPush, JitKernelPushPull, JitKernelRaw,
    compute_jit_slot_provenance,
};

// ── Extern "C" runtime helpers ─────────────────────────────

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

// ── Catchable predicate violations via setjmp/longjmp ─────────
//
// Cranelift-JIT emits DWARF unwind info (`unwind_info=true`) but
// does not call `__register_frame`; teaching the system
// unwinder about JIT frames needs either an upstream Cranelift
// change or a personality-routine shim that's a project on its
// own. We take the self-contained route instead: a setjmp
// sentinel installed by the Rust eval wrapper, and extern
// helpers that `longjmp` back to it on violation.
//
// The longjmp skips over the JIT frame entirely — no unwind,
// no personality lookup, no catch-block walk. Control returns
// to the Rust wrapper which reads the violation message from a
// thread-local and raises a normal Rust `panic!`. That panic
// unwinds through the Rust caller's frames (which have proper
// `rust_eh_personality` FDEs) and `catch_unwind` catches it
// like any other panic. Fail-path callers no longer lose the
// entire process to an abort.
//
// Safety
//   - longjmp skips C-level destructors. The JIT code is pure
//     machine code with no Drop semantics, so nothing leaks.
//     The extern helpers themselves hold no resources.
//   - The thread-local buffer is per-thread, so concurrent
//     kernels on different tokio worker threads don't share
//     state. Nesting a kernel.eval inside another kernel.eval
//     on the same thread would clobber the buffer — we don't
//     do that anywhere today; if it becomes a concern, push a
//     stack of buffers instead of a single slot.

/// Platform-independent jmp_buf shim. Allocated oversize (512
/// bytes, 16-aligned) so the biggest real platform buffer
/// (glibc Linux: ~200 bytes, macOS: ~192) fits with margin.
/// We link against the C library's `_setjmp` / `_longjmp`
/// symbols directly — the `setjmp` macro in the glibc header
/// expands to `__sigsetjmp`, which saves the signal mask; we
/// don't need that and `_setjmp` is faster.
#[repr(C, align(16))]
struct JitJmpBuf([u8; 512]);

unsafe extern "C" {
    fn _setjmp(env: *mut JitJmpBuf) -> i32;
    fn _longjmp(env: *mut JitJmpBuf, val: i32) -> !;
}

use std::cell::{Cell, RefCell};
thread_local! {
    /// Set by [`invoke_with_catch`] before entering JIT code;
    /// cleared on return. The extern longjmp helpers consult
    /// this slot to find their return target. `None` means "no
    /// wrapper installed" → fall back to abort so violations
    /// outside a catching wrapper still terminate cleanly
    /// rather than triggering undefined behavior.
    static JIT_JMP_BUF: Cell<Option<*mut JitJmpBuf>> = const { Cell::new(None) };
    /// Populated by the extern helpers right before the
    /// longjmp; drained by the wrapper after setjmp returns
    /// non-zero.
    static JIT_VIOLATION_MSG: RefCell<Option<String>> = const { RefCell::new(None) };
}

/// Store the violation message and longjmp back to the wrapper.
/// Used by every predicate extern on the fail path. If no
/// wrapper is installed on the current thread (e.g. someone
/// calling the JIT code directly without `invoke_with_catch`),
/// prints the message and aborts — matches the original
/// behavior for that call pattern.
fn jit_violation_longjmp(msg: String) -> ! {
    JIT_VIOLATION_MSG.with(|m| *m.borrow_mut() = Some(msg.clone()));
    let buf_ptr: Option<*mut JitJmpBuf> = JIT_JMP_BUF.with(|b| b.get());
    match buf_ptr {
        Some(ptr) => unsafe { _longjmp(ptr, 1) },
        None => {
            let mut err = std::io::stderr().lock();
            use std::io::Write;
            let _ = writeln!(err, "{msg}");
            let _ = err.flush();
            std::process::abort();
        }
    }
}

/// RAII restore of the enclosing thread-local `JIT_JMP_BUF`
/// slot. Ensures the wrapper's buffer pointer doesn't outlive
/// its stack frame — even if the wrapped closure panics for a
/// reason unrelated to the JIT predicate (a bug in a
/// non-JIT sub-path, an OOM, etc.) the guard's `Drop`
/// reinstates the previous slot so the next `invoke_with_catch`
/// call doesn't see a dangling pointer.
struct JmpBufGuard {
    prev: Option<*mut JitJmpBuf>,
}

impl Drop for JmpBufGuard {
    fn drop(&mut self) {
        JIT_JMP_BUF.with(|b| b.set(self.prev));
    }
}

/// Wrapper used by every kernel variant's `eval` to set up the
/// setjmp sentinel, run the closure (which calls into JIT
/// code), and translate a longjmp return into a Rust panic
/// carrying the violation message. The panic happens in Rust
/// land, so `catch_unwind` catches it normally.
///
/// Both entry/exit paths flow through the [`JmpBufGuard`] so a
/// panic from inside `f()` that isn't a JIT violation still
/// restores the outer slot correctly.
pub(crate) fn invoke_with_catch<F: FnOnce()>(f: F) {
    use std::mem::MaybeUninit;
    let mut buf: MaybeUninit<JitJmpBuf> = MaybeUninit::uninit();
    let buf_ptr = buf.as_mut_ptr();
    // Install the jmp_buf for the duration of the call. The
    // guard restores the previous slot on every exit path
    // (normal return, longjmp, or non-JIT panic unwinding
    // through our frame).
    let prev: Option<*mut JitJmpBuf> = JIT_JMP_BUF.with(|b| b.replace(Some(buf_ptr)));
    let _guard = JmpBufGuard { prev };
    let jmpval = unsafe { _setjmp(buf_ptr) };
    if jmpval == 0 {
        f();
    } else {
        // longjmp return. The guard will restore the outer
        // slot when this frame exits; drain the violation
        // message and raise a normal Rust panic so the
        // caller's `catch_unwind` can see it.
        let msg = JIT_VIOLATION_MSG.with(|m| m.borrow_mut().take())
            .unwrap_or_else(|| "JIT predicate violation (no message)".into());
        panic!("{msg}");
    }
}

/// Extern function: longjmp back to the enclosing wrapper with
/// an `is_positive` violation message. Called from JIT code on
/// the predicate-fail path.
extern "C" fn jit_is_positive_fail(value: u64) -> u64 {
    jit_violation_longjmp(
        format!("is_positive: value must be > 0, got {value}"),
    );
}

/// Extern function: longjmp back to the enclosing wrapper with
/// an `in_range` violation message.
extern "C" fn jit_in_range_fail(value: u64, lo: u64, hi: u64) -> u64 {
    jit_violation_longjmp(
        format!("in_range: value {value} outside [{lo}, {hi}]"),
    );
}

/// Extern function: longjmp back to the enclosing wrapper with
/// an `is_one_of` violation message. The allow-list isn't
/// threaded through the call (it would bloat the ABI); the
/// Phase-1 closure path retains the full list in its message
/// when callers need maximum detail.
extern "C" fn jit_is_one_of_fail(value: u64) -> u64 {
    jit_violation_longjmp(
        format!("is_one_of: value {value} not in configured allow-list"),
    );
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

// ── JitOp ──────────────────────────────────────────────────

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

    // --- Two-wire u64 integer ops ---
    /// output = input[0] + input[1]  (wrapping)
    U64Add2,
    /// output = input[0] - input[1]  (wrapping)
    U64Sub2,
    /// output = input[0] * input[1]  (wrapping)
    U64Mul2,
    /// output = input[0] / input[1]  (0 if divisor is 0)
    U64Div2,
    /// output = input[0] % input[1]  (0 if divisor is 0)
    U64Mod2,
    /// output = input[0] & input[1]
    U64And,
    /// output = input[0] | input[1]
    U64Or,
    /// output = input[0] ^ input[1]
    U64Xor,
    /// output = input[0] << input[1]
    U64Shl,
    /// output = input[0] >> input[1]  (logical)
    U64Shr,
    /// output = !input[0]  (unary bitwise NOT)
    U64Not,

    // --- Inline binary f64 arithmetic (no extern call) ---
    /// output = input as f64 (integer to float conversion, not bit reinterpret)
    ToF64,

    /// output = f64(a) + f64(b)
    F64Add,
    /// output = f64(a) - f64(b)
    F64Sub,
    /// output = f64(a) * f64(b)
    F64Mul,
    /// output = f64(a) / f64(b) (0 if b==0)
    F64Div,
    /// output = f64(a) % f64(b) (0 if b==0)
    F64Mod,

    /// Parameter predicate: pass input[0] through to output[0];
    /// if input[0] == 0, call `jit_is_positive_fail` (panics).
    IsPositiveCheck,
    /// Parameter predicate: pass input[0] through to output[0];
    /// if input[0] < lo or input[0] > hi, call
    /// `jit_in_range_fail` (panics). Stored as (lo, hi).
    InRangeCheck(u64, u64),
    /// Parameter predicate: pass input[0] through to output[0];
    /// if input[0] is not in the allow-list, call
    /// `jit_is_one_of_fail` (aborts). Allow-list baked as a
    /// vector of u64 constants.
    IsOneOfCheck(Vec<u64>),

    /// Fallback: call the Phase 2 closure
    Fallback,
}

// ── Node classification ────────────────────────────────────

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
        "to_f64" => JitOp::ToF64,
        // Two-wire u64 ops (no constants)
        "u64_add" => JitOp::U64Add2,
        "u64_sub" => JitOp::U64Sub2,
        "u64_mul" => JitOp::U64Mul2,
        "u64_div" => JitOp::U64Div2,
        "u64_mod" => JitOp::U64Mod2,
        "u64_and" => JitOp::U64And,
        "u64_or"  => JitOp::U64Or,
        "u64_xor" => JitOp::U64Xor,
        "u64_shl" => JitOp::U64Shl,
        "u64_shr" => JitOp::U64Shr,
        "u64_not" => JitOp::U64Not,

        "f64_add" => JitOp::F64Add,
        "f64_sub" => JitOp::F64Sub,
        "f64_mul" => JitOp::F64Mul,
        "f64_div" => JitOp::F64Div,
        "f64_mod" => JitOp::F64Mod,

        "weighted_pick" => {
            if consts.len() >= 5 {
                JitOp::WeightedPickConst(consts[0], consts[1], consts[2], consts[3], consts[4])
            } else {
                JitOp::Fallback
            }
        }

        // ── Parameter helpers (SRD 12) ─────────────────────────
        // `is_positive` / `in_range` are JIT-lowered inline: one
        // comparison on the happy path, an extern call on the
        // fail path (which panics). The pass-through is a plain
        // store, no function call overhead on the typical cycle.
        "is_positive" => JitOp::IsPositiveCheck,
        "in_range" => {
            if consts.len() >= 2 {
                JitOp::InRangeCheck(consts[0], consts[1])
            } else {
                JitOp::Fallback
            }
        }
        "is_one_of" => {
            if consts.is_empty() {
                JitOp::Fallback
            } else {
                JitOp::IsOneOfCheck(consts)
            }
        }
        // The remaining param helpers stay on the Phase-2
        // `compiled_u64` closure — by design, not oversight:
        //   * `required` / `this_or` rely on `Value::None`
        //     sentinel semantics that don't round-trip through
        //     the JIT's u64 buffer without tagging.
        //   * `matches` is regex-backed; the regex object lives
        //     on the node struct and can't be JIT-inlined.
        // Runtime-context nodes (`control`, `rate`, `concurrency`,
        // `phase`, `cycle`) all read from runtime globals /
        // thread-locals and return f64 or String — values that
        // don't belong on the JIT happy path. They're correctly
        // fast at Phase-1/2.
        _ => JitOp::Fallback,
    }
}

// ── Kernel constructors ────────────────────────────────────

/// Compile a set of JIT steps into a raw (no-provenance) native kernel.
///
/// Each step has: jit_op, input_slots (buffer indices), output_slots.
/// The generated function reads coords from the buffer, executes
/// all steps in order, and writes results to the buffer.
pub(crate) fn compile_jit_raw(
    coord_count: usize,
    total_slots: usize,
    steps: Vec<(JitOp, Vec<usize>, Vec<usize>)>,
    output_map: HashMap<String, usize>,
    nodes: Vec<Box<dyn GkNode>>,
) -> Result<JitKernelRaw, String> {
    let (raw_fn, _, module) = compile_jit_impl(&steps, false)?;
    Ok(JitKernelRaw {
        core: JitCore { buffer: vec![0u64; total_slots], coord_count, output_map, _module: module, _nodes: nodes },
        code_fn: raw_fn,
    })
}

/// Compile a set of JIT steps into a push (per-node dirty tracking) native kernel.
pub(crate) fn compile_jit_push(
    coord_count: usize,
    total_slots: usize,
    steps: Vec<(JitOp, Vec<usize>, Vec<usize>)>,
    output_map: HashMap<String, usize>,
    nodes: Vec<Box<dyn GkNode>>,
    input_dependents: Vec<Vec<usize>>,
) -> Result<JitKernelPush, String> {
    let step_count = steps.len();
    let (_, prov_fn, module) = compile_jit_impl(&steps, true)?;
    Ok(JitKernelPush {
        core: JitCore { buffer: vec![0u64; total_slots], coord_count, output_map, _module: module, _nodes: nodes },
        code_fn_prov: prov_fn,
        node_clean: vec![0u8; step_count],
        input_dependents,
    })
}

/// Compile a set of JIT steps into a pull (cone guard) native kernel.
pub(crate) fn compile_jit_pull(
    coord_count: usize,
    total_slots: usize,
    steps: Vec<(JitOp, Vec<usize>, Vec<usize>)>,
    output_map: HashMap<String, usize>,
    nodes: Vec<Box<dyn GkNode>>,
    input_dependents: &[Vec<usize>],
) -> Result<JitKernelPull, String> {
    let step_count = steps.len();
    let buffer_len = total_slots;
    // Pull uses the RAW jit function (no per-node clean checks)
    let (raw_fn, _, module) = compile_jit_impl(&steps, false)?;
    let slot_provenance = compute_jit_slot_provenance(coord_count, buffer_len, step_count, input_dependents);
    Ok(JitKernelPull {
        core: JitCore { buffer: vec![0u64; total_slots], coord_count, output_map, _module: module, _nodes: nodes },
        code_fn: raw_fn,
        slot_provenance,
        changed_mask: u64::MAX,
    })
}

/// Compile a set of JIT steps into a push+pull (full optimization) native kernel.
pub(crate) fn compile_jit_push_pull(
    coord_count: usize,
    total_slots: usize,
    steps: Vec<(JitOp, Vec<usize>, Vec<usize>)>,
    output_map: HashMap<String, usize>,
    nodes: Vec<Box<dyn GkNode>>,
    input_dependents: Vec<Vec<usize>>,
) -> Result<JitKernelPushPull, String> {
    let step_count = steps.len();
    let buffer_len = total_slots;
    let (_, prov_fn, module) = compile_jit_impl(&steps, true)?;
    let slot_provenance = compute_jit_slot_provenance(coord_count, buffer_len, step_count, &input_dependents);
    Ok(JitKernelPushPull {
        core: JitCore { buffer: vec![0u64; total_slots], coord_count, output_map, _module: module, _nodes: nodes },
        code_fn_prov: prov_fn,
        node_clean: vec![0u8; step_count],
        input_dependents,
        slot_provenance,
        changed_mask: u64::MAX,
    })
}

// ── Core Cranelift IR generation ───────────────────────────

/// Core JIT compilation. Returns (raw_fn, prov_fn, module).
/// If provenance=false, prov_fn is a dummy transmute of raw_fn.
/// If provenance=true, raw_fn is a dummy transmute of prov_fn.
fn compile_jit_impl(
    steps: &[(JitOp, Vec<usize>, Vec<usize>)],
    provenance: bool,
) -> Result<(
    unsafe fn(*const u64, *mut u64),
    unsafe fn(*const u64, *mut u64, *mut u8),
    JITModule,
), String> {
    let mut flag_builder = settings::builder();
    flag_builder.set("opt_level", "speed").unwrap();
    // Emit DWARF/SEH unwind tables so a panic raised from an
    // `extern "C-unwind"` helper (e.g. param-helper predicate
    // failures) can unwind through the JIT frame back to the
    // Rust caller. Without this Cranelift emits bare frames and
    // the libstd unwinder aborts on panic.
    flag_builder.set("unwind_info", "true").unwrap();
    flag_builder.set("preserve_frame_pointers", "true").unwrap();
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
    // Parameter-helper predicates (SRD 12 §"Parameter resolution
    // and validation"): happy path is inline, violation is an
    // extern call that never returns.
    jit_builder.symbol("jit_is_positive_fail", jit_is_positive_fail as *const u8);
    jit_builder.symbol("jit_in_range_fail", jit_in_range_fail as *const u8);
    jit_builder.symbol("jit_is_one_of_fail", jit_is_one_of_fail as *const u8);
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

    // Declare param-helper extern: jit_is_positive_fail(u64) -> u64
    // (never returns, but the ABI requires a return type).
    let is_positive_fail_id = {
        let mut sig = module.make_signature();
        sig.params.push(AbiParam::new(types::I64));
        sig.returns.push(AbiParam::new(types::I64));
        module.declare_function("jit_is_positive_fail", Linkage::Import, &sig)
            .map_err(|e| format!("declare is_positive_fail: {e}"))?
    };

    // Declare param-helper extern: jit_in_range_fail(u64, u64, u64) -> u64
    let in_range_fail_id = {
        let mut sig = module.make_signature();
        for _ in 0..3 { sig.params.push(AbiParam::new(types::I64)); }
        sig.returns.push(AbiParam::new(types::I64));
        module.declare_function("jit_in_range_fail", Linkage::Import, &sig)
            .map_err(|e| format!("declare in_range_fail: {e}"))?
    };

    // Declare param-helper extern: jit_is_one_of_fail(u64) -> u64
    let is_one_of_fail_id = {
        let mut sig = module.make_signature();
        sig.params.push(AbiParam::new(types::I64));
        sig.returns.push(AbiParam::new(types::I64));
        module.declare_function("jit_is_one_of_fail", Linkage::Import, &sig)
            .map_err(|e| format!("declare is_one_of_fail: {e}"))?
    };

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
        let is_positive_fail_ref = module.declare_func_in_func(is_positive_fail_id, builder.func);
        let in_range_fail_ref = module.declare_func_in_func(in_range_fail_id, builder.func);
        let is_one_of_fail_ref = module.declare_func_in_func(is_one_of_fail_id, builder.func);
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

                JitOp::ToF64 => {
                    let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let fval = builder.ins().fcvt_from_uint(types::F64, val);
                    store_slot_f64(&mut builder, buffer_ptr, output_slots[0], fval);
                }

                // Two-wire u64 integer ops — pure Cranelift, no extern call
                JitOp::U64Add2 => {
                    let a = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let b = load_slot(&mut builder, buffer_ptr, input_slots[1]);
                    let result = builder.ins().iadd(a, b);
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::U64Sub2 => {
                    let a = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let b = load_slot(&mut builder, buffer_ptr, input_slots[1]);
                    let result = builder.ins().isub(a, b);
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::U64Mul2 => {
                    let a = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let b = load_slot(&mut builder, buffer_ptr, input_slots[1]);
                    let result = builder.ins().imul(a, b);
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::U64Div2 => {
                    let a = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let b = load_slot(&mut builder, buffer_ptr, input_slots[1]);
                    // Guard: if b == 0, store 0; else store a / b.
                    // Must branch because udiv traps on zero divisor.
                    let zero = builder.ins().iconst(types::I64, 0);
                    let is_zero = builder.ins().icmp(ir::condcodes::IntCC::Equal, b, zero);
                    let div_block = builder.create_block();
                    let merge_block = builder.create_block();
                    builder.append_block_param(merge_block, types::I64);
                    builder.ins().brif(is_zero, merge_block, &[zero], div_block, &[]);
                    builder.switch_to_block(div_block);
                    builder.seal_block(div_block);
                    let div_result = builder.ins().udiv(a, b);
                    builder.ins().jump(merge_block, &[div_result]);
                    builder.switch_to_block(merge_block);
                    builder.seal_block(merge_block);
                    let result = builder.block_params(merge_block)[0];
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::U64Mod2 => {
                    let a = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let b = load_slot(&mut builder, buffer_ptr, input_slots[1]);
                    // Guard: if b == 0, store 0; else store a % b.
                    // Must branch because urem traps on zero divisor.
                    let zero = builder.ins().iconst(types::I64, 0);
                    let is_zero = builder.ins().icmp(ir::condcodes::IntCC::Equal, b, zero);
                    let rem_block = builder.create_block();
                    let merge_block = builder.create_block();
                    builder.append_block_param(merge_block, types::I64);
                    builder.ins().brif(is_zero, merge_block, &[zero], rem_block, &[]);
                    builder.switch_to_block(rem_block);
                    builder.seal_block(rem_block);
                    let rem_result = builder.ins().urem(a, b);
                    builder.ins().jump(merge_block, &[rem_result]);
                    builder.switch_to_block(merge_block);
                    builder.seal_block(merge_block);
                    let result = builder.block_params(merge_block)[0];
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::U64And => {
                    let a = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let b = load_slot(&mut builder, buffer_ptr, input_slots[1]);
                    let result = builder.ins().band(a, b);
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::U64Or => {
                    let a = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let b = load_slot(&mut builder, buffer_ptr, input_slots[1]);
                    let result = builder.ins().bor(a, b);
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::U64Xor => {
                    let a = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let b = load_slot(&mut builder, buffer_ptr, input_slots[1]);
                    let result = builder.ins().bxor(a, b);
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::U64Shl => {
                    let a = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let b = load_slot(&mut builder, buffer_ptr, input_slots[1]);
                    let result = builder.ins().ishl(a, b);
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::U64Shr => {
                    let a = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let b = load_slot(&mut builder, buffer_ptr, input_slots[1]);
                    let result = builder.ins().ushr(a, b);
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::U64Not => {
                    let a = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let result = builder.ins().bnot(a);
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }

                // Inline binary f64 arithmetic — pure Cranelift, no extern call
                JitOp::F64Add => {
                    let a = load_slot_f64(&mut builder, buffer_ptr, input_slots[0]);
                    let b = load_slot_f64(&mut builder, buffer_ptr, input_slots[1]);
                    let result = builder.ins().fadd(a, b);
                    store_slot_f64(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::F64Sub => {
                    let a = load_slot_f64(&mut builder, buffer_ptr, input_slots[0]);
                    let b = load_slot_f64(&mut builder, buffer_ptr, input_slots[1]);
                    let result = builder.ins().fsub(a, b);
                    store_slot_f64(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::F64Mul => {
                    let a = load_slot_f64(&mut builder, buffer_ptr, input_slots[0]);
                    let b = load_slot_f64(&mut builder, buffer_ptr, input_slots[1]);
                    let result = builder.ins().fmul(a, b);
                    store_slot_f64(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::F64Div => {
                    let a = load_slot_f64(&mut builder, buffer_ptr, input_slots[0]);
                    let b = load_slot_f64(&mut builder, buffer_ptr, input_slots[1]);
                    // Guard: if b == 0, result = 0; else result = a / b
                    let zero = builder.ins().f64const(0.0);
                    let is_zero = builder.ins().fcmp(ir::condcodes::FloatCC::Equal, b, zero);
                    let div_result = builder.ins().fdiv(a, b);
                    let result = builder.ins().select(is_zero, zero, div_result);
                    store_slot_f64(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::F64Mod => {
                    let a = load_slot_f64(&mut builder, buffer_ptr, input_slots[0]);
                    let b = load_slot_f64(&mut builder, buffer_ptr, input_slots[1]);
                    // a % b = a - floor(a / b) * b, guarded for b == 0
                    let zero = builder.ins().f64const(0.0);
                    let is_zero = builder.ins().fcmp(ir::condcodes::FloatCC::Equal, b, zero);
                    let quotient = builder.ins().fdiv(a, b);
                    let floored = builder.ins().floor(quotient);
                    let product = builder.ins().fmul(floored, b);
                    let mod_result = builder.ins().fsub(a, product);
                    let result = builder.ins().select(is_zero, zero, mod_result);
                    store_slot_f64(&mut builder, buffer_ptr, output_slots[0], result);
                }

                JitOp::IsPositiveCheck => {
                    // if input == 0: call jit_is_positive_fail (panics);
                    // else: store input → output.
                    // The branch splits to a fail block for the
                    // violation path; the merge reads through the
                    // common path after either branch completes.
                    let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let zero = builder.ins().iconst(types::I64, 0);
                    let is_zero = builder.ins().icmp(
                        ir::condcodes::IntCC::Equal, val, zero,
                    );
                    let fail_block = builder.create_block();
                    let ok_block = builder.create_block();
                    builder.ins().brif(is_zero, fail_block, &[], ok_block, &[]);

                    builder.switch_to_block(fail_block);
                    builder.seal_block(fail_block);
                    let _ = builder.ins().call(is_positive_fail_ref, &[val]);
                    // Extern panics — this is unreachable. Jump to
                    // ok_block to keep the IR well-formed; the
                    // branch never runs in practice.
                    builder.ins().jump(ok_block, &[]);

                    builder.switch_to_block(ok_block);
                    builder.seal_block(ok_block);
                    store_slot(&mut builder, buffer_ptr, output_slots[0], val);
                }

                JitOp::InRangeCheck(lo, hi) => {
                    // if input < lo || input > hi: call
                    // jit_in_range_fail (panics); else store
                    // input → output.
                    let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let lo_v = builder.ins().iconst(types::I64, *lo as i64);
                    let hi_v = builder.ins().iconst(types::I64, *hi as i64);
                    let below = builder.ins().icmp(
                        ir::condcodes::IntCC::UnsignedLessThan, val, lo_v,
                    );
                    let above = builder.ins().icmp(
                        ir::condcodes::IntCC::UnsignedGreaterThan, val, hi_v,
                    );
                    let out_of_range = builder.ins().bor(below, above);

                    let fail_block = builder.create_block();
                    let ok_block = builder.create_block();
                    builder.ins().brif(out_of_range, fail_block, &[], ok_block, &[]);

                    builder.switch_to_block(fail_block);
                    builder.seal_block(fail_block);
                    let _ = builder.ins().call(
                        in_range_fail_ref, &[val, lo_v, hi_v],
                    );
                    builder.ins().jump(ok_block, &[]);

                    builder.switch_to_block(ok_block);
                    builder.seal_block(ok_block);
                    store_slot(&mut builder, buffer_ptr, output_slots[0], val);
                }

                JitOp::IsOneOfCheck(allowed) => {
                    // Unroll the allow-list as N inline eq
                    // comparisons OR'd together. Fast-path is
                    // 1–8 values (the common case); pathologically
                    // large allow-lists still JIT but cost N
                    // comparisons per cycle.
                    let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let mut any_match = builder.ins().iconst(types::I8, 0);
                    for allow in allowed.iter() {
                        let c = builder.ins().iconst(types::I64, *allow as i64);
                        let eq = builder.ins().icmp(
                            ir::condcodes::IntCC::Equal, val, c,
                        );
                        any_match = builder.ins().bor(any_match, eq);
                    }
                    let fail_block = builder.create_block();
                    let ok_block = builder.create_block();
                    // If any_match == 0 (no equality hit),
                    // branch to the fail extern. Otherwise
                    // jump straight to ok_block.
                    builder.ins().brif(any_match, ok_block, &[], fail_block, &[]);

                    builder.switch_to_block(fail_block);
                    builder.seal_block(fail_block);
                    let _ = builder.ins().call(is_one_of_fail_ref, &[val]);
                    builder.ins().jump(ok_block, &[]);

                    builder.switch_to_block(ok_block);
                    builder.seal_block(ok_block);
                    store_slot(&mut builder, buffer_ptr, output_slots[0], val);
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

    if provenance {
        let prov_fn: unsafe fn(*const u64, *mut u64, *mut u8) =
            unsafe { mem::transmute(code_ptr) };
        let dummy_raw: unsafe fn(*const u64, *mut u64) =
            unsafe { mem::transmute(code_ptr) };
        Ok((dummy_raw, prov_fn, module))
    } else {
        let raw_fn: unsafe fn(*const u64, *mut u64) =
            unsafe { mem::transmute(code_ptr) };
        let dummy_prov: unsafe fn(*const u64, *mut u64, *mut u8) =
            unsafe { mem::transmute(code_ptr) };
        Ok((raw_fn, dummy_prov, module))
    }
}

// ── Buffer slot helpers ────────────────────────────────────

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

// ── Tests ──────────────────────────────────────────────────

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
        let mut kernel = compile_jit_raw(1, 2, steps, output_map, Vec::new()).unwrap();
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
        let mut kernel = compile_jit_raw(1, 2, steps, output_map, Vec::new()).unwrap();
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
        let mut kernel = compile_jit_raw(1, 2, steps, output_map, Vec::new()).unwrap();
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
        let mut kernel = compile_jit_raw(1, 2, steps, output_map, Vec::new()).unwrap();
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
        let mut kernel = compile_jit_raw(1, 2, steps, output_map, Vec::new()).unwrap();

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
        let mut kernel = compile_jit_raw(1, 2, steps, output_map, Vec::new()).unwrap();

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
        let mut kernel = compile_jit_raw(1, 3, steps, output_map, Vec::new()).unwrap();

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
        let mut kernel = compile_jit_raw(1, 2, steps, output_map, Vec::new()).unwrap();

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
        let mut kernel = compile_jit_raw(2, 3, steps, output_map, Vec::new()).unwrap();

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
        let mut kernel = compile_jit_raw(1, 4, steps, output_map, Vec::new()).unwrap();

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
        let mut kernel = compile_jit_raw(1, 2, steps, output_map, Vec::new()).unwrap();

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
        let mut kernel = compile_jit_raw(1, 2, steps, output_map, Vec::new()).unwrap();

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
        let mut kernel = compile_jit_raw(1, 2, steps, output_map, Vec::new()).unwrap();

        kernel.eval(&[3.7f64.to_bits()]);
        assert_eq!(kernel.get("out"), 3); // truncate toward zero
    }

    #[test]
    fn jit_round_to_u64() {
        let steps = vec![(JitOp::RoundToU64, vec![0], vec![1])];
        let mut output_map = HashMap::new();
        output_map.insert("out".into(), 1);
        let mut kernel = compile_jit_raw(1, 2, steps, output_map, Vec::new()).unwrap();

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
        let mut kernel = compile_jit_raw(1, 2, steps, output_map, Vec::new()).unwrap();

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
        let mut kernel = compile_jit_raw(1, 2, steps, output_map, Vec::new()).unwrap();

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
        let mut kernel = compile_jit_raw(1, 2, steps, output_map, Vec::new()).unwrap();

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
        let mut kernel = compile_jit_raw(1, 2, steps, output_map, Vec::new()).unwrap();

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
        let mut kernel = compile_jit_raw(1, 2, steps, output_map, Vec::new()).unwrap();

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
        let mut kernel = compile_jit_raw(1, 2, steps, output_map, Vec::new()).unwrap();

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
        let mut kernel = compile_jit_raw(1, 2, steps, output_map, Vec::new()).unwrap();

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
        let mut kernel = compile_jit_raw(1, 3, steps, output_map, Vec::new()).unwrap();

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
        let mut kernel = compile_jit_raw(1, 4, steps, output_map, Vec::new()).unwrap();

        kernel.eval(&[5]);
        // (5 + 10) * 3 = 45, 45 % 100 = 45
        assert_eq!(kernel.get("out"), 45);
    }

    // ── Parameter helper predicates ────────────────────────────

    #[test]
    fn jit_is_positive_check_passes_positive() {
        let steps = vec![
            (JitOp::IsPositiveCheck, vec![0], vec![1]),
        ];
        let mut output_map = HashMap::new();
        output_map.insert("out".into(), 1);
        let mut kernel = compile_jit_raw(1, 2, steps, output_map, Vec::new()).unwrap();
        kernel.eval(&[42]);
        assert_eq!(kernel.get("out"), 42);
        // Large values pass through unchanged — happy path is a
        // bare store, not a clamp.
        kernel.eval(&[u64::MAX]);
        assert_eq!(kernel.get("out"), u64::MAX);
    }

    #[test]
    fn jit_in_range_check_passes_interior() {
        let steps = vec![
            (JitOp::InRangeCheck(10, 100), vec![0], vec![1]),
        ];
        let mut output_map = HashMap::new();
        output_map.insert("out".into(), 1);
        let mut kernel = compile_jit_raw(1, 2, steps, output_map, Vec::new()).unwrap();
        kernel.eval(&[50]);
        assert_eq!(kernel.get("out"), 50);
        // Boundaries are inclusive.
        kernel.eval(&[10]);
        assert_eq!(kernel.get("out"), 10);
        kernel.eval(&[100]);
        assert_eq!(kernel.get("out"), 100);
    }

    // Violation paths for both predicates abort the process
    // (see [`jit_is_positive_fail`] for the rationale) and
    // therefore aren't exercised as in-process unit tests — a
    // JIT-frame abort tears down the whole test runner rather
    // than failing a single case. The Phase-1 and Phase-2 paths
    // in `param_helpers.rs` cover the violation messages via
    // `#[should_panic]`, which is the right tool for catching
    // the same logical failure when unwinding is available.

    #[test]
    fn classify_routes_new_param_helpers() {
        use crate::nodes::param_helpers::{InRangeU64, IsPositiveU64};
        // The classify_node entrypoint must see the new
        // predicate nodes and return the JIT-lowered op variants
        // rather than falling through to Fallback.
        let p = IsPositiveU64::new("rate");
        assert!(matches!(classify_node(&p), JitOp::IsPositiveCheck));

        let r = InRangeU64::new(1, 100);
        assert!(matches!(classify_node(&r), JitOp::InRangeCheck(1, 100)));
    }

    #[test]
    fn classify_leaves_other_param_helpers_on_fallback() {
        use crate::nodes::param_helpers::{
            MatchesStr, RequiredU64, ThisOrU64,
        };
        // By design: required/this_or/matches stay on Phase-2.
        // classify_node must pick Fallback so the closure-based
        // eval runs instead of an uninitialized JIT op.
        assert!(matches!(classify_node(&RequiredU64::new("x")), JitOp::Fallback));
        assert!(matches!(classify_node(&ThisOrU64::new()), JitOp::Fallback));
        assert!(matches!(classify_node(&MatchesStr::new(r"^\d+$")), JitOp::Fallback));
    }

    #[test]
    fn classify_routes_is_one_of_with_allow_list() {
        use crate::nodes::param_helpers::IsOneOfU64;
        let n = IsOneOfU64::new(vec![1, 3, 5, 7]);
        match classify_node(&n) {
            JitOp::IsOneOfCheck(allowed) => {
                assert_eq!(allowed, vec![1, 3, 5, 7]);
            }
            other => panic!("expected IsOneOfCheck, got {other:?}"),
        }
    }

    #[test]
    fn jit_is_one_of_check_passes_allowed_values() {
        let steps = vec![
            (JitOp::IsOneOfCheck(vec![1, 2, 3, 5, 8]), vec![0], vec![1]),
        ];
        let mut output_map = HashMap::new();
        output_map.insert("out".into(), 1);
        let mut kernel = compile_jit_raw(1, 2, steps, output_map, Vec::new()).unwrap();
        // Every allowed value passes straight through.
        for v in [1u64, 2, 3, 5, 8] {
            kernel.eval(&[v]);
            assert_eq!(kernel.get("out"), v);
        }
    }

    #[test]
    fn jit_is_one_of_check_accepts_single_element_allow_list() {
        // Degenerate case — one-value allow-list reduces to an
        // equality check with panic on mismatch.
        let steps = vec![
            (JitOp::IsOneOfCheck(vec![42]), vec![0], vec![1]),
        ];
        let mut output_map = HashMap::new();
        output_map.insert("out".into(), 1);
        let mut kernel = compile_jit_raw(1, 2, steps, output_map, Vec::new()).unwrap();
        kernel.eval(&[42]);
        assert_eq!(kernel.get("out"), 42);
    }

    // ── Catchable panic from JIT predicate fails ──────────────
    //
    // The extern fail helpers use `_longjmp` back to the Rust
    // wrapper, which then raises a Rust `panic!` carrying the
    // violation message. The panic originates in Rust land
    // (the JIT frame has already been jumped past), so its
    // unwind works through Rust-personality FDEs and
    // `std::panic::catch_unwind` catches it normally.

    fn extract_panic_msg(
        payload: Box<dyn std::any::Any + Send + 'static>,
    ) -> String {
        payload.downcast_ref::<String>()
            .cloned()
            .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
            .unwrap_or_else(|| "(non-string panic)".into())
    }

    #[test]
    fn jit_is_positive_violation_is_catchable() {
        let steps = vec![
            (JitOp::IsPositiveCheck, vec![0], vec![1]),
        ];
        let mut output_map = HashMap::new();
        output_map.insert("out".into(), 1);
        let mut kernel = compile_jit_raw(1, 2, steps, output_map, Vec::new()).unwrap();
        let err = std::panic::catch_unwind(
            std::panic::AssertUnwindSafe(|| kernel.eval(&[0])),
        ).expect_err("JIT violation should panic");
        assert!(extract_panic_msg(err).contains("must be > 0"));
    }

    #[test]
    fn jit_in_range_violation_is_catchable() {
        let steps = vec![
            (JitOp::InRangeCheck(10, 100), vec![0], vec![1]),
        ];
        let mut output_map = HashMap::new();
        output_map.insert("out".into(), 1);
        let mut kernel = compile_jit_raw(1, 2, steps, output_map, Vec::new()).unwrap();

        let err = std::panic::catch_unwind(
            std::panic::AssertUnwindSafe(|| kernel.eval(&[5])),
        ).expect_err("below-range should panic");
        assert!(extract_panic_msg(err).contains("outside [10, 100]"));

        let err = std::panic::catch_unwind(
            std::panic::AssertUnwindSafe(|| kernel.eval(&[500])),
        ).expect_err("above-range should panic");
        assert!(extract_panic_msg(err).contains("outside [10, 100]"));
    }

    #[test]
    fn jit_is_one_of_violation_is_catchable() {
        let steps = vec![
            (JitOp::IsOneOfCheck(vec![1, 3, 5]), vec![0], vec![1]),
        ];
        let mut output_map = HashMap::new();
        output_map.insert("out".into(), 1);
        let mut kernel = compile_jit_raw(1, 2, steps, output_map, Vec::new()).unwrap();
        let err = std::panic::catch_unwind(
            std::panic::AssertUnwindSafe(|| kernel.eval(&[2])),
        ).expect_err("disallowed value should panic");
        assert!(extract_panic_msg(err).contains("not in configured allow-list"));
    }

    #[test]
    fn invoke_with_catch_restores_slot_after_foreign_panic() {
        // A non-JIT panic from inside `f()` (simulating a bug
        // in a hybrid-closure step or any other non-longjmp
        // path that may run between setjmp and return) must
        // still leave the thread-local JIT_JMP_BUF slot in a
        // consistent state. The next `invoke_with_catch` that
        // actually calls into JIT code should see a clean
        // sentinel.
        let caught = std::panic::catch_unwind(|| {
            invoke_with_catch(|| panic!("foreign panic"));
        });
        assert!(caught.is_err(), "foreign panic should propagate out");

        // Subsequent legitimate JIT violation is still caught.
        let steps = vec![
            (JitOp::IsPositiveCheck, vec![0], vec![1]),
        ];
        let mut output_map = HashMap::new();
        output_map.insert("out".into(), 1);
        let mut kernel = compile_jit_raw(1, 2, steps, output_map, Vec::new()).unwrap();
        let err = std::panic::catch_unwind(
            std::panic::AssertUnwindSafe(|| kernel.eval(&[0])),
        ).expect_err("JIT violation should panic cleanly after foreign panic");
        assert!(extract_panic_msg(err).contains("must be > 0"));

        // And the happy path too — no stale pointer lingering.
        kernel.eval(&[42]);
        assert_eq!(kernel.get("out"), 42);
    }

    #[test]
    fn jit_kernel_survives_multiple_violations() {
        // After a caught violation the kernel remains usable —
        // the jmp_buf slot is correctly cleared and a
        // subsequent happy-path eval returns normally.
        let steps = vec![
            (JitOp::IsPositiveCheck, vec![0], vec![1]),
        ];
        let mut output_map = HashMap::new();
        output_map.insert("out".into(), 1);
        let mut kernel = compile_jit_raw(1, 2, steps, output_map, Vec::new()).unwrap();

        for _ in 0..3 {
            let _ = std::panic::catch_unwind(
                std::panic::AssertUnwindSafe(|| kernel.eval(&[0])),
            ).expect_err("violation should still panic");
        }
        // Happy path still works.
        kernel.eval(&[42]);
        assert_eq!(kernel.get("out"), 42);
    }
}
