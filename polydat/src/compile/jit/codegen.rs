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

use crate::ast::PolydatNode;

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
extern "C" fn jit_floor_base10(bits: u64) -> u64 {
    use crate::library::round_numbers::*;
    let x = f64::from_bits(bits);
    let r = if !positive_finite(x) { 0.0 } else { floor_pow10(x) };
    r.to_bits()
}
extern "C" fn jit_ceiling_base10(bits: u64) -> u64 {
    use crate::library::round_numbers::*;
    let x = f64::from_bits(bits);
    let r = if !positive_finite(x) { 0.0 } else { let lo = floor_pow10(x); if lo == x { lo } else { lo * 10.0 } };
    r.to_bits()
}
extern "C" fn jit_closest_base10(bits: u64) -> u64 {
    use crate::library::round_numbers::*;
    let x = f64::from_bits(bits);
    let r = if !positive_finite(x) { 0.0 } else { let lo = floor_pow10(x); let hi = if lo == x { lo } else { lo * 10.0 }; pick_closest(x, lo, hi) };
    r.to_bits()
}
extern "C" fn jit_floor_decade(bits: u64) -> u64 {
    use crate::library::round_numbers::*;
    let x = f64::from_bits(bits);
    let r = if !positive_finite(x) { 0.0 } else { let base = floor_pow10(x); (x / base).floor() * base };
    r.to_bits()
}
extern "C" fn jit_ceiling_decade(bits: u64) -> u64 {
    use crate::library::round_numbers::*;
    let x = f64::from_bits(bits);
    let r = if !positive_finite(x) { 0.0 } else { let base = floor_pow10(x); (x / base).ceil() * base };
    r.to_bits()
}
extern "C" fn jit_closest_decade(bits: u64) -> u64 {
    use crate::library::round_numbers::*;
    let x = f64::from_bits(bits);
    let r = if !positive_finite(x) { 0.0 } else { let base = floor_pow10(x); (x / base).round() * base };
    r.to_bits()
}
extern "C" fn jit_floor_binomial(bits: u64) -> u64 {
    use crate::library::round_numbers::*;
    let x = f64::from_bits(bits);
    let r = if !positive_finite(x) { 0.0 } else { floor_pow2(x) };
    r.to_bits()
}
extern "C" fn jit_ceiling_binomial(bits: u64) -> u64 {
    use crate::library::round_numbers::*;
    let x = f64::from_bits(bits);
    let r = if !positive_finite(x) { 0.0 } else { let lo = floor_pow2(x); if lo == x { lo } else { lo * 2.0 } };
    r.to_bits()
}
extern "C" fn jit_closest_binomial(bits: u64) -> u64 {
    use crate::library::round_numbers::*;
    let x = f64::from_bits(bits);
    let r = if !positive_finite(x) { 0.0 } else { let lo = floor_pow2(x); let hi = if lo == x { lo } else { lo * 2.0 }; pick_closest(x, lo, hi) };
    r.to_bits()
}
extern "C" fn jit_floor_fibonacci(bits: u64) -> u64 {
    use crate::library::round_numbers::*;
    let x = f64::from_bits(bits);
    let r = if !positive_finite(x) { 0.0 } else { floor_fibonacci_val(x) };
    r.to_bits()
}
extern "C" fn jit_ceiling_fibonacci(bits: u64) -> u64 {
    use crate::library::round_numbers::*;
    let x = f64::from_bits(bits);
    let r = if !positive_finite(x) { 0.0 } else { ceiling_fibonacci_val(x) };
    r.to_bits()
}
extern "C" fn jit_closest_fibonacci(bits: u64) -> u64 {
    use crate::library::round_numbers::*;
    let x = f64::from_bits(bits);
    let r = if !positive_finite(x) { 0.0 } else { pick_closest(x, floor_fibonacci_val(x), ceiling_fibonacci_val(x)) };
    r.to_bits()
}

extern "C" fn jit_atan2(y_bits: u64, x_bits: u64) -> u64 {
    f64::from_bits(y_bits).atan2(f64::from_bits(x_bits)).to_bits()
}
extern "C" fn jit_pow(base_bits: u64, exp_bits: u64) -> u64 {
    f64::from_bits(base_bits).powf(f64::from_bits(exp_bits)).to_bits()
}
extern "C" fn jit_round_nearest(x_bits: u64, iv_bits: u64) -> u64 {
    let x = f64::from_bits(x_bits);
    let interval = f64::from_bits(iv_bits);
    let r = if !(interval.is_finite() && interval > 0.0) { x } else { (x / interval).round() * interval };
    r.to_bits()
}
extern "C" fn jit_round_floor(x_bits: u64, iv_bits: u64) -> u64 {
    let x = f64::from_bits(x_bits);
    let interval = f64::from_bits(iv_bits);
    let r = if !(interval.is_finite() && interval > 0.0) { x } else { (x / interval).floor() * interval };
    r.to_bits()
}
extern "C" fn jit_round_ceiling(x_bits: u64, iv_bits: u64) -> u64 {
    let x = f64::from_bits(x_bits);
    let interval = f64::from_bits(iv_bits);
    let r = if !(interval.is_finite() && interval > 0.0) { x } else { (x / interval).ceil() * interval };
    r.to_bits()
}

extern "C" fn jit_pcg(input: u64, seed: u64, stream: u64) -> u64 {
    let inc = 2u64.wrapping_mul(stream).wrapping_add(1);
    crate::library::pcg::pcg_seek(seed, inc, input)
}
extern "C" fn jit_pcg_stream(input: u64, stream: u64, seed: u64) -> u64 {
    let inc = 2u64.wrapping_mul(stream).wrapping_add(1);
    crate::library::pcg::pcg_seek(seed, inc, input)
}
extern "C" fn jit_unfair_coin(input: u64, p_bits: u64) -> u64 {
    let p = f64::from_bits(p_bits);
    let h = xxhash_rust::xxh3::xxh3_64(&input.to_le_bytes());
    let unit = (h as f64) / ((u64::MAX as f64) + 1.0);
    if unit < p { 1 } else { 0 }
}
extern "C" fn jit_chance(input: u64, p_bits: u64) -> u64 {
    let p = f64::from_bits(p_bits);
    let h = xxhash_rust::xxh3::xxh3_64(&input.to_le_bytes());
    let unit = (h as f64) / ((u64::MAX as f64) + 1.0);
    let result: f64 = if unit < p { 1.0 } else { 0.0 };
    result.to_bits()
}
extern "C" fn jit_n_of(input: u64, n: u64, m: u64) -> u64 {
    if m == 0 { return 0; }
    let window = input / m;
    let pos = input % m;
    let my_hash = xxhash_rust::xxh3::xxh3_64(&[window.to_le_bytes(), pos.to_le_bytes()].concat());
    let mut rank: u64 = 0;
    for i in 0..m {
        if i == pos { continue; }
        let other_hash = xxhash_rust::xxh3::xxh3_64(&[window.to_le_bytes(), i.to_le_bytes()].concat());
        if other_hash < my_hash || (other_hash == my_hash && i < pos) {
            rank += 1;
        }
    }
    if rank < n { 1 } else { 0 }
}

extern "C" fn jit_cycle_walk(pos: u64, range: u64, seed: u64, inc: u64) -> u64 {
    let stream = inc.saturating_sub(1) / 2;
    let state = crate::library::pcg::build_cycle_walk_state(range, seed, stream);
    crate::library::pcg::cycle_walk_inner(pos, range, state.half_bits, state.half_mask, &state.round_keys)
}

extern "C" fn jit_perlin_1d(input: u64, perm_ptr: u64, freq_bits: u64) -> u64 {
    let perm = unsafe { &*(perm_ptr as *const crate::library::noise::PermTable) };
    let freq = f64::from_bits(freq_bits);
    let r = crate::library::noise::perlin_1d_algo(perm, input as f64 * freq);
    r.to_bits()
}

extern "C" fn jit_perlin_2d(x: u64, y: u64, perm_ptr: u64, freq_bits: u64) -> u64 {
    let perm = unsafe { &*(perm_ptr as *const crate::library::noise::PermTable) };
    let freq = f64::from_bits(freq_bits);
    let r = crate::library::noise::perlin_2d_algo(perm, x as f64 * freq, y as f64 * freq);
    r.to_bits()
}

extern "C" fn jit_simplex_2d(x: u64, y: u64, perm_ptr: u64, freq_bits: u64) -> u64 {
    let perm = unsafe { &*(perm_ptr as *const crate::library::noise::PermTable) };
    let freq = f64::from_bits(freq_bits);
    let r = crate::library::noise::simplex_2d_algo(perm, x as f64 * freq, y as f64 * freq);
    r.to_bits()
}

extern "C" fn jit_fractal_noise_1d(input: u64, perm_ptr: u64, freq_bits: u64, octaves: u64) -> u64 {
    let perm = unsafe { &*(perm_ptr as *const crate::library::noise::PermTable) };
    let freq = f64::from_bits(freq_bits);
    let r = crate::library::noise::fbm_1d(perm, input as f64, freq, octaves as u32);
    r.to_bits()
}

extern "C" fn jit_fractal_noise_2d(x: u64, y: u64, perm_ptr: u64, freq_bits: u64, octaves: u64) -> u64 {
    let perm = unsafe { &*(perm_ptr as *const crate::library::noise::PermTable) };
    let freq = f64::from_bits(freq_bits);
    let r = crate::library::noise::fbm_2d(perm, x as f64, y as f64, freq, octaves as u32);
    r.to_bits()
}

extern "C" fn jit_thread_id() -> u64 {
    let id = std::thread::current().id();
    let id_str = format!("{id:?}");
    let num = id_str.trim_start_matches("ThreadId(").trim_end_matches(')');
    num.parse().unwrap_or(0)
}

extern "C" fn jit_current_epoch_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
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

#[cfg(not(windows))]
unsafe extern "C" {
    fn _setjmp(env: *mut JitJmpBuf) -> i32;
    fn _longjmp(env: *mut JitJmpBuf, val: i32) -> !;
}

// MSVC CRT spelling of the same pair: it exports `longjmp`
// (no underscore — `_longjmp` doesn't exist there, LNK2019)
// and an x64 `_setjmp` whose second register argument is
// recorded as the jmp_buf's `Frame` field. The C compiler
// normally fills that in via intrinsic; calling from Rust we
// pass NULL explicitly, which is load-bearing twice over: it
// keeps rdx from carrying garbage into the buffer, and a zero
// `Frame` makes `longjmp` do a plain register restore instead
// of an `RtlUnwindEx` unwind — mandatory here because the
// frames being skipped are JIT code with no unwind tables
// registered (the exact problem this setjmp path exists to
// avoid; see the module comment above).
#[cfg(windows)]
unsafe extern "C" {
    fn _setjmp(env: *mut JitJmpBuf, frame: *mut std::ffi::c_void) -> i32;
    #[link_name = "longjmp"]
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
    #[cfg(not(windows))]
    let jmpval = unsafe { _setjmp(buf_ptr) };
    // NULL frame → non-unwinding longjmp; see the extern block.
    #[cfg(windows)]
    let jmpval = unsafe { _setjmp(buf_ptr, std::ptr::null_mut()) };
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
extern "C" fn jit_is_positive_fail(value: u64, name_ptr: u64, name_len: u64) -> u64 {
    // The pointer targets the `name` const in the node's NodeMeta;
    // the node is kept alive for the life of the compiled code by
    // `JitCore::_nodes` / the cone node's `_members`, so the str
    // data is stable. (ptr, len) == (0, 0) means the default name.
    let name = if name_ptr != 0 {
        unsafe {
            std::str::from_utf8_unchecked(std::slice::from_raw_parts(
                name_ptr as *const u8,
                name_len as usize,
            ))
        }
    } else {
        "value"
    };
    jit_violation_longjmp(
        format!("is_positive({name}): value must be > 0, got {value}"),
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
/// an `is_one_of` violation message, carrying the allow-list
/// contents so the message matches the interpreter's byte for
/// byte. The pointer targets the node's meta VecU64 const; the
/// node is kept alive for the life of the compiled code by
/// `JitCore::_nodes` / the cone node's members, so the data is
/// stable. (ptr, len) == (0, 0) degrades to an elided set.
extern "C" fn jit_is_one_of_fail(value: u64, set_ptr: u64, set_len: u64) -> u64 {
    let msg = if set_ptr != 0 {
        let set = unsafe {
            std::slice::from_raw_parts(set_ptr as *const u64, set_len as usize)
        };
        format!("is_one_of: value {value} not in allowed set {set:?}")
    } else {
        format!("is_one_of: value {value} not in allowed set [..]")
    };
    jit_violation_longjmp(msg);
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

// ── Non-scalar String & Byte extern helpers (SRD 111) ──────────

extern "C" fn jit_u64_to_str(val: u64) -> u64 {
    let s = val.to_string();
    crate::kernel::put_thread_str(&s)
}

extern "C" fn jit_i64_to_str(val: i64) -> u64 {
    let s = val.to_string();
    crate::kernel::put_thread_str(&s)
}

extern "C" fn jit_f64_to_str(val_bits: u64) -> u64 {
    let val = f64::from_bits(val_bits);
    let s = val.to_string();
    crate::kernel::put_thread_str(&s)
}

extern "C" fn jit_bool_to_str(val: u64) -> u64 {
    let s = if val != 0 { "true" } else { "false" };
    crate::kernel::put_thread_str(s)
}

extern "C" fn jit_str_to_u64(handle: u64) -> u64 {
    let s = crate::kernel::resolve_thread_str(handle);
    s.trim().parse::<u64>().unwrap_or(0)
}

extern "C" fn jit_str_to_i64(handle: u64) -> i64 {
    let s = crate::kernel::resolve_thread_str(handle);
    s.trim().parse::<i64>().unwrap_or(0)
}

extern "C" fn jit_str_to_f64(handle: u64) -> u64 {
    let s = crate::kernel::resolve_thread_str(handle);
    let f = s.trim().parse::<f64>().unwrap_or(0.0);
    f.to_bits()
}

extern "C" fn jit_str_to_bool(handle: u64) -> u64 {
    let s = crate::kernel::resolve_thread_str(handle);
    match s.trim().to_lowercase().as_str() {
        "true" | "1" | "t" | "yes" | "y" => 1,
        _ => 0,
    }
}

extern "C" fn jit_str_concat(h1: u64, h2: u64) -> u64 {
    let s1 = crate::kernel::resolve_thread_str(h1);
    let s2 = crate::kernel::resolve_thread_str(h2);
    let combined = format!("{s1}{s2}");
    crate::kernel::put_thread_str(&combined)
}

extern "C" fn jit_str_lower(h: u64) -> u64 {
    let s = crate::kernel::resolve_thread_str(h);
    let lower = s.to_lowercase();
    crate::kernel::put_thread_str(&lower)
}

extern "C" fn jit_str_upper(h: u64) -> u64 {
    let s = crate::kernel::resolve_thread_str(h);
    let upper = s.to_uppercase();
    crate::kernel::put_thread_str(&upper)
}

extern "C" fn jit_str_trim(h: u64) -> u64 {
    let s = crate::kernel::resolve_thread_str(h);
    let trimmed = s.trim();
    crate::kernel::put_thread_str(trimmed)
}

extern "C" fn jit_str_len(h: u64) -> u64 {
    let s = crate::kernel::resolve_thread_str(h);
    s.len() as u64
}

// ── JitOp ──────────────────────────────────────────────────

/// Description of a JIT step — what operation to generate.
///
/// For f64 operations, values are stored in the u64 buffer as their
/// bit representation. Cranelift `bitcast` converts between i64/f64.
#[derive(Debug, Clone, PartialEq)]
pub enum JitOp {
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
    /// output[0] = splitmix64(input[0]) (fully inlined 64-bit ALU bit mixer)
    SplitMix64,
    /// output[0] = popcount(input[0])
    Popcnt,
    /// output[0] = leading_zeros(input[0])
    Clz,
    /// output[0] = trailing_zeros(input[0])
    Ctz,
    /// output[0] = byte_swap(input[0])
    Bswap,
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
    /// if input[0] == 0, call `jit_is_positive_fail` (panics)
    /// with the configured predicate name — (ptr, len) into the
    /// node's meta const, (0, 0) for the default. Message parity
    /// with the interpreter's `is_positive({name}): …` is asserted
    /// by the SRD-105 battery.
    IsPositiveCheck { name_ptr: u64, name_len: u64 },
    /// Parameter predicate: pass input[0] through to output[0];
    /// if input[0] < lo or input[0] > hi, call
    /// `jit_in_range_fail` (panics). Stored as (lo, hi).
    InRangeCheck(u64, u64),
    /// Parameter predicate: pass input[0] through to output[0];
    /// if input[0] is not in the allow-list, call
    /// `jit_is_one_of_fail` (panics) with the allow-list contents
    /// — (ptr, len) into the node's meta VecU64 const, (0, 0)
    /// when unavailable. Message parity with the interpreter's
    /// `is_one_of: … not in allowed set […]` is asserted by the
    /// SRD-105 battery. Inline comparisons use the baked vector.
    IsOneOfCheck {
        allowed: Vec<u64>,
        set_ptr: u64,
        set_len: u64,
    },

    // --- Register-plane ops (§8.4 layer 2: native SIMD) ---
    // A register value occupies two consecutive u64 slots; the
    // codegen emits one unaligned 128-bit load/store per value
    // (buffer is only 8-aligned) and a single vector instruction.
    /// Element-wise register binop. (lane_ty index, arith index)
    /// — lanes: 0=i8x16 1=i16x8 2=i32x4 3=i64x2 4=f32x4 5=f64x2;
    /// arith: 0=add 1=sub 2=mul.
    RegBinOp(u8, u8),
    /// View retag / two-slot copy (`__reg_view_*`): one 128-bit
    /// load + store; the lane typing is static, so no instruction
    /// beyond the move.
    RegCopy,
    /// Broadcast a scalar wire into all lanes. Same lane index
    /// vocabulary as `RegBinOp`; float lanes read the f64 slot
    /// and demote as needed, integer lanes reduce from u64.
    RegSplat(u8),

    // --- Comparisons & selections (SRD 110) ---
    /// Integer comparison: output[0] = if a <cond> b { 1 } else { 0 }
    U64Cmp(ir::condcodes::IntCC),
    /// Float comparison: output[0] = if a <cond> b { 1 } else { 0 }
    F64Cmp(ir::condcodes::FloatCC),
    /// Conditional select for u64: output[0] = if cond != 0 { a } else { b }
    SelectU64,
    /// Conditional select for f64: output[0] = if cond != 0 { a } else { b }
    SelectF64,

    // --- Type conversions & lattice adapters (SRD 110) ---
    /// Signed integer to float: output[0] = (input[0] as i64 as f64).to_bits()
    I64ToF64,
    /// Float to signed integer: output[0] = (f64::from_bits(input[0]) as i64) as u64
    F64ToI64,
    /// Sign-extend 32-bit integer: output[0] = ((input[0] as i32) as i64) as u64
    SignExtendI32,
    /// Sign-extend 16-bit integer: output[0] = ((input[0] as i16) as i64) as u64
    SignExtendI16,
    /// Sign-extend 8-bit integer: output[0] = ((input[0] as i8) as i64) as u64
    SignExtendI8,
    /// Zero-extend 32-bit integer: output[0] = (input[0] as u32) as u64
    ZeroExtendU32,
    /// Zero-extend 16-bit integer: output[0] = (input[0] as u16) as u64
    ZeroExtendU16,
    /// Zero-extend 8-bit integer: output[0] = (input[0] as u8) as u64
    ZeroExtendU8,
    /// Truthiness boolean coercion: output[0] = if input[0] != 0 { 1 } else { 0 }
    ToBool,
    /// Constant u64: output[0] = val
    ConstU64(u64),
    /// Constant f64: output[0] = val_bits
    ConstF64(u64),

    // --- Interpolation & Hashing (SRD 110) ---
    /// Hash range: output[0] = if max == 0 { 0 } else { hash(input[0]) % max }
    HashRangeConst(u64),
    /// Hash interval: output[0] = min + (hash(input[0]) / MAX) * (max - min)
    HashIntervalConst(u64, u64),
    /// Inverse lerp: output[0] = ((input[0] - a) / (b - a)).clamp(0, 1)
    InvLerpConst(u64, u64),
    /// Remap: output[0] = out_min + ((input[0] - in_min) / (in_max - in_min)) * (out_max - out_min)
    RemapConst(u64, u64, u64, u64),

    // --- Context & Datetime (SRD 110) ---
    /// Epoch offset: output[0] = input[0].wrapping_add(base)
    EpochOffsetConst(u64),
    /// Epoch scale: output[0] = input[0].wrapping_mul(factor)
    EpochScaleConst(u64),
    /// OS thread ID
    ThreadId,
    /// Wall clock millis
    CurrentEpochMillis,

    // --- Coherent Noise (SRD 110) ---
    Perlin1dConst(u64, u64),
    Perlin2dConst(u64, u64),
    Simplex2dConst(u64, u64),
    FractalNoise1dConst(u64, u64, u64),
    FractalNoise2dConst(u64, u64, u64),

    // --- Variadics & wire arithmetic (SRD 110) ---
    /// Variadic sum across all inputs
    VariadicSum,
    /// Variadic product across all inputs
    VariadicProduct,
    /// Variadic minimum across all inputs (unsigned)
    VariadicMin,
    /// Variadic maximum across all inputs (unsigned)
    VariadicMax,
    /// Checked unsigned addition: output[0] = a.checked_add(b).unwrap_or(0)
    CheckedAdd,
    /// Saturating unsigned subtraction: output[0] = a.saturating_sub(b)
    CheckedSub,
    /// Checked unsigned multiplication: output[0] = a.checked_mul(b).unwrap_or(0)
    CheckedMul,
    /// Smallest multiple of multiple >= value: output[0] = if m == 0 { v } else { ((v + m - 1) / m) * m }
    CeilToMultiple,
    /// Multiples at least: output[0] = if m == 0 { 0 } else { (v + m - 1) / m }
    MultiplesAtLeast,

    // --- Probability & permutations (SRD 110) ---
    /// Fair coin flip: output[0] = input[0] & 1
    FairCoin,
    /// Float blend with constant mix: output[0] = (fa * (1 - mix) + fb * mix).round() as u64
    BlendConst(u64),
    /// LFSR advance step: output[0] = (input[0] >> 1) ^ (if input[0] & 1 != 0 { feedback } else { 0 })
    LfsrStep,
    /// PCG random with constant seed and stream: (seed, stream)
    PcgConst(u64, u64),
    /// PCG random with wire stream and constant seed: (seed)
    PcgStreamConst(u64),
    /// Cycle walk: (range, seed, inc)
    CycleWalkConst(u64, u64, u64),
    /// Unfair coin with constant probability: (p_bits)
    UnfairCoinConst(u64),
    /// Chance with constant probability: (p_bits)
    ChanceConst(u64),
    /// N-of-M selection with constant n and m: (n, m)
    NOfConst(u64, u64),

    // --- String & Non-scalar ops (SRD 111) ---
    /// output[0] = jit_u64_to_str(input[0])
    U64ToString,
    /// output[0] = jit_i64_to_str(input[0])
    I64ToString,
    /// output[0] = jit_f64_to_str(input[0])
    F64ToString,
    /// output[0] = jit_bool_to_str(input[0])
    BoolToString,
    /// output[0] = jit_str_to_u64(input[0])
    StringToU64,
    /// output[0] = jit_str_to_i64(input[0])
    StringToI64,
    /// output[0] = jit_str_to_f64(input[0])
    StringToF64,
    /// output[0] = jit_str_to_bool(input[0])
    StringToBool,
    /// output[0] = jit_str_concat(input[0], input[1])
    StrConcat,
    /// output[0] = jit_str_lower(input[0])
    StrLower,
    /// output[0] = jit_str_upper(input[0])
    StrUpper,
    /// output[0] = jit_str_trim(input[0])
    StrTrim,
    /// output[0] = jit_str_len(input[0])
    StrLen,

    /// Fallback: call the Phase 2 closure
    Fallback,
}

// ── Node classification ────────────────────────────────────

/// Classify a Polydat node into a JIT-able operation.
///
/// Uses `jit_constants()` to extract assembly-time constants
/// directly from the node — no probing hacks needed.
pub fn classify_node(node: &dyn PolydatNode) -> JitOp {
    let name = node.meta().name.as_str();
    let consts = node.jit_constants();

    match name {
        "identity" => JitOp::Identity,
        "hash" | "splitmix64" | "scatter" => JitOp::SplitMix64,
        "fair_coin" => JitOp::FairCoin,
        "unfair_coin" | "bernoulli" => {
            if let Some(&p) = consts.first() {
                JitOp::UnfairCoinConst(p)
            } else {
                JitOp::Fallback
            }
        }
        "chance" => {
            if let Some(&p) = consts.first() {
                JitOp::ChanceConst(p)
            } else {
                JitOp::Fallback
            }
        }
        "popcnt" | "count_ones" | "popcount" => JitOp::Popcnt,
        "clz" | "leading_zeros" => JitOp::Clz,
        "ctz" | "trailing_zeros" => JitOp::Ctz,
        "bswap" | "swap_bytes" => JitOp::Bswap,
        "xxhash3" | "xxh3" => JitOp::Hash,
        "hash_range" => {
            if let Some(&c) = consts.first() {
                JitOp::HashRangeConst(c)
            } else {
                JitOp::Fallback
            }
        }
        "hash_interval" => {
            if consts.len() >= 2 {
                JitOp::HashIntervalConst(consts[0], consts[1])
            } else {
                JitOp::Fallback
            }
        }
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
        "lut_sample" | "dist_normal" | "icd_normal" | "dist_exponential" | "icd_exponential"
        | "dist_uniform" | "dist_pareto" | "dist_zipf" | "dist_empirical" => {
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
        "floor_base10" => JitOp::MathUnary(10),
        "ceiling_base10" => JitOp::MathUnary(11),
        "closest_base10" => JitOp::MathUnary(12),
        "floor_decade" => JitOp::MathUnary(13),
        "ceiling_decade" => JitOp::MathUnary(14),
        "closest_decade" => JitOp::MathUnary(15),
        "floor_binomial" => JitOp::MathUnary(16),
        "ceiling_binomial" => JitOp::MathUnary(17),
        "closest_binomial" => JitOp::MathUnary(18),
        "floor_fibonacci" => JitOp::MathUnary(19),
        "ceiling_fibonacci" => JitOp::MathUnary(20),
        "closest_fibonacci" => JitOp::MathUnary(21),
        "atan2" => JitOp::MathBinary(0),
        "pow" => JitOp::MathBinary(1),
        "round_nearest" => JitOp::MathBinary(2),
        "round_floor" => JitOp::MathBinary(3),
        "round_ceiling" => JitOp::MathBinary(4),
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

        // ── Register plane (§8.4 layer 2) ──────────────────────
        "reg_add_i8" => JitOp::RegBinOp(0, 0),
        "reg_sub_i8" => JitOp::RegBinOp(0, 1),
        // `imul.i8x16` has no cranelift lowering (x86 has no
        // byte-lane multiply short of AVX-512; cranelift 0.116
        // rejects it in ISLE) — the closure path handles i8
        // multiplies.
        "reg_mul_i8" => JitOp::Fallback,
        "reg_add_i16" => JitOp::RegBinOp(1, 0),
        "reg_sub_i16" => JitOp::RegBinOp(1, 1),
        "reg_mul_i16" => JitOp::RegBinOp(1, 2),
        "reg_add_i32" => JitOp::RegBinOp(2, 0),
        "reg_sub_i32" => JitOp::RegBinOp(2, 1),
        "reg_mul_i32" => JitOp::RegBinOp(2, 2),
        "reg_add_i64" => JitOp::RegBinOp(3, 0),
        "reg_sub_i64" => JitOp::RegBinOp(3, 1),
        "reg_mul_i64" => JitOp::RegBinOp(3, 2),
        "reg_add_f32" => JitOp::RegBinOp(4, 0),
        "reg_sub_f32" => JitOp::RegBinOp(4, 1),
        "reg_mul_f32" => JitOp::RegBinOp(4, 2),
        "reg_add_f64" => JitOp::RegBinOp(5, 0),
        "reg_sub_f64" => JitOp::RegBinOp(5, 1),
        "reg_mul_f64" => JitOp::RegBinOp(5, 2),
        "__reg_view_raw" | "__reg_view_i8x16" | "__reg_view_i16x8"
        | "__reg_view_i32x4" | "__reg_view_i64x2" | "__reg_view_f16x8"
        | "__reg_view_f32x4" | "__reg_view_f64x2" => JitOp::RegCopy,
        "reg_splat_i8" => JitOp::RegSplat(0),
        "reg_splat_i16" => JitOp::RegSplat(1),
        "reg_splat_i32" => JitOp::RegSplat(2),
        "reg_splat_i64" => JitOp::RegSplat(3),
        "reg_splat_f32" => JitOp::RegSplat(4),
        "reg_splat_f64" => JitOp::RegSplat(5),

        "f64_add" => JitOp::F64Add,
        "f64_sub" => JitOp::F64Sub,
        "f64_mul" => JitOp::F64Mul,
        "f64_div" => JitOp::F64Div,
        "f64_mod" => JitOp::F64Mod,

        // ── Comparisons & Selections (SRD 110) ───────────────────
        "u64_eq" => JitOp::U64Cmp(ir::condcodes::IntCC::Equal),
        "u64_ne" => JitOp::U64Cmp(ir::condcodes::IntCC::NotEqual),
        "u64_lt" => JitOp::U64Cmp(ir::condcodes::IntCC::UnsignedLessThan),
        "u64_le" => JitOp::U64Cmp(ir::condcodes::IntCC::UnsignedLessThanOrEqual),
        "u64_gt" => JitOp::U64Cmp(ir::condcodes::IntCC::UnsignedGreaterThan),
        "u64_ge" => JitOp::U64Cmp(ir::condcodes::IntCC::UnsignedGreaterThanOrEqual),
        "f64_eq" => JitOp::F64Cmp(ir::condcodes::FloatCC::Equal),
        "f64_ne" => JitOp::F64Cmp(ir::condcodes::FloatCC::NotEqual),
        "f64_lt" => JitOp::F64Cmp(ir::condcodes::FloatCC::LessThan),
        "f64_le" => JitOp::F64Cmp(ir::condcodes::FloatCC::LessThanOrEqual),
        "f64_gt" => JitOp::F64Cmp(ir::condcodes::FloatCC::GreaterThan),
        "f64_ge" => JitOp::F64Cmp(ir::condcodes::FloatCC::GreaterThanOrEqual),
        "select_u64" | "select" => JitOp::SelectU64,
        "select_f64" => JitOp::SelectF64,

        // ── Wire Arithmetic & Multiples (SRD 110) ────────────────
        "div_wire" => JitOp::U64Div2,
        "mod_wire" => JitOp::U64Mod2,
        "ceil_to_multiple" => JitOp::CeilToMultiple,
        "multiples_at_least" => JitOp::MultiplesAtLeast,
        "checked_add" => JitOp::CheckedAdd,
        "checked_sub" => JitOp::CheckedSub,
        "checked_mul" => JitOp::CheckedMul,

        // ── Variadics (SRD 110) ──────────────────────────────────
        "sum" => JitOp::VariadicSum,
        "product" => JitOp::VariadicProduct,
        "min" => JitOp::VariadicMin,
        "max" => JitOp::VariadicMax,

        // ── PRNG & Probability (SRD 110) ─────────────────────────
        "blend" => {
            if let Some(&c) = consts.first() {
                JitOp::BlendConst(c)
            } else {
                JitOp::Fallback
            }
        }
        "lfsr_step" => JitOp::LfsrStep,
        "pcg" => {
            if consts.len() >= 2 {
                JitOp::PcgConst(consts[0], consts[1])
            } else {
                JitOp::Fallback
            }
        }
        "pcg_stream" => {
            if let Some(&seed) = consts.first() {
                JitOp::PcgStreamConst(seed)
            } else {
                JitOp::Fallback
            }
        }
        "n_of" => {
            if consts.len() >= 2 {
                JitOp::NOfConst(consts[0], consts[1])
            } else {
                JitOp::Fallback
            }
        }

        "cycle_walk" => {
            if consts.len() >= 3 {
                JitOp::CycleWalkConst(consts[0], consts[1], consts[2])
            } else {
                JitOp::Fallback
            }
        }
        "coin_flip" => {
            if let Some(&p) = consts.first() {
                JitOp::UnfairCoinConst(p)
            } else {
                JitOp::FairCoin
            }
        }
        "default_or" => JitOp::SelectU64,
        "const_u64" | "const_bool" | "session_start_millis" => {
            if let Some(&c) = consts.first() {
                JitOp::ConstU64(c)
            } else {
                JitOp::Fallback
            }
        }
        "const_f64" => {
            if let Some(&c) = consts.first() {
                JitOp::ConstF64(c)
            } else {
                JitOp::Fallback
            }
        }
        "inv_lerp" => {
            if consts.len() >= 2 {
                JitOp::InvLerpConst(consts[0], consts[1])
            } else {
                JitOp::Fallback
            }
        }
        "remap" => {
            if consts.len() >= 4 {
                JitOp::RemapConst(consts[0], consts[1], consts[2], consts[3])
            } else {
                JitOp::Fallback
            }
        }
        "epoch_offset" => {
            if let Some(&c) = consts.first() {
                JitOp::EpochOffsetConst(c)
            } else {
                JitOp::Fallback
            }
        }
        "epoch_scale" => {
            if let Some(&c) = consts.first() {
                JitOp::EpochScaleConst(c)
            } else {
                JitOp::Fallback
            }
        }
        "thread_id" => JitOp::ThreadId,
        "current_epoch_millis" => JitOp::CurrentEpochMillis,
        "perlin_1d" => {
            if consts.len() >= 2 {
                JitOp::Perlin1dConst(consts[0], consts[1])
            } else {
                JitOp::Fallback
            }
        }
        "perlin_2d" => {
            if consts.len() >= 2 {
                JitOp::Perlin2dConst(consts[0], consts[1])
            } else {
                JitOp::Fallback
            }
        }
        "simplex_2d" => {
            if consts.len() >= 2 {
                JitOp::Simplex2dConst(consts[0], consts[1])
            } else {
                JitOp::Fallback
            }
        }
        "fractal_noise_1d" => {
            if consts.len() >= 3 {
                JitOp::FractalNoise1dConst(consts[0], consts[1], consts[2])
            } else {
                JitOp::Fallback
            }
        }
        "fractal_noise_2d" => {
            if consts.len() >= 3 {
                JitOp::FractalNoise2dConst(consts[0], consts[1], consts[2])
            } else {
                JitOp::Fallback
            }
        }

        // ── Type Conversion Lattice (SRD 110) ────────────────────
        "u64_to_f64" | "__u64_to_f64" | "u32_to_f64" | "__u32_to_f64"
        | "bool_to_f64" | "__bool_to_f64" | "bool_to_f32" | "__bool_to_f32"
        | "__f32_to_f64" | "f32_to_f64"
        | "__u64_to_f32" | "u64_to_f32" | "__u32_to_f32" | "u32_to_f32"
        | "__u16_to_f32" | "u16_to_f32" | "__u8_to_f32" | "u8_to_f32"
        | "__u16_to_f64" | "u16_to_f64" | "__u8_to_f64" | "u8_to_f64"
        | "__u128_to_f64" | "__u128_to_f32" | "__u128_to_f16" => JitOp::ToF64,

        "i64_to_f64" | "__i64_to_f64" | "i32_to_f64" | "__i32_to_f64"
        | "__i64_to_f32" | "i64_to_f32" | "__i32_to_f32" | "i32_to_f32"
        | "__i16_to_f32" | "i16_to_f32" | "__i8_to_f32" | "i8_to_f32"
        | "__i16_to_f64" | "i16_to_f64" | "__i8_to_f64" | "i8_to_f64"
        | "__i128_to_f64" | "__i128_to_f32" | "__i128_to_f16" => JitOp::I64ToF64,

        "__f64_to_u64" | "__f64_to_u64_checked"
        | "f64_to_u32" | "__f64_to_u32" | "f32_to_u64" | "__f32_to_u64"
        | "f32_to_u32" | "__f32_to_u32"
        | "__f64_to_u16" | "f64_to_u16" | "__f64_to_u8" | "f64_to_u8"
        | "__f32_to_u16" | "f32_to_u16" | "__f32_to_u8" | "f32_to_u8"
        | "__f16_to_u64" | "__f16_to_u32" | "__f16_to_u16" | "__f16_to_u8"
        | "__f64_to_u128" | "__f32_to_u128" | "__f16_to_u128"
        | "round_u64" | "trunc_u64" => JitOp::F64ToU64,

        "f64_to_i64" | "__f64_to_i64" | "f64_to_i32" | "__f64_to_i32"
        | "f32_to_i64" | "__f32_to_i64" | "f32_to_i32" | "__f32_to_i32"
        | "__f64_to_i16" | "f64_to_i16" | "__f64_to_i8" | "f64_to_i8"
        | "__f32_to_i16" | "f32_to_i16" | "__f32_to_i8" | "f32_to_i8"
        | "__f16_to_i64" | "__f16_to_i32" | "__f16_to_i16" | "__f16_to_i8"
        | "__f64_to_i128" | "__f32_to_i128" | "__f16_to_i128" => JitOp::F64ToI64,

        "f64_to_f32" | "__f64_to_f32"
        | "__f16_to_f32" | "__f16_to_f64" | "__f32_to_f16" | "__f64_to_f16" => JitOp::Identity,

        "u32_to_u64" | "__u32_to_u64" | "u64_to_u32" | "__u64_to_u32"
        | "u32_to_i32" | "__u32_to_i32" | "i32_to_u32" | "__i32_to_u32"
        | "u64_to_i64" | "__u64_to_i64" | "i64_to_u64" | "__i64_to_u64"
        | "bool_to_u64" | "__bool_to_u64" | "bool_to_i64" | "__bool_to_i64"
        | "bool_to_u32" | "__bool_to_u32" | "bool_to_i32" | "__bool_to_i32"
        | "__u64_to_u16" | "__u64_to_u8" | "__u64_to_i16" | "__u64_to_i8"
        | "__i64_to_u32" | "__i64_to_u16" | "__i64_to_u8" | "__i64_to_i16" | "__i64_to_i8"
        | "__u32_to_u16" | "__u32_to_u8" | "__u32_to_i16" | "__u32_to_i8"
        | "__i32_to_u16" | "__i32_to_u8" | "__i32_to_i16" | "__i32_to_i8"
        | "__u16_to_u8" | "__u16_to_i8" | "__i16_to_u8" | "__i16_to_i8"
        | "__u128_to_u64" | "__u128_to_i64" | "__i128_to_u64" | "__i128_to_i64"
        | "__u128_to_u32" | "__u128_to_u16" | "__u128_to_u8" | "__u128_to_i32" | "__u128_to_i16" | "__u128_to_i8"
        | "__i128_to_u32" | "__i128_to_u16" | "__i128_to_u8" | "__i128_to_i32" | "__i128_to_i16" | "__i128_to_i8"
        | "__u64_to_u128" | "__u64_to_i128" | "__i64_to_u128" | "__i64_to_i128" | "__u128_to_i128" | "__i128_to_u128"
        | "__bool_to_u16" | "__bool_to_u8" | "__bool_to_i16" | "__bool_to_i8" | "__bool_to_u128" | "__bool_to_i128"
        | "__u8_to_f16" | "__u16_to_f16" | "__i8_to_f16" | "__i16_to_f16" | "__u64_to_f16" | "__i64_to_f16" | "__u32_to_f16" | "__i32_to_f16" | "__bool_to_f16" => JitOp::Identity,

        "i32_to_i64" | "__i32_to_i64" | "i32_to_u64" | "__i32_to_u64"
        | "__u32_to_i64" | "u32_to_i64" | "__u32_to_u128" | "__u32_to_i128" | "__i32_to_u128" | "__i32_to_i128" => JitOp::SignExtendI32,

        "__i16_to_i32" | "i16_to_i32" | "__i16_to_i64" | "i16_to_i64"
        | "__i16_to_u32" | "i16_to_u32" | "__i16_to_u64" | "i16_to_u64"
        | "__i16_to_u128" | "__i16_to_i128" => JitOp::SignExtendI16,

        "__i8_to_i16" | "i8_to_i16" | "__i8_to_i32" | "i8_to_i32"
        | "__i8_to_i64" | "i8_to_i64" | "__i8_to_u16" | "i8_to_u16"
        | "__i8_to_u32" | "i8_to_u32" | "__i8_to_u64" | "i8_to_u64"
        | "__i8_to_u128" | "__i8_to_i128" => JitOp::SignExtendI8,

        "__u16_to_u32" | "u16_to_u32" | "__u16_to_u64" | "u16_to_u64"
        | "__u16_to_i32" | "u16_to_i32" | "__u16_to_i64" | "u16_to_i64"
        | "__u16_to_u128" | "__u16_to_i128" | "__u16_to_i16" | "u16_to_i16"
        | "__i16_to_u16" | "i16_to_u16" => JitOp::ZeroExtendU16,

        "__u8_to_u16" | "u8_to_u16" | "__u8_to_u32" | "u8_to_u32"
        | "__u8_to_u64" | "u8_to_u64" | "__u8_to_i16" | "u8_to_i16"
        | "__u8_to_i32" | "u8_to_i32" | "__u8_to_i64" | "u8_to_i64"
        | "__u8_to_u128" | "__u8_to_i128" | "__u8_to_i8" | "u8_to_i8"
        | "__i8_to_u8" | "i8_to_u8" => JitOp::ZeroExtendU8,

        "u64_to_i32" | "__u64_to_i32" | "i64_to_i32" | "__i64_to_i32" => JitOp::ZeroExtendU32,

        "u64_to_bool" | "__u64_to_bool" | "i64_to_bool" | "__i64_to_bool"
        | "u32_to_bool" | "__u32_to_bool" | "i32_to_bool" | "__i32_to_bool"
        | "f64_to_bool" | "__f64_to_bool" | "f32_to_bool" | "__f32_to_bool"
        | "__f16_to_bool" | "f16_to_bool" | "__u8_to_bool" | "__u16_to_bool"
        | "__i8_to_bool" | "__i16_to_bool" | "__u128_to_bool" | "__i128_to_bool" => JitOp::ToBool,

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
        "is_positive" => {
            let name = node.meta().ins.iter().find_map(|slot| match slot {
                crate::ast::Slot::Const { name, value: crate::ast::ConstValue::Str(v) }
                    if name == "name" => Some(v),
                _ => None,
            });
            match name {
                Some(v) => JitOp::IsPositiveCheck {
                    name_ptr: v.as_ptr() as u64,
                    name_len: v.len() as u64,
                },
                None => JitOp::IsPositiveCheck { name_ptr: 0, name_len: 0 },
            }
        }
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
                let set = node.meta().ins.iter().find_map(|slot| match slot {
                    crate::ast::Slot::Const {
                        name,
                        value: crate::ast::ConstValue::VecU64(v),
                    } if name == "allowed" => Some(v),
                    _ => None,
                });
                let (set_ptr, set_len) = match set {
                    Some(v) => (v.as_ptr() as u64, v.len() as u64),
                    None => (0, 0),
                };
                JitOp::IsOneOfCheck { allowed: consts, set_ptr, set_len }
            }
        }
        // String & Non-scalar conversions (SRD 111)
        "__u64_to_string" | "__u64_to_str" | "u64_to_str" | "u64_to_string"
        | "__u32_to_string" | "__u32_to_str" | "u32_to_str"
        | "__u16_to_string" | "__u16_to_str"
        | "__u8_to_string" | "__u8_to_str"
        | "format_u64" => JitOp::U64ToString,

        "__i64_to_string" | "__i64_to_str" | "i64_to_str" | "i64_to_string"
        | "__i32_to_string" | "__i32_to_str" | "i32_to_str"
        | "__i16_to_string" | "__i16_to_str"
        | "__i8_to_string" | "__i8_to_str" => JitOp::I64ToString,

        "__f64_to_string" | "__f64_to_str" | "f64_to_str" | "f64_to_string"
        | "__f32_to_string" | "__f32_to_str" | "f32_to_str" => JitOp::F64ToString,

        "__bool_to_string" | "__bool_to_str" | "bool_to_str" => JitOp::BoolToString,

        "__str_to_u64" | "__string_to_u64" | "str_to_u64" | "parse_u64"
        | "__str_to_u32" | "__string_to_u32" | "str_to_u32"
        | "__str_to_u16" | "__str_to_u8" => JitOp::StringToU64,

        "__str_to_i64" | "__string_to_i64" | "str_to_i64" | "parse_i64"
        | "__str_to_i32" | "__string_to_i32" | "str_to_i32"
        | "__str_to_i16" | "__str_to_i8" => JitOp::StringToI64,

        "__str_to_f64" | "__string_to_f64" | "str_to_f64" | "parse_f64"
        | "__str_to_f32" | "__string_to_f32" | "str_to_f32" => JitOp::StringToF64,

        "__str_to_bool" | "__string_to_bool" | "str_to_bool" | "parse_bool" => JitOp::StringToBool,

        "str_concat" | "concat" => JitOp::StrConcat,
        "str_lower" | "to_lower" | "lower" | "lowercase" => JitOp::StrLower,
        "str_upper" | "to_upper" | "upper" | "uppercase" => JitOp::StrUpper,
        "str_trim" | "trim" => JitOp::StrTrim,
        "str_len" | "string_len" | "length" => JitOp::StrLen,
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
pub fn compile_jit_raw(
    coord_count: usize,
    total_slots: usize,
    steps: Vec<(JitOp, Vec<usize>, Vec<usize>)>,
    output_map: HashMap<String, usize>,
    nodes: Vec<Box<dyn PolydatNode>>,
) -> Result<JitKernelRaw, String> {
    let (raw_fn, _, module) = compile_jit_impl(&steps, false)?;
    Ok(JitKernelRaw {
        core: JitCore { buffer: vec![0u64; total_slots], coord_count, output_map, _module: module, _nodes: nodes },
        code_fn: raw_fn,
    })
}

/// SRD-105 cone entry: codegen only, no kernel wrapper — the cone
/// node owns the function pointer and module directly and provides
/// its own buffer per eval.
pub(crate) fn compile_jit_entry(
    steps: &[(JitOp, Vec<usize>, Vec<usize>)],
) -> Result<(unsafe fn(*const u64, *mut u64), JITModule), String> {
    let (raw_fn, _, module) = compile_jit_impl(steps, false)?;
    Ok((raw_fn, module))
}

/// Compile a set of JIT steps into a push (per-node dirty tracking) native kernel.
pub(crate) fn compile_jit_push(
    coord_count: usize,
    total_slots: usize,
    steps: Vec<(JitOp, Vec<usize>, Vec<usize>)>,
    output_map: HashMap<String, usize>,
    nodes: Vec<Box<dyn PolydatNode>>,
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
    nodes: Vec<Box<dyn PolydatNode>>,
    input_dependents: &[Vec<usize>],
) -> Result<JitKernelPull, String> {
    let buffer_len = total_slots;
    // Pull uses the RAW jit function (no per-node clean checks)
    let (raw_fn, _, module) = compile_jit_impl(&steps, false)?;
    let step_outs: Vec<Vec<usize>> = steps.iter().map(|(_, _, o)| o.clone()).collect();
    let slot_provenance = compute_jit_slot_provenance(coord_count, buffer_len, &step_outs, input_dependents);
    Ok(JitKernelPull {
        core: JitCore { buffer: vec![0u64; total_slots], coord_count, output_map, _module: module, _nodes: nodes },
        code_fn: raw_fn,
        slot_provenance,
        changed_mask: crate::kernel::ProvMask::all_below(coord_count),
    })
}

/// Compile a set of JIT steps into a push+pull (full optimization) native kernel.
pub(crate) fn compile_jit_push_pull(
    coord_count: usize,
    total_slots: usize,
    steps: Vec<(JitOp, Vec<usize>, Vec<usize>)>,
    output_map: HashMap<String, usize>,
    nodes: Vec<Box<dyn PolydatNode>>,
    input_dependents: Vec<Vec<usize>>,
) -> Result<JitKernelPushPull, String> {
    let step_count = steps.len();
    let buffer_len = total_slots;
    let (_, prov_fn, module) = compile_jit_impl(&steps, true)?;
    let step_outs: Vec<Vec<usize>> = steps.iter().map(|(_, _, o)| o.clone()).collect();
    let slot_provenance = compute_jit_slot_provenance(coord_count, buffer_len, &step_outs, &input_dependents);
    Ok(JitKernelPushPull {
        core: JitCore { buffer: vec![0u64; total_slots], coord_count, output_map, _module: module, _nodes: nodes },
        code_fn_prov: prov_fn,
        node_clean: vec![0u8; step_count],
        input_dependents,
        slot_provenance,
        changed_mask: crate::kernel::ProvMask::all_below(coord_count),
    })
}

// ── Core Cranelift IR generation ───────────────────────────

/// `(raw_fn, prov_fn, module)` — the trio produced by the core
/// JIT compile: the scalar entry point, the provenance-tracking
/// entry point, and the owning module that keeps both alive.
type JitCompiled = (
    unsafe fn(*const u64, *mut u64),
    unsafe fn(*const u64, *mut u64, *mut u8),
    JITModule,
);

/// Core JIT compilation. Returns (raw_fn, prov_fn, module).
/// If provenance=false, prov_fn is a dummy transmute of raw_fn.
/// If provenance=true, raw_fn is a dummy transmute of prov_fn.
fn compile_jit_impl(
    steps: &[(JitOp, Vec<usize>, Vec<usize>)],
    provenance: bool,
) -> Result<JitCompiled, String> {
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
    jit_builder.symbol("jit_pcg", jit_pcg as *const u8);
    jit_builder.symbol("jit_pcg_stream", jit_pcg_stream as *const u8);
    jit_builder.symbol("jit_unfair_coin", jit_unfair_coin as *const u8);
    jit_builder.symbol("jit_chance", jit_chance as *const u8);
    jit_builder.symbol("jit_n_of", jit_n_of as *const u8);
    jit_builder.symbol("jit_cycle_walk", jit_cycle_walk as *const u8);
    jit_builder.symbol("jit_perlin_1d", jit_perlin_1d as *const u8);
    jit_builder.symbol("jit_perlin_2d", jit_perlin_2d as *const u8);
    jit_builder.symbol("jit_simplex_2d", jit_simplex_2d as *const u8);
    jit_builder.symbol("jit_fractal_noise_1d", jit_fractal_noise_1d as *const u8);
    jit_builder.symbol("jit_fractal_noise_2d", jit_fractal_noise_2d as *const u8);
    jit_builder.symbol("jit_thread_id", jit_thread_id as *const u8);
    jit_builder.symbol("jit_current_epoch_millis", jit_current_epoch_millis as *const u8);
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
    jit_builder.symbol("jit_floor_base10", jit_floor_base10 as *const u8);
    jit_builder.symbol("jit_ceiling_base10", jit_ceiling_base10 as *const u8);
    jit_builder.symbol("jit_closest_base10", jit_closest_base10 as *const u8);
    jit_builder.symbol("jit_floor_decade", jit_floor_decade as *const u8);
    jit_builder.symbol("jit_ceiling_decade", jit_ceiling_decade as *const u8);
    jit_builder.symbol("jit_closest_decade", jit_closest_decade as *const u8);
    jit_builder.symbol("jit_floor_binomial", jit_floor_binomial as *const u8);
    jit_builder.symbol("jit_ceiling_binomial", jit_ceiling_binomial as *const u8);
    jit_builder.symbol("jit_closest_binomial", jit_closest_binomial as *const u8);
    jit_builder.symbol("jit_floor_fibonacci", jit_floor_fibonacci as *const u8);
    jit_builder.symbol("jit_ceiling_fibonacci", jit_ceiling_fibonacci as *const u8);
    jit_builder.symbol("jit_closest_fibonacci", jit_closest_fibonacci as *const u8);
    jit_builder.symbol("jit_atan2", jit_atan2 as *const u8);
    jit_builder.symbol("jit_pow", jit_pow as *const u8);
    jit_builder.symbol("jit_round_nearest", jit_round_nearest as *const u8);
    jit_builder.symbol("jit_round_floor", jit_round_floor as *const u8);
    jit_builder.symbol("jit_round_ceiling", jit_round_ceiling as *const u8);
    // String externs (SRD 111)
    jit_builder.symbol("jit_u64_to_str", jit_u64_to_str as *const u8);
    jit_builder.symbol("jit_i64_to_str", jit_i64_to_str as *const u8);
    jit_builder.symbol("jit_f64_to_str", jit_f64_to_str as *const u8);
    jit_builder.symbol("jit_bool_to_str", jit_bool_to_str as *const u8);
    jit_builder.symbol("jit_str_to_u64", jit_str_to_u64 as *const u8);
    jit_builder.symbol("jit_str_to_i64", jit_str_to_i64 as *const u8);
    jit_builder.symbol("jit_str_to_f64", jit_str_to_f64 as *const u8);
    jit_builder.symbol("jit_str_to_bool", jit_str_to_bool as *const u8);
    jit_builder.symbol("jit_str_concat", jit_str_concat as *const u8);
    jit_builder.symbol("jit_str_lower", jit_str_lower as *const u8);
    jit_builder.symbol("jit_str_upper", jit_str_upper as *const u8);
    jit_builder.symbol("jit_str_trim", jit_str_trim as *const u8);
    jit_builder.symbol("jit_str_len", jit_str_len as *const u8);

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

    let pcg_func_id = {
        let mut sig = module.make_signature();
        for _ in 0..3 { sig.params.push(AbiParam::new(types::I64)); }
        sig.returns.push(AbiParam::new(types::I64));
        module.declare_function("jit_pcg", Linkage::Import, &sig)
            .map_err(|e| format!("declare pcg: {e}"))?
    };
    let pcg_stream_func_id = {
        let mut sig = module.make_signature();
        for _ in 0..3 { sig.params.push(AbiParam::new(types::I64)); }
        sig.returns.push(AbiParam::new(types::I64));
        module.declare_function("jit_pcg_stream", Linkage::Import, &sig)
            .map_err(|e| format!("declare pcg_stream: {e}"))?
    };
    let n_of_func_id = {
        let mut sig = module.make_signature();
        for _ in 0..3 { sig.params.push(AbiParam::new(types::I64)); }
        sig.returns.push(AbiParam::new(types::I64));
        module.declare_function("jit_n_of", Linkage::Import, &sig)
            .map_err(|e| format!("declare n_of: {e}"))?
    };
    let cycle_walk_func_id = {
        let mut sig = module.make_signature();
        for _ in 0..4 { sig.params.push(AbiParam::new(types::I64)); }
        sig.returns.push(AbiParam::new(types::I64));
        module.declare_function("jit_cycle_walk", Linkage::Import, &sig)
            .map_err(|e| format!("declare cycle_walk: {e}"))?
    };
    let perlin_1d_func_id = {
        let mut sig = module.make_signature();
        for _ in 0..3 { sig.params.push(AbiParam::new(types::I64)); }
        sig.returns.push(AbiParam::new(types::I64));
        module.declare_function("jit_perlin_1d", Linkage::Import, &sig)
            .map_err(|e| format!("declare perlin_1d: {e}"))?
    };
    let perlin_2d_func_id = {
        let mut sig = module.make_signature();
        for _ in 0..4 { sig.params.push(AbiParam::new(types::I64)); }
        sig.returns.push(AbiParam::new(types::I64));
        module.declare_function("jit_perlin_2d", Linkage::Import, &sig)
            .map_err(|e| format!("declare perlin_2d: {e}"))?
    };
    let simplex_2d_func_id = {
        let mut sig = module.make_signature();
        for _ in 0..4 { sig.params.push(AbiParam::new(types::I64)); }
        sig.returns.push(AbiParam::new(types::I64));
        module.declare_function("jit_simplex_2d", Linkage::Import, &sig)
            .map_err(|e| format!("declare simplex_2d: {e}"))?
    };
    let fractal_noise_1d_func_id = {
        let mut sig = module.make_signature();
        for _ in 0..4 { sig.params.push(AbiParam::new(types::I64)); }
        sig.returns.push(AbiParam::new(types::I64));
        module.declare_function("jit_fractal_noise_1d", Linkage::Import, &sig)
            .map_err(|e| format!("declare fractal_noise_1d: {e}"))?
    };
    let fractal_noise_2d_func_id = {
        let mut sig = module.make_signature();
        for _ in 0..5 { sig.params.push(AbiParam::new(types::I64)); }
        sig.returns.push(AbiParam::new(types::I64));
        module.declare_function("jit_fractal_noise_2d", Linkage::Import, &sig)
            .map_err(|e| format!("declare fractal_noise_2d: {e}"))?
    };
    let thread_id_func_id = {
        let mut sig = module.make_signature();
        sig.returns.push(AbiParam::new(types::I64));
        module.declare_function("jit_thread_id", Linkage::Import, &sig)
            .map_err(|e| format!("declare thread_id: {e}"))?
    };
    let current_epoch_millis_func_id = {
        let mut sig = module.make_signature();
        sig.returns.push(AbiParam::new(types::I64));
        module.declare_function("jit_current_epoch_millis", Linkage::Import, &sig)
            .map_err(|e| format!("declare current_epoch_millis: {e}"))?
    };

    // Declare math externs: unary (u64) -> u64
    let math_unary_names = [
        "jit_sin", "jit_cos", "jit_tan", "jit_asin", "jit_acos",
        "jit_atan", "jit_sqrt", "jit_abs_f64", "jit_ln", "jit_exp",
        "jit_floor_base10", "jit_ceiling_base10", "jit_closest_base10",
        "jit_floor_decade", "jit_ceiling_decade", "jit_closest_decade",
        "jit_floor_binomial", "jit_ceiling_binomial", "jit_closest_binomial",
        "jit_floor_fibonacci", "jit_ceiling_fibonacci", "jit_closest_fibonacci",
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

    // Declare param-helper extern:
    // jit_is_positive_fail(u64, name_ptr, name_len) -> u64
    // (never returns, but the ABI requires a return type).
    let is_positive_fail_id = {
        let mut sig = module.make_signature();
        sig.params.push(AbiParam::new(types::I64));
        sig.params.push(AbiParam::new(types::I64));
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

    // Declare param-helper extern:
    // jit_is_one_of_fail(u64, set_ptr, set_len) -> u64
    let is_one_of_fail_id = {
        let mut sig = module.make_signature();
        sig.params.push(AbiParam::new(types::I64));
        sig.params.push(AbiParam::new(types::I64));
        sig.params.push(AbiParam::new(types::I64));
        sig.returns.push(AbiParam::new(types::I64));
        module.declare_function("jit_is_one_of_fail", Linkage::Import, &sig)
            .map_err(|e| format!("declare is_one_of_fail: {e}"))?
    };

    // Declare math externs: binary (u64, u64) -> u64
    let math_binary_names = [
        "jit_atan2", "jit_pow",
        "jit_round_nearest", "jit_round_floor", "jit_round_ceiling",
    ];
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

    // Declare string externs (SRD 111)
    let string_unary_names = [
        "jit_u64_to_str", "jit_i64_to_str", "jit_f64_to_str", "jit_bool_to_str",
        "jit_str_to_u64", "jit_str_to_i64", "jit_str_to_f64", "jit_str_to_bool",
        "jit_str_lower", "jit_str_upper", "jit_str_trim", "jit_str_len",
    ];
    let mut string_unary_ids = Vec::new();
    for name in &string_unary_names {
        let mut sig = module.make_signature();
        sig.params.push(AbiParam::new(types::I64));
        sig.returns.push(AbiParam::new(types::I64));
        string_unary_ids.push(
            module.declare_function(name, Linkage::Import, &sig)
                .map_err(|e| format!("declare {name}: {e}"))?
        );
    }

    let str_concat_id = {
        let mut sig = module.make_signature();
        sig.params.push(AbiParam::new(types::I64));
        sig.params.push(AbiParam::new(types::I64));
        sig.returns.push(AbiParam::new(types::I64));
        module.declare_function("jit_str_concat", Linkage::Import, &sig)
            .map_err(|e| format!("declare str_concat: {e}"))?
    };

    // Function signature depends on provenance mode:
    // Without: fn(coords: *const u64, buffer: *mut u64)
    // With:    fn(coords: *const u64, buffer: *mut u64, clean: *mut u8)
    let mut sig = module.make_signature();
    sig.params.push(AbiParam::new(types::I64)); // coords ptr
    sig.params.push(AbiParam::new(types::I64)); // buffer ptr
    if provenance {
        sig.params.push(AbiParam::new(types::I64)); // clean ptr
    }
    let func_id = module.declare_function("polydat_kernel", Linkage::Local, &sig)
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
        let pcg_func_ref = module.declare_func_in_func(pcg_func_id, builder.func);
        let pcg_stream_func_ref = module.declare_func_in_func(pcg_stream_func_id, builder.func);
        let n_of_func_ref = module.declare_func_in_func(n_of_func_id, builder.func);
        let cycle_walk_func_ref = module.declare_func_in_func(cycle_walk_func_id, builder.func);
        let perlin_1d_func_ref = module.declare_func_in_func(perlin_1d_func_id, builder.func);
        let perlin_2d_func_ref = module.declare_func_in_func(perlin_2d_func_id, builder.func);
        let simplex_2d_func_ref = module.declare_func_in_func(simplex_2d_func_id, builder.func);
        let fractal_noise_1d_func_ref = module.declare_func_in_func(fractal_noise_1d_func_id, builder.func);
        let fractal_noise_2d_func_ref = module.declare_func_in_func(fractal_noise_2d_func_id, builder.func);
        let thread_id_func_ref = module.declare_func_in_func(thread_id_func_id, builder.func);
        let current_epoch_millis_func_ref = module.declare_func_in_func(current_epoch_millis_func_id, builder.func);
        let math_unary_refs: Vec<_> = math_unary_ids.iter()
            .map(|id| module.declare_func_in_func(*id, builder.func))
            .collect();
        let math_binary_refs: Vec<_> = math_binary_ids.iter()
            .map(|id| module.declare_func_in_func(*id, builder.func))
            .collect();
        let string_unary_refs: Vec<_> = string_unary_ids.iter()
            .map(|id| module.declare_func_in_func(*id, builder.func))
            .collect();
        let str_concat_ref = module.declare_func_in_func(str_concat_id, builder.func);

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
                    if *c == 0 {
                        let zero = builder.ins().iconst(types::I64, 0);
                        store_slot(&mut builder, buffer_ptr, output_slots[0], zero);
                    } else {
                        let c_val = builder.ins().iconst(types::I64, *c as i64);
                        let result = builder.ins().udiv(val, c_val);
                        store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                    }
                }
                JitOp::ModConst(c) => {
                    let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    if *c == 0 {
                        let zero = builder.ins().iconst(types::I64, 0);
                        store_slot(&mut builder, buffer_ptr, output_slots[0], zero);
                    } else {
                        let c_val = builder.ins().iconst(types::I64, *c as i64);
                        let result = builder.ins().urem(val, c_val);
                        store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                    }
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
                JitOp::SplitMix64 => {
                    let x0 = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let c_gamma = builder.ins().iconst(types::I64, 0x9e3779b97f4a7c15u64 as i64);
                    let x1 = builder.ins().iadd(x0, c_gamma);
                    let s30 = builder.ins().ushr_imm(x1, 30);
                    let x2 = builder.ins().bxor(x1, s30);
                    let c_m1 = builder.ins().iconst(types::I64, 0xbf58476d1ce4e5b9u64 as i64);
                    let x3 = builder.ins().imul(x2, c_m1);
                    let s27 = builder.ins().ushr_imm(x3, 27);
                    let x4 = builder.ins().bxor(x3, s27);
                    let c_m2 = builder.ins().iconst(types::I64, 0x94d049bb133111ebu64 as i64);
                    let x5 = builder.ins().imul(x4, c_m2);
                    let s31 = builder.ins().ushr_imm(x5, 31);
                    let result = builder.ins().bxor(x5, s31);
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::FairCoin => {
                    let x0 = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let c_gamma = builder.ins().iconst(types::I64, 0x9e3779b97f4a7c15u64 as i64);
                    let x1 = builder.ins().iadd(x0, c_gamma);
                    let s30 = builder.ins().ushr_imm(x1, 30);
                    let x2 = builder.ins().bxor(x1, s30);
                    let c_m1 = builder.ins().iconst(types::I64, 0xbf58476d1ce4e5b9u64 as i64);
                    let x3 = builder.ins().imul(x2, c_m1);
                    let s27 = builder.ins().ushr_imm(x3, 27);
                    let x4 = builder.ins().bxor(x3, s27);
                    let c_m2 = builder.ins().iconst(types::I64, 0x94d049bb133111ebu64 as i64);
                    let x5 = builder.ins().imul(x4, c_m2);
                    let s31 = builder.ins().ushr_imm(x5, 31);
                    let h = builder.ins().bxor(x5, s31);
                    let one = builder.ins().iconst(types::I64, 1);
                    let result = builder.ins().band(h, one);
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::UnfairCoinConst(p_bits) => {
                    let x0 = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let c_gamma = builder.ins().iconst(types::I64, 0x9e3779b97f4a7c15u64 as i64);
                    let x1 = builder.ins().iadd(x0, c_gamma);
                    let s30 = builder.ins().ushr_imm(x1, 30);
                    let x2 = builder.ins().bxor(x1, s30);
                    let c_m1 = builder.ins().iconst(types::I64, 0xbf58476d1ce4e5b9u64 as i64);
                    let x3 = builder.ins().imul(x2, c_m1);
                    let s27 = builder.ins().ushr_imm(x3, 27);
                    let x4 = builder.ins().bxor(x3, s27);
                    let c_m2 = builder.ins().iconst(types::I64, 0x94d049bb133111ebu64 as i64);
                    let x5 = builder.ins().imul(x4, c_m2);
                    let s31 = builder.ins().ushr_imm(x5, 31);
                    let h = builder.ins().bxor(x5, s31);

                    let fval = builder.ins().fcvt_from_uint(types::F64, h);
                    let max_f = builder.ins().f64const(u64::MAX as f64);
                    let unit = builder.ins().fdiv(fval, max_f);
                    let p_f = builder.ins().f64const(f64::from_bits(*p_bits));
                    let cmp = builder.ins().fcmp(ir::condcodes::FloatCC::LessThan, unit, p_f);
                    let zero = builder.ins().iconst(types::I64, 0);
                    let one = builder.ins().iconst(types::I64, 1);
                    let result = builder.ins().select(cmp, one, zero);
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::ChanceConst(p_bits) => {
                    let x0 = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let c_gamma = builder.ins().iconst(types::I64, 0x9e3779b97f4a7c15u64 as i64);
                    let x1 = builder.ins().iadd(x0, c_gamma);
                    let s30 = builder.ins().ushr_imm(x1, 30);
                    let x2 = builder.ins().bxor(x1, s30);
                    let c_m1 = builder.ins().iconst(types::I64, 0xbf58476d1ce4e5b9u64 as i64);
                    let x3 = builder.ins().imul(x2, c_m1);
                    let s27 = builder.ins().ushr_imm(x3, 27);
                    let x4 = builder.ins().bxor(x3, s27);
                    let c_m2 = builder.ins().iconst(types::I64, 0x94d049bb133111ebu64 as i64);
                    let x5 = builder.ins().imul(x4, c_m2);
                    let s31 = builder.ins().ushr_imm(x5, 31);
                    let h = builder.ins().bxor(x5, s31);

                    let fval = builder.ins().fcvt_from_uint(types::F64, h);
                    let max_f = builder.ins().f64const(u64::MAX as f64);
                    let unit = builder.ins().fdiv(fval, max_f);
                    let p_f = builder.ins().f64const(f64::from_bits(*p_bits));
                    let cmp = builder.ins().fcmp(ir::condcodes::FloatCC::LessThan, unit, p_f);
                    let zero_bits = builder.ins().iconst(types::I64, 0.0_f64.to_bits() as i64);
                    let one_bits = builder.ins().iconst(types::I64, 1.0_f64.to_bits() as i64);
                    let result = builder.ins().select(cmp, one_bits, zero_bits);
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::Popcnt => {
                    let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let result = builder.ins().popcnt(val);
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::Clz => {
                    let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let result = builder.ins().clz(val);
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::Ctz => {
                    let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let result = builder.ins().ctz(val);
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::Bswap => {
                    let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let result = builder.ins().bswap(val);
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

                // ── Register plane: one vector instruction per op ──
                JitOp::RegBinOp(lane, arith) => {
                    let vt = reg_lane_type(*lane);
                    let a = load_reg128(&mut builder, buffer_ptr, input_slots[0], vt);
                    let b = load_reg128(&mut builder, buffer_ptr, input_slots[2], vt);
                    let is_float = matches!(*lane, 4 | 5);
                    let r = match (arith, is_float) {
                        (0, false) => builder.ins().iadd(a, b),
                        (1, false) => builder.ins().isub(a, b),
                        (2, false) => builder.ins().imul(a, b),
                        (0, true) => builder.ins().fadd(a, b),
                        (1, true) => builder.ins().fsub(a, b),
                        (2, true) => builder.ins().fmul(a, b),
                        _ => unreachable!("RegBinOp arith index out of range"),
                    };
                    store_reg128(&mut builder, buffer_ptr, output_slots[0], r);
                }
                JitOp::RegCopy => {
                    let v = load_reg128(
                        &mut builder, buffer_ptr, input_slots[0], types::I64X2);
                    store_reg128(&mut builder, buffer_ptr, output_slots[0], v);
                }
                JitOp::RegSplat(lane) => {
                    let vt = reg_lane_type(*lane);
                    let scalar = match *lane {
                        // Integer lanes: u64 slot reduced to lane width.
                        0 => {
                            let v = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                            builder.ins().ireduce(types::I8, v)
                        }
                        1 => {
                            let v = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                            builder.ins().ireduce(types::I16, v)
                        }
                        2 => {
                            let v = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                            builder.ins().ireduce(types::I32, v)
                        }
                        3 => load_slot(&mut builder, buffer_ptr, input_slots[0]),
                        // Float lanes: f64 slot, demoted for f32.
                        4 => {
                            let f = load_slot_f64(&mut builder, buffer_ptr, input_slots[0]);
                            builder.ins().fdemote(types::F32, f)
                        }
                        5 => load_slot_f64(&mut builder, buffer_ptr, input_slots[0]),
                        _ => unreachable!("RegSplat lane index out of range"),
                    };
                    let v = builder.ins().splat(vt, scalar);
                    store_reg128(&mut builder, buffer_ptr, output_slots[0], v);
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

                JitOp::IsPositiveCheck { name_ptr, name_len } => {
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
                    let np = builder.ins().iconst(types::I64, *name_ptr as i64);
                    let nl = builder.ins().iconst(types::I64, *name_len as i64);
                    let _ = builder.ins().call(is_positive_fail_ref, &[val, np, nl]);
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

                JitOp::IsOneOfCheck { allowed, set_ptr, set_len } => {
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
                    let sp = builder.ins().iconst(types::I64, *set_ptr as i64);
                    let sl = builder.ins().iconst(types::I64, *set_len as i64);
                    let _ = builder.ins().call(is_one_of_fail_ref, &[val, sp, sl]);
                    builder.ins().jump(ok_block, &[]);

                    builder.switch_to_block(ok_block);
                    builder.seal_block(ok_block);
                    store_slot(&mut builder, buffer_ptr, output_slots[0], val);
                }

                JitOp::U64Cmp(cc) => {
                    let a = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let b = load_slot(&mut builder, buffer_ptr, input_slots[1]);
                    let cmp = builder.ins().icmp(*cc, a, b);
                    let zero = builder.ins().iconst(types::I64, 0);
                    let one = builder.ins().iconst(types::I64, 1);
                    let result = builder.ins().select(cmp, one, zero);
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::F64Cmp(cc) => {
                    let a = load_slot_f64(&mut builder, buffer_ptr, input_slots[0]);
                    let b = load_slot_f64(&mut builder, buffer_ptr, if input_slots.len() > 1 { input_slots[1] } else { input_slots[0] });
                    let cmp = builder.ins().fcmp(*cc, a, b);
                    let zero = builder.ins().iconst(types::I64, 0);
                    let one = builder.ins().iconst(types::I64, 1);
                    let result = builder.ins().select(cmp, one, zero);
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::SelectU64 => {
                    let cond = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let a = load_slot(&mut builder, buffer_ptr, if input_slots.len() > 1 { input_slots[1] } else { input_slots[0] });
                    let b = load_slot(&mut builder, buffer_ptr, if input_slots.len() > 2 { input_slots[2] } else { input_slots[0] });
                    let zero = builder.ins().iconst(types::I64, 0);
                    let is_nonzero = builder.ins().icmp(ir::condcodes::IntCC::NotEqual, cond, zero);
                    let result = builder.ins().select(is_nonzero, a, b);
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::SelectF64 => {
                    let cond = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let a = load_slot_f64(&mut builder, buffer_ptr, if input_slots.len() > 1 { input_slots[1] } else { input_slots[0] });
                    let b = load_slot_f64(&mut builder, buffer_ptr, if input_slots.len() > 2 { input_slots[2] } else { input_slots[0] });
                    let zero = builder.ins().iconst(types::I64, 0);
                    let is_nonzero = builder.ins().icmp(ir::condcodes::IntCC::NotEqual, cond, zero);
                    let result = builder.ins().select(is_nonzero, a, b);
                    store_slot_f64(&mut builder, buffer_ptr, output_slots[0], result);
                }

                JitOp::I64ToF64 => {
                    let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let fval = builder.ins().fcvt_from_sint(types::F64, val);
                    store_slot_f64(&mut builder, buffer_ptr, output_slots[0], fval);
                }
                JitOp::F64ToI64 => {
                    let fval = load_slot_f64(&mut builder, buffer_ptr, input_slots[0]);
                    let ival = builder.ins().fcvt_to_sint(types::I64, fval);
                    store_slot(&mut builder, buffer_ptr, output_slots[0], ival);
                }
                JitOp::SignExtendI32 => {
                    let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let i32_val = builder.ins().ireduce(types::I32, val);
                    let sext_val = builder.ins().sextend(types::I64, i32_val);
                    store_slot(&mut builder, buffer_ptr, output_slots[0], sext_val);
                }
                JitOp::SignExtendI16 => {
                    let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let i16_val = builder.ins().ireduce(types::I16, val);
                    let sext_val = builder.ins().sextend(types::I64, i16_val);
                    store_slot(&mut builder, buffer_ptr, output_slots[0], sext_val);
                }
                JitOp::SignExtendI8 => {
                    let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let i8_val = builder.ins().ireduce(types::I8, val);
                    let sext_val = builder.ins().sextend(types::I64, i8_val);
                    store_slot(&mut builder, buffer_ptr, output_slots[0], sext_val);
                }
                JitOp::ZeroExtendU32 => {
                    let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let mask = builder.ins().iconst(types::I64, 0xFFFFFFFFu64 as i64);
                    let result = builder.ins().band(val, mask);
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::ZeroExtendU16 => {
                    let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let mask = builder.ins().iconst(types::I64, 0xFFFFu64 as i64);
                    let result = builder.ins().band(val, mask);
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::ZeroExtendU8 => {
                    let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let mask = builder.ins().iconst(types::I64, 0xFFu64 as i64);
                    let result = builder.ins().band(val, mask);
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::ToBool => {
                    let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let zero = builder.ins().iconst(types::I64, 0);
                    let one = builder.ins().iconst(types::I64, 1);
                    let cmp = builder.ins().icmp(ir::condcodes::IntCC::NotEqual, val, zero);
                    let result = builder.ins().select(cmp, one, zero);
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::ConstU64(v) | JitOp::ConstF64(v) => {
                    let result = builder.ins().iconst(types::I64, *v as i64);
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::HashRangeConst(max) => {
                    let input = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let c_gamma = builder.ins().iconst(types::I64, 0x9e3779b97f4a7c15u64 as i64);
                    let x1 = builder.ins().iadd(input, c_gamma);
                    let s30 = builder.ins().ushr_imm(x1, 30);
                    let x2 = builder.ins().bxor(x1, s30);
                    let c_m1 = builder.ins().iconst(types::I64, 0xbf58476d1ce4e5b9u64 as i64);
                    let x3 = builder.ins().imul(x2, c_m1);
                    let s27 = builder.ins().ushr_imm(x3, 27);
                    let x4 = builder.ins().bxor(x3, s27);
                    let c_m2 = builder.ins().iconst(types::I64, 0x94d049bb133111ebu64 as i64);
                    let x5 = builder.ins().imul(x4, c_m2);
                    let s31 = builder.ins().ushr_imm(x5, 31);
                    let h = builder.ins().bxor(x5, s31);
                    if *max == 0 {
                        let zero = builder.ins().iconst(types::I64, 0);
                        store_slot(&mut builder, buffer_ptr, output_slots[0], zero);
                    } else {
                        let m = builder.ins().iconst(types::I64, *max as i64);
                        let rem = builder.ins().urem(h, m);
                        store_slot(&mut builder, buffer_ptr, output_slots[0], rem);
                    }
                }
                JitOp::HashIntervalConst(min_bits, max_bits) => {
                    let input = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let c_gamma = builder.ins().iconst(types::I64, 0x9e3779b97f4a7c15u64 as i64);
                    let x1 = builder.ins().iadd(input, c_gamma);
                    let s30 = builder.ins().ushr_imm(x1, 30);
                    let x2 = builder.ins().bxor(x1, s30);
                    let c_m1 = builder.ins().iconst(types::I64, 0xbf58476d1ce4e5b9u64 as i64);
                    let x3 = builder.ins().imul(x2, c_m1);
                    let s27 = builder.ins().ushr_imm(x3, 27);
                    let x4 = builder.ins().bxor(x3, s27);
                    let c_m2 = builder.ins().iconst(types::I64, 0x94d049bb133111ebu64 as i64);
                    let x5 = builder.ins().imul(x4, c_m2);
                    let s31 = builder.ins().ushr_imm(x5, 31);
                    let h = builder.ins().bxor(x5, s31);

                    let h_f = builder.ins().fcvt_from_uint(types::F64, h);
                    let denom = builder.ins().f64const(u64::MAX as f64);
                    let unit = builder.ins().fdiv(h_f, denom);
                    let min_f = f64::from_bits(*min_bits);
                    let max_f = f64::from_bits(*max_bits);
                    let span = builder.ins().f64const(max_f - min_f);
                    let min_val = builder.ins().f64const(min_f);
                    let scaled = builder.ins().fmul(unit, span);
                    let res_f = builder.ins().fadd(min_val, scaled);
                    let res = builder.ins().bitcast(types::I64, ir::MemFlags::new(), res_f);
                    store_slot(&mut builder, buffer_ptr, output_slots[0], res);
                }
                JitOp::InvLerpConst(a_bits, b_bits) => {
                    let input = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let in_f = builder.ins().bitcast(types::F64, ir::MemFlags::new(), input);
                    let a_f = f64::from_bits(*a_bits);
                    let b_f = f64::from_bits(*b_bits);
                    let a_val = builder.ins().f64const(a_f);
                    let span = b_f - a_f;
                    let res_f = if span == 0.0 {
                        builder.ins().f64const(0.0)
                    } else {
                        let inv_span = builder.ins().f64const(1.0 / span);
                        let diff = builder.ins().fsub(in_f, a_val);
                        let t = builder.ins().fmul(diff, inv_span);
                        let zero = builder.ins().f64const(0.0);
                        let one = builder.ins().f64const(1.0);
                        let clamped_low = builder.ins().fmax(t, zero);
                        builder.ins().fmin(clamped_low, one)
                    };
                    let res = builder.ins().bitcast(types::I64, ir::MemFlags::new(), res_f);
                    store_slot(&mut builder, buffer_ptr, output_slots[0], res);
                }
                JitOp::RemapConst(in_min_bits, in_max_bits, out_min_bits, out_max_bits) => {
                    let input = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let in_f = builder.ins().bitcast(types::F64, ir::MemFlags::new(), input);
                    let in_min = f64::from_bits(*in_min_bits);
                    let in_max = f64::from_bits(*in_max_bits);
                    let out_min = f64::from_bits(*out_min_bits);
                    let out_max = f64::from_bits(*out_max_bits);
                    let in_span = in_max - in_min;
                    let in_min_val = builder.ins().f64const(in_min);
                    let out_min_val = builder.ins().f64const(out_min);
                    let out_span_val = builder.ins().f64const(out_max - out_min);
                    let res_f = if in_span == 0.0 {
                        out_min_val
                    } else {
                        let inv_in_span = builder.ins().f64const(1.0 / in_span);
                        let diff = builder.ins().fsub(in_f, in_min_val);
                        let t = builder.ins().fmul(diff, inv_in_span);
                        let scaled = builder.ins().fmul(t, out_span_val);
                        builder.ins().fadd(out_min_val, scaled)
                    };
                    let res = builder.ins().bitcast(types::I64, ir::MemFlags::new(), res_f);
                    store_slot(&mut builder, buffer_ptr, output_slots[0], res);
                }
                JitOp::EpochOffsetConst(base) => {
                    let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let b = builder.ins().iconst(types::I64, *base as i64);
                    let res = builder.ins().iadd(val, b);
                    store_slot(&mut builder, buffer_ptr, output_slots[0], res);
                }
                JitOp::EpochScaleConst(factor) => {
                    let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let f = builder.ins().iconst(types::I64, *factor as i64);
                    let res = builder.ins().imul(val, f);
                    store_slot(&mut builder, buffer_ptr, output_slots[0], res);
                }
                JitOp::ThreadId => {
                    let call = builder.ins().call(thread_id_func_ref, &[]);
                    let res = builder.inst_results(call)[0];
                    store_slot(&mut builder, buffer_ptr, output_slots[0], res);
                }
                JitOp::CurrentEpochMillis => {
                    let call = builder.ins().call(current_epoch_millis_func_ref, &[]);
                    let res = builder.inst_results(call)[0];
                    store_slot(&mut builder, buffer_ptr, output_slots[0], res);
                }
                JitOp::Perlin1dConst(perm_ptr, freq_bits) => {
                    let input = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let p = builder.ins().iconst(types::I64, *perm_ptr as i64);
                    let fb = builder.ins().iconst(types::I64, *freq_bits as i64);
                    let call = builder.ins().call(perlin_1d_func_ref, &[input, p, fb]);
                    let res = builder.inst_results(call)[0];
                    store_slot(&mut builder, buffer_ptr, output_slots[0], res);
                }
                JitOp::Perlin2dConst(perm_ptr, freq_bits) => {
                    let x = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let y = load_slot(&mut builder, buffer_ptr, input_slots[1]);
                    let p = builder.ins().iconst(types::I64, *perm_ptr as i64);
                    let fb = builder.ins().iconst(types::I64, *freq_bits as i64);
                    let call = builder.ins().call(perlin_2d_func_ref, &[x, y, p, fb]);
                    let res = builder.inst_results(call)[0];
                    store_slot(&mut builder, buffer_ptr, output_slots[0], res);
                }
                JitOp::Simplex2dConst(perm_ptr, freq_bits) => {
                    let x = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let y = load_slot(&mut builder, buffer_ptr, input_slots[1]);
                    let p = builder.ins().iconst(types::I64, *perm_ptr as i64);
                    let fb = builder.ins().iconst(types::I64, *freq_bits as i64);
                    let call = builder.ins().call(simplex_2d_func_ref, &[x, y, p, fb]);
                    let res = builder.inst_results(call)[0];
                    store_slot(&mut builder, buffer_ptr, output_slots[0], res);
                }
                JitOp::FractalNoise1dConst(perm_ptr, freq_bits, octaves) => {
                    let input = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let p = builder.ins().iconst(types::I64, *perm_ptr as i64);
                    let fb = builder.ins().iconst(types::I64, *freq_bits as i64);
                    let oct = builder.ins().iconst(types::I64, *octaves as i64);
                    let call = builder.ins().call(fractal_noise_1d_func_ref, &[input, p, fb, oct]);
                    let res = builder.inst_results(call)[0];
                    store_slot(&mut builder, buffer_ptr, output_slots[0], res);
                }
                JitOp::FractalNoise2dConst(perm_ptr, freq_bits, octaves) => {
                    let x = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let y = load_slot(&mut builder, buffer_ptr, input_slots[1]);
                    let p = builder.ins().iconst(types::I64, *perm_ptr as i64);
                    let fb = builder.ins().iconst(types::I64, *freq_bits as i64);
                    let oct = builder.ins().iconst(types::I64, *octaves as i64);
                    let call = builder.ins().call(fractal_noise_2d_func_ref, &[x, y, p, fb, oct]);
                    let res = builder.inst_results(call)[0];
                    store_slot(&mut builder, buffer_ptr, output_slots[0], res);
                }
                JitOp::CycleWalkConst(range, seed, inc) => {
                    let pos = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let r = builder.ins().iconst(types::I64, *range as i64);
                    let s = builder.ins().iconst(types::I64, *seed as i64);
                    let i = builder.ins().iconst(types::I64, *inc as i64);
                    let call = builder.ins().call(cycle_walk_func_ref, &[pos, r, s, i]);
                    let res = builder.inst_results(call)[0];
                    store_slot(&mut builder, buffer_ptr, output_slots[0], res);
                }

                JitOp::VariadicSum => {
                    if input_slots.is_empty() {
                        let zero = builder.ins().iconst(types::I64, 0);
                        store_slot(&mut builder, buffer_ptr, output_slots[0], zero);
                    } else {
                        let mut acc = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                        for &slot in &input_slots[1..] {
                            let v = load_slot(&mut builder, buffer_ptr, slot);
                            acc = builder.ins().iadd(acc, v);
                        }
                        store_slot(&mut builder, buffer_ptr, output_slots[0], acc);
                    }
                }
                JitOp::VariadicProduct => {
                    if input_slots.is_empty() {
                        let one = builder.ins().iconst(types::I64, 1);
                        store_slot(&mut builder, buffer_ptr, output_slots[0], one);
                    } else {
                        let mut acc = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                        for &slot in &input_slots[1..] {
                            let v = load_slot(&mut builder, buffer_ptr, slot);
                            acc = builder.ins().imul(acc, v);
                        }
                        store_slot(&mut builder, buffer_ptr, output_slots[0], acc);
                    }
                }
                JitOp::VariadicMin => {
                    if input_slots.is_empty() {
                        let zero = builder.ins().iconst(types::I64, 0);
                        store_slot(&mut builder, buffer_ptr, output_slots[0], zero);
                    } else {
                        let mut acc = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                        for &slot in &input_slots[1..] {
                            let v = load_slot(&mut builder, buffer_ptr, slot);
                            let cmp = builder.ins().icmp(ir::condcodes::IntCC::UnsignedLessThan, v, acc);
                            acc = builder.ins().select(cmp, v, acc);
                        }
                        store_slot(&mut builder, buffer_ptr, output_slots[0], acc);
                    }
                }
                JitOp::VariadicMax => {
                    if input_slots.is_empty() {
                        let zero = builder.ins().iconst(types::I64, 0);
                        store_slot(&mut builder, buffer_ptr, output_slots[0], zero);
                    } else {
                        let mut acc = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                        for &slot in &input_slots[1..] {
                            let v = load_slot(&mut builder, buffer_ptr, slot);
                            let cmp = builder.ins().icmp(ir::condcodes::IntCC::UnsignedGreaterThan, v, acc);
                            acc = builder.ins().select(cmp, v, acc);
                        }
                        store_slot(&mut builder, buffer_ptr, output_slots[0], acc);
                    }
                }

                JitOp::CeilToMultiple => {
                    let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let m = load_slot(&mut builder, buffer_ptr, input_slots[1]);
                    let zero = builder.ins().iconst(types::I64, 0);
                    let one = builder.ins().iconst(types::I64, 1);
                    let is_zero = builder.ins().icmp(ir::condcodes::IntCC::Equal, m, zero);
                    let calc_block = builder.create_block();
                    let merge_block = builder.create_block();
                    builder.append_block_param(merge_block, types::I64);
                    builder.ins().brif(is_zero, merge_block, &[val], calc_block, &[]);
                    builder.switch_to_block(calc_block);
                    builder.seal_block(calc_block);
                    let m_minus_1 = builder.ins().isub(m, one);
                    let num = builder.ins().iadd(val, m_minus_1);
                    let div = builder.ins().udiv(num, m);
                    let mul = builder.ins().imul(div, m);
                    builder.ins().jump(merge_block, &[mul]);
                    builder.switch_to_block(merge_block);
                    builder.seal_block(merge_block);
                    let result = builder.block_params(merge_block)[0];
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::CheckedAdd => {
                    let a = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let b = load_slot(&mut builder, buffer_ptr, input_slots[1]);
                    let sum = builder.ins().iadd(a, b);
                    let is_overflow = builder.ins().icmp(ir::condcodes::IntCC::UnsignedLessThan, sum, a);
                    let zero = builder.ins().iconst(types::I64, 0);
                    let result = builder.ins().select(is_overflow, zero, sum);
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::CheckedSub => {
                    let a = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let b = load_slot(&mut builder, buffer_ptr, input_slots[1]);
                    let is_lt = builder.ins().icmp(ir::condcodes::IntCC::UnsignedLessThan, a, b);
                    let diff = builder.ins().isub(a, b);
                    let zero = builder.ins().iconst(types::I64, 0);
                    let result = builder.ins().select(is_lt, zero, diff);
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::CheckedMul => {
                    let a = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let b = load_slot(&mut builder, buffer_ptr, input_slots[1]);
                    let prod = builder.ins().imul(a, b);
                    let zero = builder.ins().iconst(types::I64, 0);
                    let a_is_zero = builder.ins().icmp(ir::condcodes::IntCC::Equal, a, zero);
                    let div_block = builder.create_block();
                    let merge_block = builder.create_block();
                    builder.append_block_param(merge_block, types::I64);
                    builder.ins().brif(a_is_zero, merge_block, &[zero], div_block, &[]);
                    builder.switch_to_block(div_block);
                    builder.seal_block(div_block);
                    let div = builder.ins().udiv(prod, a);
                    let ok = builder.ins().icmp(ir::condcodes::IntCC::Equal, div, b);
                    let mul_res = builder.ins().select(ok, prod, zero);
                    builder.ins().jump(merge_block, &[mul_res]);
                    builder.switch_to_block(merge_block);
                    builder.seal_block(merge_block);
                    let result = builder.block_params(merge_block)[0];
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::MultiplesAtLeast => {
                    let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let m = load_slot(&mut builder, buffer_ptr, input_slots[1]);
                    let zero = builder.ins().iconst(types::I64, 0);
                    let one = builder.ins().iconst(types::I64, 1);
                    let is_zero = builder.ins().icmp(ir::condcodes::IntCC::Equal, m, zero);
                    let calc_block = builder.create_block();
                    let merge_block = builder.create_block();
                    builder.append_block_param(merge_block, types::I64);
                    builder.ins().brif(is_zero, merge_block, &[zero], calc_block, &[]);
                    builder.switch_to_block(calc_block);
                    builder.seal_block(calc_block);
                    let m_minus_1 = builder.ins().isub(m, one);
                    let num = builder.ins().iadd(val, m_minus_1);
                    let div = builder.ins().udiv(num, m);
                    builder.ins().jump(merge_block, &[div]);
                    builder.switch_to_block(merge_block);
                    builder.seal_block(merge_block);
                    let result = builder.block_params(merge_block)[0];
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }

                JitOp::BlendConst(mix_bits) => {
                    let a = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let b = load_slot(&mut builder, buffer_ptr, input_slots[1]);
                    let fa = builder.ins().fcvt_from_uint(types::F64, a);
                    let fb = builder.ins().fcvt_from_uint(types::F64, b);
                    let mix_f64 = f64::from_bits(*mix_bits);
                    let mix_val = builder.ins().f64const(mix_f64);
                    let one = builder.ins().f64const(1.0);
                    let one_minus_mix = builder.ins().fsub(one, mix_val);
                    let a_part = builder.ins().fmul(fa, one_minus_mix);
                    let b_part = builder.ins().fmul(fb, mix_val);
                    let sum = builder.ins().fadd(a_part, b_part);
                    let rounded = builder.ins().nearest(sum);
                    let result = builder.ins().fcvt_to_uint(types::I64, rounded);
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::LfsrStep => {
                    let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let feedback = load_slot(&mut builder, buffer_ptr, input_slots[1]);
                    let one = builder.ins().iconst(types::I64, 1);
                    let zero = builder.ins().iconst(types::I64, 0);
                    let shifted = builder.ins().ushr(val, one);
                    let lsb = builder.ins().band(val, one);
                    let is_odd = builder.ins().icmp(ir::condcodes::IntCC::NotEqual, lsb, zero);
                    let fb_mask = builder.ins().select(is_odd, feedback, zero);
                    let result = builder.ins().bxor(shifted, fb_mask);
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::PcgConst(seed, stream) => {
                    let input = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let s = builder.ins().iconst(types::I64, *seed as i64);
                    let st = builder.ins().iconst(types::I64, *stream as i64);
                    let call = builder.ins().call(pcg_func_ref, &[input, s, st]);
                    let result = builder.inst_results(call)[0];
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::PcgStreamConst(seed) => {
                    let input = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let st = load_slot(&mut builder, buffer_ptr, input_slots[1]);
                    let s = builder.ins().iconst(types::I64, *seed as i64);
                    let call = builder.ins().call(pcg_stream_func_ref, &[input, st, s]);
                    let result = builder.inst_results(call)[0];
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::NOfConst(n, m) => {
                    let input = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let n_val = builder.ins().iconst(types::I64, *n as i64);
                    let m_val = builder.ins().iconst(types::I64, *m as i64);
                    let call = builder.ins().call(n_of_func_ref, &[input, n_val, m_val]);
                    let result = builder.inst_results(call)[0];
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }

                // String & Non-scalar conversions (SRD 111)
                JitOp::U64ToString => {
                    let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let call = builder.ins().call(string_unary_refs[0], &[val]);
                    let result = builder.inst_results(call)[0];
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::I64ToString => {
                    let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let call = builder.ins().call(string_unary_refs[1], &[val]);
                    let result = builder.inst_results(call)[0];
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::F64ToString => {
                    let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let call = builder.ins().call(string_unary_refs[2], &[val]);
                    let result = builder.inst_results(call)[0];
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::BoolToString => {
                    let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let call = builder.ins().call(string_unary_refs[3], &[val]);
                    let result = builder.inst_results(call)[0];
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::StringToU64 => {
                    let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let call = builder.ins().call(string_unary_refs[4], &[val]);
                    let result = builder.inst_results(call)[0];
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::StringToI64 => {
                    let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let call = builder.ins().call(string_unary_refs[5], &[val]);
                    let result = builder.inst_results(call)[0];
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::StringToF64 => {
                    let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let call = builder.ins().call(string_unary_refs[6], &[val]);
                    let result = builder.inst_results(call)[0];
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::StringToBool => {
                    let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let call = builder.ins().call(string_unary_refs[7], &[val]);
                    let result = builder.inst_results(call)[0];
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::StrLower => {
                    let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let call = builder.ins().call(string_unary_refs[8], &[val]);
                    let result = builder.inst_results(call)[0];
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::StrUpper => {
                    let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let call = builder.ins().call(string_unary_refs[9], &[val]);
                    let result = builder.inst_results(call)[0];
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::StrTrim => {
                    let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let call = builder.ins().call(string_unary_refs[10], &[val]);
                    let result = builder.inst_results(call)[0];
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::StrLen => {
                    let val = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let call = builder.ins().call(string_unary_refs[11], &[val]);
                    let result = builder.inst_results(call)[0];
                    store_slot(&mut builder, buffer_ptr, output_slots[0], result);
                }
                JitOp::StrConcat => {
                    let a = load_slot(&mut builder, buffer_ptr, input_slots[0]);
                    let b = load_slot(&mut builder, buffer_ptr, if input_slots.len() > 1 { input_slots[1] } else { input_slots[0] });
                    let call = builder.ins().call(str_concat_ref, &[a, b]);
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

/// Cranelift vector type for a register lane index (the
/// `RegBinOp`/`RegSplat` vocabulary).
fn reg_lane_type(lane: u8) -> ir::Type {
    match lane {
        0 => types::I8X16,
        1 => types::I16X8,
        2 => types::I32X4,
        3 => types::I64X2,
        4 => types::F32X4,
        5 => types::F64X2,
        _ => unreachable!("register lane index out of range"),
    }
}

/// Load a 128-bit register value from its two consecutive slots
/// (layer-1 flattening guarantees adjacency). The buffer is only
/// 8-aligned, so the load must NOT carry the aligned flag —
/// `MemFlags::new()` permits unaligned 128-bit access.
fn load_reg128(
    builder: &mut FunctionBuilder,
    buffer_ptr: ir::Value,
    first_slot: usize,
    vt: ir::Type,
) -> ir::Value {
    let offset = (first_slot * 8) as i32;
    builder.ins().load(vt, ir::MemFlags::new(), buffer_ptr, offset)
}

/// Store a 128-bit register value into its two consecutive slots.
fn store_reg128(
    builder: &mut FunctionBuilder,
    buffer_ptr: ir::Value,
    first_slot: usize,
    value: ir::Value,
) {
    let offset = (first_slot * 8) as i32;
    builder.ins().store(ir::MemFlags::new(), value, buffer_ptr, offset);
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
        use crate::library::sampling::metashift::{Shuffle, feedback_for_size};
        use crate::ast::PolydatNode;
        // SRD-80b Phase E — `Shuffle::new` now takes `(feedback, size, min)`.
        // The bank-0 feedback for size=1000 is computed via the public helper.
        let size = 1000u64;
        let node = Shuffle::new(feedback_for_size(size), size, 0);
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

        let mut out = [crate::ast::Value::None];
        node.eval(&[crate::ast::Value::U64(42)], &mut out);
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
        use crate::library::sampling::lut::LutF64;
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
        use crate::library::sampling::icd;
        let lut = icd::dist_normal_lut(0.0, 1.0, icd::DEFAULT_RESOLUTION);
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
            (JitOp::IsPositiveCheck { name_ptr: 0, name_len: 0 }, vec![0], vec![1]),
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
        use crate::library::param_helpers::{InRange, IsPositive};
        // The classify_node entrypoint must see the new
        // predicate nodes and return the JIT-lowered op variants
        // rather than falling through to Fallback.
        let p = IsPositive::new("rate".to_string());
        assert!(matches!(classify_node(&p), JitOp::IsPositiveCheck { .. }));

        let r = InRange::new(1, 100);
        assert!(matches!(classify_node(&r), JitOp::InRangeCheck(1, 100)));
    }

    #[test]
    fn classify_leaves_other_param_helpers_on_fallback() {
        use crate::library::param_helpers::{
            Matches, Required, ThisOr,
        };
        // By design: required/this_or/matches stay on Phase-2.
        // classify_node must pick Fallback so the closure-based
        // eval runs instead of an uninitialized JIT op.
        assert!(matches!(classify_node(&Required::new("x".to_string())), JitOp::Fallback));
        assert!(matches!(classify_node(&ThisOr::new()), JitOp::Fallback));
        assert!(matches!(classify_node(&Matches::new(r"^\d+$".to_string())), JitOp::Fallback));
    }

    #[test]
    fn classify_routes_is_one_of_to_fallback() {
        // SRD-80b Phase C — `is_one_of` migrated to the macro's
        // `Const<Vec<C>>` shape, which is JIT-ineligible (the JIT
        // u64 buffer has no slot shape for a variable-length
        // captured list). The node now runs on the typed-eval
        // path. A future `compiled_u64_override` could reinstate
        // the JIT lowering if perf demands it.
        use crate::library::param_helpers::IsOneOf;
        let n = IsOneOf::new(vec![1, 3, 5, 7]);
        assert!(matches!(classify_node(&n), JitOp::Fallback));
    }

    #[test]
    fn jit_is_one_of_check_passes_allowed_values() {
        let steps = vec![
            (JitOp::IsOneOfCheck { allowed: vec![1, 2, 3, 5, 8], set_ptr: 0, set_len: 0 }, vec![0], vec![1]),
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
            (JitOp::IsOneOfCheck { allowed: vec![42], set_ptr: 0, set_len: 0 }, vec![0], vec![1]),
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
            (JitOp::IsPositiveCheck { name_ptr: 0, name_len: 0 }, vec![0], vec![1]),
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
            (JitOp::IsOneOfCheck { allowed: vec![1, 3, 5], set_ptr: 0, set_len: 0 }, vec![0], vec![1]),
        ];
        let mut output_map = HashMap::new();
        output_map.insert("out".into(), 1);
        let mut kernel = compile_jit_raw(1, 2, steps, output_map, Vec::new()).unwrap();
        let err = std::panic::catch_unwind(
            std::panic::AssertUnwindSafe(|| kernel.eval(&[2])),
        ).expect_err("disallowed value should panic");
        assert!(extract_panic_msg(err).contains("not in allowed set"));
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
            (JitOp::IsPositiveCheck { name_ptr: 0, name_len: 0 }, vec![0], vec![1]),
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
            (JitOp::IsPositiveCheck { name_ptr: 0, name_len: 0 }, vec![0], vec![1]),
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
