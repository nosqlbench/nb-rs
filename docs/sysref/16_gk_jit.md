# 16b: GK JIT Wiring

Companion to [SRD 16 GK Engines](16_gk_engines.md). That document
covers *which* compilation level the compiler picks; this one
covers *how* the Phase-3 native kernel plugs into the rest of
the runtime — specifically the call boundary between Rust code
and Cranelift-generated machine code, and what happens when that
code raises a predicate violation.

---

## Call boundary overview

A Phase-3 kernel compiles to a single native function:

```text
fn(coords: *const u64, buffer: *mut u64)        // raw
fn(coords: *const u64, buffer: *mut u64,
   clean:  *mut u8)                             // provenance variant
```

The Rust wrapper owns a `Vec<u64>` buffer and calls the function
pointer each cycle. Four kernel variants dispatch the same way
but apply different optimizations:

| Kernel | Optimization |
|---|---|
| `JitKernelRaw` | Runs every node unconditionally |
| `JitKernelPush` | Per-node dirty tracking (push-side step skip) |
| `JitKernelPull` | Cone guard for per-slot eval (pull-side skip) |
| `JitKernelPushPull` | Both |

The `HybridKernelRaw` / `HybridKernelPull` / `HybridKernelPushPull`
variants mix Phase-3 JIT segments with Phase-2 closure steps
inside one kernel, using the same buffer.

All of these share one entry rule: **every call into native code
goes through `codegen::invoke_with_catch`.** There is no direct
`(code_fn)(...)` invocation anywhere in the library; grep for
that pattern finds only the wrapper itself.

---

## Why predicate violations are a problem at this boundary

SRD 12 §"Parameter resolution and validation" lists three JIT-
lowered predicates that can fail at cycle time: `is_positive`,
`in_range`, `is_one_of`. When they fail the JIT emits a call to
an extern helper (`jit_is_positive_fail`, `jit_in_range_fail`,
`jit_is_one_of_fail`) that must report the violation and stop
the current evaluation.

The obvious shape — `panic!` from the extern helper and catch
it upstream — does not work with Cranelift-generated frames:

- Cranelift emits DWARF `.eh_frame` entries when
  `unwind_info=true`, and registers them with the platform
  unwinder via `JITModule::finalize_definitions()`.
- Those frames do **not** carry Rust's panic personality
  routine. The libunwind walker can traverse them, but
  `_Unwind_RaiseException` never finds a catch block and the
  panic runtime aborts with "failed to initiate panic, error 5
  (`_URC_END_OF_STACK`)".
- Switching the extern to `extern "C-unwind"` alone doesn't
  fix this — the issue is the missing personality, not the
  ABI flag.

Teaching Cranelift to emit `.gcc_except_table` entries referencing
`rust_eh_personality`, plus re-registering frames via a personality
shim, is an integration project that isn't in this crate's scope.

---

## The setjmp / longjmp workaround

Rather than unwinding *through* the JIT frame, we jump *past* it.

### Flow

```text
        ┌─────────── Rust caller (eval) ───────────┐
        │                                          │
        │  invoke_with_catch(|| {                  │
        │      _setjmp(&jmp_buf)         ← record  │
        │      (code_fn)(buf, mut_buf)   ← JIT ───┐│
        │  })                            return   ││
        │                                          │
        └──────────────────────────────────────────┘
                                                  ││
                                              JIT frame (machine code,
                                               no Rust personality)
                                                  ││
                                                  ▼▼
                                         extern "C" fn
                                         jit_is_positive_fail(v)
                                            │
                                            ├─ stash message in TLS
                                            └─ _longjmp(&jmp_buf, 1)
                                                  ▲
                                            control jumps HERE
                                                  │
        ┌─────────── Rust caller (eval) ───────────┐
        │  _setjmp returned non-zero:              │
        │  read the stashed message                │
        │  panic!(message)   ← Rust-land panic     │
        └──────────────────────────────────────────┘
                     │
          ordinary Rust unwind, proper personality
                     ▼
          std::panic::catch_unwind in the caller
```

The longjmp skips the JIT frame entirely — no unwinding through
it, no personality lookup, no catch-block walk. Control returns
into Rust land where a normal `panic!` propagates through
Rust-personality FDEs the way any other panic would.

### Safety

- **Resources in the JIT frame.** Cranelift-generated code is
  pure machine code with no Drop obligations, no heap-owning
  values, no locks to release. Skipping it is safe.
- **Resources in the extern fail functions.** Each helper
  only `format!`s a message, stashes it in a TLS slot, and
  calls `_longjmp`. The `String` allocated by `format!` is
  moved into the TLS slot *before* the longjmp, so nothing is
  dropped implicitly across the jump.
- **Thread locality.** The jmp_buf pointer and the message
  slot are both `thread_local!`. Concurrent kernels on
  different tokio worker threads don't share state.
- **Nesting.** `invoke_with_catch` saves the outer
  `JIT_JMP_BUF` slot in a stack-local variable, installs its
  own buffer, and restores the outer on every exit path. An
  inner longjmp jumps to the innermost buffer; the outer
  regains the slot when the inner frame unwinds.
- **SIMD register state.** `_setjmp` on glibc preserves only
  the core register set that `longjmp` restores. The Rust
  wrapper doesn't keep live SIMD state across the JIT call,
  so this is fine. Workloads that wanted to keep live SIMD
  data across a predicate violation would have a larger
  problem.

### Platform-portable jmp_buf shim

`libc` doesn't expose `jmp_buf` / `setjmp` / `longjmp` (they're
generally considered unsafe to reach from Rust). We declare
them directly:

```rust
#[repr(C, align(16))]
struct JitJmpBuf([u8; 512]);          // 512 > glibc (~200) > macOS (~192)

unsafe extern "C" {
    fn _setjmp(env: *mut JitJmpBuf) -> i32;
    fn _longjmp(env: *mut JitJmpBuf, val: i32) -> !;
}
```

We link against `_setjmp` / `_longjmp` (rather than plain
`setjmp` / `longjmp`) because the plain variants are glibc
macros that expand to `__sigsetjmp(env, 0)` — saving the
signal mask, which we don't need. `_setjmp` saves registers
only and is faster.

---

## `invoke_with_catch` contract

```rust
pub(crate) fn invoke_with_catch<F: FnOnce()>(f: F)
```

- Installs a stack-local jmp_buf into the thread-local
  `JIT_JMP_BUF` slot.
- Runs `f()`.
- If `f()` returns normally, a [`JmpBufGuard`] restores the
  outer slot on drop.
- If `f()` triggers a JIT predicate violation, the extern
  helper `_longjmp`s back; the wrapper reads the TLS message
  and raises `panic!`. The guard still runs (on the panic
  unwind path inside the wrapper frame) and restores the
  outer slot.
- If `f()` panics for a non-JIT reason (a bug in a non-JIT
  sub-path; a panic from a closure step in a hybrid kernel),
  the panic unwinds through the wrapper's frame. The guard's
  `Drop` restores the outer slot before the unwind
  continues. Subsequent `invoke_with_catch` calls see a clean
  sentinel. This is covered by the test
  `invoke_with_catch_restores_slot_after_foreign_panic`.

### RAII guard

```rust
struct JmpBufGuard { prev: Option<*mut JitJmpBuf> }

impl Drop for JmpBufGuard {
    fn drop(&mut self) {
        JIT_JMP_BUF.with(|b| b.set(self.prev));
    }
}
```

The guard is the only thing that writes the previous slot
back. Every exit path from `invoke_with_catch` — return,
setjmp-return-then-panic, or panic-through-wrapper — runs
through `Drop`.

---

## Where the wrapper is applied

Every `eval` / `eval_for_slot` on every JIT and Hybrid kernel
variant uses the wrapper:

| Caller | Uses |
|---|---|
| `JitKernelRaw::eval` | ✓ |
| `JitKernelPush::eval` | ✓ |
| `JitKernelPull::eval`, `eval_for_slot` | ✓ (both invocation sites) |
| `JitKernelPushPull::eval`, `eval_for_slot` | ✓ (both) |
| `HybridCore::eval_all_hybrid_steps` (used by `HybridKernelRaw::eval`, `HybridKernelPull::eval`, `HybridKernelPull::eval_for_slot`'s dirty path) | ✓ (each per-step JIT segment) |
| `HybridKernelPushPull::eval`, `eval_for_slot` | ✓ (each per-step JIT segment) |

The `JitKernelRaw::into_parts` accessor remains a raw-pointer
export for hybrid-kernel integration. Callers of `into_parts`
are expected to either build a hybrid kernel (which wraps every
call) or install their own wrapper before invoking the pointer;
calling the pointer directly without either would abort on
violation via the no-sentinel fallback.

---

## No-sentinel fallback

`jit_violation_longjmp` checks the thread-local for an installed
jmp_buf before attempting to jump:

```rust
fn jit_violation_longjmp(msg: String) -> ! {
    JIT_VIOLATION_MSG.with(|m| *m.borrow_mut() = Some(msg.clone()));
    match JIT_JMP_BUF.with(|b| b.get()) {
        Some(ptr) => unsafe { _longjmp(ptr, 1) },
        None => {
            // Raw code_fn invoked outside a wrapper.
            eprintln!("{msg}");
            std::process::abort();
        }
    }
}
```

This is the last-line defense. In practice the only way to
reach it is to call a JIT function pointer without going through
one of the wrapped kernels (e.g. a test that retrieves the raw
pointer via `into_parts` and invokes it directly). The fallback
prints the message and aborts — the same behavior the wrapper
replaces in the normal path, but without the catch-unwind
integration.

---

## Extern-helper table

Each predicate has one dedicated fail helper. The helpers live
in `nb-variates/src/jit/codegen.rs` and are registered with
Cranelift's JIT symbol table so the emitted native code can
call them.

| Extern | Arity | Called from |
|---|---|---|
| `jit_is_positive_fail` | `(u64) -> u64` | `JitOp::IsPositiveCheck` |
| `jit_in_range_fail` | `(u64, u64, u64) -> u64` (value, lo, hi) | `JitOp::InRangeCheck` |
| `jit_is_one_of_fail` | `(u64) -> u64` | `JitOp::IsOneOfCheck` |

The `u64` return type matches the extern-function ABI the JIT
uses; since each helper ends in `_longjmp` (which is `-> !`),
the return is unreachable.

The message formatting happens at the Rust side, inside the
helper:

```rust
extern "C" fn jit_in_range_fail(value: u64, lo: u64, hi: u64) -> u64 {
    jit_violation_longjmp(
        format!("in_range: value {value} outside [{lo}, {hi}]"),
    );
}
```

---

## Operator-visible semantics

From the outside looking in, a predicate violation in JIT code
behaves exactly like a predicate violation in Phase-1 or Phase-2:

- `#[should_panic(expected = "must be > 0")]` on the caller
  works.
- `std::panic::catch_unwind` catches and returns `Err`.
- The panic message carries the violating value (and, for
  `in_range`, the configured bounds).
- The workload can continue — the kernel survives catches;
  the per-cycle buffer is left partially written for the
  failing step but subsequent evals overwrite cleanly.

The only observable difference between the JIT path and the
interpreter/closure paths is the message body. The
interpreter/closure paths carry the control-name
identifier (e.g. `"is_positive(rate)"`) because they have the
full node state at panic time; the JIT path drops the
identifier because threading it through the extern ABI would
bloat the call. Workloads that need maximum diagnostic detail
can run at Phase-2 during troubleshooting.

---

## Tests

`nb-variates/src/jit/codegen.rs` carries unit coverage:

- Per-predicate happy path: value passes through.
- Per-predicate catchable-panic path: violation fires and
  `catch_unwind` returns `Err` with the expected message.
- `jit_kernel_survives_multiple_violations` — repeated
  caught violations followed by a happy-path eval all work
  on the same kernel instance, proving no state leaks
  across longjmp.
- `invoke_with_catch_restores_slot_after_foreign_panic` —
  a non-JIT panic inside the closure still restores the
  TLS slot, so a subsequent legitimate JIT violation is
  caught cleanly. This is the specific regression `JmpBufGuard`
  protects against.

---

## When to replace this with Cranelift unwind personality

The setjmp/longjmp approach is a working compromise, not the
long-term architecture. The cleaner answer is for Cranelift to
emit `.gcc_except_table` sections referencing Rust's
`rust_eh_personality` and register them alongside the `.eh_frame`
FDEs. At that point:

- The extern helpers become `extern "C-unwind"` and `panic!`
  directly; no TLS buffer, no longjmp.
- Every `eval` method drops the wrapper and calls the JIT
  code directly.
- `catch_unwind` works without the Rust-side trampoline.

Moving to that model requires upstream Cranelift work (or a
fork-and-patch). Until that lands this module stays as-is.
