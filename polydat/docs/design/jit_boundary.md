# JIT Boundary

The polydat-internal contract for the Phase-3 native kernel.
This doc specifies *how* the Cranelift-generated machine code
plugs into the rest of the runtime — the call boundary
between Rust and the native function, what happens when that
code raises a predicate violation, and how invalidation and
extern resolution cross the boundary.

For the engine-selection side (*which* compilation level the
compiler picks for a given subgraph), see
[graph_compiler.md §6 (ordered composition)](graph_compiler.md).
The clean-flag and memoization model the JIT preserves across
the boundary is in [runtime_model.md](runtime_model.md) (R-axioms).

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
in `polydat/src/jit/codegen.rs` and are registered with
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

`polydat/src/jit/codegen.rs` carries unit coverage:

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

---

## SIMD compute kernels (alignment §8.2)

`compile/jit/simd.rs` compiles four f32-lane kernels once per
process through the same cranelift engine, using real cranelift
SIMD types (`F32X4`): `dot_f32`, `l2sq_f32`, `add_f32`,
`scale_f32`. Each processes the slice body in 128-bit chunks
(unaligned loads — `SliceArc<f32>` data is only 4-aligned) with a
scalar tail loop, and reducing kernels finish with an
`extractlane` horizontal sum. Consumers are the `vec_*` nodes in
`library/vector_math.rs`, which fall back to scalar Rust loops
when the `jit` feature is off or host-ISA construction fails.
This is the extern-call integration pattern (like hash/trig):
vector *values* do not yet cross the `fn(coords, buffer)` ABI —
the (ptr, len) slot-pair design for that is
`type_system_alignment.md` §8.3 phases 5–6.

SIMD accumulation reassociates float addition, so reduced results
may differ from the scalar reference in the final ulps; the
equivalence tests compare with relative tolerance.

## Scalar buffer conventions

- `u64` rides as-is; `i64` is `as u64` (bit-identical — cranelift
  integers are sign-agnostic, signedness lives in the ops).
- `f64` rides via `to_bits()`; cranelift `bitcast` converts for
  free.
- `bool` rides as 0/1 in a u64 slot; codegen uses `types::I8`
  loads and constants for flag values — the only non-{I64, F64}
  scalar type the kernel codegen emits.
- Narrow widths (u8/i8/u16/i16/u32/i32/f32/f16) ride zero-/sign-
  extended or bit-stuffed in the u64 slot per the static
  `PortType`. The `#[polydat_node]` macro's buffer tokens are
  width-aware (its internal `JitType` carries one variant per
  width), so narrow-typed nodes get `compiled_u64` closures whose
  casts mirror the Wire storage conventions exactly — pinned by
  the P1↔P2 equivalence tests in `polydat_node_macro.rs`.
- 128-bit integers do not ride at all (interpreter-only) until a
  two-slot protocol exists.

---

## Slot-state axioms (S1–S10) — RATIFIED 2026-06-12

The normative contract for compiled-kernel buffer state under the
§8.4 vector substrate (`type_system_alignment.md`). These are
axioms in the SYSREF sense: load-bearing, cited by SAFETY
comments, and enforced by tripwires rather than comments.

**S1 — Slot color is static, total, and three-valued.** Every
`PortType` maps at kernel-build time to exactly one color:
`Imm1` (one slot, immediate value), `Imm2` (two slots, immediate
limbs — register words, u128/i128), `Ref2` (two slots,
`(ptr, len)` reference — heap slices). Width derives from color.
No runtime tags; no per-value color. Immediate slots never
contain an address, so buffer + port table is a complete state
description for everything except Ref data. *Chokepoint:
`PortType::slot_color()`; `slot_width()` derives from it.*

**S2 — Pointer containment.** A Ref pair is meaningful only
inside the engine's gather→op→scatter path. Raw readers (`get`,
`get_slot`, `eval_for_slot`) PANIC on Ref-colored slots; external
access goes through borrow-checked scratch accessors
(`read_vec_f32(&self, slot) -> &[f32]`, lifetime tied to `&self`
so holding a slice across the next `eval(&mut self)` is a compile
error) or owned copy-out. A walled-off API in the established
sense.

**S3 — Single-writer scratch.** Each scratch entry is owned by
exactly one (step, output port); only the owning op mutates it,
and every execution republishes the `(ptr, len)` pair within that
execution. The engine passes each op only its own scratch range —
ownership by sub-slice, not discipline.

**S4 — Publish-before-read.** Steps execute sequentially in
topological order within an eval pass. A Ref pair's validity
interval is [owning op's scatter completes, owning op's next
invocation begins); every consumer gather occurs inside the
producer's current interval.

**S5 — Skip coherence.** If a producer executes in a pass, every
transitive consumer executes later in the same pass. Mechanical
oracle: the Raw (never-skip) engine and the Push/Pull/PushPull
(skip) engines must produce identical outputs for arbitrary
input-change sequences (`tests/slot_state_axioms.rs`).

**S6 — Sequential-by-axiom; parallelism is a redesign gate.**
Kernel state (buffer + scratch) is single-threaded by
construction: one state per thread; cross-thread sharing only via
`Arc<PolydatProgram>`; values cross threads only as owned copies.
Intra-kernel parallel step execution is FORBIDDEN until S4 is
replaced with a new ordering proof (epochs/generations).

**S7 — One static dereference.** Ref access compiles to exactly
one pointer dereference; color/offset resolution completes at
kernel build. Index tables, arena handles, runtime color
dispatch, and any second hop are forbidden — in interpreter
closures and in JIT-generated code alike (a segment loads the
pointer from its slot and passes it).

**S8 — P1 is the semantic oracle.** Typed eval defines meaning;
every compiled tier must be bit-identical to it (cross-lane float
reductions per their declared fixed-shape contracts). No node or
op shape lands without a P1↔P2(↔P3) equivalence test.

**S9 — Deterministic runtime validation.** (a) In debug/test
builds, after every eval pass the engine asserts every
scratch-backed Ref pair equals its owning entry's current
`(as_ptr(), len())` — forgot-to-republish / wrong-slot /
dangling failures name the slot deterministically. (b) The
slice-transport tests run under Miri (no-jit configuration —
Miri cannot execute JIT'd native code) to adjudicate the formal
aliasing validity of the `from_raw_parts` pattern. Lane command:

```sh
MIRIFLAGS=-Zmiri-ignore-leaks cargo +nightly miri test \
    -p polydat --no-default-features --test slot_state_axioms
```

ADJUDICATED 2026-06-12: Stacked Borrows accepts the pattern (the
S5 oracle passes under Miri across all five engines). One caveat,
recorded rather than hidden: Miri warns on the integer-to-pointer
casts (inherent to a u64-slot transport — provenance is
necessarily reconstructed, so the check runs under permissive
int-ptr semantics rather than strict provenance). The
`-Zmiri-ignore-leaks` flag masks a known ~200-byte-per-compile
leak in `registry::lookup` (tracked for a follow-up fix).

**S10 — Unsafe is enumerable, annotated, and tripwired.** Every
Ref-deref `unsafe` lives in macro-generated `compiled_slot`
bodies (emitted from `polydat-derive`) or named engine accessor
sites; each SAFETY comment cites the axioms it relies on (S3,
S4). A CI tripwire fails when `from_raw_parts` appears outside
the allowlisted files.

**P3 corollary.** Pure-P3 kernels contain no Ref slots by
construction (slice-bearing nodes classify `Fallback`, and
`build_jit_layout` rejects Fallback); the JIT builders enforce
this defensively. Hybrid kernels carry Ref slots only in closure
steps.

**Forwarding caveat.** The §8.4 "pass-through forwards its input
pair verbatim" optimization is NOT yet implemented; when it is,
forwarded ports must be exempted from S9(a)'s validator mapping
explicitly — the validator currently assumes every Ref output is
scratch-backed.
