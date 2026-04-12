# GK Compilation Levels and Buffer Management

How the four compilation modes share a single flat buffer and how
nodes at different levels coexist in the hybrid kernel.

---

## Design Principle: Front-Load All Preparation

The setup phase of an nbrs session must complete ALL precompilation,
preconfiguration, memoization, and precomputation before the first
cycle executes. Once the work phase begins, execution should be as
optimal as possible — no lazy initialization, no first-cycle
penalties, no lock contention from concurrent setup.

This means:
- **GK kernel compilation** (P1/P2/P3/Hybrid) happens entirely at init
- **Distribution LUTs** are built at init (1000-point tables precomputed)
- **Module resolution and inlining** happens at compile time, not per-cycle
- **JIT native code generation** completes before the first `set_coordinates()`
- **Op sequence LUT** (stanza ratio pattern) is built at init
- **Adapter connections** (HTTP client, CQL session) are established at init
- **Buffer allocation** (per-fiber GkState) is done at fiber creation
- **Volatile port defaults** are pre-built once, `memcpy`'d on each reset
- **Stdlib modules** are parsed once and cached

The only per-cycle work should be:
- `set_coordinates()` — write coord slots, memcpy volatile defaults
- Node evaluation — pull-through or JIT call
- Op assembly — bind point substitution
- Adapter execution — the actual I/O
- Metrics recording — atomic increments

Nothing else. No parsing, no compilation, no file I/O, no allocation
(beyond the AssembledOp strings), no synchronization beyond the rate
limiter.

---

## The Four Levels

| Level | Name | Implementation | Per-node overhead |
|-------|------|---------------|------------------|
| **Phase 1** | Runtime | `dyn GkNode` + `Value` enum, pull-through | ~70 ns |
| **Phase 2** | Compiled closures | `Box<dyn Fn(&[u64], &mut [u64])>` + flat buffer | ~4.5 ns |
| **Phase 3** | JIT native code | Cranelift-generated machine code | ~0.2 ns |
| **Hybrid** | Per-node optimal | Mix of Phase 2 + Phase 3 in one kernel | varies |

## The Shared Buffer

All u64-only compilation modes (Phase 2, Phase 3, Hybrid) share a
single flat `Vec<u64>` buffer with this layout:

```
┌──────────────────┬──────────────────┬──────────────────┐
│  Coordinates     │  Node 0 outputs  │  Node 1 outputs  │ ...
│  [0..coord_count)│  [coord_count..) │  [slot_base[1]..)│
└──────────────────┴──────────────────┴──────────────────┘
```

- **Slots 0..coord_count** — input coordinates (written by `eval()`)
- **Remaining slots** — node output ports, allocated in topological
  order. Each node gets `meta().outputs.len()` consecutive slots
  starting at `slot_base[node_idx]`.

The slot layout is computed at assembly time and is identical across
Phase 2, Phase 3, and Hybrid. This means:

- A Phase 2 closure writes to the same buffer slot that a Phase 3
  JIT instruction reads from.
- The buffer is the universal data exchange surface — nodes don't
  need to know what level their upstream is compiled at.

## Phase 2: Closure Steps

Each step is a `Box<dyn Fn(&[u64], &mut [u64])>` closure that:
1. **Gather**: copies inputs from buffer slots into a stack array
2. **Execute**: calls the closure with the stack arrays
3. **Scatter**: copies outputs from stack array back to buffer slots

```
buffer[slot_3] ─gather→ inputs[0]
buffer[slot_7] ─gather→ inputs[1]
          closure(inputs, outputs)
outputs[0] ─scatter→ buffer[slot_12]
```

Overhead: one function pointer call + memcpy per step.

## Phase 3: JIT Native Code

The entire DAG is compiled to a single native function:
```
fn gk_kernel(coords: *const u64, buffer: *mut u64)
```

Each node becomes inline machine instructions:
```asm
; Identity: buffer[1] = buffer[0]
mov rax, [rdi]          ; load coord 0
mov [rsi + 8], rax      ; store to slot 1

; Add(100): buffer[2] = buffer[1] + 100
mov rax, [rsi + 8]      ; load slot 1
add rax, 100
mov [rsi + 16], rax     ; store to slot 2

; Hash: buffer[3] = xxh3(buffer[2])   (extern call)
mov rdi, [rsi + 16]     ; load slot 2
call jit_xxh3_hash
mov [rsi + 24], rax     ; store to slot 3
```

No gather/scatter, no function pointers. Loads and stores go directly
to buffer slots via pointer arithmetic.

For complex operations (hash), the JIT emits an `extern "C"` call
to the Rust implementation. The call overhead is a single function
pointer + ABI setup — comparable to one Phase 2 closure step.

## Hybrid: Mixed Levels

The hybrid kernel holds a sequence of steps, each either:
- A **JIT segment**: a compiled native function operating on the buffer
- A **Closure step**: a Phase 2 closure with gather/scatter

```
Step 0: JIT     (MixedRadix)     → writes slots 1,2,3 (unrolled urem/udiv)
Step 1: JIT     (Hash)           → reads slot 1, writes slot 4
Step 2: JIT     (Mod)            → reads slot 4, writes slot 5
Step 3: Closure (WeightedStrings)→ reads slot 5, writes slot 6
Step 4: JIT     (UnitInterval)   → reads slot 1, writes slot 7
Step 5: JIT     (Lerp)           → reads slot 7, writes slot 8
```

Each step reads/writes the same buffer. The JIT steps access the
buffer via the raw pointer passed at `eval()`. The closure steps
access it via gather/scatter from the kernel's `buffer` field.

### Classification

At assembly time, each node is classified by `jit::classify_node()`.
The JIT supports both u64 and f64 operations — f64 values are stored
in the u64 buffer as their bit representation, with zero-cost Cranelift
`bitcast` instructions at type boundaries.

| Node | JitOp | Cranelift IR |
|------|-------|-------------|
| Identity | Identity | load/store |
| Add/Mul/Div/Mod | ArithConst | Single integer instruction |
| Clamp (u64) | ClampConst | `umax` + `umin` |
| Hash | Hash | Extern call to xxh3 |
| Interleave | Interleave | Extern call |
| MixedRadix | MixedRadixConst | Unrolled urem/udiv chain |
| Shuffle | ShuffleConst | Extern call (LFSR loop) |
| UnitInterval | UnitInterval | `fcvt_from_uint` + `fdiv` |
| F64ToU64/Round/Floor/Ceil | F64→U64 variants | `fcvt_to_uint_sat` + rounding |
| ClampF64 | ClampF64Const | `fmax` + `fmin` |
| Lerp | LerpConst | `fsub` + `fmul` + `fadd` |
| ScaleRange | ScaleRangeConst | `fcvt_from_uint` + f64 lerp |
| Quantize | QuantizeConst | `fdiv` + `nearest` + `fmul` |
| Discretize | DiscretizeConst | f64 clamp + scale + `fcvt_to_uint_sat` |
| String/JSON/Bytes nodes | Fallback | Phase 2 closure (heap types) |
| SumN | Fallback | Variable input count |

Nodes classified as `JitOp::Fallback` get a Phase 2 closure.
All others get JIT native code.

### Buffer Invariant

The critical property: **the buffer layout is level-agnostic.** A
JIT-compiled Hash node writes to `buffer[slot_4]` using a direct
store instruction. A Phase 2 closure MixedRadix reads from
`buffer[slot_0]` (the coordinate) via gather. Neither knows or
cares what level the other is compiled at.

This is what makes hybrid compilation possible without any
inter-level marshaling.

---

## Thread Scalability

A GK kernel is a **pure function** from input coordinates to output
values. Given the same coordinates, it always produces the same
outputs regardless of which thread evaluates it. This is the
foundation of the threading model.

### GkProgram / GkState Split

The kernel is split into two structs at the type level:

```
┌───────────────────────────────────────────────────┐
│          GkProgram (Arc, immutable, shared)        │
│                                                    │
│  ┌──────────────┐  ┌──────────────────────────┐   │
│  │ Node graph   │  │ Wiring (WireSource[][])   │   │
│  │ dyn GkNode[] │  │ immutable after assembly  │   │
│  └──────────────┘  └──────────────────────────┘   │
│  ┌──────────────┐  ┌──────────────────────────┐   │
│  │ Output map   │  │ Coord names              │   │
│  │ name → slot  │  │ (read-only)              │   │
│  └──────────────┘  └──────────────────────────┘   │
│  For P2/P3: JIT fn ptr, closure vec (read-only)   │
└───────────────────────────────────────────────────┘

┌───────────────────────────────────────────────────┐
│  GkState (per-fiber, mutable, private, no sharing) │
│                                                    │
│  Fiber 0: [ buffers, generation, coords ]          │
│  Fiber 1: [ buffers, generation, coords ]          │
│  Fiber 2: [ buffers, generation, coords ]          │
│  ...                                               │
│  Fiber N: [ buffers, generation, coords ]          │
└───────────────────────────────────────────────────┘
```

`GkProgram` is `Send + Sync` and shared via `Arc`. It contains
the compiled node instances, wiring, and output map — all immutable
after assembly.

`GkState` is per-fiber mutable state: value buffers, generation
counter, and current coordinates. Each fiber creates its own state
via `program.create_state()`. No sharing, no synchronization, no
blocking.

**Isolation scope:** Calling `state.set_coordinates()` begins a new
isolation scope. The generation counter advances, implicitly
invalidating all cached node outputs. Volatile ports (SRD 28) reset
to their defaults. No other fiber can interact with a given state.

**Evaluation:** `state.pull(program, "name")` borrows the program
immutably (`&GkProgram`) and the state mutably (`&mut GkState`).
The program provides the code, the state provides the scratch space.
No locks, no atomics, no cross-fiber interference.

This gives:

- **Zero contention**: No locks, no atomics on the hot path.
  Each thread writes exclusively to its own buffer.
- **Minimal memory**: One `Vec<u64>` per thread. A kernel with
  100 nodes and 3 outputs each needs ~800 bytes per thread.
- **Cache locality**: Each thread's buffer fits in L1 cache.
  No false sharing — buffers are independent allocations.

### Per-Thread Buffer Strategies

Three strategies, chosen by kernel size:

**1. Heap-allocated Vec (default)**

```rust
struct ThreadKernel {
    shared: Arc<KernelCode>,  // JIT fn ptr, closures, layout
    buffer: Vec<u64>,         // per-thread scratch
}
```

Each executor thread allocates its own buffer once at startup.
Reused across all cycle evaluations. Cost: one allocation per
thread for the lifetime of the activity.

**2. Stack-allocated buffer (small kernels)**

For kernels with fewer than ~64 slots (512 bytes), the buffer
can live on the stack frame:

```rust
fn eval_small(shared: &KernelCode, coords: &[u64]) -> u64 {
    let mut buffer = [0u64; 64];  // stack, no allocation
    buffer[..coords.len()].copy_from_slice(coords);
    unsafe { (shared.jit_fn)(buffer.as_ptr(), buffer.as_mut_ptr()); }
    buffer[shared.output_slot]
}
```

Zero heap allocation per evaluation. The buffer lives and dies
with the stack frame.

**3. Thread-local reuse**

```rust
thread_local! {
    static BUFFER: RefCell<Vec<u64>> = RefCell::new(Vec::new());
}
```

Lazily allocated, automatically sized on first use, reused
across all subsequent evaluations on that thread. Avoids
passing the buffer through function signatures.

### Scaling Properties

Because the only per-thread resource is a small buffer, GK kernel
evaluation scales linearly with thread count:

- **No shared mutable state**: The kernel code is immutable.
  The buffer is thread-private. No synchronization needed.
- **No false sharing**: Each thread's buffer is a separate heap
  allocation with its own cache line alignment.
- **Uniform work**: Each cycle evaluation does the same amount of
  work regardless of thread ID or cycle value. No thread contention,
  no lock convoys, no hot spots.
- **Deterministic regardless of scheduling**: Thread 0 evaluating
  cycle 42 and thread 7 evaluating cycle 42 produce identical results.
  The output depends only on the input coordinates, not on which
  thread runs the evaluation or in what order.

### Interaction with PCG RNG Nodes

PCG nodes use the pure-function seek model (SRD 25), so they require
no shared RNG state between threads. Each thread independently seeks
to the target position:

```
Thread 0: pcg_seek(seed, inc, cycle=1000) → value_1000
Thread 1: pcg_seek(seed, inc, cycle=1001) → value_1001
Thread 2: pcg_seek(seed, inc, cycle=1002) → value_1002
```

No coordination, no state transfer, no ordering dependency.

**Sequential access optimization**: When cycles are assigned to
threads in monotonically increasing batches (the common case with
`CycleSource`), each thread can memoize its last PCG state and
step forward by one instead of seeking from scratch. This is a
per-thread-local optimization that doesn't affect the pure-function
contract:

```rust
struct PcgMemo {
    last_position: u64,
    last_state: u64,
}

fn eval_pcg(&mut self, position: u64, seed: u64, inc: u64) -> u64 {
    let state = if position == self.last_position + 1 {
        // O(1) step
        self.last_state.wrapping_mul(MULT).wrapping_add(inc)
    } else {
        // O(log N) seek
        pcg_seek_state(seed, inc, position)
    };
    self.last_position = position;
    self.last_state = state;
    pcg_output(state)
}
```

This drops PCG cost from ~100ns (full seek) to ~2ns (single step)
for sequential cycle assignment, while remaining correct for any
access pattern (out-of-order, skipped cycles, replayed ranges).

### What Is NOT Per-Thread

- **Op templates**: Shared via `Arc<[ParsedOp]>`, read-only.
- **Rate limiters**: Shared `Arc<RateLimiter>`, internally uses
  atomics for token management. The only shared mutable state
  in the hot path, and it's designed for high-contention access.
- **Metrics instruments**: Shared `Arc<ActivityMetrics>`. Timers
  and counters use internal atomics. Designed for concurrent writes.
- **Error router**: Shared `Arc<ErrorRouter>`, read-only after parse.

The GK kernel evaluation itself — the computationally expensive part
— is entirely contention-free.

---

## Benchmark Summary

All four levels, identity chain of 16 nodes:

| Level | Time | Speedup vs P1 |
|-------|-----:|:-------------:|
| Phase 1 (runtime) | 1,103 ns | 1x |
| Phase 2 (closures) | 73 ns | 15x |
| Hybrid (per-node) | 75 ns | 15x |
| Phase 3 (pure JIT) | 3.8 ns | 290x |

For mixed graphs (e.g., MixedRadix + Hash + Mod), the hybrid is
the only mode that works — pure Phase 3 would reject the graph,
and pure Phase 2 doesn't benefit from JIT on the JIT-able nodes.

---

## Future: Segment Batching

Currently the hybrid compiles each JIT-able node as its own segment
(one Cranelift function per node). The next optimization: batch
adjacent JIT-able nodes into a single native function, eliminating
the per-segment call overhead. This would push hybrid performance
closer to pure Phase 3 for runs of consecutive JIT-able nodes.
