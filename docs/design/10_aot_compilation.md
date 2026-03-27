# AOT Compilation — Design and Trade-offs

The GK supports two evaluation modes: a Phase 1 runtime interpreter and
a Phase 2 ahead-of-time (AOT) compiled kernel. This document describes
the AOT path, its mechanics, what it enables, and where it falls short.

---

## How It Works

### Phase 1: Runtime Interpreter (`GkKernel`)

The default evaluation mode. The assembled DAG is stored as a graph of
`dyn GkNode` trait objects with a `Value` enum carrying data between
nodes.

**Per-cycle cost structure:**
- Virtual dispatch per node (~1 indirect call)
- `Value` enum wrapping and unwrapping per port
- `Value::clone()` for each wire transfer (heap allocation for String)
- HashMap lookup per output pull
- Generation counter check per node (memoization)
- Recursive pull-through traversal

**Measured overhead:** ~36-43 ns per node in a chain, ~69 ns baseline.

### Phase 2: AOT Compiled (`CompiledKernel`)

When all nodes in the DAG are u64-only and each provides a
`compiled_u64()` implementation, the assembly phase produces a compiled
kernel instead.

**What changes:**
- The `Value` enum is eliminated. All wires are `u64` in a flat buffer.
- Trait object dispatch is eliminated. Each node becomes a captured
  closure (`Box<dyn Fn(&[u64], &mut [u64])>`).
- Pull-through memoization is eliminated. Evaluation is eager — the
  entire DAG runs in topological order on every `eval()` call.
- HashMap output lookup is eliminated. Outputs are resolved to buffer
  slot indices at assembly time; `get_slot()` is a direct array index.
- Recursive traversal is eliminated. Steps execute in a flat loop.

**Per-cycle cost structure:**
- One closure call per node (indirect but non-virtual)
- Gather: copy u64s from buffer slots into a small stack array
- Execute: the closure reads/writes u64 slices
- Scatter: copy u64s from stack array back to buffer slots
- No allocation, no enum, no HashMap, no recursion

**Measured overhead:** ~3-4 ns per node in a chain, ~8 ns baseline.
**Speedup over Phase 1:** 8-15x depending on topology.

---

## What Enables This

### Single-Responsibility Core Functions

The AOT path is practical because core functions are atomic and
single-purpose. A hash node hashes. A mod node takes a modulus. Neither
combines the two. This means:

- **Composition has near-zero cost.** In the compiled kernel, `hash →
  mod` is two closure calls with a buffer read/write between them —
  roughly 7 ns total. A fused "hash_range" node would save perhaps 2 ns
  at the cost of a larger, less composable function library.

- **Library kernels are free.** A user who defines `hash_range.gk` as
  `h := hash(input); output := mod(h, max)` pays no penalty compared to
  a built-in fused node. The AOT compiler sees through the composition.

- **The function library stays small.** ~35 core nodes replace ~522 Java
  classes because composition is cheap, not because we've lost
  expressiveness.

### u64-Only Fast Path

The AOT path requires all ports to be u64. This covers the
number-theoretic core of most workloads: hashing, arithmetic,
decomposition, distribution sampling, alias table lookup. The moment a
String, f64, or other type appears in the DAG, the kernel falls back
to Phase 1.

In practice, the u64 core does the heavy lifting and produces indices
or codes. Type-crossing (u64 → String) happens at the output boundary,
which is typically outside the hot inner loop.

---

## Trade-offs

### What's Better in Phase 2

| Aspect | Phase 1 | Phase 2 |
|--------|---------|---------|
| Per-node overhead | ~36-43 ns | ~3-4 ns |
| Baseline overhead | ~69 ns | ~8 ns |
| Memory per eval | Allocates `Value` clones | Zero allocation |
| Output access | HashMap lookup | Direct index |
| Evaluation | Lazy (pull-through) | Eager (full DAG) |

### What's Better in Phase 1

| Aspect | Phase 1 | Phase 2 |
|--------|---------|---------|
| Type support | Any `Value` variant | u64 only |
| Lazy evaluation | Only computes pulled outputs | Evaluates everything |
| String outputs | Native | Not supported |
| f64 outputs | Native | Not supported |
| Mixed-type DAGs | Works | Falls back to Phase 1 |

### The Lazy vs. Eager Trade-off

Phase 1 is pull-through: if an op template only references 3 of 10
output variates, only the DAG paths feeding those 3 are evaluated.

Phase 2 is eager: every node runs on every `eval()`, even if some
outputs are unused. For small-to-medium DAGs (typical workloads), this
is still faster because the per-node cost is so much lower. For very
large DAGs with many unused branches, Phase 1's laziness could
theoretically win — but this is an unusual case.

---

## Caveats

### 1. Closure Dispatch is Not Fully Static

Phase 2 closures are `Box<dyn Fn(...)>` — they're heap-allocated and
called through a function pointer. This is cheaper than virtual dispatch
(no vtable indirection, no `self` parameter) but not as fast as a
fully monomorphized inline function. A true Phase 3 (cranelift JIT or
proc-macro codegen) could eliminate this last indirection.

### 2. Gather/Scatter Overhead

Each step gathers inputs from the flat buffer into a stack array, calls
the closure, then scatters outputs back. For nodes with many ports,
this is a memcpy cost (~1 ns per u64). For typical 1-2 input nodes,
it's negligible.

### 3. No Dead Branch Elimination at Runtime

Phase 2 evaluates every node, every cycle. If the workload only pulls
a subset of outputs, the unused branches still execute. An optimizer
pass at assembly time could prune unreachable nodes, but this is not
yet implemented.

### 4. Fall-Back is All-or-Nothing

If any single node in the DAG lacks a `compiled_u64()` implementation,
the entire kernel falls back to Phase 1. There is no hybrid mode where
some nodes run compiled and others run interpreted. This is a deliberate
simplicity choice — hybrid evaluation would add complexity for marginal
benefit.

### 5. Assembly-Time Parameter Capture

Compiled closures capture their parameters (e.g., the modulus for Mod,
the radixes for MixedRadix) at assembly time via `move`. These values
are baked into the closure and cannot be changed without reassembling
the kernel. This is by design — the GK is a static kernel — but it
means runtime parameter changes require a full recompile.

---

## Features

### Auto-Promotion

The `try_compile()` API attempts Phase 2 and returns the compiled kernel
on success. On failure (any non-u64 node), it returns a working Phase 1
kernel. The caller doesn't need to know which mode is active:

```rust
let kernel = assembler.try_compile();
match kernel {
    Ok(compiled) => { /* fast path */ },
    Err(runtime) => { /* still works, just slower */ },
}
```

### Pre-Resolved Output Slots

In Phase 2, output names are resolved to buffer slot indices at assembly
time. The `resolve_output("name")` method returns a `usize` that can be
used with `get_slot(idx)` for zero-overhead output access in the hot
loop — no HashMap lookup per cycle.

### Compiled Node Contract

Any `GkNode` implementation can opt into Phase 2 by implementing
`compiled_u64()`:

```rust
fn compiled_u64(&self) -> Option<CompiledU64Op> {
    let param = self.param; // capture assembly-time state
    Some(Box::new(move |inputs, outputs| {
        outputs[0] = inputs[0].wrapping_add(param);
    }))
}
```

Return `None` to indicate the node cannot be compiled (non-u64 ports,
external I/O dependencies, etc.). The default trait implementation
returns `None`.

---

## Future Directions

### Phase 3: True Code Generation

The current Phase 2 still has one level of indirection per node (the
closure call). A Phase 3 could eliminate this by generating a single
monomorphized function for the entire DAG:

```rust
fn eval_kernel(coords: &[u64], buf: &mut [u64]) {
    buf[1] = xxh3_64(&coords[0].to_le_bytes());  // hash
    buf[2] = buf[1] % 10000;                       // mod
    buf[3] = buf[1] % 64;                          // mod
    // ... all nodes inlined
}
```

Approaches:
- **Proc macro** at Rust compile time (requires the DAG to be known
  at compile time — works for built-in workloads)
- **Cranelift** JIT at assembly time (runtime code generation, adds
  a heavy dependency)
- **dynasm** for hand-crafted x86-64 (maximum performance, maximum
  complexity)

The Phase 2 architecture is designed to be forward-compatible with
Phase 3: the flat buffer layout and topological step ordering are
exactly what a code generator would emit.

### Dead Branch Pruning

An assembly-time optimization pass that removes nodes whose outputs
are never referenced (directly or transitively) by any named output
variate. This would make Phase 2's eager evaluation equivalent to
Phase 1's lazy evaluation for the common case.

### Hybrid Evaluation

A mixed mode where the u64 core of the DAG runs compiled and the
type-crossing boundary nodes run interpreted. This would allow
Phase 2 speedups for the hot inner loop while still supporting
String/f64 outputs. Complexity cost is significant — deferred unless
benchmarks show the boundary is a bottleneck.
