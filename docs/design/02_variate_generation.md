# Variate Generation Subsystem

## Overview

The variate generation stage for nb-rs is a generation kernel (GK) that transforms input
coordinates into output variates. Unlike the Java virtdata system (linear
function pipelines), this is a directed acyclic graph — multiple inputs can
feed through shared intermediate nodes to produce multiple outputs.

## Core Model

```
  input coordinates: named u64 n-tuple
  ("cycle", "thread", "op")  ←  naming tuple
  (     42,        3,    7)  ←  coordinate tuple
            │
            ▼
      ┌───────────┐
      │ generation kernel (GK) │
      └───────────┘
            │
            ▼
     output variates
```

**Input coordinates** — an n-tuple of `u64` values, where each position has a
symbolic name. The coordinate tuple and naming tuple are paired: the naming
tuple gives each position a user-facing identifier (e.g., `"cycle"`,
`"thread"`, `"partition"`), and the coordinate tuple provides the
corresponding `u64` value. Together they form a named coordinate space that
the DAG operates over.

The naming tuple represents the user's view of a domain space. Each position
is a symbolic name for a real-world concept — a canonical identifier for an
instance of an entity or value in the user's domain. For example, in a
multi-tenant time-series workload, the naming tuple might be
`("tenant", "device", "timestamp")`, where each name identifies a dimension
of the domain that the user thinks in terms of.

This means:
- The dimensionality of the input is variable (not hardcoded to just "cycle").
- Each coordinate position is a named field, making the input self-describing.
- The names are domain-meaningful, not engine-internal — they reflect the
  user's mental model of their data, not the mechanics of the test harness.
- DAG nodes can reference input coordinates by name rather than by position.
- The same DAG definition can be applied to different coordinate spaces by
  remapping names.

### Cycle-to-Coordinate Mapping

The engine still has a concept of cycles, but the mapping from cycle to
coordinate tuple is flexible and user-defined. This is a deliberate
improvement over the Java model, where the unary functional flow
(cycle → value) forced users into awkward mental gymnastics to map their
domain concepts onto a single integer.

Examples of mappings a user should be able to express naturally:

- **Cartesian iteration** — a single cycle counter decomposed into a
  multi-dimensional coordinate space. E.g., cycle 0..999,999 maps to
  `(tenant=0..99, device=0..99, reading=0..99)` via modular arithmetic
  or mixed-radix decomposition.
- **Denormalized identifiers** — database rows often carry redundant key
  columns (partition key, clustering key, etc.) that are logically related.
  The coordinate tuple lets the user express these relationships directly
  rather than deriving each column independently from a cycle number.
- **Independent dimensions** — some coordinates may vary independently
  (e.g., a time dimension that increments while a partition dimension
  cycles), which is a natural fit for a multi-input DAG but was awkward
  to express as separate unary pipelines in nosqlbench.

The mapping layer sits between the engine's cycle counter and the DAG's
input coordinates. It is not part of the DAG itself — the DAG receives
a named coordinate tuple and is agnostic to how that tuple was derived
from engine state.

This mapping layer is intentionally a black box in the current design — it
is modular and replaceable. The default implementation will iterate over
the coordinate space via cartesian decomposition of the cycle counter.
Other strategies can be plugged in later without affecting the generation kernel (GK).

**Output variates** — the terminal nodes of the DAG. These are the generated
values consumed by downstream systems (op templates, bindings, etc.). Output
types may vary (u64, strings, floats, bytes, etc.) but the coordinate space
is always `u64`.

## GK Node Model

The user assembles the GK by wiring named input coordinates through a set
of transformation function nodes. Each node in the DAG is an instance of a
function drawn from a standard library.

### Function Nodes

Each function node has **\*-arity on both input and output** — a node may
consume any number of inputs and produce any number of outputs. This is a
fundamental departure from the Java virtdata model where every function was
strictly unary (`long → long` or `long → T`).

Examples of arity patterns:

- **1 → 1** — classic unary transform (hash, modulus, scale)
- **1 → N** — fan-out / decomposition (mixed-radix split, byte extraction)
- **N → 1** — combination (concatenation, composite hash, interleave)
- **N → M** — general transform (coordinate remapping, matrix-style ops)

### Function Categories

Nodes fall into (at least) two broad categories:

1. **Number-theoretic / ordinal functions** — operate in u64 space. These
   cover the broad set of mappings useful for procedural generation: hashing,
   modular arithmetic, permutations, distribution sampling, range mapping,
   mixed-radix decomposition, etc.

2. **Type-crossing functions** — produce or consume values outside u64 space.
   These handle conversions to strings, floats, byte buffers, structured
   data, etc. They bridge the u64 coordinate world to the typed value world
   that downstream consumers (op templates) require.

### Port Typing and Edge Adapters

Each function node implementation declares a fixed signature — its input
ports and output ports have specific, statically known types. For example,
a mixed-radix decomposition node might declare `(u64) → (u64, u64, u64)`,
and a string-formatting node might declare `(u64) → (String)`.

When wiring the DAG, an output port's type must be compatible with the
downstream input port's type. When they are not directly compatible, the
user can attach **edge adapter functions** to the connecting edge. These
are unary functions whose sole purpose is type conversion — they sit on an
edge and transform one type into another so that the upstream output becomes
compatible with the downstream input.

```
  Node A          edge adapter         Node B
  out:u64  ──→  [ ToString ] ──→  in:String
```

Edge adapters are distinct from DAG nodes in that:
- They are always unary (one input, one output).
- They live on edges, not as standalone nodes in the graph.
- Their purpose is exclusively type adaptation, not domain transformation.
- They are drawn from the same standard function library but used in an
  adapter role.

This keeps the DAG's node graph focused on meaningful domain transforms
while handling type plumbing at the wiring level.

### DAG Assembly

From the user's perspective, building a GK means:

1. Declaring named input coordinates (the naming tuple)
2. Selecting function nodes from the standard library
3. Wiring inputs and intermediate outputs to function node inputs
4. Designating terminal outputs as named output variates

The wiring is explicit — the user connects named values (input coordinates
or outputs of upstream nodes) to the inputs of downstream nodes.

## Design Decisions

- **Base type: `u64`** — no performance penalty vs signed on x86-64, and the
  full 64-bit range is available without sign-bit edge cases in hashing and
  modular arithmetic.
- **DAG, not pipeline** — allows shared intermediate computations and
  multi-output generation from common inputs, which is more expressive and
  potentially more efficient than the Java model of independent linear chains.
- **\*-arity nodes** — function nodes are not restricted to unary transforms.
  Any node can have any number of inputs and outputs, enabling natural
  expression of fan-out, combination, and multi-dimensional transforms.
- **High-performance runtime** — the GK is an optimized execution kernel,
  engineered for high throughput and low latency. It is strongly
  ahead-of-time compiled: all type validation, edge resolution, and
  optimization happens at assembly time. The runtime executes only
  pre-validated, type-safe function dispatch with no dynamic checks.
  JIT compilation to native machine code (via cranelift, dynasm, or
  similar) may be explored if beneficial.
- **Single-responsibility core functions** — each core function node
  does exactly one thing. A hash node hashes. A mod node takes a
  modulus. A distribution node samples. No core function combines
  hashing with another transformation (e.g., no "HashRange" that
  hashes and then bounds in one step). Users who want combined
  behaviors compose them explicitly in the DAG, or build reusable
  library kernels (`.gk` files) that package common compositions.
  This principle is reinforced by the AOT compiler: composed nodes in
  a compiled kernel have near-zero overhead compared to a single fused
  node, so there is no performance justification for fusing at the
  function level.

## Open Questions

Questions are numbered for reference. Answered questions move to the
Resolved Questions section below.

### GK Structure
- ~~Q1: resolved — see below~~
- ~~Q2: resolved — see below~~
- ~~Q3: resolved — see below~~

### Evaluation
- ~~Q4: resolved — see below~~
- ~~Q5: resolved — see below~~

### Type System
- ~~Q6: resolved — see below~~
- ~~Q7: resolved — see below~~

### Output
- ~~Q8: resolved — see below~~

### Integration
- ~~Q9: resolved — see below~~

### Function Library
- Q10: What is the standard function library for the initial release?

## Resolved Questions

- **Q2:** The DAG is static once configured and assembled. It goes through a
  configuration/assembly phase and is then compiled into a fixed runtime
  kernel. No rewiring at runtime. This enables aggressive optimization at
  compile/assembly time.
- **Q4:** Pull-through evaluation. An outer control loop sets the current
  coordinate context, then downstream consumers access output variates on
  demand. Values are computed lazily, pulling through the DAG only as
  needed. This means if only a subset of output variates is accessed for a
  given coordinate, unused branches of the DAG are never evaluated.
- **Q5:** Node port values have static semantics from the caller's
  perspective within a coordinate context — pulling the same output twice
  for the same coordinate should yield the same value. However, caching is
  not automatic or universal. Instead, explicit **cache nodes** can be
  inserted into the DAG to memoize intermediate results. Cache nodes can be
  invalidated by side-effect (e.g., when the coordinate context changes).
  This gives the user control over the economy of cached values vs.
  fully-functional recomputation — a balance that was a significant pain
  point in nosqlbench's Java implementation. The right trade-off depends on
  the cost of the upstream computation vs. the memory/invalidation overhead
  of caching.
- **Q3:** Yes, wires between nodes can carry names. This is essential
  because the GK supports **library constructs** — reusable sub-DAG
  assemblages that encapsulate a pattern of nodes and wiring. Named ports
  on a library construct's boundary provide the interface for composing it
  into a larger DAG. Without named intermediate wires, library constructs
  would have no way to expose a clear, self-documenting interface.
- **Q6:** All type validation happens at assembly time — the GK is a
  strongly ahead-of-time compiled kernel. No type checking at runtime. The
  assembly phase validates every edge, resolves adapters, and produces a
  fully type-safe execution plan. The runtime executes only pre-validated,
  type-safe n-ary functions. If Rust's runtime facilities (e.g., cranelift,
  dynasm) allow JIT compilation to machine code from the assembled DAG,
  that path may be explored. Otherwise, the baseline is pre-composed
  type-safe function dispatch.
- **Q7:** For common types (including String), the assembly phase will
  auto-insert edge adapters when an unambiguous coercion exists. The user
  can still specify adapters explicitly for non-obvious conversions or to
  override the default. This follows nosqlbench precedent and reduces
  boilerplate in typical workloads.
- **Q8:** Output variates are named individually, but they do not form a
  structured tuple type by default. Each terminal node's output port has a
  name, but the outputs are heterogeneous — they can be any type produced
  by their respective terminal functions. If a user wants a typed tuple as
  output, they wire a tuple-producing function node at the end of the DAG.
  The GK does not impose structural symmetry between inputs and outputs.
- **Q1:** The primary user-facing syntax will be a DSL designed for DAG
  definition. At minimum, the DSL expresses node function instantiations
  with their input and output port connections via named wires. A
  pseudo-visual style is preferred — the textual form should make the
  graph structure apparent to the reader. YAML remains a candidate for
  an alternative or embedding format, but the DSL is the primary
  interface. The DSL design itself is a separate design topic to be
  elaborated.
- **Q9:** Op template bind points are references to named GK output
  variates. This is the integration seam between the workload description
  language and the variate generation subsystem.

  In Java nosqlbench, each bind point (`{name}`) independently resolved to
  its own unary virtdata function chain (`long → value`). Every binding
  ran its own isolated pipeline from the cycle number, with no shared
  computation between bindings and no awareness of each other.

  In nb-rs, bind points in op templates resolve to named output variates
  of a shared GK instance. This means:
  - **Shared intermediate computation** — multiple bind points that depend
    on the same upstream transforms (e.g., a hashed tenant ID used in
    several output columns) compute it once, not redundantly per binding.
  - **Ahead-of-time type validation** — the GK assembly phase verifies
    that each output variate's type matches what the op template expects,
    before any cycles run. Java nosqlbench tested this at runtime with a
    probe call at cycle 0.
  - **Multi-dimensional input** — bind points implicitly benefit from the
    full named coordinate tuple, not just a single cycle number. A bind
    point referencing "device_name" can pull through a DAG path rooted in
    the "device" coordinate, while "tenant_name" pulls through a path
    rooted in "tenant" — all within the same GK evaluation.
  - **No cross-reference limitation** — in Java nosqlbench, op fields
    could not reference each other. In nb-rs, since all bind points pull
    from the same DAG, shared dependencies are expressed naturally as
    upstream nodes rather than as cross-references between fields.
