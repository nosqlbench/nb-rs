# SRD 27 — GK Modules

## Terminology

A **GK module** is a reusable subgraph defined in a `.gk` file. Its
interface — inputs and outputs — is inferred from the same wire
connectivity rules used everywhere else in GK: unbound references
are inputs, terminal bindings are outputs.

No special keywords, no separate signature declarations. A module
is just a `.gk` file.

## Module Interface — Inferred from Wires

The same coordinate inference from SRD 26 applies to modules:

- **Inputs**: Any name referenced but never defined as a node output.
  These become the module's parameters — either wire inputs (dynamic)
  or init-time constants, depending on how the caller provides them.
- **Outputs**: Named bindings in the module. The caller selects which
  outputs to wire into its own DAG.

```gk
// euler_circuit.gk
//
// Inputs (unbound): position, range, seed, stream
// Output: value

value := cycle_walk(position, range, seed, stream)
```

No `module` keyword. No explicit signature. The file IS the module.
The wires ARE the interface.

### Multiple outputs

A module with multiple outputs simply has multiple terminal bindings:

```gk
// decompose.gk
//
// Inputs: cycle, x_size, y_size
// Outputs: x, y, remainder

(x, y, remainder) := mixed_radix(cycle, x_size, y_size, 0)
```

### Composed modules

A module can reference other modules — they're just function calls
that trigger the same resolution:

```gk
// sensor_field.gk
//
// Inputs: device_id, reading_idx, seed
// Output: value

h := hash(interleave(device_id, reading_idx))
q := unit_interval(h)
value := icd_normal(q, seed, 1.0)
```

## Resolution

When the GK compiler encounters a function call to a name that is
not a built-in node:

1. **Same-name file**: Look for `<name>.gk` in the same directory
   as the file being compiled.

2. **Directory scan**: If no same-name file exists, scan all `.gk`
   files in that directory for a file containing a binding named
   `<name>` (since outputs = named bindings = the module's exports).

3. **Error**: If no match is found, report a compile error listing
   available `.gk` files in the directory.

### Resolution for workload YAML

When bindings are inline in a YAML workload (`bindings: |` string
mode), "same directory" is the directory containing the YAML file:

```
workloads/
  telemetry.yaml           ← references euler_circuit(...)
  euler_circuit.gk         ← resolved: same directory, same name
  sensor_helpers.gk        ← scanned if euler_circuit.gk not found
```

### Resolution order

```
1. Built-in native nodes (hash, mod, interleave, etc.)
2. <name>.gk in same directory as the referencing file
3. Any .gk file in same directory exporting a matching binding
4. Error: unknown function '<name>'
```

## Inlining

When a module is resolved, its subgraph is **inlined** into the
caller's DAG:

1. Parse the `.gk` file
2. Infer its inputs (unbound references) and outputs (bindings)
3. Wire caller's arguments to the module's inputs
4. Prefix internal names to avoid collision
5. Map the module's outputs to the caller's target names

```gk
// Caller:
user_id := euler_circuit(h, range: 1000000, seed: 42, stream: 0)

// After inlining (conceptual):
//   position → h (wire)
//   range → 1000000 (constant)
//   seed → 42 (constant)
//   stream → 0 (constant)
user_id := cycle_walk(h, 1000000, 42, 0)
```

Constants provided by the caller (named args with literal values)
become init-time values in the inlined subgraph. Wire arguments
(referencing upstream nodes) become wire connections.

## Argument Passing

The caller passes arguments positionally or by name:

```gk
// Positional — maps to inferred inputs in sorted order
result := euler_circuit(h, 1000000, 42, 0)

// Named — explicit mapping to module inputs
result := euler_circuit(position: h, range: 1000000, seed: 42, stream: 0)
```

Named arguments are preferred for modules with more than one or two
inputs — they're self-documenting and order-independent.

## Module vs Native Node

| Property | Native node | GK module |
|----------|-------------|-----------|
| Defined in | Rust (`impl GkNode`) | `.gk` file |
| Interface | Explicit in Rust types | Inferred from wires |
| Compilation | P1/P2/P3 per node | Inherits from composed nodes |
| Loop support | Yes (Rust internals) | No — delegates to native nodes |
| Distribution | Compiled into binary | `.gk` files alongside workloads |

GK modules compose native nodes into higher-level abstractions.
For operations needing loops or complex control flow (rejection
sampling, cycle walking), the module delegates to a native node.

## Example: Euler Circuit Module

The `euler_circuit` module produces a bijective permutation of
`[0, range)` — every input maps to a unique output and vice versa.
Visiting every element exactly once: an Euler circuit through the
value space.

### How it works

Uses **cycle-walking** over a PCG permutation:

1. PCG with 64-bit state is a permutation of all u64 values
   (full period = 2^64).
2. To restrict to `[0, range)`: apply PCG, if result ≥ range,
   apply PCG again to the result. Repeat until result < range.
3. Cycle-walking is guaranteed to terminate — you're following
   cycles in a finite permutation, which must return to the
   target subrange.
4. To minimize rejections, the native `cycle_walk` node
   auto-selects the smallest PCG state width ≥ range.

### The module file

```gk
// euler_circuit.gk
//
// Bijective permutation of [0, range) via PCG cycle-walking.
//
// Inputs:
//   position  — which element of the permutation (wire)
//   range     — size of the permutation space (init constant)
//   seed      — PCG seed (init constant)
//   stream    — PCG stream for independent permutations (init constant)
//
// Output:
//   value     — the permuted value in [0, range), unique per position

value := cycle_walk(position, range, seed, stream)
```

### Usage in a workload

```gk
// Every hash(cycle) maps to a UNIQUE user_id in [0, 1M)
// No collisions — bijective
h := hash(cycle)
user_id := euler_circuit(h, range: 1000000, seed: 42, stream: 0)
```

### Multiple independent permutations

```gk
// Two different bijective mappings over the same range
// using different streams
h := hash(cycle)
user_a := euler_circuit(h, range: 1000000, seed: 42, stream: 0)
user_b := euler_circuit(h, range: 1000000, seed: 42, stream: 1)
// user_a ≠ user_b for all h (different permutations)
```

## Dead Code Elimination

A GK program may define more bindings than any single op template
consumes. This is expected — bindings form a library of available
variates, and different ops reference different subsets.

The kernel compiler **must only compile the subgraph needed** by the
actual bind point references in the op templates. Unreferenced
bindings and their upstream dependencies are pruned before assembly.

```gk
// 10 bindings defined
user_id := mod(hash(cycle), 1000000)
device_id := mod(hash(cycle), 50000)
temperature := icd_normal(unit_interval(hash(cycle)), 22.0, 3.0)
humidity := icd_normal(unit_interval(hash(hash(cycle))), 55.0, 10.0)
pressure := icd_normal(unit_interval(hash(hash(hash(cycle)))), 1013.0, 5.0)
latitude := scale_range(hash(cycle), -90.0, 90.0)
longitude := scale_range(hash(hash(cycle)), -180.0, 180.0)
region := mod(hash(cycle), 50)
category := mod(hash(hash(cycle)), 8)
label := format_u64(user_id)
```

```yaml
# This op only uses 3 of the 10 bindings
ops:
  insert:
    stmt: "INSERT INTO t (id, temp, humid) VALUES ({user_id}, {temperature}, {humidity});"
```

The compiler traces backward from `{user_id}`, `{temperature}`,
`{humidity}` through the DAG and only compiles the nodes that
contribute to those outputs. The other 7 bindings are not
assembled — no nodes created, no buffer slots allocated, no
JIT code generated.

This means large binding libraries impose zero runtime cost for
ops that only use a subset.

## Strict Mode

The `--strict` flag enables stricter validation for production
workloads where catching errors early is more valuable than
convenience:

| Check | Default | `--strict` |
|-------|---------|------------|
| Coordinate declaration | Inferred from unbound refs | Required: `coordinates := (...)` |
| Module arguments | Positional or named | Named only — no positional ambiguity |
| Module inputs | Unresolved inputs become coordinates | All inputs must be provided by caller |
| Bind point coverage | Undefined bind points error | Same (always enforced) |
| Unused bindings | Allowed (pruned silently) | Allowed (pruned silently) |

Note: unused bindings are **not** an error even in strict mode.
Bindings form a library of available variates — defining more than
needed is the normal pattern. The compiler prunes unreferenced
subgraphs regardless of mode.

### Error examples in strict mode

```
// Missing coordinates declaration
h := hash(cycle)
// STRICT ERROR: no 'coordinates' declaration — add 'coordinates := (cycle)'

// Positional module argument
user_id := hashed_id(cycle, 1000000)
// STRICT ERROR: module 'hashed_id' called with positional args — use named args

// Unresolved module input
user_id := euler_circuit(input: h, range: 1000000)
// STRICT ERROR: module 'euler_circuit' input 'seed' not provided — add 'seed: <value>'
```

## Future Extensions

- **Module search path**: `--gk-lib` flag or `GK_PATH` env var for
  shared module libraries beyond same-directory resolution.
- **`describe gk modules`**: CLI command listing available modules
  in the current directory with their inferred interfaces.
- **Module caching**: Parse each `.gk` file once, cache the AST for
  repeated inlining across multiple references.
