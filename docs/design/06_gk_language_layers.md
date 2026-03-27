# GK Language Layers — Surface DSL and Kernel IR

> **Note:** The finalized surface syntax is documented in
> `14_unified_dsl_syntax.md`. This document describes the surface-to-IR
> normalization pipeline and sugar rules, which remain current. The
> `init` keyword for init-time bindings and `=` vs `:=` operator
> distinction are defined in the unified syntax spec.

The GK language has two distinct strata: a **surface DSL** that the user
writes, and a **kernel IR** (internal representation) that the engine
compiles and executes. The assembly phase translates surface DSL into
kernel IR through a set of well-defined normalization rules.

This separation is a first-class architectural decision, not an
implementation detail. The surface DSL is optimized for human
expressiveness and readability. The kernel IR is optimized for static
validation, optimization, and execution performance.

---

## Layer 1: Surface DSL

What the user writes. Expressive, convenient, supports syntactic sugar.

### Core Syntax

```
output := function(arg, ...)             // single output
(out1, out2) := function(arg, ...)       // multi-output (destructuring)
coordinates := (name1, name2, ...)       // input coordinate declaration
```

### Lexical Rules

- **Identifiers** (bare words starting with a letter): wire references
- **Numeric literals** (`100`, `3.14`): assembly-time constants
- **Quoted strings** (`"hello"`): string constants
- **Quoted strings with `{name}`**: string interpolation (sugar)
- **`//` comments**: ignored by parser

### Supported Sugar

The surface DSL supports the following sugar forms, each with a defined
normalization into kernel IR.

---

### Sugar 1: String Interpolation

The first and most common form of sugar. A quoted string containing
`{name}` bind-point references is automatically normalized into a
string-building node with explicit named wire inputs.

#### User writes

```
device_id := "{tenant_code}-{device_seq}"
```

#### Assembly normalizes to

```
device_id := __str_build_0(tenant_code, device_seq)
```

where `__str_build_0` is a string-building node with:
- A format pattern: `"{}-{}"`
- Two named input ports: `tenant_code` (position 0), `device_seq` (position 1)
- One output port: `String`
- Auto-inserted edge adapters if inputs are not already `String`

#### Normalization rules

1. Parse the quoted string for `{name}` bind-point references.
2. If no bind points found → treat as a string constant (no node needed).
3. If bind points found:
   a. Extract the names in order: `[tenant_code, device_seq]`
   b. Build a positional format pattern by replacing each `{name}` with
      `{}`: `"{}-{}"`
   c. Emit a string-building node with named inputs wired by the
      extracted names.
   d. For each input wire whose type is not `String`, insert an edge
      adapter (e.g., `u64 → String`).

#### Examples

**Simple label:**
```
// Surface
label := "{name} (age {age})"

// Kernel IR
label := __str_build_0(name, age)
// format: "{} (age {})"
// edge adapters: age (u64 → String)
```

**Single bind point (pure wire rename with type conversion):**
```
// Surface
tenant_str := "{tenant_id}"

// Kernel IR
tenant_str := __u64_to_string(tenant_id)
// Optimized: single bind point with no surrounding text reduces to
// a plain edge adapter, no string-building node needed.
```

**No bind points (constant):**
```
// Surface
separator := "---"

// Kernel IR
separator := __const_str("---")
// No node, just a constant.
```

**Multiple references to the same wire:**
```
// Surface
echo := "{val}:{val}"

// Kernel IR
echo := __str_build_0(val)
// format: "{}:{}"
// The wire is consumed once; the format references it twice.
// The string-building node handles this internally.
```

**Nested in a larger expression (sugar composes):**
```
// Surface
msg := prefix("{name} (age {age})", "USER: ")

// Kernel IR
__str_0 := __str_build_0(name, age)    // from interpolation
msg := prefix(__str_0, "USER: ")        // function call
```

---

### Sugar 2: Inline Nesting

Function calls can be nested to avoid naming trivial intermediates.

#### User writes

```
group_id := hash(uniform(age))
```

#### Assembly normalizes to

```
__anon_0 := uniform(age)
group_id := hash(__anon_0)
```

#### Normalization rules

1. Walk the expression tree bottom-up.
2. For each nested function call, emit a discrete node with an
   auto-generated wire name (`__anon_N`).
3. Replace the nested call with the auto-generated wire reference.

#### Examples

**Two levels deep:**
```
// Surface
reading_h := hash(interleave(device_h, reading))

// Kernel IR
__anon_0 := interleave(device_h, reading)
reading_h := hash(__anon_0)
```

**Three levels deep:**
```
// Surface
x := add(hash(mod(cycle, 1000)), 500)

// Kernel IR
__anon_0 := mod(cycle, 1000)
__anon_1 := hash(__anon_0)
x := add(__anon_1, 500)
```

**Nested inside destructuring:**
```
// Surface
(a, b, c) := mixed_radix(hash(cycle), 10, 10, 10)

// Kernel IR
__anon_0 := hash(cycle)
(a, b, c) := mixed_radix(__anon_0, 10, 10, 10)
```

---

### Sugar 3: Auto-Inserted Edge Adapters

When a wire's type does not match the downstream port's expected type,
the assembly phase auto-inserts a type conversion for common coercions.

#### User writes

```
tenant_code := mod(tenant_h, 10000)
partition_key := "{tenant_code}:{time_bucket}"
```

`tenant_code` is `u64`, but the string-building node expects `String`
inputs.

#### Assembly normalizes to

```
tenant_code := mod(tenant_h, 10000)
__adapt_0 := __u64_to_string(tenant_code)
__adapt_1 := __u64_to_string(time_bucket)
partition_key := __str_build_0(__adapt_0, __adapt_1)
```

#### Auto-insert rules

| From    | To      | Adapter            | Auto? |
|---------|---------|--------------------|-------|
| u64     | String  | decimal repr       | yes   |
| f64     | String  | default precision  | yes   |
| u64     | f64     | lossless (≤ 2^53)  | yes   |
| bool    | u64     | 0/1                | yes   |
| u64     | bool    | 0=false, else true | yes   |
| f64     | u64     | lossy — truncation | **no** |
| u64     | bytes   | endian-dependent   | **no** |
| String  | u64     | parsing required   | **no** |

Lossy or ambiguous conversions require the user to insert an explicit
conversion node.

#### Examples

**Auto-insert (transparent):**
```
// Surface
reading_value := normal(reading_h, 72.0, 5.0)
// normal returns f64; op template {reading_value} needs String
// Assembly auto-inserts f64 → String adapter at the output boundary
```

**Explicit (user must specify):**
```
// Surface — this would be an assembly error without the explicit cast:
truncated := f64_to_u64(some_float_value)
```

---

### Sugar 4: Bare File Interface Inference

A `.gk` file without a formal wrapper has its interface inferred.

#### User writes (`keygen.gk`)

```
hashed := hash(id)
name := weighted_lookup(hashed, "names.csv")
code := mod(hashed, 10000)
```

#### Assembly infers

- **Inputs:** `id` (referenced but never assigned → input port, type
  inferred as u64 from downstream usage)
- **Outputs:** `name` (String), `code` (u64), `hashed` (u64) — all
  assigned but not consumed internally → available as output ports

This is equivalent to the formal declaration:

```
keygen(id: u64) -> (hashed: u64, name: String, code: u64) := {
    hashed := hash(id)
    name := weighted_lookup(hashed, "names.csv")
    code := mod(hashed, 10000)
}
```

---

### Sugar Composition

Sugar forms compose naturally — multiple forms can appear in the same
statement and the assembly phase normalizes them in a single pass.

```
// Surface: combines string interpolation + inline nesting + auto-adapters
tag := "sensor-{mod(hash(device), 1000)}"

// After inline nesting normalization:
__anon_0 := hash(device)
__anon_1 := mod(__anon_0, 1000)
tag := "{__anon_1}"

// After string interpolation normalization:
__anon_0 := hash(device)
__anon_1 := mod(__anon_0, 1000)
tag := __u64_to_string(__anon_1)
// Optimized: single bind point reduces to plain adapter
```

---

## Layer 2: Kernel IR

What the engine compiles and executes. Strict, flat, fully explicit.

### Properties

- **No sugar** — every node, wire, and adapter is explicit.
- **Flat** — no nesting; every node is a discrete statement.
- **Named wires only** — all connections are by name, including
  auto-generated names for anonymous intermediates (`__anon_N`,
  `__str_build_N`, `__adapt_N`).
- **Fully typed** — every wire carries a known type; every port
  connection has been validated.
- **Topologically sorted** — statements are ordered such that every
  wire is defined before it is consumed (the assembly phase handles
  reordering from the user's arbitrary declaration order).
- **Optimizable** — the flat, explicit form enables:
  - Dead node elimination (unreferenced branches pruned)
  - Common subexpression detection
  - Cache node insertion
  - Potential fusion of adjacent nodes on the hot path

### Kernel IR for the time-series example

The surface DSL from the end-to-end example (`timeseries.gk` in
`05_gk_dsl.md`) normalizes to this kernel IR:

```
// Coordinate input
coordinates := (cycle)

// Decomposition
(tenant, device, reading) := mixed_radix(cycle, 100, 1000, 0)

// Tenant branch
tenant_h := hash(tenant)
tenant_name := weighted_lookup(tenant_h, "tenants.csv")
tenant_code := mod(tenant_h, 10000)

// Device branch
__anon_0 := interleave(tenant, device)
device_h := hash(__anon_0)
device_seq := mod(device_h, 100000)
__adapt_0 := __u64_to_string(tenant_code)
__adapt_1 := __u64_to_string(device_seq)
device_id := __str_build_0(__adapt_0, __adapt_1)
// format: "{}-{}"

// Partition key branch
time_bucket := div(reading, 1000)
__adapt_2 := __u64_to_string(tenant_code)
__adapt_3 := __u64_to_string(time_bucket)
partition_key := __str_build_1(__adapt_2, __adapt_3)
// format: "{}:{}"

// Timestamp branch
__anon_1 := epoch_millis(reading)
timestamp := add(__anon_1, 1710000000000)

// Reading value branch
__anon_2 := interleave(device_h, reading)
reading_h := hash(__anon_2)
reading_value := normal(reading_h, 72.0, 5.0)
```

### Observations

- **14 user-visible statements** in the surface DSL became **22 nodes**
  in the kernel IR. The 8 additional nodes are auto-generated
  intermediates and edge adapters.
- **No sugar remains** — every string interpolation is a `__str_build`
  node, every type mismatch is an explicit `__adapt` node, every nested
  call is flattened.
- **Duplicate adapters** — `__adapt_0` and `__adapt_2` both adapt
  `tenant_code` to String. The optimizer may merge these (common
  subexpression elimination).
- **The user never sees this form** — it exists only inside the assembly
  and execution pipeline. Diagnostics and error messages map back to
  source DSL line numbers.

---

## Assembly Pipeline

```
Surface DSL (text)
      │
      ▼
[1. Parse]
      │  Lexing, AST construction
      ▼
[2. Desugar]
      │  Inline nesting → flat nodes
      │  String interpolation → str_build nodes
      │  Bare file → interface inference
      ▼
[3. Wire Resolution]
      │  Match wire names to definitions
      │  Detect undefined references (error)
      │  Detect definition-before-use violations (warning)
      ▼
[4. Type Inference & Validation]
      │  Propagate types through the DAG
      │  Validate port compatibility
      │  Insert edge adapters where auto-coercion applies
      │  Reject remaining type mismatches (error)
      ▼
[5. Topological Sort]
      │  Order nodes for evaluation
      │  Detect cycles (error — must be a DAG)
      ▼
[6. Optimization]
      │  Dead node elimination
      │  Common subexpression elimination
      │  Cache node insertion (if configured)
      │  Potential node fusion
      ▼
Kernel IR (compiled, executable)
```

---

## Summary

| Aspect          | Surface DSL                  | Kernel IR                     |
|-----------------|------------------------------|-------------------------------|
| Audience        | User                         | Engine                        |
| Nesting         | Allowed (inline)             | Flat only                     |
| String interp   | `"{name}..."` syntax         | Explicit `__str_build` nodes  |
| Type adapters   | Implicit (auto-insert)       | Explicit adapter nodes        |
| Declaration order | Flexible (warnings only)   | Topologically sorted          |
| Wire names      | User-chosen                  | User + auto-generated (`__*`) |
| Types           | Often inferred               | Fully explicit                |
| Optimizations   | None                         | DCE, CSE, fusion, caching     |
