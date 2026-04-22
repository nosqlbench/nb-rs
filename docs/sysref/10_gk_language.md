# 10: GK Language and Compilation

The Generation Kernel (GK) is a deterministic data generation
engine. It transforms named u64 input tuples into typed
output variates via a directed acyclic graph (DAG) of composable
node functions.

---

## DSL Syntax

GK programs are written in `.gk` files or inline in workload
`bindings:` blocks.

### Input Declaration

```
inputs := (cycle)
inputs := (cycle, partition, cluster)
```

Inputs are the external values that drive the DAG. Most workloads
use a single `cycle` input. Multi-dimensional inputs enable nested
iteration patterns via mixed-radix decomposition.

> **Note:** The current Rust implementation still uses `coordinates`
> in some code paths (`coordinates := (cycle)`, `set_coordinates`,
> `coord_names`). The parser accepts both `inputs` and `coordinates`
> during transition. All new code and documentation should use
> `inputs`.

### Coordinate Decomposition

Most workloads use a single `cycle` input. Multi-dimensional
iteration is modeled inside the GK via mixed_radix decomposition:

    inputs := (cycle)
    (row, col) := mixed_radix(cycle, 1000, 1000)

This keeps the activity executor simple (it only passes `[cycle]`)
while enabling N-dimensional access patterns within the DAG.
Decomposed coordinates are ordinary GK wires — they can feed into
hash, interleave, mod, or any other node. Any traversal strategy
(nested loop, strided, random) is expressed as GK nodes rather
than activity-layer configuration, keeping domain logic in one place.

### Bindings

```
// Cycle-time binding (evaluated per cycle)
user_id := mod(hash(cycle), 1000000)

// Init-time constant (evaluated once, folded into DAG)
dim := vector_dim("glove-25-angular")

// Function composition (output of one feeds input of next)
hashed := hash(cycle)
bucket := mod(hashed, 100)
name := weighted_strings(bucket, "alice:0.3;bob:0.3;carol:0.4")
```

### String Interpolation

```
email := "{format_u64(hash(cycle), 10)}@example.com"
query := "SELECT * FROM {keyspace}.{table} WHERE id = {user_id}"
```

`{name}` references resolve to other bindings or workload
parameters. String interpolation is desugared to the built-in `printf` node
function — the compiler splits the template into a format string
and `{name}` references, then wires them into a `Printf` node
that formats the output at evaluation time.
This is pure syntactic sugar; no special runtime support is
needed beyond the standard node evaluation path.

### Comments

```
// Line comment
/// Doc comment (markdown, attached to next binding)
/* Block comment */
```

Line comments (`//`) for inline annotations. Triple-slash (`///`)
for documentation comments in markdown format, attached to the
following binding — these are extractable by tooling for
auto-generated documentation. Block comments (`/* ... */`) for
temporarily disabling sections.

### Infix Operators

GK supports arithmetic, bitwise, and power operators with
standard precedence. Operators desugar to function calls in
the DAG — `a + b` becomes `f64_add(a, b)`, `a & b` becomes
`u64_and(a, b)`.

```
// Arithmetic (f64)
wave := sin(to_f64(cycle) * 0.1)
scaled := (x + 1.0) / 2.0

// Bitwise (u64)
low_byte := hash(cycle) & 0xFF
flags := (region << 48) | (tenant << 32) | sequence
masked := hash(cycle) ^ 0xDEADBEEF

// Power
decay := amplitude ** 0.5
```

**Precedence** (lowest to highest, follows Rust):

| Level | Operators | Associativity |
|-------|-----------|---------------|
| 1 | `\|` (bitwise OR) | left |
| 2 | `^` (bitwise XOR) | left |
| 3 | `&` (bitwise AND) | left |
| 4 | `<<` `>>` (shifts) | left |
| 5 | `+` `-` (add/sub) | left |
| 6 | `*` `/` `%` (mul/div/mod) | left |
| 7 | `**` (power) | right |
| 8 | `-` `!` (unary neg/not) | prefix |

Parentheses override precedence: `(a + b) * c`.

**Operator → Node mapping:**

| Operator | Node function |
|----------|--------------|
| `+` `-` `*` `/` | `f64_add`, `f64_sub`, `f64_mul`, `f64_div` |
| `%` | `f64_mod` |
| `**` | `pow` |
| `&` `\|` `^` | `u64_and`, `u64_or`, `u64_xor` |
| `<<` `>>` | `u64_shl`, `u64_shr` |
| `!` (prefix) | `u64_not` |
| `-` (prefix) | `f64_sub(0.0, x)` |

### Literal Promotion

Literal values in wire positions are automatically promoted to
constant nodes. This means function calls with mixed wire and
literal arguments work naturally:

```
// All equivalent:
exp := 2.0
out := pow(x, exp)

out := pow(x, 2.0)   // 2.0 auto-promoted to ConstF64 node
```

The compiler inserts anonymous `ConstF64(2.0)` nodes for literals
in wire positions. These nodes are constant-folded at compile time,
so there is no runtime cost.

### Type Inference and Auto-Widening

Infix operators select the correct function variant based on
operand types:

```
cycle * 2          → u64_mul (both u64)
to_f64(cycle) * 0.5  → f64_mul (both f64)
cycle * 0.5        → f64_mul (cycle auto-widened to f64)
hash(cycle) & 0xFF → u64_and (bitwise always u64)
```

When operands have different types, the compiler auto-widens
the narrower operand (u64 → f64 via `to_f64`). This is a safe,
lossless conversion. The compiler emits an advisory event:

```
gk[advisory]: widening u64 → f64 in operator *
```

### Auto-Conversion to String

When a non-string value feeds a string wire input, the compiler
auto-inserts a conversion adapter:

| From | To | Adapter |
|------|----|---------|
| u64 | String | `__u64_to_str` (decimal) |
| f64 | String | `__f64_to_str` |
| bool | String | `__bool_to_str` ("true"/"false") |
| JSON | String | `__json_to_str` (compact JSON) |

These are inserted transparently. The compiler emits an
advisory event for each insertion, queryable via `--diagnose`.

### Compiler Diagnostics

The compiler emits tagged diagnostic events at three levels:

| Level | Tag | Meaning |
|-------|-----|---------|
| Info | `gk[info]` | Normal compilation steps |
| Advisory | `gk[advisory]` | Implicit conversions, type widenings — review for module design quality |
| Warning | `gk[warning]` | Potential performance or correctness issues |

Query advisories with `--diagnose` to review all implicit
conversions in your module:

```bash
nbrs bench gk mymodule.gk --explain
# Shows: gk[advisory]: type adapter U64→F64: cycle → sin
# Shows: gk[advisory]: widening u64 → f64 in operator *
```

---

## Bitwise Operations

GK provides six u64 bitwise node functions. Applying bitwise
operators to f64 operands is a compile-time error.

| Node | Signature | Description |
|------|-----------|-------------|
| `u64_and` | `u64, u64 → u64` | bitwise AND |
| `u64_or` | `u64, u64 → u64` | bitwise OR |
| `u64_xor` | `u64, u64 → u64` | bitwise XOR |
| `u64_shl` | `u64, u64 → u64` | left shift |
| `u64_shr` | `u64, u64 → u64` | logical right shift |
| `u64_not` | `u64 → u64` | bitwise complement |

```
// Mask the low byte
low_byte := u64_and(hash(cycle), 0xFF)

// Pack fields into a single u64
packed := u64_or(u64_shl(region, 48), u64_shl(tenant, 32))

// Flip bits deterministically
flipped := u64_xor(hash(cycle), 0xDEADBEEF)

// Complement (infix: !x)
inv := u64_not(flags)
```

Infix operator `&` desugars to `u64_and`, `|` to `u64_or`,
`^` to `u64_xor`, `<<` to `u64_shl`, `>>` to `u64_shr`,
prefix `!` to `u64_not`.

---

## Const Expression Syntax

Braces in binding values trigger compile-time evaluation.
Three equivalent forms:

```
// Implicit: bare braces around an expression
dim := {vector_dim("glove-25-angular")}

// Explicit open form
dim := {:=vector_dim("glove-25-angular")}

// Explicit bracketed form
dim := {:=vector_dim("glove-25-angular"):=}
```

Resolution order for `{expr}`:
1. Named binding — if `expr` matches a declared binding name,
   wire to that binding's output.
2. Const expression eval — evaluate `expr` at compile time
   (init-time fold). Result becomes a `ConstNode`.
3. Error — if neither succeeds, compilation fails with a
   diagnostic describing the unresolved reference.

The explicit `{:=...}` forms bypass step 1 and force const
expression evaluation regardless of name shadowing.

---

## Type Inference Details

The compiler selects operator variants according to this
dispatch table:

| Left operand | Right operand | Operator | Selected variant |
|-------------|--------------|----------|-----------------|
| u64 | u64 | `+` `-` `*` `/` `%` | u64 variant |
| f64 | f64 | `+` `-` `*` `/` `%` | f64 variant |
| u64 | f64 | `+` `-` `*` `/` `%` | u64 auto-widened → f64, f64 variant |
| f64 | u64 | `+` `-` `*` `/` `%` | u64 auto-widened → f64, f64 variant |
| any | any | `**` | always f64 (`pow`) |
| u64 | u64 | `&` `\|` `^` `<<` `>>` | always u64 |
| f64 | any | `&` `\|` `^` `<<` `>>` `!` | **compile error** |

Auto-widening inserts an implicit `to_f64` adapter and emits
a `gk[advisory]` diagnostic. Narrowing (f64 → u64) is never
implicit — use an explicit cast function.

---

## Compilation Pipeline

```
Source text
  │
  ▼
Parse ─────────▶ AST (assignments, function calls, wiring)
  │
  ▼
Desugar ───────▶ Normalize sugar forms:
  │               - String interpolation → StringBuild nodes
  │               - Inline nesting → auto-named intermediates
  │               - Bare {name} → wire references
  │
  ▼
Wire Resolution ▶ Map names to node outputs, input indices,
  │               or external ports
  │
  ▼
Type Inference ─▶ Validate port types match wiring.
  │               Insert auto-adapters (u64→f64, etc.)
  │
  ▼
Topological Sort ▶ Determine evaluation order
  │
  ▼
Output Selection ▶ Mark which nodes are outputs (referenced by
  │               op fields, params, or extra bindings)
  │
  ▼
Constant Folding ▶ Evaluate init-time nodes (no input
  │               dependency), replace with constants
  │
  ▼
GkProgram ──────▶ Immutable compiled DAG (shared via Arc)
```

### Output Selection

Not all bindings become program outputs. Only bindings referenced
by consumers are included:

- Op field bind points: `{user_id}` in a statement
- Param bind points: `{ground_truth}` in `relevancy.expected`
- Extra bindings: validation layer declarations

The compiler scans op fields AND params for `{name}` references.
Unreferenced bindings are dead code — compiled into the DAG but
never pulled, so constant folding may eliminate them entirely.

---

## Type System

GK values are dynamically typed via the `Value` enum:

```rust
pub enum Value {
    None,
    U64(u64),
    F64(f64),
    Bool(bool),
    Str(String),
    Bytes(Vec<u8>),
}
```

Nodes declare their port types via `NodeMeta`. The compiler
inserts type adapter nodes where wiring crosses types (e.g.,
`u64 → f64` auto-conversion). Type mismatches that can't be
adapted are compile-time errors.

Type names in the DSL and diagnostics use Rust-standard names:
`u64`, `f64`, `bool`, `String`, `Vec<u8>`. These are familiar to
Rust users and unambiguous. The internal `Value` enum mirrors
these names directly, avoiding any mapping layer.

---

## Node Contract

Every node implements `GkNode`:

```rust
pub trait GkNode: Send + Sync {
    fn meta(&self) -> &NodeMeta;
    fn evaluate(&self, inputs: &[Value], outputs: &mut [Value]);
}
```

`NodeMeta` declares:
- `name: String` — function name for DSL and diagnostics
- `ins: Vec<PortMeta>` — input port names and types
- `outs: Vec<PortMeta>` — output port names and types

Nodes are pure functions of their inputs. No internal mutable
state (state lives in `GkState` buffers, not in nodes).

---

## Wiring Model

The DAG is stored as parallel vectors:

```rust
pub struct GkProgram {
    nodes: Vec<Box<dyn GkNode>>,      // node instances
    wiring: Vec<Vec<WireSource>>,     // per-node input sources
    input_names: Vec<String>,          // input dimensions
    output_map: HashMap<String, (usize, usize)>,  // name → (node, port)
}

pub enum WireSource {
    Input(usize),               // input from graph input dimension
    NodeOutput(usize, usize),   // input from (node_index, port_index)
    VolatilePort(usize),        // external input (resets per cycle)
    StickyPort(usize),          // external input (persists across cycles)
}
```

Evaluation proceeds in topological order. Each node reads inputs
from upstream node output buffers or graph input values, and writes
to its own output buffer slots in `GkState`.

### GK as Unified State Holder

The GK kernel should be the single state holder for all inter-op
data flow, not just input-driven generation. Captured values
from op results are already injected via ports, but the
current model treats them as second-class inputs. The target
design unifies captures with GK inputs:

- Captured values write into named GK buffers (the same buffers
  that nodes write to)
- Downstream nodes that depend on captured values re-evaluate
  when the capture changes — the same invalidation mechanism
  that handles input changes
- Complex derived values from captures (e.g., parsing a captured
  JSON string into structured fields) are expressed as GK nodes,
  not as special-case logic in the executor

This means the GK kernel acts as general-purpose named registers
for inter-op state, with the DAG providing derived-value
computation on top.

### Incremental Invalidation

**Design topic for Memo:** The current implementation resets all
GK state on input mutation. This is correct but wasteful —
nodes that don't transitively depend on the changed input don't
need re-evaluation.

The target model: **provenance-based invalidation**. When an
input (graph input or captured value) changes, only nodes
downstream of that input are invalidated. This requires:

1. Organizing buffers so downstream nodes can be invalidated
   efficiently (contiguous ranges or bitmask per input)
2. Tracking which input each node transitively depends on
3. On input change: invalidate only the affected subset
4. Diamond-shaped flows: a node at the bottom of a diamond
   re-evaluates only when its actual inputs change, not when
   unrelated siblings change

For simple linear chains, this is straightforward. For complex
DAGs with shared intermediates, the trade-off is tracking cost
vs re-evaluation cost. A memo should explore the specific
mechanisms and when the optimization pays for itself.

---

## GK Scoping Model

GK programs exist within a scope hierarchy. Each scope level
can define bindings, and inner scopes inherit from and override
outer scopes.

### Scope Levels

GK scopes are created at **lifecycle boundaries** — activities
and phases — not at arbitrary iteration points. A new GK scope
means a new compiled kernel, new state, new constant folding.
This is justified only when a long-lived process is launched.

```
Workload scope (outermost)
  │
  │  bindings: |
  │    inputs := (cycle)
  │    dim := vector_dim("{dataset}")
  │    base_count := vector_count("{dataset}")
  │
  └── Phase scope (one per phase in the scenario)
        │
        │  Each phase is a lifecycle boundary — it launches
        │  its own Activity with its own fiber pool, cycle
        │  counter, and metrics. A phase MAY declare its own
        │  bindings that replace the workload bindings.
        │
        │  bindings: |
        │    id := format_u64(cycle, 10)
        │    train_vector := vector_at(cycle, "{dataset}")
        │
        └── Cycle evaluation (per-fiber, NOT a new GK scope)
              set_input(0, cycle)
              pull("train_vector") → [0.12, 0.34, ...]
```

A `for_each` phase expands into multiple sequential phase
executions. Each expansion is a full phase lifecycle — its own
Activity, its own kernel, its own cycle counter. This is not
"nested" scoping in the closure sense; it is sequential
instantiation of phase lifecycles with different parameters.

### When GK Scopes Are Created

| Event | New GK scope? | Reason |
|-------|---------------|--------|
| Workload start | Yes | Root scope, compiled once |
| Phase start | Yes, if phase has own `bindings:` | Phase is a lifecycle boundary |
| Phase start (no own bindings) | No — uses workload kernel | No new lifecycle constants needed |
| `for_each` iteration | Yes | Each iteration is a separate phase lifecycle with different constants |
| Cycle within a phase | No | Cycles are evaluations within an existing scope |
| Stanza within a phase | No | Stanzas are groups of cycles |
| Op within a stanza | No | Ops share the phase's kernel |

GK scopes are **never** created for:
- Individual cycle evaluations
- Per-op dispatch
- Retry attempts
- Conditional skips
- Op-level `bindings:` blocks (see below)

### Op-Level Bindings

Ops may declare their own `bindings:` block as syntactic
convenience — a way to define bindings close to the op that
uses them. Op bindings do NOT create a new scope. They are
merged into the enclosing scope's DAG at compile time.

**Rules:**
1. Op bindings augment the enclosing scope's kernel. They
   add nodes to the same DAG that all ops in the scope share.
2. Op bindings that shadow a name from the enclosing scope
   are a **compile error**. The enclosing scope owns the DAG;
   ops contribute to it but cannot override it.
3. Each op dispenser holds a reference to the enclosing
   scope's GK context (program + state). There is no
   per-op kernel.
4. `shared`/`final` constraints from outer scopes are
   enforced — an op binding cannot redefine a `final` name.

If different ops need incompatible bindings, they belong in
different phases. Phases are the scope boundary; ops are not.

**Strict mode** additionally detects cross-op binding
references: if op A declares a binding and op B uses it in
its template, that's an error in strict mode. Each op should
only reference the enclosing scope's bindings or its own.
Cross-op coupling via bindings is a code smell — promote the
shared binding to the enclosing scope instead.

### Scope Assembly: Auto-Extern Composition

GK scopes use **auto-extern composition** — the inner scope
is compiled as its own small kernel with `extern` input
declarations for names inherited from the outer scope. There
is no source text duplication, no runtime delegation chain.

See **sysref 16** for the complete scope model specification.

This is a critical mechanical distinction:

- **Auto-extern composition** (used): The outer kernel is
  compiled and constant-folded. The runner extracts its output
  manifest (names + types + modifiers). When compiling an inner
  scope, any name referenced but not defined is looked up in the
  outer manifest. If found, it becomes an `extern` input on the
  inner kernel. The runner copies folded constant values from
  the outer state into the inner state's input slots at scope
  creation time. The inner kernel is small — only its own nodes.

- **Delegation** (NOT used): Would require the inner kernel
  to hold a reference to the outer kernel and resolve names
  upward at evaluation time. This breaks constant folding
  (the inner kernel can't fold what it can't see at compile
  time) and complicates provenance tracking.

- **Flattening** (NOT used): Would duplicate the outer scope's
  source text into every inner scope, causing redundant
  compilation and node instantiation.

The assembly process for each scope level:

```
Workload scope:
  source = workload bindings text
  Compiled once. Outputs form the outer manifest.

Phase scope (with own bindings):
  source = phase bindings text + auto-generated extern declarations
  Names from the outer manifest that are referenced but not
  defined in the phase bindings become extern inputs.
  The runner copies outer constant values into the inner kernel.

Phase scope (no own bindings):
  Uses the workload kernel directly — no recompilation.

for_each iteration (structural variable):
  source = phase bindings (with {var} substituted) + auto externs
  Each iteration compiles its own kernel from substituted source.

for_each iteration (parametric variable):
  Variable only in op fields, not in bindings.
  Reuses outer kernel — no recompilation. Only op field strings
  are substituted per iteration.
```

### Inheritance Rules

1. **Workload bindings** are the base. The workload kernel is
   compiled first. Its outputs form the outer manifest.

2. **Phase bindings** (`bindings:` on a phase) define the
   inner scope. Names from the outer manifest that are
   referenced in the phase ops but not defined in the phase
   bindings become auto-generated `extern` inputs on the
   inner kernel. The runner wires outer constant values into
   these inputs at phase start.

3. **Shadowing**: if a phase binding defines a name that
   exists in the outer manifest, the phase definition wins
   (the outer name is not auto-externed). Exception: `final`
   bindings in the outer scope cannot be shadowed — attempting
   to redefine them is a compile error.

4. **Phases without own bindings** use the workload kernel
   directly. No recompilation, no overhead.

5. **for_each substitution** happens on the phase's own
   bindings source (not a flattened copy of the outer source).
   The iteration variable `{var}` is replaced in:
   - The phase bindings source text (if structural)
   - All op field values (`stmt:`, `raw:`, `prepared:`)
   - The phase's `cycles:` config expression

6. **Per-iteration kernel**: each structural `for_each`
   iteration compiles its own GK kernel. Parametric iterations
   reuse the outer kernel. Either way:
   - Init-time constants resolve per-iteration
   - The cycle count can vary per iteration
   - Each iteration is a complete phase lifecycle

### Binding Modifiers

Bindings can carry modifiers that control scope behavior:

```
shared counter := hash(cycle)    # mutable across iteration boundaries
final dim := 128                 # immutable; cannot be shadowed
shared init budget = 100         # combined: shared + init-time
```

- **`shared`**: the runner propagates the value back to the
  outer scope after a `for_each` loop completes. Subsequent
  phases see the updated value.

- **`final`**: inner scopes cannot redefine this name.
  The runner enforces this at compile time when generating
  auto-extern declarations.

### Cursor Declaration

A cursor is a named `u64` position tracker that drives data
access. The `cursor` keyword declares a cursor and wires it
to a constructor that determines its data source:

```
cursor base = dataset_source("example:label_00", "base")
cursor users = range(0, 1000000)
```

The cursor itself is just a position value (a `u64` ordinal).
It does not carry fields or schema — it is a pure position
tracker. Data access happens through **accessor functions**
that take the cursor's ordinal as input and return typed values:

```
cursor base = dataset_source("example:label_00", "base")
id := format_u64(base, 10)
train_vector := vector_at(base, "example:label_00")
```

Here `base` resolves to the cursor's current ordinal value.
Accessor functions like `vector_at` use that ordinal to look up
the corresponding data. This separation means the cursor is a
simple GK node with a `u64` output, and all data access is
expressed through standard GK function composition.

**Cursor-to-accessor wiring**: the compiler resolves `base`
in accessor function arguments to the cursor node's output.
The cursor node is an input to the GK graph — the runtime
advances it externally, and downstream accessor nodes
re-evaluate via standard provenance-based invalidation.

**Phase completion** is determined by cursor exhaustion. When
all cursors in a phase are exhausted (no more positions to
advance to), the phase completes. This replaces `cycles:` for
cursor-driven phases.

**Planned: auto-cursors.** When a phase references accessor
functions that imply a data source but no explicit cursor is
declared, the compiler will auto-generate a cursor. This
reduces boilerplate for single-source phases.

**Planned: cardinality discovery.** The cursor's extent
(total number of positions) is discovered at init time by
interrogating the constructor's data source. This enables
automatic cycle count derivation and progress reporting.

**Cursor constructor types:**
- `range(start, end)` — finite ordinal sequence (replaces `cycles:`)
- `dataset_source(spec, facet)` — dataset vectors, queries, metadata

### Scope Configuration

`for_each` phases support two scope knobs:

```yaml
phases:
  rampup:
    for_each: "pname in ..."
    loop_scope: clean       # clean (default) | inherit
    iter_scope: inherit     # inherit (default for for_each) | clean
```

- **`loop_scope`**: how the loop is seeded from the outer scope.
  `clean` = original workload snapshot. `inherit` = current
  outer state (includes shared mutations from prior phases).

- **`iter_scope`**: how each iteration is seeded from the loop
  scope. `inherit` = iterations see each other's state (default
  for for_each). `clean` = fully isolated iterations.

See **sysref 16** for the complete scope lifecycle specification.

### for_each Syntax

```yaml
phases:
  rampup:
    for_each: "pname in matching_profiles('{dataset}', '{prefix}')"
    cycles: "{vector_count('{dataset}:{pname}')}"
    bindings: |
      inputs := (cycle)
      train_vector := vector_at(cycle, "{dataset}:{pname}")
    ops:
      insert:
        prepared: "INSERT INTO t.vec_{pname} VALUES ('{id}', {train_vector})"
```

The `for_each` expression is evaluated as a GK const expression
after workload param substitution. It must produce a
comma-separated string. Each element becomes one iteration.

### Compilation Flow with for_each

```
1. Parse workload YAML
2. Compile workload-level GK (Phase 1-3 pipeline)
   → shared kernel for non-for_each phases
   → extract outer manifest (names, types, modifiers)
   → snapshot outer_scope_values (folded constants)
3. For each phase in scenario:
   a. If no for_each and no own bindings → use shared kernel
   b. If own bindings → auto-extern composition, compile phase kernel
   c. If for_each:
      i.   Evaluate for_each expression via eval_const_expr
      ii.  Split result on commas → iteration list
      iii. Detect structural vs parametric variable
      iv.  For each iteration value:
           - If structural: substitute {var} in bindings, generate
             auto-externs from outer manifest, compile inner kernel
           - If parametric: substitute {var} in op fields only,
             reuse outer kernel (no recompilation)
           - Wire scope values into inner kernel's extern inputs
           - Resolve per-iteration cycles from kernel constants
           - Run phase with per-iteration kernel and ops
      v.   After loop: write-back shared outputs to outer_scope_values
```

### Phase Lifecycle Isolation

Each phase execution (including each `for_each` iteration) is
a complete Activity lifecycle:

- **Own GK kernel**: compiled from phase bindings (or workload
  bindings if no phase-level override)
- **Own cycle counter**: starts at 0
- **Own fiber pool**: created fresh
- **Own metrics**: activity name includes the iteration label
  (e.g., `rampup (pname=label-1)`)
- **Own error handler**: errors don't carry between phases

Captures do NOT flow between phases. Each phase is a
self-contained activity execution. State flows between phases
only through the database (or other external system), or via
`shared` variable write-back (see sysref 16).

### Design Rationale

**Why structural variables require per-iteration compilation?**

GK bindings like `vector_at(cycle, "example:label-1")` need the
dataset source at node construction time — the node opens a file
handle, loads metadata, and preallocates buffers. This is init-time
work that can't be deferred to cycle-time. Structural variable
substitution before compilation ensures the source string is a
literal that the node constructor can act on.

Different profiles may have different vector counts, dimensions,
or available facets. A shared kernel would need to handle all
profiles simultaneously, which breaks the "one cycle = one vector
ordinal" invariant. Per-iteration compilation keeps the cycle
semantics clean: cycle 0 is always the first vector in THIS
profile, not an offset into a global index.

**Why parametric variables skip recompilation?**

When the for_each variable only appears in op field strings
(e.g., table names in SQL templates), no GK nodes change
between iterations. The DAG topology is identical. The runner
detects this automatically and reuses the outer kernel, avoiding
unnecessary recompilation for simple iteration patterns like
iterating over table names or keyspaces.

**Why not merge phase bindings with workload bindings?**

Merging creates ambiguity about which definition wins. Replacement
is explicit — if you need workload bindings in a phase, include
them. This makes each phase's GK program self-contained and
readable without tracing inheritance chains.
