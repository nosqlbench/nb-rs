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
  │               or external ports (volatile/sticky)
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
from op results are already injected via sticky ports, but the
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
