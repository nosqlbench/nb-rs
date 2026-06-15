# Language Spec

The detailed specification of the Polydat DSL surface: syntax
productions, type system, node contract, wiring model, and
compilation pipeline. This doc is the mechanism-level
companion to [grammar.md](grammar.md), which states the
axioms (G1-G6); read grammar.md first for the formal
contract; come here for the operator catalog, precedence
tables, type-system enum, the `PolydatNode` trait surface, and
the pipeline-stage breakdown.

This doc extends axiom-level statements across multiple
substrate docs:
- [grammar.md §2 productions + §3 type rules + G2 const lifecycle](grammar.md)
- [composition_substrate.md §2 slot contract + T1 typed return + L2 effectively-const](composition_substrate.md)
- [graph_compiler.md §2 pipeline + §5 Node Fusion catalog + §6 ordered composition](graph_compiler.md)
- [runtime_model.md R2 hybrid push/pull invalidation](runtime_model.md)
- [expression_engine.md §3.1 const expression evaluation + §5 embedding contract](expression_engine.md)

The host-side framing (why a host uses Polydat as its unified
access surface, output selection, Polydat as unified state
holder, op-level bindings, cursor declarations) is documented
host-side.

---

## DSL Syntax

GK programs are written in `.polydat` files or inline in workload
`bindings:` blocks.

### Input Declaration

```
input cycle: u64
input (cycle: u64, partition: u64, cluster: u64)
```

Inputs are the external values that drive the DAG. A workload
declares any cursor names it wants as inputs; the compiler
treats them as unbound wires that the host must supply at
runtime.

**Inputs are inferred when the declaration is omitted.** A
binding block that references `cycle` (or any other unbound
name) implicitly declares those names as its input set —
the compiler's closure inference already identifies unbound
wires on both the input and output sides, so requiring an
explicit `input ...: u64` line in every block was redundant.
Strict checking still applies: the host closure feeding the
kernel must provide every inferred input, and the compiler
reports a mismatch if it doesn't.

**`cycle` is not a magic identifier.** It's a conventional
name for the primary cursor — common in examples because it
matches the default cursor the runner supplies — but inputs
can be named anything and any cursor shape (single, nested,
decomposed via `mixed_radix`) is fine. The engine treats
`cycle` identically to any other user-named input.

> **Note:** `inputs` is the only accepted keyword. The legacy
> `coordinates` alias is gone — the lexer rejects it. Some
> internal AST/struct names (`Statement::Coordinates`, `coord_count`,
> `coord_names`) retain historical naming for AST stability;
> these are implementation details and don't surface in user-
> visible source or error messages.

### Coordinate Decomposition

Most workloads use a single `cycle` input. Multi-dimensional
iteration is modeled inside the Polydat via mixed_radix decomposition:

    input cycle: u64
    (row, col) := mixed_radix(cycle, 1000, 1000)

This keeps the activity executor simple (it only passes `[cycle]`)
while enabling N-dimensional access patterns within the DAG.
Decomposed coordinates are ordinary Polydat wires — they can feed into
hash, interleave, mod, or any other node. Any traversal strategy
(nested loop, strided, random) is expressed as Polydat nodes rather
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
sum   := "x + y = {x + y}"
slot  := "row {row.ordinal}"
```

The body inside each `{ … }` is parsed as a full Polydat expression
— bare identifiers, function calls, infix arithmetic, and field
access all work, exactly the same as on the right-hand side of
any binding. The compiler:

1. Splits the literal into segments: literal text + placeholder
   bodies. The scan is brace-aware (parens / brackets nest)
   and string-aware (a `}` inside a `"…"` doesn't terminate the
   placeholder).
2. Lexes and parses each placeholder body via the same
   expression parser the rest of the language uses.
3. Emits a `printf(fmt, expr1, expr2, ...)` call where `fmt`
   is the literal segments joined by `{}` placeholders, in the
   same positional order as `expr1, expr2, ...`.

`{{` and `}}` are printf's own escapes for emitting literal
braces — they keep their meaning and don't open a placeholder.
A printf format spec the user wrote by hand
(`"x={:05}"`, `"{0:.3}"`) isn't a valid Polydat expression, so the
literal stays unchanged and the user's format spec reaches
printf intact. An unbalanced `{` likewise leaves the literal
alone.

This is pure syntactic sugar — no special runtime support is
needed beyond the standard `printf` node. Iteration variables
that appear inside string literals
(`vector_dim("{dataset}:{profile}")`) flow through the same
wire mechanism as any other identifier reference: they're
declared as `extern` ports on the scope, the runner sets them
per iteration, and the dataset function reads its `source`
input wire at eval time.

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

GK supports arithmetic, bitwise, comparison, and power
operators with standard precedence. Operators desugar to
function calls in the DAG — `a + b` becomes `f64_add(a, b)`,
`a & b` becomes `u64_and(a, b)`, `a < b` becomes `u64_lt(a, b)`
or `f64_lt(a, b)`.

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

// Comparisons (yield u64 truth: 0 or 1)
hot   := err_rate > 0.05
exact := flags == 0
```

**Precedence** (lowest to highest, follows Rust):

| Level | Operators | Associativity |
|-------|-----------|---------------|
| 1 | `==` `!=` (equality) | left |
| 2 | `<` `>` `<=` `>=` (relational) | left |
| 3 | `\|` (bitwise OR) | left |
| 4 | `^` (bitwise XOR) | left |
| 5 | `&` (bitwise AND) | left |
| 6 | `<<` `>>` (shifts) | left |
| 7 | `+` `-` (add/sub) | left |
| 8 | `*` `/` `%` (mul/div/mod) | left |
| 9 | `**` (power) | right |
| 10 | `-` `!` (unary neg/not) | prefix |

Parentheses override precedence: `(a + b) * c`. Comparison
binds looser than arithmetic and bitwise, so `a + b < c * d`
parses as `(a + b) < (c * d)`. Equality is below relational, so
`a < b == c` parses as `(a < b) == c`.

**Operator → Node mapping:**

| Operator | Node function |
|----------|--------------|
| `+` `-` `*` `/` | `f64_add`, `f64_sub`, `f64_mul`, `f64_div` |
| `%` | `f64_mod` |
| `**` | `pow` |
| `&` `\|` `^` | `u64_and`, `u64_or`, `u64_xor` |
| `<<` `>>` | `u64_shl`, `u64_shr` |
| `==` `!=` | `u64_eq` / `u64_ne` (or `f64_*` if either operand is f64) |
| `<` `>` `<=` `>=` | `u64_lt` / `u64_gt` / `u64_le` / `u64_ge` (or `f64_*`) |
| `!` (prefix) | `u64_not` |
| `-` (prefix) | `f64_sub(0.0, x)` |

Comparison results are always `u64` truth values (0 = false,
1 = true) regardless of operand types — they compose cleanly
with bitwise operators (`a < b & c < d`) and with the `if(...)`
intrinsic below.

### Conditional Selection — `if(cond, a, b)`

`if` is a compiler intrinsic, not a registered function: at
compile time it desugars to `select_u64(cond, a, b)` or
`select_f64(cond, a, b)` based on the inferred types of `a`
and `b`. When one branch is u64 and the other f64, the u64
branch is auto-widened via `to_f64`. The condition is u64 —
any nonzero value selects `a`, zero selects `b`.

```
// Step throttle: above a 5%-error threshold the multiplier
// drops to 0.5; below it sits at 1.05.
factor := if(err_rate > 0.05, 0.5, 1.05)

// Mixed branches widen automatically (cycle is u64, 100.0 is f64).
default := if(cycle == 0, 100.0, to_f64(cycle))

// Pure u64 stays u64.
clamped := if(x > 1000, 1000, x)
```

Both branches are *always* evaluated — there is no short-
circuit. `if` is an expression-level select, not a control-flow
construct. Side-effecting nodes inside an unselected branch
still run (they're part of the DAG); design accordingly.

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
polydat[advisory]: widening u64 → f64 in operator *
```

### Auto-Conversion to String

Auto-adapters are one half of the compiler's type-and-value
wiring story. The other half is assertion nodes: runtime guards
the compiler can splice in when it can't *prove* a wire already
satisfies a downstream node's contract. Both are invisible to the
module author — adapters handle type coercion, assertions handle
value validity — and both are skipped whenever the static type
system already proves the wire is safe. The host's input-validity
model defines the full two-layer design (unsafe-by-default fast path,
opt-in strict wire guards, const constraint metadata, type and
value assertion families).

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
| Info | `polydat[info]` | Normal compilation steps |
| Advisory | `polydat[advisory]` | Implicit conversions, type widenings — review for module design quality |
| Warning | `polydat[warning]` | Potential performance or correctness issues |

Query advisories with `--diagnose` to review all implicit
conversions in your module:

```bash
nbrs bench Polydat mymodule.gk --explain
# Shows: polydat[advisory]: type adapter U64→F64: cycle → sin
# Shows: polydat[advisory]: widening u64 → f64 in operator *
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

Braces in binding values trigger compile-time evaluation:

```
dim := {vector_dim("glove-25-angular")}    // implicit
dim := {:=vector_dim("...")}              // explicit-open
dim := {:=vector_dim("..."):=}            // explicit-bracketed
```

Resolution: named-binding lookup first, then const-eval
fallback (per [GK Evaluation Model](evaluation_model.md)'s
compile-const lifecycle), then error. The explicit `{:=...}`
forms bypass the binding lookup and force const evaluation.

The same `{...}` form is what activity config fields parse
— the syntax is shared across the DSL and the YAML config
surface. The const-evaluation API and embedding mechanics
are formalised in
[expression_engine.md §3.1](../design/expression_engine.md);
the host-side resolution order and param-substitution
interaction are a host concern.

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
a `polydat[advisory]` diagnostic. Narrowing (f64 → u64) is never
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
Constant Folding ▶ Evaluate compile-const nodes (no
  │               extern / cycle-input dependency), replace
  │               with leaf const nodes — see
  │               [GK Evaluation Model](evaluation_model.md)
  │
  ▼
GkProgram ──────▶ Immutable compiled DAG (shared via Arc)
```

The Output Selection step's host-facing details — which op
fields, params, and extra bindings count as output consumers —
are a host concern, documented by the application that embeds polydat.

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
    Json(serde_json::Value),
    Ext(Box<dyn ReflectedValue>),
    Handle(Arc<dyn Any + Send + Sync>),
    VecF32(Arc<[f32]>),
    VecI32(Arc<[i32]>),
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

`Handle` is the typed-resource carrier (`PortType::Handle`):
an `Arc<dyn Any + Send + Sync>` produced by resolver nodes
(e.g., `dataset_open`) and consumed by reader nodes that
downcast to the concrete resource type. Cloning a `Value::Handle`
during input-gather is one `Arc::clone` (atomic increment,
zero allocations) — the design that lets resolved resources
flow on wires between scope-stable resolvers (compile-const or
scope-init) and per-cycle readers without re-doing the
resolution work. See
[GK Evaluation Model](evaluation_model.md) §"Three
Evaluation Lifecycles" for the lifecycle taxonomy; the host's
dataset-handle surface is the canonical use case.

`VecF32` / `VecI32` are typed-vector carriers
(`PortType::VecF32`, `PortType::VecI32`) — `Arc<[f32]>` and
`Arc<[i32]>` respectively. They flow on wires the same as any
other value, but adapter binding code can serialize them
directly (`SerializeValue` for `[T]` writes wire bytes
without intermediate boxing). Cloning is one `Arc::clone`,
zero allocations. The `to_display_string()` fallback renders
them as JSON-array text (`"[0.1,0.2,...]"`), so workloads can
mix typed-vector and string-substitution paths without a
separate node family. (Adapter-side native-vector binding is a
host concern.)

---

## Node Contract

Every node implements `PolydatNode` (defined in `polydat/src/ast.rs`):

```rust
pub trait GkNode: Send + Sync {
    fn meta(&self) -> &NodeMeta;
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]);
    fn commutativity(&self) -> Commutativity { Commutativity::Positional }
    fn accepts_none_inputs(&self) -> bool { false }
    fn compiled_u64(&self) -> Option<CompiledU64Op> { None }
    fn jit_constants(&self) -> Vec<u64> { Vec::new() }
    fn purity(&self) -> Purity { Purity::Pure }
}
```

`NodeMeta` declares:
- `name: String` — function name for DSL and diagnostics
- `ins: Vec<Slot>` — input port names and types (Slot::Wire or Slot::Const)
- `outs: Vec<Port>` — output port names and types

Nodes default to `Purity::Pure`; nodes with observable
side channels (logging, file I/O) override `purity()` per
the substrate's D2 axiom. The slot contract is formalised
in
[polydat composition_substrate.md §2](../design/composition_substrate.md).

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
to its own output buffer slots in `PolydatState`.

---

## Incremental Invalidation

**Design topic for Memo:** The current implementation resets all
GK state on input mutation. This is correct but wasteful —
nodes that don't transitively depend on the changed input don't
need re-evaluation.

The target model: **provenance-based invalidation**. When an
input (graph input or externally-written port value) changes,
only nodes downstream of that input are invalidated. This requires:

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

The shipped runtime implementation lives in
[polydat runtime_model.md §3-§4 (R1, R2)](../design/runtime_model.md);
the `node_clean` + `input_dependents` mechanism is the
hybrid push/pull realisation of the design above. The
"design topic for Memo" framing predates the implementation
and is preserved here as historical context — reconciliation
should collapse this section into runtime_model R2.

---

## Polydat Scope Model

GK programs exist within a scope hierarchy formed by the
scenario tree (workload root, phases, `for_each` iterations,
scope groups). Each scope is a self-contained kernel that
sees its outer scopes' values via auto-generated `extern`
input slots. The full model — scope hierarchy, visibility
and mutability rules, lifecycle configuration
(`loop_scope` / `iter_scope`), and the auto-extern
composition mechanism — is specified in
[scope_model.md](scope_model.md), with axiom-level
coverage in
[composition_substrate.md](composition_substrate.md).

The language-level surface that intersects scopes — op-level
bindings (which are syntactic sugar, not new scopes) and
cursor declarations — is documented host-side, by the
application that embeds polydat.
