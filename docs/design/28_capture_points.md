# SRD 28 — Capture Points and Inter-Op Data Flow

## Overview

Capture points enable data to flow from the output of one operation
to the input of another within a stanza. They bridge the deterministic
GK generation world with the non-deterministic runtime world — a
query result contains values that only exist after execution, and
those values must feed into subsequent operations.

This SRD covers the capture mechanism, the inter-op GK wiring model,
stanza-scoped isolation, and the serialization format requirements
for externalized data flow.

## Capture Declaration Syntax

Capture points use `[name]` syntax in op templates, consistent with
Java nosqlbench:

```yaml
ops:
  read_user:
    stmt: "SELECT [username], [balance] FROM users WHERE id={id};"
  update:
    stmt: "UPDATE accounts SET owner={capture:username} WHERE amount={amount};"
```

- `[name]` — capture field `name` from the operation result
- `[source as alias]` — capture `source` under the name `alias`
- `[(Type)name]` — capture with a type qualifier (see Type Qualification)

Wildcard `[*]` is not supported. Captures must be explicitly named.

## Qualified Bind Points

Bind points support optional namespace qualifiers to disambiguate
between value sources. The syntax is `{qualifier:name}`:

| Qualifier | Source | Example |
|-----------|--------|---------|
| (none) | Auto-resolved (see below) | `{user_id}` |
| `coord:` | Coordinate input | `{coord:cycle}` |
| `capture:` | Capture context (volatile or sticky port) | `{capture:balance}` |
| `port:` | External input port (explicit) | `{port:auth_token}` |
| `bind:` | GK binding output | `{bind:hashed_id}` |

`coord:` and `coordinate:` are interchangeable. `port:` is an alias
for explicit external input port access — `capture:` and `port:` both
read from the same port slots, but `capture:` implies the value came
from an op result while `port:` is for ports set by other means.

### Resolution Order (unqualified)

When a bind point has no qualifier (`{name}`), it is resolved in
this order:

1. GK binding outputs (names defined in the `bindings:` section)
2. Capture context (volatile and sticky ports)
3. Coordinate inputs

The **first match wins**. If a name exists in multiple namespaces,
the unqualified form resolves to the highest-priority source.

**Ambiguity (default mode):** If a name exists in exactly one
namespace, unqualified access is fine. If a name exists in multiple
namespaces, unqualified access is **allowed** but the compiler emits
a warning suggesting qualification.

**Ambiguity (strict mode):** If a name exists in multiple namespaces,
unqualified access is a **compile error**. All ambiguous bind points
must use qualifiers.

### Strict Mode Bind Point Requirements

In `--strict` mode:

- All bind points referencing captures must use `{capture:name}`
- All bind points referencing coordinates must use `{coord:name}`
- Unambiguous names (only one source) may still be unqualified
- Ambiguous names require qualifiers — unqualified is a compile error

```yaml
# Strict mode — all qualified
stmt: "INSERT INTO t (id, ts, user) VALUES ({bind:user_id}, {coord:cycle}, {capture:username});"

# Default mode — unqualified is fine when unambiguous
stmt: "INSERT INTO t (id, ts, user) VALUES ({user_id}, {cycle}, {username});"
```

### Coordinates as Bind Points

Coordinates are accessible as bind points alongside GK outputs.
The raw cycle value (or any named coordinate) can be used directly
in op templates:

```yaml
bindings: |
  user_id := mod(hash(cycle), 1000000)

ops:
  insert:
    # {cycle} reads the coordinate directly, {user_id} reads the GK output
    stmt: "INSERT INTO t (seq, id) VALUES ({coord:cycle}, {user_id});"
```

Previously, coordinates were only visible inside the GK kernel.
Now they're also available at the op template level.

## Type Qualification

Operation results are not always self-describing. A CQL row has
column types, but an HTTP response body is opaque bytes until parsed.
To enable type-safe GK wiring between ops, capture points may carry
type qualifiers:

```yaml
stmt: "SELECT [(u64)user_id], [(f64)balance], [(String)name] FROM users WHERE id={id};"
```

Type qualifiers serve two purposes:
1. **Validation**: The adapter asserts the captured value matches the
   declared type at runtime. Mismatch is a hard error.
2. **GK wiring**: When a captured value feeds into an inter-op GK
   kernel, the type qualifier determines the port type of the
   external input, enabling AOT compilation and type checking.

When the adapter can infer types (e.g., CQL column definitions),
type qualifiers are optional — the adapter provides them. When the
adapter cannot infer types (e.g., HTTP JSON body), qualifiers are
required for any captured value that feeds into a typed GK kernel.

## Inter-Op GK Wiring

### The Problem

Today, GK kernels are attached to op templates as bindings — they
generate values that feed INTO an operation. Capture points create
values that come OUT of an operation. The gap: how do output values
from one op become input values for the next op's GK kernel?

### Architectural Boundary: GK Stays Pure

GK kernels are **pure-function DAGs** for deterministic data
generation. They have no side effects, no non-deterministic outputs,
and their properties (P3 JIT, shared immutable code, zero-contention
threading) depend on this purity.

Operations have side effects and non-deterministic outputs. They are
NOT GK nodes. The capture system bridges these two worlds without
merging them:

```
┌──────────────────────────────────────────────────────┐
│  GK kernel (pure, deterministic, JIT-compiled)       │
│    coordinates → [nodes] → named outputs             │
└──────────────────┬───────────────────────────────────┘
                   ↓ bind point values
┌──────────────────┴───────────────────────────────────┐
│  Op execution (side effects, non-deterministic)      │
│    adapter.execute(assembled_op) → result             │
└──────────────────┬───────────────────────────────────┘
                   ↓ captured values
┌──────────────────┴───────────────────────────────────┐
│  Capture context (stanza-scoped key-value store)     │
│    name → Value                                       │
└──────────────────┬───────────────────────────────────┘
                   ↓ read by next op's bindings
┌──────────────────┴───────────────────────────────────┐
│  GK kernel for next op (pure, may read from context) │
│    coordinates + context values → named outputs       │
└──────────────────────────────────────────────────────┘
```

These are **three separate runtime layers**, not one merged graph.

### Inter-Op Data Flow

When a captured value needs transformation before feeding into the
next op, the next op's GK kernel handles it. The GK kernel reads
captured values through **external input ports** — named values
injected from the capture context, distinct from coordinate inputs:

```yaml
ops:
  read:
    stmt: "SELECT [(u64)user_id], [(f64)balance] FROM users WHERE id={id};"
  write:
    bindings: |
      adjusted_balance := mul(balance, 110)  // 10% increase
      target_bucket := mod(user_id, 100)
    stmt: "INSERT INTO ledger (bucket, amount) VALUES ({target_bucket}, {adjusted_balance});"
```

Here `balance` and `user_id` are external input ports on the write
op's GK kernel. They come from the capture context, not from
coordinates. The GK kernel is still pure — it takes inputs and
produces outputs — it just has an additional input source.

### Direct Capture Pass-Through

When captures flow directly to bind points without transformation,
no GK kernel is needed — the executor reads directly from the
capture context:

```yaml
ops:
  read:
    stmt: "SELECT [username] FROM users WHERE id={id};"
  write:
    stmt: "UPDATE log SET user={username} WHERE ts={ts};"
```

The compiler sees `{username}` in the write op, finds no binding for
it, finds a capture `[username]` in a preceding op, and resolves it
as a direct context read. No inter-op GK kernel compiled.

### External Input Ports

GK kernels that consume captured values have **external input ports**
beyond their coordinate inputs. External ports are declared with a
lifecycle mode and an optional default value.

#### Declaration

```gk
// Volatile: resets to default on each set_coordinates() call.
// Must be re-populated by a capture each stanza.
extern volatile balance: f64 = 0.0
extern volatile row_id: u64            // no default — unwired access is error

// Sticky: persists across coordinate changes until explicitly overwritten.
// Initialized to default at kernel construction time.
extern sticky auth_token: String = "anonymous"
extern sticky session_id: u64 = 0
```

#### Terminology: "Wired"

A port is **wired** if it has any way to receive a value — whether
from a coordinate, a capture, a default, a GK binding, or a direct
assignment. "Wired" means "there exists a path to a value." An
unwired port has no source and no default — reading it is an error.

#### Lifecycle Modes

| Mode | On set_coordinates() | Use case |
|------|---------------------|----------|
| **Volatile** | Resets to declared default (or unset if no default) | Per-cycle capture results: query fields, response values |
| **Sticky** | Retains current value | Session-level state: tokens, IDs captured once and reused |

The default value applies at two points:
1. **Init time**: Before any capture has fired, the port holds its
   declared default.
2. **Reset time** (volatile only): When `set_coordinates()` is called
   (new cycle/stanza), volatile ports revert to their default.

Sticky ports never auto-reset — they hold whatever was last written
until explicitly overwritten by a new capture.

#### Failure Modes

| State | Meaning | Behavior |
|-------|---------|----------|
| **Wired** | Has a path to a value (capture, default, binding) | Normal evaluation |
| **Undefined** | Name not declared anywhere | Compile error |
| **Unwired** | Declared but no capture, no default, no binding | Compile warning; runtime error on read |

The compiler validates at AOT time that all port names are either
wired or have a declared default. Undefined references are compile
errors. Unwired ports (declared volatile, no default, no upstream
capture) produce a compile warning and a runtime error if read.

#### Buffer Layout

Volatile and sticky ports are **structurally separated** in the
buffer so that resetting volatile ports is a single fast memcpy
of the defaults, without touching sticky state or node outputs:

```
┌────────────┬───────────────┬──────────────┬──────────────────┐
│ Coordinates│ Volatile ports│ Sticky ports │ Node outputs     │
│ [0..C)     │ [C..C+V)      │ [C+V..C+V+S)│ [C+V+S..)        │
└────────────┴───────────────┴──────────────┴──────────────────┘
```

On `set_coordinates()`:
- Slots `[0..C)` — written with new coordinate values
- Slots `[C..C+V)` — **bulk reset**: copy pre-built defaults array
  (`memcpy` of V slots, one operation)
- Slots `[C+V..C+V+S)` — untouched (sticky, retain values)
- Slots `[C+V+S..)` — node output cache invalidated (new generation)

The defaults array for volatile ports is built once at assembly
time and reused on every coordinate reset. Resetting V volatile
ports costs one `memcpy` regardless of how many there are.

When a capture fires, the executor writes the value directly into
the port's buffer slot — volatile or sticky. The GK kernel reads
it on the next evaluation like any other input.

#### Linearized Op Flow with Port Wiring

With external input ports, the full stanza data flow becomes a
linear sequence of port writes and GK evaluations:

```
1. set_coordinates(cycle)        → writes coord slots, resets volatile slots
2. eval GK kernel for op1        → reads coords, writes bind point values
3. execute op1                   → adapter runs, returns result
4. capture op1 result            → writes to volatile/sticky port slots
5. eval GK kernel for op2        → reads coords + port slots, writes bind points
6. execute op2                   → adapter runs, returns result
7. capture op2 result            → writes to port slots
   ...
```

Each step either writes to port slots or reads from them. The GK
kernel doesn't know whether a value in its buffer came from a
coordinate, a capture, or a default — it just reads the slot.

This means the full stanza flow is **observable as a linear sequence
of slot writes and GK evaluations**. A diagnostic tool can display
this as a table:

```
Step  Action                Slot writes            Slot reads
────  ────────────────────  ─────────────────────  ──────────────
1     set_coordinates(42)   cycle=42, balance=0.0  —
2     eval GK (op1)         user_id=527897         cycle
3     execute op1           —                      user_id
4     capture op1           balance=123.45         —
5     eval GK (op2)         adjusted=135.80        balance
6     execute op2           —                      adjusted
```

This is a presentation of the runtime behavior, not a change to
the GK model. The GK kernels at steps 2 and 5 are still pure
functions — they just have additional input slots populated by
the executor between evaluations.

## Stanza-Scoped Isolation

### Stanza as Isolation Unit

The **stanza** (not the stride) is the isolation boundary for
capture state. A stanza is one complete pass through the op sequence
for a given cycle. Within a stanza:

- Operations execute in sequence
- Captures from op N are available to op N+1, N+2, etc.
- The capture context is cleared at the start of each stanza

This aligns with the op sequencing model from SRD 22: the stanza
is the natural unit of work, and captures are stanza-local state.

### Capture Context

```rust
struct CaptureContext {
    /// Named values captured from operation results.
    values: HashMap<String, Value>,
    /// The cycle this context is evaluating.
    cycle: u64,
}

impl CaptureContext {
    fn reset(&mut self, cycle: u64) {
        self.values.clear();
        self.cycle = cycle;
    }

    fn set(&mut self, name: &str, value: Value) {
        self.values.insert(name.to_string(), value);
    }

    fn get(&self, name: &str) -> Option<&Value> {
        self.values.get(name)
    }
}
```

Each executor thread has its own `CaptureContext`, reused across
cycles with `reset()` at stanza start. No cross-thread sharing.

### Interior Concurrency

Future enhancement: operations within a stanza may execute
concurrently when their data dependencies allow it. This is a
stanza-internal scheduling optimization, not a change to the
isolation model. The capture context remains single-threaded per
stanza — concurrent ops within a stanza would need dependency
analysis to determine which can overlap.

This is a form of stanza inner-wiring at the task fiber level —
the stanza's extended GK graph determines the parallelism
opportunities.

## Visualization (Presentation Only)

For user inspection, the full stanza data flow can be rendered as
a unified diagram — but this is a **presentation aid**, not a
runtime model. The runtime remains three separate layers (GK
kernels, op execution, capture context).

```
nbrs describe flow workload=my_workload.yaml
```

The diagram would show:
- Coordinate inputs
- Binding GK nodes (pure, compiled)
- Op nodes (marked as side-effecting, non-deterministic)
- Capture wires (context reads, not GK edges)
- Inter-op GK nodes (pure, compiled, reading from context)

Operations appear visually as nodes with inputs/outputs, but they
are **not** GK nodes at runtime. The diagram conflates the three
layers for readability. The underlying execution model respects
the boundary: GK is pure, ops have side effects, captures are a
separate key-value context.

## Serialization Format Requirements

Captured values and inter-op data flow need an externalizable format
for:
- Debug inspection (what was captured at each op?)
- Replay (re-inject captured values without executing the op)
- Wire protocol (distributed stanza execution across nodes)
- Persistent capture logs for post-hoc analysis

### Requirements

| Requirement | Priority | Notes |
|-------------|----------|-------|
| Human-readable | High | Debug and inspection |
| Schema-describable | High | Type safety across boundaries |
| Streaming-friendly | High | Per-cycle capture events |
| Binary-efficient | Medium | Wire format for distributed execution |
| Self-describing | Medium | Schema embedded or referenced |
| Widely supported | Medium | Tooling ecosystem |

### Format Candidates

**JSON / JSONL** — Primary human-readable format. JSONL (one JSON
object per line) is natural for per-cycle capture events. Widely
supported, self-describing, but verbose and no native binary types.

**CBOR** — Binary JSON-compatible format (RFC 8949). Compact, fast
to parse, supports binary blobs natively. Schema-compatible with
JSON tooling. Good wire format candidate.

**MessagePack** — Similar to CBOR. Slightly more compact, widely
adopted. Less formally specified than CBOR.

**CDDL** — Concise Data Definition Language (RFC 8610). Not a wire
format itself — it's a schema language for describing CBOR/JSON
structures. Supports:
- Precise type constraints
- Optional/required field marking
- Regex patterns for string fields
- Range constraints for numeric fields

CDDL schemas could describe the capture context structure, enabling
validation at compile time and across wire boundaries.

**Flatbuffers / Cap'n Proto** — Zero-copy binary formats. Maximum
performance but require schema compilation. Overkill for capture
contexts unless distributed execution becomes primary.

### Recommended Approach

| Use case | Format |
|----------|--------|
| Debug / human inspection | JSON (pretty) or JSONL |
| Capture event log | JSONL (streaming, one event per line) |
| Wire protocol | CBOR (binary JSON, compact, schema-compatible) |
| Schema definition | CDDL (describes both JSON and CBOR structures) |
| Configuration / workload | YAML (existing, keep as-is) |

CDDL schemas describe the shape of capture contexts. The same
schema validates JSON (for debugging) and CBOR (for wire format).
This gives one schema language across both human-readable and
machine-efficient representations.

### Requirements Study — Next Steps

A focused study is needed to evaluate:
1. CBOR vs MessagePack for the binary wire format
2. CDDL tooling availability in the Rust ecosystem
3. Whether Flatbuffers/Cap'n Proto offer meaningful advantages for
   the distributed execution case
4. Interop with existing nosqlbench tooling that expects JSON

## Adapter Contract for Captures

Each adapter implements capture extraction:

```rust
trait CaptureExtractor {
    type Result;

    /// Extract named values from an operation result.
    ///
    /// Called after successful op execution when the op template
    /// declares capture points. Returns a map of capture name →
    /// typed value.
    fn extract_captures(
        &self,
        result: &Self::Result,
        captures: &[CaptureDecl],
    ) -> Result<HashMap<String, Value>, AdapterError>;
}
```

The adapter is responsible for:
- Extracting the named field from its native result type
- Applying type casting if a qualifier is present
- Returning an error if a declared capture is missing from the result

## Relationship to Other SRDs

- **SRD 02 (Variate Generation)**: GK remains a pure-function DAG.
  Captures do not make GK impure — they're an external input source,
  like coordinates.
- **SRD 22 (Op Sequencing)**: Stanza is the isolation unit for captures
- **SRD 24 (Compilation Levels)**: Inter-op GK kernels compile to the
  same P1/P2/P3 levels as binding GK kernels. External input ports
  are injected the same way coordinates are — as buffer slot values
  set before evaluation.
- **SRD 26 (Coordinate Spaces)**: Captures are NOT coordinates — they're
  external input ports. Coordinates are deterministic (derived from
  cycle). Captures are non-deterministic (derived from op results).
- **SRD 27 (GK Modules)**: Modules can be used in inter-op GK kernels
  the same way they're used in binding GK kernels
