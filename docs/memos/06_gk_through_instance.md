# Memo 06: GK as Through Instance — Unified Value Routing

The GK kernel should be the single path through which all values
flow: workload params, cycle-time bindings, inter-op captures,
and validation ground truth. This eliminates multiple separate
routing mechanisms.

---

## Current State (What's Done)

### Params Through GK (Done)

Workload params are injected as GK bindings before compilation,
not as runtime globals. The runner performs two phases:

1. **String substitution** inside GK source text: `{dataset}`
   in `vector_dim("{dataset}")` becomes `vector_dim("sift1m")`
2. **Binding injection**: params referenced in op templates
   (e.g., `{keyspace}` in a CQL statement) get injected as
   literal GK bindings: `keyspace := "baselines"`

This means the GK compiler sees only literals — no "runtime
global inputs." Single-pass constant folding handles everything.
`vector_dim("sift1m")` folds to a constant at compile time.

**What this eliminated:**
- No `global` keyword needed in the GK DSL
- No two-pass folding (compile-time pass + fiber-creation pass)
- No `global_values` on `GkProgram`
- No `Arc<HashMap<String, String>>` params on `FiberBuilder`
- Params resolve from the same GK output namespace as bindings

### Qualifier Resolution (Partially Done)

Unqualified `{name}` resolves via priority chain:
1. GK binding output
2. Capture context
3. Coordinate input

For workloads without captures, this already behaves as single
namespace — everything resolves from GK outputs.

### Inline Expressions (Done)

`{expr}`, `{:=expr}`, `{:=expr:=}`, and `{{expr}}` forms are
all rewritten to GK bindings at compile time. The resolver only
sees `{__expr_N}` or `{__inline_N}` references — simple GK
output lookups.

### Extra Bindings for Validation (Done)

`resolve_with_extras()` pulls named GK outputs for validation
ground truth. These are regular GK outputs — the "extra" part
is just telling the resolver which additional names to pull
beyond op template fields.

---

## Remaining Work

### Step 1: Route Captures Through GK State Directly

**Done.** `CaptureContext` has been removed. `FiberBuilder::capture()`
writes directly to GK state ports via `state.set_port(idx, value)`.
Ports persist across `set_inputs()` calls within a stanza and
are reset at stanza boundaries via `fiber.reset_ports()`.

The volatile/sticky distinction has been collapsed to a single
port type. Ports persist by default (sticky semantics). The
provenance system handles invalidation — when a port value
changes, downstream nodes are marked dirty.

### Step 2: Simplify Qualifiers

**Current:** Four qualifier forms exist:
- `{bind:name}` → GK output only
- `{capture:name}` → capture context only  
- `{input:name}` → coordinate value only
- `{name}` → priority chain (GK → capture → input)

**Target:** All names resolve from one namespace. Qualifiers
become optional disambiguation hints, not routing directives.
Since captures will write to GK state ports (step 1), they
become GK outputs too — the priority chain collapses.

| Current | Target |
|---------|--------|
| `{bind:name}` | `{name}` |
| `{capture:name}` | `{name}` |
| `{input:name}` | `{name}` |

Strict mode may retain qualifier syntax for documentation
purposes, but resolution always goes through GK.

### Step 3: Remove `resolve_with_extras`

Once qualifiers are unified, the resolver is a single GK output
lookup. The "extras" parameter is just a list of additional
names to pull — which can be a property of the op template
itself rather than a resolver parameter.

---

## What This Architecture Preserves

- **Per-fiber isolation.** Each fiber has its own `GkState`.
  No shared mutable state.
- **Constant folding.** Params fold at compile time (injected
  as literals). Cycle-time bindings evaluate per cycle.
- **Determinism.** Same cycle + same params + same captures
  = same outputs.
- **Zero-cost for simple workloads.** Workloads with no
  captures behave identically — no capture infrastructure
  is allocated or evaluated.

---

## Open Questions

- **Capture port declaration.** Ports are currently declared
  in GK assembly via `extern name: type = default`. Should
  the compiler auto-declare ports from `{capture:name}`
  references in op templates? Or should the workload YAML
  capture point syntax (`[name]`) auto-declare them?
