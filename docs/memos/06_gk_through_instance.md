# Memo 06: GK as Through Instance — Unified Value Routing

The GK kernel should be the single path through which all values
flow: workload params, cycle-time bindings, inter-op captures,
and validation ground truth. This eliminates the four separate
routing mechanisms currently in the codebase.

---

## Current State: Four Routing Mechanisms

```
Workload YAML params ──▶ string substitution (pre-compile)
                           └─▶ {param:name} qualifier (cycle-time)
                                  └─▶ Arc<HashMap> on FiberBuilder

GK bindings ───────────▶ kernel evaluation (per-cycle)
                           └─▶ {bind:name} qualifier

Captures ──────────────▶ CaptureContext (per-stanza)
                           └─▶ sticky/volatile ports
                           └─▶ {capture:name} qualifier

Extra bindings ────────▶ resolve_with_extras hack
                           └─▶ validation ground truth
```

Each mechanism has its own resolution path, its own storage,
and its own qualifier syntax. They interact at `resolve()` time
via a priority chain. This works but has friction:

- Params are expanded pre-compile (string substitution), which
  means GK can't optimize based on param values at compile time
- Captures go through a side channel (CaptureContext → sticky
  ports) instead of being regular GK values
- Extra bindings exist solely because validation needs GK
  outputs that aren't in op fields
- The resolver has special-case logic for each source

---

## Proposed: Single GK Path

```
All values ─────────────▶ GK kernel
                           ├── global inputs (params, config)
                           ├── per-evaluation inputs (cycle)
                           ├── per-stanza inputs (captures)
                           └── all outputs (op fields, validation, config)
```

### Input Lifecycles

Every GK input has a lifecycle that determines when it changes
and how it affects downstream evaluation:

| Lifecycle | Scope | Changes When | Example |
|-----------|-------|-------------|---------|
| `global` | All fibers, all cycles | Never (set at startup) | `keyspace`, `dataset`, `k` |
| `cycle` | Per-fiber, per-evaluation | Every `set_inputs()` call | `cycle` |
| `stanza` | Per-fiber, per-stanza | On capture application | `username` from SELECT |

### Global Inputs

Workload params become `global` GK inputs. Set once at fiber
state creation. From the kernel's perspective, they're constants
— downstream nodes fold at init time.

```
// Current (pre-compile string substitution):
params:
  dataset: glove-25-angular

bindings: |
  inputs := (cycle)
  dim := vector_dim("{dataset}")   // {dataset} replaced before compile

// Proposed (GK global input):
bindings: |
  inputs := (cycle)
  global dataset := "glove-25-angular"   // or from CLI override
  dim := vector_dim(dataset)             // dataset is a graph input
```

The `global` keyword declares an input that is provided at
runtime but constant across all evaluations. The compiler knows
it won't change, so `vector_dim(dataset)` folds to a constant
at init time — same optimization as today, but through the GK
graph instead of string substitution.

CLI override: `dataset=mnist-784` sets the global input value
before compilation, overriding the YAML default. The compiler
sees the override value and folds accordingly.

### Per-Stanza Inputs (Captures)

Captures become `stanza`-lifecycle GK inputs. When a capture is
applied (`fiber.apply_captures()`), it writes to the
corresponding GK input, invalidating downstream nodes.

```
bindings: |
  inputs := (cycle)
  stanza username := ""           // default empty, populated by capture

  // Downstream node uses capture value
  greeting := printf("Hello, %s", username)
```

When op A captures `username = "alice"`, the stanza input is
updated. Op B's resolution of `{greeting}` sees `"Hello, alice"`
because the GK kernel re-evaluates `printf` with the new input.

This eliminates:
- `CaptureContext` as a separate type
- Sticky/volatile ports as special wire sources
- `{capture:name}` as a distinct qualifier
- The `apply_captures()` bridging step

Captures just write GK inputs. The graph handles the rest.

### Qualifier Simplification

With everything flowing through GK, the qualifier system
simplifies:

| Current | Proposed |
|---------|----------|
| `{bind:name}` | `{name}` — all names are GK outputs |
| `{capture:name}` | `{name}` — captures are GK inputs → outputs |
| `{input:name}` | `{name}` — graph inputs are just inputs |
| `{param:name}` | `{name}` — params are global inputs |

Every `{name}` resolves from one namespace: GK outputs. No
priority chain, no ambiguity, no qualifiers needed. If strict
mode wants disambiguation, the lifecycle is the qualifier:
`{global:keyspace}`, `{stanza:username}`, `{cycle:hash_value}`.

---

## Global Inputs Across Fibers

Global values are resolved once at startup and stored on the
`GkProgram` itself — not on per-fiber state, not in a separate
params map, and never re-resolved from external sources.

```
Startup (once):
  CLI + YAML + env → resolve params → HashMap<String, String>
  compile GK (globals from params) → GkProgram
    └── global_values: Vec<Value>   ← stored on the program

Per fiber (N times):
  program.create_state()
    └── copies global_values into state input slots (memcpy)
    └── pass 2 folding: fold nodes downstream of globals
```

```rust
pub struct GkProgram {
    nodes: Vec<Box<dyn GkNode>>,
    wiring: Vec<Vec<WireSource>>,
    input_names: Vec<String>,
    output_map: HashMap<String, (usize, usize)>,
    // NEW: resolved global values, immutable after construction
    global_values: Vec<(String, Value)>,
}

impl GkProgram {
    pub fn create_state(&self) -> GkState {
        let mut state = GkState::new(/* ... */);
        // Initialize global input slots from program's stored values
        for (name, value) in &self.global_values {
            if let Some(idx) = self.global_input_index(name) {
                state.inputs[idx] = value.clone();
            }
        }
        state
    }
}
```

**No re-resolution.** Fibers don't go back to params, CLI, or
any external source. The program holds the resolved values.
`create_state()` copies them into the new state's input slots.

**No shared mutable state.** The program is immutable (`Arc`).
Each fiber's state has its own copy of the global values in its
input buffer. The copy is a value clone, not a reference.

**Eliminates `Arc<HashMap<String, String>>` on FiberBuilder.**
Currently each fiber carries two shared structures: the program
and the params map. With globals on the program, it's one.

---

## Constant Folding with Runtime Globals

Currently, constant folding happens at compile time on the
`GkProgram`. With global inputs, folding happens in two passes:

**Pass 1 (compile time):** Fold nodes whose inputs are all
source-code literals. Same as today.

**Pass 2 (fiber creation time):** With global input values
provided, fold additional nodes whose inputs are now all known.
`vector_dim(dataset)` doesn't fold at pass 1 (dataset is an
input, not a literal), but folds at pass 2 when `dataset =
"glove-25-angular"` is provided.

This two-pass approach means the `GkProgram` contains the
unfolded graph, and each `GkState` gets a per-fiber folded
view. In practice, all fibers fold the same way (same globals),
so the folding result is identical across fibers — but each
fiber independently computes it to avoid shared mutation.

Optimization: fold once in the first fiber, cache the result,
copy to subsequent fibers. But this is a performance concern,
not a correctness concern.

---

## What This Eliminates

| Current Mechanism | Replaced By |
|-------------------|-------------|
| Pre-compile string substitution | `global` inputs + constant folding |
| `CaptureContext` | `stanza` inputs |
| Sticky/volatile ports | Input lifecycle (`stanza`/`cycle`) |
| `resolve_with_extras()` | All outputs in one namespace |
| `{param:name}` qualifier | Just `{name}` |
| `{capture:name}` qualifier | Just `{name}` |
| `{bind:name}` qualifier | Just `{name}` |
| `{input:name}` qualifier | Just `{name}` |
| Four-source priority chain | Single GK output namespace |

---

## What This Preserves

- **Per-fiber isolation.** Each fiber has its own `GkState`.
  No shared mutable state.
- **Constant folding.** Global inputs fold at init time.
  Cycle-time inputs don't. Same optimization model.
- **Determinism.** Same cycle + same globals + same captures
  = same outputs.
- **Zero-cost for simple workloads.** Workloads with no
  captures and no global inputs behave identically to today.

---

## Migration Path

1. **Add `global` input lifecycle to GK.** Alongside existing
   `cycle` inputs. Compiler recognizes `global name := default`
   syntax.

2. **Two-pass folding.** First pass at compile time (literals),
   second pass at fiber creation (globals provided).

3. **Route params through GK.** Runner provides param values as
   globals instead of pre-compile string substitution.

4. **Route captures through GK.** `fiber.capture()` writes to
   stanza-lifecycle inputs instead of CaptureContext.

5. **Remove CaptureContext.** All capture state lives in GkState.

6. **Remove extra bindings.** Validation ground truth is a
   regular GK output (its input is `cycle`, which is already
   available).

7. **Simplify qualifiers.** Single namespace, lifecycle-based
   qualifiers optional for strict mode.

Steps 1-3 can ship independently. Steps 4-7 follow as the
architecture proves out.

---

## Open Questions

- **Type system.** Global inputs are currently strings (from
  YAML params). Should GK support typed global inputs
  (`global k: u64 = 100`)? Or convert from string at the
  boundary?

- **Invalidation scope.** When a stanza input changes (capture
  applied), which downstream nodes re-evaluate? Currently all
  nodes reset on `set_inputs()`. With lifecycle-aware
  invalidation, only nodes downstream of the changed input
  would re-evaluate. (Connects to Memo 06 incremental
  invalidation study topic.)

- **Compile-time vs runtime global distinction.** Should the
  compiler distinguish between "this global is known at compile
  time" (foldable) vs "this global is provided at runtime"
  (foldable only at fiber creation)? Or treat them uniformly
  and let the two-pass folding handle it?
