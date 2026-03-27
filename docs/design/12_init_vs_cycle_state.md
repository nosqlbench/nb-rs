# Init-Time vs. Cycle-Time State

> **Note:** The finalized syntax (`init name = expr` vs `name := expr`)
> is documented in `14_unified_dsl_syntax.md`. The `$` sigil explored
> in this document was superseded by the `init` keyword.

Two distinct lifecycles of data exist in the GK. Both must be
first-class in the design.

---

## The Two Lifecycles

### Cycle-time state

The data that flows through wires on every evaluation. This is what
the GK has modeled so far: coordinates enter, values propagate through
nodes, output variates emerge. Changes every cycle.

- **Lifetime:** one coordinate context (one `eval()` call).
- **Cost model:** must be fast — this is the hot path.
- **Examples:** hashed coordinates, decomposed dimensions, sampled
  values, formatted strings.

### Init-time state

Configuration data computed once at assembly time and frozen for the
duration of the session. These are "initialization vectors" — too
expensive or nonsensical to recompute per cycle, but required by nodes
to function.

- **Lifetime:** entire session (from assembly to teardown).
- **Cost model:** can be expensive to build — it happens once.
- **Examples:** interpolation tables (LUTs), alias tables, CSV
  datasets, connection strings, pre-computed CDFs, lookup indices.

Init-time state can have **cascading dependencies**: a distribution
table might depend on a shape parameter loaded from a config file,
which was resolved from an environment variable. These dependencies
form their own DAG — resolved once, at assembly time, before the
cycle-time DAG begins executing.

---

## The Design Gap

The current DSL and assembly API only model cycle-time wiring. Init-
time state is handled implicitly:

- In the **programmatic API**, it's constructor arguments:
  `IcdSample::normal(72.0, 5.0)` builds the LUT inside `new()`.
- In the **DSL**, it's invisible. There's no way to express "build a
  normal distribution LUT with these parameters" as a distinct step.

This works for simple cases but breaks down when:
- Init-time values depend on other init-time values.
- The user wants to share an init-time artifact (e.g., one LUT used
  by multiple sampling nodes).
- Init-time configuration comes from external sources (files, env
  vars, CLI args) that need to be resolved before node construction.

---

## Proposed Model

### Inferred classification with opt-in pinning

The lifecycle of each wire is **inferred from graph topology**, not
declared by default. A wire is init-time if and only if its entire
upstream subgraph terminates at literals, constants, and init-time
sources — with no path back to any coordinate input.

The assembly phase traces each wire's provenance:
- If all roots are literals, config sources, or other init-time
  values → **init-time**. Resolved once at assembly.
- If any root is a coordinate input → **cycle-time**. Evaluated per
  cycle.

This means most init-time classification happens automatically. The
user doesn't need to annotate anything in the common case:

```
// The assembly phase infers that temp_lut is init-time because
// dist_normal's inputs are both literals — no coordinate dependency.
temp_lut := dist_normal(72.0, 5.0)

coordinates := (cycle)
seed := hash(cycle)
quantile := unit_interval(seed)

// quantile is cycle-time (depends on cycle). temp_lut is init-time.
// The assembly phase sees this and freezes temp_lut at init.
temperature := lut_sample(quantile, temp_lut)
```

### The `@` sigil: deliberate pinning

The `$` prefix is an **assertion**, not a declaration. It says: "this
binding must be init-time; reject the DAG if it isn't."

```
// The user is deliberate: this must not be cycle-variant.
$temp_lut := dist_normal(72.0, 5.0)

// If someone later accidentally wires a cycle-time value into
// temp_lut's upstream, the assembly phase errors — the @
// constraint is violated.
```

Without `$`, the system still infers init-time correctly. With `$`,
the user adds a guard rail that catches accidental cycle-time
contamination of expensive initialization paths.

### Warnings and errors

The assembly phase applies these rules:

1. **Cycle-time wire into an init-only port → error.** Some node
   inputs are inherently init-time (e.g., the LUT input to
   `lut_sample`, the weights input to `alias_table`). Nodes declare
   which inputs are init-only. Wiring a cycle-time value there is
   always an error — you can't rebuild a LUT every cycle.

2. **`$`-pinned binding has cycle-time dependency → error.** The user
   asserted init-time, but the graph says otherwise. Assembly rejects
   with a clear message showing the offending path.

3. **Cycle-time wire into an expensive node → warning.** If the
   assembly phase detects a cycle-time wire feeding a node that is
   known to be expensive (LUT construction, CSV loading), it warns
   even without `$` pinning. The user can suppress the warning by
   explicitly acknowledging the cost.

### Init-time dependency chains

Init-time bindings can reference each other. Dependencies are resolved
in topological order at assembly time:

```
base_path := env("DATA_DIR", "/default/data")
dataset := load_csv("{base_path}/sensors.csv", "name", "weight")
sensor_table := alias_table(dataset)
```

All three are inferred as init-time (no coordinate inputs anywhere
in the chain). The assembly phase resolves them eagerly, once, before
the cycle DAG begins.

### Sharing init-time artifacts

An init-time binding referenced by multiple cycle-time nodes is
resolved once and shared:

```
lut := dist_normal(0.0, 1.0)

// Both nodes share the same precomputed table.
temp_a := lut_sample(quantile_a, lut)
temp_b := lut_sample(quantile_b, lut)
```

No duplication — both nodes hold a reference (`Arc`) to the same
frozen data. This is automatic; the user doesn't request sharing.

---

## Rust Implementation Sketch

### Init-time values

```rust
/// An init-time value: built once, frozen, shareable.
pub enum InitValue {
    Scalar(f64),
    Str(String),
    Lut(Arc<LutF64>),
    AliasTable(Arc<AliasTableU64>),
    Dataset(Arc<Vec<(String, f64)>>),
    // Extensible as needed.
}
```

### Node port annotations

Nodes declare which input ports are init-only:

```rust
pub struct Port {
    pub name: String,
    pub typ: PortType,
    pub lifecycle: Lifecycle,  // Init or Cycle
}

pub enum Lifecycle {
    /// This port accepts cycle-time (per-evaluation) values.
    Cycle,
    /// This port requires init-time (frozen) values. Wiring a
    /// cycle-time value here is a compile error.
    Init,
}
```

For example, `LutSample` declares:
- `input: f64, Cycle` — the quantile, varies per cycle.
- `table: LutF64, Init` — the precomputed table, frozen.

### Assembly flow

```
1. Parse DSL → identify $ pinned bindings (optional).
2. Trace provenance for every wire:
   - If all roots are literals/config → classify as init-time.
   - If any root is a coordinate → classify as cycle-time.
3. Validate:
   - Cycle-time wire into Init port → error.
   - $ pinned binding with cycle-time dependency → error.
   - Cycle-time wire into expensive node → warning.
4. Resolve init subgraph eagerly (topological order).
5. Construct cycle-time nodes, injecting resolved init values.
6. Wire cycle DAG, validate types, topological sort.
7. Compile (Phase 1 or Phase 2).
```

---

## What This Enables

- **Explicit lifecycle boundaries.** The user sees exactly what's
  computed once vs. per-cycle. No hidden costs.
- **Init-time dependency chains.** A LUT can depend on a config param
  that depends on an environment variable — all resolved before the
  first cycle.
- **Shared artifacts.** One alias table or LUT used by many nodes,
  without duplication.
- **External data sources.** CSV files, HDF5 datasets, environment
  variables — all resolved at init time, not on the hot path.
- **Clear cost model.** Init-time can be slow (file I/O, table
  construction). Cycle-time must be fast. The `@` marker makes this
  obvious.

---

## Open Questions

- ~~Q21: resolved — `$name` for init-time pinning, `${name}` for the
  strict/explicit form. Familiar shell-variable syntax, visually
  distinct from bare identifiers (cycle-time wires) and `{name}`
  string interpolation bind points.~~
- Q22: Should init-time bindings support a limited expression language
  (arithmetic, string interpolation) or just function calls?
- Q23: How do init-time bindings interact with the `TEMPLATE(key,
  default)` parameterization macro? (TEMPLATE is itself an init-time
  concept — it resolves at parse time, before assembly.)
- Q24: Should `Arc` sharing be automatic (all init values are
  `Arc`-wrapped) or opt-in? (Proposed: automatic — the assembly phase
  wraps all init-time artifacts in `Arc` since they are immutable and
  potentially shared.)
- Q25: How should the "expensive node" warning be calibrated? Should
  nodes self-report their cost class (cheap/expensive), or should the
  assembly phase infer it from node type?
