# GK DSL — Design Scope

> **Note:** The finalized syntax is documented in
> `14_unified_dsl_syntax.md`. This document retains the design
> exploration history and resolved questions that led there.

The domain-specific language for defining generation kernel DAGs. This is
the primary user-facing syntax for assembling input coordinates, function
nodes, named wires, and output variates into a compiled GK.

---

## Requirements (derived from prior design decisions)

The DSL must express:

1. **Named input coordinates** — the naming tuple (e.g., `tenant`,
   `device`, `timestamp`), all typed as u64.
2. **Function node instantiation** — selecting a node from the standard
   library with its assembly-time parameters (e.g., `Mod(1000)`,
   `Normal(0.0, 1.0)`).
3. **Named wires** — connecting output ports of upstream nodes (or input
   coordinates) to input ports of downstream nodes, by name.
4. **Edge adapters** — optionally attaching type-conversion functions on
   edges (auto-inserted for common coercions, explicit for others).
5. **Named output variates** — designating terminal node outputs as the
   GK's named outputs.
6. **Library constructs** — referencing reusable sub-DAG assemblages by
   name, with named port interfaces.
7. ***-arity** — nodes with any number of input and output ports, each
   with a declared type.

The DSL should also be:

- **Pseudo-visual** — the textual form should make the graph structure
  apparent to the reader.
- **Named, not positional** — all user-facing references use names, not
  indices.
- **Declarative** — describes the graph structure, not an execution
  sequence.
- **Statically validatable** — the assembly phase can fully type-check
  and validate the DAG from the DSL alone.

---

## Working Example

```
coordinates:=(name, age, cc)
assigned_group:=assign_group(region, group_id)
region:=assign_region(name, cc)
group_id:=hash(uniform(age))
bin_def:=create_or_find_bin(region, group_id)
```

### Observations from this example

**Syntax pattern:** `output_name := function(input_wire, ...)`

- The `:=` operator binds a name to the output of a function node.
- Function arguments are references to named wires — either input
  coordinates or outputs of other nodes.
- The graph topology is implicit from name references. No explicit edge
  declarations are needed — wiring is inferred by name matching.
- Input coordinates are declared with a tuple syntax:
  `coordinates:=(name, age, cc)`.
- Functions can be nested inline: `hash(uniform(age))` composes two
  nodes without requiring an intermediate named wire.
- Order of statements does not matter — `assigned_group` references
  `region` which is defined on the next line. The DSL is declarative,
  not sequential. However, referencing a name before it is defined
  produces a warning (not an error) to encourage top-down readability.

**What this implies:**

- The DSL reads like named assignments in a dataflow language.
- The "pseudo-visual" quality comes from being able to trace names
  through the graph by following references.
- Multi-input nodes are natural: `assign_group(region, group_id)` takes
  two named wires.
- Multi-output nodes use destructuring on the left side:
  `(tenant, device, reading) := mixed_radix(cycle, 100, 100, 100)`
  Each name in the tuple binds to the corresponding output port.
- Inline nesting (`hash(uniform(age))`) is syntactic sugar — the
  assembly phase expands it into discrete nodes with auto-generated
  intermediate wire names.
- Output variates are simply the names that downstream consumers
  (op templates) reference — any named wire can be pulled as an output.
- **Lexical convention for arguments:** bare identifiers are wire
  references, numeric literals and quoted strings are constants.
  `mixed_radix(cycle, 100, 100, 100)` — `cycle` is a wire, `100` is a
  constant. Wire names must start with a letter (no numeric-only names)
  to keep the distinction unambiguous.
- **String interpolation sugar:** a quoted string containing `{name}`
  references is syntactic sugar for a string-building node with named
  wire inputs. The user writes:
  ```
  device_id := "{tenant_code}-{device_seq}"
  ```
  The assembly phase normalizes this into a strict internal form — a
  string-building node with `tenant_code` and `device_seq` as explicit
  named wire inputs. The user sees the sugar; the engine sees a lean
  DAG node. This uses the same `{name}` bind-point syntax as op
  templates, keeping the convention consistent across layers.

### User-Facing Sugar vs. Internal Representation

The DSL supports several forms of syntactic sugar that the assembly
phase normalizes into strict DAG form:

| User writes                        | Engine sees                          |
|------------------------------------|--------------------------------------|
| `hash(uniform(age))`               | Two discrete nodes with an auto-named intermediate wire |
| `"{tenant_code}-{device_seq}"`     | String-building node with two named wire inputs |
| `u64` wire into `String` port      | Edge adapter node auto-inserted      |
| Bare `.gk` file                    | Kernel with inferred interface       |

The principle: the user-facing DSL is expressive and convenient; the
internal kernel representation is strict, lean, and optimizable. Sugar
is a formally supported feature, not an afterthought — but the engine
never executes sugar directly.

---

## Library Constructs

GK kernels are composable — a kernel can be used as a node function
inside another kernel.

### Defining kernels

**1. Bare `.gk` file** — a file containing DSL statements defines a
kernel implicitly. Its interface is the ad-hoc collection of unconnected
input parameters (wires referenced but never assigned) and unconnected
output parameters (wires assigned but never consumed within the file).
The kernel name is the file name.

```
// tenant_keygen.gk
hashed_id := hash(tenant)
tenant_name := weighted_lookup(hashed_id, "tenants.csv")
tenant_code := mod(hashed_id, 10000)
```

Here `tenant` is an unconnected input, `tenant_name` and `tenant_code`
are unconnected outputs. The kernel is referenced elsewhere as
`tenant_keygen`.

**2. Formal wrapper** — multiple kernels can be defined in a single file
with explicit type signatures on inputs and outputs:

```
tenant_keygen(tenant: u64) -> (tenant_name: String, tenant_code: u64) := {
    hashed_id := hash(tenant)
    tenant_name := weighted_lookup(hashed_id, "tenants.csv")
    tenant_code := mod(hashed_id, 10000)
}
```

The formal wrapper clarifies the interface contract — input/output
names and types — making the kernel self-documenting and statically
validatable at the embedding site.

### Using kernels as nodes

A kernel is referenced by name, just like any built-in function node:

```
coordinates := (cycle)
(tenant, device, reading) := mixed_radix(cycle, 100, 100, 100)
(tenant_name, tenant_code) := tenant_keygen(tenant)
device_label := device_namer(device)
```

The assembly phase resolves `tenant_keygen` by name — either from the
formal wrapper in the current file, or by locating a `.gk` file with
that name. The input/output wiring follows the same named-parameter
conventions as built-in nodes.

### Interface inference vs. declaration

| Form             | Inputs                      | Outputs                      | Types          |
|------------------|-----------------------------|------------------------------|----------------|
| Bare `.gk` file  | Inferred (unreferenced sources) | Inferred (unconsumed sinks) | Inferred from connected nodes |
| Formal wrapper   | Declared with names and types | Declared with names and types | Explicit       |

The formal wrapper is preferred for shared/library kernels where the
interface must be stable and self-documenting. Bare files are convenient
for ad-hoc or single-use kernels.

### Naming rules for file-based kernels

Within a `.gk` file, the formal wrapper syntax may be used, but with
constraints:

- If the wrapper includes a name, it **must match the file name**
  (e.g., `tenant_keygen` in `tenant_keygen.gk`). A mismatch is an
  error.
- Alternatively, the wrapper can be **anonymous** — just the signature
  without a name — and the file name is automatically used:

```
// tenant_keygen.gk — anonymous wrapper, name comes from file
(tenant: u64) -> (tenant_name: String, tenant_code: u64) := {
    hashed_id := hash(tenant)
    tenant_name := weighted_lookup(hashed_id, "tenants.csv")
    tenant_code := mod(hashed_id, 10000)
}
```

This keeps file-based kernels unambiguous — one file, one name, one
kernel — while allowing the user to add an explicit type signature
without redundantly restating the name.

---

## End-to-End Example: Multi-Tenant Time-Series Workload

A workload with 100 tenants, 1000 devices per tenant, writing sensor
readings. The op template needs `tenant_name`, `device_id`, `timestamp`,
`reading_value`, and `partition_key`.

### GK definition (`timeseries.gk`)

```
// Input: a single cycle coordinate
coordinates := (cycle)

// Decompose cycle into domain dimensions:
//   100 tenants × 1000 devices × unlimited readings
(tenant, device, reading) := mixed_radix(cycle, 100, 1000, 0)

// Tenant identity — hash for dispersion, then derive name and code
tenant_h := hash(tenant)
tenant_name := weighted_lookup(tenant_h, "tenants.csv")
tenant_code := mod(tenant_h, 10000)

// Device identity — hash combines tenant + device for uniqueness
device_h := hash(interleave(tenant, device))
device_seq := mod(device_h, 100000)
device_id := "{tenant_code}-{device_seq}"

// Partition key — composite of tenant and time bucket
time_bucket := div(reading, 1000)
partition_key := "{tenant_code}:{time_bucket}"

// Timestamp — base epoch plus reading offset in milliseconds
timestamp := add(epoch_millis(reading), 1710000000000)

// Sensor reading value — shaped as a normal distribution
reading_h := hash(interleave(device_h, reading))
reading_value := normal(reading_h, 72.0, 5.0)
```

### Op template referencing the GK outputs

```yaml
ops:
  write_reading:
    stmt: |
      INSERT INTO sensor_data
        (partition_key, device_id, timestamp, tenant_name, reading_value)
      VALUES
        ({partition_key}, {device_id}, {timestamp}, {tenant_name}, {reading_value})
```

### What happens at assembly time

1. The DSL is parsed into a DAG of 14 nodes plus the coordinate input.
2. Topological sort orders the nodes (warnings if definition-before-use
   is violated).
3. Type validation checks every edge:
   - `mixed_radix` outputs u64 tuples ✓
   - `hash` takes u64, returns u64 ✓
   - `weighted_lookup` takes u64, returns String ✓
   - `template` takes named wires of any type, returns String —
     auto-inserts u64→String edge adapters for `tenant_code`,
     `mod(device_h, ...)`, and `time_bucket` ✓
   - `normal` takes u64, returns f64 ✓
   - `reading_value` (f64) wired to op template `{reading_value}` —
     auto-inserts f64→String edge adapter ✓
4. The DAG is compiled into a static execution kernel.

### What happens at runtime (per cycle)

1. Control loop sets coordinate context: `cycle = 4201337`
2. Op template pulls `{partition_key}` →
   - pulls `tenant_code` → pulls `tenant_h` → pulls `tenant` →
     pulls `mixed_radix(4201337, ...)` → `(42, 1, 337)`
   - `tenant` = 42, `tenant_h` = hash(42), `tenant_code` = hash(42) % 10000
   - pulls `time_bucket` → pulls `reading` = 337, `div(337, 1000)` = 0
   - `partition_key` = template → "7182:0"
3. Op template pulls `{device_id}` →
   - pulls `device_h` → `hash(interleave(42, 1))`
   - `device_id` = template → "7182-83291"
4. Op template pulls `{timestamp}` →
   - `reading` already resolved (337), `add(epoch_millis(337), ...)` → epoch value
5. Op template pulls `{tenant_name}` →
   - `tenant_h` already resolved (cache node or re-eval, same result)
   - `weighted_lookup(tenant_h, "tenants.csv")` → "Acme Corp"
6. Op template pulls `{reading_value}` →
   - `reading_h` = hash(interleave(device_h, 337))
   - `normal(reading_h, 72.0, 5.0)` → 68.3

Final assembled statement:
```sql
INSERT INTO sensor_data
  (partition_key, device_id, timestamp, tenant_name, reading_value)
VALUES
  ('7182:0', '7182-83291', 1710000337000, 'Acme Corp', 68.3)
```

### What this demonstrates

- **Cartesian decomposition** — `mixed_radix` turns a flat cycle into
  three domain dimensions without manual Div/Mod chains.
- **Shared intermediate computation** — `tenant_h` is used by both
  `tenant_name` and `tenant_code`; `device_h` feeds both `device_id`
  and `reading_h`. The DAG makes sharing explicit.
- **Explicit hashing provenance** — every hash call is visible. The
  user sees exactly where entropy enters (`tenant_h`, `device_h`,
  `reading_h`) and what it's derived from.
- **Pull-through evaluation** — only the outputs referenced by the op
  template are computed. If the op didn't need `tenant_name`, that
  entire branch (including the CSV lookup) would be skipped.
- **Named parameters throughout** — `template("{prefix}-{seq}", ...)`,
  `{partition_key}` in the op template, `coordinates := (cycle)` —
  names everywhere.
- **Auto-inserted edge adapters** — `tenant_code` (u64) flows into a
  template expecting String; the assembly phase inserts the conversion.

---

## Open Questions

- ~~Q11: resolved — line-oriented := assignments; see 06_gk_language_layers.md~~
- ~~Q12: resolved — destructuring tuple on the left side of :=~~
- ~~Q13: resolved — see Library Constructs section below~~
- ~~Q14: resolved — both; YAML block quotes handle embedding naturally~~
- ~~Q15: resolved — lexical convention, identifiers are wires, literals are constants~~
- ~~Q16: resolved — see end-to-end example above and full IR walkthrough in 06_gk_language_layers.md~~
