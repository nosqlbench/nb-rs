# Uniform Workload Specification — Assay

Analysis of the nosqlbench workload specification to inform the
nb-workload crate design. The living spec documents are now in
`nb-workload/tests/workload_definition/`.

---

## What It Is

A YAML-based format for defining parameterized, templated database
operations. The specification normalizes many convenient shorthand
forms into a single canonical model that driver adapters consume.

The spec has two audiences:
- **Users** write YAML with flexible shorthand
- **Drivers** consume a normalized `ParsedOp` API

---

## Key Structures

### Document (top level)

```yaml
description: "workload description"
scenarios:           # named execution plans
bindings:            # data generation recipes
params:              # configuration parameters
tags:                # metadata for filtering
blocks:              # groups of operations
```

### Block (within a document)

A named group of ops. Blocks are NOT recursive. Every op in a block
auto-receives a `block` tag with the block's name.

### Op Template (within a block)

A recipe for building one operation at a given cycle. Contains:
- **op fields** — the actual operation payload (stmt, fields, etc.)
- **bindings** — data generation functions (can reference doc/block level)
- **params** — configuration (consistency level, timeout, etc.)
- **tags** — metadata for filtering
- **name** — identifier for diagnostics

### Scenario (execution plan)

Named sequences of activity commands:
```yaml
scenarios:
  default:
    schema: run driver=cql tags==block:schema threads==1
    rampup: run driver=cql tags==block:rampup cycles=100K
    main: run driver=cql tags==block:main cycles=1M
```

Parameter locking:
- `param=value` — user can override
- `param==value` — silently locked
- `param===value` — error on override attempt

---

## Normalization Rules

The spec's core value: many YAML shorthand forms normalize to one
canonical model.

### Op template forms (all equivalent after normalization)

```yaml
# Scalar string
op: "SELECT * FROM t;"

# List of strings
ops:
  - "SELECT * FROM t;"

# Map of strings
ops:
  read: "SELECT * FROM t;"

# Map of maps
ops:
  read:
    stmt: "SELECT * FROM t;"

# Explicit op field
ops:
  read:
    op:
      stmt: "SELECT * FROM t;"
```

All normalize to:
```json
{
  "name": "read",
  "op": { "stmt": "SELECT * FROM t;" },
  "tags": { "name": "read", "block": "block0", "op": "read" }
}
```

### Field names

`op`, `ops`, `operations`, `stmt`, `statement`, `statements` are
all equivalent. Any can be scalar, list, or map.

### Auto-naming

Missing names are auto-generated:
- Blocks: `block0`, `block1`, ...
- Ops: `stmt1`, `stmt2`, ...

### Property inheritance

Document → Block → Op, inner scope wins:
- **bindings**: merged, inner overrides outer
- **params**: merged, inner overrides outer
- **tags**: merged, inner overrides outer

### Auto-tagging

Every op template always receives:
- `block` — containing block name
- `name` — op template name
- `op` — op template name

---

## Template Variables (TEMPLATE macro)

Resolved BEFORE YAML parsing:

```yaml
keycount: TEMPLATE(keycount, 1000000)
```

- `TEMPLATE(name, default)` — use default if not provided
- `TEMPLATE(name)` — required, errors if not provided
- `TEMPLATE(name,)` — null default
- Same variable referenced multiple times → consistent substitution

---

## Bind Points

How dynamic per-cycle values enter op templates:

- `{name}` — reference a named binding
- `{{expr}}` — inline binding definition
- Bind points in values only, never keys

---

## ParsedOp Normalized Model

The canonical form drivers consume:

```json
{
  "name": "op_name",
  "description": "optional",
  "op": {
    "field1": "value or {bindpoint}",
    "field2": "static value"
  },
  "bindings": {
    "bindpoint": "Hash(); ToString()"
  },
  "params": {
    "prepared": true
  },
  "tags": {
    "block": "block_name",
    "name": "op_name",
    "op": "op_name"
  }
}
```

Fields are classified as:
- **Static** — no bind points, constant across cycles
- **Dynamic** — contains `{bindpoint}`, resolved per cycle

---

## Op Synthesis Pipeline (9 stages)

1. Template variable expansion (`TEMPLATE(...)` → values)
2. Jsonnet evaluation (if `.jsonnet` source)
3. Structural normalization (various YAML forms → canonical)
4. Auto-naming (missing names filled)
5. Auto-tagging (block, name, op tags injected)
6. Property de-normalization (inheritance applied)
7. Tag filtering (activity's tag param selects ops)
8. Bind point resolution (strings → functions)
9. Op mapping (driver categorizes and builds dispensers)

---

## SpecTest Format

The living spec uses a three-part test pattern validated at build:

1. `*yaml:*` — YAML source
2. `*json:*` — JSON equivalent (validated against YAML parse)
3. `*ops:*` — ParsedOp API view (validated against normalization)

All markdown files in the spec directory are scanned and verified.
These files are now in `nb-workload/tests/workload_definition/`.

---

## Implications for nb-workload

### Must implement
- YAML parsing with all shorthand forms
- Normalization to canonical ParsedOp model
- Property inheritance (doc → block → op)
- Auto-naming and auto-tagging
- TEMPLATE macro expansion
- Bind point detection (static vs dynamic classification)
- Tag-based op filtering
- Scenario parsing

### Can simplify
- Drop Jsonnet support initially (rare usage)
- Drop Groovy expression preprocessing (`{{= ... }}`) — replace
  with the surface expression language from the GK DSL design
- Drop capture points (`[userid]` syntax) initially

### Must port
- The spectest markdown files as test fixtures — parse the YAML,
  verify JSON equivalence, verify ParsedOp normalization

### Architecture
- `nb-workload` parses YAML → normalized op templates
- `nb-variates` resolves bind points to GK output variates
- The bridge: op template bind points (`{name}`) map to GK named
  output wires, exactly as designed in `02_variate_generation.md` Q9
