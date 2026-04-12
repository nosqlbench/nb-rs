# 34: Capture Points and Inter-Op Data Flow

Captures enable data flow between operations within a stanza.
A SELECT result feeds a subsequent UPDATE. A token from one
request authenticates the next.

**Status:** Core capture flow implemented with automatic
linearization (dependency group analysis). Captured values write
directly to GK ports on the fiber's GkState.

---

## Capture Flow

Captured values write directly to GK state ports — no
intermediate storage. Each fiber owns its own `GkState` with
port buffers that persist across `set_inputs()` calls.

### Lifecycle

1. **Stanza start**: `fiber.reset_ports()` — ports reset to
   defaults (prevents capture leakage across stanzas)
2. **After each op**: `fiber.capture(name, value)` writes
   directly to the matching port in GkState
3. **Resolution**: `{name}` in bind points resolves from GK
   outputs (which may read from ports via graph wiring)

### Isolation

Captures are scoped to a single stanza:
- Within a stanza: ops can read captures from earlier ops
- Across stanzas: ports reset to defaults, no leakage
- Across fibers: each fiber has its own GkState, no sharing

---

## Declaring Captures

### In Op Fields

Use `[name]` syntax to declare capture extraction:

```yaml
ops:
  read_user:
    prepared: "SELECT [username], [age as user_age] FROM users WHERE id = {id}"
```

`[username]` → capture source `username`, alias `username`
`[age as user_age]` → capture source `age`, alias `user_age`

### Consuming Captures

```yaml
ops:
  update_user:
    prepared: "UPDATE users SET name = {username} WHERE id = {id}"
```

Unqualified `{username}` resolves from GK outputs. If the
program has a port named `username` and a GK node wired
to it, the captured value flows through the DAG. The
`{capture:name}` qualifier is also accepted for explicit
disambiguation.

---

## Extraction Mechanism

The `TraversingDispenser` extracts captures from the result:

1. Check if template has capture declarations (parsed at init)
2. If yes, serialize result body to JSON via `to_json()`
3. Look up each capture source name in the JSON
4. Store as `Value` in the result's `captures` HashMap
5. Executor calls `fiber.capture(name, value)` which writes
   directly to GkState ports

Current implementation: top-level JSON field lookup (naive).
Future: JSON pointer paths for nested results
(`rows.0.username`).

---

## Capture and Stanza Concurrency

With `stanza_concurrency=1`: captures flow between every op.
Op A's captures are available to op B.

With `stanza_concurrency=M>1`: captures flow between windows
but NOT within a window. Ops in the same concurrent window
cannot see each other's captures.

```
Window 1: [op_A, op_B] execute concurrently
  → captures from A and B written to GkState after window

Window 2: [op_C, op_D] execute concurrently
  → op_C and op_D can see captures from window 1
```

---

## Port Model

GkState has a single external port type for captures. Ports
persist across `set_inputs()` calls within a stanza and are
reset to defaults at stanza boundaries via `reset_ports()`.

```
extern balance: f64 = 0.0
extern session_id: u64 = 0
```

`fiber.capture(name, value)` writes directly to the matching
port by looking up the name in the program's port definitions.
The provenance system marks downstream nodes dirty when a port
value changes, ensuring re-evaluation of dependent outputs.

---

## Design: GK as Unified State Holder

Captured values write to GK state ports, which are read by GK
nodes via the standard wiring model. This means:

1. Captures are GK inputs — they feed into the DAG
2. Downstream GK nodes can compute derived values from captures
3. Op templates consume those derived values via `{name}`
4. Everything flows through one namespace: GK outputs

The GK kernel acts as the single state holder for all inter-op
data flow. There is no separate capture storage, no priority
chain between multiple resolution sources — just GK state and
GK evaluation.
