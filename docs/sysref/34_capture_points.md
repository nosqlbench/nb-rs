# 34: Capture Points and Inter-Op Data Flow

Captures enable data flow between operations within a stanza.
A SELECT result feeds a subsequent UPDATE. A token from one
request authenticates the next.

**Status:** Core capture flow implemented with automatic
linearization (dependency group analysis). Target design: GK as
unified state holder for all inter-op data (sysref 10).

---

## Capture Context

Each fiber owns a `CaptureContext` — a key-value store for
captured values:

```rust
pub struct CaptureContext {
    values: HashMap<String, Value>,
    cycle: u64,
}
```

### Lifecycle

1. **Stanza start**: `fiber.reset_captures(base_cycle)` — clears
   all captured values
2. **After each op**: `fiber.capture(name, value)` — stores a
   captured value from the result
3. **Before next window**: `fiber.apply_captures()` — writes
   captured values to sticky ports in GkState
4. **Resolution**: `{capture:name}` in bind points reads from
   the capture context

### Isolation

Captures are scoped to a single stanza:
- Within a stanza: ops can read captures from earlier ops
- Across stanzas: captures reset, no leakage
- Across fibers: each fiber has its own context, no sharing

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
    prepared: "UPDATE users SET name = {capture:username} WHERE id = {id}"
```

`{capture:username}` reads from the capture context. Also
available as unqualified `{username}` (capture is checked after
GK bindings).

---

## Extraction Mechanism

The `TraversingDispenser` extracts captures from the result:

1. Check if template has capture declarations (parsed at init)
2. If yes, serialize result body to JSON via `to_json()`
3. Look up each capture source name in the JSON
4. Store as `Value` in the result's `captures` HashMap
5. Executor stores captures in `CaptureContext`

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
  → captures from A and B collected after window completes

fiber.apply_captures()

Window 2: [op_C, op_D] execute concurrently
  → op_C and op_D can see captures from window 1
```

---

## Port Types

GkState has two external port types for captures:

- **Volatile**: reset to defaults on `set_inputs()`.
  Per-cycle external inputs.
- **Sticky**: persist across `set_inputs()` within a stanza.
  Used for capture flow — a captured value from op A remains
  available for op B's GK evaluation even though inputs
  changed.

`apply_captures()` writes captured values to sticky ports.
`reset_captures()` clears both captures and sticky ports.
