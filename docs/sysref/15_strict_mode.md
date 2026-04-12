# 15: Strict Mode

Strict mode enforces additional compile-time constraints that
catch common mistakes and ambiguities. It trades convenience for
explicitness — every decision that non-strict mode makes
implicitly must be stated explicitly in strict mode.

---

## Scope

Strict mode applies to:
- GK source compilation (`.gk` files and inline `bindings:`)
- Op template bind point resolution (future)

It does NOT apply to:
- Activity config parameters (these are runtime, not compile-time)
- Error routing specs (runtime config)
- Adapter-specific parameters (validated by each adapter)

Strict mode is a compile-time contract. It catches problems before
any cycles execute, with **zero cost at cycle time**. All strict
mode checks are configuration and initialization time checks — they
add no overhead to the per-cycle execution path.

Strict mode is intended for reusable library modules and
production workloads. Non-strict mode is the default for
interactive development and ad-hoc testing.

Enable via: `compile_gk_strict(source, path, true)` or
`--strict` on the CLI.

---

## Warning Policy

**Every strict-mode condition produces a warning even in
non-strict mode.** The difference between modes:

| Mode | Behavior |
|------|----------|
| Non-strict | Warning to stderr + compile event log |
| Strict | Hard error — compilation fails |

This means users always see diagnostic feedback about potential
problems. Strict mode simply promotes those warnings to errors.
A clean non-strict compile (no warnings) will also pass strict.

---

## Config Wire Promotion (implemented)

Config wire inputs (ports marked `WireCost::Config`) must be
connected to init-time sources in strict mode.

| Mode | Config wire ← cycle-time source |
|------|-------------------------------|
| Non-strict | Warning to stderr + compile event log |
| Strict | Hard error — compilation fails |

This prevents accidental per-cycle LUT rebuilds and similar
expensive recomputation. The user must wire config inputs to
init-time constants or explicitly acknowledge the cost.

**Rationale:** A node like `dynamic_weighted_select` rebuilds its
alias table (O(n)) when the weights spec changes. Wiring this to
a cycle-time source means O(n) work per cycle — almost certainly
a mistake. Strict mode catches it at compile time.

---

## Input Declaration (design target)

In non-strict mode, omitting `inputs := (cycle)` implies a
single `cycle` input. In strict mode, inputs must be declared
explicitly.

| Mode | Missing input declaration |
|------|-------------------------------|
| Non-strict | Implicit `inputs := (cycle)` |
| Strict | Error: "inputs must be declared explicitly" |

**Rationale:** Implicit defaults hide assumptions. A module that
works with `cycle` might silently break when composed with a
multi-input kernel. Explicit declaration makes the interface
contract visible.

---

## Unused Bindings (design target)

A binding that's computed but never consumed by any output or
downstream node.

| Mode | Unused binding |
|------|---------------|
| Non-strict | Silent dead code (pruned by DCE) |
| Strict | Error: "binding 'unused_val' is never referenced" |

**Rationale:** Unused bindings are often typos or leftover
artifacts. In library modules, every binding should have a
purpose. Strict mode forces cleanup.

---

## Implicit Type Coercions (design target)

The compiler auto-inserts type adapter nodes when wiring crosses
types (e.g., `u64 → f64`).

| Mode | Implicit u64 → f64 coercion |
|------|----------------------------|
| Non-strict | Auto-inserted silently |
| Strict | Error: "explicit conversion required: use u64_to_f64(x)" |

**Rationale:** Silent coercion can mask precision loss or semantic
errors. In strict mode, conversions must be spelled out so the
intent is clear. The compiler can suggest the appropriate
conversion function in its error message.

---

## Unqualified Bind Points (design target)

In op templates, `{name}` tries multiple resolution sources in
order: GK binding → capture context → graph input.

| Mode | Unqualified `{name}` in op field |
|------|--------------------------------|
| Non-strict | Resolved by precedence (binding first) |
| Strict | Error: "use `{bind:name}`, `{capture:name}`, or `{input:name}`" |

**Rationale:** When a binding and a capture share the same name,
the binding silently wins. This can hide bugs where a captured
value was intended. Qualified syntax removes the ambiguity.

---

## Non-Deterministic Nodes (design target)

Nodes like `counter`, `current_epoch_millis`, `thread_id` are
non-deterministic — their output varies across evaluations even
with the same inputs. They are excluded from constant folding
but otherwise allowed in non-strict mode.

| Mode | Non-deterministic node without annotation |
|------|------------------------------------------|
| Non-strict | Allowed, excluded from folding |
| Strict | Error: "use a deterministic alternative" |

**Rationale:** Non-deterministic nodes break the core invariant
that the same `(cycle, template)` produces the same output. In
strict mode, their use must be explicitly acknowledged.

---

## Undeclared String Interpolation References (design target)

In string templates like `"hello {name}"`, if `name` doesn't
resolve to any binding, capture, or graph input:

| Mode | Unresolved `{name}` in string |
|------|------------------------------|
| Non-strict | Left as literal text `{name}` |
| Strict | Error: "unresolved reference '{name}' in string template" |

**Rationale:** Leaving `{name}` as literal text is almost always
a typo. Strict mode catches it immediately rather than producing
unexpected output downstream.

---

## Named Function Arguments (design target)

Function calls can use positional or named arguments.

| Mode | Positional arguments |
|------|---------------------|
| Non-strict | Allowed: `hash_range(cycle, 1000)` |
| Strict | Error: "use named arguments: hash_range(input: cycle, range: 1000)" |

**Rationale:** Named arguments are self-documenting. For library
modules that will be read by others, positional arguments obscure
the meaning of each parameter.

---

## Implementation Status

| Rule | Status |
|------|--------|
| Config wire promotion | Implemented | `compile_strict(true)` on assembler |
| Input declaration | Implemented | Strict: error. Non-strict: warning |
| Unused bindings | Implemented | Nodes with no consumers and not in output_map |
| Implicit type coercions | Implemented | Auto-inserted `__adapt_*` nodes detected |
| Non-deterministic nodes | Implemented | Zero-input non-init nodes (counter, etc.) |
| Unqualified bind points | Design target | Requires nb-activity changes |
| Undeclared string refs | Design target | Requires string template resolution changes |
| Named function arguments | Implemented | Already enforced in module calls |
