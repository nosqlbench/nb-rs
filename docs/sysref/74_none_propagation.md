# SRD 74: None Propagation and Optional-Value Semantics in GK

*(NORMATIVE for Rule 1 — IMPLEMENTED; Rules 2 & 3 — DESIGN, not yet implemented)*

## Motivation

GK's `Value` enum carries `Value::None` as the canonical "absent"
sentinel. The kernel's name-resolution chokepoints
(`GkKernel::get_constant`, `GkKernel::lookup`) already treat
`Value::None` outputs as "not present in this scope" and fall
through accordingly. But the same discipline wasn't applied at
the language surface: string-literal interpolation in source
code and bind-point substitution at op-template render time
both silently coerced `None` into either `Str("")` or the literal
text `"None"` (Debug formatting). This conflated **absent** with
**present-but-empty** — a type-system error that corrupted
downstream wire-protocol bytes (an empty `'source_model': ''`
field reached a CQL cluster; cndb echoed it back as `"NONE"`).

This SRD pins down the semantics: `Value::None` propagates
through every operation that consumes a value, and only converts
to a concrete string when the author explicitly opts in.

The principle is **None-propagation**, the same pattern SQL uses
for NULL (`NULL || 'foo'` → NULL) and Rust uses for `Option`'s
`?` operator. Absence is sticky; mixing it into a string yields
no string at all.

## Three orthogonal rules

The full semantic discipline factors into three rules, applied
at three different sites. Each is independently necessary; the
combination produces the workload-author surface the SRD-73
`set:` desugar (and any other "shadow if present" pattern)
relies on.

### Rule 1 — String-interpolation propagates None *(IMPLEMENTED)*

Status: **shipped** as of this SRD's land commit. See
`nbrs-variates/src/nodes/format.rs::Printf::eval`.

A source-level string literal containing one or more `{X_i}`
placeholders desugars (via `parse_interpolated_string` in
`dsl/parser.rs`) to a `printf(fmt, X_1, ..., X_n)` call. The
`Printf` node's `eval` now applies:

> If any input slot referenced by a `{}` placeholder holds
> `Value::None`, the output is `Value::None`. Otherwise the
> output is the formatted `Value::Str(...)`.

Inputs not referenced by any placeholder (unusual but
permitted by the variadic signature) do not trigger
propagation.

Cascading consequence through `const`:

```
const X := "{Y}"            // Y is unbound (read as Value::None)
   ↓
   printf("{}", Y) → Value::None     // Rule 1
   ↓
   const-fold writes Value::None to X's output buffer
   ↓
   GkKernel::get_constant("X") filters Value::None → returns None
   ↓
   GkKernel::lookup("X") falls through to find_input tier
```

The pre-fix bug was that printf used the catch-all `_ =>
format!("{val:?}")` arm and produced `Value::Str("None")` — a
real string named "None" that *would* be returned by
`get_constant`, shadowing any outer binding for `X` with text
that just happened to be the variant's Debug name.

### Rule 2 — Optional vs required bind-points *(DESIGN)*

A `{X}` bind-point has one meaning today: substitute. With
Rule 1, an unresolved `X` no longer silently substitutes empty
or "None" — but the workload author still needs explicit
control over *what should happen* when `X` is absent:

| Syntax        | Behavior if X resolves to a real value | Behavior if X resolves to None        |
|---------------|----------------------------------------|----------------------------------------|
| `{X}`         | substitute the value                   | propagate None (Rule 1)                |
| `{X ?? Y}`    | substitute the value                   | substitute Y (a literal or other name) |
| `{X?}`        | substitute the value                   | substitute empty string EXPLICITLY     |

The unmarked form propagates None. `{X?}` is the *only* syntax
that produces `""` from an absent value, and it does so by
author opt-in. `{X ?? "default"}` is the explicit-default form.

Parser landing site: extension of `parse_placeholder_body` in
`nbrs-variates/src/dsl/parser.rs`. Each variant lowers to a
GK call that the kernel can JIT (e.g.
`coalesce(X, "default")` for `{X ?? "default"}`).

### Rule 3 — Op-template render refuses silent None *(DESIGN)*

`Value::to_display_string()` (`nbrs-variates/src/node.rs:488`)
currently renders `Value::None` as `""`. Convenient in some
contexts, fatal at the wire-protocol boundary — it pushed the
absent→empty conflation past the kernel and into bytes sent
to remote systems.

The render path (`substitute_via_wires` in `nbrs-activity` and
analogous adapter-side renderers) must use a *strict*
primitive that returns `Option<String>` (or
`Result<String, RenderError>`) and errors on None unless the
bind-point is marked optional per Rule 2. Concretely:

```rust
pub fn to_display_strict(&self) -> Option<String> {
    match self {
        Value::None => None,
        other => Some(other.to_display_string()),
    }
}
```

`to_display_string` stays as a convenience method for callers
that want the legacy lossy behavior; the render path uses
`to_display_strict` and surfaces a clear error when an
unmarked bind-point resolves to None.

A complementary surface for structural omission (whole
`key: value` segments elided when the value is absent):

```
WITH OPTIONS = {
  'similarity_function': '{similarity_function}',
  ?'source_model': '{source_model}',     // entry dropped if source_model is None
}
```

The `?`-prefixed segment is a renderer-level construct: parse
the surrounding YAML literal, identify the segment boundaries,
and elide the segment if any bind-point inside resolves to
None.

## Interaction with `set:` and the GK-grammar invariant

The workload-parser sugar `set: { X: "{Y}" }` desugars to:

```
const X := "{Y}"
```

This is canonical GK grammar — the desugar produces no
special-case AST. The semantic correctness lives in **how GK
compiles and evaluates `const NAME := <expr>`**, not in the
sugar layer.

With Rule 1 alone, `const X := "{Y}"` where `Y` is unbound
yields `Value::None` as X's output buffer. `get_constant`
filters it. `lookup` falls through to `find_input` — which
finds nothing because `set:` only emitted `const X` and not
also `extern X`.

The full "set: as conditional shadow" semantics need one more
compiler-level change, described next.

## Conditional-shadow semantics for `const`

**Design (not yet implemented):** when the GK compiler sees
`const NAME := <expr>`, it implicitly auto-externs `NAME` so
the surrounding scope's binding for `NAME` (if any) is wired
into this scope's input slot at construction time. The
existing two-tier read in `lookup` then provides the fall-
through automatically:

1. `get_constant(NAME)` — own scope's folded output. If the
   const evaluated to a real value, return it. If it
   evaluated to `Value::None`, **fall through** (existing
   filter).
2. `find_input(NAME)` — own scope's input slot, populated at
   `materialize_wiring_from_outer` time from the outer
   scope's `NAME` binding (if any). If wired, return that
   value.

Net behavior: `const NAME := <expr>` becomes a *conditional
shadow*. Real value → shadows outer. None value → outer's
binding shows through. No explicit `extern NAME` declaration
needed from the author.

This is the missing piece for the `set:` sugar to behave the
way authors expect: an attempted shadow that didn't produce a
value just leaves the upstream default in place, instead of
forcing the author to either always-populate or write a
dummy fall-back.

Note: when both outer and inner declare `const NAME := <lit>`
with non-None values, inner's shadow wins via Rule 1 — Rule 1
applies only when the inner evaluates to None.

## Test contract

Three layers of test coverage, mirroring the rules:

1. `nbrs-variates/src/nodes/format.rs::tests` — Printf eval
   directly. `printf_none_input_yields_none` and
   `printf_partial_none_taints_whole_result` are the
   normative tests for Rule 1. Existing tests
   (`printf_simple`, `printf_multiple`, etc.) act as
   regression guards.

2. `nbrs-variates/tests/scope_composition.rs` — scope-
   composition integration tests proving the const →
   get_constant → lookup chain. `const_with_unbound_interpolation_yields_none_locally`
   pins Rule 1 at the scope level;
   `const_with_unbound_interpolation_with_explicit_extern_falls_through`
   demonstrates the two-tier fall-through, anticipating the
   conditional-shadow-for-const change.

3. *(future)* Op-template render-path tests once Rule 3 lands.
   Should cover: required-bind-point None → render error;
   optional-bind-point None → empty-substitution; optional-
   structural-segment None → segment elided.

## Phased delivery

**P1 — Rule 1** (shipped). Single-site change in
`Printf::eval`. Existing kernel filter machinery handles the
rest. No syntax additions.

**P2 — Conditional-shadow `const`** (next). Compiler emits
implicit auto-extern for every `const NAME := <expr>`
declaration. Materialize-wiring runs against the outer scope
at construction; lookup's existing two-tier read does the
fall-through. Should be a small compiler change in
`dsl/compile.rs`.

**P3 — Rule 3 strict rendering**. New `to_display_strict`
primitive; `substitute_via_wires` and adapter renderers
migrate. Adds an "unresolved bind-point" diagnostic class.

**P4 — Rule 2 syntax**. `{X ?? default}` and `{X?}` parser
additions; optional-structural-segment `?'key': '...'`
renderer support. Lands when real workloads demand the
expressivity that P3's hard-error default forces.

## Why this is safe

- **The desugar invariant is preserved.** `set:` writes
  canonical GK; the new semantics emerge from how GK compiles
  `const`, not from special-casing the sugar.
- **Existing workloads where every bind-point resolves to a
  real value keep working unchanged.** Rule 1's hot path is
  one boolean check per placeholder slot — already negligible.
- **Existing workloads with silent-empty-substitution bugs
  fail visibly instead.** Pre-fix: empty string sent to remote
  system, cluster surfaces a confusing error. Post-fix:
  Value::None propagates, eventually hitting either the
  fall-through path (correct), Rule 3's render-time error
  (loud), or — if author opted in — empty substitution
  (explicit).
- **`Value::None` semantics are unified.** Before: kernel
  treated None as "absent" (filter), but printf treated it as
  "render as Debug-string". After: every operation that
  consumes a value either propagates None upward or refuses
  to silently coerce it.

## See also

- [SRD 13c](13c_gk_scope_model.md) — scope chain, visibility
  rules, two-tier lookup. The existing machinery this SRD
  leverages.
- [SRD 13d](13d_op_template_scope.md) — op-template GK scope;
  the rendering site Rule 3 targets.
- [SRD 13f](13f_cross_scope_wire_materialization.md) —
  cross-scope wire materialization invariants. Conditional-
  shadow `const` extends the read invariant: a None const
  output is structurally identical to "no const declared" for
  consumers.
- [SRD 73](73_op_field_modifiers.md) — the immediate consumer
  of `set:` as a configuration-shadow mechanism; provided the
  motivating bug for this SRD.
