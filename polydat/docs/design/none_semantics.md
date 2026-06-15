# None Semantics

The polydat-internal contract for how `Value::None` flows
through the kernel and the language surface. This doc
extends three axiom-level statements with the mechanism
detail they reference:

- [composition_substrate.md T1 (typed return)](composition_substrate.md)
  — None as the canonical "absent" sentinel in the Value enum.
- [runtime_model.md D1 (typed return)](runtime_model.md)
  + clean-flag interaction at the runtime layer.
- [grammar.md G1 (auto-extern)](grammar.md)
  — interaction with the interpolation boundary and the
  conditional-shadow `const` compilation rule.

*(NORMATIVE — Rule 1, Rule 3, and the conditional-shadow `const` compiler change all IMPLEMENTED; Rule 2 explicit-optionality syntax — DESIGN, not yet implemented)*

## Motivation

Polydat's `Value` enum carries `Value::None` as the canonical "absent"
sentinel. The kernel's name-resolution chokepoints
(`PolydatKernel::get_constant`, `PolydatKernel::lookup`) already treat
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
`polydat/src/nodes/format.rs::Printf::eval`.

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
   PolydatKernel::get_constant("X") filters Value::None → returns None
   ↓
   PolydatKernel::lookup("X") falls through to find_input tier
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
`polydat/src/dsl/parser.rs`. Each variant lowers to a
GK call that the kernel can JIT (e.g.
`coalesce(X, "default")` for `{X ?? "default"}`).

### Rule 3 — Op-template render refuses silent None *(IMPLEMENTED)*

Status: **shipped**. `Value::to_display_strict` lives in
`polydat/src/node.rs` and returns `Option<String>` (None for
`Value::None`, Some(string) otherwise). `substitute_via_wires` in
`nbrs-activity/src/wires.rs` uses the strict primitive and returns
an explicit "wire resolved to `Value::None`" error naming the
bind-point and pointing operators at the resolution options
(workload-param default, `bindings:` / `set:`, or optional-syntax
opt-in once Rule 2 lands). Unit tests:
`substitute_via_wires_errors_when_wire_resolves_to_none` and
`to_display_strict_returns_none_for_value_none`.

The render path is the wire-protocol boundary — bytes leaving via
this path go to a remote system, so the silent-empty default
that `to_display_string` used to provide had to be removed here.
`to_display_string` stays unchanged for log / diagnostic contexts
where empty is acceptable (it's renamed conceptually as the "lossy"
form; render sites must use the strict variant).

A complementary surface for structural omission (whole
`key: value` segments elided when the value is absent) is
parked under Rule 2 below — the unmarked-bind-point-is-error
default from Rule 3 makes the explicit-optionality syntax
necessary for legitimate optional cases, but the syntax
itself isn't wired up yet.

## Interaction with `set:` and the GK-grammar invariant

The workload-parser sugar `set: { X: "{Y}" }` desugars to:

```
const X := "{Y}"
```

This is canonical Polydat grammar — the desugar produces no
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

## Conditional-shadow semantics for `const` *(IMPLEMENTED)*

Status: **shipped**. `polydat/src/dsl/compile.rs` (both
the main `compile()` and `compile_filtered()` paths) detects
const declarations whose RHS references at least one name
(via the existing `dsl::validate::collect_references` walker)
and emits an implicit `extern NAME: Ext` input slot in
addition to the const output. The two-tier read in `lookup`
then provides the fall-through automatically:

1. `get_constant(NAME)` — own scope's folded output. Real
   value → return it. `Value::None` → **fall through**
   (existing filter, polydatkernel.rs:458-462).
2. `find_input(NAME)` — own scope's input slot, populated at
   `materialize_wiring_from_outer` time from the outer
   scope's `NAME` binding (if any). If wired, return that
   value.

Net behavior: `const NAME := <expr>` becomes a *conditional
shadow* when its RHS could fold to None. Real value → shadows
outer. None → outer's binding shows through. The `set:` sugar
from SRD-73 is unchanged canonical GK; the new semantics
emerge from the compiler's handling of every const, not from
special-casing the sugar.

**Pure-literal exception (SRD-13f Gate 2):** consts whose RHS
has zero name references (e.g. `const x := 1` from polydat's
per-iteration comprehension synthesis path — see polydat spec
§9.5's `scope_once`) are NOT auto-externed. They always fold
to a real value, never reach the fall-through path, and must
not appear as input slots per the Gate 2 invariant. The
reference-presence check is the precise discriminator.

**Wiring composition (`materialize_wiring_from_outer`):** the
read invariant from SRD-13f §"The read invariant" requires
`inner.read_input(X) ≡ outer.lookup(X)`. For const outputs
where the buffer may hold `Value::None` (Rule 1) but
`outer.lookup` falls through to outer's wired-from-grandparent
slot, the wiring uses value-copy via `outer.lookup` instead
of cell-attach (polydatkernel.rs:711-727). Cell-attach would
broadcast the raw `None` buffer and defeat the chain walk;
value-copy of `lookup` result keeps inner aligned with the
invariant. For non-const computed outputs (per-cycle dynamic
values) cell-attach is still the right primitive — those
truly need live broadcast.

Note: when both outer and inner declare `const NAME := <lit>`
with non-None values, inner's shadow wins via the normal
get_constant path — the fall-through only fires when the
inner const evaluates to None.

## Test contract

Four layers of test coverage, mirroring the rules:

1. `polydat/src/nodes/format.rs::tests` — Printf eval
   directly. `printf_none_input_yields_none`,
   `printf_partial_none_taints_whole_result`, and
   `printf_all_present_unchanged` are the normative tests
   for Rule 1. Existing tests (`printf_simple`,
   `printf_multiple`, etc.) act as regression guards.

2. `polydat/tests/scope_composition.rs` — scope-
   composition integration tests proving the const →
   get_constant → lookup chain:
   - `const_with_bound_interpolation_shadows_outer` — happy
     path (real value shadows).
   - `const_with_unbound_interpolation_falls_through_to_outer`
     — conditional-shadow fall-through (None → outer wins).
   - `three_scope_chain_transitive_fall_through` — None in
     a middle scope is transparent across descendants
     (covers Step 2's wiring fix).
   - `pure_literal_const_does_not_auto_extern` — Gate 2
     invariant preserved.

3. Polydat's comprehension synthesis test suite includes a
   Gate 2 regression guard (`iter_var_as_final_const`) that
   protects pure-literal iter-var consts from getting
   auto-externed.

4. `nbrs-activity/src/wires.rs::tests` — Rule 3 strict
   rendering:
   - `to_display_strict_returns_none_for_value_none` — the
     primitive's contract.
   - `substitute_via_wires_errors_when_wire_resolves_to_none`
     — render-time refusal of None substitution.
   - `substitute_via_wires_errors_on_unresolved_name` —
     unchanged behavior for the existing unresolved-name
     diagnostic.

## Phased delivery

**P1 — Rule 1** *(SHIPPED)*. Single-site change in
`Printf::eval`. Existing kernel filter machinery handles the
rest. No syntax additions.

**P2 — Conditional-shadow `const`** *(SHIPPED)*. Compiler
emits implicit auto-extern for every `const NAME := <expr>`
whose RHS references at least one name (the Gate 2 invariant
is preserved by skipping pure-literal consts).
Materialize-wiring updates: for const outputs, value-copy via
`outer.lookup` replaces cell-attach, so the chain-walked
fall-through value propagates through any number of scope
layers.

**P3 — Rule 3 strict rendering** *(SHIPPED)*. `Value::
to_display_strict` returns `Option<String>` (None for
`Value::None`). `substitute_via_wires` migrated to use it
and returns an explicit "wire resolved to None" error naming
the bind-point. Adapter renderers that build wire-protocol
bytes through `substitute_via_wires` inherit the strict
behavior automatically.

**P4 — Rule 2 syntax** *(DESIGN — not yet implemented)*.
`{X ?? default}` and `{X?}` parser additions; optional-
structural-segment `?'key': '...'` renderer support. Lands
when real workloads demand the expressivity that P3's
hard-error default forces. Until then, every bind-point must
resolve to a real value somewhere in the scope chain — the
P3 error names the field if any don't.

## Why this is safe

- **The desugar invariant is preserved.** `set:` writes
  canonical GK; the new semantics emerge from how Polydat compiles
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

- [Scope Model](scope_model.md) — scope chain, visibility
  rules, two-tier lookup. The existing machinery this SRD
  leverages.
- [Wire Materialization](wire_materialization.md) —
  cross-scope wire materialization invariants. Conditional-
  shadow `const` extends the read invariant: a None const
  output is structurally identical to "no const declared" for
  consumers.
