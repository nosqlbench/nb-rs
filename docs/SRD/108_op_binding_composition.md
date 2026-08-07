# SRD-108 — Blueprint/Implementation Op Composition

Status: IMPLEMENTED. Coverage: `nbrs-workload/src/implements.rs`
unit tests (binder rules), `nbrs/tests/tag_composition.rs` (Part
A e2e), `nbrs/tests/implements_binding.rs` (Part B e2e — both
invocation forms, unbound-slot load error, target mismatch). Two
composition forms for binding a **blueprint** — a
protocol-agnostic workload carrying phases with concurrency,
extents, stop conditions, metrics, provenance classes, and op
*placeholders* — to physical op implementations, without the
blueprint knowing the protocol. "Blueprint" is the term of art
throughout: the fully specified, driver-independent form of a
testing method, awaiting realization by an implementation.

## The invariant that shapes everything

Binding is a **load-time** operation. Both forms below resolve
fully during workload parse, and the interface's type proof lands
during pre-map **synthesis** — the same place every wire is
type-checked today (op-template kernel compilation, auto-extern,
port types). The execution tree the walker receives is exactly as
resolved and type-safe as it is now; nothing about polydat kernel
composition changes, and no check moves into a runtime critical
section.

## Part A — tag-contract composition (ad-hoc form)

SRD-20 documents that a phase declares "inline ops or a tag
filter to select from blocks"; the selector half was never
implemented (a selector-only phase panicked at dispatch with
"must have at least one op"). This SRD completes it:

- A phase with **no inline ops** and a `tags:` selector resolves
  its ops at parse time (Stage 6.5, after the op pool is
  assembled and auto-tagged) by applying the existing
  `TagFilter` grammar to the merged document's top-level and
  block ops. Selected ops are cloned onto the phase, gaining the
  `phase` auto-tag their inline siblings get.
- A selector that matches **nothing** is a load error naming the
  phase and the filter — never a runtime panic. (The dispatch
  panic becomes a structured error regardless, as a backstop.)
- A phase with BOTH inline ops and a selector is rejected at
  load — a phase has exactly one source of ops
  (never-ignore-silently).

Composition then rides `extends:` unchanged: the blueprint
declares selector-only phases (`tags: "role:load"`); an
implementation workload extends it and contributes blocks of
protocol ops tagged to match. The contract is implicit — tag
strings and wire names — which is exactly what makes this the
ad-hoc form: quick to wire, weakly checked (unmatched selector =
load error; a missing yielded wire surfaces at synthesis as an
ordinary unresolved-wire error).

## Part B — typed interface binding (`abstract:` / `implements:`)

### Vocabulary

The blueprint declares op **slots** — ops whose body is an
interface instead of adapter fields:

```yaml
phases:
  probe:
    cycles: "{query_count}"
    concurrency: "{query_concurrency}"
    ops:
      search:
        abstract:
          needs:              # wires the blueprint scope GUARANTEES
            query_vector: vec_f32
            suite_k: u64
          yields:             # wires the implementation MUST deliver
            key: String       # relevancy scoring reads `key`
```

An implementation workload names its blueprint and provides
concrete bodies keyed by `<phase>.<op>` slot coordinates, using
the ordinary phase/op shape (only op-level content is legal —
scaffolding belongs to the blueprint):

```yaml
implements: vector_suite_blueprint

phases:
  probe:
    ops:
      search:
        prepared: |
          SELECT key FROM {keyspace}.{table}
          ORDER BY value ANN OF {query_vector} LIMIT {suite_k}
        captures: "[@key]"
```

### Binding semantics (parse time)

- Invocation forms, both supported:
  - `workload=<impl>` — the impl's `implements:` pulls the
    blueprint (resolved local-first, then bundled catalog,
    exactly like `extends:` targets) and binds into it.
  - `workload=<blueprint> impl=<impl>` — the blueprint is the
    entry point; `impl=` names the implementation. When both
    forms are in play the impl's declared `implements:` must
    resolve to the same blueprint — a mismatch is a load error
    naming both.
- The bound op = the blueprint slot's op with the
  implementation's contribution folded in:
  - implementation **op fields** (the adapter request shape,
    `adapter:`, `captures:`) fill the slot;
  - **bindings** concatenate blueprint-first (dataset access and
    other protocol-agnostic generation stays blueprint-side);
  - any key BOTH sides declare is a load error (the blueprint
    scaffolding is authoritative; an implementation may not
    silently override `metrics:`, `evaluations:`, `condition:`,
    tags, or any other blueprint surface).
- Coverage is total, both directions, at load:
  - a remaining unbound `abstract` op → error naming the slot
    ("abstract op 'probe.search' unbound — pass impl= or run an
    implementing workload");
  - an implementation entry naming no blueprint slot → error;
  - an implementation phase/op carrying scaffolding fields →
    error.
- Both documents may use `extends:` chains; each side resolves
  its own chain first, then binding runs on the merged results.

### Where each check lives

| Check | When | Mechanism |
|---|---|---|
| slot coverage (both directions), scaffolding-field rejection, key collisions, `implements:` target identity | load (parse) | the binder, structured errors naming slot coordinates |
| `yields` names present | load (parse) | slot's bound op must declare each yield as a capture `as`-name (or a standard result wire) |
| `needs` names available | load (parse) | each need must be a declared param, workload/phase binding output, or cursor of the blueprint |
| `results` names delivered (SRD-109 Part 3) | load (parse) | slot's bound op must declare each results name as a `result:` binding LHS; a blueprint-side `result:` entry for an interface name is a collision (paths are protocol matter) |
| `needs`/`yields`/`results` TYPES | pre-map **synthesis** | when the op-template kernel compiles, every interface name that materializes as a program slot is verified against its port type (capture-only yields deliver through the wrapper stack's wire writes and hold no slot; their presence was the load-time check; `results` wires are *declared from* their interface type) — the same pass that type-checks every other wire |
| everything else (wire resolution, adapter fields) | synthesis, unchanged | normal GK compile; SRD-30 unknown-field rejection |

The interface types use the polydat DSL type vocabulary (`u64`,
`f64`, `String`, `vec_f32`, …). Load-time checks are *named*
and *early*; the synthesis check is the *proof*, in the same
place it has always been.

### Provenance interaction (SRD-106/107)

Binding happens before the workload model reaches provenance, so
the bound ops are ordinary phase config: the SRD-107 config
digest covers the implementation's request shapes, and the
consumed-params closure sees the implementation's programs.
Swapping implementations re-runs idempotent prereqs (different
digest — correct: a different protocol produced different
physical state); re-running the same blueprint+impl pair skips
as usual.

## Non-goals (this SRD)

- No multi-impl mixing in one run (one `impl=`; per-phase or
  per-adapter mixing is future work if a real case demands it).
- No driver-manifest / registration layer (the web-client driver
  packaging builds ON this — separate SRD when that phase
  starts).
- No changes to `extends:` merge semantics.
