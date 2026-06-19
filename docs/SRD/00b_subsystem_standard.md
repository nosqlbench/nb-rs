# 00b: The Subsystem Treatment Standard

This is the rubric every subsystem in nb-rs is held to. **polydat is the worked
exemplar** — it was built to this bar first (standalone crate, rigid contractual
boundary, tiered design docs, CI-gated axioms). The purpose of this document is to
generalize that treatment to *every* subsystem so the whole system reads with one
level of architectural rigor.

A "subsystem" is a workspace crate (or a cohesive cluster — e.g. the adapters) that
owns a contract. Each subsystem's documentation in `docs/SRD` (and, for the
extractable exemplar, in `polydat/docs/`) must satisfy the five pillars below.

---

## The five pillars

### Pillar 1 — Contract surface

The crate's **public exports** (what it offers) and its **required inbound contract**
(what it consumes) are named explicitly. The intended public surface is small and
deliberate; everything else is internal and may change freely.

- *polydat reference:* a tight ~9-module public surface
  (`ast` / `kernel` / `compile` / `dsl` / `library` / `viz` / `binder` / `iteration`)
  exposed from `lib.rs`, with the cross-boundary contract row in
  [SRD 01 §Contract Boundaries](01_system_overview.md) (`PolydatProgram` + `PolydatState`).
- *Anti-pattern:* a crate that re-exports its entire internal module tree as `pub`,
  so consumers reach arbitrarily deep and the "contract" is undefined. `nbrs-runtime`
  is in this state today (~50 public modules) — its Contract section must declare the
  *intended* surface and mark the rest internal, even before the visibility is narrowed.

Every subsystem registers one row in the **Contract Registry**
([SRD 05](05_dependency_rules.md)): `{ crate, public surface, inbound contract, allowed edges }`.
That registry is the single source of truth the CI gate reads.

### Pillar 2 — Axiom tier

The **load-bearing invariants** of the subsystem — the rules a design proposal cannot
contradict. These are short, named, and stable. Mechanism detail delegates *up* to them.

- *polydat reference:* `polydat/docs/design/composition_substrate.md` (the S/T/L
  pillars) and the slot-state axioms S1–S10 (CI-gated in
  `polydat/tests/slot_state_axioms.rs`).
- For nbrs subsystems, the axioms already exist but are scattered across
  [SYSREF.md](../SYSREF.md)'s rules table and the per-SRD prose. The treatment **hoists**
  each subsystem's axioms into a named "Axioms" block at the top of its SRD (e.g. for
  metrics: lock-free reporter, nanoseconds-internal, no std `RwLock` for render state).

### Pillar 3 — Mechanism tier

The concrete, detailed layer: API tables, worked examples, diagnostic formats,
"what works / what doesn't" enumerations. This is the bulk of an SRD. It **cross-references**
the axioms rather than restating them.

- *polydat reference:* the (reduced) sysref polydat-SRDs are mechanism docs that point
  at the axiom docs in `polydat/docs/design/`; the substrate axioms do not live in
  `docs/SRD`.

### Pillar 4 — Cross-reference

Bidirectional traceability: **SRD ↔ crate ↔ top-level modules ↔ tests**. A reader at
any of those four can find the others.

- Each SRD names its owning crate and the modules that implement it.
- Each crate's `lib.rs`/`main.rs` module doc points back at its SRD(s).
- The [00_index.md](00_index.md) row for each SRD carries the owning-crate + module map.

### Pillar 5 — Enforced edges

The subsystem's **allowed dependency edges** and **public-surface boundary** are
machine-checked, not merely documented. Drift fails CI.

- *polydat reference:* rule **D1** (polydat depends only on `polydat-derive`) in
  `nbrs/tests/architecture_rules.rs`.
- The gate reads the Contract Registry (Pillar 1) so the contracts have teeth:
  forbidden crate edges, adapter→adapter edges, upward imports, and reaches past a
  crate's declared public surface all fail.

---

## Subsystem section template (for `docs/SRD`)

Every subsystem SRD should open with these blocks, in order:

```
# NN: <Subsystem>            (owning crate: <crate>; modules: <a, b, c>; tests: <…>)

## Contract                  ← Pillar 1
- Exports: <public types/traits>
- Consumes (inbound contract): <what it requires from below>
- Allowed edges: <see SRD 05 Contract Registry row>

## Axioms                    ← Pillar 2
- A1 …  A2 …                 (named, load-bearing, stable)

## <mechanism sections…>     ← Pillar 3 (the existing detailed content)

## See also                  ← Pillar 4
- crate `lib.rs`, modules …, tests …, related SRDs …
```

---

## Compliance matrix

Tracks each subsystem against the five pillars. Tick as the treatment lands.
(`◐` = partial / pre-existing but not yet hoisted to the standard.)

| Subsystem (crate) | P1 Contract | P2 Axioms | P3 Mechanism | P4 Xref | P5 Enforced | Owning SRDs |
|---|:--:|:--:|:--:|:--:|:--:|---|
| **polydat** (exemplar) | ✅ | ✅ | ✅ | ✅ | ✅ (D1) | **09** contract; substrate in `polydat/docs/` |
| nbrs-runtime | ✅ | ✅ | ✅ | ✅ | ✅ | **29** + 30/31/32/32a/33/34/35/68/71/73/75/76/82/83 |
| nbrs-workload | ✅ | ✅ | ✅ | ✅ | ✅ | **25** + 18/18b–f, 20, 21, 22, 72, 85 |
| nbrs-metrics | ✅ | ✅ | ✅ | ✅ | ✅ | **39** + 40/40a/40b/40c, 42, 43, 24 |
| nbrs-metricsql | ✅ | ✅ | ✅ | ✅ | ✅ (D5-exempt: own contract; L3 atop nbrs-metrics) | **08** + 40c, 47, 48, 49 |
| nbrs-rate | ✅ | ✅ | ✅ | ✅ | ✅ | **06** (+ 02, 23) |
| nbrs-errorhandler | ✅ | ✅ | ✅ | ✅ | ✅ | **07** (+ 03, 82, 83) |
| nbrs-tui | ✅ | ✅ | ✅ | ✅ | ✅ | **59** + 62, 63, 81 |
| nbrs-web | ✅ | ✅ | ◐ | ✅ | ✅ | **54** + (folds `internals/32`) |
| adapters/* | ✅ | ✅ | ✅ | ✅ | ✅ | 30 §Contract (inbound `DriverAdapter`) + 50/51/52/53 |

See [SRD 05 §Dependency Rules](05_dependency_rules.md) for the enforced edges and the
Contract Registry, and [00_index.md](00_index.md) for the full document map.
