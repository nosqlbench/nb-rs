# SRD-18e: Comprehension Canonical Reference (REDIRECT STUB)

**Status:** SUPERSEDED 2026-05-28 by the polydat comprehension
spec at `polydat/docs/design/comprehension_forms.md`.

This SRD's original content has been retired. The polydat spec
is now the single authoritative reference for the comprehension
algebra — its constructors, validity axioms, metadata
propagation rules, optimizer rewrites, IR opcodes, and
consumption surfaces. Apparent contradictions between this SRD
and the polydat spec resolve in favor of the polydat spec.

This stub remains as a redirect target for existing
cross-references. The section map below points each former
SRD-18e topic to its current home in the polydat spec.

---

## Section map

| Former SRD-18e topic | Current authoritative section |
|---|---|
| The canonical AST | polydat spec §3 (Constructors in detail), §13 (Migration relative to current code) |
| Mode detection — Cartesian vs Union | polydat spec §3.2 (cartesian), §3.4 (union), §8.4 (inferred union) |
| Coordinate-set contract | polydat spec §3 (per-constructor tuple-shape rules), §5 V3 (filter name closure) |
| Execution pipeline (`enumerate → filter → order → materialize`) | polydat spec §9.1 (Operator IR), §9.2 (correctness contract), §9.3 (resource bounds) |
| Index-space contract for orderings | polydat spec §5 V4 (strategy input-shape contract), §5 V5 (filter is transparent), §10.7 (metadata algebra — `IndexFn`) |
| Union mode + non-lex orderings | polydat spec §5 V4 + §3.6 strategy table; specifically, lattice-geometric strategies require Lattice IndexFn so they reject union (Concatenation IndexFn) per the per-strategy input table |
| `where` predicate semantics | polydat spec §3.5 (filter constructor), §5 V3 (name closure), §10.9 (predicate analyzer) |
| Layer 7 extension path | polydat spec §3.3 (zip — Layer 7a parallel landed as zip-Strict), §13 (Migration); destructure stays a parser-layer concern in SRD-18c |
| `custom` ordering | **Removed from the algebra**. polydat spec §3.6 documents the deliberate exclusion of user-callback orderings; if a workload need can't be met by the named strategies, add a named strategy upstream (polydat spec §14.4) |
| Per-strategy implementation status | polydat spec §3.6 (full strategy table with input requirements and continuous behavior), §10.2 R2 (push-down rules per strategy) |

## What changed structurally

The polydat spec replaces SRD-18e's flat
`Comprehension { mode: ComprehensionMode, filter: Option<String>,
order: Option<TraversalOrder> }` struct with an **operator-tree**
algebra of six constructors (clause, cartesian, zip, union,
filter, order) closed under composition. The flat struct's
`filter` and `order` fields become first-class AST nodes that
wrap the comprehension they apply to. See polydat spec §3
(Constructors in detail), §4 (Closure and identity axioms), and
§13 (Migration relative to current code) for the full mapping.

The execution pipeline collapses from a fixed
"enumerate → filter → order → materialize" sequence into the
operator tree's bottom-up evaluation. The compilation model
(polydat spec §9) compiles the operator tree to a stream-
transducer IR (eight opcodes); the post-parse optimizer
(polydat spec §10) rewrites the AST into push-down forms before
compilation.

## Why the retirement

The polydat crate is the canonical owner of comprehension
semantics. Having two authoritative references (SRD-18e + the
polydat spec) created drift potential — every comprehension
change had to be applied twice and any divergence would create
a "which one is authoritative?" question. SRD-18e as a 691-line
parallel canonical reference violated the polydat spec's §15.5
ownership invariant.

## Cross-references

- **`polydat/docs/design/comprehension_forms.md`** — the
  authoritative comprehension reference. Start here.
- [SRD-18b](18b_scenario_tree_and_scheduler.md) — scenario-tree
  integration of polydat comprehensions.
- [SRD-18c](18c_comprehension_syntax.md) — parser-layer surface
  grammar that produces polydat comprehension ASTs.
- [SRD-18d](18d_comprehension_traversal_order.md) — per-strategy
  algorithmic detail (Halton recurrence, Sobol direction numbers,
  etc.); the polydat spec §3.6 owns compositional behavior and
  per-strategy input requirements.
- [SRD-78](78_polystreamer.md) — PolyStreamer runtime hosting
  the polydat spec §9.5 consumption surfaces.
