# polydat/docs/imported/ — SRD imports awaiting reconciliation

This directory holds polydat-related design content that
was originally written as nbrs SRDs in `docs/sysref/`.
The content has been **structurally moved** out of nbrs
into polydat — where it belongs — but has **not yet been
reconciled** with the axiom-level polydat design docs in
`polydat/docs/design/`.

## Why this directory exists

The polydat crate owns the definitive design for the
variates + Polydat substrate. As polydat's own design docs
matured (composition_substrate, grammar, graph_compiler,
runtime_model, expression_engine, comprehension_forms),
substantial overlap accumulated with older nbrs SRDs that
predated polydat as a separate crate.

Rather than try to revise each SRD in place — interleaving
"keep this paragraph / delete that one / cross-reference
here" edits across two repositories of docs at once — the
imports-first approach is:

1. **Move polydat-owned SRD content into polydat first**,
   intact and unchanged, with a manifest header recording
   where it came from and which axiom-level polydat docs
   it overlaps.
2. **Reconcile inside polydat**, after the move, with all
   the relevant content co-located. Each imported doc is
   drained over time by collapsing duplication against the
   axiom docs, hoisting mechanism detail upward, or
   retiring overlapping passages.

The audit and per-SRD move plan are in
[`docs/polydat_srd_audit.md`](../../../docs/polydat_srd_audit.md)
in the nbrs repo root.

## Manifest header convention

Every imported doc starts with a manifest block:

```markdown
---
imported_from: docs/sysref/<original>.md
imported_on: 2026-MM-DD
reconciliation_status: pending
overlaps_with:
  - polydat/docs/design/<doc>.md (§<section> + §<section>)
  - polydat/docs/design/<doc>.md (§<section>)
---
```

The `overlaps_with` list seeds reconciliation: each entry
names where the content competes with an axiom-level
treatment. Reconciliation decides per overlap:

- **Collapse** — content is fully covered by the axiom doc;
  delete the imported passage and (if needed) update the
  axiom doc with a cross-reference back to whichever doc
  is the human-facing entry point.
- **Delegate** — both docs cover the topic at different
  tiers; reduce the imported doc to the mechanism-level
  detail and add a cross-reference to the axiom-level
  framing.
- **Hoist** — mechanism detail in the imported doc belongs
  in the axiom doc as a worked example or specification
  detail; move it upward and delete from the import.
- **Keep as authority** — content is genuinely polydat-
  internal but the axiom docs don't cover it (e.g., JIT
  boundary internals, parent-gated construction protocol).
  Promote the imported doc out of `imported/` into
  `polydat/docs/design/` as a peer of the axiom docs and
  delete the `imported_from` manifest.

## Reconciliation status values

- `pending` — moved here as part of the import push;
  reconciliation work has not started.
- `in-progress` — reconciliation pass underway; the doc
  may have partial revisions but is not yet collapsed.
- `drained` — content has been distributed (collapsed,
  delegated, hoisted, or promoted); the file is ready for
  deletion at the next cleanup pass.
- `promoted-to-design` — the doc was promoted out of
  `imported/` into `polydat/docs/design/` as a peer of
  the axiom docs. The file in `imported/` (if it still
  exists) is a stub pointer.

## Out of scope for reconciliation

The reconciliation pass operates **only inside
`polydat/docs/`**. It does not edit `docs/sysref/`. If
reconciliation finds that an axiom doc needs to grow
(hoist case) or that an imported doc should be promoted
(keep-as-authority case), those changes happen entirely
inside the polydat crate.

The nbrs-side SRD stubs left behind by the import
(short header pointers to `polydat/docs/imported/`) are
**not** updated by reconciliation. They continue to point
at `imported/`, and reconciliation maintains that
contract by either (a) leaving the imported file in place
or (b) replacing it with a stub that forwards to the
final destination.
