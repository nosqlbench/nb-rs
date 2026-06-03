# SRD-67 — Parent-Gated Polydat Sub-Context Construction — moved to polydat

This SRD's content has been moved into the polydat crate
as part of the import-first reorganization (see
[docs/polydat_srd_audit.md](../polydat_srd_audit.md)).

**New location:**
[polydat/docs/design/subcontext_construction.md](../../polydat/docs/design/subcontext_construction.md)

The walled-off construction protocol
(`SubcontextBuilder`, `ScopeKernel<M>`, `ScopeModule`),
the parent-walking lookup contract, Rule-2 SharedCell
write-through, the named-child registry, and the
compile_fail seals over `bind_outer_scope` / `from_program`
are all polydat-internal substrate. The protocol enforces
the chokepoint that
[composition_substrate.md L1/L4](../../polydat/docs/design/composition_substrate.md)
identifies as load-bearing.

nbrs-side composition concerns (typed `ScopeModule`
interface tailored for nbrs-activity, integration with
SRD-13e) remain in
[SRD 13e Scope-as-Module Refinement](13e_scope_as_module.md).

Reconciliation against the axiom-level polydat design docs
is pending, and may promote the imported doc to a peer of
the axiom docs (out of `imported/` into `design/`).
