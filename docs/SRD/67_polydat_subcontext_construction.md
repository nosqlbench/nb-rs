# SRD-67 — Parent-Gated Polydat Sub-Context Construction — moved to polydat

> **Planned (SRD-84):** the matter layer (`PolydatMatter` /
> `PolydatMatterBuilder` / `BodyFragment`) gains a **grammar-safe**
> fragment form (parsed AST / typed bindings) to replace synthesizers'
> reliance on `BodyFragment::PolydatSource(String)`; raw source is kept
> only for user-authored workload text parsed at the ingest boundary.
> Plus a **caller-native expression-stub API** to construct + attach a
> typed expression to a kernel without a source string. See
> [SRD-84](84_grammar_safe_matter.md) Parts 2–3.

This SRD's content has been moved into the polydat crate
as part of the import-first reorganization (see
[docs/polydat_srd_audit.md](../polydat_srd_audit.md)).

**New location:**
[polydat/docs/design/subcontext_construction.md](https://github.com/nosqlbench/polydat/blob/main/crates/polydat/docs/design/subcontext_construction.md)

The walled-off construction protocol
(`SubcontextBuilder`, `ScopeKernel<M>`, `ScopeModule`),
the parent-walking lookup contract, Rule-2 SharedCell
write-through, the named-child registry, and the
compile_fail seals over `bind_outer_scope` / `from_program`
are all polydat-internal substrate. The protocol enforces
the chokepoint that
[composition_substrate.md L1/L4](https://github.com/nosqlbench/polydat/blob/main/crates/polydat/docs/design/composition_substrate.md)
identifies as load-bearing.

nmbrs-side composition concerns (typed `ScopeModule`
interface tailored for nmbrs-runtime, integration with
SRD-13e) remain in
[SRD 13e Scope-as-Module Refinement](13e_scope_as_module.md).

The linked polydat doc is authoritative. For the public contract nmbrs depends
on, see [SRD 09 Polydat Contract](09_polydat_contract.md).
