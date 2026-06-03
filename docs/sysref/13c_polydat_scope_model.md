# 13c: Polydat Scope Model — moved to polydat

This SRD's content has been moved into the polydat crate
as part of the import-first reorganization (see
[docs/polydat_srd_audit.md](../polydat_srd_audit.md)).

**New location:**
[polydat/docs/design/scope_model.md](../../polydat/docs/design/scope_model.md)

How Polydat kernels compose across lifecycle boundaries
(phases, `for_each` iterations, scope groups) with
visibility, mutability, and isolation rules is polydat-
substrate. The principles, scope hierarchy, no-flattening
guarantee, visibility/mutability rules, and `for_each`
lifecycle now live in the polydat crate.

nbrs-side concerns (op-template scope realisation,
typed `ScopeModule` integration) remain in:

- [SRD 13b Polydat Combination Modes](13b_polydat_combination_modes.md)
- [SRD 13d Op-template Polydat Scope Layer](13d_op_template_scope.md)
- [SRD 13e Scope-as-Module Refinement](13e_scope_as_module.md)

Reconciliation against the axiom-level polydat design docs
([composition_substrate.md](../../polydat/docs/design/composition_substrate.md),
[graph_compiler.md](../../polydat/docs/design/graph_compiler.md),
[grammar.md](../../polydat/docs/design/grammar.md))
is pending.
