# SRD 74: None Propagation — moved to polydat

This SRD's content has been moved into the polydat crate
as part of the import-first reorganization (see
[docs/polydat_srd_audit.md](../polydat_srd_audit.md)).

**New location:**
[polydat/docs/design/none_semantics.md](../../polydat/docs/design/none_semantics.md)

The three orthogonal None-propagation rules (kernel
input None-pass-through, string-interpolation None
suppression, conditional-shadow `const` semantics),
the typed return contract, and the interaction with
`set:` and the GK-grammar invariant are all polydat
substrate concerns. The Rule-2 explicit-optionality
syntax remains as future work documented in the moved
SRD.

Reconciliation against the axiom-level polydat design docs
([composition_substrate.md T1](../../polydat/docs/design/composition_substrate.md),
[runtime_model.md D1](../../polydat/docs/design/runtime_model.md),
[grammar.md G1](../../polydat/docs/design/grammar.md))
is pending.
