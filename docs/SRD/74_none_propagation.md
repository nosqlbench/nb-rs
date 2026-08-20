# SRD 74: None Propagation — moved to polydat

This SRD's content has been moved into the polydat crate
as part of the import-first reorganization (see
[docs/polydat_srd_audit.md](../polydat_srd_audit.md)).

**New location:**
[polydat/docs/design/none_semantics.md](https://github.com/nosqlbench/polydat/blob/main/crates/polydat/docs/design/none_semantics.md)

The three orthogonal None-propagation rules (kernel
input None-pass-through, string-interpolation None
suppression, conditional-shadow `const` semantics),
the typed return contract, and the interaction with
`set:` and the GK-grammar invariant are all polydat
substrate concerns. The Rule-2 explicit-optionality
syntax remains as future work documented in the moved
SRD.

The linked polydat doc is authoritative. For the public contract nb-rs depends
on, see [SRD 09 Polydat Contract](09_polydat_contract.md).
