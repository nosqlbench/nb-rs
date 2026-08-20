# 16: Polydat Engines and Optimization — moved to polydat

This SRD's content has been moved into the polydat crate
as part of the import-first reorganization (see
[docs/polydat_srd_audit.md](../polydat_srd_audit.md)).

**New location:**
[polydat/docs/design/engines.md](https://github.com/nosqlbench/polydat/blob/main/crates/polydat/docs/design/engines.md)

Engine selection (P1/P2/P3), compilation levels,
provenance-based invalidation, automatic selection
heuristic, and the type-system role in engine choice
are all polydat substrate concerns. The Phase-3 JIT
boundary moved alongside as
[polydat/docs/design/jit_boundary.md](https://github.com/nosqlbench/polydat/blob/main/crates/polydat/docs/design/jit_boundary.md).

The linked polydat docs are authoritative. For the public contract nmbrs depends
on, see [SRD 09 Polydat Contract](09_polydat_contract.md).
