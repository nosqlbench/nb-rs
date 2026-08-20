# 16b: Polydat JIT Wiring — moved to polydat

This SRD's content has been moved into the polydat crate
as part of the import-first reorganization (see
[docs/polydat_srd_audit.md](../polydat_srd_audit.md)).

**New location:**
[polydat/docs/design/jit_boundary.md](https://github.com/nosqlbench/polydat/blob/main/crates/polydat/docs/design/jit_boundary.md)

The Phase-3 native-kernel call boundary, Cranelift
extern-helper table, setjmp/longjmp predicate-violation
plumbing, fallback paths, and per-engine state model are
all polydat-internal contracts. Their authoritative
location is now inside the polydat crate, where the JIT
implementation lives.

The linked polydat docs are authoritative. For the public contract nb-rs depends
on, see [SRD 09 Polydat Contract](09_polydat_contract.md).
