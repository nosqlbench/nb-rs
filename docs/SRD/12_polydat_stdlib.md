# 12: Polydat Standard Library — moved to polydat

This SRD's content has been moved into the polydat crate
as part of the import-first reorganization (see
[docs/polydat_srd_audit.md](../polydat_srd_audit.md)).

**New location:**
[polydat/docs/design/library_catalog.md](../../polydat/docs/design/library_catalog.md)

The wire cost classes, node-category catalog, registration
conventions, and node-fusion catalog are all polydat
substrate concerns. The library lives in `polydat/src/library/`;
its design + catalog now live in the polydat crate as
well.

The linked polydat doc is authoritative. For the public contract nb-rs depends
on, see [SRD 09 Polydat Contract](09_polydat_contract.md).
