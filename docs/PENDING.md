# Pending Work

Items not yet implemented. See `docs/sysref/` for design context.

## Serialization

- [ ] Compare nb-rs serialization needs with `veks-*` and
  `vectordata` crate formats. Determine if CBOR/CDDL from those
  crates can be leveraged or if nb-rs needs its own wire format.

## Study Topics

- [ ] Incremental invalidation in GK evaluation — provenance-based
  node invalidation instead of full state reset on input change.
  (memo topic from sysref 10)
