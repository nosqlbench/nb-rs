# Design sketch: unified `AdapterSpec` as the single source of truth

## Problem

Today the CQL adapter declares its parameter and op-field surface
in **four** independent places, and they're all hand-maintained:

1. `CqlConfig::from_params` reads `params.get("hosts")`,
   `params.get("port")`, etc. — the *real* parser.
2. `DriverImpl::known_params: &["hosts", "host", "port", …]` —
   what the CLI validator accepts (and what
   `nbrs describe adapter=cql` enumerates).
3. `STMT_FIELD_NAMES = &["raw", "simple", "prepared", "stmt"]`
   — the op-template discriminator the dispenser-mapper iterates.
4. `DriverAdapter::known_op_fields()` (per-engine) — what the
   "core-first field processing" gate (SRD 30) checks against.

Each of these is the same idea expressed in a different shape.
They drift. Adding `request_timeout_ms` to `CqlConfig` and
forgetting to bump `known_params` produces a working binary that
silently ignores an unrecognized param at the CLI layer (or, with
strict mode, rejects it for the wrong reason). Adding a new
dispenser branch and forgetting to extend `known_op_fields` makes
strict mode reject the op template the adapter was about to
handle correctly.

`nbrs describe adapter=cql` exposed this directly. The
`Adapter params: cqldriver` line is real (one source); the per-
driver `params: hosts, host, port, …` line is the hand-maintained
slice — not what `from_params` actually parses.

## Principle

> Every param/field name and every dispatch decision an adapter
> makes should be readable from **one** declarative structure
> that the runtime, the validator, the dispenser-dispatcher,
> and `describe` all walk. If a name appears in code, it appears
> in the structure; if it appears in the structure, the runtime
> uses it. No second list.

This mirrors the GK principle that one canonical kernel answers
all in-scope names — see *GK Is Canonical Scope* in MEMORY.md.

## Sketch

```rust
// nbrs-activity::adapter::spec

pub struct AdapterSpec {
    /// User-facing adapter name (matches AdapterRegistration::names)
    pub name: &'static str,
    pub doc: &'static str,

    /// Session-level params (host, port, keyspace, …).
    /// Authoritative input to the config parser AND to validators.
    pub session_params: &'static [SessionParam],

    /// Op-template fields the adapter recognizes — both the
    /// dispenser discriminators (`raw:`, `prepared:`, `stmt:`)
    /// and the modifiers (`batchtype:`, `max_batch_size:`).
    pub op_fields: &'static [OpField],

    /// Drivers backing this adapter (or empty for single-impl).
    /// Each driver shares `session_params`; this is just the
    /// compiled-in implementations and their default ranking.
    pub drivers: &'static [DriverEntry],
}

pub struct SessionParam {
    pub name: &'static str,
    pub aliases: &'static [&'static str],   // e.g. "host" → "hosts"
    pub doc: &'static str,
    pub default: Option<&'static str>,
    pub kind: ParamKind,
}

pub enum ParamKind {
    String,
    U16, U64,
    Bool,
    Enum { values: &'static [&'static str] }, // consistency
    Secret,                                   // password — masked in describe
}

pub struct OpField {
    pub name: &'static str,
    pub doc: &'static str,
    pub role: OpFieldRole,
}

pub enum OpFieldRole {
    /// Presence selects a dispenser. The discriminator with the
    /// lowest `precedence` wins when multiple are present.
    Discriminator { dispenser: DispenserKind, precedence: u8 },
    /// Tweaks the chosen dispenser without selecting one
    /// (e.g. `batchtype` promotes a prepared op to a batch).
    Modifier,
    /// Pure metadata (`max_batch_size:`, `timeout:` per-op).
    Hint { kind: ParamKind },
}

pub struct DriverEntry {
    pub name: &'static str,
    pub default_rank: u32,
    /// Driver-specific extras *layered on top* of session_params
    /// (rare — most adapters share the schema across drivers).
    pub extra_session_params: &'static [SessionParam],
}
```

## How each consumer reads the spec

**Config parser** (`CqlConfig::from_params`): walks
`session_params`, reads each by name + aliases, parses by `kind`.
No hand-curated keys; adding a field to the spec is the only
edit needed.

**CLI validator** (`registered_adapter_params()` today): unions
`session_params.name` + every alias + `cqldriver` (the selector,
declared once at the registration level). Drops the per-driver
`known_params` slice entirely.

**Op-template dispatcher** (`map_op`): walks `op_fields`, finds
the highest-precedence `Discriminator` present in the template,
constructs the corresponding dispenser, then applies any
`Modifier` entries also present. The "unknown field" gate from
SRD 30 walks the same list — every key in the template that
isn't in `op_fields` is a hard error.

**`nbrs describe adapter=…`**: prints `name`/`doc`/`drivers`,
then a table of `session_params` (with `default`, `kind`, doc),
then a table of `op_fields` (grouped by role), then the selector
hint if `drivers.len() > 1`. Output is generated, not hand-written.

**Strict mode** (SRD 15): the same `kind` field that drives
parsing also tells strict mode which values are admissible
(e.g. `Enum { values: ["LOCAL_ONE", "QUORUM", …] }` → strict
mode rejects `consistency=BOGUS` at validation, not at first
connect).

## Migration path

1. Land the types in `nbrs-activity::adapter::spec`. New types
   only — no consumer changes yet.
2. Convert one adapter (stdout — smallest surface) end-to-end:
   declare its `AdapterSpec`, route `known_op_fields()` through
   it, route the validator through it, retire its hand-maintained
   slices.
3. CQL next. The interesting one: it has multi-driver semantics
   and a real `from_params` parser. Driver inventory submits
   stay; the per-driver `known_params` slice is replaced by
   `AdapterSpec::session_params` walk-through.
4. HTTP, plotter, testkit follow.
5. Once every adapter has migrated, drop the legacy
   `DriverImpl::known_params` and `DriverAdapter::known_op_fields`
   methods. The `AdapterSpec` is the only declared surface.

## Why one shape covers both session-params and op-fields

They look like different problems but the dispatch shape is the
same: take a string-keyed map (session params from CLI/workload,
op fields from YAML), match keys against a declared vocabulary,
parse values according to declared kinds, route the result.

The only thing that's adapter-private is *what the dispenser
does once dispatched*. The matching/parsing/rejection logic is
pure registry walking.

## Non-goals

- Not a full schema language. `kind` is a small flat enum, not
  an embedded JSON-Schema. Validation of structured payloads
  (CQL `evaluations:`, op-level `params:` blocks) stays in the
  core, where it already lives.
- Not a documentation generator beyond `describe`. The SRDs
  remain the authoritative narrative; `AdapterSpec.doc` is a
  one-line reference, not prose.
- Not pluggable at runtime. The spec is `&'static`, populated
  at link time via `inventory::submit!` like the existing
  registrations. No dynamic loading.

## Open questions

- **Where does `cqldriver` live?** It's declared on the
  `AdapterRegistration` today, not on `AdapterSpec`. Cleanest
  is to merge `AdapterRegistration` into `AdapterSpec` so the
  spec is the only inventory submission per adapter; the
  factory closure moves to a separate `AdapterFactory` field
  (or stays per-driver via `DriverEntry::create`).
- **Per-driver session-param overrides.** `extra_session_params`
  on `DriverEntry` is hedged for cases like a driver-only
  knob (e.g. `cassandra_cpp_log_level`). If no real driver
  needs it, drop the field — YAGNI.
- **Field precedence in op-template dispatch.** Today CQL's
  `STMT_FIELD_NAMES` is iteration order. Making precedence
  explicit (`u8` per discriminator) is more honest and lets
  HTTP say "if both `body:` and `form:` are present, `form:`
  wins" without reordering a slice.
