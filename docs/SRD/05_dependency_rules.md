# 05: Crate & Module Dependency Rules

Authoritative rules for which crates and modules may depend on which, and how.
This is **Pillar 5 (Enforced edges)** of the [Subsystem Treatment Standard](00b_subsystem_standard.md)
made system-wide: the rules here are machine-checked by
[`nbrs/tests/architecture_rules.rs`](../../nbrs/tests/architecture_rules.rs) and run in CI.
A proposal that contradicts a rule here is wrong, not the rule.

This SRD promotes and formalizes the informal "Dependency Rules" and "Contract
Boundaries" that previously lived in [SRD 01](01_system_overview.md).

---

## Dependency layers

The workspace is a DAG. Every crate has a **layer**; an edge in `[dependencies]` may
only point to a **strictly lower** layer. (`[dev-dependencies]` may point upward — tests
legitimately pull in real adapters as fixtures.)

| Layer | Crates | Rationale |
|------:|--------|-----------|
| **L0** | `polydat-derive`, `nbrs-errorhandler`, `nbrs-metricsql` | leaf substrates; zero internal deps; independently extractable |
| **L1** | `polydat` | depends only on `polydat-derive`; the deterministic variate kernel |
| **L2** | `nbrs-metrics`, `nbrs-workload` | depend only on `polydat` |
| **L3** | `nbrs-rate`, `nbrs-adapter-openapi` | `rate`→`metrics`; `openapi`→`workload` only |
| **L4** | `nbrs-activity` | the integration hub: `polydat` + `metrics` + `rate` + `errorhandler` + `workload` |
| **L5** | `nbrs-adapter-{stdout,http,plotter,cql}`, `nbrs-tui`, `nbrs-web` | implement / consume the activity contract |
| **L6** | `nbrs-adapter-testkit` | composite adapter (`→ stdout`) |
| **L7** | `nbrs` (binary) | composition root; depends on everything |

`vendor/cassandra-cpp` is a vendored third-party fork and is exempt (it is `publish = false`
and outside the layer rules; only `nbrs-adapter-cql` consumes it, feature-gated).

---

## Rules

- **D1 — polydat is standalone.** `polydat`'s only internal dependency is `polydat-derive`.
  It can be extracted and reused in other projects. (`tests::d1_polydat_standalone`)
- **D2 — no upward edges.** Every `[dependencies]` edge points to a strictly lower layer.
  Subsumes "foundation crates never depend on the integration/presentation tier."
  (`tests::d2_edges_point_down`)
- **D3 — activity sits below presentation.** `nbrs-activity` (L4) depends only on L0–L3;
  it never depends on an adapter, the TUI, the web UI, or the binary. (covered by D2)
- **D4 — adapters don't depend on adapters.** No adapter crate depends on another adapter,
  with the single allowlisted exception `nbrs-adapter-testkit → nbrs-adapter-stdout`
  (structured fall-through output). (`tests::d4_no_cross_adapter_edges`)
- **D5 — internal modules stay `pub(crate)`.** A **workspace-internal** crate's
  declared-internal modules are `pub(crate)`, so the **compiler itself** forbids any foreign
  crate reaching them — the surface can't re-grow by accident. `tests::d5_public_surface` is
  the regression guard (fails if one is re-widened to bare `pub`). **Scope:** D5 applies only
  to crates whose public API *is* "what the workspace consumes" (`nbrs-activity`, `nbrs-tui`,
  `nbrs-web`). **Standalone, extractable libraries** (`polydat`, `nbrs-metricsql`, `nbrs-rate`,
  `nbrs-errorhandler`) are **exempt** — their public API is their own library contract,
  legitimately broader than any single consumer; narrowing it would amputate real API.
  Modules used by a crate's *own* integration tests also stay `pub` (white-box tests are
  external crates) and are exempt.
- **D6 — no reaching past polydat's surface.** No non-polydat crate may `use` a deep
  polydat internal path (`polydat::compile::jit::…`, `polydat::library::support::…`).
  (`tests::d6_no_polydat_deep_paths`)
- **D7 — polydat is self-contained (no upward doc references).** Neither `polydat/docs/**`
  nor `polydat/src/**` may reference the nbrs `docs/SRD/` layer. polydat is independently
  extractable (D1); its design must not depend on the consumer's docs, or lifting it out
  would leave dangling references. The docs-level analog of D1/D6. Conceptual mentions of
  the host are fine — only references *into* `docs/SRD/` are forbidden.
  (`tests::d7_polydat_self_contained`)
- **Discovery is by inventory, not wiring.** Adapters register via the
  `nbrs_activity` adapter inventory; polydat nodes via `polydat`'s `inventory`
  (emitted by `polydat-derive`); metric reporters via `nbrs_metrics`. The binary
  discovers implementations at link time, not by hard-coded lists.

---

## Contract Registry

The single source of truth for Pillar 1 (Contract surface) and the input the CI gate
reads for D5. One row per crate: **public surface** (what it exports), **inbound
contract** (what it requires from below), **allowed edges** (lower-layer crates it may
depend on).

| Crate | Public surface (exported) | Inbound contract (consumed) | Allowed edges |
|---|---|---|---|
| `polydat-derive` | `#[polydat_node]` proc-macro | — | — |
| `polydat` | `ast`,`kernel`,`compile`,`dsl`,`library`,`viz`,`binder`,`iteration`,`audit` (host-log sink bridge) | — | `polydat-derive` |
| `nbrs-metricsql` ([SRD 08](08_metricsql.md)) | **full library API** (`lexer`,`parser`,`prettifier`,`ast`,`query_rewrite`,`eval`,`streaming`,`adapters`,`catalog`,`grammar`,`runtime`) — standalone library, **D5-exempt** | pluggable `DataSource` impl (caller-provided) | — |
| `nbrs-errorhandler` ([SRD 07](07_error_routing.md)) | `ErrorDetail`,`ErrorHandler`,`ErrorRouter` (re-exported), `handlers` | — | — |
| `nbrs-metrics` ([SRD 39](39_metrics_contract.md)) | `component`,`instruments`,`labels`,`selector`,`controls`,`snapshot`,`cadence`,`cadence_reporter`,`scheduler`,`summaries`,`metrics_query`,`reporters`,`diag` — *internal: `validation`* | `polydat::{ast,kernel}` | `polydat` |
| `nbrs-workload` ([SRD 25](25_workload_contract.md)) | `model`,`parse`,`bindpoints`,`tags`,`inline`,`catalog`,`edit`,`extends`,`report`,`metric_format`,`polydat_matter` — *internal: `template`,`spectest`* | `polydat::{ast,dsl}` | `polydat` |
| `nbrs-rate` ([SRD 06](06_rate_limiter.md)) | `RateSpec`,`RateLimiter`,`RateLimiterApplier` (re-exported; modules private) | `nbrs_metrics::controls` | `nbrs-metrics` |
| `nbrs-adapter-openapi` | `DriverAdapter` impl (inventory) | `nbrs_workload::model` | `nbrs-workload` |
| `nbrs-activity` | `adapter`,`op_modifier`,`wrapper_registry`/`wrapper_resolver`,`runner`,`activity`,`session`,`scene_tree`,`scope_tree`,`bindings`,`phase_outcome`,`phase_end_triggers`,`refine_plan`,`checkpoint`,`observer`,`diag!`,`readouts`,`lifecycle`,`log_sink`,`wires`,`polydat_nodes`,`resource_pool`,`fixture`(test) — **full surface + the ~25-module internal narrowing target in [SRD 29](29_execution_engine.md)** | `polydat`,`nbrs-metrics`,`nbrs-rate`,`nbrs-errorhandler`,`nbrs-workload` | L0–L3 |
| `nbrs-adapter-{stdout,http,plotter,cql}` | `DriverAdapter`/`OpDispenser` impls (inventory) | `nbrs_activity::adapter` | `nbrs-activity`,`nbrs-workload`,`polydat`(+`metrics`/`cassandra-cpp` for cql) |
| `nbrs-tui` ([SRD 59](59_tui_contract.md)) | `observer`(`TuiObserver`),`state`,`run_state_actor`, sink seam (`display_sink`/`log_only_sink`/…), inspector (`inspector_server`/`repl_state`/`key_watcher`) — *internal: `app`/`widgets`/`reporter`/`frame_broker`/`prompt_state`/`readout_panel`/`readout_sink`/`tui_sink`* | `nbrs_activity::observer`, `nbrs_metrics` | `nbrs-activity`,`nbrs-metrics`,`polydat` |
| `nbrs-web` ([SRD 54](54_web_ui.md)) | `server`,`ws` — *internal: `routes`,`models`,`graph`* | `nbrs_activity`, `nbrs_metrics` | `nbrs-activity`,`nbrs-metrics`,`polydat` |
| `nbrs-adapter-testkit` | `DriverAdapter` impl (inventory) | `nbrs_activity::adapter` | `nbrs-activity`,`nbrs-adapter-stdout`,`nbrs-workload`,`polydat` |
| `nbrs` | — (binary) | all of the above | all (L0–L6) |

> `nbrs-activity` exposed 51 modules; **18 are now `pub(crate)`** (the dispatch/synthesis
> machinery — see [SRD 29 §Internal](29_execution_engine.md); three superseded modules —
> `cycle`/`binder`/`linearize` — were deleted as dead code the narrowing revealed). Four more (`scope_synth`,
> `scope`, `wrappers`, `validation`) stay `pub` only because the crate's own integration
> tests use them. D5 is **enforced** — the compiler walls off the `pub(crate)` set and
> `tests::d5_public_surface` guards against re-widening. Likewise `nbrs-tui` (6 narrowed).
> (`nbrs-metricsql` is a standalone library and is D5-exempt — see the D5 scope note above.)

---

## See also
- [SRD 00b — Subsystem Treatment Standard](00b_subsystem_standard.md) (Pillar 5)
- [SRD 01 — System Overview](01_system_overview.md) (crate map, data flow)
- [`nbrs/tests/architecture_rules.rs`](../../nbrs/tests/architecture_rules.rs) (the gate)
