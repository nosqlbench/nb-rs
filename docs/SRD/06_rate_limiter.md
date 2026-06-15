# 06: Rate Limiter — Contract & Axioms (nbrs-rate)

Front door for **nbrs-rate** (layer L3): the async token-bucket rate limiter that paces
op dispatch. An exemplar of a **tight contract** — three re-exported types, all modules
private. Pillars 1+2 of the [Subsystem Treatment Standard](00b_subsystem_standard.md).

## Contract

- **Public surface:** `RateSpec`, `RateLimiter`, `RateLimiterApplier` (re-exported at the
  crate root; the `spec` / `limiter` / `applier` modules are private). This is the whole
  surface — nothing else is reachable.
- **Inbound contract:** `nbrs_metrics::controls` (`ControlApplier`) for live retargeting.
- **Allowed edges:** `nbrs-metrics`. See [SRD 05 §Contract Registry](05_dependency_rules.md).

## Axioms

- **R1 — Acquire never blocks the runtime.** `acquire()` awaits asynchronously; token
  refill runs as an async task. No sync `sleep`/`recv` on the dispatch path. [SRD 02 §No
  Blocking Primitives in Async Contexts].
- **R2 — Rate is a live control, not a restart.** `RateLimiterApplier` is a
  `ControlApplier`; changing ops/sec is a confirmed-apply write through the component
  tree. [SRD 23].
- **R3 — Tight surface.** Three types, modules private. The contract cannot drift because
  there is nothing internal to reach.

## Mechanism (Pillar 3)
The token/leaky-bucket model and the three-pool design: [notes/19_rate_limiter.md](notes/19_rate_limiter.md)
(the design brief), framed by [SRD 02](02_concurrency_model.md) §"Rate limiting".

## See also
`nbrs-rate/src/lib.rs`; [SRD 02](02_concurrency_model.md); [SRD 23](23_dynamic_controls.md).
