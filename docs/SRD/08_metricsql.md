# 08: MetricsQL — Contract & Axioms (nbrs-metricsql)

Front door for **nbrs-metricsql** (layer L0, standalone): a Rust port of VictoriaMetrics
MetricsQL — lexer, parser, evaluator, and the streaming/continuous-query runtime. Pillars
1+2 of the [Subsystem Treatment Standard](00b_subsystem_standard.md).

## Contract

**Surface and edges:** authoritative in [SRD 05 §Contract Registry](05_dependency_rules.md).
In brief — `parser::parse` + `ast::Expr`, `eval::{DataSource, …}` (evaluator + its pluggable
storage trait), `streaming::{StreamingPlan, …}`, `adapters` (built-in `DataSource` impls),
`runtime` (feature-gated); the inbound contract is a caller-provided `DataSource`.

As a **standalone, extractable library**, its *whole* module set (`lexer`, `parser`,
`prettifier`, `ast`, `query_rewrite`, `eval`, `streaming`, `adapters`, `catalog` —
`MetricCatalog` + OpenMetrics types — `grammar`, `runtime`) is public: its public API is its
own contract, broader than what the nb-rs workspace consumes, so the SRD-05 **D5 narrowing
does not apply** (D5 is for workspace-internal crates).

## Axioms

- **Q1 — Standalone, extractable.** Zero internal dependencies; usable outside nb-rs.
- **Q2 — Pluggable storage (storage-as-a-service boundary).** The `DataSource` trait is
  the seam between query/compute (this crate) and sample storage (the host). The evaluator
  computes; it never owns storage. [SRD 49](49_metricsql_supported_scope.md).
- **Q3 — Streaming ≡ batch.** A `StreamingPlan` must produce the same result as batch
  evaluation; the `Reducer` algebra (distributive / algebraic / holistic) is the
  load-bearing artifact and is property-tested for equivalence. [SRD 47](47_metricsql_streaming.md).

## Mechanism (Pillar 3)
[SRD 47](47_metricsql_streaming.md) (streaming aggregation), [SRD 48](48_metricsql_continuous_query.md)
(continuous-query runtime), [SRD 49](49_metricsql_supported_scope.md) (supported scope + drift tests).

## See also
`nbrs-metricsql/src/lib.rs`; [SRD 47](47_metricsql_streaming.md); [SRD 48](48_metricsql_continuous_query.md); [SRD 49](49_metricsql_supported_scope.md).
