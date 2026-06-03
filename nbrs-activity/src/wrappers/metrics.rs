// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Synthetic-metric recorder (SRD-40b §6). After the inner
//! adapter returns, pulls each declared metric's value through
//! the per-fiber op-template kernel via `ctx.wires.get` and
//! records it onto the kind-specialised instrument
//! (gauge / histogram / counter).

use std::collections::HashMap;
use std::sync::Arc;

use crate::adapter::{ExecutionError, OpDispenser, OpResult};
use crate::adapter::WrappingDispenser;
use crate::wrapper_registry::{WrapperName, WrapperRegistration};
use nbrs_workload::model::ParsedOp;

/// SRD-32a wrapper name.
pub const NAME: WrapperName = WrapperName::new("metrics");

/// Trigger: op declares a non-empty `metrics:` map.
fn triggers(template: &ParsedOp) -> bool {
    !template.metrics.is_empty()
}

fn describe_assignment(template: &ParsedOp) -> Option<String> {
    if template.metrics.is_empty() {
        return None;
    }
    let mut names: Vec<&str> = template.metrics.keys().map(|s| s.as_str()).collect();
    names.sort();
    Some(format!("metrics: emits {}", names.join(", ")))
}

/// `metrics` must sit outside every other non-dryrun wrapper —
/// it observes their per-cycle effects on `ctx.wires`. Listed
/// as the `forbids_outer` set on metrics' registration so the
/// constraint validator hard-errors if anything tries to slip
/// outside it (except DRYRUN, which is strictly outermost via
/// its own `forbids_outer` declaration).
const FORBIDS_OUTER: &[WrapperName] = &[
    super::traversing::NAME,
    super::throttle::NAME,
    crate::validation::WRAPPER_NAME,
    super::polling::NAME,
    super::conditional::NAME,
    super::emit::NAME,
    super::result::NAME,
];

inventory::submit! {
    WrapperRegistration {
        name: NAME,
        // `metrics:` is parsed into ParsedOp.metrics, not into
        // params, so there's no owned `params`-key. Trigger
        // fires only when the metrics map is non-empty.
        owned_fields: &[],
        triggers,
        requires_inner: &[],
        forbids_outer: FORBIDS_OUTER,
        mutually_exclusive_with: &[],
        describe_assignment,
    }
}

/// Wraps an inner OpDispenser to publish per-cycle synthetic
/// metrics declared in the op template's `metrics:` map
/// (SRD-40b §6).
///
/// Per-cycle responsibilities (in order, matching SRD-40b §5.2 →
/// §6 pipeline):
///
/// 1. Await the inner dispenser's `execute`. With a
///    [`crate::wrappers::ResultDispenser`] in the wrapper stack
///    between the inner adapter and this one, declared `result:`
///    wires are already written through `ctx.wires.write` to the
///    per-fiber kernel by the time we run.
/// 2. For each declared metric, read the value through
///    `ctx.wires.get(name)` (bare-binding-name canonical form per
///    SRD-40b §1). The read pulls fresh through the eval cone
///    so any computed output (e.g. `row_count := count`) reflects
///    this cycle's value.
/// 3. Apply the optional [`metric_format::FormatSpec`] sanitiser
///    to round to the configured precision (Phase B).
/// 4. Dispatch to the kind-specific instrument record method
///    (SRD-40b §6.1):
///    - [`MetricKind::Gauge`] → [`ValueGauge::set`] (f64).
///    - [`MetricKind::Histogram`] → [`Histogram::record`]
///      (truncated to u64 after format rounding).
///    - [`MetricKind::Counter`] → [`Counter::inc_by`] (u64);
///      non-positive values warn and skip — counters are
///      monotonic by definition.
///
/// Non-bare-name expressions (`factor * 2.0`, `if(...)`, …) are
/// **deferred** — the wrap step errors when `spec.value` is not
/// a bare identifier.
pub struct MetricsDispenser {
    inner: Arc<dyn OpDispenser>,
    /// One slot per declared metric. Stable ordering by metric
    /// name keeps per-cycle dispatch deterministic for tests and
    /// makes any per-cycle warning sequence reproducible.
    slots: Vec<MetricSlot>,
}

/// One compiled metric slot: instrument storage + sanitiser +
/// pre-bound GK pull handle.
struct MetricSlot {
    /// Family name registered with the [`Component`]. Used in
    /// diagnostic messages (e.g. the counter non-positive warning).
    family: String,
    /// The original `value:` text from the workload, kept for
    /// diagnostics. Per-cycle resolution reads through the
    /// internal `binding_name` below; the user's original text
    /// surfaces in error messages so operators see what they wrote.
    value_expr: String,
    /// Internal kernel-output name (`__metric_<name>`) the
    /// op-template synthesiser created from this metric's
    /// `value:` expression. Cycle-time reads go through
    /// `ctx.wires.get(&binding_name)`.
    binding_name: String,
    /// Optional value sanitiser. Applied after the value is
    /// pulled, before the instrument record.
    format: Option<nbrs_workload::metric_format::FormatSpec>,
    /// Resolved instrument storage — exactly one variant is
    /// populated per slot, matching `MetricSpec.kind`.
    instrument: MetricInstrument,
}

/// Kind-specialised instrument storage owned by a [`MetricSlot`].
///
/// The same `Arc<...>` is shared with the dispenser's `Component`
/// instrument registry — registered via
/// `Component::register_instrument` at [`MetricsDispenser::wrap`] time.
/// Per-cycle code records through the slot's typed `Arc`; the cadence
/// reporter snapshots through the registry. One source of truth, two
/// access paths.
#[derive(Clone)]
enum MetricInstrument {
    Gauge(Arc<nbrs_metrics::instruments::gauge::ValueGauge>),
    Histogram(Arc<nbrs_metrics::instruments::histogram::Histogram>),
    Counter(Arc<nbrs_metrics::instruments::counter::Counter>),
}

impl MetricInstrument {
    /// Promote the kind-erased slot value into the canonical
    /// [`InstrumentRef`] for registry storage.
    fn as_ref(&self) -> nbrs_metrics::component::InstrumentRef {
        match self {
            MetricInstrument::Gauge(g) =>
                nbrs_metrics::component::InstrumentRef::Gauge(g.clone()),
            MetricInstrument::Histogram(h) =>
                nbrs_metrics::component::InstrumentRef::Histogram(h.clone()),
            MetricInstrument::Counter(c) =>
                nbrs_metrics::component::InstrumentRef::Counter(c.clone()),
        }
    }
}

/// Numeric coercion for capture-map lookups. Returns `None`
/// for non-numeric variants (string, vector, none) so the
/// MetricsDispenser slot path logs + skips rather than panicking
/// through `Value::as_f64`'s strict matcher.
fn value_to_f64(v: &polydat::ast::Value) -> Option<f64> {
    match v {
        polydat::ast::Value::F64(f) => Some(*f),
        polydat::ast::Value::U64(u) => Some(*u as f64),
        polydat::ast::Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
        _ => None,
    }
}

impl MetricsDispenser {
    /// Wrap an inner dispenser with synthetic-metric publication
    /// for the op template's `metrics:` declarations.
    ///
    /// Init steps (SRD-40b §6 init):
    /// 1. Empty declaration → return `inner` unchanged. No
    ///    overhead for ops that don't publish synthetic metrics.
    /// 2. For each `(name, spec)`, allocate the kind-specific
    ///    instrument and register it on the component via
    ///    `Component::register_instrument`. A duplicate-family
    ///    collision (§7.2) errors here, before any cycle runs.
    /// 3. Pre-parse the optional `format:` string into a
    ///    [`FormatSpec`].
    ///
    /// `component` is borrowed mutably so `register_instrument`
    /// can claim the family slot atomically with the instrument
    /// allocation. The same `Arc<...>` is held both on the
    /// component (for cadence-reporter capture) and in the
    /// returned dispenser's slots (for per-cycle record).
    pub fn wrap(
        inner: Arc<dyn OpDispenser>,
        metrics: &HashMap<String, nbrs_workload::model::MetricSpec>,
        component: &mut nbrs_metrics::component::Component,
        fx: &mut crate::fixture::ScopeFixture,
    ) -> Result<Arc<dyn OpDispenser>, String> {
        if metrics.is_empty() {
            return Ok(inner);
        }
        // Stable ordering on metric names so init-time
        // diagnostics + per-cycle dispatch are reproducible.
        let mut entries: Vec<_> = metrics.iter().collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));

        let component_labels = component.effective_labels().clone();
        let mut slots = Vec::with_capacity(entries.len());
        for (name, spec) in entries {
            let family = spec.family.clone().unwrap_or_else(|| name.clone());

            let format = match &spec.format {
                Some(s) => Some(
                    nbrs_workload::metric_format::parse_format_spec(s)
                        .map_err(|e| format!("metric '{name}' format: {e}"))?,
                ),
                None => None,
            };

            let kind = spec.kind.unwrap_or_default();
            // Instrument labels carry the family name as a label
            // alongside the component's effective labels. The
            // `family` argument to `register_instrument` is the
            // canonical family-name string; the labels are the
            // dimensional cell.
            let instr_labels = component_labels.with("family", family.clone());
            let instrument = match kind {
                nbrs_workload::model::MetricKind::Gauge => {
                    MetricInstrument::Gauge(Arc::new(
                        nbrs_metrics::instruments::gauge::ValueGauge::new(instr_labels),
                    ))
                }
                nbrs_workload::model::MetricKind::Histogram => {
                    MetricInstrument::Histogram(Arc::new(
                        nbrs_metrics::instruments::histogram::Histogram::new(instr_labels),
                    ))
                }
                nbrs_workload::model::MetricKind::Counter => {
                    MetricInstrument::Counter(Arc::new(
                        nbrs_metrics::instruments::counter::Counter::new(instr_labels),
                    ))
                }
            };

            // Resolve the metric's value expression against the
            // GK kernel up front. The op-template synthesiser
            // appended each metric's `value:` expression as a
            // `__metric_<name> := <expr>` binding on the kernel
            // (see `crate::scope::synthesize_metric_binding_name`),
            // so cycle-time reads go through that internal output —
            // arbitrary GK expressions work, not just bare names.
            // The closure-binding-economy walker injected magic
            // externs (body/count/ok) for any of those names this
            // expression referenced, so a workload that writes
            // `value: count` no longer needs a fake result-binding
            // to wedge the slot open.
            let binding_name = crate::scope::synthesize_metric_binding_name(name);
            let _ = fx.register_pull(&binding_name).map_err(|e| {
                format!(
                    "metric '{name}' value '{value}': {e} (synthesised binding \
                     '{binding_name}' should have been registered by the \
                     op-template kernel synthesiser — this is a bug)",
                    value = spec.value,
                )
            })?;

            // SRD-40b §7.2 — collide-on-duplicate at init. The
            // single registry on `Component` is the canonical
            // store; the slot's `Arc<...>` shares the same
            // instrument for the per-cycle hot path. The
            // optional `unit` rides through to drive the
            // `_<unit>` suffix on `metric_family.name` and the
            // `unit` column at capture time (SRD-40a §4.3).
            component.register_instrument_with_unit(
                family.clone(),
                spec.unit.clone(),
                instrument.as_ref(),
            )?;

            slots.push(MetricSlot {
                family,
                value_expr: spec.value.clone(),
                binding_name,
                format,
                instrument,
            });
        }

        Ok(Arc::new(Self { inner, slots }))
    }
}

impl WrappingDispenser for MetricsDispenser {}

impl OpDispenser for MetricsDispenser {
    fn inner_dispenser(&self) -> Option<&dyn OpDispenser> { Some(self.inner.as_ref()) }
    fn execute<'a>(
        &'a self,
        cycle: u64,
        ctx: &'a crate::fixture::ExecCtx<'a>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        Box::pin(async move {
            let result = self.inner.execute(cycle, ctx).await?;
            // Skipped ops produce no measurement — SRD-40b §5.2 /
            // §6 pipeline only fires on a successfully-executed op.
            if result.skipped {
                return Ok(result);
            }
            for slot in &self.slots {
                // Sole resolution path: ctx.wires reads through the
                // live per-fiber kernel handle (project rule
                // "GK Is Canonical Scope"). The op-template synthesiser
                // compiled this metric's `value:` expression into the
                // kernel as `__metric_<name> := <expr>`; pulling that
                // wire fires the expression's eval cone — including
                // magic-extern reads (body/count/ok) ResultDispenser
                // wrote earlier in the same cycle. ctx.wires.get is
                // the live read; no pre-stack snapshot.
                let Some(value) = ctx.wires.get(&slot.binding_name) else {
                    return Err(ExecutionError::Op(crate::adapter::AdapterError {
                        error_name: "metric_value_unresolved".into(),
                        message: format!(
                            "metric '{family}' on cycle {cycle}: synthesised \
                             binding '{binding}' (from `value: {expr}`) did not \
                             resolve through ctx.wires — this is a wiring bug \
                             between scope synthesis and the metrics wrapper",
                            family = slot.family,
                            binding = slot.binding_name,
                            expr = slot.value_expr,
                        ),
                        retryable: false,
                    }));
                };
                let raw = match value_to_f64(&value) {
                    Some(v) => v,
                    None => {
                        // The wire resolved but its type can't
                        // coerce to a numeric metric value (Str,
                        // vector, handle, etc.). Surface as a
                        // hard ExecutionError so the activity's
                        // `errors:` policy decides — by default
                        // (errors=stop) the phase + run halt.
                        return Err(ExecutionError::Op(crate::adapter::AdapterError {
                            error_name: "metric_value_non_numeric".into(),
                            message: format!(
                                "metric '{family}' on cycle {cycle}: \
                                 binding '{expr}' is not coercible to f64 \
                                 (got value variant {disc:?}); metric \
                                 values must be numeric (U64 / F64 / Bool)",
                                family = slot.family,
                                expr = slot.value_expr,
                                disc = std::mem::discriminant(&value),
                            ),
                            retryable: false,
                        }));
                    }
                };
                let sanitised = slot.format.as_ref().map(|f| f.apply(raw)).unwrap_or(raw);
                match &slot.instrument {
                    MetricInstrument::Gauge(g) => g.set(sanitised),
                    MetricInstrument::Histogram(h) => h.record(sanitised as u64),
                    MetricInstrument::Counter(c) => {
                        if sanitised <= 0.0 {
                            crate::diag!(
                                crate::observer::LogLevel::Warn,
                                "counter '{}' got non-positive value {sanitised}; skipping",
                                slot.family,
                            );
                        } else {
                            c.inc_by(sanitised as u64);
                        }
                    }
                }
            }
            Ok(result)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::{ExecutionError, OpResult};
    use crate::fixture::ExecCtx;
    use nbrs_workload::model::{MetricKind, MetricSpec};

    struct CapturesInner;
    impl OpDispenser for CapturesInner {
        fn execute<'a>(
            &'a self,
            _cycle: u64,
            _ctx: &'a ExecCtx<'a>,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
            Box::pin(async move {
                Ok(OpResult { body: None, skipped: false })
            })
        }
    }

    fn fresh_component() -> nbrs_metrics::component::Component {
        nbrs_metrics::component::Component::new(
            nbrs_metrics::labels::Labels::empty(),
            HashMap::new(),
        )
    }

    fn fresh_fixture() -> crate::fixture::ScopeFixture {
        use polydat::compile::assembly::{GkAssembler, WireRef};
        use polydat::library::identity::Identity;
        let mut asm = GkAssembler::new(vec!["cycle".into()]);
        asm.add_node("cycle_id", Box::new(Identity::new()), vec![WireRef::input("cycle")]);
        asm.add_output("cycle_id", WireRef::node("cycle_id"));
        let kernel = asm.compile().expect("test fixture asm.compile");
        crate::fixture::ScopeFixture::new(kernel.program().clone())
    }

    fn make_spec(value: &str, kind: MetricKind, format: Option<&str>) -> MetricSpec {
        MetricSpec {
            value: value.to_string(),
            family: None,
            kind: Some(kind),
            unit: None,
            format: format.map(|s| s.to_string()),
        }
    }

    #[test]
    fn metrics_dispenser_empty_returns_inner_unchanged() {
        let inner: Arc<dyn OpDispenser> = Arc::new(CapturesInner);
        let inner_ptr = Arc::as_ptr(&inner);
        let mut comp = fresh_component();
        let mut fx = fresh_fixture();
        let wrapped = MetricsDispenser::wrap(
            inner.clone(), &HashMap::new(), &mut comp, &mut fx,
        ).unwrap();
        assert_eq!(Arc::as_ptr(&wrapped), inner_ptr);
    }

    /// Test-only introspection: peek at allocated instrument
    /// `Arc`s by family name. Tests need this because `wrap`
    /// returns `Arc<dyn OpDispenser>` and we want to assert
    /// against the same `ValueGauge` / `Histogram` / `Counter`
    /// the wrapper writes through.
    impl MetricsDispenser {
        fn slot_gauge(&self, family: &str) -> Option<Arc<nbrs_metrics::instruments::gauge::ValueGauge>> {
            self.slots.iter().find(|s| s.family == family).and_then(|s| match &s.instrument {
                MetricInstrument::Gauge(g) => Some(g.clone()),
                _ => None,
            })
        }
        fn slot_histogram(&self, family: &str) -> Option<Arc<nbrs_metrics::instruments::histogram::Histogram>> {
            self.slots.iter().find(|s| s.family == family).and_then(|s| match &s.instrument {
                MetricInstrument::Histogram(h) => Some(h.clone()),
                _ => None,
            })
        }
        fn slot_counter(&self, family: &str) -> Option<Arc<nbrs_metrics::instruments::counter::Counter>> {
            self.slots.iter().find(|s| s.family == family).and_then(|s| match &s.instrument {
                MetricInstrument::Counter(c) => Some(c.clone()),
                _ => None,
            })
        }
    }

    fn kernel_with_const_outputs(
        consts: &[(&str, f64)],
    ) -> (
        polydat::kernel::GkKernel,
        crate::fixture::ScopeFixture,
    ) {
        use polydat::compile::assembly::{GkAssembler, WireRef};
        use polydat::library::fixed::ConstF64;
        let mut asm = GkAssembler::new(vec!["cycle".into()]);
        for (name, val) in consts {
            let binding = crate::scope::synthesize_metric_binding_name(name);
            asm.add_node(&binding, Box::new(ConstF64::new(*val)), vec![]);
            asm.add_output(&binding, WireRef::node(&binding));
        }
        let kernel = asm.compile().expect("test kernel asm.compile");
        let fx = crate::fixture::ScopeFixture::new(kernel.program().clone());
        (kernel, fx)
    }

    fn typed_wrap_with_kernel(
        inner: Arc<dyn OpDispenser>,
        decls: &HashMap<String, MetricSpec>,
        consts: &[(&str, f64)],
    ) -> Result<
        (
            Arc<MetricsDispenser>,
            crate::fixture::ResolvedPulls,
            polydat::kernel::GkKernel,
        ),
        String,
    > {
        let (mut kernel, mut fx) = kernel_with_const_outputs(consts);
        let mut comp = fresh_component();

        if decls.is_empty() {
            return Err("typed_wrap_with_kernel requires non-empty decls".into());
        }
        let mut entries: Vec<_> = decls.iter().collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));
        let component_labels = comp.effective_labels().clone();
        let mut slots = Vec::with_capacity(entries.len());
        for (name, spec) in entries {
            let family = spec.family.clone().unwrap_or_else(|| name.clone());
            let format = match &spec.format {
                Some(s) => Some(
                    nbrs_workload::metric_format::parse_format_spec(s)
                        .map_err(|e| format!("metric '{name}' format: {e}"))?,
                ),
                None => None,
            };
            let kind = spec.kind.unwrap_or_default();
            let instr_labels = component_labels.with("family", family.clone());
            let instrument = match kind {
                MetricKind::Gauge => MetricInstrument::Gauge(Arc::new(
                    nbrs_metrics::instruments::gauge::ValueGauge::new(instr_labels),
                )),
                MetricKind::Histogram => MetricInstrument::Histogram(Arc::new(
                    nbrs_metrics::instruments::histogram::Histogram::new(instr_labels),
                )),
                MetricKind::Counter => MetricInstrument::Counter(Arc::new(
                    nbrs_metrics::instruments::counter::Counter::new(instr_labels),
                )),
            };
            comp.register_instrument_with_unit(
                family.clone(), spec.unit.clone(), instrument.as_ref(),
            )?;
            let binding_name = crate::scope::synthesize_metric_binding_name(name);
            let _ = fx.register_pull(&binding_name)?;
            slots.push(MetricSlot {
                family,
                value_expr: spec.value.clone(),
                binding_name,
                format,
                instrument,
            });
        }
        let typed = Arc::new(MetricsDispenser { inner, slots });

        let plan = fx.seal();
        kernel.set_inputs(&[0]);
        let pulls = plan.resolve_with(&mut kernel);
        Ok((typed, pulls, kernel))
    }

    fn run_dispenser(
        dispenser: Arc<dyn OpDispenser>,
        pulls: &crate::fixture::ResolvedPulls,
        kernel: &mut polydat::kernel::GkKernel,
    ) -> Result<OpResult, ExecutionError> {
        let fields = crate::adapter::ResolvedFields::new(vec![], vec![]);
        let cw = crate::wires::CycleWires::new(kernel);
        let ctx = ExecCtx::with_wires(&fields, pulls, &cw);
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        rt.block_on(dispenser.execute(0, &ctx))
    }

    #[test]
    fn metrics_dispenser_gauge_records_f64() {
        let inner: Arc<dyn OpDispenser> = Arc::new(CapturesInner);
        let mut decl = HashMap::new();
        decl.insert("my_factor".into(), make_spec("my_factor", MetricKind::Gauge, None));

        let (typed, pulls, mut kernel) = typed_wrap_with_kernel(
            inner, &decl, &[("my_factor", 3.14)],
        ).unwrap();
        let gauge = typed.slot_gauge("my_factor").unwrap();
        run_dispenser(typed.clone() as Arc<dyn OpDispenser>, &pulls, &mut kernel).unwrap();

        assert!((gauge.get() - 3.14).abs() < 1e-9);
    }

    #[test]
    fn metrics_dispenser_histogram_truncates_to_u64() {
        let inner: Arc<dyn OpDispenser> = Arc::new(CapturesInner);
        let mut decl = HashMap::new();
        decl.insert("latency_ms".into(), make_spec("latency_ms", MetricKind::Histogram, None));

        let (typed, pulls, mut kernel) = typed_wrap_with_kernel(
            inner, &decl, &[("latency_ms", 7.9)],
        ).unwrap();
        let hist = typed.slot_histogram("latency_ms").unwrap();
        run_dispenser(typed.clone() as Arc<dyn OpDispenser>, &pulls, &mut kernel).unwrap();

        let snap = hist.peek_snapshot();
        assert_eq!(snap.max(), 7);
        assert_eq!(snap.len(), 1);
    }

    #[test]
    fn metrics_dispenser_counter_positive_inc_and_skip_non_positive() {
        let inner: Arc<dyn OpDispenser> = Arc::new(CapturesInner);
        let mut decl = HashMap::new();
        decl.insert("ok_inc".into(), make_spec("ok_inc", MetricKind::Counter, None));
        decl.insert("skip_inc".into(), make_spec("skip_inc", MetricKind::Counter, None));

        let (typed, pulls, mut kernel) = typed_wrap_with_kernel(
            inner, &decl, &[("ok_inc", 5.0), ("skip_inc", 0.0)],
        ).unwrap();
        let ok_counter = typed.slot_counter("ok_inc").unwrap();
        let skip_counter = typed.slot_counter("skip_inc").unwrap();
        run_dispenser(typed.clone() as Arc<dyn OpDispenser>, &pulls, &mut kernel).unwrap();

        assert_eq!(ok_counter.get(), 5);
        assert_eq!(skip_counter.get(), 0);
    }

    #[test]
    fn metrics_dispenser_format_rounds_value() {
        let inner: Arc<dyn OpDispenser> = Arc::new(CapturesInner);
        let mut decl = HashMap::new();
        decl.insert(
            "ratio".into(),
            make_spec("ratio", MetricKind::Gauge, Some("#.##")),
        );

        let (typed, pulls, mut kernel) = typed_wrap_with_kernel(
            inner, &decl, &[("ratio", 1.234)],
        ).unwrap();
        let gauge = typed.slot_gauge("ratio").unwrap();
        run_dispenser(typed.clone() as Arc<dyn OpDispenser>, &pulls, &mut kernel).unwrap();

        assert!((gauge.get() - 1.23).abs() < 1e-9);
    }

    #[test]
    fn metrics_dispenser_duplicate_family_errors() {
        let inner: Arc<dyn OpDispenser> = Arc::new(CapturesInner);
        let mut comp = fresh_component();
        comp.register_instrument(
            "recall_at_10",
            nbrs_metrics::component::InstrumentRef::Counter(Arc::new(
                nbrs_metrics::instruments::counter::Counter::new(
                    nbrs_metrics::labels::Labels::of("name", "recall_at_10"),
                ),
            )),
        ).unwrap();

        let mut decl = HashMap::new();
        decl.insert("recall_at_10".into(), make_spec("recall_at_10", MetricKind::Gauge, None));

        let (_kernel, mut fx) = kernel_with_const_outputs(&[("recall_at_10", 0.0)]);
        let err = match MetricsDispenser::wrap(inner, &decl, &mut comp, &mut fx) {
            Ok(_) => panic!("expected duplicate-family error, got Ok"),
            Err(e) => e,
        };
        assert!(err.contains("duplicate family name"), "unexpected error: {err}");
    }

    #[test]
    fn metrics_dispenser_skipped_op_records_nothing() {
        struct SkipInner;
        impl OpDispenser for SkipInner {
            fn execute<'a>(
                &'a self,
                _cycle: u64,
                _ctx: &'a ExecCtx<'a>,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
                Box::pin(async move { Ok(OpResult::skipped()) })
            }
        }
        let mut decl = HashMap::new();
        decl.insert("g".into(), make_spec("g", MetricKind::Gauge, None));

        let (typed, pulls, mut kernel) = typed_wrap_with_kernel(
            Arc::new(SkipInner), &decl, &[("g", 1.0)],
        ).unwrap();
        let gauge = typed.slot_gauge("g").unwrap();

        let res = run_dispenser(typed.clone() as Arc<dyn OpDispenser>, &pulls, &mut kernel).unwrap();
        assert!(res.skipped);
        assert_eq!(gauge.get(), 0.0);
    }

    #[test]
    fn metrics_dispenser_accepts_arbitrary_gk_expression() {
        let inner: Arc<dyn OpDispenser> = Arc::new(CapturesInner);
        let mut decl = HashMap::new();
        decl.insert(
            "computed".into(),
            make_spec("factor * 2.0", MetricKind::Gauge, None),
        );

        let (mut kernel, mut fx) = kernel_with_const_outputs(&[("computed", 6.0)]);
        let mut comp = fresh_component();
        let _ = MetricsDispenser::wrap(inner, &decl, &mut comp, &mut fx)
            .expect("arbitrary GK expression should wrap cleanly");
        let plan = fx.seal();
        kernel.set_inputs(&[0]);
        let _pulls = plan.resolve_with(&mut kernel);
    }

    #[test]
    fn metrics_dispenser_missing_wire_errors_at_init() {
        let inner: Arc<dyn OpDispenser> = Arc::new(CapturesInner);
        let mut decl = HashMap::new();
        decl.insert(
            "missing_metric".into(),
            make_spec("absent_wire", MetricKind::Gauge, None),
        );

        let (_kernel, mut fx) = kernel_with_const_outputs(&[("present", 1.0)]);
        let mut comp = fresh_component();
        let err = MetricsDispenser::wrap(inner, &decl, &mut comp, &mut fx)
            .err()
            .expect("missing-wire metric should error at init");
        assert!(err.contains("absent_wire"), "msg: {err}");
        assert!(err.contains("Available"), "msg: {err}");
    }

    /// Silence dead-code warning for value_to_f64 (used inside
    /// wrapper but private to this module otherwise).
    #[test]
    fn value_to_f64_smoke() {
        assert_eq!(value_to_f64(&polydat::ast::Value::U64(5)), Some(5.0));
        assert_eq!(value_to_f64(&polydat::ast::Value::Bool(true)), Some(1.0));
        assert_eq!(value_to_f64(&polydat::ast::Value::Str("x".into())), None);
    }
}
