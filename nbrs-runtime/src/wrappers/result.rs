// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Result-as-GK adapter (SRD-40b §5). After the inner adapter
//! returns its `OpResult`, this wrapper exposes declared
//! op-result fields plus the magic externs (`body`, `count`,
//! `ok`) as Polydat named wires on the per-fiber op-template kernel
//! via `ctx.wires.write`. Sits between the inner adapter and
//! the metrics layer in the wrapper stack.

use std::sync::Arc;

use crate::adapter::{ExecutionError, OpDispenser, OpResult};
use crate::adapter::WrappingDispenser;
use super::traverse::json_to_value;
use crate::wrapper_registry::{WrapperName, WrapperRegistration, WrapperSubject};

/// SRD-32a wrapper name. Always present — no-op when the op
/// declares no `result:` wires.
pub const NAME: WrapperName = WrapperName::new("result");

fn triggers(s: WrapperSubject) -> bool { s.op().is_some() }

fn describe_assignment(s: WrapperSubject) -> Option<String> {
    let template = s.op()?;
    let spec = template.result.as_ref()?;
    if spec.is_empty() {
        return None;
    }
    let mut names: Vec<String> = Vec::new();
    spec.walk_fragments(|frag| match frag {
        nbrs_workload::model::ResultFragment::Named { name, .. } => {
            names.push(name.to_string());
        }
        nbrs_workload::model::ResultFragment::Source(source) => {
            for line in source.lines() {
                let line = line.trim();
                if let Some((name, _)) = line.split_once(":=") {
                    names.push(name.trim().to_string());
                }
            }
        }
    });
    if names.is_empty() {
        return None;
    }
    names.sort();
    names.dedup();
    Some(format!("result: captures {}", names.join(", ")))
}

inventory::submit! {
    WrapperRegistration {
        name: NAME,
        // `result:` is parsed into ParsedOp.result, not into
        // params, so this wrapper has no owned `params`-keys
        // to declare — the trigger always fires (the cascade
        // wraps unconditionally, no-op when result map is
        // empty).
        owned_fields: &[],
        triggers,
        requires_inner: &[super::traverse::NAME],
        forbids_outer: &[],
        mutually_exclusive_with: &[],
        describe_assignment,
        levels: &[crate::wrapper_registry::WrapperLevel::Op],
    }
}

/// Wraps an inner OpDispenser to expose declared op-result fields
/// as Polydat named wires (SRD-40b §5).
///
/// Per cycle, after the inner adapter returns its `OpResult`, this
/// wrapper walks the op template's `result: HashMap<String,
/// ResultWireSpec>` declarations, computes each value from the
/// result body, and writes it through `ctx.wires.write(name, value)`
/// onto the per-fiber op-template kernel's input slot. Wrappers
/// later in the stack (e.g. `MetricsDispenser`) read freshly through
/// `ctx.wires.get(name)`, so SRD-40b §5.2 metric evaluation sees
/// the values landed this cycle — no HashMap intermediary.
///
/// Insertion order in the wrapper stack (SRD-40b §5.2): inner
/// adapter → ResultDispenser → MetricsDispenser (Phase E).
///
/// Source grammars (SRD-40b §5.1):
/// - `count` — built-in; `OpResult::body.element_count()`.
/// - `ok` — built-in; `true` iff the inner adapter returned `Ok(_)`
///   (errors short-circuit before this wrapper's body runs, so
///   reaching this code already implies success — `ok` is `true`
///   unless the result was a skip).
/// - `<path-expr>` — JSON-pointer-style lookup into the result
///   body. Supports bare names (`field`), dotted paths
///   (`rows.0.field`), and bracketed indices (`rows[0].field`).
/// - `<polydat-call>` — DEFERRED. Recognized as anything containing a
///   `(` token; currently logged once and skipped. Phase E or a
///   follow-up adds the GK-eval-against-result-context path.
pub struct ResultDispenser {
    inner: Arc<dyn OpDispenser>,
    /// Map-shape `count` / `ok` / path-expr declarations that
    /// stay on the dispenser's evaluator (SRD-40b §5.1
    /// backwards compat). SRD-66 string-shape and polydat-call
    /// entries are NOT here — they're compiled into the
    /// op-template kernel's body via SRD-67 Phase 5
    /// `add_result_bindings` and evaluated by GK; the dispenser
    /// just feeds them inputs through the
    /// `populate_kernel_inputs` flag.
    specs: Vec<ResultSlot>,
    /// SRD-67 Phase 5 — when the op's `result:` source contains
    /// any string-shape or polydat-call entries, the dispenser writes
    /// the magic pre-bound inputs (`body` / `count` / `ok`)
    /// through `ctx.wires.write` onto the op-template kernel's
    /// input slots before result-binding expressions evaluate.
    /// The kernel's closure-binding economy returns `NoSlot` for
    /// slots it doesn't reference, so unconditional writes are
    /// safe. Under `KernelOptLevel::Diagnostic` every slot is
    /// allocated regardless.
    populate_kernel_inputs: bool,
}

/// Parsed form of one `result:` declaration.
struct ResultSlot {
    /// Wire name (the map key in `ParsedOp.result`). Drives the
    /// `ctx.wires.write(name, …)` call inside `execute` — the
    /// kernel's matching input slot receives the value.
    wire: String,
    /// Decoded source grammar.
    source: ResultSource,
    /// Optional default rendered as a Polydat Value (string fallback)
    /// when the source resolves to nothing.
    default: Option<polydat::ast::Value>,
}

/// Decoded SRD-40b §5.1 source grammar.
enum ResultSource {
    /// `count` — element count of the result body.
    Count,
    /// `ok` — success boolean.
    Ok,
    /// `<path-expr>` — JSON path into the result body, pre-parsed
    /// into segments.
    Path(Vec<PathSeg>),
    /// `<polydat-call>` — deferred. Carries the raw source for the
    /// follow-up implementation.
    #[allow(dead_code)]
    PolydatCall(String),
}

/// One segment of a parsed path expression.
#[derive(Debug, Clone)]
enum PathSeg {
    Field(String),
    Index(usize),
}

/// Parse `rows[0].field` / `rows.0.field` / `field` into segments.
/// Returns `Err` for empty paths only — anything else parses as a
/// best-effort sequence of identifiers + indices.
fn parse_path_expr(src: &str) -> Result<Vec<PathSeg>, String> {
    let trimmed = src.trim();
    if trimmed.is_empty() {
        return Err("empty path".into());
    }
    let mut segs = Vec::new();
    let mut cur = String::new();
    let mut iter = trimmed.chars().peekable();
    let push_field = |segs: &mut Vec<PathSeg>, cur: &mut String| {
        if !cur.is_empty() {
            // Numeric bareword (after a dot) becomes an index.
            if let Ok(n) = cur.parse::<usize>() {
                segs.push(PathSeg::Index(n));
            } else {
                segs.push(PathSeg::Field(std::mem::take(cur)));
            }
            cur.clear();
        }
    };
    while let Some(&c) = iter.peek() {
        match c {
            '.' => {
                push_field(&mut segs, &mut cur);
                iter.next();
            }
            '[' => {
                push_field(&mut segs, &mut cur);
                iter.next();
                let mut idx = String::new();
                for c2 in iter.by_ref() {
                    if c2 == ']' { break; }
                    idx.push(c2);
                }
                let n: usize = idx.trim().parse().map_err(|_| {
                    format!("path '{src}': invalid index '[{idx}]'")
                })?;
                segs.push(PathSeg::Index(n));
            }
            _ => {
                cur.push(c);
                iter.next();
            }
        }
    }
    push_field(&mut segs, &mut cur);
    if segs.is_empty() {
        return Err(format!("path '{src}': no segments"));
    }
    Ok(segs)
}

/// Walk a parsed path against a JSON value. Returns `None` when
/// any segment misses (object lacks key, array shorter than index,
/// scalar where a container was expected).
fn resolve_path<'a>(
    json: &'a serde_json::Value,
    segs: &[PathSeg],
) -> Option<&'a serde_json::Value> {
    let mut cur = json;
    for seg in segs {
        cur = match (cur, seg) {
            (serde_json::Value::Object(m), PathSeg::Field(k)) => m.get(k)?,
            (serde_json::Value::Array(a),  PathSeg::Index(i)) => a.get(*i)?,
            // Bareword field on an array, or numeric on an object —
            // we don't try to coerce; the path doesn't match.
            _ => return None,
        };
    }
    Some(cur)
}

/// Decode one `(name, source-string)` pair from a SRD-66
/// map-shape `result:` fragment into a `ResultSlot`. Unknown
/// / unparseable sources land as `None` (caller logs and
/// drops them — SRD-40b §5.1 calls for "log a warning and
/// skip" over a hard failure here, since the value mechanism
/// is supposed to be best-effort per cycle).
///
/// String-shape and list-shape `result:` fragments don't go
/// through this function — they compile into the auxiliary
/// kernel under SRD-66 §"Compilation lifecycle" (TBD when
/// the structural-body Value variant lands).
fn decode_slot(
    name: &str,
    raw_source: &str,
) -> Option<ResultSlot> {
    let raw = raw_source.trim();
    let source = if raw == "count" {
        ResultSource::Count
    } else if raw == "ok" {
        ResultSource::Ok
    } else if raw.contains('(') {
        // SRD-66 Surface 1 — GK-expression form. The full
        // kernel-driven path (compile auxiliary kernel,
        // wire body/count/ok externs via the closure-binding
        // rule, evaluate per-cycle) is staged behind this
        // diagnostic until the structural-body Value variant
        // and op-template kernel extension land. Today the
        // form is recognised but evaluates to its default.
        crate::diag!(
            crate::observer::LogLevel::Warn,
            "result wire '{name}': GK-expression source '{raw}' is not yet \
             evaluated end-to-end — slot will resolve to its default. \
             SRD-66 Push 2 follow-up wires the kernel-driven path.",
        );
        ResultSource::PolydatCall(raw.to_string())
    } else {
        // Path expression. Parse failures degrade to skip.
        match parse_path_expr(raw) {
            Ok(segs) => ResultSource::Path(segs),
            Err(e) => {
                crate::diag!(
                    crate::observer::LogLevel::Warn,
                    "result wire '{name}': source '{raw}' is not parseable as a \
                     path expression ({e}) — slot will be skipped.",
                );
                return None;
            }
        }
    };
    Some(ResultSlot { wire: name.to_string(), source, default: None })
}

impl ResultDispenser {
    /// Wrap an inner dispenser with result-as-GK exposure
    /// (SRD-40b §5.2's result-as-GK adapter layer).
    ///
    /// **Always wraps.** Per the SRD, this layer is part of the
    /// canonical per-cycle pipeline: it writes the magic externs
    /// (`body` / `count` / `ok`) into the op-template kernel's
    /// input slots after the inner adapter returns, so any
    /// downstream wrapper (metrics, validation, conditional next
    /// op) reading those names through `ctx.wires.get` sees fresh
    /// values. Writes to slots the kernel didn't allocate (the
    /// closure-binding economy's DCE) silently no-op via
    /// `WriteOutcome::NoSlot` — no overhead for ops whose
    /// op-template kernel doesn't reference any magic extern.
    ///
    /// The optional `result_spec` adds *additional* dispenser-side
    /// dispatch slots (legacy SRD-40b §5.1 path-expr / `count` /
    /// `ok` map-shape forms). Kernel-driven entries (string-shape
    /// source blocks, polydat-call entries) need no per-cycle code
    /// here — `add_result_bindings` compiled them into the
    /// op-template kernel; the magic-extern population this
    /// wrapper always performs is what makes them resolve.
    pub fn wrap(
        inner: Arc<dyn OpDispenser>,
        result_spec: Option<&nbrs_workload::model::ResultSpec>,
    ) -> Arc<dyn OpDispenser> {
        let mut specs: Vec<ResultSlot> = Vec::new();

        if let Some(spec) = result_spec {
            spec.walk_fragments(|frag| match frag {
                nbrs_workload::model::ResultFragment::Named { name, source } => {
                    let raw = source.trim();
                    if raw == "count" || raw == "ok" {
                        if let Some(slot) = decode_slot(name, source) {
                            specs.push(slot);
                        }
                    } else if !raw.contains('(') {
                        // Path expression — keep the legacy
                        // JSON-path path for SRD-40b §5.1 back-compat.
                        if let Some(slot) = decode_slot(name, source) {
                            specs.push(slot);
                        }
                    }
                    // polydat-call entries (raw.contains('(')) are
                    // kernel-driven; nothing per-cycle to do here.
                }
                nbrs_workload::model::ResultFragment::Source(_source) => {
                    // String-shape — fully kernel-driven. No
                    // per-cycle code here.
                }
            });
        }

        // Stable order so wire-resolution warnings (and the
        // per-cycle insertion order) are reproducible.
        specs.sort_by(|a, b| a.wire.cmp(&b.wire));
        Arc::new(Self {
            inner,
            specs,
            // Magic-extern population always fires. The
            // populate_kernel_inputs field is retained for the
            // diagnostic-trace conditional below but its value
            // is now always-true.
            populate_kernel_inputs: true,
        })
    }

    /// Compute the Polydat value for one slot from the cycle's result.
    /// Returns `None` when the slot resolves to nothing and has no
    /// default — caller logs at debug and moves on.
    fn evaluate(
        slot: &ResultSlot,
        result: &OpResult,
    ) -> Option<polydat::ast::Value> {
        match &slot.source {
            ResultSource::Count => {
                let n = result.body.as_ref().map(|b| b.element_count()).unwrap_or(0);
                Some(polydat::ast::Value::U64(n))
            }
            ResultSource::Ok => {
                // Reached only on Ok(_) from the inner adapter; a
                // skipped op also counts as "not a failure" — we
                // treat skip as ok=true, matching the SRD-40b §5
                // intent that this is a binary success signal.
                Some(polydat::ast::Value::Bool(true))
            }
            ResultSource::Path(segs) => {
                let body = result.body.as_ref()?;
                let json = body.to_json();
                resolve_path(&json, segs).map(json_to_value)
                    .or_else(|| slot.default.clone())
            }
            ResultSource::PolydatCall(_) => slot.default.clone(),
        }
    }
}

impl WrappingDispenser for ResultDispenser {}

impl OpDispenser for ResultDispenser {
    fn execute<'a>(
        &'a self,
        cycle: u64,
        ctx: &'a crate::fixture::ExecCtx<'a>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        Box::pin(async move {
            let result = self.inner.execute(cycle, ctx).await?;
            // Skipped ops carry no body; per SRD-40b §5.2 the
            // metric pipeline doesn't fire on skips either. The
            // Phase E wrappers observe `result.skipped` and bail
            // before evaluating.
            if result.skipped {
                return Ok(result);
            }
            for slot in &self.specs {
                if let Some(v) = Self::evaluate(slot, &result) {
                    // Canonical write — lands directly on the op-template
                    // kernel's input slot via ctx.wires. Subsequent
                    // wrapper reads (e.g. MetricsDispenser) see the
                    // fresh value through wires.get on the same cycle.
                    let _ = ctx.wires.write(&slot.wire, v);
                }
                // Per-cycle missing-wire is silent. If a downstream
                // consumer (e.g. MetricsDispenser) references a wire
                // that didn't land on the kernel, that consumer
                // surfaces the failure as a hard ExecutionError —
                // logging it here would just add per-cycle session.log
                // spam without telling the user anything actionable.
            }

            // SRD-67 Phase 5 — magic-extern population. When the
            // op declares any kernel-driven result-bindings
            // (string-shape OR map-shape polydat-call), inject the
            // standard `body` / `count` / `ok` inputs through
            // ctx.wires so the op-template kernel's input slots
            // are populated before any wrapper above this one in
            // the stack reads them. The closure-binding economy
            // drops slots the kernel doesn't reference, so this
            // is safe even if the user's source only references
            // a subset — NoSlot writes are silently ignored.
            // Under KernelOptLevel::Diagnostic every magic extern
            // gets a slot, so all three always land.
            if self.populate_kernel_inputs {
                let count = result.body.as_ref().map(|b| b.element_count()).unwrap_or(0);
                // SRD-66 §"Surface 4 §Open: body type" resolved
                // to `Value::Json` — body rides the kernel as a
                // structural value so `exactly_one_value(body)`
                // can walk row × column shape (per
                // `polydat::library::exactly_one`). For ops
                // whose body has no structural projection the
                // adapter's `to_json()` returns a JSON String,
                // which `exactly_one_value` collapses to
                // `Value::Str`.
                let body_json = result
                    .body
                    .as_ref()
                    .map(|b| b.to_json())
                    .unwrap_or(serde_json::Value::Null);
                let _ = ctx.wires.write("body", polydat::ast::Value::Json(std::sync::Arc::new(body_json)));
                let _ = ctx.wires.write("count", polydat::ast::Value::U64(count));
                let _ = ctx.wires.write("ok", polydat::ast::Value::Bool(true));
            }

            let _ = cycle;
            Ok(result)
        })
    }
    fn inner_dispenser(&self) -> Option<&dyn OpDispenser> { Some(self.inner.as_ref()) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::{AdapterError, ExecutionError, OpResult, ResultBody};
    use crate::fixture::{ExecCtx, ResolvedPulls};
    use std::sync::Arc;

    #[derive(Debug)]
    struct ResultDispBody {
        value: serde_json::Value,
        count: u64,
    }
    impl ResultBody for ResultDispBody {
        fn to_json(&self) -> serde_json::Value { self.value.clone() }
        fn as_any(&self) -> &dyn std::any::Any { self }
        fn element_count(&self) -> u64 { self.count }
    }

    struct FakeInner {
        body: Option<ResultDispBody>,
        error: Option<&'static str>,
    }
    impl OpDispenser for FakeInner {
        fn execute<'a>(
            &'a self,
            _cycle: u64,
            _ctx: &'a ExecCtx<'a>,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
            Box::pin(async move {
                if let Some(msg) = self.error {
                    return Err(ExecutionError::Op(AdapterError {
                        error_name: "test".into(),
                        message: msg.into(),
                        retryable: false,
                    }));
                }
                Ok(OpResult {
                    body: self.body.as_ref().map(|b| Box::new(ResultDispBody {
                        value: b.value.clone(),
                        count: b.count,
                    }) as Box<dyn ResultBody>),
                    skipped: false,
                })
            })
        }
    }

    fn empty_ctx() -> (crate::adapter::ResolvedFields, ResolvedPulls) {
        let fields = crate::adapter::ResolvedFields::new(vec![], vec![]);
        let pulls = ResolvedPulls::empty();
        (fields, pulls)
    }

    fn kernel_with_extern_inputs(names: &[(&str, &str)]) -> polydat::kernel::PolydatKernel {
        use polydat::dsl::compile::compile_polydat;
        let mut src = String::from("input cycle: u64\n");
        for (n, ty) in names {
            src.push_str(&format!("extern {n}: {ty}\n"));
        }
        let mut k = compile_polydat(&src).expect("kernel_with_extern_inputs compile");
        k.set_inputs(&[0]);
        k
    }

    fn run_with_wires(
        dispenser: Arc<dyn OpDispenser>,
        kernel: &mut polydat::kernel::PolydatKernel,
    ) -> Result<OpResult, ExecutionError> {
        let fields = crate::adapter::ResolvedFields::new(vec![], vec![]);
        let pulls = ResolvedPulls::empty();
        let cw = crate::wires::CycleWires::new(kernel);
        let ctx = ExecCtx::with_wires(&fields, &pulls, &cw);
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        rt.block_on(dispenser.execute(0, &ctx))
    }

    #[test]
    fn parse_path_dotted_and_bracketed_equivalent() {
        let a = parse_path_expr("rows[0].value").unwrap();
        let b = parse_path_expr("rows.0.value").unwrap();
        assert_eq!(a.len(), 3);
        assert_eq!(b.len(), 3);
        match (&a[0], &b[0]) {
            (PathSeg::Field(f1), PathSeg::Field(f2)) => assert_eq!(f1, f2),
            _ => panic!("expected leading field"),
        }
        match (&a[1], &b[1]) {
            (PathSeg::Index(0), PathSeg::Index(0)) => {}
            _ => panic!("expected index 0"),
        }
    }

    fn map_spec(entries: &[(&str, &str)]) -> nbrs_workload::model::ResultSpec {
        let mut m = std::collections::BTreeMap::new();
        for (k, v) in entries {
            m.insert((*k).to_string(), (*v).to_string());
        }
        nbrs_workload::model::ResultSpec::Map(m)
    }

    #[test]
    fn result_dispenser_count_and_path() {
        let inner = Arc::new(FakeInner {
            body: Some(ResultDispBody {
                value: serde_json::json!({"rows": [{"value": 42}]}),
                count: 1,
            }),
            error: None,
        });
        let decl = map_spec(&[
            ("row_count", "count"),
            ("first_value", "rows[0].value"),
        ]);

        let wrapped = ResultDispenser::wrap(inner, Some(&decl));
        let mut kernel = kernel_with_extern_inputs(&[
            ("row_count", "u64"),
            ("first_value", "u64"),
        ]);
        let _ = run_with_wires(wrapped, &mut kernel).unwrap();

        let cw = crate::wires::CycleWires::new(&mut kernel);
        let w: &dyn crate::wires::WireSource = &cw;
        assert_eq!(w.get("row_count").map(|v| v.as_u64()), Some(1));
        assert_eq!(w.get("first_value").map(|v| v.as_u64()), Some(42));
    }

    #[test]
    fn result_dispenser_ok_builtin_on_success() {
        let inner = Arc::new(FakeInner {
            body: Some(ResultDispBody { value: serde_json::json!({}), count: 0 }),
            error: None,
        });
        let decl = map_spec(&[("succeeded", "ok")]);

        let wrapped = ResultDispenser::wrap(inner, Some(&decl));
        let mut kernel = kernel_with_extern_inputs(&[("succeeded", "bool")]);
        let _ = run_with_wires(wrapped, &mut kernel).unwrap();

        let cw = crate::wires::CycleWires::new(&mut kernel);
        let w: &dyn crate::wires::WireSource = &cw;
        match w.get("succeeded") {
            Some(polydat::ast::Value::Bool(b)) => assert!(b),
            other => panic!("expected Bool(true), got {other:?}"),
        }
    }

    #[test]
    fn result_dispenser_error_propagates_no_capture_write() {
        let inner = Arc::new(FakeInner {
            body: None,
            error: Some("boom"),
        });
        let decl = map_spec(&[("succeeded", "ok")]);

        let wrapped = ResultDispenser::wrap(inner, Some(&decl));
        let (fields, pulls) = empty_ctx();
        let ctx = ExecCtx::new(&fields, &pulls);
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        let err = rt.block_on(wrapped.execute(0, &ctx)).unwrap_err();
        assert!(format!("{err}").contains("boom"));
    }

    #[test]
    fn result_dispenser_unresolved_path_skips_silently() {
        let inner = Arc::new(FakeInner {
            body: Some(ResultDispBody {
                value: serde_json::json!({"rows": []}),
                count: 0,
            }),
            error: None,
        });
        let decl = map_spec(&[
            ("missing", "rows[0].value"),
        ]);

        let wrapped = ResultDispenser::wrap(inner, Some(&decl));
        let mut kernel = kernel_with_extern_inputs(&[("missing", "u64")]);
        let _ = run_with_wires(wrapped, &mut kernel).unwrap();

        let cw = crate::wires::CycleWires::new(&mut kernel);
        let w: &dyn crate::wires::WireSource = &cw;
        assert!(matches!(w.get("missing"), Some(polydat::ast::Value::None) | None));
    }

    #[test]
    fn result_dispenser_always_wraps_per_srd_40b() {
        let inner: Arc<dyn OpDispenser> = Arc::new(FakeInner {
            body: Some(ResultDispBody { value: serde_json::json!({}), count: 0 }),
            error: None,
        });
        let inner_ptr = Arc::as_ptr(&inner);
        let wrapped = ResultDispenser::wrap(inner.clone(), None);
        assert_ne!(Arc::as_ptr(&wrapped), inner_ptr,
            "ResultDispenser must always wrap so magic-extern population fires");
    }

    #[test]
    fn result_dispenser_skipped_op_writes_no_captures() {
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
        let decl = map_spec(&[("c", "count")]);
        let wrapped = ResultDispenser::wrap(Arc::new(SkipInner), Some(&decl));
        let mut kernel = kernel_with_extern_inputs(&[("c", "u64")]);
        let result = run_with_wires(wrapped, &mut kernel).unwrap();
        assert!(result.skipped);
        let cw = crate::wires::CycleWires::new(&mut kernel);
        let w: &dyn crate::wires::WireSource = &cw;
        assert!(matches!(w.get("c"), Some(polydat::ast::Value::None) | None));
    }

    /// Silence dead-code warnings for `decode_slot` when only
    /// the path-expr branch is exercised by the public tests.
    #[test]
    fn decode_slot_reachable() {
        let slot = decode_slot("c", "count").expect("count slot decodes");
        assert!(matches!(slot.source, ResultSource::Count));
    }
}
