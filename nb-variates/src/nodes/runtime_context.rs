// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Runtime context nodes (SRD 12 §"Runtime context nodes").
//!
//! These nodes project stable runtime surfaces — the current
//! phase, the current cycle ordinal, the value of a dynamic
//! control, the active rate-limiter target, the active fiber
//! count — into GK-readable wires. They are the read-side of
//! the reification principle (SRD 10 §"GK as the unified access
//! surface"): any value a workload might want to read is reached
//! through a GK binding, not a side channel.
//!
//! Like the metric nodes (see `metrics.rs`), these are
//! non-deterministic context projections — their output changes
//! between cycles by definition, so the constant-folder is not
//! allowed to collapse them. They read from globals / thread
//! locals the runtime sets during bootstrap and on every cycle
//! tick.

use std::future::Future;
use std::sync::{Arc, LazyLock, Mutex, RwLock};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::dsl::registry::{Arity, FuncCategory, FuncSig, ParamSpec};
use crate::node::{GkNode, NodeMeta, Port, PortType, Slot, SlotType, Value};

use nb_metrics::component::Component;

// =========================================================================
// Global session-root handle + per-fiber task-local context
// =========================================================================

/// Global handle to the session's component root. Set by the
/// runner during scenario bootstrap so context nodes can resolve
/// `control(...)` reads against the live tree.
static SESSION_ROOT: LazyLock<Mutex<Option<Arc<RwLock<Component>>>>> =
    LazyLock::new(|| Mutex::new(None));

/// Install the session root for every runtime-context node that
/// reads tree state. Call once at scenario bootstrap; subsequent
/// calls overwrite.
pub fn set_session_root(root: Arc<RwLock<Component>>) {
    *SESSION_ROOT.lock().unwrap_or_else(|e| e.into_inner()) = Some(root);
}

fn session_root() -> Option<Arc<RwLock<Component>>> {
    SESSION_ROOT.lock().unwrap_or_else(|e| e.into_inner()).clone()
}

/// Public accessor for the runner-installed session root.
///
/// Used by integration layers that need to walk the component
/// tree from outside this crate — the TUI's control-edit
/// handler, the web API's control endpoints, the
/// `dryrun=controls` renderer, etc. Returns `None` when no
/// session is running (pre-bootstrap, after teardown, tests
/// that never called [`set_session_root`]).
pub fn session_root_handle() -> Option<Arc<RwLock<Component>>> {
    session_root()
}

/// Per-fiber execution context carried across the async call
/// chain via a [`tokio::task_local!`] binding. Thread-locals are
/// unsafe here because tokio's work-stealing scheduler can
/// migrate a task between worker threads at any `.await`; a
/// task-local is bound to the task itself and survives
/// migration.
pub struct FiberContext {
    /// Name of the phase this fiber is running under. Arc'd so
    /// two fibers under the same phase share one allocation.
    pub phase: Arc<str>,
    /// Current cycle ordinal. `AtomicU64` rather than `Cell` so
    /// the context is `Sync` — tokio requires task-local values
    /// to be `Send + Sync`.
    pub cycle: AtomicU64,
}

tokio::task_local! {
    /// Task-local fiber context. Set once per fiber at spawn
    /// time via [`with_fiber_context`]; updated per cycle via
    /// [`set_task_cycle`]. Reads outside a scope (e.g. unit
    /// tests, non-fiber code paths) silently return defaults.
    static FIBER_CTX: FiberContext;
}

/// Wrap a fiber's async body in a `FiberContext` scope. Every
/// runtime-context node read performed inside the future sees
/// the phase name and cycle counter set here.
///
/// Cycle starts at 0 and is updated via [`set_task_cycle`] on
/// every iteration of the fiber's loop.
pub async fn with_fiber_context<F>(phase: Arc<str>, fut: F) -> F::Output
where
    F: Future,
{
    FIBER_CTX.scope(
        FiberContext { phase, cycle: AtomicU64::new(0) },
        fut,
    ).await
}

/// Update the cycle counter in the enclosing [`FiberContext`].
/// Safe to call outside a scope — the update is a no-op if
/// there is no active fiber context (e.g. when the node is
/// evaluated from a unit test that didn't install one).
pub fn set_task_cycle(cycle: u64) {
    let _ = FIBER_CTX.try_with(|ctx| ctx.cycle.store(cycle, Ordering::Relaxed));
}

fn task_phase() -> Option<Arc<str>> {
    FIBER_CTX.try_with(|ctx| ctx.phase.clone()).ok()
}

fn task_cycle() -> u64 {
    FIBER_CTX.try_with(|ctx| ctx.cycle.load(Ordering::Relaxed)).unwrap_or(0)
}

// =========================================================================
// control_set(name, value) — GK-driven write into a control
// =========================================================================

/// GK write node. Submits an f64 write against the named
/// control via the enclosing session root's walk-up lookup.
/// Returns `1` if the write was dispatched, `0` if the session
/// root isn't installed (i.e. outside a running scenario).
///
/// Writes are **non-blocking** — the node spawns a tokio task
/// that calls [`nb_metrics::controls::ErasedControl::set_f64`]
/// and does not await it. The confirmed-apply contract of the
/// control layer still runs in the background; failures are
/// logged but do not stall the fiber that issued the write.
/// See SRD 23 §"Mutation entry points →
/// GK-driven feedback loops".
///
/// `binding` is the name of the GK binding that issued the
/// write — surfaces in control logs so operators can attribute
/// a change to a specific DSL expression rather than just
/// "from GK".
pub struct ControlSet {
    meta: NodeMeta,
    name: String,
    binding: String,
    root: Option<Arc<RwLock<Component>>>,
}

impl ControlSet {
    pub fn new(name: &str, binding: &str) -> Self {
        Self {
            meta: NodeMeta {
                name: "control_set".into(),
                outs: vec![Port::u64("submitted")],
                ins: vec![
                    Slot::const_str("name", name),
                    Slot::Wire(Port::new("value", PortType::F64)),
                ],
            },
            name: name.to_string(),
            binding: binding.to_string(),
            root: session_root(),
        }
    }
}

impl GkNode for ControlSet {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let value = match inputs.first() {
            Some(v) => v.as_f64(),
            None => {
                outputs[0] = Value::U64(0);
                return;
            }
        };
        let Some(ref root) = self.root else {
            // No session root installed (e.g. pre-bootstrap or
            // a pure-kernel test). Silently drop — the test or
            // pre-bootstrap path can't resolve a control anyway.
            outputs[0] = Value::U64(0);
            return;
        };
        let name = self.name.clone();
        let binding = self.binding.clone();
        let root = root.clone();

        // Dispatch the write on a background task. The fiber
        // returning from this eval cannot await an async `set`
        // without blocking the runtime (which would deadlock on
        // the single-threaded variant), so the write fans out
        // asynchronously. Failures log; the next eval reflects
        // the new committed value if any.
        tokio::spawn(async move {
            let erased = {
                let Ok(guard) = root.read() else { return; };
                match guard.find_control_erased_up(&name) {
                    Some(e) => e,
                    None => {
                        eprintln!(
                            "control_set({name}, {value}): no control found via walk-up",
                        );
                        return;
                    }
                }
            };
            let origin = nb_metrics::controls::ControlOrigin::Gk { binding };
            if let Err(e) = erased.set_f64(value, origin).await {
                eprintln!("control_set({name}, {value}) failed: {e}");
            }
        });

        outputs[0] = Value::U64(1);
    }
}

// =========================================================================
// control(name) — read a dynamic control's gauge-projection
// =========================================================================

/// Read the current committed value of a dynamic control as an
/// `f64`, projected through the control's reified gauge.
///
/// Signature: `control(name: String) -> f64`
///
/// Resolves by walking up the component tree from the session
/// root for the first declaration that matches the given name
/// (honoring branch scope). Returns `0.0` in every case where
/// no numeric value is available:
///
/// - Name doesn't resolve to any declared control.
/// - Control is declared but was built without
///   [`nb_metrics::controls::ControlBuilder::reify_as_gauge`],
///   i.e. no f64 projection was registered.
/// - Control has a projection but the current value's
///   `to_f64` returned `None` (e.g. an enum-valued control
///   sitting on a `None`-projected variant).
///
/// This is deliberately silent rather than error-raising: a
/// workload running at cycle time cannot usefully "handle" a
/// missing control, and the alternative (panicking or
/// returning a sentinel like `NaN`) propagates poison
/// downstream. For typed reads of non-numeric controls use
/// [`ControlStr`] (`control_str`) / [`ControlBool`]
/// (`control_bool`), which have explicit defaults for the same
/// missing-control cases. Operators verify controls exist via
/// `dryrun=controls` before running.
pub struct ControlValue {
    meta: NodeMeta,
    name: String,
    root: Option<Arc<RwLock<Component>>>,
}

impl ControlValue {
    pub fn new(name: &str) -> Self {
        Self {
            meta: NodeMeta {
                name: "control".into(),
                outs: vec![Port::new("output", PortType::F64)],
                ins: vec![Slot::const_str("name", name)],
            },
            name: name.to_string(),
            root: session_root(),
        }
    }

    pub(crate) fn read_f64(&self) -> f64 {
        let Some(ref root) = self.root else { return 0.0; };
        let Ok(guard) = root.read() else { return 0.0; };
        guard.find_control_erased_up(&self.name)
            .and_then(|c| c.gauge_f64())
            .unwrap_or(0.0)
    }
}

impl GkNode for ControlValue {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::F64(self.read_f64());
    }
}

// =========================================================================
// control_u64(name) / control_str(name) — typed read sugar
// =========================================================================

/// Read a dynamic control's current value and cast to `u64`.
///
/// Signature: `control_u64(name: String) -> u64`
///
/// Resolves the control via walk-up from the session root;
/// reads the reified-gauge f64 projection and casts to u64
/// (saturating at 0 for negative values). Missing controls /
/// controls without a reified gauge return 0.
///
/// Intended for integer-valued controls — `concurrency`,
/// `max_retries` — that are conceptually u64 but carry an f64
/// reified gauge. Workloads reading one of these as a cycle-
/// time parameter get a `u64` wire directly, without having
/// to pipe `control("…")` through `f64_to_u64`.
pub struct ControlU64 {
    meta: NodeMeta,
    inner: ControlValue,
}

impl ControlU64 {
    pub fn new(name: &str) -> Self {
        Self {
            meta: NodeMeta {
                name: "control_u64".into(),
                outs: vec![Port::u64("output")],
                ins: vec![Slot::const_str("name", name)],
            },
            inner: ControlValue::new(name),
        }
    }
}

impl GkNode for ControlU64 {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        let v = self.inner.read_f64();
        let out = if v < 0.0 { 0 } else { v as u64 };
        outputs[0] = Value::U64(out);
    }
}

/// Read a dynamic control's current value as a boolean.
///
/// Signature: `control_bool(name: String) -> bool`
///
/// Resolves the control via walk-up and interprets the
/// reified-gauge projection as `true` iff the gauge value is
/// non-zero. Missing controls / unreified controls return
/// `false`.
pub struct ControlBool {
    meta: NodeMeta,
    inner: ControlValue,
}

impl ControlBool {
    pub fn new(name: &str) -> Self {
        Self {
            meta: NodeMeta {
                name: "control_bool".into(),
                outs: vec![Port::bool("output")],
                ins: vec![Slot::const_str("name", name)],
            },
            inner: ControlValue::new(name),
        }
    }
}

impl GkNode for ControlBool {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        let v = self.inner.read_f64();
        outputs[0] = Value::Bool(v != 0.0);
    }
}

/// Read a dynamic control's current value as a human-readable
/// string.
///
/// Signature: `control_str(name: String) -> String`
///
/// Resolves the control via walk-up and renders via the
/// erased-control `value_string()` hook. For controls whose
/// `T` derives a sensible `Debug` (u32, bool, enum, a wrapper
/// struct) this returns what an operator would type. Missing
/// controls return the empty string.
pub struct ControlStr {
    meta: NodeMeta,
    name: String,
    root: Option<Arc<RwLock<Component>>>,
}

impl ControlStr {
    pub fn new(name: &str) -> Self {
        Self {
            meta: NodeMeta {
                name: "control_str".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::const_str("name", name)],
            },
            name: name.to_string(),
            root: session_root(),
        }
    }
}

impl GkNode for ControlStr {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        let s = self.root.as_ref()
            .and_then(|r| r.read().ok()
                .and_then(|g| g.find_control_erased_up(&self.name))
                .map(|c| c.value_string()))
            .unwrap_or_default();
        outputs[0] = Value::Str(s);
    }
}

// =========================================================================
// rate() / concurrency() — thin aliases over control(...)
// =========================================================================

/// Sugar for `control("rate")` — read the current rate-limiter
/// target as ops/sec.
pub struct RateNow {
    meta: NodeMeta,
    inner: ControlValue,
}

impl Default for RateNow {
    fn default() -> Self { Self::new() }
}

impl RateNow {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "rate".into(),
                outs: vec![Port::new("output", PortType::F64)],
                ins: Vec::new(),
            },
            inner: ControlValue::new("rate"),
        }
    }
}

impl GkNode for RateNow {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::F64(self.inner.read_f64());
    }
}

/// Sugar for `control("concurrency")` — read the current fiber
/// count for the nearest phase.
pub struct ConcurrencyNow {
    meta: NodeMeta,
    inner: ControlValue,
}

impl Default for ConcurrencyNow {
    fn default() -> Self { Self::new() }
}

impl ConcurrencyNow {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "concurrency".into(),
                outs: vec![Port::new("output", PortType::F64)],
                ins: Vec::new(),
            },
            inner: ControlValue::new("concurrency"),
        }
    }
}

impl GkNode for ConcurrencyNow {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::F64(self.inner.read_f64());
    }
}

// =========================================================================
// phase() — current phase name (thread-local)
// =========================================================================

/// Current phase name. Reads a thread-local set by the phase
/// executor; returns an empty string when unset (e.g. outside
/// of a cycle, in tests that don't install a phase).
pub struct PhaseName {
    meta: NodeMeta,
}

impl Default for PhaseName {
    fn default() -> Self { Self::new() }
}

impl PhaseName {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "phase".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: Vec::new(),
            },
        }
    }
}

impl GkNode for PhaseName {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        let name = task_phase()
            .map(|s| s.to_string())
            .unwrap_or_default();
        outputs[0] = Value::Str(name);
    }
}

// =========================================================================
// cycle() — current cycle ordinal (thread-local)
// =========================================================================

/// Current cycle ordinal. Reads a thread-local set by the phase
/// executor. For bindings that already declare `cycle` as a
/// named input this is redundant; it exists so bindings which
/// never named `cycle` explicitly can still reach it (SRD 10's
/// "cycle is not magic" rule — the node is context, not a
/// privileged input).
pub struct CycleNow {
    meta: NodeMeta,
}

impl Default for CycleNow {
    fn default() -> Self { Self::new() }
}

impl CycleNow {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "cycle".into(),
                outs: vec![Port::u64("output")],
                ins: Vec::new(),
            },
        }
    }
}

impl GkNode for CycleNow {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(task_cycle());
    }
}

// =========================================================================
// Registration
// =========================================================================

pub fn signatures() -> &'static [FuncSig] {
    use FuncCategory as C;
    &[
        FuncSig {
            name: "control", category: C::Context, outputs: 1,
            description: "read a dynamic control's current value as f64",
            help: "Projects a [dynamic control](SRD 23) into the GK graph. Walks\nup the component tree from the session root, honors branch\nscope, and returns the control's reified gauge projection\n(f64). Missing controls and un-projected values return 0.0.\nParameters:\n  name — control name to resolve",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "name", slot_type: SlotType::ConstStr, required: true, example: "\"rate\"", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "control_u64", category: C::Context, outputs: 1,
            description: "read a dynamic control's current value as u64",
            help: "Sugar for casting control(name) from f64 to u64. Negative\ngauge values saturate at 0; missing controls return 0.\nPrefer over `f64_to_u64(control(name))` for clarity.\nParameters:\n  name — control name to resolve",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "name", slot_type: SlotType::ConstStr, required: true, example: "\"concurrency\"", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "control_bool", category: C::Context, outputs: 1,
            description: "read a dynamic control's current value as bool",
            help: "Reads the control's reified-gauge projection and returns\ntrue iff the value is non-zero. Missing controls / unreified\ncontrols return false.\nParameters:\n  name — control name to resolve",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "name", slot_type: SlotType::ConstStr, required: true, example: "\"enabled\"", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "control_str", category: C::Context, outputs: 1,
            description: "read a dynamic control's current value as String",
            help: "Reads the erased-control value_string() rendering. Useful\nfor enum-valued or string-valued controls (error policy,\nlog level). Missing controls return \"\".\nParameters:\n  name — control name to resolve",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "name", slot_type: SlotType::ConstStr, required: true, example: "\"log_level\"", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "control_set", category: C::Context, outputs: 1,
            description: "write a dynamic control value from GK (non-blocking)",
            help: "Submits an f64 write against a named dynamic control. The\nwrite dispatches on a background tokio task; the fiber does\nnot block. The target control must declare a from_f64\nconverter (see ControlBuilder::from_f64) or the write is\nrejected with a ValidationFailed message in the log.\nReturns 1 if dispatched, 0 if no session root is installed.\nParameters:\n  name  — control name to resolve (walk-up from session root)\n  value — f64 wire; the control's converter maps this to its\n          native type (e.g. f64 → u32 for concurrency)",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "name", slot_type: SlotType::ConstStr, required: true, example: "\"rate\"", constraint: None },
                ParamSpec { name: "value", slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "rate", category: C::Context, outputs: 1,
            description: "current rate-limiter target (f64 ops/sec)",
            help: "Sugar for control(\"rate\"). Returns 0.0 if no rate control\nis declared.",
            identity: None, variadic_ctor: None,
            params: &[],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "concurrency", category: C::Context, outputs: 1,
            description: "current fiber count (f64)",
            help: "Sugar for control(\"concurrency\"). Returns 0.0 if no\nconcurrency control is declared.",
            identity: None, variadic_ctor: None,
            params: &[],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "phase", category: C::Context, outputs: 1,
            description: "current phase name (String)",
            help: "Returns the name of the phase the current fiber is running\nunder. Thread-local — outside a phase this reads as empty.",
            identity: None, variadic_ctor: None,
            params: &[],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "cycle", category: C::Context, outputs: 1,
            description: "current cycle ordinal (u64)",
            help: "Returns the cycle ordinal of the current fiber. Sugar for\nreaching the cycle value without naming it as an explicit\ninput declaration.",
            identity: None, variadic_ctor: None,
            params: &[],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
    ]
}

pub(crate) fn build_node(
    name: &str,
    _wires: &[crate::assembly::WireRef],
    consts: &[crate::dsl::factory::ConstArg],
) -> Option<Result<Box<dyn crate::node::GkNode>, String>> {
    match name {
        "control" => {
            let n = consts.first().map(|c| c.as_str().to_string()).unwrap_or_default();
            Some(Ok(Box::new(ControlValue::new(&n))))
        }
        "control_u64" => {
            let n = consts.first().map(|c| c.as_str().to_string()).unwrap_or_default();
            Some(Ok(Box::new(ControlU64::new(&n))))
        }
        "control_bool" => {
            let n = consts.first().map(|c| c.as_str().to_string()).unwrap_or_default();
            Some(Ok(Box::new(ControlBool::new(&n))))
        }
        "control_str" => {
            let n = consts.first().map(|c| c.as_str().to_string()).unwrap_or_default();
            Some(Ok(Box::new(ControlStr::new(&n))))
        }
        "control_set" => {
            let n = consts.first().map(|c| c.as_str().to_string()).unwrap_or_default();
            // The DSL compiler installs the enclosing binding's
            // name via `factory::compile_ctx::scoped_binding`
            // before each `build_node` call. That binding is
            // what appears in `ControlOrigin::Gk { binding }`
            // for attribution. If no scope is active (library
            // tests that call build_node directly) we fall back
            // to the control name.
            let binding = crate::dsl::factory::compile_ctx::current_binding()
                .unwrap_or_else(|| n.clone());
            Some(Ok(Box::new(ControlSet::new(&n, &binding))))
        }
        "rate" => Some(Ok(Box::new(RateNow::new()))),
        "concurrency" => Some(Ok(Box::new(ConcurrencyNow::new()))),
        "phase" => Some(Ok(Box::new(PhaseName::new()))),
        "cycle" => Some(Ok(Box::new(CycleNow::new()))),
        _ => None,
    }
}

crate::register_nodes!(signatures, build_node);

#[cfg(test)]
mod tests {
    use super::*;
    use nb_metrics::controls::{BranchScope, ControlBuilder};
    use nb_metrics::labels::Labels;
    use std::collections::HashMap;
    use std::sync::{Mutex, MutexGuard};

    /// Serializes test access to the global `SESSION_ROOT` and the
    /// thread-locals. Every test that mutates a global holds this
    /// mutex for its entire body so writes don't interleave across
    /// the parallel test runner.
    static TEST_LOCK: Mutex<()> = Mutex::new(());

    fn serial_test() -> MutexGuard<'static, ()> {
        TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Build a session root with a declared control, install it
    /// as the global, and return the root handle so callers can
    /// mutate the control further.
    fn install_session_with_control(
        name: &str,
        initial: u32,
    ) -> Arc<RwLock<Component>> {
        let root = Component::root(
            Labels::empty().with("type", "session").with("session", "t"),
            HashMap::new(),
        );
        root.read().unwrap().controls().declare(
            ControlBuilder::new(name, initial)
                .reify_as_gauge(|v| Some(*v as f64))
                .branch_scope(BranchScope::Subtree)
                .build(),
        );
        set_session_root(root.clone());
        root
    }

    #[test]
    fn control_reads_current_value() {
        let _g = serial_test();
        install_session_with_control("rate", 500);
        let node = ControlValue::new("rate");
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        assert_eq!(out[0].as_f64(), 500.0);
    }

    #[test]
    fn control_missing_name_returns_zero() {
        let _g = serial_test();
        install_session_with_control("rate", 500);
        let node = ControlValue::new("not_declared");
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        assert_eq!(out[0].as_f64(), 0.0);
    }

    #[tokio::test]
    async fn control_tracks_live_writes() {
        let _g = serial_test();
        let root = install_session_with_control("rate", 100);
        let node = ControlValue::new("rate");

        // Read initial.
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        assert_eq!(out[0].as_f64(), 100.0);

        // Mutate via the control API; the node sees the new value
        // on the next eval.
        let c: nb_metrics::controls::Control<u32> = root.read().unwrap()
            .controls().get("rate").unwrap();
        c.set(4242, nb_metrics::controls::ControlOrigin::Test)
            .await
            .unwrap();

        node.eval(&[], &mut out);
        assert_eq!(out[0].as_f64(), 4242.0);
    }

    #[test]
    fn rate_node_is_alias_of_control_rate() {
        let _g = serial_test();
        install_session_with_control("rate", 750);
        let node = RateNow::new();
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        assert_eq!(out[0].as_f64(), 750.0);
    }

    #[test]
    fn concurrency_node_reads_concurrency_control() {
        let _g = serial_test();
        install_session_with_control("concurrency", 32);
        let node = ConcurrencyNow::new();
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        assert_eq!(out[0].as_f64(), 32.0);
    }

    #[tokio::test]
    async fn phase_and_cycle_read_from_task_locals() {
        let phase_arc: Arc<str> = Arc::from("rampup");
        with_fiber_context(phase_arc.clone(), async {
            set_task_cycle(4242);

            let p = PhaseName::new();
            let mut out = [Value::None];
            p.eval(&[], &mut out);
            assert_eq!(out[0].as_str(), "rampup");

            let c = CycleNow::new();
            let mut cycle_out = [Value::None];
            c.eval(&[], &mut cycle_out);
            assert_eq!(cycle_out[0].as_u64(), 4242);
        }).await;
    }

    #[test]
    fn phase_is_empty_outside_fiber_scope() {
        // Reading outside a fiber context — e.g. from a unit test
        // or a non-fiber call site — silently returns the empty
        // string rather than panicking. The task_local's `try_with`
        // returns Err, which we map to the default.
        let p = PhaseName::new();
        let mut out = [Value::None];
        p.eval(&[], &mut out);
        assert_eq!(out[0].as_str(), "");
    }

    #[test]
    fn cycle_is_zero_outside_fiber_scope() {
        let c = CycleNow::new();
        let mut out = [Value::None];
        c.eval(&[], &mut out);
        assert_eq!(out[0].as_u64(), 0);
    }

    #[tokio::test]
    async fn set_task_cycle_is_noop_outside_scope() {
        // A stray call with no active scope must not panic.
        set_task_cycle(99);
        let c = CycleNow::new();
        let mut out = [Value::None];
        c.eval(&[], &mut out);
        assert_eq!(out[0].as_u64(), 0);
    }

    // ---- control_set ------------------------------------------

    #[tokio::test]
    async fn control_set_writes_through_converter_and_reaches_committed() {
        let _g = serial_test();
        // Install a root with a concurrency control that accepts
        // f64 writes via an explicit from_f64 converter.
        let root = Component::root(
            Labels::empty().with("type", "session").with("session", "s_cs"),
            std::collections::HashMap::new(),
        );
        let c: nb_metrics::controls::Control<u32> = nb_metrics::controls::
            ControlBuilder::new("concurrency", 4u32)
                .reify_as_gauge(|v| Some(*v as f64))
                .from_f64(|v| {
                    if v < 0.0 || v > u32::MAX as f64 {
                        Err(format!("out of range: {v}"))
                    } else {
                        Ok(v as u32)
                    }
                })
                .branch_scope(nb_metrics::controls::BranchScope::Subtree)
                .build();
        root.read().unwrap().controls().declare(c.clone());
        set_session_root(root);

        // Issue the write through the GK node.
        let node = ControlSet::new("concurrency", "feedback_loop");
        let mut out = [Value::None];
        node.eval(&[Value::F64(64.0)], &mut out);
        assert_eq!(out[0].as_u64(), 1, "write should report submitted");

        // The write is async; yield a few times for the spawned
        // task to run through validate → fanout → commit.
        for _ in 0..10 {
            tokio::task::yield_now().await;
            if c.value() == 64u32 { break; }
        }
        assert_eq!(c.value(), 64u32);
        let committed = c.get();
        assert!(matches!(
            committed.origin,
            nb_metrics::controls::ControlOrigin::Gk { .. }
        ));
    }

    #[test]
    fn control_u64_casts_gauge_to_integer() {
        let _g = serial_test();
        install_session_with_control("concurrency", 64);
        let node = ControlU64::new("concurrency");
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        assert_eq!(out[0].as_u64(), 64);
    }

    #[test]
    fn control_u64_missing_name_returns_zero() {
        let _g = serial_test();
        install_session_with_control("concurrency", 5);
        let node = ControlU64::new("not_there");
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        assert_eq!(out[0].as_u64(), 0);
    }

    #[test]
    fn control_bool_projects_gauge_to_boolean() {
        let _g = serial_test();
        install_session_with_control("enabled", 1);
        let node = ControlBool::new("enabled");
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        assert_eq!(out[0].as_bool(), true);
    }

    #[test]
    fn control_bool_zero_is_false() {
        let _g = serial_test();
        install_session_with_control("enabled", 0);
        let node = ControlBool::new("enabled");
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        assert_eq!(out[0].as_bool(), false);
    }

    #[test]
    fn control_bool_missing_name_is_false() {
        let _g = serial_test();
        install_session_with_control("enabled", 1);
        let node = ControlBool::new("absent");
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        assert_eq!(out[0].as_bool(), false);
    }

    #[test]
    fn control_str_renders_value_string() {
        let _g = serial_test();
        install_session_with_control("concurrency", 42);
        let node = ControlStr::new("concurrency");
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        // u32's Debug rendering is its decimal representation.
        assert_eq!(out[0].as_str(), "42");
    }

    #[test]
    fn control_str_missing_name_returns_empty() {
        let _g = serial_test();
        install_session_with_control("concurrency", 42);
        let node = ControlStr::new("log_level");
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        assert_eq!(out[0].as_str(), "");
    }

    #[tokio::test]
    async fn control_set_records_compile_time_binding_attribution() {
        let _g = serial_test();
        // Build a control that accepts f64 writes and install
        // the session root.
        let root = Component::root(
            Labels::empty().with("type", "session").with("session", "attr"),
            std::collections::HashMap::new(),
        );
        let c: nb_metrics::controls::Control<f64> = nb_metrics::controls::
            ControlBuilder::new("rate", 100.0)
                .reify_as_gauge(|v| Some(*v))
                .from_f64(|v| Ok(v))
                .branch_scope(nb_metrics::controls::BranchScope::Subtree)
                .build();
        root.read().unwrap().controls().declare(c.clone());
        set_session_root(root.clone());

        // Simulate a compiler constructing a control_set factory
        // under a binding scope named `rate_adj`. We can't call
        // `build_node` from inside the same crate's private
        // factory path directly, so reach into the nodes
        // registration helper via the same build-by-name route
        // the compiler uses.
        let _scope = crate::dsl::factory::compile_ctx::scoped_binding("rate_adj");
        let consts = [crate::dsl::factory::ConstArg::Str("rate".into())];
        let node = crate::dsl::factory::build_node(
            "control_set", &[], &consts,
        ).expect("control_set should build");
        let mut out = [Value::None];
        node.eval(&[Value::F64(4242.0)], &mut out);

        // Let the spawned write complete.
        for _ in 0..40 {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            if c.value() == 4242.0 { break; }
        }
        assert_eq!(c.value(), 4242.0);
        match c.get().origin {
            nb_metrics::controls::ControlOrigin::Gk { ref binding } => {
                assert_eq!(binding, "rate_adj",
                    "attribution should be the DSL binding name, not the control name");
            }
            other => panic!("expected Gk origin, got {other:?}"),
        }
    }

    #[test]
    fn control_set_returns_zero_without_session_root() {
        let _g = serial_test();
        // Explicitly clear the session root so the node can't
        // resolve anything.
        *SESSION_ROOT.lock().unwrap_or_else(|e| e.into_inner()) = None;

        let node = ControlSet::new("anything", "b");
        let mut out = [Value::None];
        node.eval(&[Value::F64(1.0)], &mut out);
        assert_eq!(out[0].as_u64(), 0);
    }
}
