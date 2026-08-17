// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Context state nodes: non-deterministic, session-scoped values.
//!
//! These nodes produce values from the execution environment rather
//! than the coordinate space. They break the deterministic model
//! and should be used deliberately.
//!
//! SRD-80b Phase E migration. All authoring goes through
//! `#[polydat_node]`. Three shapes appear here:
//!
//! * Pure clock / OS reads (`current_epoch_millis`, `thread_id`) —
//!   plain body, marked `Nondeterministic`.
//! * Construction-frozen captures (`session_start_millis`,
//!   `elapsed_millis`, `tmp_dir`, `env_or`) — use
//!   `#[poly_const(setup_fn, from = ())]` (or `from = <const_arg>`
//!   when the capture depends on a const) to compute the cached
//!   value once at construction. The body just reads the cache.
//! * Fallible construction (`env`) — body returns
//!   `Result<String, String>`. The macro emits `try_new` and
//!   propagates `Err` as a workload-compile error via the build
//!   closure.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::ast::{PolydatNode, NodeMeta, Port, Slot, SlotType, Value};
use crate::derive_support::Const;

/// Current wall-clock time in epoch milliseconds.
///
/// Signature: `() -> (u64)`. Non-deterministic — clock read per eval.
#[crate::polydat_node(
    category = Context,
    purity = Nondeterministic("reads system clock"),
)]
fn current_epoch_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// Helper for the time-capture setup fns: read epoch millis now.
/// Plain function pointer compatible with `#[poly_const(fn, from = ())]`.
fn capture_epoch_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn session_start_millis_jit_constants(node: &SessionStartMillis) -> Vec<u64> {
    vec![node.start]
}

/// Session start time in epoch milliseconds, frozen at construction.
///
/// Signature: `() -> (u64)`. Deterministic within a session.
///
/// Captured-at-construction values are marked Nondeterministic so
/// they are excluded from const-fold identity (workload hash stays
/// stable across runs even though the captured value differs).
#[crate::polydat_node(
    category = Context,
    purity = Nondeterministic("session start time captured from system clock"),
    jit_constants = session_start_millis_jit_constants,
)]
fn session_start_millis(
    #[poly_const(capture_epoch_millis, from = ())]
    start: &u64,
) -> u64 {
    *start
}

fn elapsed_millis_jit_constants(node: &ElapsedMillis) -> Vec<u64> {
    vec![node.start]
}

/// Elapsed milliseconds since session start.
///
/// Signature: `() -> (u64)`. Non-deterministic, grows monotonically.
#[crate::polydat_node(
    category = Context,
    purity = Nondeterministic("monotonic elapsed time from system clock"),
    jit_constants = elapsed_millis_jit_constants,
)]
fn elapsed_millis(
    #[poly_const(capture_epoch_millis, from = ())]
    start: &u64,
) -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    now.saturating_sub(*start)
}

/// Current OS thread numeric identifier.
///
/// Signature: `() -> (u64)`. Non-deterministic — value depends on
/// the scheduling thread.
#[crate::polydat_node(
    category = Context,
    purity = Nondeterministic("OS thread identity varies across fibers"),
)]
fn thread_id() -> u64 {
    // `ThreadId` is opaque; we extract the numeric id via the
    // Debug formatter (`ThreadId(N)`).
    let id = std::thread::current().id();
    let id_str = format!("{id:?}");
    let num = id_str.trim_start_matches("ThreadId(").trim_end_matches(')');
    num.parse().unwrap_or(0)
}

/// Environment variable read, frozen at construction.
///
/// Signature: `env(name: const str) -> str`. Reads the named env
/// var once at workload-compile time; the captured value is
/// returned on every eval. Errors at construction when the
/// variable is unset — use `env_or` for a defaulted form.
///
/// SRD-80b Phase E: fallible construction. The body returns
/// `Result<String, String>`; the macro runs it once inside
/// `try_new`, caches the Ok value, and propagates Err as a
/// build-time error.
#[crate::polydat_node(category = Context)]
fn env(name: Const<&str>) -> Result<String, String> {
    let var = name.0;
    std::env::var(var).map_err(|_| format!(
        "env('{var}'): environment variable not set; \
         use env_or('{var}', '<default>') if a fallback is acceptable",
    ))
}

/// Environment variable read with default, frozen at construction.
///
/// Signature: `env_or(name: const str, default: const str) -> str`.
/// Reads the named env var at construction; falls back to the
/// literal `default` when the variable is unset. The captured
/// value is constant for the session.
#[crate::polydat_node(category = Context)]
fn env_or(
    name: Const<&str>,
    default: Const<&str>,
    #[poly_const(capture_env_opt, from = name)]
    captured: &Option<String>,
) -> String {
    match captured {
        Some(v) => v.clone(),
        None => default.0.to_string(),
    }
}

/// Setup helper for `env_or`: read the env var into `Option<String>`.
/// `None` indicates the var is unset; the body picks the default.
fn capture_env_opt(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

/// System temp directory, frozen at construction.
///
/// Signature: `tmp_dir() -> str`.
#[crate::polydat_node(category = Context)]
fn tmp_dir(
    #[poly_const(capture_tmp_dir, from = ())]
    path: &String,
) -> String {
    path.clone()
}

/// Setup helper for `tmp_dir`: capture `std::env::temp_dir()` as
/// a UTF-8 string. Falls back to `/tmp` on non-UTF-8 paths
/// (extremely rare on modern systems).
fn capture_tmp_dir() -> String {
    std::env::temp_dir()
        .to_str()
        .map(String::from)
        .unwrap_or_else(|| "/tmp".to_string())
}

/// Monotonic counter (non-deterministic). SRD-80 PR B.11 migration.
///
/// Returns 0, 1, 2, ... across all calls. Thread-safe via AtomicU64.
#[crate::polydat_node(
    category = Context,
    purity = Nondeterministic("monotonic counter incremented per call"),
)]
fn counter(
    #[poly_default(0u64)] start: Const<u64>,
    #[poly_const(AtomicU64::new, from = start)]
    count: &AtomicU64,
) -> u64 {
    count.fetch_add(1, Ordering::Relaxed)
}

// ---------------------------------------------------------------------------
// Cursor limit — not a #[polydat_node]: it's a passthrough whose
// `max_items` value is read via the const-slot meta by the cursor
// machinery, and is constructed directly by the cursor compiler
// (`polydat::dsl::compile`) rather than via the DSL function
// registry. Keeping the hand-written shape preserves the explicit
// constructor used at that one call site.
// ---------------------------------------------------------------------------

/// Cursor limit node: passes through the input value unchanged.
///
/// Inserted by the compiler when the `limit` activity parameter is present.
/// The node is a visible, documented passthrough in the Polydat graph that
/// clamps the cursor's extent. The `max_items` value is used by the
/// `Cursors` system to determine when to stop advancing.
///
/// Signature: `limit(input: u64, max_items: u64) -> u64`
pub struct CursorLimit {
    meta: NodeMeta,
    /// Maximum number of items the cursor should yield.
    pub max_items: u64,
}

impl CursorLimit {
    pub fn new(max_items: u64) -> Self {
        Self {
            meta: NodeMeta {
                name: "limit".into(),
                outs: vec![Port::u64("output")],
                ins: vec![Slot::Wire(Port::u64("input"))],
            },
            max_items,
        }
    }
}

impl PolydatNode for CursorLimit {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        // Pure passthrough — the limit is enforced by the cursor system,
        // not by the node evaluation. The node exists to be visible in
        // the graph and to carry the max_items metadata.
        outputs[0] = inputs[0].clone();
    }
}

// ---------------------------------------------------------------------------
// Signature declarations for the cursor-limit node only. Every
// other context node registers itself via the `#[polydat_node]`
// macro's inventory submission.
// ---------------------------------------------------------------------------

use crate::dsl::registry::{Arity, FuncCategory, FuncSig, ParamSpec};

/// Signature for the cursor-limit passthrough.
pub fn signatures() -> &'static [FuncSig] {
    use FuncCategory as C;
    &[
        FuncSig {
            name: "limit", category: C::Context, outputs: 1,
            description: "cursor limit — clamps extent for smoke testing",
            help: "Passes through the input value unchanged. Inserted by the compiler\n\
                   when the `limit` activity parameter is present. The max_items value\n\
                   is used by the cursor system to stop advancing early.\n\
                   Parameters:\n  input — cursor wire (u64)\n  max_items — maximum items to yield\n\
                   Example: row = limit(row, 100)  // stop after 100 items",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "row", constraint: None },
                ParamSpec { name: "max_items", slot_type: SlotType::ConstU64, required: true, example: "100", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: crate::ast::Commutativity::Positional,
            default_resolver: None,
            output_type: crate::dsl::registry::OutputType::Fixed,
            // Hand registration: no static return-port declaration;
            // type inference falls back to the name heuristic.
            output_port: None,
        },
    ]
}

/// Build the cursor-limit node by name. Other context nodes
/// register via the `#[polydat_node]` macro's inventory hook.
pub(crate) fn build_node(name: &str, _wires: &[crate::compile::assembly::WireRef], _wire_types: &[crate::ast::PortType], consts: &[crate::dsl::factory::ConstArg]) -> Option<Result<Box<dyn crate::ast::PolydatNode>, String>> {
    match name {
        "limit" => {
            let max_items = consts.first().map(|c| c.as_u64()).unwrap_or(u64::MAX);
            Some(Ok(Box::new(CursorLimit::new(max_items))))
        }
        _ => None,
    }
}

crate::register_nodes!(signatures, build_node);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_epoch_millis_reasonable() {
        let node = CurrentEpochMillis::new();
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        let millis = out[0].as_u64();
        // Should be after 2024-01-01 (1704067200000)
        assert!(millis > 1_704_067_200_000);
    }

    #[test]
    fn session_start_frozen() {
        let node = SessionStartMillis::new();
        let mut out1 = [Value::None];
        let mut out2 = [Value::None];
        node.eval(&[], &mut out1);
        node.eval(&[], &mut out2);
        assert_eq!(out1[0].as_u64(), out2[0].as_u64());
    }

    #[test]
    fn elapsed_grows() {
        let node = ElapsedMillis::new();
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        let e1 = out[0].as_u64();
        // Elapsed should be non-negative
        assert!(e1 < 1000, "elapsed should be small right after creation");
    }

    #[test]
    fn counter_increments() {
        let node = Counter::new(0);
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        assert_eq!(out[0].as_u64(), 0);
        node.eval(&[], &mut out);
        assert_eq!(out[0].as_u64(), 1);
        node.eval(&[], &mut out);
        assert_eq!(out[0].as_u64(), 2);
    }

    #[test]
    fn counter_starting_at() {
        let node = Counter::new(100);
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        assert_eq!(out[0].as_u64(), 100);
        node.eval(&[], &mut out);
        assert_eq!(out[0].as_u64(), 101);
    }

    /// Generate a unique env-var name per test so concurrent test
    /// threads can't collide on the same key. The process env is
    /// global state; using fixed names like `TEST_VAR` makes
    /// tests order-dependent.
    fn unique_var(tag: &str) -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        format!("__NBRS_TEST_{tag}_{nanos:x}")
    }

    #[test]
    fn env_captures_value_at_construction() {
        let var = unique_var("ENV");
        unsafe { std::env::set_var(&var, "captured-value"); }
        let node = Env::try_new(var.clone()).expect("env should read the set var");
        // Mutating the env after construction must NOT change the
        // node's output — the value is frozen at construction.
        unsafe { std::env::set_var(&var, "later-value"); }
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        assert_eq!(out[0].as_str().to_string(), "captured-value");
        unsafe { std::env::remove_var(&var); }
    }

    #[test]
    fn env_errors_when_var_unset() {
        let var = unique_var("ENV_MISSING");
        unsafe { std::env::remove_var(&var); }
        match Env::try_new(var.clone()) {
            Ok(_) => panic!("Env::try_new should fail when the var is unset"),
            Err(err) => {
                assert!(err.contains(&var),
                    "error should name the missing var: {err}");
                assert!(err.contains("env_or"),
                    "error should suggest env_or as the defaulted alternative: {err}");
            }
        }
    }

    #[test]
    fn env_or_uses_default_when_var_unset() {
        let var = unique_var("ENV_OR_MISSING");
        unsafe { std::env::remove_var(&var); }
        let node = EnvOr::new(var.clone(), "fallback".to_string());
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        assert_eq!(out[0].as_str().to_string(), "fallback");
    }

    #[test]
    fn env_or_uses_var_value_when_set() {
        let var = unique_var("ENV_OR_SET");
        unsafe { std::env::set_var(&var, "real-value"); }
        let node = EnvOr::new(var.clone(), "fallback".to_string());
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        assert_eq!(out[0].as_str().to_string(), "real-value");
        unsafe { std::env::remove_var(&var); }
    }

    #[test]
    fn env_or_captures_at_construction_not_each_eval() {
        let var = unique_var("ENV_OR_FROZEN");
        unsafe { std::env::set_var(&var, "first"); }
        let node = EnvOr::new(var.clone(), "ignored-default".to_string());
        unsafe { std::env::set_var(&var, "second"); }
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        assert_eq!(out[0].as_str().to_string(), "first",
            "env_or must freeze its value at construction; later env mutations are invisible");
        unsafe { std::env::remove_var(&var); }
    }

    #[test]
    fn tmp_dir_returns_a_path() {
        let node = TmpDir::new();
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        let s = out[0].as_str().to_string();
        assert!(!s.is_empty(), "tmp_dir() should produce a non-empty path");
    }

    #[test]
    fn tmp_dir_is_stable_across_evals() {
        let node = TmpDir::new();
        let mut a = [Value::None];
        let mut b = [Value::None];
        node.eval(&[], &mut a);
        node.eval(&[], &mut b);
        assert_eq!(a[0].as_str(), b[0].as_str());
    }

    /// DSL-level integration: env_or / tmp_dir resolve through the
    /// registry and produce kernels that compile cleanly.
    #[test]
    fn env_or_compiles_through_dsl() {
        let var = unique_var("DSL_ENV_OR");
        unsafe { std::env::set_var(&var, "x-value"); }
        let src = format!(
            "v := env_or(\"{var}\", \"fallback\")\n",
        );
        let kernel = crate::dsl::compile_polydat(&src).expect("compile env_or");
        unsafe { std::env::remove_var(&var); }
        // The output should be the captured value. We can't read
        // the kernel's outputs directly without an eval pass; the
        // shape check (compiled cleanly, registered in DSL) is
        // what this test asserts.
        let names = kernel.program().output_names();
        assert!(names.contains(&"v"), "expected output 'v' in {names:?}");
    }

    #[test]
    fn tmp_dir_compiles_through_dsl_in_string_template() {
        // Confirms the existing string-template machinery accepts
        // function calls like `{tmp_dir()}` in Polydat string literals
        // — no new syntax needed for the resumable-test-fixture
        // workload's path composition.
        let src = "path := \"{tmp_dir()}/data\"\n";
        let kernel = crate::dsl::compile_polydat(src)
            .expect("compile tmp_dir() interpolated in a string");
        let names = kernel.program().output_names();
        assert!(names.contains(&"path"), "expected output 'path' in {names:?}");
    }

    #[test]
    fn elapsed_from_injected_origin_compiles_in_dsl() {
        // Phase-duration metrics need no dedicated node: a phase-level
        // `metrics:` value of `current_epoch_millis() - phase_start`
        // (with `phase_start` an executor-injected origin wire) is the
        // canonical form. Confirm the expression compiles.
        let src = "extern phase_start: u64 = 0\n\
                   volatile te := current_epoch_millis() - phase_start\n";
        let k = crate::dsl::compile_polydat(src)
            .expect("clock-minus-injected-origin must compile");
        assert!(k.program().output_names().contains(&"te"),
            "expected output 'te' in {:?}", k.program().output_names());
    }
}
