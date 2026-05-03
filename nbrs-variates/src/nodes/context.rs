// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Context state nodes: non-deterministic, session-scoped values.
//!
//! These nodes produce values from the execution environment rather
//! than the coordinate space. They break the deterministic model
//! and should be used deliberately.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::node::{GkNode, NodeMeta, Port, Slot, SlotType, Value};

/// Current wall-clock time in epoch milliseconds.
///
/// Signature: `() -> (u64)`
///
/// Non-deterministic: returns a different value on each call.
pub struct CurrentEpochMillis {
    meta: NodeMeta,
}

impl Default for CurrentEpochMillis {
    fn default() -> Self {
        Self::new()
    }
}

impl CurrentEpochMillis {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "current_epoch_millis".into(),
                outs: vec![Port::u64("output")],
                ins: Vec::new(),
            },
        }
    }
}

impl GkNode for CurrentEpochMillis {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        outputs[0] = Value::U64(millis);
    }
}

/// Session start time in epoch milliseconds, frozen at construction.
///
/// Signature: `() -> (u64)`
///
/// Deterministic within a session: always returns the same value.
pub struct SessionStartMillis {
    meta: NodeMeta,
    start: u64,
}

impl Default for SessionStartMillis {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionStartMillis {
    pub fn new() -> Self {
        let start = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        Self {
            meta: NodeMeta {
                name: "session_start_millis".into(),
                outs: vec![Port::u64("output")],
                ins: Vec::new(),
            },
            start,
        }
    }
}

impl GkNode for SessionStartMillis {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(self.start);
    }
}

/// Elapsed milliseconds since session start.
///
/// Signature: `() -> (u64)`
///
/// Non-deterministic: grows monotonically over the session.
pub struct ElapsedMillis {
    meta: NodeMeta,
    start: u64,
}

impl Default for ElapsedMillis {
    fn default() -> Self {
        Self::new()
    }
}

impl ElapsedMillis {
    pub fn new() -> Self {
        let start = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        Self {
            meta: NodeMeta {
                name: "elapsed_millis".into(),
                outs: vec![Port::u64("output")],
                ins: Vec::new(),
            },
            start,
        }
    }
}

impl GkNode for ElapsedMillis {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        outputs[0] = Value::U64(now.saturating_sub(self.start));
    }
}

/// Current OS thread numeric identifier.
///
/// Signature: `() -> (u64)`
///
/// Non-deterministic: returns a different value per thread.
/// Useful for partitioning or sharding in multi-threaded workloads.
pub struct ThreadId {
    meta: NodeMeta,
}

impl Default for ThreadId {
    fn default() -> Self {
        Self::new()
    }
}

impl ThreadId {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "thread_id".into(),
                outs: vec![Port::u64("output")],
                ins: Vec::new(),
            },
        }
    }
}

impl GkNode for ThreadId {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        // Use the thread ID as a u64. std::thread::current().id() returns an
        // opaque ThreadId; we convert via Debug format to extract the numeric ID.
        let id = std::thread::current().id();
        let id_str = format!("{id:?}");
        // ThreadId(N) format
        let num = id_str.trim_start_matches("ThreadId(").trim_end_matches(')');
        let n: u64 = num.parse().unwrap_or(0);
        outputs[0] = Value::U64(n);
    }
}

/// Environment variable read, frozen at construction.
///
/// Signature: `env(name: const str) -> str`
///
/// Reads the named env var at node construction (session-init) and
/// returns the captured value on every evaluation. Errors at
/// construction if the variable is unset — use `env_or` for a
/// defaulted form.
///
/// Convention: env values are static for a process lifetime
/// (per project convention). The captured value is constant
/// within a session; resume invocations re-read the env in their
/// own process, so resume identity correctly distinguishes runs
/// with different env values once `hash_const` lands.
pub struct Env {
    meta: NodeMeta,
    value: String,
}

impl Env {
    pub fn new(var: &str) -> Result<Self, String> {
        let value = std::env::var(var).map_err(|_| format!(
            "env('{var}'): environment variable not set; \
             use env_or('{var}', '<default>') if a fallback is acceptable",
        ))?;
        Ok(Self {
            meta: NodeMeta {
                name: "env".into(),
                outs: vec![Port::str("output")],
                ins: Vec::new(),
            },
            value,
        })
    }
}

impl GkNode for Env {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Str(self.value.clone());
    }
}

/// Environment variable read with default, frozen at construction.
///
/// Signature: `env_or(name: const str, default: const str) -> str`
///
/// Reads the named env var at node construction; falls back to
/// the literal `default` when the variable is unset. The captured
/// value is then constant for the session.
///
/// See [`Env`] for the rationale on session-static behavior.
pub struct EnvOr {
    meta: NodeMeta,
    value: String,
}

impl EnvOr {
    pub fn new(var: &str, default: &str) -> Self {
        let value = std::env::var(var).unwrap_or_else(|_| default.to_string());
        Self {
            meta: NodeMeta {
                name: "env_or".into(),
                outs: vec![Port::str("output")],
                ins: Vec::new(),
            },
            value,
        }
    }
}

impl GkNode for EnvOr {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Str(self.value.clone());
    }
}

/// System temp directory, frozen at construction.
///
/// Signature: `tmp_dir() -> str`
///
/// Returns `std::env::temp_dir()` captured at node construction.
/// Like [`Env`] / [`EnvOr`], the value is treated as session-static
/// per project convention.
pub struct TmpDir {
    meta: NodeMeta,
    value: String,
}

impl Default for TmpDir {
    fn default() -> Self { Self::new() }
}

impl TmpDir {
    pub fn new() -> Self {
        let value = std::env::temp_dir()
            .to_str()
            .map(String::from)
            .unwrap_or_else(|| "/tmp".to_string());
        Self {
            meta: NodeMeta {
                name: "tmp_dir".into(),
                outs: vec![Port::str("output")],
                ins: Vec::new(),
            },
            value,
        }
    }
}

impl GkNode for TmpDir {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Str(self.value.clone());
    }
}

/// Monotonically incrementing counter (thread-safe).
///
/// Signature: `() -> (u64)`
///
/// Returns 0, 1, 2, ... across all calls. Not coordinate-derived.
pub struct Counter {
    meta: NodeMeta,
    count: AtomicU64,
}

impl Default for Counter {
    fn default() -> Self {
        Self::new()
    }
}

impl Counter {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "counter".into(),
                outs: vec![Port::u64("output")],
                ins: Vec::new(),
            },
            count: AtomicU64::new(0),
        }
    }

    pub fn starting_at(start: u64) -> Self {
        Self {
            meta: NodeMeta {
                name: "counter".into(),
                outs: vec![Port::u64("output")],
                ins: Vec::new(),
            },
            count: AtomicU64::new(start),
        }
    }
}

impl GkNode for Counter {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(self.count.fetch_add(1, Ordering::Relaxed));
    }
}

// ---------------------------------------------------------------------------
// Signature declarations for the DSL registry
// ---------------------------------------------------------------------------

use crate::dsl::registry::{Arity, FuncCategory, FuncSig, ParamSpec};

/// Signatures for non-deterministic context nodes.
pub fn signatures() -> &'static [FuncSig] {
    use FuncCategory as C;
    &[
        FuncSig {
            name: "current_epoch_millis", category: C::Context, outputs: 1,
            description: "current wall-clock time (non-deterministic)",
            help: "Returns the current wall-clock time as epoch milliseconds.\nNON-DETERMINISTIC: returns a different value on each evaluation.\nUse for real-time timestamps in generated records.\nTakes no wire inputs.",
            identity: None, variadic_ctor: None,
            params: &[],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            default_resolver: None,
        },
        FuncSig {
            name: "counter", category: C::Context, outputs: 1,
            description: "monotonic counter (non-deterministic)",
            help: "Returns a monotonically increasing u64 counter.\nNON-DETERMINISTIC: increments on each evaluation across all threads.\nUse for sequence numbers, unique IDs, or ordering guarantees.\nTakes no wire inputs.",
            identity: None, variadic_ctor: None,
            params: &[],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            default_resolver: None,
        },
        FuncSig {
            name: "session_start_millis", category: C::Context, outputs: 1,
            description: "session start time (frozen at init)",
            help: "Returns the epoch milliseconds when the session was initialized.\nFrozen at init time — returns the same value on every evaluation.\nUse as a stable base timestamp for relative time calculations.\nTakes no wire inputs.",
            identity: None, variadic_ctor: None,
            params: &[],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            default_resolver: None,
        },
        FuncSig {
            name: "elapsed_millis", category: C::Context, outputs: 1,
            description: "elapsed milliseconds since session start",
            help: "Returns elapsed milliseconds since the session was initialized.\nNon-deterministic: grows monotonically over the session.\nUse for relative time offsets in generated records.\nTakes no wire inputs.",
            identity: None, variadic_ctor: None,
            params: &[],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            default_resolver: None,
        },
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
            commutativity: crate::node::Commutativity::Positional,
            default_resolver: None,
        },
        FuncSig {
            name: "thread_id", category: C::Context, outputs: 1,
            description: "current OS thread numeric ID",
            help: "Returns the current OS thread's numeric identifier as u64.\nNon-deterministic: different fibers may run on different threads.\nUseful for partitioning or sharding in multi-threaded workloads.\nTakes no wire inputs.",
            identity: None, variadic_ctor: None,
            params: &[],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            default_resolver: None,
        },
        FuncSig {
            name: "env", category: C::Context, outputs: 1,
            description: "process environment variable, frozen at session-init",
            help: "Reads the named environment variable at session-init and returns the captured string on every evaluation.\nFails at workload load time if the variable is unset — use env_or for a defaulted form.\nValues are treated as session-static per project convention.\nParameters:\n  name — environment variable name (const string)\nExample: dataset := env(\"DATASET\")",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "name", slot_type: SlotType::ConstStr, required: true, example: "\"DATASET\"", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            default_resolver: None,
        },
        FuncSig {
            name: "env_or", category: C::Context, outputs: 1,
            description: "process environment variable with default, frozen at session-init",
            help: "Reads the named environment variable at session-init; falls back to the literal default when the variable is unset. The captured value is constant for the session.\nValues are treated as session-static per project convention.\nParameters:\n  name    — environment variable name (const string)\n  default — fallback string (const)\nExample: dataset := env_or(\"DATASET\", \"default\")",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "name", slot_type: SlotType::ConstStr, required: true, example: "\"DATASET\"", constraint: None },
                ParamSpec { name: "default", slot_type: SlotType::ConstStr, required: true, example: "\"default\"", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            default_resolver: None,
        },
        FuncSig {
            name: "tmp_dir", category: C::Context, outputs: 1,
            description: "system temp directory, frozen at session-init",
            help: "Returns std::env::temp_dir() captured at session-init.\nValue is constant for the session.\nTakes no parameters.\nExample: path := \"{tmp_dir()}/cache\"",
            identity: None, variadic_ctor: None,
            params: &[],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            default_resolver: None,
        },
    ]
}

/// Cursor limit node: passes through the input value unchanged.
///
/// Inserted by the compiler when the `limit` activity parameter is present.
/// The node is a visible, documented passthrough in the GK graph that
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

impl GkNode for CursorLimit {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        // Pure passthrough — the limit is enforced by the cursor system,
        // not by the node evaluation. The node exists to be visible in
        // the graph and to carry the max_items metadata.
        outputs[0] = inputs[0].clone();
    }
}

/// Try to build a context node from a function name and const args.
///
/// Returns `None` if the name is not handled by this module.
pub(crate) fn build_node(name: &str, _wires: &[crate::assembly::WireRef], consts: &[crate::dsl::factory::ConstArg]) -> Option<Result<Box<dyn crate::node::GkNode>, String>> {
    match name {
        "current_epoch_millis" => Some(Ok(Box::new(CurrentEpochMillis::new()))),
        "counter" => Some(Ok(Box::new(Counter::new()))),
        "session_start_millis" => Some(Ok(Box::new(SessionStartMillis::new()))),
        "elapsed_millis" => Some(Ok(Box::new(ElapsedMillis::new()))),
        "thread_id" => Some(Ok(Box::new(ThreadId::new()))),
        "limit" => {
            let max_items = consts.first().map(|c| c.as_u64()).unwrap_or(u64::MAX);
            Some(Ok(Box::new(CursorLimit::new(max_items))))
        }
        "env" => {
            let var = consts.first().map(|c| c.as_str()).unwrap_or("");
            if var.is_empty() {
                return Some(Err("env(): missing variable name argument".into()));
            }
            Some(Env::new(var).map(|n| Box::new(n) as Box<dyn crate::node::GkNode>))
        }
        "env_or" => {
            let var = consts.first().map(|c| c.as_str()).unwrap_or("");
            let default = consts.get(1).map(|c| c.as_str()).unwrap_or("");
            if var.is_empty() {
                return Some(Err("env_or(): missing variable name argument".into()));
            }
            Some(Ok(Box::new(EnvOr::new(var, default))))
        }
        "tmp_dir" => Some(Ok(Box::new(TmpDir::new()))),
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
        let node = Counter::new();
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
        let node = Counter::starting_at(100);
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
        let node = Env::new(&var).expect("env should read the set var");
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
        match Env::new(&var) {
            Ok(_) => panic!("Env::new should fail when the var is unset"),
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
        let node = EnvOr::new(&var, "fallback");
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        assert_eq!(out[0].as_str().to_string(), "fallback");
    }

    #[test]
    fn env_or_uses_var_value_when_set() {
        let var = unique_var("ENV_OR_SET");
        unsafe { std::env::set_var(&var, "real-value"); }
        let node = EnvOr::new(&var, "fallback");
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        assert_eq!(out[0].as_str().to_string(), "real-value");
        unsafe { std::env::remove_var(&var); }
    }

    #[test]
    fn env_or_captures_at_construction_not_each_eval() {
        let var = unique_var("ENV_OR_FROZEN");
        unsafe { std::env::set_var(&var, "first"); }
        let node = EnvOr::new(&var, "ignored-default");
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
        let kernel = crate::dsl::compile_gk(&src).expect("compile env_or");
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
        // function calls like `{tmp_dir()}` in GK string literals
        // — no new syntax needed for the resumable-test-fixture
        // workload's path composition.
        let src = "path := \"{tmp_dir()}/data\"\n";
        let kernel = crate::dsl::compile_gk(src)
            .expect("compile tmp_dir() interpolated in a string");
        let names = kernel.program().output_names();
        assert!(names.contains(&"path"), "expected output 'path' in {names:?}");
    }
}
