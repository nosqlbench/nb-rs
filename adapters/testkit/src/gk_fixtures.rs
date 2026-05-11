// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Testkit-only GK function nodes for resumable-workload testing.
//!
//! See `docs/design/resumable_test_fixture.md` for the full
//! design and usage scenarios. The nodes here are intentionally
//! obtrusively named (`side_effect_*`, `throw_at`) so workload
//! authors don't reach for them by accident — they exist to
//! exercise the resume / failure-injection paths that real
//! workloads must not depend on.
//!
//! ## Surface
//!
//! - [`ThrowAt`] — `throw_at(value, threshold, errorname)`. Pass-through
//!   identity on `value`; panics with a synthetic error tagged
//!   `errorname` when `value == threshold`. Used to inject a
//!   deterministic failure point inside binding eval.
//!
//! - [`SideEffectSequenceNextCycling`] —
//!   `side_effect_sequence_next_cycling(statefile_path, csv_values) -> u64`.
//!   Returns the next value from a CSV-encoded sequence on each
//!   *session* (not each cycle), advancing a state file. After
//!   the last value is consumed the state file is deleted and
//!   the next session starts fresh from index 0.
//!
//! - [`SideEffectSequenceNextNoncycling`] — same but errors at
//!   construction time when the sequence is exhausted instead of
//!   auto-looping.
//!
//! - [`SideEffectSequenceReset`] — deletes a named state file at
//!   construction time so the staircase test can be re-armed
//!   between runs without manual fs operations.
//!
//! All four are registered through the standard `register_nodes!`
//! inventory mechanism — testkit just needs to be linked into the
//! binary for them to appear in the GK function registry.

use std::collections::HashMap;
use std::sync::Mutex;

use nbrs_variates::node::{GkNode, NodeMeta, Port, Slot, SlotType, Value};

/// Process-wide cache of advanced sequence values, keyed by
/// statefile path. The GK assembly path constructs the node
/// multiple times per session (pre-map + per-phase compile),
/// and we only want the file to advance ONCE per session
/// regardless of how many constructions happen. First
/// construction for a given path reads the file, advances,
/// caches the picked value; subsequent constructions for the
/// same path return the cached value untouched.
///
/// Process lifetime is the right scope — each `nbrs run`
/// invocation is its own process, and the cache is empty at
/// process start. Resume sessions are separate processes, so
/// they hit a fresh cache and re-read the file (which is
/// exactly what we want — each invocation picks the next
/// threshold).
static SEQUENCE_VALUE_CACHE: Mutex<Option<HashMap<String, u64>>> = Mutex::new(None);

fn cached_or_advance(
    path: &str,
    values: &[u64],
    cycling: bool,
) -> Result<u64, String> {
    let mut guard = SEQUENCE_VALUE_CACHE.lock()
        .unwrap_or_else(|e| e.into_inner());
    let map = guard.get_or_insert_with(HashMap::new);
    if let Some(&v) = map.get(path) {
        return Ok(v);
    }
    let v = advance_state_file(path, values, cycling)?;
    map.insert(path.to_string(), v);
    Ok(v)
}

/// **Test-only**: clear the in-process advance cache for the
/// given path so a subsequent `new()` re-reads the state file.
/// Models the process boundary that production runs naturally
/// provide (each `nbrs run` is a fresh process) without
/// spawning real subprocesses in the tests.
///
/// Public so cross-crate integration tests in nbrs-activity can
/// simulate multiple resume invocations within one cargo-test
/// process. Real workloads must not call this — it's a test-
/// affordance only.
pub fn clear_sequence_cache_for(path: &str) {
    let mut guard = SEQUENCE_VALUE_CACHE.lock()
        .unwrap_or_else(|e| e.into_inner());
    if let Some(map) = guard.as_mut() {
        map.remove(path);
    }
}

// ---------------------------------------------------------------------------
// throw_at(value, threshold, errorname) -> u64
// ---------------------------------------------------------------------------

/// Pass-through identity on `value`; panics when `value == threshold`.
///
/// `errorname` is a string const-arg used as the synthetic error
/// label; the existing errors-cascade machinery treats it the same
/// as any driver-emitted error name.
pub struct ThrowAt {
    meta: NodeMeta,
    errorname: String,
}

impl ThrowAt {
    pub fn new(errorname: impl Into<String>) -> Self {
        let errorname = errorname.into();
        Self {
            meta: NodeMeta {
                name: "throw_at".into(),
                outs: vec![Port::u64("output")],
                ins: vec![
                    Slot::Wire(Port::u64("value")),
                    Slot::Wire(Port::u64("threshold")),
                ],
            },
            errorname,
        }
    }
}

impl GkNode for ThrowAt {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let value = inputs[0].as_u64();
        let threshold = inputs[1].as_u64();
        if value == threshold {
            // Synthesized failure surfaces through the standard
            // errors cascade — which classifies via regex on the
            // panic payload's string form. Including the threshold
            // value gives operators a reproducible signature.
            panic!(
                "throw_at[{}]: value reached threshold {threshold}",
                self.errorname,
            );
        }
        outputs[0] = Value::U64(value);
    }
}

// ---------------------------------------------------------------------------
// side_effect_sequence_next_*  +  side_effect_sequence_reset
// ---------------------------------------------------------------------------

/// Cycling variant: auto-loops back to index 0 after the last
/// value is consumed (file is deleted, next session starts fresh).
pub struct SideEffectSequenceNextCycling {
    meta: NodeMeta,
    value: u64,
}

impl SideEffectSequenceNextCycling {
    pub fn new(path: &str, csv: &str) -> Result<Self, String> {
        let values = parse_csv_values(csv)?;
        let value = cached_or_advance(path, &values, /* cycling */ true)?;
        Ok(Self {
            meta: NodeMeta {
                name: "side_effect_sequence_next_cycling".into(),
                outs: vec![Port::u64("output")],
                ins: Vec::new(),
            },
            value,
        })
    }
}

impl GkNode for SideEffectSequenceNextCycling {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(self.value);
    }
}

/// Non-cycling variant: hard error at session-init when the
/// sequence has been fully consumed (per design memo OQ-D-prime).
pub struct SideEffectSequenceNextNoncycling {
    meta: NodeMeta,
    value: u64,
}

impl SideEffectSequenceNextNoncycling {
    pub fn new(path: &str, csv: &str) -> Result<Self, String> {
        let values = parse_csv_values(csv)?;
        let value = cached_or_advance(path, &values, /* cycling */ false)?;
        Ok(Self {
            meta: NodeMeta {
                name: "side_effect_sequence_next_noncycling".into(),
                outs: vec![Port::u64("output")],
                ins: Vec::new(),
            },
            value,
        })
    }
}

impl GkNode for SideEffectSequenceNextNoncycling {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(self.value);
    }
}

/// Companion node — deletes the named state file at construction
/// time. Output value is a sentinel `0`; consumers shouldn't
/// read it.
pub struct SideEffectSequenceReset {
    meta: NodeMeta,
}

impl SideEffectSequenceReset {
    pub fn new(path: &str) -> Result<Self, String> {
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(format!(
                "side_effect_sequence_reset: failed to remove {path}: {e}",
            )),
        }
        Ok(Self {
            meta: NodeMeta {
                name: "side_effect_sequence_reset".into(),
                outs: vec![Port::u64("output")],
                ins: Vec::new(),
            },
        })
    }
}

impl GkNode for SideEffectSequenceReset {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(0);
    }
}

// ---------------------------------------------------------------------------
// State-file machinery
// ---------------------------------------------------------------------------

/// Parse a comma-separated u64 list. Whitespace per element is
/// trimmed. Empty list / non-numeric entry / negative value
/// rejected at workload load time.
fn parse_csv_values(csv: &str) -> Result<Vec<u64>, String> {
    let mut out = Vec::new();
    for (i, raw) in csv.split(',').enumerate() {
        let s = raw.trim();
        if s.is_empty() {
            return Err(format!(
                "side_effect_sequence: empty element at position {i} in csv: {csv:?}",
            ));
        }
        let v: u64 = s.parse().map_err(|_| format!(
            "side_effect_sequence: element {i} is not a u64: {s:?}",
        ))?;
        out.push(v);
    }
    if out.is_empty() {
        return Err("side_effect_sequence: csv must not be empty".into());
    }
    Ok(out)
}

/// Read the index from `path`, pick `values[index]`, advance to
/// `index + 1`, write back. When `index + 1 == values.len()`:
///
/// - `cycling = true`: delete the file so the next session restarts
///   from index 0.
/// - `cycling = false`: keep the file at `index + 1` (out-of-bounds
///   on the next read) so the next session sees the exhaustion and
///   errors out.
///
/// Errors when the file says we're already past-the-end and
/// `cycling = false`.
fn advance_state_file(
    path: &str,
    values: &[u64],
    cycling: bool,
) -> Result<u64, String> {
    let n = values.len();
    let current_index = read_index(path)?;
    if current_index >= n {
        if cycling {
            // Defensive — `cycling = true` deletes the file at
            // index `n`, so we shouldn't normally see this state.
            // If we do (e.g. operator manually wrote `n` into the
            // file), treat as a fresh start.
            let value = values[0];
            write_index(path, 1)?;
            if 1 == n {
                let _ = std::fs::remove_file(path);
            }
            return Ok(value);
        } else {
            return Err(format!(
                "side_effect_sequence_next_noncycling: state file {path} \
                 reports index {current_index} which is past the end of the \
                 {n}-value sequence. Use side_effect_sequence_reset(...) or \
                 manually delete the file to re-arm the test.",
            ));
        }
    }
    let value = values[current_index];
    let next_index = current_index + 1;
    if cycling && next_index == n {
        // Remove the file so the next session reads index 0.
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(format!(
                "side_effect_sequence_next_cycling: failed to remove {path} after exhaustion: {e}",
            )),
        }
    } else {
        write_index(path, next_index)?;
    }
    Ok(value)
}

fn read_index(path: &str) -> Result<usize, String> {
    match std::fs::read_to_string(path) {
        Ok(s) => s.trim().parse::<usize>().map_err(|_| format!(
            "side_effect_sequence: state file {path} contains non-integer content: {s:?}",
        )),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(e) => Err(format!(
            "side_effect_sequence: failed to read state file {path}: {e}",
        )),
    }
}

fn write_index(path: &str, index: usize) -> Result<(), String> {
    if let Some(parent) = std::path::Path::new(path).parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!(
            "side_effect_sequence: failed to create parent {}: {e}",
            parent.display(),
        ))?;
    }
    std::fs::write(path, index.to_string()).map_err(|e| format!(
        "side_effect_sequence: failed to write state file {path}: {e}",
    ))
}

// ---------------------------------------------------------------------------
// Signature declarations + builder + registration
// ---------------------------------------------------------------------------

use nbrs_variates::dsl::registry::{Arity, FuncCategory, FuncSig, ParamSpec};

pub fn signatures() -> &'static [FuncSig] {
    use FuncCategory as C;
    &[
        FuncSig {
            name: "throw_at", category: C::Diagnostic, outputs: 1,
            description: "TEST FIXTURE: identity-on-value, panics when value == threshold",
            help: "Pass-through identity on `value` for every cycle except when value equals threshold; in that case panics with a synthetic error tagged `errorname` so the workload's errors cascade can route the failure.\nThis is a test-only fixture function — no real workload should depend on its behavior.\nParameters:\n  value     — wire input (u64); the observed value\n  threshold — wire input (u64); the trip value\n  errorname — const string; error label for cascade matching\nExample: trip := throw_at(cycle, threshold, \"staircase\")",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "value",     slot_type: SlotType::Wire,     required: true, example: "cycle",        constraint: None },
                ParamSpec { name: "threshold", slot_type: SlotType::Wire,     required: true, example: "threshold",    constraint: None },
                ParamSpec { name: "errorname", slot_type: SlotType::ConstStr, required: true, example: "\"staircase\"", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: nbrs_variates::node::Commutativity::Positional,
            default_resolver: None,
            output_type: nbrs_variates::dsl::registry::OutputType::Fixed,
        },
        FuncSig {
            name: "side_effect_sequence_next_cycling", category: C::Diagnostic, outputs: 1,
            description: "TEST FIXTURE: per-session sequence stepper with auto-loop",
            help: "Advances a hidden state file once per session and returns the picked value. After the last value is consumed the state file is deleted; the next session starts fresh from index 0.\nThe path is composed via the standard GK string-template form, so {tmp_dir()}/{session_id()} idioms work without any new syntax.\nParameters:\n  statefile_path — const string; path to the state file\n  csv_values     — const string; comma-separated u64 sequence\nExample: t := side_effect_sequence_next_cycling(\"{tmp_dir()}/{session_id()}_seq\", \"10,51,101\")",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "statefile_path", slot_type: SlotType::ConstStr, required: true, example: "\"/tmp/seq.txt\"", constraint: None },
                ParamSpec { name: "csv_values",     slot_type: SlotType::ConstStr, required: true, example: "\"10,51,101\"",   constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: nbrs_variates::node::Commutativity::Positional,
            default_resolver: None,
            output_type: nbrs_variates::dsl::registry::OutputType::Fixed,
        },
        FuncSig {
            name: "side_effect_sequence_next_noncycling", category: C::Diagnostic, outputs: 1,
            description: "TEST FIXTURE: per-session sequence stepper, hard-error on exhaustion",
            help: "Same per-session advance behavior as the _cycling variant, but after the last value is consumed the next session-init fails with a clear error. Forces operator intervention via side_effect_sequence_reset(...).\nParameters: same as _cycling.\nExample: t := side_effect_sequence_next_noncycling(\"/tmp/seq.txt\", \"10,51,101\")",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "statefile_path", slot_type: SlotType::ConstStr, required: true, example: "\"/tmp/seq.txt\"", constraint: None },
                ParamSpec { name: "csv_values",     slot_type: SlotType::ConstStr, required: true, example: "\"10,51,101\"",   constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: nbrs_variates::node::Commutativity::Positional,
            default_resolver: None,
            output_type: nbrs_variates::dsl::registry::OutputType::Fixed,
        },
        FuncSig {
            name: "side_effect_sequence_reset", category: C::Diagnostic, outputs: 1,
            description: "TEST FIXTURE: delete a sequence state file (re-arm)",
            help: "Deletes the named state file at session-init so the next side_effect_sequence_next_* call starts fresh from index 0. No-op if the file doesn't exist.\nReturns 0 (sentinel); consumers shouldn't read the value.\nParameters:\n  statefile_path — const string; path to the state file\nExample: _ := side_effect_sequence_reset(\"/tmp/seq.txt\")",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "statefile_path", slot_type: SlotType::ConstStr, required: true, example: "\"/tmp/seq.txt\"", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: nbrs_variates::node::Commutativity::Positional,
            default_resolver: None,
            output_type: nbrs_variates::dsl::registry::OutputType::Fixed,
        },
    ]
}

pub fn build_node(
    name: &str,
    _wires: &[nbrs_variates::assembly::WireRef],
    _wire_types: &[nbrs_variates::node::PortType],
    consts: &[nbrs_variates::dsl::ConstArg],
) -> Option<Result<Box<dyn GkNode>, String>> {
    match name {
        "throw_at" => {
            let errorname = consts.first().map(|c| c.as_str()).unwrap_or("");
            if errorname.is_empty() {
                return Some(Err("throw_at: missing errorname argument".into()));
            }
            Some(Ok(Box::new(ThrowAt::new(errorname))))
        }
        "side_effect_sequence_next_cycling" => {
            let path = consts.first().map(|c| c.as_str()).unwrap_or("");
            let csv = consts.get(1).map(|c| c.as_str()).unwrap_or("");
            Some(SideEffectSequenceNextCycling::new(path, csv)
                .map(|n| Box::new(n) as Box<dyn GkNode>))
        }
        "side_effect_sequence_next_noncycling" => {
            let path = consts.first().map(|c| c.as_str()).unwrap_or("");
            let csv = consts.get(1).map(|c| c.as_str()).unwrap_or("");
            Some(SideEffectSequenceNextNoncycling::new(path, csv)
                .map(|n| Box::new(n) as Box<dyn GkNode>))
        }
        "side_effect_sequence_reset" => {
            let path = consts.first().map(|c| c.as_str()).unwrap_or("");
            Some(SideEffectSequenceReset::new(path)
                .map(|n| Box::new(n) as Box<dyn GkNode>))
        }
        _ => None,
    }
}

nbrs_variates::register_nodes!(signatures, build_node);

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpfile(tag: &str) -> String {
        let n = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        let p = std::env::temp_dir().join(format!("nbrs-fixture-{tag}-{n:x}.txt"));
        let _ = std::fs::remove_file(&p);
        p.to_string_lossy().into_owned()
    }

    #[test]
    fn throw_at_passes_value_through_when_below_threshold() {
        let node = ThrowAt::new("test");
        let mut out = [Value::None];
        node.eval(&[Value::U64(5), Value::U64(10)], &mut out);
        assert_eq!(out[0].as_u64(), 5);
    }

    #[test]
    fn throw_at_panics_at_threshold() {
        let node = ThrowAt::new("staircase");
        let mut out = [Value::None];
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            node.eval(&[Value::U64(10), Value::U64(10)], &mut out);
        }));
        assert!(result.is_err());
        let payload = result.unwrap_err();
        let msg = payload.downcast_ref::<String>().cloned()
            .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
            .unwrap_or_default();
        assert!(msg.contains("staircase"), "expected errorname in payload: {msg}");
    }

    #[test]
    fn cycling_walks_through_then_loops() {
        let path = tmpfile("cycle");
        let csv = "10,20,30";
        // Each `simulate_new_session` boundary clears the in-
        // process cache so the next `new()` re-reads the file —
        // matching the production semantics where each `nbrs run`
        // is a fresh process.
        let simulate_new_session = || super::clear_sequence_cache_for(&path);

        let v1 = SideEffectSequenceNextCycling::new(&path, csv).unwrap();
        let mut out = [Value::None];
        v1.eval(&[], &mut out);
        assert_eq!(out[0].as_u64(), 10);
        simulate_new_session();
        let v2 = SideEffectSequenceNextCycling::new(&path, csv).unwrap();
        v2.eval(&[], &mut out);
        assert_eq!(out[0].as_u64(), 20);
        simulate_new_session();
        let v3 = SideEffectSequenceNextCycling::new(&path, csv).unwrap();
        v3.eval(&[], &mut out);
        assert_eq!(out[0].as_u64(), 30);
        simulate_new_session();
        // After exhaustion, file is removed → next session loops.
        assert!(!std::path::Path::new(&path).exists(), "state file should be removed after exhaustion");
        let v4 = SideEffectSequenceNextCycling::new(&path, csv).unwrap();
        v4.eval(&[], &mut out);
        assert_eq!(out[0].as_u64(), 10, "cycling variant must loop back");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn noncycling_walks_through_then_errors() {
        let path = tmpfile("noncycle");
        let csv = "10,20";
        SideEffectSequenceNextNoncycling::new(&path, csv).unwrap();  // 10
        super::clear_sequence_cache_for(&path);
        SideEffectSequenceNextNoncycling::new(&path, csv).unwrap();  // 20
        super::clear_sequence_cache_for(&path);
        match SideEffectSequenceNextNoncycling::new(&path, csv) {
            Ok(_) => panic!("noncycling should hard-error after exhaustion"),
            Err(e) => assert!(e.contains("past the end"), "got: {e}"),
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn reset_clears_state_file() {
        let path = tmpfile("reset");
        let csv = "10,20,30";
        SideEffectSequenceNextCycling::new(&path, csv).unwrap();  // 10
        super::clear_sequence_cache_for(&path);
        SideEffectSequenceReset::new(&path).unwrap();
        let v = SideEffectSequenceNextCycling::new(&path, csv).unwrap();
        let mut out = [Value::None];
        v.eval(&[], &mut out);
        assert_eq!(out[0].as_u64(), 10, "reset must rewind to index 0");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn reset_is_no_op_when_file_missing() {
        let path = tmpfile("reset-missing");
        SideEffectSequenceReset::new(&path).expect("reset without file present should succeed");
    }

    #[test]
    fn parse_csv_values_rejects_empty() {
        assert!(parse_csv_values("").is_err());
    }

    #[test]
    fn parse_csv_values_rejects_non_numeric() {
        assert!(parse_csv_values("10,abc,30").is_err());
    }

    #[test]
    fn parse_csv_values_trims_whitespace() {
        let v = parse_csv_values("  10 , 20 ,30  ").expect("trimmed parse");
        assert_eq!(v, vec![10, 20, 30]);
    }

    #[test]
    fn cycling_caches_within_a_session() {
        // The GK assembly path constructs the node multiple times
        // per session (pre-map + per-phase compile). The advance
        // must happen ONCE per process; subsequent constructions
        // for the same path return the cached value untouched.
        let path = tmpfile("cache");
        let csv = "10,20,30";
        let a = SideEffectSequenceNextCycling::new(&path, csv).unwrap();
        let b = SideEffectSequenceNextCycling::new(&path, csv).unwrap();
        let c = SideEffectSequenceNextCycling::new(&path, csv).unwrap();
        let mut out = [Value::None];
        a.eval(&[], &mut out); assert_eq!(out[0].as_u64(), 10);
        b.eval(&[], &mut out); assert_eq!(out[0].as_u64(), 10);
        c.eval(&[], &mut out); assert_eq!(out[0].as_u64(), 10);
        // State file should have advanced to index 1 (next session
        // would pick value 20).
        assert_eq!(std::fs::read_to_string(&path).unwrap_or_default().trim(), "1");
        let _ = std::fs::remove_file(&path);
        super::clear_sequence_cache_for(&path);
    }
}
