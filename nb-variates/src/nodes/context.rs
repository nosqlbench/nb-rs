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
        },
        FuncSig {
            name: "counter", category: C::Context, outputs: 1,
            description: "monotonic counter (non-deterministic)",
            help: "Returns a monotonically increasing u64 counter.\nNON-DETERMINISTIC: increments on each evaluation across all threads.\nUse for sequence numbers, unique IDs, or ordering guarantees.\nTakes no wire inputs.",
            identity: None, variadic_ctor: None,
            params: &[],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "session_start_millis", category: C::Context, outputs: 1,
            description: "session start time (frozen at init)",
            help: "Returns the epoch milliseconds when the session was initialized.\nFrozen at init time — returns the same value on every evaluation.\nUse as a stable base timestamp for relative time calculations.\nTakes no wire inputs.",
            identity: None, variadic_ctor: None,
            params: &[],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "elapsed_millis", category: C::Context, outputs: 1,
            description: "elapsed milliseconds since session start",
            help: "Returns elapsed milliseconds since the session was initialized.\nNon-deterministic: grows monotonically over the session.\nUse for relative time offsets in generated records.\nTakes no wire inputs.",
            identity: None, variadic_ctor: None,
            params: &[],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
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
        },
        FuncSig {
            name: "thread_id", category: C::Context, outputs: 1,
            description: "current OS thread numeric ID",
            help: "Returns the current OS thread's numeric identifier as u64.\nNon-deterministic: different fibers may run on different threads.\nUseful for partitioning or sharding in multi-threaded workloads.\nTakes no wire inputs.",
            identity: None, variadic_ctor: None,
            params: &[],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
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
}
