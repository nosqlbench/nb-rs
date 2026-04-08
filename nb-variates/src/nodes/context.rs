// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Context state nodes: non-deterministic, session-scoped values.
//!
//! These nodes produce values from the execution environment rather
//! than the coordinate space. They break the deterministic model
//! and should be used deliberately.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::node::{Commutativity, GkNode, NodeMeta, Port, Value};

/// Current wall-clock time in epoch milliseconds.
///
/// Signature: `() -> (u64)`
///
/// Non-deterministic: returns a different value on each call.
pub struct CurrentEpochMillis {
    meta: NodeMeta,
}

impl CurrentEpochMillis {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "current_epoch_millis".into(),
                inputs: vec![],
                outputs: vec![Port::u64("output")],
                commutativity: Commutativity::Positional,
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

impl SessionStartMillis {
    pub fn new() -> Self {
        let start = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        Self {
            meta: NodeMeta {
                name: "session_start_millis".into(),
                inputs: vec![],
                outputs: vec![Port::u64("output")],
                commutativity: Commutativity::Positional,
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

impl ElapsedMillis {
    pub fn new() -> Self {
        let start = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        Self {
            meta: NodeMeta {
                name: "elapsed_millis".into(),
                inputs: vec![],
                outputs: vec![Port::u64("output")],
                commutativity: Commutativity::Positional,
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

/// Monotonically incrementing counter (thread-safe).
///
/// Signature: `() -> (u64)`
///
/// Returns 0, 1, 2, ... across all calls. Not coordinate-derived.
pub struct Counter {
    meta: NodeMeta,
    count: AtomicU64,
}

impl Counter {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "counter".into(),
                inputs: vec![],
                outputs: vec![Port::u64("output")],
                commutativity: Commutativity::Positional,
            },
            count: AtomicU64::new(0),
        }
    }

    pub fn starting_at(start: u64) -> Self {
        Self {
            meta: NodeMeta {
                name: "counter".into(),
                inputs: vec![],
                outputs: vec![Port::u64("output")],
                commutativity: Commutativity::Positional,
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
