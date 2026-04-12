// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! CaptureContext: stanza-scoped capture context for operation result values.

use std::collections::HashMap;

use crate::node::Value;
use super::engines::GkState;
use super::program::GkProgram;

/// Stanza-scoped capture context.
///
/// Stores captured values from operation results within a single stanza.
/// Values are written by the adapter extraction step and read by
/// subsequent operations' GK kernels (via volatile/sticky ports) or
/// directly by bind point resolution.
///
/// Each executor fiber owns one. Reset at stanza start.
pub struct CaptureContext {
    /// Named captured values.
    values: HashMap<String, Value>,
    /// The cycle this context is evaluating.
    cycle: u64,
}

impl CaptureContext {
    /// Create an empty capture context.
    pub fn new() -> Self {
        Self {
            values: HashMap::new(),
            cycle: 0,
        }
    }

    /// Reset for a new stanza/cycle. Clears all captured values.
    pub fn reset(&mut self, cycle: u64) {
        self.values.clear();
        self.cycle = cycle;
    }

    /// Store a captured value.
    pub fn set(&mut self, name: &str, value: Value) {
        self.values.insert(name.to_string(), value);
    }

    /// Read a captured value. Returns None if not yet captured.
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.values.get(name)
    }

    /// The current cycle.
    pub fn cycle(&self) -> u64 {
        self.cycle
    }

    /// All captured name-value pairs.
    pub fn values(&self) -> &HashMap<String, Value> {
        &self.values
    }

    /// Transfer captured values into a GkState's volatile/sticky ports.
    ///
    /// For each captured name, if the program has a matching volatile
    /// or sticky port, write the value into the state's port buffer.
    pub fn apply_to_state(&self, program: &GkProgram, state: &mut GkState) {
        for (name, value) in &self.values {
            if let Some(idx) = program.find_volatile_port(name) {
                state.set_volatile(idx, value.clone());
            } else if let Some(idx) = program.find_sticky_port(name) {
                state.set_sticky(idx, value.clone());
            }
        }
    }
}

impl Default for CaptureContext {
    fn default() -> Self { Self::new() }
}
