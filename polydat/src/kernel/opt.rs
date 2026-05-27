// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `KernelOptLevel` — session-wide optimization knob for op-template
//! kernel synthesis.
//!
//! Today's closure-binding economy (Rule 5) drops input slots for
//! names nothing in the body references — magic externs `body` /
//! `count` / `ok` and result-binding LHSs whose values nothing
//! downstream reads get DCE'd at slot-allocation time. Runtime
//! writes to those names land on a kernel that has no slot for them
//! and silently no-op.
//!
//! That's fine in production: the workload didn't ask for the value,
//! we don't pay to track it. It's actively harmful for step-debug /
//! cycle-replay / "show me what the adapter actually wrote this
//! cycle even though nothing read it" inspection.
//!
//! `Diagnostic` mode relaxes the DCE: every magic extern referenced
//! or not is allocated, every result-binding LHS gets a kernel slot
//! whether or not anything reads it. The writes land, `wires.get`
//! answers, the step-debugger sees the real values. Compute path
//! unchanged — the extra slots have no eval cone hanging off them.
//!
//! The knob threads through [`crate::subcontext::CompileOptions`]
//! and is consulted at op-template synthesis. The nbrs CLI exposes
//! it as `--kernel-opt=release|diagnostic` (default `release`).

/// Optimization level for op-template kernel synthesis.
///
/// `Release` is the production default — closure-binding economy
/// elides slots for names nothing references. `Diagnostic` keeps
/// every magic-extern and result-binding-LHS slot allocated so
/// step-debug / cycle-replay introspection can see the values the
/// runtime would otherwise drop.
///
/// Naming: matches the rustc convention (`opt-level=0..3`) but
/// collapsed to two semantically-distinct positions; there's no
/// useful middle ground between "DCE on" and "keep everything for
/// inspection."
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum KernelOptLevel {
    /// Production default. Closure-binding economy elides slots
    /// for unreferenced magic externs and result-binding LHSs.
    /// Writes to elided names silently no-op.
    #[default]
    Release,
    /// Step-debug / cycle-replay mode. Force-allocate every magic
    /// extern (`body` / `count` / `ok`) and every result-binding
    /// LHS slot regardless of downstream reference. Runtime writes
    /// always land; `wires.get` always answers.
    Diagnostic,
}

impl KernelOptLevel {
    /// True when slot allocation should ignore the "is this name
    /// referenced?" check and force-allocate every candidate slot.
    pub fn keep_unreferenced_slots(self) -> bool {
        matches!(self, Self::Diagnostic)
    }

    /// Parse from a CLI-style string. Returns `Err(input)` on an
    /// unrecognised value so the caller can format its own
    /// diagnostic.
    pub fn parse(s: &str) -> Result<Self, &str> {
        match s {
            "release" => Ok(Self::Release),
            "diagnostic" => Ok(Self::Diagnostic),
            _ => Err(s),
        }
    }

    /// Canonical CLI spelling for this level. Inverse of
    /// [`Self::parse`].
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Release => "release",
            Self::Diagnostic => "diagnostic",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_release() {
        assert_eq!(KernelOptLevel::default(), KernelOptLevel::Release);
    }

    #[test]
    fn keep_unreferenced_slots_release() {
        assert!(!KernelOptLevel::Release.keep_unreferenced_slots());
    }

    #[test]
    fn keep_unreferenced_slots_diagnostic() {
        assert!(KernelOptLevel::Diagnostic.keep_unreferenced_slots());
    }

    #[test]
    fn parse_roundtrip() {
        for lvl in [KernelOptLevel::Release, KernelOptLevel::Diagnostic] {
            assert_eq!(KernelOptLevel::parse(lvl.as_str()), Ok(lvl));
        }
    }

    #[test]
    fn parse_unknown_returns_err() {
        assert_eq!(KernelOptLevel::parse("none"), Err("none"));
        assert_eq!(KernelOptLevel::parse(""), Err(""));
    }
}
