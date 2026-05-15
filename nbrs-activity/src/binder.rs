// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Centralized typed binder surfaces — the staged "parse → curry →
//! bind" pattern adapters use to turn op-template text into a
//! kernel-bound output.
//!
//! The pattern shows up everywhere an adapter consumes a template
//! whose `{name}` references need to be resolved against a GK
//! kernel: CQL raw mode rendering the SQL, CQL prepared mode
//! mapping `?` markers to typed values, batch mode iterating per
//! row, validation wrappers reading config params, stdout
//! rendering field values. Before SRD-68 each consumer rolled its
//! own walker. This module unifies them.
//!
//! ## Stages
//!
//! ```text
//!     ┌───────────────┐  parse   ┌─────────────┐  curry   ┌───────────────┐
//!     │ template text │────────► │  BindNames  │────────► │ CurriedBinder │
//!     └───────────────┘          └─────────────┘          └───────────────┘
//!                                       │ pull                  │ pull
//!                                       ▼                       ▼
//!                                 ┌─────────────┐         ┌───────────────┐
//!                                 │ BoundValues │         │  BoundValues  │
//!                                 └─────────────┘         └───────────────┘
//! ```
//!
//! - **Stage 1 — `BindNames::from_template`**: parses the template
//!   and extracts bind point names in declaration order. The
//!   "before" type — adapter holds this on the dispenser, reuses
//!   across cycles.
//!
//! - **Stage 2a — `BindNames::pull`**: applies a `WireSource` and
//!   returns owned `Value`s in name order. Single-step binding for
//!   adapters that resolve everything at cycle time (CQL raw,
//!   stdout). The "after" type.
//!
//! - **Stage 2b — `BindNames::curry_static`**: applies a static
//!   `lookup` (typically the dispenser's canonical kernel at
//!   construction time) to bake in structural names that can't be
//!   `?` markers (CQL prepared keyspace / table / option values).
//!   Returns a `CurriedBinder` that knows the per-cycle names and
//!   can be re-applied per cycle.
//!
//! - **Stage 3 — `CurriedBinder::pull`**: applies a per-fiber
//!   `WireSource` to fetch the remaining per-cycle values. CQL
//!   prepared mode at execute, CQL batch mode per row.
//!
//! ## Type-safety
//!
//! Each stage produces a distinct type. `BindNames` can't be
//! mistaken for `BoundValues`; `CurriedBinder` retains the
//! per-cycle name list separately from the static-resolved text.
//! Adapters declare what they need by which method they call;
//! the compiler tracks the chain.
//!
//! ## Curryable
//!
//! `curry_static` is partial application of the binder against
//! the static kernel. The returned `CurriedBinder` is a closure-
//! like value carrying the partial result; `pull` against any
//! `WireSource` completes the application. Storeable on a
//! dispenser, callable per cycle.

use crate::wires::WireSource;
use nbrs_variates::node::Value;
use std::fmt;

/// Result of a binder operation. Surfaces unresolved bind point
/// names with the same diagnostic shape across raw / prepared /
/// batch / validation paths.
#[derive(Debug, Clone)]
pub enum BindError {
    /// A bind name didn't resolve in the kernel's wire scope.
    /// Carries the failing name so the adapter can render the
    /// error in its preferred shape.
    Unresolved(String),
    /// A qualifier-prefixed reference (`{bind:name}`,
    /// `{capture:name}`, `{input:name}`) appeared at a stage
    /// that doesn't accept them. SRD-68 cycle-time `WireSource`
    /// reads only handle bare names.
    UnsupportedQualifier(String),
}

impl fmt::Display for BindError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BindError::Unresolved(name) => write!(
                f,
                "unresolved bind point `{{{name}}}`: no wire named \
                 `{name}` in the binder's GK context"
            ),
            BindError::UnsupportedQualifier(body) => write!(
                f,
                "qualifier-prefixed bind point `{{{body}}}` is not supported \
                 at this binder stage; only bare `{{name}}` references resolve here"
            ),
        }
    }
}

impl std::error::Error for BindError {}

/// Stage 1: a list of bind point names extracted from a template,
/// in declaration order. Reusable across cycles — adapters parse
/// once at `map_op` and store on the dispenser.
#[derive(Debug, Clone, Default)]
pub struct BindNames {
    names: Vec<String>,
}

impl BindNames {
    /// Parse a template and extract bind names in declaration
    /// order via the standard `nbrs_workload::bindpoints` rules
    /// (CQL map literals, qualified refs, inline expressions all
    /// honoured — see [`nbrs_workload::bindpoints::extract_bind_points`]).
    pub fn from_template(template: &str) -> Self {
        Self {
            names: nbrs_workload::bindpoints::referenced_bindings(template),
        }
    }

    /// Construct directly from a vector of names. Useful when
    /// the bind list comes from a non-template source (e.g. the
    /// post-`replace_bind_points_with_markers` `?`-position
    /// list a CQL prepared dispenser already computed).
    pub fn from_names(names: Vec<String>) -> Self {
        Self { names }
    }

    pub fn names(&self) -> &[String] { &self.names }
    pub fn len(&self) -> usize { self.names.len() }
    pub fn is_empty(&self) -> bool { self.names.is_empty() }
    pub fn iter(&self) -> impl Iterator<Item = &str> {
        self.names.iter().map(|s| s.as_str())
    }

    /// Stage 2a (one-shot): pull each bind name's value from
    /// `wires`. Returns owned values in name order. Errors with
    /// the failing name on first unresolved reference.
    pub fn pull(&self, wires: &dyn WireSource) -> Result<Vec<Value>, BindError> {
        self.names.iter()
            .map(|n| wires.get(n).ok_or_else(|| BindError::Unresolved(n.clone())))
            .collect()
    }

    /// Stage 2a paired form: `(name, value)` tuples in
    /// declaration order. Same error semantics as [`Self::pull`].
    pub fn pull_named(&self, wires: &dyn WireSource) -> Result<Vec<(String, Value)>, BindError> {
        self.names.iter()
            .map(|n| wires.get(n)
                .map(|v| (n.clone(), v))
                .ok_or_else(|| BindError::Unresolved(n.clone())))
            .collect()
    }

    /// Stage 2b: curry against a static lookup. Names that
    /// resolve via `static_lookup` get baked in as `Value`s
    /// stored on the returned [`CurriedBinder`]; remaining names
    /// stay in the per-cycle list. Used by adapters whose
    /// underlying API requires structural positions to be text-
    /// resolved before any `?` markers (CQL prepared mode).
    ///
    /// The returned binder is a curried closure — applying it
    /// (via [`CurriedBinder::pull`]) against any per-cycle
    /// `WireSource` yields the full ordered value list.
    pub fn curry_static<F>(self, mut static_lookup: F) -> CurriedBinder
    where
        F: FnMut(&str) -> Option<Value>,
    {
        let mut static_values: Vec<Option<Value>> = Vec::with_capacity(self.names.len());
        let mut per_cycle_names: Vec<String> = Vec::new();
        for name in &self.names {
            match static_lookup(name) {
                Some(v) => static_values.push(Some(v)),
                None => {
                    static_values.push(None);
                    per_cycle_names.push(name.clone());
                }
            }
        }
        CurriedBinder {
            names: self.names,
            static_values,
            per_cycle_names,
        }
    }
}

/// Stage 2: a partially-bound binder. Static names have been
/// looked up against the canonical kernel; per-cycle names await
/// per-fiber resolution.
///
/// Holds:
/// - The full ordered name list (parallel to the original
///   template's bind points).
/// - Per-position pre-resolved values (`Some(v)` for static,
///   `None` for per-cycle).
/// - The reduced per-cycle name list, in declaration order.
#[derive(Debug, Clone)]
pub struct CurriedBinder {
    names: Vec<String>,
    static_values: Vec<Option<Value>>,
    per_cycle_names: Vec<String>,
}

impl CurriedBinder {
    /// All bind names in declaration order (both static and
    /// per-cycle).
    pub fn names(&self) -> &[String] { &self.names }

    /// Just the per-cycle names — what the cycle-time wires
    /// surface needs to answer for. Useful when the adapter
    /// stores these as a separate `bind_names` list (e.g. CQL
    /// prepared `?`-position order).
    pub fn per_cycle_names(&self) -> &[String] { &self.per_cycle_names }

    /// Stage 3: complete the application. Per-cycle names are
    /// resolved via `wires.get`; static names use the curried-
    /// in `Value`s. Returns the full ordered value list (one
    /// per original bind point, in declaration order).
    pub fn pull(&self, wires: &dyn WireSource) -> Result<Vec<Value>, BindError> {
        let mut out = Vec::with_capacity(self.names.len());
        for (i, name) in self.names.iter().enumerate() {
            match &self.static_values[i] {
                Some(v) => out.push(v.clone()),
                None => out.push(
                    wires.get(name).ok_or_else(|| BindError::Unresolved(name.clone()))?
                ),
            }
        }
        Ok(out)
    }

    /// Pull only the per-cycle names — useful when the adapter
    /// already inlined the static values into its statement text
    /// at construction (e.g. the CQL prepared SQL has the static
    /// names as text, only the `?` positions need values).
    pub fn pull_per_cycle(&self, wires: &dyn WireSource) -> Result<Vec<Value>, BindError> {
        self.per_cycle_names.iter()
            .map(|n| wires.get(n).ok_or_else(|| BindError::Unresolved(n.clone())))
            .collect()
    }

    /// Pull paired with the per-cycle name. Same error semantics
    /// as [`Self::pull_per_cycle`].
    pub fn pull_per_cycle_named(&self, wires: &dyn WireSource) -> Result<Vec<(String, Value)>, BindError> {
        self.per_cycle_names.iter()
            .map(|n| wires.get(n)
                .map(|v| (n.clone(), v))
                .ok_or_else(|| BindError::Unresolved(n.clone())))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wires::CycleWires;
    use nbrs_variates::dsl::compile::compile_gk;

    #[test]
    fn bindnames_parse_extracts_in_declaration_order() {
        let template = "INSERT INTO {keyspace}.{table} VALUES ('{id}', {value})";
        let names = BindNames::from_template(template);
        assert_eq!(names.names(), &["keyspace", "table", "id", "value"]);
    }

    #[test]
    fn bindnames_pull_resolves_via_wires() {
        let mut k = compile_gk(
            "input cycle: u64\n\
             keyspace := \"baselines\"\n\
             table := \"t\"\n",
        ).unwrap();
        let cw = CycleWires::new(&mut k);
        let names = BindNames::from_template("FROM {keyspace}.{table}");
        let values = names.pull(&cw).unwrap();
        assert_eq!(values.len(), 2);
        assert_eq!(values[0].as_str(), "baselines");
        assert_eq!(values[1].as_str(), "t");
    }

    #[test]
    fn bindnames_pull_errors_with_failing_name() {
        let mut k = compile_gk("input cycle: u64\nx := \"a\"\n").unwrap();
        let cw = CycleWires::new(&mut k);
        let names = BindNames::from_template("hi {nope}");
        let err = names.pull(&cw).unwrap_err();
        assert!(matches!(err, BindError::Unresolved(ref n) if n == "nope"),
            "expected Unresolved(nope), got {err:?}");
    }

    #[test]
    fn curry_static_partitions_names() {
        // Three names: two structural (resolved at curry time),
        // one per-cycle (deferred to pull time).
        let mut canonical = compile_gk(
            "input cycle: u64\n\
             keyspace := \"baselines\"\n\
             table := \"t\"\n",
        ).unwrap();
        canonical.set_inputs(&[0]);
        let names = BindNames::from_names(vec![
            "keyspace".into(),
            "table".into(),
            "id".into(),
        ]);
        let canonical_lookup = |n: &str| canonical.lookup(n);
        let curried = names.curry_static(canonical_lookup);
        assert_eq!(curried.names().len(), 3);
        assert_eq!(curried.per_cycle_names(), &["id"],
            "only `id` should be per-cycle (no canonical resolution)");
    }

    #[test]
    fn curry_then_pull_completes_application() {
        // Curry against canonical (structural names baked in),
        // then pull against per-fiber wires (id resolves to the
        // formatted cycle).
        let canonical = compile_gk(
            "input cycle: u64\n\
             keyspace := \"baselines\"\n\
             table := \"t\"\n",
        ).unwrap();
        let names = BindNames::from_names(vec![
            "keyspace".into(),
            "table".into(),
            "id".into(),
        ]);
        let canonical_lookup = |n: &str| canonical.lookup(n);
        let curried = names.curry_static(canonical_lookup);

        // Per-fiber wires has `id` as an output binding.
        let mut fiber = compile_gk(
            "input cycle: u64\n\
             keyspace := \"baselines\"\n\
             table := \"t\"\n\
             id := format_u64(cycle, 10)\n",
        ).unwrap();
        fiber.set_inputs(&[42]);
        let wires = CycleWires::new(&mut fiber);

        let values = curried.pull(&wires).unwrap();
        assert_eq!(values.len(), 3);
        assert_eq!(values[0].as_str(), "baselines");
        assert_eq!(values[1].as_str(), "t");
        assert_eq!(values[2].as_str(), "42");
    }

    #[test]
    fn pull_per_cycle_returns_only_uncurried_names() {
        // CQL prepared adapter use: structural names already
        // inlined into the SQL text at curry time; only the
        // per-cycle `?` positions need values.
        let canonical = compile_gk(
            "input cycle: u64\n\
             keyspace := \"baselines\"\n\
             table := \"t\"\n",
        ).unwrap();
        let names = BindNames::from_names(vec!["keyspace".into(), "id".into()]);
        let curried = names.curry_static(|n| canonical.lookup(n));

        let mut fiber = compile_gk(
            "input cycle: u64\n\
             id := format_u64(cycle, 10)\n",
        ).unwrap();
        fiber.set_inputs(&[7]);
        let wires = CycleWires::new(&mut fiber);

        let per_cycle = curried.pull_per_cycle(&wires).unwrap();
        assert_eq!(per_cycle.len(), 1, "only `id` is per-cycle");
        assert_eq!(per_cycle[0].as_str(), "7");
    }
}
