// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! SRD-13d §3.1 — declarative GK-content classification for
//! every node in the workload AST. The scope-tree pre-walker
//! uses [`HasGkMatter::gk_matter`] as the **first question** at
//! every scope decision: most workloads short-circuit here
//! and never reach the program-hash-equivalence refinement
//! (§3.2).
//!
//! Implementations cover the AST types — runtime objects
//! (the `Component` tree, fibers, dispensers) consume the
//! marks the trait produced; they don't implement the trait
//! themselves. GK content lives on the AST, not on runtime
//! state.

use crate::model::{
    BindingsDef, ParsedOp, ScenarioNode, Workload, WorkloadPhase,
};

/// SRD-13d §3.1 classification of how much GK content a
/// scope-tree node carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GkMatter {
    /// No GK references at all — no `bindings:`, no
    /// `metrics:`, no inline `{{<expr>}}`, no GK-typed
    /// fields. Walker skips kernel construction entirely.
    None,
    /// References parent-scope GK names but **defines
    /// nothing new**. Examples: `metrics:` declarations whose
    /// `value:` is a bare name resolving to a parent binding;
    /// inline `{{<name>}}` substitution where `<name>` is a
    /// parent binding; op fields that bind parent-scope
    /// wires without declaring new ones. Walker skips kernel
    /// construction; reads thread through the parent's
    /// kernel state directly.
    Readonly,
    /// Declares new bindings, wire expressions, or constants
    /// that the parent doesn't supply. Walker materialises a
    /// kernel for this node — possibly subject to hash-check
    /// flattening (§3.2) if the new content turns out to be
    /// equivalent to the parent's.
    Definitions,
}

/// Implemented by every workload-AST type that can sit in
/// the construction tree. Pure function of the parsed AST;
/// no runtime state, no compilation.
pub trait HasGkMatter {
    /// Classify this node's contribution to GK content.
    fn gk_matter(&self) -> GkMatter;
}

// -----------------------------------------------------------
// Helpers
// -----------------------------------------------------------

/// `bindings:` block contributes definitions when non-empty.
fn bindings_def_matter(b: &BindingsDef) -> GkMatter {
    if b.is_empty() {
        GkMatter::None
    } else {
        GkMatter::Definitions
    }
}

/// True when any value field on the op uses inline `{{<expr>}}`
/// substitution. Promotes to `Definitions` because the
/// rewrite pass (`crate::scope::rewrite_inline_exprs` in
/// nbrs-activity) hoists each into a `__expr_N := <expr>`
/// binding owned by the op.
fn has_inline_expr(op: &ParsedOp) -> bool {
    fn scan(v: &serde_json::Value) -> bool {
        match v {
            serde_json::Value::String(s) => s.contains("{{") && s.contains("}}"),
            serde_json::Value::Array(arr) => arr.iter().any(scan),
            serde_json::Value::Object(map) => map.values().any(scan),
            _ => false,
        }
    }
    op.op.values().any(scan) || op.params.values().any(scan)
}

/// True when a metric `value:` expression references something
/// other than a single bare binding name. Bare-name references
/// don't add GK content (they read parent bindings); anything
/// with operators / function calls / spaces / etc. is a new
/// expression that contributes Definitions.
fn metric_value_is_bare_name(value: &str) -> bool {
    let trimmed = value.trim();
    if trimmed.is_empty() { return false; }
    trimmed.chars().all(|c|
        c.is_alphanumeric() || c == '_' || c == '.'
    )
}

// -----------------------------------------------------------
// Trait impls
// -----------------------------------------------------------

impl HasGkMatter for ParsedOp {
    fn gk_matter(&self) -> GkMatter {
        // Op-level bindings always promote when non-empty.
        let by_bindings = bindings_def_matter(&self.bindings);
        if by_bindings == GkMatter::Definitions {
            return GkMatter::Definitions;
        }

        // Inline `{{<expr>}}` constructs on any op field
        // hoist into anonymous bindings during pre-compile.
        if has_inline_expr(self) {
            return GkMatter::Definitions;
        }

        // metrics: with non-bare-name value: contributes
        // Definitions. Bare-name value: alone is Readonly
        // (the wire is read from the parent scope).
        if !self.metrics.is_empty() {
            let any_definition = self.metrics.values()
                .any(|m| !metric_value_is_bare_name(&m.value));
            if any_definition {
                return GkMatter::Definitions;
            }
        }

        // result: declarations expose result-body fields as
        // GK wires — a definition by construction (the
        // wire didn't exist before this op).
        if !self.result.is_empty() {
            return GkMatter::Definitions;
        }

        // Anything that gets here either reads the parent
        // (metrics: with bare names; result: empty) or has
        // nothing GK-shaped at all.
        if !self.metrics.is_empty() {
            GkMatter::Readonly
        } else if self.condition.is_some() || self.delay.is_some() {
            // `if:` / `delay:` reference parent bindings.
            GkMatter::Readonly
        } else {
            GkMatter::None
        }
    }
}

impl HasGkMatter for WorkloadPhase {
    fn gk_matter(&self) -> GkMatter {
        // Phase-level `bindings:` block on the phase AST.
        // Today's parser also legacy-merges this into per-op
        // bindings; the phase still owns the structural fact
        // that it declared the binding (SRD-13d §3.1's
        // classification operates on the AST, not on the
        // post-merge runtime view).
        let by_bindings = bindings_def_matter(&self.bindings);
        if by_bindings == GkMatter::Definitions {
            return GkMatter::Definitions;
        }
        // `for_each:` clauses always bind iteration variables.
        if self.for_each.is_some() {
            return GkMatter::Definitions;
        }
        // `cycles` / `concurrency` referencing workload-param
        // GK names (`{train_count}` etc.) ⇒ Readonly. `rate`
        // is f64-typed today (no GK refs) but counted as a
        // parent reference for symmetry; revisit when rate
        // grows GK-expression support.
        let refs_parent = [&self.cycles, &self.concurrency]
            .iter()
            .any(|opt| opt.as_ref().is_some_and(|s|
                s.contains('{') && s.contains('}')));
        if refs_parent || self.rate.is_some() {
            return GkMatter::Readonly;
        }
        GkMatter::None
    }
}

impl HasGkMatter for ScenarioNode {
    fn gk_matter(&self) -> GkMatter {
        match self {
            // Iteration constructs always declare iteration
            // variables — Definitions by construction.
            ScenarioNode::Comprehension { .. }
            | ScenarioNode::DoWhile { .. }
            | ScenarioNode::DoUntil { .. }
                => GkMatter::Definitions,
            // Phase reference + scenario-include wrappers
            // don't add GK content on their own; the
            // wrapped phase / included scenario carries it.
            ScenarioNode::Phase(_)
            | ScenarioNode::IncludedScenario { .. }
                => GkMatter::None,
        }
    }
}

impl HasGkMatter for Workload {
    fn gk_matter(&self) -> GkMatter {
        // Workload root carries the top-level `bindings:`
        // block + workload-level params. Either contributes
        // Definitions when non-empty.
        let by_bindings = bindings_def_matter(&self.bindings);
        if by_bindings == GkMatter::Definitions {
            return GkMatter::Definitions;
        }
        if !self.params.is_empty() {
            // Params turn into `final <name> := <literal>`
            // bindings on the workload-params kernel
            // (nbrs-activity/src/params.rs), so any param
            // declaration is GK content.
            return GkMatter::Definitions;
        }
        GkMatter::None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{MetricSpec, ResultWireSpec};

    fn empty_op(name: &str) -> ParsedOp {
        ParsedOp::simple(name, "noop")
    }

    // ── ParsedOp ─────────────────────────────────────────

    #[test]
    fn parsed_op_with_no_gk_content_is_none() {
        let op = empty_op("x");
        assert_eq!(op.gk_matter(), GkMatter::None);
    }

    #[test]
    fn parsed_op_with_bindings_is_definitions() {
        let mut op = empty_op("x");
        op.bindings = BindingsDef::GkSource("k := 5".into());
        assert_eq!(op.gk_matter(), GkMatter::Definitions);
    }

    #[test]
    fn parsed_op_empty_bindings_string_is_none() {
        let mut op = empty_op("x");
        op.bindings = BindingsDef::GkSource("   \n  ".into());
        assert_eq!(op.gk_matter(), GkMatter::None);
    }

    #[test]
    fn parsed_op_with_inline_expr_is_definitions() {
        let mut op = empty_op("x");
        op.op.insert("stmt".into(),
            serde_json::Value::String("SELECT {{cycle}}".into()));
        assert_eq!(op.gk_matter(), GkMatter::Definitions);
    }

    #[test]
    fn parsed_op_metrics_bare_value_is_readonly() {
        // `metrics: foo: { value: existing_wire }` — references
        // a parent binding, no new wire declared on the op.
        let mut op = empty_op("x");
        op.metrics.insert("foo".into(), MetricSpec {
            value: "existing_wire".into(),
            family: None, kind: None, unit: None, format: None,
        });
        assert_eq!(op.gk_matter(), GkMatter::Readonly);
    }

    #[test]
    fn parsed_op_metrics_dotted_name_is_readonly() {
        // Dotted bare name (e.g. `phase.recall`) is still a
        // bare-name reference, not an expression.
        let mut op = empty_op("x");
        op.metrics.insert("foo".into(), MetricSpec {
            value: "phase.recall_at_10".into(),
            family: None, kind: None, unit: None, format: None,
        });
        assert_eq!(op.gk_matter(), GkMatter::Readonly);
    }

    #[test]
    fn parsed_op_metrics_expression_is_definitions() {
        let mut op = empty_op("x");
        op.metrics.insert("foo".into(), MetricSpec {
            value: "factor * 2.0".into(),
            family: None, kind: None, unit: None, format: None,
        });
        assert_eq!(op.gk_matter(), GkMatter::Definitions);
    }

    #[test]
    fn parsed_op_with_result_is_definitions() {
        let mut op = empty_op("x");
        op.result.insert("rows_returned".into(),
            ResultWireSpec::String("count".into()));
        assert_eq!(op.gk_matter(), GkMatter::Definitions);
    }

    #[test]
    fn parsed_op_with_only_condition_is_readonly() {
        let mut op = empty_op("x");
        op.condition = Some("ok".into());
        assert_eq!(op.gk_matter(), GkMatter::Readonly);
    }

    // ── ScenarioNode ─────────────────────────────────────

    // ── WorkloadPhase ───────────────────────────────────

    #[test]
    fn workload_phase_with_phase_bindings_is_definitions() {
        let phase = WorkloadPhase {
            cycles: None, concurrency: None, rate: None,
            adapter: None, errors: None, tags: None,
            ops: vec![], for_each: None,
            loop_scope: None, iter_scope: None,
            checkpoint: None, status_metrics: vec![],
            bindings: BindingsDef::GkSource("k := 5".into()),
        };
        assert_eq!(phase.gk_matter(), GkMatter::Definitions);
    }

    #[test]
    fn workload_phase_with_for_each_is_definitions() {
        let phase = WorkloadPhase {
            cycles: None, concurrency: None, rate: None,
            adapter: None, errors: None, tags: None,
            ops: vec![],
            for_each: Some("k in 1,2,3".into()),
            loop_scope: None, iter_scope: None,
            checkpoint: None, status_metrics: vec![],
            bindings: BindingsDef::default(),
        };
        assert_eq!(phase.gk_matter(), GkMatter::Definitions);
    }

    #[test]
    fn workload_phase_bare_is_none() {
        let phase = WorkloadPhase {
            cycles: None, concurrency: None, rate: None,
            adapter: None, errors: None, tags: None,
            ops: vec![], for_each: None,
            loop_scope: None, iter_scope: None,
            checkpoint: None, status_metrics: vec![],
            bindings: BindingsDef::default(),
        };
        assert_eq!(phase.gk_matter(), GkMatter::None);
    }

    #[test]
    fn workload_phase_cycles_param_ref_is_readonly() {
        let phase = WorkloadPhase {
            cycles: Some("{train_count}".into()),
            concurrency: None, rate: None,
            adapter: None, errors: None, tags: None,
            ops: vec![], for_each: None,
            loop_scope: None, iter_scope: None,
            checkpoint: None, status_metrics: vec![],
            bindings: BindingsDef::default(),
        };
        assert_eq!(phase.gk_matter(), GkMatter::Readonly);
    }

    // ── ScenarioNode ─────────────────────────────────────

    #[test]
    fn scenario_node_phase_is_none() {
        let node = ScenarioNode::Phase("p".into());
        assert_eq!(node.gk_matter(), GkMatter::None);
    }

    #[test]
    fn scenario_node_do_while_is_definitions() {
        let node = ScenarioNode::DoWhile {
            condition: "ok".into(),
            counter: None,
            children: vec![],
        };
        assert_eq!(node.gk_matter(), GkMatter::Definitions);
    }
}
