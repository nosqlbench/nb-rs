// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Operator-tree comprehension AST — spec §3.
//!
//! Six constructors closed under composition: one source
//! (`clause`), three combinators (`cartesian`, `zip`, `union`),
//! two modifiers (`filter`, `order`). Every comprehension AST is
//! a tree whose nodes are one of these six variants.
//!
//! Closure (spec §4.1):
//!
//! - C1 — every constructor returns and consumes
//!   `Comprehension`. There is no auxiliary value type at the
//!   AST level.
//! - C2 — well-formedness is decidable in one bottom-up pass
//!   (the [`crate::iteration::comprehension::validate`] module
//!   implements the check).

use serde::{Deserialize, Serialize};

use super::source::Source;
use super::strategy::{StrategyName, ZipMode};

/// The six-variant operator-tree comprehension.
///
/// Closure under composition (spec §4.1 C1): every variant
/// holds one or more `Comprehension` operands plus
/// constructor-specific scalar parameters (predicate, strategy,
/// truncation, zip mode, source).
///
/// `Box<Comprehension>` appears wherever a variant needs a
/// single child operand; `Vec<Comprehension>` wherever a
/// constructor takes N children (cartesian, zip, union).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Comprehension {
    /// Leaf source per spec §3.1. Binds `name` to one value
    /// per dispense, drawn from `source`.
    Clause { name: String, source: Source },

    /// Cross-product combinator per spec §3.2. Children must
    /// have disjoint name sets (V1).
    Cartesian { children: Vec<Comprehension> },

    /// Lockstep combinator per spec §3.3. Children must be
    /// discrete (V7) and have disjoint name sets (V1).
    Zip { children: Vec<Comprehension>, mode: ZipMode },

    /// Concatenation combinator per spec §3.4. Children must
    /// share an identical tuple shape (V2) and all be discrete
    /// (V9).
    Union { children: Vec<Comprehension> },

    /// Selection modifier per spec §3.5. Predicate is a GK
    /// boolean expression; names must close over the child's
    /// coordinates plus the parent scope (V3).
    Filter { child: Box<Comprehension>, predicate: String },

    /// Permutation modifier per spec §3.6. `strategy` must
    /// accept the child's IndexFn (V4); `truncation` limits the
    /// dispensed count.
    Order { child: Box<Comprehension>, strategy: StrategyName, truncation: Option<u64> },
}

impl Comprehension {
    /// Construct a leaf clause.
    pub fn clause<S: Into<String>>(name: S, source: Source) -> Self {
        Comprehension::Clause { name: name.into(), source }
    }

    /// Construct a cartesian over the supplied children.
    pub fn cartesian(children: Vec<Comprehension>) -> Self {
        Comprehension::Cartesian { children }
    }

    /// Construct a zip over the supplied children with the
    /// given mode.
    pub fn zip(children: Vec<Comprehension>, mode: ZipMode) -> Self {
        Comprehension::Zip { children, mode }
    }

    /// Construct a union over the supplied children.
    pub fn union(children: Vec<Comprehension>) -> Self {
        Comprehension::Union { children }
    }

    /// Construct a filter wrapping `child` with `predicate`.
    pub fn filter<S: Into<String>>(child: Comprehension, predicate: S) -> Self {
        Comprehension::Filter { child: Box::new(child), predicate: predicate.into() }
    }

    /// Construct an order node wrapping `child`.
    pub fn order(
        child: Comprehension,
        strategy: StrategyName,
        truncation: Option<u64>,
    ) -> Self {
        Comprehension::Order { child: Box::new(child), strategy, truncation }
    }

    /// Compute the comprehension's coordinate name set,
    /// recursively. The result preserves declaration order
    /// (per spec §3.2 + §3.4's "in declaration order" tuple
    /// shape rules). Used by V1, V2, V3, and the predicate
    /// analyzer's coord-set input.
    pub fn coordinate_names(&self) -> Vec<String> {
        let mut acc = Vec::new();
        self.collect_coordinate_names(&mut acc);
        acc
    }

    /// Compute `(coordinate_name, source_text)` pairs in
    /// declaration order, deduplicated by name (first
    /// occurrence wins). Source text is the round-trip-to-
    /// legacy form — `IntRange { 1, 10, 1 }` → `"1..10"`,
    /// `Literal { [10, 100] }` → `"10, 100"`, etc.
    ///
    /// Used by the runtime's per-iter scope-kernel synthesis
    /// to construct a `[(var, spec_expr)]` list for type
    /// detection (per `build_for_each_scope_kernel`'s probe
    /// pre-evaluation).
    pub fn coordinate_specs(&self) -> Vec<(String, String)> {
        let mut acc = Vec::new();
        let mut seen = std::collections::HashSet::new();
        self.collect_coordinate_specs(&mut acc, &mut seen);
        acc
    }

    /// Grammar-based extraction of the free names referenced by
    /// every source spec in this comprehension subtree —
    /// workload params, outer iter-vars, and wires that a
    /// `Generator` spec (`concat(foo)`, bare `eh_values`)
    /// consumes. Each spec is parsed with the canonical Polydat
    /// expression grammar (`crate::dsl::refs::referenced_names`)
    /// rather than byte-scanned, so a bare source reference is
    /// recognised exactly as the kernel compiler would resolve
    /// it. `WorkloadParamList { name }` contributes `name`
    /// directly; literals / ranges / intervals contribute
    /// nothing. Used by the workload validator's
    /// declared-but-unreferenced check.
    pub fn referenced_source_names(&self) -> std::collections::BTreeSet<String> {
        use super::source::Source;
        let mut out = std::collections::BTreeSet::new();
        self.walk_sources(&mut |source| match source {
            Source::WorkloadParamList { name, .. } => {
                out.insert(name.clone());
            }
            Source::Generator { expr, .. } => {
                // A generator spec references names two ways: as
                // parsed free identifiers (`concat(foo)`) and as
                // `{name}` interpolation placeholders
                // (`concat({foo_values})`, where the braces are
                // string-interpolation, not expression syntax —
                // so the expression parser alone wouldn't see
                // them). Collect both.
                out.extend(crate::dsl::refs::referenced_names(expr));
                crate::dsl::refs::collect_string_interpolation_refs(expr, &mut out);
            }
            Source::Literal { .. }
            | Source::IntRange { .. }
            | Source::ContinuousInterval { .. }
            | Source::Distribution { .. } => {}
        });
        out
    }

    /// Visit every leaf [`Source`] in this comprehension subtree.
    fn walk_sources(&self, visit: &mut impl FnMut(&super::source::Source)) {
        match self {
            Comprehension::Clause { source, .. } => visit(source),
            Comprehension::Cartesian { children }
            | Comprehension::Zip { children, .. }
            | Comprehension::Union { children } => {
                for c in children {
                    c.walk_sources(visit);
                }
            }
            Comprehension::Filter { child, .. } | Comprehension::Order { child, .. } => {
                child.walk_sources(visit);
            }
        }
    }

    fn collect_coordinate_specs(
        &self,
        acc: &mut Vec<(String, String)>,
        seen: &mut std::collections::HashSet<String>,
    ) {
        use super::source::Source;
        match self {
            Comprehension::Clause { name, source } => {
                if seen.insert(name.clone()) {
                    let spec_text = match source {
                        Source::IntRange { lo, hi, step } => {
                            if *step == 1 { format!("{lo}..{hi}") }
                            else { format!("{lo}..{hi}..{step}") }
                        }
                        Source::Literal { values } if values.len() == 1 => {
                            literal_value_text(&values[0])
                        }
                        Source::Literal { values } => {
                            values.iter().map(literal_value_text).collect::<Vec<_>>().join(", ")
                        }
                        Source::Generator { expr, .. } => expr.clone(),
                        Source::WorkloadParamList { name, .. } => format!("{{{name}}}"),
                        Source::ContinuousInterval { interval, .. } => {
                            if interval.hi_open { format!("{}..{}", interval.lo, interval.hi) }
                            else { format!("{}..={}", interval.lo, interval.hi) }
                        }
                        Source::Distribution { .. } => "<distribution>".to_string(),
                    };
                    acc.push((name.clone(), spec_text));
                }
            }
            Comprehension::Cartesian { children } | Comprehension::Zip { children, .. } => {
                for c in children {
                    c.collect_coordinate_specs(acc, seen);
                }
            }
            Comprehension::Union { children } => {
                for c in children {
                    c.collect_coordinate_specs(acc, seen);
                }
            }
            Comprehension::Filter { child, .. } | Comprehension::Order { child, .. } => {
                child.collect_coordinate_specs(acc, seen);
            }
        }
    }

    fn collect_coordinate_names(&self, acc: &mut Vec<String>) {
        match self {
            Comprehension::Clause { name, .. } => {
                if !acc.contains(name) {
                    acc.push(name.clone());
                }
            }
            Comprehension::Cartesian { children } | Comprehension::Zip { children, .. } => {
                for c in children {
                    c.collect_coordinate_names(acc);
                }
            }
            Comprehension::Union { children } => {
                // V2 requires identical shape; take the first
                // child's coordinates as canonical.
                if let Some(first) = children.first() {
                    first.collect_coordinate_names(acc);
                }
            }
            Comprehension::Filter { child, .. } | Comprehension::Order { child, .. } => {
                child.collect_coordinate_names(acc);
            }
        }
    }

    /// `true` if this node is a leaf clause.
    pub fn is_clause(&self) -> bool {
        matches!(self, Comprehension::Clause { .. })
    }

    /// `true` if this node is one of the three combinators.
    pub fn is_combinator(&self) -> bool {
        matches!(
            self,
            Comprehension::Cartesian { .. }
                | Comprehension::Zip { .. }
                | Comprehension::Union { .. }
        )
    }

    /// `true` if this node is a modifier (`filter` or `order`).
    pub fn is_modifier(&self) -> bool {
        matches!(self, Comprehension::Filter { .. } | Comprehension::Order { .. })
    }

    /// Iterate this node's direct operand children. Returns
    /// an empty iterator for leaf clauses.
    pub fn children(&self) -> Box<dyn Iterator<Item = &Comprehension> + '_> {
        match self {
            Comprehension::Clause { .. } => Box::new(std::iter::empty()),
            Comprehension::Cartesian { children } | Comprehension::Zip { children, .. } | Comprehension::Union { children } => {
                Box::new(children.iter())
            }
            Comprehension::Filter { child, .. } | Comprehension::Order { child, .. } => {
                Box::new(std::iter::once(child.as_ref()))
            }
        }
    }

    /// Count of nodes in the AST (this node + all descendants).
    /// Used by the optimizer's well-founded measure for
    /// termination (spec §10.6.3).
    pub fn node_count(&self) -> usize {
        1 + self.children().map(|c| c.node_count()).sum::<usize>()
    }

    /// Maximum depth of the AST. Constant for flat composition,
    /// O(log N) for balanced trees. Bounds the operator stack
    /// per spec §9.3.
    pub fn depth(&self) -> usize {
        1 + self
            .children()
            .map(|c| c.depth())
            .max()
            .unwrap_or(0)
    }
}

/// Render a [`super::source::LiteralValue`] in legacy
/// source-text form for [`Comprehension::coordinate_specs`].
///
/// Numeric / bool variants render bare; identifier-like
/// strings render bare; strings with special characters
/// render quoted with backslash escapes. Matches the
/// round-trip rendering in `algebra::spec::legacy_convert`'s
/// `literal_value_to_legacy_text`.
fn literal_value_text(v: &super::source::LiteralValue) -> String {
    use super::source::LiteralValue;
    match v {
        LiteralValue::Int(n) => n.to_string(),
        LiteralValue::Float(f) => {
            if f.fract() == 0.0 && f.is_finite() {
                format!("{f:.1}")
            } else {
                format!("{f}")
            }
        }
        LiteralValue::Bool(b) => b.to_string(),
        LiteralValue::String(s) => {
            let bare_ok = !s.is_empty()
                && s.chars().all(|c| c.is_alphanumeric() || c == '_');
            if bare_ok {
                s.clone()
            } else {
                format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iteration::comprehension::source::{LiteralValue, Source};

    fn lit_int_clause(name: &str, values: &[i64]) -> Comprehension {
        Comprehension::clause(
            name,
            Source::Literal {
                values: values.iter().map(|n| LiteralValue::Int(*n)).collect(),
            },
        )
    }

    #[test]
    fn clause_coordinates() {
        let c = lit_int_clause("k", &[1, 2, 3]);
        assert_eq!(c.coordinate_names(), vec!["k"]);
        assert!(c.is_clause());
        assert!(!c.is_combinator());
        assert!(!c.is_modifier());
    }

    #[test]
    fn referenced_source_names_grammar_based() {
        // `eh in eh_values` — a bare source reference parses to
        // a Generator whose free name is the workload param.
        let bare = Comprehension::clause(
            "eh",
            Source::Generator { expr: "eh_values".into(), cardinality_hint: None },
        );
        let got: Vec<String> = bare.referenced_source_names().into_iter().collect();
        assert_eq!(got, vec!["eh_values"]);

        // `(nbo) in (concat(nbo_v_values))` — the source is a
        // function call; the callee `concat` is NOT a reference
        // but its argument IS.
        let call = Comprehension::clause(
            "nbo",
            Source::Generator { expr: "concat(nbo_v_values)".into(), cardinality_hint: None },
        );
        let got: Vec<String> = call.referenced_source_names().into_iter().collect();
        assert_eq!(got, vec!["nbo_v_values"]);

        // `{profiles}` — an explicit WorkloadParamList contributes
        // its name directly.
        let wpl = Comprehension::clause(
            "p",
            Source::WorkloadParamList { name: "profiles".into(), len_hint: None },
        );
        let got: Vec<String> = wpl.referenced_source_names().into_iter().collect();
        assert_eq!(got, vec!["profiles"]);

        // Literal sources contribute nothing.
        let lit = lit_int_clause("k", &[1, 2, 3]);
        assert!(lit.referenced_source_names().is_empty());

        // Cartesian unions the per-clause references.
        let cart = Comprehension::cartesian(vec![bare, call]);
        let got: Vec<String> = cart.referenced_source_names().into_iter().collect();
        assert_eq!(got, vec!["eh_values", "nbo_v_values"]);
    }

    #[test]
    fn cartesian_coordinates_in_declaration_order() {
        let c = Comprehension::cartesian(vec![
            lit_int_clause("k", &[1, 2]),
            lit_int_clause("limit", &[10, 20, 30]),
        ]);
        assert_eq!(c.coordinate_names(), vec!["k", "limit"]);
        assert!(c.is_combinator());
    }

    #[test]
    fn zip_coordinates() {
        let c = Comprehension::zip(
            vec![
                lit_int_clause("x", &[1, 2, 3]),
                lit_int_clause("y", &[10, 20, 30]),
            ],
            ZipMode::Strict,
        );
        assert_eq!(c.coordinate_names(), vec!["x", "y"]);
    }

    #[test]
    fn union_takes_first_childs_shape() {
        let a = Comprehension::cartesian(vec![
            lit_int_clause("k", &[10]),
            lit_int_clause("limit", &[10, 20]),
        ]);
        let b = Comprehension::cartesian(vec![
            lit_int_clause("k", &[100]),
            lit_int_clause("limit", &[100, 200]),
        ]);
        let u = Comprehension::union(vec![a, b]);
        assert_eq!(u.coordinate_names(), vec!["k", "limit"]);
    }

    #[test]
    fn filter_and_order_pass_through_coordinates() {
        let inner = Comprehension::cartesian(vec![
            lit_int_clause("k", &[1, 2]),
            lit_int_clause("limit", &[10]),
        ]);
        let filtered = Comprehension::filter(inner.clone(), "{k} > 0");
        assert_eq!(filtered.coordinate_names(), vec!["k", "limit"]);
        assert!(filtered.is_modifier());

        let ordered = Comprehension::order(inner, StrategyName::Lex, Some(5));
        assert_eq!(ordered.coordinate_names(), vec!["k", "limit"]);
        assert!(ordered.is_modifier());
    }

    #[test]
    fn node_count_and_depth() {
        let inner = Comprehension::cartesian(vec![
            lit_int_clause("k", &[1, 2]),
            lit_int_clause("limit", &[10]),
        ]);
        // inner: 1 (cartesian) + 2 (clauses) = 3 nodes; depth 2
        assert_eq!(inner.node_count(), 3);
        assert_eq!(inner.depth(), 2);

        let filtered = Comprehension::filter(inner, "{k} > 0");
        // filtered: 1 (filter) + 3 = 4 nodes; depth 3
        assert_eq!(filtered.node_count(), 4);
        assert_eq!(filtered.depth(), 3);
    }

    #[test]
    fn round_trip_serde() {
        let c = Comprehension::order(
            Comprehension::filter(
                Comprehension::cartesian(vec![
                    lit_int_clause("k", &[1, 2, 3]),
                    lit_int_clause("limit", &[10, 20]),
                ]),
                "{k} * {limit} > 5",
            ),
            StrategyName::Halton,
            Some(10),
        );
        let json = serde_json::to_string(&c).unwrap();
        let back: Comprehension = serde_json::from_str(&json).unwrap();
        assert_eq!(c, back);
    }
}
