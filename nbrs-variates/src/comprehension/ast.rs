// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Comprehension AST — the static shape of an iteration scope.
//!
//! ## Shape
//!
//! A [`Comprehension`] declares one or more
//! [`Clause`]s — pairs of `(var, expr)` where `var` is the
//! name bound on each iteration and `expr` is the source
//! expression (a workload parameter reference, a literal
//! comma list, a stdlib call, or any GK-evaluable string)
//! whose result is split into the iteration values for that
//! variable.
//!
//! The [`ComprehensionMode`] decides how multi-clause
//! comprehensions combine:
//!
//! - **Cartesian** (the common case): one ordered list of
//!   clauses; the iteration emits the cross product. With a
//!   single clause this collapses to the simple
//!   `for_each var in expr` shape.
//! - **Union**: a list of sub-spaces, each its own
//!   Cartesian list of clauses; the iteration concatenates
//!   each sub-space's product. Used when only certain
//!   coordinate combinations are valid (e.g. `(k=10, limit
//!   ∈ 10..50)` and `(k=100, limit ∈ 100..500)` — skipping
//!   the invalid corners).
//!
//! ## Detection rule
//!
//! When a YAML / textual form lists multiple clauses, the
//! parser (`crate::comprehension::parse`, Phase B) decides
//! which mode to emit by checking variable names: if any
//! name repeats across the supplied pairs, it's
//! [`ComprehensionMode::Union`] (the repetition is the
//! signal that the user wanted parallel sub-spaces, not a
//! cross-product). Otherwise — all distinct var names —
//! it's [`ComprehensionMode::Cartesian`].
//!
//! ## Coordinate-set relationship
//!
//! At run time, each iteration of a comprehension scope has
//! its scope-coordinate set
//! ([`crate::kernel::ScopeCoord`]) populated with one
//! `(name, value)` for every distinct variable name the
//! comprehension declares. The names come from
//! [`Comprehension::coordinate_names`]; the values come
//! from [`crate::comprehension::eval::enumerate_tuples`]
//! (Phase C). With this AST in place, that wiring is a
//! 1:1 structural mapping rather than a string-parse
//! round-trip.

use serde::{Deserialize, Serialize};

/// One clause of a comprehension: a variable and the
/// expression whose value list it iterates over.
///
/// `var` is the name bound on each iteration (becomes a
/// scope coordinate). `expr` is the textual source — typically
/// a `{name}` reference to a workload parameter or a literal
/// comma list (`"1,10,100"`), but any GK-evaluable expression
/// works; the evaluator (Phase C) interpolates parent-scope
/// names against a sub-kernel and parses the result.
///
/// String fields are owned because the AST gets stored on
/// long-lived scope-tree / scenario-tree nodes; sharing
/// references back into the source text would force
/// lifetimes through every consumer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Clause {
    pub var: String,
    pub expr: String,
}

impl Clause {
    pub fn new(var: impl Into<String>, expr: impl Into<String>) -> Self {
        Self { var: var.into(), expr: expr.into() }
    }
}

/// How the clauses of a comprehension combine.
///
/// See the module-level doc for the detection rule and the
/// motivating examples.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ComprehensionMode {
    /// One ordered list of clauses; iteration emits the
    /// cross product. `Cartesian(vec![one_clause])` is the
    /// degenerate single-variable form (`for_each var in expr`).
    Cartesian(Vec<Clause>),
    /// A list of sub-spaces. Each inner `Vec<Clause>` is one
    /// sub-space (its own Cartesian list); iteration emits
    /// each sub-space's product, concatenated in declaration
    /// order. Variable names typically repeat across sub-spaces
    /// so children see the same binding shape regardless of
    /// which sub-space the current tuple came from.
    Union(Vec<Vec<Clause>>),
}

/// Traversal order for emitted tuples. See SRD-18d.
///
/// Default emission is lexicographic with rightmost clause
/// varying fastest — equivalent to `Lex { count: None }`. The
/// `count` / `strata` / `depth` fields are the natural
/// truncation parameter for each strategy and correspond to
/// the `name/N` terse form in the text grammar.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TraversalOrder {
    /// Lexicographic, rightmost varies fastest. Equivalent to
    /// no `order` clause at all; included so consumers can
    /// represent "explicitly default" if needed.
    Lex { #[serde(default, skip_serializing_if = "Option::is_none")] count: Option<usize> },
    /// Lexicographic, leftmost varies fastest.
    ReverseLex { #[serde(default, skip_serializing_if = "Option::is_none")] count: Option<usize> },
    /// Sort by sum-of-indices ascending; ties broken by lex.
    Diagonal { #[serde(default, skip_serializing_if = "Option::is_none")] count: Option<usize> },
    /// Sort by sum-of-indices descending.
    Antidiagonal { #[serde(default, skip_serializing_if = "Option::is_none")] count: Option<usize> },
    /// All-extrema first, stratified by interior count.
    /// `strata = Some(N)` keeps the first N strata; `Some(1)` =
    /// corners only.
    Extrema { #[serde(default, skip_serializing_if = "Option::is_none")] strata: Option<usize> },
    /// Concentric L∞ shells from a chosen origin.
    Shells {
        #[serde(default)]
        origin: ShellOrigin,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        depth: Option<usize>,
    },
    /// Halton low-discrepancy sequence.
    Halton { #[serde(default, skip_serializing_if = "Option::is_none")] count: Option<usize> },
    /// Sobol low-discrepancy sequence.
    Sobol { #[serde(default, skip_serializing_if = "Option::is_none")] count: Option<usize> },
    /// Latin Hypercube samples.
    Lhs {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        count: Option<usize>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        seed: Option<u64>,
    },
    /// User-supplied GK function name. Function takes the tuple
    /// list and returns a permutation/subset.
    Custom { function: String },
}

/// Origin for shell stratification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ShellOrigin {
    /// Shells from the boundary inward. Shell 0 = boundary,
    /// shell N = deepest interior.
    #[default]
    Outer,
    /// Shells from the center outward. Shell 0 = center,
    /// shell N = boundary.
    Center,
    /// Shells from the (0, …, 0) corner outward.
    Corner,
}

/// The static shape of an iteration scope. See module doc.
///
/// `filter` is an optional GK predicate evaluated against each
/// emitted tuple. Tuples for which the predicate evaluates to
/// `Value::Bool(false)` are skipped — children don't run for
/// them.
///
/// **Predicate syntax**: a string interpolation expression in
/// the same shape as clause spec text — clause-bound names and
/// inherited scope names appear as `{name}` placeholders, the
/// rest is a const-evaluable expression. The evaluator
/// interpolates `{var}` placeholders against the per-tuple
/// kernel (which has every clause value installed and
/// parent-scope wiring done), then runs `eval_const_expr` on
/// the result. The expression must yield `Value::Bool`.
///
/// Examples:
/// - `{k} * {limit} < 1000`
/// - `{profile} == "ann"`
/// - `{k} > {threshold}` (where `threshold` is an inherited name)
///
/// Cartesian and Union modes both honor the same single
/// filter — it composes uniformly over the cross product (one
/// predicate against each tuple) regardless of how the tuple
/// space was assembled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Comprehension {
    pub mode: ComprehensionMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order: Option<TraversalOrder>,
}

impl Comprehension {
    pub fn cartesian(clauses: Vec<Clause>) -> Self {
        Self {
            mode: ComprehensionMode::Cartesian(clauses),
            filter: None,
            order: None,
        }
    }

    pub fn union(subspaces: Vec<Vec<Clause>>) -> Self {
        Self {
            mode: ComprehensionMode::Union(subspaces),
            filter: None,
            order: None,
        }
    }

    /// Attach a filter predicate, returning `self` for builder
    /// chaining. The predicate is a GK expression that must
    /// evaluate to `Value::Bool` per tuple — anything else is a
    /// runtime error from the comprehension's evaluator.
    pub fn with_filter(mut self, predicate: impl Into<String>) -> Self {
        self.filter = Some(predicate.into());
        self
    }

    /// Attach a traversal order, returning `self` for builder
    /// chaining. See [`TraversalOrder`] and SRD-18d.
    pub fn with_order(mut self, order: TraversalOrder) -> Self {
        self.order = Some(order);
        self
    }

    /// Variable names this comprehension declares, in
    /// declaration order, **deduplicated**. For Cartesian
    /// mode that's exactly the LHS of each clause. For Union
    /// mode the names typically repeat across sub-spaces;
    /// dedup gives the operator-visible coordinate set
    /// (children see one extern per name regardless of
    /// sub-space count).
    ///
    /// Order is first-occurrence (preserves the user's
    /// authored intent — `k` before `limit` in
    /// `"k in …, limit in …"` stays in that order even
    /// after dedup).
    pub fn coordinate_names(&self) -> Vec<&str> {
        let mut out: Vec<&str> = Vec::new();
        for clause in self.flat_clauses() {
            if !out.iter().any(|n| *n == clause.var.as_str()) {
                out.push(&clause.var);
            }
        }
        out
    }

    /// Every clause in the comprehension flattened into one
    /// iterator, in declaration order. For Cartesian mode
    /// that's the clause list directly; for Union mode it's
    /// the concatenation of all sub-spaces' clauses
    /// (preserving order, including any repeats — callers
    /// that want unique names should use
    /// [`Self::coordinate_names`]).
    pub fn flat_clauses(&self) -> Vec<&Clause> {
        match &self.mode {
            ComprehensionMode::Cartesian(clauses) => clauses.iter().collect(),
            ComprehensionMode::Union(subspaces) => {
                let mut out = Vec::new();
                for sub in subspaces {
                    for clause in sub {
                        out.push(clause);
                    }
                }
                out
            }
        }
    }

    /// Number of clauses across all sub-spaces (with
    /// repetition for Union mode). For Cartesian mode this
    /// is also the number of *coordinates*; for Union mode
    /// see [`Self::coordinate_names`] for the deduplicated
    /// count.
    pub fn clause_count(&self) -> usize {
        self.flat_clauses().len()
    }

    pub fn is_cartesian(&self) -> bool {
        matches!(self.mode, ComprehensionMode::Cartesian(_))
    }

    pub fn is_union(&self) -> bool {
        matches!(self.mode, ComprehensionMode::Union(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cartesian_coordinate_names_in_declaration_order() {
        let c = Comprehension::cartesian(vec![
            Clause::new("k", "{k_values}"),
            Clause::new("limit", "{k_{k}_limits}"),
        ]);
        assert_eq!(c.coordinate_names(), vec!["k", "limit"]);
        assert!(c.is_cartesian());
        assert_eq!(c.clause_count(), 2);
    }

    #[test]
    fn union_dedupes_repeated_names_first_occurrence_wins() {
        // Sub-space 1 binds k, limit. Sub-space 2 also binds
        // k, limit. The dedup'd coordinate names are still
        // `[k, limit]` in their first-occurrence order — not
        // doubled.
        let c = Comprehension::union(vec![
            vec![Clause::new("k", "10"), Clause::new("limit", "10,20,30")],
            vec![Clause::new("k", "100"), Clause::new("limit", "100,200,300")],
        ]);
        assert_eq!(c.coordinate_names(), vec!["k", "limit"]);
        assert!(c.is_union());
        // Flat clauses preserve repetition (4 entries — two
        // per sub-space).
        assert_eq!(c.flat_clauses().len(), 4);
        assert_eq!(c.clause_count(), 4);
    }

    #[test]
    fn single_clause_cartesian_is_the_simple_form() {
        let c = Comprehension::cartesian(vec![
            Clause::new("profile", "matching_profiles('{dataset}', '{prefix}')"),
        ]);
        assert_eq!(c.coordinate_names(), vec!["profile"]);
        assert_eq!(c.clause_count(), 1);
    }

    #[test]
    fn union_with_distinct_names_per_subspace_keeps_all_in_order() {
        // Pathological case (probably not real-world): two
        // sub-spaces each with their own distinct vars.
        // First-occurrence ordering preserves authoring intent.
        let c = Comprehension::union(vec![
            vec![Clause::new("a", "1")],
            vec![Clause::new("b", "2")],
        ]);
        assert_eq!(c.coordinate_names(), vec!["a", "b"]);
    }
}
