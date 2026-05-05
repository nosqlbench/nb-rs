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

use std::fmt;

use serde::{Deserialize, Serialize};

/// One clause of a comprehension: one or more variable
/// names paired with their source expression(s).
///
/// **Single-var clause** (the common case): one variable
/// binds successive values from one source list.
/// `Clause::new("k", "1..10")` is the construction shortcut.
///
/// **Parallel clause** (SRD-18c Layer 7a): multiple
/// variables advance in lockstep ("zip") from multiple
/// source expressions. `Clause::parallel(["x", "y"],
/// ["1..10", "100..1000..100"])` builds the parallel form.
///
/// The string fields are owned because the AST gets stored
/// on long-lived scope-tree / scenario-tree nodes; sharing
/// references back into the source text would force
/// lifetimes through every consumer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Clause {
    pub vars: Vec<String>,
    pub source: ClauseSource,
}

/// A clause's source of values. See [`Clause`] for context.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ClauseSource {
    /// A single expression yielding `Vec<Value>`. Length
    /// of the value list matches the iteration cardinality.
    /// `vars.len()` is 1 for the single-var common case;
    /// `vars.len() > 1` would mean destructure form (Layer
    /// 7b, gated on `Value::Tuple`).
    Single(String),
    /// One expression per var; the sources zip in lockstep.
    /// `vars.len() == exprs.len() ≥ 2`. Length policy is
    /// controlled by [`ZipMode`]: strict (default) errors
    /// on mismatch, truncate cuts to the shortest, cycle
    /// repeats shorter sources to the longest.
    Parallel { mode: ZipMode, exprs: Vec<String> },
}

/// Length-policy for parallel-iter clauses (SRD-18c Layer 7a).
///
/// Authored as the RHS form: bare parens `(e1, e2)` = strict;
/// `zip_truncate(e1, e2)` = truncate; `zip_cycle(e1, e2)` =
/// cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ZipMode {
    /// Every expression must produce the same number of
    /// values. Mismatch is an error at iteration time.
    /// Default for the bare `(e1, e2)` syntax.
    #[default]
    Strict,
    /// Truncate every expression to the length of the
    /// shortest. The user opts in via `zip_truncate(...)`.
    Truncate,
    /// Cycle shorter expressions to match the longest. The
    /// user opts in via `zip_cycle(...)`.
    Cycle,
}

impl Clause {
    /// Single-var construction: `Clause::new("k", "1..10")`.
    /// Backward-compatible with the pre-Layer-7a shape — every
    /// existing call site works unchanged.
    pub fn new(var: impl Into<String>, expr: impl Into<String>) -> Self {
        Self {
            vars: vec![var.into()],
            source: ClauseSource::Single(expr.into()),
        }
    }

    /// Parallel-iter construction (SRD-18c Layer 7a):
    /// `Clause::parallel(["x", "y"], ["1..10", "fib(8)"])`.
    /// Defaults to [`ZipMode::Strict`] — length mismatch
    /// across the parallel group is an iteration-time error.
    /// Use [`Clause::parallel_with_mode`] to opt into
    /// truncate / cycle.
    ///
    /// Length mismatch between `vars` and `exprs` is a
    /// programming error and panics — the parser's input
    /// validation should catch malformed user input before
    /// it reaches this constructor.
    pub fn parallel(
        vars: impl IntoIterator<Item = impl Into<String>>,
        exprs: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self::parallel_with_mode(ZipMode::Strict, vars, exprs)
    }

    /// Parallel-iter construction with explicit zip mode.
    /// See [`ZipMode`].
    pub fn parallel_with_mode(
        mode: ZipMode,
        vars: impl IntoIterator<Item = impl Into<String>>,
        exprs: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        let vars: Vec<String> = vars.into_iter().map(Into::into).collect();
        let exprs: Vec<String> = exprs.into_iter().map(Into::into).collect();
        assert_eq!(vars.len(), exprs.len(),
            "Clause::parallel: vars and exprs must have equal length");
        assert!(vars.len() >= 2,
            "Clause::parallel: parallel form requires ≥ 2 variables (use Clause::new for single-var)");
        Self { vars, source: ClauseSource::Parallel { mode, exprs } }
    }

    /// Single-var convenience: returns the lone variable
    /// name when the clause is single-var. `None` for
    /// parallel-iter forms — those have multiple names.
    pub fn single_var(&self) -> Option<&str> {
        if self.vars.len() == 1 { Some(&self.vars[0]) } else { None }
    }

    /// Single-source convenience: returns the lone source
    /// expression when the clause uses `ClauseSource::Single`.
    /// `None` for parallel forms.
    pub fn single_expr(&self) -> Option<&str> {
        match &self.source {
            ClauseSource::Single(s) => Some(s),
            ClauseSource::Parallel { .. } => None,
        }
    }

    /// True for parallel-iter clauses (Layer 7a).
    pub fn is_parallel(&self) -> bool {
        matches!(self.source, ClauseSource::Parallel { .. })
    }

    /// Backward-compat accessor: returns the first variable
    /// name regardless of clause shape. Single-var clauses
    /// have `vars.len() == 1`; parallel clauses have ≥ 2.
    /// Most existing callers operate on single-var clauses
    /// and treat parallel forms as either-or — those should
    /// migrate to `single_var()` for explicit handling.
    pub fn first_var(&self) -> &str {
        &self.vars[0]
    }

    /// Convenience for single-var clauses (the historical
    /// common case): the lone variable name. For parallel
    /// clauses, returns the first variable's name. Most
    /// existing call sites treat clauses as single-var; this
    /// keeps them working with a one-line `c.var` →
    /// `c.var()` migration.
    pub fn var(&self) -> &str {
        &self.vars[0]
    }

    /// Convenience for single-source clauses: the lone
    /// source-expression text. For parallel clauses, returns
    /// the first expression — single-var-assuming callers
    /// see the same shape they did before Layer 7a.
    pub fn expr(&self) -> &str {
        match &self.source {
            ClauseSource::Single(s) => s,
            ClauseSource::Parallel { exprs, .. } => &exprs[0],
        }
    }

    /// Flatten this clause to its scalar `(var, expr)` pairs.
    ///
    /// - **Single-var** (`vars = [v]`, `Single(s)`): returns
    ///   `[(v, s)]`.
    /// - **Parallel-iter** (`vars = [v0, v1, ...]`,
    ///   `Parallel { exprs: [e0, e1, ...], .. }`): returns
    ///   `[(v0, e0), (v1, e1), ...]`.
    ///
    /// One canonical place to expand the var↔expr mapping —
    /// previously open-coded at three callers (synthesis
    /// representative-vars expansion, runner canonical-input
    /// declaration, runner param-ref scan).
    pub fn scalar_bindings(&self) -> Vec<(&str, &str)> {
        match &self.source {
            ClauseSource::Single(s) => {
                self.vars.iter().map(|v| (v.as_str(), s.as_str())).collect()
            }
            ClauseSource::Parallel { exprs, .. } => {
                self.vars.iter().zip(exprs.iter())
                    .map(|(v, e)| (v.as_str(), e.as_str()))
                    .collect()
            }
        }
    }
}

/// Canonical text rendering of a clause:
/// - Single-var: `var in expr`.
/// - Parallel-iter: `(a, b) in (e1, e2)` for [`ZipMode::Strict`],
///   `zip_truncate(...)` / `zip_cycle(...)` for the other modes.
///
/// `parse_clause(&clause.to_string())` round-trips back to the
/// same AST.
impl fmt::Display for Clause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.source {
            ClauseSource::Single(s) => write!(f, "{} in {}", self.vars[0], s),
            ClauseSource::Parallel { mode, exprs } => {
                write!(f, "({}) in ", self.vars.join(", "))?;
                let inner = exprs.join(", ");
                match mode {
                    ZipMode::Strict   => write!(f, "({inner})"),
                    ZipMode::Truncate => write!(f, "zip_truncate({inner})"),
                    ZipMode::Cycle    => write!(f, "zip_cycle({inner})"),
                }
            }
        }
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
    /// A list of sub-spaces. Each [`Subspace`] is one Cartesian
    /// list of clauses; iteration emits each sub-space's product,
    /// concatenated in declaration order. Variable names typically
    /// repeat across sub-spaces so children see the same binding
    /// shape regardless of which sub-space the current tuple came
    /// from.
    Union(Vec<Subspace>),
}

/// One sub-space of a [`ComprehensionMode::Union`]: an ordered
/// list of clauses whose Cartesian product is one chunk of the
/// emitted tuple stream.
///
/// Wrapper struct (rather than a bare `Vec<Clause>`) so future
/// per-subspace metadata — sub-filters, labels, ordering hints —
/// can land additively without breaking match sites.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Subspace {
    pub clauses: Vec<Clause>,
}

impl Subspace {
    pub fn new(clauses: Vec<Clause>) -> Self { Self { clauses } }
    pub fn is_empty(&self) -> bool { self.clauses.is_empty() }
    pub fn len(&self) -> usize { self.clauses.len() }
    pub fn iter(&self) -> std::slice::Iter<'_, Clause> { self.clauses.iter() }
}

impl<'a> IntoIterator for &'a Subspace {
    type Item = &'a Clause;
    type IntoIter = std::slice::Iter<'a, Clause>;
    fn into_iter(self) -> Self::IntoIter { self.clauses.iter() }
}

impl From<Vec<Clause>> for Subspace {
    fn from(clauses: Vec<Clause>) -> Self { Self { clauses } }
}

impl std::ops::Index<usize> for Subspace {
    type Output = Clause;
    fn index(&self, i: usize) -> &Clause { &self.clauses[i] }
}

impl std::ops::Deref for Subspace {
    type Target = [Clause];
    fn deref(&self) -> &[Clause] { &self.clauses }
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
            mode: ComprehensionMode::Union(
                subspaces.into_iter().map(Subspace::new).collect()
            ),
            filter: None,
            order: None,
        }
    }

    /// Construct a Union from already-wrapped [`Subspace`]
    /// values. Use this when subspaces carry metadata; the
    /// `union(...)` shorthand wraps bare `Vec<Clause>` lists.
    pub fn union_from(subspaces: Vec<Subspace>) -> Self {
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
            for v in &clause.vars {
                if !out.iter().any(|n| *n == v.as_str()) {
                    out.push(v);
                }
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
                    for clause in &sub.clauses {
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

    /// Validate the comprehension's static structure. Returns
    /// the empty `Ok(())` if every invariant holds; otherwise
    /// `Err(messages)` where each message names one violation.
    ///
    /// This is the **single** entry point for AST-shape
    /// invariants — `parse_comprehension_text`,
    /// workload-load, dryrun, and any future linter all route
    /// through here so the rule set lives in exactly one place.
    ///
    /// Checks performed:
    ///
    /// 1. **Non-empty clause set.** Cartesian must have ≥ 1
    ///    clause; Union must have ≥ 1 sub-space, each with
    ///    ≥ 1 clause.
    /// 2. **No coordinate-name collisions in Cartesian mode.**
    ///    Every variable must be unique across the clause list
    ///    (Cartesian product of two clauses with the same name
    ///    is undefined). Union mode permits repeated names —
    ///    that's the structural Union signal.
    /// 3. **Order/mode compatibility.** Index-space orderings
    ///    (reverse_lex, diagonal, antidiagonal, extrema,
    ///    shells, halton, sobol, lhs) require a single
    ///    Cartesian lattice — they're rejected on Union mode
    ///    where there is no such lattice. `lex` and `custom`
    ///    are accepted on both modes.
    ///
    /// Filter / parallel-iter length checks happen at iteration
    /// time (they require evaluation, not just structure).
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors: Vec<String> = Vec::new();
        match &self.mode {
            ComprehensionMode::Cartesian(clauses) => {
                if clauses.is_empty() {
                    errors.push("Cartesian comprehension has no clauses".to_string());
                }
                let mut seen: Vec<&str> = Vec::new();
                for clause in clauses {
                    for v in &clause.vars {
                        if seen.iter().any(|n| *n == v.as_str()) {
                            errors.push(format!(
                                "Cartesian comprehension repeats variable name '{v}' \
                                 — name collision across clauses (use Union mode for \
                                 alternative sub-spaces with shared coordinate names)"
                            ));
                        } else {
                            seen.push(v.as_str());
                        }
                    }
                }
            }
            ComprehensionMode::Union(subspaces) => {
                if subspaces.is_empty() {
                    errors.push("Union comprehension has no sub-spaces".to_string());
                }
                for (i, sub) in subspaces.iter().enumerate() {
                    if sub.is_empty() {
                        errors.push(format!(
                            "Union sub-space #{i} has no clauses"
                        ));
                    }
                }
            }
        }
        if let Err(e) = check_order_for_mode(&self.mode, &self.order) {
            errors.push(e);
        }
        if errors.is_empty() { Ok(()) } else { Err(errors) }
    }
}

/// Order/mode compatibility check used by
/// [`Comprehension::validate`]. SRD-18e §"Union mode +
/// non-lex orderings" specifies the rule: every
/// index-space strategy (`reverse_lex`, `diagonal`,
/// `antidiagonal`, `extrema`, `shells`, `halton`, `sobol`,
/// `lhs`) requires a single Cartesian lattice. `lex` (no
/// geometric reasoning) and `custom` (the user's function
/// decides) remain valid for Union mode.
pub(crate) fn check_order_for_mode(
    mode: &ComprehensionMode,
    order: &Option<TraversalOrder>,
) -> Result<(), String> {
    let ComprehensionMode::Union(_) = mode else { return Ok(()); };
    let Some(order) = order else { return Ok(()); };
    let strategy_name = match order {
        TraversalOrder::Lex { .. } => return Ok(()),
        TraversalOrder::Custom { .. } => return Ok(()),
        TraversalOrder::ReverseLex { .. } => "reverse_lex",
        TraversalOrder::Diagonal { .. } => "diagonal",
        TraversalOrder::Antidiagonal { .. } => "antidiagonal",
        TraversalOrder::Extrema { .. } => "extrema",
        TraversalOrder::Shells { .. } => "shells",
        TraversalOrder::Halton { .. } => "halton",
        TraversalOrder::Sobol { .. } => "sobol",
        TraversalOrder::Lhs { .. } => "lhs",
    };
    Err(format!(
        "ordering '{strategy_name}' has no defined behavior on Union mode \
         (no single Cartesian lattice). Use Cartesian mode, or pick \
         'lex' / 'custom' which are well-defined on Union."
    ))
}

/// Canonical text rendering of a comprehension:
/// `<clauses> [where <filter>] [order <spec>]`.
///
/// Cartesian: clauses are joined by `, `. Union: each
/// sub-space is rendered as a parenthesised clause group
/// joined by ` | ` to make the sub-space boundaries visible
/// (the textual short-form parser detects Union via
/// repeated-name signal, but the explicit form is what
/// `Display` emits to keep the round-trip semantics-preserving
/// regardless of sub-space layout).
impl fmt::Display for Comprehension {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.mode {
            ComprehensionMode::Cartesian(clauses) => {
                let parts: Vec<String> = clauses.iter().map(|c| c.to_string()).collect();
                write!(f, "{}", parts.join(", "))?;
            }
            ComprehensionMode::Union(subspaces) => {
                let parts: Vec<String> = subspaces.iter().map(|sub| {
                    let inner: Vec<String> = sub.clauses.iter()
                        .map(|c| c.to_string()).collect();
                    inner.join(", ")
                }).collect();
                write!(f, "{}", parts.join(" | "))?;
            }
        }
        if let Some(predicate) = &self.filter {
            write!(f, " where {predicate}")?;
        }
        if let Some(order) = &self.order {
            write!(f, " order {}", format_order(order))?;
        }
        Ok(())
    }
}

/// Render a [`TraversalOrder`] as text matching
/// [`crate::comprehension::parse::parse_order_spec`]'s
/// accepted forms.
fn format_order(order: &TraversalOrder) -> String {
    fn count_suffix(n: Option<usize>) -> String {
        n.map(|n| format!("/{n}")).unwrap_or_default()
    }
    match order {
        TraversalOrder::Lex { count } => format!("lex{}", count_suffix(*count)),
        TraversalOrder::ReverseLex { count } => format!("reverse_lex{}", count_suffix(*count)),
        TraversalOrder::Diagonal { count } => format!("diagonal{}", count_suffix(*count)),
        TraversalOrder::Antidiagonal { count } => format!("antidiagonal{}", count_suffix(*count)),
        TraversalOrder::Extrema { strata } => format!("extrema{}", count_suffix(*strata)),
        TraversalOrder::Shells { origin, depth } => {
            let origin_part = match origin {
                ShellOrigin::Outer => "",
                ShellOrigin::Center => "/center",
                ShellOrigin::Corner => "/corner",
            };
            format!("shells{}{}", origin_part, count_suffix(*depth))
        }
        TraversalOrder::Halton { count } => format!("halton{}", count_suffix(*count)),
        TraversalOrder::Sobol { count } => format!("sobol{}", count_suffix(*count)),
        TraversalOrder::Lhs { count, seed } => {
            let mut s = format!("lhs{}", count_suffix(*count));
            if let Some(k) = seed { s.push_str(&format!(" seed={k}")); }
            s
        }
        TraversalOrder::Custom { function } => format!("custom({function})"),
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

    // ---- Display contract ------------------------------------

    #[test]
    fn display_single_var_clause() {
        let c = Clause::new("k", "1..10");
        assert_eq!(c.to_string(), "k in 1..10");
    }

    #[test]
    fn display_parallel_clause_strict() {
        let c = Clause::parallel(["x", "y"], ["fib(8)", "pow2(8)"]);
        assert_eq!(c.to_string(), "(x, y) in (fib(8), pow2(8))");
    }

    #[test]
    fn display_parallel_clause_truncate() {
        let c = Clause::parallel_with_mode(
            ZipMode::Truncate, ["x", "y"], ["fib(8)", "pow2(4)"]
        );
        assert_eq!(c.to_string(), "(x, y) in zip_truncate(fib(8), pow2(4))");
    }

    #[test]
    fn display_parallel_clause_cycle() {
        let c = Clause::parallel_with_mode(
            ZipMode::Cycle, ["x", "y"], ["1..4", "10..20..10"]
        );
        assert_eq!(c.to_string(), "(x, y) in zip_cycle(1..4, 10..20..10)");
    }

    #[test]
    fn display_cartesian_comprehension() {
        let c = Comprehension::cartesian(vec![
            Clause::new("k", "1..10"),
            Clause::new("limit", "10,20,30"),
        ]);
        assert_eq!(c.to_string(), "k in 1..10, limit in 10,20,30");
    }

    #[test]
    fn display_comprehension_with_filter_and_order() {
        let c = Comprehension::cartesian(vec![Clause::new("k", "1..10")])
            .with_filter("{k} > 3")
            .with_order(TraversalOrder::Extrema { strata: Some(2) });
        assert_eq!(c.to_string(), "k in 1..10 where {k} > 3 order extrema/2");
    }

    // ---- Validate contract -----------------------------------

    #[test]
    fn validate_accepts_valid_cartesian() {
        let c = Comprehension::cartesian(vec![
            Clause::new("k", "1..10"),
            Clause::new("limit", "10,20,30"),
        ]);
        assert!(c.validate().is_ok());
    }

    #[test]
    fn validate_accepts_valid_union() {
        let c = Comprehension::union(vec![
            vec![Clause::new("k", "10"), Clause::new("limit", "10,20")],
            vec![Clause::new("k", "100"), Clause::new("limit", "100,200")],
        ]);
        assert!(c.validate().is_ok());
    }

    #[test]
    fn validate_rejects_empty_cartesian() {
        let c = Comprehension::cartesian(vec![]);
        let errs = c.validate().unwrap_err();
        assert!(errs.iter().any(|e| e.contains("no clauses")), "got: {errs:?}");
    }

    #[test]
    fn validate_rejects_empty_union() {
        let c = Comprehension::union(vec![]);
        let errs = c.validate().unwrap_err();
        assert!(errs.iter().any(|e| e.contains("no sub-spaces")), "got: {errs:?}");
    }

    #[test]
    fn validate_rejects_empty_subspace_inside_union() {
        let c = Comprehension::union(vec![
            vec![Clause::new("k", "10")],
            vec![],  // empty sub-space
        ]);
        let errs = c.validate().unwrap_err();
        assert!(errs.iter().any(|e| e.contains("Union sub-space #1 has no clauses")),
            "got: {errs:?}");
    }

    #[test]
    fn validate_rejects_cartesian_name_collision() {
        let c = Comprehension::cartesian(vec![
            Clause::new("k", "10"),
            Clause::new("k", "20"),  // same name in Cartesian
        ]);
        let errs = c.validate().unwrap_err();
        assert!(errs.iter().any(|e| e.contains("repeats variable name 'k'")),
            "got: {errs:?}");
    }

    #[test]
    fn validate_rejects_cartesian_collision_with_parallel_clause() {
        let c = Comprehension::cartesian(vec![
            Clause::parallel(["x", "y"], ["1..10", "10..100..10"]),
            Clause::new("y", "100"),  // conflicts with parallel-group y
        ]);
        let errs = c.validate().unwrap_err();
        assert!(errs.iter().any(|e| e.contains("repeats variable name 'y'")),
            "got: {errs:?}");
    }

    #[test]
    fn validate_permits_repeated_names_across_union_subspaces() {
        // Repeated `k` across sub-spaces is the structural Union
        // signal — must NOT be rejected.
        let c = Comprehension::union(vec![
            vec![Clause::new("k", "10")],
            vec![Clause::new("k", "100")],
        ]);
        assert!(c.validate().is_ok());
    }

    #[test]
    fn display_union_uses_pipe_separator_per_subspace() {
        let c = Comprehension::union(vec![
            vec![Clause::new("k", "10"), Clause::new("limit", "10,20")],
            vec![Clause::new("k", "100"), Clause::new("limit", "100,200")],
        ]);
        assert_eq!(c.to_string(),
            "k in 10, limit in 10,20 | k in 100, limit in 100,200");
    }
}
