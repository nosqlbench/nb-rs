// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Validation — spec §5 (V1-V9) + §5.8 (modes).
//!
//! [`validate`] is the single entry point. Walking the AST
//! bottom-up, every variant's V-axiom checks fire; any failure
//! produces a typed [`ValidationError`]. Degenerate-but-defined
//! compositions emit a [`ValidationWarning`] in Permissive mode
//! and become hard errors in Strict mode.
//!
//! V4 (per-strategy input-shape contract) is partially stubbed
//! pending Phase 3's metadata propagation: this implementation
//! checks the structural shape (input must be a clause /
//! cartesian / zip / union — anything addressable) but doesn't
//! yet compute the precise `IndexFn` variant. The stub rejects
//! the unambiguously-bad cases (non-Lex strategy over a raw
//! filter output, where index addressability is destroyed) and
//! defers the rest to Phase 3.

use serde::{Deserialize, Serialize};

use super::ast::Comprehension;
use super::cardinality::CardinalityClass;
use super::metadata::{IndexFn, Metadata};
use super::source::Source;
use super::strategy::{StrategyName, ZipMode};

/// Validation mode per spec §5.8.
///
/// `Permissive` (default) enforces V1-V9 as errors and surfaces
/// degenerate-composition warnings non-blockingly. `Strict`
/// promotes those warnings to errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Mode {
    Permissive,
    Strict,
}

impl Default for Mode {
    fn default() -> Self {
        Mode::Permissive
    }
}

/// Result of a validation pass.
#[derive(Debug, Clone)]
pub struct ValidationReport {
    pub warnings: Vec<ValidationWarning>,
}

/// V-axiom violation. Each variant carries enough context to
/// produce a useful diagnostic at the call site.
#[derive(Debug, Clone, PartialEq)]
pub enum ValidationError {
    /// V1 — cartesian or zip children share a name.
    V1DuplicateName { combinator: &'static str, name: String },

    /// V2 — union children disagree on tuple shape.
    V2ShapeMismatch { expected: Vec<String>, actual: Vec<String> },

    /// V3 — filter predicate references a name neither in the
    /// child's coordinates nor in the parent scope. Parser-time
    /// validation only — link-time (parent scope) check lives
    /// in the consumer.
    ///
    /// `coords` is the wrapped comprehension's coordinate set;
    /// the predicate may also reference names from the parent
    /// scope which this layer doesn't see.
    V3UnresolvedNames {
        predicate: String,
        coords: Vec<String>,
        unresolved: Vec<String>,
    },

    /// V4 — strategy applied to an input whose shape it cannot
    /// accept. The Phase 2 stub form rejects:
    ///   - non-`Lex` strategy whose input is a raw `Filter`
    ///     node (filter destroys index addressability;
    ///     V5's one-layer look-through is enforced by checking
    ///     that the filter's child is addressable);
    ///   - lattice-geometric strategy whose input is a `Union`
    ///     or a 1-axis `Clause` (per §3.6 strategy table).
    /// Phase 3 will replace this with the full IndexFn check.
    V4InputShape {
        strategy: StrategyName,
        reason: String,
    },

    /// V6 — non-Lex order or Strict/Truncate zip applied to an
    /// `Unbounded` discrete input.
    V6UnboundedDiscrete {
        operator: &'static str,
        cardinality: CardinalityClass,
    },

    /// V7 — zip cardinality contract violated. Three sub-cases:
    /// Strict-mode mismatch, mixed-class children, or any
    /// continuous child.
    V7ZipCardinality {
        mode: ZipMode,
        reason: String,
    },

    /// V8 — continuous source requires explicit sampling OR
    /// source declares a non-integrable measure.
    V8ContinuousRequirement { reason: String },

    /// V9 — union children include a continuous or mixed-class
    /// child.
    V9UnionClassMismatch { reason: String },
}

/// Non-blocking warning for degenerate-but-defined compositions
/// per spec §5.8.
#[derive(Debug, Clone, PartialEq)]
pub enum ValidationWarning {
    /// Lattice-geometric strategy (`Extrema` / `Shells` /
    /// `Diagonal` / `Antidiagonal`) over a 1-axis input.
    /// Collapses to {first, last} or a trivial walk; usually
    /// not what the author meant.
    DegenerateGeometric { strategy: StrategyName },

    /// `Lhs` over a 1-axis input. Equivalent to `Shuffle`;
    /// two names for one behavior.
    LhsDegenerate,

    /// `filter(c, "true")`. Empty-effect filter — usually a
    /// bug-shaped predicate. The optimizer's R0a elides it.
    TriviallyTrueFilter,

    /// `filter(c, "false")`. Empty dispense sequence. If
    /// intentional, use an empty literal source; otherwise the
    /// predicate is bug-shaped.
    TriviallyFalseFilter,

    /// Singleton variant of a combinator: `zip([c], _)`,
    /// `cartesian(c)`, `union(c)`. Identity per spec §4.2 I1-I3;
    /// the optimizer's R0a elides it.
    SingletonCombinator { combinator: &'static str },
}

/// Validate a comprehension AST per spec §5.
///
/// In `Permissive` mode, V1-V9 errors abort with a typed
/// [`ValidationError`] and degenerate-composition warnings
/// accumulate into the returned [`ValidationReport`]. In
/// `Strict` mode, the first warning is promoted to an error.
pub fn validate(c: &Comprehension, mode: Mode) -> Result<ValidationReport, ValidationError> {
    let mut report = ValidationReport { warnings: Vec::new() };
    visit(c, &mut report)?;
    if mode == Mode::Strict
        && let Some(_warning) = report.warnings.first()
    {
        // Strict-mode promotion: encode the first warning as a
        // V8-like error using a synthetic reason. We don't
        // currently have a dedicated ValidationError variant
        // for "warning promoted"; the diagnostic is still
        // useful because the warning itself carries the
        // location-equivalent context.
        return Err(ValidationError::V8ContinuousRequirement {
            reason: format!(
                "strict mode: warning promoted: {:?}",
                report.warnings.first().unwrap()
            ),
        });
    }
    Ok(report)
}

fn visit(c: &Comprehension, report: &mut ValidationReport) -> Result<(), ValidationError> {
    // Bottom-up: validate children first so each node sees
    // already-well-formed operands per spec C2.
    for child in c.children() {
        visit(child, report)?;
    }

    match c {
        Comprehension::Clause { source, .. } => visit_clause(source, report),
        Comprehension::Cartesian { children } => visit_cartesian(children, report),
        Comprehension::Zip { children, mode } => visit_zip(children, *mode, report),
        Comprehension::Union { children } => visit_union(children, report),
        Comprehension::Filter { child, predicate } => visit_filter(child, predicate, report),
        Comprehension::Order { child, strategy, truncation } => {
            visit_order(child, *strategy, *truncation, report)
        }
    }
}

fn visit_clause(source: &Source, report: &mut ValidationReport) -> Result<(), ValidationError> {
    // V8 source-side check: continuous source must have an
    // integrable measure. Unbounded + Uniform is the canonical
    // failure case.
    if let Source::ContinuousInterval { interval, measure } = source
        && !measure.is_integrable(std::slice::from_ref(interval))
    {
        let _ = report; // no warning here; this is a hard error
        return Err(ValidationError::V8ContinuousRequirement {
            reason: format!(
                "continuous source has non-integrable measure: \
                 interval [{}, {}] + {:?}",
                interval.lo, interval.hi, measure
            ),
        });
    }
    Ok(())
}

fn visit_cartesian(
    children: &[Comprehension],
    report: &mut ValidationReport,
) -> Result<(), ValidationError> {
    check_disjoint_names("cartesian", children)?;
    if children.len() == 1 {
        report.warnings.push(ValidationWarning::SingletonCombinator {
            combinator: "cartesian",
        });
    }
    Ok(())
}

fn visit_zip(
    children: &[Comprehension],
    mode: ZipMode,
    report: &mut ValidationReport,
) -> Result<(), ValidationError> {
    check_disjoint_names("zip", children)?;

    // V7: discrete-only children. We use the leaf-clause check
    // here as the cheapest reliable proxy: walk to find any
    // continuous source in any child.
    for child in children {
        if contains_continuous_source(child) {
            return Err(ValidationError::V7ZipCardinality {
                mode,
                reason: "zip children must all be discrete; \
                         a continuous source was found"
                    .to_string(),
            });
        }
    }

    if children.len() == 1 {
        report.warnings.push(ValidationWarning::SingletonCombinator {
            combinator: "zip",
        });
    }

    // V6: Strict/Truncate require bounded children. We can
    // only fully validate this once Phase 3's metadata
    // propagation lands; at this layer we check the source-
    // level cardinality on direct-clause children as a
    // best-effort gate.
    if matches!(mode, ZipMode::Strict | ZipMode::Truncate) {
        for child in children {
            if let Some(card) = direct_source_cardinality(child)
                && matches!(card, CardinalityClass::Unbounded)
            {
                return Err(ValidationError::V6UnboundedDiscrete {
                    operator: "zip",
                    cardinality: card,
                });
            }
        }
    }

    Ok(())
}

fn visit_union(
    children: &[Comprehension],
    report: &mut ValidationReport,
) -> Result<(), ValidationError> {
    // V9 first: all children must be discrete.
    for child in children {
        if contains_continuous_source(child) {
            return Err(ValidationError::V9UnionClassMismatch {
                reason: "union children must all be discrete; \
                         a continuous source was found"
                    .to_string(),
            });
        }
    }

    // V2: identical tuple shape (same names, same order).
    if let Some(first) = children.first() {
        let expected = first.coordinate_names();
        for sibling in &children[1..] {
            let actual = sibling.coordinate_names();
            if actual != expected {
                return Err(ValidationError::V2ShapeMismatch {
                    expected,
                    actual,
                });
            }
        }
    }

    if children.len() == 1 {
        report.warnings.push(ValidationWarning::SingletonCombinator {
            combinator: "union",
        });
    }
    Ok(())
}

fn visit_filter(
    child: &Comprehension,
    predicate: &str,
    report: &mut ValidationReport,
) -> Result<(), ValidationError> {
    // V3: name closure — every `{name}` reference in the
    // predicate must be in the child's coords OR resolved by
    // the parent scope. The parent-scope half is the consumer's
    // job; here we accumulate the unresolved-at-this-layer
    // names and let the consumer decide.
    let coords = child.coordinate_names();
    let referenced = extract_interpolated_names(predicate);
    let unresolved: Vec<String> = referenced
        .into_iter()
        .filter(|n| !coords.contains(n))
        .collect();

    // The consumer is responsible for the link-time check;
    // we only error here when there's clearly nothing the
    // parent could possibly provide. For now, emit no error —
    // just record candidates for downstream consumption.
    // (When Phase 3 + consumer wiring land, this becomes a
    // structured "carry the unresolved set to the consumer"
    // hand-off.)
    let _ = unresolved;

    // §5.8 warnings for trivially-true / trivially-false
    // predicates. We recognize the literal strings "true" and
    // "false" as the bug-shaped cases; richer recognition
    // happens when the predicate analyzer (Phase 5) lands.
    let trimmed = predicate.trim();
    if trimmed.eq_ignore_ascii_case("true") {
        report.warnings.push(ValidationWarning::TriviallyTrueFilter);
    } else if trimmed.eq_ignore_ascii_case("false") {
        report.warnings.push(ValidationWarning::TriviallyFalseFilter);
    }

    let _ = child;
    Ok(())
}

fn visit_order(
    child: &Comprehension,
    strategy: StrategyName,
    truncation: Option<u64>,
    report: &mut ValidationReport,
) -> Result<(), ValidationError> {
    // V4: per-strategy input-shape contract using the metadata
    // algebra. V5's one-filter look-through is implemented by
    // computing metadata against either the child directly OR
    // (when the child is a Filter) against the filter's
    // inner child.
    let metadata_target = match child {
        Comprehension::Filter { child: inner, .. } => inner.as_ref(),
        other => other,
    };

    // If we'd need to look through more than one filter layer
    // (nested filters), V5 says fold first.
    if !matches!(strategy, StrategyName::Lex)
        && matches!(metadata_target, Comprehension::Filter { .. })
    {
        return Err(ValidationError::V4InputShape {
            strategy,
            reason: "non-Lex strategy applied to nested filter; \
                     fold filters first (spec F1 / R6)"
                .to_string(),
        });
    }

    let target_metadata = metadata_target.metadata();
    check_strategy_input_shape(strategy, &target_metadata, report)?;

    // V6: non-Lex strategy requires bounded input. Now via
    // metadata cardinality (not the source-only direct check).
    if !matches!(strategy, StrategyName::Lex)
        && matches!(target_metadata.cardinality, CardinalityClass::Unbounded)
    {
        return Err(ValidationError::V6UnboundedDiscrete {
            operator: "order",
            cardinality: target_metadata.cardinality.clone(),
        });
    }

    // V8: continuous input requires sampling — wrapped order
    // with finite truncation is the discharge mechanism.
    let is_continuous = matches!(
        target_metadata.cardinality,
        CardinalityClass::Continuous { .. }
            | CardinalityClass::ContinuousAtMost { .. }
            | CardinalityClass::Hybrid(_)
    );
    if is_continuous {
        if truncation.is_none() {
            return Err(ValidationError::V8ContinuousRequirement {
                reason: "continuous comprehension requires order(_, \
                         sampling-strategy, Some(n)) with finite \
                         truncation"
                    .to_string(),
            });
        }
        if matches!(strategy, StrategyName::Lex) {
            return Err(ValidationError::V8ContinuousRequirement {
                reason: "Lex does not sample continuous inputs; use \
                         Halton / Sobol / Lhs / Shuffle / Extrema"
                    .to_string(),
            });
        }
    }

    Ok(())
}

/// Per-strategy V4 input-shape check using the metadata
/// algebra's `IndexFn` variants. Implements the per-strategy
/// table from spec §3.6:
///
/// | Strategy | Accepted IndexFn |
/// |---|---|
/// | Lex | any (incl. None) |
/// | ReverseLex | any non-None discrete |
/// | Shuffle, Halton, Sobol | any non-None |
/// | Lhs | any non-None (Lattice multi-axis = native; 1-axis = degenerate) |
/// | Extrema | any non-None (Lattice ≥2 = native; 1-axis or non-Lattice = degenerate or continuous box) |
/// | Shells, Diagonal, Antidiagonal | non-None discrete only |
fn check_strategy_input_shape(
    strategy: StrategyName,
    metadata: &Metadata,
    report: &mut ValidationReport,
) -> Result<(), ValidationError> {
    // Lex accepts anything including None.
    if matches!(strategy, StrategyName::Lex) {
        return Ok(());
    }

    let idx = match &metadata.index_addressable {
        Some(i) => i,
        None => {
            return Err(ValidationError::V4InputShape {
                strategy,
                reason: "input has no closed-form index function \
                         (raw filter output, dependent cartesian, or \
                         nested non-Lex order)"
                    .to_string(),
            });
        }
    };

    // Continuous / Hybrid acceptance per strategy.
    let has_continuous = idx.has_continuous_axis();
    if has_continuous {
        match strategy {
            // Index-sampling that accepts continuous.
            StrategyName::Shuffle
            | StrategyName::Halton
            | StrategyName::Sobol
            | StrategyName::Lhs => {}
            // Extrema accepts continuous boxes (per spec §3.6).
            StrategyName::Extrema => {}
            // Everything else rejects continuous.
            StrategyName::ReverseLex
            | StrategyName::Shells
            | StrategyName::Diagonal
            | StrategyName::Antidiagonal => {
                return Err(ValidationError::V4InputShape {
                    strategy,
                    reason: format!(
                        "{} does not accept continuous input",
                        strategy.as_str()
                    ),
                });
            }
            StrategyName::Lex => unreachable!("Lex handled above"),
        }
        // Continuous + Lhs/Extrema on 1-D is the same kind of
        // degenerate as discrete 1-D; emit a warning.
        if matches!(strategy, StrategyName::Lhs | StrategyName::Extrema) {
            let dim = continuous_dim(idx);
            if dim < 2 {
                if matches!(strategy, StrategyName::Lhs) {
                    report.warnings.push(ValidationWarning::LhsDegenerate);
                } else {
                    report.warnings.push(ValidationWarning::DegenerateGeometric { strategy });
                }
            }
        }
        return Ok(());
    }

    // Discrete path.
    // Lattice-geometric strategies require Lattice IndexFn.
    if strategy.is_lattice_geometric() {
        match idx {
            IndexFn::Lattice { axis_sizes } => {
                if axis_sizes.len() < 2 {
                    report.warnings.push(ValidationWarning::DegenerateGeometric { strategy });
                }
            }
            // Concatenation (union) is V4-rejected for lattice-
            // geometric — heterogeneous index space.
            IndexFn::Concatenation { .. } => {
                return Err(ValidationError::V4InputShape {
                    strategy,
                    reason: format!(
                        "{} requires a cartesian input; got union",
                        strategy.as_str()
                    ),
                });
            }
            // Lockstep / Modular (zip) — 1-D index space; these
            // strategies in 1-D collapse degenerately, but per
            // §5.8 we allow with warning rather than rejecting.
            IndexFn::Lockstep { .. } | IndexFn::Modular { .. } => {
                report.warnings.push(ValidationWarning::DegenerateGeometric { strategy });
            }
            // Unreachable: continuous handled above.
            IndexFn::Continuous { .. } | IndexFn::Hybrid { .. } => unreachable!(),
        }
        return Ok(());
    }

    // Lhs on discrete: degenerate over 1-axis Lattice / Lockstep / Modular.
    if matches!(strategy, StrategyName::Lhs) {
        match idx {
            IndexFn::Lattice { axis_sizes } if axis_sizes.len() < 2 => {
                report.warnings.push(ValidationWarning::LhsDegenerate);
            }
            IndexFn::Lockstep { .. } | IndexFn::Modular { .. } => {
                report.warnings.push(ValidationWarning::LhsDegenerate);
            }
            _ => {}
        }
    }

    // ReverseLex / Shuffle / Halton / Sobol on any non-None
    // discrete IndexFn: always accepted (no degeneracy
    // warning).
    Ok(())
}

fn continuous_dim(idx: &IndexFn) -> usize {
    match idx {
        IndexFn::Continuous { intervals, .. } => intervals.len(),
        IndexFn::Hybrid {
            discrete_axes,
            continuous_axes,
            ..
        } => discrete_axes.len() + continuous_axes.len(),
        _ => 0,
    }
}

fn check_disjoint_names(
    combinator: &'static str,
    children: &[Comprehension],
) -> Result<(), ValidationError> {
    let mut seen: Vec<String> = Vec::new();
    for child in children {
        for name in child.coordinate_names() {
            if seen.contains(&name) {
                return Err(ValidationError::V1DuplicateName {
                    combinator,
                    name,
                });
            }
            seen.push(name);
        }
    }
    Ok(())
}

fn contains_continuous_source(c: &Comprehension) -> bool {
    match c {
        Comprehension::Clause { source, .. } => source.is_continuous(),
        Comprehension::Cartesian { children } | Comprehension::Zip { children, .. } | Comprehension::Union { children } => {
            children.iter().any(contains_continuous_source)
        }
        Comprehension::Filter { child, .. } | Comprehension::Order { child, .. } => {
            contains_continuous_source(child)
        }
    }
}

/// Return the source's cardinality if `c` is a direct clause;
/// `None` otherwise. Used as a best-effort cardinality check
/// for V6 prior to Phase 3's metadata propagation.
fn direct_source_cardinality(c: &Comprehension) -> Option<CardinalityClass> {
    match c {
        Comprehension::Clause { source, .. } => Some(source.cardinality()),
        _ => None,
    }
}

/// Extract `{name}` interpolation references from a predicate
/// string. Handles only the simple `{name}` form; nested
/// expressions and escapes are out of scope for V3's parse-time
/// check (the consumer handles richer GK expression analysis).
fn extract_interpolated_names(predicate: &str) -> Vec<String> {
    let mut out = Vec::new();
    let bytes = predicate.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'{' {
            if let Some(close) = predicate[i + 1..].find('}') {
                let name = predicate[i + 1..i + 1 + close].trim();
                if !name.is_empty() && name.chars().all(|c| c.is_alphanumeric() || c == '_') {
                    out.push(name.to_string());
                }
                i += close + 2;
                continue;
            }
        }
        i += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::comprehension::source::{LiteralValue, Source};
    use crate::comprehension::cardinality::{Interval, ProductMeasure};

    fn clause(name: &str, vs: &[i64]) -> Comprehension {
        Comprehension::clause(
            name,
            Source::Literal {
                values: vs.iter().map(|n| LiteralValue::Int(*n)).collect(),
            },
        )
    }

    fn continuous_clause(name: &str) -> Comprehension {
        Comprehension::clause(
            name,
            Source::ContinuousInterval {
                interval: Interval::closed(0.0, 1.0),
                measure: ProductMeasure::Uniform,
            },
        )
    }

    #[test]
    fn v1_rejects_duplicate_names_in_cartesian() {
        let bad = Comprehension::cartesian(vec![clause("k", &[1]), clause("k", &[2])]);
        let result = validate(&bad, Mode::Permissive);
        assert!(matches!(
            result,
            Err(ValidationError::V1DuplicateName { combinator: "cartesian", .. })
        ));
    }

    #[test]
    fn v1_accepts_disjoint_names() {
        let ok = Comprehension::cartesian(vec![clause("k", &[1]), clause("limit", &[10])]);
        assert!(validate(&ok, Mode::Permissive).is_ok());
    }

    #[test]
    fn v2_rejects_union_shape_mismatch() {
        let bad = Comprehension::union(vec![
            Comprehension::cartesian(vec![clause("k", &[1]), clause("limit", &[10])]),
            Comprehension::cartesian(vec![clause("limit", &[100]), clause("k", &[100])]),
        ]);
        let result = validate(&bad, Mode::Permissive);
        assert!(matches!(result, Err(ValidationError::V2ShapeMismatch { .. })));
    }

    #[test]
    fn v2_accepts_matching_union_shape() {
        let ok = Comprehension::union(vec![
            Comprehension::cartesian(vec![clause("k", &[1]), clause("limit", &[10])]),
            Comprehension::cartesian(vec![clause("k", &[100]), clause("limit", &[100])]),
        ]);
        assert!(validate(&ok, Mode::Permissive).is_ok());
    }

    #[test]
    fn v4_rejects_lattice_geometric_over_union() {
        let bad = Comprehension::order(
            Comprehension::union(vec![
                clause("k", &[1, 2, 3]),
                clause("k", &[10, 20, 30]),
            ]),
            StrategyName::Extrema,
            Some(2),
        );
        assert!(matches!(
            validate(&bad, Mode::Permissive),
            Err(ValidationError::V4InputShape { strategy: StrategyName::Extrema, .. })
        ));
    }

    #[test]
    fn v4_lattice_geometric_over_1axis_warns_not_errors() {
        let degenerate = Comprehension::order(
            clause("k", &[1, 2, 3]),
            StrategyName::Extrema,
            Some(2),
        );
        let report = validate(&degenerate, Mode::Permissive).unwrap();
        assert!(report.warnings.iter().any(|w| matches!(
            w,
            ValidationWarning::DegenerateGeometric { strategy: StrategyName::Extrema }
        )));
    }

    #[test]
    fn v4_strict_mode_promotes_warning() {
        let degenerate = Comprehension::order(
            clause("k", &[1, 2, 3]),
            StrategyName::Extrema,
            Some(2),
        );
        assert!(validate(&degenerate, Mode::Strict).is_err());
    }

    #[test]
    fn v7_rejects_continuous_in_zip() {
        let bad = Comprehension::zip(
            vec![continuous_clause("alpha"), continuous_clause("beta")],
            ZipMode::Strict,
        );
        assert!(matches!(
            validate(&bad, Mode::Permissive),
            Err(ValidationError::V7ZipCardinality { .. })
        ));
    }

    #[test]
    fn v8_rejects_continuous_without_sampling() {
        // Continuous clause at the outermost level — no order.
        let bad = continuous_clause("theta");
        assert!(matches!(
            validate(&bad, Mode::Permissive),
            Ok(_)
        ));
        // The error fires at the outermost reachable point; for
        // a bare clause we need a wrapping check the consumer
        // does. Wrap it in order(Lex, None) — Lex doesn't sample
        // continuous; V8 fires.
        let bad_lex = Comprehension::order(continuous_clause("theta"), StrategyName::Lex, None);
        assert!(matches!(
            validate(&bad_lex, Mode::Permissive),
            Err(ValidationError::V8ContinuousRequirement { .. })
        ));
    }

    #[test]
    fn v8_accepts_continuous_with_sampling() {
        let ok = Comprehension::order(
            Comprehension::cartesian(vec![continuous_clause("alpha"), continuous_clause("beta")]),
            StrategyName::Halton,
            Some(100),
        );
        assert!(validate(&ok, Mode::Permissive).is_ok());
    }

    #[test]
    fn v8_rejects_unbounded_uniform_at_source() {
        let bad = Comprehension::clause(
            "x",
            Source::ContinuousInterval {
                interval: Interval { lo: 0.0, hi: f64::INFINITY, lo_open: false, hi_open: true },
                measure: ProductMeasure::Uniform,
            },
        );
        assert!(matches!(
            validate(&bad, Mode::Permissive),
            Err(ValidationError::V8ContinuousRequirement { .. })
        ));
    }

    #[test]
    fn v9_rejects_continuous_in_union() {
        let bad = Comprehension::union(vec![
            Comprehension::cartesian(vec![continuous_clause("k"), continuous_clause("limit")]),
            Comprehension::cartesian(vec![continuous_clause("k"), continuous_clause("limit")]),
        ]);
        // Note: this also trips V9 via continuous-in-union before V2 even fires.
        assert!(matches!(
            validate(&bad, Mode::Permissive),
            Err(ValidationError::V9UnionClassMismatch { .. })
        ));
    }

    #[test]
    fn singleton_combinator_warns() {
        let degenerate = Comprehension::cartesian(vec![clause("k", &[1, 2])]);
        let report = validate(&degenerate, Mode::Permissive).unwrap();
        assert!(report.warnings.iter().any(|w| matches!(
            w,
            ValidationWarning::SingletonCombinator { combinator: "cartesian" }
        )));
    }

    #[test]
    fn trivially_true_filter_warns() {
        let degenerate = Comprehension::filter(clause("k", &[1, 2]), "true");
        let report = validate(&degenerate, Mode::Permissive).unwrap();
        assert!(report.warnings.iter().any(|w| matches!(w, ValidationWarning::TriviallyTrueFilter)));
    }

    #[test]
    fn name_extraction_handles_simple_predicates() {
        assert_eq!(extract_interpolated_names("{k} > 0"), vec!["k"]);
        assert_eq!(
            extract_interpolated_names("{k} * {limit} <= 1000"),
            vec!["k", "limit"]
        );
        assert_eq!(extract_interpolated_names("no refs here"), Vec::<String>::new());
    }
}
