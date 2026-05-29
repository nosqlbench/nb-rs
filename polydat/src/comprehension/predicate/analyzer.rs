// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Predicate analyzer entry point — spec §10.9.2.
//!
//! Single public function: [`analyze`]. Takes a predicate
//! string + a coord set, dispatches through the §10.9.5
//! pattern recognizers, and returns a [`PredicateInfo`].
//!
//! The analyzer's correctness contract per spec §10.9.4:
//!
//! 1. **Sound.** Every assertion in the returned `PredicateInfo`
//!    is true of the predicate.
//! 2. **Conservatively incomplete.** Unrecognized patterns →
//!    `Opaque(UnknownPattern)`; missing an optimization is
//!    acceptable, false assertions are not.
//! 3. **Total.** Every well-formed boolean expression
//!    produces a `PredicateInfo`. The trivial bundle
//!    (everything `None` / `Opaque`) is the worst case but
//!    never a failure.
//! 4. **Deterministic.** Same `(predicate, coords)` always
//!    produces the same `PredicateInfo`.
//! 5. **Constant-time per node.** Single walk through the
//!    predicate text; no SMT, no fixed-point iteration.

use super::coordset::CoordSet;
use super::info::PredicateInfo;
use super::recognizers;

/// Analyze a predicate string in the context of a coordinate
/// set. Returns a structured [`PredicateInfo`] consumable by
/// the optimizer's R5 and the deferred R8 / R9 / R10 rules.
///
/// The coord set's per-coord `CoordKind` short-circuits
/// continuous-coord predicates to `Opaque(Continuous)` per
/// spec §10.9 + F20 — continuous-coord predicate analysis is
/// deliberately deferred.
pub fn analyze(predicate: &str, coords: &CoordSet) -> PredicateInfo {
    recognizers::recognize(predicate, coords)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::comprehension::predicate::info::{
        Determinism, Factorization, OpaqueReason,
    };

    #[test]
    fn empty_predicate_is_unknown() {
        let info = analyze("", &CoordSet::all_discrete::<[&str; 0], &str>([]));
        assert!(matches!(
            info.factorization,
            Factorization::Opaque(OpaqueReason::UnknownPattern)
        ));
    }

    #[test]
    fn deterministic_run_twice() {
        let coords = CoordSet::all_discrete(["k", "limit"]);
        let a = analyze("{k} > 0 && {limit} <= 100", &coords);
        let b = analyze("{k} > 0 && {limit} <= 100", &coords);
        assert_eq!(a, b);
    }

    #[test]
    fn integration_per_axis_via_analyze_entry() {
        let coords = CoordSet::all_discrete(["k"]);
        let info = analyze("{k} >= 5", &coords);
        assert!(matches!(info.factorization, Factorization::PerAxis(_)));
        assert_eq!(info.determinism, Determinism::Deterministic);
    }
}
