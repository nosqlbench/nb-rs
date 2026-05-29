// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Strategy taxonomy and zip modes — spec §3.6 + §3.3.
//!
//! `StrategyName` is a closed enum per spec §10.7.5: adding a
//! new strategy is a coordinated type extension (parser
//! keyword, §3.6 table row, §10.2 R2 push-down rule). No
//! user-defined `Custom` callback escape hatch — the spec
//! removed it in favor of named strategies whose closed-form
//! semantics the optimizer can analyze.

use serde::{Deserialize, Serialize};

/// Named ordering strategies per spec §3.6 (plus `Shuffle`).
///
/// Each strategy declares its accepted input `IndexFn` shape
/// per the spec §3.6 strategy table. V4 (§5) enforces the
/// per-strategy input-shape contract; R2 (§10.2) implements
/// the per-strategy push-down rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StrategyName {
    /// Natural enumeration order — pass-through. Accepts any
    /// input including `None`. The only strategy whose
    /// materialization is `Streaming` (§10.2 R1).
    Lex,

    /// Reverse the input's index range. Accepts any non-`None`
    /// discrete IndexFn; rejected over continuous.
    ReverseLex,

    /// Random permutation. PRNG seed captured at
    /// materialization. Accepts any non-`None` IndexFn
    /// including continuous.
    Shuffle,

    /// Halton low-discrepancy sequence. K-D over Lattice;
    /// 1-D over single-axis index spaces; native to
    /// Continuous (canonical use case).
    Halton,

    /// Sobol low-discrepancy sequence. Same shape as Halton.
    Sobol,

    /// Latin Hypercube stratified sampling. K-D over Lattice;
    /// degenerate (= Shuffle) over 1-D; native to Continuous.
    Lhs,

    /// K-D lattice corners (2^N). Sorted by distance metric;
    /// emit top k. Discrete Lattice with N≥2 axes or
    /// Continuous box; degenerate over 1-D.
    Extrema,

    /// Concentric shell partitioning. Discrete only; rejected
    /// over continuous (ill-defined without discretization).
    Shells,

    /// Index-sum-ascending walk. Discrete Lattice with N≥2
    /// axes; rejected over continuous.
    Diagonal,

    /// Index-sum-descending walk. Discrete Lattice with N≥2
    /// axes; rejected over continuous.
    Antidiagonal,
}

impl StrategyName {
    /// `true` if the strategy is `Lex` — the only streaming
    /// strategy (§3.6, §6.2, §10.2 R1).
    pub fn is_streaming(self) -> bool {
        matches!(self, StrategyName::Lex)
    }

    /// `true` if the strategy operates over a 1-D index space
    /// without geometric interpretation. Index-sampling
    /// strategies (Halton/Sobol/Lhs/Shuffle/ReverseLex) work
    /// over any non-`None` IndexFn; lattice-geometric
    /// strategies (Extrema/Shells/Diagonal/Antidiagonal)
    /// require Lattice with ≥2 axes.
    pub fn is_index_sampling(self) -> bool {
        matches!(
            self,
            StrategyName::Halton
                | StrategyName::Sobol
                | StrategyName::Lhs
                | StrategyName::Shuffle
                | StrategyName::ReverseLex
        )
    }

    /// `true` if the strategy is a lattice-geometric measure
    /// (corners, shells, diagonals). These require a multi-axis
    /// Lattice and reject continuous inputs.
    pub fn is_lattice_geometric(self) -> bool {
        matches!(
            self,
            StrategyName::Extrema
                | StrategyName::Shells
                | StrategyName::Diagonal
                | StrategyName::Antidiagonal
        )
    }

    /// Human-readable strategy name used in surface syntax
    /// (e.g., `order halton/n`).
    pub fn as_str(self) -> &'static str {
        match self {
            StrategyName::Lex => "lex",
            StrategyName::ReverseLex => "reverse_lex",
            StrategyName::Shuffle => "shuffle",
            StrategyName::Halton => "halton",
            StrategyName::Sobol => "sobol",
            StrategyName::Lhs => "lhs",
            StrategyName::Extrema => "extrema",
            StrategyName::Shells => "shells",
            StrategyName::Diagonal => "diagonal",
            StrategyName::Antidiagonal => "antidiagonal",
        }
    }
}

impl std::fmt::Display for StrategyName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Zip combination mode per spec §3.3.
///
/// - `Strict` errors on length mismatch (V7).
/// - `Truncate` cuts to the shortest child.
/// - `Cycle` repeats shorter children to the longest's length;
///   permits one Unbounded child (the longest) with the others
///   Bounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ZipMode {
    Strict,
    Truncate,
    Cycle,
}

impl ZipMode {
    pub fn as_str(self) -> &'static str {
        match self {
            ZipMode::Strict => "strict",
            ZipMode::Truncate => "truncate",
            ZipMode::Cycle => "cycle",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lex_is_the_only_streaming_strategy() {
        assert!(StrategyName::Lex.is_streaming());
        for s in [
            StrategyName::ReverseLex,
            StrategyName::Shuffle,
            StrategyName::Halton,
            StrategyName::Sobol,
            StrategyName::Lhs,
            StrategyName::Extrema,
            StrategyName::Shells,
            StrategyName::Diagonal,
            StrategyName::Antidiagonal,
        ] {
            assert!(!s.is_streaming(), "{s:?} should not be streaming");
        }
    }

    #[test]
    fn index_sampling_strategies_classified_correctly() {
        for s in [
            StrategyName::Halton,
            StrategyName::Sobol,
            StrategyName::Lhs,
            StrategyName::Shuffle,
            StrategyName::ReverseLex,
        ] {
            assert!(s.is_index_sampling(), "{s:?} should be index-sampling");
        }
    }

    #[test]
    fn lattice_geometric_strategies_classified_correctly() {
        for s in [
            StrategyName::Extrema,
            StrategyName::Shells,
            StrategyName::Diagonal,
            StrategyName::Antidiagonal,
        ] {
            assert!(s.is_lattice_geometric(), "{s:?} should be lattice-geometric");
        }
    }

    #[test]
    fn strategy_classes_are_disjoint() {
        // Lex is neither index-sampling nor lattice-geometric;
        // every other strategy is in exactly one of the two
        // classes.
        for s in [
            StrategyName::ReverseLex,
            StrategyName::Shuffle,
            StrategyName::Halton,
            StrategyName::Sobol,
            StrategyName::Lhs,
            StrategyName::Extrema,
            StrategyName::Shells,
            StrategyName::Diagonal,
            StrategyName::Antidiagonal,
        ] {
            let sampling = s.is_index_sampling();
            let geometric = s.is_lattice_geometric();
            assert!(sampling ^ geometric, "{s:?} must be in exactly one class");
        }
    }

    #[test]
    fn strategy_string_round_trip() {
        let s = StrategyName::Halton;
        let json = serde_json::to_string(&s).unwrap();
        let back: StrategyName = serde_json::from_str(&json).unwrap();
        assert_eq!(s, back);
        assert_eq!(s.as_str(), "halton");
    }
}
