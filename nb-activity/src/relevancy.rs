// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Information retrieval relevancy metrics (SRD 47).
//!
//! Pure functions for computing recall@k, precision@k, F1@k,
//! reciprocal rank, and average precision. All operate on sorted,
//! deduplicated `i64` slices representing ground truth (relevant)
//! and actual result indices.

use std::collections::HashSet;

/// Count elements present in both sorted slices.
///
/// Two-pointer O(n+m) scan — no allocation. Both slices must be
/// sorted in ascending order with no duplicates.
pub fn intersection_count(a: &[i64], b: &[i64]) -> usize {
    let (mut i, mut j, mut count) = (0, 0, 0usize);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                count += 1;
                i += 1;
                j += 1;
            }
        }
    }
    count
}

/// Recall@k: fraction of the top-k ground truth items found in the results.
///
/// Both `relevant` and `actual` should be pre-truncated to at most k
/// elements and sorted. Returns `|relevant ∩ actual| / k`.
pub fn recall(relevant: &[i64], actual: &[i64], k: usize) -> f64 {
    if k == 0 {
        return 0.0;
    }
    intersection_count(relevant, actual) as f64 / k as f64
}

/// Precision@k: fraction of the returned results that are relevant.
///
/// Returns `|relevant ∩ actual| / |actual|`.
pub fn precision(relevant: &[i64], actual: &[i64]) -> f64 {
    if actual.is_empty() {
        return 0.0;
    }
    intersection_count(relevant, actual) as f64 / actual.len() as f64
}

/// F1@k: harmonic mean of recall and precision.
///
/// `F1 = 2 · (recall · precision) / (recall + precision)`
pub fn f1(relevant: &[i64], actual: &[i64], k: usize) -> f64 {
    let r = recall(relevant, actual, k);
    let p = precision(relevant, actual);
    if r + p == 0.0 {
        return 0.0;
    }
    2.0 * r * p / (r + p)
}

/// Reciprocal rank: `1 / (position of first relevant result + 1)`.
///
/// `actual` must be in result order (not sorted). Scans linearly and
/// returns `1/(i+1)` for the first element found in `relevant`.
/// Returns 0.0 if no relevant item appears in `actual`.
pub fn reciprocal_rank(relevant: &[i64], actual: &[i64]) -> f64 {
    let relevant_set: HashSet<i64> = relevant.iter().copied().collect();
    for (i, &item) in actual.iter().enumerate() {
        if relevant_set.contains(&item) {
            return 1.0 / (i as f64 + 1.0);
        }
    }
    0.0
}

/// Average precision: mean precision at each relevant position.
///
/// `actual` must be in result order (not sorted). For each position i
/// where `actual[i]` is in `relevant`:
///   `precision_at_i = (relevant items seen so far) / (i + 1)`
///
/// AP = sum of all `precision_at_i` / `|relevant|`.
pub fn average_precision(relevant: &[i64], actual: &[i64]) -> f64 {
    if relevant.is_empty() {
        return 0.0;
    }
    let relevant_set: HashSet<i64> = relevant.iter().copied().collect();
    let mut hits = 0u64;
    let mut sum = 0.0f64;
    for (i, &item) in actual.iter().enumerate() {
        if relevant_set.contains(&item) {
            hits += 1;
            sum += hits as f64 / (i as f64 + 1.0);
        }
    }
    if hits == 0 {
        0.0
    } else {
        sum / relevant.len() as f64
    }
}

/// Truncate a slice to at most `k` elements and return a sorted copy.
pub fn truncate_and_sort(items: &[i64], k: usize) -> Vec<i64> {
    let end = items.len().min(k);
    let mut v = items[..end].to_vec();
    v.sort_unstable();
    v
}

/// Which relevancy function to compute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelevancyFn {
    Recall,
    Precision,
    F1,
    ReciprocalRank,
    AveragePrecision,
}

impl RelevancyFn {
    /// Parse from a string name (case-insensitive).
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "recall" => Some(Self::Recall),
            "precision" => Some(Self::Precision),
            "f1" => Some(Self::F1),
            "reciprocal_rank" | "reciprocalrank" | "mrr" => Some(Self::ReciprocalRank),
            "average_precision" | "averageprecision" | "ap" | "map" => {
                Some(Self::AveragePrecision)
            }
            _ => None,
        }
    }

    /// Metric name for display and histogram keys.
    pub fn metric_name(&self) -> &'static str {
        match self {
            Self::Recall => "recall",
            Self::Precision => "precision",
            Self::F1 => "f1",
            Self::ReciprocalRank => "reciprocal_rank",
            Self::AveragePrecision => "average_precision",
        }
    }

    /// Compute this function's score.
    ///
    /// For recall, precision, and F1: `relevant` and `actual` should be
    /// sorted slices (pre-truncated to k). For reciprocal_rank and
    /// average_precision: `actual_ordered` is the original result order.
    pub fn compute(
        &self,
        relevant_sorted: &[i64],
        actual_sorted: &[i64],
        actual_ordered: &[i64],
        k: usize,
    ) -> f64 {
        match self {
            Self::Recall => recall(relevant_sorted, actual_sorted, k),
            Self::Precision => precision(relevant_sorted, actual_sorted),
            Self::F1 => f1(relevant_sorted, actual_sorted, k),
            Self::ReciprocalRank => reciprocal_rank(relevant_sorted, actual_ordered),
            Self::AveragePrecision => average_precision(relevant_sorted, actual_ordered),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intersection_count_basic() {
        assert_eq!(intersection_count(&[1, 3, 5, 7], &[2, 3, 5, 8]), 2);
        assert_eq!(intersection_count(&[1, 2, 3], &[1, 2, 3]), 3);
        assert_eq!(intersection_count(&[1, 2], &[3, 4]), 0);
        assert_eq!(intersection_count(&[], &[1, 2, 3]), 0);
        assert_eq!(intersection_count(&[1], &[]), 0);
    }

    #[test]
    fn recall_perfect() {
        // All 5 relevant items found in actual
        let relevant = vec![1, 2, 3, 4, 5];
        let actual = vec![1, 2, 3, 4, 5];
        assert!((recall(&relevant, &actual, 5) - 1.0).abs() < 1e-10);
    }

    #[test]
    fn recall_partial() {
        // 3 of 5 relevant items found
        let relevant = vec![1, 2, 3, 4, 5];
        let actual = vec![1, 3, 5, 6, 7];
        assert!((recall(&relevant, &actual, 5) - 0.6).abs() < 1e-10);
    }

    #[test]
    fn recall_zero() {
        let relevant = vec![1, 2, 3];
        let actual = vec![4, 5, 6];
        assert!((recall(&relevant, &actual, 3)).abs() < 1e-10);
    }

    #[test]
    fn recall_k_zero() {
        assert_eq!(recall(&[1, 2], &[1, 2], 0), 0.0);
    }

    #[test]
    fn precision_perfect() {
        let relevant = vec![1, 2, 3, 4, 5];
        let actual = vec![1, 2, 3, 4, 5];
        assert!((precision(&relevant, &actual) - 1.0).abs() < 1e-10);
    }

    #[test]
    fn precision_half() {
        // 2 of 4 actual items are relevant
        let relevant = vec![1, 2, 3, 4, 5];
        let actual = vec![1, 3, 6, 7];
        assert!((precision(&relevant, &actual) - 0.5).abs() < 1e-10);
    }

    #[test]
    fn precision_empty_actual() {
        assert_eq!(precision(&[1, 2, 3], &[]), 0.0);
    }

    #[test]
    fn f1_perfect() {
        let relevant = vec![1, 2, 3];
        let actual = vec![1, 2, 3];
        assert!((f1(&relevant, &actual, 3) - 1.0).abs() < 1e-10);
    }

    #[test]
    fn f1_zero_when_no_overlap() {
        let relevant = vec![1, 2, 3];
        let actual = vec![4, 5, 6];
        assert!((f1(&relevant, &actual, 3)).abs() < 1e-10);
    }

    #[test]
    fn f1_known_value() {
        // recall = 2/5 = 0.4, precision = 2/3 = 0.667
        // f1 = 2 * 0.4 * 0.667 / (0.4 + 0.667) = 0.5
        let relevant = vec![1, 2, 3, 4, 5];
        let actual = vec![1, 3, 6];
        let score = f1(&relevant, &actual, 5);
        assert!((score - 0.5).abs() < 0.01, "f1={score}");
    }

    #[test]
    fn reciprocal_rank_first_position() {
        // First result is relevant → RR = 1.0
        let relevant = vec![1, 2, 3];
        let actual = vec![1, 5, 6, 7];
        assert!((reciprocal_rank(&relevant, &actual) - 1.0).abs() < 1e-10);
    }

    #[test]
    fn reciprocal_rank_third_position() {
        // First relevant at position 2 (0-indexed) → RR = 1/3
        let relevant = vec![1, 2, 3];
        let actual = vec![5, 6, 2, 7];
        assert!((reciprocal_rank(&relevant, &actual) - 1.0 / 3.0).abs() < 1e-10);
    }

    #[test]
    fn reciprocal_rank_no_match() {
        let relevant = vec![1, 2, 3];
        let actual = vec![4, 5, 6];
        assert_eq!(reciprocal_rank(&relevant, &actual), 0.0);
    }

    #[test]
    fn average_precision_perfect_order() {
        // All relevant, in order: AP@3 = (1/1 + 2/2 + 3/3) / 3 = 1.0
        let relevant = vec![1, 2, 3];
        let actual = vec![1, 2, 3];
        assert!((average_precision(&relevant, &actual) - 1.0).abs() < 1e-10);
    }

    #[test]
    fn average_precision_interleaved() {
        // relevant = [1,2,3], actual = [1, 4, 2, 5, 3]
        // pos 0: hit(1), prec=1/1=1.0
        // pos 1: miss
        // pos 2: hit(2), prec=2/3=0.667
        // pos 3: miss
        // pos 4: hit(3), prec=3/5=0.6
        // AP = (1.0 + 0.667 + 0.6) / 3 = 0.756
        let relevant = vec![1, 2, 3];
        let actual = vec![1, 4, 2, 5, 3];
        let ap = average_precision(&relevant, &actual);
        assert!((ap - 0.756).abs() < 0.01, "ap={ap}");
    }

    #[test]
    fn average_precision_no_hits() {
        let relevant = vec![1, 2, 3];
        let actual = vec![4, 5, 6];
        assert_eq!(average_precision(&relevant, &actual), 0.0);
    }

    #[test]
    fn average_precision_empty_relevant() {
        assert_eq!(average_precision(&[], &[1, 2, 3]), 0.0);
    }

    #[test]
    fn truncate_and_sort_basic() {
        let items = vec![5, 3, 1, 4, 2, 10, 8];
        let result = truncate_and_sort(&items, 4);
        assert_eq!(result, vec![1, 3, 4, 5]);
    }

    #[test]
    fn truncate_and_sort_k_larger_than_len() {
        let items = vec![3, 1, 2];
        let result = truncate_and_sort(&items, 100);
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[test]
    fn relevancy_fn_parse() {
        assert_eq!(RelevancyFn::parse("recall"), Some(RelevancyFn::Recall));
        assert_eq!(RelevancyFn::parse("PRECISION"), Some(RelevancyFn::Precision));
        assert_eq!(RelevancyFn::parse("f1"), Some(RelevancyFn::F1));
        assert_eq!(RelevancyFn::parse("reciprocal_rank"), Some(RelevancyFn::ReciprocalRank));
        assert_eq!(RelevancyFn::parse("mrr"), Some(RelevancyFn::ReciprocalRank));
        assert_eq!(RelevancyFn::parse("average_precision"), Some(RelevancyFn::AveragePrecision));
        assert_eq!(RelevancyFn::parse("ap"), Some(RelevancyFn::AveragePrecision));
        assert_eq!(RelevancyFn::parse("map"), Some(RelevancyFn::AveragePrecision));
        assert_eq!(RelevancyFn::parse("unknown"), None);
    }

    #[test]
    fn relevancy_fn_compute_dispatch() {
        let relevant = vec![1, 2, 3, 4, 5];
        let actual_sorted = vec![1, 2, 3, 6, 7];
        let actual_ordered = vec![1, 6, 2, 7, 3];

        let r = RelevancyFn::Recall.compute(&relevant, &actual_sorted, &actual_ordered, 5);
        assert!((r - 0.6).abs() < 1e-10);

        let p = RelevancyFn::Precision.compute(&relevant, &actual_sorted, &actual_ordered, 5);
        assert!((p - 0.6).abs() < 1e-10);

        let rr = RelevancyFn::ReciprocalRank.compute(&relevant, &actual_sorted, &actual_ordered, 5);
        assert!((rr - 1.0).abs() < 1e-10); // first item in actual_ordered is relevant
    }
}
