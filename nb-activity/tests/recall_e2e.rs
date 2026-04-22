// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! End-to-end validation tests for the recall@k relevancy pipeline
//! (SRD 33). Drives `ValidatingDispenser` with mock adapter results
//! shaped like a CQL `SELECT key FROM ... ANN OF ...` response and
//! verifies that non-zero recall is produced when the returned keys
//! match a ground-truth binding.
//!
//! Motivated by a bug report where `recall@10 = 0.0000` was produced
//! for every query in the CQL vector-search workload despite the
//! queries returning the correct rows. These tests pin the behavior
//! of the end-to-end validation pipeline so regressions surface here
//! rather than in a full CQL run.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use nb_activity::adapter::{
    ExecutionError, OpDispenser, OpResult, ResolvedFields, ResultBody,
};
use nb_activity::validation::ValidatingDispenser;
use nb_metrics::labels::Labels;

/// Result body shaped like `CqlResultBody.to_json()` — a JSON array of
/// row objects, each with a `"key"` column holding the decimal-text
/// representation of the row index (matching what `format_u64(row, 10)`
/// produces in the workload's rampup phase).
#[derive(Debug)]
struct RowsBody {
    rows: Vec<HashMap<String, serde_json::Value>>,
}

impl ResultBody for RowsBody {
    fn to_json(&self) -> serde_json::Value {
        serde_json::Value::Array(
            self.rows.iter()
                .map(|row| serde_json::Value::Object(
                    row.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
                ))
                .collect()
        )
    }
    fn as_any(&self) -> &dyn Any { self }
    fn element_count(&self) -> u64 { self.rows.len() as u64 }
}

/// Build a result body with `keys` as string-valued `key` columns.
fn rows_body(keys: &[i64]) -> Box<dyn ResultBody> {
    let rows = keys.iter().map(|k| {
        let mut row = HashMap::new();
        row.insert("key".into(), serde_json::Value::String(k.to_string()));
        row
    }).collect();
    Box::new(RowsBody { rows })
}

/// Fake inner dispenser: returns a fixed, pre-built body.
struct FixedBodyDispenser {
    keys: Vec<i64>,
}

impl OpDispenser for FixedBodyDispenser {
    fn execute<'a>(
        &'a self,
        _cycle: u64,
        _fields: &'a ResolvedFields,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        let body = rows_body(&self.keys);
        Box::pin(async move {
            Ok(OpResult {
                body: Some(body),
                captures: HashMap::new(),
                skipped: false,
            })
        })
    }
}

/// Build a ParsedOp that carries a `relevancy:` block.
fn make_template_with_relevancy(k: u64) -> nb_workload::model::ParsedOp {
    let mut template = nb_workload::model::ParsedOp::simple(
        "select_ann",
        "SELECT key FROM t ORDER BY value ANN OF :v LIMIT :k",
    );
    template.params.insert("relevancy".into(), serde_json::json!({
        "actual": "key",
        "expected": "{ground_truth}",
        "k": k,
        "functions": ["recall"],
    }));
    template
}

/// Snapshot the mean of a named relevancy stat.
fn snapshot_mean(vm: &Arc<nb_activity::validation::ValidationMetrics>, name: &str) -> (usize, f64) {
    let stats = vm.relevancy_stats.get(name)
        .unwrap_or_else(|| panic!("missing relevancy stat '{name}'"));
    let snap = stats.snapshot();
    (snap.len(), snap.mean())
}

#[tokio::test]
async fn perfect_recall_when_keys_match_ground_truth() {
    // Given: a query that returns exactly the k ground-truth neighbors
    // (in any order — both are sorted before intersection).
    let ground_truth = [4_i64, 17, 42, 99, 123, 256, 512, 777, 1001, 2048];
    let returned_keys = ground_truth.to_vec();

    let inner: Arc<dyn OpDispenser> = Arc::new(FixedBodyDispenser {
        keys: returned_keys,
    });
    let template = make_template_with_relevancy(10);
    let labels = Labels::of("session", "test");
    let (validated, vm) = ValidatingDispenser::wrap(inner, &template, &labels, None);
    let vm = vm.expect("relevancy declared — metrics should be created");

    // Ground truth as a string matching what `neighbor_indices_at` produces.
    let gt_string = format!("[{}]",
        ground_truth.iter().map(i64::to_string).collect::<Vec<_>>().join(","));
    let fields = ResolvedFields::new(
        vec!["ground_truth".into()],
        vec![nb_variates::node::Value::Str(gt_string)],
    );

    validated.execute(0, &fields).await.expect("validation should not error");

    let (count, mean) = snapshot_mean(&vm, "recall@10");
    assert_eq!(count, 1, "one recall score should be recorded");
    assert!((mean - 1.0).abs() < 1e-9,
        "perfect-recall scenario should produce recall=1.0, got {mean}");
}

#[tokio::test]
async fn partial_recall_when_half_keys_match() {
    // 5 of 10 returned keys are in the ground truth → recall@10 = 0.5
    let ground_truth = [0_i64, 1, 2, 3, 4, 5, 6, 7, 8, 9];
    let returned_keys = vec![0_i64, 1, 2, 3, 4, 100, 200, 300, 400, 500];

    let inner: Arc<dyn OpDispenser> = Arc::new(FixedBodyDispenser {
        keys: returned_keys,
    });
    let template = make_template_with_relevancy(10);
    let labels = Labels::of("session", "test");
    let (validated, vm) = ValidatingDispenser::wrap(inner, &template, &labels, None);
    let vm = vm.expect("relevancy declared — metrics should be created");

    let gt_string = format!("[{}]",
        ground_truth.iter().map(i64::to_string).collect::<Vec<_>>().join(","));
    let fields = ResolvedFields::new(
        vec!["ground_truth".into()],
        vec![nb_variates::node::Value::Str(gt_string)],
    );

    validated.execute(0, &fields).await.expect("validation should not error");

    let (count, mean) = snapshot_mean(&vm, "recall@10");
    assert_eq!(count, 1);
    assert!((mean - 0.5).abs() < 1e-9,
        "half-match scenario should produce recall=0.5, got {mean}");
}

#[tokio::test]
async fn recall_across_multiple_cycles_averages_per_cycle() {
    // Three cycles: recall values 1.0, 0.5, 0.0 → mean = 0.5
    let gt = [1_i64, 2, 3, 4];
    let templates = [
        vec![1_i64, 2, 3, 4],        // recall = 4/4 = 1.0
        vec![1_i64, 2, 10, 20],      // recall = 2/4 = 0.5
        vec![100_i64, 200, 300, 400],// recall = 0/4 = 0.0
    ];

    let template = make_template_with_relevancy(4);
    let labels = Labels::of("session", "test");
    let gt_string = format!("[{}]",
        gt.iter().map(i64::to_string).collect::<Vec<_>>().join(","));
    let fields = ResolvedFields::new(
        vec!["ground_truth".into()],
        vec![nb_variates::node::Value::Str(gt_string)],
    );

    // Wrap once, execute three times with different inner bodies.
    // (To vary the inner result per cycle we rewrap each iteration — the
    // ValidationMetrics is owned per wrap, so aggregate manually across
    // runs by sharing the same ValidatingDispenser.)
    let multi_inner: Arc<dyn OpDispenser> = Arc::new(MultiBodyDispenser {
        per_cycle_keys: templates.iter().map(|v| v.clone()).collect(),
    });
    let (validated, vm) = ValidatingDispenser::wrap(multi_inner, &template, &labels, None);
    let vm = vm.expect("relevancy declared — metrics should be created");

    for cycle in 0u64..3 {
        validated.execute(cycle, &fields).await.expect("ok");
    }

    let (count, mean) = snapshot_mean(&vm, "recall@4");
    assert_eq!(count, 3, "three cycles → three recorded scores");
    assert!((mean - 0.5).abs() < 1e-9,
        "average of (1.0, 0.5, 0.0) should be 0.5, got {mean}");
}

/// Returns a different body per cycle based on the `cycle` index.
struct MultiBodyDispenser {
    per_cycle_keys: Vec<Vec<i64>>,
}

impl OpDispenser for MultiBodyDispenser {
    fn execute<'a>(
        &'a self,
        cycle: u64,
        _fields: &'a ResolvedFields,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        let keys = self.per_cycle_keys[cycle as usize].clone();
        Box::pin(async move {
            Ok(OpResult {
                body: Some(rows_body(&keys)),
                captures: HashMap::new(),
                skipped: false,
            })
        })
    }
}

#[tokio::test]
async fn zero_recall_when_column_name_does_not_match() {
    // Regression guard for a failure mode observed in the CQL workload:
    // the adapter's `CqlResultBody::from_cass_result` falls back to `"?"`
    // when `column_name(idx)` errors, so rows come back as `{"?": "42"}`
    // instead of `{"key": "42"}`. The relevancy config asks for `actual:
    // key`, so no indices extract, intersection is empty, recall=0 on
    // every cycle — matching the pathological output.md pattern.
    //
    // This test pins the failure mode so if the CQL column-naming bug is
    // ever fixed upstream, the test here documents why recall would
    // otherwise come back as 0.0 forever.
    #[derive(Debug)]
    struct WrongColumnBody {
        values: Vec<i64>,
    }
    impl ResultBody for WrongColumnBody {
        fn to_json(&self) -> serde_json::Value {
            serde_json::Value::Array(
                self.values.iter().map(|v| {
                    let mut row = serde_json::Map::new();
                    row.insert("?".into(), serde_json::Value::String(v.to_string()));
                    serde_json::Value::Object(row)
                }).collect()
            )
        }
        fn as_any(&self) -> &dyn Any { self }
        fn element_count(&self) -> u64 { self.values.len() as u64 }
    }
    struct WrongColumnDispenser;
    impl OpDispenser for WrongColumnDispenser {
        fn execute<'a>(
            &'a self,
            _cycle: u64,
            _fields: &'a ResolvedFields,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
            Box::pin(async move {
                Ok(OpResult {
                    body: Some(Box::new(WrongColumnBody { values: vec![1, 2, 3, 4] })),
                    captures: HashMap::new(),
                    skipped: false,
                })
            })
        }
    }

    let inner: Arc<dyn OpDispenser> = Arc::new(WrongColumnDispenser);
    let template = make_template_with_relevancy(4);
    let labels = Labels::of("session", "test");
    let (validated, vm) = ValidatingDispenser::wrap(inner, &template, &labels, None);
    let vm = vm.expect("relevancy declared");

    let fields = ResolvedFields::new(
        vec!["ground_truth".into()],
        vec![nb_variates::node::Value::Str("[1,2,3,4]".into())],
    );

    validated.execute(0, &fields).await.expect("ok");

    let (_, mean) = snapshot_mean(&vm, "recall@4");
    assert_eq!(mean, 0.0,
        "mismatched column name must produce recall=0 (reproduces the bug)");
}

#[tokio::test]
async fn zero_recall_when_body_is_empty() {
    // A query that returns no rows (body=None): recall must be 0.0
    // and the pipeline should not crash or emit misleading scores.
    struct NoBodyDispenser;
    impl OpDispenser for NoBodyDispenser {
        fn execute<'a>(
            &'a self,
            _cycle: u64,
            _fields: &'a ResolvedFields,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
            Box::pin(async move {
                Ok(OpResult {
                    body: None,
                    captures: HashMap::new(),
                    skipped: false,
                })
            })
        }
    }

    let inner: Arc<dyn OpDispenser> = Arc::new(NoBodyDispenser);
    let template = make_template_with_relevancy(10);
    let labels = Labels::of("session", "test");
    let (validated, vm) = ValidatingDispenser::wrap(inner, &template, &labels, None);
    let vm = vm.expect("relevancy declared");

    let fields = ResolvedFields::new(
        vec!["ground_truth".into()],
        vec![nb_variates::node::Value::Str("[1,2,3]".into())],
    );

    validated.execute(0, &fields).await.expect("ok");

    let (count, mean) = snapshot_mean(&vm, "recall@10");
    assert_eq!(count, 1, "empty-body should still record a score");
    assert_eq!(mean, 0.0, "empty body → recall=0.0");
}
