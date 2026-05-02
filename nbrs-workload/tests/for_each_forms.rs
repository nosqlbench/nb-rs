// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Coverage for the six syntactic shapes of `for_each` and the
//! detection rule that picks between Cartesian (single or
//! multi-clause) and Union comprehension modes.
//!
//! See [`nbrs_variates::comprehension::Comprehension`] and
//! `comprehension_from_subspaces` for the detection rule:
//! any repeated var name across sub-spaces ⇒ Union; otherwise
//! Cartesian over the flattened clause list.

use std::collections::HashMap;
use nbrs_variates::comprehension::ComprehensionMode;
use nbrs_workload::model::ScenarioNode;
use nbrs_workload::parse::parse_workload;

fn first_scenario_node(yaml: &str) -> ScenarioNode {
    let wl = parse_workload(yaml, &HashMap::new())
        .unwrap_or_else(|e| panic!("parse failed: {e}\n--- yaml ---\n{yaml}"));
    let nodes = wl.scenarios.values().next().expect("at least one scenario");
    nodes.first().cloned().expect("at least one scenario node")
}

const PHASES: &str = r#"
phases:
  p:
    ops:
      step:
        stmt: "noop"
"#;

#[test]
fn string_single_clause_is_single_clause_cartesian() {
    let yaml = format!(r#"
scenarios:
  s:
    - for_each: "x in 1,2,3"
      phases: [p]
{PHASES}
"#);
    let node = first_scenario_node(&yaml);
    let ScenarioNode::Comprehension { comprehension, .. } = node else {
        panic!("expected Comprehension");
    };
    let ComprehensionMode::Cartesian(clauses) = &comprehension.mode else {
        panic!("expected Cartesian, got {:?}", comprehension.mode);
    };
    assert_eq!(clauses.len(), 1);
    assert_eq!(clauses[0].var, "x");
    assert_eq!(clauses[0].expr, "1,2,3");
}

#[test]
fn string_multi_clause_distinct_vars_is_multi_clause_cartesian() {
    let yaml = format!(r#"
scenarios:
  s:
    - for_each: "x in 1,2, y in a,b"
      phases: [p]
{PHASES}
"#);
    let node = first_scenario_node(&yaml);
    let ScenarioNode::Comprehension { comprehension, .. } = node else {
        panic!("expected Comprehension");
    };
    let ComprehensionMode::Cartesian(clauses) = &comprehension.mode else {
        panic!("expected Cartesian, got {:?}", comprehension.mode);
    };
    assert_eq!(clauses.len(), 2);
    assert_eq!(clauses[0].var, "x");
    assert_eq!(clauses[1].var, "y");
}

#[test]
fn string_multi_clause_repeated_var_is_union() {
    // Same var name twice in a single string ⇒ Union (each
    // clause is its own sub-space).
    let yaml = format!(r#"
scenarios:
  s:
    - for_each: "x in 1, x in 2"
      phases: [p]
{PHASES}
"#);
    let node = first_scenario_node(&yaml);
    let ScenarioNode::Comprehension { comprehension, .. } = node else {
        panic!("expected Comprehension");
    };
    let ComprehensionMode::Union(subspaces) = &comprehension.mode else {
        panic!("expected Union, got {:?}", comprehension.mode);
    };
    assert_eq!(subspaces.len(), 2);
    assert_eq!(subspaces[0][0].var, "x");
    assert_eq!(subspaces[0][0].expr, "1");
    assert_eq!(subspaces[1][0].var, "x");
    assert_eq!(subspaces[1][0].expr, "2");
}

#[test]
fn array_single_clause_distinct_vars_is_multi_clause_cartesian() {
    let yaml = format!(r#"
scenarios:
  s:
    - for_each:
        - "x in 1,2"
        - "y in a,b"
      phases: [p]
{PHASES}
"#);
    let node = first_scenario_node(&yaml);
    let ScenarioNode::Comprehension { comprehension, .. } = node else {
        panic!("expected Comprehension");
    };
    let ComprehensionMode::Cartesian(clauses) = &comprehension.mode else {
        panic!("expected Cartesian, got {:?}", comprehension.mode);
    };
    assert_eq!(clauses.len(), 2);
    assert_eq!(clauses[0].var, "x");
    assert_eq!(clauses[1].var, "y");
}

#[test]
fn array_single_clause_repeated_var_is_union() {
    // Two array entries each with one clause, same var name ⇒
    // Union of single-var sub-spaces.
    let yaml = format!(r#"
scenarios:
  s:
    - for_each:
        - "x in 1"
        - "x in 2"
      phases: [p]
{PHASES}
"#);
    let node = first_scenario_node(&yaml);
    let ScenarioNode::Comprehension { comprehension, .. } = node else {
        panic!("expected Comprehension");
    };
    let ComprehensionMode::Union(subspaces) = &comprehension.mode else {
        panic!("expected Union, got {:?}", comprehension.mode);
    };
    assert_eq!(subspaces.len(), 2);
    assert_eq!(subspaces[0][0].expr, "1");
    assert_eq!(subspaces[1][0].expr, "2");
}

#[test]
fn array_multi_clause_repeated_vars_is_union() {
    // The motivating union shape: two array entries, each is a
    // multi-dim sub-space; vars repeat across entries.
    let yaml = format!(r#"
scenarios:
  s:
    - for_each:
        - "k in 10, limit in 10,20,30"
        - "k in 100, limit in 100,200,300"
      phases: [p]
{PHASES}
"#);
    let node = first_scenario_node(&yaml);
    let ScenarioNode::Comprehension { comprehension, .. } = node else {
        panic!("expected Comprehension");
    };
    let ComprehensionMode::Union(subspaces) = &comprehension.mode else {
        panic!("expected Union, got {:?}", comprehension.mode);
    };
    assert_eq!(subspaces.len(), 2);
    assert_eq!(subspaces[0].len(), 2);
    assert_eq!(subspaces[0][0].var, "k");
    assert_eq!(subspaces[0][0].expr, "10");
    assert_eq!(subspaces[0][1].var, "limit");
    assert_eq!(subspaces[0][1].expr, "10,20,30");
    assert_eq!(subspaces[1][0].expr, "100");
}

#[test]
fn array_multi_clause_distinct_vars_is_multi_clause_cartesian() {
    let yaml = format!(r#"
scenarios:
  s:
    - for_each:
        - "x in 1,2, y in a,b, z in p,q"
      phases: [p]
{PHASES}
"#);
    let node = first_scenario_node(&yaml);
    let ScenarioNode::Comprehension { comprehension, .. } = node else {
        panic!("expected Comprehension");
    };
    let ComprehensionMode::Cartesian(clauses) = &comprehension.mode else {
        panic!("expected Cartesian, got {:?}", comprehension.mode);
    };
    assert_eq!(clauses.len(), 3);
    assert_eq!(clauses[0].var, "x");
    assert_eq!(clauses[1].var, "y");
    assert_eq!(clauses[2].var, "z");
}

#[test]
fn for_keyword_alias_maps_to_same_comprehension() {
    // `for: ...` is accepted as a synonym for `for_each: ...`.
    let yaml = format!(r#"
scenarios:
  s:
    - for: "k in 10,100"
      phases: [p]
{PHASES}
"#);
    let node = first_scenario_node(&yaml);
    let ScenarioNode::Comprehension { comprehension, .. } = node else {
        panic!("expected Comprehension");
    };
    assert!(comprehension.is_cartesian());
    assert_eq!(comprehension.coordinate_names(), vec!["k"]);
    assert_eq!(comprehension.filter, None);
}

#[test]
fn where_clause_attaches_to_comprehension() {
    let yaml = format!(r#"
scenarios:
  s:
    - for: "k in 10,100, limit in 10,20,30"
      where: "{{k}} * {{limit}} < 1000"
      phases: [p]
{PHASES}
"#);
    let node = first_scenario_node(&yaml);
    let ScenarioNode::Comprehension { comprehension, .. } = node else {
        panic!("expected Comprehension");
    };
    assert_eq!(
        comprehension.filter,
        Some("{k} * {limit} < 1000".to_string()),
    );
}

#[test]
fn for_each_in_paren_call_does_not_split() {
    // Regression guard: commas inside `matching_profiles('a','b')`
    // must not be treated as top-level clause separators.
    let yaml = format!(r#"
scenarios:
  s:
    - for_each: "p in matching_profiles('ds', 'pre')"
      phases: [p]
{PHASES}
"#);
    let node = first_scenario_node(&yaml);
    let ScenarioNode::Comprehension { comprehension, .. } = node else {
        panic!("expected Comprehension");
    };
    let ComprehensionMode::Cartesian(clauses) = &comprehension.mode else {
        panic!("expected Cartesian, got {:?}", comprehension.mode);
    };
    assert_eq!(clauses.len(), 1);
    assert!(clauses[0].expr.contains("matching_profiles"), "got: {}", clauses[0].expr);
    assert!(clauses[0].expr.contains(","));
}
