// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Capture dependency analysis and op linearization (sysref 02).
//!
//! Analyzes capture declarations and references across ops in a stanza
//! to partition them into dependency groups. Groups execute sequentially
//! (with capture application between them); ops within a group execute
//! concurrently.

use std::collections::HashSet;
use nb_workload::model::ParsedOp;
use nb_workload::bindpoints;

/// A dependency group: a set of op indices that can execute concurrently.
/// All ops in a group have their capture dependencies satisfied by
/// prior groups.
#[derive(Debug, Clone)]
pub struct DepGroup {
    /// Op indices (positions in the stanza's op sequence) in this group.
    pub op_indices: Vec<usize>,
    /// Capture names that ops in this group require from prior groups.
    pub required_captures: HashSet<String>,
    /// Capture names that ops in this group produce.
    pub produced_captures: HashSet<String>,
}

/// Analyze capture dependencies across ops in a stanza and partition
/// into dependency groups.
///
/// Returns groups in execution order. Ops within a group are independent
/// and can execute concurrently. Groups must execute sequentially with
/// capture application between them.
///
/// If no capture dependencies exist, returns a single group containing
/// all ops (maximum concurrency).
pub fn analyze_dependencies(templates: &[ParsedOp]) -> Vec<DepGroup> {
    if templates.is_empty() {
        return Vec::new();
    }

    // Step 1: For each op, collect what it produces and consumes
    let mut produces: Vec<HashSet<String>> = Vec::with_capacity(templates.len());
    let mut consumes: Vec<HashSet<String>> = Vec::with_capacity(templates.len());

    for template in templates {
        let mut prod = HashSet::new();
        let mut cons = HashSet::new();

        for value in template.op.values() {
            if let serde_json::Value::String(s) = value {
                // Capture declarations: [name], [name as alias]
                let result = bindpoints::parse_capture_points(s);
                for cp in &result.captures {
                    prod.insert(cp.as_name.clone());
                }

                // Capture references: {capture:name} or unqualified {name}
                // that might resolve to a capture
                let bps = bindpoints::extract_bind_points(s);
                for bp in &bps {
                    if let bindpoints::BindPoint::Reference { name, qualifier } = bp {
                        match qualifier {
                            bindpoints::BindQualifier::Capture => {
                                cons.insert(name.clone());
                            }
                            bindpoints::BindQualifier::None => {
                                // Unqualified — could be a capture reference.
                                // We conservatively include it as a potential
                                // consumer. If it resolves to a GK binding
                                // instead, the dependency is spurious but safe
                                // (over-linearization, not under-linearization).
                                cons.insert(name.clone());
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        produces.push(prod);
        consumes.push(cons);
    }

    // Step 2: Build dependency edges
    // op[j] depends on op[i] if op[j] consumes a name that op[i] produces
    // and i < j (earlier in stanza order)
    let n = templates.len();
    let mut depends_on: Vec<HashSet<usize>> = vec![HashSet::new(); n];

    for j in 0..n {
        for name in &consumes[j] {
            // Find the latest producer of this name before j
            for i in (0..j).rev() {
                if produces[i].contains(name) {
                    depends_on[j].insert(i);
                    break; // only need the most recent producer
                }
            }
        }
    }

    // Step 3: Topological grouping (Kahn's algorithm variant)
    let mut remaining: HashSet<usize> = (0..n).collect();
    let mut groups = Vec::new();

    while !remaining.is_empty() {
        // Find all ops whose dependencies are fully satisfied
        // (all deps are in already-scheduled groups)
        let ready: Vec<usize> = remaining.iter()
            .filter(|&&i| depends_on[i].iter().all(|dep| !remaining.contains(dep)))
            .copied()
            .collect();

        if ready.is_empty() {
            // Cycle in dependency graph — shouldn't happen with
            // well-formed stanzas, but break to avoid infinite loop
            eprintln!("warning: circular capture dependency detected; \
                       scheduling remaining ops sequentially");
            let mut fallback: Vec<usize> = remaining.into_iter().collect();
            fallback.sort();
            for idx in fallback {
                groups.push(DepGroup {
                    op_indices: vec![idx],
                    required_captures: consumes[idx].clone(),
                    produced_captures: produces[idx].clone(),
                });
            }
            break;
        }

        for &idx in &ready {
            remaining.remove(&idx);
        }

        let mut sorted_ready = ready;
        sorted_ready.sort(); // maintain stanza order within group

        let mut required = HashSet::new();
        let mut produced = HashSet::new();
        for &idx in &sorted_ready {
            for name in &consumes[idx] {
                // Only "required" if it comes from a prior group (not self-produced)
                if !produces[idx].contains(name) {
                    required.insert(name.clone());
                }
            }
            for name in &produces[idx] {
                produced.insert(name.clone());
            }
        }

        groups.push(DepGroup {
            op_indices: sorted_ready,
            required_captures: required,
            produced_captures: produced,
        });
    }

    groups
}

/// Check if dependency groups differ from a single "all concurrent" group.
/// Returns true if there are actual dependencies requiring linearization.
pub fn has_dependencies(groups: &[DepGroup], total_ops: usize) -> bool {
    groups.len() > 1 || (groups.len() == 1 && groups[0].op_indices.len() < total_ops)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn op(name: &str, stmt: &str) -> ParsedOp {
        ParsedOp::simple(name, stmt)
    }

    #[test]
    fn no_captures_single_group() {
        let templates = vec![
            op("insert1", "INSERT INTO t VALUES ({id1})"),
            op("insert2", "INSERT INTO t VALUES ({id2})"),
            op("insert3", "INSERT INTO t VALUES ({id3})"),
        ];
        let groups = analyze_dependencies(&templates);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].op_indices, vec![0, 1, 2]);
    }

    #[test]
    fn simple_capture_dependency() {
        // Op 0 produces [username], Op 1 consumes {capture:username}
        let templates = vec![
            op("read", "SELECT [username] FROM users WHERE id={id}"),
            op("update", "UPDATE users SET name={capture:username} WHERE id={id}"),
        ];
        let groups = analyze_dependencies(&templates);
        assert_eq!(groups.len(), 2, "should have 2 groups: {groups:?}");
        assert_eq!(groups[0].op_indices, vec![0]);
        assert_eq!(groups[1].op_indices, vec![1]);
    }

    #[test]
    fn independent_ops_concurrent() {
        // All three ops are independent — single group
        let templates = vec![
            op("a", "INSERT INTO t1 VALUES ({x})"),
            op("b", "INSERT INTO t2 VALUES ({y})"),
            op("c", "INSERT INTO t3 VALUES ({z})"),
        ];
        let groups = analyze_dependencies(&templates);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].op_indices, vec![0, 1, 2]);
    }

    #[test]
    fn chain_dependency() {
        // Op 0 → Op 1 → Op 2 (each depends on previous)
        let templates = vec![
            op("read_a", "SELECT [val_a] FROM t WHERE id={id}"),
            op("read_b", "SELECT [val_b] FROM t WHERE a={capture:val_a}"),
            op("write", "INSERT INTO t2 VALUES ({capture:val_b})"),
        ];
        let groups = analyze_dependencies(&templates);
        assert_eq!(groups.len(), 3, "chain should have 3 groups: {groups:?}");
        assert_eq!(groups[0].op_indices, vec![0]);
        assert_eq!(groups[1].op_indices, vec![1]);
        assert_eq!(groups[2].op_indices, vec![2]);
    }

    #[test]
    fn diamond_dependency() {
        // Op 0 produces [token], Op 1 and Op 2 consume it, Op 3 consumes both
        let templates = vec![
            op("source", "SELECT [token] FROM auth WHERE id={id}"),
            op("read1", "SELECT [val1] FROM t1 WHERE tok={capture:token}"),
            op("read2", "SELECT [val2] FROM t2 WHERE tok={capture:token}"),
            op("combine", "INSERT INTO results VALUES ({capture:val1}, {capture:val2})"),
        ];
        let groups = analyze_dependencies(&templates);
        // Group 0: [source] (produces token)
        // Group 1: [read1, read2] (both consume token, independent of each other)
        // Group 2: [combine] (depends on read1 and read2)
        assert_eq!(groups.len(), 3, "diamond: {groups:?}");
        assert_eq!(groups[0].op_indices, vec![0]);
        assert_eq!(groups[1].op_indices, vec![1, 2]);
        assert_eq!(groups[2].op_indices, vec![3]);
    }

    #[test]
    fn mixed_dependent_and_independent() {
        // Op 0: read (produces [user_name])
        // Op 1: independent insert
        // Op 2: dependent on Op 0's capture
        let templates = vec![
            op("read", "SELECT [user_name] FROM users WHERE id={id}"),
            op("insert", "INSERT INTO log VALUES ({cycle})"),
            op("update", "UPDATE users SET tag={capture:user_name} WHERE id={id}"),
        ];
        let groups = analyze_dependencies(&templates);
        // Group 0: [read, insert] — insert is independent
        // Group 1: [update] — depends on read's capture
        assert_eq!(groups.len(), 2, "mixed: {groups:?}");
        assert_eq!(groups[0].op_indices, vec![0, 1]);
        assert_eq!(groups[1].op_indices, vec![2]);
    }

    #[test]
    fn empty_stanza() {
        let groups = analyze_dependencies(&[]);
        assert!(groups.is_empty());
    }

    #[test]
    fn single_op() {
        let templates = vec![op("only", "SELECT * FROM t")];
        let groups = analyze_dependencies(&templates);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].op_indices, vec![0]);
    }
}
