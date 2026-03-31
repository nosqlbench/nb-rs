// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! End-to-end test: YAML workload → GK kernel → stdout activity.
//!
//! This is the full pipeline test — the closest thing to running
//! `nb-rs run workload.yaml driver=stdout`.

use std::sync::Arc;

use nb_activity::activity::{Activity, ActivityConfig};
use nb_activity::adapters::stdout::{StdoutAdapter, StdoutConfig, StdoutFormat};
use nb_activity::opseq::{OpSequence, SequencerType};
use nb_activity::synthesis::OpBuilder;
use nb_metrics::labels::Labels;
use nb_variates::assembly::{GkAssembler, WireRef};
use nb_variates::nodes::hash::Hash64;
use nb_variates::nodes::arithmetic::ModU64;
use nb_variates::nodes::identity::Identity;
use nb_workload::parse::parse_ops;

/// Parse a workload YAML, build a GK kernel from bindings that
/// cover all referenced bind points, wire up the activity, and
/// run it through the stdout adapter.
#[tokio::test]
async fn full_pipeline_simple_workload() {
    let yaml = r#"
bindings:
  user_id: Identity()
ops:
  write_user:
    stmt: "INSERT INTO users (id) VALUES ({user_id});"
"#;

    // Step 1: Parse workload
    let ops = parse_ops(yaml).unwrap();
    assert_eq!(ops.len(), 1);
    assert_eq!(ops[0].name, "write_user");

    // Step 2: Build GK kernel from bindings
    // The workload declares "user_id: Identity()" — for now, we
    // wire it manually. The DSL compiler would handle this in the
    // full product.
    let mut asm = GkAssembler::new(vec!["cycle".into()]);
    asm.add_node("user_id", Box::new(Identity::new()), vec![WireRef::coord("cycle")]);
    asm.add_output("user_id", WireRef::node("user_id"));
    let kernel = asm.compile().unwrap();

    // Step 3: Create op builder
    let builder = Arc::new(OpBuilder::new(kernel));

    // Step 4: Create activity
    let config = ActivityConfig {
        name: "test".into(),
        cycles: 5,
        concurrency: 1,
        ..Default::default()
    };
    let seq = OpSequence::from_ops(ops, SequencerType::Bucket);
    let activity = Activity::new(config, &Labels::of("session", "e2e"), seq);

    // Step 5: Create stdout adapter writing to a temp file
    let path = std::env::temp_dir().join("nb_e2e_test.txt");
    let adapter = Arc::new(StdoutAdapter::with_config(StdoutConfig {
        filename: path.to_str().unwrap().into(),
        newline: true,
        format: StdoutFormat::Statement,
    }));

    // Step 6: Run!
    let b = builder.clone();
    activity.run(
        adapter,
        Arc::new(move |cycle, template| b.build(cycle, template)),
    ).await;

    // Step 7: Verify output
    let content = std::fs::read_to_string(&path).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines.len(), 5, "should have 5 lines (5 cycles)");

    // Each line should be the resolved INSERT with a numeric user_id
    for (i, line) in lines.iter().enumerate() {
        assert!(line.contains("INSERT INTO users"), "line {i}: {line}");
        assert!(!line.contains("{user_id}"), "bind point should be resolved: {line}");
        // Identity() means user_id = cycle number
        assert!(line.contains(&i.to_string()), "line {i} should contain cycle {i}: {line}");
    }

    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn full_pipeline_with_hash_and_mod() {
    let yaml = r#"
bindings:
  user_id: Hash(); Mod(1000000)
  bucket: Hash(); Mod(64)
ops:
  read:
    stmt: "SELECT * FROM users WHERE id={user_id} AND bucket={bucket};"
"#;

    let ops = parse_ops(yaml).unwrap();

    // Build kernel: hash → mod for user_id, hash → mod for bucket
    let mut asm = GkAssembler::new(vec!["cycle".into()]);
    asm.add_node("h1", Box::new(Hash64::new()), vec![WireRef::coord("cycle")]);
    asm.add_node("user_id", Box::new(ModU64::new(1_000_000)), vec![WireRef::node("h1")]);
    asm.add_node("h2", Box::new(Hash64::new()), vec![WireRef::node("h1")]);
    asm.add_node("bucket", Box::new(ModU64::new(64)), vec![WireRef::node("h2")]);
    asm.add_output("user_id", WireRef::node("user_id"));
    asm.add_output("bucket", WireRef::node("bucket"));
    let kernel = asm.compile().unwrap();
    let builder = Arc::new(OpBuilder::new(kernel));

    let config = ActivityConfig {
        name: "readtest".into(),
        cycles: 10,
        concurrency: 2,
        ..Default::default()
    };
    let seq = OpSequence::from_ops(ops, SequencerType::Bucket);
    let activity = Activity::new(config, &Labels::of("session", "e2e"), seq);

    let path = std::env::temp_dir().join("nb_e2e_hash_test.txt");
    let adapter = Arc::new(StdoutAdapter::with_config(StdoutConfig {
        filename: path.to_str().unwrap().into(),
        newline: true,
        format: StdoutFormat::Statement,
    }));

    let b = builder.clone();
    activity.run(adapter, Arc::new(move |cycle, template| b.build(cycle, template))).await;

    let content = std::fs::read_to_string(&path).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines.len(), 10);

    for line in &lines {
        assert!(line.contains("SELECT * FROM users WHERE id="));
        assert!(line.contains("AND bucket="));
        assert!(!line.contains("{user_id}"));
        assert!(!line.contains("{bucket}"));
    }

    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn full_pipeline_weighted_ops() {
    let yaml = r#"
ops:
  read:
    ratio: 3
    stmt: "READ"
  write:
    ratio: 1
    stmt: "WRITE"
"#;

    let ops = parse_ops(yaml).unwrap();

    // No bindings needed — static ops
    let mut asm = GkAssembler::new(vec!["cycle".into()]);
    asm.add_node("id", Box::new(Identity::new()), vec![WireRef::coord("cycle")]);
    asm.add_output("id", WireRef::node("id"));
    let kernel = asm.compile().unwrap();
    let builder = Arc::new(OpBuilder::new(kernel));

    let config = ActivityConfig {
        name: "weighted".into(),
        cycles: 8, // 2 full stanzas of 4
        concurrency: 1,
        ..Default::default()
    };
    let seq = OpSequence::from_ops(ops, SequencerType::Bucket);
    assert_eq!(seq.stanza_length(), 4); // 3 + 1

    let activity = Activity::new(config, &Labels::of("session", "e2e"), seq);

    let path = std::env::temp_dir().join("nb_e2e_weighted_test.txt");
    let adapter = Arc::new(StdoutAdapter::with_config(StdoutConfig {
        filename: path.to_str().unwrap().into(),
        newline: true,
        format: StdoutFormat::Statement,
    }));

    let b = builder.clone();
    activity.run(adapter, Arc::new(move |cycle, template| b.build(cycle, template))).await;

    let content = std::fs::read_to_string(&path).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines.len(), 8);

    let reads = lines.iter().filter(|l| l.contains("READ")).count();
    let writes = lines.iter().filter(|l| l.contains("WRITE")).count();
    assert_eq!(reads, 6, "3:1 ratio over 8 cycles = 6 reads");
    assert_eq!(writes, 2, "3:1 ratio over 8 cycles = 2 writes");

    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn full_pipeline_json_format() {
    let yaml = r#"
ops:
  create:
    method: POST
    url: "/api/users/{user_id}"
    content_type: "application/json"
"#;

    let ops = parse_ops(yaml).unwrap();

    let mut asm = GkAssembler::new(vec!["cycle".into()]);
    asm.add_node("h", Box::new(Hash64::new()), vec![WireRef::coord("cycle")]);
    asm.add_node("user_id", Box::new(ModU64::new(10000)), vec![WireRef::node("h")]);
    asm.add_output("user_id", WireRef::node("user_id"));
    let kernel = asm.compile().unwrap();
    let builder = Arc::new(OpBuilder::new(kernel));

    let config = ActivityConfig {
        name: "jsontest".into(),
        cycles: 3,
        concurrency: 1,
        ..Default::default()
    };
    let seq = OpSequence::from_ops(ops, SequencerType::Bucket);
    let activity = Activity::new(config, &Labels::of("session", "e2e"), seq);

    let path = std::env::temp_dir().join("nb_e2e_json_test.txt");
    let adapter = Arc::new(StdoutAdapter::with_config(StdoutConfig {
        filename: path.to_str().unwrap().into(),
        newline: true,
        format: StdoutFormat::Json,
    }));

    let b = builder.clone();
    activity.run(adapter, Arc::new(move |cycle, template| b.build(cycle, template))).await;

    let content = std::fs::read_to_string(&path).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines.len(), 3);

    for line in &lines {
        assert!(line.starts_with('{'), "should be JSON: {line}");
        assert!(line.contains("\"method\":\"POST\""), "should have method: {line}");
        assert!(line.contains("/api/users/"), "should have url with resolved id: {line}");
        assert!(!line.contains("{user_id}"), "bind point should be resolved: {line}");
    }

    let _ = std::fs::remove_file(&path);
}
