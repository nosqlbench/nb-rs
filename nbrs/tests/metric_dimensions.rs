// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `dimensions:` declarations and metric `cell:` placement, end to end from YAML.
//!
//! A metric identity is its label set with the family name promoted into it — a
//! closed 1:1 association. `cell:` does not attach a label to a sample; it
//! selects the dimensional cell the sample belongs to, refining the identity its
//! registration site already composes.
//!
//! The dimension is *reified*: scope synthesis lowers each coordinate to a
//! compiled kernel binding (`__cell_<metric>__<dim>`) beside the metric's value
//! binding. That is what lets a `cell:` reference be checked against the program
//! at load — rather than surfacing later as a component-tree attach panic, or
//! not at all.
//!
//! These tests cover the load-time surface. Runtime consumption of the
//! coordinate (resolving the cell and registering the family on it) is a
//! separate layer.

use std::path::PathBuf;
use std::process::Command;

struct Sandbox {
    path: PathBuf,
}

impl Sandbox {
    fn new(tag: &str) -> Self {
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("nbrs-dims-{tag}-{pid}-{nanos}"));
        std::fs::create_dir_all(&path).expect("create sandbox");
        Self { path }
    }

    fn run(&self, body: &str) -> String {
        std::fs::write(self.path.join("w.yaml"), body).expect("write workload");
        let out = Command::new(env!("CARGO_BIN_EXE_nbrs"))
            .args(["run", "workload=w.yaml", "adapter=stdout", "--session-keep=1000"])
            .current_dir(&self.path)
            .output()
            .expect("spawn nbrs");
        format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        )
    }
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// The happy path: a declared dimension, a metric placed in it by expression.
/// The coordinate must compile as part of the op kernel — if it did not, the
/// run would fail on the synthesized binding.
#[test]
fn a_declared_dimension_and_a_metric_cell_load_and_run() {
    let sb = Sandbox::new("ok");
    let out = sb.run(
        r#"
params:
  adapter: stdout
phases:
  t:
    cycles: 1
    dimensions:
      tier: { type: str }
    ops:
      probe:
        bindings: |
          tier_name := "tier24"
          measured := 41.0 + 1.0
        metrics:
          bytes_out:
            kind: gauge
            value: measured
            cell: { tier: tier_name }
        stmt: "placed"
"#,
    );
    assert!(
        out.contains("placed"),
        "the op must run with a cell-placed metric:\n{out}"
    );
    assert!(
        !out.to_lowercase().contains("error"),
        "a declared dimension must not produce an error:\n{out}"
    );
}

/// The shorthand declaration form.
#[test]
fn a_dimension_may_be_declared_by_bare_type_name() {
    let sb = Sandbox::new("shorthand");
    let out = sb.run(
        r#"
params:
  adapter: stdout
phases:
  t:
    cycles: 1
    dimensions:
      tier: str
    ops:
      probe:
        bindings: |
          tier_name := "tier24"
          measured := 1.0
        metrics:
          bytes_out: { kind: gauge, value: measured, cell: { tier: tier_name } }
        stmt: "placed"
"#,
    );
    assert!(out.contains("placed"), "shorthand `tier: str` must parse:\n{out}");
}

/// The payoff for reifying: an undeclared dimension is a load error naming the
/// declared set, not a surprise at attach time.
#[test]
fn an_undeclared_cell_dimension_is_rejected_at_load() {
    let sb = Sandbox::new("undeclared");
    let out = sb.run(
        r#"
params:
  adapter: stdout
phases:
  t:
    cycles: 1
    dimensions:
      tier: str
    ops:
      probe:
        bindings: |
          v := 1.0
        metrics:
          bytes_out: { kind: gauge, value: v, cell: { teir: v } }
        stmt: "x"
"#,
    );
    assert!(
        out.contains("not declared"),
        "a typo'd dimension must be rejected:\n{out}"
    );
    assert!(
        out.contains("tier"),
        "the error should name what IS declared, since the common mistake is a \
         near-miss:\n{out}"
    );
}

/// A phase with no `dimensions:` at all still rejects a `cell:`, rather than
/// treating the absence as permission.
#[test]
fn a_cell_without_any_declaration_is_rejected() {
    let sb = Sandbox::new("nodecl");
    let out = sb.run(
        r#"
params:
  adapter: stdout
phases:
  t:
    cycles: 1
    ops:
      probe:
        bindings: |
          v := 1.0
        metrics:
          bytes_out: { kind: gauge, value: v, cell: { tier: v } }
        stmt: "x"
"#,
    );
    assert!(
        out.contains("not declared"),
        "an undeclared dimension must be rejected even when the phase declares \
         none:\n{out}"
    );
}

/// Unknown-field hygiene: a misspelled `cell:` must not be silently dropped,
/// which would leave the metric quietly unplaced.
#[test]
fn a_misspelled_cell_key_is_rejected() {
    let sb = Sandbox::new("typo");
    let out = sb.run(
        r#"
params:
  adapter: stdout
phases:
  t:
    cycles: 1
    dimensions:
      tier: str
    ops:
      probe:
        bindings: |
          v := 1.0
        metrics:
          bytes_out: { kind: gauge, value: v, cel: { tier: v } }
        stmt: "x"
"#,
    );
    assert!(
        out.contains("unknown field") && out.contains("cel"),
        "a misspelled metric field must be rejected, not dropped:\n{out}"
    );
}

/// A dimension name becomes a label name, so it must be a valid identifier.
#[test]
fn an_invalid_dimension_name_is_rejected() {
    let sb = Sandbox::new("badname");
    let out = sb.run(
        r#"
params:
  adapter: stdout
phases:
  t:
    cycles: 1
    dimensions:
      "tier-name": str
    ops:
      probe:
        stmt: "x"
"#,
    );
    assert!(
        out.contains("valid identifier"),
        "a dimension name that cannot be a label must be rejected:\n{out}"
    );
}

/// Only `str`: a dimension keyed on a float would key cells on formatting
/// rather than on identity.
#[test]
fn a_non_string_dimension_type_is_rejected() {
    let sb = Sandbox::new("badtype");
    let out = sb.run(
        r#"
params:
  adapter: stdout
phases:
  t:
    cycles: 1
    dimensions:
      tier: { type: f64 }
    ops:
      probe:
        stmt: "x"
"#,
    );
    assert!(
        out.contains("str"),
        "an unsupported dimension type must be rejected with the supported \
         one named:\n{out}"
    );
}
