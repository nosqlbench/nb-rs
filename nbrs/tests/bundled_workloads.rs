// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! SRD-85 bundled workloads — end-to-end coverage.
//!
//! Every test runs the real binary from a **bare temporary
//! directory** (no repo files in sight), because that is the
//! whole point of the catalog: a distributed artifact carries
//! its workloads with it.

use std::path::{Path, PathBuf};
use std::process::Command;

struct Sandbox {
    dir: PathBuf,
}

impl Sandbox {
    fn new(tag: &str) -> Self {
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("nbrs-bundled-{tag}-{pid}-{nanos}"));
        std::fs::create_dir_all(&dir).expect("create sandbox");
        Self { dir }
    }
    fn path(&self) -> &Path {
        &self.dir
    }
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Run nbrs with the sandbox as cwd and a sandbox-local session
/// path (tests must never run workloads from the project root).
fn nbrs_in(sandbox: &Sandbox, args: &[&str]) -> (String, String, bool) {
    let session = sandbox.path().join("session");
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_nbrs"));
    cmd.current_dir(sandbox.path());
    let needs_session = args.first().is_some_and(|a| *a == "run");
    if needs_session {
        cmd.arg("run")
            .arg("--session-path")
            .arg(&session)
            .args(&args[1..]);
    } else {
        cmd.args(args);
    }
    let out = cmd.output().expect("run nbrs");
    (
        String::from_utf8_lossy(&out.stdout).to_string(),
        String::from_utf8_lossy(&out.stderr).to_string(),
        out.status.success(),
    )
}

#[test]
fn bundled_impl_implements_resolves_sibling_blueprint() {
    // `examples/composition/reported_impl_testkit` declares
    // `implements: ./reported_blueprint.yaml`; invoked by CATALOG
    // name there is no file directory, so the reference must
    // resolve namespace-relative in the catalog — the same
    // sibling-by-filename idiom `extends:` already honors
    // (SRD-85). Both invocation forms bind and run.
    let sb = Sandbox::new("ns-implements");
    let (stdout, stderr, ok) = nbrs_in(&sb, &[
        "run", "workload=examples/composition/reported_impl_testkit",
        "tui=off",
    ]);
    assert!(ok, "catalog impl must pull its sibling blueprint:\n{stdout}\n{stderr}");
    assert!(format!("{stdout}{stderr}").contains("2 completed, 0 failed"),
        "both phases complete:\n{stdout}\n{stderr}");

    let sb = Sandbox::new("ns-implements-2");
    let (stdout, stderr, ok) = nbrs_in(&sb, &[
        "run", "workload=examples/composition/reported_blueprint",
        "impl=examples/composition/reported_impl_testkit",
        "tui=off",
    ]);
    assert!(ok, "catalog blueprint + impl= must bind:\n{stdout}\n{stderr}");
    assert!(format!("{stdout}{stderr}").contains("2 completed, 0 failed"),
        "both phases complete:\n{stdout}\n{stderr}");
}

// ─────────────────────────────────────────────────────────────────
// Running bundled workloads by catalog name
// ─────────────────────────────────────────────────────────────────

#[test]
fn bundled_example_runs_from_bare_directory() {
    let sb = Sandbox::new("example-run");
    let (stdout, stderr, ok) =
        nbrs_in(&sb, &["run", "workload=examples/signals/lfsr"]);
    assert!(ok, "bundled example failed: {stderr}");
    // The catalog name is the session's workload identity.
    let combined = format!("{stdout}\n{stderr}");
    assert!(combined.contains("examples/signals/lfsr"),
        "catalog name should be the workload identity:\n{combined}");
}

#[test]
fn curated_selfcheck_runs_green() {
    let sb = Sandbox::new("selfcheck");
    let (stdout, stderr, ok) = nbrs_in(&sb, &["run", "workload=selfcheck"]);
    assert!(ok, "selfcheck failed: {stderr}");
    let combined = format!("{stdout}\n{stderr}");
    assert!(combined.contains("5 completed, 0 failed"),
        "selfcheck should complete all 5 phases:\n{combined}");
}

#[test]
fn local_file_and_catalog_name_collision_is_fatal() {
    let sb = Sandbox::new("ambiguity");
    // A local file with exactly a catalog name (the resolver
    // probes the exact path first).
    std::fs::write(sb.path().join("selfcheck"), "phases: {}\n").unwrap();
    let (stdout, stderr, ok) = nbrs_in(&sb, &["run", "workload=selfcheck"]);
    assert!(!ok, "local/catalog collision must be fatal");
    let combined = format!("{stdout}\n{stderr}");
    assert!(combined.contains("ambiguous"), "diagnostic: {combined}");
}

// ─────────────────────────────────────────────────────────────────
// extends: through the catalog (SRD-72 × SRD-85)
// ─────────────────────────────────────────────────────────────────

#[test]
fn local_child_extends_bundled_parent() {
    let sb = Sandbox::new("extends");
    // A local workload inheriting a bundled curated parent and
    // overriding one param — the `copy`-free customization path.
    std::fs::write(
        sb.path().join("mini_probe.yaml"),
        "extends: capacity_probe\nparams:\n  ops_per_step: \"50\"\n",
    )
    .unwrap();
    let (stdout, stderr, ok) =
        nbrs_in(&sb, &["run", "workload=./mini_probe.yaml"]);
    assert!(ok, "extends-from-bundled failed: {stderr}");
    let combined = format!("{stdout}\n{stderr}");
    assert!(combined.contains("3 completed, 0 failed"),
        "inherited sweep should run all 3 steps:\n{combined}");
}

#[test]
fn bundled_sibling_extends_resolves_in_namespace() {
    // `cql/full_cql_vector_sweep` extends its sibling by
    // filename; in the catalog that resolves namespace-relative.
    // Introspection performs the full merge without needing a
    // CQL target. Skipped when this binary carries no cql
    // bundle (engine features off).
    let sb = Sandbox::new("ns-extends");
    let (stdout, _stderr, ok) = nbrs_in(&sb, &["describe", "workloads", "--all", "--json"]);
    assert!(ok);
    if !stdout.contains("cql/full_cql_vector_sweep") {
        eprintln!("skipping: no cql bundle in this binary");
        return;
    }
    let (stdout, stderr, ok) =
        nbrs_in(&sb, &["describe", "workloads", "cql/full_cql_vector_sweep"]);
    assert!(ok, "namespace-relative extends merge failed: {stderr}");
    assert!(stdout.contains("Parameter-space sweep sibling"),
        "child description should survive the merge:\n{stdout}");
}

// ─────────────────────────────────────────────────────────────────
// Discovery — describe workloads
// ─────────────────────────────────────────────────────────────────

#[test]
fn describe_workloads_tiers() {
    let sb = Sandbox::new("describe");
    // Default: curated only, with a pointer at the hidden tier.
    let (stdout, stderr, ok) = nbrs_in(&sb, &["describe", "workloads"]);
    assert!(ok, "describe workloads failed: {stderr}");
    assert!(stdout.contains("selfcheck"), "curated listing:\n{stdout}");
    assert!(stdout.contains("capacity_probe"), "curated listing:\n{stdout}");
    assert!(!stdout.contains("examples/"),
        "examples tier must be hidden by default:\n{stdout}");
    assert!(stdout.contains("--all"),
        "default listing should point at the hidden tier:\n{stdout}");

    // --all reveals the examples tier.
    let (stdout, _, ok) = nbrs_in(&sb, &["describe", "workloads", "--all"]);
    assert!(ok);
    assert!(stdout.contains("examples/signals/lfsr"), "--all listing:\n{stdout}");

    // `examples` subtopic: examples only.
    let (stdout, _, ok) = nbrs_in(&sb, &["describe", "workloads", "examples"]);
    assert!(ok);
    assert!(stdout.contains("examples/cursors/timeboxed_partition_sweep"));
    assert!(!stdout.contains("selfcheck"),
        "examples listing must not carry curated entries:\n{stdout}");
}

#[test]
fn curated_lint_every_entry_described_and_introspectable() {
    // The SRD-85 curated-tier lint: every curated entry carries
    // a structured `description:` and renders a detail view.
    let sb = Sandbox::new("lint");
    let (stdout, stderr, ok) = nbrs_in(&sb, &["describe", "workloads", "--json"]);
    assert!(ok, "json listing failed: {stderr}");
    let items: Vec<serde_json::Value> =
        serde_json::from_str(&stdout).expect("valid json listing");
    assert!(!items.is_empty(), "curated tier must not be empty (SRD-85 P1)");
    for item in &items {
        let name = item["name"].as_str().unwrap();
        assert_eq!(item["tier"].as_str().unwrap(), "curated");
        assert!(item["described"].as_bool().unwrap(),
            "curated workload `{name}` is missing a `description:` field");
        assert!(!item["summary"].as_str().unwrap().is_empty(),
            "curated workload `{name}` has an empty summary");
        let (_, dstderr, dok) = nbrs_in(&sb, &["describe", "workloads", name]);
        assert!(dok, "detail view failed for `{name}`: {dstderr}");
    }
}

#[test]
fn describe_workloads_detail_renders_run_and_copy_hints() {
    let sb = Sandbox::new("detail");
    let (stdout, stderr, ok) =
        nbrs_in(&sb, &["describe", "workloads", "capacity_probe"]);
    assert!(ok, "detail failed: {stderr}");
    assert!(stdout.contains("tier:     curated"), "{stdout}");
    assert!(stdout.contains("Concurrency-vs-throughput"), "{stdout}");
    assert!(stdout.contains("ops_per_step"), "params with defaults:\n{stdout}");
    assert!(stdout.contains("run:      nbrs run workload=capacity_probe"), "{stdout}");
    assert!(stdout.contains("copy:     nbrs copy capacity_probe"), "{stdout}");
}

// ─────────────────────────────────────────────────────────────────
// Materialization — nbrs copy
// ─────────────────────────────────────────────────────────────────

#[test]
fn copy_stamps_provenance_and_refuses_overwrite() {
    let sb = Sandbox::new("copy");
    let (stdout, stderr, ok) = nbrs_in(&sb, &["copy", "selfcheck"]);
    assert!(ok, "copy failed: {stderr}");
    assert!(stdout.contains("selfcheck.yaml"), "{stdout}");
    let copied = std::fs::read_to_string(sb.path().join("selfcheck.yaml")).unwrap();
    assert!(copied.starts_with("# Copied from bundled workload `selfcheck`"),
        "provenance header missing:\n{}", &copied[..120.min(copied.len())]);
    assert!(copied.contains("extends: selfcheck"),
        "provenance should mention the extends alternative");
    // Refuses to overwrite.
    let (_, stderr, ok) = nbrs_in(&sb, &["copy", "selfcheck"]);
    assert!(!ok, "second copy must refuse");
    assert!(stderr.contains("refusing to overwrite"), "{stderr}");
    // `to=` destination.
    let (_, stderr, ok) = nbrs_in(&sb, &["copy", "selfcheck", "to=mine.yaml"]);
    assert!(ok, "copy to= failed: {stderr}");
    assert!(sb.path().join("mine.yaml").exists());
    // The copy is runnable as-is.
    let (_, stderr, ok) = nbrs_in(&sb, &["run", "workload=./mine.yaml"]);
    assert!(ok, "copied workload failed to run: {stderr}");
}

#[test]
fn copy_unknown_name_points_at_discovery() {
    let sb = Sandbox::new("copy-miss");
    let (_, stderr, ok) = nbrs_in(&sb, &["copy", "nosuch/workload"]);
    assert!(!ok);
    assert!(stderr.contains("describe workloads"), "{stderr}");
}
