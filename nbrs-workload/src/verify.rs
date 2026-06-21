// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Workload verification — run a workload and check its output against rules
//! embedded in the workload file. Shared by the `nbrs check` subcommand and the
//! example-walker test, so "how CI checks the examples" and "how a user checks
//! their own workload" are the same code.
//!
//! A verification target is resolved the same way `nbrs run` resolves
//! `workload=…`: a directory (walk every workload under it), an existing
//! `.yaml`/`.yml` file (run it by path), or a **bundled catalog name** such as
//! `examples/cursors/all_cursor/enumerate` (run it by name, read its rules from the embedded
//! source). So whatever tab-completion offers for `nbrs check <TAB>` — local
//! files *and* catalog names — checks the same way it runs.
//!
//! Two **equivalent** rule surfaces (a file may use either, or both — their
//! cases combine):
//!
//! 1. **`#@` comment directives** — trailing YAML comments, inert to the
//!    runtime:
//!    ```text
//!    #@ run scenario=enumerate
//!    #@ expect 50 completed, 0 failed
//!    #@ case overload
//!    #@   run concurrency=32 rate=100000
//!    #@   expect-fail error_rate_exceeded
//!    #@ requires backend (needs a live service)
//!    ```
//! 2. **A `verify:` YAML block** — also inert (the runtime ignores unknown
//!    top-level keys). Three equivalent shapes:
//!    ```yaml
//!    # single case (a directive map)
//!    verify: { run: scenario=enumerate, expect: "50 completed, 0 failed" }
//!    # a list of cases
//!    verify:
//!      - { case: a, run: scenario=a, expect: "..." }
//!      - { case: b, expect-fail: "..." }
//!    # a name-keyed map (key = case name)
//!    verify:
//!      a: { run: scenario=a, expect: "..." }
//!      b: { expect: ["x", "y"] }
//!    ```
//!
//! `expect` / `expect-fail` accept a single regex or a list of regexes. Each
//! must match the run's combined stdout+stderr; `expect-fail` additionally
//! requires a non-zero exit.

use regex::Regex;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Default per-case run timeout, in seconds.
pub const DEFAULT_TIMEOUT_SECS: u64 = 90;

/// Keywords recognized inside a `verify:` directive map (vs. case names).
const DIRECTIVE_KEYS: &[&str] =
    &["run", "expect", "expect-fail", "expect_fail", "requires", "timeout", "case"];

/// One verification case: an invocation plus the regexes its output must match.
pub struct VerifyCase {
    pub name: String,
    pub run_args: Vec<String>,
    pub expects: Vec<Regex>,
    pub expect_fails: Vec<Regex>,
    pub timeout: u64,
}

impl VerifyCase {
    fn new(name: impl Into<String>) -> Self {
        VerifyCase {
            name: name.into(),
            run_args: Vec::new(),
            expects: Vec::new(),
            expect_fails: Vec::new(),
            timeout: DEFAULT_TIMEOUT_SECS,
        }
    }
}

/// The parsed verification plan for one workload file.
pub struct VerifyPlan {
    /// `Some(reason)` ⇒ skip this file (e.g. needs external infra).
    pub requires: Option<String>,
    pub cases: Vec<VerifyCase>,
}

impl VerifyPlan {
    /// Parse both rule surfaces from a workload file's text and combine them.
    pub fn parse(src: &str) -> Result<VerifyPlan, String> {
        let mut plan = parse_directives(src)?;
        merge_verify_block(src, &mut plan)?;
        Ok(plan)
    }

    /// True when the file declares no verification rules at all.
    pub fn is_empty(&self) -> bool {
        self.requires.is_none() && self.cases.is_empty()
    }
}

fn compile(v: &str) -> Result<Regex, String> {
    Regex::new(v).map_err(|e| format!("bad regex {v:?}: {e}"))
}

// ── `#@` comment directives ───────────────────────────────────────────────

fn parse_directives(src: &str) -> Result<VerifyPlan, String> {
    let mut requires: Option<String> = None;
    let mut cases: Vec<VerifyCase> = Vec::new();
    let mut cur: Option<VerifyCase> = None;

    for raw in src.lines() {
        let line = raw.trim_start();
        let Some(rest) = line.strip_prefix("#@") else { continue };
        let rest = rest.trim();
        let (kw, val) = match rest.split_once(char::is_whitespace) {
            Some((k, v)) => (k.trim_end_matches(':'), v.trim()),
            None => (rest.trim_end_matches(':'), ""),
        };
        macro_rules! case {
            () => {
                cur.get_or_insert_with(|| VerifyCase::new("default"))
            };
        }
        match kw {
            "requires" => requires = Some(val.to_string()),
            "case" => {
                if let Some(c) = cur.take() {
                    cases.push(c);
                }
                cur = Some(VerifyCase::new(val));
            }
            "run" => case!().run_args = val.split_whitespace().map(String::from).collect(),
            "expect" => case!().expects.push(compile(val)?),
            "expect-fail" | "expect_fail" => case!().expect_fails.push(compile(val)?),
            "timeout" => {
                case!().timeout = val.parse().map_err(|_| format!("bad timeout {val:?}"))?
            }
            other => return Err(format!("unknown `#@ {other}` directive")),
        }
    }
    if let Some(c) = cur.take() {
        cases.push(c);
    }
    Ok(VerifyPlan { requires, cases })
}

// ── `verify:` YAML block ──────────────────────────────────────────────────

fn merge_verify_block(src: &str, plan: &mut VerifyPlan) -> Result<(), String> {
    // The file may not be a YAML mapping (or may be a `#@`-only file); either
    // way, no `verify:` block to merge.
    let Ok(doc) = serde_yaml::from_str::<serde_yaml::Value>(src) else {
        return Ok(());
    };
    let Some(block) = doc.get("verify") else {
        return Ok(());
    };
    match block {
        serde_yaml::Value::Sequence(items) => {
            for (i, item) in items.iter().enumerate() {
                plan.cases.push(case_from_map(item, None, i)?);
            }
        }
        serde_yaml::Value::Mapping(m) => {
            let all_directives = m.keys().all(|k| {
                k.as_str().is_some_and(|s| DIRECTIVE_KEYS.contains(&s))
            });
            if all_directives {
                // A single directive map — either a file-level `requires` skip
                // or one unnamed case.
                if let Some(req) = m.get("requires").and_then(|v| v.as_str()) {
                    plan.requires = Some(req.to_string());
                } else {
                    plan.cases.push(case_from_map(block, None, 0)?);
                }
            } else {
                // A name-keyed map: each key is a case name.
                for (k, v) in m {
                    let name = k.as_str().ok_or("verify: case name must be a string")?;
                    plan.cases.push(case_from_map(v, Some(name), 0)?);
                }
            }
        }
        _ => return Err("verify: must be a map or a list of cases".to_string()),
    }
    Ok(())
}

/// Build a case from a `verify:` entry map. `name_override` (the key in a
/// name-keyed map) wins; else the entry's `case:` field; else `case-<idx>`.
fn case_from_map(
    v: &serde_yaml::Value,
    name_override: Option<&str>,
    idx: usize,
) -> Result<VerifyCase, String> {
    let m = v.as_mapping().ok_or("verify: each case must be a map")?;
    let get = |k: &str| m.get(serde_yaml::Value::from(k));
    let name = name_override
        .map(String::from)
        .or_else(|| get("case").and_then(|x| x.as_str()).map(String::from))
        .unwrap_or_else(|| format!("case-{idx}"));
    let mut case = VerifyCase::new(name);
    if let Some(run) = get("run").and_then(|x| x.as_str()) {
        case.run_args = run.split_whitespace().map(String::from).collect();
    }
    for s in strings_of(get("expect")) {
        case.expects.push(compile(&s)?);
    }
    for s in strings_of(get("expect-fail").or_else(|| get("expect_fail"))) {
        case.expect_fails.push(compile(&s)?);
    }
    if let Some(t) = get("timeout").and_then(|x| x.as_u64()) {
        case.timeout = t;
    }
    Ok(case)
}

/// A scalar string or a sequence of strings → `Vec<String>`.
fn strings_of(v: Option<&serde_yaml::Value>) -> Vec<String> {
    match v {
        Some(serde_yaml::Value::String(s)) => vec![s.clone()],
        Some(serde_yaml::Value::Sequence(items)) => items
            .iter()
            .filter_map(|x| x.as_str().map(String::from))
            .collect(),
        _ => Vec::new(),
    }
}

// ── Run + check ───────────────────────────────────────────────────────────

/// What one verification case produced.
pub enum Outcome {
    Pass,
    Skip(String),
    Fail(String),
}

/// Run one case via `<binary> run workload=… <args> --session-path …` (wrapped
/// in `timeout`) from `sandbox`, capture combined output, and check the rules.
/// `workload_ref` is whatever goes after `workload=` — an absolute file path
/// or a bundled catalog name; the subprocess resolves it exactly as a normal
/// `nbrs run` would.
pub fn run_case(binary: &Path, workload_ref: &str, sandbox: &Path, label: &str, case: &VerifyCase) -> Result<(), String> {
    let session = sandbox.join(format!("session-{}", label.replace(['/', ' ', ':'], "_")));
    let _ = std::fs::remove_dir_all(&session);
    let output = Command::new("timeout")
        .arg(case.timeout.to_string())
        .arg(binary)
        .arg("run")
        .arg(format!("workload={workload_ref}"))
        .args(&case.run_args)
        .arg("--session-path")
        .arg(&session)
        .current_dir(sandbox)
        .output()
        .map_err(|e| format!("spawn failed: {e}"))?;

    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    if output.status.code() == Some(124) {
        return Err(format!("timed out after {}s", case.timeout));
    }
    let succeeded = output.status.success();

    if case.expect_fails.is_empty() {
        if !succeeded {
            let err = combined
                .lines()
                .find(|l| l.contains("error:") || l.contains("panic"))
                .unwrap_or("(no error line)");
            return Err(format!("run failed (expected success): {err}"));
        }
    } else {
        if succeeded {
            return Err("expected a failure (`expect-fail`) but the run succeeded".to_string());
        }
        for re in &case.expect_fails {
            if !re.is_match(&combined) {
                return Err(format!("expect-fail /{re}/ did not match the failure output"));
            }
        }
    }
    for re in &case.expects {
        if !re.is_match(&combined) {
            return Err(format!("expect /{re}/ did not match the output"));
        }
    }
    Ok(())
}

/// Verify a workload from its rule text + run reference: parse the rules, run
/// every case, return one `(label, Outcome)` per case (or a single Skip / Fail
/// for the whole workload). `run_ref` is what to pass as `workload=` — an
/// absolute file path or a catalog name.
pub fn verify_source(
    binary: &Path,
    label_root: &str,
    run_ref: &str,
    rule_text: &str,
    sandbox: &Path,
) -> Vec<(String, Outcome)> {
    let plan = match VerifyPlan::parse(rule_text) {
        Ok(p) => p,
        Err(e) => return vec![(label_root.to_string(), Outcome::Fail(e))],
    };
    if let Some(reason) = plan.requires {
        return vec![(label_root.to_string(), Outcome::Skip(reason))];
    }
    if plan.cases.is_empty() {
        return vec![(
            label_root.to_string(),
            Outcome::Fail(
                "no verification rules — add `#@ expect …` comments or a `verify:` block".into(),
            ),
        )];
    }
    plan.cases
        .iter()
        .map(|c| {
            let label = format!("{label_root}::{}", c.name);
            let outcome = match run_case(binary, run_ref, sandbox, &label, c) {
                Ok(()) => Outcome::Pass,
                Err(e) => Outcome::Fail(format!("{label}: {e}")),
            };
            (label, outcome)
        })
        .collect()
}

/// Verify one workload file: read it, then run it by its absolute path. The
/// file is read relative to *this* process's cwd, but the run reference is made
/// absolute because each case is launched from a sandbox cwd.
pub fn verify_file(binary: &Path, label_root: &str, workload: &Path, sandbox: &Path) -> Vec<(String, Outcome)> {
    let src = match std::fs::read_to_string(workload) {
        Ok(s) => s,
        Err(e) => return vec![(label_root.to_string(), Outcome::Fail(format!("read error: {e}")))],
    };
    let abs = workload.canonicalize().unwrap_or_else(|_| workload.to_path_buf());
    verify_source(binary, label_root, &abs.to_string_lossy(), &src, sandbox)
}

/// Where a verification target's rule text and run reference come from. Mirrors
/// `nbrs run`'s `workload=…` resolution.
pub enum WorkloadSource {
    /// A workload file on disk: run by (absolute) path, rules from the file.
    File(PathBuf),
    /// A bundled catalog workload: run by name, rules from the embedded source.
    Catalog { name: String, source: String },
}

/// Resolve a single workload reference the way `nbrs run` does: an existing
/// `.yaml`/`.yml` file path first, then a bundled catalog name. `None` if it is
/// neither. Directories are not a single workload — callers handle those
/// separately (via [`verify_path`]).
pub fn resolve_ref(reference: &str) -> Option<WorkloadSource> {
    let p = Path::new(reference);
    if p.is_file() {
        // Absolute so the sandbox-cwd subprocess can still find it.
        return Some(WorkloadSource::File(
            p.canonicalize().unwrap_or_else(|_| p.to_path_buf()),
        ));
    }
    crate::catalog::lookup(reference).map(|w| WorkloadSource::Catalog {
        name: w.name.to_string(),
        source: w.source.to_string(),
    })
}

/// Peek a workload reference's **declared top-level `params:`** (string
/// scalars), following its `extends:` chain — the way `nbrs run` resolves
/// `workload=…`. Returns `None` if the reference resolves to nothing or has
/// no `params:` block.
///
/// Used by `run.rs` to recognize a **console-owning adapter declared in the
/// workload** (e.g. `params: { adapter: plotter }`) rather than only on the
/// CLI, so the dashboard yields to the adapter on a TTY (SRD-41/87
/// console-ownership). The returned params also carry the adapter's
/// display-shaping keys (e.g. stdout's `filename`) so the preference is
/// decided correctly, not just by the adapter name.
pub fn peek_declared_params(reference: &str) -> Option<std::collections::HashMap<String, String>> {
    let merged = match resolve_ref(reference)? {
        WorkloadSource::File(path) => crate::extends::load_and_merge(&path).ok()?,
        WorkloadSource::Catalog { name, .. } => {
            crate::extends::load_and_merge_bundled(crate::catalog::lookup(&name)?).ok()?
        }
    };
    let doc: serde_yaml::Value = serde_yaml::from_str(&merged).ok()?;
    let params = doc.get("params")?.as_mapping()?;
    let mut out = std::collections::HashMap::new();
    for (k, v) in params {
        let Some(key) = k.as_str() else { continue };
        // Only scalar params shape the display decision; skip nested
        // structures. Numbers / bools render to their lexical form.
        let val = match v {
            serde_yaml::Value::String(s) => s.clone(),
            serde_yaml::Value::Number(n) => n.to_string(),
            serde_yaml::Value::Bool(b) => b.to_string(),
            _ => continue,
        };
        out.insert(key.to_string(), val);
    }
    Some(out)
}

/// Aggregate verification result across one or more files.
#[derive(Default)]
pub struct VerifySummary {
    pub passed: usize,
    pub skipped: Vec<String>,
    pub failures: Vec<String>,
}

/// Verify a workload file or every `*.yaml` under a directory (recursively).
/// Files run concurrently (cases within a file are sequential).
pub fn verify_path(binary: &Path, path: &Path, sandbox: &Path) -> VerifySummary {
    let _ = std::fs::create_dir_all(sandbox);
    let mut files: Vec<PathBuf> = Vec::new();
    if path.is_dir() {
        collect_yaml(path, &mut files);
        files.sort();
    } else {
        files.push(path.to_path_buf());
    }

    let acc: std::sync::Mutex<VerifySummary> = std::sync::Mutex::new(VerifySummary::default());
    // Work-stealing over `files`: a shared atomic cursor each worker pulls from
    // when it frees up. Static chunking serialized the slow demos (settle /
    // servo workloads that dwell on real-time metric windows) behind one chunk
    // while other chunks idled; a shared queue runs them concurrently, so the
    // wall-clock floor is the single slowest file, not a chunk-sum. Workers
    // spend almost all their time blocked on the child `nbrs` process, so we
    // oversubscribe past core count (2× cores, capped) to overlap the waits.
    let next = std::sync::atomic::AtomicUsize::new(0);
    let workers = files.len().min(
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .saturating_mul(2)
            .clamp(1, 16),
    );
    std::thread::scope(|s| {
        for _ in 0..workers {
            s.spawn(|| loop {
                let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let Some(f) = files.get(i) else { break };
                let label = f.file_name().and_then(|n| n.to_str()).unwrap_or("?").to_string();
                // Run the file (a sequence of cases) outside the lock, then
                // fold its outcomes under a single lock acquisition.
                let outcomes = verify_file(binary, &label, f, sandbox);
                let mut g = acc.lock().unwrap();
                for (lbl, outcome) in outcomes {
                    match outcome {
                        Outcome::Pass => g.passed += 1,
                        Outcome::Skip(r) => g.skipped.push(format!("{lbl}: {r}")),
                        Outcome::Fail(m) => g.failures.push(m),
                    }
                }
            });
        }
    });
    let mut sum = acc.into_inner().unwrap();
    sum.skipped.sort();
    sum.failures.sort();
    sum
}

/// Verify a target named the way `nbrs run` names workloads: a directory (walk
/// every workload under it), an existing workload file, or a bundled catalog
/// name (`examples/cursors/all_cursor/enumerate`, …). This is the `nbrs check` entry point — so
/// anything the binary can `run` by name, it can `check` by the same name.
pub fn verify_target(binary: &Path, target: &str, sandbox: &Path) -> VerifySummary {
    let p = Path::new(target);
    if p.is_dir() {
        return verify_path(binary, p, sandbox);
    }
    let _ = std::fs::create_dir_all(sandbox);
    let cases: Vec<(String, Outcome)> = match resolve_ref(target) {
        Some(WorkloadSource::File(path)) => {
            let label = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or(target)
                .to_string();
            verify_file(binary, &label, &path, sandbox)
        }
        Some(WorkloadSource::Catalog { name, source }) => {
            verify_source(binary, &name, &name, &source, sandbox)
        }
        None => vec![(
            target.to_string(),
            Outcome::Fail(format!(
                "no such workload '{target}': not a local file, not a directory, and \
                 no bundled workload by that name (try `nbrs describe workloads --all`)"
            )),
        )],
    };
    let mut sum = VerifySummary::default();
    for (lbl, outcome) in cases {
        match outcome {
            Outcome::Pass => sum.passed += 1,
            Outcome::Skip(r) => sum.skipped.push(format!("{lbl}: {r}")),
            Outcome::Fail(m) => sum.failures.push(m),
        }
    }
    sum
}

fn collect_yaml(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for e in entries.flatten() {
        let p = e.path();
        if p.is_dir() {
            collect_yaml(&p, out);
        } else if p.extension().is_some_and(|x| x == "yaml" || x == "yml") {
            out.push(p);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn comment_and_yaml_forms_are_equivalent() {
        let comment = "ops: { a: { raw: x } }\n#@ run cycles=3\n#@ expect 0 failed\n";
        let single = "ops: { a: { raw: x } }\nverify: { run: cycles=3, expect: \"0 failed\" }\n";
        let listed = "ops: { a: { raw: x } }\nverify:\n  - { run: cycles=3, expect: \"0 failed\" }\n";
        let named = "ops: { a: { raw: x } }\nverify:\n  smoke: { run: cycles=3, expect: \"0 failed\" }\n";
        for src in [comment, single, listed, named] {
            let p = VerifyPlan::parse(src).expect("parse");
            assert_eq!(p.cases.len(), 1, "one case for: {src}");
            assert_eq!(p.cases[0].run_args, vec!["cycles=3"], "run for: {src}");
            assert_eq!(p.cases[0].expects.len(), 1, "expect for: {src}");
        }
    }

    #[test]
    fn name_keyed_map_yields_named_cases() {
        let src = "verify:\n  alpha: { run: scenario=a, expect: \"x\" }\n  beta: { expect: [\"y\", \"z\"] }\n";
        let p = VerifyPlan::parse(src).unwrap();
        let names: Vec<&str> = p.cases.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"alpha") && names.contains(&"beta"), "names: {names:?}");
        let beta = p.cases.iter().find(|c| c.name == "beta").unwrap();
        assert_eq!(beta.expects.len(), 2);
    }

    #[test]
    fn requires_block_skips_the_file() {
        let p = VerifyPlan::parse("verify: { requires: needs a backend }\n").unwrap();
        assert_eq!(p.requires.as_deref(), Some("needs a backend"));
        assert!(p.cases.is_empty());
    }

    #[test]
    fn comment_and_block_cases_combine() {
        let src = "#@ case fromcomment\n#@   expect a\nverify:\n  fromblock: { expect: b }\n";
        let p = VerifyPlan::parse(src).unwrap();
        let names: Vec<&str> = p.cases.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"fromcomment") && names.contains(&"fromblock"), "{names:?}");
    }

    #[test]
    fn resolve_ref_finds_files_and_rejects_unknown_names() {
        // An on-disk file resolves to an absolute `File` source.
        let dir = std::env::temp_dir().join(format!("nbrs-verify-resolve-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let file = dir.join("w.yaml");
        std::fs::write(&file, "ops: { a: { raw: x } }\n").unwrap();
        match resolve_ref(file.to_str().unwrap()) {
            Some(WorkloadSource::File(p)) => assert!(p.is_absolute(), "absolute: {p:?}"),
            other => panic!("expected File, got {}", matches!(other, Some(WorkloadSource::Catalog { .. })) as i32),
        }
        // A name that is neither a file nor (in this test process) a bundled
        // workload resolves to nothing — the CLI reports it as not found.
        assert!(resolve_ref("definitely/not/a/workload").is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
