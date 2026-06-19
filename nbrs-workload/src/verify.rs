// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Workload verification — run a workload and check its output against rules
//! embedded in the workload file. Shared by the `nbrs check` subcommand and the
//! example-walker test, so "how CI checks the examples" and "how a user checks
//! their own workload" are the same code.
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
pub fn run_case(binary: &Path, workload: &Path, sandbox: &Path, label: &str, case: &VerifyCase) -> Result<(), String> {
    let session = sandbox.join(format!("session-{}", label.replace(['/', ' ', ':'], "_")));
    let _ = std::fs::remove_dir_all(&session);
    let output = Command::new("timeout")
        .arg(case.timeout.to_string())
        .arg(binary)
        .arg("run")
        .arg(format!("workload={}", workload.display()))
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

/// Verify one workload file: parse its rules, run every case, return one
/// `(label, Outcome)` per case (or a single Skip / Fail for the whole file).
pub fn verify_file(binary: &Path, label_root: &str, workload: &Path, sandbox: &Path) -> Vec<(String, Outcome)> {
    let src = match std::fs::read_to_string(workload) {
        Ok(s) => s,
        Err(e) => return vec![(label_root.to_string(), Outcome::Fail(format!("read error: {e}")))],
    };
    let plan = match VerifyPlan::parse(&src) {
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
            let outcome = match run_case(binary, workload, sandbox, &label, c) {
                Ok(()) => Outcome::Pass,
                Err(e) => Outcome::Fail(format!("{label}: {e}")),
            };
            (label, outcome)
        })
        .collect()
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
    let par = std::thread::available_parallelism()
        .map(|n| n.get().saturating_sub(1).clamp(1, 8))
        .unwrap_or(4);
    let chunks: Vec<&[PathBuf]> = if files.is_empty() {
        Vec::new()
    } else {
        files.chunks(files.len().div_ceil(par)).collect()
    };
    std::thread::scope(|s| {
        for chunk in &chunks {
            s.spawn(|| {
                for f in *chunk {
                    let label = f.file_name().and_then(|n| n.to_str()).unwrap_or("?").to_string();
                    for (lbl, outcome) in verify_file(binary, &label, f, sandbox) {
                        let mut g = acc.lock().unwrap();
                        match outcome {
                            Outcome::Pass => g.passed += 1,
                            Outcome::Skip(r) => g.skipped.push(format!("{lbl}: {r}")),
                            Outcome::Fail(m) => g.failures.push(m),
                        }
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
}
