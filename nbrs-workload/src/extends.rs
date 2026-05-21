// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Workload `extends:` resolution and merging (SRD-72).
//!
//! Resolves single-parent workload composition: a child workload
//! YAML may declare `extends: <relative-path>` at the top level
//! to inherit from a parent. The parent is loaded (recursively,
//! if it has its own `extends:`) and merged field-by-field before
//! the resulting workload is fed to `parse::parse_workload`.
//!
//! The merge runs on parsed `serde_json::Value` trees, not on
//! raw YAML text — every merge rule is structural (per-key
//! merge, per-name replace, list union with dedup). The result
//! is re-serialised back to YAML so the existing
//! `parse_workload(&str, …)` entry point can consume it
//! unchanged; template expansion then runs on the merged text
//! with the caller's params, matching the SRD-72 rule that
//! validation and templating run **once**, on the merged whole.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use serde_json::Value as JVal;

/// Load a workload YAML from disk, follow its `extends:` chain
/// to completion, and return the merged YAML text ready for
/// `parse::parse_workload`.
///
/// The returned text has every `extends:` directive stripped.
pub fn load_and_merge(path: &Path) -> Result<String, String> {
    let mut chain: Vec<PathBuf> = Vec::new();
    let merged_jval = load_recursive(path, &mut chain)?;
    serde_yaml::to_string(&merged_jval)
        .map_err(|e| format!("re-serialising merged workload: {e}"))
}

/// Recursive loader: parses `path`, resolves any `extends:`,
/// applies merge rules. `chain` is the parent-chain of files
/// already being loaded, used for cycle detection.
fn load_recursive(path: &Path, chain: &mut Vec<PathBuf>) -> Result<JVal, String> {
    let canonical = path.canonicalize()
        .map_err(|e| format!(
            "extends: target not found: {} ({e})",
            path.display()
        ))?;

    if let Some(idx) = chain.iter().position(|p| p == &canonical) {
        return Err(format_cycle(chain, idx, &canonical));
    }

    chain.push(canonical.clone());

    let text = std::fs::read_to_string(&canonical)
        .map_err(|e| format!("read {}: {e}", canonical.display()))?;

    let mut jval: JVal = serde_yaml::from_str(&text)
        .map_err(|e| format!("YAML parse error in {}: {e}", canonical.display()))?;

    let extends_target = extract_extends_field(&jval, &canonical)?;

    let result = if let Some(extends_str) = extends_target {
        let parent_path = canonical.parent()
            .ok_or_else(|| format!(
                "{} has no parent directory to resolve `extends:` against",
                canonical.display()
            ))?
            .join(&extends_str);

        let parent_jval = load_recursive(&parent_path, chain)
            .map_err(|e| format!(
                "while loading {}'s parent {}: {e}",
                canonical.display(), parent_path.display()
            ))?;

        // Strip `extends:` from the child before merging so the
        // merge fn doesn't need to special-case it.
        if let Some(obj) = jval.as_object_mut() {
            obj.remove("extends");
        }

        merge(parent_jval, jval)
    } else {
        jval
    };

    chain.pop();
    Ok(result)
}

/// Extract `extends:` as a string. Returns `Ok(None)` if absent,
/// `Err` if present but malformed (non-string, empty, or nested
/// inside something other than the top-level mapping).
fn extract_extends_field(jval: &JVal, source: &Path) -> Result<Option<String>, String> {
    let Some(obj) = jval.as_object() else {
        return Err(format!(
            "{} top level must be a YAML mapping",
            source.display()
        ));
    };
    let Some(v) = obj.get("extends") else { return Ok(None); };
    match v {
        JVal::String(s) if !s.is_empty() => Ok(Some(s.clone())),
        JVal::String(_) => Err(format!(
            "{}: `extends:` value is empty",
            source.display()
        )),
        _ => Err(format!(
            "{}: `extends:` must be a single scalar string, got {}",
            source.display(),
            describe_kind(v)
        )),
    }
}

fn describe_kind(v: &JVal) -> &'static str {
    match v {
        JVal::Null => "null",
        JVal::Bool(_) => "bool",
        JVal::Number(_) => "number",
        JVal::String(_) => "string",
        JVal::Array(_) => "list",
        JVal::Object(_) => "mapping",
    }
}

fn format_cycle(chain: &[PathBuf], cycle_start_idx: usize, repeat: &Path) -> String {
    let mut out = String::from("extends: cycle detected\n");
    for (i, p) in chain.iter().enumerate() {
        let arrow = if i == 0 { "  " } else { "  → " };
        out.push_str(&format!("{arrow}{}\n", p.display()));
        let _ = cycle_start_idx; // referenced for clarity below
    }
    out.push_str(&format!("  → {}  (cycle)\n", repeat.display()));
    out
}

/// Merge a child workload onto an already-merged parent per the
/// SRD-72 per-field rules.
fn merge(parent: JVal, child: JVal) -> JVal {
    // Both should be objects in practice (extract_extends_field
    // already validated). Be defensive: a non-object child or
    // parent falls back to whichever is an object.
    let Some(mut merged) = parent.as_object().cloned() else { return child; };
    let Some(child_obj) = child.as_object() else { return JVal::Object(merged); };

    for (key, child_val) in child_obj {
        let parent_val = merged.remove(key);
        let new_val = match (key.as_str(), parent_val) {
            ("extends", _) => continue, // defensive — caller should have stripped
            ("description", _) => child_val.clone(),

            ("params", Some(p)) => merge_per_key(p, child_val.clone()),
            ("tags", Some(p))   => merge_per_key(p, child_val.clone()),

            ("bindings", Some(p)) => concat_bindings(p, child_val.clone()),

            ("status_metrics", Some(p)) => union_lists(p, child_val.clone()),

            ("report", Some(p))    => merge_per_name(p, child_val.clone()),
            ("scenarios", Some(p)) => merge_per_name(p, child_val.clone()),
            ("phases", Some(p))    => merge_per_name(p, child_val.clone()),
            ("blocks", Some(p))    => merge_per_name(p, child_val.clone()),

            ("ops", Some(p)) => merge_ops(p, child_val.clone()),

            (_, _) => child_val.clone(),
        };
        merged.insert(key.clone(), new_val);
    }

    JVal::Object(merged)
}

/// Per-key merge: child wins on conflict, new keys added.
fn merge_per_key(parent: JVal, child: JVal) -> JVal {
    let Some(mut p_map) = parent.as_object().cloned() else { return child; };
    let Some(c_map) = child.as_object() else { return JVal::Object(p_map); };
    for (k, v) in c_map {
        p_map.insert(k.clone(), v.clone());
    }
    JVal::Object(p_map)
}

/// Per-name merge: child entry replaces parent entry of same
/// name (whole-entry replace). Same shape as `merge_per_key`
/// but kept as a separate fn to make the intent explicit at
/// call sites — the contract differs (whole-entry replace is
/// stricter than per-key merge, even though the operation is
/// identical at this layer).
fn merge_per_name(parent: JVal, child: JVal) -> JVal {
    merge_per_key(parent, child)
}

/// Bindings concat: parent's GK source emitted first, child's
/// appended. Handles both the string form (the common case) and
/// the legacy map form. Mixed forms (one string, one map) fall
/// back to child-wins because there's no sensible concatenation.
fn concat_bindings(parent: JVal, child: JVal) -> JVal {
    match (&parent, &child) {
        (JVal::String(p), JVal::String(c)) => {
            let mut out = String::with_capacity(p.len() + c.len() + 1);
            out.push_str(p);
            if !p.ends_with('\n') { out.push('\n'); }
            out.push_str(c);
            JVal::String(out)
        }
        (JVal::Object(_), JVal::Object(_)) => merge_per_key(parent, child),
        _ => child,
    }
}

/// List union with first-occurrence ordering and dedup. Parent's
/// entries come first, child's appended; duplicates suppressed.
fn union_lists(parent: JVal, child: JVal) -> JVal {
    let Some(p_list) = parent.as_array().cloned() else { return child; };
    let Some(c_list) = child.as_array() else { return JVal::Array(p_list); };
    let mut seen: HashSet<String> = HashSet::new();
    let mut out: Vec<JVal> = Vec::with_capacity(p_list.len() + c_list.len());
    for v in p_list.into_iter().chain(c_list.iter().cloned()) {
        // dedup by serialised form so non-string list entries
        // (rare in status_metrics, but possible) still compare.
        let key = match &v {
            JVal::String(s) => s.clone(),
            other => other.to_string(),
        };
        if seen.insert(key) {
            out.push(v);
        }
    }
    JVal::Array(out)
}

/// Top-level `ops:` merge. If both forms are map-shaped, do a
/// per-name merge (child entry replaces parent entry of same
/// name). Otherwise (list form, mixed forms) the child wholly
/// replaces the parent — positions are not stable identifiers
/// so by-name override is impossible.
fn merge_ops(parent: JVal, child: JVal) -> JVal {
    match (&parent, &child) {
        (JVal::Object(_), JVal::Object(_)) => merge_per_name(parent, child),
        _ => child,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vstr(s: &str) -> JVal { JVal::String(s.to_string()) }
    fn arr(items: Vec<JVal>) -> JVal { JVal::Array(items) }

    fn mp(pairs: &[(&str, JVal)]) -> JVal {
        let mut m = serde_json::Map::new();
        for (k, v) in pairs {
            m.insert((*k).to_string(), v.clone());
        }
        JVal::Object(m)
    }

    #[test]
    fn per_key_merge_child_wins() {
        let p = mp(&[("a", vstr("p")), ("b", vstr("p"))]);
        let c = mp(&[("b", vstr("c")), ("d", vstr("c"))]);
        let merged = merge_per_key(p, c);
        let obj = merged.as_object().unwrap();
        assert_eq!(obj.get("a").unwrap(), &vstr("p"));
        assert_eq!(obj.get("b").unwrap(), &vstr("c"));
        assert_eq!(obj.get("d").unwrap(), &vstr("c"));
    }

    #[test]
    fn concat_bindings_string_form() {
        let merged = concat_bindings(
            vstr("a := 1"),
            vstr("b := 2"),
        );
        assert_eq!(merged, vstr("a := 1\nb := 2"));
    }

    #[test]
    fn concat_bindings_preserves_trailing_newline() {
        let merged = concat_bindings(
            vstr("a := 1\n"),
            vstr("b := 2"),
        );
        assert_eq!(merged, vstr("a := 1\nb := 2"));
    }

    #[test]
    fn union_lists_dedup_preserves_first_occurrence() {
        let merged = union_lists(
            arr(vec![vstr("a"), vstr("b"), vstr("c")]),
            arr(vec![vstr("b"), vstr("d")]),
        );
        assert_eq!(
            merged,
            arr(vec![vstr("a"), vstr("b"), vstr("c"), vstr("d")])
        );
    }

    #[test]
    fn merge_strips_extends() {
        let parent = mp(&[("description", vstr("parent"))]);
        let child  = mp(&[("extends", vstr("./p.yaml")), ("description", vstr("child"))]);
        let merged = merge(parent, child);
        let obj = merged.as_object().unwrap();
        assert!(obj.get("extends").is_none());
        assert_eq!(obj.get("description").unwrap(), &vstr("child"));
    }

    #[test]
    fn merge_phases_per_name_replace() {
        let parent = mp(&[("phases", mp(&[
            ("a", mp(&[("kind", vstr("p_a"))])),
            ("b", mp(&[("kind", vstr("p_b"))])),
        ]))]);
        let child = mp(&[("phases", mp(&[
            ("b", mp(&[("kind", vstr("c_b"))])),
            ("c", mp(&[("kind", vstr("c_c"))])),
        ]))]);
        let merged = merge(parent, child);
        let phases = merged.as_object().unwrap().get("phases").unwrap().as_object().unwrap();
        assert_eq!(phases.len(), 3);
        assert_eq!(phases.get("a").unwrap(), &mp(&[("kind", vstr("p_a"))]));
        assert_eq!(phases.get("b").unwrap(), &mp(&[("kind", vstr("c_b"))]));
        assert_eq!(phases.get("c").unwrap(), &mp(&[("kind", vstr("c_c"))]));
    }
}
