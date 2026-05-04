// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `nbrs metrics list [<expr>]` and `nbrs metrics show [<expr>]`
//!
//! Reads `metric_instance` rows from the active session's
//! `metrics.db` and renders them as a hierarchical tree keyed
//! on metric family then label dimensions in declaration order.
//! `show` adds a one-line summary of the most recent
//! `sample_value` row per instance.
//!
//! Filter expression accepts:
//!   - bare glob on the family name: `recall*`
//!   - OpenMetrics-style label match: `recall@10.mean{k="10"}`
//!   - substring filter via `~`: `{profile=~label}` or
//!     `recall{profile=~label}`
//!
//! Honors the `--session <path>` umbrella (consumed at startup
//! via `apply_session_directory_at_startup`); the active db
//! defaults to `logs/latest/metrics.db`. `--db <path>` overrides.

use std::collections::BTreeMap;
use std::path::PathBuf;

pub fn metrics_command(args: &[String]) {
    let sub = args.first().map(String::as_str);
    let rest = args.get(1..).unwrap_or(&[]);
    match sub {
        Some("list") => list(rest, false),
        Some("show") => list(rest, true),
        Some("match") => match_specs(rest),
        Some("query") => crate::metricsql_cmd::query(rest),
        Some("watch") => crate::metricsql_cmd::watch(rest),
        Some(other) => {
            eprintln!("nbrs metrics: unknown subcommand '{other}'");
            print_metrics_usage();
            std::process::exit(2);
        }
        None => {
            eprintln!("nbrs metrics: missing subcommand");
            print_metrics_usage();
            std::process::exit(2);
        }
    }
}

fn print_metrics_usage() {
    eprintln!();
    eprintln!("Usage:");
    eprintln!("  nbrs metrics list  [<expr>]  List metric families + dimensions");
    eprintln!("                               as a tree.");
    eprintln!("  nbrs metrics show  [<expr>]  Same as `list` plus a one-line");
    eprintln!("                               value summary at each leaf.");
    eprintln!("  nbrs metrics match  <expr>   Flat list of full");
    eprintln!("                               `family{{labels}}` specs that");
    eprintln!("                               match — copy-paste into other");
    eprintln!("                               commands or sanity-check a");
    eprintln!("                               filter pattern.");
    eprintln!("  nbrs metrics query  <expr>   Evaluate a metricsql");
    eprintln!("                               expression against the db.");
    eprintln!("                               Run `nbrs metrics query` with");
    eprintln!("                               no args for full flag list.");
    eprintln!("  nbrs metrics watch  <expr>   Live-update a metricsql");
    eprintln!("                               expression on a polling");
    eprintln!("                               interval. Uses the streaming");
    eprintln!("                               engine when supported, batch");
    eprintln!("                               eval otherwise.");
    eprintln!();
    eprintln!("Filter expressions:");
    eprintln!("  recall*                      Family-name glob.");
    eprintln!("  recall@10.mean{{k=\"10\"}}      OpenMetrics-style label match.");
    eprintln!("  {{profile=~label}}             Substring filter.");
    eprintln!();
    eprintln!("Source selection:");
    eprintln!("  --db <path>                  Override metrics db.");
    eprintln!("                               Default: logs/latest/metrics.db");
    eprintln!("  --session <path-or-name>     SRD-04 umbrella; redirects");
    eprintln!("                               logs/latest before reading.");
}

/// `nbrs metrics match <expr>` — print a flat list of every
/// `family{labels}` spec that matches the filter, one per line.
/// Unlike `list`/`show` (which group hierarchically by label
/// dimension), `match` preserves the spec verbatim so the
/// output round-trips into other commands that take a fully
/// qualified metric instance reference.
fn match_specs(args: &[String]) {
    let mut db_path: Option<PathBuf> = None;
    let mut filter_expr: Option<String> = None;
    let mut iter = args.iter();
    while let Some(a) = iter.next() {
        match a.as_str() {
            "--db" => { db_path = iter.next().map(PathBuf::from); }
            other if other.starts_with("--db=") => {
                db_path = Some(PathBuf::from(&other[5..]));
            }
            "--session" | "--session-name" | "--session-path"
            | "--session-reuse" | "--session-keep" | "--session-shelflife" => {
                let _ = iter.next();
            }
            other if other.starts_with("--session") => {}
            other if !other.starts_with("--") => {
                filter_expr = Some(other.to_string());
            }
            other => {
                eprintln!("nbrs metrics match: unknown flag '{other}'");
                std::process::exit(2);
            }
        }
    }
    let Some(expr) = filter_expr else {
        eprintln!("nbrs metrics match: pattern required");
        eprintln!("  e.g. `nbrs metrics match 'recall*'`");
        eprintln!("       `nbrs metrics match 'recall@10.mean{{k=\"10\"}}'`");
        std::process::exit(2);
    };
    let filter = match parse_filter(&expr) {
        Ok(f) => f,
        Err(e) => { eprintln!("nbrs metrics match: filter: {e}"); std::process::exit(2); }
    };
    let db = db_path.unwrap_or_else(|| PathBuf::from("logs/latest/metrics.db"));
    if !db.exists() {
        eprintln!("nbrs metrics match: db not found at '{}'", db.display());
        std::process::exit(2);
    }
    let conn = match rusqlite::Connection::open(&db) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("nbrs metrics match: open '{}': {e}", db.display());
            std::process::exit(2);
        }
    };
    let mut stmt = match conn.prepare(
        "SELECT spec FROM metric_instance ORDER BY spec"
    ) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("nbrs metrics match: query: {e}");
            std::process::exit(2);
        }
    };
    let rows: Vec<String> = stmt.query_map([], |r| r.get::<_, String>(0))
        .map(|it| it.flatten().collect())
        .unwrap_or_default();

    let mut count = 0;
    for spec in &rows {
        let (family, labels) = split_spec(spec);
        if filter.matches(&family, &labels) {
            println!("{spec}");
            count += 1;
        }
    }
    eprintln!("# {} match{} ({} total instances)",
        count,
        if count == 1 { "" } else { "es" },
        rows.len(),
    );
}

fn list(args: &[String], show_values: bool) {
    let mut db_path: Option<PathBuf> = None;
    let mut filter_expr: Option<String> = None;
    let mut iter = args.iter();
    while let Some(a) = iter.next() {
        match a.as_str() {
            "--db" => { db_path = iter.next().map(PathBuf::from); }
            other if other.starts_with("--db=") => {
                db_path = Some(PathBuf::from(&other[5..]));
            }
            // Globals consumed at startup.
            "--session" | "--session-name" | "--session-path"
            | "--session-reuse" | "--session-keep" | "--session-shelflife" => {
                let _ = iter.next();
            }
            other if other.starts_with("--session") => {}
            other if !other.starts_with("--") => {
                filter_expr = Some(other.to_string());
            }
            other => {
                eprintln!("nbrs metrics: unknown flag '{other}'");
                std::process::exit(2);
            }
        }
    }
    let db = db_path.unwrap_or_else(|| PathBuf::from("logs/latest/metrics.db"));
    if !db.exists() {
        eprintln!("nbrs metrics: db not found at '{}'", db.display());
        std::process::exit(2);
    }
    let conn = match rusqlite::Connection::open(&db) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("nbrs metrics: open '{}': {e}", db.display());
            std::process::exit(2);
        }
    };
    let filter = filter_expr.as_deref().map(parse_filter).transpose();
    let filter = match filter {
        Ok(f) => f,
        Err(e) => { eprintln!("nbrs metrics: filter: {e}"); std::process::exit(2); }
    };
    let mut stmt = match conn.prepare(
        "SELECT mi.id, mi.spec FROM metric_instance mi ORDER BY mi.spec"
    ) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("nbrs metrics: query: {e}");
            std::process::exit(2);
        }
    };
    let rows: Vec<(i64, String)> = stmt.query_map([], |r| {
        Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
    }).map(|it| it.flatten().collect()).unwrap_or_default();

    // Bucket by family then by sorted label tuple.
    let mut tree: BTreeMap<String, BTreeMap<Vec<(String, String)>, i64>> = BTreeMap::new();
    for (id, spec) in &rows {
        let (family, labels) = split_spec(spec);
        if let Some(f) = filter.as_ref() {
            if !f.matches(&family, &labels) { continue; }
        }
        let mut sorted = labels.clone();
        sorted.sort();
        tree.entry(family).or_default().insert(sorted, *id);
    }

    if tree.is_empty() {
        eprintln!("(no metrics{})", match filter_expr {
            Some(ref e) => format!(" matching '{e}'"),
            None => String::new(),
        });
        return;
    }

    println!("# {} ({} famil{}, {} instance{})",
        db.display(),
        tree.len(),
        if tree.len() == 1 { "y" } else { "ies" },
        rows.len(),
        if rows.len() == 1 { "" } else { "s" },
    );
    for (family, instances) in &tree {
        println!();
        // Collapse dimensions that have a single shared value
        // across every instance under this family — they're
        // not adding information to the tree, just noise.
        // Print them once at the family-level header.
        let label_sets: Vec<Vec<(String, String)>> = instances.keys().cloned().collect();
        let (constant_dims, varying_label_sets) = factor_constant_dims(&label_sets);
        if constant_dims.is_empty() {
            println!("{family}");
        } else {
            let const_str = constant_dims.iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>().join(", ");
            println!("{family}  [{const_str}]");
        }
        // Reduce instances map to the constant-stripped key set.
        let varying_instances: BTreeMap<Vec<(String, String)>, i64> = instances.iter()
            .zip(varying_label_sets.iter())
            .map(|((_, id), labels)| (labels.clone(), *id))
            .collect();
        let dim_tree = build_dim_tree(varying_label_sets);
        print_dim_tree(&dim_tree, "  ", &varying_instances, &conn, show_values);
    }
}

/// Split a list of label sets into (constant dims, per-set
/// varying dims). A "constant dim" is a `(key, value)` pair
/// shared by every input set; those are factored out so the
/// tree depth reflects only the dimensions that actually vary.
fn factor_constant_dims(
    label_sets: &[Vec<(String, String)>],
) -> (Vec<(String, String)>, Vec<Vec<(String, String)>>) {
    if label_sets.is_empty() { return (Vec::new(), Vec::new()); }
    let first: BTreeMap<String, String> = label_sets[0].iter().cloned().collect();
    let mut shared: BTreeMap<String, String> = first;
    for set in &label_sets[1..] {
        let cur: BTreeMap<String, String> = set.iter().cloned().collect();
        shared.retain(|k, v| cur.get(k) == Some(v));
        if shared.is_empty() { break; }
    }
    let const_keys: std::collections::HashSet<String> = shared.keys().cloned().collect();
    let varying: Vec<Vec<(String, String)>> = label_sets.iter()
        .map(|s| s.iter()
            .filter(|(k, _)| !const_keys.contains(k))
            .cloned()
            .collect())
        .collect();
    let constant: Vec<(String, String)> = shared.into_iter().collect();
    (constant, varying)
}

#[derive(Debug, Clone, PartialEq)]
struct LabelMatcher {
    /// Family glob pattern (`*` allowed); `None` means match-all.
    family: Option<String>,
    /// Per-label match: equals, regex/substring (`~`), or
    /// just-presence.
    labels: Vec<(String, LabelMatch)>,
}

#[derive(Debug, Clone, PartialEq)]
enum LabelMatch {
    Equals(String),
    Substring(String),
}

impl LabelMatcher {
    fn matches(&self, family: &str, labels: &[(String, String)]) -> bool {
        if let Some(g) = self.family.as_deref()
            && !glob_matches(g, family) { return false; }
        for (k, want) in &self.labels {
            let v = labels.iter().find(|(lk, _)| lk == k).map(|(_, v)| v);
            let ok = match (v, want) {
                (Some(v), LabelMatch::Equals(e)) => v == e,
                (Some(v), LabelMatch::Substring(s)) => v.contains(s),
                (None, _) => false,
            };
            if !ok { return false; }
        }
        true
    }
}

/// Parse a metric filter expression. Accepted shapes:
///   - `family_glob`
///   - `family_glob{label="value", label2=~substring}`
///   - `{label="value"}` (label-only filter, any family)
fn parse_filter(expr: &str) -> Result<LabelMatcher, String> {
    let expr = expr.trim();
    let (family_part, labels_part) = match expr.find('{') {
        Some(i) => (expr[..i].trim(), Some(expr[i..].to_string())),
        None => (expr, None),
    };
    let family = if family_part.is_empty() { None } else { Some(family_part.to_string()) };
    let mut labels: Vec<(String, LabelMatch)> = Vec::new();
    if let Some(lp) = labels_part {
        let lp = lp.trim();
        let inner = lp.strip_prefix('{').and_then(|s| s.strip_suffix('}'))
            .ok_or_else(|| "label block must be `{...}`".to_string())?;
        for raw in inner.split(',') {
            let raw = raw.trim();
            if raw.is_empty() { continue; }
            let (key, op_val) = if let Some((k, v)) = raw.split_once("=~") {
                (k.trim().to_string(), LabelMatch::Substring(unquote(v.trim()).to_string()))
            } else if let Some((k, v)) = raw.split_once('=') {
                (k.trim().to_string(), LabelMatch::Equals(unquote(v.trim()).to_string()))
            } else if let Some((k, v)) = raw.split_once('~') {
                (k.trim().to_string(), LabelMatch::Substring(unquote(v.trim()).to_string()))
            } else {
                return Err(format!("label clause '{raw}': expected `key=value` or `key=~substring`"));
            };
            labels.push((key, op_val));
        }
    }
    Ok(LabelMatcher { family, labels })
}

fn unquote(s: &str) -> &str {
    s.strip_prefix('"').and_then(|x| x.strip_suffix('"'))
        .or_else(|| s.strip_prefix('\'').and_then(|x| x.strip_suffix('\'')))
        .unwrap_or(s)
}

fn glob_matches(glob: &str, name: &str) -> bool {
    fn rec(g: &[u8], n: &[u8]) -> bool {
        match (g.first(), n.first()) {
            (None, None) => true,
            (Some(b'*'), _) => {
                if rec(&g[1..], n) { return true; }
                if !n.is_empty() && rec(g, &n[1..]) { return true; }
                false
            }
            (Some(b'?'), Some(_)) => rec(&g[1..], &n[1..]),
            (Some(gc), Some(nc)) if gc == nc => rec(&g[1..], &n[1..]),
            _ => false,
        }
    }
    rec(glob.as_bytes(), name.as_bytes())
}

pub(crate) fn split_spec(spec: &str) -> (String, Vec<(String, String)>) {
    let (family, labels_text) = match spec.find('{') {
        Some(i) => (spec[..i].to_string(), &spec[i + 1..]),
        None => return (spec.to_string(), Vec::new()),
    };
    let inner = labels_text.strip_suffix('}').unwrap_or(labels_text);
    let mut out = Vec::new();
    let bytes = inner.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        while i < bytes.len() && matches!(bytes[i], b' ' | b'\t' | b',') { i += 1; }
        if i >= bytes.len() { break; }
        let key_start = i;
        while i < bytes.len() && bytes[i] != b'=' { i += 1; }
        if i >= bytes.len() { break; }
        let key = inner[key_start..i].trim().to_string();
        i += 1;
        if i < bytes.len() && bytes[i] == b'"' {
            i += 1;
            let vs = i;
            while i < bytes.len() && bytes[i] != b'"' { i += 1; }
            let val = inner[vs..i].to_string();
            if i < bytes.len() { i += 1; }
            out.push((key, val));
        } else {
            let vs = i;
            while i < bytes.len() && !matches!(bytes[i], b',') { i += 1; }
            out.push((key, inner[vs..i].trim().to_string()));
        }
    }
    (family, out)
}

/// One node in the dimensional tree. Ordered by label-key
/// occurrence so deeper levels reflect which dimensions vary.
#[derive(Debug, Default)]
struct DimNode {
    /// Distinct label-tuple paths reaching this node.
    leaves: Vec<Vec<(String, String)>>,
    /// Children keyed by `(label_key, label_value)`.
    children: BTreeMap<(String, String), DimNode>,
}

fn build_dim_tree(label_sets: Vec<Vec<(String, String)>>) -> DimNode {
    let mut root = DimNode::default();
    for ls in label_sets {
        insert_into_dim_tree(&mut root, &ls, 0);
    }
    root
}

fn insert_into_dim_tree(node: &mut DimNode, labels: &[(String, String)], depth: usize) {
    if depth >= labels.len() {
        node.leaves.push(labels.to_vec());
        return;
    }
    let (k, v) = &labels[depth];
    let child = node.children.entry((k.clone(), v.clone())).or_default();
    insert_into_dim_tree(child, labels, depth + 1);
}

fn print_dim_tree(
    node: &DimNode,
    indent: &str,
    instances: &BTreeMap<Vec<(String, String)>, i64>,
    conn: &rusqlite::Connection,
    show_values: bool,
) {
    let n_children = node.children.len();
    for (idx, ((k, v), child)) in node.children.iter().enumerate() {
        let is_last = idx + 1 == n_children;
        let connector = if is_last { "└── " } else { "├── " };
        let next_indent = if is_last {
            format!("{indent}    ")
        } else {
            format!("{indent}│   ")
        };
        // Leaf detection: any node with at least one leaf at
        // *this exact level* prints its summary inline.
        let inline_leaf: Option<&Vec<(String, String)>> = child.leaves.iter()
            .find(|ls| ls.last().map(|kv| kv == &(k.clone(), v.clone())).unwrap_or(false));

        if let Some(ls) = inline_leaf
            && child.children.is_empty() {
            let id = instances.get(ls).copied().unwrap_or(-1);
            let summary = if show_values {
                format!("  {}", value_summary(conn, id))
            } else {
                String::new()
            };
            println!("{indent}{connector}{k}={v}{summary}");
        } else {
            println!("{indent}{connector}{k}={v}");
            print_dim_tree(child, &next_indent, instances, conn, show_values);
        }
    }
}

fn value_summary(conn: &rusqlite::Connection, instance_id: i64) -> String {
    if instance_id < 0 { return String::new(); }
    let row: Result<(Option<f64>, Option<f64>, Option<f64>, Option<f64>, Option<f64>, Option<i64>), _> =
        conn.query_row(
            "SELECT mean, p50, p99, min, max, count
             FROM sample_value
             WHERE instance_id = ?1
             ORDER BY count DESC
             LIMIT 1",
            [instance_id],
            |r| Ok((
                r.get::<_, Option<f64>>(0)?,
                r.get::<_, Option<f64>>(1)?,
                r.get::<_, Option<f64>>(2)?,
                r.get::<_, Option<f64>>(3)?,
                r.get::<_, Option<f64>>(4)?,
                r.get::<_, Option<i64>>(5)?,
            )),
        );
    match row {
        Ok((mean, p50, p99, min, max, count)) => {
            let mut parts: Vec<String> = Vec::new();
            if let Some(c) = count { parts.push(format!("n={c}")); }
            if let Some(m) = mean { parts.push(format!("mean={m:.4}")); }
            if let Some(p) = p50 { parts.push(format!("p50={p:.4}")); }
            if let Some(p) = p99 { parts.push(format!("p99={p:.4}")); }
            if let (Some(mn), Some(mx)) = (min, max) {
                parts.push(format!("[{mn:.4}..{mx:.4}]"));
            }
            if parts.is_empty() { String::new() }
            else { format!("({})", parts.join(", ")) }
        }
        Err(_) => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_spec_basic() {
        let (f, l) = split_spec(r#"recall@10.mean{profile="label_03",k="10",limit="50"}"#);
        assert_eq!(f, "recall@10.mean");
        assert_eq!(l, vec![
            ("profile".into(), "label_03".into()),
            ("k".into(), "10".into()),
            ("limit".into(), "50".into()),
        ]);
    }

    #[test]
    fn parse_filter_family_only() {
        let m = parse_filter("recall*").unwrap();
        assert_eq!(m.family.as_deref(), Some("recall*"));
        assert!(m.labels.is_empty());
    }

    #[test]
    fn parse_filter_label_eq() {
        let m = parse_filter(r#"recall{k="10"}"#).unwrap();
        assert_eq!(m.family.as_deref(), Some("recall"));
        assert_eq!(m.labels.len(), 1);
        assert!(matches!(m.labels[0].1, LabelMatch::Equals(ref s) if s == "10"));
    }

    #[test]
    fn parse_filter_label_substring_em() {
        let m = parse_filter(r#"{profile=~label}"#).unwrap();
        assert!(m.family.is_none());
        assert!(matches!(m.labels[0].1, LabelMatch::Substring(ref s) if s == "label"));
    }

    #[test]
    fn parse_filter_label_substring_tilde_only() {
        let m = parse_filter("{profile~label}").unwrap();
        assert!(matches!(m.labels[0].1, LabelMatch::Substring(ref s) if s == "label"));
    }

    #[test]
    fn matcher_substring_matches() {
        let m = parse_filter(r#"{profile=~label}"#).unwrap();
        assert!(m.matches("any", &[("profile".into(), "label_03".into())]));
        assert!(!m.matches("any", &[("profile".into(), "default".into())]));
    }
}
