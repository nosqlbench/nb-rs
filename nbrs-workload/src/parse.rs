// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! YAML workload parser and normalizer.
//!
//! Parses a YAML workload definition and normalizes all shorthand
//! forms into the canonical `ParsedOp` model.

use std::collections::HashMap;
use serde_json::Value as JVal;
use nbrs_variates::comprehension::{parse_clause, parse_clause_list, parse_order_spec};
use crate::model::{
    BindingsDef, MetricSpec, ParsedOp, ScenarioNode, Workload, WorkloadPhase,
};
use crate::template::expand_templates;

/// Parse a YAML workload string into a normalized Workload.
pub fn parse_workload(yaml_source: &str, params: &HashMap<String, String>) -> Result<Workload, String> {
    // Stage 1: TEMPLATE expansion
    let expanded = expand_templates(yaml_source, params);

    // Stage 2: Parse YAML into generic Value
    let doc: JVal = serde_yaml::from_str(&expanded)
        .map_err(|e| format!("YAML parse error: {e}"))?;

    let obj = doc.as_object()
        .ok_or("workload must be a YAML mapping")?;

    // Stage 3: Extract top-level fields
    let description = obj.get("description")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let mut scenarios = parse_scenarios(obj.get("scenarios"));
    // Resolve `scenario: <name>` includes after every scenario
    // has been parsed so forward references work and cycles are
    // detected with the full graph available.
    resolve_scenario_includes(&mut scenarios)?;

    let doc_bindings = extract_bindings(obj.get("bindings"));
    let doc_params = extract_value_map(obj.get("params"));
    let doc_tags = extract_string_map(obj.get("tags"));

    // Stage 4: Parse ops from blocks or top-level
    let mut all_ops = Vec::new();

    if let Some(blocks_val) = obj.get("blocks") {
        parse_blocks(blocks_val, &doc_bindings, &doc_params, &doc_tags, &mut all_ops)?;
    }

    // Also check for top-level ops (no blocks)
    for key in ["ops", "op", "operations", "statements", "statement"] {
        if let Some(ops_val) = obj.get(key)
            && obj.get("blocks").is_none() {
                parse_ops_field(ops_val, "block0", &doc_bindings, &doc_params, &doc_tags, &mut all_ops)?;
            }
    }

    // Stage 5: Parse phases
    let (mut phases, phase_order) = parse_phases(obj.get("phases"), &doc_bindings, &doc_params, &doc_tags)?;

    // Stage 6: Auto-tag all ops (top-level and phase inline ops)
    for op in &mut all_ops {
        if !op.tags.contains_key("name") {
            op.tags.insert("name".to_string(), op.name.clone());
        }
        if !op.tags.contains_key("op") {
            op.tags.insert("op".to_string(), op.name.clone());
        }
    }

    // Stage 7: Resolve workload parameters
    // Priority: CLI params > workload defaults > env vars
    let yaml_params = extract_string_map(obj.get("params"));
    let mut resolved_params = HashMap::new();
    for (key, default_value) in &yaml_params {
        let resolved = if let Some(cli_value) = params.get(key) {
            // CLI override
            cli_value.clone()
        } else if let Some(env_name) = default_value.strip_prefix("env:") {
            // Environment variable lookup
            std::env::var(env_name).unwrap_or_else(|_| default_value.clone())
        } else {
            default_value.clone()
        };
        resolved_params.insert(key.clone(), resolved);
    }
    // Also include CLI params that aren't in the workload defaults
    // (ad-hoc parameters passed on the command line)
    for (key, value) in params {
        if !resolved_params.contains_key(key) {
            resolved_params.insert(key.clone(), value.clone());
        }
    }

    let declared_params: Vec<String> = yaml_params.keys().cloned().collect();

    // Legacy `summary:` and `plot:` keys: removed, no shim.
    // Operators must migrate to the unified `report:` block
    // (SRD-46). The error message names both new homes.
    if obj.contains_key("summary") || obj.contains_key("summaries") {
        return Err(
            "`summary:` / `summaries:` removed; use `report:` with \
             `table <name> ...` directives instead (SRD-46)".to_string()
        );
    }
    if obj.contains_key("plot") || obj.contains_key("plots") {
        return Err(
            "`plot:` / `plots:` removed; use `report:` with \
             `plot <name> ...` directives instead (SRD-46)".to_string()
        );
    }

    // Unified `report:` block (SRD-46) — plots, tables, defaults,
    // groups. Parser is in `crate::report::parse_report`. The
    // returned warnings are stashed on the Workload for
    // strict-mode promotion downstream (SRD-15).
    let (report, report_warnings) = if let Some(val) = obj.get("report") {
        let parsed = crate::report::parse_report(val)
            .map_err(|e| format!("report: {e}"))?;
        (parsed.report, parsed.warnings)
    } else {
        (crate::report::Report::default(), Vec::new())
    };

    // SRD 21 §"Parameter Resolution": CLI overrides are the
    // outermost layer. Each op has already absorbed the
    // doc → block → op closest-wins merge for YAML-declared
    // params; now overlay the CLI map so `nbrs run ...
    // concurrency=200` replaces any inherited block-level
    // value. Workload-level `resolved_params` was already
    // CLI-resolved above (line 66–87); this pass extends the
    // same rule down to per-op params.
    if !params.is_empty() {
        for op in &mut all_ops {
            for (key, value) in params {
                op.params.insert(key.clone(), serde_json::Value::String(value.clone()));
            }
        }
    }

    // SRD-44 §"Resume protocol" + design memo
    // `workload_checkpointing.md`: a phase declared
    // `checkpoint: idempotent` that lives inside a do_while /
    // do_until loop is rejected at workload load time. The
    // do-loop iterates the same phase many times under one
    // checkpoint identity; checkpointing presumes each phase
    // execution is a discrete unit, which the loop directly
    // contradicts. Operators who really want a do-loop'd phase
    // to skip on resume must wrap the loop with explicit
    // identity, not lean on the phase's `checkpoint:` flag.
    for (scenario_name, nodes) in &scenarios {
        let mut bad: Vec<String> = Vec::new();
        collect_idempotent_under_do_loop(nodes, false, &phases, &mut bad);
        if !bad.is_empty() {
            return Err(format!(
                "scenario '{scenario_name}': phase{plural} {names} \
                 declared `checkpoint: idempotent` while nested inside \
                 a do_while / do_until loop. The loop iterates the \
                 same phase identity multiple times, which contradicts \
                 the per-execution unit checkpointing assumes. Either \
                 remove the `checkpoint:` declaration or restructure \
                 the loop. (SRD-44 §\"Resume protocol\".)",
                plural = if bad.len() == 1 { "" } else { "s" },
                names = bad.join(", "),
            ));
        }
    }

    // Doc-root `status_metrics:` — workload-wide default that
    // any phase without its own `status_metrics:` inherits.
    // Same accept-list shapes as the per-phase parser: list of
    // strings, single string, or comma-separated string.
    let doc_status_metrics: Vec<String> = match obj.get("status_metrics") {
        None => Vec::new(),
        Some(JVal::Array(items)) => items.iter()
            .filter_map(|v| v.as_str().map(|s| s.trim().to_string()))
            .filter(|s| !s.is_empty())
            .collect(),
        Some(JVal::String(s)) => s.split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect(),
        Some(other) => return Err(format!(
            "status_metrics: must be a list of names/patterns, a \
             comma-separated string, or omitted; got {other:?}"
        )),
    };
    if !doc_status_metrics.is_empty() {
        for phase in phases.values_mut() {
            if phase.status_metrics.is_empty() {
                phase.status_metrics = doc_status_metrics.clone();
            }
        }
    }

    // Doc-root `readouts:` block (SRD-63 §5.0). Three
    // accepted shapes:
    //   A. Single scalar string  → bound at on_update.
    //   B. Mapping of slot → name | body string.
    //   C. Mapping of slot → list of (name | body) strings.
    // The slot keys must match the lower-cased
    // Event::slot_name values (`on_update`, `on_phase_end`, …).
    // Inline body strings keep their full text — the
    // body-grammar parser in nbrs-activity::readouts::parse
    // bakes them at activity-init time.
    let readouts = parse_readouts_block(obj.get("readouts"))?;

    Ok(Workload {
        description, scenarios, ops: all_ops, bindings: doc_bindings,
        params: resolved_params, phases, phase_order, declared_params,
        report, report_warnings,
        status_metrics: doc_status_metrics,
        readouts,
    })
}

/// Parse the workload's `readouts:` block per SRD-63 §5.0.
/// Returns the populated bindings struct or a load-time
/// error.
fn parse_readouts_block(value: Option<&JVal>) -> Result<crate::model::ReadoutsBindings, String> {
    use crate::model::ReadoutsBindings;
    let mut out = ReadoutsBindings::default();
    let Some(value) = value else {
        return Ok(out);
    };

    // Form A — scalar shorthand for `on_update`.
    if let JVal::String(s) = value {
        let trimmed = s.trim();
        if trimmed.is_empty() {
            return Ok(out);
        }
        out.on_update.push(trimmed.to_string());
        return Ok(out);
    }

    // Forms B / C — mapping with slot keys.
    let JVal::Object(map) = value else {
        return Err(format!(
            "readouts: must be a scalar (sugar for on_update) or a mapping \
             of slot name → readout body; got {value:?}"
        ));
    };
    for (key, val) in map {
        let bodies: Vec<String> = match val {
            JVal::String(s) => vec![s.trim().to_string()],
            JVal::Array(items) => items.iter()
                .map(|item| match item {
                    JVal::String(s) => Ok(s.trim().to_string()),
                    other => Err(format!(
                        "readouts.{key}: list entries must be strings; got {other:?}"
                    )),
                })
                .collect::<Result<Vec<_>, _>>()?,
            JVal::Null => continue,
            other => return Err(format!(
                "readouts.{key}: must be a string or list of strings; got {other:?}"
            )),
        };
        let bodies: Vec<String> = bodies.into_iter()
            .filter(|s| !s.is_empty())
            .collect();
        // Wildcard expansion (SRD-63 §4.1.1). The yaml key
        // can match a family rather than a single slot:
        //   `each_*`    → on_each_start + on_each_end
        //   `phase_*`   → on_phase_start + on_phase_end
        //   `scope_*`   → on_scope_start + on_scope_end
        //   `session_*` → on_session_start + on_session_end
        //   `*`         → every slot
        // Wildcard bindings are duplicated into each
        // matching slot so the binder's resolution doesn't
        // need a separate wildcard list. Render order:
        // explicit bindings first in declaration order
        // (handled by parser top-down), then any wildcard
        // expansions appended.
        let target_slots: Vec<&str> = match key.as_str() {
            "on_session_start" => vec!["on_session_start"],
            "on_session_end"   => vec!["on_session_end"],
            "on_phase_start"   => vec!["on_phase_start"],
            "on_phase_end"     => vec!["on_phase_end"],
            "on_each_start"    => vec!["on_each_start"],
            "on_each_end"      => vec!["on_each_end"],
            "on_scope_start"   => vec!["on_scope_start"],
            "on_scope_end"     => vec!["on_scope_end"],
            "on_update"        => vec!["on_update"],
            "session_*"        => vec!["on_session_start", "on_session_end"],
            "phase_*"          => vec!["on_phase_start",   "on_phase_end"],
            "each_*"           => vec!["on_each_start",    "on_each_end"],
            "scope_*"          => vec!["on_scope_start",   "on_scope_end"],
            "*"                => vec![
                "on_session_start", "on_session_end",
                "on_phase_start",   "on_phase_end",
                "on_each_start",    "on_each_end",
                "on_scope_start",   "on_scope_end",
                "on_update",
            ],
            other => return Err(format!(
                "readouts: unknown slot '{other}'. Known: \
                 on_session_start/end, on_phase_start/end, \
                 on_each_start/end, on_scope_start/end, on_update; \
                 wildcards: each_*, phase_*, scope_*, session_*, *"
            )),
        };
        for slot in target_slots {
            let target: &mut Vec<String> = match slot {
                "on_session_start" => &mut out.on_session_start,
                "on_session_end"   => &mut out.on_session_end,
                "on_phase_start"   => &mut out.on_phase_start,
                "on_phase_end"     => &mut out.on_phase_end,
                "on_each_start"    => &mut out.on_each_start,
                "on_each_end"      => &mut out.on_each_end,
                "on_scope_start"   => &mut out.on_scope_start,
                "on_scope_end"     => &mut out.on_scope_end,
                "on_update"        => &mut out.on_update,
                _ => unreachable!(),
            };
            target.extend(bodies.iter().cloned());
        }
    }
    Ok(out)
}

/// Walk a scenario tree and collect names of phases declared
/// `checkpoint: idempotent` that live under a `do_while` /
/// `do_until` ancestor. Used by the workload-load validation
/// step above (SRD-44).
fn collect_idempotent_under_do_loop(
    nodes: &[crate::model::ScenarioNode],
    in_do_loop: bool,
    phases: &HashMap<String, crate::model::WorkloadPhase>,
    out: &mut Vec<String>,
) {
    use crate::model::ScenarioNode;
    for node in nodes {
        match node {
            ScenarioNode::Phase(name) => {
                if in_do_loop {
                    let idempotent = phases.get(name)
                        .and_then(|p| p.checkpoint.as_ref())
                        .map(|c| c.idempotent)
                        .unwrap_or(false);
                    if idempotent {
                        out.push(format!("'{name}'"));
                    }
                }
            }
            ScenarioNode::DoWhile { children, .. }
            | ScenarioNode::DoUntil { children, .. } => {
                collect_idempotent_under_do_loop(children, true, phases, out);
            }
            ScenarioNode::Comprehension { children, .. } => {
                collect_idempotent_under_do_loop(children, in_do_loop, phases, out);
            }
            ScenarioNode::IncludedScenario { children, .. } => {
                collect_idempotent_under_do_loop(children, in_do_loop, phases, out);
            }
        }
    }
}

/// Parse a YAML source into just the list of normalized ParsedOps.
/// Normalise an op-level `if:` clause so callers can write
/// expressions naturally. The downstream pipeline expects
/// the condition to be either a binding-name reference
/// (`{name}`) or an inline expression (`{{expr}}`). When
/// the operator writes a bare expression like
/// `cql_dialect == 'cass'`, that doesn't match either
/// form and the conditional dispenser fails at init when
/// it tries to look up a GK binding literally named
/// `cql_dialect == 'cass'`.
///
/// Heuristic: if the trimmed value already starts with `{`
/// (any braced form), or is a plain identifier
/// (`[A-Za-z_][A-Za-z0-9_]*`), pass through unchanged.
/// Otherwise treat as an inline expression and wrap with
/// `{{...}}` so the existing inline-expression machinery
/// in `nbrs-activity::scope::build_scope` synthesises a
/// hidden binding (`__expr_N := <expr>`) and rewrites the
/// condition to reference it.
pub fn normalize_condition_clause(s: &str) -> String {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return trimmed.to_string();
    }
    // Already in any braced form — pass through. The
    // downstream extractor handles `{{expr}}`, `{:=expr:=}`,
    // `{name}`, and `{expr-with-operators}` itself.
    if trimmed.starts_with('{') {
        return trimmed.to_string();
    }
    // Plain identifier — leave as-is so the legacy "if:
    // points at a single binding name" form keeps working.
    let is_plain_ident = trimmed
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && trimmed
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_');
    if is_plain_ident {
        return trimmed.to_string();
    }
    // Anything else (operators, quotes, parens, …) → wrap
    // as an inline expression.
    format!("{{{{{trimmed}}}}}")
}

pub fn parse_ops(yaml_source: &str) -> Result<Vec<ParsedOp>, String> {
    let workload = parse_workload(yaml_source, &HashMap::new())?;
    Ok(workload.ops)
}

// -----------------------------------------------------------------
// Scenarios
// -----------------------------------------------------------------

fn parse_scenarios(val: Option<&JVal>) -> HashMap<String, Vec<ScenarioNode>> {
    let mut scenarios = HashMap::new();
    let Some(val) = val else { return scenarios; };
    let Some(obj) = val.as_object() else { return scenarios; };

    for (scenario_name, steps_val) in obj {
        let nodes = parse_scenario_nodes(steps_val);
        scenarios.insert(scenario_name.clone(), nodes);
    }
    scenarios
}

/// Recursively parse scenario nodes from YAML.
///
/// Handles:
/// - String: phase name
/// - Object with `for_each` + `phases`: for_each loop (phases parsed recursively)
/// - Array: list of nodes
fn parse_scenario_nodes(val: &JVal) -> Vec<ScenarioNode> {
    match val {
        JVal::String(s) => vec![ScenarioNode::Phase(s.clone())],
        JVal::Array(arr) => arr.iter().flat_map(|item| parse_scenario_nodes(item)).collect(),
        JVal::Object(obj) => {
            let children = obj.get("phases")
                .map(|v| parse_scenario_nodes(v))
                .unwrap_or_default();
            let counter = obj.get("counter")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            // `for_each` is the canonical key; `for` is accepted as
            // a shorter synonym ("for k in 10,100" reads more
            // naturally and matches the GK comprehension text
            // grammar). Both keys are interchangeable; if both
            // appear, `for_each` wins so misconfigured workloads
            // don't silently change shape.
            if let Some(for_each_val) = obj.get("for_each").or_else(|| obj.get("for")) {
                // for_each supports six syntactic shapes that
                // collapse to three semantic variants
                // (`ForEach`, `ForCombinations`, `ForEachUnion`).
                // The detection rule is the same for every shape:
                //
                //   1. Parse the spec into structural sub-spaces
                //      (each top-level clause for string form;
                //      each entry for array form).
                //   2. Collect every (var, expr) pair across all
                //      sub-spaces.
                //   3. If any variable name repeats across pairs
                //      → ForEachUnion (sub-spaces preserved).
                //      Else if more than one pair (all distinct
                //      var names) → ForCombinations (single
                //      Cartesian sub-space).
                //      Else (one pair) → ForEach.
                //
                // This unifies:
                //   "x in 1, y in 2"               → cartesian
                //   "x in 1, x in 2"               → union
                //   ["x in 1", "y in 2"]           → cartesian
                //   ["x in 1", "x in 2"]           → union (two
                //                                    single-clause
                //                                    sub-spaces)
                //   ["x in 1, y in a", "x in 2, y in b"]
                //                                  → union (two
                //                                    multi-clause
                //                                    sub-spaces)
                let sub_spaces: Vec<Vec<nbrs_variates::comprehension::Clause>> = match for_each_val {
                    JVal::String(spec) => {
                        // One sub-space per top-level clause —
                        // structural unit for the union case is
                        // the single clause, since a string can't
                        // express explicit grouping.
                        match parse_clause_list(spec) {
                            Ok(clauses) => clauses.into_iter()
                                .map(|c| vec![c])
                                .collect(),
                            Err(e) => {
                                eprintln!("warning: {e}");
                                Vec::new()
                            }
                        }
                    }
                    JVal::Array(arr) => {
                        // One sub-space per array entry. Each
                        // entry's clauses are its dims; cartesian
                        // within the entry, union across entries.
                        arr.iter().filter_map(|item| {
                            let s = item.as_str()?;
                            let clauses = match parse_clause_list(s) {
                                Ok(clauses) => clauses,
                                Err(e) => {
                                    eprintln!("warning: {e}");
                                    return None;
                                }
                            };
                            if clauses.is_empty() { None } else { Some(clauses) }
                        }).collect()
                    }
                    JVal::Object(map) => {
                        // Map form is always a single sub-space
                        // (keys are unique, so no repetition is
                        // expressible). Map keys can't represent
                        // parallel-iter LHS tuples, so every entry
                        // becomes a single-var clause.
                        vec![map.iter().map(|(k, v)|
                            nbrs_variates::comprehension::Clause::new(
                                k.clone(),
                                v.as_str().unwrap_or("").to_string(),
                            )
                        ).collect()]
                    }
                    _ => Vec::new(),
                };

                if sub_spaces.is_empty() {
                    vec![]
                } else {
                    // Detection (Cartesian vs Union) lives in
                    // `comprehension_from_subspaces` — single
                    // source of truth. Parallel-iter Clauses
                    // pass through unchanged.
                    let mut comprehension = nbrs_variates::comprehension::comprehension_from_subspaces(
                        sub_spaces,
                    );
                    // Optional `where:` key carries a filter
                    // predicate evaluated per emitted tuple.
                    if let Some(filter) = obj.get("where").and_then(|v| v.as_str()) {
                        comprehension = comprehension.with_filter(filter);
                    }
                    // Optional `order:` key carries a traversal
                    // order spec. Accepts either the GK text form
                    // (e.g. "extrema/1") or — future — a YAML
                    // object form (`order: { shells: { … } }`).
                    if let Some(order_val) = obj.get("order") {
                        if let Some(s) = order_val.as_str() {
                            match parse_order_spec(s) {
                                Ok(o) => comprehension = comprehension.with_order(o),
                                Err(e) => eprintln!("warning: order: {e}"),
                            }
                        }
                    }
                    // Single-source invariant check — same
                    // entry point parse_comprehension_text uses.
                    if let Err(errs) = comprehension.validate() {
                        for e in errs {
                            eprintln!("warning: comprehension: {e}");
                        }
                    }
                    vec![ScenarioNode::Comprehension { comprehension, children }]
                }
            } else if let Some(scenario_val) = obj.get("scenario").and_then(|v| v.as_str()) {
                // `scenario: <name>` — logical inclusion of
                // another scenario at this point in the tree.
                // Children remain empty here; resolution happens
                // post-parse via `resolve_scenario_includes`,
                // once every scenario in the workload is known.
                vec![ScenarioNode::IncludedScenario {
                    name: scenario_val.to_string(),
                    children: Vec::new(),
                }]
            } else if let Some(scenarios_val) = obj.get("scenarios") {
                // `scenarios: [name, name, ...]` — plural form
                // for composing several named scenarios at one
                // node in the tree. Each list entry expands to
                // its own `IncludedScenario`; resolution happens
                // post-parse via `resolve_scenario_includes`.
                // Reads more naturally than repeating
                // `- scenario: foo` for each entry; both forms
                // are interchangeable.
                //
                // Map / object entries (`{ scenario: foo }`) are
                // also accepted so a list can mix bare-string
                // includes with other scenario-node shapes
                // already supported by `parse_scenario_nodes`.
                match scenarios_val {
                    JVal::Array(arr) => arr.iter().flat_map(|item| {
                        match item {
                            JVal::String(s) => vec![ScenarioNode::IncludedScenario {
                                name: s.clone(),
                                children: Vec::new(),
                            }],
                            // Anything else (object with
                            // `scenario:`, `for_each:`, etc.)
                            // routes through the standard parse
                            // path so list entries can be
                            // heterogeneous.
                            _ => parse_scenario_nodes(item),
                        }
                    }).collect(),
                    JVal::String(s) => vec![ScenarioNode::IncludedScenario {
                        name: s.clone(),
                        children: Vec::new(),
                    }],
                    _ => Vec::new(),
                }
            } else if let Some(combo_val) = obj.get("for_combinations") {
                // Explicit for_combinations keyword (alias for
                // multi-clause for_each).
                let specs = parse_combination_specs(combo_val);
                let canonical: Vec<nbrs_variates::comprehension::Clause> = specs.into_iter()
                    .map(|(v, e)| nbrs_variates::comprehension::Clause::new(v, e))
                    .collect();
                let mut comprehension = nbrs_variates::comprehension::Comprehension::cartesian(canonical);
                if let Some(filter) = obj.get("where").and_then(|v| v.as_str()) {
                    comprehension = comprehension.with_filter(filter);
                }
                if let Some(s) = obj.get("order").and_then(|v| v.as_str()) {
                    match parse_order_spec(s) {
                        Ok(o) => comprehension = comprehension.with_order(o),
                        Err(e) => eprintln!("warning: order: {e}"),
                    }
                }
                if let Err(errs) = comprehension.validate() {
                    for e in errs {
                        eprintln!("warning: comprehension: {e}");
                    }
                }
                vec![ScenarioNode::Comprehension { comprehension, children }]
            } else if let Some(cond) = obj.get("do_while").and_then(|v| v.as_str()) {
                vec![ScenarioNode::DoWhile { condition: cond.to_string(), counter, children }]
            } else if let Some(cond) = obj.get("do_until").and_then(|v| v.as_str()) {
                vec![ScenarioNode::DoUntil { condition: cond.to_string(), counter, children }]
            } else {
                obj.iter().map(|(name, _cmd)| ScenarioNode::Phase(name.clone())).collect()
            }
        }
        _ => Vec::new(),
    }
}

/// Resolve every `IncludedScenario { name, children: [] }` node
/// produced by [`parse_scenario_nodes`] into one whose `children`
/// hold a clone of the referenced scenario's resolved nodes.
///
/// Two failure modes are surfaced as parse errors:
///
/// 1. **Unknown scenario name** — `scenario: foo` where no
///    `scenarios.foo` exists.
/// 2. **Cycle** — `A` includes `B` which (transitively) includes
///    `A`. The error names the full cycle path so the operator
///    can fix the offending edge.
///
/// Resolution is depth-first with memoization so each scenario
/// is resolved at most once regardless of how many places
/// reference it. After this pass the workload model carries no
/// unresolved `IncludedScenario` nodes; downstream consumers
/// (scope tree, executor, runner) can treat the variant as a
/// fully-formed wrapper scope.
pub fn resolve_scenario_includes(
    scenarios: &mut HashMap<String, Vec<ScenarioNode>>,
) -> Result<(), String> {
    use std::collections::HashSet;

    // Snapshot the input so resolution reads from a stable map
    // while we mutate the output. Each resolved scenario is
    // recorded back into `out`.
    let input: HashMap<String, Vec<ScenarioNode>> = scenarios.clone();
    let mut out: HashMap<String, Vec<ScenarioNode>> = HashMap::new();

    fn resolve_nodes(
        nodes: &[ScenarioNode],
        input: &HashMap<String, Vec<ScenarioNode>>,
        out: &mut HashMap<String, Vec<ScenarioNode>>,
        stack: &mut Vec<String>,
    ) -> Result<Vec<ScenarioNode>, String> {
        let mut resolved = Vec::with_capacity(nodes.len());
        for n in nodes {
            resolved.push(resolve_one(n, input, out, stack)?);
        }
        Ok(resolved)
    }

    fn resolve_one(
        node: &ScenarioNode,
        input: &HashMap<String, Vec<ScenarioNode>>,
        out: &mut HashMap<String, Vec<ScenarioNode>>,
        stack: &mut Vec<String>,
    ) -> Result<ScenarioNode, String> {
        match node {
            ScenarioNode::Phase(name) => Ok(ScenarioNode::Phase(name.clone())),
            ScenarioNode::IncludedScenario { name, .. } => {
                if stack.iter().any(|s| s == name) {
                    let mut path = stack.clone();
                    path.push(name.clone());
                    return Err(format!(
                        "scenario include cycle detected: {}",
                        path.join(" -> "),
                    ));
                }
                let target = input.get(name).ok_or_else(|| format!(
                    "scenario include 'scenario: {name}' references an unknown \
                     scenario. Known scenarios: {}",
                    {
                        let mut names: Vec<&str> = input.keys().map(|s| s.as_str()).collect();
                        names.sort();
                        names.join(", ")
                    },
                ))?;
                stack.push(name.clone());
                let children = resolve_nodes(target, input, out, stack)?;
                stack.pop();
                // Memoize the resolved scenario for any later
                // include reference. Idempotent: equivalent
                // resolved children produced regardless of
                // entry point.
                out.entry(name.clone()).or_insert_with(|| children.clone());
                Ok(ScenarioNode::IncludedScenario {
                    name: name.clone(),
                    children,
                })
            }
            ScenarioNode::Comprehension { comprehension, children } => {
                Ok(ScenarioNode::Comprehension {
                    comprehension: comprehension.clone(),
                    children: resolve_nodes(children, input, out, stack)?,
                })
            }
            ScenarioNode::DoWhile { condition, counter, children } => {
                Ok(ScenarioNode::DoWhile {
                    condition: condition.clone(),
                    counter: counter.clone(),
                    children: resolve_nodes(children, input, out, stack)?,
                })
            }
            ScenarioNode::DoUntil { condition, counter, children } => {
                Ok(ScenarioNode::DoUntil {
                    condition: condition.clone(),
                    counter: counter.clone(),
                    children: resolve_nodes(children, input, out, stack)?,
                })
            }
        }
    }

    let mut visited: HashSet<String> = HashSet::new();
    let names: Vec<String> = scenarios.keys().cloned().collect();
    for name in names {
        if visited.contains(&name) { continue; }
        let mut stack = vec![name.clone()];
        let resolved = resolve_nodes(&input[&name], &input, &mut out, &mut stack)?;
        out.insert(name.clone(), resolved);
        visited.insert(name);
    }
    *scenarios = out;
    Ok(())
}

/// Parse combination specs from any of three YAML forms:
///
/// **Map form** (keys = variables, values = expressions):
/// ```yaml
/// for_combinations:
///   profile: "matching_profiles('{dataset}', '{prefix}')"
///   k: "{k_values}"
/// ```
///
/// **List form** (reuses for_each "var in expr" syntax):
/// ```yaml
/// for_combinations:
///   - "profile in matching_profiles('{dataset}', '{prefix}')"
///   - "k in {k_values}"
/// ```
///
/// **Inline form** (compact comma-separated):
/// ```yaml
/// for_combinations: "profile in profiles, k in {k_values}"
/// ```
fn parse_combination_specs(val: &JVal) -> Vec<(String, String)> {
    match val {
        // Map form: { "profile": "expr", "k": "expr" }
        JVal::Object(map) => {
            map.iter()
                .map(|(key, val)| {
                    let expr = val.as_str().unwrap_or("").to_string();
                    (key.clone(), expr)
                })
                .collect()
        }
        // List form: ["profile in expr", "k in expr"]
        JVal::Array(arr) => {
            arr.iter()
                .filter_map(|item| {
                    let s = item.as_str()?;
                    match parse_clause(s) {
                        Ok(c) => Some((c.var().to_string(), c.expr().to_string())),
                        Err(e) => {
                            eprintln!("warning: for_combinations: {e}");
                            None
                        }
                    }
                })
                .collect()
        }
        // Inline form: "profile in expr, k in expr"
        // Split on commas that are NOT inside parentheses (respects
        // function calls like `matching_profiles('{dataset}', '{prefix}')`).
        JVal::String(s) => {
            match parse_clause_list(s) {
                Ok(clauses) => clauses.into_iter()
                    .map(|c| (c.var().to_string(), c.expr().to_string()))
                    .collect(),
                Err(e) => {
                    eprintln!("warning: for_combinations: {e}");
                    Vec::new()
                }
            }
        }
        _ => {
            eprintln!("warning: for_combinations value must be a map, list, or string");
            Vec::new()
        }
    }
}

// -----------------------------------------------------------------
// Phases
// -----------------------------------------------------------------

/// Parse the `phases:` section of a workload YAML.
///
/// Each phase is a named map with optional `cycles`, `concurrency`,
/// `rate`, `adapter`, `errors`, `tags`, and `ops` fields.
/// Returns the phase map and a Vec preserving YAML definition order.
fn parse_phases(
    val: Option<&JVal>,
    doc_bindings: &BindingsDef,
    doc_params: &HashMap<String, JVal>,
    doc_tags: &HashMap<String, String>,
) -> Result<(HashMap<String, WorkloadPhase>, Vec<String>), String> {
    let mut phases = HashMap::new();
    let mut phase_order = Vec::new();
    let Some(val) = val else { return Ok((phases, phase_order)); };
    let Some(obj) = val.as_object() else { return Ok((phases, phase_order)); };

    for (phase_name, phase_val) in obj {
        let Some(phase_obj) = phase_val.as_object() else { continue; };

        let cycles = phase_obj.get("cycles")
            .map(|v| match v {
                JVal::Number(n) => n.to_string(),
                JVal::String(s) => s.clone(),
                other => other.to_string(),
            });

        let concurrency = phase_obj.get("concurrency")
            .map(|v| match v {
                JVal::Number(n) => n.to_string(),
                JVal::String(s) => s.clone(),
                other => other.to_string(),
            });

        let rate = phase_obj.get("rate")
            .and_then(|v| v.as_f64());

        let adapter = phase_obj.get("adapter")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let errors = phase_obj.get("errors")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let tags = phase_obj.get("tags")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        // Phase-level YAML `bindings:` block. Captured raw on
        // the phase AST so SRD-13d's `HasGkMatter` impl can
        // classify it correctly and a future runtime can layer
        // a phase kernel between workload and op kernels.
        let phase_bindings_only = extract_bindings(phase_obj.get("bindings"));
        // Legacy merge: combine workload + phase bindings into
        // the bindings the parser hands to each op. SRD-13d
        // phases 3–9 will remove this merge in favour of
        // proper kernel chaining; today's runtime still
        // expects per-op bindings to carry phase-level
        // declarations.
        let phase_bindings = merge_bindings(doc_bindings, &phase_bindings_only);

        // Parse inline ops if present
        let mut inline_ops = Vec::new();
        for key in ["ops", "op", "operations", "statements", "statement"] {
            if let Some(ops_val) = phase_obj.get(key) {
                let phase_tags = {
                    let mut t = doc_tags.clone();
                    t.insert("phase".to_string(), phase_name.clone());
                    t
                };
                parse_ops_field(ops_val, phase_name, &phase_bindings, doc_params, &phase_tags, &mut inline_ops)?;
                break;
            }
        }

        // Auto-tag inline ops
        for op in &mut inline_ops {
            if !op.tags.contains_key("name") {
                op.tags.insert("name".to_string(), op.name.clone());
            }
            if !op.tags.contains_key("op") {
                op.tags.insert("op".to_string(), op.name.clone());
            }
        }

        let for_each = phase_obj.get("for_each")
            .or_else(|| phase_obj.get("for"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let loop_scope = phase_obj.get("loop_scope")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let iter_scope = phase_obj.get("iter_scope")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        // Phase-level `summary:` is gone (SRD-46 made `report:`
        // the canonical surface). Reject explicitly so silent
        // drops don't mislead operators migrating workloads.
        if phase_obj.contains_key("summary") {
            return Err(format!(
                "phase '{phase_name}': `summary:` is removed at phase level; \
                 use a `report:` block with `table <name> ...` items instead \
                 (SRD-46)"
            ));
        }
        // Per-phase `checkpoint:` declaration. Three forms
        // (short string, bool/none, full mapping) handled by
        // [`Checkpoint`]'s custom deserialize. Absent → None →
        // phase always re-runs on resume (per SRD-44 §"No
        // workload-level default").
        let checkpoint = phase_obj.get("checkpoint")
            .map(|v| serde_json::from_value::<crate::model::Checkpoint>(v.clone()))
            .transpose()
            .map_err(|e| format!("phase '{phase_name}' checkpoint: {e}"))?;

        // `status_metrics:` — names of relevancy aggregates to
        // surface on the inline progress line and the per-phase
        // ✓ DONE summary. Accepts a YAML list (`[name, name]`),
        // a single string, or a comma-separated string. Empty /
        // absent → no metrics tail (nothing presumed present).
        let status_metrics: Vec<String> = match phase_obj.get("status_metrics") {
            None => Vec::new(),
            Some(JVal::Array(items)) => items.iter()
                .filter_map(|v| v.as_str().map(|s| s.trim().to_string()))
                .filter(|s| !s.is_empty())
                .collect(),
            Some(JVal::String(s)) => s.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect(),
            Some(other) => return Err(format!(
                "phase '{phase_name}' status_metrics: must be a list of metric \
                 names, a comma-separated string, or omitted; got {other:?}"
            )),
        };

        phases.insert(phase_name.clone(), WorkloadPhase {
            cycles,
            concurrency,
            rate,
            adapter,
            errors,
            tags,
            ops: inline_ops,
            for_each,
            loop_scope,
            iter_scope,
            checkpoint,
            status_metrics,
            bindings: phase_bindings_only,
        });
        phase_order.push(phase_name.clone());
    }

    Ok((phases, phase_order))
}

// -----------------------------------------------------------------
// Blocks
// -----------------------------------------------------------------

fn parse_blocks(
    blocks_val: &JVal,
    doc_bindings: &BindingsDef,
    doc_params: &HashMap<String, JVal>,
    doc_tags: &HashMap<String, String>,
    all_ops: &mut Vec<ParsedOp>,
) -> Result<(), String> {
    match blocks_val {
        JVal::Object(map) => {
            for (block_name, block_val) in map {
                parse_single_block(block_name, block_val, doc_bindings, doc_params, doc_tags, all_ops)?;
            }
        }
        JVal::Array(arr) => {
            for (i, block_val) in arr.iter().enumerate() {
                let name = block_val.get("name")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| format!("block{}", i + 1));
                parse_single_block(&name, block_val, doc_bindings, doc_params, doc_tags, all_ops)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn parse_single_block(
    block_name: &str,
    block_val: &JVal,
    doc_bindings: &BindingsDef,
    doc_params: &HashMap<String, JVal>,
    doc_tags: &HashMap<String, String>,
    all_ops: &mut Vec<ParsedOp>,
) -> Result<(), String> {
    let obj = match block_val.as_object() {
        Some(o) => o,
        None => return Ok(()),
    };

    // Merge block-level properties with doc-level (block overrides doc)
    let block_bindings = merge_bindings(doc_bindings, &extract_bindings(obj.get("bindings")));
    let block_params = merge_value_maps(doc_params, &extract_value_map(obj.get("params")));
    let mut block_tags = merge_string_maps(doc_tags, &extract_string_map(obj.get("tags")));
    block_tags.insert("block".to_string(), block_name.to_string());

    // Find ops field
    for key in ["ops", "op", "operations", "statements", "statement"] {
        if let Some(ops_val) = obj.get(key) {
            parse_ops_field(ops_val, block_name, &block_bindings, &block_params, &block_tags, all_ops)?;
            return Ok(());
        }
    }

    // If no ops field, check if the block value itself is a string (single op)
    if let Some(s) = block_val.as_str() {
        let mut op = ParsedOp::simple("stmt1", s);
        op.bindings = block_bindings;
        op.params = block_params;
        op.tags = block_tags;
        all_ops.push(op);
    }

    Ok(())
}

// -----------------------------------------------------------------
// Ops
// -----------------------------------------------------------------

fn parse_ops_field(
    ops_val: &JVal,
    block_name: &str,
    bindings: &BindingsDef,
    params: &HashMap<String, JVal>,
    tags: &HashMap<String, String>,
    all_ops: &mut Vec<ParsedOp>,
) -> Result<(), String> {
    let mut op_counter = 0;

    match ops_val {
        // Single string: op: "SELECT ..."
        JVal::String(s) => {
            op_counter += 1;
            let name = format!("stmt{op_counter}");
            let mut op = ParsedOp::simple(&name, s);
            op.bindings = bindings.clone();
            op.params = params.clone();
            op.tags = tags.clone();
            op.tags.insert("block".to_string(), block_name.to_string());
            all_ops.push(op);
        }

        // List of ops
        JVal::Array(arr) => {
            for item in arr {
                op_counter += 1;
                let auto_name = format!("stmt{op_counter}");
                let op = normalize_op_item(item, &auto_name, block_name, bindings, params, tags)?;
                all_ops.push(op);
            }
        }

        // Map of named ops
        JVal::Object(map) => {
            for (key, val) in map {
                let op = normalize_op_entry(key, val, block_name, bindings, params, tags)?;
                all_ops.push(op);
            }
        }

        _ => {}
    }

    Ok(())
}

/// Normalize a single op from a list item.
fn normalize_op_item(
    item: &JVal,
    auto_name: &str,
    block_name: &str,
    bindings: &BindingsDef,
    params: &HashMap<String, JVal>,
    tags: &HashMap<String, String>,
) -> Result<ParsedOp, String> {
    match item {
        JVal::String(s) => {
            let mut op = ParsedOp::simple(auto_name, s);
            op.bindings = bindings.clone();
            op.params = params.clone();
            op.tags = tags.clone();
            op.tags.insert("block".to_string(), block_name.to_string());
            Ok(op)
        }
        JVal::Object(map) => {
            // Check if first entry is name:stmt pattern
            if let Some((first_key, first_val)) = map.iter().next()
                && map.len() == 1 && first_val.is_string() {
                    let mut op = ParsedOp::simple(first_key, first_val.as_str().unwrap());
                    op.bindings = bindings.clone();
                    op.params = params.clone();
                    op.tags = tags.clone();
                    op.tags.insert("block".to_string(), block_name.to_string());
                    return Ok(op);
                }
            // Full op object
            normalize_op_object(map, auto_name, block_name, bindings, params, tags)
        }
        _ => Ok(ParsedOp::simple(auto_name, "")),
    }
}

/// Normalize a named op from a map entry.
fn normalize_op_entry(
    key: &str,
    val: &JVal,
    block_name: &str,
    bindings: &BindingsDef,
    params: &HashMap<String, JVal>,
    tags: &HashMap<String, String>,
) -> Result<ParsedOp, String> {
    match val {
        JVal::String(s) => {
            let mut op = ParsedOp::simple(key, s);
            op.bindings = bindings.clone();
            op.params = params.clone();
            op.tags = tags.clone();
            op.tags.insert("block".to_string(), block_name.to_string());
            Ok(op)
        }
        JVal::Object(map) => {
            normalize_op_object(map, key, block_name, bindings, params, tags)
        }
        JVal::Array(arr) => {
            // Array at op level → moved to op.stmt
            let mut op_fields = HashMap::new();
            op_fields.insert("stmt".to_string(), JVal::Array(arr.clone()));
            let mut op = ParsedOp {
                name: key.to_string(),
                description: None,
                op: op_fields,
                bindings: bindings.clone(),
                params: params.clone(),
                tags: tags.clone(),
                condition: None,
                delay: None,
                metrics: HashMap::new(),
                result: HashMap::new(),
            };
            op.tags.insert("block".to_string(), block_name.to_string());
            Ok(op)
        }
        _ => Ok(ParsedOp::simple(key, "")),
    }
}

/// Human-readable name for a JSON value kind, for parse-time
/// error messages. ("string", "number", "array", etc.)
fn eval_value_kind(v: &JVal) -> &'static str {
    match v {
        JVal::Null => "null",
        JVal::Bool(_) => "bool",
        JVal::Number(_) => "number",
        JVal::String(_) => "string",
        JVal::Array(_) => "array",
        JVal::Object(_) => "mapping",
    }
}

/// Sub-keys allowed inside an op-template's `evaluations:`
/// block. The block is a reserved closed-vocab wrapper for
/// post-execution validation / scoring config — distinct from
/// per-adapter op fields. Anything else inside it is rejected at
/// parse time so silent-ignore traps (a misspelled `relevency:`,
/// a misplaced wrapper) cannot hide a misconfigured op. New
/// evaluation kinds are added here.
const EVALUATIONS_VOCAB: &[&str] = &["relevancy", "verify"];

/// Normalize a full op object (map of fields).
fn normalize_op_object(
    map: &serde_json::Map<String, JVal>,
    default_name: &str,
    block_name: &str,
    parent_bindings: &BindingsDef,
    parent_params: &HashMap<String, JVal>,
    parent_tags: &HashMap<String, String>,
) -> Result<ParsedOp, String> {
    let name = map.get("name")
        .and_then(|v| v.as_str())
        .unwrap_or(default_name)
        .to_string();

    let description = map.get("description")
        .or_else(|| map.get("desc"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    // Extract recognized fields
    let mut op_bindings = merge_bindings(parent_bindings, &extract_bindings(map.get("bindings")));
    let op_params = merge_value_maps(parent_params, &extract_value_map(map.get("params")));
    let mut op_tags = merge_string_maps(parent_tags, &extract_string_map(map.get("tags")));
    op_tags.insert("block".to_string(), block_name.to_string());

    // Determine op payload
    //
    // `reserved` lists keys handled by the workload model itself
    // (name, bindings, etc.) — they never reach the adapter.
    // `evaluations` is in this list because it's a closed-vocab
    // wrapper for validation/scoring config (relevancy, verify)
    // — its sub-keys are extracted and hoisted into `op_params`
    // below so downstream consumers
    // (`crate::validation::parse_relevancy` etc.) find them at
    // the same address whether the workload writes the
    // canonical wrapped form or the legacy top-level shorthand.
    let reserved = ["name", "description", "desc", "bindings", "params", "tags", "if", "delay",
        "evaluations"];
    let op_field_names = ["op", "ops", "operations", "stmt", "statement", "statements"];
    // Activity-level params excised from op fields before the
    // adapter sees them. `relevancy` / `verify` stay listed here
    // for the legacy top-level shorthand
    // (`relevancy: { ... }` directly under the op); the canonical
    // form puts them inside `evaluations:` and is handled
    // separately below.
    let activity_params = ["ratio", "driver", "space", "instrument", "start-timers", "stop-timers",
        "verify", "relevancy", "strict", "poll", "poll_interval_ms", "timeout_ms", "poll_metric_name", "emit",
        "batch", "max_batch_size", "batchtype"];

    let op_fields = if let Some(explicit_op) = op_field_names.iter()
        .find_map(|k| map.get(*k))
    {
        let mut m: HashMap<String, JVal> = match explicit_op {
            JVal::String(s) => {
                let mut m = HashMap::new();
                m.insert("stmt".to_string(), JVal::String(s.clone()));
                m
            }
            JVal::Object(o) => o.iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            other => {
                let mut m = HashMap::new();
                m.insert("stmt".to_string(), other.clone());
                m
            }
        };
        // Preserve sibling op-level fields so adapter-specific
        // extras (e.g. testkit's `result-latency`, `result-capacity`)
        // aren't silently dropped when the user writes shorthand:
        //
        //     insert:
        //       stmt: "INSERT ..."
        //       result-latency: "5ms"
        //
        // Without this loop the whole object would collapse to just
        // `stmt` and the sibling fields would never reach the adapter.
        // Keys already present in the explicit op payload win, so an
        // `op:` sub-object still has final say over its own shape.
        for (k, v) in map.iter() {
            if reserved.contains(&k.as_str())
                || op_field_names.contains(&k.as_str())
                || activity_params.contains(&k.as_str())
            {
                continue;
            }
            m.entry(k.clone()).or_insert_with(|| v.clone());
        }
        m
    } else {
        // All non-reserved, non-activity-param fields become op fields
        map.iter()
            .filter(|(k, _)| !reserved.contains(&k.as_str())
                && !op_field_names.contains(&k.as_str())
                && !activity_params.contains(&k.as_str()))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    };

    // Excise activity-level params from op fields into params
    let mut op_params = op_params;
    for ap in &activity_params {
        if let Some(val) = map.get(*ap) {
            // Activity params excised from op fields into params map
            op_params.insert(ap.to_string(), val.clone());
        }
    }

    // Canonical `evaluations:` wrapper — closed-vocab
    // validation/scoring config. Sub-keys are extracted and
    // hoisted into `op_params` so downstream consumers (e.g.
    // `crate::validation::parse_relevancy`,
    // `crate::validation::parse_assertions`) find them at the
    // same address whether the workload uses this canonical
    // form or the legacy top-level shorthand. Anything inside
    // `evaluations:` that isn't in `EVALUATIONS_VOCAB` is
    // rejected up front — the whole point of the wrapper is to
    // catch misspellings (`relevency:`) and misplaced wrappers
    // that the silent-routing path would otherwise drop on the
    // floor.
    if let Some(eval_val) = map.get("evaluations") {
        let eval_obj = eval_val.as_object().ok_or_else(|| format!(
            "op '{name}' (block '{block_name}'): `evaluations:` must be a \
             mapping, got {kind}. Expected shape: \
             `evaluations: {{ relevancy: {{...}}, verify: [...] }}`.",
            kind = eval_value_kind(eval_val),
        ))?;
        for (k, v) in eval_obj.iter() {
            if !EVALUATIONS_VOCAB.contains(&k.as_str()) {
                return Err(format!(
                    "op '{name}' (block '{block_name}'): unknown key \
                     '{k}' under `evaluations:`. Allowed keys: [{}]. \
                     Each entry under `evaluations:` is a distinct \
                     post-execution evaluation kind — typos and \
                     misplaced wrappers are rejected here so silent \
                     skipped recall / verify can't happen.",
                    EVALUATIONS_VOCAB.join(", "),
                ));
            }
            // Top-level shorthand wins on collision so users
            // who already have `relevancy: {...}` at the op
            // level don't see their config replaced if they
            // also added `evaluations: { relevancy: {...} }`.
            // Warn so the duplicate is visible.
            if op_params.contains_key(k.as_str()) {
                eprintln!(
                    "warning: op '{name}' has '{k}' both at top level \
                     and under `evaluations:` — top-level wins. Pick \
                     one form.",
                );
                continue;
            }
            op_params.insert(k.clone(), v.clone());
        }
    }

    let condition = map.get("if")
        .and_then(|v| v.as_str())
        .map(normalize_condition_clause);

    let delay = map.get("delay")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let metrics = parse_metrics_field(map.get("metrics"), &name, &mut op_bindings)
        .map_err(|e| format!("op '{name}' metrics: {e}"))?;
    let result = parse_result_field(map.get("result"), &name)
        .map_err(|e| format!("op '{name}' result: {e}"))?;

    Ok(ParsedOp {
        name,
        description,
        op: op_fields,
        bindings: op_bindings,
        params: op_params,
        tags: op_tags,
        condition,
        delay,
        metrics,
        result,
    })
}

/// SRD-40b §1 + §2: parse the `metrics:` field on an op
/// template. Three YAML shapes accepted, dispatched on the
/// value's type:
///
/// - **Scalar** (bare string, §2.1): one metric with the
///   string as both family and `value:`.
/// - **Sequence** (list, §2.2): each entry is a bare-name
///   string OR a `name := <gk expression>` wire-expression.
///   Wire expressions are auto-injected into the op's
///   `bindings:` block; the metric is then a bare-name
///   reference to the new wire.
/// - **Mapping** (object, §2.3): canonical full-shape form
///   keyed by metric name. Each value is either a string
///   (treated as `value:`) or a full `MetricSpec` mapping.
fn parse_metrics_field(
    val: Option<&JVal>,
    op_name: &str,
    op_bindings: &mut BindingsDef,
) -> Result<HashMap<String, MetricSpec>, String> {
    use crate::model::MetricSpec;
    let Some(v) = val else { return Ok(HashMap::new()); };
    let mut out: HashMap<String, MetricSpec> = HashMap::new();
    match v {
        JVal::String(s) => {
            let name = s.trim().to_string();
            if name.is_empty() {
                return Err("scalar form requires a metric name".into());
            }
            out.insert(name.clone(), MetricSpec {
                value: name, family: None, kind: None,
                unit: None, format: None,
            });
        }
        JVal::Array(items) => {
            for (idx, item) in items.iter().enumerate() {
                let raw = item.as_str().ok_or_else(|| format!(
                    "metrics list entry {idx}: must be a string \
                     (bare name or `name := <gk expr>`)"))?;
                let trimmed = raw.trim();
                if let Some((name, expr)) = trimmed.split_once(":=") {
                    // Wire-expression form: declare the binding +
                    // register the metric.
                    let name = name.trim();
                    let expr = expr.trim();
                    if name.is_empty() || expr.is_empty() {
                        return Err(format!(
                            "metrics list entry {idx} '{raw}': wire \
                             expression must be `name := <expression>`"));
                    }
                    inject_wire_into_bindings(op_bindings, name, expr, op_name)?;
                    if out.contains_key(name) {
                        return Err(format!(
                            "duplicate metric wire '{name}' in metrics list"));
                    }
                    out.insert(name.to_string(), MetricSpec {
                        value: name.to_string(), family: None, kind: None,
                        unit: None, format: None,
                    });
                } else {
                    // Bare-name form.
                    if trimmed.is_empty() {
                        return Err(format!(
                            "metrics list entry {idx}: empty name"));
                    }
                    if out.contains_key(trimmed) {
                        return Err(format!(
                            "duplicate metric '{trimmed}' in metrics list"));
                    }
                    out.insert(trimmed.to_string(), MetricSpec {
                        value: trimmed.to_string(), family: None, kind: None,
                        unit: None, format: None,
                    });
                }
            }
        }
        JVal::Object(map) => {
            for (key, val) in map {
                if out.contains_key(key) {
                    return Err(format!(
                        "duplicate metric key '{key}' in metrics map"));
                }
                let mut spec = parse_metric_spec_value(val, key)?;
                // SRD-13d Phase 9 mapping-form auto-inject: if
                // `value:` isn't a bare name, promote it to an
                // op-template binding `<key> := <value>` and
                // replace the spec's `value:` with the bare key.
                // Mirrors the list-form `name := expr` flow.
                let value_trimmed = spec.value.trim();
                let bare = !value_trimmed.is_empty()
                    && value_trimmed.chars().all(|c| c.is_alphanumeric() || c == '_');
                if !bare {
                    if !is_valid_ident(key) {
                        return Err(format!(
                            "metric '{key}' value '{value}' is a non-bare \
                             expression so the metric key must itself be a \
                             valid identifier (alphanumerics + underscore, \
                             not starting with a digit) so it can be used \
                             as a binding name. Rename the metric key, or \
                             move the expression into `bindings:` and set \
                             `value:` to the bare name.",
                            value = spec.value));
                    }
                    inject_wire_into_bindings(op_bindings, key, value_trimmed, op_name)?;
                    spec.value = key.clone();
                }
                out.insert(key.clone(), spec);
            }
        }
        _ => return Err(format!(
            "metrics: expected scalar, sequence, or mapping; got {v:?}")),
    }
    Ok(out)
}

/// Parse one entry under the mapping form of `metrics:`.
/// Accepts a bare string (treated as `value:`) or a full
/// `MetricSpec` object.
fn parse_metric_spec_value(v: &JVal, key: &str) -> Result<crate::model::MetricSpec, String> {
    use crate::model::MetricSpec;
    match v {
        JVal::String(s) => Ok(MetricSpec {
            value: s.clone(), family: None, kind: None,
            unit: None, format: None,
        }),
        JVal::Object(map) => {
            let value = map.get("value")
                .and_then(|v| v.as_str())
                .ok_or_else(|| format!(
                    "metric '{key}': required field `value:` missing or \
                     not a string"))?
                .to_string();
            let family = map.get("family")
                .and_then(|v| v.as_str()).map(String::from);
            let unit = map.get("unit")
                .and_then(|v| v.as_str()).map(String::from);
            let format = map.get("format")
                .and_then(|v| v.as_str()).map(String::from);
            // Validate format syntax at parse time so the user
            // hears about a bad `#.##` pattern at workload
            // load, not first-cycle. SRD-40b §1.
            if let Some(f) = format.as_deref() {
                crate::metric_format::parse_format_spec(f)
                    .map_err(|e| format!(
                        "metric '{key}' format '{f}': {e}"))?;
            }
            let kind = match map.get("kind") {
                None => None,
                Some(JVal::String(s)) => Some(parse_metric_kind(s, key)?),
                Some(other) => return Err(format!(
                    "metric '{key}' kind: expected string, got {other:?}")),
            };
            Ok(MetricSpec { value, family, kind, unit, format })
        }
        _ => Err(format!(
            "metric '{key}': expected string or mapping, got {v:?}")),
    }
}

fn parse_metric_kind(s: &str, key: &str) -> Result<crate::model::MetricKind, String> {
    use crate::model::MetricKind;
    match s.to_ascii_lowercase().as_str() {
        "gauge" => Ok(MetricKind::Gauge),
        "histogram" => Ok(MetricKind::Histogram),
        "counter" => Ok(MetricKind::Counter),
        other => Err(format!(
            "metric '{key}' kind '{other}': expected one of \
             gauge / histogram / counter")),
    }
}

/// Auto-inject `name := expr` into the op template's
/// `bindings:` block (per SRD-40b §2.2). Conflicts with an
/// existing declaration of the same name are a strict
/// workload parse error per §2.2.
fn inject_wire_into_bindings(
    bindings: &mut BindingsDef,
    name: &str,
    expr: &str,
    op_name: &str,
) -> Result<(), String> {
    // Look for an existing same-name declaration to refuse
    // shadowing. The check is textual: a line beginning with
    // `<name>` followed by whitespace + `:=`. Prefix matching
    // would surface false positives for `foo` vs `foobar`,
    // hence the boundary check.
    let line_to_inject = format!("{name} := {expr}\n");
    // BindingsDef has no `Empty` variant — `Map(empty)` is the
    // default. Detect emptiness via the existing helper, then
    // promote to GkSource for injection (we're adding a real
    // GK statement, not a name→expr pair the legacy Map form
    // can't carry alone).
    if bindings.is_empty() {
        *bindings = BindingsDef::GkSource(line_to_inject);
        return Ok(());
    }
    match bindings {
        BindingsDef::GkSource(src) => {
            if has_binding_named(src, name) {
                return Err(format!(
                    "metric wire '{name}' (op '{op_name}') collides \
                     with existing `bindings:` declaration of the \
                     same name"));
            }
            if !src.ends_with('\n') { src.push('\n'); }
            src.push_str(&line_to_inject);
        }
        BindingsDef::Map(map) => {
            if map.contains_key(name) {
                return Err(format!(
                    "metric wire '{name}' (op '{op_name}') collides \
                     with existing `bindings:` declaration of the \
                     same name"));
            }
            map.insert(name.to_string(), expr.to_string());
        }
    }
    Ok(())
}

/// True when `s` is a valid GK identifier: non-empty, first
/// char is a letter or underscore, remaining chars are
/// alphanumerics or underscore. Used by the mapping-form
/// metric auto-inject to confirm the metric key can stand in
/// as a binding name.
fn is_valid_ident(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_alphanumeric() || c == '_')
}

/// True when the GK source contains a binding line
/// `<name> := …` at the start of any (whitespace-trimmed)
/// line. Used by the wire-expression injection to detect
/// shadowing without parsing the GK grammar.
fn has_binding_named(src: &str, name: &str) -> bool {
    for raw in src.lines() {
        let line = raw.trim_start();
        if let Some(rest) = line.strip_prefix(name) {
            // Boundary: next char must be whitespace or `:=`.
            let rest = rest.trim_start();
            if rest.starts_with(":=") {
                return true;
            }
        }
    }
    false
}

/// SRD-40b §5: parse the `result:` field on an op template.
/// Mapping shape only — each entry names a GK wire to
/// populate from the op's result body.
fn parse_result_field(
    val: Option<&JVal>,
    op_name: &str,
) -> Result<HashMap<String, crate::model::ResultWireSpec>, String> {
    use crate::model::ResultWireSpec;
    let Some(v) = val else { return Ok(HashMap::new()); };
    let map = v.as_object().ok_or_else(|| format!(
        "op '{op_name}' result: expected mapping (name → source)"))?;
    let mut out: HashMap<String, ResultWireSpec> = HashMap::new();
    for (key, val) in map {
        if out.contains_key(key) {
            return Err(format!(
                "duplicate result wire '{key}'"));
        }
        let spec = match val {
            JVal::String(s) => ResultWireSpec::String(s.clone()),
            JVal::Object(m) => {
                let source = m.get("source")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| format!(
                        "result '{key}': required field `source:` \
                         missing or not a string"))?
                    .to_string();
                let default = m.get("default")
                    .and_then(|v| v.as_str())
                    .map(String::from);
                ResultWireSpec::Object { source, default }
            }
            _ => return Err(format!(
                "result '{key}': expected string or mapping")),
        };
        out.insert(key.clone(), spec);
    }
    Ok(out)
}

// -----------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------

/// Extract bindings from a YAML value.
///
/// If the value is a string, it's native GK grammar source.
/// If it's a mapping, it's legacy name→expression pairs.
fn extract_bindings(val: Option<&JVal>) -> BindingsDef {
    match val {
        Some(JVal::String(s)) => BindingsDef::GkSource(s.clone()),
        Some(JVal::Object(obj)) => {
            let mut map = HashMap::new();
            for (k, v) in obj {
                if let Some(s) = v.as_str() {
                    map.insert(k.clone(), s.to_string());
                } else {
                    map.insert(k.clone(), v.to_string());
                }
            }
            BindingsDef::Map(map)
        }
        _ => BindingsDef::default(),
    }
}

/// Merge bindings from parent and child levels.
///
/// GkSource at child level completely replaces parent (no merging).
/// Map at child level merges with parent map at key level.
/// If parent is GkSource and child is empty, parent is inherited.
fn merge_bindings(parent: &BindingsDef, child: &BindingsDef) -> BindingsDef {
    match (parent, child) {
        // Child GK source replaces everything
        (_, BindingsDef::GkSource(s)) if !s.trim().is_empty() => {
            BindingsDef::GkSource(s.clone())
        }
        // Child map merges with parent map
        (BindingsDef::Map(p), BindingsDef::Map(c)) => {
            let mut merged = p.clone();
            for (k, v) in c {
                merged.insert(k.clone(), v.clone());
            }
            BindingsDef::Map(merged)
        }
        // Empty child inherits parent
        (_, BindingsDef::Map(c)) if c.is_empty() => parent.clone(),
        // Otherwise child wins
        (_, child) => child.clone(),
    }
}

fn extract_string_map(val: Option<&JVal>) -> HashMap<String, String> {
    let mut map = HashMap::new();
    if let Some(JVal::Object(obj)) = val {
        for (k, v) in obj {
            if let Some(s) = v.as_str() {
                map.insert(k.clone(), s.to_string());
            } else {
                map.insert(k.clone(), v.to_string());
            }
        }
    }
    map
}

fn extract_value_map(val: Option<&JVal>) -> HashMap<String, JVal> {
    let mut map = HashMap::new();
    if let Some(JVal::Object(obj)) = val {
        for (k, v) in obj {
            map.insert(k.clone(), v.clone());
        }
    }
    map
}

fn merge_string_maps(parent: &HashMap<String, String>, child: &HashMap<String, String>) -> HashMap<String, String> {
    let mut merged = parent.clone();
    for (k, v) in child {
        merged.insert(k.clone(), v.clone());
    }
    merged
}

fn merge_value_maps(parent: &HashMap<String, JVal>, child: &HashMap<String, JVal>) -> HashMap<String, JVal> {
    let mut merged = parent.clone();
    for (k, v) in child {
        merged.insert(k.clone(), v.clone());
    }
    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readouts_block_form_a_scalar_binds_on_update() {
        let workload: serde_yaml::Value = serde_yaml::from_str(
            r#"readouts: phase_status"#
        ).unwrap();
        let json = serde_json::to_value(&workload).unwrap();
        let r = parse_readouts_block(json.get("readouts")).unwrap();
        assert_eq!(r.on_update, vec!["phase_status".to_string()]);
        assert!(r.on_phase_end.is_empty());
    }

    #[test]
    fn readouts_block_form_b_mapping_binds_explicit_slots() {
        let yaml = serde_yaml::from_str::<serde_yaml::Value>(
            r#"
readouts:
  on_phase_end: phase_done
  on_update: "phase_status lod=compact"
"#).unwrap();
        let json = serde_json::to_value(&yaml).unwrap();
        let r = parse_readouts_block(json.get("readouts")).unwrap();
        assert_eq!(r.on_phase_end, vec!["phase_done".to_string()]);
        assert_eq!(r.on_update, vec!["phase_status lod=compact".to_string()]);
    }

    #[test]
    fn readouts_block_form_c_list_composes() {
        let yaml = serde_yaml::from_str::<serde_yaml::Value>(
            r#"
readouts:
  on_phase_end:
    - phase_done
    - phase_failure_hint
"#).unwrap();
        let json = serde_json::to_value(&yaml).unwrap();
        let r = parse_readouts_block(json.get("readouts")).unwrap();
        assert_eq!(r.on_phase_end, vec![
            "phase_done".to_string(),
            "phase_failure_hint".to_string(),
        ]);
    }

    #[test]
    fn readouts_block_each_wildcard_expands() {
        let yaml = serde_yaml::from_str::<serde_yaml::Value>(
            r#"
readouts:
  each_*: scope_bracket
"#).unwrap();
        let json = serde_json::to_value(&yaml).unwrap();
        let r = parse_readouts_block(json.get("readouts")).unwrap();
        assert_eq!(r.on_each_start, vec!["scope_bracket".to_string()]);
        assert_eq!(r.on_each_end,   vec!["scope_bracket".to_string()]);
        // Other slots untouched.
        assert!(r.on_phase_end.is_empty());
        assert!(r.on_update.is_empty());
    }

    #[test]
    fn readouts_block_phase_wildcard_expands() {
        let yaml = serde_yaml::from_str::<serde_yaml::Value>(
            r#"
readouts:
  phase_*: trace
"#).unwrap();
        let json = serde_json::to_value(&yaml).unwrap();
        let r = parse_readouts_block(json.get("readouts")).unwrap();
        assert_eq!(r.on_phase_start, vec!["trace".to_string()]);
        assert_eq!(r.on_phase_end,   vec!["trace".to_string()]);
        assert!(r.on_each_start.is_empty());
    }

    #[test]
    fn readouts_block_universal_wildcard_expands_to_all() {
        let yaml = serde_yaml::from_str::<serde_yaml::Value>(
            r#"
readouts:
  "*": trace
"#).unwrap();
        let json = serde_json::to_value(&yaml).unwrap();
        let r = parse_readouts_block(json.get("readouts")).unwrap();
        for slot in [
            &r.on_session_start, &r.on_session_end,
            &r.on_phase_start,   &r.on_phase_end,
            &r.on_each_start,    &r.on_each_end,
            &r.on_scope_start,   &r.on_scope_end,
            &r.on_update,
        ] {
            assert_eq!(slot, &vec!["trace".to_string()]);
        }
    }

    #[test]
    fn readouts_block_unknown_slot_is_error() {
        let yaml = serde_yaml::from_str::<serde_yaml::Value>(
            r#"
readouts:
  on_unknown: phase_done
"#).unwrap();
        let json = serde_json::to_value(&yaml).unwrap();
        let err = parse_readouts_block(json.get("readouts")).unwrap_err();
        assert!(err.contains("unknown slot 'on_unknown'"),
            "wrong message: {err}");
    }

    #[test]
    fn parse_single_string_op() {
        let ops = parse_ops("op: select * from bar.table;").unwrap();
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].name, "stmt1");
        assert_eq!(ops[0].op["stmt"], "select * from bar.table;");
    }

    #[test]
    fn parse_ops_list_of_strings() {
        let yaml = r#"
ops:
  - select * from t1;
  - select * from t2;
"#;
        let ops = parse_ops(yaml).unwrap();
        assert_eq!(ops.len(), 2);
        assert_eq!(ops[0].op["stmt"], "select * from t1;");
        assert_eq!(ops[1].op["stmt"], "select * from t2;");
    }

    #[test]
    fn parse_ops_map_of_strings() {
        let yaml = r#"
ops:
  read: select * from t1;
  write: insert into t1 values (1);
"#;
        let ops = parse_ops(yaml).unwrap();
        assert_eq!(ops.len(), 2);
        let read = ops.iter().find(|o| o.name == "read").unwrap();
        assert_eq!(read.op["stmt"], "select * from t1;");
    }

    #[test]
    fn parse_named_blocks() {
        let yaml = r#"
blocks:
  schema:
    ops:
      create: "CREATE TABLE t (id int PRIMARY KEY);"
  main:
    ops:
      read: "SELECT * FROM t WHERE id={id};"
"#;
        let ops = parse_ops(yaml).unwrap();
        assert_eq!(ops.len(), 2);
        let create = ops.iter().find(|o| o.name == "create").unwrap();
        assert_eq!(create.tags["block"], "schema");
        let read = ops.iter().find(|o| o.name == "read").unwrap();
        assert_eq!(read.tags["block"], "main");
    }

    #[test]
    fn parse_property_inheritance() {
        let yaml = r#"
bindings:
  id: Identity()
params:
  prepared: true
tags:
  workload: test
blocks:
  main:
    bindings:
      id: Hash()
    ops:
      op1: "SELECT * FROM t;"
"#;
        let ops = parse_ops(yaml).unwrap();
        assert_eq!(ops.len(), 1);
        // Block-level binding overrides doc-level
        assert_eq!(ops[0].bindings.as_map()["id"], "Hash()");
        // Doc-level param inherited
        assert_eq!(ops[0].params["prepared"], true);
        // Doc-level tag inherited
        assert_eq!(ops[0].tags["workload"], "test");
        // Auto-tag
        assert_eq!(ops[0].tags["block"], "main");
    }

    #[test]
    fn parse_auto_naming() {
        let yaml = r#"
ops:
  - "first op"
  - "second op"
"#;
        let ops = parse_ops(yaml).unwrap();
        assert_eq!(ops[0].name, "stmt1");
        assert_eq!(ops[1].name, "stmt2");
    }

    #[test]
    fn parse_auto_tagging() {
        let yaml = r#"
ops:
  myop: "SELECT 1;"
"#;
        let ops = parse_ops(yaml).unwrap();
        assert_eq!(ops[0].tags["name"], "myop");
        assert_eq!(ops[0].tags["op"], "myop");
        assert_eq!(ops[0].tags["block"], "block0");
    }

    #[test]
    fn condition_clause_passthrough_for_identifier() {
        // Bare identifier — legacy "name a binding" form.
        assert_eq!(normalize_condition_clause("my_flag"), "my_flag");
        assert_eq!(normalize_condition_clause(" my_flag "), "my_flag");
    }

    #[test]
    fn condition_clause_passthrough_for_braced_forms() {
        assert_eq!(normalize_condition_clause("{my_flag}"), "{my_flag}");
        assert_eq!(normalize_condition_clause("{{x == 1}}"), "{{x == 1}}");
        assert_eq!(normalize_condition_clause("{:=x == 1:=}"), "{:=x == 1:=}");
    }

    #[test]
    fn condition_clause_wraps_bare_expressions() {
        assert_eq!(
            normalize_condition_clause("cql_dialect == 'cass'"),
            "{{cql_dialect == 'cass'}}",
        );
        assert_eq!(
            normalize_condition_clause("a > 0 && b < 10"),
            "{{a > 0 && b < 10}}",
        );
        assert_eq!(
            normalize_condition_clause("foo(bar)"),
            "{{foo(bar)}}",
        );
    }

    #[test]
    fn condition_clause_empty_passthrough() {
        assert_eq!(normalize_condition_clause(""), "");
        assert_eq!(normalize_condition_clause("   "), "");
    }

    #[test]
    fn parse_op_with_fields() {
        let yaml = r#"
ops:
  op1:
    field1: value1
    field2: value2
"#;
        let ops = parse_ops(yaml).unwrap();
        assert_eq!(ops[0].op["field1"], "value1");
        assert_eq!(ops[0].op["field2"], "value2");
    }

    #[test]
    fn parse_explicit_op_field() {
        let yaml = r#"
ops:
  op1:
    op:
      stmt: "SELECT * FROM t;"
      type: query
"#;
        let ops = parse_ops(yaml).unwrap();
        assert_eq!(ops[0].op["stmt"], "SELECT * FROM t;");
        assert_eq!(ops[0].op["type"], "query");
    }

    #[test]
    fn parse_scenarios() {
        let yaml = r#"
scenarios:
  default:
    schema: run driver=cql tags==block:schema threads==1
    main: run driver=cql tags==block:main cycles=1M
ops:
  op1: "test"
"#;
        let workload = parse_workload(yaml, &HashMap::new()).unwrap();
        let default = &workload.scenarios["default"];
        assert_eq!(default.len(), 2);
        // Legacy command-string format: names are preserved as Phase nodes
        assert!(matches!(&default[0], ScenarioNode::Phase(n) if n == "schema"));
        assert!(matches!(&default[1], ScenarioNode::Phase(n) if n == "main"));
    }

    #[test]
    fn parse_template_expansion() {
        let yaml = r#"
ops:
  op1: "SELECT * FROM t LIMIT TEMPLATE(limit, 100);"
"#;
        let ops = parse_ops(yaml).unwrap();
        assert_eq!(ops[0].op["stmt"], "SELECT * FROM t LIMIT 100;");
    }

    #[test]
    fn parse_description() {
        let yaml = r#"
description: |
  This is a test workload.
ops:
  op1: "test"
"#;
        let workload = parse_workload(yaml, &HashMap::new()).unwrap();
        assert!(workload.description.unwrap().contains("test workload"));
    }

    #[test]
    fn parse_gk_source_bindings() {
        // Native GK grammar: explicit named wires, full DAG
        let yaml = r#"
bindings: |
  // Explicit wiring — every intermediate is named
  inputs := (cycle)
  h := hash(cycle)
  user_id := mod(h, 1000000)
  code_hash := hash(user_id)
  code := combinations(code_hash, '0-9A-Z')

  // Equivalent concise form (nested composition):
  // user_id := mod(hash(cycle), 1000000)
  // code := combinations(hash(user_id), '0-9A-Z')
ops:
  insert: "INSERT INTO users (id, code) VALUES ({user_id}, '{code}');"
"#;
        let ops = parse_ops(yaml).unwrap();
        assert_eq!(ops.len(), 1);
        match &ops[0].bindings {
            BindingsDef::GkSource(src) => {
                assert!(src.contains("inputs := (cycle)"));
                assert!(src.contains("user_id := mod(h, 1000000)"));
            }
            BindingsDef::Map(_) => panic!("expected GkSource, got Map"),
        }
    }

    #[test]
    fn parse_map_bindings_still_works() {
        let yaml = r#"
bindings:
  id: "Hash(); Mod(100)"
ops:
  op1: "SELECT * FROM t WHERE id={id};"
"#;
        let ops = parse_ops(yaml).unwrap();
        assert_eq!(ops[0].bindings.as_map()["id"], "Hash(); Mod(100)");
    }

    #[test]
    fn parse_phased_workload() {
        let yaml = r#"
scenarios:
  default:
    - schema
    - main

phases:
  schema:
    cycles: 1
    concurrency: 1
    ops:
      create_table:
        stmt: "CREATE TABLE t (id int PRIMARY KEY);"
  main:
    cycles: 1000
    concurrency: 10
    rate: 500.0
    ops:
      read:
        stmt: "SELECT * FROM t WHERE id={id};"
      write:
        stmt: "INSERT INTO t (id) VALUES ({id});"
"#;
        let workload = parse_workload(yaml, &HashMap::new()).unwrap();

        // Phases parsed
        assert_eq!(workload.phases.len(), 2);
        assert!(workload.phases.contains_key("schema"));
        assert!(workload.phases.contains_key("main"));

        // Phase order preserved
        assert_eq!(workload.phase_order, vec!["schema", "main"]);

        // Schema phase config
        let schema = &workload.phases["schema"];
        assert_eq!(schema.cycles.as_deref(), Some("1"));
        assert_eq!(schema.concurrency.as_deref(), Some("1"));
        assert_eq!(schema.rate, None);
        assert_eq!(schema.ops.len(), 1);
        assert_eq!(schema.ops[0].name, "create_table");

        // Main phase config
        let main = &workload.phases["main"];
        assert_eq!(main.cycles.as_deref(), Some("1000"));
        assert_eq!(main.concurrency.as_deref(), Some("10"));
        assert_eq!(main.rate, Some(500.0));
        assert_eq!(main.ops.len(), 2);

        // Scenario parsed as phase name list
        let default = &workload.scenarios["default"];
        assert_eq!(default.len(), 2);
        assert!(matches!(&default[0], ScenarioNode::Phase(n) if n == "schema"));
        assert!(matches!(&default[1], ScenarioNode::Phase(n) if n == "main"));
    }

    #[test]
    fn parse_phased_workload_with_tags() {
        let yaml = r#"
blocks:
  schema:
    ops:
      create: "CREATE TABLE t (id int PRIMARY KEY);"
  main:
    ops:
      read: "SELECT * FROM t;"

phases:
  setup:
    tags: "block:schema"
    cycles: 1
  run:
    tags: "block:main"
    cycles: 1000
"#;
        let workload = parse_workload(yaml, &HashMap::new()).unwrap();
        assert_eq!(workload.phases.len(), 2);

        let setup = &workload.phases["setup"];
        assert_eq!(setup.tags.as_deref(), Some("block:schema"));
        assert!(setup.ops.is_empty()); // No inline ops, uses tag filter

        let run = &workload.phases["run"];
        assert_eq!(run.tags.as_deref(), Some("block:main"));
    }

    #[test]
    fn parse_phased_workload_gk_cycles() {
        let yaml = r#"
phases:
  rampup:
    cycles: "{train_count}"
    concurrency: 100
    ops:
      insert:
        stmt: "INSERT INTO t (id) VALUES ({id});"
"#;
        let workload = parse_workload(yaml, &HashMap::new()).unwrap();
        let rampup = &workload.phases["rampup"];
        assert_eq!(rampup.cycles.as_deref(), Some("{train_count}"));
    }

    // ── SRD-40b §1 + §2: `metrics:` discriminant on op template ──

    #[test]
    fn parse_metrics_full_mapping_form() {
        let yaml = r##"
phases:
  predict:
    bindings: |
      example_factor := 1.0 + 2.5
    ops:
      synth:
        stmt: "noop"
        metrics:
          example_factor:
            value: example_factor
            kind: gauge
            unit: ratio
            format: "#.##"
"##;
        let wl = parse_workload(yaml, &HashMap::new()).unwrap();
        let op = &wl.phases["predict"].ops[0];
        assert_eq!(op.name, "synth");
        let m = &op.metrics["example_factor"];
        assert_eq!(m.value, "example_factor");
        assert_eq!(m.kind, Some(crate::model::MetricKind::Gauge));
        assert_eq!(m.unit.as_deref(), Some("ratio"));
        assert_eq!(m.format.as_deref(), Some("#.##"));
    }

    #[test]
    fn parse_metrics_bare_string_sugar() {
        let yaml = r#"
phases:
  p:
    bindings: |
      overscan := 1.0
    ops:
      o:
        stmt: "noop"
        metrics: overscan
"#;
        let wl = parse_workload(yaml, &HashMap::new()).unwrap();
        let op = &wl.phases["p"].ops[0];
        let m = &op.metrics["overscan"];
        // Bare-string form: family + value both = "overscan"
        // (defaults), kind unset (defaults to gauge at runtime).
        assert_eq!(m.value, "overscan");
        assert_eq!(m.family, None);
        assert_eq!(m.kind, None);
    }

    #[test]
    fn parse_metrics_list_with_wire_expression() {
        let yaml = r#"
phases:
  p:
    ops:
      o:
        stmt: "noop"
        metrics:
          - latency_pred := 0.5 + 1.5 * pow(limit, -0.4)
          - already_bound
"#;
        let wl = parse_workload(yaml, &HashMap::new()).unwrap();
        let op = &wl.phases["p"].ops[0];
        // Both metric entries registered.
        assert!(op.metrics.contains_key("latency_pred"));
        assert!(op.metrics.contains_key("already_bound"));
        // Wire expression auto-injected into op bindings.
        match &op.bindings {
            BindingsDef::GkSource(src) => {
                assert!(src.contains("latency_pred := 0.5 + 1.5 * pow(limit, -0.4)"),
                    "wire not injected; bindings: {src:?}");
            }
            other => panic!("expected GkSource bindings, got {other:?}"),
        }
    }

    #[test]
    fn parse_metrics_mapping_form_with_wire_expression() {
        let yaml = r#"
phases:
  p:
    bindings: |
      base := 10
    ops:
      o:
        stmt: "noop"
        metrics:
          scaled:
            value: base * 2
            kind: gauge
"#;
        let wl = parse_workload(yaml, &HashMap::new()).unwrap();
        let op = &wl.phases["p"].ops[0];
        let m = &op.metrics["scaled"];
        // After auto-inject the spec's `value:` is the bare key.
        assert_eq!(m.value, "scaled");
        // The non-bare expression landed in op-template bindings.
        match &op.bindings {
            BindingsDef::GkSource(src) => {
                assert!(src.contains("scaled := base * 2"),
                    "expression not injected; bindings: {src:?}");
            }
            other => panic!("expected GkSource bindings, got {other:?}"),
        }
    }

    #[test]
    fn parse_metrics_mapping_form_invalid_key_for_non_bare_value() {
        // Non-bare value + key that can't be a binding name → reject.
        let yaml = r#"
phases:
  p:
    ops:
      o:
        stmt: "noop"
        metrics:
          "1bad":
            value: foo + 1
            kind: gauge
"#;
        let err = parse_workload(yaml, &HashMap::new()).unwrap_err();
        assert!(err.contains("must itself be a valid identifier"),
            "expected identifier diagnostic, got: {err}");
    }

    #[test]
    fn parse_metrics_format_validation_runs_at_load() {
        let yaml = r##"
phases:
  p:
    ops:
      o:
        stmt: "noop"
        metrics:
          x:
            value: x
            format: "%3.2f"
"##;
        let err = parse_workload(yaml, &HashMap::new()).unwrap_err();
        assert!(err.contains("printf-style"),
            "format error not surfaced at parse time: {err}");
    }

    #[test]
    fn parse_metrics_wire_expression_collision_errors() {
        let yaml = r#"
phases:
  p:
    ops:
      o:
        stmt: "noop"
        bindings: |
          foo := 1.0
        metrics:
          - foo := 2.0
"#;
        let err = parse_workload(yaml, &HashMap::new()).unwrap_err();
        assert!(err.contains("collides"),
            "collision not detected: {err}");
    }

    #[test]
    fn parse_phase_bindings_round_trip() {
        // SRD-13d Phase 1: phase-level `bindings:` block must
        // land on WorkloadPhase.bindings (not just merged into
        // ops) so HasGkMatter can classify it.
        let yaml = r#"
phases:
  p:
    bindings: |
      phase_factor := 7
    ops:
      o:
        stmt: "noop"
"#;
        let wl = parse_workload(yaml, &HashMap::new()).unwrap();
        match &wl.phases["p"].bindings {
            BindingsDef::GkSource(s) => assert!(s.contains("phase_factor := 7")),
            other => panic!("expected GkSource, got {other:?}"),
        }
    }

    #[test]
    fn parse_phased_workload_default_scenario_from_order() {
        // No scenarios section — phases should run in definition order
        let yaml = r#"
phases:
  alpha:
    cycles: 1
    ops:
      op1:
        stmt: "a"
  beta:
    cycles: 2
    ops:
      op2:
        stmt: "b"
  gamma:
    cycles: 3
    ops:
      op3:
        stmt: "c"
"#;
        let workload = parse_workload(yaml, &HashMap::new()).unwrap();
        assert_eq!(workload.phase_order, vec!["alpha", "beta", "gamma"]);
        assert!(workload.scenarios.is_empty());
    }

    #[test]
    fn parse_backward_compat_no_phases() {
        // Workload without phases should work exactly as before
        let yaml = r#"
ops:
  op1: "SELECT 1;"
  op2: "SELECT 2;"
"#;
        let workload = parse_workload(yaml, &HashMap::new()).unwrap();
        assert!(workload.phases.is_empty());
        assert!(workload.phase_order.is_empty());
        assert_eq!(workload.ops.len(), 2);
    }

    #[test]
    fn block_level_params_override_workload_default() {
        // SRD 21 §"Parameter Resolution": closest-wins. The DDL
        // block declares concurrency=1 and that overrides the
        // workload-level default of 100 for ops in that block.
        let yaml = r#"
params:
  concurrency: "100"
blocks:
  ddl:
    params:
      concurrency: "1"
    ops:
      schema_create: "CREATE TABLE foo (id int PRIMARY KEY);"
  bulk:
    ops:
      insert: "INSERT INTO foo (id) VALUES (?);"
"#;
        let ops = parse_ops(yaml).unwrap();
        let ddl = ops.iter().find(|o| o.name == "schema_create").unwrap();
        let bulk = ops.iter().find(|o| o.name == "insert").unwrap();
        assert_eq!(
            ddl.params.get("concurrency").and_then(|v| v.as_str()),
            Some("1"),
            "block-level override should win for ddl op",
        );
        assert_eq!(
            bulk.params.get("concurrency").and_then(|v| v.as_str()),
            Some("100"),
            "non-overriding block inherits workload-level default",
        );
    }

    #[test]
    fn cli_overrides_block_level_params() {
        // CLI is the outermost layer per SRD 21 — it wins even
        // over block-level explicit overrides.
        let yaml = r#"
params:
  concurrency: "100"
blocks:
  ddl:
    params:
      concurrency: "1"
    ops:
      schema_create: "CREATE TABLE foo (id int PRIMARY KEY);"
"#;
        let mut cli = HashMap::new();
        cli.insert("concurrency".to_string(), "200".to_string());
        let workload = parse_workload(yaml, &cli).unwrap();
        let ddl = workload.ops.iter()
            .find(|o| o.name == "schema_create").unwrap();
        assert_eq!(
            ddl.params.get("concurrency").and_then(|v| v.as_str()),
            Some("200"),
            "CLI override should beat block-level",
        );
        // Workload-level params likewise reflect CLI.
        assert_eq!(
            workload.params.get("concurrency").map(|s| s.as_str()),
            Some("200"),
        );
    }

    #[test]
    fn parse_gk_source_overrides_parent_map() {
        // Block-level GK source completely replaces doc-level map bindings
        let yaml = r#"
bindings:
  id: "Hash()"
blocks:
  main:
    bindings: |
      inputs := (cycle)
      h := hash(cycle)
      id := mod(h, 1000)
      // Concise equivalent:
      // id := mod(hash(cycle), 1000)
    ops:
      op1: "SELECT * FROM t WHERE id={id};"
"#;
        let ops = parse_ops(yaml).unwrap();
        match &ops[0].bindings {
            BindingsDef::GkSource(src) => {
                assert!(src.contains("inputs := (cycle)"));
                assert!(src.contains("id := mod(h, 1000)"));
            }
            BindingsDef::Map(_) => panic!("expected GkSource, got Map"),
        }
    }

    #[test]
    fn parse_scenarios_plural_list_form() {
        // The plural `scenarios: [a, b, c]` form composes
        // several named scenarios at one node. Each list
        // entry expands to an `IncludedScenario` and resolves
        // post-parse. Equivalent to `[- scenario: a, -
        // scenario: b, ...]` but reads more naturally for the
        // "just compose these" case.
        let yaml = r#"
scenarios:
  rampup:
    - prep
  query:
    - run

  composed:
    - scenarios:
        - rampup
        - query

phases:
  prep:
    ops:
      create:
        raw: "select 1"
  run:
    ops:
      sel:
        raw: "select {cycle}"
"#;
        let workload = parse_workload(yaml, &HashMap::new()).unwrap();
        let composed = workload.scenarios.get("composed")
            .expect("composed scenario must parse");
        // After resolution the IncludedScenario wrappers carry
        // their resolved children. The scenario tree shape is:
        //   composed
        //     └── IncludedScenario("rampup") [Phase("prep")]
        //     └── IncludedScenario("query")  [Phase("run")]
        // Walk one level deep into each include to assert.
        assert_eq!(composed.len(), 2,
            "scenarios: [a, b] should produce two top-level nodes");
        let names: Vec<&str> = composed.iter().filter_map(|n| match n {
            ScenarioNode::IncludedScenario { name, .. } => Some(name.as_str()),
            _ => None,
        }).collect();
        assert_eq!(names, vec!["rampup", "query"]);
        // First include resolves to its sole `Phase("prep")`.
        let first_children = match &composed[0] {
            ScenarioNode::IncludedScenario { children, .. } => children,
            _ => panic!("expected IncludedScenario at index 0"),
        };
        let first_phase = first_children.iter().find_map(|n| match n {
            ScenarioNode::Phase(p) => Some(p.as_str()),
            _ => None,
        });
        assert_eq!(first_phase, Some("prep"));
    }

    #[test]
    fn parse_scenarios_plural_mixes_with_other_node_shapes() {
        // List entries can be a mix of bare strings and other
        // scenario-node shapes (objects with `scenario:`,
        // `for_each:`, etc.). This matches the heterogeneous
        // shape `parse_scenario_nodes` already accepts at the
        // top level, so the plural form composes naturally
        // with everything else.
        let yaml = r#"
scenarios:
  rampup:
    - prep
  composed:
    - scenarios:
        - rampup
        - { scenario: rampup }

phases:
  prep:
    ops:
      create:
        raw: "select 1"
"#;
        let workload = parse_workload(yaml, &HashMap::new()).unwrap();
        let composed = workload.scenarios.get("composed").unwrap();
        // Both list entries should resolve to an IncludedScenario
        // wrapping the same `prep` phase.
        assert_eq!(composed.len(), 2);
        for node in composed {
            match node {
                ScenarioNode::IncludedScenario { name, children } => {
                    assert_eq!(name, "rampup");
                    assert!(children.iter().any(|c| matches!(c, ScenarioNode::Phase(p) if p == "prep")));
                }
                other => panic!("expected IncludedScenario, got {other:?}"),
            }
        }
    }

    // -----------------------------------------------------------------
    // Checkpoint declaration parsing — SRD-44 §"Forms"
    // -----------------------------------------------------------------

    fn parse_checkpoint_field(yaml: &str) -> Option<crate::model::Checkpoint> {
        let yaml = format!(
            "phases:\n  p:\n{}\n    ops:\n      - select 1;\n",
            yaml.lines().map(|l| format!("    {l}")).collect::<Vec<_>>().join("\n")
        );
        let v: serde_yaml::Value = serde_yaml::from_str(&yaml).expect("yaml parse");
        let json: serde_json::Value = serde_json::to_value(&v).expect("json convert");
        let phases_obj = json.get("phases").and_then(|p| p.as_object()).expect("phases");
        let phase = phases_obj.get("p").and_then(|p| p.as_object()).expect("phase p");
        phase.get("checkpoint")
            .map(|v| serde_json::from_value::<crate::model::Checkpoint>(v.clone()).expect("checkpoint parse"))
    }

    #[test]
    fn checkpoint_short_form_idempotent() {
        let cp = parse_checkpoint_field("checkpoint: idempotent").expect("present");
        assert!(cp.idempotent);
        assert!(cp.hashed);
        assert!(cp.verify.is_none());
    }

    #[test]
    fn checkpoint_short_form_none_disables_skip() {
        let cp = parse_checkpoint_field("checkpoint: none").expect("present");
        assert!(!cp.idempotent);
        // hashed default-true is preserved even when disabled —
        // the disabled state is about skip eligibility, not
        // about the hash field.
        assert!(cp.hashed);
        assert!(cp.verify.is_none());
    }

    #[test]
    fn checkpoint_short_form_no_and_false_and_off_all_disable() {
        for word in &["no", "false", "off"] {
            let cp = parse_checkpoint_field(&format!("checkpoint: {word}")).expect("present");
            assert!(!cp.idempotent, "expected disabled for '{word}'");
        }
    }

    #[test]
    fn checkpoint_bool_false_disables() {
        // YAML's bare `false` should map to disabled.
        let cp = parse_checkpoint_field("checkpoint: false").expect("present");
        assert!(!cp.idempotent);
    }

    #[test]
    fn checkpoint_full_form_all_explicit() {
        let cp = parse_checkpoint_field(
            "checkpoint:\n  idempotent: true\n  hashed: false"
        ).expect("present");
        assert!(cp.idempotent);
        assert!(!cp.hashed);
        assert!(cp.verify.is_none());
    }

    #[test]
    fn checkpoint_full_form_with_verify() {
        let cp = parse_checkpoint_field(
            "checkpoint:\n  idempotent: true\n  verify:\n    raw: 'SELECT 1'\n    poll: assert_one"
        ).expect("present");
        assert!(cp.idempotent);
        assert!(cp.hashed); // default
        let v = cp.verify.expect("verify body");
        assert_eq!(v.get("raw").and_then(|x| x.as_str()), Some("SELECT 1"));
        assert_eq!(v.get("poll").and_then(|x| x.as_str()), Some("assert_one"));
    }

    #[test]
    fn checkpoint_full_form_idempotent_false_equivalent_to_none() {
        let cp = parse_checkpoint_field(
            "checkpoint:\n  idempotent: false\n  hashed: true"
        ).expect("present");
        assert!(!cp.idempotent);
        assert!(cp.hashed);
    }

    #[test]
    fn checkpoint_unknown_short_form_errors() {
        // Should fail to parse — an unknown short string is a
        // workload bug, not silently treated as `none`.
        let yaml = "phases:\n  p:\n    checkpoint: maybe\n    ops:\n      - select 1;\n";
        let v: serde_yaml::Value = serde_yaml::from_str(yaml).unwrap();
        let json: serde_json::Value = serde_json::to_value(&v).unwrap();
        let phases_obj = json.get("phases").and_then(|p| p.as_object()).unwrap();
        let phase = phases_obj.get("p").and_then(|p| p.as_object()).unwrap();
        let cp_val = phase.get("checkpoint").unwrap().clone();
        let err = serde_json::from_value::<crate::model::Checkpoint>(cp_val).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("unknown short form"), "expected unknown-short-form error, got: {msg}");
        assert!(msg.contains("'maybe'"), "expected the bad token in error, got: {msg}");
    }

    #[test]
    fn checkpoint_unknown_key_errors() {
        let yaml = "phases:\n  p:\n    checkpoint:\n      idempotent: true\n      bogus: yes\n    ops:\n      - select 1;\n";
        let v: serde_yaml::Value = serde_yaml::from_str(yaml).unwrap();
        let json: serde_json::Value = serde_json::to_value(&v).unwrap();
        let cp_val = json.pointer("/phases/p/checkpoint").unwrap().clone();
        let err = serde_json::from_value::<crate::model::Checkpoint>(cp_val).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("unknown key 'bogus'"), "expected unknown-key error, got: {msg}");
    }

    #[test]
    fn checkpoint_field_absent_yields_none() {
        let cp = parse_checkpoint_field("# no checkpoint declared\n");
        assert!(cp.is_none(), "absent declaration should yield None, not Default");
    }

    /// SRD-44 validation: a phase declared `checkpoint:
    /// idempotent` inside a do_while loop must be rejected at
    /// workload-load time. The do-loop iterates the same phase
    /// identity multiple times, contradicting the per-execution
    /// unit checkpointing assumes.
    #[test]
    fn rejects_idempotent_phase_inside_do_while() {
        let yaml = r#"
scenarios:
  default:
    - do_while: "true"
      phases:
        - probe
phases:
  probe:
    checkpoint: idempotent
    cycles: 1
    ops:
      step:
        stmt: "probe"
"#;
        let err = super::parse_workload(yaml, &HashMap::new())
            .expect_err("expected validation rejection");
        assert!(err.contains("checkpoint: idempotent"),
            "error should explain the rejection: {err}");
        assert!(err.contains("'probe'"),
            "error should name the offending phase: {err}");
        assert!(err.contains("do_while") || err.contains("do_until"),
            "error should mention the do-loop ancestor: {err}");
    }

    /// `do_until` triggers the same rejection.
    #[test]
    fn rejects_idempotent_phase_inside_do_until() {
        let yaml = r#"
scenarios:
  default:
    - do_until: "false"
      phases:
        - probe
phases:
  probe:
    checkpoint: idempotent
    cycles: 1
    ops:
      step:
        stmt: "probe"
"#;
        let err = super::parse_workload(yaml, &HashMap::new())
            .expect_err("expected validation rejection");
        assert!(err.contains("'probe'"));
    }

    /// A do_while'd phase WITHOUT checkpoint declaration is
    /// fine — only the combination with `checkpoint:
    /// idempotent` is rejected.
    #[test]
    fn allows_do_while_phase_without_checkpoint_declaration() {
        let yaml = r#"
scenarios:
  default:
    - do_while: "true"
      phases:
        - probe
phases:
  probe:
    cycles: 1
    ops:
      step:
        stmt: "probe"
"#;
        super::parse_workload(yaml, &HashMap::new())
            .expect("plain do_while phase should parse");
    }

    /// Idempotent phases NOT inside a do-loop are fine.
    #[test]
    fn allows_idempotent_phase_outside_do_loop() {
        let yaml = r#"
scenarios:
  default:
    - probe
phases:
  probe:
    checkpoint: idempotent
    cycles: 1
    ops:
      step:
        stmt: "probe"
"#;
        super::parse_workload(yaml, &HashMap::new())
            .expect("idempotent phase outside loop should parse");
    }
}
