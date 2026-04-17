// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! YAML workload parser and normalizer.
//!
//! Parses a YAML workload definition and normalizes all shorthand
//! forms into the canonical `ParsedOp` model.

use std::collections::HashMap;
use serde_json::Value as JVal;
use crate::model::{BindingsDef, ParsedOp, ScenarioNode, Workload, WorkloadPhase};
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

    let scenarios = parse_scenarios(obj.get("scenarios"));

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
    let (phases, phase_order) = parse_phases(obj.get("phases"), &doc_bindings, &doc_params, &doc_tags);

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

    // Summary report configuration: top-level `summary:` key.
    // Absent = no summary printed. Present = master switch + format config.
    let summary = obj.get("summary")
        .and_then(|v| v.as_str())
        .map(|s| crate::model::SummaryConfig::parse(s));

    Ok(Workload { description, scenarios, ops: all_ops, params: resolved_params, phases, phase_order, declared_params, summary })
}

/// Parse a YAML source into just the list of normalized ParsedOps.
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

            if let Some(for_each_val) = obj.get("for_each") {
                // for_each supports all three forms:
                // String with single "var in expr" → ForEach
                // String with multiple "var in expr, var2 in expr2" → ForCombinations
                // Map or Array → ForCombinations (same as for_combinations)
                match for_each_val {
                    JVal::String(spec) => {
                        // Check if it's multi-variable (contains multiple "in" clauses)
                        let clause_count = spec.matches(" in ").count();
                        if clause_count > 1 {
                            let specs = parse_combination_specs(for_each_val);
                            vec![ScenarioNode::ForCombinations { specs, children }]
                        } else {
                            vec![ScenarioNode::ForEach { spec: spec.clone(), children }]
                        }
                    }
                    JVal::Object(_) | JVal::Array(_) => {
                        let specs = parse_combination_specs(for_each_val);
                        vec![ScenarioNode::ForCombinations { specs, children }]
                    }
                    _ => vec![]
                }
            } else if let Some(combo_val) = obj.get("for_combinations") {
                // Explicit for_combinations keyword (alias for multi-var for_each)
                let specs = parse_combination_specs(combo_val);
                vec![ScenarioNode::ForCombinations { specs, children }]
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
                    let parts: Vec<&str> = s.splitn(2, " in ").collect();
                    if parts.len() == 2 {
                        Some((parts[0].trim().to_string(), parts[1].trim().to_string()))
                    } else {
                        eprintln!("warning: invalid for_combinations spec: '{s}'");
                        None
                    }
                })
                .collect()
        }
        // Inline form: "profile in expr, k in expr"
        // Split on commas that are NOT inside parentheses (respects
        // function calls like `matching_profiles('{dataset}', '{prefix}')`).
        JVal::String(s) => {
            split_respecting_parens(s)
                .iter()
                .filter_map(|part| {
                    let parts: Vec<&str> = part.trim().splitn(2, " in ").collect();
                    if parts.len() == 2 {
                        Some((parts[0].trim().to_string(), parts[1].trim().to_string()))
                    } else {
                        eprintln!("warning: invalid for_combinations spec: '{}'", part.trim());
                        None
                    }
                })
                .collect()
        }
        _ => {
            eprintln!("warning: for_combinations value must be a map, list, or string");
            Vec::new()
        }
    }
}

/// Split a string on commas, respecting parenthesized groups.
///
/// Commas inside `(...)` are not treated as separators.
/// This allows function calls like `matching_profiles('{a}', '{b}')`
/// to survive the multi-variable `for_each` split.
fn split_respecting_parens(s: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut depth = 0u32;
    for ch in s.chars() {
        match ch {
            '(' => { depth += 1; current.push(ch); }
            ')' => { depth = depth.saturating_sub(1); current.push(ch); }
            ',' if depth == 0 => {
                parts.push(current.clone());
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    if !current.trim().is_empty() {
        parts.push(current);
    }
    parts
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
) -> (HashMap<String, WorkloadPhase>, Vec<String>) {
    let mut phases = HashMap::new();
    let mut phase_order = Vec::new();
    let Some(val) = val else { return (phases, phase_order); };
    let Some(obj) = val.as_object() else { return (phases, phase_order); };

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

        // Phase-level bindings override workload bindings for ops in this phase
        let phase_bindings = merge_bindings(doc_bindings, &extract_bindings(phase_obj.get("bindings")));

        // Parse inline ops if present
        let mut inline_ops = Vec::new();
        for key in ["ops", "op", "operations", "statements", "statement"] {
            if let Some(ops_val) = phase_obj.get(key) {
                let phase_tags = {
                    let mut t = doc_tags.clone();
                    t.insert("phase".to_string(), phase_name.clone());
                    t
                };
                let _ = parse_ops_field(ops_val, phase_name, &phase_bindings, doc_params, &phase_tags, &mut inline_ops);
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
        let summary = phase_obj.get("summary").cloned();

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
            summary,
        });
        phase_order.push(phase_name.clone());
    }

    (phases, phase_order)
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
                let op = normalize_op_item(item, &auto_name, block_name, bindings, params, tags);
                all_ops.push(op);
            }
        }

        // Map of named ops
        JVal::Object(map) => {
            for (key, val) in map {
                let op = normalize_op_entry(key, val, block_name, bindings, params, tags);
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
) -> ParsedOp {
    match item {
        JVal::String(s) => {
            let mut op = ParsedOp::simple(auto_name, s);
            op.bindings = bindings.clone();
            op.params = params.clone();
            op.tags = tags.clone();
            op.tags.insert("block".to_string(), block_name.to_string());
            op
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
                    return op;
                }
            // Full op object
            normalize_op_object(map, auto_name, block_name, bindings, params, tags)
        }
        _ => ParsedOp::simple(auto_name, ""),
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
) -> ParsedOp {
    match val {
        JVal::String(s) => {
            let mut op = ParsedOp::simple(key, s);
            op.bindings = bindings.clone();
            op.params = params.clone();
            op.tags = tags.clone();
            op.tags.insert("block".to_string(), block_name.to_string());
            op
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
            };
            op.tags.insert("block".to_string(), block_name.to_string());
            op
        }
        _ => ParsedOp::simple(key, ""),
    }
}

/// Normalize a full op object (map of fields).
fn normalize_op_object(
    map: &serde_json::Map<String, JVal>,
    default_name: &str,
    block_name: &str,
    parent_bindings: &BindingsDef,
    parent_params: &HashMap<String, JVal>,
    parent_tags: &HashMap<String, String>,
) -> ParsedOp {
    let name = map.get("name")
        .and_then(|v| v.as_str())
        .unwrap_or(default_name)
        .to_string();

    let description = map.get("description")
        .or_else(|| map.get("desc"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    // Extract recognized fields
    let op_bindings = merge_bindings(parent_bindings, &extract_bindings(map.get("bindings")));
    let op_params = merge_value_maps(parent_params, &extract_value_map(map.get("params")));
    let mut op_tags = merge_string_maps(parent_tags, &extract_string_map(map.get("tags")));
    op_tags.insert("block".to_string(), block_name.to_string());

    // Determine op payload
    let reserved = ["name", "description", "desc", "bindings", "params", "tags", "if", "delay"];
    let op_field_names = ["op", "ops", "operations", "stmt", "statement", "statements"];
    // Activity-level params excised from op fields before the adapter sees them
    let activity_params = ["ratio", "driver", "space", "instrument", "start-timers", "stop-timers",
        "verify", "relevancy", "strict", "poll", "poll_interval_ms", "timeout_ms", "poll_metric_name", "emit",
        "batch", "batchtype"];

    let op_fields = if let Some(explicit_op) = op_field_names.iter()
        .find_map(|k| map.get(*k))
    {
        let m = match explicit_op {
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
            eprintln!("[parse] op '{}': excising activity param '{}' = {}", name, ap, val);
            op_params.insert(ap.to_string(), val.clone());
        }
    }

    let condition = map.get("if")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let delay = map.get("delay")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    ParsedOp {
        name,
        description,
        op: op_fields,
        bindings: op_bindings,
        params: op_params,
        tags: op_tags,
        condition,
        delay,
    }
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
  coordinates := (cycle)
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
                assert!(src.contains("coordinates := (cycle)"));
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
    fn parse_gk_source_overrides_parent_map() {
        // Block-level GK source completely replaces doc-level map bindings
        let yaml = r#"
bindings:
  id: "Hash()"
blocks:
  main:
    bindings: |
      coordinates := (cycle)
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
                assert!(src.contains("coordinates := (cycle)"));
                assert!(src.contains("id := mod(h, 1000)"));
            }
            BindingsDef::Map(_) => panic!("expected GkSource, got Map"),
        }
    }
}
