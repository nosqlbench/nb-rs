// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! GK kernel analysis output for diagnostic modes.
//!
//! Called by the runner when `dryrun=gk` is active. Renders
//! provenance, data flow, and scope composition from a compiled
//! GkProgram's introspection APIs.

use std::sync::Arc;
use nb_variates::kernel::GkProgram;

/// Print kernel analysis for a phase/iteration scope.
///
/// Called by the runner at the point where it would normally
/// dispatch cycles. The kernel has already been compiled through
/// the exact same pipeline as execution.
pub fn print_kernel_analysis(
    phase_name: &str,
    iter_note: &str,
    program: &Arc<GkProgram>,
) {
    let input_names = program.input_names();
    let coord_count = program.coord_count();

    println!("  Phase '{phase_name}'{iter_note} ({} nodes, {} outputs):",
        program.node_count(), program.output_count());

    for (i, name) in input_names.iter().enumerate() {
        let kind = if i < coord_count { "coordinate" } else { "extern" };
        println!("    input {name}: {kind}");
    }

    for i in 0..program.output_count() {
        let name = program.output_name(i);
        let (node_idx, port_idx) = program.resolve_output_by_index(i);
        let meta = program.node_meta(node_idx);
        let provenance = program.input_provenance_for(node_idx);
        let modifier = program.output_modifier(name);
        let is_const = program.node_wiring(node_idx).is_empty();

        let mut deps: Vec<String> = Vec::new();
        for (j, inp_name) in input_names.iter().enumerate() {
            if provenance & (1 << j) != 0 {
                deps.push(inp_name.clone());
            }
        }

        let mod_str = match modifier {
            nb_variates::dsl::ast::BindingModifier::Shared => " [shared]",
            nb_variates::dsl::ast::BindingModifier::Final => " [final]",
            nb_variates::dsl::ast::BindingModifier::None => "",
        };
        let out_type = if port_idx < meta.outs.len() {
            format!("{:?}", meta.outs[port_idx].typ)
        } else { "?".into() };

        print!("    {name}{mod_str}: {out_type}");
        if is_const {
            println!("  (const-folded at compile time)");
        } else if deps.is_empty() {
            println!("  (no input deps)");
        } else {
            println!("  (per-cycle, depends on: {})", deps.join(", "));
        }

        let wiring = program.node_wiring(node_idx);
        if !wiring.is_empty() {
            let descs: Vec<String> = wiring.iter().map(|w| match w {
                nb_variates::kernel::WireSource::Input(idx) => {
                    if *idx < input_names.len() {
                        format!("input:{}", input_names[*idx])
                    } else {
                        format!("input:{idx}")
                    }
                }
                nb_variates::kernel::WireSource::NodeOutput(ni, pi) => {
                    let u = program.node_meta(*ni);
                    if *pi == 0 { u.name.clone() } else { format!("{}[{pi}]", u.name) }
                }
            }).collect();
            println!("      node: {}({})", meta.name, descs.join(", "));
        }
    }
    println!();
}
