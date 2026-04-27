// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! The `describe` subcommand: introspect GK functions, stdlib, modules, and DAGs.

use nb_variates::dsl::registry;

pub fn describe_command(args: &[String]) {
    let topic = args.first().map(|s| s.as_str()).unwrap_or("");
    let subtopic = args.get(1).map(|s| s.as_str()).unwrap_or("");

    match (topic, subtopic) {
        ("gk", "functions") => describe_gk_functions(),
        ("gk", "functions-md") => {
            let rest: Vec<String> = args.iter().skip(2).cloned().collect();
            let path = rest.first().map(|s| s.as_str()).unwrap_or("gk_functions.md");
            describe_gk_functions_md(path);
        }
        ("gk", "stdlib") => describe_gk_stdlib(),
        ("gk", "dag") => {
            // Remaining args after "describe gk dag" are the GK source or file
            let rest: Vec<String> = args.iter().skip(2).cloned().collect();
            describe_gk_dag(&rest);
        }
        ("gk", "modules") => {
            let rest: Vec<String> = args.iter().skip(2).cloned().collect();
            describe_gk_modules(&rest);
        }
        ("gk", _) => {
            eprintln!("nbrs describe gk <subtopic>");
            eprintln!("  functions    List all GK node functions");
            eprintln!("  functions-md Dump all GK node functions to a markdown file");
            eprintln!("  stdlib       List embedded standard library modules");
            eprintln!("  dag          Render a GK source as DOT, Mermaid, or SVG");
            eprintln!("  modules      List modules from a directory");
        }
        _ => {
            eprintln!("nbrs describe <topic>");
            eprintln!("  gk           Generation kernel topics");
            eprintln!();
            eprintln!("For workload analysis, use: nbrs run workload=file.yaml dryrun=phase,gk");
        }
    }
}

fn describe_gk_functions() {
    use nb_activity::bindings::probe_compile_level;

    let grouped = registry::by_category();
    let is_tty = std::io::IsTerminal::is_terminal(&std::io::stdout());

    // ANSI color codes
    let (bold, dim, reset, green, cyan, magenta) = if is_tty {
        ("\x1b[1m", "\x1b[2m", "\x1b[0m", "\x1b[32m", "\x1b[36m", "\x1b[35m")
    } else {
        ("", "", "", "", "", "")
    };

    println!();
    println!("{bold}GK Node Functions{reset}");
    println!("{bold}═════════════════{reset}");
    println!();

    for (cat, funcs) in &grouped {
        let cat_name = cat.display_name();
        println!("  {bold}{cyan}── {cat_name} ──{reset}");
        println!();

        for sig in funcs {
            let level = probe_compile_level(sig.name);
            let (p1, p2, p3) = match level {
                registry::CompileLevel::Phase3 => (
                    format!("{green}\u{2713}{reset}"),
                    format!("{green}\u{2713}{reset}"),
                    format!("{green}\u{2713}{reset}"),
                ),
                registry::CompileLevel::Phase2 => (
                    format!("{green}\u{2713}{reset}"),
                    format!("{green}\u{2713}{reset}"),
                    format!("{dim}\u{2717}{reset}"),
                ),
                registry::CompileLevel::Phase1 => (
                    format!("{green}\u{2713}{reset}"),
                    format!("{dim}\u{2717}{reset}"),
                    format!("{dim}\u{2717}{reset}"),
                ),
            };
            let level_col = format!("{bold}P{reset}{p1}{p2}{p3}");

            let const_info = sig.const_param_info();
            let params_desc = if const_info.is_empty() {
                String::new()
            } else {
                let p: Vec<String> = const_info.iter()
                    .map(|(name, required)| {
                        if *required { name.to_string() } else { format!("[{name}]") }
                    })
                    .collect();
                format!("({})", p.join(", "))
            };

            let arity = if sig.outputs == 0 {
                format!("{}→N", sig.wire_input_count())
            } else {
                format!("{}→{}", sig.wire_input_count(), sig.outputs)
            };

            let name_padded = format!("{:<24}", sig.name);
            let params_padded = format!("{:<24}", params_desc);
            let arity_padded = format!("{:<5}", arity);

            print!("  {bold}{magenta}{name_padded}{reset}");
            print!(" {dim}{params_padded}{reset}");
            print!(" {arity_padded}");
            print!("  {level_col}");
            println!("  {dim}{}{reset}", sig.description);
        }
        println!();
    }

    println!("  {bold}Legend:{reset}  {bold}P{reset}{green}\u{2713}{reset}{green}\u{2713}{reset}{green}\u{2713}{reset} = supported levels  {green}\u{2713}{reset} = yes  {dim}\u{2717}{reset} = no");
    println!("    {bold}P{reset}3  Cranelift native code       {dim}(~0.2ns/node){reset}");
    println!("    {bold}P{reset}2  Compiled u64 closure        {dim}(~4.5ns/node){reset}");
    println!("    {bold}P{reset}1  Runtime Value interpreter   {dim}(~70ns/node){reset}");
    println!();
    println!("  {dim}Levels probed from live node instances.{reset}");
    println!("  {dim}Nodes with constant params (mod, div, etc.) reach P3 when{reset}");
    println!("  {dim}constants are known at assembly time, P2 otherwise.{reset}");
    println!();
}

/// Dump all GK node function metadata to a markdown file.
///
/// Writes a complete reference of all registered functions grouped
/// by category, including signatures, parameters, descriptions,
/// and help text.
fn describe_gk_functions_md(path: &str) {
    use nb_activity::bindings::probe_compile_level;
    use std::io::Write;

    let grouped = registry::by_category();
    let mut out = String::new();

    out.push_str("# GK Node Functions Reference\n\n");
    out.push_str("Auto-generated by `nbrs describe gk functions-md`.\n\n");

    // Summary table
    let total: usize = grouped.iter().map(|(_, funcs)| funcs.len()).sum();
    out.push_str(&format!("**{total} functions** across {} categories.\n\n", grouped.len()));

    out.push_str("## Table of Contents\n\n");
    for (cat, funcs) in &grouped {
        let anchor = cat.display_name().to_lowercase().replace(' ', "-");
        out.push_str(&format!("- [{}](#{})", cat.display_name(), anchor));
        out.push_str(&format!(" ({} functions)\n", funcs.len()));
    }
    out.push('\n');

    for (cat, funcs) in &grouped {
        out.push_str(&format!("## {}\n\n", cat.display_name()));

        // Summary table for this category
        out.push_str("| Function | Params | Arity | JIT | Description |\n");
        out.push_str("|----------|--------|-------|-----|-------------|\n");

        for sig in funcs {
            let level = probe_compile_level(sig.name);
            let jit = match level {
                registry::CompileLevel::Phase3 => "P3",
                registry::CompileLevel::Phase2 => "P2",
                registry::CompileLevel::Phase1 => "P1",
            };

            let const_info = sig.const_param_info();
            let params_desc = if const_info.is_empty() {
                String::new()
            } else {
                let p: Vec<String> = const_info.iter()
                    .map(|(name, required)| {
                        if *required { name.to_string() } else { format!("[{name}]") }
                    })
                    .collect();
                format!("({})", p.join(", "))
            };

            let arity = if sig.outputs == 0 {
                format!("{}→N", sig.wire_input_count())
            } else {
                format!("{}→{}", sig.wire_input_count(), sig.outputs)
            };

            // Escape pipes in description
            let desc = sig.description.replace('|', "\\|");
            out.push_str(&format!(
                "| `{}` | {} | {} | {} | {} |\n",
                sig.name, params_desc, arity, jit, desc
            ));
        }
        out.push('\n');

        // Detailed entries with help text
        for sig in funcs {
            out.push_str(&format!("### `{}`\n\n", sig.name));

            // Build full signature
            let mut all_params: Vec<String> = Vec::new();
            for p in sig.params {
                match p.slot_type {
                    nb_variates::node::SlotType::Wire => {
                        all_params.push(format!("{}: wire", p.name));
                    }
                    nb_variates::node::SlotType::ConstStr => {
                        if p.required {
                            all_params.push(format!("{}: str", p.name));
                        } else {
                            all_params.push(format!("[{}]: str", p.name));
                        }
                    }
                    nb_variates::node::SlotType::ConstU64 => {
                        if p.required {
                            all_params.push(format!("{}: u64", p.name));
                        } else {
                            all_params.push(format!("[{}]: u64", p.name));
                        }
                    }
                    nb_variates::node::SlotType::ConstF64 => {
                        if p.required {
                            all_params.push(format!("{}: f64", p.name));
                        } else {
                            all_params.push(format!("[{}]: f64", p.name));
                        }
                    }
                    nb_variates::node::SlotType::ConstVecU64 => {
                        all_params.push(format!("{}: vec<u64>", p.name));
                    }
                    nb_variates::node::SlotType::ConstVecF64 => {
                        all_params.push(format!("{}: vec<f64>", p.name));
                    }
                }
            }
            let sig_str = format!("{}({}) → {}", sig.name, all_params.join(", "), sig.outputs);
            out.push_str(&format!("**Signature:** `{sig_str}`\n\n"));
            out.push_str(&format!("**Category:** {}  \n", sig.category.display_name()));

            let level = probe_compile_level(sig.name);
            let jit = match level {
                registry::CompileLevel::Phase3 => "P3 (Cranelift native)",
                registry::CompileLevel::Phase2 => "P2 (compiled u64 closure)",
                registry::CompileLevel::Phase1 => "P1 (Value interpreter)",
            };
            out.push_str(&format!("**JIT Level:** {jit}  \n"));

            if sig.is_variadic() {
                out.push_str("**Variadic:** yes  \n");
            }

            out.push_str(&format!("\n{}\n\n", sig.description));

            if !sig.help.is_empty() {
                out.push_str("```\n");
                out.push_str(sig.help);
                out.push_str("\n```\n\n");
            }

            out.push_str("---\n\n");
        }
    }

    let mut f = std::fs::File::create(path)
        .unwrap_or_else(|e| {
            eprintln!("error: failed to create {path}: {e}");
            std::process::exit(1);
        });
    f.write_all(out.as_bytes()).unwrap_or_else(|e| {
        eprintln!("error: failed to write {path}: {e}");
        std::process::exit(1);
    });
    eprintln!("nbrs: wrote {total} functions to {path}");
}

/// Display embedded stdlib modules with their typed signatures.
///
/// Parses each `.gk` source from the compiled-in standard library,
/// extracts `ModuleDef` statements, and prints them grouped by
/// category (source filename) with ANSI coloring.
fn describe_gk_stdlib() {
    use nb_variates::dsl::lexer::lex;
    use nb_variates::dsl::parser::parse;
    use nb_variates::dsl::ast::Statement;

    let sources = nb_variates::dsl::stdlib_sources();
    let is_tty = std::io::IsTerminal::is_terminal(&std::io::stdout());

    let (bold, dim, reset, green, cyan, magenta) = if is_tty {
        ("\x1b[1m", "\x1b[2m", "\x1b[0m", "\x1b[32m", "\x1b[36m", "\x1b[35m")
    } else {
        ("", "", "", "", "", "")
    };

    println!();
    println!("{bold}GK Standard Library{reset}");
    println!("{bold}═══════════════════{reset}");
    println!();

    for (filename, source) in sources {
        // Category name: filename without .gk extension, title-cased
        let category = filename
            .strip_suffix(".gk")
            .unwrap_or(filename);
        let category_title = category
            .chars()
            .enumerate()
            .map(|(i, c)| if i == 0 { c.to_ascii_uppercase() } else { c })
            .collect::<String>();

        let tokens = match lex(source) {
            Ok(t) => t,
            Err(e) => { eprintln!("warning: failed to lex stdlib file: {e}"); continue; }
        };
        let ast = match parse(tokens) {
            Ok(a) => a,
            Err(e) => { eprintln!("warning: failed to parse stdlib file: {e}"); continue; }
        };

        // Collect module defs from this file
        let mut modules = Vec::new();
        for stmt in &ast.statements {
            if let Statement::ModuleDef(mdef) = stmt {
                modules.push(mdef);
            }
        }

        if modules.is_empty() {
            continue;
        }

        println!("  {bold}{cyan}── {category_title} ──{reset}");
        println!();

        for mdef in &modules {
            // Build typed params string: (name: type, name: type, ...)
            let params_str = mdef.params.iter()
                .map(|p| format!("{}: {}", p.name, p.typ))
                .collect::<Vec<_>>()
                .join(", ");

            // Build typed outputs string: (name: type, ...)
            let outputs_str = mdef.outputs.iter()
                .map(|p| format!("{}: {}", p.name, p.typ))
                .collect::<Vec<_>>()
                .join(", ");

            let signature = format!("({params_str}) -> ({outputs_str})");

            // Extract the first comment line immediately before this module def
            // by scanning the source text for the comment block above the def
            let description = extract_first_comment(source, &mdef.name);

            // Name column: bold magenta, padded to 24 chars
            let name_padded = format!("{:<24}", mdef.name);
            print!("  {bold}{magenta}{name_padded}{reset}");

            // Signature in green
            println!(" {green}{signature}{reset}");

            // Description on the next line, indented and dim
            if let Some(desc) = description {
                println!("  {:<24} {dim}{desc}{reset}", "");
            }

            println!();
        }
    }
}

/// Display GK modules found in a directory.
///
/// Scans a directory for `.gk` files, parses each one, extracts
/// `ModuleDef` statements, and displays them with their typed
/// signatures — same format as `describe gk stdlib`.
///
/// Usage:
///   nbrs describe gk modules [--dir=path]
fn describe_gk_modules(args: &[String]) {
    use nb_variates::dsl::lexer::lex;
    use nb_variates::dsl::parser::parse;
    use nb_variates::dsl::ast::Statement;

    let dir = args.iter()
        .find_map(|a| a.strip_prefix("--dir="))
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from(".")));

    let is_tty = std::io::IsTerminal::is_terminal(&std::io::stdout());

    let (bold, dim, reset, green, cyan, magenta) = if is_tty {
        ("\x1b[1m", "\x1b[2m", "\x1b[0m", "\x1b[32m", "\x1b[36m", "\x1b[35m")
    } else {
        ("", "", "", "", "", "")
    };

    println!();
    println!("{bold}GK Modules in {}{reset}", dir.display());
    println!("{bold}{}{reset}", "═".repeat(15 + dir.display().to_string().len()));
    println!();

    let entries = match std::fs::read_dir(&dir) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("error: cannot read directory '{}': {e}", dir.display());
            return;
        }
    };

    let mut gk_files: Vec<std::path::PathBuf> = entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("gk"))
        .collect();
    gk_files.sort();

    if gk_files.is_empty() {
        println!("  {dim}(no .gk files found){reset}");
        println!();
        return;
    }

    for path in &gk_files {
        let source = match std::fs::read_to_string(path) {
            Ok(s) => s,
            Err(e) => { eprintln!("warning: failed to read {}: {e}", path.display()); continue; }
        };

        let filename = path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown");

        let category = filename
            .strip_suffix(".gk")
            .unwrap_or(filename);
        let category_title = category
            .chars()
            .enumerate()
            .map(|(i, c)| if i == 0 { c.to_ascii_uppercase() } else { c })
            .collect::<String>();

        let tokens = match lex(&source) {
            Ok(t) => t,
            Err(e) => { eprintln!("warning: failed to lex {filename}: {e}"); continue; }
        };
        let ast = match parse(tokens) {
            Ok(a) => a,
            Err(e) => { eprintln!("warning: failed to parse {filename}: {e}"); continue; }
        };

        let mut modules = Vec::new();
        for stmt in &ast.statements {
            if let Statement::ModuleDef(mdef) = stmt {
                modules.push(mdef);
            }
        }

        if modules.is_empty() {
            continue;
        }

        println!("  {bold}{cyan}-- {category_title} ({filename}) --{reset}");
        println!();

        for mdef in &modules {
            let params_str = mdef.params.iter()
                .map(|p| format!("{}: {}", p.name, p.typ))
                .collect::<Vec<_>>()
                .join(", ");

            let outputs_str = mdef.outputs.iter()
                .map(|p| format!("{}: {}", p.name, p.typ))
                .collect::<Vec<_>>()
                .join(", ");

            let signature = format!("({params_str}) -> ({outputs_str})");

            let description = extract_first_comment(&source, &mdef.name);

            let name_padded = format!("{:<24}", mdef.name);
            print!("  {bold}{magenta}{name_padded}{reset}");
            println!(" {green}{signature}{reset}");

            if let Some(desc) = description {
                println!("  {:<24} {dim}{desc}{reset}", "");
            }

            println!();
        }
    }
}

/// Extract the first comment line above a module definition.
///
/// Scans for `// <text>` lines in the comment block immediately
/// preceding the line that starts with `<name>(`. Only the nearest
/// contiguous comment block is considered — a blank line ends the
/// block. Returns the first non-empty line from that block.
fn extract_first_comment(source: &str, name: &str) -> Option<String> {
    let lines: Vec<&str> = source.lines().collect();
    // Find the line where the module def starts
    let def_prefix = format!("{name}(");
    let def_idx = lines.iter().position(|l| l.trim_start().starts_with(&def_prefix))?;

    // Walk backwards from the def line, collecting the nearest comment block.
    // Stop at the first blank line or non-comment line.
    let mut comment_lines = Vec::new();
    let mut i = def_idx;
    let mut seen_comment = false;
    while i > 0 {
        i -= 1;
        let trimmed = lines[i].trim();
        if trimmed.starts_with("//") {
            let text = trimmed.strip_prefix("//").unwrap().trim();
            comment_lines.push(text);
            seen_comment = true;
        } else if trimmed.is_empty() {
            if seen_comment {
                // Blank line after we already found comments — end of block
                break;
            }
            // Blank line directly above def (before any comment) — skip
            continue;
        } else {
            break;
        }
    }

    // comment_lines is in reverse order; flip to get first-to-last
    comment_lines.reverse();
    // Return the first non-empty line
    for line in &comment_lines {
        if !line.is_empty() {
            return Some(line.to_string());
        }
    }
    None
}

/// Render a GK source file as DOT, Mermaid, or SVG.
///
/// Usage:
///   nbrs describe gk dag <file.gk> [--format=dot|mermaid|svg] [--output=file]
fn describe_gk_dag(args: &[String]) {
    use nb_variates::viz;

    let file = args.iter().find(|a| !a.starts_with("--"));
    let format = args.iter()
        .find_map(|a| a.strip_prefix("--format="))
        .unwrap_or("dot");
    let output = args.iter()
        .find_map(|a| a.strip_prefix("--output="));

    let source = match file {
        Some(path) => match std::fs::read_to_string(path) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("error: failed to read '{}': {e}", path);
                return;
            }
        },
        None => {
            eprintln!("nbrs describe gk dag <file.gk> [--format=dot|mermaid|svg] [--output=file]");
            eprintln!();
            eprintln!("Renders a GK source file as a DAG diagram.");
            eprintln!("  --format=dot       DOT digraph (default)");
            eprintln!("  --format=mermaid   Mermaid flowchart");
            eprintln!("  --format=svg       Self-contained SVG (pure Rust, no external tools)");
            eprintln!("  --output=file      Write to file instead of stdout");
            return;
        }
    };

    let result = match format {
        "dot" => viz::gk_to_dot(&source),
        "mermaid" => viz::gk_to_mermaid(&source),
        "svg" => viz::gk_to_svg(&source),
        other => {
            eprintln!("error: unknown format '{other}' (use dot, mermaid, or svg)");
            return;
        }
    };

    match result {
        Ok(content) => {
            if let Some(path) = output {
                match std::fs::write(path, &content) {
                    Ok(()) => eprintln!("wrote {} bytes to {path}", content.len()),
                    Err(e) => eprintln!("error: failed to write '{path}': {e}"),
                }
            } else {
                println!("{content}");
            }
        }
        Err(e) => eprintln!("error: {e}"),
    }
}
