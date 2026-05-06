// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! The `describe` subcommand: introspect GK functions, stdlib, modules, and DAGs.

use nbrs_variates::dsl::registry;

pub fn describe_command(args: &[String]) {
    let first = args.first().map(|s| s.as_str()).unwrap_or("");

    // `nbrs describe adapter=<name>` / `nbrs describe adapter`
    // shorthand. The `key=value` form mirrors the rest of nbrs's
    // CLI, so it composes with the user's muscle memory.
    if let Some((topic, value)) = first.split_once('=')
        && topic == "adapter"
    {
        describe_adapter(value);
        return;
    }

    let topic = first;
    let subtopic = args.get(1).map(|s| s.as_str()).unwrap_or("");

    match (topic, subtopic) {
        ("adapter", "") => describe_adapters_list(),
        ("adapter", name) => describe_adapter(name),
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
            eprintln!("  adapter[=<name>]   List adapters / show one adapter's params + drivers");
            eprintln!("  gk                 Generation kernel topics");
            eprintln!();
            eprintln!("For workload analysis, use: nbrs run workload=file.yaml dryrun=phase,gk");
        }
    }
}

fn describe_adapters_list() {
    use nbrs_activity::adapter::{registered_driver_names, default_drivers};

    let mut names = registered_driver_names();
    names.sort();
    names.dedup();
    if names.is_empty() {
        println!("No adapters registered in this binary.");
        return;
    }
    println!("Registered adapters:");
    for name in names {
        let drivers = default_drivers(name);
        if drivers.is_empty() {
            println!("  {name}");
        } else {
            // Multi-driver adapter — show the rank-derived default
            // and the alternative drivers compiled in.
            let default = drivers.first().copied().unwrap_or("");
            let alts: Vec<&str> = drivers.iter().skip(1).copied().collect();
            if alts.is_empty() {
                println!("  {name}    (driver: {default})");
            } else {
                println!("  {name}    (drivers: {default} [default], {})", alts.join(", "));
            }
        }
    }
    println!();
    println!("For details: nbrs describe adapter=<name>");
}

fn describe_adapter(name: &str) {
    use nbrs_activity::adapter::{
        find_adapter_registration, default_drivers, find_driver,
    };

    let Some(reg) = find_adapter_registration(name) else {
        eprintln!("No adapter named '{name}' is registered in this binary.");
        eprintln!();
        describe_adapters_list();
        return;
    };

    let aliases = (reg.names)();
    println!("Adapter: {name}");
    if aliases.len() > 1 {
        println!("  Aliases:        {}", aliases.join(", "));
    }
    println!("  Display:        {:?}", (reg.display_preference)());

    let adapter_params = (reg.known_params)();
    if !adapter_params.is_empty() {
        println!("  Adapter params: {}", adapter_params.join(", "));
    }

    let drivers = default_drivers(name);
    if drivers.is_empty() {
        println!();
        return;
    }

    let default = drivers.first().copied().unwrap_or("");
    println!();
    println!("  Drivers (compiled into this binary, rank order — first is default):");
    for driver in &drivers {
        let marker = if *driver == default { " [default]" } else { "" };
        match find_driver(name, driver) {
            Some(impl_) => {
                let dparams = (impl_.known_params)();
                println!("    {driver}{marker}  rank={}", impl_.default_rank);
                if !dparams.is_empty() {
                    println!("      params: {}", dparams.join(", "));
                }
            }
            None => println!("    {driver}{marker}"),
        }
    }
    if drivers.len() > 1 {
        println!();
        // Selector convention: drivers are picked via
        // `<adapter>driver=…` (e.g. `cqldriver=scylla`). Surface
        // the exact knob + accepted values so the user doesn't
        // have to read the source.
        println!("  Select a driver with: {name}driver=<{}>",
            drivers.join("|"));
    }
}

fn describe_gk_functions() {
    use nbrs_activity::bindings::probe_compile_level;

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
    use nbrs_activity::bindings::probe_compile_level;
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
                    nbrs_variates::node::SlotType::Wire => {
                        all_params.push(format!("{}: wire", p.name));
                    }
                    nbrs_variates::node::SlotType::ConstStr => {
                        if p.required {
                            all_params.push(format!("{}: str", p.name));
                        } else {
                            all_params.push(format!("[{}]: str", p.name));
                        }
                    }
                    nbrs_variates::node::SlotType::ConstU64 => {
                        if p.required {
                            all_params.push(format!("{}: u64", p.name));
                        } else {
                            all_params.push(format!("[{}]: u64", p.name));
                        }
                    }
                    nbrs_variates::node::SlotType::ConstF64 => {
                        if p.required {
                            all_params.push(format!("{}: f64", p.name));
                        } else {
                            all_params.push(format!("[{}]: f64", p.name));
                        }
                    }
                    nbrs_variates::node::SlotType::ConstVecU64 => {
                        all_params.push(format!("{}: vec<u64>", p.name));
                    }
                    nbrs_variates::node::SlotType::ConstVecF64 => {
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
    use nbrs_variates::dsl::lexer::lex;
    use nbrs_variates::dsl::parser::parse;
    use nbrs_variates::dsl::ast::Statement;

    let sources = nbrs_variates::dsl::stdlib_sources();
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
    use nbrs_variates::dsl::lexer::lex;
    use nbrs_variates::dsl::parser::parse;
    use nbrs_variates::dsl::ast::Statement;

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
///   nbrs describe gk dag --with-flattening <workload.yaml>
fn describe_gk_dag(args: &[String]) {
    use nbrs_variates::viz;

    let file = args.iter().find(|a| !a.starts_with("--"));
    let format = args.iter()
        .find_map(|a| a.strip_prefix("--format="))
        .unwrap_or("dot");
    let output = args.iter()
        .find_map(|a| a.strip_prefix("--output="));
    let with_flattening = args.iter().any(|a| a == "--with-flattening");

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
            eprintln!("nbrs describe gk dag --with-flattening <workload.yaml>");
            eprintln!();
            eprintln!("Renders a GK source file as a DAG diagram.");
            eprintln!("  --format=dot         DOT digraph (default)");
            eprintln!("  --format=mermaid     Mermaid flowchart");
            eprintln!("  --format=svg         Self-contained SVG (pure Rust, no external tools)");
            eprintln!("  --output=file        Write to file instead of stdout");
            eprintln!("  --with-flattening    Treat <file> as a workload YAML; print");
            eprintln!("                       the SRD-13d scope-flattening summary");
            eprintln!("                       (materialised bit, logical_name, bind_outer)");
            return;
        }
    };

    // SRD-13d Phase 8: --with-flattening switches the surface from
    // "render a GK source string" to "parse a workload YAML, build
    // its scope tree, run mark_scope_flattening with the
    // 'materialise everything' stub predicate, and print the
    // per-node summary." When SRD-13d Phase 3 lands and supplies
    // the real predicate, swap it in here — the rest of the pipe
    // stays.
    if with_flattening {
        let path = file.map(|s| s.as_str()).unwrap_or("<missing>");
        let summary = render_flattening_summary(&source, path);
        match summary {
            Ok(content) => {
                if let Some(p) = output {
                    match std::fs::write(p, &content) {
                        Ok(()) => eprintln!("wrote {} bytes to {p}", content.len()),
                        Err(e) => eprintln!("error: failed to write '{p}': {e}"),
                    }
                } else {
                    print!("{content}");
                }
            }
            Err(e) => eprintln!("error: {e}"),
        }
        return;
    }

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

/// SRD-13d Phase 8 entry point: parse `yaml_source` as a
/// workload, build its [`ScopeTree`], run
/// [`ScopeTree::mark_scope_flattening`] with a stub
/// "materialise everything" predicate, and produce a textual
/// summary listing each node's logical_name, materialised
/// bit, and the nearest_materialised ancestor it would bind
/// to (the SRD-13d "walking parent" reference).
///
/// Today's predicate is a stub — SRD-13d Phase 3 will install
/// the real one (consulting `HasGkMatter` + program-hash
/// equivalence). Wiring everything else end-to-end now means
/// that swap is a one-liner when the predicate lands.
///
/// `path` is the file path the user supplied; it's surfaced
/// in the header line so the caller can confirm which file
/// was read.
fn render_flattening_summary(yaml_source: &str, path: &str) -> Result<String, String> {
    use nbrs_activity::scope_tree::{ScopeTree, ScopeKind};
    use nbrs_workload::parse::parse_workload;
    use std::collections::HashMap;

    let workload = parse_workload(yaml_source, &HashMap::new())
        .map_err(|e| format!("parse_workload('{path}'): {e}"))?;

    // Pick the scenario the same way the runner does: take the
    // user-named scenario or, if absent, synthesise a default
    // from `phase_order`. We don't accept a `--scenario=` knob
    // here today — the diagnostic surface is structural, not
    // configurable.
    let scenario_name = "default";
    let scenario_nodes: Vec<_> = if let Some(nodes) = workload.scenarios.get(scenario_name) {
        nodes.clone()
    } else if !workload.phase_order.is_empty() {
        workload.phase_order.iter()
            .map(|n| nbrs_workload::model::ScenarioNode::Phase(n.clone()))
            .collect()
    } else {
        return Err(format!(
            "workload '{path}' has neither a 'default' scenario nor any phases"
        ));
    };

    let mut tree = ScopeTree::build(scenario_name, &scenario_nodes);
    // SRD-13d Phase 3 stub: every node materialises. Swap in
    // the real predicate (HasGkMatter classification +
    // program-hash equivalence) when Phase 3 lands.
    tree.mark_scope_flattening(|_kind, _idx| true);

    let mut out = String::new();
    out.push_str(&format!("# scope flattening summary: {path}\n"));
    out.push_str(&format!("# scenario: {scenario_name}\n"));
    out.push_str("# predicate: stub (materialise everything) — SRD-13d Phase 3 pending\n");
    out.push('\n');
    out.push_str(&format!(
        "{:<5} {:<6} {:<14} {:<50} {:<50} {}\n",
        "idx", "depth", "materialised", "logical_name", "kind", "bind_outer",
    ));
    out.push_str(&format!(
        "{:<5} {:<6} {:<14} {:<50} {:<50} {}\n",
        "---", "-----", "------------", "------------", "----", "----------",
    ));
    for (idx, node) in tree.iter_dfs() {
        let mat = match node.materialised {
            Some(true)  => "true",
            Some(false) => "false",
            None        => "?",
        };
        // bind_outer = nearest materialised ancestor, walking
        // *strict* parents when this node is itself flattened.
        // For materialised nodes we surface the same identity
        // (own logical_name) so the summary is self-contained:
        // a reader knows where every node binds at a glance.
        let bind_outer = match node.materialised {
            Some(false) => match node.parent
                .and_then(|p| tree.nearest_materialised(p))
            {
                Some(anc) => tree.nodes[anc].logical_name.clone(),
                None => "<none>".to_string(),
            },
            _ => node.logical_name.clone(),
        };
        let kind_label = match &node.kind {
            ScopeKind::Workload => "workload".to_string(),
            other => other.label(),
        };
        out.push_str(&format!(
            "{:<5} {:<6} {:<14} {:<50} {:<50} {}\n",
            idx, node.depth, mat, node.logical_name, kind_label, bind_outer,
        ));
    }
    Ok(out)
}

// ── cli_spec entry ─────────────────────────────────────────

/// `nbrs describe <topic> …` — sub-topic dispatch is
/// parser-internal (each topic has its own `parse_*_args`).
/// raw_args=true: the spec advertises the command for
/// completion+help; per-topic flag declarations remain inside
/// `describe_command`.
///
/// **Open gap:** topics like `describe gk`, `describe adapter`
/// could be modelled as nested `Command`s with their own
/// flags. Future work would walk each topic's parser and
/// lift its flag set into a Command subtree.
pub fn spec() -> crate::cli_spec::Command {
    use crate::cli_spec::{Category, Command, Handler, Level, ParsedCommand};
    fn handle(p: ParsedCommand) -> Result<(), String> {
        describe_command(&p.raw);
        Ok(())
    }
    Command {
        name: "describe",
        help: "Documentation surface (`describe gk`, `describe adapter`, …).",
        category: Category::Documentation,
        level: Level::FullSurface,
        flags: Vec::new(),
        positionals: Vec::new(),
        subcommands: Vec::new(),
        handler: Some(Handler::Sync(handle)),
        raw_args: true,
        completion_override: None,
    }
}

#[cfg(test)]
mod describe_gk_dag_flattening_tests {
    //! SRD-13d Phase 8 — the `--with-flattening` surface on
    //! `nbrs describe gk dag`. Drives the same code path the CLI
    //! does (parse YAML → build ScopeTree → mark → render
    //! summary) and asserts the produced text contains the
    //! per-node fields the SRD calls out: logical_name,
    //! materialised, and the bind_outer reference.
    use super::render_flattening_summary;

    /// Minimal flat workload — one phase under the implicit
    /// scenario. Exercises the simplest path: workload, scenario,
    /// phase. With the all-materialise stub every node should be
    /// `materialised=true` and `bind_outer` equal to its own
    /// `logical_name`.
    #[test]
    fn flat_phase_workload_summary_contains_logical_names() {
        let yaml = r#"
phases:
  setup:
    ops:
      - op: noop
"#;
        let out = render_flattening_summary(yaml, "test.yaml")
            .expect("flat workload should parse and render");
        // Header sanity.
        assert!(out.contains("# scope flattening summary: test.yaml"));
        assert!(out.contains("# scenario: default"));
        // Logical names per SRD-13d §5.3.
        assert!(out.contains("workload"),
            "root node logical_name 'workload' missing:\n{out}");
        assert!(out.contains("workload.scenario.default"),
            "scenario logical_name missing:\n{out}");
        assert!(out.contains("workload.scenario.default.phase.setup"),
            "phase logical_name missing:\n{out}");
        // Stub predicate ⇒ everyone's materialised.
        assert!(out.contains("true"),
            "expected at least one materialised=true row:\n{out}");
        // No 'false' rows under the all-materialise stub. (We
        // can't assert "no false" by literal substring because
        // 'false' could appear inside a logical_name; the regex
        // would be brittle. Spot-check the column instead by
        // counting "    false    " patterns.)
        assert!(!out.contains(" false "),
            "stub predicate should not flag any node as flattened:\n{out}");
    }

    /// Multi-phase workload — verify each phase appears with its
    /// own logical_name path, and the bind_outer column points
    /// at the materialised ancestor (here itself, since stub
    /// materialises everything).
    #[test]
    fn multi_phase_workload_lists_every_phase() {
        let yaml = r#"
phases:
  setup:
    ops:
      - op: noop
  run:
    ops:
      - op: noop
"#;
        let out = render_flattening_summary(yaml, "two.yaml")
            .expect("two-phase workload should render");
        assert!(out.contains("phase.setup"), "setup phase row missing:\n{out}");
        assert!(out.contains("phase.run"), "run phase row missing:\n{out}");
    }

    /// Bad workload YAML surfaces an Err with the file path
    /// embedded, so the diagnostic tells the user *which* file
    /// failed (matters when running the binary against a
    /// directory of workloads or via shell expansion).
    #[test]
    fn malformed_workload_returns_path_tagged_error() {
        let bad = "not: valid: yaml: workload";
        let err = render_flattening_summary(bad, "bad.yaml").unwrap_err();
        assert!(err.contains("bad.yaml"),
            "error should embed the offending path: {err}");
    }

    /// `bind_outer` for a materialised node points at its own
    /// logical_name (so the summary is self-describing). When
    /// SRD-13d Phase 3 ships and a non-trivial predicate flags
    /// some node as flattened, the same row will instead point
    /// at the nearest materialised ancestor — but the column
    /// shape stays.
    #[test]
    fn bind_outer_column_is_self_when_node_is_materialised() {
        let yaml = r#"
phases:
  p:
    ops:
      - op: noop
"#;
        let out = render_flattening_summary(yaml, "x.yaml").unwrap();
        // The phase row should mention its own name twice — once
        // in the logical_name column, once in bind_outer.
        let phase_lines: Vec<&str> = out.lines()
            .filter(|l| l.contains("phase.p"))
            .collect();
        assert_eq!(phase_lines.len(), 1,
            "expected exactly one phase row, got: {phase_lines:?}");
        let line = phase_lines[0];
        let occurrences = line.matches("workload.scenario.default.phase.p").count();
        assert_eq!(occurrences, 2,
            "materialised phase row should list its logical_name twice (logical_name + bind_outer): {line}");
    }
}
