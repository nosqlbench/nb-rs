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
        // SRD-32a Push 4 — discoverability commands.
        // `describe wrappers` dumps the wrapper registry; the
        // resolver isn't consulted here because the table is a
        // pure registry view.
        ("wrappers", _) => {
            print!("{}", render_wrappers_table());
        }
        // `describe op <workload> <op>` loads a workload, finds
        // the named op-template, and renders its resolved
        // wrapper stack with provenance.
        ("op", workload_path) if !workload_path.is_empty() => {
            let op_name = args.get(2).map(|s| s.as_str()).unwrap_or("");
            if op_name.is_empty() {
                eprintln!("nbrs describe op <workload> <op>");
                eprintln!("  loads <workload>, finds the op template named <op>,");
                eprintln!("  and prints its resolved wrapper stack.");
                return;
            }
            match render_op_description(workload_path, op_name) {
                Ok(text) => print!("{text}"),
                Err(e) => eprintln!("error: {e}"),
            }
        }
        ("op", _) => {
            eprintln!("nbrs describe op <workload> <op>");
            eprintln!("  loads <workload>, finds the op template named <op>,");
            eprintln!("  and prints its resolved wrapper stack.");
        }
        _ => {
            eprintln!("nbrs describe <topic>");
            eprintln!("  adapter[=<name>]   List adapters / show one adapter's params + drivers");
            eprintln!("  gk                 Generation kernel topics");
            eprintln!("  wrappers           List the registered op-template wrappers");
            eprintln!("  op <wkl> <op>      Show the resolved wrapper stack for one op");
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

// ── SRD-32a Push 4 — wrapper discoverability ─────────────────

/// Render the `nbrs describe wrappers` table.
///
/// Pulls every registration from the live wrapper inventory and
/// prints one row per wrapper with the columns NAME, OWNED FIELDS,
/// TRIGGER, CONSTRAINTS. The "RANK" column from the SRD draft was
/// dropped on purpose — wrapper composition is constraint-driven
/// now, so rank would be misleading. The trigger column shows a
/// human label ("always", "delay set", "verify/relevancy", …) the
/// caller can grep for; the constraints column is empty for most
/// wrappers and lists `requires_inner=`, `forbids_outer=`, and
/// `mutually_exclusive_with=` only when the wrapper declares them.
///
/// Returned as a String (rather than printed directly) so the
/// test suite can pin the exact output. Iteration order is the
/// alphabetical order the registry hands us — stable across runs.
pub fn render_wrappers_table() -> String {
    use nbrs_activity::wrapper_registry::WrapperRegistry;
    use std::fmt::Write;

    let registry = WrapperRegistry::from_inventory();

    // Build rows first so we can compute column widths once.
    struct Row {
        name: String,
        owned: String,
        trigger: String,
        constraints: String,
    }
    let mut rows: Vec<Row> = Vec::with_capacity(registry.len());
    for reg in registry.iter() {
        let owned = if reg.owned_fields.is_empty() {
            "(none)".to_string()
        } else {
            reg.owned_fields.join(", ")
        };
        let trigger = trigger_label(reg.name.as_str(), reg.owned_fields);
        let mut constraint_parts: Vec<String> = Vec::new();
        if !reg.requires_inner.is_empty() {
            let names: Vec<&str> = reg.requires_inner.iter().map(|n| n.as_str()).collect();
            constraint_parts.push(format!("requires_inner=[{}]", names.join(", ")));
        }
        if !reg.forbids_outer.is_empty() {
            let names: Vec<&str> = reg.forbids_outer.iter().map(|n| n.as_str()).collect();
            constraint_parts.push(format!("forbids_outer=[{}]", names.join(", ")));
        }
        if !reg.mutually_exclusive_with.is_empty() {
            let names: Vec<&str> =
                reg.mutually_exclusive_with.iter().map(|n| n.as_str()).collect();
            constraint_parts.push(format!(
                "mutually_exclusive_with=[{}]",
                names.join(", "),
            ));
        }
        rows.push(Row {
            name: reg.name.as_str().to_string(),
            owned,
            trigger,
            constraints: constraint_parts.join("; "),
        });
    }

    let name_w = "NAME"
        .len()
        .max(rows.iter().map(|r| r.name.len()).max().unwrap_or(0));
    let owned_w = "OWNED FIELDS"
        .len()
        .max(rows.iter().map(|r| r.owned.len()).max().unwrap_or(0));
    let trigger_w = "TRIGGER"
        .len()
        .max(rows.iter().map(|r| r.trigger.len()).max().unwrap_or(0));

    let mut out = String::new();
    let _ = writeln!(
        out,
        "{:<name_w$}  {:<owned_w$}  {:<trigger_w$}  {}",
        "NAME",
        "OWNED FIELDS",
        "TRIGGER",
        "CONSTRAINTS",
        name_w = name_w,
        owned_w = owned_w,
        trigger_w = trigger_w,
    );
    for r in &rows {
        let _ = writeln!(
            out,
            "{:<name_w$}  {:<owned_w$}  {:<trigger_w$}  {}",
            r.name,
            r.owned,
            r.trigger,
            r.constraints,
            name_w = name_w,
            owned_w = owned_w,
            trigger_w = trigger_w,
        );
    }
    out
}

/// Human-readable label for a wrapper's trigger predicate.
///
/// The registration carries a `fn(&ParsedOp) -> bool`, which
/// tells us *whether* a wrapper applies but not *what shape of
/// op* drives it. The label is hand-curated per wrapper so the
/// table reads like the SRD's prose. Falls back to
/// "owned field set" for any future wrapper not enumerated here.
fn trigger_label(name: &str, owned_fields: &[&str]) -> String {
    match name {
        "traverse" => "always".to_string(),
        "throttle" => "delay set".to_string(),
        "validate" => "verify/relevancy set".to_string(),
        "poll" => "poll: set".to_string(),
        "if" => "if: set".to_string(),
        "emit" => "emit: true".to_string(),
        "result" => "always (no-op when result map empty)".to_string(),
        "metrics" => "non-empty metrics map".to_string(),
        _ if owned_fields.is_empty() => "always".to_string(),
        _ => format!("any of: {}", owned_fields.join(", ")),
    }
}

/// Render the `nbrs describe op <workload> <op>` text.
///
/// Loads `workload_path` via `nbrs_workload::parse::parse_workload`
/// (the same idiom as `report_cmd::resolve_items`), then walks
/// every phase's `ops` and the top-level `ops` list looking for
/// an op-template whose `name` matches `op_name`. The first match
/// wins; the phase column tells the caller where it came from
/// (`(phase: foo)` or `(top-level ops:)` when found at the
/// workload root).
///
/// Returns the formatted text on success, or a single-line error
/// string suitable for `eprintln!`. Resolver errors (constraint
/// violations) are surfaced via `Display`, not `Debug` — the user
/// shouldn't see Rust's struct-debug for a config diagnostic.
pub fn render_op_description(workload_path: &str, op_name: &str) -> Result<String, String> {
    use nbrs_activity::wrapper_registry::WrapperRegistry;
    use nbrs_activity::wrapper_resolver::{WrapperActivation, WrapperResolver};
    use nbrs_workload::model::ParsedOp;
    use std::collections::HashMap;
    use std::fmt::Write;

    let resolved = crate::cli::resolve_workload_path(workload_path)
        .unwrap_or_else(|| workload_path.to_string());
    let path = std::path::PathBuf::from(&resolved);
    if !path.exists() {
        return Err(format!("workload '{resolved}' not found"));
    }
    let text = std::fs::read_to_string(&path)
        .map_err(|e| format!("read '{}': {e}", path.display()))?;
    let workload = nbrs_workload::parse::parse_workload(&text, &HashMap::new())
        .map_err(|e| format!("parse workload '{}': {e}", path.display()))?;

    // Collect every (phase_label, &ParsedOp) pair so we can
    // both find the requested op and list candidates in the
    // not-found path. Walk PHASES first — `parse_workload`
    // flattens phase ops into the top-level `workload.ops` list
    // as well, and the phase context is the more useful label
    // for the user (matches the SRD's example output).
    let mut all_ops: Vec<(Option<String>, &ParsedOp)> = Vec::new();
    for phase_name in &workload.phase_order {
        if let Some(phase) = workload.phases.get(phase_name) {
            for op in &phase.ops {
                all_ops.push((Some(phase_name.clone()), op));
            }
        }
    }
    // Pick up any phase that wasn't in phase_order (defensive —
    // parse_workload always populates phase_order, but we don't
    // want to silently drop ops if it ever doesn't).
    for (phase_name, phase) in &workload.phases {
        if !workload.phase_order.contains(phase_name) {
            for op in &phase.ops {
                all_ops.push((Some(phase_name.clone()), op));
            }
        }
    }
    // Top-level ops come last. Skip any that share a name with
    // a phase op already collected — those are the same template
    // and would only confuse the candidate list.
    let phase_op_names: std::collections::HashSet<&str> =
        all_ops.iter().map(|(_, op)| op.name.as_str()).collect();
    for op in &workload.ops {
        if !phase_op_names.contains(op.name.as_str()) {
            all_ops.push((None, op));
        }
    }

    let found = all_ops.iter().find(|(_, op)| op.name == op_name);
    let (phase_label, template) = match found {
        Some(hit) => hit,
        None => {
            // List candidate names so the user sees what's available.
            let mut candidates: Vec<String> = all_ops
                .iter()
                .map(|(p, op)| match p {
                    Some(ph) => format!("  {} (phase: {ph})", op.name),
                    None => format!("  {} (top-level)", op.name),
                })
                .collect();
            candidates.sort();
            candidates.dedup();
            let mut msg = format!(
                "no op template named '{op_name}' in workload '{}'",
                path.display(),
            );
            if !candidates.is_empty() {
                msg.push_str("\navailable op templates:\n");
                msg.push_str(&candidates.join("\n"));
            }
            return Err(msg);
        }
    };

    let registry = WrapperRegistry::from_inventory();
    let resolver = WrapperResolver::with_default_order(&registry).map_err(|e| e.to_string())?;
    let plan = resolver
        .resolve(template, &registry)
        .map_err(|e| e.to_string())?;

    let mut out = String::new();
    match phase_label {
        Some(ph) => {
            let _ = writeln!(out, "op '{op_name}' (phase: {ph})");
        }
        None => {
            let _ = writeln!(out, "op '{op_name}' (top-level ops:)");
        }
    }
    out.push_str("  wrapper stack (innermost -> outermost):\n");
    for (i, reg) in plan.iter_innermost_first().enumerate() {
        let line = (reg.describe_assignment)(template)
            .unwrap_or_else(|| reg.name.as_str().to_string());
        // Prepend the wrapper name to assignments that don't
        // already start with it — `describe_assignment` lines
        // typically lead with `<name>: …` already, but the
        // `traverse` and similar None-returners don't.
        let display = if line.starts_with(reg.name.as_str()) {
            line
        } else {
            format!("{}: {line}", reg.name.as_str())
        };
        let provenance = match plan.activation(reg.name) {
            Some(WrapperActivation::OwnedField { field, .. }) => {
                format!(" (triggered by `{field}:` field)")
            }
            Some(WrapperActivation::TransitiveFrom { requested_by, .. }) => {
                format!(" (transitive via {requested_by})")
            }
            Some(WrapperActivation::AlwaysOn { .. }) | None => String::new(),
        };
        let _ = writeln!(out, "    {n}. {display}{provenance}", n = i + 1);
    }
    Ok(out)
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

#[cfg(test)]
mod describe_wrappers_tests {
    //! SRD-32a Push 4 — discoverability commands. Tests pin the
    //! shape of `nbrs describe wrappers` and `nbrs describe op`
    //! so the human-readable surface doesn't drift silently.
    use super::{render_op_description, render_wrappers_table};

    /// The wrapper table must include every built-in wrapper
    /// from the registry. The registry is alphabetical, so the
    /// rows arrive in alphabetical order by wrapper name.
    #[test]
    fn wrappers_table_lists_every_built_in() {
        let out = render_wrappers_table();
        // Header row.
        assert!(out.contains("NAME"), "header missing NAME column:\n{out}");
        assert!(out.contains("OWNED FIELDS"), "header missing OWNED FIELDS:\n{out}");
        assert!(out.contains("TRIGGER"), "header missing TRIGGER:\n{out}");
        assert!(out.contains("CONSTRAINTS"), "header missing CONSTRAINTS:\n{out}");
        // Each registered wrapper appears.
        for name in [
            "traverse", "throttle", "validate", "poll",
            "if", "emit", "result", "metrics",
        ] {
            assert!(out.contains(name),
                "wrapper `{name}` missing from describe wrappers output:\n{out}");
        }
    }

    /// Trigger labels for the built-in wrappers — matches the
    /// SRD §"Discoverability" prose. Pinning these strings keeps
    /// the documentation surface stable.
    #[test]
    fn wrappers_table_uses_human_trigger_labels() {
        let out = render_wrappers_table();
        // The traverse row must say "always".
        let traverse_line = out
            .lines()
            .find(|l| l.starts_with("traverse"))
            .expect("traverse row missing");
        assert!(traverse_line.contains("always"),
            "traverse trigger should be `always`: {traverse_line}");
        let validate_line = out
            .lines()
            .find(|l| l.starts_with("validate"))
            .expect("validate row missing");
        assert!(validate_line.contains("verify/relevancy"),
            "validate trigger should mention verify/relevancy: {validate_line}");
        let metrics_line = out
            .lines()
            .find(|l| l.starts_with("metrics"))
            .expect("metrics row missing");
        assert!(metrics_line.contains("non-empty metrics map"),
            "metrics trigger should be `non-empty metrics map`: {metrics_line}");
        // metrics declares forbids_outer for every other wrapper —
        // surface that in the constraints column.
        assert!(metrics_line.contains("forbids_outer="),
            "metrics row should advertise its forbids_outer constraint: {metrics_line}");
    }

    /// Owned-fields column lists the registry's owned-field names
    /// (validate, poll, etc.). Wrappers with no owned fields must
    /// render `(none)` rather than an empty cell.
    #[test]
    fn wrappers_table_owned_fields_column_uses_none_for_empty() {
        let out = render_wrappers_table();
        let traverse_line = out
            .lines()
            .find(|l| l.starts_with("traverse"))
            .expect("traverse row missing");
        assert!(traverse_line.contains("(none)"),
            "traverse owned fields should render as `(none)`: {traverse_line}");
        let validate_line = out
            .lines()
            .find(|l| l.starts_with("validate"))
            .expect("validate row missing");
        for f in ["verify", "relevancy", "strict"] {
            assert!(validate_line.contains(f),
                "validate row missing owned field `{f}`: {validate_line}");
        }
    }

    /// `describe op` against a workload that defines a phase op
    /// renders the resolved stack innermost-to-outermost, names
    /// the phase, and labels each line. The empty `noop` op only
    /// triggers `traverse` + `result`, so the stack is two lines.
    #[test]
    fn describe_op_simple_phase_shows_default_stack() {
        let yaml = r#"
phases:
  setup:
    ops:
      noop:
        stmt: "noop"
"#;
        let dir = std::env::temp_dir().join("nbrs_describe_op_simple");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("simple.yaml");
        std::fs::write(&path, yaml).expect("write workload");

        let out = render_op_description(path.to_str().unwrap(), "noop")
            .expect("simple workload should resolve");
        assert!(out.contains("op 'noop'"), "header missing op name: {out}");
        assert!(out.contains("phase: setup"), "header missing phase: {out}");
        assert!(out.contains("wrapper stack (innermost -> outermost)"),
            "stack header missing: {out}");
        // Empty op fires only the always-on wrappers.
        let traverse_idx = out.find("traverse").expect("traverse missing");
        let result_idx = out.find("result").expect("result missing");
        assert!(traverse_idx < result_idx,
            "traverse should print before result: {out}");
        // None of the optional wrappers should appear in the
        // stack. We check the numbered stack lines so a phase or
        // op whose name contains "metrics" or "validate" can't
        // false-positive a substring search.
        let stack_lines: Vec<&str> = out
            .lines()
            .filter(|l| l.trim_start().starts_with(|c: char| c.is_ascii_digit()))
            .collect();
        for unexpected in ["throttle", "validate", "poll", "emit", "metrics"] {
            for line in &stack_lines {
                assert!(!line.contains(unexpected),
                    "unexpected wrapper `{unexpected}` in stack line: {line}");
            }
        }
    }

    /// An op declaring `verify:` triggers validate; the resolver
    /// pulls in traverse transitively. Provenance text must
    /// distinguish the two activations.
    #[test]
    fn describe_op_validate_shows_owned_field_and_transitive() {
        let yaml = r#"
phases:
  go:
    ops:
      check:
        stmt: "SELECT 1"
        verify: "min_rows >= 1"
"#;
        let dir = std::env::temp_dir().join("nbrs_describe_op_validate");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("validate.yaml");
        std::fs::write(&path, yaml).expect("write workload");

        let out = render_op_description(path.to_str().unwrap(), "check")
            .expect("validate workload should resolve");
        // Validate fires on `verify:` — the line should say so.
        assert!(out.contains("triggered by `verify:` field"),
            "validate provenance missing: {out}");
        // Traverse is transitive (always-on wrapper, but it would
        // also be pulled in transitively by validate). The
        // resolver tags it AlwaysOn because the trigger fires
        // first. Either way, the line should not falsely claim
        // a `verify:` trigger.
        let traverse_line = out
            .lines()
            .find(|l| l.contains("1.") && l.contains("traverse"))
            .expect("traverse line missing");
        assert!(!traverse_line.contains("triggered by"),
            "traverse line should not claim a field trigger: {traverse_line}");
    }

    /// Unknown op-template names surface a clean error including
    /// the candidate list. The error path must NOT panic and
    /// must not propagate a Debug-formatted ResolveError.
    #[test]
    fn describe_op_unknown_lists_candidates() {
        let yaml = r#"
phases:
  go:
    ops:
      alpha:
        stmt: "noop"
      beta:
        stmt: "noop"
"#;
        let dir = std::env::temp_dir().join("nbrs_describe_op_unknown");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("two.yaml");
        std::fs::write(&path, yaml).expect("write workload");

        let err = render_op_description(path.to_str().unwrap(), "missing").unwrap_err();
        assert!(err.contains("no op template named 'missing'"),
            "error should name the missing template: {err}");
        assert!(err.contains("alpha"), "candidate list should include alpha: {err}");
        assert!(err.contains("beta"), "candidate list should include beta: {err}");
    }

    /// Missing-file path returns a clean error string, not a
    /// panic and not a Debug-format. The path must be embedded so
    /// the operator sees what was attempted.
    #[test]
    fn describe_op_missing_file_returns_clean_error() {
        let err = render_op_description("/nonexistent/path/never.yaml", "x").unwrap_err();
        assert!(err.contains("never.yaml"),
            "error should embed the file path: {err}");
    }
}
