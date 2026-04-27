// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! The `plot gk` subcommand: evaluate a GK expression or .gk file for N cycles
//! and render each numeric output as a braille line plot and each string/bool
//! output as a horizontal-bar histogram.

use std::collections::HashMap;
use nb_variates::dsl::compile::compile_gk;
use nb_variates::node::Value;

// ── Terminal helpers ────────────────────────────────────────────

/// Query the terminal size via `TIOCGWINSZ`.  Returns `None` when unavailable.
fn term_size() -> Option<(usize, usize)> {
    use std::os::unix::io::AsRawFd;
    let fd = std::io::stdout().as_raw_fd();
    let mut ws: libc::winsize = unsafe { std::mem::zeroed() };
    if unsafe { libc::ioctl(fd, libc::TIOCGWINSZ, &mut ws) } == 0
        && ws.ws_col > 0
        && ws.ws_row > 0
    {
        Some((ws.ws_col as usize, ws.ws_row as usize))
    } else {
        None
    }
}

// ── Argument parsing ────────────────────────────────────────────

/// Parsed arguments for `plot gk`.
struct PlotArgs {
    /// GK inline expression or `.gk` file path.
    expr: String,
    /// Number of evaluation cycles.
    cycles: u64,
    /// Restrict plotting to this single output (empty = all outputs).
    output_filter: Option<String>,
    /// Plot width in character columns.
    width: Option<usize>,
    /// Plot height in character rows.
    height: Option<usize>,
    /// Maximum distinct label count before truncating histograms.
    max_labels: usize,
    /// Suppress ANSI escape codes.
    no_color: bool,
    /// X scale: 1.0 = one data point per character column (2 dots).
    /// < 1.0 compresses (more cycles per column), > 1.0 stretches.
    /// Default: auto-fit to terminal width.
    xscale: Option<f64>,
    /// Y scale: units per character row (4 dots).
    /// Default: auto-fit to data range / plot height.
    yscale: Option<f64>,
    /// Rendering mode: "plot" (default line plot), "histogram" (bucket all
    /// numeric outputs into a horizontal bar histogram).
    mode: String,
}

fn parse_plot_args(args: &[String]) -> Result<PlotArgs, String> {
    let mut pa = PlotArgs {
        expr: String::new(),
        cycles: 1000,
        output_filter: None,
        width: None,
        height: None,
        max_labels: 100,
        no_color: false,
        xscale: None,
        yscale: None,
        mode: "plot".to_string(),
    };

    for arg in args {
        if let Some(v) = arg.strip_prefix("cycles=") {
            pa.cycles = v.parse().map_err(|_| format!("invalid cycles value: '{v}'"))?;
        } else if let Some(v) = arg.strip_prefix("output=") {
            pa.output_filter = Some(v.to_string());
        } else if let Some(v) = arg.strip_prefix("--width=") {
            pa.width = Some(v.parse().map_err(|_| format!("invalid width: '{v}'"))?);
        } else if let Some(v) = arg.strip_prefix("--height=") {
            pa.height = Some(v.parse().map_err(|_| format!("invalid height: '{v}'"))?);
        } else if let Some(v) = arg.strip_prefix("--max-labels=") {
            pa.max_labels = v.parse().map_err(|_| format!("invalid max-labels: '{v}'"))?;
        } else if let Some(v) = arg.strip_prefix("--xscale=") {
            pa.xscale = Some(v.parse().map_err(|_| format!("invalid xscale: '{v}'"))?);
        } else if let Some(v) = arg.strip_prefix("--yscale=") {
            pa.yscale = Some(v.parse().map_err(|_| format!("invalid yscale: '{v}'"))?);
        } else if let Some(v) = arg.strip_prefix("--mode=") {
            match v {
                "plot" | "histogram" | "parametric" | "xy" => pa.mode = v.to_string(),
                other => return Err(format!("unknown --mode='{other}' (supported: plot, histogram, parametric)")),
            }
        } else if arg == "--no-color" {
            pa.no_color = true;
        } else if arg.starts_with('-') {
            eprintln!("warning: unrecognized option '{arg}' (ignored)");
        } else {
            if pa.expr.is_empty() {
                pa.expr = arg.clone();
            }
        }
    }

    if pa.expr.is_empty() {
        return Err("missing GK expression or .gk file argument".to_string());
    }
    Ok(pa)
}

// ── Source normalization (mirrors bench.rs) ─────────────────────

/// Convert an inline expression or `.gk` file path into full GK source.
fn normalize_source(expr: &str) -> Result<String, String> {
    if expr.ends_with(".gk") {
        std::fs::read_to_string(expr)
            .map_err(|e| format!("failed to read '{expr}': {e}"))
    } else {
        let expr = expr.replace(';', "\n");
        if expr.contains(":=") {
            let lines: Vec<&str> = expr.lines().map(|l| l.trim())
                .filter(|l| !l.is_empty() && !l.starts_with("//"))
                .collect();
            let mut out_lines = vec!["coordinates := (cycle)".to_string()];
            for (i, line) in lines.iter().enumerate() {
                if line.contains(":=") {
                    out_lines.push(line.to_string());
                } else if i == lines.len() - 1 {
                    out_lines.push(format!("out := {line}"));
                } else {
                    out_lines.push(format!("__expr_{i} := {line}"));
                }
            }
            Ok(out_lines.join("\n"))
        } else {
            Ok(format!("coordinates := (cycle)\nout := {expr}"))
        }
    }
}

// ── Braille plot ────────────────────────────────────────────────

/// Braille dot-matrix bit positions per (dx, dy) offset within a 2×4 cell.
///
/// The Unicode braille block starts at U+2800. Each code point is
/// U+2800 + bitmask, where the bit layout is:
///
/// ```text
/// bit 0 = (col 0, row 0)   bit 3 = (col 1, row 0)
/// bit 1 = (col 0, row 1)   bit 4 = (col 1, row 1)
/// bit 2 = (col 0, row 2)   bit 5 = (col 1, row 2)
/// bit 6 = (col 0, row 3)   bit 7 = (col 1, row 3)
/// ```
#[inline]
fn braille_bit(dx: usize, dy: usize) -> u8 {
    match (dx, dy) {
        (0, 0) => 1 << 0,
        (0, 1) => 1 << 1,
        (0, 2) => 1 << 2,
        (0, 3) => 1 << 6,
        (1, 0) => 1 << 3,
        (1, 1) => 1 << 4,
        (1, 2) => 1 << 5,
        (1, 3) => 1 << 7,
        _ => 0,
    }
}

/// Render a sequence of f64 values as a braille line plot.
///
/// `xscale` controls how many dot columns one sample occupies:
/// - `1.0` auto-fits all samples to the plot width (default behaviour).
/// - Values are cycles-per-dot-column; computed externally and passed in.
///
/// `yscale` controls the value range spanned per dot row:
/// - When `None`, the full data range fills the plot height (default).
/// - When `Some(units_per_row)`, the y-axis uses that fixed scale.
///
/// Returns one `String` per row; each string has `width` characters and is
/// ready for direct `println!` output.
fn draw_braille_plot(
    values: &[f64],
    width: usize,
    height: usize,
    xscale: f64,
    yscale: Option<f64>,
) -> Vec<String> {
    if values.is_empty() || width == 0 || height == 0 {
        return vec!["(no data)".to_string()];
    }

    let dot_w = width * 2;
    let dot_h = height * 4;

    let min_val = values.iter().cloned().fold(f64::MAX, f64::min);
    let max_val = values.iter().cloned().fold(f64::MIN, f64::max);
    // Avoid zero-range: if all values identical, give a flat line at middle.
    let range = if (max_val - min_val).abs() < 1e-10 { 1.0 } else { max_val - min_val };

    // Effective y-range covered by the plot in value units.
    // When yscale is provided, use it; otherwise auto-fit to data range.
    let effective_range = if let Some(units_per_row) = yscale {
        let units_per_dot = units_per_row / 4.0;
        (units_per_dot * dot_h as f64).max(range)
    } else {
        range
    };

    let mut grid = vec![vec![0u8; width]; height];

    // xscale is cycles-per-dot-column: 1.0 = each sample → 1 dot column
    // (auto-fit equivalent for len==dot_w). We convert to a multiplier:
    // dot column index = i / xscale.
    for (i, &v) in values.iter().enumerate() {
        let x_dot = (i as f64 / xscale) as usize;
        if x_dot >= dot_w {
            break; // past the right edge — remaining samples are off-screen
        }

        let y_frac = if effective_range <= 1e-10 {
            0.5
        } else {
            (v - min_val) / effective_range
        };
        let y_dot = (y_frac * (dot_h - 1) as f64).round() as usize;
        let y_dot = (dot_h - 1).saturating_sub(y_dot); // flip: row 0 is top

        let cx = x_dot / 2;
        let cy = y_dot / 4;
        let dx = x_dot % 2;
        let dy = y_dot % 4;

        if cx < width && cy < height {
            grid[cy][cx] |= braille_bit(dx, dy);
        }
    }

    grid.iter().map(|row| {
        row.iter().map(|&bits| {
            char::from_u32(0x2800 + bits as u32).unwrap_or(' ')
        }).collect()
    }).collect()
}

// ── Histogram ───────────────────────────────────────────────────

/// Render a sorted, proportional horizontal bar histogram.
///
/// Returns one `String` per label row.  Warns to stderr if `counts` exceeds
/// `max_labels`.
fn draw_histogram(
    counts: &HashMap<String, u64>,
    max_labels: usize,
    width: usize,
) -> Vec<String> {
    let mut sorted: Vec<(&str, u64)> = counts.iter()
        .map(|(k, v)| (k.as_str(), *v))
        .collect();
    sorted.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));

    let truncated = sorted.len() > max_labels;
    if truncated {
        eprintln!(
            "warning: {} distinct values exceed --max-labels={}, showing top {}",
            sorted.len(),
            max_labels,
            max_labels
        );
        sorted.truncate(max_labels);
    }

    let max_count = sorted.iter().map(|(_, c)| *c).max().unwrap_or(1);
    let max_label_len = sorted.iter().map(|(k, _)| k.len()).max().unwrap_or(0).min(20);
    // "label │bar count"  — bar_width fills the remainder.
    let overhead = max_label_len + 3 + 1 + 1; // " │" + space + count digits
    let count_digits = format!("{max_count}").len();
    let bar_width = width.saturating_sub(overhead + count_digits);

    sorted.iter().map(|(label, count)| {
        let bar_len = if max_count > 0 {
            (*count as f64 / max_count as f64 * bar_width as f64).round() as usize
        } else {
            0
        };
        let bar: String = "█".repeat(bar_len);
        let label_trunc = if label.len() > max_label_len {
            &label[..max_label_len]
        } else {
            label
        };
        format!(
            "{:>lw$} │{:<bw$} {}",
            label_trunc,
            bar,
            count,
            lw = max_label_len,
            bw = bar_width,
        )
    }).collect()
}

// ── Section header ──────────────────────────────────────────────

/// Print a decorated section header spanning `width` columns.
fn print_header(label: &str, detail: &str, width: usize, color: &str, reset: &str) {
    let prefix = "───  ";
    let inner = format!("{label} ({detail})");
    let available = width.saturating_sub(prefix.len());
    // Truncate inner if it would exceed the line width
    let inner_display = if inner.len() > available.saturating_sub(2) {
        format!("{} ", &inner[..available.saturating_sub(3)])
    } else {
        let pad = available.saturating_sub(inner.len() + 1);
        format!("{inner} {}", "─".repeat(pad))
    };
    println!("{color}{prefix}{inner_display}{reset}");
}

/// Print the X-axis label row for a line plot.
fn print_xaxis(cycles: u64, width: usize) {
    let prefix = "cycles: ";
    let start = "0";
    let end = format!("{cycles}");
    let available = width.saturating_sub(prefix.len() + start.len() + end.len());
    let dashes = "─".repeat(available);
    println!("{prefix}{start}{dashes}{end}");
}

// ── 24-bit truecolor palette ───────────────────────────────────

/// Perceptually distinct 24-bit truecolor escape sequences.
/// Chosen for readability on both dark and light terminals.
const TRUECOLORS: &[(u8, u8, u8)] = &[
    (0, 187, 255),   // electric blue
    (255, 167, 38),   // warm orange
    (0, 230, 118),    // spring green
    (255, 82, 82),    // coral red
    (170, 128, 255),  // soft purple
    (255, 234, 0),    // golden yellow
    (0, 200, 200),    // teal
    (255, 128, 171),  // rose pink
    (100, 221, 23),   // lime
    (121, 134, 203),  // muted indigo
];

fn truecolor_fg(idx: usize) -> String {
    let (r, g, b) = TRUECOLORS[idx % TRUECOLORS.len()];
    format!("\x1b[38;2;{r};{g};{b}m")
}

// ── Collected output data ───────────────────────────────────────

/// Classification of a GK output's data.
enum OutputData {
    /// Collected f64 samples, plus auto-detected min/max.
    Numeric(Vec<f64>),
    /// Counted unique string representations (including booleans).
    Categorical(HashMap<String, u64>),
}

// ── Public entry point ──────────────────────────────────────────

/// Entry point for `nbrs plot gk <args>`.
pub fn plot_command(args: &[String]) {
    let topic = args.first().map(|s| s.as_str()).unwrap_or("");
    if topic != "gk" {
        eprintln!("Usage: nbrs plot gk <expr|file.gk> [cycles=N] [output=name]");
        eprintln!("       [--width=N] [--height=N] [--max-labels=N] [--no-color]");
        eprintln!("       [--xscale=F] [--yscale=F] [--mode=plot|histogram|parametric]");
        std::process::exit(1);
    }

    let pa = match parse_plot_args(&args[1..]) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    };

    let source = match normalize_source(&pa.expr) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    };

    let mut kernel = match compile_gk(&source) {
        Ok(k) => k,
        Err(e) => {
            eprintln!("error: failed to compile GK expression: {e}");
            std::process::exit(1);
        }
    };

    // Determine terminal dimensions.
    let is_tty = std::io::IsTerminal::is_terminal(&std::io::stdout());
    let (term_w, term_h) = if is_tty {
        term_size().unwrap_or((120, 40))
    } else {
        (120, 40)
    };
    let plot_w = pa.width.unwrap_or(term_w).max(20);
    let plot_h_total = pa.height.unwrap_or(term_h);

    // Build output name list, filtering internal names.
    let all_output_names: Vec<String> = kernel.output_names()
        .into_iter()
        .filter(|n| !n.starts_with("__"))
        .map(|n| n.to_string())
        .collect();

    let selected_outputs: Vec<String> = if let Some(ref filter) = pa.output_filter {
        let wanted: Vec<&str> = filter.split(',').map(|s| s.trim()).collect();
        // Preserve requested order (not declaration order) when filtering
        wanted.iter()
            .filter_map(|w| all_output_names.iter().find(|n| n == w).cloned())
            .collect()
    } else {
        all_output_names
    };

    if selected_outputs.is_empty() {
        eprintln!("error: no matching outputs found");
        std::process::exit(1);
    }

    // ── Sample one cycle to classify each output ─────────────────
    kernel.set_inputs(&[0]);
    let mut classifications: HashMap<String, bool> = HashMap::new(); // true = numeric
    for name in &selected_outputs {
        let val = kernel.pull(name).clone();
        let numeric = matches!(val, Value::U64(_) | Value::F64(_));
        classifications.insert(name.clone(), numeric);
    }

    // ── Collect data across all cycles ───────────────────────────
    let mut numeric_data: HashMap<String, Vec<f64>> = HashMap::new();
    let mut categorical_data: HashMap<String, HashMap<String, u64>> = HashMap::new();

    // Pre-allocate numeric vecs.
    for name in &selected_outputs {
        if *classifications.get(name).unwrap_or(&false) {
            numeric_data.insert(name.clone(), Vec::with_capacity(pa.cycles as usize));
        } else {
            categorical_data.insert(name.clone(), HashMap::new());
        }
    }

    // Evaluate every cycle.
    for cycle in 0..pa.cycles {
        kernel.set_inputs(&[cycle]);
        for name in &selected_outputs {
            let val = kernel.pull(name).clone();
            if *classifications.get(name).unwrap_or(&false) {
                let fval = match val {
                    Value::U64(v) => v as f64,
                    Value::F64(v) => v,
                    _ => 0.0,
                };
                if let Some(vec) = numeric_data.get_mut(name) {
                    vec.push(fval);
                }
            } else {
                let key = match val {
                    Value::Str(s) => s,
                    Value::Bool(b) => b.to_string(),
                    Value::U64(v) => v.to_string(),
                    Value::F64(v) => format!("{v:.6}"),
                    _ => "(none)".to_string(),
                };
                if let Some(map) = categorical_data.get_mut(name) {
                    *map.entry(key).or_insert(0) += 1;
                }
            }
        }
    }

    // ── Group outputs into typed buckets for rendering ────────────
    let mut output_data: Vec<(String, OutputData)> = Vec::new();
    for name in &selected_outputs {
        if let Some(vals) = numeric_data.remove(name) {
            output_data.push((name.clone(), OutputData::Numeric(vals)));
        } else if let Some(counts) = categorical_data.remove(name) {
            output_data.push((name.clone(), OutputData::Categorical(counts)));
        }
    }

    // ── Compute per-band height for numeric outputs ───────────────
    let numeric_count = output_data.iter()
        .filter(|(_, d)| matches!(d, OutputData::Numeric(_)))
        .count();
    // Minimum 10 rows per band, or half the terminal height, whichever is more.
    let min_band_h = if is_tty { (term_h / 2).max(10) } else { 10 };
    let band_h = if numeric_count > 0 {
        (plot_h_total / numeric_count).max(min_band_h)
    } else {
        min_band_h
    };

    // ── ANSI helpers ──────────────────────────────────────────────
    let use_color = is_tty && !pa.no_color;
    let reset = if use_color { "\x1b[0m" } else { "" };

    // ── Parametric (XY) mode ──────────────────────────────────────
    if pa.mode == "parametric" || pa.mode == "xy" {
        // Take first two numeric outputs as X and Y
        let numeric_outputs: Vec<(&str, &[f64])> = output_data.iter()
            .filter_map(|(name, data)| match data {
                OutputData::Numeric(vals) => Some((name.as_str(), vals.as_slice())),
                _ => None,
            })
            .collect();

        if numeric_outputs.len() < 2 {
            eprintln!("error: parametric mode requires at least 2 numeric outputs (got {})", numeric_outputs.len());
            std::process::exit(1);
        }

        let (x_name, x_vals) = numeric_outputs[0];
        let (y_name, y_vals) = numeric_outputs[1];
        let n = x_vals.len().min(y_vals.len());

        let x_min = x_vals[..n].iter().cloned().fold(f64::MAX, f64::min);
        let x_max = x_vals[..n].iter().cloned().fold(f64::MIN, f64::max);
        let y_min = y_vals[..n].iter().cloned().fold(f64::MAX, f64::min);
        let y_max = y_vals[..n].iter().cloned().fold(f64::MIN, f64::max);
        let x_range = if (x_max - x_min).abs() < 1e-10 { 1.0 } else { x_max - x_min };
        let y_range = if (y_max - y_min).abs() < 1e-10 { 1.0 } else { y_max - y_min };

        let chart_w = plot_w;
        let chart_h = band_h.max(20);

        // Braille scatter: 2 dots wide × 4 dots tall per character cell
        let dot_w = chart_w * 2;
        let dot_h = chart_h * 4;
        let mut grid = vec![vec![false; dot_w]; dot_h];

        for i in 0..n {
            let dx = ((x_vals[i] - x_min) / x_range * (dot_w - 1) as f64) as usize;
            let dy = ((y_vals[i] - y_min) / y_range * (dot_h - 1) as f64) as usize;
            let dx = dx.min(dot_w - 1);
            let dy = dy.min(dot_h - 1);
            grid[dot_h - 1 - dy][dx] = true; // flip Y so up is positive
        }

        // Render braille
        let color_owned = if use_color { truecolor_fg(0) } else { String::new() };
        let color = color_owned.as_str();

        let detail = format!("{x_name} vs {y_name}, {n} points");
        print_header(&format!("{x_name} × {y_name}"), &detail, plot_w, color, reset);

        for row in (0..dot_h).step_by(4) {
            let mut line = String::new();
            for col in (0..dot_w).step_by(2) {
                let mut dots = 0u8;
                // Braille dot mapping (Unicode offset from 0x2800):
                // col+0,row+0 = bit 0   col+1,row+0 = bit 3
                // col+0,row+1 = bit 1   col+1,row+1 = bit 4
                // col+0,row+2 = bit 2   col+1,row+2 = bit 5
                // col+0,row+3 = bit 6   col+1,row+3 = bit 7
                for (dr, bits) in [(0, [0, 3]), (1, [1, 4]), (2, [2, 5]), (3, [6, 7])] {
                    let r = row + dr;
                    if r < dot_h {
                        if col < dot_w && grid[r][col] { dots |= 1 << bits[0]; }
                        if col + 1 < dot_w && grid[r][col + 1] { dots |= 1 << bits[1]; }
                    }
                }
                line.push(char::from_u32(0x2800 + dots as u32).unwrap_or(' '));
            }
            println!("{color}{line}{reset}");
        }

        // Axis labels
        println!("{color}  X: [{x_min:.4}, {x_max:.4}]  Y: [{y_min:.4}, {y_max:.4}]{reset}");
        println!();
        return;
    }

    // ── Render ────────────────────────────────────────────────────
    let mut color_idx = 0usize;
    for (name, data) in &output_data {
        let color_owned = if use_color { truecolor_fg(color_idx) } else { String::new() };
        let color = color_owned.as_str();
        color_idx += 1;

        match data {
            OutputData::Numeric(vals) if pa.mode == "histogram" => {
                // Histogram mode: bucket numeric values into 20 equal-width bins
                // and render using the existing histogram renderer.
                let min_val = vals.iter().cloned().fold(f64::MAX, f64::min);
                let max_val = vals.iter().cloned().fold(f64::MIN, f64::max);
                let range = if (max_val - min_val).abs() < 1e-10 { 1.0 } else { max_val - min_val };
                let bucket_count: usize = 20;
                let bucket_width = range / bucket_count as f64;

                let mut counts: HashMap<String, u64> = HashMap::new();
                for &v in vals.iter() {
                    let bucket_idx = ((v - min_val) / bucket_width).floor() as usize;
                    let bucket_idx = bucket_idx.min(bucket_count - 1);
                    let lo = min_val + bucket_idx as f64 * bucket_width;
                    let hi = lo + bucket_width;
                    let key = format!("[{lo:.4}, {hi:.4})");
                    *counts.entry(key).or_insert(0) += 1;
                }

                let detail = format!("{} values, {} buckets", vals.len(), bucket_count);
                print_header(name, &detail, plot_w, color, reset);

                let rows = draw_histogram(&counts, pa.max_labels, plot_w);
                for row in &rows {
                    println!("{color}{row}{reset}");
                }
                println!();
            }
            OutputData::Numeric(vals) => {
                let min_val = vals.iter().cloned().fold(f64::MAX, f64::min);
                let max_val = vals.iter().cloned().fold(f64::MIN, f64::max);
                let range = if (max_val - min_val).abs() < 1e-10 { 1.0 } else { max_val - min_val };

                // Compute auto x-scale: cycles per dot-column.
                // xscale=1.0 means 1 cycle per character column (2 dot columns).
                // The auto value maps all samples to exactly fill the plot width.
                let dot_w = plot_w * 2;
                let auto_xscale = vals.len() as f64 / dot_w as f64;
                let xscale = pa.xscale.unwrap_or(auto_xscale);

                // Compute auto y-scale: value units per dot-row.
                // Shown in the header for informational purposes.
                let dot_h = band_h * 4;
                let auto_yscale = range / dot_h as f64;
                let yscale_display = pa.yscale.unwrap_or(auto_yscale);

                let detail = format!(
                    "min={min_val:.4}, max={max_val:.4}, xscale={xscale:.2} cycles/col, yscale={yscale_display:.4} units/row"
                );
                print_header(name, &detail, plot_w, color, reset);

                let rows = draw_braille_plot(vals, plot_w, band_h, xscale, pa.yscale);
                for row in &rows {
                    println!("{color}{row}{reset}");
                }
                print_xaxis(pa.cycles, plot_w);
                println!();
            }
            OutputData::Categorical(counts) => {
                let distinct = counts.len();
                let detail = format!("{distinct} distinct values");
                print_header(name, &detail, plot_w, color, reset);

                let rows = draw_histogram(counts, pa.max_labels, plot_w);
                for row in &rows {
                    println!("{color}{row}{reset}");
                }
                println!();
            }
        }
    }
}
