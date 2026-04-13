// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Plotter adapter: live-updating terminal plot of workload outputs.
//!
//! Renders numeric workload outputs as braille scatter/line plots
//! that update incrementally as data arrives. Uses a framebuffer
//! with dirty-region tracking — only changed cells are redrawn.
//!
//! Modes:
//!   plot        — line plot per numeric field (default)
//!   parametric  — scatter plot using first two numeric fields as (x, y)
//!
//! The display refreshes at up to 10Hz. New data points are buffered
//! and merged into the framebuffer on each refresh tick.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use nb_activity::adapter::{
    AdapterError, DriverAdapter, ExecutionError, OpDispenser, OpResult, ResolvedFields,
};
use nb_variates::node::Value;
use nb_workload::model::ParsedOp;

// ─── Configuration ─────────────────────────────────────────────

pub struct PlotterConfig {
    pub mode: String,
    pub width: usize,
    pub height: usize,
    pub no_color: bool,
}

impl Default for PlotterConfig {
    fn default() -> Self {
        Self { mode: "plot".into(), width: 0, height: 0, no_color: false }
    }
}

// ─── Framebuffer ───────────────────────────────────────────────

/// A braille character cell. Each cell is a 2×4 dot matrix encoded
/// as a byte (bits 0-7 map to the 8 braille dots).
#[derive(Clone, Copy, PartialEq)]
struct Cell(u8);

impl Cell {
    fn empty() -> Self { Cell(0) }
    fn set_dot(&mut self, dx: usize, dy: usize) {
        // Braille dot positions:
        //   (0,0)=bit0  (1,0)=bit3
        //   (0,1)=bit1  (1,1)=bit4
        //   (0,2)=bit2  (1,2)=bit5
        //   (0,3)=bit6  (1,3)=bit7
        let bit = match (dx, dy) {
            (0, 0) => 0, (1, 0) => 3,
            (0, 1) => 1, (1, 1) => 4,
            (0, 2) => 2, (1, 2) => 5,
            (0, 3) => 6, (1, 3) => 7,
            _ => return,
        };
        self.0 |= 1 << bit;
    }
    fn to_char(self) -> char {
        char::from_u32(0x2800 + self.0 as u32).unwrap_or(' ')
    }
}

/// Terminal framebuffer with dirty-cell tracking.
///
/// Dirty cells are tracked during `set_dot` — no full-buffer diff
/// needed at flush time. Only cells that actually changed are
/// redrawn.
struct FrameBuffer {
    cells: Vec<Vec<Cell>>,
    width: usize,
    height: usize,
    /// Dirty cell coordinates (y, x) accumulated since last flush.
    /// BTreeSet keeps them sorted and deduped for efficient run consolidation.
    dirty: std::collections::BTreeSet<(usize, usize)>,
    /// Whether this is the first frame (needs full draw).
    first_frame: bool,
}

impl FrameBuffer {
    fn new(width: usize, height: usize) -> Self {
        Self {
            cells: vec![vec![Cell::empty(); width]; height],
            width,
            height,
            dirty: std::collections::BTreeSet::new(),
            first_frame: true,
        }
    }

    /// Set a dot at pixel coordinates (2x width, 4x height resolution).
    /// Tracks which character cells become dirty.
    fn set_dot(&mut self, px: usize, py: usize) {
        let cx = px / 2;
        let cy = py / 4;
        if cx < self.width && cy < self.height {
            let old = self.cells[cy][cx];
            self.cells[cy][cx].set_dot(px % 2, py % 4);
            if self.cells[cy][cx] != old {
                self.dirty.insert((cy, cx));
            }
        }
    }

    /// Clear all cells, marking everything dirty.
    fn clear(&mut self) {
        for row in &mut self.cells {
            for cell in row.iter_mut() {
                *cell = Cell::empty();
            }
        }
        self.first_frame = true;
        self.dirty.clear();
    }

    /// Flush dirty cells to terminal. On first frame, draws everything.
    /// On subsequent frames, only redraws cells in the dirty set.
    fn flush(&mut self, color: &str, reset: &str, row_offset: usize) {
        use std::io::Write;

        if self.first_frame {
            for y in 0..self.height {
                let line: String = self.cells[y].iter().map(|c| c.to_char()).collect();
                println!("{color}{line}{reset}");
            }
            self.first_frame = false;
            self.dirty.clear();
            return;
        }

        if self.dirty.is_empty() { return; }

        // BTreeSet is already sorted — group into contiguous runs
        let dirty_vec: Vec<(usize, usize)> = self.dirty.iter().copied().collect();
        let mut i = 0;
        while i < dirty_vec.len() {
            let (y, start_x) = dirty_vec[i];
            let mut end_x = start_x;
            while i + 1 < dirty_vec.len()
                && dirty_vec[i + 1].0 == y
                && dirty_vec[i + 1].1 == end_x + 1
            {
                i += 1;
                end_x = dirty_vec[i].1;
            }
            let row = row_offset + y + 1;
            let col = start_x + 1;
            let chars: String = (start_x..=end_x)
                .map(|cx| self.cells[y][cx].to_char())
                .collect();
            print!("\x1b[{row};{col}H{color}{chars}{reset}");
            i += 1;
        }

        // Move cursor below the plot
        let bottom = row_offset + self.height + 1;
        print!("\x1b[{bottom};1H");
        let _ = std::io::stdout().flush();

        self.dirty.clear();
    }
}

// ─── Shared data ───────────────────────────────────────────────

struct PlotData {
    numeric: HashMap<String, Vec<f64>>,
    field_order: Vec<String>,
    new_since_render: bool,
}

impl PlotData {
    fn new() -> Self {
        Self { numeric: HashMap::new(), field_order: Vec::new(), new_since_render: false }
    }

    fn record(&mut self, fields: &ResolvedFields) {
        for (i, name) in fields.names.iter().enumerate() {
            if !self.field_order.contains(name) {
                self.field_order.push(name.clone());
            }
            let f = match &fields.values[i] {
                Value::U64(v) => *v as f64,
                Value::F64(v) => *v,
                Value::Bool(v) => if *v { 1.0 } else { 0.0 },
                _ => continue,
            };
            self.numeric.entry(name.clone()).or_default().push(f);
        }
        self.new_since_render = true;
    }
}

// ─── Adapter ───────────────────────────────────────────────────

pub struct PlotterAdapter {
    data: Arc<Mutex<PlotData>>,
    config: PlotterConfig,
    running: Arc<AtomicBool>,
    render_thread: Option<std::thread::JoinHandle<()>>,
}

impl PlotterAdapter {
    pub fn new() -> Self { Self::with_config(PlotterConfig::default()) }

    pub fn with_config(config: PlotterConfig) -> Self {
        let data = Arc::new(Mutex::new(PlotData::new()));
        let running = Arc::new(AtomicBool::new(true));

        let term_w = if config.width > 0 { config.width } else { terminal_width().unwrap_or(120) };
        let term_h = if config.height > 0 { config.height } else { terminal_height().unwrap_or(30) };
        let plot_h = term_h.saturating_sub(4);
        let use_color = !config.no_color && atty_stdout();
        let mode = config.mode.clone();

        // Hide cursor and clear screen region
        if use_color { print!("\x1b[?25l"); } // hide cursor
        println!(); // blank line before plot

        let render_data = data.clone();
        let render_running = running.clone();
        let render_thread = std::thread::spawn(move || {
            let color = if use_color { truecolor_fg(0) } else { String::new() };
            let reset = if use_color { "\x1b[0m" } else { "" };
            let mut fb = FrameBuffer::new(term_w, plot_h);
            let refresh_interval = Duration::from_millis(100); // 10Hz

            while render_running.load(Ordering::Relaxed) {
                std::thread::sleep(refresh_interval);

                let mut data = render_data.lock().unwrap();
                if !data.new_since_render { continue; }
                data.new_since_render = false;

                // Rebuild framebuffer from all data
                fb.clear();

                let ordered: Vec<String> = data.field_order.iter()
                    .filter(|n| data.numeric.contains_key(*n))
                    .cloned().collect();

                if ordered.is_empty() { continue; }

                match mode.as_str() {
                    "parametric" | "xy" if ordered.len() >= 2 => {
                        let x_vals = &data.numeric[&ordered[0]];
                        let y_vals = &data.numeric[&ordered[1]];
                        plot_parametric_to_fb(&mut fb, x_vals, y_vals);
                    }
                    _ => {
                        // Line plot: divide height among fields
                        let band_h = (fb.height / ordered.len().max(1)).max(3);
                        for (fi, name) in ordered.iter().enumerate() {
                            let vals = &data.numeric[name];
                            let y_offset = fi * band_h;
                            plot_line_to_fb(&mut fb, vals, y_offset, band_h);
                        }
                    }
                }

                drop(data);

                fb.flush(&color, reset, 1);
            }

            // Final render
            let mut data = render_data.lock().unwrap();
            if data.new_since_render {
                data.new_since_render = false;
                fb.clear();
                let ordered: Vec<String> = data.field_order.iter()
                    .filter(|n| data.numeric.contains_key(*n))
                    .cloned().collect();
                match mode.as_str() {
                    "parametric" | "xy" if ordered.len() >= 2 => {
                        plot_parametric_to_fb(&mut fb, &data.numeric[&ordered[0]], &data.numeric[&ordered[1]]);
                    }
                    _ => {
                        let band_h = (fb.height / ordered.len().max(1)).max(3);
                        for (fi, name) in ordered.iter().enumerate() {
                            plot_line_to_fb(&mut fb, &data.numeric[name], fi * band_h, band_h);
                        }
                    }
                }
                drop(data);
                fb.flush(&color, reset, 1);
            }

            if use_color { print!("\x1b[?25h"); } // show cursor
            use std::io::Write;
            let _ = std::io::stdout().flush();
        });

        Self {
            data,
            config,
            running,
            render_thread: Some(render_thread),
        }
    }
}

impl DriverAdapter for PlotterAdapter {
    fn name(&self) -> &str { "plotter" }
    fn map_op(&self, _template: &ParsedOp) -> Result<Box<dyn OpDispenser>, String> {
        Ok(Box::new(PlotterDispenser { data: self.data.clone() }))
    }
}

impl Drop for PlotterAdapter {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
        if let Some(handle) = self.render_thread.take() {
            let _ = handle.join();
        }
    }
}

struct PlotterDispenser {
    data: Arc<Mutex<PlotData>>,
}

impl OpDispenser for PlotterDispenser {
    fn execute<'a>(
        &'a self,
        _cycle: u64,
        fields: &'a ResolvedFields,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        Box::pin(async move {
            self.data.lock().unwrap().record(fields);
            Ok(OpResult { body: None, captures: HashMap::new(), skipped: false })
        })
    }
}

// ─── Plot helpers ──────────────────────────────────────────────

fn plot_parametric_to_fb(fb: &mut FrameBuffer, x_vals: &[f64], y_vals: &[f64]) {
    let n = x_vals.len().min(y_vals.len());
    if n == 0 { return; }
    let x_min = x_vals[..n].iter().cloned().fold(f64::MAX, f64::min);
    let x_max = x_vals[..n].iter().cloned().fold(f64::MIN, f64::max);
    let y_min = y_vals[..n].iter().cloned().fold(f64::MAX, f64::min);
    let y_max = y_vals[..n].iter().cloned().fold(f64::MIN, f64::max);
    let x_range = if (x_max - x_min).abs() < 1e-10 { 1.0 } else { x_max - x_min };
    let y_range = if (y_max - y_min).abs() < 1e-10 { 1.0 } else { y_max - y_min };

    let pw = fb.width * 2;
    let ph = fb.height * 4;
    for i in 0..n {
        let px = ((x_vals[i] - x_min) / x_range * (pw - 1) as f64) as usize;
        let py = ((y_vals[i] - y_min) / y_range * (ph - 1) as f64) as usize;
        fb.set_dot(px.min(pw - 1), (ph - 1).saturating_sub(py));
    }
}

fn plot_line_to_fb(fb: &mut FrameBuffer, vals: &[f64], y_offset: usize, band_h: usize) {
    if vals.is_empty() { return; }
    let min_v = vals.iter().cloned().fold(f64::MAX, f64::min);
    let max_v = vals.iter().cloned().fold(f64::MIN, f64::max);
    let range = if (max_v - min_v).abs() < 1e-10 { 1.0 } else { max_v - min_v };

    let pw = fb.width * 2;
    let ph = band_h * 4;
    let n = vals.len();
    for (i, &v) in vals.iter().enumerate() {
        let px = (i as f64 / n as f64 * (pw - 1) as f64) as usize;
        let py = ((v - min_v) / range * (ph - 1) as f64) as usize;
        fb.set_dot(px.min(pw - 1), y_offset * 4 + (ph - 1).saturating_sub(py));
    }
}

fn truecolor_fg(idx: usize) -> String {
    let palette: [(u8, u8, u8); 10] = [
        (86, 180, 233), (230, 159, 0), (0, 158, 115), (240, 228, 66),
        (0, 114, 178), (213, 94, 0), (204, 121, 167), (0, 0, 0),
        (140, 86, 75), (148, 103, 189),
    ];
    let (r, g, b) = palette[idx % palette.len()];
    format!("\x1b[38;2;{r};{g};{b}m")
}

fn terminal_width() -> Option<usize> {
    #[cfg(unix)]
    {
        let mut ws = libc::winsize { ws_row: 0, ws_col: 0, ws_xpixel: 0, ws_ypixel: 0 };
        if unsafe { libc::ioctl(1, libc::TIOCGWINSZ, &mut ws) } == 0 && ws.ws_col > 0 {
            return Some(ws.ws_col as usize);
        }
    }
    None
}

fn terminal_height() -> Option<usize> {
    #[cfg(unix)]
    {
        let mut ws = libc::winsize { ws_row: 0, ws_col: 0, ws_xpixel: 0, ws_ypixel: 0 };
        if unsafe { libc::ioctl(1, libc::TIOCGWINSZ, &mut ws) } == 0 && ws.ws_row > 0 {
            return Some(ws.ws_row as usize);
        }
    }
    None
}

fn atty_stdout() -> bool {
    #[cfg(unix)]
    { unsafe { libc::isatty(1) != 0 } }
    #[cfg(not(unix))]
    { false }
}
