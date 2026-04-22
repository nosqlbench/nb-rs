// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Plotter adapter: live-updating terminal plot of workload outputs.
//!
//! Modes:
//!   plot        — line plot per numeric field (default)
//!   parametric  — scatter using first two numeric fields as (x, y)
//!   polar       — polar plot using first two fields as (r, theta)

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use nb_activity::adapter::{
    DriverAdapter, ExecutionError, OpDispenser, OpResult, ResolvedFields,
};
use nb_variates::node::Value;
use nb_workload::model::ParsedOp;

const PALETTE: [(u8, u8, u8); 10] = [
    (86, 180, 233), (230, 159, 0), (0, 158, 115), (240, 228, 66),
    (0, 114, 178), (213, 94, 0), (204, 121, 167), (100, 100, 100),
    (140, 86, 75), (148, 103, 189),
];

pub struct PlotterConfig {
    pub mode: String,
    pub width: usize,
    pub height: usize,
    pub no_color: bool,
    pub fade: f32,
    /// Lane assignment: each inner Vec is a lane containing field names.
    /// `lanes=x,y;z` → `[["x","y"], ["z"]]`.
    /// Empty means auto (one lane per field).
    pub lanes: Vec<Vec<String>>,
}

impl Default for PlotterConfig {
    fn default() -> Self {
        Self { mode: "plot".into(), width: 0, height: 0, no_color: false, fade: 0.0, lanes: Vec::new() }
    }
}

// ─── Cell & FrameBuffer ────────────────────────────────────────

#[derive(Clone, Copy, PartialEq)]
struct Cell { dots: u8, r: u8, g: u8, b: u8, bright: f32 }

impl Cell {
    fn empty() -> Self { Cell { dots: 0, r: 0, g: 0, b: 0, bright: 0.0 } }

    fn set_dot(&mut self, dx: usize, dy: usize, r: u8, g: u8, b: u8) {
        let bit = match (dx, dy) {
            (0,0)=>0,(1,0)=>3,(0,1)=>1,(1,1)=>4,(0,2)=>2,(1,2)=>5,(0,3)=>6,(1,3)=>7,_=>return
        };
        self.dots |= 1 << bit;
        self.r = r; self.g = g; self.b = b; self.bright = 1.0;
    }

    fn to_char(self) -> char {
        if self.bright < 0.01 { ' ' } else { char::from_u32(0x2800 + self.dots as u32).unwrap_or(' ') }
    }

    fn decay(&mut self, factor: f32) {
        self.bright *= factor;
        if self.bright < 0.01 { self.bright = 0.0; self.dots = 0; }
    }

    fn color_esc(&self) -> String {
        let r = (self.r as f32 * self.bright) as u8;
        let g = (self.g as f32 * self.bright) as u8;
        let b = (self.b as f32 * self.bright) as u8;
        format!("\x1b[38;2;{r};{g};{b}m")
    }
}

struct FrameBuffer {
    cells: Vec<Vec<Cell>>,
    width: usize,
    height: usize,
    dirty: std::collections::BTreeSet<(usize, usize)>,
    first_frame: bool,
}

impl FrameBuffer {
    fn new(w: usize, h: usize) -> Self {
        Self { cells: vec![vec![Cell::empty(); w]; h], width: w, height: h,
               dirty: std::collections::BTreeSet::new(), first_frame: true }
    }

    fn set_dot(&mut self, px: usize, py: usize, r: u8, g: u8, b: u8) {
        let (cx, cy) = (px / 2, py / 4);
        if cx < self.width && cy < self.height {
            let old = self.cells[cy][cx];
            self.cells[cy][cx].set_dot(px % 2, py % 4, r, g, b);
            if self.cells[cy][cx] != old { self.dirty.insert((cy, cx)); }
        }
    }

    fn set_dot_idx(&mut self, px: usize, py: usize, ci: usize) {
        let (r, g, b) = PALETTE[ci % PALETTE.len()];
        self.set_dot(px, py, r, g, b);
    }

    fn clear(&mut self) {
        for row in &mut self.cells { for c in row.iter_mut() { *c = Cell::empty(); } }
        self.first_frame = true;
        self.dirty.clear();
    }

    fn decay_all(&mut self, factor: f32) {
        for y in 0..self.height { for x in 0..self.width {
            let old = self.cells[y][x];
            if old.bright > 0.01 {
                self.cells[y][x].decay(factor);
                if self.cells[y][x] != old { self.dirty.insert((y, x)); }
            }
        }}
    }

    fn flush(&mut self, use_color: bool, row_offset: usize) {
        use std::io::Write;
        let reset = if use_color { "\x1b[0m" } else { "" };

        if self.first_frame {
            // First frame: draw every cell with absolute positioning
            for y in 0..self.height {
                let row = row_offset + y + 1;
                let mut line = String::new();
                for c in &self.cells[y] {
                    if use_color && c.bright > 0.01 { line.push_str(&c.color_esc()); }
                    line.push(c.to_char());
                    if use_color { line.push_str(reset); }
                }
                print!("\x1b[{row};1H{line}");
            }
            let bottom = row_offset + self.height + 1;
            print!("\x1b[{bottom};1H");
            let _ = std::io::stdout().flush();
            self.first_frame = false;
            self.dirty.clear();
            return;
        }
        if self.dirty.is_empty() { return; }

        let dv: Vec<(usize,usize)> = self.dirty.iter().copied().collect();
        for &(y, x) in &dv {
            let row = row_offset + y + 1;
            let col = x + 1;
            let c = &self.cells[y][x];
            if use_color && c.bright > 0.01 {
                print!("\x1b[{row};{col}H{}{}{reset}", c.color_esc(), c.to_char());
            } else {
                print!("\x1b[{row};{col}H{}", c.to_char());
            }
        }
        let bottom = row_offset + self.height + 1;
        print!("\x1b[{bottom};1H");
        let _ = std::io::stdout().flush();
        self.dirty.clear();
    }
}

// ─── Data collector ────────────────────────────────────────────

struct PlotData {
    numeric: HashMap<String, Vec<f64>>,
    field_order: Vec<String>,
    new_since_render: bool,
}

impl PlotData {
    fn new() -> Self { Self { numeric: HashMap::new(), field_order: Vec::new(), new_since_render: false } }
    fn record(&mut self, fields: &ResolvedFields) {
        for (i, name) in fields.names.iter().enumerate() {
            if !self.field_order.contains(name) { self.field_order.push(name.clone()); }
            let f = match &fields.values[i] {
                Value::U64(v) => *v as f64, Value::F64(v) => *v,
                Value::Bool(v) => if *v { 1.0 } else { 0.0 }, _ => continue,
            };
            self.numeric.entry(name.clone()).or_default().push(f);
        }
        self.new_since_render = true;
    }
}

// ─── Adapter ───────────────────────────────────────────────────

pub struct PlotterAdapter {
    data: Arc<Mutex<PlotData>>,
    #[allow(dead_code)]
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
        let fade = config.fade;
        let lanes = config.lanes.clone();

        // Enter alternate screen buffer for clean canvas
        if use_color {
            print!("\x1b[?1049h"); // alt screen
            print!("\x1b[?25l");   // hide cursor
            print!("\x1b[2J");     // clear screen
            use std::io::Write;
            let _ = std::io::stdout().flush();
        }

        let rd = data.clone();
        let rr = running.clone();
        let render_thread = std::thread::spawn(move || {
            let mut fb = FrameBuffer::new(term_w, plot_h);
            let interval = Duration::from_millis(100);
            let mut last_len = 0usize;

            while rr.load(Ordering::Relaxed) {
                std::thread::sleep(interval);
                if fade > 0.0 { fb.decay_all(1.0 - fade); }

                let data = rd.lock().unwrap();
                if !data.new_since_render && fade == 0.0 { drop(data); continue; }

                let ordered: Vec<String> = data.field_order.iter()
                    .filter(|n| data.numeric.contains_key(*n)).cloned().collect();
                if ordered.is_empty() { drop(data); continue; }

                if fade == 0.0 { fb.clear(); last_len = 0; }

                match mode.as_str() {
                    "parametric" | "xy" if ordered.len() >= 2 =>
                        plot_xy(&mut fb, &data.numeric[&ordered[0]], &data.numeric[&ordered[1]], last_len, 0),
                    "polar" if ordered.len() >= 2 =>
                        plot_polar(&mut fb, &data.numeric[&ordered[0]], &data.numeric[&ordered[1]], last_len, 0),
                    _ => {
                        // Build lane groups: explicit lanes or auto (one per field)
                        let lane_groups: Vec<Vec<&str>> = if lanes.is_empty() {
                            ordered.iter().map(|n| vec![n.as_str()]).collect()
                        } else {
                            lanes.iter().map(|lane| {
                                lane.iter()
                                    .filter(|n| data.numeric.contains_key(*n))
                                    .map(|n| n.as_str())
                                    .collect()
                            }).filter(|g: &Vec<&str>| !g.is_empty()).collect()
                        };
                        let bh = (fb.height / lane_groups.len().max(1)).max(3);
                        for (li, group) in lane_groups.iter().enumerate() {
                            for (fi, &name) in group.iter().enumerate() {
                                if let Some(vals) = data.numeric.get(name) {
                                    plot_line(&mut fb, vals, li * bh, bh, fi, last_len);
                                }
                            }
                        }
                    }
                }
                if let Some(f) = ordered.first() { last_len = data.numeric[f].len(); }
                drop(data);
                fb.flush(use_color, 1);
            }

            // Final frame: full redraw
            fb.clear();
            let data = rd.lock().unwrap();
            let ordered: Vec<String> = data.field_order.iter()
                .filter(|n| data.numeric.contains_key(*n)).cloned().collect();
            if !ordered.is_empty() {
                match mode.as_str() {
                    "parametric" | "xy" if ordered.len() >= 2 =>
                        plot_xy(&mut fb, &data.numeric[&ordered[0]], &data.numeric[&ordered[1]], 0, 0),
                    "polar" if ordered.len() >= 2 =>
                        plot_polar(&mut fb, &data.numeric[&ordered[0]], &data.numeric[&ordered[1]], 0, 0),
                    _ => {
                        let bh = (fb.height / ordered.len().max(1)).max(3);
                        for (fi, name) in ordered.iter().enumerate() {
                            plot_line(&mut fb, &data.numeric[name], fi * bh, bh, fi, 0);
                        }
                    }
                }
            }
            drop(data);
            fb.flush(use_color, 1);

            // Leave alt screen and print final image to normal buffer
            if use_color {
                print!("\x1b[?25h");   // show cursor
                print!("\x1b[?1049l"); // leave alt screen
            }
            use std::io::Write;
            let _ = std::io::stdout().flush();

            // Print the final frame to normal scrollback so it persists
            let reset = if use_color { "\x1b[0m" } else { "" };
            for y in 0..fb.height {
                let mut line = String::new();
                for c in &fb.cells[y] {
                    if use_color && c.bright > 0.01 { line.push_str(&c.color_esc()); }
                    line.push(c.to_char());
                    if use_color { line.push_str(reset); }
                }
                println!("{line}");
            }
        });

        Self { data, config, running, render_thread: Some(render_thread) }
    }
}

impl DriverAdapter for PlotterAdapter {
    fn name(&self) -> &str { "plotter" }
    fn map_op(&self, _t: &ParsedOp) -> Result<Box<dyn OpDispenser>, String> {
        Ok(Box::new(PlotterDispenser { data: self.data.clone() }))
    }
    fn display_preference(&self) -> nb_activity::adapter::DisplayPreference {
        nb_activity::adapter::DisplayPreference::Off
    }
}

impl Drop for PlotterAdapter {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
        if let Some(h) = self.render_thread.take() { let _ = h.join(); }
    }
}

struct PlotterDispenser { data: Arc<Mutex<PlotData>> }

impl OpDispenser for PlotterDispenser {
    fn execute<'a>(&'a self, _cycle: u64, fields: &'a ResolvedFields)
        -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>>
    {
        Box::pin(async move {
            self.data.lock().unwrap().record(fields);
            Ok(OpResult { body: None, captures: HashMap::new(), skipped: false })
        })
    }
}

// ─── Plot helpers ──────────────────────────────────────────────

fn plot_xy(fb: &mut FrameBuffer, xv: &[f64], yv: &[f64], from: usize, ci: usize) {
    let n = xv.len().min(yv.len());
    if n == 0 { return; }
    let (xmin, xmax) = minmax(&xv[..n]);
    let (ymin, ymax) = minmax(&yv[..n]);
    let xr = safe_range(xmin, xmax);
    let yr = safe_range(ymin, ymax);
    let pw = fb.width * 2;
    let ph = fb.height * 4;
    for i in from..n {
        let px = ((xv[i] - xmin) / xr * (pw - 1) as f64) as usize;
        let py = ((yv[i] - ymin) / yr * (ph - 1) as f64) as usize;
        fb.set_dot_idx(px.min(pw-1), (ph-1).saturating_sub(py), ci);
    }
}

fn plot_polar(fb: &mut FrameBuffer, rv: &[f64], tv: &[f64], from: usize, ci: usize) {
    let n = rv.len().min(tv.len());
    if n == 0 { return; }
    // Convert polar to cartesian, centered in the framebuffer
    let rmax = rv[..n].iter().cloned().fold(0.0f64, f64::max).max(0.001);
    let pw = fb.width * 2;
    let ph = fb.height * 4;
    let cx = pw / 2;
    let cy = ph / 2;
    let scale = cx.min(cy) as f64;
    for i in from..n {
        let r_norm = rv[i] / rmax;
        let x = cx as f64 + r_norm * tv[i].cos() * scale;
        let y = cy as f64 - r_norm * tv[i].sin() * scale;
        let px = (x as usize).min(pw - 1);
        let py = (y as usize).min(ph - 1);
        fb.set_dot_idx(px, py, ci);
    }
}

fn plot_line(fb: &mut FrameBuffer, vals: &[f64], y_off: usize, bh: usize, ci: usize, from: usize) {
    if vals.is_empty() { return; }
    let (mn, mx) = minmax(vals);
    let range = safe_range(mn, mx);
    let pw = fb.width * 2;
    let ph = bh * 4;
    let n = vals.len();
    for i in from..n {
        let px = (i as f64 / n as f64 * (pw-1) as f64) as usize;
        let py = ((vals[i] - mn) / range * (ph-1) as f64) as usize;
        fb.set_dot_idx(px.min(pw-1), y_off * 4 + (ph-1).saturating_sub(py), ci);
    }
}

fn minmax(v: &[f64]) -> (f64, f64) {
    let mn = v.iter().cloned().fold(f64::MAX, f64::min);
    let mx = v.iter().cloned().fold(f64::MIN, f64::max);
    (mn, mx)
}

fn safe_range(mn: f64, mx: f64) -> f64 {
    if (mx - mn).abs() < 1e-10 { 1.0 } else { mx - mn }
}

#[allow(dead_code)]
fn truecolor_fg(idx: usize) -> String {
    let (r, g, b) = PALETTE[idx % PALETTE.len()];
    format!("\x1b[38;2;{r};{g};{b}m")
}

fn terminal_width() -> Option<usize> {
    let mut ws = libc::winsize { ws_row: 0, ws_col: 0, ws_xpixel: 0, ws_ypixel: 0 };
    if unsafe { libc::ioctl(1, libc::TIOCGWINSZ, &mut ws) } == 0 && ws.ws_col > 0 {
        Some(ws.ws_col as usize)
    } else { None }
}

fn terminal_height() -> Option<usize> {
    let mut ws = libc::winsize { ws_row: 0, ws_col: 0, ws_xpixel: 0, ws_ypixel: 0 };
    if unsafe { libc::ioctl(1, libc::TIOCGWINSZ, &mut ws) } == 0 && ws.ws_row > 0 {
        Some(ws.ws_row as usize)
    } else { None }
}

fn atty_stdout() -> bool { unsafe { libc::isatty(1) != 0 } }

// =========================================================================
// Adapter Registration (inventory-based, link-time)
// =========================================================================

inventory::submit! {
    nb_activity::adapter::AdapterRegistration {
        names: || &["plotter", "plot"],
        known_params: || &["mode", "fade", "lanes"],
        display_preference: || nb_activity::adapter::DisplayPreference::Off,
        create: |params| Box::pin(async move {
            let mode = params.get("mode").cloned().unwrap_or_else(|| "plot".into());
            let fade = params.get("fade")
                .and_then(|s| s.parse::<f32>().ok())
                .unwrap_or(0.0);
            let lanes = params.get("lanes")
                .map(|s| s.split(';')
                    .map(|lane| lane.split(',').map(|f| f.trim().to_string()).collect())
                    .collect())
                .unwrap_or_default();
            Ok(std::sync::Arc::new(PlotterAdapter::with_config(PlotterConfig {
                mode,
                fade,
                lanes,
                ..Default::default()
            })) as std::sync::Arc<dyn nb_activity::adapter::DriverAdapter>)
        }),
    }
}
