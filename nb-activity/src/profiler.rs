// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Built-in CPU profiler for nb-rs workloads.
//!
//! Two modes:
//!
//! - **`profiler=flamegraph`** — Rust-native sampling via `pprof` crate.
//!   Fast, no external tools needed, but only captures Rust frames.
//!   Good for profiling GK resolution, scope composition, and other
//!   pure-Rust hot paths.
//!
//! - **`profiler=perf`** — Full-system profiling via Linux `perf record`.
//!   Captures all frames including C/C++ libraries (CQL driver, etc.).
//!   Requires `perf` and `inferno` tools installed. Produces an SVG
//!   flamegraph that shows the complete call graph.

use std::collections::HashMap;

/// A running profiler guard. Drop to stop profiling and write output.
pub struct ProfileGuard {
    mode: ProfileMode,
}

enum ProfileMode {
    #[cfg(feature = "flamegraph")]
    Pprof {
        guard: pprof::ProfilerGuard<'static>,
        output_path: String,
    },
    Perf {
        perf_data: String,
        output_path: String,
    },
}

impl ProfileGuard {
    /// Start profiling if `profiler=flamegraph` or `profiler=perf` is set.
    /// Returns `None` if profiling is not requested or not available.
    pub fn maybe_start(params: &HashMap<String, String>) -> Option<Self> {
        let mode = params.get("profiler")?;
        match mode.as_str() {
            "flamegraph" => Self::start_pprof(),
            "perf" => Self::start_perf(),
            other => {
                eprintln!("warning: unknown profiler mode '{other}', expected 'flamegraph' or 'perf'");
                None
            }
        }
    }

    fn start_pprof() -> Option<Self> {
        #[cfg(not(feature = "flamegraph"))]
        {
            eprintln!("warning: profiler=flamegraph requested but the 'flamegraph' feature is not enabled. \
                        Rebuild with: cargo build --features flamegraph");
            return None;
        }

        #[cfg(feature = "flamegraph")]
        {
            match pprof::ProfilerGuardBuilder::default()
                .frequency(997)
                .blocklist(&["libc", "libgcc", "pthread", "vdso"])
                .build()
            {
                Ok(guard) => {
                    let ts = timestamp();
                    let output_path = format!("flamegraph-{ts}.svg");
                    eprintln!("profiler: pprof started (997 Hz, Rust frames only), output → {output_path}");
                    Some(Self {
                        mode: ProfileMode::Pprof { guard, output_path },
                    })
                }
                Err(e) => {
                    eprintln!("warning: failed to start pprof profiler: {e}");
                    eprintln!("  (requires Linux perf_event_open; try: echo 1 > /proc/sys/kernel/perf_event_paranoid)");
                    None
                }
            }
        }
    }

    fn start_perf() -> Option<Self> {
        // Check that perf is available
        let perf_check = std::process::Command::new("perf")
            .arg("version")
            .output();
        if perf_check.is_err() || !perf_check.as_ref().unwrap().status.success() {
            eprintln!("warning: profiler=perf requested but `perf` is not available.");
            eprintln!("  Install with: sudo apt install linux-tools-$(uname -r)");
            return None;
        }

        let ts = timestamp();
        let perf_data = format!("perf-{ts}.data");
        let output_path = format!("flamegraph-perf-{ts}.svg");
        let pid = std::process::id();

        // Attach perf to our own process
        let result = std::process::Command::new("perf")
            .args(["record", "-g", "--call-graph", "dwarf,16384",
                   "-F", "997", "-p", &pid.to_string(),
                   "-o", &perf_data])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn();

        match result {
            Ok(_child) => {
                // Give perf a moment to attach
                std::thread::sleep(std::time::Duration::from_millis(100));
                eprintln!("profiler: perf record attached (997 Hz, all frames), output → {output_path}");
                Some(Self {
                    mode: ProfileMode::Perf { perf_data, output_path },
                })
            }
            Err(e) => {
                eprintln!("warning: failed to start perf record: {e}");
                None
            }
        }
    }

    /// Stop profiling and write the flamegraph SVG.
    pub fn finish(self) {
        match self.mode {
            #[cfg(feature = "flamegraph")]
            ProfileMode::Pprof { guard, output_path } => {
                match guard.report().build() {
                    Ok(report) => {
                        match std::fs::File::create(&output_path) {
                            Ok(file) => {
                                if let Err(e) = report.flamegraph(file) {
                                    eprintln!("profiler: failed to write flamegraph: {e}");
                                } else {
                                    eprintln!("profiler: wrote {output_path}");
                                }
                            }
                            Err(e) => eprintln!("profiler: failed to create {output_path}: {e}"),
                        }
                    }
                    Err(e) => eprintln!("profiler: failed to build report: {e}"),
                }
            }
            ProfileMode::Perf { perf_data, output_path } => {
                // Send SIGINT to perf to stop recording cleanly
                let _ = std::process::Command::new("pkill")
                    .args(["-INT", "-f", &format!("perf record.*{perf_data}")])
                    .output();
                // Give perf a moment to flush
                std::thread::sleep(std::time::Duration::from_millis(500));

                if !std::path::Path::new(&perf_data).exists() {
                    eprintln!("profiler: perf.data not found at {perf_data}");
                    return;
                }

                // Try inferno pipeline: perf script | collapse | flamegraph
                let has_inferno = std::process::Command::new("inferno-collapse-perf")
                    .arg("--help")
                    .output()
                    .map(|o| o.status.success())
                    .unwrap_or(false);

                if has_inferno {
                    eprintln!("profiler: post-processing {perf_data} with inferno...");
                    let script = std::process::Command::new("perf")
                        .args(["script", "-i", &perf_data])
                        .output();

                    if let Ok(script_output) = script {
                        let collapse = std::process::Command::new("inferno-collapse-perf")
                            .stdin(std::process::Stdio::piped())
                            .stdout(std::process::Stdio::piped())
                            .spawn();

                        if let Ok(mut collapse_proc) = collapse {
                            use std::io::Write;
                            if let Some(ref mut stdin) = collapse_proc.stdin {
                                let _ = stdin.write_all(&script_output.stdout);
                            }
                            if let Ok(collapsed) = collapse_proc.wait_with_output() {
                                let flamegraph = std::process::Command::new("inferno-flamegraph")
                                    .stdin(std::process::Stdio::piped())
                                    .stdout(std::process::Stdio::piped())
                                    .spawn();

                                if let Ok(mut fg_proc) = flamegraph {
                                    if let Some(ref mut stdin) = fg_proc.stdin {
                                        let _ = stdin.write_all(&collapsed.stdout);
                                    }
                                    if let Ok(svg_output) = fg_proc.wait_with_output() {
                                        if let Err(e) = std::fs::write(&output_path, &svg_output.stdout) {
                                            eprintln!("profiler: failed to write {output_path}: {e}");
                                        } else {
                                            let size_kb = svg_output.stdout.len() / 1024;
                                            eprintln!("profiler: wrote {output_path} ({size_kb} KB)");
                                        }
                                    }
                                }
                            }
                        }
                    }
                } else {
                    // No inferno — leave perf.data for manual processing
                    eprintln!("profiler: perf data saved to {perf_data}");
                    eprintln!("  To generate flamegraph manually:");
                    eprintln!("    perf script -i {perf_data} | inferno-collapse-perf | inferno-flamegraph > {output_path}");
                    eprintln!("  Install inferno: cargo install inferno");
                }

                // Clean up perf.data (it can be large)
                // Keep it if inferno wasn't available (user needs it)
                if has_inferno {
                    let _ = std::fs::remove_file(&perf_data);
                }
            }
        }
    }
}

fn timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
