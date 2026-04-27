// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Unix daemonization and web instance discovery for `nbrs web`.
//!
//! When `nbrs web` starts, it writes a `.nbrs-web.json` anchor file
//! in the working directory recording the host, port, and PID.
//! When `nbrs run` starts from the same directory, it discovers the
//! anchor and auto-configures metrics push.

use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::cli;

/// Entry point for `nbrs web` — daemon lifecycle, bind, and serve.
pub fn web_command(args: &[String]) {
    // Handle --stop: kill a running daemon
    if args.iter().any(|a| a == "--stop") {
        match stop_daemon() {
            Ok(()) => {}
            Err(e) => {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        }
        return;
    }

    // Handle --restart: stop the old daemon, re-launch with its saved args.
    // Falls back to process-table scan if no anchor file exists.
    // If nothing is found at all, starts fresh.
    if args.iter().any(|a| a == "--restart") {
        if let Some(anchor) = read_anchor() {
            // Anchor file exists — use it for a clean restart.
            let _ = stop_daemon();
            if !anchor.args.is_empty() {
                let exe = std::env::current_exe().unwrap_or_else(|_| "nbrs".into());
                eprintln!("nbrs web: restarting with: {} {}", exe.display(),
                    anchor.args.join(" "));
                let status = std::process::Command::new(&exe)
                    .args(&anchor.args)
                    .status()
                    .unwrap_or_else(|e| {
                        eprintln!("error: failed to restart: {e}");
                        std::process::exit(1);
                    });
                std::process::exit(status.code().unwrap_or(1));
            }
            eprintln!("nbrs web: anchor has no saved args, starting with defaults");
        } else {
            // No anchor — scan the process table for orphaned nbrs web processes.
            let procs = find_nbrs_web_processes();
            if procs.is_empty() {
                eprintln!("nbrs web: no running instance found, starting fresh");
            } else {
                eprintln!("nbrs web: no anchor file, but found {} running nbrs web process(es):",
                    procs.len());
                for p in &procs {
                    eprintln!("  pid {} — {}", p.pid, p.cmdline);
                }
                if confirm_prompt("Kill these and start fresh?") {
                    for p in &procs {
                        match kill_pid(p.pid) {
                            Ok(()) => eprintln!("  stopped pid {}", p.pid),
                            Err(e) => eprintln!("  warning: {e}"),
                        }
                    }
                    // Clean up any leftover PID/anchor files.
                    let _ = fs::remove_file(pid_file_path());
                    remove_anchor();
                } else {
                    eprintln!("nbrs web: aborted");
                    return;
                }
            }
        }
    }

    // Warn about unrecognized --flags.
    let known_flags = ["--daemon", "--stop", "--restart"];
    for a in args.iter().filter(|a| a.starts_with("--")) {
        let key = a.split('=').next().unwrap_or(a);
        if !known_flags.contains(&key) && key != "--bind" && key != "--port" {
            eprintln!("warning: unrecognized option '{a}' (known: --daemon, --stop, --restart, --bind=, --port=)");
        }
    }

    let bind_raw = args.iter()
        .find_map(|a| a.strip_prefix("bind=").or_else(|| a.strip_prefix("--bind=")))
        .unwrap_or("0.0.0.0");
    let port_raw = args.iter()
        .find_map(|a| a.strip_prefix("port=").or_else(|| a.strip_prefix("--port=")));

    // Parse bind flexibly: accept bare IP, host:port, or full URL
    let (bind, port) = cli::parse_bind_address(bind_raw, port_raw);
    let addr: SocketAddr = format!("{bind}:{port}").parse()
        .unwrap_or_else(|e| { eprintln!("error: invalid bind address '{bind}:{port}': {e}"); std::process::exit(1); });

    // Clean up stale anchor if the recorded PID is dead.
    cleanup_stale_anchor();

    // Check if the port is already in use before attempting to bind.
    if let Err(msg) = check_port_available(&addr) {
        eprintln!("error: {msg}");
        std::process::exit(1);
    }

    // Handle --daemon: fork to background
    if args.iter().any(|a| a == "--daemon") {
        eprintln!("nbrs web: daemonizing on {addr}...");
        daemonize().unwrap_or_else(|e| {
            eprintln!("error: failed to daemonize: {e}");
            std::process::exit(1);
        });
    }

    // Write anchor file so `nbrs run` in this directory auto-discovers us.
    // Save the full "web ..." args (excluding --restart) for --restart.
    let saved_args: Vec<String> = std::env::args().skip(1)
        .filter(|a| a != "--restart")
        .collect();
    write_anchor(&addr, &saved_args);

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let broadcast = nb_web::ws::MetricsBroadcast::new(16);
        if let Err(e) = nb_web::server::serve_with(addr, broadcast).await {
            eprintln!("error: web server failed: {e}");
        }
    });

    // Clean up on exit.
    let _ = fs::remove_file(pid_file_path());
    remove_anchor();
}

/// Name of the anchor file written to the working directory.
const ANCHOR_FILE: &str = ".nbrs-web.json";

/// Anchor describing a running `nbrs web` instance.
#[derive(Debug, Serialize, Deserialize)]
pub struct WebAnchor {
    pub host: String,
    pub port: u16,
    pub pid: u32,
    /// Original CLI args used to start the daemon, for `--restart`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
}

impl WebAnchor {
    /// The OpenMetrics push URL for this instance.
    #[allow(dead_code)]
    pub fn push_url(&self) -> String {
        format!("http://{}:{}/api/v1/import/prometheus", self.host, self.port)
    }
}

/// Path to the PID file for the daemon (used by `--stop`).
pub fn pid_file_path() -> PathBuf {
    std::env::var("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/tmp"))
        .join("nbrs-web.pid")
}

/// Path to the anchor file in the current working directory.
fn anchor_path() -> PathBuf {
    PathBuf::from(ANCHOR_FILE)
}

/// Write the anchor file for a running web instance.
pub fn write_anchor(addr: &SocketAddr, args: &[String]) {
    // When bound to 0.0.0.0 or ::, use localhost for the push URL.
    let host = match addr.ip() {
        std::net::IpAddr::V4(ip) if ip.is_unspecified() => "127.0.0.1".to_string(),
        std::net::IpAddr::V6(ip) if ip.is_unspecified() => "::1".to_string(),
        other => other.to_string(),
    };
    let anchor = WebAnchor {
        host,
        port: addr.port(),
        pid: std::process::id(),
        args: args.to_vec(),
    };
    if let Ok(json) = serde_json::to_string_pretty(&anchor)
        && let Err(e) = fs::write(anchor_path(), &json) {
            eprintln!("warning: failed to write daemon anchor file: {e}");
        }
}

/// Remove the anchor file on shutdown.
pub fn remove_anchor() {
    let _ = fs::remove_file(anchor_path());
}

/// Read the anchor file, if it exists.
pub fn read_anchor() -> Option<WebAnchor> {
    let content = fs::read_to_string(anchor_path()).ok()?;
    serde_json::from_str(&content).ok()
}

/// Discover a running `nbrs web` instance from the anchor file.
///
/// Returns `Some(url)` if the anchor exists and the PID is alive.
/// Cleans up stale anchors automatically.
#[allow(dead_code)]
pub fn discover_web_instance() -> Option<String> {
    let path = anchor_path();
    let content = fs::read_to_string(&path).ok()?;
    let anchor: WebAnchor = serde_json::from_str(&content).ok()?;

    // Verify the process is still alive.
    let alive = unsafe { libc::kill(anchor.pid as i32, 0) } == 0;
    if !alive {
        let _ = fs::remove_file(&path);
        return None;
    }

    Some(anchor.push_url())
}

/// Daemonize the current process via double-fork.
///
/// After this call returns `Ok(())`, the process is fully detached
/// from the terminal and running as a background daemon. The PID
/// file has been written.
pub fn daemonize() -> Result<(), String> {
    // First fork — parent exits, child continues.
    match unsafe { libc::fork() } {
        -1 => return Err("first fork failed".into()),
        0 => {}
        _ => std::process::exit(0),
    }

    // Create new session (detach from terminal).
    if unsafe { libc::setsid() } == -1 {
        return Err("setsid failed".into());
    }

    // Second fork — session leader exits, grandchild continues.
    match unsafe { libc::fork() } {
        -1 => return Err("second fork failed".into()),
        0 => {}
        _ => std::process::exit(0),
    }

    // Write PID file.
    let pid = std::process::id();
    let path = pid_file_path();
    fs::write(&path, pid.to_string())
        .map_err(|e| format!("failed to write PID file {}: {e}", path.display()))?;

    // Redirect stdin/stdout/stderr to /dev/null.
    unsafe {
        let devnull = libc::open(b"/dev/null\0".as_ptr() as *const _, libc::O_RDWR);
        if devnull >= 0 {
            libc::dup2(devnull, libc::STDIN_FILENO);
            libc::dup2(devnull, libc::STDOUT_FILENO);
            libc::dup2(devnull, libc::STDERR_FILENO);
            if devnull > 2 {
                libc::close(devnull);
            }
        }
    }

    Ok(())
}

/// Remove the anchor file if the recorded PID is no longer alive.
///
/// Called on startup so that a stale anchor from a crashed daemon
/// doesn't confuse port-in-use diagnostics.
pub fn cleanup_stale_anchor() {
    if let Some(anchor) = read_anchor() {
        let alive = unsafe { libc::kill(anchor.pid as i32, 0) } == 0;
        if !alive {
            eprintln!("nbrs web: cleaning up stale anchor (pid {} no longer running)", anchor.pid);
            remove_anchor();
        }
    }
}

/// Check whether the target address/port is available for binding.
///
/// Returns `Ok(())` if the port is free, or `Err(message)` with
/// actionable diagnostics (including the owning PID from the anchor
/// file, if available).
pub fn check_port_available(addr: &SocketAddr) -> Result<(), String> {
    match std::net::TcpListener::bind(addr) {
        Ok(_listener) => Ok(()), // drops immediately, freeing the port
        Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => {
            let mut msg = format!("port {} is already in use on {}", addr.port(), addr.ip());
            if let Some(anchor) = read_anchor() {
                let alive = unsafe { libc::kill(anchor.pid as i32, 0) } == 0;
                if alive {
                    msg.push_str(&format!(
                        "\n  → an nbrs web instance is running (pid {})\n  → use 'nbrs web --stop' to stop it, or 'nbrs web --restart' to restart",
                        anchor.pid
                    ));
                } else {
                    msg.push_str("\n  → stale anchor file found (process dead) — another program may hold the port");
                    remove_anchor();
                }
            } else {
                // No anchor — check process table for nbrs web processes.
                let procs = find_nbrs_web_processes();
                if procs.is_empty() {
                    // Try to identify what's holding the port via ss/lsof
                    let port = addr.port();
                    let holder = identify_port_holder(port);
                    if let Some(info) = holder {
                        msg.push_str(&format!("\n  → held by: {info}"));
                    } else {
                        msg.push_str("\n  → another program is using this port");
                    }
                    msg.push_str(&"\n  → try a different port with port=<N>".to_string());
                } else {
                    for p in &procs {
                        msg.push_str(&format!("\n  → found nbrs web process: pid {} — {}", p.pid, p.cmdline));
                    }
                    msg.push_str("\n  → use 'nbrs web --restart' to kill and restart");
                }
            }
            Err(msg)
        }
        Err(e) => Err(format!("cannot bind to {addr}: {e}")),
    }
}

/// Information about a running `nbrs web` process found via /proc scan.
pub struct NbrsWebProcess {
    pub pid: u32,
    pub cmdline: String,
}

/// Scan the process table for `nbrs` processes whose command line
/// contains "web", excluding the current process.
///
/// Uses `/proc/*/cmdline` on Linux. Returns an empty vec on
/// non-Linux or if `/proc` is unavailable.
pub fn find_nbrs_web_processes() -> Vec<NbrsWebProcess> {
    let my_pid = std::process::id();
    let mut results = Vec::new();

    let Ok(entries) = fs::read_dir("/proc") else {
        return results;
    };

    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(pid_str) = name.to_str() else { continue };
        let Ok(pid) = pid_str.parse::<u32>() else { continue };
        if pid == my_pid { continue; }

        let cmdline_path = entry.path().join("cmdline");
        let Ok(raw) = fs::read(&cmdline_path) else { continue };

        // /proc/*/cmdline uses NUL separators between args.
        let cmdline: String = raw.iter()
            .map(|&b| if b == 0 { ' ' } else { b as char })
            .collect::<String>()
            .trim()
            .to_string();

        // Match processes that look like "nbrs web ..."
        if cmdline.contains("nbrs") && cmdline.contains("web") {
            results.push(NbrsWebProcess { pid, cmdline });
        }
    }

    results
}

/// Try to identify what process holds a given TCP port.
///
/// Uses `ss -tlnp` on Linux. Returns a human-readable description
/// like "pid 1234 (nginx)" or None if it can't determine.
fn identify_port_holder(port: u16) -> Option<String> {
    let output = std::process::Command::new("ss")
        .args(["-tlnp", &format!("sport = :{port}")])
        .output()
        .ok()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    // Parse ss output: look for lines containing the port
    for line in stdout.lines().skip(1) {
        if line.contains(&format!(":{port}")) {
            // Extract the process info from the "users:" field
            // Format: users:(("program",pid=1234,fd=5))
            if let Some(users_start) = line.find("users:((") {
                let rest = &line[users_start + 8..];
                if let Some(end) = rest.find("))") {
                    return Some(rest[..end].to_string());
                }
            }
            // If no users field, return the whole line trimmed
            return Some(line.trim().to_string());
        }
    }
    None
}

/// Prompt the user on stderr/stdin with a yes/no question.
///
/// Returns `true` if the user answers `y` or `yes` (case-insensitive).
/// Returns `false` on `n`, `no`, EOF, or any other input.
pub fn confirm_prompt(message: &str) -> bool {
    eprint!("{message} [y/N] ");
    let mut input = String::new();
    if std::io::stdin().read_line(&mut input).is_err() {
        return false;
    }
    matches!(input.trim().to_lowercase().as_str(), "y" | "yes")
}

/// Send SIGTERM to a process by PID and wait briefly for it to exit.
pub fn kill_pid(pid: u32) -> Result<(), String> {
    let result = unsafe { libc::kill(pid as i32, libc::SIGTERM) };
    if result == -1 {
        return Err(format!("failed to signal pid {pid}"));
    }
    // Wait briefly for the process to exit.
    std::thread::sleep(std::time::Duration::from_millis(500));
    // Verify it actually exited.
    let still_alive = unsafe { libc::kill(pid as i32, 0) } == 0;
    if still_alive {
        eprintln!("nbrs web: pid {pid} still alive after SIGTERM, sending SIGKILL");
        unsafe { libc::kill(pid as i32, libc::SIGKILL); }
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
    Ok(())
}

/// Stop a running daemon by reading the PID file and sending SIGTERM.
pub fn stop_daemon() -> Result<(), String> {
    let path = pid_file_path();
    let pid_str = fs::read_to_string(&path)
        .map_err(|_| format!("no running daemon found (no PID file at {})", path.display()))?;
    let pid: i32 = pid_str.trim().parse()
        .map_err(|_| "invalid PID file contents".to_string())?;

    // Verify the process exists and is an nbrs process.
    let cmdline_path = format!("/proc/{pid}/cmdline");
    if let Ok(cmdline) = fs::read_to_string(&cmdline_path)
        && !cmdline.contains("nbrs") {
            let _ = fs::remove_file(&path);
            return Err(format!("PID {pid} is not an nbrs process — stale PID file removed"));
        }

    let result = unsafe { libc::kill(pid, libc::SIGTERM) };
    if result == -1 {
        let _ = fs::remove_file(&path);
        return Err(format!("failed to signal PID {pid} — stale PID file removed"));
    }

    // Wait briefly for the process to exit.
    std::thread::sleep(std::time::Duration::from_millis(500));
    let _ = fs::remove_file(&path);
    // Also clean up the anchor file.
    remove_anchor();
    eprintln!("nbrs web: stopped daemon (pid {pid})");
    Ok(())
}
