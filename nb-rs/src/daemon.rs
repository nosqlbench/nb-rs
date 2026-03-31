// Copyright 2024-2026 nosqlbench contributors
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
    if let Ok(json) = serde_json::to_string_pretty(&anchor) {
        let _ = fs::write(anchor_path(), json);
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

/// Stop a running daemon by reading the PID file and sending SIGTERM.
pub fn stop_daemon() -> Result<(), String> {
    let path = pid_file_path();
    let pid_str = fs::read_to_string(&path)
        .map_err(|_| format!("no running daemon found (no PID file at {})", path.display()))?;
    let pid: i32 = pid_str.trim().parse()
        .map_err(|_| "invalid PID file contents".to_string())?;

    // Verify the process exists and is an nbrs process.
    let cmdline_path = format!("/proc/{pid}/cmdline");
    if let Ok(cmdline) = fs::read_to_string(&cmdline_path) {
        if !cmdline.contains("nbrs") {
            let _ = fs::remove_file(&path);
            return Err(format!("PID {pid} is not an nbrs process — stale PID file removed"));
        }
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
