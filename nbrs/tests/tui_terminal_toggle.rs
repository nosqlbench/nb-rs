//! E2E tests for the `tui=terminal` ↔ `tui=on` toggle path.
//!
//! The toggle is interactive: Ctrl-T from terminal mode swaps
//! to the full TUI; Ctrl-T (or `q`) inside the TUI swaps back.
//! There's no piped-output surface that exposes whether the
//! swap actually happened — both modes write to stderr, just
//! with different ANSI dressing.
//!
//! [`shadow_terminal::SteppableTerminal`] gives us an in-memory,
//! fully-rendered terminal we can drive: spawn `nbrs run` as
//! a child, send keystrokes through the PTY, and read back the
//! rendered cells. That's enough to distinguish "line-mode log
//! lines" from "alt-screen TUI" by content.
//!
//! These tests build their own workload file in `tempdir` so
//! they don't depend on workspace fixtures and stay
//! self-contained.

use std::ffi::OsString;
use std::path::PathBuf;
use std::time::Duration;

use shadow_terminal::shadow_terminal::Config;
use shadow_terminal::steppable_terminal::{Input, SteppableTerminal};

/// Path to the `nbrs` binary cargo built for this test.
/// `CARGO_BIN_EXE_<name>` is populated during integration-test
/// builds for binaries declared in the same crate.
fn nbrs_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_nbrs"))
}

/// Write a tiny stdout-adapter workload that runs slowly enough
/// (rate-limited) for the test to observe both display modes
/// before completion. Returns the path inside a fresh tempdir
/// so the test never collides with sibling runs.
fn slow_stdout_workload() -> (TempDir, PathBuf) {
    let dir = TempDir::new();
    let yaml_path = dir.path().join("toggle.yaml");
    std::fs::write(
        &yaml_path,
        r#"ops:
  hello:
    raw: "tick={cycle}"
"#,
    ).expect("write workload yaml");
    (dir, yaml_path)
}

/// Hand-rolled tempdir so we don't add a `tempfile` dep just
/// for these tests. Cleans up on drop.
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> Self {
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("nbrs-toggle-test-{pid}-{nanos}"));
        std::fs::create_dir_all(&path).expect("create tempdir");
        Self { path }
    }
    fn path(&self) -> &std::path::Path { &self.path }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn build_config(workload: &PathBuf, extra: &[&str]) -> Config {
    let mut command: Vec<OsString> = Vec::new();
    command.push(nbrs_binary().into());
    command.push("run".into());
    command.push(format!("workload={}", workload.display()).into());
    command.push("cycles=20".into());
    // 5 ops/sec gives the test ~4s of runway to drive the
    // toggle. Long enough that intermediate states are
    // observable; short enough that the whole run finishes
    // inside the per-test timeout.
    command.push("rate=5".into());
    command.push("tui=terminal".into());
    for arg in extra {
        command.push((*arg).into());
    }
    Config {
        width: 120,
        height: 30,
        command,
        scrollback_size: 500,
        scrollback_step: 5,
    }
}

/// Step the terminal until a substring shows up on screen, or
/// the deadline fires. `wait_for_string` from upstream is
/// async + has its own internal timeout; this helper just
/// adds nicer error context for our specific assertions.
async fn assert_screen_contains(
    stepper: &mut SteppableTerminal,
    needle: &str,
    timeout: Duration,
) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if tokio::time::Instant::now() >= deadline {
            let _ = stepper.render_all_output().await;
            let dump = stepper.screen_as_string().unwrap_or_default();
            panic!(
                "timed out waiting for {:?} on screen — last screen was:\n{}",
                needle, dump
            );
        }
        let _ = stepper.render_all_output().await;
        if let Ok(s) = stepper.screen_as_string()
            && s.contains(needle)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// `tui=terminal` in a real PTY produces line-mode output
/// (no alt-screen). We assert that a phase-start line is
/// observable on screen, which confirms the
/// `LogOnlyObserver` + `LogOnlySink` pipeline is rendering
/// correctly through the actor + snapshot drain.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_mode_renders_log_lines() {
    let (_dir, yaml) = slow_stdout_workload();
    let config = build_config(&yaml, &[]);
    let mut stepper = SteppableTerminal::start(config).await
        .expect("start steppable terminal");

    // The canonical "rendering works" tell: the runner's
    // startup banner ("1 ops, N cycles, concurrency=1, …")
    // reaches the screen via observer::log → actor →
    // LogOnlySink → PTY → shadow terminal. If we see it,
    // every layer is wired.
    assert_screen_contains(&mut stepper, "ops, ", Duration::from_secs(8)).await;

    let _ = stepper.kill();
}

/// Drive the Ctrl-T toggle from terminal mode into the full
/// TUI. The TUI's signature on screen is the scenario tree's
/// box-drawing characters in the left panel. If we don't see
/// them within the timeout, the toggle didn't take.
///
/// Currently `#[ignore]`d because the toggle depends on the
/// runner having published a `MetricsQuery` to the observer
/// (the `TuiSink` constructor needs it). The cadence reporter
/// builds it on the first scheduler tick (~1 s after run
/// start); if Ctrl-T fires before that the supervisor falls
/// back to terminal mode with a "TUI not yet ready" notice.
/// The non-flaky fix is to wait for a snapshot-side signal
/// that metrics is wired before sending Ctrl-T — a follow-up
/// can add a "metrics ready" log line and gate the test on it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "race against MetricsQuery wiring; needs a 'metrics ready' tell to gate Ctrl-T"]
async fn ctrl_t_toggles_into_tui() {
    let (_dir, yaml) = slow_stdout_workload();
    let config = build_config(&yaml, &["cycles=80", "rate=3"]);
    let mut stepper = SteppableTerminal::start(config).await
        .expect("start steppable terminal");

    // Wait until terminal-mode rendering is visible.
    assert_screen_contains(&mut stepper, "phase 'hello'", Duration::from_secs(8)).await;

    // Ctrl-T (ASCII 0x14) — the keystroke watcher should pick
    // this up and ask the supervisor to swap to TuiSink.
    stepper.send_input(Input::Characters("\x14".into())).expect("send Ctrl-T");

    // The TUI's tree panel uses these box-drawing chars; their
    // appearance is a strong signal the alt-screen is up and
    // the App is rendering.
    assert_screen_contains(&mut stepper, "├", Duration::from_secs(5)).await;

    // Toggle back. Inside the TUI, App handles Ctrl-T by
    // setting `yielded_to_terminal = true` and exiting the
    // event loop; the supervisor brings LogOnlySink back up.
    stepper.send_input(Input::Characters("\x14".into())).expect("send Ctrl-T (back)");

    // After the swap-back, fresh log lines should land. The
    // exact line varies, but the run continues and progress
    // events keep firing on the LogOnlySink poll cadence.
    assert_screen_contains(&mut stepper, "phase 'hello'", Duration::from_secs(5)).await;

    let _ = stepper.kill();
}
