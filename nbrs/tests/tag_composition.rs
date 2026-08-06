// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! SRD-108 Part A e2e — tag-contract composition (the ad-hoc
//! form, completing SRD-20's documented phase tag selectors).
//!
//! A logical scaffold declares selector-only phases; an
//! implementation workload `extends:` it and contributes tagged
//! block ops. The phase runs the implementation's ops under the
//! scaffold's cycles/concurrency.
//!
//! Sandbox discipline per `feedback_tests_no_project_root`.

use std::path::PathBuf;
use std::process::Command;

const SCAFFOLD: &str = r#"
params:
  probe_conc: "2"
phases:
  measure:
    cycles: 4
    concurrency: "{probe_conc}"
    tags: "role:search"
"#;

const IMPL_A: &str = r#"
extends: ./scaffold.yaml
blocks:
  proto_a:
    ops:
      probe:
        tags:
          role: search
        stmt: "PROTO_A_TICK"
"#;

struct Sandbox {
    dir: PathBuf,
}

impl Sandbox {
    fn new(tag: &str) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir()
            .join(format!("nbrs-tagcomp-{tag}-{}-{nanos}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create sandbox");
        std::fs::write(dir.join("scaffold.yaml"), SCAFFOLD).expect("write scaffold");
        std::fs::write(dir.join("impl_a.yaml"), IMPL_A).expect("write impl");
        Self { dir }
    }

    fn run(&self, workload: &str) -> (String, String, bool) {
        let out = Command::new(env!("CARGO_BIN_EXE_nbrs"))
            .current_dir(&self.dir)
            .arg("run")
            .arg(format!("workload={workload}"))
            .arg("tui=off")
            .arg("--session-path")
            .arg(self.dir.join("session"))
            .output()
            .expect("run nbrs");
        (
            String::from_utf8_lossy(&out.stdout).to_string(),
            String::from_utf8_lossy(&out.stderr).to_string(),
            out.status.success(),
        )
    }
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// The extends-composed pair: the scaffold's extent (4 cycles)
/// drives the implementation's tagged op.
#[test]
fn extends_contributed_block_ops_bind_via_phase_selector() {
    let sandbox = Sandbox::new("pair");
    let (stdout, stderr, ok) = sandbox.run("impl_a.yaml");
    assert!(ok, "composed run must complete; stderr:\n{stderr}");
    let ticks = stdout.lines().filter(|l| l.trim() == "PROTO_A_TICK").count();
    assert_eq!(ticks, 4, "scaffold cycles × impl op; stdout:\n{stdout}");
}

/// The scaffold alone fails at LOAD — its selector matches
/// nothing without a contributing implementation.
#[test]
fn scaffold_alone_fails_at_load_with_a_named_error() {
    let sandbox = Sandbox::new("alone");
    let (_, stderr, ok) = sandbox.run("scaffold.yaml");
    assert!(!ok, "a selector-only scaffold must not run bare");
    assert!(stderr.contains("measure") && stderr.contains("matched no ops"),
        "the error names the phase and the empty match; stderr:\n{stderr}");
}
