// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Assembles the root [`Command`] spec by pulling each
//! subcommand's `spec()` from its owning module. main.rs
//! calls [`root`] once at startup; the same value drives the
//! parser, completion, and help.

use super::*;

pub fn root() -> Command {
    Command {
        name: "nbrs",
        help: "nbrs — the nb-rs command-line tool.\n\
               \n\
               Run a workload, attach to a running session,\n\
               render reports, query metrics, etc.",
        category: Category::Tools,
        level: Level::Workload,
        flags: Vec::new(),
        positionals: Vec::new(),
        handler: None,
        raw_args: false,
        completion_override: None,
        subcommands: vec![
            crate::run::spec(),
            crate::inspector::spec(),
            crate::report_cmd::spec(),
            crate::report_cmd::plot_alias_spec(),
            crate::report_cmd::table_alias_spec(),
            crate::metrics_cmd::spec(),
            crate::describe::spec(),
            crate::bench::spec(),
            crate::replay::spec(),
            crate::checkpoint_cmd::spec(),
            crate::daemon::spec(),
            crate::plot::spec(),
            crate::completion::spec(),
            #[cfg(feature = "openapi")]
            crate::openapi::describe_spec(),
            #[cfg(feature = "openapi")]
            crate::openapi::run_spec(),
        ],
    }
}
