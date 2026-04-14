// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! # nb-activity
//!
//! Activity execution engine: async dispatch loop, adapter trait,
//! cycle source, op sequencing, and integration of all nb-rs crates.

pub mod cycle;
pub mod adapter;
pub mod opseq;
pub mod activity;
pub mod adapters;
pub mod synthesis;
pub mod bindings;
pub mod runner;
pub mod executor;
pub mod describe;
pub mod wrappers;
pub mod relevancy;
pub mod validation;
pub mod linearize;
