// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! # nb-workload
//!
//! Uniform workload specification parsing and processing.
//! Handles YAML workload definitions: op templates, bindings,
//! blocks, scenarios, tags, and parameter inheritance.

pub mod model;
pub mod template;
pub mod parse;
pub mod inline;
pub mod bindpoints;
pub mod tags;
pub mod spectest;
