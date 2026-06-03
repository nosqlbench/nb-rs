// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Support utilities used by Polydat function nodes.
//!
//! Both [`cache`] and [`audit`] are infrastructure that
//! library nodes consume — they're not part of the Polydat node
//! contract, just helpers the implementations reach for. Kept
//! co-located here so the library's internal dependency
//! footprint stays visible.

pub mod cache;
pub mod audit;
