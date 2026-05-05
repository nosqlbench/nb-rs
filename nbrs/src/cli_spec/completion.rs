// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Adapter that turns a [`Command`] tree into a
//! [`veks_completion::CommandTree`].
//!
//! Every flag declared in the spec automatically appears in
//! tab; every [`ValueProvider::Custom`] becomes a per-flag
//! value provider. There is no second list of names to keep
//! in sync — the spec is canonical.

use veks_completion::{CommandTree, Node, StrictNode, fn_provider};

use super::*;

/// Convert the root spec into a `CommandTree`. Subcommands at
/// the root are wired via `strict_command` (carrying category
/// + level metadata); deeper subcommands are plain `Node::group`
/// children.
pub fn build_command_tree(root: &Command) -> CommandTree {
    let mut tree = CommandTree::new(root.name);
    for sub in &root.subcommands {
        tree = tree.strict_command(sub.name, to_strict_node(sub));
    }
    tree
}

fn to_strict_node(cmd: &Command) -> StrictNode<true, true> {
    let strict: StrictNode<false, false> = if cmd.subcommands.is_empty() {
        leaf_strict(cmd)
    } else {
        let children: Vec<(&str, Node)> = cmd.subcommands.iter()
            .map(|s| (s.name, to_node(s)))
            .collect();
        StrictNode::group(children)
    };
    strict
        .with_category(cmd.category.tag())
        .with_level(cmd.level.rank())
}

fn leaf_strict(cmd: &Command) -> StrictNode<false, false> {
    let value_flags: Vec<&str> = cmd.flags.iter()
        .filter(|f| matches!(f.arity, Arity::Value))
        .map(|f| f.long)
        .collect();
    let bool_flags: Vec<&str> = cmd.flags.iter()
        .filter(|f| matches!(f.arity, Arity::Bool))
        .map(|f| f.long)
        .collect();
    let mut node = StrictNode::leaf_with_flags(&value_flags, &bool_flags);
    for f in &cmd.flags {
        if let ValueProvider::Custom(provider) = f.value {
            node = node.with_value_provider(f.long, fn_provider(provider));
            for a in f.aliases {
                node = node.with_value_provider(a, fn_provider(provider));
            }
        }
    }
    node
}

fn to_node(cmd: &Command) -> Node {
    if let Some(provider) = cmd.completion_override {
        return provider();
    }
    if cmd.subcommands.is_empty() {
        leaf_node(cmd)
    } else {
        Node::group(
            cmd.subcommands.iter()
                .map(|s| (s.name, to_node(s)))
                .collect()
        )
    }
}

fn leaf_node(cmd: &Command) -> Node {
    let value_flags: Vec<&str> = cmd.flags.iter()
        .filter(|f| matches!(f.arity, Arity::Value))
        .map(|f| f.long)
        .collect();
    let bool_flags: Vec<&str> = cmd.flags.iter()
        .filter(|f| matches!(f.arity, Arity::Bool))
        .map(|f| f.long)
        .collect();
    let mut node = Node::leaf_with_flags(&value_flags, &bool_flags);
    for f in &cmd.flags {
        if let ValueProvider::Custom(provider) = f.value {
            node = node.with_value_provider(f.long, fn_provider(provider));
            for a in f.aliases {
                node = node.with_value_provider(a, fn_provider(provider));
            }
        }
    }
    node
}
