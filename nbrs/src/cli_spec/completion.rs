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
/// the root are wired via `strict_command` (carrying category +
/// level metadata); deeper subcommands are plain `Node::group`
/// children.
pub fn build_command_tree(root: &Command) -> CommandTree {
    let mut tree = CommandTree::new(root.name).require_metadata();
    for sub in &root.subcommands {
        tree = tree.strict_command(sub.name, to_strict_node(sub));
    }
    debug_assert!(
        tree.validate().is_ok(),
        "command tree metadata: {:?}",
        tree.validate().err()
    );
    tree
}

fn to_strict_node(cmd: &Command) -> StrictNode<true, true> {
    // `completion_override` is checked HERE as well as in `to_node`, because
    // root-level commands are built by this function alone. Without it a root
    // command carrying an override got the plain leaf node instead — and since
    // an override is used precisely by commands that declare no `flags` /
    // `kv_params` of their own, that node is EMPTY: the command name completes,
    // then nothing after it. `nbrs table`/`nbrs plot` behaved that way while
    // `nbrs report table` (a deeper child, built by `to_node`) worked.
    let strict: StrictNode<false, false> = if let Some(provider) = cmd.completion_override {
        StrictNode::from_node(provider())
    } else if cmd.subcommands.is_empty() {
        leaf_strict(cmd)
    } else {
        let children: Vec<(&str, Node)> = cmd
            .subcommands
            .iter()
            .map(|s| (s.name, to_node(s)))
            .collect();
        StrictNode::group(children)
    };
    strict
        .with_category(cmd.category.tag())
        .with_level(cmd.level.rank())
}

fn leaf_strict(cmd: &Command) -> StrictNode<false, false> {
    // One leaf builder: the strict wrapper starts from the same
    // fully-equipped Node (flags, kv params, providers, dynamic
    // options, positional provider) and only adds the type-state
    // metadata gates.
    StrictNode::from_node(leaf_node(cmd))
}

fn to_node(cmd: &Command) -> Node {
    if let Some(provider) = cmd.completion_override {
        return provider()
            .with_category(cmd.category.tag())
            .with_level(cmd.level.rank());
    }
    let node = if cmd.subcommands.is_empty() {
        leaf_node(cmd)
    } else {
        Node::group(
            cmd.subcommands
                .iter()
                .map(|s| (s.name, to_node(s)))
                .collect(),
        )
    };
    node.with_category(cmd.category.tag())
        .with_level(cmd.level.rank())
}

fn leaf_node(cmd: &Command) -> Node {
    let value_flags: Vec<&str> = cmd
        .flags
        .iter()
        .filter(|f| matches!(f.arity, Arity::Value))
        .flat_map(|f| std::iter::once(f.long).chain(f.short))
        .collect();
    let bool_flags: Vec<&str> = cmd
        .flags
        .iter()
        .filter(|f| matches!(f.arity, Arity::Bool))
        .map(|f| f.long)
        .collect();
    // kv params are option tokens too — `workload=` shows up in
    // the option list AND completes its value.
    let mut value_flags = value_flags;
    value_flags.extend(cmd.kv_params.iter().map(|kv| kv.key));
    let mut node = Node::leaf_with_flags(&value_flags, &bool_flags);
    for f in &cmd.flags {
        if let ValueProvider::Custom(provider) = f.value {
            node = node.with_value_provider(f.long, fn_provider(provider));
            if let Some(short) = f.short {
                node = node.with_value_provider(short, fn_provider(provider));
            }
            for a in f.aliases {
                node = node.with_value_provider(a, fn_provider(provider));
            }
        }
    }
    for kv in cmd.kv_params {
        node = node.with_value_provider(kv.key, fn_provider(kv.provider));
    }
    if let Some(dynamic) = cmd.dynamic_options {
        node = node.with_dynamic_options(dynamic);
    }
    if let Some(p) = cmd.positionals.first() {
        match p.value {
            ValueProvider::Custom(provider) => {
                node = node.with_positional_provider(fn_provider(provider));
            }
            ValueProvider::Path => {
                node =
                    node.with_positional_provider(veks_completion::providers::fs_paths_provider());
            }
            ValueProvider::None => {}
        }
    }
    node
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A root-level command whose completion comes from an override must expose
    /// that node's options. Root children are built by [`to_strict_node`] alone,
    /// which used to fall through to the plain leaf builder and drop the
    /// override — and since override-carrying commands declare no `flags` of
    /// their own, the result was an empty node: the command name completed and
    /// nothing after it did. `nbrs table` / `nbrs plot` were in that state while
    /// `nbrs report table` worked.
    #[test]
    fn root_level_completion_override_is_honoured() {
        fn override_node() -> Node {
            Node::leaf_with_flags(&["--only-via-override"], &[])
        }
        let cmd = Command {
            name: "aliased",
            help: "",
            category: Category::Tools,
            level: Level::Secondary,
            flags: Vec::new(),
            kv_params: &[],
            dynamic_options: None,
            positionals: Vec::new(),
            subcommands: Vec::new(),
            handler: None,
            raw_args: true,
            completion_override: Some(override_node),
        };
        let root = Command {
            name: "nbrs",
            help: "",
            category: Category::Tools,
            level: Level::Workload,
            flags: Vec::new(),
            kv_params: &[],
            dynamic_options: None,
            positionals: Vec::new(),
            subcommands: vec![cmd],
            handler: None,
            raw_args: false,
            completion_override: None,
        };
        let tree = build_command_tree(&root);
        let got = veks_completion::complete(&tree, &["nbrs", "aliased", "--only"]);
        assert!(
            got.iter().any(|c| c == "--only-via-override"),
            "root-level override node must supply completions, got {got:?}"
        );
    }
}
