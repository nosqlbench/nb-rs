// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! GK runtime: unified compilation context with factory registration.
//!
//! The `GkRuntime` holds the complete set of available node functions
//! (built-in + factory-provided), module search paths, and stdlib.
//! All compilation goes through the runtime — there is no separate
//! "built-in" vs "external" distinction visible to the user.

use std::path::PathBuf;

use crate::dsl::registry::{FuncSig, FuncCategory};
use crate::node::GkNode;

/// Constant argument passed to a node factory at build time.
#[derive(Debug, Clone)]
pub enum FactoryArg {
    Int(u64),
    Float(f64),
    Str(String),
}

/// Trait for external node providers.
///
/// External crates implement this to contribute GK node functions.
/// Once registered on a `GkRuntime`, the factory's nodes are
/// indistinguishable from built-in nodes: same registry, same
/// describe output, same category grouping, same type checking.
pub trait NodeFactory: Send + Sync {
    /// Return signatures for all functions this factory provides.
    ///
    /// Called once at registration time. The returned signatures are
    /// merged into the runtime's unified registry.
    fn signatures(&self) -> Vec<FuncSig>;

    /// Build a node by name with the given constant arguments.
    ///
    /// Called by the compiler when assembling a kernel that references
    /// one of this factory's functions. `wire_count` is the number of
    /// wire inputs at the call site.
    fn build(
        &self,
        name: &str,
        wire_count: usize,
        consts: &[FactoryArg],
    ) -> Result<Box<dyn GkNode>, String>;
}

/// The GK runtime: unified compilation context.
///
/// Holds the complete function registry (built-in + factory-provided),
/// factory instances for node construction, module search paths, and
/// stdlib sources. All compilation goes through the runtime.
///
/// Multiple runtimes can coexist with different factory sets.
pub struct GkRuntime {
    /// Registered factories. Built-in nodes are handled separately
    /// (they're hardcoded in build_node), but their signatures are
    /// included in the unified registry.
    factories: Vec<Box<dyn NodeFactory>>,
    /// Additional module search paths (from --gk-lib).
    gk_lib_paths: Vec<PathBuf>,
}

impl GkRuntime {
    /// Create a new runtime with only built-in nodes.
    pub fn new() -> Self {
        Self {
            factories: Vec::new(),
            gk_lib_paths: Vec::new(),
        }
    }

    /// Register an external node factory.
    ///
    /// The factory's signatures are merged into the unified registry.
    /// Its nodes become available for compilation immediately.
    pub fn register_factory(&mut self, factory: Box<dyn NodeFactory>) {
        self.factories.push(factory);
    }

    /// Add a module search path (from --gk-lib).
    pub fn add_gk_lib(&mut self, path: PathBuf) {
        self.gk_lib_paths.push(path);
    }

    /// Return the unified function registry: built-in + all factories.
    pub fn registry(&self) -> Vec<FuncSig> {
        let mut sigs = crate::dsl::registry::registry();
        for factory in &self.factories {
            sigs.extend(factory.signatures());
        }
        sigs
    }

    /// Return functions grouped by category from the unified registry.
    pub fn by_category(&self) -> Vec<(FuncCategory, Vec<FuncSig>)> {
        let sigs = self.registry();
        let mut groups: std::collections::HashMap<FuncCategory, Vec<FuncSig>> =
            std::collections::HashMap::new();
        for sig in sigs {
            groups.entry(sig.category).or_default().push(sig);
        }
        FuncCategory::display_order().iter()
            .filter_map(|cat| groups.remove(cat).map(|funcs| (*cat, funcs)))
            .collect()
    }

    /// Try to build a node through registered factories.
    ///
    /// Called by the compiler when the built-in build_node doesn't
    /// match. Returns None if no factory handles this function name.
    pub fn build_from_factory(
        &self,
        name: &str,
        wire_count: usize,
        consts: &[FactoryArg],
    ) -> Option<Result<Box<dyn GkNode>, String>> {
        for factory in &self.factories {
            if factory.signatures().iter().any(|s| s.name == name) {
                return Some(factory.build(name, wire_count, consts));
            }
        }
        None
    }

    /// Number of registered factories.
    pub fn factory_count(&self) -> usize {
        self.factories.len()
    }

    /// The --gk-lib search paths.
    pub fn gk_lib_paths(&self) -> &[PathBuf] {
        &self.gk_lib_paths
    }
}

impl Default for GkRuntime {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::registry::{Arity, ParamSpec};
    use crate::node::SlotType;

    #[test]
    fn default_runtime_has_builtins() {
        let rt = GkRuntime::new();
        let reg = rt.registry();
        assert!(reg.len() >= 50);
    }

    #[test]
    fn factory_signatures_merged() {
        struct TestFactory;
        impl NodeFactory for TestFactory {
            fn signatures(&self) -> Vec<FuncSig> {
                vec![FuncSig {
                    name: "test_node",
                    category: FuncCategory::Diagnostic,
                    outputs: 1,
                    description: "a test node from a factory",
                    help: "",
                    identity: None,
                    variadic_ctor: None,
                    params: &[
                        ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle" },
                    ],
                    arity: Arity::Fixed,
                    commutativity: crate::node::Commutativity::Positional,
                }]
            }
            fn build(&self, _name: &str, _wc: usize, _consts: &[FactoryArg])
                -> Result<Box<dyn GkNode>, String> {
                Ok(Box::new(crate::nodes::identity::Identity::new()))
            }
        }

        let mut rt = GkRuntime::new();
        let before = rt.registry().len();
        rt.register_factory(Box::new(TestFactory));
        let after = rt.registry().len();
        assert_eq!(after, before + 1);

        // The test_node should appear in the unified registry
        assert!(rt.registry().iter().any(|s| s.name == "test_node"));
    }

    #[test]
    fn factory_build_dispatch() {
        struct TestFactory;
        impl NodeFactory for TestFactory {
            fn signatures(&self) -> Vec<FuncSig> {
                vec![FuncSig {
                    name: "custom_identity",
                    category: FuncCategory::Diagnostic,
                    outputs: 1,
                    description: "custom identity from factory",
                    help: "",
                    identity: None,
                    variadic_ctor: None,
                    params: &[
                        ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle" },
                    ],
                    arity: Arity::Fixed,
                    commutativity: crate::node::Commutativity::Positional,
                }]
            }
            fn build(&self, name: &str, _wc: usize, _consts: &[FactoryArg])
                -> Result<Box<dyn GkNode>, String> {
                match name {
                    "custom_identity" => Ok(Box::new(crate::nodes::identity::Identity::new())),
                    _ => Err(format!("unknown: {name}")),
                }
            }
        }

        let mut rt = GkRuntime::new();
        rt.register_factory(Box::new(TestFactory));

        // Should find and build via factory
        let result = rt.build_from_factory("custom_identity", 1, &[]);
        assert!(result.is_some());
        assert!(result.unwrap().is_ok());

        // Should not find built-in nodes via factory
        let result = rt.build_from_factory("hash", 1, &[]);
        assert!(result.is_none());
    }

    #[test]
    fn by_category_includes_factory_nodes() {
        struct TestFactory;
        impl NodeFactory for TestFactory {
            fn signatures(&self) -> Vec<FuncSig> {
                vec![FuncSig {
                    name: "factory_hash",
                    category: FuncCategory::Hashing,
                    outputs: 1,
                    description: "a factory hashing node",
                    help: "",
                    identity: None,
                    variadic_ctor: None,
                    params: &[
                        ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle" },
                    ],
                    arity: Arity::Fixed,
                    commutativity: crate::node::Commutativity::Positional,
                }]
            }
            fn build(&self, _: &str, _: usize, _: &[FactoryArg])
                -> Result<Box<dyn GkNode>, String> {
                Ok(Box::new(crate::nodes::identity::Identity::new()))
            }
        }

        let mut rt = GkRuntime::new();
        rt.register_factory(Box::new(TestFactory));

        let grouped = rt.by_category();
        let hashing = grouped.iter().find(|(c, _)| *c == FuncCategory::Hashing).unwrap();
        assert!(hashing.1.iter().any(|s| s.name == "factory_hash"));
        // Built-in hash should also be there
        assert!(hashing.1.iter().any(|s| s.name == "hash"));
    }
}
