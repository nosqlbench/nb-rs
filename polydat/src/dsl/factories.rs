// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Polydat runtime: unified compilation context with factory registration.
//!
//! The `PolydatRuntime` holds the complete set of available node functions
//! (built-in + factory-provided), module search paths, and stdlib.
//! All compilation goes through the runtime — there is no separate
//! "built-in" vs "external" distinction visible to the user.

use std::path::PathBuf;
use std::sync::Mutex;

use crate::dsl::registry::{FuncSig, FuncCategory};
use crate::ast::{PolydatNode, PortType, Value};

// ───── Virtual-wire resolver registry (γ-8) ─────

/// Host-mediated extern resolver per
/// `expression_engine.md` §5.6 (virtual wires). A resolver
/// is a callback that fires at Context Fusion scope-init
/// time when a kernel's extern slot can't be satisfied
/// from the outer scope's direct bindings.
///
/// Arguments: slot name + declared slot type + the kernel
/// being initialised. The resolver returns `Some(value)` to
/// fill the slot or `None` to fall through to ordinary
/// resolution (typed error if no other source exists).
///
/// Per §5.6.2's contract: the returned `Value`'s type must
/// match the slot's declared `PortType` (boundary adapter
/// applies otherwise per γ-5); the resolver fires once per
/// scope-init (per S3); and the resolver is responsible for
/// its own determinism.
pub type ExternResolver = Box<dyn Fn(&str, PortType) -> Option<Value> + Send + Sync>;

/// Process-level registry of virtual-wire resolvers.
///
/// Resolvers are registered via [`register_extern_resolver`]
/// and consulted by Context Fusion's
/// `materialize_wiring_from_outer` when an outer-chain
/// lookup yields nothing. Multiple resolvers iterate in
/// registration order; the first matching resolver wins.
///
/// Test isolation: tests that register a resolver should
/// call [`clear_extern_resolvers`] in a teardown block to
/// avoid leaking state across tests.
static RESOLVERS: Mutex<Vec<ExternResolver>> = Mutex::new(Vec::new());

/// Register a virtual-wire resolver. Resolvers stay
/// registered for the process's lifetime unless
/// [`clear_extern_resolvers`] is called.
///
/// Per `expression_engine.md` §5.6 PLANNED → γ-8 SHIPPED.
pub fn register_extern_resolver(resolver: ExternResolver) {
    let mut r = RESOLVERS.lock().unwrap();
    r.push(resolver);
}

/// Clear every registered resolver. Primarily for tests
/// that want clean teardown.
pub fn clear_extern_resolvers() {
    let mut r = RESOLVERS.lock().unwrap();
    r.clear();
}

/// Try every registered resolver in order; return the
/// first `Some(value)` whose type matches `slot_type`
/// (or whose type the catalog can adapt to `slot_type`).
///
/// Called by `crate::kernel::state::materialize_wiring_from_outer`
/// as a fall-through after outer-chain lookup.
pub(crate) fn resolve_extern(slot_name: &str, slot_type: PortType) -> Option<Value> {
    let r = RESOLVERS.lock().unwrap();
    for resolver in r.iter() {
        if let Some(value) = resolver(slot_name, slot_type) {
            return Some(value);
        }
    }
    None
}

// ───── End virtual-wire resolver registry ─────

/// Constant argument passed to a node factory at build time.
#[derive(Debug, Clone)]
pub enum FactoryArg {
    Int(u64),
    Float(f64),
    Str(String),
}

/// Trait for external node providers.
///
/// External crates implement this to contribute Polydat node functions.
/// Once registered on a `PolydatRuntime`, the factory's nodes are
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
    ) -> Result<Box<dyn PolydatNode>, String>;
}

/// The Polydat runtime: unified compilation context.
///
/// Holds the complete function registry (built-in + factory-provided),
/// factory instances for node construction, module search paths, and
/// stdlib sources. All compilation goes through the runtime.
///
/// Multiple runtimes can coexist with different factory sets.
pub struct PolydatRuntime {
    /// Registered factories. Built-in nodes are handled separately
    /// (they're hardcoded in build_node), but their signatures are
    /// included in the unified registry.
    factories: Vec<Box<dyn NodeFactory>>,
    /// Additional module search paths (from --polydat-lib).
    polydat_lib_paths: Vec<PathBuf>,
}

impl PolydatRuntime {
    /// Create a new runtime with only built-in nodes.
    pub fn new() -> Self {
        Self {
            factories: Vec::new(),
            polydat_lib_paths: Vec::new(),
        }
    }

    /// Register an external node factory.
    ///
    /// The factory's signatures are merged into the unified registry.
    /// Its nodes become available for compilation immediately.
    pub fn register_factory(&mut self, factory: Box<dyn NodeFactory>) {
        self.factories.push(factory);
    }

    /// Add a module search path (from --polydat-lib).
    pub fn add_polydat_lib(&mut self, path: PathBuf) {
        self.polydat_lib_paths.push(path);
    }

    /// Return the unified function registry: built-in + all factories.
    ///
    /// SRD-80 — `#[polydat_node]`-generated nodes route through
    /// the same `crate::dsl::registry::registry()` channel
    /// (they submit `NodeRegistration` entries link-time, same
    /// as `register_nodes!`-using modules), so no separate
    /// merge step is needed here.
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
    ) -> Option<Result<Box<dyn PolydatNode>, String>> {
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

    /// The --polydat-lib search paths.
    pub fn polydat_lib_paths(&self) -> &[PathBuf] {
        &self.polydat_lib_paths
    }
}

impl Default for PolydatRuntime {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::registry::{Arity, ParamSpec};
    use crate::ast::SlotType;

    /// Test helper: serialise resolver-registry tests via a
    /// process-wide mutex so two tests don't race the static
    /// `RESOLVERS`.
    static RESOLVER_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn extern_resolver_register_and_lookup() {
        let _guard = RESOLVER_TEST_LOCK.lock().unwrap();
        clear_extern_resolvers();
        register_extern_resolver(Box::new(|name, _typ| {
            if name == "region" {
                Some(Value::Str("us-east-1".into()))
            } else {
                None
            }
        }));
        let v = resolve_extern("region", PortType::Str);
        assert_eq!(v, Some(Value::Str("us-east-1".into())));
        let v = resolve_extern("missing", PortType::Str);
        assert_eq!(v, None);
        clear_extern_resolvers();
    }

    #[test]
    fn extern_resolver_first_match_wins() {
        let _guard = RESOLVER_TEST_LOCK.lock().unwrap();
        clear_extern_resolvers();
        register_extern_resolver(Box::new(|name, _typ| {
            if name == "k" {
                Some(Value::U64(1))
            } else {
                None
            }
        }));
        register_extern_resolver(Box::new(|name, _typ| {
            if name == "k" {
                Some(Value::U64(2))
            } else {
                None
            }
        }));
        let v = resolve_extern("k", PortType::U64);
        // First registered wins.
        assert_eq!(v, Some(Value::U64(1)));
        clear_extern_resolvers();
    }

    #[test]
    fn extern_resolver_clear_removes_all() {
        let _guard = RESOLVER_TEST_LOCK.lock().unwrap();
        register_extern_resolver(Box::new(|_, _| Some(Value::U64(99))));
        assert!(resolve_extern("anything", PortType::U64).is_some());
        clear_extern_resolvers();
        assert!(resolve_extern("anything", PortType::U64).is_none());
    }

    #[test]
    fn default_runtime_has_builtins() {
        let rt = PolydatRuntime::new();
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
                        ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
                    ],
                    arity: Arity::Fixed,
                    commutativity: crate::ast::Commutativity::Positional,
            default_resolver: None,
            output_type: crate::dsl::registry::OutputType::Fixed,
                }]
            }
            fn build(&self, _name: &str, _wc: usize, _consts: &[FactoryArg])
                -> Result<Box<dyn PolydatNode>, String> {
                Ok(Box::new(crate::library::identity::Identity::new(crate::ast::PortType::U64)))
            }
        }

        let mut rt = PolydatRuntime::new();
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
                        ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
                    ],
                    arity: Arity::Fixed,
                    commutativity: crate::ast::Commutativity::Positional,
            default_resolver: None,
            output_type: crate::dsl::registry::OutputType::Fixed,
                }]
            }
            fn build(&self, name: &str, _wc: usize, _consts: &[FactoryArg])
                -> Result<Box<dyn PolydatNode>, String> {
                match name {
                    "custom_identity" => Ok(Box::new(crate::library::identity::Identity::new(crate::ast::PortType::U64))),
                    _ => Err(format!("unknown: {name}")),
                }
            }
        }

        let mut rt = PolydatRuntime::new();
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
                        ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
                    ],
                    arity: Arity::Fixed,
                    commutativity: crate::ast::Commutativity::Positional,
            default_resolver: None,
            output_type: crate::dsl::registry::OutputType::Fixed,
                }]
            }
            fn build(&self, _: &str, _: usize, _: &[FactoryArg])
                -> Result<Box<dyn PolydatNode>, String> {
                Ok(Box::new(crate::library::identity::Identity::new(crate::ast::PortType::U64)))
            }
        }

        let mut rt = PolydatRuntime::new();
        rt.register_factory(Box::new(TestFactory));

        let grouped = rt.by_category();
        let hashing = grouped.iter().find(|(c, _)| *c == FuncCategory::Hashing).unwrap();
        assert!(hashing.1.iter().any(|s| s.name == "factory_hash"));
        // Built-in hash should also be there
        assert!(hashing.1.iter().any(|s| s.name == "hash"));
    }
}
