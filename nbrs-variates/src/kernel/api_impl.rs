// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Trait implementations of the GK context API ([`Metadata`],
//! [`Dataflow`], [`Construction`]) on [`GkKernel`].
//!
//! GkKernel is the singular caller-facing interface that fuses
//! the compiled context (program) and per-fiber state. All
//! external (non-GK-internal) callers should reach the kernel
//! exclusively through these three traits — `state()` /
//! `state_ref()` / `program()` are kernel-internal hooks.

use crate::kernel::{Dataflow, GkKernel, Metadata, Construction};
use crate::node::{PortType, Value};

impl Metadata for GkKernel {
    #[inline]
    fn find_input(&self, name: &str) -> Option<usize> {
        self.program().find_input(name)
    }

    #[inline]
    fn input_names(&self) -> Vec<String> {
        self.program().input_names()
    }

    #[inline]
    fn output_names(&self) -> Vec<String> {
        self.program().output_names().iter().map(|s| s.to_string()).collect()
    }

    #[inline]
    fn coord_count(&self) -> usize {
        self.program().coord_count()
    }

    #[inline]
    fn input_port_type(&self, name: &str) -> Option<PortType> {
        self.program().input_port_type(name)
    }
}

impl Dataflow for GkKernel {
    #[inline]
    fn set_wire_idx(&mut self, idx: usize, value: Value) {
        self.state().set_input(idx, value);
    }

    #[inline]
    fn get_wire_idx(&self, idx: usize) -> Value {
        self.state_ref().get_input(idx)
    }
}

impl Construction for GkKernel {
    type Error = crate::subcontext::ContractViolation;

    fn root(matter: crate::subcontext::GkMatter<'_>) -> Result<Self, Self::Error> {
        use crate::subcontext::GkMatterInner;
        match matter.inner {
            GkMatterInner::Source(s) => {
                crate::dsl::compile::compile_gk_with_libs_and_limit(
                    &s.body,
                    s.options.workload_dir.as_deref(),
                    s.options.gk_lib_paths,
                    &s.options.required_outputs,
                    s.options.strict,
                    s.options.context_label.as_deref().unwrap_or(&s.label),
                    s.options.cursor_limit,
                )
                .map_err(crate::subcontext::ContractViolation::Compile)
            }
            GkMatterInner::Statements(s) => {
                // Pre-parsed AST — go through the compile-from-AST
                // path. The `GkFile` AST root takes the statements
                // verbatim; the same options surface as the source
                // path.
                let file = crate::dsl::ast::GkFile { statements: s.statements };
                crate::dsl::compile::compile_ast_with_libs(
                    &file,
                    s.options.workload_dir.as_deref(),
                    s.options.gk_lib_paths,
                    &s.options.required_outputs,
                    s.options.strict,
                    s.options.context_label.as_deref().unwrap_or(&s.label),
                )
                .map_err(crate::subcontext::ContractViolation::Compile)
            }
            GkMatterInner::Program(p) => {
                let mut k = GkKernel::from_program(p.program);
                for (var, value) in p.iter_bindings {
                    if let Some(idx) = k.program().find_input(var) {
                        k.state().set_input(idx, value.clone());
                    }
                }
                Ok(k)
            }
        }
    }

    fn subscope(
        &self,
        matter: crate::subcontext::GkMatter<'_>,
    ) -> Result<Self, Self::Error> {
        // Delegate to GkKernel's existing typed subscope path.
        GkKernel::build_subscope(self, matter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::compile::compile_gk;

    /// Indexed wire access works.
    #[test]
    fn dataflow_indexed_set_get() {
        let mut k = compile_gk(
            "input cycle: u64\nconst x := 7\n"
        ).unwrap();
        // cycle is index 0
        k.set_wire(0_usize, Value::U64(42));
        assert_eq!(k.get_wire(0_usize), Some(Value::U64(42)));
    }

    /// Named wire access resolves through metadata.
    #[test]
    fn dataflow_named_set_get() {
        let mut k = compile_gk(
            "input cycle: u64\nextern n: u64\n"
        ).unwrap();
        k.set_wire("n", Value::U64(5));
        match k.get_wire("n") {
            Some(Value::U64(5)) => {}
            other => panic!("expected U64(5), got {other:?}"),
        }
    }

    /// String key works alongside &str.
    #[test]
    fn dataflow_string_key() {
        let mut k = compile_gk(
            "input cycle: u64\nextern n: u64\n"
        ).unwrap();
        let name = String::from("n");
        k.set_wire(&name, Value::U64(99));
        assert_eq!(k.get_wire(name.clone()), Some(Value::U64(99)));
    }

    /// Unknown name returns false / None — no panic.
    #[test]
    fn dataflow_unknown_name_safe() {
        let mut k = compile_gk("input cycle: u64\n").unwrap();
        assert!(!k.set_wire("nonexistent", Value::U64(1)));
        assert!(k.get_wire("nonexistent").is_none());
    }

    /// Metadata trait surfaces names + types.
    #[test]
    fn metadata_listings() {
        let k = compile_gk(
            "input (cycle: u64, thread: u64)\nextern n: u64\nconst x := 7\n"
        ).unwrap();
        let inputs: Vec<String> = k.input_names();
        assert!(inputs.iter().any(|s| s == "cycle"));
        assert!(inputs.iter().any(|s| s == "n"));
        assert_eq!(k.coord_count(), 2); // cycle + thread
        assert!(k.find_input("n").is_some());
        assert_eq!(k.input_port_type("n"), Some(PortType::U64));
    }

    /// Construction trait — both paths take the same gk
    /// matter type. Verify symmetry: root from source, then
    /// subscope from source against the root.
    #[test]
    fn construction_symmetric_paths() {
        let root_opts = crate::subcontext::CompileOptions {
            workload_dir: None,
            gk_lib_paths: Vec::new(),
            strict: false,
            required_outputs: Vec::new(),
            context_label: Some("root".to_string()),
            cursor_limit: None,
            ..Default::default()
        };
        let root_matter = crate::subcontext::GkMatter::builder()
            .label("root")
            .source("input cycle: u64\nshared flag := 0\n")
            .options(root_opts)
            .build()
            .expect("matter build");
        let root = <GkKernel as Construction>::root(root_matter)
            .expect("root from source matter");

        let sub_opts = crate::subcontext::CompileOptions {
            workload_dir: None,
            gk_lib_paths: Vec::new(),
            strict: false,
            required_outputs: Vec::new(),
            context_label: Some("sub".to_string()),
            cursor_limit: None,
            ..Default::default()
        };
        let sub_matter = crate::subcontext::GkMatter::builder()
            .label("sub")
            .source("input cycle: u64\n")
            .options(sub_opts)
            .build()
            .expect("matter build");
        let _sub = root
            .subscope(sub_matter)
            .expect("subscope from source matter");
    }

    /// Root construction also accepts pre-compiled program
    /// matter (re-instance with fresh state). Verifies via
    /// the input slot — `n` is an extern input.
    #[test]
    fn construction_root_from_program() {
        let template = compile_gk("input cycle: u64\nextern n: u64\n").unwrap();
        let program = template.program().clone();
        let matter = crate::subcontext::GkMatter::builder()
            .program(program)
            .build()
            .expect("matter build");
        let mut root = <GkKernel as Construction>::root(matter)
            .expect("root from program matter");
        root.set_wire("n", Value::U64(13));
        assert_eq!(root.get_wire("n"), Some(Value::U64(13)));
    }

    /// Builder rejects ambiguous matter (multiple input forms).
    #[test]
    fn builder_rejects_multiple_forms() {
        let template = compile_gk("input cycle: u64\n").unwrap();
        match crate::subcontext::GkMatter::builder()
            .source("input cycle: u64\n")
            .program(template.program().clone())
            .build()
        {
            Err(msg) => assert!(msg.contains("multiple"), "expected multiple-forms error, got: {msg}"),
            Ok(_) => panic!("multiple forms must error"),
        }
    }

    /// Builder rejects empty matter.
    #[test]
    fn builder_rejects_empty() {
        match crate::subcontext::GkMatter::builder().build() {
            Err(msg) => assert!(msg.contains("no input form"), "expected no-form error, got: {msg}"),
            Ok(_) => panic!("empty matter must error"),
        }
    }
}
