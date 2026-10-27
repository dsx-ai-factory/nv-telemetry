// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Generates Rust bindings from the vendored gNMI schemas.

use std::path::Path;
use std::path::PathBuf;

use prost_build::Module;

const PROTO_DIR: &str = "sources/gnmi/proto";
const PROTO_FILE: &str = "github.com/openconfig/gnmi/proto/gnmi/gnmi.proto";
const OUTPUT_DIR: &str = "sources/gnmi/src/generated/protocol";

const HEADER: &str = "\
// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-FileCopyrightText: Copyright 2016 Google Inc. All Rights Reserved.
// SPDX-FileCopyrightText: Copyright 2018 Google Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Generated from the vendored OpenConfig gNMI schemas by `make codegen`. Do not edit.
";

const MODULES: &str = "\
// Generated names, types, and comments follow the upstream protocol.
// Some types and client methods are unused.
#![allow(
    clippy::pedantic,
    clippy::enum_variant_names,
    clippy::exhaustive_structs,
    dead_code,
    unreachable_pub,
    unused_qualifications,
    rustdoc::all
)]

#[rustfmt::skip]
pub mod gnmi;

#[rustfmt::skip]
pub mod gnmi_ext;
";

/// Generates gNMI bindings and returns the output paths and contents in path order.
pub(super) fn generate(root: &Path) -> Result<Vec<(PathBuf, String)>, String> {
    let include = root.join(PROTO_DIR);

    let mut compiler = protox::Compiler::new([&include])
        .map_err(|error| format!("gNMI include path {}: {error}", include.display()))?;

    compiler.include_imports(true);
    compiler.include_source_info(true);

    compiler
        .open_file(PROTO_FILE)
        .map_err(|error| format!("gNMI protocol {PROTO_FILE}: {error}"))?;

    // Prost needs imported message definitions to generate trait implementations.
    // It uses prost-types for Google's well-known types instead of generating copies.
    let files = compiler
        .file_descriptor_set()
        .file
        .into_iter()
        .map(|file| (Module::from_protobuf_package_name(file.package()), file))
        .collect();

    let services = tonic_prost_build::configure()
        .build_server(false)
        .build_transport(false)
        .service_generator();

    let mut config = prost_build::Config::new();
    config.service_generator(services);

    let modules = config
        .generate(files)
        .map_err(|error| format!("gNMI bindings could not be generated: {error}"))?;

    let output = root.join(OUTPUT_DIR);

    let mut artifacts = vec![(output.join("mod.rs"), format!("{HEADER}\n{MODULES}"))];

    for (module, body) in modules {
        artifacts.push((
            output.join(module.to_file_name_or("gnmi")),
            format!("{HEADER}\n{body}"),
        ));
    }

    artifacts.sort_by(|left, right| left.0.cmp(&right.0));

    Ok(artifacts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_artifacts_are_repeatable_and_include_both_packages() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");

        let first = generate(&root).expect("vendored protocol generates");
        let second = generate(&root).expect("vendored protocol generates again");

        assert_eq!(first, second);

        assert_eq!(
            first
                .iter()
                .map(|(path, _)| path.clone())
                .collect::<Vec<_>>(),
            ["gnmi.rs", "gnmi_ext.rs", "mod.rs"]
                .map(|file| root.join("sources/gnmi/src/generated/protocol").join(file))
        );
    }

    #[test]
    fn missing_protocol_inputs_return_a_generation_error() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));

        let error = generate(root).expect_err("a checkout without protocol inputs is refused");

        assert!(error.contains("gnmi.proto"), "{error}");
    }
}
