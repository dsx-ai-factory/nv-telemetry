// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Proves the generated wire types and the shipped descriptors agree.
//!
//! Structural spot-checking — "does every message exist, is every optional an
//! Option" — cannot catch a field number that shifted or a type that was
//! rendered as the wrong width, because both sides would still look
//! plausible. Encoding with one and decoding with the other does catch it:
//! the two representations have to agree byte for byte or the round trip
//! fails.
//!
//! This matters because the generated types are checked in. A consumer holds
//! them, a source builds them, and nothing else in the pipeline re-derives
//! them from the schema at runtime.

#[test]
fn the_checked_in_wire_types_match_the_schema() {
    // `make check-codegen` covers this in CI, but only there. A hand-edit to
    // the generated file — a shifted tag, a hand-"fixed" type — otherwise
    // survives a full `cargo test` run, and the generated types are checked
    // in precisely so that people read and touch them.
    let pool = nv_telemetry_codegen::pool().expect("shipped schema decodes");
    let rendered = nv_telemetry_codegen::wire::generate(&pool).expect("wire types render");

    let committed = std::fs::read_to_string(
        nv_telemetry_codegen::workspace_root()
            .expect("run from the repo")
            .join("model/src/generated/wire.rs"),
    )
    .expect("the generated wire types are committed");

    assert_eq!(committed, rendered, "run `make codegen`");
}

#[test]
fn a_contract_sub_package_is_refused_rather_than_dropped() {
    // The file filter matches sub-packages but only one module is emitted, so
    // a sub-package would be linted and locked while
    // having no type in the data plane — and every gate would stay green.
    let manifest = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let root = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("{}-subpkg", std::process::id()));
    let dir = root.join("nv/telemetry/v1/gpu");
    std::fs::create_dir_all(&dir).expect("fixture directory is writable");
    std::fs::write(
        dir.join("gpu.proto"),
        "syntax = \"proto3\";\npackage nv.telemetry.v1.gpu;\nmessage GpuThing { optional string id = 1; }\n",
    )
    .expect("fixture is writable");

    let mut compiler = protox::Compiler::new([&root, &manifest.join("../schema/proto")])
        .expect("include paths resolve");
    compiler.include_imports(true);
    // Both the sub-package and a real contract file, so the generator sees the
    // shape it would see in the repo: two modules where it emits one.
    compiler
        .open_files([
            std::path::Path::new("nv/telemetry/v1/gpu/gpu.proto"),
            std::path::Path::new("nv/telemetry/v1/subject.proto"),
        ])
        .expect("fixture compiles");
    let pool =
        prost_reflect::DescriptorPool::decode(compiler.encode_file_descriptor_set().as_slice())
            .expect("fixture descriptors load");

    let error = nv_telemetry_codegen::wire::generate(&pool)
        .expect_err("a sub-package must be refused, not silently dropped");
    assert!(error.contains("sub-package"), "unexpected error: {error}");
}
