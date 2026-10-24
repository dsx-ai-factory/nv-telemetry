// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Checks that protocol bindings and projection files do not overwrite each other.

use std::fs;
use std::path::Path;
use std::process::Command;

#[test]
fn protocol_bindings_coexist_with_manifests_and_share_freshness_checks() {
    let fixture = tempfile::tempdir().expect("create an isolated checkout");
    let root = fixture.path();
    let checkout = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let proto = "sources/gnmi/proto/github.com/openconfig/gnmi/proto";

    fs::write(root.join("buf.yaml"), "").expect("create workspace marker");

    for file in ["gnmi/gnmi.proto", "gnmi_ext/gnmi_ext.proto"] {
        let path = root.join(proto).join(file);
        fs::create_dir_all(path.parent().expect("protocol parent"))
            .expect("create protocol directory");

        fs::copy(checkout.join(proto).join(file), path).expect("copy vendored protocol");
    }

    let manifests = root.join("sources/gnmi/manifests");
    fs::create_dir_all(&manifests).expect("create manifest directory");

    for name in ["gnmi", "gnmi_ext"] {
        fs::write(
            manifests.join(format!("{name}.textpb")),
            "source: \"gnmi\"\nbackend: BACKEND_SCHEMA_INDEX\nindex: \"nv-redfish-schema/dmtf\"\n",
        )
        .expect("write an accepted manifest");
    }

    let run = |argument| {
        Command::new(env!("CARGO_BIN_EXE_nv-telemetry-codegen"))
            .arg(argument)
            .current_dir(root)
            .output()
            .expect("run the compiler in the fixture")
    };

    for argument in ["generate", "--check"] {
        let output = run(argument);
        assert!(output.status.success(), "{output:?}");
    }

    let repeated = run("generate");

    assert!(repeated.status.success(), "{repeated:?}");
    assert!(String::from_utf8_lossy(&repeated.stdout).contains("0 file(s) written"));

    let generated = root
        .join("sources")
        .join("gnmi")
        .join("src")
        .join("generated");

    let protocol = generated.join("protocol");
    let stray = protocol.join("obsolete.rs");

    fs::write(&stray, "// obsolete\n").expect("add a nested orphan");

    let output = run("--check");

    assert!(!output.status.success(), "{output:?}");
    assert!(String::from_utf8_lossy(&output.stderr).contains(stray.to_string_lossy().as_ref()));

    fs::remove_file(stray).expect("remove the nested orphan");
    fs::remove_dir_all(manifests).expect("remove the fixture manifests");

    let output = run("--check");
    let diagnostic = String::from_utf8_lossy(&output.stderr);

    assert!(!output.status.success(), "{output:?}");

    assert!(
        diagnostic.contains("4 file(s) under a generated directory"),
        "{diagnostic}"
    );

    assert!(
        !diagnostic.contains(protocol.to_string_lossy().as_ref()),
        "{diagnostic}"
    );
}
