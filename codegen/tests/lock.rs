// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Covers the contract lock.
//!
//! The guarantee is that a schema edit without a regenerated tree fails, which
//! rests on the rendered snapshot matching the committed file byte for byte.
//!
//! The contract is under development; this checks current-schema freshness.

use nv_telemetry_codegen::lock::Snapshot;
use nv_telemetry_codegen::options::Vocabulary;

#[test]
fn the_shipped_lock_matches_the_shipped_schema() {
    // The same comparison `make check-codegen` runs, so a schema edit without
    // a regenerated lock fails here too rather than only in CI.
    let pool = nv_telemetry_codegen::pool().expect("shipped schema decodes");
    let vocabulary = Vocabulary::resolve(&pool).expect("shipped schema defines the vocabulary");
    let rendered = Snapshot::capture(&pool, &vocabulary).render();

    let committed = std::fs::read_to_string(
        nv_telemetry_codegen::workspace_root()
            .expect("run from the repo")
            .join("schema/contract.lock"),
    )
    .expect("the contract lock is committed");

    assert_eq!(committed, rendered, "run `make codegen`");
}
