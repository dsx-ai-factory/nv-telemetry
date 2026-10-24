// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Generated `gNMI` messages and client code.
//!
//! The bindings are private. Telemetry collection and projection into
//! observations are not implemented yet.
//!
//! Run `make codegen` to regenerate the bindings and `make check-codegen`
//! to check them. Builds use the checked-in files and do not need `protoc`.

#[path = "generated/protocol/mod.rs"]
mod protocol;
