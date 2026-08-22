// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Redfish acquisition.
//!
//! Two providers ship: [`SensorRead`] — one sensor `OData` GET projected
//! into a readings batch and a states batch, per `docs/DATA-MODEL.md`'s
//! worked example — and [`ChassisRead`] — one chassis GET projected into an
//! inventory batch and a states batch. Transport rides nv-redfish's `Bmc`
//! trait, so the providers are generic over HTTP and the mock the fixture
//! corpus replays through; reqwest and tokio enter the workspace only here,
//! behind the `bmc-http` feature.
//!
//! Projection is *declared* in `manifests/` and compiled into
//! `src/generated/` by `make codegen`: deterministic, I/O-free functions
//! from a decoded source type and its location to observation parts plus
//! issues, pinned by the corpus under `tests/`. `projection/` re-exports
//! the generated boundary the provider consumes.
//!
//! Reserved for later milestones: catalog stages (`ServiceRoot` walks that
//! retain sensor links privately), the bulk `TelemetryService` provider,
//! session handling, and the vendor-leniency layer real BMCs require.

mod failure;
mod generated;
mod projection;
mod provider;
mod uri;

pub use failure::ClassifyError;
pub use provider::ChassisRead;
pub use provider::SensorRead;
