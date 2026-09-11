// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Redfish acquisition.
//!
//! Three providers ship, one [`Read`] envelope each: [`SensorRead`] — one
//! sensor `OData` GET projected into a readings batch and a states batch,
//! per `docs/DATA-MODEL.md`'s worked example — [`ChassisRead`] — one chassis
//! GET projected into an inventory batch and a states batch — and
//! [`LogRead`] — a walk over a log service's entries projected into logs
//! batches. Transport rides nv-redfish's `Bmc` trait, so the providers are
//! generic over HTTP and the mock the fixture corpus replays through;
//! reqwest and tokio enter the workspace only here, behind the `bmc-http`
//! feature.
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
mod instant;
mod projection;
mod provider;
mod uri;

pub use failure::ClassifyError;
pub use provider::ChassisKind;
pub use provider::ChassisRead;
pub use provider::LogCursor;
pub use provider::LogKind;
pub use provider::LogRead;
pub use provider::Read;
pub use provider::ReadKind;
pub use provider::SensorKind;
pub use provider::SensorRead;
pub use provider::WalkBudget;
pub use provider::FILTER_REFUSED_LOCATOR;
pub use provider::IN_FLIGHT_WALK_LOCATOR;
pub use provider::TRUNCATED_WALK_LOCATOR;
