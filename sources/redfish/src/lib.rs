// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Redfish acquisition.
//!
//! Four polled providers ship, one [`Read`] envelope each: [`SensorRead`] —
//! one sensor `OData` GET projected into a readings batch and a states
//! batch, per `docs/DATA-MODEL.md`'s worked example — [`ChassisRead`] — one
//! chassis GET projected into an inventory batch and a states batch —
//! [`LogRead`] — a walk over a log service's entries projected into logs
//! batches — and [`FirmwareRead`] — a walk over the update service's
//! firmware inventory projected into an inventory batch and a states batch.
//! One streamed provider ships beside them: [`EventStream`] — the
//! endpoint's server-sent events, one item per `Event` payload, projected
//! into logs batches under the event service's own scope.
//!
//! Transport is the embedder's: it implements [`RedfishTransport`] over its
//! own HTTP client and classifies its own failures, and every provider of
//! one endpoint reads through that endpoint's [`RedfishService`]. nv-redfish
//! decodes, navigates, and repairs behind it, its platform classified once
//! per endpoint, and none of its types is part of this crate's API, so its
//! releases never force one of this crate's.
//!
//! Projection is *declared* in `manifests/` and compiled into
//! `src/generated/` by `make codegen`: deterministic, I/O-free functions
//! from a decoded source type and its location to observation parts plus
//! issues, pinned by the corpus under `tests/`. `projection/` re-exports
//! the generated boundary the provider consumes.
//!
//! Reserved for later milestones: catalog stages (`ServiceRoot` walks that
//! retain sensor links privately), the bulk `TelemetryService` provider,
//! and session handling.

mod failure;
mod generated;
mod instant;
mod projection;
mod provider;
mod stream;
mod transport;
mod uri;

pub use failure::classify_status;
pub use provider::ChassisKind;
pub use provider::ChassisRead;
pub use provider::FirmwareKind;
pub use provider::FirmwareRead;
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
pub use stream::EmptyEventId;
pub use stream::EventStream;
pub use stream::ResumePosition;
pub use stream::StreamRun;
pub use stream::GAP_LOCATOR;
pub use stream::METRIC_REPORT_LOCATOR;
pub use stream::PAYLOAD_LOCATOR;
pub use transport::EventFrames;
pub use transport::RedfishService;
pub use transport::RedfishTransport;
pub use transport::SseEvent;
