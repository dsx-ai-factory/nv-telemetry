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
//! own HTTP client, reporting what went wrong as a [`TransportFault`] whose
//! meaning is this crate's policy, and every provider of one endpoint reads
//! through that endpoint's [`RedfishService`]. nv-redfish decodes,
//! navigates, and repairs behind it, its platform classified per endpoint
//! and again after the endpoint fails, and none of its types is part of this crate's API, so its
//! releases never force one of this crate's.
//!
//! Projection is *declared* in `manifests/` and compiled into
//! `src/generated/` by `make codegen`: deterministic, I/O-free functions
//! from a decoded source type and its location to observation parts plus
//! issues, pinned by the corpus under `tests/`. `projection/` re-exports
//! the generated boundary the provider consumes.
//!
//! # Sensor discovery
//!
//! [`SensorCatalogRead`] walks advertised chassis and modern Sensors collections
//! without reading sensor documents. The immutable [`SensorCatalog`] contains
//! locations, discovery issues, and traversal completeness. The caller admits
//! discovery work and owns refresh, scheduling, and any status or issue envelopes.
//! Missing root Chassis navigation is unsupported; an absent optional Sensors
//! navigation contributes no modern sensors. Chassis and collection faults that
//! are resource-local retain usable branches in a partial catalog. Service-root,
//! endpoint, and internal failures return an error.
//!
//! Call [`nv_telemetry_source::Acquire::perform`] directly for the artifact.
//! `nv_telemetry_source::acquire` and `orchestration::PollUnit` currently accept
//! `AcquisitionParts` and do not consume a `SensorCatalog`.
//!
//! ```
//! use std::sync::Arc;
//!
//! use nv_telemetry_model::Completeness;
//! use nv_telemetry_model::EndpointContext;
//! use nv_telemetry_redfish::RedfishService;
//! use nv_telemetry_redfish::RedfishTransport;
//! use nv_telemetry_redfish::SensorCatalogRead;
//! use nv_telemetry_redfish::SensorRead;
//! use nv_telemetry_redfish::WalkBudget;
//! use nv_telemetry_source::Acquire;
//!
//! async fn discover<T: RedfishTransport>(
//!     endpoint: EndpointContext,
//!     service: Arc<RedfishService<T>>,
//! ) -> Result<Vec<SensorRead<T>>, Box<dyn std::error::Error>> {
//!     let discovery = SensorCatalogRead::new(
//!         endpoint.clone(), Arc::clone(&service), WalkBudget::DEFAULT,
//!     )?;
//!
//!     let catalog = discovery.perform().await?;
//!     if catalog.completeness() == Completeness::Partial {
//!         // This caller requires complete traversal before scheduling reads.
//!         return Err(format!("partial sensor discovery: {:?}", catalog.issues()).into());
//!     }
//!
//!     Ok(catalog.locations().iter().map(|location| {
//!         SensorRead::new(endpoint.clone(), location.clone(), Arc::clone(&service))
//!     }).collect())
//! }
//! ```
//!
//! Reserved for later milestones: bulk `TelemetryService` acquisition and
//! session handling.

mod catalog;
mod failure;
mod generated;
mod instant;
mod projection;
mod provider;
mod stream;
mod transport;
mod uri;

pub use catalog::SensorCatalog;
pub use catalog::SensorCatalogRead;
pub use failure::TransportFault;
pub use provider::ChassisKind;
pub use provider::ChassisRead;
pub use provider::FirmwareKind;
pub use provider::FirmwareRead;
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
