// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The single-document reads: one `OData` GET each, up to two batches.

use std::fmt;
use std::sync::Arc;

use nv_redfish::core::ODataId;
use nv_redfish::schema::chassis::Chassis;
use nv_redfish::schema::sensor::Sensor;
use nv_redfish::Bmc;
use nv_telemetry_model::Completeness;
use nv_telemetry_model::Coverage;
use nv_telemetry_model::EndpointContext;
use nv_telemetry_model::Invalid;
use nv_telemetry_model::Inventory;
use nv_telemetry_model::Origin;
use nv_telemetry_model::Payload;
use nv_telemetry_model::Readings;
use nv_telemetry_model::States;
use nv_telemetry_source::Acquire;
use nv_telemetry_source::AcquisitionFailure;
use nv_telemetry_source::AcquisitionFailureClass;
use nv_telemetry_source::AcquisitionParts;
use nv_telemetry_source::ProviderDeclaration;

use crate::failure::ClassifyError;
use crate::projection::project_chassis;
use crate::projection::project_sensor;
use crate::projection::ChassisParts;
use crate::projection::SensorParts;

/// One sensor, read over one endpoint's `Bmc`.
///
/// A dispatched leaf: the planner names the sensor URI, the dispatcher
/// decides when this runs, and this type only knows how — GET the document,
/// project it, assemble the batches. Generic over the transport so the same
/// provider runs against HTTP and against the mock the corpus replays
/// through.
pub struct SensorRead<B> {
    endpoint: EndpointContext,
    origin: Origin,
    sensor: ODataId,
    /// The requested location string. Generated subject matchers canonicalize
    /// it before deriving identity, which never comes from the payload's own
    /// claim.
    location: String,
    bmc: Arc<B>,
}

impl<B> fmt::Debug for SensorRead<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // A requested URI may carry query credentials, and a transport may
        // own authentication material. Scheduling identity is sufficient to
        // identify this task without exposing either one.
        f.debug_struct("SensorRead")
            .field("endpoint_id", &self.endpoint.endpoint_id())
            .field("provider", &self.origin.provider())
            .field("request_class", &self.origin.request_class())
            .field("sensor", &"<redacted>")
            .finish_non_exhaustive()
    }
}

impl<B> Clone for SensorRead<B> {
    fn clone(&self) -> Self {
        Self {
            endpoint: self.endpoint.clone(),
            origin: self.origin.clone(),
            sensor: self.sensor.clone(),
            location: self.location.clone(),
            bmc: Arc::clone(&self.bmc),
        }
    }
}

impl<B> SensorRead<B> {
    /// Provider identity, as `Origin.provider` carries it.
    pub const PROVIDER: &'static str = "redfish.sensor.odata";

    /// Request class, as dispatcher lanes and breakers key it.
    pub const REQUEST_CLASS: &'static str = "sensor-read";

    /// This provider's declaration, single-sourced from the same constants
    /// its `Origin` is built from, so the plan and the wire always name the
    /// same identity.
    #[must_use]
    pub fn declaration() -> ProviderDeclaration {
        ProviderDeclaration::polled(Self::PROVIDER, Self::REQUEST_CLASS, 1)
    }

    /// A read of `sensor` on the endpoint `bmc` reaches.
    ///
    /// # Panics
    ///
    /// Never in practice: the origin is built from this provider's own
    /// constants, and the unit test below pins that they satisfy the
    /// origin's bounds.
    #[must_use]
    pub fn new(endpoint: EndpointContext, sensor: ODataId, bmc: Arc<B>) -> Self {
        let origin = Origin::builder()
            .provider(Self::PROVIDER)
            .request_class(Self::REQUEST_CLASS)
            .build()
            .expect("the provider's own constants satisfy the origin's bounds");
        let location = sensor.to_string();
        Self {
            endpoint,
            origin,
            sensor,
            location,
            bmc,
        }
    }

    /// Assembles the batches the projection's parts call for: a readings
    /// batch when a descriptor exists (zero or one sample — a descriptor
    /// with no sample is the null-reading story, and the sample-key rule
    /// holds trivially), a states batch when there are observations, and no
    /// batch at all otherwise.
    fn assemble(parts: SensorParts) -> Result<AcquisitionParts, Invalid> {
        let mut payloads = Vec::new();
        let coverage = Coverage::builder()
            .completeness(Completeness::Partial)
            .build()?;
        // Samples without descriptors cannot be silently dropped: either the
        // payload builder accepts them or its refusal surfaces as the
        // residual tier — never a reading that vanishes.
        if !parts.signal_descriptors.is_empty() || !parts.readings.is_empty() {
            let readings = Readings::builder()
                .descriptors(parts.signal_descriptors)
                .samples(parts.readings)
                .build()?;
            payloads.push((coverage.clone(), Payload::Readings(readings)));
        }
        if !parts.state_observations.is_empty() {
            let states = States::builder()
                .observations(parts.state_observations)
                .build()?;
            payloads.push((coverage, Payload::States(states)));
        }
        Ok(AcquisitionParts::new(payloads, parts.issues))
    }
}

/// A projection or assembly failure past the triage tiers is this crate's
/// bug: an operational fact for the status stream, never device data.
fn internal_bug(error: &Invalid) -> AcquisitionFailure {
    AcquisitionFailure::new(AcquisitionFailureClass::Internal)
        .with_retryable(false)
        .with_detail(format!("projection bug: {error}"))
}

impl<B> Acquire for SensorRead<B>
where
    B: Bmc,
    B::Error: ClassifyError,
{
    type Output = AcquisitionParts;

    fn endpoint(&self) -> &EndpointContext {
        &self.endpoint
    }

    fn origin(&self) -> &Origin {
        &self.origin
    }

    async fn perform(&self) -> Result<AcquisitionParts, AcquisitionFailure> {
        let sensor = self
            .bmc
            .get::<Sensor>(&self.sensor)
            .await
            .map_err(|error| error.classify())?;
        let parts =
            project_sensor(&sensor, &self.location).map_err(|error| internal_bug(&error))?;
        Self::assemble(parts).map_err(|error| internal_bug(&error))
    }
}

/// One chassis, read over one endpoint's `Bmc`.
///
/// The same dispatched-leaf shape as [`SensorRead`]: the planner names the
/// chassis URI, this type GETs the document, projects it, and assembles an
/// inventory batch and a states batch.
pub struct ChassisRead<B> {
    endpoint: EndpointContext,
    origin: Origin,
    chassis: ODataId,
    /// The requested location string; also the emitted item's provenance,
    /// canonicalized by the generated projection.
    location: String,
    bmc: Arc<B>,
}

impl<B> fmt::Debug for ChassisRead<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Same redaction story as `SensorRead`: scheduling identity only.
        f.debug_struct("ChassisRead")
            .field("endpoint_id", &self.endpoint.endpoint_id())
            .field("provider", &self.origin.provider())
            .field("request_class", &self.origin.request_class())
            .field("chassis", &"<redacted>")
            .finish_non_exhaustive()
    }
}

impl<B> Clone for ChassisRead<B> {
    fn clone(&self) -> Self {
        Self {
            endpoint: self.endpoint.clone(),
            origin: self.origin.clone(),
            chassis: self.chassis.clone(),
            location: self.location.clone(),
            bmc: Arc::clone(&self.bmc),
        }
    }
}

impl<B> ChassisRead<B> {
    /// Provider identity, as `Origin.provider` carries it.
    pub const PROVIDER: &'static str = "redfish.chassis.odata";

    /// Request class, as dispatcher lanes and breakers key it.
    pub const REQUEST_CLASS: &'static str = "chassis-read";

    /// This provider's declaration, single-sourced from the same constants
    /// its `Origin` is built from.
    #[must_use]
    pub fn declaration() -> ProviderDeclaration {
        ProviderDeclaration::polled(Self::PROVIDER, Self::REQUEST_CLASS, 1)
    }

    /// A read of `chassis` on the endpoint `bmc` reaches.
    ///
    /// # Panics
    ///
    /// Never in practice: the origin is built from this provider's own
    /// constants, and the unit test below pins that they satisfy the
    /// origin's bounds.
    #[must_use]
    pub fn new(endpoint: EndpointContext, chassis: ODataId, bmc: Arc<B>) -> Self {
        let origin = Origin::builder()
            .provider(Self::PROVIDER)
            .request_class(Self::REQUEST_CLASS)
            .build()
            .expect("the provider's own constants satisfy the origin's bounds");
        let location = chassis.to_string();
        Self {
            endpoint,
            origin,
            chassis,
            location,
            bmc,
        }
    }

    /// Assembles the batches the projection's parts call for: an inventory
    /// batch when the item emitted, a states batch when there are
    /// observations, and no batch at all otherwise. Coverage is partial:
    /// one chassis of many, so an absent item never implies removal.
    fn assemble(parts: ChassisParts) -> Result<AcquisitionParts, Invalid> {
        let mut payloads = Vec::new();
        let coverage = Coverage::builder()
            .completeness(Completeness::Partial)
            .build()?;
        if !parts.inventory_items.is_empty() {
            let inventory = Inventory::builder().items(parts.inventory_items).build()?;
            payloads.push((coverage.clone(), Payload::Inventory(inventory)));
        }
        if !parts.state_observations.is_empty() {
            let states = States::builder()
                .observations(parts.state_observations)
                .build()?;
            payloads.push((coverage, Payload::States(states)));
        }
        Ok(AcquisitionParts::new(payloads, parts.issues))
    }
}

impl<B> Acquire for ChassisRead<B>
where
    B: Bmc,
    B::Error: ClassifyError,
{
    type Output = AcquisitionParts;

    fn endpoint(&self) -> &EndpointContext {
        &self.endpoint
    }

    fn origin(&self) -> &Origin {
        &self.origin
    }

    async fn perform(&self) -> Result<AcquisitionParts, AcquisitionFailure> {
        let chassis = self
            .bmc
            .get::<Chassis>(&self.chassis)
            .await
            .map_err(|error| error.classify())?;
        let parts =
            project_chassis(&chassis, &self.location).map_err(|error| internal_bug(&error))?;
        Self::assemble(parts).map_err(|error| internal_bug(&error))
    }
}

#[cfg(test)]
mod tests {
    use std::fmt;
    use std::sync::Arc;

    use nv_telemetry_model::EndpointContext;
    use nv_telemetry_model::Origin;
    use nv_telemetry_model::StateObservation;
    use nv_telemetry_source::AcquisitionFailureClass;
    use nv_telemetry_source::AcquisitionMode;

    use super::internal_bug;
    use super::ChassisRead;
    use super::SensorRead;

    struct NonCloneBmc;

    struct SensitiveBmc;

    impl fmt::Debug for SensitiveBmc {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("transport-secret")
        }
    }

    fn assert_clone<T: Clone>() {}

    #[test]
    fn sharing_a_read_does_not_require_the_transport_to_be_clone() {
        assert_clone::<SensorRead<NonCloneBmc>>();
        assert_clone::<ChassisRead<NonCloneBmc>>();
    }

    #[test]
    fn debug_exposes_only_scheduling_identity() {
        let read = SensorRead::new(
            EndpointContext::builder()
                .endpoint_id("endpoint-a")
                .build()
                .expect("a valid endpoint"),
            "/redfish/v1/Chassis/1/Sensors/CPU?token=query-secret"
                .to_owned()
                .into(),
            Arc::new(SensitiveBmc),
        );

        let rendered = format!("{read:?}");
        assert!(rendered.contains("endpoint-a"));
        assert!(rendered.contains(SensorRead::<SensitiveBmc>::PROVIDER));
        assert!(!rendered.contains("/redfish/"));
        assert!(!rendered.contains("query-secret"));
        assert!(!rendered.contains("transport-secret"));
    }

    #[test]
    fn the_declaration_names_the_origins_identity() {
        // The planner selects by declaration and the wire carries the
        // origin; this pin keeps the two from ever naming different
        // providers.
        let declaration = SensorRead::<()>::declaration();
        assert_eq!(declaration.provider(), SensorRead::<()>::PROVIDER);
        assert_eq!(declaration.request_class(), SensorRead::<()>::REQUEST_CLASS);
        assert_eq!(declaration.mode(), AcquisitionMode::Polled);
        assert_eq!(declaration.cost(), 1);
    }

    #[test]
    fn the_chassis_declaration_names_the_origins_identity() {
        let declaration = ChassisRead::<()>::declaration();
        assert_eq!(declaration.provider(), ChassisRead::<()>::PROVIDER);
        assert_eq!(
            declaration.request_class(),
            ChassisRead::<()>::REQUEST_CLASS
        );
        assert_eq!(declaration.mode(), AcquisitionMode::Polled);
        assert_eq!(declaration.cost(), 1);
    }

    #[test]
    fn chassis_debug_exposes_only_scheduling_identity() {
        let read = ChassisRead::new(
            EndpointContext::builder()
                .endpoint_id("endpoint-a")
                .build()
                .expect("a valid endpoint"),
            "/redfish/v1/Chassis/1U?token=query-secret"
                .to_owned()
                .into(),
            Arc::new(SensitiveBmc),
        );

        let rendered = format!("{read:?}");
        assert!(rendered.contains("endpoint-a"));
        assert!(rendered.contains(ChassisRead::<SensitiveBmc>::PROVIDER));
        assert!(!rendered.contains("/redfish/"));
        assert!(!rendered.contains("query-secret"));
        assert!(!rendered.contains("transport-secret"));
    }

    #[test]
    fn the_providers_constants_satisfy_the_origins_bounds() {
        // Both `new` constructors build their origins with `expect` on this
        // exact invariant; this is the pin that keeps those expects
        // unreachable.
        let origin = Origin::builder()
            .provider(SensorRead::<()>::PROVIDER)
            .request_class(SensorRead::<()>::REQUEST_CLASS)
            .build()
            .expect("provider constants are valid origin fields");
        assert_eq!(origin.provider(), "redfish.sensor.odata");
        assert_eq!(origin.request_class(), "sensor-read");

        let origin = Origin::builder()
            .provider(ChassisRead::<()>::PROVIDER)
            .request_class(ChassisRead::<()>::REQUEST_CLASS)
            .build()
            .expect("provider constants are valid origin fields");
        assert_eq!(origin.provider(), "redfish.chassis.odata");
        assert_eq!(origin.request_class(), "chassis-read");
    }

    #[test]
    fn a_synthetic_plan_model_disagreement_reaches_the_internal_tripwire() {
        // Compilation proves every supported projection plan covers required
        // fields. Bypass that boundary deliberately to pin the one residual
        // tier: if generated assembly and the model ever disagree, the
        // refusal is operational Internal, never a device projection issue.
        let mismatch = StateObservation::builder()
            .build()
            .expect_err("an observation without its planned fields is invalid");
        let failure = internal_bug(&mismatch);

        assert_eq!(failure.class(), AcquisitionFailureClass::Internal);
        assert_eq!(failure.retryable(), Some(false));
        assert!(
            failure
                .detail()
                .is_some_and(|detail| detail.starts_with("projection bug: ")),
            "the mismatch remains operator-visible: {failure:?}"
        );
    }
}
