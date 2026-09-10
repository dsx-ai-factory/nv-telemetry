// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The reads a plan dispatches. One envelope, [`Read`], carries what every
//! read shares — the endpoint it is bound to, the origin its batches carry,
//! a `Debug` that exposes scheduling identity only — and a [`ReadKind`]
//! supplies what varies: the provider's identity and how one target becomes
//! parts. Single documents are one `OData` GET each, up to two batches; a
//! log service is a walk over its entries collection.

use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use nv_redfish::core::ODataId;
use nv_redfish::schema::chassis::Chassis;
use nv_redfish::schema::log_service::LogService;
use nv_redfish::schema::sensor::Sensor;
use nv_redfish::Bmc;
use nv_telemetry_model::limits::LOGS_RECORDS_MAX_ITEMS;
use nv_telemetry_model::Completeness;
use nv_telemetry_model::Coverage;
use nv_telemetry_model::EndpointContext;
use nv_telemetry_model::Invalid;
use nv_telemetry_model::Inventory;
use nv_telemetry_model::LogRecord;
use nv_telemetry_model::Logs;
use nv_telemetry_model::Origin;
use nv_telemetry_model::Payload;
use nv_telemetry_model::Readings;
use nv_telemetry_model::States;
use nv_telemetry_model::Subject;
use nv_telemetry_source::Acquire;
use nv_telemetry_source::AcquisitionFailure;
use nv_telemetry_source::AcquisitionFailureClass;
use nv_telemetry_source::AcquisitionParts;
use nv_telemetry_source::ProjectionIssue;
use nv_telemetry_source::ProviderDeclaration;

use crate::failure::ClassifyError;
use crate::projection::project_chassis;
use crate::projection::project_log_entry;
use crate::projection::project_sensor;
use crate::projection::ChassisParts;
use crate::projection::SensorParts;
use crate::uri;

/// The locator of the issue a log walk records when its budget stops it
/// short: a fact about the walk, not about any source field.
pub const TRUNCATED_WALK_LOCATOR: &str = "@truncated";

mod sealed {
    pub trait Sealed {}
}

/// What one kind of read is: its provider identity, and how a target on an
/// endpoint becomes acquisition parts.
///
/// Implementations are this crate's unit types — the trait is sealed, so
/// the origin bounds every kind's constants must satisfy are pinned by the
/// tests below and nowhere else. [`Read`] carries the state. The hook
/// receives the transport, the target, and the requested location string,
/// which generated subject matchers canonicalize before deriving identity,
/// so identity never comes from the payload's own claim.
pub trait ReadKind: sealed::Sealed + Send + Sync + 'static {
    /// Provider identity, as `Origin.provider` carries it.
    const PROVIDER: &'static str;

    /// Request class, as dispatcher lanes and breakers key it.
    const REQUEST_CLASS: &'static str;

    /// Performs the read: fetch, project, assemble.
    fn acquire<B>(
        bmc: &B,
        target: &ODataId,
        location: &str,
    ) -> impl Future<Output = Result<AcquisitionParts, AcquisitionFailure>> + Send
    where
        B: Bmc,
        B::Error: ClassifyError;
}

/// One dispatched leaf: the planner names the target, the dispatcher decides
/// when this runs, and the kind knows how. Generic over the transport so the
/// same provider runs against HTTP and against the mock the corpus replays
/// through.
pub struct Read<B, K> {
    endpoint: EndpointContext,
    origin: Origin,
    target: ODataId,
    /// The requested location string, as the kind's projection expects it.
    location: String,
    bmc: Arc<B>,
    kind: PhantomData<fn() -> K>,
}

/// One sensor, read over one endpoint's `Bmc`: readings and states.
pub type SensorRead<B> = Read<B, SensorKind>;

/// One chassis, read over one endpoint's `Bmc`: inventory and states.
pub type ChassisRead<B> = Read<B, ChassisKind>;

/// One log service's entries, read over one endpoint's `Bmc`: log records.
pub type LogRead<B> = Read<B, LogKind>;

impl<B, K: ReadKind> fmt::Debug for Read<B, K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // A requested URI may carry query credentials, and a transport may
        // own authentication material. Scheduling identity is sufficient to
        // identify this task without exposing either one; the provider names
        // the kind.
        f.debug_struct("Read")
            .field("endpoint_id", &self.endpoint.endpoint_id())
            .field("provider", &self.origin.provider())
            .field("request_class", &self.origin.request_class())
            .field("target", &"<redacted>")
            .finish_non_exhaustive()
    }
}

// Sharing a read must not require the transport to be `Clone`.
impl<B, K> Clone for Read<B, K> {
    fn clone(&self) -> Self {
        Self {
            endpoint: self.endpoint.clone(),
            origin: self.origin.clone(),
            target: self.target.clone(),
            location: self.location.clone(),
            bmc: Arc::clone(&self.bmc),
            kind: PhantomData,
        }
    }
}

impl<B, K: ReadKind> Read<B, K> {
    /// Provider identity, as `Origin.provider` carries it.
    pub const PROVIDER: &'static str = K::PROVIDER;

    /// Request class, as dispatcher lanes and breakers key it.
    pub const REQUEST_CLASS: &'static str = K::REQUEST_CLASS;

    /// This provider's declaration, single-sourced from the same constants
    /// its `Origin` is built from, so the plan and the wire always name the
    /// same identity.
    #[must_use]
    pub fn declaration() -> ProviderDeclaration {
        ProviderDeclaration::polled(K::PROVIDER, K::REQUEST_CLASS, 1)
    }

    /// A read of `target` on the endpoint `bmc` reaches.
    ///
    /// # Panics
    ///
    /// Never in practice: the origin is built from the kind's own constants,
    /// the trait is sealed, and the unit test below pins that every kind's
    /// constants satisfy the origin's bounds.
    #[must_use]
    pub fn new(endpoint: EndpointContext, target: ODataId, bmc: Arc<B>) -> Self {
        let origin = Origin::builder()
            .provider(K::PROVIDER)
            .request_class(K::REQUEST_CLASS)
            .build()
            .expect("the kind's constants satisfy the origin's bounds");
        let location = target.to_string();
        Self {
            endpoint,
            origin,
            target,
            location,
            bmc,
            kind: PhantomData,
        }
    }
}

impl<B, K> Acquire for Read<B, K>
where
    B: Bmc,
    B::Error: ClassifyError,
    K: ReadKind,
{
    type Output = AcquisitionParts;

    fn endpoint(&self) -> &EndpointContext {
        &self.endpoint
    }

    fn origin(&self) -> &Origin {
        &self.origin
    }

    async fn perform(&self) -> Result<AcquisitionParts, AcquisitionFailure> {
        K::acquire(self.bmc.as_ref(), &self.target, &self.location).await
    }
}

/// A projection or assembly failure past the triage tiers is this crate's
/// bug: an operational fact for the status stream, never device data.
fn internal_bug(error: &Invalid) -> AcquisitionFailure {
    AcquisitionFailure::new(AcquisitionFailureClass::Internal)
        .with_retryable(false)
        .with_detail(format!("projection bug: {error}"))
}

/// Every read here covers one resource of many, so an absence never implies
/// removal.
fn partial_coverage() -> Result<Coverage, Invalid> {
    Coverage::builder()
        .completeness(Completeness::Partial)
        .build()
}

/// GET the sensor document, project it, assemble the batches.
#[derive(Debug)]
#[non_exhaustive]
pub struct SensorKind;

impl sealed::Sealed for SensorKind {}

impl ReadKind for SensorKind {
    const PROVIDER: &'static str = "redfish.sensor.odata";
    const REQUEST_CLASS: &'static str = "sensor-read";

    async fn acquire<B>(
        bmc: &B,
        target: &ODataId,
        location: &str,
    ) -> Result<AcquisitionParts, AcquisitionFailure>
    where
        B: Bmc,
        B::Error: ClassifyError,
    {
        let sensor = bmc
            .get::<Sensor>(target)
            .await
            .map_err(|error| error.classify())?;
        let parts = project_sensor(&sensor, location).map_err(|error| internal_bug(&error))?;
        assemble_sensor(parts).map_err(|error| internal_bug(&error))
    }
}

/// A readings batch when a descriptor exists (zero or one sample — a
/// descriptor with no sample is the null-reading story, and the sample-key
/// rule holds trivially), a states batch when there are observations, and
/// no batch at all otherwise.
fn assemble_sensor(parts: SensorParts) -> Result<AcquisitionParts, Invalid> {
    let mut payloads = Vec::new();
    let coverage = partial_coverage()?;
    // Samples without descriptors cannot be silently dropped: either the
    // payload builder accepts them or its refusal surfaces as the residual
    // tier — never a reading that vanishes.
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

/// GET the chassis document, project it, assemble the batches. The requested
/// location is also the emitted item's provenance, canonicalized by the
/// generated projection.
#[derive(Debug)]
#[non_exhaustive]
pub struct ChassisKind;

impl sealed::Sealed for ChassisKind {}

impl ReadKind for ChassisKind {
    const PROVIDER: &'static str = "redfish.chassis.odata";
    const REQUEST_CLASS: &'static str = "chassis-read";

    async fn acquire<B>(
        bmc: &B,
        target: &ODataId,
        location: &str,
    ) -> Result<AcquisitionParts, AcquisitionFailure>
    where
        B: Bmc,
        B::Error: ClassifyError,
    {
        let chassis = bmc
            .get::<Chassis>(target)
            .await
            .map_err(|error| error.classify())?;
        let parts = project_chassis(&chassis, location).map_err(|error| internal_bug(&error))?;
        assemble_chassis(parts).map_err(|error| internal_bug(&error))
    }
}

/// An inventory batch when the item emitted, a states batch when there are
/// observations, and no batch at all otherwise.
fn assemble_chassis(parts: ChassisParts) -> Result<AcquisitionParts, Invalid> {
    let mut payloads = Vec::new();
    let coverage = partial_coverage()?;
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

/// Walk a log service: GET the service, its entries collection, and each
/// member the collection did not carry expanded inline — a `NavProperty`
/// already expanded resolves without I/O — and project every entry to at
/// most one record.
///
/// A walk has element semantics a single document does not. Each entry's
/// issues are prefixed `Members[i]`, so two entries with one fault stay two
/// facts, and each entry projects at its own location. A member the device
/// answered for but would not serve — rotated out between the collection
/// and the member GET, say — is recorded against `Members[i]` and the walk
/// continues. Anything else ends the walk and discards what it had
/// projected: an endpoint-scoped failure indicts the endpoint rather than one
/// entry, and the collector's own fault is never device data. The
/// acquisition contract is all-or-nothing for the unit, so a recurring
/// per-member timeout on a long log means the log never ships and the
/// endpoint breaker samples the timeout.
///
/// The walk is budgeted ([`WalkBudget`]): it holds the endpoint's admission
/// slot and buffers projected records for its duration. The dispatcher meters
/// it as one unit of cost. A deadline cancels pending I/O, and the member cap
/// bounds projected records; neither bounds the collection response decoded
/// by the transport. The Bmc implementation must provide response-size limits;
/// nv-redfish 0.16's typed API exposes no such limit here. What the
/// member cap cuts off is reported at [`TRUNCATED_WALK_LOCATOR`]; deadline
/// expiry fails the acquisition with Timeout and discards its output. A
/// successful batch is `PARTIAL`, and the next poll starts over. Records ride
/// one `Logs` batch per bound's worth; the issues envelope's own bound is kept by
/// `AcquisitionParts`.
///
/// The batch's `Coverage.scope` names the service — kind `log-service`,
/// scoped by the resource that owns it, identified by its `Id` — which is
/// the namespace of every record's `entry_id`: two services on one endpoint
/// both number their entries from 1.
///
/// Only the collection's first page is walked: nv-redfish 0.16 surfaces no
/// `Members@odata.nextLink`, so a paged log is truncated silently. Recorded
/// in the plan as the upstream follow-up it is.
#[derive(Debug)]
#[non_exhaustive]
pub struct LogKind;

impl sealed::Sealed for LogKind {}

/// What one log walk may spend: members visited and wall-clock time. The
/// members bound also bounds buffering, since every visited member holds at
/// most one record and a few issues until the walk ends.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WalkBudget {
    members: usize,
    elapsed: Duration,
}

impl WalkBudget {
    /// The budget every log walk runs under today. Kinds are unit types, so
    /// the budget is a constant; a per-service budget is a `Read` carrying
    /// kind state, when a deployment asks for one.
    pub const DEFAULT: Self = Self::new(1024, Duration::from_secs(30));

    #[must_use]
    pub const fn new(members: usize, elapsed: Duration) -> Self {
        Self { members, elapsed }
    }

    /// Why the walk stops before its next member, if it does.
    fn exhausted(self, visited: usize, elapsed: Duration) -> Option<&'static str> {
        if visited >= self.members {
            Some("member budget spent")
        } else if elapsed >= self.elapsed {
            Some("time budget spent")
        } else {
            None
        }
    }
}

impl ReadKind for LogKind {
    const PROVIDER: &'static str = "redfish.log-service.odata";
    const REQUEST_CLASS: &'static str = "log-read";

    async fn acquire<B>(
        bmc: &B,
        target: &ODataId,
        location: &str,
    ) -> Result<AcquisitionParts, AcquisitionFailure>
    where
        B: Bmc,
        B::Error: ClassifyError,
    {
        with_deadline(WalkBudget::DEFAULT.elapsed, async {
            let started = Instant::now();
            let service = bmc
                .get::<LogService>(target)
                .await
                .map_err(|error| error.classify())?;
            // A service without an entries collection cannot serve what was
            // asked of it: a request-scoped fact about this resource, not a
            // document field that failed to project.
            let Some(entries) = &service.entries else {
                return Err(
                    AcquisitionFailure::new(AcquisitionFailureClass::Unsupported)
                        .with_retryable(false)
                        .with_detail("the log service carries no Entries collection"),
                );
            };
            let scope = service_scope(location, &service.base.id)?;
            let collection = entries.get(bmc).await.map_err(|error| error.classify())?;
            let mut records = Vec::new();
            let mut issues = Vec::new();
            for (index, member) in collection.members.iter().enumerate() {
                if let Some(reason) = WalkBudget::DEFAULT.exhausted(index, started.elapsed()) {
                    issues.push(ProjectionIssue::invalid(
                        TRUNCATED_WALK_LOCATOR,
                        format!(
                            "walk stopped at member {index} of {}: {reason}",
                            collection.members.len()
                        ),
                    ));
                    break;
                }
                let entry = match member.get(bmc).await {
                    Ok(entry) => entry,
                    Err(error) => {
                        issues.push(member_disposition(index, error.classify())?);
                        continue;
                    }
                };
                let location = member.id().to_string();
                let parts =
                    project_log_entry(&entry, &location).map_err(|error| internal_bug(&error))?;
                records.extend(parts.log_records);
                issues.extend(
                    parts
                        .issues
                        .into_iter()
                        .map(|issue| issue.at_index("Members", index)),
                );
            }
            assemble_logs(records, issues, scope).map_err(|error| internal_bug(&error))
        })
        .await
    }
}

/// Cancels the whole acquisition, including initial GETs, when its deadline
/// expires. The timer is executor-independent. As with any async timeout, a
/// transport must yield; synchronous decoding cannot be preempted here.
async fn with_deadline<T>(
    elapsed: Duration,
    work: impl Future<Output = Result<T, AcquisitionFailure>>,
) -> Result<T, AcquisitionFailure> {
    let mut timer = std::pin::pin!(futures_timer::Delay::new(elapsed));
    let mut work = std::pin::pin!(work);
    std::future::poll_fn(|cx| {
        if timer.as_mut().poll(cx).is_ready() {
            return std::task::Poll::Ready(Err(AcquisitionFailure::new(
                AcquisitionFailureClass::Timeout,
            )
            .with_retryable(true)
            .with_detail("log walk deadline exceeded")));
        }
        work.as_mut().poll(cx)
    })
    .await
}

/// Log-service identity retains the owner's collection and local id. Unknown
/// location grammars cannot supply a safe deduplication namespace.
fn service_scope(location: &str, service_id: &str) -> Result<Subject, AcquisitionFailure> {
    let (kind, owner) = uri::log_service_owner(location).ok_or_else(|| {
        AcquisitionFailure::new(AcquisitionFailureClass::Protocol)
            .with_retryable(false)
            .with_detail("log service owner cannot be resolved from requested location")
    })?;
    Subject::builder()
        .kind("log-service")
        .id(service_id)
        .scope(vec![kind.to_owned(), owner.to_owned()])
        .build()
        .map_err(|_| {
            AcquisitionFailure::new(AcquisitionFailureClass::Protocol)
                .with_retryable(false)
                .with_detail("log service identity violates subject bounds")
        })
}

/// What one member's failure does to the walk: an answer the device gave
/// about that member is recorded against it, with its classification; a
/// failure to reach the endpoint, or the collector's own fault, ends the
/// walk as the unit's failure.
fn member_disposition(
    index: usize,
    failure: AcquisitionFailure,
) -> Result<ProjectionIssue, AcquisitionFailure> {
    if !failure.class().is_device_answer() {
        return Err(failure);
    }
    Ok(ProjectionIssue::invalid(
        format!("Members[{index}]"),
        format!(
            "member not read ({:?}): {}",
            failure.class(),
            failure.detail().unwrap_or("no detail")
        ),
    ))
}

/// One `Logs` batch per bound's worth of records, none when nothing
/// projected. Coverage is partial and scoped to the service: one service of
/// many, the entries a device has already rotated out are not an absence to
/// report, and the scope is the namespace of every record's `entry_id`.
fn assemble_logs(
    mut records: Vec<LogRecord>,
    issues: Vec<ProjectionIssue>,
    scope: Subject,
) -> Result<AcquisitionParts, Invalid> {
    let coverage = Coverage::builder()
        .completeness(Completeness::Partial)
        .scope(scope)
        .build()?;
    let mut payloads = Vec::new();
    while !records.is_empty() {
        let rest = records.split_off(records.len().min(LOGS_RECORDS_MAX_ITEMS as usize));
        let logs = Logs::builder().records(records).build()?;
        payloads.push((coverage.clone(), Payload::Logs(logs)));
        records = rest;
    }
    Ok(AcquisitionParts::new(payloads, issues))
}

#[cfg(test)]
mod tests {
    use std::fmt;
    use std::sync::Arc;
    use std::time::Duration;

    use nv_telemetry_model::EndpointContext;
    use nv_telemetry_model::Origin;
    use nv_telemetry_model::StateObservation;
    use nv_telemetry_source::AcquisitionFailure;
    use nv_telemetry_source::AcquisitionFailureClass;
    use nv_telemetry_source::AcquisitionMode;

    use super::internal_bug;
    use super::member_disposition;
    use super::service_scope;
    use super::ChassisKind;
    use super::ChassisRead;
    use super::LogKind;
    use super::LogRead;
    use super::Read;
    use super::ReadKind;
    use super::SensorKind;
    use super::SensorRead;
    use super::WalkBudget;

    struct NonCloneBmc;

    struct SensitiveBmc;

    impl fmt::Debug for SensitiveBmc {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("transport-secret")
        }
    }

    fn assert_clone<T: Clone>() {}

    fn debug_of<K: ReadKind>() -> String {
        let read: Read<SensitiveBmc, K> = Read::new(
            EndpointContext::builder()
                .endpoint_id("endpoint-a")
                .build()
                .expect("a valid endpoint"),
            "/redfish/v1/Chassis/1U?token=query-secret"
                .to_owned()
                .into(),
            Arc::new(SensitiveBmc),
        );
        format!("{read:?}")
    }

    fn assert_declaration_matches_origin<K: ReadKind>(provider: &str, request_class: &str) {
        // The planner selects by declaration and the wire carries the
        // origin; this pin keeps the two from ever naming different
        // providers. `new` builds its origin with `expect` on the bounds
        // this also pins.
        let declaration = Read::<(), K>::declaration();
        assert_eq!(declaration.provider(), provider);
        assert_eq!(declaration.request_class(), request_class);
        assert_eq!(declaration.mode(), AcquisitionMode::Polled);
        assert_eq!(declaration.cost(), 1);
        let origin = Origin::builder()
            .provider(K::PROVIDER)
            .request_class(K::REQUEST_CLASS)
            .build()
            .expect("provider constants are valid origin fields");
        assert_eq!(origin.provider(), provider);
        assert_eq!(origin.request_class(), request_class);
    }

    #[test]
    fn sharing_a_read_does_not_require_the_transport_to_be_clone() {
        assert_clone::<SensorRead<NonCloneBmc>>();
        assert_clone::<ChassisRead<NonCloneBmc>>();
        assert_clone::<LogRead<NonCloneBmc>>();
    }

    #[test]
    fn debug_exposes_only_scheduling_identity() {
        for (rendered, provider) in [
            (debug_of::<SensorKind>(), SensorKind::PROVIDER),
            (debug_of::<ChassisKind>(), ChassisKind::PROVIDER),
            (debug_of::<LogKind>(), LogKind::PROVIDER),
        ] {
            assert!(rendered.contains("endpoint-a"));
            assert!(rendered.contains(provider));
            assert!(!rendered.contains("/redfish/"));
            assert!(!rendered.contains("query-secret"));
            assert!(!rendered.contains("transport-secret"));
        }
    }

    #[test]
    fn every_declaration_names_its_origins_identity() {
        assert_declaration_matches_origin::<SensorKind>("redfish.sensor.odata", "sensor-read");
        assert_declaration_matches_origin::<ChassisKind>("redfish.chassis.odata", "chassis-read");
        assert_declaration_matches_origin::<LogKind>("redfish.log-service.odata", "log-read");
    }

    #[test]
    fn a_devices_answer_about_a_member_is_recorded_and_anything_else_ends_the_walk() {
        let answered = AcquisitionFailure::new(AcquisitionFailureClass::Device)
            .with_retryable(true)
            .with_detail("HTTP 503");
        let issue = member_disposition(3, answered).expect("a device answer is recorded");
        assert_eq!(issue.path(), "Members[3]");
        assert!(format!("{issue:?}").contains("member not read (Device): HTTP 503"));

        for class in [
            AcquisitionFailureClass::Connectivity,
            AcquisitionFailureClass::Authentication,
            AcquisitionFailureClass::Timeout,
            AcquisitionFailureClass::Internal,
        ] {
            let failure = member_disposition(0, AcquisitionFailure::new(class))
                .expect_err("not an answer about the member");
            assert_eq!(failure.class(), class);
        }
    }

    #[tokio::test]
    async fn a_deadline_cancels_a_pending_request_and_drops_its_state() {
        use std::sync::atomic::AtomicBool;
        use std::sync::atomic::Ordering;
        struct Dropped(Arc<AtomicBool>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let dropped = Arc::new(AtomicBool::new(false));
        let guard = Dropped(Arc::clone(&dropped));
        let failure = super::with_deadline(Duration::from_millis(10), async move {
            let _guard = guard;
            std::future::pending::<Result<(), AcquisitionFailure>>().await
        })
        .await
        .expect_err("a pending request is cancelled");
        assert_eq!(failure.class(), AcquisitionFailureClass::Timeout);
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(
            super::with_deadline(Duration::from_secs(1), async { Ok(7) })
                .await
                .expect("ready work"),
            7
        );
    }

    #[test]
    fn the_budget_stops_a_walk_on_members_or_time() {
        let budget = WalkBudget::new(3, Duration::from_secs(10));
        assert_eq!(budget.exhausted(2, Duration::from_secs(1)), None);
        assert_eq!(
            budget.exhausted(3, Duration::from_secs(1)),
            Some("member budget spent")
        );
        assert_eq!(
            budget.exhausted(0, Duration::from_secs(10)),
            Some("time budget spent")
        );
    }

    #[test]
    fn the_service_scope_is_the_owning_resource_and_the_service_id() {
        let scoped = service_scope("/redfish/v1/Systems/1/LogServices/SEL?token=x", "SEL")
            .expect("a valid subject");
        assert_eq!(scoped.kind(), "log-service");
        assert_eq!(scoped.scope(), ["Systems", "1"]);
        assert_eq!(scoped.id(), "SEL");

        let manager =
            service_scope("/redfish/v1/Managers/1/LogServices/SEL", "SEL").expect("manager scope");
        assert_ne!(scoped, manager);
        assert!(service_scope("/redfish/v1/Odd/SEL", "SEL").is_err());
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
