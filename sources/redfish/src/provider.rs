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

use nv_redfish::core::EntityTypeRef;
use nv_redfish::core::NavProperty;
use nv_redfish::core::ODataETag;
use nv_redfish::core::ODataId;
use nv_redfish::schema::chassis::Chassis;
use nv_redfish::schema::log_entry::LogEntry;
use nv_redfish::schema::log_service::LogService;
use nv_redfish::schema::sensor::Sensor;
use nv_redfish::schema::service_root::ServiceRoot;
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
use nv_telemetry_model::Timestamp;
use nv_telemetry_source::Acquire;
use nv_telemetry_source::AcquisitionFailure;
use nv_telemetry_source::AcquisitionFailureClass;
use nv_telemetry_source::AcquisitionParts;
use nv_telemetry_source::ProjectionIssue;
use nv_telemetry_source::ProviderDeclaration;
use serde::Deserialize;

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

/// Locator of the issue a log poll raises when it finds a walk of the same
/// read still in flight and ships nothing rather than queue behind it.
pub const IN_FLIGHT_WALK_LOCATOR: &str = "@in-flight";

/// The locator of the issue a log walk records, once, when the device
/// refuses the `$filter` its service root advertises and the read falls
/// back to reading the collection by `$skip`.
pub const FILTER_REFUSED_LOCATOR: &str = "@filter";

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

    /// What one read keeps between its acquisitions. `()` for a read whose
    /// every acquisition stands alone; a log walk keeps [`LogCursor`], where
    /// its previous walk ended. Shared by every clone of one [`Read`], so the
    /// dispatcher's per-tick clones see one position.
    type State: Default + Send + Sync + 'static;

    /// Performs the read: fetch, project, assemble.
    fn acquire<B>(
        bmc: &B,
        target: &ODataId,
        location: &str,
        state: &Self::State,
    ) -> impl Future<Output = Result<AcquisitionParts, AcquisitionFailure>> + Send
    where
        B: Bmc,
        B::Error: ClassifyError;
}

/// One dispatched leaf: the planner names the target, the dispatcher decides
/// when this runs, and the kind knows how. Generic over the transport so the
/// same provider runs against HTTP and against the mock the corpus replays
/// through.
pub struct Read<B, K: ReadKind> {
    endpoint: EndpointContext,
    origin: Origin,
    target: ODataId,
    /// The requested location string, as the kind's projection expects it.
    location: String,
    bmc: Arc<B>,
    state: Arc<K::State>,
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

// Sharing a read must not require the transport to be `Clone`, and every
// clone shares the kind's state: one position per read, not per clone.
impl<B, K: ReadKind> Clone for Read<B, K> {
    fn clone(&self) -> Self {
        Self {
            endpoint: self.endpoint.clone(),
            origin: self.origin.clone(),
            target: self.target.clone(),
            location: self.location.clone(),
            bmc: Arc::clone(&self.bmc),
            state: Arc::clone(&self.state),
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
            state: Arc::new(K::State::default()),
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
        K::acquire(
            self.bmc.as_ref(),
            &self.target,
            &self.location,
            self.state.as_ref(),
        )
        .await
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
    type State = ();

    async fn acquire<B>(
        bmc: &B,
        target: &ODataId,
        location: &str,
        (): &(),
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
    type State = ();

    async fn acquire<B>(
        bmc: &B,
        target: &ODataId,
        location: &str,
        (): &(),
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

/// Walk a log service: GET the service, its entries collection page by page,
/// and each member the collection did not carry expanded inline — a
/// `NavProperty` already expanded resolves without I/O — and project every
/// entry to at most one record.
///
/// A walk has element semantics a single document does not. Each entry's
/// issues are prefixed `Members[i]`, `i` being the member's position in the
/// whole collection, so two entries with one fault stay two facts, and each
/// entry projects at its own location. A member the device answered for but
/// would not serve — rotated out between the collection and the member GET,
/// say — is recorded against `Members[i]` and the walk continues. Anything
/// else ends the walk and discards what it had projected: an endpoint-scoped
/// failure indicts the endpoint rather than one entry, and the collector's
/// own fault is never device data. The acquisition contract is all-or-nothing
/// for the unit, so a recurring per-member timeout on a long log means the
/// log never ships and the endpoint breaker samples the timeout.
///
/// The walk is budgeted ([`WalkBudget`]): it holds the endpoint's admission
/// slot and buffers projected records for its duration, and the dispatcher
/// meters it as one unit of cost. **The budget keeps the newest entries.**
/// Most logs list entries oldest first, so a walk that spent its budget from
/// the head would never show a consumer the records that arrived since the
/// last poll; instead the walk reads the order off the member ids, visits the
/// newest members first, and when an oldest-first collection is paged jumps
/// to the tail with `$skip` before following `Members@odata.nextLink` to the
/// end. What the budget cuts off is the
/// oldest, reported once at [`TRUNCATED_WALK_LOCATOR`]. The deadline cancels
/// pending I/O and fails the acquisition with Timeout, discarding its output.
/// Neither bound covers the collection response the transport decodes; that
/// is the transport's response-size limit, which nv-redfish 0.16's typed API
/// does not expose here. A successful batch is `PARTIAL`, and the next poll
/// starts where this one ended: the read keeps a [`LogCursor`], so a poll
/// ships the entries that arrived since the last one shipped, and the budget
/// caps how many new entries one poll may carry. Records ride one `Logs`
/// batch per bound's worth; the issues envelope's own bound is kept by
/// `AcquisitionParts`.
///
/// The batch's `Coverage.scope` names the service — kind `log-service`,
/// scoped by the resource that owns it, identified by its `Id` — which is
/// the namespace of every record's `entry_id`: two services on one endpoint
/// both number their entries from 1.
#[derive(Debug)]
#[non_exhaustive]
pub struct LogKind;

impl sealed::Sealed for LogKind {}

/// What one log walk may spend: members visited and wall-clock time. The
/// members bound also bounds buffering, since every visited member holds at
/// most one record and a few issues until the walk ends, and it bounds the
/// page GETs a paged collection costs, one page being at least one member.
/// See [`WalkBudget::deadline`].
const WALK_DEADLINE_HEADROOM: Duration = Duration::from_secs(5);

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

    /// The acquisition's deadline: the time budget plus headroom for the
    /// service GET, the member in flight, and assembly, so a walk that spends
    /// its time budget stops, ships, and advances the cursor instead of being
    /// cut off wholesale and repeating identically next poll.
    fn deadline(self) -> Duration {
        self.elapsed.saturating_add(WALK_DEADLINE_HEADROOM)
    }

    /// Why the walk stops before its next member, if it does.
    fn exhausted(self, visited: usize, elapsed: Duration) -> Option<Stop> {
        if visited >= self.members {
            Some(Stop::Members)
        } else if elapsed >= self.elapsed {
            Some(Stop::Time)
        } else {
            None
        }
    }
}

/// Which budget stopped a walk before its next member.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stop {
    Members,
    Time,
}

impl Stop {
    fn as_str(self) -> &'static str {
        match self {
            Self::Members => "member budget spent",
            Self::Time => "time budget spent",
        }
    }
}

/// One page of an entries collection, read raw: nv-redfish 0.16's typed
/// collection drops `Members@odata.count` and `Members@odata.nextLink`, and
/// without them a paged log is silently its first page. Members keep their
/// `NavProperty` form, so an entry the device expanded inline still resolves
/// without I/O.
#[derive(Debug, Deserialize)]
struct EntryPage {
    #[serde(rename = "@odata.id")]
    odata_id: ODataId,
    /// Kept so the transport's cache can revalidate the page with
    /// `If-None-Match`, as it did for the typed collection.
    #[serde(rename = "@odata.etag")]
    etag: Option<ODataETag>,
    #[serde(rename = "Members")]
    members: Vec<NavProperty<LogEntry>>,
    #[serde(rename = "Members@odata.count")]
    count: Option<u64>,
    #[serde(rename = "Members@odata.nextLink")]
    next_link: Option<String>,
}

impl EntityTypeRef for EntryPage {
    fn odata_id(&self) -> &ODataId {
        &self.odata_id
    }

    fn etag(&self) -> Option<&ODataETag> {
        self.etag.as_ref()
    }
}

/// How a collection lists its members. Redfish leaves the order to the
/// device; most append the newest last.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Order {
    OldestFirst,
    NewestFirst,
}

impl Order {
    fn flipped(self) -> Self {
        match self {
            Self::OldestFirst => Self::NewestFirst,
            Self::NewestFirst => Self::OldestFirst,
        }
    }

    /// The order a page's member ids imply: devices number entries as they
    /// arrive, so ids that rise along the page put the oldest first. The
    /// first run of digits in each id's last segment is compared with the
    /// next, which reads `12`, `1701234567_1`, and `Sel.12` alike, and the
    /// direction most neighbours agree on wins, so a numbering that wraps
    /// once still reads right. Ids without digits, a tie, and a single
    /// member say nothing.
    fn hinted_by(page: &EntryPage) -> Option<Self> {
        let sequence = |member: &NavProperty<LogEntry>| -> Option<u64> {
            let digits: String = member
                .id()
                .last_segment()?
                .chars()
                .skip_while(|c| !c.is_ascii_digit())
                .take_while(char::is_ascii_digit)
                .collect();
            digits.parse().ok()
        };
        let numbers: Vec<u64> = page.members.iter().map(sequence).collect::<Option<_>>()?;
        let (rising, falling) =
            numbers
                .windows(2)
                .fold((0usize, 0usize), |(rising, falling), pair| {
                    match pair[0].cmp(&pair[1]) {
                        std::cmp::Ordering::Less => (rising + 1, falling),
                        std::cmp::Ordering::Greater => (rising, falling + 1),
                        std::cmp::Ordering::Equal => (rising, falling),
                    }
                });
        match rising.cmp(&falling) {
            std::cmp::Ordering::Greater => Some(Self::OldestFirst),
            std::cmp::Ordering::Less => Some(Self::NewestFirst),
            std::cmp::Ordering::Equal => None,
        }
    }
}

/// How a walk settles the collection's order before reading it.
#[derive(Clone, Copy, Debug)]
enum OrderChoice {
    /// From the ids when they number the entries; else what the cursor
    /// remembers, which a probe may have established and which then
    /// outranks the ids; else oldest first.
    Detect {
        remembered: Option<Order>,
        probed: bool,
    },
    /// A probe just showed the other end is the newer one.
    Force(Order),
}

/// The pages a walk read, in collection order, the first of them starting
/// at `offset` in a collection of `total` members listed in `order`. Pages
/// stay shared with the transport's cache; the walk borrows members from
/// them. `incomplete` says why the pages stopped before the collection's
/// last one, if they did. The first page is kept for the order probe, which
/// needs the collection's head whatever the walk jumped to.
struct EntryWindow {
    pages: Vec<Arc<EntryPage>>,
    first_page: Arc<EntryPage>,
    offset: usize,
    total: usize,
    order: Order,
    /// The order was established by a probe rather than read or assumed.
    probed: bool,
    incomplete: Option<&'static str>,
    /// `offset` is where the cursor's member was expected, not what the
    /// budget left: the members before it are presumed shipped, and a walk
    /// that does not find the cursor there reads them after all.
    cursor_jump: bool,
    /// The pages are the device's answer to `$filter`, not the collection:
    /// `total` counts the members that matched, and nothing about the
    /// collection's size can be read off them.
    filtered: bool,
}

/// Which request opened the window.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Start {
    /// The collection itself, from its head.
    Head,
    /// The collection's `$filter` page, by this id.
    Filtered(ODataId),
}

/// Why a walk's pages stopped before the collection's last one.
const PAGES_PAST_BUDGET: &str = "member budget spent before the last page";
const PAGES_PAST_BUDGET_FROM_START: &str =
    "member budget spent before the last page; the members kept are the collection's first, not its newest";
const NEXT_LINK_UNRESOLVED: &str = "the collection's nextLink could not be resolved";
const NEXT_LINK_LOOPS: &str = "the collection's nextLink returned to a page already read";
const EMPTY_PAGE: &str = "the collection answered an empty page";

impl EntryWindow {
    fn members_read(&self) -> usize {
        self.pages.iter().map(|page| page.members.len()).sum()
    }

    /// The members read, newest first, each with its position in the whole
    /// collection.
    fn newest_first(&self) -> Vec<(usize, &NavProperty<LogEntry>)> {
        let members: Vec<(usize, &NavProperty<LogEntry>)> = self
            .pages
            .iter()
            .flat_map(|page| page.members.iter())
            .enumerate()
            .map(|(position, member)| (self.offset + position, member))
            .collect();
        match self.order {
            Order::OldestFirst => members.into_iter().rev().collect(),
            Order::NewestFirst => members,
        }
    }

    /// The member at the end the walk did not treat as newest, with its
    /// position, when the window reaches that end.
    fn other_end(&self) -> Option<(usize, &NavProperty<LogEntry>)> {
        match self.order {
            Order::OldestFirst => self.first_page.members.first().map(|member| (0, member)),
            Order::NewestFirst if self.incomplete.is_none() => self
                .pages
                .last()
                .and_then(|page| page.members.last())
                .map(|member| (self.offset + self.members_read() - 1, member)),
            Order::NewestFirst => None,
        }
    }
}

/// A `Members@odata.nextLink` as the id the transport resolves against the
/// endpoint. Devices write it as an absolute path; a URL, absolute or
/// protocol-relative, is reduced to its path and query, since the transport
/// only ever addresses the endpoint; and a relative reference is resolved
/// against the collection's own path as RFC 3986 does, so `Entries?$skip=50`
/// and `?$skip=50` both name the next page.
fn next_page_id(entries: &ODataId, link: &str) -> Option<ODataId> {
    let resolved = if let Some((_, rest)) = link.split_once("://") {
        rest[rest.find('/')?..].to_owned()
    } else if let Some(rest) = link.strip_prefix("//") {
        rest[rest.find('/')?..].to_owned()
    } else if link.starts_with('/') {
        link.to_owned()
    } else if link.is_empty() {
        return None;
    } else {
        let base = entries.to_string();
        let path = base.split_once('?').map_or(base.as_str(), |(path, _)| path);
        if link.starts_with('?') {
            format!("{path}{link}")
        } else {
            format!("{}{link}", &path[..=path.rfind('/')?])
        }
    };
    Some(ODataId::from(resolved))
}

/// The `$skip` page of `entries`, joined onto any query the id already carries.
fn skip_page_id(entries: &ODataId, skip: usize) -> ODataId {
    with_query_option(entries, &format!("$skip={skip}"))
}

/// The `$filter` page of `entries`: the members stamped at or after `since`,
/// the cursor's own second included so its member is there to anchor the
/// walk. The instant is rendered to the second, as devices stamp, and quoted,
/// as nv-redfish's filter builder quotes every string literal; a device that
/// wants the bare `OData` form refuses with 400 and is remembered as not
/// filtering.
fn filter_page_id(entries: &ODataId, since: Timestamp) -> ODataId {
    with_query_option(
        entries,
        &format!("$filter=Created ge '{}'", rfc3339_second(since)),
    )
}

fn with_query_option(entries: &ODataId, option: &str) -> ODataId {
    let base = entries.to_string();
    let separator = if base.contains('?') { '&' } else { '?' };
    ODataId::from(format!("{base}{separator}{option}"))
}

/// `at` as the UTC `Created` literal a device compares against, to the second.
fn rfc3339_second(at: Timestamp) -> String {
    let instant = time::OffsetDateTime::from_unix_timestamp(at.seconds())
        .unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        instant.year(),
        u8::from(instant.month()),
        instant.day(),
        instant.hour(),
        instant.minute(),
        instant.second()
    )
}

/// A status the device chose to answer a query option with, as opposed to a
/// failure to answer at all: `Unsupported` or `Protocol`, and not retryable.
/// The option is dropped and the request made without it.
fn refused(failure: &AcquisitionFailure) -> bool {
    matches!(
        failure.class(),
        AcquisitionFailureClass::Unsupported | AcquisitionFailureClass::Protocol
    ) && failure.retryable() != Some(true)
}

/// The tail an oldest-first collection larger than the budget serves for
/// `$skip`, when the device honors it. A device may ignore `$skip` and answer
/// its first page again, refuse it with a status it chose, or report a count
/// past the members it holds and answer an empty page; each is `None`, and
/// the walk follows the pages from the start instead. A retryable status, or
/// a failure to reach the device, is the poll's failure, since falling back
/// would ship the log's head as new and anchor the cursor there.
async fn skipped_tail<B>(
    bmc: &B,
    entries: &ODataId,
    skip: usize,
    first: &EntryPage,
) -> Result<Option<Arc<EntryPage>>, AcquisitionFailure>
where
    B: Bmc,
    B::Error: ClassifyError,
{
    match bmc.get::<EntryPage>(&skip_page_id(entries, skip)).await {
        Ok(tail) => {
            let honored = tail.members.first().is_some_and(|member| {
                Some(member.id()) != first.members.first().map(NavProperty::id)
            });
            Ok(honored.then_some(tail))
        }
        Err(error) => {
            let failure = error.classify();
            if refused(&failure) {
                Ok(None)
            } else {
                Err(failure)
            }
        }
    }
}

/// Reads the collection's pages from `first`, its answer to `start`, and
/// chooses the members to visit.
///
/// A collection that fits in one document is the common case. A paged one
/// listing oldest first reports its count, and the walk asks for its tail
/// with `$skip` (see [`skipped_tail`]) rather than reading every page: from
/// where the cursor's member sat when the cursor was stored — `cursor_index`,
/// the collection's count then less one — so an idle poll costs the first
/// page, the tail page, and one member whatever the log's size; or from the
/// budget's worth before the end when the count exceeds the budget, whichever
/// is later. A collection listing newest first is read from its head and
/// needs no jump. A `$filter` answer is the tail already and its count is
/// the filter's, so the cursor's index means nothing in it, but one larger
/// than the budget takes the budget's jump like the collection would. Pages
/// are followed until the last or until a budget's worth of members has
/// been read, which bounds the page GETs and the members buffered; a walk
/// whose pages stop short says so in its issue.
async fn collect_entries<B>(
    bmc: &B,
    entries: &ODataId,
    budget: WalkBudget,
    preference: OrderChoice,
    cursor_index: Option<usize>,
    first: Arc<EntryPage>,
    start: Start,
) -> Result<EntryWindow, AcquisitionFailure>
where
    B: Bmc,
    B::Error: ClassifyError,
{
    let filtered = matches!(start, Start::Filtered(_));
    let (order, probed) = match preference {
        OrderChoice::Force(order)
        | OrderChoice::Detect {
            remembered: Some(order),
            probed: true,
        } => (order, true),
        OrderChoice::Detect { remembered, .. } => (
            Order::hinted_by(&first)
                .or(remembered)
                .unwrap_or(Order::OldestFirst),
            false,
        ),
    };
    let first_page = Arc::clone(&first);
    let mut cursor_jump = false;
    let window = |pages: Vec<Arc<EntryPage>>,
                  offset: usize,
                  count: usize,
                  incomplete: Option<&'static str>,
                  cursor_jump: bool| {
        let read: usize = pages.iter().map(|page| page.members.len()).sum();
        EntryWindow {
            pages,
            first_page,
            offset,
            total: count.max(offset + read),
            order,
            probed,
            incomplete,
            cursor_jump,
            filtered,
        }
    };
    if first.next_link.is_none() {
        return Ok(window(vec![first], 0, 0, None, false));
    }
    let count = first
        .count
        .and_then(|count| usize::try_from(count).ok())
        .unwrap_or(0);
    let mut offset = 0;
    let mut page = first;
    let opened = match start {
        Start::Head => entries.clone(),
        Start::Filtered(id) => id,
    };
    let mut seen = std::collections::HashSet::from([opened.clone()]);
    if order == Order::OldestFirst {
        let budget_skip = (count > budget.members).then(|| count - budget.members);
        // A cursor on the first page needs no jump; one at or past the
        // count is gone, and the pages tell what replaced it.
        let cursor_skip = cursor_index.filter(|index| (page.members.len()..count).contains(index));
        let skip = match (budget_skip, cursor_skip) {
            (Some(budget_skip), Some(cursor_skip)) => Some(budget_skip.max(cursor_skip)),
            (budget_skip, cursor_skip) => budget_skip.or(cursor_skip),
        };
        if let Some(skip) = skip {
            if let Some(tail) = skipped_tail(bmc, &opened, skip, &page).await? {
                offset = skip;
                page = tail;
                seen.insert(skip_page_id(&opened, skip));
                cursor_jump = cursor_skip == Some(skip);
            }
        }
    }
    let mut read = page.members.len();
    let mut next = page.next_link.clone();
    let mut pages = vec![page];
    let mut incomplete = None;
    while let Some(link) = next.take() {
        if read >= budget.members {
            incomplete = Some(if order == Order::OldestFirst && offset == 0 {
                PAGES_PAST_BUDGET_FROM_START
            } else {
                PAGES_PAST_BUDGET
            });
            break;
        }
        let Some(id) = next_page_id(entries, &link) else {
            incomplete = Some(NEXT_LINK_UNRESOLVED);
            break;
        };
        if !seen.insert(id.clone()) {
            incomplete = Some(NEXT_LINK_LOOPS);
            break;
        }
        let page = bmc
            .get::<EntryPage>(&id)
            .await
            .map_err(|error| error.classify())?;
        if page.members.is_empty() {
            incomplete = Some(EMPTY_PAGE);
            break;
        }
        read += page.members.len();
        next.clone_from(&page.next_link);
        pages.push(page);
    }
    Ok(window(pages, offset, count, incomplete, cursor_jump))
}

impl ReadKind for LogKind {
    const PROVIDER: &'static str = "redfish.log-service.odata";
    const REQUEST_CLASS: &'static str = "log-read";
    type State = LogCursor;

    async fn acquire<B>(
        bmc: &B,
        target: &ODataId,
        location: &str,
        cursor: &LogCursor,
    ) -> Result<AcquisitionParts, AcquisitionFailure>
    where
        B: Bmc,
        B::Error: ClassifyError,
    {
        let started = Instant::now();
        // One walk of this read at a time. A poll that finds one in flight
        // does not queue behind it, holding the endpoint's admission slot
        // for the other walk's duration: it ships nothing, says so, and the
        // next tick starts from what the in-flight walk stores.
        let Some(mut position) = cursor.position.try_lock() else {
            return Ok(AcquisitionParts::new(
                Vec::new(),
                vec![ProjectionIssue::invalid(
                    IN_FLIGHT_WALK_LOCATOR,
                    "a walk of this log is still in flight; nothing shipped this poll",
                )],
            ));
        };
        with_deadline(WalkBudget::DEFAULT.deadline(), async {
            // Whether the device filters is worth asking only once there is
            // a stamp to filter by: the first walk reads the whole window
            // regardless.
            let filter = if position.is_some() {
                cursor.filter_support(bmc).await?
            } else {
                FilterSupport::Unknown
            };
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
            // A filter by stamp presumes the device's clock has not moved
            // back past the cursor's second. A service whose own clock reads
            // earlier says it has, and this poll walks the head, which
            // places by position; the cursor then follows what shipped, so
            // the next filter asks from the stepped-back time.
            let filter = if clock_behind(&service, position.as_ref()) {
                FilterSupport::Absent
            } else {
                filter
            };
            let mut walk = walk_entries(
                bmc,
                entries.id(),
                WalkBudget::DEFAULT,
                started,
                position.as_ref(),
                filter,
            )
            .await?;
            if walk.filter_refused {
                // Said once, on the poll that learned it: the read stops
                // asking, and an operator can tell the fallback from a
                // device that never advertised.
                cursor.refuse_filter();
                walk.issues.push(ProjectionIssue::invalid(
                    FILTER_REFUSED_LOCATOR,
                    "the device refused the $filter its service root advertises; this log is \
                     read by $skip from now on",
                ));
            }
            let parts = assemble_logs(walk.records, walk.issues, scope)
                .map_err(|error| internal_bug(&error))?;
            // The acquisition is now certain to ship; a walk that failed or
            // was cancelled before this point leaves the cursor where the
            // last shipped walk put it.
            *position = walk.position;
            Ok(parts)
        })
        .await
    }
}

/// Whether the service's own clock, when it reports one, reads earlier than
/// the cursor's stamp: the clock stepped back since the cursor's member was
/// written, and entries written since may be stamped before it.
fn clock_behind(service: &LogService, position: Option<&Position>) -> bool {
    let (Some(now), Some(at)) = (
        service.date_time.flatten(),
        position.and_then(|position| position.at),
    ) else {
        return false;
    };
    crate::instant::timestamp(now).is_ok_and(|now| now < at)
}

/// Where the previous shipped walk of a log service ended: the newest
/// member it shipped, by `@odata.id`, with that member's stamp, plus how the
/// collection listed its members and how many it held.
///
/// The next walk, reading newest first, stops at that member, so a poll
/// costs the new entries plus one member rather than the whole window, and a
/// consumer stops seeing every record repeated each poll. Position, not
/// time, places a record: an entry stamped before the cursor's member is
/// simply a newer member — a device whose clock stepped back, or an event
/// from another clock, ships once and moves the cursor on — and an entry
/// the device does not stamp is placed like any other. The cursor's member
/// ends the walk whatever it answers: one that cannot be read this poll is
/// reported and still stops the walk, rather than letting it overshoot into
/// entries already shipped.
///
/// The stamp tells a refilled log apart from the same entry. A log cleared
/// and refilled reuses its ids; meeting the cursor's id under another stamp
/// means nothing present was shipped, so the cursor is discarded and the
/// whole window ships once. A refill that lands the same id on the same
/// second is invisible, which the second's resolution makes unavoidable. A
/// cursor whose member has left the log — rotated out, or cleared without a
/// refill reaching its id — is never met, and the window ships as new, which
/// on an append-only log it is.
///
/// Entries that projected no record are never shipped and never anchor the
/// cursor: a faulty entry is reported each time it is met.
///
/// The order the collection lists members in is read off numbered ids and
/// otherwise assumed oldest first. A wrong assumption shows itself in one of
/// two ways: the log grows, yet the end taken for newest holds only the
/// cursor's member; or the cursor's member is gone while the log did not
/// grow, which on the true order takes a whole log's worth of new entries
/// but on the wrong order takes one. Either way the walk reads the member at
/// the other end, and only if it is stamped after the one taken for newest
/// flips the order and walks again — once re-shipping what the first walk
/// anchored behind. An insertion elsewhere in an oldest-first log fails that
/// check and flips nothing. An order a probe established outranks the ids
/// from then on.
///
/// Where the next walk starts depends on what the device offers. A device
/// whose service root advertises `ProtocolFeaturesSupported.FilterQuery` is
/// asked for the members stamped at or after the cursor's — `$filter` on
/// `Created`, the cursor's own second included so its member anchors the
/// walk — and an idle poll is one collection request and one member. The
/// cursor's own test still decides what ships, so a device that advertises
/// the filter and answers with everything costs pages, never duplicates; a
/// device that refuses the filtered request is remembered as not filtering,
/// and the poll that learned it says so once at [`FILTER_REFUSED_LOCATOR`].
/// Support is learned once per read: a device that starts advertising later
/// is asked again only when the read is recreated. A filtered answer places
/// by stamp where the walk places by position: an entry written after the
/// cursor's member but stamped before its second — a clock stepped back, an
/// event stamped by another clock — is not in it. Two signs of that send a
/// poll to the head instead. A service whose own `DateTime` reads earlier
/// than the cursor's stamp has stepped back; and an honored filter answers
/// the cursor's own member at least, so an empty answer means the log was
/// cleared, refilled at earlier stamps, or the device answered nothing to a
/// filter it did not parse. An entry stamped behind the cursor by a clock the
/// service does not report stays out of view while the filter is honored. A
/// filtered answer wider than the cursor's member yet walked to it first has
/// the order checked as a head walk stalled on growth would, and one larger
/// than the budget is entered at its tail with `$skip` as the collection
/// would be.
/// Otherwise the count the collection held when the position was stored is
/// where an oldest-first paged log is entered: `$skip` to that index less
/// one, the cursor's member if nothing rotated, so an idle poll reads the
/// first page, the tail page, and one member whatever the log's size. A walk
/// that does not meet the cursor's member there, or meets its id under
/// another stamp, reads the collection from its head instead. Filtered
/// polls leave that count as it was: their count is the filter's, and the
/// index only matters once the filter stops being honored.
///
/// One per [`Read`], shared by its clones; not persisted, so a restart
/// replays one window, bounded by [`WalkBudget`]. When the endpoint policy
/// admits more than one poll at a time, a slow walk can overlap the next
/// tick; the cursor is held for the whole load–walk–store, and the
/// overlapping poll ships nothing rather than queue behind it.
#[derive(Debug, Default)]
pub struct LogCursor {
    position: futures_util::lock::Mutex<Option<Position>>,
    /// Device knowledge rather than walk progress, so it has its own lock:
    /// learned inside a walk, never held across one.
    filter: std::sync::Mutex<FilterSupport>,
}

/// Whether the device honors `$filter` on its collections, as its service
/// root advertises with `ProtocolFeaturesSupported.FilterQuery`. Learned once
/// per read, on the first poll that has a stamp to filter by.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum FilterSupport {
    #[default]
    Unknown,
    Advertised,
    /// Not advertised, or advertised and then refused.
    Absent,
}

impl LogCursor {
    async fn filter_support<B>(&self, bmc: &B) -> Result<FilterSupport, AcquisitionFailure>
    where
        B: Bmc,
        B::Error: ClassifyError,
    {
        let known = *self.filter.lock().expect("filter support poisoned");
        if known != FilterSupport::Unknown {
            return Ok(known);
        }
        let root = bmc
            .get::<ServiceRoot>(&ODataId::service_root())
            .await
            .map_err(|error| error.classify())?;
        let advertised = root
            .protocol_features_supported
            .as_ref()
            .and_then(|features| features.filter_query)
            .unwrap_or(false);
        let support = if advertised {
            FilterSupport::Advertised
        } else {
            FilterSupport::Absent
        };
        *self.filter.lock().expect("filter support poisoned") = support;
        Ok(support)
    }

    fn refuse_filter(&self) {
        *self.filter.lock().expect("filter support poisoned") = FilterSupport::Absent;
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Position {
    /// The newest member shipped.
    member: ODataId,
    /// Its `occurred_at`, if the device stamped it.
    at: Option<Timestamp>,
    /// How the collection listed its members when this position was taken.
    order: Order,
    /// That order was established by a probe, not read or assumed.
    probed: bool,
    /// Members the collection held then.
    count: usize,
}

/// Where one member stands relative to a [`Position`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Placement {
    /// Not the cursor's member: not yet shipped.
    Newer,
    /// The cursor's member, as shipped.
    Covered,
    /// The cursor's id under another stamp: the log was cleared and refilled.
    Refilled,
}

impl Position {
    fn place(&self, member: &ODataId, record: Option<&LogRecord>) -> Placement {
        if *member != self.member {
            return Placement::Newer;
        }
        if record.and_then(LogRecord::occurred_at) == self.at.as_ref() {
            Placement::Covered
        } else {
            Placement::Refilled
        }
    }
}

/// What a walk projected: records, the issues against their members, and
/// the position to store once the records have shipped.
struct Walk {
    records: Vec<LogRecord>,
    issues: Vec<ProjectionIssue>,
    position: Option<Position>,
    /// The device refused the `$filter` it advertised; the read stops asking.
    filter_refused: bool,
}

/// One pass over the window, before its order is judged.
struct Visit {
    walk: Walk,
    window: EntryWindow,
    /// The first member visited was the cursor's: nothing new at the end
    /// taken for newest.
    stalled: bool,
    reached_cursor: bool,
    /// The cursor's id was met under another stamp: a refill, not a
    /// question of order.
    refilled: bool,
    /// The stamp of the newest member that shipped.
    newest_stamp: Option<Timestamp>,
    /// The budget ended the walk before its members ran out.
    stopped: Option<Stop>,
}

/// Visits the newest members, newest first, until the budget is spent or
/// the cursor's member is reached, and projects each. A walk that casts
/// doubt on the collection's order has it checked at the other end and, if
/// wrong, is walked again the right way round.
async fn walk_entries<B>(
    bmc: &B,
    entries: &ODataId,
    budget: WalkBudget,
    started: Instant,
    previous: Option<&Position>,
    filter: FilterSupport,
) -> Result<Walk, AcquisitionFailure>
where
    B: Bmc,
    B::Error: ClassifyError,
{
    let preference = OrderChoice::Detect {
        remembered: previous.map(|position| position.order),
        probed: previous.is_some_and(|position| position.probed),
    };
    let mut visit = visit_entries(
        bmc, entries, budget, started, previous, preference, true, filter,
    )
    .await?;
    let filter_refused = visit.walk.filter_refused;
    let unfiltered = FilterSupport::Absent;
    // The jump presumed the cursor's member where the collection last held
    // it. Not meeting it there — rotated on, or cleared and refilled past
    // that index — means the members before the jump were not shipped after
    // all, so the walk reads the collection from its head this once. A walk
    // the budget stopped short is not that case: the head holds nothing the
    // same budget would reach.
    if visit.window.cursor_jump
        && visit.stopped.is_none()
        && (!visit.reached_cursor || visit.refilled)
    {
        visit = visit_entries(
            bmc, entries, budget, started, previous, preference, false, unfiltered,
        )
        .await?;
    }
    if let Some(position) = previous {
        let doubted = if visit.window.filtered {
            // A filtered answer counts what matched, not what the collection
            // holds, so growth cannot be read off it. What can is an answer
            // wider than the cursor's member alone yet walked to the cursor
            // first: on the right order the rest is older and shares the
            // cursor's second, on the wrong one it is newer.
            visit.stalled && visit.window.members_read() > 1
        } else {
            let total = visit.window.total;
            let stalled_growth = visit.stalled && total > position.count;
            let rotated =
                !visit.reached_cursor && !visit.refilled && total > 0 && total == position.count;
            stalled_growth || rotated
        };
        if doubted {
            let reference = visit.newest_stamp.or(position.at);
            if newer_at_other_end(bmc, &visit.window, reference).await? {
                let forced = OrderChoice::Force(visit.window.order.flipped());
                visit = visit_entries(
                    bmc, entries, budget, started, previous, forced, false, unfiltered,
                )
                .await?;
            }
        }
    }
    visit.walk.filter_refused |= filter_refused;
    Ok(visit.walk)
}

/// Whether the member at the end the walk did not treat as newest is
/// stamped after `reference`. A member absent or without a stamp is no
/// evidence; one the device refuses is reported like any member, and one
/// that cannot be reached fails the poll like any member.
async fn newer_at_other_end<B>(
    bmc: &B,
    window: &EntryWindow,
    reference: Option<Timestamp>,
) -> Result<bool, AcquisitionFailure>
where
    B: Bmc,
    B::Error: ClassifyError,
{
    let (Some((index, member)), Some(reference)) = (window.other_end(), reference) else {
        return Ok(false);
    };
    let entry = match member.get(bmc).await {
        Ok(entry) => entry,
        Err(error) => {
            member_disposition(index, error.classify())?;
            return Ok(false);
        }
    };
    let parts = project_log_entry(&entry, &member.id().to_string())
        .map_err(|error| internal_bug(&error))?;
    Ok(parts
        .log_records
        .first()
        .and_then(|record| record.occurred_at().copied())
        .is_some_and(|stamp| stamp > reference))
}

/// The issue for members a walk left unread: pages the window never
/// reached, members a tail jump skipped, or members the budget cut — unless
/// the cursor was met, when everything before it had shipped already. Time
/// running out is the story whatever the pages did; pages stopping short
/// says more than a member budget that would have been spent on them anyway.
fn truncation(
    window: &EntryWindow,
    visited: usize,
    reached_cursor: bool,
    stopped: Option<Stop>,
) -> Option<ProjectionIssue> {
    let truncated = window.incomplete.is_some()
        || (!reached_cursor && (window.offset > 0 || visited < window.members_read()));
    truncated.then(|| {
        let reason = match (stopped, window.incomplete) {
            (Some(Stop::Time), _) => Stop::Time.as_str(),
            (_, Some(incomplete)) => incomplete,
            _ => Stop::Members.as_str(),
        };
        ProjectionIssue::invalid(
            TRUNCATED_WALK_LOCATOR,
            format!(
                "walk kept the newest {visited} of {} members: {reason}",
                window.total
            ),
        )
    })
}

/// The collection's first answer and how it was asked for.
struct Opened {
    first: Arc<EntryPage>,
    start: Start,
    /// The device refused the `$filter` its root advertises; `first` is the
    /// head instead.
    filter_refused: bool,
}

/// Opens the collection: its `$filter` page when the device advertises
/// filtering and the cursor has a stamp to filter by, else its head. A
/// refused filter is reported through [`Opened::filter_refused`] and the
/// head read instead; an empty filtered answer is read from the head too,
/// unreported; any other failure is the poll's.
async fn open_collection<B>(
    bmc: &B,
    entries: &ODataId,
    previous: Option<&Position>,
    filter: FilterSupport,
) -> Result<Opened, AcquisitionFailure>
where
    B: Bmc,
    B::Error: ClassifyError,
{
    let head = |filter_refused: bool| async move {
        let first = bmc
            .get::<EntryPage>(entries)
            .await
            .map_err(|error| error.classify())?;
        Ok(Opened {
            first,
            start: Start::Head,
            filter_refused,
        })
    };
    let since = match filter {
        FilterSupport::Advertised => previous.and_then(|position| position.at),
        FilterSupport::Unknown | FilterSupport::Absent => None,
    };
    let Some(since) = since else {
        return head(false).await;
    };
    let id = filter_page_id(entries, since);
    match bmc.get::<EntryPage>(&id).await {
        // An honored filter answers the cursor's own member at least. An
        // empty answer means that member is gone with nothing at or after
        // its second — the log cleared, refilled at earlier stamps, or a
        // device answering nothing to a filter it did not parse — and each
        // of those is read from the head.
        Ok(first) if first.members.is_empty() => head(false).await,
        Ok(first) => Ok(Opened {
            first,
            start: Start::Filtered(id),
            filter_refused: false,
        }),
        Err(error) => {
            let failure = error.classify();
            if !refused(&failure) {
                return Err(failure);
            }
            head(true).await
        }
    }
}

/// One pass: open the window — with `$filter` when the device advertises it
/// and the cursor has a stamp, else from the collection's head, jumping to
/// the cursor's last known index when `jump_to_cursor` allows and the order
/// is oldest first — and visit its members newest first.
#[allow(clippy::too_many_arguments)]
async fn visit_entries<B>(
    bmc: &B,
    entries: &ODataId,
    budget: WalkBudget,
    started: Instant,
    previous: Option<&Position>,
    preference: OrderChoice,
    jump_to_cursor: bool,
    filter: FilterSupport,
) -> Result<Visit, AcquisitionFailure>
where
    B: Bmc,
    B::Error: ClassifyError,
{
    let Opened {
        first,
        start,
        filter_refused,
    } = open_collection(bmc, entries, previous, filter).await?;
    let cursor_index = previous
        .filter(|_| jump_to_cursor && start == Start::Head)
        .and_then(|position| position.count.checked_sub(1));
    let window =
        collect_entries(bmc, entries, budget, preference, cursor_index, first, start).await?;
    let mut previous = previous;
    let mut records = Vec::new();
    // Issues are keyed by member so they read in collection order whatever
    // order the walk visited members in.
    let mut member_issues: Vec<(usize, ProjectionIssue)> = Vec::new();
    let mut visited = 0;
    let mut stopped = None;
    let mut reached_cursor = false;
    let mut stalled = false;
    let mut refilled = false;
    let mut newest_shipped: Option<(ODataId, Option<Timestamp>)> = None;
    for (index, member) in window.newest_first() {
        if let Some(reason) = budget.exhausted(visited, started.elapsed()) {
            stopped = Some(reason);
            break;
        }
        visited += 1;
        let is_cursor = previous.is_some_and(|position| position.member == *member.id());
        let entry = match member.get(bmc).await {
            Ok(entry) => entry,
            Err(error) => {
                member_issues.push((index, member_disposition(index, error.classify())?));
                if is_cursor {
                    // The cursor's member, unreadable this poll: everything
                    // past it was shipped, so the walk stops here as if it
                    // had been read.
                    reached_cursor = true;
                    stalled = visited == 1;
                    break;
                }
                continue;
            }
        };
        let location = member.id().to_string();
        let parts = project_log_entry(&entry, &location).map_err(|error| internal_bug(&error))?;
        match previous.map(|position| position.place(member.id(), parts.log_records.first())) {
            Some(Placement::Covered) => {
                reached_cursor = true;
                stalled = visited == 1;
                break;
            }
            Some(Placement::Refilled) => {
                refilled = true;
                previous = None;
            }
            Some(Placement::Newer) | None => {}
        }
        if newest_shipped.is_none() {
            newest_shipped = parts
                .log_records
                .first()
                .map(|record| (member.id().clone(), record.occurred_at().copied()));
        }
        records.extend(parts.log_records);
        member_issues.extend(
            parts
                .issues
                .into_iter()
                .map(|issue| (index, issue.at_index("Members", index))),
        );
    }
    member_issues.sort_by_key(|(index, _)| *index);
    let mut issues: Vec<ProjectionIssue> =
        member_issues.into_iter().map(|(_, issue)| issue).collect();
    if let Some(issue) = truncation(&window, visited, reached_cursor, stopped) {
        issues.push(issue);
    }
    let newest_stamp = newest_shipped.as_ref().and_then(|(_, at)| *at);
    let position = Position::after(&window, previous, newest_shipped);
    Ok(Visit {
        walk: Walk {
            records,
            issues,
            position,
            filter_refused,
        },
        window,
        stalled,
        reached_cursor,
        refilled,
        newest_stamp,
        stopped,
    })
}

impl Position {
    /// The position after a walk over `window` that shipped `newest_shipped`
    /// as its newest member, or nothing new on top of `previous`. A filtered
    /// answer counts what matched, not what the collection holds, so the
    /// collection's count stays what the last unfiltered walk saw.
    fn after(
        window: &EntryWindow,
        previous: Option<&Self>,
        newest_shipped: Option<(ODataId, Option<Timestamp>)>,
    ) -> Option<Self> {
        let count = match (window.filtered, previous) {
            (true, Some(position)) => position.count,
            _ => window.total,
        };
        match newest_shipped {
            Some((member, at)) => Some(Self {
                member,
                at,
                order: window.order,
                probed: window.probed,
                count,
            }),
            None => previous.map(|position| Self {
                order: window.order,
                probed: window.probed,
                count,
                ..position.clone()
            }),
        }
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
            Some(super::Stop::Members)
        );
        assert_eq!(
            budget.exhausted(0, Duration::from_secs(10)),
            Some(super::Stop::Time)
        );
    }

    const ENTRIES: &str = "/redfish/v1/Systems/1/LogServices/SEL/Entries";

    /// One page of a six-entry collection: members `range`, the device's
    /// count, and the link onward if any.
    fn page(range: std::ops::Range<usize>, count: usize, next: Option<&str>) -> String {
        let ids: Vec<String> = range.map(|index| index.to_string()).collect();
        page_of(
            &ids.iter().map(String::as_str).collect::<Vec<_>>(),
            count,
            next,
        )
    }

    /// One page listing `ids` in that order.
    fn page_of(ids: &[&str], count: usize, next: Option<&str>) -> String {
        let members: Vec<String> = ids
            .iter()
            .map(|id| format!(r#"{{ "@odata.id": "{ENTRIES}/{id}" }}"#))
            .collect();
        let next = next.map_or_else(String::new, |link| {
            format!(r#", "Members@odata.nextLink": "{link}""#)
        });
        format!(
            r##"{{ "@odata.id": "{ENTRIES}", "@odata.type": "#LogEntryCollection.LogEntryCollection",
                 "Name": "Entries", "Members@odata.count": {count}, "Members": [{}]{next} }}"##,
            members.join(",")
        )
    }

    /// Entry `index`, stamped at second `index` of one minute so instants
    /// follow ids.
    fn entry(index: usize) -> (String, String) {
        entry_at(index, &format!("2026-03-01T10:00:{index:02}Z"))
    }

    fn entry_at(index: usize, created: &str) -> (String, String) {
        entry_named(&index.to_string(), created)
    }

    fn entry_named(id: &str, created: &str) -> (String, String) {
        (
            format!("{ENTRIES}/{id}"),
            format!(
                r##"{{ "@odata.id": "{ENTRIES}/{id}", "@odata.type": "#LogEntry.v1_21_0.LogEntry",
                     "Id": "{id}", "Name": "Entry", "EntryType": "Event",
                     "Created": "{created}", "Message": "m{id}" }}"##
            ),
        )
    }

    /// An entry without the required `Message`: an issue, never a record.
    fn faulty_entry(index: usize) -> (String, String) {
        (
            format!("{ENTRIES}/{index}"),
            format!(
                r##"{{ "@odata.id": "{ENTRIES}/{index}", "@odata.type": "#LogEntry.v1_21_0.LogEntry",
                     "Id": "{index}", "Name": "Entry", "EntryType": "Event",
                     "Created": "2026-03-01T10:00:{index:02}Z" }}"##
            ),
        )
    }

    type MockBmc = nv_redfish_bmc_mock::Bmc<nv_redfish_bmc_mock::Error>;

    /// Primes the strict-FIFO mock in the walk's request order.
    fn prime(bmc: &MockBmc, answers: &[(String, String)]) {
        for (uri, body) in answers {
            bmc.expect(nv_redfish_bmc_mock::Expect::get(uri, body));
        }
    }

    /// The single-page collection of `members`, with the entries the walk
    /// will ask for, newest first.
    fn whole_log(members: std::ops::Range<usize>) -> Vec<(String, String)> {
        let mut answers = vec![(ENTRIES.to_owned(), page(members.clone(), members.end, None))];
        answers.extend(members.rev().map(entry));
        answers
    }

    /// One walk under `budget` from `cursor`, shipped: the position is stored
    /// as `acquire` stores it once the batch is certain. Returns the entry
    /// ids projected and the issues raised.
    /// One walk of `cursor` under `budget`, its records' `entry_id`s in walk
    /// order and its issues; the position is stored as `acquire` stores it.
    async fn walk_shipping(
        bmc: &MockBmc,
        cursor: &super::LogCursor,
        budget: WalkBudget,
    ) -> (Vec<String>, Vec<nv_telemetry_source::ProjectionIssue>) {
        walk_shipping_with(bmc, cursor, budget, super::FilterSupport::Absent).await
    }

    /// `walk_shipping` under the given `$filter` support, as `acquire` would
    /// have learned it from the service root.
    async fn walk_shipping_with(
        bmc: &MockBmc,
        cursor: &super::LogCursor,
        budget: WalkBudget,
        filter: super::FilterSupport,
    ) -> (Vec<String>, Vec<nv_telemetry_source::ProjectionIssue>) {
        let mut position = cursor.position.lock().await;
        let walk = super::walk_entries(
            bmc,
            &ENTRIES.to_owned().into(),
            budget,
            std::time::Instant::now(),
            position.as_ref(),
            filter,
        )
        .await
        .expect("the walk completes");
        *position = walk.position;
        let ids = walk
            .records
            .iter()
            .filter_map(|record| record.entry_id().map(str::to_owned))
            .collect();
        (ids, walk.issues)
    }

    async fn position(cursor: &super::LogCursor) -> super::Position {
        cursor.position.lock().await.clone().expect("a position")
    }

    async fn walk_with(
        bmc: &MockBmc,
        cursor: &super::LogCursor,
        budget: WalkBudget,
    ) -> (Vec<usize>, Vec<nv_telemetry_source::ProjectionIssue>) {
        let (shipped, issues) = walk_shipping(bmc, cursor, budget).await;
        let mut ids: Vec<usize> = shipped.iter().filter_map(|id| id.parse().ok()).collect();
        ids.sort_unstable();
        (ids, issues)
    }

    /// A first walk over a fresh mock and a fresh cursor.
    async fn walk(
        answers: &[(String, String)],
        budget: WalkBudget,
    ) -> (Vec<usize>, Vec<nv_telemetry_source::ProjectionIssue>) {
        let bmc = MockBmc::default();
        prime(&bmc, answers);
        walk_with(&bmc, &super::LogCursor::default(), budget).await
    }

    fn generous() -> WalkBudget {
        WalkBudget::new(10, Duration::from_secs(10))
    }

    #[tokio::test]
    async fn a_second_poll_stops_at_the_cursor_and_ships_nothing() {
        let bmc = MockBmc::default();
        let cursor = super::LogCursor::default();
        prime(&bmc, &whole_log(0..3));
        assert_eq!(walk_with(&bmc, &cursor, generous()).await.0, [0, 1, 2]);

        // The newest member is read and recognized; nothing older is asked
        // for, and the collection's size is not a truncation.
        prime(&bmc, &[(ENTRIES.to_owned(), page(0..3, 3, None)), entry(2)]);
        let (ids, issues) = walk_with(&bmc, &cursor, generous()).await;
        assert!(ids.is_empty());
        assert!(issues.is_empty());
    }

    #[tokio::test]
    async fn an_overlapping_poll_ships_nothing_and_says_so() {
        const SERVICE: &str = "/redfish/v1/Systems/1/LogServices/SEL";
        let bmc = MockBmc::default();
        let cursor = super::LogCursor::default();
        let target = SERVICE.to_owned().into();
        // A poll in flight holds the cursor for its whole load–walk–store. A
        // poll that finds it held asks the device nothing — the strict mock
        // would fail it — and reports the overlap instead.
        let in_flight = cursor.position.lock().await;
        let skipped = <LogKind as ReadKind>::acquire(&bmc, &target, SERVICE, &cursor)
            .await
            .expect("an overlapping poll is not a failure");
        assert!(skipped.payloads().is_empty());
        assert_eq!(skipped.issues().len(), 1);
        assert_eq!(skipped.issues()[0].path(), super::IN_FLIGHT_WALK_LOCATOR);
        drop(in_flight);

        prime(
            &bmc,
            &[(
                SERVICE.to_owned(),
                include_str!("../tests/fixtures/logs/service.json").to_owned(),
            )],
        );
        prime(&bmc, &whole_log(0..2));
        <LogKind as ReadKind>::acquire(&bmc, &target, SERVICE, &cursor)
            .await
            .expect("the next poll walks");
        assert!(
            cursor.position.lock().await.is_some(),
            "and stores where it ended"
        );
    }

    #[tokio::test]
    async fn an_unreadable_cursor_member_still_ends_the_walk() {
        let bmc = MockBmc::default();
        let cursor = super::LogCursor::default();
        prime(&bmc, &whole_log(0..3));
        walk_with(&bmc, &cursor, generous()).await;

        // The device refuses the cursor's own member this poll: it is
        // reported, and the walk stops there rather than overshooting into
        // the shipped entries behind it. The cursor moves forward, never back.
        prime(&bmc, &[(ENTRIES.to_owned(), page(0..4, 4, None)), entry(3)]);
        bmc.expect(nv_redfish_bmc_mock::Expect {
            request: nv_redfish_bmc_mock::ExpectedRequest::Get {
                id: format!("{ENTRIES}/2").into(),
            },
            response: Err(nv_redfish_bmc_mock::Error::NotSupported),
        });
        let (ids, issues) = walk_with(&bmc, &cursor, generous()).await;
        assert_eq!(ids, [3]);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].path(), "Members[2]");
        assert_eq!(
            position(&cursor).await.member.to_string(),
            format!("{ENTRIES}/3")
        );
    }

    #[tokio::test]
    async fn only_entries_past_the_cursor_ship_and_the_cursor_follows_them() {
        let bmc = MockBmc::default();
        let cursor = super::LogCursor::default();
        prime(&bmc, &whole_log(0..3));
        walk_with(&bmc, &cursor, generous()).await;

        prime(&bmc, &[(ENTRIES.to_owned(), page(0..5, 5, None))]);
        prime(&bmc, &[entry(4), entry(3), entry(2)]);
        let (ids, issues) = walk_with(&bmc, &cursor, generous()).await;
        assert_eq!(ids, [3, 4]);
        assert!(issues.is_empty());

        prime(&bmc, &[(ENTRIES.to_owned(), page(0..5, 5, None)), entry(4)]);
        assert!(walk_with(&bmc, &cursor, generous()).await.0.is_empty());
    }

    #[tokio::test]
    async fn a_burst_within_the_cursors_second_is_told_apart_by_member() {
        let same_second = "2026-03-01T10:00:02Z";
        let bmc = MockBmc::default();
        let cursor = super::LogCursor::default();
        prime(&bmc, &[(ENTRIES.to_owned(), page(0..3, 3, None))]);
        prime(&bmc, &[entry_at(2, same_second), entry(1), entry(0)]);
        walk_with(&bmc, &cursor, generous()).await;

        // Two more entries stamped on the cursor's own second: position, not
        // time, places them, so they are new and the cursor's member ends
        // the walk.
        prime(&bmc, &[(ENTRIES.to_owned(), page(0..5, 5, None))]);
        prime(
            &bmc,
            &[
                entry_at(4, same_second),
                entry_at(3, same_second),
                entry_at(2, same_second),
            ],
        );
        assert_eq!(walk_with(&bmc, &cursor, generous()).await.0, [3, 4]);

        // The cursor moved to the newest of them, so the burst is not re-shipped.
        prime(&bmc, &[(ENTRIES.to_owned(), page(0..5, 5, None))]);
        prime(&bmc, &[entry_at(4, same_second)]);
        assert!(walk_with(&bmc, &cursor, generous()).await.0.is_empty());
    }

    #[tokio::test]
    async fn an_entry_stamped_before_the_cursor_ships_once_and_moves_the_cursor_on() {
        let bmc = MockBmc::default();
        let cursor = super::LogCursor::default();
        prime(&bmc, &whole_log(0..3));
        walk_with(&bmc, &cursor, generous()).await;

        // The device clock stepped back, or the event came from another
        // clock: the new entry is simply the newest member. It ships once,
        // the cursor moves to it, and the next poll ships nothing.
        let earlier = "2026-03-01T09:00:00Z";
        prime(&bmc, &[(ENTRIES.to_owned(), page(0..4, 4, None))]);
        prime(&bmc, &[entry_at(3, earlier), entry(2)]);
        let (ids, issues) = walk_with(&bmc, &cursor, generous()).await;
        assert_eq!(ids, [3]);
        assert!(issues.is_empty());
        prime(
            &bmc,
            &[
                (ENTRIES.to_owned(), page(0..4, 4, None)),
                entry_at(3, earlier),
            ],
        );
        assert!(walk_with(&bmc, &cursor, generous()).await.0.is_empty());
    }

    #[tokio::test]
    async fn a_log_whose_cursor_member_is_gone_is_read_again() {
        let bmc = MockBmc::default();
        let cursor = super::LogCursor::default();
        prime(&bmc, &whole_log(0..3));
        walk_with(&bmc, &cursor, generous()).await;

        // Wiped and refilled with fewer entries: the cursor's member is never
        // met, so everything present is new and the whole log ships.
        prime(&bmc, &whole_log(0..2));
        let (ids, issues) = walk_with(&bmc, &cursor, generous()).await;
        assert_eq!(ids, [0, 1]);
        assert!(issues.is_empty());

        prime(&bmc, &[(ENTRIES.to_owned(), page(0..2, 2, None)), entry(1)]);
        assert!(walk_with(&bmc, &cursor, generous()).await.0.is_empty());
    }

    #[tokio::test]
    async fn a_log_refilled_under_reused_ids_is_read_again() {
        let bmc = MockBmc::default();
        let cursor = super::LogCursor::default();
        prime(&bmc, &whole_log(0..3));
        walk_with(&bmc, &cursor, generous()).await;

        // Cleared and refilled to the same size: the cursor's id is back
        // under a new stamp, which is a new entry, so nothing present was
        // shipped and the whole log ships.
        let later = |index: usize| entry_at(index, &format!("2026-03-01T11:00:{index:02}Z"));
        prime(&bmc, &[(ENTRIES.to_owned(), page(0..3, 3, None))]);
        prime(&bmc, &[later(2), later(1), later(0)]);
        let (ids, issues) = walk_with(&bmc, &cursor, generous()).await;
        assert_eq!(ids, [0, 1, 2]);
        assert!(issues.is_empty());
    }

    #[tokio::test]
    async fn a_newest_first_collection_is_read_from_its_head() {
        // Ids fall along the page, so the head is the newest end: the walk
        // starts there, and a paged log needs no `$skip` jump.
        let bmc = MockBmc::default();
        let cursor = super::LogCursor::default();
        prime(
            &bmc,
            &[(ENTRIES.to_owned(), page_of(&["2", "1", "0"], 3, None))],
        );
        prime(&bmc, &[entry(2), entry(1), entry(0)]);
        assert_eq!(walk_with(&bmc, &cursor, generous()).await.0, [0, 1, 2]);

        prime(
            &bmc,
            &[(
                ENTRIES.to_owned(),
                page_of(&["4", "3", "2", "1", "0"], 5, None),
            )],
        );
        prime(&bmc, &[entry(4), entry(3), entry(2)]);
        let (ids, issues) = walk_with(&bmc, &cursor, generous()).await;
        assert_eq!(ids, [3, 4]);
        assert!(issues.is_empty());

        let budget = WalkBudget::new(2, Duration::from_secs(10));
        let skip2 = format!("{ENTRIES}?$skip=2");
        prime(
            &bmc,
            &[(ENTRIES.to_owned(), page_of(&["6", "5"], 7, Some(&skip2)))],
        );
        prime(&bmc, &[entry(6), entry(5)]);
        let (ids, issues) = walk_with(&bmc, &cursor, budget).await;
        assert_eq!(ids, [5, 6]);
        assert_eq!(issues, [truncated(2, 7, super::PAGES_PAST_BUDGET)]);
    }

    #[tokio::test]
    async fn a_wrong_order_guess_is_caught_by_probing_the_other_end() {
        // Ids without digits give no hint, so the first walk assumes oldest
        // first on a device that lists newest first.
        async fn shipped(bmc: &MockBmc, cursor: &super::LogCursor) -> Vec<String> {
            walk_shipping(bmc, cursor, generous()).await.0
        }
        let stamp = |second: usize| format!("2026-03-01T10:00:{second:02}Z");
        let bmc = MockBmc::default();
        let cursor = super::LogCursor::default();
        prime(
            &bmc,
            &[(ENTRIES.to_owned(), page_of(&["c", "b", "a"], 3, None))],
        );
        prime(
            &bmc,
            &[
                entry_named("a", &stamp(0)),
                entry_named("b", &stamp(1)),
                entry_named("c", &stamp(2)),
            ],
        );
        assert_eq!(shipped(&bmc, &cursor).await, ["a", "b", "c"]);
        assert_eq!(position(&cursor).await.order, super::Order::OldestFirst);

        // The log grew, yet the tail — taken for newest — holds only the
        // cursor's member. The head is stamped after it, so the order flips
        // and the walk runs again from the head, re-shipping once what the
        // first walk anchored behind.
        let grown = || page_of(&["d", "c", "b", "a"], 4, None);
        prime(
            &bmc,
            &[(ENTRIES.to_owned(), grown()), entry_named("a", &stamp(0))],
        );
        prime(&bmc, &[entry_named("d", &stamp(3))]);
        prime(&bmc, &[(ENTRIES.to_owned(), grown())]);
        prime(
            &bmc,
            &[
                entry_named("d", &stamp(3)),
                entry_named("c", &stamp(2)),
                entry_named("b", &stamp(1)),
                entry_named("a", &stamp(0)),
            ],
        );
        assert_eq!(shipped(&bmc, &cursor).await, ["d", "c", "b"]);
        let after = position(&cursor).await;
        assert_eq!(after.order, super::Order::NewestFirst);
        assert!(after.probed, "a probed order outranks the ids from now on");
        assert_eq!(after.member.to_string(), format!("{ENTRIES}/d"));

        // Read the right way round from now on: the head is the cursor.
        prime(
            &bmc,
            &[(ENTRIES.to_owned(), grown()), entry_named("d", &stamp(3))],
        );
        assert!(shipped(&bmc, &cursor).await.is_empty());
    }

    #[tokio::test]
    async fn a_full_log_that_rotates_the_cursor_out_has_its_order_checked() {
        // A newest-first device with a full, fixed-size log and no numbering:
        // read as oldest first, the cursor anchors on the true oldest member,
        // which one new entry rotates out. The log did not grow yet the
        // cursor is gone, so the other end is checked and found newer.
        async fn shipped(bmc: &MockBmc, cursor: &super::LogCursor) -> Vec<String> {
            walk_shipping(bmc, cursor, generous()).await.0
        }
        let stamp = |second: usize| format!("2026-03-01T10:00:{second:02}Z");
        let bmc = MockBmc::default();
        let cursor = super::LogCursor::default();
        prime(
            &bmc,
            &[(ENTRIES.to_owned(), page_of(&["c", "b", "a"], 3, None))],
        );
        prime(
            &bmc,
            &[
                entry_named("a", &stamp(0)),
                entry_named("b", &stamp(1)),
                entry_named("c", &stamp(2)),
            ],
        );
        assert_eq!(shipped(&bmc, &cursor).await, ["a", "b", "c"]);

        let rotated = || page_of(&["d", "c", "b"], 3, None);
        prime(&bmc, &[(ENTRIES.to_owned(), rotated())]);
        prime(
            &bmc,
            &[
                entry_named("b", &stamp(1)),
                entry_named("c", &stamp(2)),
                entry_named("d", &stamp(3)),
            ],
        );
        prime(&bmc, &[entry_named("d", &stamp(3))]);
        prime(&bmc, &[(ENTRIES.to_owned(), rotated())]);
        prime(
            &bmc,
            &[
                entry_named("d", &stamp(3)),
                entry_named("c", &stamp(2)),
                entry_named("b", &stamp(1)),
            ],
        );
        assert_eq!(shipped(&bmc, &cursor).await, ["d", "c", "b"]);
        assert_eq!(position(&cursor).await.order, super::Order::NewestFirst);

        prime(
            &bmc,
            &[(ENTRIES.to_owned(), page_of(&["e", "d", "c"], 3, None))],
        );
        prime(
            &bmc,
            &[entry_named("e", &stamp(4)), entry_named("d", &stamp(3))],
        );
        assert_eq!(shipped(&bmc, &cursor).await, ["e"]);
    }

    #[tokio::test]
    async fn a_head_only_window_keeps_reporting_what_it_cannot_reach() {
        // A device that ignores `$skip` shows the walk only its first pages.
        // Once the cursor sits inside them, every poll meets it at once, and
        // must still say the newest entries lie beyond the window.
        let budget = WalkBudget::new(3, Duration::from_secs(10));
        let skip2 = format!("{ENTRIES}?$skip=2");
        let skip3 = format!("{ENTRIES}?$skip=3");
        let skip4 = format!("{ENTRIES}?$skip=4");
        let skip5 = format!("{ENTRIES}?$skip=5");
        let bmc = MockBmc::default();
        let cursor = super::LogCursor::default();
        // The first poll jumps for the budget (`$skip=3`); the second for the
        // cursor's last index (`$skip=5`). The device answers both with its
        // first page.
        let head_only = |bmc: &MockBmc, ignored_skip: &str| {
            prime(
                bmc,
                &[
                    (ENTRIES.to_owned(), page(0..2, 6, Some(&skip2))),
                    (ignored_skip.to_owned(), page(0..2, 6, Some(&skip2))),
                    (skip2.clone(), page(2..4, 6, Some(&skip4))),
                ],
            );
        };
        head_only(&bmc, &skip3);
        prime(&bmc, &[entry(3), entry(2), entry(1)]);
        assert_eq!(walk_with(&bmc, &cursor, budget).await.0, [1, 2, 3]);

        head_only(&bmc, &skip5);
        prime(&bmc, &[entry(3)]);
        let (ids, issues) = walk_with(&bmc, &cursor, budget).await;
        assert!(ids.is_empty());
        assert_eq!(
            issues,
            [truncated(1, 6, super::PAGES_PAST_BUDGET_FROM_START)]
        );
    }

    #[tokio::test]
    async fn a_stale_count_is_not_a_truncation_once_the_cursor_is_met() {
        // The device reports six members but its tail holds fewer: the walk
        // meets the cursor in the tail, so nothing unread lies before it.
        let budget = WalkBudget::new(3, Duration::from_secs(10));
        let bmc = MockBmc::default();
        let cursor = super::LogCursor::default();
        prime(&bmc, &whole_log(0..5));
        walk_with(&bmc, &cursor, generous()).await;

        let skip2 = format!("{ENTRIES}?$skip=2");
        // The cursor was stored with five members, so the walk asks for
        // index four, which the device serves as its last member.
        let skip4 = format!("{ENTRIES}?$skip=4");
        prime(
            &bmc,
            &[
                (ENTRIES.to_owned(), page(0..2, 6, Some(&skip2))),
                (skip4, page(4..5, 6, None)),
                entry(4),
                // The count grew while the tail showed nothing new, so the
                // head is probed and found older: the order stands.
                entry(0),
            ],
        );
        let (ids, issues) = walk_with(&bmc, &cursor, budget).await;
        assert!(ids.is_empty());
        assert!(issues.is_empty());
    }
    #[tokio::test]
    async fn an_insertion_below_the_newest_end_does_not_flip_the_order() {
        // An oldest-first log that grew, yet whose tail is still the cursor's
        // member: the head is older than the cursor, so the order stands and
        // nothing is re-shipped.
        async fn shipped(bmc: &MockBmc, cursor: &super::LogCursor) -> Vec<String> {
            walk_shipping(bmc, cursor, generous()).await.0
        }
        let stamp = |second: usize| format!("2026-03-01T10:00:{second:02}Z");
        let bmc = MockBmc::default();
        let cursor = super::LogCursor::default();
        prime(&bmc, &[(ENTRIES.to_owned(), page_of(&["a", "b"], 2, None))]);
        prime(
            &bmc,
            &[entry_named("b", &stamp(1)), entry_named("a", &stamp(0))],
        );
        assert_eq!(shipped(&bmc, &cursor).await, ["b", "a"]);

        prime(
            &bmc,
            &[(ENTRIES.to_owned(), page_of(&["a", "x", "b"], 3, None))],
        );
        prime(
            &bmc,
            &[entry_named("b", &stamp(1)), entry_named("a", &stamp(0))],
        );
        assert!(shipped(&bmc, &cursor).await.is_empty());
        assert_eq!(position(&cursor).await.order, super::Order::OldestFirst);
    }

    #[tokio::test]
    async fn a_faulty_newest_entry_is_reported_on_every_poll() {
        let bmc = MockBmc::default();
        let cursor = super::LogCursor::default();
        let faulty = || {
            let mut answers = vec![(ENTRIES.to_owned(), page(0..2, 2, None))];
            answers.push(faulty_entry(1));
            answers.push(entry(0));
            answers
        };
        prime(&bmc, &faulty());
        let (ids, issues) = walk_with(&bmc, &cursor, generous()).await;
        assert_eq!(ids, [0]);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].path(), "Members[1].LogEntry.Message");

        // Never shipped, so never covered: the fault is met again, and the
        // record behind it ends the walk.
        prime(&bmc, &faulty());
        let (ids, issues) = walk_with(&bmc, &cursor, generous()).await;
        assert!(ids.is_empty());
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].path(), "Members[1].LogEntry.Message");
    }

    /// A paged log of `ids`, `per_page` members a page, as its pages answer
    /// from the head: the first page under `ENTRIES`, the rest under `$skip`.
    fn paged_log(ids: &[&str], per_page: usize) -> Vec<(String, String)> {
        let count = ids.len();
        let skip_uri = |skip: usize| format!("{ENTRIES}?$skip={skip}");
        (0..count)
            .step_by(per_page)
            .map(|start| {
                let end = (start + per_page).min(count);
                let next = (end < count).then(|| skip_uri(end));
                let uri = if start == 0 {
                    ENTRIES.to_owned()
                } else {
                    skip_uri(start)
                };
                (uri, page_of(&ids[start..end], count, next.as_deref()))
            })
            .collect()
    }

    /// The tail of a paged log from index `skip`, as the device answers
    /// `$skip` with `per_page` members a page.
    fn tail_pages(ids: &[&str], per_page: usize, skip: usize) -> Vec<(String, String)> {
        let count = ids.len();
        (skip..count)
            .step_by(per_page)
            .map(|start| {
                let end = (start + per_page).min(count);
                let next = (end < count).then(|| format!("{ENTRIES}?$skip={end}"));
                (
                    format!("{ENTRIES}?$skip={start}"),
                    page_of(&ids[start..end], count, next.as_deref()),
                )
            })
            .collect()
    }

    const SIX: [&str; 6] = ["0", "1", "2", "3", "4", "5"];

    #[tokio::test]
    async fn an_idle_poll_of_a_paged_log_reads_its_tail_page_and_one_member() {
        let bmc = MockBmc::default();
        let cursor = super::LogCursor::default();
        prime(&bmc, &paged_log(&SIX, 2));
        prime(&bmc, &(0..6).rev().map(entry).collect::<Vec<_>>());
        assert_eq!(
            walk_with(&bmc, &cursor, generous()).await.0,
            [0, 1, 2, 3, 4, 5]
        );
        assert_eq!(position(&cursor).await.count, 6);

        // The first page says six members; the jump lands on the cursor's
        // member at index five; nothing else is asked for.
        prime(&bmc, &paged_log(&SIX, 2)[..1]);
        prime(&bmc, &tail_pages(&SIX, 2, 5));
        prime(&bmc, &[entry(5)]);
        let (shipped, issues) = walk_with(&bmc, &cursor, generous()).await;
        assert!(shipped.is_empty());
        assert!(issues.is_empty());
    }

    #[tokio::test]
    async fn a_poll_after_growth_reads_only_the_pages_past_the_cursor() {
        let bmc = MockBmc::default();
        let cursor = super::LogCursor::default();
        prime(&bmc, &paged_log(&SIX, 2));
        prime(&bmc, &(0..6).rev().map(entry).collect::<Vec<_>>());
        walk_with(&bmc, &cursor, generous()).await;

        // Two more entries: the jump to index five reads the cursor's page
        // and the page after it, never the four pages before.
        let eight = ["0", "1", "2", "3", "4", "5", "6", "7"];
        prime(&bmc, &paged_log(&eight, 2)[..1]);
        prime(&bmc, &tail_pages(&eight, 2, 5));
        prime(&bmc, &[entry(7), entry(6), entry(5)]);
        let (shipped, issues) = walk_with(&bmc, &cursor, generous()).await;
        assert_eq!(shipped, [6, 7]);
        assert!(issues.is_empty());
        assert_eq!(position(&cursor).await.count, 8);
    }

    #[tokio::test]
    async fn a_rotated_log_is_read_from_its_head_when_the_jump_misses_the_cursor() {
        let bmc = MockBmc::default();
        let cursor = super::LogCursor::default();
        prime(&bmc, &paged_log(&SIX, 2));
        prime(&bmc, &(0..6).rev().map(entry).collect::<Vec<_>>());
        walk_with(&bmc, &cursor, generous()).await;

        // Four entries rotated in and four out: the count is unchanged, but
        // index five now holds entry 9, not the cursor's 5. The jump misses,
        // so the walk starts over from the head and stops at 5 where it is.
        let rotated = ["4", "5", "6", "7", "8", "9"];
        prime(&bmc, &paged_log(&rotated, 2)[..1]);
        prime(&bmc, &tail_pages(&rotated, 2, 5));
        prime(&bmc, &[entry(9)]);
        prime(&bmc, &paged_log(&rotated, 2));
        prime(&bmc, &[entry(9), entry(8), entry(7), entry(6), entry(5)]);
        let (shipped, issues) = walk_with(&bmc, &cursor, generous()).await;
        assert_eq!(shipped, [6, 7, 8, 9]);
        assert!(issues.is_empty());
        assert_eq!(
            position(&cursor).await.member.to_string(),
            format!("{ENTRIES}/9")
        );
    }

    #[tokio::test]
    async fn a_refill_past_the_cursors_index_is_read_from_its_head() {
        let bmc = MockBmc::default();
        let cursor = super::LogCursor::default();
        prime(&bmc, &paged_log(&SIX, 2));
        prime(&bmc, &(0..6).rev().map(entry).collect::<Vec<_>>());
        walk_with(&bmc, &cursor, generous()).await;

        // Cleared and refilled with eight entries under the same ids and
        // later stamps: the jump finds id 5 under another stamp, which says
        // nothing before it was shipped either, so the walk reads the head.
        let eight = ["0", "1", "2", "3", "4", "5", "6", "7"];
        let refilled = |index: usize| entry_at(index, &format!("2026-03-01T11:00:{index:02}Z"));
        prime(&bmc, &paged_log(&eight, 2)[..1]);
        prime(&bmc, &tail_pages(&eight, 2, 5));
        prime(&bmc, &[refilled(7), refilled(6), refilled(5)]);
        prime(&bmc, &paged_log(&eight, 2));
        prime(&bmc, &(0..8).rev().map(refilled).collect::<Vec<_>>());
        let (shipped, issues) = walk_with(&bmc, &cursor, generous()).await;
        assert_eq!(shipped, [0, 1, 2, 3, 4, 5, 6, 7]);
        assert!(issues.is_empty());
        assert_eq!(position(&cursor).await.count, 8);
    }

    fn filtered_uri(since: &str) -> String {
        format!("{ENTRIES}?$filter=Created ge '{since}'")
    }

    async fn walk_filtered(
        bmc: &MockBmc,
        cursor: &super::LogCursor,
    ) -> (Vec<usize>, Vec<nv_telemetry_source::ProjectionIssue>) {
        let (shipped, issues) =
            walk_shipping_with(bmc, cursor, generous(), super::FilterSupport::Advertised).await;
        let mut ids: Vec<usize> = shipped.iter().filter_map(|id| id.parse().ok()).collect();
        ids.sort_unstable();
        (ids, issues)
    }

    #[tokio::test]
    async fn a_filtering_device_is_asked_only_for_what_came_after_the_cursor() {
        let bmc = MockBmc::default();
        let cursor = super::LogCursor::default();
        // No stamp to filter by yet: the first walk reads the head.
        prime(&bmc, &whole_log(0..3));
        assert_eq!(walk_filtered(&bmc, &cursor).await.0, [0, 1, 2]);

        // Idle: one request for the cursor's second onward, one member.
        prime(
            &bmc,
            &[
                (
                    filtered_uri("2026-03-01T10:00:02Z"),
                    page_of(&["2"], 1, None),
                ),
                entry(2),
            ],
        );
        let (ids, issues) = walk_filtered(&bmc, &cursor).await;
        assert!(ids.is_empty());
        assert!(issues.is_empty());
        assert_eq!(
            position(&cursor).await.count,
            3,
            "a filter's count is not the collection's"
        );

        // Growth: the answer runs from the cursor's second; the two new
        // entries ship and the cursor follows.
        prime(
            &bmc,
            &[(
                filtered_uri("2026-03-01T10:00:02Z"),
                page_of(&["2", "3", "4"], 3, None),
            )],
        );
        prime(&bmc, &[entry(4), entry(3), entry(2)]);
        let (ids, issues) = walk_filtered(&bmc, &cursor).await;
        assert_eq!(ids, [3, 4]);
        assert!(issues.is_empty());
        assert_eq!(position(&cursor).await.count, 3);

        prime(
            &bmc,
            &[
                (
                    filtered_uri("2026-03-01T10:00:04Z"),
                    page_of(&["4"], 1, None),
                ),
                entry(4),
            ],
        );
        assert!(walk_filtered(&bmc, &cursor).await.0.is_empty());
    }

    #[tokio::test]
    async fn a_filtered_answer_without_the_cursors_member_ships_all_of_it() {
        let bmc = MockBmc::default();
        let cursor = super::LogCursor::default();
        prime(&bmc, &whole_log(0..3));
        walk_filtered(&bmc, &cursor).await;

        // The cursor's member rotated out; everything the filter returns is
        // newer, and an answer walked to its end is not a truncation.
        prime(
            &bmc,
            &[(
                filtered_uri("2026-03-01T10:00:02Z"),
                page_of(&["5", "6"], 2, None),
            )],
        );
        prime(&bmc, &[entry(6), entry(5)]);
        let (ids, issues) = walk_filtered(&bmc, &cursor).await;
        assert_eq!(ids, [5, 6]);
        assert!(issues.is_empty());
    }

    #[tokio::test]
    async fn an_empty_filtered_answer_is_read_from_the_head() {
        let bmc = MockBmc::default();
        let cursor = super::LogCursor::default();
        prime(&bmc, &whole_log(0..3));
        walk_filtered(&bmc, &cursor).await;

        // Cleared and refilled with entries stamped before the cursor's
        // second: the filter answers nothing, which an honored filter never
        // does while the cursor's member is present, so the head is read
        // and the refill ships.
        prime(
            &bmc,
            &[(filtered_uri("2026-03-01T10:00:02Z"), page_of(&[], 0, None))],
        );
        prime(&bmc, &whole_log(0..2));
        let (ids, issues) = walk_filtered(&bmc, &cursor).await;
        assert_eq!(ids, [0, 1]);
        assert!(issues.is_empty());
        assert_eq!(
            position(&cursor).await.member.to_string(),
            format!("{ENTRIES}/1")
        );
    }

    #[tokio::test]
    async fn a_filtered_walk_that_stalls_on_a_wider_answer_has_its_order_checked() {
        // Ids without digits give no hint, so the walk assumes oldest first
        // on a device that lists newest first, and the cursor anchors on the
        // true oldest member.
        async fn shipped(bmc: &MockBmc, cursor: &super::LogCursor) -> Vec<String> {
            walk_shipping_with(bmc, cursor, generous(), super::FilterSupport::Advertised)
                .await
                .0
        }
        let stamp = |second: usize| format!("2026-03-01T10:00:{second:02}Z");
        let bmc = MockBmc::default();
        let cursor = super::LogCursor::default();
        prime(
            &bmc,
            &[(ENTRIES.to_owned(), page_of(&["c", "b", "a"], 3, None))],
        );
        prime(
            &bmc,
            &[
                entry_named("a", &stamp(0)),
                entry_named("b", &stamp(1)),
                entry_named("c", &stamp(2)),
            ],
        );
        assert_eq!(shipped(&bmc, &cursor).await, ["a", "b", "c"]);

        // The filter from the cursor's second answers the whole log, walked
        // to the cursor first. The answer holds more than the cursor's
        // member, so the other end is checked, found newer, and the walk
        // runs again from the head the right way round.
        let grown = || page_of(&["d", "c", "b", "a"], 4, None);
        prime(
            &bmc,
            &[
                (filtered_uri(&stamp(0)), grown()),
                entry_named("a", &stamp(0)),
            ],
        );
        prime(&bmc, &[entry_named("d", &stamp(3))]);
        prime(&bmc, &[(ENTRIES.to_owned(), grown())]);
        prime(
            &bmc,
            &[
                entry_named("d", &stamp(3)),
                entry_named("c", &stamp(2)),
                entry_named("b", &stamp(1)),
                entry_named("a", &stamp(0)),
            ],
        );
        assert_eq!(shipped(&bmc, &cursor).await, ["d", "c", "b"]);
        let after = position(&cursor).await;
        assert_eq!(after.order, super::Order::NewestFirst);
        assert!(after.probed);

        // An idle filtered poll answers the cursor's member alone: nothing
        // to doubt.
        prime(
            &bmc,
            &[
                (filtered_uri(&stamp(3)), page_of(&["d"], 1, None)),
                entry_named("d", &stamp(3)),
            ],
        );
        assert!(shipped(&bmc, &cursor).await.is_empty());
    }

    #[tokio::test]
    async fn a_filtered_backlog_larger_than_the_budget_is_read_from_its_tail() {
        let bmc = MockBmc::default();
        let cursor = super::LogCursor::default();
        prime(&bmc, &whole_log(0..2));
        walk_filtered(&bmc, &cursor).await;

        // Eight matches under a budget of three: the walk jumps to `$skip=5`
        // on the filtered request and never reads the pages between.
        let filtered = filtered_uri("2026-03-01T10:00:01Z");
        let filtered_skip2 = format!("{filtered}&$skip=2");
        let filtered_skip5 = format!("{filtered}&$skip=5");
        prime(
            &bmc,
            &[
                (
                    filtered.clone(),
                    page_of(&["1", "2"], 8, Some(&filtered_skip2)),
                ),
                (filtered_skip5, page_of(&["6", "7", "8"], 8, None)),
            ],
        );
        prime(&bmc, &[entry(8), entry(7), entry(6)]);
        let budget = WalkBudget::new(3, Duration::from_secs(10));
        let (shipped, issues) =
            walk_shipping_with(&bmc, &cursor, budget, super::FilterSupport::Advertised).await;
        assert_eq!(shipped, ["8", "7", "6"]);
        assert_eq!(issues, [truncated(3, 8, "member budget spent")]);
        let after = position(&cursor).await;
        assert_eq!(after.member.to_string(), format!("{ENTRIES}/8"));
        assert_eq!(after.count, 2, "a filter's count is not the collection's");
    }

    fn service_root(filters: bool) -> String {
        let features = if filters {
            r#", "ProtocolFeaturesSupported": { "FilterQuery": true }"#
        } else {
            ""
        };
        format!(
            r##"{{ "@odata.id": "/redfish/v1", "@odata.type": "#ServiceRoot.v1_10_0.ServiceRoot",
                 "Id": "RootService", "Name": "Root Service", "RedfishVersion": "1.10.0",
                 "Links": {{ "Sessions": {{ "@odata.id": "/redfish/v1/SessionService/Sessions" }} }}{features} }}"##
        )
    }

    /// One `LogKind::acquire` of the corpus service over `bmc`.
    async fn acquire_parts(
        bmc: &MockBmc,
        cursor: &super::LogCursor,
    ) -> nv_telemetry_source::AcquisitionParts {
        const SERVICE: &str = "/redfish/v1/Systems/1/LogServices/SEL";
        <LogKind as ReadKind>::acquire(bmc, &SERVICE.to_owned().into(), SERVICE, cursor)
            .await
            .expect("the poll completes")
    }

    /// `acquire_parts`, as how many payloads shipped.
    async fn acquire_service(bmc: &MockBmc, cursor: &super::LogCursor) -> usize {
        acquire_parts(bmc, cursor).await.payloads().len()
    }

    /// The corpus service, its clock reading `clock`.
    fn service_with_clock(clock: &str) -> String {
        format!(
            r##"{{ "@odata.id": "/redfish/v1/Systems/1/LogServices/SEL", "@odata.type": "#LogService.v1_8_0.LogService",
                 "Id": "SEL", "Name": "System Event Log", "LogEntryType": "SEL", "DateTime": "{clock}",
                 "Entries": {{ "@odata.id": "{ENTRIES}" }} }}"##
        )
    }

    #[tokio::test]
    async fn filter_support_is_read_off_the_root_once_there_is_a_stamp_to_filter_by() {
        const SERVICE: &str = "/redfish/v1/Systems/1/LogServices/SEL";
        let service = include_str!("../tests/fixtures/logs/service.json").to_owned();
        let bmc = MockBmc::default();
        let cursor = super::LogCursor::default();
        // The first poll never asks: nothing to filter by.
        prime(&bmc, &[(SERVICE.to_owned(), service.clone())]);
        prime(&bmc, &whole_log(0..2));
        assert_eq!(acquire_service(&bmc, &cursor).await, 1);

        // The second asks the root once and, told yes, filters from then on.
        prime(
            &bmc,
            &[
                ("/redfish/v1".to_owned(), service_root(true)),
                (SERVICE.to_owned(), service.clone()),
                (
                    filtered_uri("2026-03-01T10:00:01Z"),
                    page_of(&["1"], 1, None),
                ),
                entry(1),
            ],
        );
        assert_eq!(acquire_service(&bmc, &cursor).await, 0);
        prime(
            &bmc,
            &[
                (SERVICE.to_owned(), service),
                (
                    filtered_uri("2026-03-01T10:00:01Z"),
                    page_of(&["1"], 1, None),
                ),
                entry(1),
            ],
        );
        assert_eq!(acquire_service(&bmc, &cursor).await, 0);
    }

    #[tokio::test]
    async fn a_root_that_does_not_advertise_filtering_keeps_the_head_walk() {
        const SERVICE: &str = "/redfish/v1/Systems/1/LogServices/SEL";
        let service = include_str!("../tests/fixtures/logs/service.json").to_owned();
        let bmc = MockBmc::default();
        let cursor = super::LogCursor::default();
        prime(&bmc, &[(SERVICE.to_owned(), service.clone())]);
        prime(&bmc, &whole_log(0..2));
        acquire_service(&bmc, &cursor).await;

        prime(
            &bmc,
            &[
                ("/redfish/v1".to_owned(), service_root(false)),
                (SERVICE.to_owned(), service),
                (ENTRIES.to_owned(), page(0..2, 2, None)),
                entry(1),
            ],
        );
        assert_eq!(acquire_service(&bmc, &cursor).await, 0);
    }

    #[tokio::test]
    async fn a_refused_filter_is_said_once_and_not_asked_again() {
        const SERVICE: &str = "/redfish/v1/Systems/1/LogServices/SEL";
        let service = include_str!("../tests/fixtures/logs/service.json").to_owned();
        let bmc = MockBmc::default();
        let cursor = super::LogCursor::default();
        prime(&bmc, &[(SERVICE.to_owned(), service.clone())]);
        prime(&bmc, &whole_log(0..2));
        acquire_service(&bmc, &cursor).await;

        // The root advertises filtering and the collection refuses it: the
        // head is read this poll, and the refusal is the poll's one issue.
        prime(
            &bmc,
            &[
                ("/redfish/v1".to_owned(), service_root(true)),
                (SERVICE.to_owned(), service.clone()),
            ],
        );
        bmc.expect(nv_redfish_bmc_mock::Expect {
            request: nv_redfish_bmc_mock::ExpectedRequest::Get {
                id: filtered_uri("2026-03-01T10:00:01Z").into(),
            },
            response: Err(nv_redfish_bmc_mock::Error::NotSupported),
        });
        prime(&bmc, &[(ENTRIES.to_owned(), page(0..2, 2, None)), entry(1)]);
        let parts = acquire_parts(&bmc, &cursor).await;
        assert!(parts.payloads().is_empty());
        assert_eq!(
            parts.issues(),
            [nv_telemetry_source::ProjectionIssue::invalid(
                super::FILTER_REFUSED_LOCATOR,
                "the device refused the $filter its service root advertises; this log is read \
                 by $skip from now on",
            )]
        );

        // From then on the head is read without asking the root or the
        // collection's filter again, and nothing more is said.
        prime(
            &bmc,
            &[
                (SERVICE.to_owned(), service),
                (ENTRIES.to_owned(), page(0..2, 2, None)),
                entry(1),
            ],
        );
        let parts = acquire_parts(&bmc, &cursor).await;
        assert!(parts.payloads().is_empty());
        assert!(parts.issues().is_empty());
    }

    #[tokio::test]
    async fn a_service_whose_clock_stepped_back_is_walked_from_its_head_that_poll() {
        const SERVICE: &str = "/redfish/v1/Systems/1/LogServices/SEL";
        let bmc = MockBmc::default();
        let cursor = super::LogCursor::default();
        prime(
            &bmc,
            &[(
                SERVICE.to_owned(),
                service_with_clock("2026-03-01T10:00:05Z"),
            )],
        );
        prime(&bmc, &whole_log(0..2));
        acquire_service(&bmc, &cursor).await;

        // The service's clock now reads before the cursor's stamp, so an
        // entry written since may be stamped before it, which the filter
        // would hide: the head is walked, and the entry ships.
        prime(
            &bmc,
            &[
                ("/redfish/v1".to_owned(), service_root(true)),
                (
                    SERVICE.to_owned(),
                    service_with_clock("2026-03-01T09:59:00Z"),
                ),
                (ENTRIES.to_owned(), page(0..3, 3, None)),
                entry_at(2, "2026-03-01T09:59:30Z"),
                entry(1),
            ],
        );
        assert_eq!(acquire_service(&bmc, &cursor).await, 1);

        // The clock ahead of the cursor again, the filter resumes from the
        // cursor's stepped-back stamp.
        prime(
            &bmc,
            &[
                (
                    SERVICE.to_owned(),
                    service_with_clock("2026-03-01T10:01:00Z"),
                ),
                (
                    filtered_uri("2026-03-01T09:59:30Z"),
                    page_of(&["2"], 1, None),
                ),
                entry_at(2, "2026-03-01T09:59:30Z"),
            ],
        );
        assert_eq!(acquire_service(&bmc, &cursor).await, 0);
    }

    #[test]
    fn a_refusal_is_a_status_the_device_chose() {
        let status = |class, retryable| AcquisitionFailure::new(class).with_retryable(retryable);
        assert!(super::refused(&status(
            AcquisitionFailureClass::Unsupported,
            false
        )));
        assert!(super::refused(&status(
            AcquisitionFailureClass::Protocol,
            false
        )));
        assert!(!super::refused(&status(
            AcquisitionFailureClass::Protocol,
            true
        )));
        assert!(!super::refused(&status(
            AcquisitionFailureClass::Device,
            false
        )));
        assert!(!super::refused(&status(
            AcquisitionFailureClass::Connectivity,
            true
        )));
    }

    #[test]
    fn the_filter_names_the_cursors_second_in_utc() {
        let at = nv_telemetry_model::Timestamp::new(1_772_359_205, 7).expect("a valid instant");
        assert_eq!(
            super::filter_page_id(&"/x/Entries".to_owned().into(), at).to_string(),
            "/x/Entries?$filter=Created ge '2026-03-01T10:00:05Z'"
        );
        assert_eq!(
            super::filter_page_id(&"/x/Entries?$top=5".to_owned().into(), at).to_string(),
            "/x/Entries?$top=5&$filter=Created ge '2026-03-01T10:00:05Z'"
        );
    }

    #[tokio::test]
    async fn the_budget_caps_the_new_entries_one_poll_carries() {
        let budget = WalkBudget::new(3, Duration::from_secs(10));
        let bmc = MockBmc::default();
        let cursor = super::LogCursor::default();
        prime(&bmc, &whole_log(0..3));
        walk_with(&bmc, &cursor, budget).await;

        // Four new entries behind a three-member budget on a paged log: the
        // walk jumps to the tail, ships the newest three, and says the
        // fourth was dropped — the cursor was never reached.
        let skip2 = format!("{ENTRIES}?$skip=2");
        let skip4 = format!("{ENTRIES}?$skip=4");
        prime(
            &bmc,
            &[
                (ENTRIES.to_owned(), page(0..2, 7, Some(&skip2))),
                (skip4, page(4..7, 7, None)),
            ],
        );
        prime(&bmc, &[entry(6), entry(5), entry(4)]);
        let (ids, issues) = walk_with(&bmc, &cursor, budget).await;
        assert_eq!(ids, [4, 5, 6]);
        assert_eq!(issues, [truncated(3, 7, "member budget spent")]);
    }

    fn truncated(kept: usize, total: usize, reason: &str) -> nv_telemetry_source::ProjectionIssue {
        nv_telemetry_source::ProjectionIssue::invalid(
            super::TRUNCATED_WALK_LOCATOR,
            format!("walk kept the newest {kept} of {total} members: {reason}"),
        )
    }

    #[tokio::test]
    async fn a_paged_collection_within_the_budget_is_read_to_its_end() {
        let skip3 = format!("{ENTRIES}?$skip=3");
        let mut answers = vec![
            (ENTRIES.to_owned(), page(0..3, 5, Some(&skip3))),
            (skip3.clone(), page(3..5, 5, None)),
        ];
        answers.extend((0..5).rev().map(entry));
        let (ids, issues) = walk(&answers, WalkBudget::new(10, Duration::from_secs(10))).await;
        assert_eq!(ids, [0, 1, 2, 3, 4]);
        assert!(issues.is_empty());
    }

    #[tokio::test]
    async fn a_log_larger_than_the_budget_is_read_from_its_tail() {
        // Six entries, budget three: the walk jumps to `$skip=3` and never
        // reads the pages before it.
        let skip2 = format!("{ENTRIES}?$skip=2");
        let skip3 = format!("{ENTRIES}?$skip=3");
        let mut answers = vec![
            (ENTRIES.to_owned(), page(0..2, 6, Some(&skip2))),
            (skip3, page(3..6, 6, None)),
        ];
        answers.extend((3..6).rev().map(entry));
        let (ids, issues) = walk(&answers, WalkBudget::new(3, Duration::from_secs(10))).await;
        assert_eq!(ids, [3, 4, 5]);
        assert_eq!(issues, [truncated(3, 6, "member budget spent")]);
    }

    #[tokio::test]
    async fn a_device_that_ignores_skip_is_read_from_the_start() {
        // The `$skip` answer is the first page again, so the walk follows
        // the pages from the start, stops once a budget's worth is read, and
        // says the members it kept are the collection's first.
        let skip2 = format!("{ENTRIES}?$skip=2");
        let skip3 = format!("{ENTRIES}?$skip=3");
        let skip4 = format!("{ENTRIES}?$skip=4");
        let mut answers = vec![
            (ENTRIES.to_owned(), page(0..2, 6, Some(&skip2))),
            (skip3, page(0..2, 6, Some(&skip2))),
            (skip2, page(2..4, 6, Some(&skip4))),
        ];
        answers.extend((1..4).rev().map(entry));
        let (ids, issues) = walk(&answers, WalkBudget::new(3, Duration::from_secs(10))).await;
        assert_eq!(ids, [1, 2, 3]);
        assert_eq!(
            issues,
            [truncated(3, 6, super::PAGES_PAST_BUDGET_FROM_START)]
        );
    }

    #[tokio::test]
    async fn a_device_that_refuses_skip_is_read_from_the_start() {
        // `$skip` answered with a status the device chose: not this poll's
        // failure, but a device without `$skip`, read from its first page.
        let skip2 = format!("{ENTRIES}?$skip=2");
        let skip4 = format!("{ENTRIES}?$skip=4");
        let bmc = MockBmc::default();
        prime(&bmc, &[(ENTRIES.to_owned(), page(0..2, 6, Some(&skip2)))]);
        bmc.expect(nv_redfish_bmc_mock::Expect {
            request: nv_redfish_bmc_mock::ExpectedRequest::Get {
                id: format!("{ENTRIES}?$skip=3").into(),
            },
            response: Err(nv_redfish_bmc_mock::Error::NotSupported),
        });
        prime(&bmc, &[(skip2, page(2..4, 6, Some(&skip4)))]);
        prime(&bmc, &[entry(3), entry(2), entry(1)]);
        let budget = WalkBudget::new(3, Duration::from_secs(10));
        let (ids, issues) = walk_with(&bmc, &super::LogCursor::default(), budget).await;
        assert_eq!(ids, [1, 2, 3]);
        assert_eq!(
            issues,
            [truncated(3, 6, super::PAGES_PAST_BUDGET_FROM_START)]
        );
    }

    #[tokio::test]
    async fn an_empty_skip_tail_is_not_honored() {
        // The device reports six members but serves fewer, so the tail is
        // empty: the walk follows the pages from the start instead of
        // treating the empty page as the whole window.
        let skip2 = format!("{ENTRIES}?$skip=2");
        let skip3 = format!("{ENTRIES}?$skip=3");
        let skip4 = format!("{ENTRIES}?$skip=4");
        let mut answers = vec![
            (ENTRIES.to_owned(), page(0..2, 6, Some(&skip2))),
            (skip3, page(3..3, 6, None)),
            (skip2, page(2..4, 6, Some(&skip4))),
        ];
        answers.extend((1..4).rev().map(entry));
        let (ids, issues) = walk(&answers, WalkBudget::new(3, Duration::from_secs(10))).await;
        assert_eq!(ids, [1, 2, 3]);
        assert_eq!(
            issues,
            [truncated(3, 6, super::PAGES_PAST_BUDGET_FROM_START)]
        );
    }

    #[tokio::test]
    async fn a_relative_next_link_is_followed() {
        let mut answers = vec![
            (ENTRIES.to_owned(), page(0..3, 5, Some("Entries?$skip=3"))),
            (format!("{ENTRIES}?$skip=3"), page(3..5, 5, None)),
        ];
        answers.extend((0..5).rev().map(entry));
        let (ids, issues) = walk(&answers, WalkBudget::new(10, Duration::from_secs(10))).await;
        assert_eq!(ids, [0, 1, 2, 3, 4]);
        assert!(issues.is_empty());
    }

    #[tokio::test]
    async fn an_unresolvable_next_link_is_reported_even_without_a_count() {
        // No `Members@odata.count` and a nextLink naming only a host: the
        // pages stop after the first, and the walk says so rather than
        // passing the first page off as the whole log.
        let first = format!(
            r##"{{ "@odata.id": "{ENTRIES}", "@odata.type": "#LogEntryCollection.LogEntryCollection",
                 "Name": "Entries",
                 "Members": [{{ "@odata.id": "{ENTRIES}/0" }}, {{ "@odata.id": "{ENTRIES}/1" }}],
                 "Members@odata.nextLink": "https://bmc.example" }}"##
        );
        let answers = vec![(ENTRIES.to_owned(), first), entry(1), entry(0)];
        let (ids, issues) = walk(&answers, generous()).await;
        assert_eq!(ids, [0, 1]);
        assert_eq!(issues, [truncated(2, 2, super::NEXT_LINK_UNRESOLVED)]);
    }

    #[tokio::test]
    async fn a_spent_time_budget_stops_before_the_first_member_and_says_so() {
        let answers = vec![(ENTRIES.to_owned(), page(0..2, 2, None))];
        let (ids, issues) = walk(&answers, WalkBudget::new(10, Duration::ZERO)).await;
        assert!(ids.is_empty());
        assert_eq!(issues, [truncated(0, 2, "time budget spent")]);
    }

    #[test]
    fn a_next_link_is_the_path_the_transport_resolves() {
        let entries = ENTRIES.to_owned().into();
        let id = |link: &str| super::next_page_id(&entries, link).map(|id| id.to_string());
        assert_eq!(
            id("/redfish/v1/x?$skip=1"),
            Some("/redfish/v1/x?$skip=1".into())
        );
        assert_eq!(
            id("https://bmc.example/redfish/v1/x?$skip=2"),
            Some("/redfish/v1/x?$skip=2".into())
        );
        assert_eq!(
            id("//bmc.example/redfish/v1/x?$skip=2"),
            Some("/redfish/v1/x?$skip=2".into())
        );
        // Relative references resolve against the collection, per RFC 3986.
        assert_eq!(id("Entries?$skip=3"), Some(format!("{ENTRIES}?$skip=3")));
        assert_eq!(id("?$skip=3"), Some(format!("{ENTRIES}?$skip=3")));
        assert_eq!(id("https://bmc.example"), None);
        assert_eq!(id(""), None);
        // A `$skip` request joins any query the collection id already carries.
        let tokened: super::ODataId = format!("{ENTRIES}?token=x").into();
        assert_eq!(
            super::skip_page_id(&tokened, 5).to_string(),
            format!("{ENTRIES}?token=x&$skip=5")
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
