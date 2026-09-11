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
/// meters it as one unit of cost. **The budget keeps the newest entries.** A
/// log lists its entries oldest first, so a walk that spent its budget from
/// the head would never show a consumer the records that arrived since the
/// last poll; instead the walk visits the newest members first, and when the
/// collection is paged it jumps to the tail with `$skip` before following
/// `Members@odata.nextLink` to the end. What the budget cuts off is the
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

    /// How many ids the cursor keeps at one instant: four walks' worth, so a
    /// frozen device clock cannot grow the cursor for the life of the process.
    fn retained_ids(self) -> usize {
        self.members.saturating_mul(4)
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

/// One page of an entries collection, read raw: nv-redfish 0.16's typed
/// collection drops `Members@odata.count` and `Members@odata.nextLink`, and
/// without them a paged log is silently its first page. Members keep their
/// `NavProperty` form, so an entry the device expanded inline still resolves
/// without I/O.
#[derive(Debug, Deserialize)]
struct EntryPage {
    #[serde(rename = "@odata.id")]
    odata_id: ODataId,
    #[serde(rename = "Members", default)]
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
        None
    }
}

/// The pages a walk read, in collection order, the first of them starting
/// at `offset` in a collection of `total` members. Pages stay shared with
/// the transport's cache; the walk borrows members from them.
struct EntryWindow {
    pages: Vec<Arc<EntryPage>>,
    offset: usize,
    total: usize,
}

impl EntryWindow {
    fn new(pages: Vec<Arc<EntryPage>>, offset: usize, count: usize) -> Self {
        let read: usize = pages.iter().map(|page| page.members.len()).sum();
        Self {
            pages,
            offset,
            total: count.max(offset + read),
        }
    }

    /// The newest `budget` members read, each with its position in the
    /// whole collection.
    fn newest_first(&self, budget: WalkBudget) -> Vec<(usize, &NavProperty<LogEntry>)> {
        let members: Vec<&NavProperty<LogEntry>> = self
            .pages
            .iter()
            .flat_map(|page| page.members.iter())
            .collect();
        members
            .into_iter()
            .enumerate()
            .map(|(position, member)| (self.offset + position, member))
            .rev()
            .take(budget.members)
            .collect()
    }
}

/// A `Members@odata.nextLink` as the id the transport resolves against the
/// endpoint. Devices write it as a path; one that writes an absolute URL is
/// reduced to its path and query, and the transport's same-origin rule keeps
/// it from naming another host.
fn next_page_id(link: &str) -> Option<ODataId> {
    if link.starts_with('/') {
        return Some(ODataId::from(link.to_owned()));
    }
    let after_scheme = link.split_once("://")?.1;
    let path = &after_scheme[after_scheme.find('/')?..];
    Some(ODataId::from(path.to_owned()))
}

/// Reads the collection's pages and chooses the members to visit.
///
/// A collection that fits in one document is the common case. A paged one
/// reports its count, and when the count exceeds the budget the walk asks
/// for the tail with `$skip`, which every Redfish service must honor; one
/// that ignores it answers with its first page again, which the walk
/// recognizes and falls back to following the pages from the start. Pages
/// are followed until the last, bounded by the member budget, since a page
/// holds at least one member.
async fn collect_entries<B>(
    bmc: &B,
    entries: &ODataId,
    budget: WalkBudget,
) -> Result<EntryWindow, AcquisitionFailure>
where
    B: Bmc,
    B::Error: ClassifyError,
{
    let first = bmc
        .get::<EntryPage>(entries)
        .await
        .map_err(|error| error.classify())?;
    if first.next_link.is_none() {
        return Ok(EntryWindow::new(vec![first], 0, 0));
    }
    let count = first
        .count
        .and_then(|count| usize::try_from(count).ok())
        .unwrap_or(0);
    let mut offset = 0;
    let mut page = first;
    if count > budget.members {
        let skip = count - budget.members;
        let tail = bmc
            .get::<EntryPage>(&ODataId::from(format!("{entries}?$skip={skip}")))
            .await
            .map_err(|error| error.classify())?;
        let honored =
            tail.members.first().map(NavProperty::id) != page.members.first().map(NavProperty::id);
        if honored {
            offset = skip;
            page = tail;
        }
    }
    let mut next = page.next_link.clone();
    let mut pages = vec![page];
    while let Some(link) = next.take() {
        if pages.len() >= budget.members {
            break;
        }
        let Some(id) = next_page_id(&link) else {
            break;
        };
        let page = bmc
            .get::<EntryPage>(&id)
            .await
            .map_err(|error| error.classify())?;
        if page.members.is_empty() {
            break;
        }
        next.clone_from(&page.next_link);
        pages.push(page);
    }
    Ok(EntryWindow::new(pages, offset, count))
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
        with_deadline(WalkBudget::DEFAULT.elapsed, async {
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
            let walk = walk_entries(bmc, entries.id(), WalkBudget::DEFAULT, cursor).await?;
            let parts = assemble_logs(walk.records, walk.issues, scope)
                .map_err(|error| internal_bug(&error))?;
            // The acquisition is now certain to ship; a walk that failed or
            // was cancelled before this point leaves the cursor where the
            // last shipped walk put it.
            cursor.store(walk.position);
            Ok(parts)
        })
        .await
    }
}

/// Where the previous shipped walk of a log service ended: the newest
/// `occurred_at` it emitted and every `entry_id` emitted at that instant.
///
/// The next walk, reading newest first, stops at the first record the
/// cursor already covers, so a poll costs the new entries plus one member
/// rather than the whole window, and a consumer stops seeing every record
/// repeated each poll. Devices stamp to the second and a burst lands many
/// records on one instant, which is why the instant alone cannot place a
/// record and the ids emitted at that instant ride along.
///
/// The ids kept at one instant are bounded to four walks' worth. A device
/// whose clock is frozen stamps every entry on one second, which would
/// otherwise grow the cursor for the life of the process; past the bound
/// only the latest walk's ids are kept, which is where a newest-first walk
/// stops anyway. A device whose member order is not chronological may then
/// re-ship a few same-second entries after an overflow.
///
/// Two things move the cursor backwards. A log that was wiped and refilled,
/// or a device clock stepped back, makes the newest entry present *older*
/// than the cursor; the walk then discards the cursor and emits the whole
/// window. A wipe refilled within the cursor's own second under the same
/// ids is invisible, which the second's resolution makes unavoidable.
///
/// Records the cursor cannot place are emitted every poll: entries the
/// device does not stamp, and entries that projected no record at all — a
/// faulty entry is reported each time it is met, since it was never shipped.
///
/// One per [`Read`], shared by its clones; not persisted, so a restart
/// replays one window, bounded by [`WalkBudget`].
#[derive(Debug, Default)]
pub struct LogCursor {
    position: std::sync::Mutex<Option<Position>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Position {
    at: Timestamp,
    ids: std::collections::BTreeSet<String>,
}

/// Where one record stands relative to a [`Position`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Placement {
    /// After the position: not yet shipped.
    Newer,
    /// At or before the position: shipped by an earlier walk.
    Covered,
    /// Before the position's instant while being the newest entry present:
    /// the log no longer holds what the position described.
    Older,
}

impl Position {
    fn place(&self, record: &LogRecord) -> Option<Placement> {
        let at = record.occurred_at()?;
        Some(match at.cmp(&self.at) {
            std::cmp::Ordering::Greater => Placement::Newer,
            std::cmp::Ordering::Less => Placement::Older,
            std::cmp::Ordering::Equal => {
                if record.entry_id().is_some_and(|id| self.ids.contains(id)) {
                    Placement::Covered
                } else {
                    Placement::Newer
                }
            }
        })
    }

    /// The position after shipping `records` on top of `previous`: the
    /// newest instant among them, with the ids at that instant — merged
    /// with the previous ids when the instant did not move, up to
    /// `retained` ids.
    fn after(previous: Option<&Self>, records: &[LogRecord], retained: usize) -> Option<Self> {
        let at = records
            .iter()
            .filter_map(LogRecord::occurred_at)
            .max()
            .copied();
        let Some(at) = at else {
            return previous.cloned();
        };
        let ids = Self::ids_at(records, &at);
        match previous {
            Some(previous) if previous.at == at => {
                let mut merged = previous.ids.clone();
                merged.extend(ids.iter().cloned());
                // Past the bound, keep this walk's ids alone: read newest
                // first, they are what the next walk meets before any older
                // shipped entry.
                let ids = if merged.len() > retained { ids } else { merged };
                Some(Self { at, ids })
            }
            Some(previous) if previous.at > at => Some(previous.clone()),
            _ => Some(Self { at, ids }),
        }
    }

    /// The entry ids among `records` stamped exactly at `at`.
    fn ids_at(records: &[LogRecord], at: &Timestamp) -> std::collections::BTreeSet<String> {
        records
            .iter()
            .filter(|record| record.occurred_at() == Some(at))
            .filter_map(|record| record.entry_id().map(str::to_owned))
            .collect()
    }
}

impl LogCursor {
    fn load(&self) -> Option<Position> {
        self.position.lock().expect("log cursor poisoned").clone()
    }

    fn store(&self, position: Option<Position>) {
        *self.position.lock().expect("log cursor poisoned") = position;
    }
}

/// What a walk projected: records, the issues against their members, and
/// the position to store once the records have shipped.
struct Walk {
    records: Vec<LogRecord>,
    issues: Vec<ProjectionIssue>,
    position: Option<Position>,
}

/// Visits the newest members, newest first, until the budget is spent or
/// the cursor's position is reached, and projects each.
async fn walk_entries<B>(
    bmc: &B,
    entries: &ODataId,
    budget: WalkBudget,
    cursor: &LogCursor,
) -> Result<Walk, AcquisitionFailure>
where
    B: Bmc,
    B::Error: ClassifyError,
{
    let started = Instant::now();
    let window = collect_entries(bmc, entries, budget).await?;
    let mut previous = cursor.load();
    let mut records = Vec::new();
    // Issues are keyed by member so they read in collection order whatever
    // order the walk visited members in.
    let mut member_issues: Vec<(usize, ProjectionIssue)> = Vec::new();
    let mut visited = 0;
    let mut stopped = None;
    let mut reached_cursor = false;
    let mut placed_newest = false;
    for (index, member) in window.newest_first(budget) {
        if let Some(reason) = budget.exhausted(visited, started.elapsed()) {
            stopped = Some(reason);
            break;
        }
        visited += 1;
        let entry = match member.get(bmc).await {
            Ok(entry) => entry,
            Err(error) => {
                member_issues.push((index, member_disposition(index, error.classify())?));
                continue;
            }
        };
        let location = member.id().to_string();
        let parts = project_log_entry(&entry, &location).map_err(|error| internal_bug(&error))?;
        let placement = previous
            .as_ref()
            .zip(parts.log_records.first())
            .and_then(|(position, record)| position.place(record));
        match placement {
            Some(Placement::Older) if !placed_newest => previous = None,
            Some(Placement::Covered | Placement::Older) => {
                reached_cursor = true;
                break;
            }
            Some(Placement::Newer) | None => {}
        }
        placed_newest |= placement.is_some();
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
    if !reached_cursor && visited < window.total {
        issues.push(ProjectionIssue::invalid(
            TRUNCATED_WALK_LOCATOR,
            format!(
                "walk kept the newest {visited} of {} members: {}",
                window.total,
                stopped.unwrap_or("member budget spent")
            ),
        ));
    }
    let position = Position::after(previous.as_ref(), &records, budget.retained_ids());
    Ok(Walk {
        records,
        issues,
        position,
    })
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

    const ENTRIES: &str = "/redfish/v1/Systems/1/LogServices/SEL/Entries";

    /// One page of a six-entry collection: members `range`, the device's
    /// count, and the link onward if any.
    fn page(range: std::ops::Range<usize>, count: usize, next: Option<&str>) -> String {
        let members: Vec<String> = range
            .map(|index| format!(r#"{{ "@odata.id": "{ENTRIES}/{index}" }}"#))
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
        (
            format!("{ENTRIES}/{index}"),
            format!(
                r##"{{ "@odata.id": "{ENTRIES}/{index}", "@odata.type": "#LogEntry.v1_21_0.LogEntry",
                     "Id": "{index}", "Name": "Entry", "EntryType": "Event",
                     "Created": "{created}", "Message": "m{index}" }}"##
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
    async fn walk_with(
        bmc: &MockBmc,
        cursor: &super::LogCursor,
        budget: WalkBudget,
    ) -> (Vec<usize>, Vec<nv_telemetry_source::ProjectionIssue>) {
        let walk = super::walk_entries(bmc, &ENTRIES.to_owned().into(), budget, cursor)
            .await
            .expect("the walk completes");
        cursor.store(walk.position);
        let mut ids: Vec<usize> = walk
            .records
            .iter()
            .filter_map(|record| record.entry_id()?.parse().ok())
            .collect();
        ids.sort_unstable();
        (ids, walk.issues)
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
    async fn a_burst_within_the_cursors_second_is_told_apart_by_id() {
        let same_second = "2026-03-01T10:00:02Z";
        let bmc = MockBmc::default();
        let cursor = super::LogCursor::default();
        prime(&bmc, &[(ENTRIES.to_owned(), page(0..3, 3, None))]);
        prime(&bmc, &[entry_at(2, same_second), entry(1), entry(0)]);
        walk_with(&bmc, &cursor, generous()).await;

        // Two more entries stamped on the cursor's own second: new ids at a
        // known instant are new records; the known id ends the walk.
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

        // The ids at that instant accumulate, so the burst is not re-shipped.
        prime(&bmc, &[(ENTRIES.to_owned(), page(0..5, 5, None))]);
        prime(&bmc, &[entry_at(4, same_second)]);
        assert!(walk_with(&bmc, &cursor, generous()).await.0.is_empty());
    }

    #[tokio::test]
    async fn a_frozen_clock_does_not_grow_the_cursor_without_bound() {
        // Two members per walk, so the cursor keeps at most eight ids at one
        // instant. Every entry carries the same stamp, as from a device whose
        // clock never advances.
        let frozen = "2026-03-01T10:00:00Z";
        let budget = WalkBudget::new(2, Duration::from_secs(10));
        let bmc = MockBmc::default();
        let cursor = super::LogCursor::default();
        let retained = |cursor: &super::LogCursor| cursor.load().expect("a position").ids.len();

        // Two new entries per poll: the ids accumulate up to the bound.
        for end in [2, 4, 6, 8] {
            prime(&bmc, &[(ENTRIES.to_owned(), page(0..end, end, None))]);
            prime(
                &bmc,
                &[entry_at(end - 1, frozen), entry_at(end - 2, frozen)],
            );
            assert_eq!(walk_with(&bmc, &cursor, budget).await.0, [end - 2, end - 1]);
            assert_eq!(retained(&cursor), end);
        }

        // One more poll would exceed the bound, so only its own ids remain.
        prime(&bmc, &[(ENTRIES.to_owned(), page(0..10, 10, None))]);
        prime(&bmc, &[entry_at(9, frozen), entry_at(8, frozen)]);
        assert_eq!(walk_with(&bmc, &cursor, budget).await.0, [8, 9]);
        assert_eq!(retained(&cursor), 2);

        // Those ids are the newest shipped, so they still end the next walk.
        prime(
            &bmc,
            &[
                (ENTRIES.to_owned(), page(0..10, 10, None)),
                entry_at(9, frozen),
            ],
        );
        assert!(walk_with(&bmc, &cursor, budget).await.0.is_empty());
        prime(&bmc, &[(ENTRIES.to_owned(), page(0..11, 11, None))]);
        prime(&bmc, &[entry_at(10, frozen), entry_at(9, frozen)]);
        assert_eq!(walk_with(&bmc, &cursor, budget).await.0, [10]);
    }

    #[tokio::test]
    async fn a_log_whose_newest_entry_predates_the_cursor_is_read_again() {
        let bmc = MockBmc::default();
        let cursor = super::LogCursor::default();
        prime(&bmc, &whole_log(0..3));
        walk_with(&bmc, &cursor, generous()).await;

        // Wiped and refilled with older stamps, or the device clock stepped
        // back: the newest entry is older than the cursor, so the cursor is
        // discarded and the whole log ships.
        prime(&bmc, &whole_log(0..2));
        let (ids, issues) = walk_with(&bmc, &cursor, generous()).await;
        assert_eq!(ids, [0, 1]);
        assert!(issues.is_empty());

        prime(&bmc, &[(ENTRIES.to_owned(), page(0..2, 2, None)), entry(1)]);
        assert!(walk_with(&bmc, &cursor, generous()).await.0.is_empty());
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
        // the pages from the start and still keeps the newest three.
        let skip2 = format!("{ENTRIES}?$skip=2");
        let skip3 = format!("{ENTRIES}?$skip=3");
        let skip4 = format!("{ENTRIES}?$skip=4");
        let mut answers = vec![
            (ENTRIES.to_owned(), page(0..2, 6, Some(&skip2))),
            (skip3, page(0..2, 6, Some(&skip2))),
            (skip2, page(2..4, 6, Some(&skip4))),
            (skip4, page(4..6, 6, None)),
        ];
        answers.extend((3..6).rev().map(entry));
        let (ids, issues) = walk(&answers, WalkBudget::new(3, Duration::from_secs(10))).await;
        assert_eq!(ids, [3, 4, 5]);
        assert_eq!(issues, [truncated(3, 6, "member budget spent")]);
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
        let id = |link: &str| super::next_page_id(link).map(|id| id.to_string());
        assert_eq!(
            id("/redfish/v1/x?$skip=1"),
            Some("/redfish/v1/x?$skip=1".into())
        );
        assert_eq!(
            id("https://bmc.example/redfish/v1/x?$skip=2"),
            Some("/redfish/v1/x?$skip=2".into())
        );
        assert_eq!(id("nonsense"), None);
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
