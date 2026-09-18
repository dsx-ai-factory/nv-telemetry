// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The event stream provider: a device's server-sent events as log records.
//!
//! A connect attempt reads the service root and its `EventService` and opens
//! the SSE stream the service advertises; each `Event` payload becomes one
//! item, its records projected by the `EventRecord` manifest into one logs
//! batch. The batch is scoped to the event service and to one run of the
//! device's event ids: an `EventId` restarts with the device and is unique
//! only within the sequence one subscription reads, so the scope carries a
//! [`StreamRun`] minted whenever a stream starts live, and kept while a
//! later instance resumes it. The provider remembers the SSE id in effect
//! after each payload and reopens the stream after it, so a reconnect loses
//! nothing the device retained and ships under the scope it left. A device
//! that refuses the position — the 400 the mock gives for an id outside its
//! history — answers with a protocol failure the provider reports, naming
//! the id, and does not act on: the answer cannot tell a history that aged
//! out, where the run continues, from a device that restarted its ids,
//! where it must not, so the position stays and the choice is the
//! embedder's, which starts a stream afresh or hands a position back.
//! bmcweb does not refuse: it replays what it has and sends
//! `EventBufferExceeded` first, which is the gap issue below, within the
//! same run. The consumer's dedup key — endpoint, scope, `entry_id` — thus
//! never collapses distinct events. The polled log walk is a separate
//! source with its own scope; on devices whose events do not name their log
//! entry, which is most, a consumer that enables both sees each event under
//! both.
//!
//! Records arrive inline; one the device only references is recorded as
//! an issue and never fetched, because the stream is drained outside the
//! dispatcher and a request from it would be one nothing admitted.
//!
//! The stream is live delivery, not completeness. An `EventBufferExceeded`
//! record says the device dropped events before this stream caught up: it
//! is an issue at [`GAP_LOCATOR`], never a record, and the polled walk fills
//! the gap. A `MetricReport` payload is not this provider's data; the first
//! one on a stream is an issue at [`METRIC_REPORT_LOCATOR`] and the rest
//! yield nothing. The stream ends as the contract requires, with one
//! terminal failure: the transport's, or, when the device closes it without
//! one, a retryable `Protocol` failure, so the embedder reconnects either
//! way.

use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::PoisonError;

use futures_util::stream::unfold;
use futures_util::stream::BoxStream;
use futures_util::Stream;
use futures_util::StreamExt as _;
use nv_redfish::core::BoxTryStream;
use nv_redfish::core::NavProperty;
use nv_redfish::core::StreamEvent;
use nv_redfish::event_service::EventStreamPayload;
use nv_redfish::schema::event::Event;
use nv_redfish::schema::event::EventRecord;
use nv_redfish::Bmc;
use nv_redfish::ServiceRoot;
use nv_telemetry_model::EndpointContext;
use nv_telemetry_model::Invalid;
use nv_telemetry_model::Origin;
use nv_telemetry_model::Subject;
use nv_telemetry_source::Acquire;
use nv_telemetry_source::AcquisitionFailure;
use nv_telemetry_source::AcquisitionFailureClass;
use nv_telemetry_source::AcquisitionParts;
use nv_telemetry_source::ProjectionIssue;
use nv_telemetry_source::ProviderDeclaration;
use nv_telemetry_source::SubscriptionItem;

use crate::failure::ClassifyError;
use crate::projection::project_event_record;
use crate::provider::assemble_logs;
use crate::provider::internal_bug;

/// The issue a stream raises when the device reports that events were
/// dropped before this stream caught up.
pub const GAP_LOCATOR: &str = "@gap";

/// The issue a stream raises, once, when the device sends metric reports
/// this provider does not carry.
pub const METRIC_REPORT_LOCATOR: &str = "@metric-report";

/// The scope's kind for every batch a stream ships.
const SCOPE_KIND: &str = "event-service";

/// One run of a device's event ids, as the batches' scope names it: minted
/// when an instance starts live, continued by the instances that resume it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamRun(String);

impl StreamRun {
    /// A run named `run`, checked against the scope bounds it must fit.
    ///
    /// # Errors
    ///
    /// The scope's own refusal when the name is empty or over its bound.
    pub fn new(run: impl Into<String>) -> Result<Self, Invalid> {
        let run = run.into();
        scope_of("EventService", &run)?;
        Ok(Self(run))
    }

    /// A fresh run.
    fn mint() -> Self {
        Self::new(uuid::Uuid::new_v4().to_string()).expect("a UUID fits the scope's bounds")
    }

    /// The run's name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for StreamRun {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A resume position without an id: the SSE processing model reads an
/// empty id as none, so such a position would start live while still
/// claiming the run it names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct EmptyEventId;

impl fmt::Display for EmptyEventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a resume position needs a non-empty event id")
    }
}

impl std::error::Error for EmptyEventId {}

/// Where a stream resumes: the SSE id in effect after the last payload
/// read, and the run it belongs to, so the instance that resumes ships
/// under the scope of the one it continues. An embedder that keeps this
/// across its own restart hands it back through
/// [`EventStream::with_resume_position`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResumePosition {
    last_event_id: String,
    run: StreamRun,
}

impl ResumePosition {
    /// After `last_event_id`, continuing `run`.
    ///
    /// # Errors
    ///
    /// [`EmptyEventId`] when there is no id to resume after.
    pub fn new(last_event_id: impl Into<String>, run: StreamRun) -> Result<Self, EmptyEventId> {
        let last_event_id = last_event_id.into();
        if last_event_id.is_empty() {
            return Err(EmptyEventId);
        }
        Ok(Self { last_event_id, run })
    }

    /// The SSE id the next instance asks the device to resume after.
    #[must_use]
    pub fn last_event_id(&self) -> &str {
        &self.last_event_id
    }

    /// The run the next instance continues.
    #[must_use]
    pub fn run(&self) -> &StreamRun {
        &self.run
    }
}

/// Where the stream stands between instances.
#[derive(Debug, Default)]
struct Cursor {
    /// The run the current or last instance read, minted when an instance
    /// starts live.
    run: Option<StreamRun>,
    /// The SSE id in effect after the last payload read, when the device
    /// sends ids at all.
    last_event_id: Option<String>,
}

/// How the next instance starts.
struct Start {
    run: StreamRun,
    /// The id to resume after; `None` starts live.
    after: Option<String>,
}

impl Cursor {
    /// Where the next instance starts: after the last id, in the run it
    /// belongs to, or live in a new run when there is no id to resume from.
    fn start(&mut self) -> Start {
        if let (Some(run), Some(after)) = (&self.run, &self.last_event_id) {
            return Start {
                run: run.clone(),
                after: Some(after.clone()),
            };
        }
        let run = StreamRun::mint();
        self.run = Some(run.clone());
        self.last_event_id = None;
        Start { run, after: None }
    }

    fn advance(&mut self, last_event_id: String) {
        self.last_event_id = Some(last_event_id);
    }

    fn position(&self) -> Option<ResumePosition> {
        Some(ResumePosition {
            last_event_id: self.last_event_id.clone()?,
            run: self.run.clone()?,
        })
    }
}

/// The critical sections never panic, so a poisoned lock still holds a
/// consistent cursor.
fn lock(cursor: &Mutex<Cursor>) -> MutexGuard<'_, Cursor> {
    cursor.lock().unwrap_or_else(PoisonError::into_inner)
}

/// One endpoint's event stream, opened over its `Bmc`: log records from
/// server-sent events.
pub struct EventStream<B> {
    endpoint: EndpointContext,
    origin: Origin,
    bmc: Arc<B>,
    cursor: Arc<Mutex<Cursor>>,
}

impl<B> EventStream<B> {
    /// Provider identity, as `Origin.provider` carries it.
    pub const PROVIDER: &'static str = "redfish.event-service.sse";

    /// Request class, as dispatcher lanes and breakers key it.
    pub const REQUEST_CLASS: &'static str = "event-stream";

    /// The declared weight of one connect attempt: the service root, the
    /// event service, and the stream itself are three requests.
    pub const COST: u64 = 3;

    /// This provider's declaration, single-sourced from the same constants
    /// its `Origin` is built from.
    #[must_use]
    pub fn declaration() -> ProviderDeclaration {
        ProviderDeclaration::streamed(Self::PROVIDER, Self::REQUEST_CLASS, Self::COST)
    }

    /// The event stream of the endpoint `bmc` reaches. The service root
    /// names the event service, so no target is asked for. The first
    /// instance starts live; each later one resumes after the last id read.
    ///
    /// # Panics
    ///
    /// Never in practice: the origin is built from this type's own
    /// constants, which satisfy the origin's bounds.
    #[must_use]
    pub fn new(endpoint: EndpointContext, bmc: Arc<B>) -> Self {
        let origin = Origin::builder()
            .provider(Self::PROVIDER)
            .request_class(Self::REQUEST_CLASS)
            .build()
            .expect("the provider's constants satisfy the origin's bounds");
        Self {
            endpoint,
            origin,
            bmc,
            cursor: Arc::new(Mutex::new(Cursor::default())),
        }
    }

    /// Starts the first instance at `position` instead of live: what
    /// [`Self::resume_position`] gave an embedder before it restarted.
    #[must_use]
    pub fn with_resume_position(self, position: ResumePosition) -> Self {
        *lock(&self.cursor) = Cursor {
            run: Some(position.run),
            last_event_id: Some(position.last_event_id),
        };
        self
    }

    /// Where the next instance resumes, once the device has sent an id;
    /// `None` before the first id and on a device that sends none. A
    /// position the device refused stays, for the embedder to keep or
    /// drop.
    #[must_use]
    pub fn resume_position(&self) -> Option<ResumePosition> {
        lock(&self.cursor).position()
    }
}

impl<B> fmt::Debug for EventStream<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventStream")
            .field("endpoint_id", &self.endpoint.endpoint_id())
            .field("provider", &self.origin.provider())
            .field("request_class", &self.origin.request_class())
            .field("resume_position", &self.resume_position())
            .finish_non_exhaustive()
    }
}

impl<B> Acquire for EventStream<B>
where
    B: Bmc + 'static,
    B::Error: ClassifyError + 'static,
{
    type Output = BoxStream<'static, SubscriptionItem>;

    fn endpoint(&self) -> &EndpointContext {
        &self.endpoint
    }

    fn origin(&self) -> &Origin {
        &self.origin
    }

    async fn perform(&self) -> Result<Self::Output, AcquisitionFailure> {
        let root = ServiceRoot::new(Arc::clone(&self.bmc))
            .await
            .map_err(|error| classify_wrapper(&error))?;
        let service = root
            .event_service()
            .await
            .map_err(|error| classify_wrapper(&error))?
            .ok_or_else(|| {
                AcquisitionFailure::new(AcquisitionFailureClass::Unsupported)
                    .with_retryable(false)
                    .with_detail("the service root advertises no EventService")
            })?;
        let start = lock(&self.cursor).start();
        let scope = stream_scope(&service.raw().id, &start.run)?;
        let events = service
            .events_from(start.after.as_deref())
            .await
            .map_err(|error| {
                told_against_resume(start.after.as_deref(), classify_wrapper(&error))
            })?;
        Ok(Box::pin(items(Live {
            events,
            bmc: Arc::clone(&self.bmc),
            scope,
            cursor: Arc::clone(&self.cursor),
            metric_reports_seen: false,
        })))
    }
}

/// A failed open, told against the position it resumed from when there
/// was one: the same class and retryability the answer classified to, its
/// detail naming the id, so the embedder can tell a refused position from
/// any other failed connect. The position itself is left as it was.
fn told_against_resume(after: Option<&str>, failure: AcquisitionFailure) -> AcquisitionFailure {
    let Some(after) = after else {
        return failure;
    };
    let mut told = AcquisitionFailure::new(failure.class()).with_detail(format!(
        "open while resuming after event {after} failed: {}",
        failure.detail().unwrap_or("no detail")
    ));
    if let Some(retryable) = failure.retryable() {
        told = told.with_retryable(retryable);
    }
    told
}

/// One open stream and what its items are built with.
struct Live<B: Bmc> {
    events: BoxTryStream<StreamEvent<EventStreamPayload>, nv_redfish::Error<B>>,
    bmc: Arc<B>,
    scope: Subject,
    cursor: Arc<Mutex<Cursor>>,
    metric_reports_seen: bool,
}

/// The payloads as items: one per `Event`, one for the first
/// `MetricReport`, and one terminal failure that ends the stream. Every
/// payload's id, of either kind, moves the cursor before the item is built.
fn items<B>(live: Live<B>) -> impl Stream<Item = SubscriptionItem> + Send
where
    B: Bmc + 'static,
    B::Error: ClassifyError + 'static,
{
    unfold(Some(live), |live| async move {
        let mut live = live?;
        loop {
            let payload = match live.events.next().await {
                Some(Ok(StreamEvent {
                    last_event_id,
                    data,
                })) => {
                    if let Some(id) = last_event_id {
                        lock(&live.cursor).advance(id);
                    }
                    data
                }
                Some(Err(error)) => return Some((Err(classify_wrapper(&error)), None)),
                None => {
                    let closed = AcquisitionFailure::new(AcquisitionFailureClass::Protocol)
                        .with_retryable(true)
                        .with_detail("the device closed the event stream");
                    return Some((Err(closed), None));
                }
            };
            match payload {
                EventStreamPayload::Event(event) => {
                    return Some(
                        match project_event(&event, live.bmc.as_ref(), &live.scope).await {
                            Ok(parts) => (Ok(parts), Some(live)),
                            Err(failure) => (Err(failure), None),
                        },
                    );
                }
                EventStreamPayload::MetricReport(_) => {
                    if live.metric_reports_seen {
                        continue;
                    }
                    live.metric_reports_seen = true;
                    let issue = ProjectionIssue::invalid(
                        METRIC_REPORT_LOCATOR,
                        "the stream carries metric reports, which this provider does not ship",
                    );
                    return Some((
                        Ok(AcquisitionParts::new(Vec::new(), vec![issue])),
                        Some(live),
                    ));
                }
            }
        }
    })
}

/// One `Event`'s records as a logs batch under `scope`. A record the device
/// did not inline is an issue against its index, never a request.
async fn project_event<B>(
    event: &Event,
    bmc: &B,
    scope: &Subject,
) -> Result<AcquisitionParts, AcquisitionFailure>
where
    B: Bmc,
    B::Error: ClassifyError,
{
    let mut records = Vec::new();
    let mut issues = Vec::new();
    for (index, member) in event.events.iter().enumerate() {
        if matches!(member, NavProperty::Reference(_)) {
            issues.push(ProjectionIssue::invalid(
                format!("Events[{index}]"),
                "record not inline; a stream fetches nothing",
            ));
            continue;
        }
        // The wrapper's one accessor for an inline record takes the
        // transport and, for an inline record, never uses it.
        let record = member.get(bmc).await.map_err(|_| {
            AcquisitionFailure::new(AcquisitionFailureClass::Internal)
                .with_retryable(false)
                .with_detail("projection bug: an inline event record reported a transport failure")
        })?;
        if reports_dropped_events(&record) {
            issues.push(ProjectionIssue::invalid(
                GAP_LOCATOR,
                "the device dropped events before this stream caught up; the polled log fills the gap",
            ));
            continue;
        }
        let parts = project_event_record(&record, &member.id().to_string())
            .map_err(|error| internal_bug(&error))?;
        records.extend(parts.log_records);
        issues.extend(
            parts
                .issues
                .into_iter()
                .map(|issue| issue.at_index("Events", index)),
        );
    }
    assemble_logs(records, issues, scope.clone()).map_err(|error| internal_bug(&error))
}

/// The registry message a device sends when its event buffer overflowed:
/// `<Registry>.<Major>.<Minor>.EventBufferExceeded`.
fn reports_dropped_events(record: &EventRecord) -> bool {
    record.message_id.rsplit('.').next() == Some("EventBufferExceeded")
}

/// The scope of a batch from `service_id`'s stream reading `run`, as the
/// model bounds it.
fn scope_of(service_id: &str, run: &str) -> Result<Subject, Invalid> {
    Subject::builder()
        .kind(SCOPE_KIND)
        .id(service_id)
        .scope(vec!["stream".to_owned(), run.to_owned()])
        .build()
}

/// The scope every record of one stream ships under: the event service, and
/// the run of event ids this stream reads. The run is bounded at
/// construction; only the device's service id is left to refuse.
fn stream_scope(service_id: &str, run: &StreamRun) -> Result<Subject, AcquisitionFailure> {
    scope_of(service_id, run.as_str()).map_err(|_| {
        AcquisitionFailure::new(AcquisitionFailureClass::Protocol)
            .with_retryable(false)
            .with_detail("event service identity violates subject bounds")
    })
}

/// The wrapper's failures as this crate classifies them: the transport's
/// through its own table; a document that did not decode, the root's, the
/// service's, or a payload's, as a protocol answer; anything else as a
/// capability the service lacks.
fn classify_wrapper<B>(error: &nv_redfish::Error<B>) -> AcquisitionFailure
where
    B: Bmc,
    B::Error: ClassifyError,
{
    match error {
        nv_redfish::Error::Bmc(transport) => transport.classify(),
        nv_redfish::Error::Json(decode) => {
            AcquisitionFailure::new(AcquisitionFailureClass::Protocol)
                .with_retryable(false)
                .with_detail(format!("document did not decode: {decode}"))
        }
        other => AcquisitionFailure::new(AcquisitionFailureClass::Unsupported)
            .with_retryable(false)
            .with_detail(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(name: &str) -> StreamRun {
        StreamRun::new(name).expect("a short name fits the scope")
    }

    #[test]
    fn a_run_is_bounded_like_the_scope_it_names() {
        assert!(
            StreamRun::new("").is_err(),
            "an empty segment names nothing"
        );
        assert!(
            StreamRun::new("r".repeat(4096)).is_err(),
            "a segment over the scope's bound is refused"
        );
        StreamRun::mint();
    }

    #[test]
    fn a_cursor_resumes_a_run_it_has_an_id_for_and_otherwise_starts_a_new_one() {
        let mut cursor = Cursor::default();
        assert_eq!(cursor.position(), None);

        // Nothing to resume: a run is minted, and the instance starts live.
        let first = cursor.start();
        assert!(first.after.is_none());
        assert_eq!(cursor.position(), None, "a run alone is not a position");

        // An id read: the next instance resumes after it, in the same run.
        cursor.advance("41".to_owned());
        assert_eq!(
            cursor.position(),
            Some(ResumePosition::new("41", first.run.clone()).expect("a non-empty id"))
        );
        let second = cursor.start();
        assert_eq!(second.run, first.run);
        assert_eq!(second.after.as_deref(), Some("41"));

        // A run without an id, as after a device that sent none: the next
        // instance starts live in a new run.
        cursor.last_event_id = None;
        let third = cursor.start();
        assert!(third.after.is_none());
        assert_ne!(third.run, first.run);
    }

    #[test]
    fn a_failed_open_while_resuming_names_the_id_and_keeps_the_answer() {
        // The device's answer stands: class and retryability as classified,
        // the detail naming the position so the embedder can tell a refused
        // resume from any other failed connect.
        let refused = told_against_resume(
            Some("41"),
            AcquisitionFailure::new(AcquisitionFailureClass::Protocol)
                .with_retryable(false)
                .with_detail("HTTP 400"),
        );
        assert_eq!(refused.class(), AcquisitionFailureClass::Protocol);
        assert_eq!(refused.retryable(), Some(false));
        assert_eq!(
            refused.detail(),
            Some("open while resuming after event 41 failed: HTTP 400")
        );

        // A source that left retryability to policy still does.
        let unrefined = told_against_resume(
            Some("41"),
            AcquisitionFailure::new(AcquisitionFailureClass::Connectivity),
        );
        assert_eq!(unrefined.class(), AcquisitionFailureClass::Connectivity);
        assert_eq!(unrefined.retryable(), None);
        assert_eq!(
            unrefined.detail(),
            Some("open while resuming after event 41 failed: no detail")
        );

        // Without a position, the failure is what it was.
        let live = told_against_resume(
            None,
            AcquisitionFailure::new(AcquisitionFailureClass::Protocol).with_detail("HTTP 400"),
        );
        assert_eq!(live.detail(), Some("HTTP 400"));
        assert_eq!(live.retryable(), None);
    }

    #[test]
    fn a_position_needs_an_id_to_resume_after() {
        assert_eq!(
            ResumePosition::new("", run("run-a")),
            Err(EmptyEventId),
            "an empty id would start live in the run it claims"
        );
    }

    #[test]
    fn a_position_handed_in_is_where_the_first_instance_starts() {
        let position = ResumePosition::new("41", run("run-a")).expect("a non-empty id");
        let stream = EventStream::<()>::new(
            EndpointContext::builder()
                .endpoint_id("bmc-lab-07")
                .build()
                .expect("a valid endpoint"),
            Arc::new(()),
        )
        .with_resume_position(position.clone());
        assert_eq!(stream.resume_position(), Some(position.clone()));
        let start = lock(&stream.cursor).start();
        assert_eq!(start.run, *position.run());
        assert_eq!(start.after.as_deref(), Some("41"));
    }
}
