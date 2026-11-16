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
//! [`StreamRun`] minted whenever a stream starts live or the device reports
//! a gap, and kept while a later instance resumes it. The consumer's dedup
//! key — endpoint, scope, `entry_id` — thus does not collapse distinct
//! events, within the two limits below.
//!
//! The provider remembers the SSE id in effect after each payload and
//! reopens the stream after it, so a reconnect loses nothing the device
//! retained and ships under the scope it left. A failed open keeps the
//! position, unless the device refused it. A device that no longer holds
//! the position cannot say whether its history aged out, where the run
//! continues, or its ids restarted, where it must not, so the provider takes
//! the safe reading of either answer it gives: a new run. A device that
//! refuses — the 400 the mock gives for an id outside its history — has the
//! position dropped and the stream opened live within the same attempt, the
//! first item an issue at [`GAP_LOCATOR`] naming the id. bmcweb does not
//! refuse: it sends `EventBufferExceeded` and goes on with ids that count
//! from one again after a restart, and that record, the gap issue below,
//! starts the new run. A new run where the old one continued only changes
//! the scope; reusing it after a restart would collapse distinct events.
//!
//! Two limits remain on a device that restarts its ids without marking
//! them, as bmcweb does. After a restart, once it has issued the resumed id
//! again, it resumes after it without a sign: the events before that id
//! never reach the stream, and no gap is reported. And a position older
//! than the last one read, which only [`EventStream::with_resume_position`]
//! can supply, then resumes inside the ids its run has already shipped,
//! where the key does collapse distinct events. A device that marks its ids
//! with their run, as the mock does, refuses both.
//!
//! The polled log walk is a separate source with its own scope; on devices
//! whose events do not name their log entry, which is most, a consumer that
//! enables both sees each event under both.
//!
//! Records arrive inline; one the device only references is recorded as
//! an issue and never fetched, because the stream is drained outside the
//! dispatcher and a request from it would be one nothing admitted.
//!
//! The stream is live delivery, not completeness. An `EventBufferExceeded`
//! record says the device dropped events before this stream caught up: it
//! is an issue at [`GAP_LOCATOR`], never a record, its payload and those
//! after it ship under a new run, and the polled walk fills the gap. A
//! `MetricReport` payload is not this provider's data; the first
//! one on a stream is an issue at [`METRIC_REPORT_LOCATOR`] and the rest
//! yield nothing. A payload that does not decode, even after the platform's
//! repairs, is an issue at [`PAYLOAD_LOCATOR`] and the stream reads on: the
//! payloads after it are unaffected, and ending the stream would only
//! resume into the same payload. The stream ends as the contract requires, with one
//! terminal failure: the transport's, or, when the device closes it without
//! one, a retryable `Protocol` failure, so the embedder reconnects either
//! way.

use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::PoisonError;

use futures_util::future::ready;
use futures_util::stream::once;
use futures_util::stream::unfold;
use futures_util::stream::BoxStream;
use futures_util::Stream;
use futures_util::StreamExt as _;
use nv_redfish::core::repair::decode;
use nv_redfish::core::BoxTryStream;
use nv_redfish::core::NavProperty;
use nv_redfish::core::RepairError;
use nv_redfish::core::StreamEvent;
use nv_redfish::event_service::EventStreamPayload;
use nv_redfish::quirks::CompatBmc;
use nv_redfish::schema::event::Event;
use nv_redfish::schema::event::EventRecord;
use nv_redfish::Bmc;
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
use serde_json::Value;

use crate::failure::refuses_position;
use crate::failure::ClassifyError;
use crate::projection::project_event_record;
use crate::provider::assemble_logs;
use crate::provider::internal_bug;
use crate::transport::Link;
use crate::transport::LinkError;
use crate::transport::RedfishService;
use crate::transport::RedfishTransport;
use crate::transport::Root;

/// The issue a stream raises when the device reports that events were
/// dropped before this stream caught up.
pub const GAP_LOCATOR: &str = "@gap";

/// The issue a stream raises, once, when the device sends metric reports
/// this provider does not carry.
pub const METRIC_REPORT_LOCATOR: &str = "@metric-report";

/// The issue a stream raises for each payload that does not decode.
pub const PAYLOAD_LOCATOR: &str = "@payload";

/// The scope's kind for every batch a stream ships.
const SCOPE_KIND: &str = "event-service";

/// One run of a device's event ids, as the batches' scope names it: minted
/// when an instance starts live or the device reports a gap, continued by
/// the instances that resume it.
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
/// across its own restart hands the last one it read back through
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
enum Cursor {
    /// No id read: the next instance starts live, in a new run.
    #[default]
    Live,
    /// The next instance resumes here.
    Resume(ResumePosition),
    /// The device refused to resume after this id: the stream opens live,
    /// in a new run, and reports the gap once it does.
    Refused(String),
}

/// How the next instance starts.
struct Start {
    run: StreamRun,
    /// The id to resume after; `None` starts live.
    after: Option<String>,
}

impl Cursor {
    fn start(&self) -> Start {
        match self {
            Self::Resume(position) => Start {
                run: position.run.clone(),
                after: Some(position.last_event_id.clone()),
            },
            Self::Live | Self::Refused(_) => Start {
                run: StreamRun::mint(),
                after: None,
            },
        }
    }

    /// The instance reading `run` read a payload with this id.
    fn advance(&mut self, last_event_id: String, run: &StreamRun) {
        *self = Self::Resume(ResumePosition {
            last_event_id,
            run: run.clone(),
        });
    }

    /// An instance opened: the refused id, if any, is its gap to report.
    fn take_refused(&mut self) -> Option<String> {
        let Self::Refused(after) = self else {
            return None;
        };
        let after = std::mem::take(after);
        *self = Self::Live;
        Some(after)
    }

    fn position(&self) -> Option<ResumePosition> {
        match self {
            Self::Resume(position) => Some(position.clone()),
            Self::Live | Self::Refused(_) => None,
        }
    }
}

/// The critical sections never panic, so a poisoned lock still holds a
/// consistent cursor.
fn lock(cursor: &Mutex<Cursor>) -> MutexGuard<'_, Cursor> {
    cursor.lock().unwrap_or_else(PoisonError::into_inner)
}

/// One endpoint's event stream, opened over its transport: log records from
/// server-sent events.
pub struct EventStream<T> {
    endpoint: EndpointContext,
    origin: Origin,
    service: Arc<RedfishService<T>>,
    cursor: Arc<Mutex<Cursor>>,
}

impl<T> EventStream<T> {
    /// Provider identity, as `Origin.provider` carries it.
    pub const PROVIDER: &'static str = "redfish.event-service.sse";

    /// Request class, as dispatcher lanes and breakers key it.
    pub const REQUEST_CLASS: &'static str = "event-stream";

    /// The declared weight of one connect attempt: the service root, the
    /// event service, and the stream itself are three requests. A refused
    /// resume opens the stream a second time.
    pub const COST: u64 = 3;

    /// This provider's declaration, single-sourced from the same constants
    /// its `Origin` is built from.
    #[must_use]
    pub fn declaration() -> ProviderDeclaration {
        ProviderDeclaration::streamed(Self::PROVIDER, Self::REQUEST_CLASS, Self::COST)
    }

    /// The event stream of the endpoint `service` reaches. The service
    /// root names the event service, so no target is asked for. The first
    /// instance starts live; each later one resumes after the last id read.
    ///
    /// # Panics
    ///
    /// Never in practice: the origin is built from this type's own
    /// constants, which satisfy the origin's bounds.
    #[must_use]
    pub fn new(endpoint: EndpointContext, service: Arc<RedfishService<T>>) -> Self {
        let origin = Origin::builder()
            .provider(Self::PROVIDER)
            .request_class(Self::REQUEST_CLASS)
            .build()
            .expect("the provider's constants satisfy the origin's bounds");
        Self {
            endpoint,
            origin,
            service,
            cursor: Arc::new(Mutex::new(Cursor::default())),
        }
    }

    /// Starts the first instance at `position` instead of live: what
    /// [`Self::resume_position`] gave an embedder before it restarted.
    ///
    /// The position must be the last one read. A device that restarts its
    /// ids without marking them, as bmcweb does, accepts an older one once
    /// it has issued that id again, and the stream then ships new events
    /// under ids its run has already used.
    #[must_use]
    pub fn with_resume_position(self, position: ResumePosition) -> Self {
        *lock(&self.cursor) = Cursor::Resume(position);
        self
    }

    /// Where the next instance resumes, once the device has sent an id;
    /// `None` before the first id, on a device that sends none, and after
    /// the device refused the position.
    #[must_use]
    pub fn resume_position(&self) -> Option<ResumePosition> {
        lock(&self.cursor).position()
    }
}

impl<T> fmt::Debug for EventStream<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventStream")
            .field("endpoint_id", &self.endpoint.endpoint_id())
            .field("provider", &self.origin.provider())
            .field("request_class", &self.origin.request_class())
            .field("resume_position", &self.resume_position())
            .finish_non_exhaustive()
    }
}

impl<T: RedfishTransport> Acquire for EventStream<T> {
    type Output = BoxStream<'static, SubscriptionItem>;

    fn endpoint(&self) -> &EndpointContext {
        &self.endpoint
    }

    fn origin(&self) -> &Origin {
        &self.origin
    }

    async fn perform(&self) -> Result<Self::Output, AcquisitionFailure> {
        let Root { bmc, document } = self.service.root().await?;
        let service = document
            .event_service
            .as_ref()
            .ok_or_else(|| unsupported("the service root advertises no EventService"))?
            .get(&bmc)
            .await
            .map_err(|error| error.classify())?;
        let uri = service
            .server_sent_event_uri
            .as_ref()
            .ok_or_else(|| unsupported("the event service advertises no ServerSentEventUri"))?;
        // The layer repairs each payload under the SSE id it arrived with;
        // decoding is left to the items, so a payload that does not decode
        // still moves the cursor past it.
        let (start, events) = loop {
            let start = lock(&self.cursor).start();
            match bmc
                .stream_events::<Value>(uri, start.after.as_deref())
                .await
            {
                Ok(events) => break (start, events),
                Err(error) => {
                    let Some(after) = start.after else {
                        return Err(error.classify());
                    };
                    if !refuses_position(&error) {
                        return Err(told_against_resume(&after, &error.classify()));
                    }
                    // The next turn starts live, so it cannot come back here.
                    *lock(&self.cursor) = Cursor::Refused(after);
                }
            }
        };
        let scope = stream_scope(&service.id, &start.run)?;
        let refused = lock(&self.cursor).take_refused();
        let items = items(Live {
            events,
            bmc,
            scope,
            run: start.run,
            cursor: Arc::clone(&self.cursor),
            metric_reports_seen: false,
        });
        Ok(match refused {
            Some(after) => Box::pin(once(ready(Ok(refused_gap(&after)))).chain(items)),
            None => Box::pin(items),
        })
    }
}

/// The first item of an instance that opened live after a refused position.
fn refused_gap(after: &str) -> AcquisitionParts {
    let issue = ProjectionIssue::invalid(
        GAP_LOCATOR,
        format!(
            "the device refused to resume after event {after}; events since may be missing, \
             and the polled log fills the gap"
        ),
    );
    AcquisitionParts::new(Vec::new(), vec![issue])
}

fn unsupported(detail: &'static str) -> AcquisitionFailure {
    AcquisitionFailure::new(AcquisitionFailureClass::Unsupported)
        .with_retryable(false)
        .with_detail(detail)
}

/// A failed open other than a refused position, told against the position
/// it resumed from: the same class and retryability the answer classified
/// to, its detail naming the id. The position itself is left as it was.
fn told_against_resume(after: &str, failure: &AcquisitionFailure) -> AcquisitionFailure {
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
struct Live<T: RedfishTransport> {
    events: BoxTryStream<StreamEvent<Value>, RepairError<LinkError>>,
    bmc: CompatBmc<Link<T>>,
    scope: Subject,
    run: StreamRun,
    cursor: Arc<Mutex<Cursor>>,
    metric_reports_seen: bool,
}

impl<T: RedfishTransport> Live<T> {
    /// One payload as an item, or nothing for a metric report after the
    /// first.
    async fn item(&mut self, data: Value) -> Option<SubscriptionItem> {
        let issue = match decode::<EventStreamPayload>(data) {
            Ok(EventStreamPayload::Event(event)) => return Some(self.event(&event).await),
            Ok(EventStreamPayload::MetricReport(_)) => {
                if self.metric_reports_seen {
                    return None;
                }
                self.metric_reports_seen = true;
                ProjectionIssue::invalid(
                    METRIC_REPORT_LOCATOR,
                    "the stream carries metric reports, which this provider does not ship",
                )
            }
            // The path only: serde's message quotes device values.
            Err(error) => ProjectionIssue::invalid(
                PAYLOAD_LOCATOR,
                format!("a payload did not decode at `{}`", error.path()),
            ),
        };
        Some(Ok(AcquisitionParts::new(Vec::new(), vec![issue])))
    }

    /// One `Event`'s records as a logs batch under the run's scope. A record
    /// the device did not inline is an issue against its index, never a
    /// request. A record that reports dropped events is the gap, and starts
    /// the run its payload and those after it ship under.
    async fn event(&mut self, event: &Event) -> SubscriptionItem {
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
            let record = member.get(&self.bmc).await.map_err(|_| {
                AcquisitionFailure::new(AcquisitionFailureClass::Internal)
                    .with_retryable(false)
                    .with_detail(
                        "projection bug: an inline event record reported a transport failure",
                    )
            })?;
            if reports_dropped_events(&record) {
                issues.push(ProjectionIssue::invalid(
                    GAP_LOCATOR,
                    "the device dropped events before this stream caught up; the polled log fills the gap",
                ));
                self.run = StreamRun::mint();
                self.scope = stream_scope(self.scope.id(), &self.run)?;
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
        assemble_logs(records, issues, self.scope.clone()).map_err(|error| internal_bug(&error))
    }
}

/// The payloads as items: one per `Event`, one for the first
/// `MetricReport`, one per payload that does not decode, and one terminal
/// failure that ends the stream. Every payload's id, decoded or not, moves
/// the cursor before its item is yielded, in the run the payload left the
/// stream in.
fn items<T: RedfishTransport>(live: Live<T>) -> impl Stream<Item = SubscriptionItem> + Send {
    unfold(Some(live), |live| async move {
        let mut live = live?;
        loop {
            let StreamEvent {
                last_event_id,
                data,
            } = match live.events.next().await {
                Some(Ok(event)) => event,
                Some(Err(error)) => return Some((Err(error.classify()), None)),
                None => {
                    let closed = AcquisitionFailure::new(AcquisitionFailureClass::Protocol)
                        .with_retryable(true)
                        .with_detail("the device closed the event stream");
                    return Some((Err(closed), None));
                }
            };
            let item = live.item(data).await;
            if let Some(id) = last_event_id {
                lock(&live.cursor).advance(id, &live.run);
            }
            match item {
                Some(Ok(parts)) => return Some((Ok(parts), Some(live))),
                Some(Err(failure)) => return Some((Err(failure), None)),
                None => {}
            }
        }
    })
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
        cursor.advance("41".to_owned(), &first.run);
        assert_eq!(
            cursor.position(),
            Some(ResumePosition::new("41", first.run.clone()).expect("a non-empty id"))
        );
        let second = cursor.start();
        assert_eq!(second.run, first.run);
        assert_eq!(second.after.as_deref(), Some("41"));
    }

    #[test]
    fn a_failed_open_while_resuming_names_the_id_and_keeps_the_answer() {
        // The device's answer stands: class and retryability as classified,
        // the detail naming the position.
        let failed = told_against_resume(
            "41",
            &AcquisitionFailure::new(AcquisitionFailureClass::Device)
                .with_retryable(true)
                .with_detail("HTTP 503"),
        );
        assert_eq!(failed.class(), AcquisitionFailureClass::Device);
        assert_eq!(failed.retryable(), Some(true));
        assert_eq!(
            failed.detail(),
            Some("open while resuming after event 41 failed: HTTP 503")
        );

        // A source that left retryability to policy still does.
        let unrefined = told_against_resume(
            "41",
            &AcquisitionFailure::new(AcquisitionFailureClass::Connectivity),
        );
        assert_eq!(unrefined.class(), AcquisitionFailureClass::Connectivity);
        assert_eq!(unrefined.retryable(), None);
        assert_eq!(
            unrefined.detail(),
            Some("open while resuming after event 41 failed: no detail")
        );
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
        let stream = EventStream::new(
            EndpointContext::builder()
                .endpoint_id("bmc-lab-07")
                .build()
                .expect("a valid endpoint"),
            Arc::new(RedfishService::new(())),
        )
        .with_resume_position(position.clone());
        assert_eq!(stream.resume_position(), Some(position.clone()));
        let start = lock(&stream.cursor).start();
        assert_eq!(start.run, *position.run());
        assert_eq!(start.after.as_deref(), Some("41"));
    }
}
