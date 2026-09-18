// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The event stream provider: a device's server-sent events as log records.
//!
//! A connect attempt reads the service root and its `EventService` and opens
//! the SSE stream the service advertises; each `Event` payload becomes one
//! item, its records projected by the `EventRecord` manifest into one logs
//! batch. The batch is scoped to the event service and to one run of the
//! device's event ids: an `EventId` restarts with the device and is unique
//! only within the sequence one subscription reads, so the scope carries
//! an id minted whenever a stream starts without a resume position, to be
//! kept across a resumed connection once the wrapper's resume is wired in.
//! Today every connect starts without one and mints. The consumer's dedup
//! key — endpoint, scope, `entry_id` — thus never collapses distinct
//! events. The polled log walk is a separate source with its own scope; on
//! devices whose events do not name their log entry, which is most, a
//! consumer that enables both sees each event under both.
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

use futures_util::stream::unfold;
use futures_util::stream::BoxStream;
use futures_util::Stream;
use futures_util::StreamExt as _;
use nv_redfish::core::BoxTryStream;
use nv_redfish::core::NavProperty;
use nv_redfish::event_service::EventStreamPayload;
use nv_redfish::schema::event::Event;
use nv_redfish::schema::event::EventRecord;
use nv_redfish::Bmc;
use nv_redfish::ServiceRoot;
use nv_telemetry_model::EndpointContext;
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

/// One endpoint's event stream, opened over its `Bmc`: log records from
/// server-sent events.
pub struct EventStream<B> {
    endpoint: EndpointContext,
    origin: Origin,
    bmc: Arc<B>,
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
    /// names the event service, so no target is asked for.
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
        }
    }
}

impl<B> fmt::Debug for EventStream<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventStream")
            .field("endpoint_id", &self.endpoint.endpoint_id())
            .field("provider", &self.origin.provider())
            .field("request_class", &self.origin.request_class())
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
        let scope = stream_scope(&service.raw().base.id, &uuid::Uuid::new_v4().to_string())?;
        let events = service
            .events()
            .await
            .map_err(|error| classify_wrapper(&error))?;
        Ok(Box::pin(items(Live {
            events,
            bmc: Arc::clone(&self.bmc),
            scope,
            metric_reports_seen: false,
        })))
    }
}

/// One open stream and what its items are built with.
struct Live<B: Bmc> {
    events: BoxTryStream<EventStreamPayload, nv_redfish::Error<B>>,
    bmc: Arc<B>,
    scope: Subject,
    metric_reports_seen: bool,
}

/// The payloads as items: one per `Event`, one for the first
/// `MetricReport`, and one terminal failure that ends the stream.
fn items<B>(live: Live<B>) -> impl Stream<Item = SubscriptionItem> + Send
where
    B: Bmc + 'static,
    B::Error: ClassifyError + 'static,
{
    unfold(Some(live), |live| async move {
        let mut live = live?;
        loop {
            match live.events.next().await {
                Some(Ok(EventStreamPayload::Event(event))) => {
                    return Some(
                        match project_event(&event, live.bmc.as_ref(), &live.scope).await {
                            Ok(parts) => (Ok(parts), Some(live)),
                            Err(failure) => (Err(failure), None),
                        },
                    );
                }
                Some(Ok(EventStreamPayload::MetricReport(_))) => {
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
                Some(Err(error)) => return Some((Err(classify_wrapper(&error)), None)),
                None => {
                    let closed = AcquisitionFailure::new(AcquisitionFailureClass::Protocol)
                        .with_retryable(true)
                        .with_detail("the device closed the event stream");
                    return Some((Err(closed), None));
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

/// The scope every record of one stream ships under: the event service, and
/// the run of event ids this stream reads.
fn stream_scope(service_id: &str, run: &str) -> Result<Subject, AcquisitionFailure> {
    Subject::builder()
        .kind("event-service")
        .id(service_id)
        .scope(vec!["stream".to_owned(), run.to_owned()])
        .build()
        .map_err(|_| {
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
