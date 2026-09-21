// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The event stream provider over the mock transport: what one `Event`
//! payload becomes, what the device's own gap report becomes, how metric
//! reports and the stream's end are told, and where the stream's identity
//! comes from.

use std::sync::Arc;

use futures_util::StreamExt as _;
use nv_redfish_bmc_mock::Bmc;
use nv_redfish_bmc_mock::Expect;
use nv_telemetry_model::EndpointContext;
use nv_telemetry_model::Payload;
use nv_telemetry_model::Severity;
use nv_telemetry_model::Timestamp;
use nv_telemetry_model::Value;
use nv_telemetry_redfish::EventStream;
use nv_telemetry_redfish::GAP_LOCATOR;
use nv_telemetry_redfish::METRIC_REPORT_LOCATOR;
use nv_telemetry_source::Acquire;
use nv_telemetry_source::AcquisitionFailure;
use nv_telemetry_source::AcquisitionFailureClass;
use nv_telemetry_source::AcquisitionParts;
use nv_telemetry_source::ProjectionIssue;
use nv_telemetry_source::SubscriptionItem;
use serde_json::json;
use serde_json::Value as Json;

const ROOT: &str = "/redfish/v1";
const EVENT_SERVICE: &str = "/redfish/v1/EventService";
const SSE: &str = "/redfish/v1/EventService/SSE";

fn endpoint() -> EndpointContext {
    EndpointContext::builder()
        .endpoint_id("bmc-lab-07")
        .build()
        .expect("a valid endpoint")
}

/// An `Event` payload with one record, as the mock's lifecycle events are
/// shaped: a `MessageSeverity`, a member id, and an origin.
fn powered_on(event_id: u32) -> Json {
    json!({
        "@odata.id": format!("{SSE}#/Event{event_id}"),
        "@odata.type": "#Event.v1_9_2.Event",
        "Id": event_id.to_string(),
        "Name": "Event Array",
        "Events": [{
            "@odata.id": format!("{SSE}#/Events/0"),
            "MemberId": "0",
            "EventId": event_id.to_string(),
            "EventType": "Alert",
            "EventTimestamp": "2026-03-01T10:05:00Z",
            "MessageId": "ResourceEvent.1.3.ResourcePoweredOn",
            "Message": "The resource /redfish/v1/Systems/1 has powered on.",
            "MessageSeverity": "OK",
            "OriginOfCondition": { "@odata.id": "/redfish/v1/Systems/1/LogServices/SEL/Entries/12" }
        }]
    })
}

/// The registry message a device sends when its event buffer overflowed.
fn buffer_exceeded(event_id: u32) -> Json {
    json!({
        "@odata.id": format!("{SSE}#/Event{event_id}"),
        "@odata.type": "#Event.v1_9_2.Event",
        "Id": event_id.to_string(),
        "Name": "Event Array",
        "Events": [{
            "@odata.id": format!("{SSE}#/Events/0"),
            "MemberId": "0",
            "EventId": event_id.to_string(),
            "EventType": "Alert",
            "EventTimestamp": "2026-03-01T10:05:01Z",
            "MessageId": "Base.1.19.EventBufferExceeded",
            "Message": "Undelivered events may have been lost.",
            "MessageSeverity": "Warning"
        }]
    })
}

fn metric_report(id: &str) -> Json {
    json!({
        "@odata.id": format!("/redfish/v1/TelemetryService/MetricReports/{id}"),
        "@odata.type": "#MetricReport.v1_3_0.MetricReport",
        "Id": id,
        "Name": "A metric report",
        "MetricReportDefinition": {
            "@odata.id": format!("/redfish/v1/TelemetryService/MetricReportDefinitions/{id}")
        },
        "MetricValues": []
    })
}

/// A stream over `payloads`, scripted after the root and the event service.
async fn open(payloads: &[Json]) -> Vec<SubscriptionItem> {
    let bmc = Arc::new(Bmc::<nv_redfish_bmc_mock::Error>::default());
    bmc.expect(Expect::get(
        ROOT,
        include_str!("fixtures/events/service-root.json"),
    ));
    bmc.expect(Expect::get(
        EVENT_SERVICE,
        include_str!("fixtures/events/event-service.json"),
    ));
    bmc.expect(Expect::stream(SSE, Json::Array(payloads.to_vec())));
    let stream = EventStream::new(endpoint(), bmc);
    let items = stream.perform().await.expect("the stream opens");
    items.collect().await
}

fn parts(item: &SubscriptionItem) -> &AcquisitionParts {
    item.as_ref().expect("a payload, not the stream's end")
}

fn failure(item: &SubscriptionItem) -> &AcquisitionFailure {
    item.as_ref().expect_err("the stream's end, not a payload")
}

#[tokio::test]
async fn an_event_payload_is_one_logs_batch_under_the_streams_scope() {
    let items = open(&[powered_on(7)]).await;
    assert_eq!(items.len(), 2, "one payload, then the stream's end");

    let parts = parts(&items[0]);
    assert!(parts.issues().is_empty());
    assert_eq!(parts.payloads().len(), 1);
    let (coverage, payload) = &parts.payloads()[0];

    // The scope is the event service and this connection, so the device's
    // per-boot event ids cannot collide across streams under the dedup key.
    let scope = coverage.scope().expect("a scoped batch");
    assert_eq!(scope.kind(), "event-service");
    assert_eq!(scope.id(), "EventService");
    assert_eq!(scope.scope().len(), 2);
    assert_eq!(scope.scope()[0], "stream");
    assert_eq!(scope.scope()[1].len(), 36, "a UUID names the connection");

    let Payload::Logs(logs) = payload else {
        panic!("events project into logs");
    };
    assert_eq!(logs.records().len(), 1);
    let record = &logs.records()[0];
    assert_eq!(record.entry_id(), Some("7"));
    assert_eq!(
        record.occurred_at(),
        Some(&Timestamp::new(1_772_359_500, 0).expect("a valid instant"))
    );
    assert_eq!(record.severity(), Some(Severity::Info));
    assert_eq!(
        record.message(),
        "The resource /redfish/v1/Systems/1 has powered on."
    );
    let attribute = |key: &str| record.attributes().and_then(|map| map.get(key)).cloned();
    assert_eq!(
        attribute("message-id"),
        Some(Value::string("ResourceEvent.1.3.ResourcePoweredOn").expect("text"))
    );
    assert_eq!(
        attribute("event-type"),
        Some(Value::string("Alert").expect("text"))
    );
    assert_eq!(
        attribute("member-id"),
        Some(Value::string("0").expect("text"))
    );

    // The device closed the stream: one terminal, retryable failure, so the
    // embedder reconnects.
    let end = failure(&items[1]);
    assert_eq!(end.class(), AcquisitionFailureClass::Protocol);
    assert_eq!(end.retryable(), Some(true));
    assert_eq!(end.detail(), Some("the device closed the event stream"));
}

/// `powered_on`, with the record's `MessageSeverity` replaced by the
/// deprecated free-text `Severity`, as a device declaring an Event schema
/// before v1.5 sends it.
fn powered_on_deprecated_severity(event_id: u32, severity: &str) -> Json {
    let mut payload = powered_on(event_id);
    let record = payload["Events"][0]
        .as_object_mut()
        .expect("a record object");
    record.remove("MessageSeverity");
    record.insert("Severity".to_owned(), json!(severity));
    payload
}

#[tokio::test]
async fn severity_falls_back_to_the_deprecated_property_when_the_current_is_absent() {
    let mut both = powered_on(8);
    both["Events"][0]["MessageSeverity"] = json!("Critical");
    both["Events"][0]["Severity"] = json!("OK");
    let items = open(&[
        powered_on_deprecated_severity(7, "Warning"),
        both,
        powered_on_deprecated_severity(9, "Bogus"),
    ])
    .await;
    assert_eq!(items.len(), 4);
    let severity = |item: &SubscriptionItem| {
        let Payload::Logs(logs) = &parts(item).payloads()[0].1 else {
            panic!("events project into logs");
        };
        logs.records()[0].severity()
    };

    // The deprecated property alone: the same value set, the same severity,
    // and no issue.
    assert_eq!(severity(&items[0]), Some(Severity::Warning));
    assert!(parts(&items[0]).issues().is_empty());
    // Both present: the current property decides, the deprecated one is
    // not read.
    assert_eq!(severity(&items[1]), Some(Severity::Critical));
    // A deprecated value outside the set is that property's issue, at the
    // record's place in the payload; the record still ships, without a
    // severity.
    assert_eq!(severity(&items[2]), None);
    assert_eq!(
        parts(&items[2]).issues(),
        [
            ProjectionIssue::invalid("EventRecord.Severity", "outside the known value set")
                .at_index("Events", 0)
        ]
    );
}

#[tokio::test]
async fn a_buffer_exceeded_record_is_a_gap_not_a_record() {
    let items = open(&[buffer_exceeded(3)]).await;
    let parts = parts(&items[0]);
    assert!(
        parts.payloads().is_empty(),
        "a gap report is not a log record"
    );
    assert_eq!(parts.issues().len(), 1);
    assert_eq!(
        parts.issues()[0],
        ProjectionIssue::invalid(
            GAP_LOCATOR,
            "the device dropped events before this stream caught up; the polled log fills the gap",
        )
    );
}

#[tokio::test]
async fn metric_reports_are_said_once_and_otherwise_yield_nothing() {
    let items = open(&[
        metric_report("A"),
        metric_report("B"),
        powered_on(8),
        metric_report("C"),
    ])
    .await;
    // The first report is an issue; the others yield no item, so the event
    // and the stream's end are the only other items.
    assert_eq!(items.len(), 3);
    let first = parts(&items[0]);
    assert!(first.payloads().is_empty());
    assert_eq!(first.issues().len(), 1);
    assert_eq!(first.issues()[0].path(), METRIC_REPORT_LOCATOR);
    assert_eq!(parts(&items[1]).payloads().len(), 1);
    failure(&items[2]);
}

#[tokio::test]
async fn a_payload_that_does_not_decode_ends_the_stream() {
    // A record without the required `MemberId`, as bmcweb's event-log
    // events are shaped when no vendor quirk patches them: the wrapper
    // cannot decode it, and that is this stream's terminal failure.
    let undecodable = json!({
        "@odata.type": "#Event.v1_9_2.Event",
        "Id": "9",
        "Name": "Event Array",
        "Events": [{
            "EventId": "9",
            "EventType": "Alert",
            "EventTimestamp": "2026-03-01T10:05:02Z",
            "MessageId": "ResourceEvent.1.3.ResourcePoweredOn",
            "Message": "powered on",
            "Severity": "OK"
        }]
    });
    let items = open(&[powered_on(7), undecodable, powered_on(10)]).await;
    assert_eq!(
        items.len(),
        2,
        "the failure is the last item; nothing follows"
    );
    parts(&items[0]);
    let end = failure(&items[1]);
    assert_eq!(end.class(), AcquisitionFailureClass::Protocol);
    assert_eq!(end.retryable(), Some(false));
    assert!(end
        .detail()
        .is_some_and(|detail| detail.starts_with("document did not decode")));
}

#[tokio::test]
async fn a_record_the_device_did_not_inline_is_an_issue_not_a_request() {
    // A reference in place of a record. The mock expects no GET for it, so
    // fetching would have failed the stream; the provider records the
    // reference against its index and reads on.
    let referenced = json!({
        "@odata.id": format!("{SSE}#/Event11"),
        "@odata.type": "#Event.v1_9_2.Event",
        "Id": "11",
        "Name": "Event Array",
        "Events": [{ "@odata.id": "/redfish/v1/EventService/Events/11" }]
    });
    let items = open(&[referenced, powered_on(12)]).await;
    assert_eq!(items.len(), 3);
    let first = parts(&items[0]);
    assert!(first.payloads().is_empty());
    assert_eq!(first.issues().len(), 1);
    assert_eq!(first.issues()[0].path(), "Events[0]");
    assert_eq!(parts(&items[1]).payloads().len(), 1);
    failure(&items[2]);
}

/// One connect attempt's three requests, the stream scripted with ids and
/// expected to resume `after` the given one.
fn expect_connect(
    bmc: &Bmc<nv_redfish_bmc_mock::Error>,
    after: Option<&str>,
    events: Vec<(Option<&str>, Json)>,
) {
    bmc.expect(Expect::get(
        ROOT,
        include_str!("fixtures/events/service-root.json"),
    ));
    bmc.expect(Expect::get(
        EVENT_SERVICE,
        include_str!("fixtures/events/event-service.json"),
    ));
    bmc.expect(Expect::stream_events(SSE, after, events));
}

/// The run a batch's scope names.
fn run_of(item: &SubscriptionItem) -> String {
    let (coverage, _) = &parts(item).payloads()[0];
    coverage.scope().expect("a scoped batch").scope()[1].clone()
}

#[tokio::test]
async fn a_reconnect_resumes_after_the_last_id_in_the_same_run() {
    let bmc = Arc::new(Bmc::<nv_redfish_bmc_mock::Error>::default());
    expect_connect(
        &bmc,
        None,
        vec![(Some("7"), powered_on(7)), (Some("8"), powered_on(8))],
    );
    expect_connect(&bmc, Some("8"), vec![(Some("9"), powered_on(9))]);
    let stream = EventStream::new(endpoint(), Arc::clone(&bmc));
    assert_eq!(stream.resume_position(), None, "nothing read yet");

    // The first instance starts live; the id in effect after its last
    // payload is where the next one resumes, in the run the scope names.
    let first: Vec<SubscriptionItem> = stream
        .perform()
        .await
        .expect("the stream opens")
        .collect()
        .await;
    assert_eq!(first.len(), 3, "two payloads, then the stream's end");
    let run = run_of(&first[0]);
    let position = stream.resume_position().expect("the device sent ids");
    assert_eq!(position.last_event_id(), "8");
    assert_eq!(position.run().as_str(), run);

    // The next instance asks for what follows and ships under the same
    // scope, so the consumer's dedup key continues rather than restarts.
    let second: Vec<SubscriptionItem> = stream
        .perform()
        .await
        .expect("the stream resumes")
        .collect()
        .await;
    assert_eq!(second.len(), 2);
    assert_eq!(run_of(&second[0]), run);
    let Payload::Logs(logs) = &parts(&second[0]).payloads()[0].1 else {
        panic!("events project into logs");
    };
    assert_eq!(logs.records()[0].entry_id(), Some("9"));
    assert_eq!(
        stream
            .resume_position()
            .map(|position| position.last_event_id().to_owned()),
        Some("9".to_owned())
    );
}

#[tokio::test]
async fn an_instance_without_ids_leaves_nothing_to_resume_from() {
    // A device that sends no ids: each instance starts live, and each is
    // its own run, since nothing says how their event ids relate.
    let bmc = Arc::new(Bmc::<nv_redfish_bmc_mock::Error>::default());
    for _ in 0..2 {
        bmc.expect(Expect::get(
            ROOT,
            include_str!("fixtures/events/service-root.json"),
        ));
        bmc.expect(Expect::get(
            EVENT_SERVICE,
            include_str!("fixtures/events/event-service.json"),
        ));
        bmc.expect(Expect::stream(SSE, Json::Array(vec![powered_on(7)])));
    }
    let stream = EventStream::new(endpoint(), Arc::clone(&bmc));
    let first: Vec<SubscriptionItem> = stream
        .perform()
        .await
        .expect("the stream opens")
        .collect()
        .await;
    assert_eq!(stream.resume_position(), None, "no id was ever in effect");
    let second: Vec<SubscriptionItem> = stream
        .perform()
        .await
        .expect("the stream opens again, live")
        .collect()
        .await;
    assert_ne!(run_of(&first[0]), run_of(&second[0]));
}

#[tokio::test]
async fn a_root_without_an_event_service_cannot_stream() {
    let bmc = Arc::new(Bmc::<nv_redfish_bmc_mock::Error>::default());
    bmc.expect(Expect::get(
        ROOT,
        include_str!("../tests/fixtures/logs/service-root.json"),
    ));
    let stream = EventStream::new(endpoint(), bmc);
    let error = stream
        .perform()
        .await
        .err()
        .expect("no event service, no stream");
    assert_eq!(error.class(), AcquisitionFailureClass::Unsupported);
    assert_eq!(error.retryable(), Some(false));
}
