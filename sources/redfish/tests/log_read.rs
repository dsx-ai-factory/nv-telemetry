// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The log corpus, replayed: a service, its entries collection, and each
//! entry are separate device answers, and each test asserts *both* the
//! batches and the exact issue list, the same discipline as the sensor and
//! chassis corpora. The pins this corpus owns: `occurred_at` is the device's
//! instant with its offset folded in — never the collection time; a
//! severity outside the mapping is reported, never guessed at, so
//! `SEVERITY_UNSPECIFIED` cannot reach the wire; and the walk has element
//! semantics — each entry's issues name their member, and only a failure
//! that indicts the endpoint or the collector ends the walk.

use std::collections::BTreeMap;
use std::sync::Arc;

use nv_redfish_bmc_mock::Bmc;
use nv_redfish_bmc_mock::Expect;
use nv_telemetry_model::Completeness;
use nv_telemetry_model::Coverage;
use nv_telemetry_model::EndpointContext;
use nv_telemetry_model::LogRecord;
use nv_telemetry_model::Logs;
use nv_telemetry_model::ObservationBatch;
use nv_telemetry_model::ObservationWindow;
use nv_telemetry_model::Origin;
use nv_telemetry_model::Payload;
use nv_telemetry_model::Severity;
use nv_telemetry_model::Subject;
use nv_telemetry_model::Timestamp;
use nv_telemetry_model::Value;
use nv_telemetry_redfish::LogRead;
use nv_telemetry_redfish::WalkBudget;
use nv_telemetry_redfish::TRUNCATED_WALK_LOCATOR;
use nv_telemetry_source::acquire as run_acquisition;
use nv_telemetry_source::Acquired;
use nv_telemetry_source::AcquisitionFailure;
use nv_telemetry_source::AcquisitionFailureClass;
use nv_telemetry_source::ProjectionIssue;

const SERVICE: &str = "/redfish/v1/Systems/1/LogServices/SEL";
const ENTRIES: &str = "/redfish/v1/Systems/1/LogServices/SEL/Entries";
const ENTRY_1: &str = "/redfish/v1/Systems/1/LogServices/SEL/Entries/1";
const ENTRY_2: &str = "/redfish/v1/Systems/1/LogServices/SEL/Entries/2";

/// 2026-03-01T10:00:00Z: what the nominal entry's `Created`, stamped
/// `12:00:00+02:00`, denotes.
const NOMINAL_CREATED: i64 = 1_772_359_200;

fn at() -> Timestamp {
    Timestamp::new(1_785_621_243, 0).expect("a valid instant")
}

fn instant(seconds: i64) -> Timestamp {
    Timestamp::new(seconds, 0).expect("a valid instant")
}

fn endpoint() -> EndpointContext {
    EndpointContext::builder()
        .endpoint_id("bmc-lab-07")
        .build()
        .expect("a valid endpoint")
}

/// Primes the mock with the device's answers in the order the provider asks
/// — service, collection, then each member — and runs the acquisition.
async fn run(answers: &[(&str, &str)]) -> Result<Acquired, AcquisitionFailure> {
    let bmc = Arc::new(Bmc::<nv_redfish_bmc_mock::Error>::default());
    for (uri, body) in answers {
        bmc.expect(Expect::get(uri, body));
    }
    let read = LogRead::new(endpoint(), SERVICE.to_string().into(), bmc);
    run_acquisition(&read, at()).await
}

async fn acquire(answers: &[(&str, &str)]) -> Acquired {
    run(answers).await.expect("the device answered")
}

/// One entry behind the nominal service and a one-member collection.
async fn acquire_entry(entry: &str) -> Acquired {
    acquire(&[
        (SERVICE, include_str!("fixtures/logs/service.json")),
        (ENTRIES, include_str!("fixtures/logs/entries-one.json")),
        (ENTRY_1, entry),
    ])
    .await
}

fn text(value: &str) -> Value {
    Value::string(value).expect("a short value")
}

/// The service the batch covers — the namespace of every `entry_id` — from
/// the requested location: system `1`'s log service `SEL`.
fn service_scope() -> Subject {
    Subject::builder()
        .kind("log-service")
        .scope(vec!["Systems".to_owned(), "1".to_owned()])
        .id("SEL")
        .build()
        .expect("a valid subject")
}

fn batch(records: Vec<LogRecord>) -> ObservationBatch {
    ObservationBatch::builder()
        .endpoint(endpoint())
        .origin(
            Origin::builder()
                .provider("redfish.log-service.odata")
                .request_class("log-read")
                .build()
                .expect("a valid origin"),
        )
        .window(
            ObservationWindow::builder()
                .start(at())
                .build()
                .expect("a valid window"),
        )
        .coverage(
            Coverage::builder()
                .completeness(Completeness::Partial)
                .scope(service_scope())
                .build()
                .expect("valid coverage"),
        )
        .payload(Payload::Logs(
            Logs::builder()
                .records(records)
                .build()
                .expect("a valid logs payload"),
        ))
        .build()
        .expect("a valid batch")
}

/// The nominal entry: every field present, the offset folded into the
/// instants, the SEL specifics as attributes.
fn nominal_record() -> LogRecord {
    let attributes: BTreeMap<String, Value> = [
        ("entry-code", text("Upper Critical - going high")),
        ("entry-type", text("SEL")),
        (
            "event-timestamp",
            Value::timestamp(instant(NOMINAL_CREATED - 2)),
        ),
        ("message-id", text("Base.1.19.ThresholdCrossed")),
        ("oem-record-format", text("Legacy")),
        ("resolved", Value::bool(false)),
        ("sensor-number", Value::int(7)),
        ("sensor-type", text("Temperature")),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_owned(), value))
    .collect();
    LogRecord::builder()
        .occurred_at(instant(NOMINAL_CREATED))
        .severity(Severity::Critical)
        .message("CPU1 Temp upper critical threshold crossed")
        .entry_id("1")
        .attributes(attributes)
        .build()
        .expect("a valid record")
}

/// `EntryType` is Redfish-required, so even the barest entry carries it.
fn attributes_of(entry_type: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("entry-type".to_owned(), text(entry_type))])
}

#[tokio::test]
async fn a_service_projects_every_entry_into_one_batch() {
    let acquired = acquire(&[
        (SERVICE, include_str!("fixtures/logs/service.json")),
        (ENTRIES, include_str!("fixtures/logs/entries-two.json")),
        (ENTRY_1, include_str!("fixtures/logs/entry-nominal.json")),
        (ENTRY_2, include_str!("fixtures/logs/entry-minimal.json")),
    ])
    .await;

    let minimal = LogRecord::builder()
        .occurred_at(instant(NOMINAL_CREATED + 300))
        .message("System boot complete")
        .entry_id("2")
        .attributes(attributes_of("Event"))
        .build()
        .expect("a valid record");
    let expected = [batch(vec![nominal_record(), minimal])];
    assert_eq!(acquired.batches(), expected);
    assert_eq!(acquired.issues(), &[]);

    // One byte-level pin: equal validated values must be equal canonical
    // bytes, and this is the logs arm's anchor case.
    assert_eq!(
        acquired.batches()[0].encode_to_vec(),
        expected[0].encode_to_vec()
    );
}

#[tokio::test]
async fn the_batch_names_the_service_its_entry_ids_belong_to() {
    // `entry_id` is only unique within one service: the coverage scope is
    // the namespace a consumer deduplicates under, derived from the
    // requested location — not from the payload's own claim.
    let acquired = acquire_entry(include_str!("fixtures/logs/entry-nominal.json")).await;

    let coverage = acquired.batches()[0].coverage();
    assert_eq!(coverage.completeness(), Completeness::Partial);
    assert_eq!(coverage.scope(), Some(&service_scope()));
}

#[tokio::test]
async fn a_walk_past_its_member_budget_stops_and_says_so() {
    // One more member than the budget admits; every member primed, so the
    // budget — not the device — is what stops the walk.
    let over = 1025;
    let members: Vec<String> = (0..over)
        .map(|index| format!(r#"{{ "@odata.id": "{ENTRIES}/{index}" }}"#))
        .collect();
    let collection = format!(
        r##"{{ "@odata.id": "{ENTRIES}", "@odata.type": "#LogEntryCollection.LogEntryCollection",
             "Name": "Entries", "Members@odata.count": {over}, "Members": [{}] }}"##,
        members.join(",")
    );
    let entries: Vec<(String, String)> = (0..over)
        .map(|index| {
            (
                format!("{ENTRIES}/{index}"),
                format!(
                    r##"{{ "@odata.id": "{ENTRIES}/{index}", "@odata.type": "#LogEntry.v1_21_0.LogEntry",
                         "Id": "{index}", "Name": "Entry", "EntryType": "Event",
                         "Created": "2026-03-01T10:00:00Z", "Message": "m" }}"##
                ),
            )
        })
        .collect();
    let mut answers: Vec<(&str, &str)> = vec![
        (SERVICE, include_str!("fixtures/logs/service.json")),
        (ENTRIES, collection.as_str()),
    ];
    answers.extend(
        entries
            .iter()
            .map(|(uri, body)| (uri.as_str(), body.as_str())),
    );
    let acquired = acquire(&answers).await;

    let Payload::Logs(logs) = acquired.batches()[0].payload() else {
        panic!("a logs payload");
    };
    assert_eq!(logs.records().len(), 1024, "the budget's worth of members");
    assert_eq!(
        acquired.batches()[0].coverage().completeness(),
        Completeness::Partial
    );
    assert_eq!(
        acquired.issues(),
        &[ProjectionIssue::invalid(
            TRUNCATED_WALK_LOCATOR,
            "walk stopped at member 1024 of 1025: member budget spent"
        )]
    );
    // The constant the corpus depends on, pinned where a change would be seen.
    assert_eq!(
        WalkBudget::DEFAULT,
        WalkBudget::new(1024, std::time::Duration::from_secs(30))
    );
}

#[tokio::test]
async fn the_instant_is_the_devices_not_the_collectors() {
    let acquired = acquire_entry(include_str!("fixtures/logs/entry-nominal.json")).await;

    let Payload::Logs(logs) = acquired.batches()[0].payload() else {
        panic!("a logs payload");
    };
    let record = &logs.records()[0];
    // Stamped `2026-03-01T12:00:00+02:00`; the record names the instant.
    assert_eq!(record.occurred_at(), Some(&instant(NOMINAL_CREATED)));
    assert_ne!(record.occurred_at(), Some(&at()));
}

#[tokio::test]
async fn an_empty_message_is_the_devices_text_not_a_fault() {
    let acquired = acquire_entry(include_str!("fixtures/logs/entry-empty-message.json")).await;

    let mut attributes = attributes_of("Event");
    attributes.insert("message-id".to_owned(), text("Base.1.19.ResourceCreated"));
    let record = LogRecord::builder()
        .occurred_at(instant(NOMINAL_CREATED))
        .message("")
        .entry_id("1")
        .attributes(attributes)
        .build()
        .expect("a valid record");
    assert_eq!(acquired.batches(), [batch(vec![record])]);
    assert_eq!(acquired.issues(), &[]);
}

#[tokio::test]
async fn an_entry_without_a_message_is_reported_and_not_a_record() {
    let acquired = acquire_entry(include_str!("fixtures/logs/entry-without-message.json")).await;

    assert_eq!(acquired.batches(), &[]);
    assert_eq!(
        acquired.issues(),
        &[ProjectionIssue::missing("LogEntry.Message").at_index("Members", 0)]
    );
}

#[tokio::test]
async fn two_entries_with_one_fault_are_two_facts() {
    // Without the element prefix the two issues would share a path and the
    // envelope would reject them, discarding every record with them.
    let acquired = acquire(&[
        (SERVICE, include_str!("fixtures/logs/service.json")),
        (ENTRIES, include_str!("fixtures/logs/entries-two.json")),
        (
            ENTRY_1,
            include_str!("fixtures/logs/entry-without-message.json"),
        ),
        (
            ENTRY_2,
            include_str!("fixtures/logs/entry-without-message-2.json"),
        ),
    ])
    .await;

    assert_eq!(acquired.batches(), &[]);
    assert_eq!(
        acquired.issues(),
        &[
            ProjectionIssue::missing("LogEntry.Message").at_index("Members", 0),
            ProjectionIssue::missing("LogEntry.Message").at_index("Members", 1),
        ]
    );
}

#[tokio::test]
async fn a_member_failure_that_is_not_the_devices_answer_ends_the_walk() {
    // The second member is never primed: the mock fails its GET as a
    // harness fault — Internal, neither an answer the device gave about the
    // member nor an endpoint fact — so the walk ends as the unit's failure
    // and the record already projected does not ship. (A device's own
    // answer about a member — a 404 for a rotated-out entry — is recorded
    // against `Members[i]` instead; the mock cannot replay one, so that
    // disposition is pinned in the provider's unit tests.)
    let failure = run(&[
        (SERVICE, include_str!("fixtures/logs/service.json")),
        (ENTRIES, include_str!("fixtures/logs/entries-two.json")),
        (ENTRY_1, include_str!("fixtures/logs/entry-nominal.json")),
    ])
    .await
    .expect_err("the walk ends");

    assert_eq!(failure.class(), AcquisitionFailureClass::Internal);
    assert_eq!(failure.retryable(), Some(false));
}

#[tokio::test]
async fn a_severity_outside_the_mapping_is_reported_never_guessed() {
    let acquired = acquire_entry(include_str!("fixtures/logs/entry-unknown-severity.json")).await;

    let record = LogRecord::builder()
        .occurred_at(instant(NOMINAL_CREATED))
        .message("Uncorrectable memory error")
        .entry_id("1")
        .attributes(attributes_of("Event"))
        .build()
        .expect("a valid record");
    assert_eq!(acquired.batches(), [batch(vec![record])]);
    assert_eq!(
        acquired.issues(),
        &[
            ProjectionIssue::invalid("LogEntry.Severity", "outside the known value set")
                .at_index("Members", 0)
        ]
    );
}

#[tokio::test]
async fn an_unknown_entry_type_is_reported_while_the_record_survives() {
    let acquired = acquire_entry(include_str!("fixtures/logs/entry-unknown-entry-type.json")).await;

    // The one attribute the entry offered is unusable, so no attributes
    // map at all: an assembly emits only what it can vouch for.
    let record = LogRecord::builder()
        .occurred_at(instant(NOMINAL_CREATED))
        .message("Vendor trace record")
        .entry_id("1")
        .build()
        .expect("a valid record");
    assert_eq!(acquired.batches(), [batch(vec![record])]);
    assert_eq!(
        acquired.issues(),
        &[
            ProjectionIssue::invalid("LogEntry.EntryType", "outside the known value set")
                .at_index("Members", 0)
        ]
    );
}

#[tokio::test]
async fn an_empty_log_is_no_batch_and_no_issue() {
    let acquired = acquire(&[
        (SERVICE, include_str!("fixtures/logs/service.json")),
        (ENTRIES, include_str!("fixtures/logs/entries-empty.json")),
    ])
    .await;

    assert_eq!(acquired.batches(), &[]);
    assert_eq!(acquired.issues(), &[]);
}

#[tokio::test]
async fn a_service_without_entries_is_unsupported_not_a_projection_fault() {
    let failure = run(&[(
        SERVICE,
        include_str!("fixtures/logs/service-without-entries.json"),
    )])
    .await
    .expect_err("nothing to read");

    assert_eq!(failure.class(), AcquisitionFailureClass::Unsupported);
    assert_eq!(failure.retryable(), Some(false));
}

#[tokio::test]
async fn a_transport_failure_is_classified_never_a_batch() {
    // No expectation queued: the mock fails the GET, standing in for any
    // transport failure. A mock error is a harness fault, hence Internal.
    let failure = run(&[]).await.expect_err("the transport failed");

    assert_eq!(failure.class(), AcquisitionFailureClass::Internal);
    assert_eq!(failure.retryable(), Some(false));
}
