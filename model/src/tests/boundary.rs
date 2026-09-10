// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The validated boundary: construction through builders, decoding through
//! `TryFrom`, and the maximal round trip that catches a dropped field.

use prost::Message as _;

use crate::generated::wire;
use crate::AcquisitionStatus;
use crate::Completeness;
use crate::Coverage;
use crate::EndpointContext;
use crate::FailureClass;
use crate::IssueKind;
use crate::NumericValue;
use crate::ObservationBatch;
use crate::ObservationWindow;
use crate::ObservedResource;
use crate::Origin;
use crate::Outcome;
use crate::Payload;
use crate::ProjectionIssue;
use crate::ProjectionIssues;
use crate::Reading;
use crate::Readings;
use crate::ResourceGraph;
use crate::ResourceRelation;
use crate::SignalDescriptor;
use crate::SignalKey;
use crate::StateObservation;
use crate::States;
use crate::Subject;
use crate::Timestamp;
use crate::Value;
use crate::ValueRange;
use crate::Violation;

pub(super) fn built_subject(kind: &str, id: &str) -> Subject {
    Subject::builder()
        .kind(kind)
        .id(id)
        .build()
        .expect("a valid subject")
}

pub(super) fn built_key(id: &str) -> SignalKey {
    SignalKey::builder()
        .subject(built_subject("sensor", id))
        .build()
        .expect("a valid signal key")
}

#[test]
fn an_empty_identity_is_unrepresentable() {
    // The case the vocabulary branch was for: "" is a well-formed identity
    // that every failed read would collapse into, and Subject is hashable.
    let error = Subject::builder()
        .kind("sensor")
        .id("")
        .build()
        .unwrap_err();
    assert_eq!(error.path(), "id");
    assert_eq!(error.violation(), &Violation::Empty);

    let error = Subject::builder().id("x").build().unwrap_err();
    assert_eq!(error.path(), "kind");
    assert_eq!(error.violation(), &Violation::Absent);
}

fn valid_batch() -> ObservationBatch {
    let key = built_key("CPU1Temp");
    let descriptor = SignalDescriptor::builder()
        .key(key.clone())
        .kind("temperature")
        .unit("Cel")
        .build()
        .expect("a valid descriptor");
    let sample = Reading::builder()
        .key(key)
        .value(NumericValue::double(47.5).expect("finite"))
        .build()
        .expect("a valid reading");
    let readings = Readings::builder()
        .descriptors(vec![descriptor])
        .samples(vec![sample])
        .build()
        .expect("a valid readings payload");

    ObservationBatch::builder()
        .endpoint(
            EndpointContext::builder()
                .endpoint_id("bmc-lab-07")
                .build()
                .expect("a valid endpoint"),
        )
        .origin(
            Origin::builder()
                .provider("redfish.sensor.odata")
                .request_class("sensor-read")
                .build()
                .expect("a valid origin"),
        )
        .window(
            ObservationWindow::builder()
                .start(Timestamp::new(1_785_621_243, 0).expect("a valid instant"))
                .build()
                .expect("a valid window"),
        )
        .coverage(
            Coverage::builder()
                .completeness(Completeness::Partial)
                .build()
                .expect("valid coverage"),
        )
        .payload(Payload::Readings(readings))
        .build()
        .expect("a valid batch")
}

#[test]
fn a_batch_round_trips_through_the_validated_boundary() {
    let batch = valid_batch();
    let bytes = batch.encode_to_vec();
    let decoded = ObservationBatch::decode(&bytes).expect("wire round trip");
    assert_eq!(decoded, batch);
}

#[test]
fn a_sample_without_its_descriptor_is_rejected() {
    // "Every key referenced by a sample must resolve here — a wrapper rule."
    let sample = Reading::builder()
        .key(built_key("CPU1Temp"))
        .value(NumericValue::double(1.0).expect("finite"))
        .build()
        .expect("a valid reading");
    let error = Readings::builder()
        .samples(vec![sample])
        .build()
        .unwrap_err();
    assert_eq!(error.path(), "samples[0]");
}

#[test]
fn two_descriptors_for_one_key_are_rejected() {
    // The unique_by case the annotation was written for: same key, different
    // units, and a consumer with no rule for choosing.
    let descriptor = |unit: &str| {
        SignalDescriptor::builder()
            .key(built_key("PSU1"))
            .unit(unit)
            .build()
            .expect("a valid descriptor")
    };
    let error = Readings::builder()
        .descriptors(vec![descriptor("W"), descriptor("kW.h")])
        .build()
        .unwrap_err();
    assert_eq!(error.path(), "descriptors[1]");
    assert_eq!(error.violation(), &Violation::Duplicate);
}

#[test]
fn a_failure_carries_its_class_and_a_success_does_not() {
    let base = || {
        AcquisitionStatus::builder()
            .endpoint_id("bmc-lab-07")
            .provider("redfish.sensor.odata")
            .request_class("sensor-read")
            .started_at(Timestamp::new(1_785_621_243, 0).expect("a valid instant"))
    };

    let error = base().outcome(Outcome::Failed).build().unwrap_err();
    assert_eq!(error.path(), "failure_class");

    let error = base()
        .outcome(Outcome::Succeeded)
        .failure_class(FailureClass::Timeout)
        .build()
        .unwrap_err();
    assert_eq!(error.path(), "failure_class");

    assert!(base()
        .outcome(Outcome::Failed)
        .failure_class(FailureClass::Timeout)
        .build()
        .is_ok());
}

#[test]
fn an_invalid_issue_quotes_its_failure_and_silence_does_not() {
    let base = |kind: IssueKind| ProjectionIssue::builder().path("Reading").kind(kind);

    let error = base(IssueKind::Invalid).build().unwrap_err();
    assert_eq!(error.path(), "detail");

    let error = base(IssueKind::MissingRequired)
        .detail("noise")
        .build()
        .unwrap_err();
    assert_eq!(error.path(), "detail");

    assert!(base(IssueKind::MissingRequired).build().is_ok());
    assert!(base(IssueKind::Invalid)
        .detail("not finite")
        .build()
        .is_ok());
}

#[test]
fn one_issue_per_failed_field_and_never_an_empty_envelope() {
    let issue = |path: &str| {
        ProjectionIssue::builder()
            .path(path)
            .kind(IssueKind::MissingRequired)
            .build()
            .expect("a valid issue")
    };
    let base = || {
        ProjectionIssues::builder()
            .endpoint(
                EndpointContext::builder()
                    .endpoint_id("bmc-lab-07")
                    .build()
                    .expect("a valid endpoint"),
            )
            .origin(
                Origin::builder()
                    .provider("redfish")
                    .request_class("read")
                    .build()
                    .expect("a valid origin"),
            )
            .at(Timestamp::new(1_785_621_243, 0).expect("a valid instant"))
    };

    // Identity is the path alone: a second issue for the same field is a
    // duplicate even when its kind differs.
    let other_kind = ProjectionIssue::builder()
        .path("Id")
        .kind(IssueKind::Invalid)
        .detail("not finite")
        .build()
        .expect("a valid issue");
    let error = base()
        .issues(vec![issue("Id"), other_kind])
        .build()
        .unwrap_err();
    assert_eq!(error.path(), "issues[1]");
    assert_eq!(error.violation(), &Violation::Duplicate);

    let error = base().build().unwrap_err();
    assert_eq!(error.path(), "issues");
    assert_eq!(
        error.violation(),
        &Violation::Rule("no issues is no message, not an empty one")
    );

    // The decode path runs the same rule: an empty envelope on the wire is
    // the fabrication the rule exists to refuse.
    let empty = wire::ProjectionIssues {
        endpoint: Some(wire::EndpointContext {
            endpoint_id: Some("bmc-lab-07".into()),
            attributes: None,
        }),
        origin: Some(wire::Origin {
            provider: Some("redfish".into()),
            request_class: Some("read".into()),
        }),
        at: Some(wire::Timestamp {
            seconds: Some(1),
            nanos: Some(0),
        }),
        issues: vec![],
    };
    let error = ProjectionIssues::try_from(empty).unwrap_err();
    assert_eq!(error.path(), "issues");
    assert_eq!(
        error.violation(),
        &Violation::Rule("no issues is no message, not an empty one")
    );

    assert!(base().issues(vec![issue("Id")]).build().is_ok());
}

#[test]
fn a_window_end_must_follow_its_start() {
    let start = Timestamp::new(100, 0).expect("a valid instant");

    // Equal would be a second spelling of a point, which omitting the end
    // already spells.
    let error = ObservationWindow::builder()
        .start(start)
        .end(start)
        .build()
        .unwrap_err();
    assert_eq!(error.path(), "end");

    assert!(ObservationWindow::builder()
        .start(start)
        .end(Timestamp::new(100, 1).expect("a valid instant"))
        .build()
        .is_ok());
}

#[test]
fn an_unrecognized_enum_value_survives_and_unspecified_does_not() {
    let ahead = wire::Coverage {
        completeness: Some(99),
        scope: None,
    };
    let coverage = Coverage::try_from(ahead).expect("a newer producer's value decodes");
    assert_eq!(coverage.completeness(), Completeness::Unrecognized(99));

    let rebuilt = wire::Coverage::from(coverage);
    assert_eq!(rebuilt.completeness, Some(99), "re-encoding lost the value");

    let unspecified = wire::Coverage {
        completeness: Some(0),
        scope: None,
    };
    let error = Coverage::try_from(unspecified).unwrap_err();
    assert_eq!(error.violation(), &Violation::Unspecified);
}

#[test]
fn an_unrecognized_value_may_not_alias_a_recognized_one() {
    let base = || ProjectionIssue::builder().path("Reading");

    // A genuinely newer value builds; aliases of the unspecified value or
    // of a recognized variant do not — their bytes would fail this build's
    // own decode.
    assert!(base().kind(IssueKind::Unrecognized(99)).build().is_ok());

    let error = base().kind(IssueKind::Unrecognized(0)).build().unwrap_err();
    assert_eq!(error.path(), "kind");
    assert_eq!(error.violation(), &Violation::Unspecified);

    let error = base().kind(IssueKind::Unrecognized(2)).build().unwrap_err();
    assert_eq!(error.path(), "kind");
    assert_eq!(
        error.violation(),
        &Violation::Rule("an unrecognized value must not alias a recognized one")
    );
}

#[test]
fn a_complete_scoped_graph_must_hang_off_its_root() {
    let chassis = built_subject("chassis", "1U");
    let sensor = built_subject("sensor", "Inlet");
    let resource = |subject: &Subject| {
        ObservedResource::builder()
            .subject(subject.clone())
            .source_key(format!("/redfish/v1/{}", subject.id()))
            .properties_complete(true)
            .build()
            .expect("a valid resource")
    };

    let batch = |graph: ResourceGraph| {
        ObservationBatch::builder()
            .endpoint(
                EndpointContext::builder()
                    .endpoint_id("bmc-lab-07")
                    .build()
                    .expect("a valid endpoint"),
            )
            .origin(
                Origin::builder()
                    .provider("redfish.walker")
                    .request_class("subtree")
                    .build()
                    .expect("a valid origin"),
            )
            .window(
                ObservationWindow::builder()
                    .start(Timestamp::new(1_785_621_243, 0).expect("a valid instant"))
                    .build()
                    .expect("a valid window"),
            )
            .coverage(
                Coverage::builder()
                    .completeness(Completeness::Complete)
                    .scope(chassis.clone())
                    .build()
                    .expect("valid coverage"),
            )
            .payload(Payload::Resources(graph))
            .build()
    };

    // "A walk recording only each child naming its parent produces a graph
    // its own root cannot reach": no chassis->sensor edge, so the sensor is
    // unreachable and the complete claim is refused.
    let disconnected = ResourceGraph::builder()
        .resources(vec![resource(&chassis), resource(&sensor)])
        .build()
        .expect("structurally valid graph");
    let error = batch(disconnected).unwrap_err();
    assert_eq!(error.path(), "payload.resources[1]");

    let edge = ResourceRelation::builder()
        .source(chassis.clone())
        .target(sensor.clone())
        .kind("contains")
        .build()
        .expect("a valid relation");
    let connected = ResourceGraph::builder()
        .resources(vec![resource(&chassis), resource(&sensor)])
        .relations(vec![edge])
        .build()
        .expect("structurally valid graph");
    assert!(batch(connected).is_ok());
}

#[test]
fn repeated_state_facets_require_distinct_timestamps() {
    for stamps in [
        [None, None],
        [None, Some(10)],
        [Some(10), Some(10)],
        [Some(10), Some(11)],
    ] {
        let observations = ["down", "up"]
            .into_iter()
            .zip(stamps)
            .map(|(value, stamp)| {
                let mut builder = StateObservation::builder()
                    .subject(built_subject("port", "eth0"))
                    .name("state")
                    .value(Value::string(value).expect("short value"));
                if let Some(seconds) = stamp {
                    builder = builder.observed_at(Timestamp::new(seconds, 0).expect("instant"));
                }
                builder.build().expect("observation")
            })
            .collect::<Vec<_>>();
        let decoded = wire::States {
            observations: observations.iter().cloned().map(Into::into).collect(),
        };
        let built = States::builder().observations(observations).build();
        let decoded = States::try_from(decoded);
        let valid = stamps == [Some(10), Some(11)];
        assert_eq!(built.is_ok(), valid, "builder: {stamps:?}");
        assert_eq!(decoded.is_ok(), valid, "decode: {stamps:?}");
        if valid {
            assert_eq!(built.unwrap(), decoded.unwrap());
        }
    }
}

#[test]
fn a_range_needs_a_bound_one_arm_and_order() {
    assert!(ValueRange::builder().build().is_err(), "no bound at all");

    assert!(ValueRange::builder()
        .min(NumericValue::Int(0))
        .build()
        .is_ok());

    let mixed = ValueRange::builder()
        .min(NumericValue::Int(0))
        .max(NumericValue::double(5.0).expect("finite"))
        .build();
    assert!(mixed.is_err(), "bounds must share the signal's arm");

    let backwards = ValueRange::builder()
        .min(NumericValue::Int(5))
        .max(NumericValue::Int(1))
        .build();
    assert!(backwards.is_err(), "min must not exceed max");
}

fn wire_timestamp(seconds: i64) -> wire::Timestamp {
    wire::Timestamp {
        seconds: Some(seconds),
        nanos: Some(7),
    }
}

fn wire_subject(id: &str) -> wire::Subject {
    wire::Subject {
        kind: Some("sensor".into()),
        scope: vec!["1U".into()],
        id: Some(id.into()),
    }
}

fn wire_key(id: &str) -> wire::SignalKey {
    wire::SignalKey {
        subject: Some(wire_subject(id)),
        facet: Some("state/counters".into()),
    }
}

/// A single entry, so canonical sorting cannot reorder it and byte equality
/// stays the assertion.
fn wire_map() -> wire::value::Map {
    wire::value::Map {
        entries: vec![wire::value::map::Entry {
            key: Some("serial".into()),
            value: Some(wire::Value {
                kind: Some(wire::value::Kind::StringValue("SN-1".into())),
            }),
        }],
    }
}

fn wire_numeric(value: f64) -> wire::NumericValue {
    wire::NumericValue {
        kind: Some(wire::numeric_value::Kind::DoubleValue(value)),
    }
}

fn wire_endpoint() -> wire::EndpointContext {
    wire::EndpointContext {
        endpoint_id: Some("bmc-lab-07".into()),
        attributes: Some(wire_map()),
    }
}

fn wire_origin() -> wire::Origin {
    wire::Origin {
        provider: Some("redfish".into()),
        request_class: Some("read".into()),
    }
}

/// Maximal wire batches: every optional field set, one per payload domain.
// Long because it is exhaustive — one construction per payload domain with
// every field populated. Trimming it to a length limit would reopen exactly
// the blind spot it exists to close.
#[allow(clippy::too_many_lines)]
fn maximal_batches() -> Vec<wire::ObservationBatch> {
    let ts = wire_timestamp;
    let subject = wire_subject;
    let key = wire_key;
    let map = wire_map();
    let numeric = wire_numeric;

    let payloads = vec![
        wire::observation_batch::Payload::Readings(wire::Readings {
            descriptors: vec![wire::SignalDescriptor {
                key: Some(key("CPU1Temp")),
                kind: Some("temperature".into()),
                unit: Some("Cel".into()),
                range: Some(wire::ValueRange {
                    min: Some(numeric(0.0)),
                    max: Some(numeric(100.0)),
                }),
            }],
            samples: vec![wire::Reading {
                key: Some(key("CPU1Temp")),
                value: Some(numeric(47.5)),
                observed_at: Some(ts(10)),
            }],
        }),
        wire::observation_batch::Payload::Logs(wire::Logs {
            records: vec![wire::LogRecord {
                occurred_at: Some(ts(20)),
                severity: Some(3),
                message: Some("fan failed".into()),
                subject: Some(subject("Fan1")),
                entry_id: Some("sel-41".into()),
                attributes: Some(map.clone()),
            }],
        }),
        wire::observation_batch::Payload::States(wire::States {
            observations: vec![wire::StateObservation {
                subject: Some(subject("Fan1")),
                name: Some("health".into()),
                value: Some(wire::Value {
                    kind: Some(wire::value::Kind::StringValue("OK".into())),
                }),
                observed_at: Some(ts(30)),
            }],
        }),
        wire::observation_batch::Payload::Inventory(wire::Inventory {
            items: vec![wire::InventoryItem {
                subject: Some(subject("PSU1")),
                attributes: Some(map.clone()),
                source_key: Some("/redfish/v1/PSU1".into()),
            }],
        }),
        wire::observation_batch::Payload::Resources(wire::ResourceGraph {
            resources: vec![wire::ObservedResource {
                subject: Some(subject("1U")),
                source_key: Some("/redfish/v1/Chassis/1U".into()),
                source_schema: Some("#Chassis.v1_2_0.Chassis".into()),
                entity_tag: Some("W/\"e-1\"".into()),
                observed_at: Some(ts(40)),
                properties: Some(map.clone()),
                properties_complete: Some(true),
                unresolved: vec![wire::UnresolvedReference {
                    location: Some("/redfish/v1/Chassis/2U".into()),
                    property: Some("Links.ContainedBy".into()),
                }],
            }],
            relations: vec![wire::ResourceRelation {
                source: Some(subject("1U")),
                target: Some(subject("2U")),
                kind: Some("contains".into()),
            }],
        }),
    ];

    payloads
        .into_iter()
        .map(|payload| wire::ObservationBatch {
            endpoint: Some(wire_endpoint()),
            origin: Some(wire_origin()),
            window: Some(wire::ObservationWindow {
                start: Some(ts(1)),
                end: Some(ts(2)),
            }),
            coverage: Some(wire::Coverage {
                completeness: Some(wire::Completeness::Partial as i32),
                scope: Some(subject("1U")),
            }),
            payload: Some(payload),
        })
        .collect()
}

fn maximal_status() -> wire::AcquisitionStatus {
    wire::AcquisitionStatus {
        endpoint_id: Some("bmc-lab-07".into()),
        provider: Some("redfish".into()),
        request_class: Some("read".into()),
        outcome: Some(2),
        failure_class: Some(3),
        retryable: Some(true),
        started_at: Some(wire_timestamp(50)),
        duration_nanos: Some(125_000),
        detail: Some("timed out".into()),
    }
}

/// Issues are given in canonical (path) order so the unordered sort cannot
/// reorder them, keeping byte equality the assertion.
fn maximal_issues() -> wire::ProjectionIssues {
    wire::ProjectionIssues {
        endpoint: Some(wire_endpoint()),
        origin: Some(wire_origin()),
        at: Some(wire_timestamp(60)),
        issues: vec![
            wire::ProjectionIssue {
                path: Some("Id".into()),
                kind: Some(wire::projection_issue::IssueKind::MissingRequired as i32),
                detail: None,
            },
            wire::ProjectionIssue {
                path: Some("Sensors[3].Reading".into()),
                kind: Some(wire::projection_issue::IssueKind::Invalid as i32),
                detail: Some("not a finite number".into()),
            },
        ],
    }
}

/// Maximal wire messages: every optional field set, every payload domain
/// exercised. `TryFrom` consumes the wire message field by field and `From`
/// rebuilds it; a field one of them forgets is silent data loss, and sparse
/// round trips cannot see it.
#[test]
fn every_field_survives_the_validated_round_trip() {
    for maximal in maximal_batches() {
        let validated = ObservationBatch::try_from(maximal.clone()).expect("maximal batch valid");
        // The direct encoder against prost encoding the rebuilt wire tree:
        // byte equality, with every field of every domain present — the
        // strongest form of "same wire form, no intermediate tree".
        let direct = validated.encode_to_vec();
        let rebuilt = wire::ObservationBatch::from(validated);
        assert_eq!(rebuilt, maximal, "a field was dropped on the round trip");
        assert_eq!(
            direct,
            rebuilt.encode_to_vec(),
            "the direct encoder diverged from prost"
        );
    }

    let status = maximal_status();
    let validated = AcquisitionStatus::try_from(status.clone()).expect("maximal status valid");
    let direct = validated.encode_to_vec();
    assert_eq!(
        wire::AcquisitionStatus::from(validated),
        status,
        "a status field was dropped on the round trip"
    );
    assert_eq!(
        direct,
        status.encode_to_vec(),
        "the direct status encoder diverged from prost"
    );

    let issues = maximal_issues();
    let validated = ProjectionIssues::try_from(issues.clone()).expect("maximal issues valid");
    let direct = validated.encode_to_vec();
    assert_eq!(
        wire::ProjectionIssues::from(validated),
        issues,
        "an issue field was dropped on the round trip"
    );
    assert_eq!(
        direct,
        issues.encode_to_vec(),
        "the direct issues encoder diverged from prost"
    );
}
