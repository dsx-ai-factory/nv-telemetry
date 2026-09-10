// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The sensor corpus, replayed: each fixture is one device answer, and each
//! test asserts *both* the batches and the exact issue list — a projection
//! that silently dropped a field would pass any test that checked only one.
//!
//! The mock runs typed decode inside `Bmc::get`, exactly the seam the real
//! transport presents, so a fixture exercises decode plus projection.
//! Raw-bytes leniency belongs to nv-redfish's own tests.

use std::sync::Arc;

use nv_redfish_bmc_mock::Bmc;
use nv_redfish_bmc_mock::Expect;
use nv_telemetry_model::Completeness;
use nv_telemetry_model::Coverage;
use nv_telemetry_model::EndpointContext;
use nv_telemetry_model::NumericValue;
use nv_telemetry_model::ObservationBatch;
use nv_telemetry_model::ObservationWindow;
use nv_telemetry_model::Origin;
use nv_telemetry_model::Payload;
use nv_telemetry_model::Reading;
use nv_telemetry_model::Readings;
use nv_telemetry_model::SignalDescriptor;
use nv_telemetry_model::SignalKey;
use nv_telemetry_model::StateObservation;
use nv_telemetry_model::States;
use nv_telemetry_model::Subject;
use nv_telemetry_model::Timestamp;
use nv_telemetry_model::Value;
use nv_telemetry_model::ValueRange;
use nv_telemetry_redfish::SensorRead;
use nv_telemetry_source::acquire as run_acquisition;
use nv_telemetry_source::Acquired;
use nv_telemetry_source::ProjectionIssue;

const URI: &str = "/redfish/v1/Chassis/1U/Sensors/CPU1Temp";

fn at() -> Timestamp {
    Timestamp::new(1_785_621_243, 0).expect("a valid instant")
}

fn endpoint() -> EndpointContext {
    EndpointContext::builder()
        .endpoint_id("bmc-lab-07")
        .build()
        .expect("a valid endpoint")
}

/// Replays one fixture through the full provider and returns what it
/// acquired.
async fn acquire(uri: &str, fixture: &str) -> Acquired {
    let bmc = Arc::new(Bmc::<nv_redfish_bmc_mock::Error>::default());
    bmc.expect(Expect::get(uri, fixture));
    let read = SensorRead::new(endpoint(), uri.to_string().into(), bmc);
    run_acquisition(&read, at())
        .await
        .expect("the device answered")
}

fn subject() -> Subject {
    Subject::builder()
        .kind("sensor")
        .scope(vec!["1U".into()])
        .id("CPU1Temp")
        .build()
        .expect("a valid subject")
}

fn key() -> SignalKey {
    SignalKey::builder()
        .subject(subject())
        .build()
        .expect("a valid key")
}

fn double(value: f64) -> NumericValue {
    NumericValue::double(value).expect("finite")
}

fn batch(payload: Payload) -> ObservationBatch {
    ObservationBatch::builder()
        .endpoint(endpoint())
        .origin(
            Origin::builder()
                .provider("redfish.sensor.odata")
                .request_class("sensor-read")
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
                .build()
                .expect("valid coverage"),
        )
        .payload(payload)
        .build()
        .expect("a valid batch")
}

/// The readings batch around one descriptor and its optional sample.
fn readings_batch(descriptor: SignalDescriptor, sample: Option<Reading>) -> ObservationBatch {
    batch(Payload::Readings(
        Readings::builder()
            .descriptors(vec![descriptor])
            .samples(sample.into_iter().collect())
            .build()
            .expect("a valid readings payload"),
    ))
}

fn states_batch(observations: Vec<StateObservation>) -> ObservationBatch {
    batch(Payload::States(
        States::builder()
            .observations(observations)
            .build()
            .expect("a valid states payload"),
    ))
}

/// The nominal fixture's descriptor: temperature, Celsius, 0..=105.
fn nominal_descriptor() -> SignalDescriptor {
    SignalDescriptor::builder()
        .key(key())
        .kind("temperature")
        .unit("Cel")
        .range(
            ValueRange::builder()
                .min(double(0.0))
                .max(double(105.0))
                .build()
                .expect("a valid range"),
        )
        .build()
        .expect("a valid descriptor")
}

fn nominal_sample() -> Reading {
    Reading::builder()
        .key(key())
        .value(double(47.5))
        .build()
        .expect("a valid reading")
}

fn state(name: &str, value: &str) -> StateObservation {
    StateObservation::builder()
        .subject(subject())
        .name(name)
        .value(Value::string(value).expect("a short value"))
        .build()
        .expect("a valid observation")
}

fn threshold(name: &str, activation: Option<&str>, reading: Option<f64>) -> StateObservation {
    let mut entries: Vec<(String, Value)> = Vec::new();
    if let Some(activation) = activation {
        entries.push((
            "activation".to_owned(),
            Value::string(activation).expect("a short value"),
        ));
    }
    if let Some(reading) = reading {
        entries.push((
            "reading".to_owned(),
            Value::double(reading).expect("finite"),
        ));
    }
    StateObservation::builder()
        .subject(subject())
        .name(name)
        .value(Value::map(entries).expect("a valid map"))
        .build()
        .expect("a valid observation")
}

fn nominal_states() -> Vec<StateObservation> {
    vec![
        state("state", "Enabled"),
        state("health", "OK"),
        threshold("threshold.upper-critical", Some("Increasing"), Some(95.0)),
    ]
}

#[tokio::test]
async fn the_worked_example_produces_its_two_batches() {
    let acquired = acquire(URI, include_str!("fixtures/sensor/nominal.json")).await;

    let expected = [
        readings_batch(nominal_descriptor(), Some(nominal_sample())),
        states_batch(nominal_states()),
    ];
    assert_eq!(acquired.batches(), expected);
    assert_eq!(acquired.issues(), &[]);

    // One byte-level pin: equal validated values must be equal canonical
    // bytes, and this is the corpus's anchor case.
    assert_eq!(
        acquired.batches()[0].encode_to_vec(),
        expected[0].encode_to_vec()
    );
}

#[tokio::test]
async fn query_options_do_not_change_resource_identity() {
    let uri = format!("{URI}?$select=Id,Reading");
    let acquired = acquire(&uri, include_str!("fixtures/sensor/nominal.json")).await;

    assert_eq!(
        acquired.batches(),
        [
            readings_batch(nominal_descriptor(), Some(nominal_sample())),
            states_batch(nominal_states()),
        ]
    );
    assert_eq!(acquired.issues(), &[]);
}

#[tokio::test]
async fn a_threshold_selects_the_sensors_exact_signal_key() {
    let acquired = acquire(URI, include_str!("fixtures/sensor/nominal.json")).await;
    let Payload::Readings(readings) = acquired.batches()[0].payload() else {
        panic!("expected readings");
    };
    let Payload::States(states) = acquired.batches()[1].payload() else {
        panic!("expected states");
    };
    let threshold = states
        .observations()
        .iter()
        .find(|state| state.name() == "threshold.upper-critical")
        .expect("threshold projected");
    let descriptor = readings
        .descriptors()
        .iter()
        .find(|descriptor| {
            descriptor.key().subject() == threshold.subject()
                && descriptor.key().facet() == threshold.facet()
        })
        .expect("threshold resolves to its exact signal key");
    assert_eq!(threshold.facet(), None);
    assert_eq!(descriptor.unit(), Some("Cel"));
}

#[tokio::test]
async fn a_null_reading_is_a_quiet_device_not_an_issue() {
    let acquired = acquire(URI, include_str!("fixtures/sensor/reading-null.json")).await;

    // No sample and no issue; the descriptor still says the signal exists,
    // and the condition is visible in the states domain.
    let expected = [
        readings_batch(nominal_descriptor(), None),
        states_batch(nominal_states()),
    ];
    assert_eq!(acquired.batches(), expected);
    assert_eq!(acquired.issues(), &[]);
}

#[tokio::test]
async fn an_absent_reading_reads_exactly_like_a_null_one() {
    let acquired = acquire(URI, include_str!("fixtures/sensor/reading-absent.json")).await;
    assert_eq!(
        acquired.batches(),
        [
            readings_batch(nominal_descriptor(), None),
            states_batch(nominal_states()),
        ]
    );
    assert_eq!(acquired.issues(), &[]);
}

#[tokio::test]
async fn a_sensor_outside_a_chassis_yields_issues_and_nothing_else() {
    let uri = "/redfish/v1/Systems/1/Sensors/CPU1Temp";
    let acquired = acquire(uri, include_str!("fixtures/sensor/no-chassis-uri.json")).await;

    // Identity is all-or-nothing: no scope, no batch items — a guessed
    // subject would join to the wrong resource.
    assert_eq!(acquired.batches(), &[]);
    assert_eq!(
        acquired.issues(),
        &[ProjectionIssue::invalid(
            "@location.chassis",
            "requested location has no chassis segment"
        )]
    );
}

#[tokio::test]
async fn an_identity_failure_never_copies_query_secrets_into_an_issue() {
    let uri = "/redfish/v1/Systems/1/Sensors/CPU1Temp?token=do-not-copy";
    let acquired = acquire(uri, include_str!("fixtures/sensor/no-chassis-uri.json")).await;

    assert_eq!(acquired.batches(), &[]);
    assert_eq!(
        acquired.issues(),
        &[ProjectionIssue::invalid(
            "@location.chassis",
            "requested location has no chassis segment"
        )]
    );
    assert!(
        acquired
            .issues()
            .iter()
            .all(|issue| !issue.to_string().contains("do-not-copy")),
        "the requested URI escaped into projection diagnostics"
    );
}

#[tokio::test]
async fn an_empty_id_fails_identity_with_everything_else_still_evaluated() {
    let acquired = acquire(URI, include_str!("fixtures/sensor/id-empty.json")).await;

    assert_eq!(acquired.batches(), &[]);
    // The body is otherwise valid, so the identity fault is the whole list —
    // pinning that evaluation continued past it.
    assert_eq!(
        acquired.issues(),
        &[ProjectionIssue::invalid(
            "Sensor.Id",
            "`id`: present but empty"
        )]
    );
}

#[tokio::test]
async fn every_identity_fault_is_reported_before_output_is_suppressed() {
    let uri = "/redfish/v1/Systems/1/Sensors/CPU1Temp";
    let acquired = acquire(uri, include_str!("fixtures/sensor/id-empty.json")).await;

    assert_eq!(acquired.batches(), &[]);
    assert_eq!(
        acquired.issues(),
        &[
            ProjectionIssue::invalid("Sensor.Id", "`id`: present but empty"),
            ProjectionIssue::invalid(
                "@location.chassis",
                "requested location has no chassis segment"
            ),
        ]
    );
}

#[tokio::test]
async fn an_inverted_range_costs_the_range_and_nothing_else() {
    let acquired = acquire(URI, include_str!("fixtures/sensor/range-inverted.json")).await;

    let descriptor = SignalDescriptor::builder()
        .key(key())
        .kind("temperature")
        .unit("Cel")
        .build()
        .expect("a valid descriptor");
    assert_eq!(
        acquired.batches(),
        [
            readings_batch(descriptor, Some(nominal_sample())),
            states_batch(nominal_states()),
        ]
    );
    assert_eq!(
        acquired.issues(),
        &[ProjectionIssue::invalid(
            "Sensor.ReadingRangeMax",
            "`max`: the minimum must not exceed the maximum"
        )]
    );
}

#[tokio::test]
async fn one_bound_is_a_legal_range() {
    let acquired = acquire(URI, include_str!("fixtures/sensor/range-min-only.json")).await;

    let descriptor = SignalDescriptor::builder()
        .key(key())
        .kind("temperature")
        .unit("Cel")
        .range(
            ValueRange::builder()
                .min(double(0.0))
                .build()
                .expect("one bound is legal"),
        )
        .build()
        .expect("a valid descriptor");
    assert_eq!(
        acquired.batches(),
        [
            readings_batch(descriptor, Some(nominal_sample())),
            states_batch(nominal_states()),
        ]
    );
    assert_eq!(acquired.issues(), &[]);
}

#[tokio::test]
async fn an_empty_unit_is_not_dimensionless() {
    let acquired = acquire(URI, include_str!("fixtures/sensor/unit-empty.json")).await;

    let descriptor = SignalDescriptor::builder()
        .key(key())
        .kind("temperature")
        .range(
            ValueRange::builder()
                .min(double(0.0))
                .max(double(105.0))
                .build()
                .expect("a valid range"),
        )
        .build()
        .expect("a valid descriptor");
    assert_eq!(
        acquired.batches(),
        [
            readings_batch(descriptor, Some(nominal_sample())),
            states_batch(nominal_states()),
        ]
    );
    // The detail quotes the contract's own violation — the same spelling
    // the id-empty and range-inverted pins carry — because the compiler
    // derives checks from the schema's bounds and never invents prose.
    assert_eq!(
        acquired.issues(),
        &[ProjectionIssue::invalid(
            "Sensor.ReadingUnits",
            "`unit`: present but empty"
        )]
    );
}

#[tokio::test]
async fn an_unknown_reading_type_never_fabricates_a_kind() {
    let acquired = acquire(
        URI,
        include_str!("fixtures/sensor/unknown-reading-type.json"),
    )
    .await;

    let descriptor = SignalDescriptor::builder()
        .key(key())
        .unit("Cel")
        .range(
            ValueRange::builder()
                .min(double(0.0))
                .max(double(105.0))
                .build()
                .expect("a valid range"),
        )
        .build()
        .expect("a valid descriptor");
    assert_eq!(
        acquired.batches(),
        [
            readings_batch(descriptor, Some(nominal_sample())),
            states_batch(nominal_states()),
        ]
    );
    assert_eq!(
        acquired.issues(),
        &[ProjectionIssue::invalid(
            "Sensor.ReadingType",
            "outside the known value set"
        )]
    );
}

#[tokio::test]
async fn an_unknown_state_is_reported_and_health_survives() {
    let acquired = acquire(URI, include_str!("fixtures/sensor/unknown-state.json")).await;

    let states = vec![
        state("health", "OK"),
        threshold("threshold.upper-critical", Some("Increasing"), Some(95.0)),
    ];
    assert_eq!(
        acquired.batches(),
        [
            readings_batch(nominal_descriptor(), Some(nominal_sample())),
            states_batch(states),
        ]
    );
    assert_eq!(
        acquired.issues(),
        &[ProjectionIssue::invalid(
            "Sensor.Status.State",
            "outside the known value set"
        )]
    );
}

#[tokio::test]
async fn a_partial_threshold_carries_what_it_has() {
    let acquired = acquire(URI, include_str!("fixtures/sensor/threshold-partial.json")).await;

    let states = vec![
        state("state", "Enabled"),
        state("health", "OK"),
        threshold("threshold.upper-critical", None, Some(95.0)),
    ];
    assert_eq!(
        acquired.batches(),
        [
            readings_batch(nominal_descriptor(), Some(nominal_sample())),
            states_batch(states),
        ]
    );
    assert_eq!(acquired.issues(), &[]);
}

#[tokio::test]
async fn a_disabled_threshold_is_still_observed_configuration() {
    let acquired = acquire(URI, include_str!("fixtures/sensor/threshold-disabled.json")).await;

    let states = vec![
        state("state", "Enabled"),
        state("health", "OK"),
        threshold("threshold.lower-caution", Some("Disabled"), Some(5.0)),
    ];
    assert_eq!(
        acquired.batches(),
        [
            readings_batch(nominal_descriptor(), Some(nominal_sample())),
            states_batch(states),
        ]
    );
    assert_eq!(acquired.issues(), &[]);
}

#[tokio::test]
async fn a_bare_sensor_is_a_signal_that_exists_and_nothing_more() {
    let acquired = acquire(URI, include_str!("fixtures/sensor/bare-minimum.json")).await;

    let descriptor = SignalDescriptor::builder()
        .key(key())
        .build()
        .expect("a valid descriptor");
    // No states batch: nothing observed no facet.
    assert_eq!(acquired.batches(), [readings_batch(descriptor, None)]);
    assert_eq!(acquired.issues(), &[]);
}

#[tokio::test]
async fn a_transport_failure_is_classified_never_a_batch() {
    use nv_telemetry_source::AcquisitionFailureClass;

    let bmc = Arc::new(Bmc::<nv_redfish_bmc_mock::Error>::default());
    // No expectation queued: the mock fails the GET, standing in for any
    // transport failure. A mock error is a harness fault, hence Internal.
    let read = SensorRead::new(endpoint(), URI.to_string().into(), bmc);
    let failure = run_acquisition(&read, at())
        .await
        .expect_err("the transport failed");

    assert_eq!(failure.class(), AcquisitionFailureClass::Internal);
    assert_eq!(failure.retryable(), Some(false));
}
