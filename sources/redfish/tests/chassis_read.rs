// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The chassis corpus, replayed: each fixture is one device answer, and
//! each test asserts *both* the batches and the exact issue list, the same
//! discipline as the sensor corpus. The new pin this corpus owns is
//! provenance: `source_key` carries the canonical location, so query
//! secrets never reach the wire.

use std::collections::BTreeMap;
use std::sync::Arc;

use nv_redfish_bmc_mock::Bmc;
use nv_redfish_bmc_mock::Expect;
use nv_telemetry_model::Completeness;
use nv_telemetry_model::Coverage;
use nv_telemetry_model::EndpointContext;
use nv_telemetry_model::Inventory;
use nv_telemetry_model::InventoryItem;
use nv_telemetry_model::ObservationBatch;
use nv_telemetry_model::ObservationWindow;
use nv_telemetry_model::Origin;
use nv_telemetry_model::Payload;
use nv_telemetry_model::StateObservation;
use nv_telemetry_model::States;
use nv_telemetry_model::Subject;
use nv_telemetry_model::Timestamp;
use nv_telemetry_model::Value;
use nv_telemetry_redfish::ChassisRead;
use nv_telemetry_source::acquire as run_acquisition;
use nv_telemetry_source::Acquired;
use nv_telemetry_source::ProjectionIssue;

const URI: &str = "/redfish/v1/Chassis/1U";

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
    let read = ChassisRead::new(endpoint(), uri.to_string().into(), bmc);
    run_acquisition(&read, at())
        .await
        .expect("the device answered")
}

fn subject() -> Subject {
    Subject::builder()
        .kind("chassis")
        .id("1U")
        .build()
        .expect("a valid subject")
}

fn attributes(entries: &[(&str, &str)]) -> BTreeMap<String, Value> {
    entries
        .iter()
        .map(|(key, value)| {
            (
                (*key).to_owned(),
                Value::string(*value).expect("a short value"),
            )
        })
        .collect()
}

fn batch(payload: Payload) -> ObservationBatch {
    ObservationBatch::builder()
        .endpoint(endpoint())
        .origin(
            Origin::builder()
                .provider("redfish.chassis.odata")
                .request_class("chassis-read")
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

fn inventory_batch(attributes: BTreeMap<String, Value>) -> ObservationBatch {
    let item = InventoryItem::builder()
        .subject(subject())
        .source_key(URI)
        .attributes(attributes)
        .build()
        .expect("a valid item");
    batch(Payload::Inventory(
        Inventory::builder()
            .items(vec![item])
            .build()
            .expect("a valid inventory payload"),
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

fn state(name: &str, value: &str) -> StateObservation {
    StateObservation::builder()
        .subject(subject())
        .name(name)
        .value(Value::string(value).expect("a short value"))
        .build()
        .expect("a valid observation")
}

/// The nominal fixture's identity plate, every attribute present.
fn nominal_attributes() -> BTreeMap<String, Value> {
    attributes(&[
        ("asset-tag", "rack7-slot3"),
        ("chassis-type", "RackMount"),
        ("manufacturer", "NVIDIA"),
        ("model", "DGX H100"),
        ("part-number", "965-2G506-0031-000"),
        ("serial-number", "SN-1234567"),
        ("sku", "965-2G506"),
        ("spare-part-number", "SP-2G506"),
        ("version", "A02"),
    ])
}

#[tokio::test]
async fn the_identity_plate_produces_its_two_batches() {
    let acquired = acquire(URI, include_str!("fixtures/chassis/nominal.json")).await;

    let expected = [
        inventory_batch(nominal_attributes()),
        states_batch(vec![state("state", "Enabled"), state("health", "OK")]),
    ];
    assert_eq!(acquired.batches(), expected);
    assert_eq!(acquired.issues(), &[]);

    // One byte-level pin: equal validated values must be equal canonical
    // bytes, and this is the inventory arm's anchor case.
    assert_eq!(
        acquired.batches()[0].encode_to_vec(),
        expected[0].encode_to_vec()
    );
}

#[tokio::test]
async fn provenance_is_canonical_and_never_carries_query_secrets() {
    let uri = format!("{URI}?token=do-not-copy");
    let acquired = acquire(&uri, include_str!("fixtures/chassis/nominal.json")).await;

    // `source_key` in the expected item is the bare URI: the canonical
    // location, with the query stripped before anything reaches the wire.
    assert_eq!(
        acquired.batches(),
        [
            inventory_batch(nominal_attributes()),
            states_batch(vec![state("state", "Enabled"), state("health", "OK")]),
        ]
    );
    assert_eq!(acquired.issues(), &[]);
    assert!(
        !format!("{:?}", acquired.batches()).contains("do-not-copy"),
        "the requested URI escaped into wire data"
    );
}

#[tokio::test]
async fn a_bare_chassis_is_an_item_that_exists_and_nothing_more() {
    let acquired = acquire(URI, include_str!("fixtures/chassis/bare-minimum.json")).await;

    // ChassisType is Redfish-required, so a decodable chassis always
    // carries at least that; no states batch, nothing observed no facet.
    assert_eq!(
        acquired.batches(),
        [inventory_batch(attributes(&[(
            "chassis-type",
            "RackMount"
        )]))]
    );
    assert_eq!(acquired.issues(), &[]);
}

#[tokio::test]
async fn null_and_absent_attributes_are_omitted_without_issues() {
    let acquired = acquire(URI, include_str!("fixtures/chassis/attributes-null.json")).await;

    assert_eq!(
        acquired.batches(),
        [
            inventory_batch(attributes(&[
                ("chassis-type", "RackMount"),
                ("model", "DGX H100"),
            ])),
            states_batch(vec![state("state", "Enabled"), state("health", "OK")]),
        ]
    );
    assert_eq!(acquired.issues(), &[]);
}

#[tokio::test]
async fn an_empty_id_fails_identity_with_everything_else_still_evaluated() {
    let acquired = acquire(URI, include_str!("fixtures/chassis/id-empty.json")).await;

    assert_eq!(acquired.batches(), &[]);
    assert_eq!(
        acquired.issues(),
        &[ProjectionIssue::invalid(
            "Chassis.Id",
            "`id`: present but empty"
        )]
    );
}

#[tokio::test]
async fn an_unknown_state_is_reported_while_health_and_inventory_survive() {
    let acquired = acquire(URI, include_str!("fixtures/chassis/unknown-state.json")).await;

    assert_eq!(
        acquired.batches(),
        [
            inventory_batch(nominal_attributes()),
            states_batch(vec![state("health", "OK")]),
        ]
    );
    assert_eq!(
        acquired.issues(),
        &[ProjectionIssue::invalid(
            "Chassis.Status.State",
            "outside the known value set"
        )]
    );
}

#[tokio::test]
async fn a_transport_failure_is_classified_never_a_batch() {
    use nv_telemetry_source::AcquisitionFailureClass;

    let bmc = Arc::new(Bmc::<nv_redfish_bmc_mock::Error>::default());
    // No expectation queued: the mock fails the GET, standing in for any
    // transport failure. A mock error is a harness fault, hence Internal.
    let read = ChassisRead::new(endpoint(), URI.to_string().into(), bmc);
    let failure = run_acquisition(&read, at())
        .await
        .expect_err("the transport failed");

    assert_eq!(failure.class(), AcquisitionFailureClass::Internal);
    assert_eq!(failure.retryable(), Some(false));
}
