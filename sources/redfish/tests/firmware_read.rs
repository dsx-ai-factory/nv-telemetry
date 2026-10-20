// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The firmware corpus, replayed: the update service, its inventory
//! collection, and each member are separate device answers, and each test
//! asserts *both* the batches and the exact issue list, the same discipline
//! as the other corpora. The pins this corpus owns: the walk claims
//! `COMPLETE` only when every listed member was read — a member the device
//! would not serve, or a walk cut short, ships `PARTIAL` — an empty
//! collection is still a complete claim, shipped as an empty batch, and a
//! collection that lists one member twice, as a GH200's does, is read once.

use std::collections::BTreeMap;
use std::sync::Arc;

use nv_redfish_bmc_mock::Bmc;
use nv_redfish_bmc_mock::Error;
use nv_redfish_bmc_mock::Expect;
use nv_redfish_bmc_mock::ExpectedRequest;
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
use nv_telemetry_redfish::FirmwareRead;
use nv_telemetry_redfish::TRUNCATED_WALK_LOCATOR;
use nv_telemetry_source::acquire as run_acquisition;
use nv_telemetry_source::Acquired;
use nv_telemetry_source::AcquisitionFailure;
use nv_telemetry_source::AcquisitionFailureClass;
use nv_telemetry_source::ProjectionIssue;

const SERVICE: &str = "/redfish/v1/UpdateService";
const INVENTORY: &str = "/redfish/v1/UpdateService/FirmwareInventory";
const INVENTORY_PAGE_2: &str = "/redfish/v1/UpdateService/FirmwareInventory?$skip=1";
const BMC: &str = "/redfish/v1/UpdateService/FirmwareInventory/HostBMC_0";
const BIOS: &str = "/redfish/v1/UpdateService/FirmwareInventory/HostBIOS_0";

const UPDATE_SERVICE: &str = include_str!("fixtures/firmware/update-service.json");
const INVENTORY_TWO: &str = include_str!("fixtures/firmware/inventory-two.json");
const ITEM_NOMINAL: &str = include_str!("fixtures/firmware/item-nominal.json");
const ITEM_MINIMAL: &str = include_str!("fixtures/firmware/item-minimal.json");

/// 2026-01-15T00:00:00Z: the nominal item's `ReleaseDate`.
const NOMINAL_RELEASE: i64 = 1_768_435_200;

fn at() -> Timestamp {
    Timestamp::new(1_785_621_243, 0).expect("a valid instant")
}

fn endpoint() -> EndpointContext {
    EndpointContext::builder()
        .endpoint_id("bmc-lab-07")
        .build()
        .expect("a valid endpoint")
}

/// Primes the mock with the device's answers in the order the provider asks
/// — service, collection, then each member — and runs the acquisition.
async fn run(answers: Vec<Expect<Error>>) -> Result<Acquired, AcquisitionFailure> {
    let bmc = Arc::new(Bmc::<Error>::default());
    for answer in answers {
        bmc.expect(answer);
    }
    let read = FirmwareRead::new(endpoint(), SERVICE.to_string().into(), bmc);
    run_acquisition(&read, at()).await
}

fn answers(pairs: &[(&str, &str)]) -> Vec<Expect<Error>> {
    pairs
        .iter()
        .map(|(uri, body)| Expect::get(uri, body))
        .collect()
}

/// The device refusing one GET, classified as a 501 would be.
fn refused(uri: &str) -> Expect<Error> {
    Expect {
        request: ExpectedRequest::Get {
            id: uri.to_owned().into(),
        },
        response: Err(Error::NotSupported),
    }
}

async fn acquire(pairs: &[(&str, &str)]) -> Acquired {
    run(answers(pairs)).await.expect("the device answered")
}

fn text(value: &str) -> Value {
    Value::string(value).expect("a short value")
}

fn subject(id: &str) -> Subject {
    Subject::builder()
        .kind("firmware")
        .id(id)
        .build()
        .expect("a valid subject")
}

/// The update service the batch covers: the population every component
/// belongs to.
fn scope() -> Subject {
    Subject::builder()
        .kind("update-service")
        .id("UpdateService")
        .build()
        .expect("a valid subject")
}

fn batch(completeness: Completeness, payload: Payload) -> ObservationBatch {
    ObservationBatch::builder()
        .endpoint(endpoint())
        .origin(
            Origin::builder()
                .provider("redfish.update-service.odata")
                .request_class("firmware-read")
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
                .completeness(completeness)
                .scope(scope())
                .build()
                .expect("valid coverage"),
        )
        .payload(payload)
        .build()
        .expect("a valid batch")
}

fn inventory(completeness: Completeness, items: Vec<InventoryItem>) -> ObservationBatch {
    batch(
        completeness,
        Payload::Inventory(
            Inventory::builder()
                .items(items)
                .build()
                .expect("a valid inventory payload"),
        ),
    )
}

fn states(completeness: Completeness, observations: Vec<StateObservation>) -> ObservationBatch {
    batch(
        completeness,
        Payload::States(
            States::builder()
                .observations(observations)
                .build()
                .expect("a valid states payload"),
        ),
    )
}

fn item(id: &str, location: &str, attributes: BTreeMap<String, Value>) -> InventoryItem {
    InventoryItem::builder()
        .subject(subject(id))
        .source_key(location)
        .attributes(attributes)
        .build()
        .expect("a valid item")
}

fn state(id: &str, name: &str, value: &str) -> StateObservation {
    StateObservation::builder()
        .subject(subject(id))
        .name(name)
        .value(text(value))
        .build()
        .expect("a valid observation")
}

/// The nominal item, every attribute present: text, a boolean, an instant.
fn nominal_item() -> InventoryItem {
    item(
        "HostBMC_0",
        BMC,
        BTreeMap::from([
            ("lowest-supported-version".to_owned(), text("47.00.00")),
            ("manufacturer".to_owned(), text("NVIDIA")),
            ("name".to_owned(), text("Host BMC Firmware")),
            (
                "release-date".to_owned(),
                Value::timestamp(Timestamp::new(NOMINAL_RELEASE, 0).expect("a valid instant")),
            ),
            ("software-id".to_owned(), text("BMC-47")),
            ("updateable".to_owned(), Value::bool(true)),
            ("version".to_owned(), text("47.20.02")),
        ]),
    )
}

/// A component the device names and nothing more: still a component.
fn minimal_item() -> InventoryItem {
    item(
        "HostBIOS_0",
        BIOS,
        BTreeMap::from([("name".to_owned(), text("Host UEFI"))]),
    )
}

fn nominal_states() -> Vec<StateObservation> {
    vec![
        state("HostBMC_0", "state", "Enabled"),
        state("HostBMC_0", "health", "OK"),
    ]
}

/// What a walk that read `HostBMC_0` and no more ships: the component it
/// read, and no claim about the rest of the population.
fn partial_nominal() -> [ObservationBatch; 2] {
    [
        inventory(Completeness::Partial, vec![nominal_item()]),
        states(Completeness::Partial, nominal_states()),
    ]
}

/// The one issue a walk over the two-member collection records when it
/// stops after the first.
fn truncated(reason: &str) -> ProjectionIssue {
    ProjectionIssue::invalid(
        TRUNCATED_WALK_LOCATOR,
        format!("walk read the first 1 of 2 members: {reason}"),
    )
}

#[tokio::test]
async fn a_fully_read_inventory_is_one_complete_batch_under_the_update_services_scope() {
    let acquired = acquire(&[
        (SERVICE, UPDATE_SERVICE),
        (INVENTORY, INVENTORY_TWO),
        (BMC, ITEM_NOMINAL),
        (BIOS, ITEM_MINIMAL),
    ])
    .await;

    assert_eq!(
        acquired.batches(),
        [
            inventory(Completeness::Complete, vec![nominal_item(), minimal_item()]),
            states(Completeness::Complete, nominal_states()),
        ]
    );
    assert_eq!(acquired.issues(), &[]);
}

#[tokio::test]
async fn a_paged_inventory_is_followed_to_its_end() {
    let acquired = acquire(&[
        (SERVICE, UPDATE_SERVICE),
        (
            INVENTORY,
            include_str!("fixtures/firmware/inventory-page-1.json"),
        ),
        (BMC, ITEM_NOMINAL),
        (
            INVENTORY_PAGE_2,
            include_str!("fixtures/firmware/inventory-page-2.json"),
        ),
        (BIOS, ITEM_MINIMAL),
    ])
    .await;

    assert_eq!(
        acquired.batches(),
        [
            inventory(Completeness::Complete, vec![nominal_item(), minimal_item()]),
            states(Completeness::Complete, nominal_states()),
        ]
    );
    assert_eq!(acquired.issues(), &[]);
}

#[tokio::test]
async fn an_empty_inventory_is_still_a_complete_claim() {
    let acquired = acquire(&[
        (SERVICE, UPDATE_SERVICE),
        (
            INVENTORY,
            include_str!("fixtures/firmware/inventory-empty.json"),
        ),
    ])
    .await;

    assert_eq!(
        acquired.batches(),
        [inventory(Completeness::Complete, Vec::new())]
    );
    assert_eq!(acquired.issues(), &[]);
}

#[tokio::test]
async fn a_member_the_device_would_not_serve_is_recorded_and_the_batch_is_partial() {
    let mut answers = answers(&[
        (SERVICE, UPDATE_SERVICE),
        (INVENTORY, INVENTORY_TWO),
        (BMC, ITEM_NOMINAL),
    ]);
    answers.push(refused(BIOS));
    let acquired = run(answers).await.expect("the device answered");

    // The component the device listed but would not serve is not a removed
    // one, so nothing here may retire it.
    assert_eq!(acquired.batches(), partial_nominal());
    assert_eq!(
        acquired.issues(),
        &[ProjectionIssue::invalid(
            "Members[1]",
            "member not read (Unsupported): Redfish mock answered not supported"
        )]
    );
}

#[tokio::test]
async fn a_walk_that_cannot_reach_its_next_page_says_so_and_is_partial() {
    let acquired = acquire(&[
        (SERVICE, UPDATE_SERVICE),
        (
            INVENTORY,
            include_str!("fixtures/firmware/inventory-broken-link.json"),
        ),
        (BMC, ITEM_NOMINAL),
    ])
    .await;

    assert_eq!(acquired.batches(), partial_nominal());
    assert_eq!(
        acquired.issues(),
        &[truncated("the collection's nextLink could not be resolved")]
    );
}

#[tokio::test]
async fn a_next_link_back_to_a_page_already_read_stops_the_walk() {
    let acquired = acquire(&[
        (SERVICE, UPDATE_SERVICE),
        (
            INVENTORY,
            include_str!("fixtures/firmware/inventory-looping-link.json"),
        ),
        (BMC, ITEM_NOMINAL),
    ])
    .await;

    assert_eq!(acquired.batches(), partial_nominal());
    assert_eq!(
        acquired.issues(),
        &[truncated(
            "the collection's nextLink returned to a page already read"
        )]
    );
}

#[tokio::test]
async fn a_later_page_without_members_stops_the_walk() {
    let acquired = acquire(&[
        (SERVICE, UPDATE_SERVICE),
        (
            INVENTORY,
            include_str!("fixtures/firmware/inventory-page-1.json"),
        ),
        (BMC, ITEM_NOMINAL),
        (
            INVENTORY_PAGE_2,
            include_str!("fixtures/firmware/inventory-page-empty.json"),
        ),
    ])
    .await;

    assert_eq!(acquired.batches(), partial_nominal());
    assert_eq!(
        acquired.issues(),
        &[truncated("the collection answered an empty page")]
    );
}

#[tokio::test]
async fn a_listing_short_of_its_own_count_is_not_a_complete_claim() {
    // The device says two and lists one: a consumer must not retire the
    // one it did not list.
    let acquired = acquire(&[
        (SERVICE, UPDATE_SERVICE),
        (
            INVENTORY,
            include_str!("fixtures/firmware/inventory-short.json"),
        ),
        (BMC, ITEM_NOMINAL),
    ])
    .await;

    assert_eq!(acquired.batches(), partial_nominal());
    assert_eq!(
        acquired.issues(),
        &[truncated(
            "the collection listed fewer members than its count"
        )]
    );
}

#[tokio::test]
async fn an_unknown_state_is_reported_while_health_and_the_item_survive() {
    let acquired = acquire(&[
        (SERVICE, UPDATE_SERVICE),
        (INVENTORY, INVENTORY_TWO),
        (
            BMC,
            include_str!("fixtures/firmware/item-unknown-state.json"),
        ),
        (BIOS, ITEM_MINIMAL),
    ])
    .await;

    let unknown = item(
        "HostBMC_0",
        BMC,
        BTreeMap::from([
            ("name".to_owned(), text("Host BMC Firmware")),
            ("version".to_owned(), text("47.20.02")),
        ]),
    );
    assert_eq!(
        acquired.batches(),
        [
            inventory(Completeness::Complete, vec![unknown, minimal_item()]),
            states(
                Completeness::Complete,
                vec![state("HostBMC_0", "health", "OK")]
            ),
        ]
    );
    assert_eq!(
        acquired.issues(),
        &[ProjectionIssue::invalid(
            "SoftwareInventory.Status.State",
            "outside the known value set"
        )
        .at_index("Members", 0)]
    );
}

#[tokio::test]
async fn a_member_listed_twice_is_read_once() {
    // Three listings, two members: the repeat is recorded at its own
    // position, costs no request, and the population is still whole.
    let acquired = acquire(&[
        (SERVICE, UPDATE_SERVICE),
        (
            INVENTORY,
            include_str!("fixtures/firmware/inventory-duplicate-listing.json"),
        ),
        (BMC, ITEM_NOMINAL),
        (BIOS, ITEM_MINIMAL),
    ])
    .await;

    assert_eq!(
        acquired.batches(),
        [
            inventory(Completeness::Complete, vec![nominal_item(), minimal_item()]),
            states(Completeness::Complete, nominal_states()),
        ]
    );
    assert_eq!(
        acquired.issues(),
        &[ProjectionIssue::invalid(
            "Members[2]",
            "member listed twice, first at Members[0]"
        )]
    );
}

#[tokio::test]
async fn a_member_claiming_anothers_identity_is_recorded_and_dropped() {
    // Two members, one `Id`: the batch admits a subject once, and the
    // component that went unrepresented keeps the claim partial.
    let acquired = acquire(&[
        (SERVICE, UPDATE_SERVICE),
        (INVENTORY, INVENTORY_TWO),
        (BMC, ITEM_NOMINAL),
        (BIOS, include_str!("fixtures/firmware/item-claims-bmc.json")),
    ])
    .await;

    assert_eq!(acquired.batches(), partial_nominal());
    assert_eq!(
        acquired.issues(),
        &[ProjectionIssue::invalid(
            "Members[1]",
            "claims the identity of Members[0]"
        )]
    );
}

#[tokio::test]
async fn an_update_service_without_a_firmware_inventory_is_unsupported() {
    let failure = run(answers(&[(
        SERVICE,
        include_str!("fixtures/firmware/update-service-without-inventory.json"),
    )]))
    .await
    .expect_err("nothing to walk");

    assert_eq!(failure.class(), AcquisitionFailureClass::Unsupported);
    assert_eq!(failure.retryable(), Some(false));
    assert_eq!(
        failure.detail(),
        Some("update service has no firmware inventory")
    );
}
