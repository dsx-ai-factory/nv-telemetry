// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Optional HTTP boundary checks against the sibling standalone mock.
//! Run with `BMC_MOCK_ROOT` set via `make test-bmc-mock`.
//! Projection details and dispatcher scheduling remain in their own corpora.

mod support {
    pub(crate) mod bmc_mock;
}

use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use nv_telemetry_model::EndpointContext;
use nv_telemetry_model::LogRecord;
use nv_telemetry_model::NumericValue;
use nv_telemetry_model::Payload;
use nv_telemetry_model::Timestamp;
use nv_telemetry_redfish::ChassisRead;
use nv_telemetry_redfish::LogRead;
use nv_telemetry_redfish::SensorRead;
use nv_telemetry_source::acquire;
use nv_telemetry_source::Acquired;
use nv_telemetry_source::AcquisitionFailure;
use nv_telemetry_source::AcquisitionFailureClass as Failure;
use serde_json::json;
use serde_json::Value;
use support::bmc_mock::text;
use support::bmc_mock::Bmc;
use support::bmc_mock::Resources;
use support::bmc_mock::Server;
use support::bmc_mock::BMC_RESET_WINDOW;

fn endpoint() -> EndpointContext {
    EndpointContext::builder()
        .endpoint_id("http-test")
        .build()
        .unwrap()
}

async fn sensor(bmc: Arc<Bmc>, path: &str) -> Result<Acquired, AcquisitionFailure> {
    acquire(
        &SensorRead::new(endpoint(), path.to_owned().into(), bmc),
        Timestamp::new(0, 0).unwrap(),
    )
    .await
}

async fn logs(server: &Server, resources: &Resources) -> Result<Acquired, AcquisitionFailure> {
    acquire(
        &LogRead::new(endpoint(), resources.log.clone().into(), server.bmc()),
        Timestamp::new(0, 0).unwrap(),
    )
    .await
}

fn rule(id: &str, path: &str, action: Value) -> Value {
    let mut rule = json!({"id": id, "selector": {"OdataId": path}});
    rule["action"] = action;
    rule
}

#[tokio::test]
#[ignore = "requires a built standalone mock: BMC_MOCK_ROOT=... make test-bmc-mock"]
async fn standalone_http_boundary() {
    let mut server = Server::start().await;
    let resources = server.discover().await;
    nominal(&server, &resources).await;
    failures(&server, &resources).await;
    log_members(&server, &resources).await;
    log_snapshots(&server, &resources).await;
    log_pages_and_clearing(&server, &resources).await;
    bmc_reset_window(&server, &resources).await;
    log_cursor(&server, &resources).await;

    server
        .rules(&[rule(
            "deadline",
            &resources.log,
            json!({"Latency": {"mean": "35s", "jitter": "0ms"}}),
        )])
        .await;
    let started = Instant::now();
    let failure = tokio::time::timeout(Duration::from_secs(34), logs(&server, &resources))
        .await
        .expect("walk must stop before the injected response")
        .unwrap_err();
    assert_eq!(failure.class(), Failure::Timeout);
    assert!(started.elapsed() >= Duration::from_secs(29));

    server.stop();
    let failure = sensor(server.bmc(), &resources.sensor).await.unwrap_err();
    assert_eq!(failure.class(), Failure::Connectivity);
    assert_eq!(
        server
            .probe(
                &[("--sensor", &resources.sensor)],
                1,
                true,
                text(&server.fixture, "password")
            )
            .await,
        1
    );
}

async fn nominal(server: &Server, resources: &Resources) {
    let reading = server.fixture["reading"].as_f64().expect("fixture reading");
    server
        .rules(&[rule(
            "reading",
            &resources.sensor,
            json!({"JsonMerge": {"Reading": reading}}),
        )])
        .await;
    let result = sensor(server.bmc(), &resources.sensor).await.unwrap();
    assert!(result.issues().is_empty());
    let readings = result
        .batches()
        .iter()
        .find_map(|batch| match batch.payload() {
            Payload::Readings(readings) => Some(readings),
            _ => None,
        })
        .expect("sensor readings");
    assert_eq!(readings.samples().len(), 1);
    assert_eq!(
        readings.samples()[0].value(),
        &NumericValue::double(reading).unwrap()
    );
    assert!(result
        .batches()
        .iter()
        .any(|batch| matches!(batch.payload(), Payload::States(_))));
    let chassis = acquire(
        &ChassisRead::new(endpoint(), resources.chassis.clone().into(), server.bmc()),
        Timestamp::new(0, 0).unwrap(),
    )
    .await
    .unwrap();
    assert!(chassis.issues().is_empty());
    assert!(chassis
        .batches()
        .iter()
        .any(|batch| matches!(batch.payload(), Payload::Inventory(_))));
    let log = logs(server, resources).await.unwrap();
    assert!(log.issues().is_empty());
    assert!(log
        .batches()
        .iter()
        .any(|batch| matches!(batch.payload(), Payload::Logs(_))));
    // The actual CLI must also complete a mixed run. Payload assertions above
    // use the model, never the CLI's Debug rendering.
    assert_eq!(
        server
            .probe(
                &[
                    ("--sensor", &resources.sensor),
                    ("--chassis", &resources.chassis),
                    ("--log-service", &resources.log)
                ],
                6,
                true,
                text(&server.fixture, "password")
            )
            .await,
        0
    );
}

async fn failures(server: &Server, resources: &Resources) {
    server.rules(&[]).await;
    let wrong_password = format!("{}-invalid", text(&server.fixture, "password"));
    let bad_bmc = server.bmc_with(server.credentials_with(&wrong_password));
    assert_eq!(
        sensor(bad_bmc, &resources.sensor)
            .await
            .unwrap_err()
            .class(),
        Failure::Authentication
    );
    assert_eq!(
        server
            .probe(&[("--sensor", &resources.sensor)], 1, true, &wrong_password)
            .await,
        1
    );

    for (action, expected) in [
        (json!({"Status": 404}), Failure::Unsupported),
        (
            json!({"JsonMerge": {"Reading": "invalid"}}),
            Failure::Protocol,
        ),
        (json!({"Status": 503}), Failure::Device),
    ] {
        server
            .rules(&[rule("failure", &resources.sensor, action)])
            .await;
        let error = sensor(server.bmc(), &resources.sensor).await.unwrap_err();
        assert_eq!(error.class(), expected);
        if expected == Failure::Device {
            assert_eq!(error.retryable(), Some(true));
        }
    }
    for (strict, expected) in [(true, 1), (false, 0)] {
        assert_eq!(
            server
                .probe(
                    &[("--sensor", &resources.sensor)],
                    1,
                    strict,
                    text(&server.fixture, "password")
                )
                .await,
            expected
        );
    }
    server
        .rules(&[rule(
            "quiet",
            &resources.sensor,
            json!({"JsonMerge": {"Reading": null}}),
        )])
        .await;
    let quiet = sensor(server.bmc(), &resources.sensor).await.unwrap();
    assert!(quiet.issues().is_empty());
    let readings = quiet
        .batches()
        .iter()
        .find_map(|batch| match batch.payload() {
            Payload::Readings(readings) => Some(readings),
            _ => None,
        })
        .expect("quiet sensor still has a descriptor");
    assert_eq!(readings.descriptors().len(), 1);
    assert!(readings.samples().is_empty());
}

async fn log_members(server: &Server, resources: &Resources) {
    server.rules(&[]).await;
    let entries = server.get(&resources.entries).await;
    let entry = entries["Members"]
        .as_array()
        .expect("entries collection")
        .first()
        .expect("profile must have a seed log entry")
        .clone();
    let member = text(&entry, "@odata.id");
    let mut failure = rule("member-failure", member, json!({"Status": 404}));
    failure["remaining"] = json!(1);
    server.rules(&[failure.clone()]).await;
    let expanded = logs(server, resources).await.unwrap();
    assert!(expanded.issues().is_empty());
    assert!(!expanded.batches().is_empty());
    let pending = server.get("/Injection/rules").await;
    assert_eq!(
        pending[0]["remaining"], 1,
        "expanded members need no request"
    );

    server.rules(&[rule("links-only", &resources.entries,
        json!({"JsonMerge": {"Members": [{"@odata.id": member}], "Members@odata.count": 1}})), failure]).await;
    let linked = logs(server, resources).await.unwrap();
    assert!(linked.batches().is_empty());
    assert_eq!(linked.issues().len(), 1);
    assert_eq!(linked.issues()[0].path(), "Members[0]");
    let pending = server.get("/Injection/rules").await;
    assert!(pending
        .as_array()
        .unwrap()
        .iter()
        .all(|rule| rule["id"] != "member-failure"));
    // Reinstall the consumed failure for the independent CLI exit assertion.
    server.rules(&[rule("links-only", &resources.entries,
        json!({"JsonMerge": {"Members": [{"@odata.id": member}], "Members@odata.count": 1}})),
        rule("member-failure", member, json!({"Status": 404}))]).await;
    assert_eq!(
        server
            .probe(
                &[("--log-service", &resources.log)],
                1,
                true,
                text(&server.fixture, "password")
            )
            .await,
        1
    );
}

/// Every record of the log, sorted by the device's numeric entry id.
fn entry_ids(acquired: &Acquired) -> Vec<u64> {
    let mut ids: Vec<u64> = acquired
        .batches()
        .iter()
        .filter_map(|batch| match batch.payload() {
            Payload::Logs(logs) => Some(logs),
            _ => None,
        })
        .flat_map(|logs| logs.records().iter())
        .filter_map(|record| record.entry_id()?.parse().ok())
        .collect();
    ids.sort_unstable();
    ids
}

async fn log_pages_and_clearing(server: &Server, resources: &Resources) {
    server.rules(&[]).await;
    // The Dell profile serves fifty entries per page. Sixty-one entries make
    // two pages, and the walk must read both: the newest entries live on the
    // last page, which a first-page reader never sees.
    server.grow_log(resources, 60).await;
    let collection = server.get(&resources.entries).await;
    let total = collection["Members@odata.count"]
        .as_u64()
        .expect("the device counts its entries");
    assert!(
        collection.get("Members@odata.nextLink").is_some(),
        "the profile must page its log for this scenario"
    );
    let paged = logs(server, resources).await.unwrap();
    assert!(paged.issues().is_empty());
    let ids = entry_ids(&paged);
    assert_eq!(ids.len() as u64, total);
    assert_eq!(
        ids.last().copied(),
        Some(total - 1),
        "the newest entry is read"
    );

    // `ClearLog` empties the service and the device numbers from zero again:
    // the next record reuses an id an earlier poll already reported under
    // the same coverage scope, which is why `entry_id` alone is not a key.
    server.post(&resources.clear_log, &json!({})).await;
    let cleared = logs(server, resources).await.unwrap();
    assert!(cleared.batches().is_empty(), "an empty log is no batch");
    server.grow_log(resources, 2).await;
    let reused = logs(server, resources).await.unwrap();
    assert_eq!(entry_ids(&reused), [0, 1]);
    assert_eq!(
        reused.batches()[0].coverage().scope(),
        paged.batches()[0].coverage().scope(),
        "same service, same scope: only occurred_at tells the records apart"
    );
}

async fn log_cursor(server: &Server, resources: &Resources) {
    // One read polled repeatedly, as the dispatcher polls it: the first
    // poll ships the log, a poll with nothing new ships nothing, a poll
    // after growth ships exactly the new entries, and a wipe that refills
    // under reused ids still ships the refill.
    server.rules(&[]).await;
    let read = LogRead::new(endpoint(), resources.log.clone().into(), server.bmc());
    let at = Timestamp::new(0, 0).unwrap();
    let first = acquire(&read, at).await.unwrap();
    let shipped = entry_ids(&first);
    assert!(!shipped.is_empty());

    let unchanged = acquire(&read, at).await.unwrap();
    assert!(unchanged.batches().is_empty(), "nothing new, nothing sent");
    assert!(unchanged.issues().is_empty());

    server.grow_log(resources, 2).await;
    let grown = acquire(&read, at).await.unwrap();
    let new_ids = entry_ids(&grown);
    assert_eq!(new_ids.len(), 2);
    assert!(new_ids.iter().all(|id| !shipped.contains(id)));
    assert!(grown.issues().is_empty());

    server.post(&resources.clear_log, &json!({})).await;
    server.grow_log(resources, 2).await;
    let refilled = acquire(&read, at).await.unwrap();
    assert_eq!(entry_ids(&refilled), [0, 1], "reused ids are new records");
}

async fn bmc_reset_window(server: &Server, resources: &Resources) {
    // A BMC reset takes the endpoint offline for a while. The read during
    // that window is the device's own 503 — retryable, request-scoped — and
    // the log, including the reset's own entry, is there when it returns.
    let before = entry_ids(&logs(server, resources).await.unwrap()).len();
    server
        .post(
            &resources.manager_reset,
            &json!({"ResetType": "GracefulRestart"}),
        )
        .await;
    let offline = logs(server, resources).await.unwrap_err();
    assert_eq!(offline.class(), Failure::Device);
    assert_eq!(offline.retryable(), Some(true));
    tokio::time::sleep(BMC_RESET_WINDOW + Duration::from_millis(500)).await;
    let recovered = logs(server, resources).await.unwrap();
    assert_eq!(
        entry_ids(&recovered).len(),
        before + 1,
        "the reset logged itself"
    );
}

async fn log_snapshots(server: &Server, resources: &Resources) {
    server.rules(&[]).await;
    let entries = server.get(&resources.entries).await;
    let entry = entries["Members"]
        .as_array()
        .unwrap()
        .first()
        .expect("seed log entry")
        .clone();
    for messages in [
        vec![],
        vec!["first"],
        vec!["first", "second"],
        vec!["rotated"],
    ] {
        let mut snapshot = entries.clone();
        snapshot["Members"] = messages
            .iter()
            .enumerate()
            .map(|(index, message)| {
                let mut entry = entry.clone();
                entry["Id"] = json!(index.to_string());
                entry["@odata.id"] = json!(format!("{}/{index}", resources.entries));
                entry["Message"] = json!(message);
                entry
            })
            .collect();
        snapshot["Members@odata.count"] = json!(messages.len());
        server
            .rules(&[rule(
                "snapshot",
                &resources.entries,
                json!({"Replace": snapshot}),
            )])
            .await;
        let acquired = logs(server, resources).await.unwrap();
        assert!(acquired.issues().is_empty());
        let mut observed = acquired
            .batches()
            .iter()
            .flat_map(|batch| {
                let Payload::Logs(logs) = batch.payload() else {
                    panic!("logs payload")
                };
                logs.records().iter().map(LogRecord::message)
            })
            .collect::<Vec<_>>();
        observed.sort_unstable();
        assert_eq!(observed, messages);
    }

    let mut broken = entries;
    let mut bad_entry = entry.clone();
    bad_entry.as_object_mut().unwrap().remove("Message");
    broken["Members"] = json!([entry, bad_entry]);
    broken["Members@odata.count"] = json!(2);
    server
        .rules(&[rule(
            "malformed",
            &resources.entries,
            json!({"Replace": broken}),
        )])
        .await;
    let partial = logs(server, resources).await.unwrap();
    assert_eq!(partial.issues().len(), 1);
    assert_eq!(partial.batches().len(), 1);
    let Payload::Logs(logs) = partial.batches()[0].payload() else {
        panic!("logs payload")
    };
    assert_eq!(logs.records().len(), 1);
}
